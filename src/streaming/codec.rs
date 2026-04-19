//! Message codecs for streaming
//! 
//! Implements various serialization formats for stream messages:
//! - JSON
//! - CSV
//! - Protocol Buffers (schema)
//! - MessagePack

use crate::streaming::{Message, StreamingError};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CodecError {
    #[error("Encoding error: {0}")]
    Encoding(String),
    #[error("Decoding error: {0}")]
    Decoding(String),
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
}

/// Trait for message codecs
pub trait Codec: Send + Sync {
    /// Encode a message
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError>;
    
    /// Decode a message
    fn decode<T: DeserializeOwned>(&self, data: &[u8]) -> Result<T, CodecError>;
    
    /// Encode a batch of messages
    fn encode_batch<T: Serialize>(&self, values: &[T]) -> Result<Vec<Vec<u8>>, CodecError> {
        values.iter().map(|v| self.encode(v)).collect()
    }
    
    /// Decode a batch of messages
    fn decode_batch<T: DeserializeOwned>(&self, data: &[Vec<u8>]) -> Result<Vec<T>, CodecError> {
        data.iter().map(|v| self.decode(v)).collect()
    }
    
    /// Get codec name
    fn name(&self) -> &'static str;
}

/// JSON codec
pub struct JsonCodec;

impl JsonCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Codec for JsonCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(value)
            .map_err(|e| CodecError::Encoding(e.to_string()))
    }
    
    fn decode<T: DeserializeOwned>(&self, data: &[u8]) -> Result<T, CodecError> {
        serde_json::from_slice(data)
            .map_err(|e| CodecError::Decoding(e.to_string()))
    }
    
    fn name(&self) -> &'static str {
        "json"
    }
}

impl Default for JsonCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// CSV codec
pub struct CsvCodec {
    delimiter: u8,
    headers: bool,
}

impl CsvCodec {
    pub fn new() -> Self {
        Self {
            delimiter: b',',
            headers: true,
        }
    }
    
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }
    
    pub fn with_headers(mut self, has_headers: bool) -> Self {
        self.headers = has_headers;
        self
    }
}

impl Codec for CsvCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        let mut wtr = csv::Writer::from_writer(vec![]);
        wtr.serialize(value)
            .map_err(|e| CodecError::Encoding(e.to_string()))?;
        wtr.into_inner()
            .map_err(|e| CodecError::Encoding(e.to_string()))
    }
    
    fn decode<T: DeserializeOwned>(&self, data: &[u8]) -> Result<T, CodecError> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.headers)
            .from_reader(data);
        
        let record = rdr.records()
            .next()
            .ok_or_else(|| CodecError::Decoding("No records".to_string()))?
            .map_err(|e| CodecError::Decoding(e.to_string()))?;
        
        // This is simplified - real implementation would handle dynamic deserialization
        Ok(serde_json::from_slice(record.as_slice())
            .map_err(|e| CodecError::Decoding(e.to_string()))?)
    }
    
    fn name(&self) -> &'static str {
        "csv"
    }
}

impl Default for CsvCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// MessagePack codec
pub struct MessagePackCodec;

impl MessagePackCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Codec for MessagePackCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        rmp_serde::serialize(value)
            .map_err(|e| CodecError::Encoding(e.to_string()))
    }
    
    fn decode<T: DeserializeOwned>(&self, data: &[u8]) -> Result<T, CodecError> {
        rmp_serde::deserialize(data)
            .map_err(|e| CodecError::Decoding(e.to_string()))
    }
    
    fn name(&self) -> &'static str {
        "msgpack"
    }
}

impl Default for MessagePackCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// Protocol Buffers-like codec (schema-based)
pub struct ProtobufCodec {
    schema: ProtobufSchema,
}

#[derive(Debug, Clone)]
pub struct ProtobufSchema {
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    pub field_type: FieldType,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub enum FieldType {
    Int64,
    Float64,
    String,
    Bool,
    Bytes,
}

impl ProtobufCodec {
    pub fn new(schema: ProtobufSchema) -> Self {
        Self { schema }
    }
    
    pub fn encode_message(&self, values: &[serde_json::Value]) -> Result<Vec<u8>, CodecError> {
        let mut buf = Vec::new();
        
        for field in &self.schema.fields {
            if field.index < values.len() {
                let value = &values[field.index];
                self.encode_field(&mut buf, field, value)?;
            }
        }
        
        Ok(buf)
    }
    
    fn encode_field(&self, buf: &mut Vec<u8>, field: &FieldDef, value: &serde_json::Value) 
        -> Result<(), CodecError> 
    {
        match field.field_type {
            FieldType::Int64 => {
                if let Some(n) = value.as_i64() {
                    buf.extend_from_slice(&n.to_le_bytes());
                }
            }
            FieldType::Float64 => {
                if let Some(n) = value.as_f64() {
                    buf.extend_from_slice(&n.to_le_bytes());
                }
            }
            FieldType::String => {
                if let Some(s) = value.as_str() {
                    let bytes = s.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
            }
            FieldType::Bool => {
                buf.push(if value.as_bool().unwrap_or(false) { 1 } else { 0 });
            }
            FieldType::Bytes => {
                if let Some(b) = value.as_str() {
                    let bytes = b.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
            }
        }
        Ok(())
    }
    
    pub fn decode_message(&self, data: &[u8]) -> Result<Vec<serde_json::Value>, CodecError> {
        let mut values = Vec::new();
        let mut pos = 0;
        
        for field in &self.schema.fields {
            if pos >= data.len() {
                values.push(serde_json::Value::Null);
                continue;
            }
            
            let value = match field.field_type {
                FieldType::Int64 => {
                    if pos + 8 <= data.len() {
                        let n = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                        pos += 8;
                        serde_json::json!(n)
                    } else {
                        serde_json::Value::Null
                    }
                }
                FieldType::Float64 => {
                    if pos + 8 <= data.len() {
                        let n = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                        pos += 8;
                        serde_json::json!(n)
                    } else {
                        serde_json::Value::Null
                    }
                }
                FieldType::String | FieldType::Bytes => {
                    if pos + 4 <= data.len() {
                        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                        pos += 4;
                        if pos + len <= data.len() {
                            let s = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                            pos += len;
                            serde_json::json!(s)
                        } else {
                            serde_json::Value::Null
                        }
                    } else {
                        serde_json::Value::Null
                    }
                }
                FieldType::Bool => {
                    if pos < data.len() {
                        let b = data[pos] != 0;
                        pos += 1;
                        serde_json::json!(b)
                    } else {
                        serde_json::Value::Null
                    }
                }
            };
            
            values.push(value);
        }
        
        Ok(values)
    }
}

impl Codec for ProtobufCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        let json = serde_json::to_value(value)
            .map_err(|e| CodecError::Encoding(e.to_string()))?;
        
        let values: Vec<serde_json::Value> = if let Some(arr) = json.as_array() {
            arr.clone()
        } else {
            vec![json]
        };
        
        self.encode_message(&values)
    }
    
    fn decode<T: DeserializeOwned>(&self, data: &[u8]) -> Result<T, CodecError> {
        let values = self.decode_message(data)?;
        let json = serde_json::json!({ 
            "fields": values
        });
        
        serde_json::from_value(json)
            .map_err(|e| CodecError::Decoding(e.to_string()))
    }
    
    fn name(&self) -> &'static str {
        "protobuf"
    }
}

/// Codec registry
pub struct CodecRegistry {
    codecs: std::collections::HashMap<String, Box<dyn Codec>>,
}

impl CodecRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            codecs: std::collections::HashMap::new(),
        };
        
        registry.register("json", Box::new(JsonCodec::new()));
        registry.register("csv", Box::new(CsvCodec::new()));
        registry.register("msgpack", Box::new(MessagePackCodec::new()));
        
        registry
    }
    
    pub fn register(&mut self, name: &str, codec: Box<dyn Codec>) {
        self.codecs.insert(name.to_string(), codec);
    }
    
    pub fn get(&self, name: &str) -> Option<&dyn Codec> {
        self.codecs.get(name).map(|c| c.as_ref())
    }
    
    pub fn list(&self) -> Vec<&'static str> {
        self.codecs.keys()
            .map(|s| s.as_str())
            .collect()
    }
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}
