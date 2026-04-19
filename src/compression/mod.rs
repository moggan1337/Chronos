//! Compression module
//! 
//! Implements various compression algorithms optimized for time-series data:
//! - Gorilla (TSZ) encoding for float values
//! - CHIMP encoding for integer values
//! - Zstd and Snappy for general purpose compression

pub mod gorilla;
pub mod chimp;
pub mod zstd;

pub use gorilla::{gorilla_encode, gorilla_decode};
pub use chimp::{chimp_encode, chimp_decode};
pub use zstd::{zstd_compress, zstd_decompress};

use crate::storage::column::ColumnData;
use crate::storage::column::ColumnType;
use serde::{Serialize, Deserialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompressionError {
    #[error("Compression failed: {0}")]
    CompressFailed(String),
    #[error("Decompression failed: {0}")]
    DecompressFailed(String),
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Compression type enumeration
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression
    None,
    /// Gorilla encoding (TSZ) - optimized for float time-series
    Gorilla,
    /// CHIMP encoding - optimized for integer time-series
    Chimp,
    /// Zstandard compression
    Zstd,
    /// Snappy compression
    Snappy,
    /// LZ4 compression
    Lz4,
    /// Auto-select based on data characteristics
    Auto,
}

impl CompressionType {
    /// Get compression ratio hint
    pub fn compression_ratio_hint(&self) -> f64 {
        match self {
            CompressionType::None => 1.0,
            CompressionType::Gorilla => 0.15,
            CompressionType::Chimp => 0.20,
            CompressionType::Zstd => 0.25,
            CompressionType::Snappy => 0.35,
            CompressionType::Lz4 => 0.30,
            CompressionType::Auto => 0.25,
        }
    }
    
    /// Check if this is a specialized time-series encoding
    pub fn is_timeseries_encoding(&self) -> bool {
        matches!(self, CompressionType::Gorilla | CompressionType::Chimp)
    }
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::Zstd
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Gorilla => write!(f, "gorilla"),
            CompressionType::Chimp => write!(f, "chimp"),
            CompressionType::Zstd => write!(f, "zstd"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Lz4 => write!(f, "lz4"),
            CompressionType::Auto => write!(f, "auto"),
        }
    }
}

/// Compression statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_type: CompressionType,
    pub ratio: f64,
    pub encoding_time_ms: u64,
    pub decoding_time_ms: u64,
}

impl CompressionStats {
    /// Calculate compression ratio
    pub fn ratio(&self) -> f64 {
        if self.original_size == 0 {
            return 1.0;
        }
        self.compressed_size as f64 / self.original_size as f64
    }
}

/// Compressor trait for custom compression implementations
pub trait CompressorTrait: Send + Sync {
    /// Compress data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError>;
    
    /// Decompress data
    fn decompress(&self, data: &[u8], expected_size: usize) -> Result<Vec<u8>, CompressionError>;
    
    /// Get compression type
    fn compression_type(&self) -> CompressionType;
}

/// Compressor for managing multiple compression algorithms
pub struct Compressor {
    default_type: CompressionType,
}

impl Compressor {
    /// Create a new compressor
    pub fn new() -> Self {
        Self {
            default_type: CompressionType::default(),
        }
    }
    
    /// Create with default compression type
    pub fn with_type(compression_type: CompressionType) -> Self {
        Self {
            default_type: compression_type,
        }
    }
    
    /// Set default compression type
    pub fn set_default_type(&mut self, compression_type: CompressionType) {
        self.default_type = compression_type;
    }
    
    /// Compress data with specified type
    pub fn compress(&self, data: &[u8], compression_type: CompressionType) -> Result<Vec<u8>, CompressionError> {
        match compression_type {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Gorilla => {
                // For float data
                let values: Vec<f64> = bincode::deserialize(data)
                    .map_err(|e| CompressionError::InvalidData(e.to_string()))?;
                Ok(gorilla_encode_float(&values))
            }
            CompressionType::Chimp => {
                let values: Vec<i64> = bincode::deserialize(data)
                    .map_err(|e| CompressionError::InvalidData(e.to_string()))?;
                Ok(chimp_encode(&values))
            }
            CompressionType::Zstd => zstd_compress(data, 3),
            CompressionType::Snappy => Ok(snappy_compress(data)),
            CompressionType::Lz4 => Ok(lz4_compress(data)),
            CompressionType::Auto => zstd_compress(data, 3),
        }
    }
    
    /// Decompress data
    pub fn decompress(&self, data: &[u8], original_size: usize, compression_type: CompressionType) 
        -> Result<Vec<u8>, CompressionError> 
    {
        match compression_type {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Gorilla => {
                let values = gorilla_decode_float(data, original_size)?;
                Ok(bincode::serialize(&values)
                    .map_err(|e| CompressionError::InvalidData(e.to_string()))?)
            }
            CompressionType::Chimp => {
                let values = chimp_decode(data, original_size)?;
                Ok(bincode::serialize(&values)
                    .map_err(|e| CompressionError::InvalidData(e.to_string()))?)
            }
            CompressionType::Zstd => zstd_decompress(data, original_size),
            CompressionType::Snappy => Ok(snappy_decompress(data, original_size)),
            CompressionType::Lz4 => Ok(lz4_decompress(data, original_size)),
            CompressionType::Auto => zstd_decompress(data, original_size),
        }
    }
    
    /// Compress column data
    pub fn compress_column(&self, column_data: &ColumnData) -> Result<CompressedColumn, CompressionError> {
        match column_data {
            ColumnData::Float64(values) => {
                let compressed = gorilla_encode_float(values);
                Ok(CompressedColumn {
                    data: compressed,
                    compression_type: CompressionType::Gorilla,
                    original_size: values.len() * 8,
                    num_values: values.len(),
                })
            }
            ColumnData::Int64(values) => {
                let compressed = chimp_encode(values);
                Ok(CompressedColumn {
                    data: compressed,
                    compression_type: CompressionType::Chimp,
                    original_size: values.len() * 8,
                    num_values: values.len(),
                })
            }
            ColumnData::Timestamp(values) => {
                let compressed = chimp_encode(values);
                Ok(CompressedColumn {
                    data: compressed,
                    compression_type: CompressionType::Chimp,
                    original_size: values.len() * 8,
                    num_values: values.len(),
                })
            }
            _ => Err(CompressionError::InvalidData("Unsupported type".to_string())),
        }
    }
    
    /// Decompress column data
    pub fn decompress_column(&self, compressed: &CompressedColumn) -> Result<ColumnData, CompressionError> {
        match compressed.compression_type {
            CompressionType::Gorilla => {
                let values = gorilla_decode_float(&compressed.data, compressed.num_values)?;
                Ok(ColumnData::Float64(values))
            }
            CompressionType::Chimp => {
                let values = chimp_decode(&compressed.data, compressed.num_values)?;
                Ok(ColumnData::Int64(values))
            }
            _ => Err(CompressionError::InvalidData("Unknown compression type".to_string())),
        }
    }
}

impl Default for Compressor {
    fn default() -> Self {
        Self::new()
    }
}

/// Compressed column data
#[derive(Debug, Clone)]
pub struct CompressedColumn {
    pub data: Vec<u8>,
    pub compression_type: CompressionType,
    pub original_size: usize,
    pub num_values: usize,
}

impl CompressedColumn {
    /// Get compression ratio
    pub fn ratio(&self) -> f64 {
        if self.original_size == 0 {
            return 1.0;
        }
        self.data.len() as f64 / self.original_size as f64
    }
}

/// Encode float column data using Gorilla encoding
fn gorilla_encode_float(values: &[Option<f64>]) -> Vec<u8> {
    let mut output = Vec::new();
    let mut prev_value: Option<f64> = None;
    let mut prev_timestamp: Option<i64> = None;
    let mut first = true;
    
    for (i, &value) in values.iter().enumerate() {
        if first {
            // First value: store 1 bit (0) + value + timestamp
            if let Some(v) = value {
                let bits = encode_value_bits(v);
                write_bits(&mut output, &[BitChunk::new(0, 1)]);
                write_bits(&mut output, &encode_value_bits_as_chunks(v));
            } else {
                write_bits(&mut output, &[BitChunk::new(1, 1)]); // null
            }
            first = false;
        } else if let Some(prev) = prev_value {
            if let Some(v) = value {
                let xor = v.to_bits() ^ prev.to_bits();
                if xor == 0 {
                    // Same value: write 10
                    write_bits(&mut output, &[BitChunk::new(2, 2)]);
                } else {
                    // Different value
                    let leading = leading_zeros(xor);
                    let trailing = trailing_zeros(xor);
                    
                    if leading >= 10 && trailing >= 10 {
                        // Store 11 + value
                        write_bits(&mut output, &[BitChunk::new(3, 2)]);
                        write_bits(&mut output, &encode_value_bits_as_chunks(v));
                    } else {
                        // Store 01 + block
                        write_bits(&mut output, &[BitChunk::new(1, 2)]);
                        // Store leading (4 bits), trailing (4 bits), value (variable)
                        let block_size = 64 - leading - trailing;
                        write_bits(&mut output, &[BitChunk::new(leading as u64, 6)]);
                        write_bits(&mut output, &[BitChunk::new(trailing as u64, 6)]);
                        let significant = (xor >> trailing) & ((1u64 << block_size) - 1);
                        write_bits(&mut output, &[BitChunk::new(significant, block_size as u8)]);
                    }
                }
            }
            // Handle nulls
            let _ = prev_timestamp;
        }
        
        prev_value = value;
        prev_timestamp = Some(i as i64);
    }
    
    output
}

/// Decode float column data from Gorilla encoding
fn gorilla_decode_float(data: &[u8], num_values: usize) -> Result<Vec<Option<f64>>, CompressionError> {
    let mut values = Vec::with_capacity(num_values);
    let mut bits_reader = BitReader::new(data);
    let mut prev_value: Option<f64> = None;
    let mut first = true;
    
    while values.len() < num_values {
        if first {
            let flag = bits_reader.read_bits(1)?;
            if flag == 0 {
                let v = bits_reader.read_f64()?;
                values.push(Some(v));
                prev_value = Some(v);
            } else {
                values.push(None);
            }
            first = false;
        } else if let Some(prev) = prev_value {
            let flag = bits_reader.read_bits(2)?;
            
            match flag {
                0 => {
                    // Same value
                    values.push(Some(prev));
                }
                1 => {
                    // XOR block
                    let leading = bits_reader.read_bits(6)? as usize;
                    let trailing = bits_reader.read_bits(6)? as usize;
                    let block_size = 64 - leading - trailing;
                    let significant = bits_reader.read_bits(block_size)?;
                    let xor = (significant << trailing);
                    let value_bits = prev.to_bits() ^ xor;
                    let value = f64::from_bits(value_bits);
                    values.push(Some(value));
                    prev_value = Some(value);
                }
                2 => {
                    // Different value
                    let v = bits_reader.read_f64()?;
                    values.push(Some(v));
                    prev_value = Some(v);
                }
                3 => {
                    // Raw value
                    let v = bits_reader.read_f64()?;
                    values.push(Some(v));
                    prev_value = Some(v);
                }
                _ => unreachable!(),
            }
        }
    }
    
    Ok(values)
}

/// Bit chunk for writing
#[derive(Debug, Clone, Copy)]
pub struct BitChunk {
    pub value: u64,
    pub bits: u8,
}

impl BitChunk {
    pub fn new(value: u64, bits: u8) -> Self {
        Self { value, bits }
    }
}

/// Write bits to output buffer
fn write_bits(output: &mut Vec<u8>, chunks: &[BitChunk]) {
    for chunk in chunks {
        for i in (0..chunk.bits).rev() {
            let bit = (chunk.value >> i) & 1;
            output.push(bit as u8);
        }
    }
}

/// Bit reader for decoding
struct BitReader<'a> {
    data: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, bit_pos: 0 }
    }
    
    fn read_bits(&mut self, num_bits: usize) -> Result<u64, CompressionError> {
        let mut value = 0u64;
        for _ in 0..num_bits {
            let byte_pos = self.bit_pos / 8;
            let bit_offset = 7 - (self.bit_pos % 8);
            
            if byte_pos >= self.data.len() {
                return Err(CompressionError::InvalidData("Unexpected end of data".to_string()));
            }
            
            let bit = (self.data[byte_pos] >> bit_offset) & 1;
            value = (value << 1) | bit;
            self.bit_pos += 1;
        }
        Ok(value)
    }
    
    fn read_f64(&mut self) -> Result<f64, CompressionError> {
        let bits = self.read_bits(64)?;
        Ok(f64::from_bits(bits))
    }
}

/// Count leading zeros
fn leading_zeros(x: u64) -> usize {
    x.leading_zeros() as usize
}

/// Count trailing zeros
fn trailing_zeros(x: u64) -> usize {
    x.trailing_zeros() as usize
}

/// Encode value as bit chunks
fn encode_value_bits_as_chunks(value: f64) -> Vec<BitChunk> {
    vec![BitChunk::new(value.to_bits(), 64)]
}

/// Encode value bits
fn encode_value_bits(value: f64) -> Vec<BitChunk> {
    encode_value_bits_as_chunks(value)
}

// Snappy helpers
fn snappy_compress(data: &[u8]) -> Vec<u8> {
    snappy::compress(data)
}

fn snappy_decompress(data: &[u8], _original_size: usize) -> Vec<u8> {
    snappy::decompress(data).unwrap_or_else(|_| data.to_vec())
}

// LZ4 helpers
fn lz4_compress(data: &[u8]) -> Vec<u8> {
    let mut compressed = Vec::with_capacity(data.len());
    let size = lz4::block::compress_to_buffer(data, None, true, &mut compressed).unwrap_or(0);
    compressed.truncate(size);
    compressed
}

fn lz4_decompress(data: &[u8], original_size: usize) -> Vec<u8> {
    let mut decompressed = vec![0u8; original_size];
    let size = lz4::block::decompress_to_buffer(data, &mut decompressed).unwrap_or(0);
    decompressed.truncate(size);
    decompressed
}

// Import bincode for serialization
mod bincode {
    pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
        bincode::serde::encode_to_vec(value, bincode::config::standard())
    }
    
    pub fn deserialize<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, bincode::Error> {
        bincode::serde::decode_from_slice(data, bincode::config::standard()).map(|(v, _)| v)
    }
}
