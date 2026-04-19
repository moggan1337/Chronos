//! Schema definition for time-series tables
//! 
//! Defines the structure of time-series data including time columns, tags, and fields.

use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema, DataType};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    #[error("Field not found: {0}")]
    FieldNotFound(String),
    #[error("Duplicate field: {0}")]
    DuplicateField(String),
}

/// A field in a schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    metadata: Option<std::collections::HashMap<String, String>>,
}

impl Field {
    /// Create a new field
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable: true,
            metadata: None,
        }
    }
    
    /// Create a non-nullable field
    pub fn new_required(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable: false,
            metadata: None,
        }
    }
    
    /// Get field name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get data type
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
    
    /// Check if nullable
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
    
    /// Get metadata
    pub fn metadata(&self) -> Option<&std::collections::HashMap<String, String>> {
        self.metadata.as_ref()
    }
    
    /// Set metadata
    pub fn with_metadata(mut self, metadata: std::collections::HashMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }
    
    /// Convert to Arrow field
    pub fn to_arrow_field(&self) -> ArrowField {
        ArrowField::new(&self.name, self.data_type.clone(), self.nullable)
            .with_metadata(self.metadata.clone().unwrap_or_default())
    }
    
    /// Convert from Arrow field
    pub fn from_arrow_field(field: &ArrowField) -> Self {
        Self {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            metadata: if field.metadata().is_empty() {
                None
            } else {
                Some(field.metadata().clone())
            },
        }
    }
}

/// Schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    name: String,
    fields: Vec<Field>,
}

impl Schema {
    /// Create a new schema
    pub fn new(fields: Vec<Field>) -> Self {
        Self {
            name: "default".to_string(),
            fields,
        }
    }
    
    /// Create a named schema
    pub fn named(name: &str, fields: Vec<Field>) -> Self {
        Self {
            name: name.to_string(),
            fields,
        }
    }
    
    /// Get schema name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Set schema name
    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    
    /// Get all fields
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
    
    /// Get number of fields
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }
    
    /// Add a field
    pub fn add_field(&mut self, field: Field) -> Result<(), SchemaError> {
        if self.fields.iter().any(|f| f.name == field.name) {
            return Err(SchemaError::DuplicateField(field.name.clone()));
        }
        self.fields.push(field);
        Ok(())
    }
    
    /// Get field by index
    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }
    
    /// Get field by name
    pub fn field_with_name(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }
    
    /// Get field index
    pub fn index_of(&self, name: &str) -> Result<usize, SchemaError> {
        self.fields
            .iter()
            .position(|f| f.name == name)
            .ok_or_else(|| SchemaError::FieldNotFound(name.to_string()))
    }
    
    /// Check if schema contains a field
    pub fn contains(&self, name: &str) -> bool {
        self.fields.iter().any(|f| f.name == name)
    }
    
    /// Merge another schema into this one
    pub fn merge(&mut self, other: &Schema) -> Result<(), SchemaError> {
        for field in &other.fields {
            if !self.contains(field.name()) {
                self.fields.push(field.clone());
            }
        }
        Ok(())
    }
    
    /// Convert to Arrow schema
    pub fn to_arrow_schema(&self) -> ArrowSchema {
        ArrowSchema::new(self.fields.iter().map(|f| f.to_arrow_field()).collect())
    }
    
    /// Convert from Arrow schema
    pub fn from_arrow_schema(schema: &ArrowSchema) -> Self {
        Self {
            name: "from_arrow".to_string(),
            fields: schema.fields().iter().map(Field::from_arrow_field).collect(),
        }
    }
    
    /// Parse schema from SQL-like string
    pub fn parse(sql: &str) -> Result<Self, SchemaError> {
        let mut fields = Vec::new();
        
        for part in sql.split(',') {
            let part = part.trim();
            let parts: Vec<&str> = part.split_whitespace().collect();
            
            if parts.len() < 2 {
                continue;
            }
            
            let name = parts[0];
            let type_str = parts[1].to_uppercase();
            
            let data_type = match type_str.as_str() {
                "INT64" | "BIGINT" => DataType::Int64,
                "FLOAT64" | "DOUBLE" => DataType::Float64,
                "BOOLEAN" | "BOOL" => DataType::Boolean,
                "STRING" | "TEXT" => DataType::Utf8,
                "TIMESTAMP" => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                "DATE" => DataType::Date32,
                _ => DataType::Utf8,
            };
            
            let nullable = !parts.contains(&"NOT");
            
            let field = if nullable {
                Field::new(name, data_type)
            } else {
                Field::new_required(name, data_type)
            };
            
            fields.push(field);
        }
        
        Ok(Self::new(fields))
    }
    
    /// Validate the schema
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.fields.is_empty() {
            return Err(SchemaError::InvalidSchema("Schema must have at least one field".to_string()));
        }
        
        // Check for duplicate names
        let mut names = std::collections::HashSet::new();
        for field in &self.fields {
            if names.contains(&field.name) {
                return Err(SchemaError::DuplicateField(field.name.clone()));
            }
            names.insert(field.name.clone());
        }
        
        Ok(())
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new(vec![
            Field::new("time", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)),
        ])
    }
}

/// Schema builder for fluent construction
pub struct SchemaBuilder {
    schema: Schema,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new(name: &str) -> Self {
        Self {
            schema: Schema::named(name, Vec::new()),
        }
    }
    
    /// Add a timestamp field (required for time-series)
    pub fn timestamp(mut self, name: &str) -> Self {
        self.schema.add_field(Field::new(
            name,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )).ok();
        self
    }
    
    /// Add a tag field (indexed)
    pub fn tag(mut self, name: &str) -> Self {
        self.schema.add_field(Field::new(name, DataType::Utf8)).ok();
        self
    }
    
    /// Add a field
    pub fn field(mut self, name: &str, data_type: DataType) -> Self {
        self.schema.add_field(Field::new(name, data_type)).ok();
        self
    }
    
    /// Add a required field
    pub fn required_field(mut self, name: &str, data_type: DataType) -> Self {
        self.schema.add_field(Field::new_required(name, data_type)).ok();
        self
    }
    
    /// Build the schema
    pub fn build(self) -> Schema {
        self.schema
    }
}
