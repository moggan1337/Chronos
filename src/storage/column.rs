//! Column storage implementation
//! 
//! Provides column-oriented storage with SIMD-optimized operations.

use arrow::array::{Array, ArrayRef, PrimitiveArray, BooleanArray};
use arrow::datatypes::{DataType, ArrowNativeType};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ColumnError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

/// Column type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
    Float64,
    Boolean,
    String,
    Timestamp,
}

impl ColumnType {
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::String => DataType::Utf8,
            ColumnType::Timestamp => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        }
    }
    
    pub fn from_arrow_type(dt: &DataType) -> Option<Self> {
        match dt {
            DataType::Int64 => Some(ColumnType::Int64),
            DataType::Float64 => Some(ColumnType::Float64),
            DataType::Boolean => Some(ColumnType::Boolean),
            DataType::Utf8 => Some(ColumnType::String),
            DataType::Timestamp(_) => Some(ColumnType::Timestamp),
            _ => None,
        }
    }
}

/// A column in columnar storage
#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data_type: ColumnType,
    values: ColumnData,
    null_count: usize,
    compression_ratio: f64,
}

#[derive(Debug, Clone)]
enum ColumnData {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    Timestamp(Vec<Option<i64>>),
}

impl Column {
    /// Create a new column
    pub fn new(name: &str, data_type: DataType) -> Self {
        let col_type = ColumnType::from_arrow_type(&data_type)
            .unwrap_or(ColumnType::String);
        
        Self {
            name: name.to_string(),
            data_type: col_type,
            values: ColumnData::empty(col_type),
            null_count: 0,
            compression_ratio: 1.0,
        }
    }
    
    /// Get column name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get data type
    pub fn data_type(&self) -> &ColumnType {
        &self.data_type
    }
    
    /// Get number of values
    pub fn len(&self) -> usize {
        match &self.values {
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::Boolean(v) => v.len(),
            ColumnData::String(v) => v.len(),
            ColumnData::Timestamp(v) => v.len(),
        }
    }
    
    /// Check if column is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Append an Arrow array to the column
    pub fn append_array(&mut self, array: ArrayRef) -> Result<(), ColumnError> {
        use arrow::array::*;
        
        match &mut self.values {
            ColumnData::Int64(v) => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ColumnError::TypeMismatch("Expected Int64".to_string()))?;
                for i in 0..arr.len() {
                    v.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
                }
            }
            ColumnData::Float64(v) => {
                let arr = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| ColumnError::TypeMismatch("Expected Float64".to_string()))?;
                for i in 0..arr.len() {
                    v.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
                }
            }
            ColumnData::Boolean(v) => {
                let arr = array.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| ColumnError::TypeMismatch("Expected Boolean".to_string()))?;
                for i in 0..arr.len() {
                    v.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
                }
            }
            ColumnData::String(v) => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| ColumnError::TypeMismatch("Expected String".to_string()))?;
                for i in 0..arr.len() {
                    v.push(if arr.is_null(i) { None } else { Some(arr.value(i).to_string()) });
                }
            }
            ColumnData::Timestamp(v) => {
                let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| ColumnError::TypeMismatch("Expected Timestamp".to_string()))?;
                for i in 0..arr.len() {
                    v.push(if arr.is_null(i) { None } else { Some(arr.value(i)) });
                }
            }
        }
        
        Ok(())
    }
    
    /// Convert to Arrow array
    pub fn to_array(&self) -> Result<ArrayRef, ColumnError> {
        use arrow::array::*;
        
        let array: ArrayRef = match &self.values {
            ColumnData::Int64(v) => {
                let values: Vec<i64> = v.iter().map(|x| x.unwrap_or(0)).collect();
                let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
                Arc::new(build_int64_array(&values, &nulls)?)
            }
            ColumnData::Float64(v) => {
                let values: Vec<f64> = v.iter().map(|x| x.unwrap_or(0.0)).collect();
                let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
                Arc::new(build_float64_array(&values, &nulls)?)
            }
            ColumnData::Boolean(v) => {
                let values: Vec<bool> = v.iter().map(|x| x.unwrap_or(false)).collect();
                let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
                Arc::new(BooleanArray::from(vec![(values.as_slice(), &nulls)]))
            }
            ColumnData::String(v) => {
                let values: Vec<&str> = v.iter().map(|x| x.as_deref().unwrap_or("")).collect();
                Arc::new(StringArray::from(values))
            }
            ColumnData::Timestamp(v) => {
                let values: Vec<i64> = v.iter().map(|x| x.unwrap_or(0)).collect();
                let nulls: Vec<bool> = v.iter().map(|x| x.is_none()).collect();
                Arc::new(build_timestamp_array(&values, &nulls)?)
            }
        };
        
        Ok(array)
    }
    
    /// Merge another column into this one
    pub fn merge(&mut self, other: &Column) -> Result<(), ColumnError> {
        if self.name != other.name || self.data_type != other.data_type {
            return Err(ColumnError::TypeMismatch("Column mismatch".to_string()));
        }
        
        match (&mut self.values, &other.values) {
            (ColumnData::Int64(v1), ColumnData::Int64(v2)) => v1.extend(v2.clone()),
            (ColumnData::Float64(v1), ColumnData::Float64(v2)) => v1.extend(v2.clone()),
            (ColumnData::Boolean(v1), ColumnData::Boolean(v2)) => v1.extend(v2.clone()),
            (ColumnData::String(v1), ColumnData::String(v2)) => v1.extend(v2.clone()),
            (ColumnData::Timestamp(v1), ColumnData::Timestamp(v2)) => v1.extend(v2.clone()),
            _ => return Err(ColumnError::TypeMismatch("Data mismatch".to_string())),
        }
        
        self.null_count += other.null_count;
        Ok(())
    }
    
    /// Calculate basic statistics
    pub fn stats(&self) -> ColumnStats {
        match &self.values {
            ColumnData::Int64(v) => {
                let valid: Vec<i64> = v.iter().filter_map(|x| *x).collect();
                ColumnStats {
                    count: v.len(),
                    null_count: v.len() - valid.len(),
                    min: valid.iter().copied().min(),
                    max: valid.iter().copied().max(),
                    mean: if valid.is_empty() { None } else { Some(valid.iter().sum::<i64>() as f64 / valid.len() as f64) },
                }
            }
            ColumnData::Float64(v) => {
                let valid: Vec<f64> = v.iter().filter_map(|x| *x).collect();
                ColumnStats {
                    count: v.len(),
                    null_count: v.len() - valid.len(),
                    min: valid.iter().copied().fold(f64::INFINITY, f64::min).into(),
                    max: valid.iter().copied().fold(f64::NEG_INFINITY, f64::max).into(),
                    mean: if valid.is_empty() { None } else { Some(valid.iter().sum::<f64>() / valid.len() as f64) },
                }
            }
            ColumnData::Boolean(v) => ColumnStats {
                count: v.len(),
                null_count: v.iter().filter(|x| x.is_none()).count(),
                min: v.iter().any(|x| x == Some(true)).then(|| 0.0),
                max: v.iter().any(|x| x == Some(false)).then(|| 1.0),
                mean: None,
            },
            ColumnData::String(v) => ColumnStats {
                count: v.len(),
                null_count: v.iter().filter(|x| x.is_none()).count(),
                min: None,
                max: None,
                mean: None,
            },
            ColumnData::Timestamp(v) => {
                let valid: Vec<i64> = v.iter().filter_map(|x| *x).collect();
                ColumnStats {
                    count: v.len(),
                    null_count: v.len() - valid.len(),
                    min: valid.iter().copied().min(),
                    max: valid.iter().copied().max(),
                    mean: None,
                }
            }
        }
    }
    
    /// Compress the column using Gorilla encoding
    pub fn compress_gorilla(&self) -> Vec<u8> {
        crate::compression::gorilla_encode(&self.values)
    }
    
    /// Decompress from Gorilla encoding
    pub fn decompress_gorilla(&self, data: &[u8]) -> Result<Self, ColumnError> {
        let values = crate::compression::gorilla_decode(data, self.data_type)?;
        let mut col = self.clone();
        col.values = values;
        Ok(col)
    }
}

impl ColumnData {
    fn empty(col_type: ColumnType) -> Self {
        match col_type {
            ColumnType::Int64 => ColumnData::Int64(Vec::new()),
            ColumnType::Float64 => ColumnData::Float64(Vec::new()),
            ColumnType::Boolean => ColumnData::Boolean(Vec::new()),
            ColumnType::String => ColumnData::String(Vec::new()),
            ColumnType::Timestamp => ColumnData::Timestamp(Vec::new()),
        }
    }
}

/// Column statistics
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub count: usize,
    pub null_count: usize,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
}

// Helper functions for building Arrow arrays
fn build_int64_array(values: &[i64], nulls: &[bool]) -> Result<PrimitiveArray<i64>, ColumnError> {
    use arrow::array::Int64Array;
    use arrow::buffer::Buffer;
    
    let value_buf = Buffer::from_iter_values(values.iter().copied());
    let null_buf = Buffer::from_null_bits(nulls);
    
    Ok(Int64Array::from_data(
        DataType::Int64,
        value_buf,
        Some(null_buf),
    ))
}

fn build_float64_array(values: &[f64], nulls: &[bool]) -> Result<PrimitiveArray<f64>, ColumnError> {
    use arrow::array::Float64Array;
    use arrow::buffer::Buffer;
    
    let value_buf = Buffer::from_iter_values(values.iter().copied());
    let null_buf = Buffer::from_null_bits(nulls);
    
    Ok(Float64Array::from_data(
        DataType::Float64,
        value_buf,
        Some(null_buf),
    ))
}

fn build_timestamp_array(values: &[i64], nulls: &[bool]) -> Result<PrimitiveArray<i64>, ColumnError> {
    use arrow::array::TimestampMicrosecondArray;
    use arrow::buffer::Buffer;
    
    let value_buf = Buffer::from_iter_values(values.iter().copied());
    let null_buf = Buffer::from_null_bits(nulls);
    
    Ok(TimestampMicrosecondArray::from_data(
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        value_buf,
        Some(null_buf),
    ))
}

trait BufferExt {
    fn from_iter_values<T: ArrowNativeType>(iter: impl Iterator<Item = T>) -> Self;
    fn from_null_bits(nulls: &[bool]) -> Self;
}

impl BufferExt for arrow::buffer::Buffer {
    fn from_iter_values<T: ArrowNativeType>(iter: impl Iterator<Item = T>) -> Self {
        let mut buf = Vec::new();
        for v in iter {
            let bytes = T::to_bytes(&v);
            buf.extend_from_slice(bytes.as_ref());
        }
        arrow::buffer::Buffer::from_bytes(Arc::from(buf.into_boxed_slice()))
    }
    
    fn from_null_bits(nulls: &[bool]) -> Self {
        arrow::buffer::Buffer::from_bytes(Arc::from(nulls.to_vec()))
    }
}

use std::sync::Arc as StdArc;
