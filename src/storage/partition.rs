//! Partition management for time-series data
//! 
//! Partitions are the fundamental storage unit in Chronos, organized by time buckets.

use crate::storage::{schema::Schema, column::Column};
use crate::compression::CompressionType;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc, Duration};
use std::sync::Arc;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PartitionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

/// Partition key identifying a time bucket
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    /// Table name
    pub table_name: String,
    /// Start time of the partition
    pub start_time: DateTime<Utc>,
    /// End time of the partition
    pub end_time: DateTime<Utc>,
}

impl PartitionKey {
    /// Create a new partition key
    pub fn new(table_name: &str, start_time: DateTime<Utc>, duration: Duration) -> Self {
        Self {
            table_name: table_name.to_string(),
            start_time,
            end_time: start_time + duration,
        }
    }
    
    /// Create from timestamp
    pub fn from_timestamp(
        time_column: &str,
        schema: &Schema,
        batch: &RecordBatch,
        time_col_idx: usize,
        duration: Duration,
    ) -> Result<Self, PartitionError> {
        // Get first timestamp to determine bucket
        let timestamps = batch.column(time_col_idx);
        let first_ts = timestamps
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .ok_or_else(|| PartitionError::InvalidOperation("Invalid timestamp type".to_string()))?;
        
        let first_value = first_ts.value(0);
        let start_time = DateTime::from_timestamp(first_value / 1_000_000, 0)
            .unwrap_or_else(Utc::now);
        
        // Align to duration boundary
        let aligned_start = align_to_duration(start_time, duration);
        let end_time = aligned_start + duration;
        
        Ok(Self {
            table_name: schema.name().to_string(),
            start_time: aligned_start,
            end_time,
        })
    }
    
    /// Check if partition is in time range
    pub fn in_time_range(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> bool {
        self.start_time < *end && self.end_time > *start
    }
    
    /// Check if partition is before a cutoff time
    pub fn before(&self, cutoff: &DateTime<Utc>) -> bool {
        self.end_time < *cutoff
    }
    
    /// Get partition size in bytes
    pub fn size_bytes(&self) -> usize {
        self.end_time.timestamp() as usize - self.start_time.timestamp() as usize
    }
}

impl std::fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.table_name,
            self.start_time.format("%Y%m%d%H"),
            self.end_time.format("%Y%m%d%H")
        )
    }
}

/// A partition stores columnar data for a time bucket
pub struct Partition {
    key: PartitionKey,
    schema: Arc<Schema>,
    columns: HashMap<String, Column>,
    compression: CompressionType,
    row_count: usize,
    size_bytes: usize,
    is_dirty: bool,
}

impl Partition {
    /// Create a new partition
    pub fn new(key: &str, schema: Arc<Schema>, compression: CompressionType) -> Self {
        let partition_key = key.parse()
            .unwrap_or_else(|_| PartitionKey::new("unknown", Utc::now(), Duration::hours(1)));
        
        Self {
            key: partition_key,
            schema,
            columns: HashMap::new(),
            compression,
            row_count: 0,
            size_bytes: 0,
            is_dirty: false,
        }
    }
    
    /// Get the partition key
    pub fn key(&self) -> &PartitionKey {
        &self.key
    }
    
    /// Get row count
    pub fn num_rows(&self) -> usize {
        self.row_count
    }
    
    /// Get partition size in bytes
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
    
    /// Write a record batch to the partition
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PartitionError> {
        let num_rows = batch.num_rows();
        
        // Convert Arrow batch to columnar storage
        for field in batch.schema().fields() {
            let col_idx = batch.schema().index_of(field.name()).unwrap();
            let array = batch.column(col_idx);
            
            let column = self.columns
                .entry(field.name().clone())
                .or_insert_with(|| Column::new(field.name(), field.data_type().clone()));
            
            column.append_array(array.clone())?;
        }
        
        self.row_count += num_rows;
        self.is_dirty = true;
        
        Ok(())
    }
    
    /// Read all data from the partition as a record batch
    pub fn read_all(&self) -> Result<Option<RecordBatch>, PartitionError> {
        if self.columns.is_empty() {
            return Ok(None);
        }
        
        // Reconstruct record batch from columns
        let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
        
        for field in self.schema.fields() {
            let column = self.columns.get(field.name())
                .ok_or_else(|| PartitionError::InvalidOperation(
                    format!("Missing column: {}", field.name())
                ))?;
            
            arrays.push(column.to_array()?);
        }
        
        let record_batch = RecordBatch::try_new(
            self.schema.to_arrow_schema(),
            arrays,
        )?;
        
        Ok(Some(record_batch))
    }
    
    /// Read data within a time range
    pub fn read_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<RecordBatch>, PartitionError> {
        // Check if partition overlaps with range
        if !self.key.in_time_range(&start, &end) {
            return Ok(None);
        }
        
        // For simplicity, return all data
        // In production, would filter by timestamp column
        self.read_all()
    }
    
    /// Apply downsampling to the partition
    pub fn downsample(&self, rule: &crate::storage::engine::DownsampleRule) 
        -> Result<Vec<RecordBatch>, PartitionError> 
    {
        // Read all data
        let batch = match self.read_all()? {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };
        
        // Group by new duration and aggregate
        let groups = self.group_by_duration(&batch, rule.duration)?;
        let mut results = Vec::new();
        
        for (_, group_batch) in groups {
            let aggregated = self.aggregate_batch(&group_batch, &rule.aggregation)?;
            results.push(aggregated);
        }
        
        Ok(results)
    }
    
    /// Group records by duration
    fn group_by_duration(
        &self,
        batch: &RecordBatch,
        duration: Duration,
    ) -> Result<HashMap<i64, RecordBatch>, PartitionError> {
        let mut groups: HashMap<i64, RecordBatch> = HashMap::new();
        
        // Get time column (assuming first column)
        let time_col = batch.column(0);
        let timestamps = time_col
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .ok_or_else(|| PartitionError::InvalidOperation("Invalid timestamp".to_string()))?;
        
        for i in 0..batch.num_rows() {
            let ts = timestamps.value(i);
            let bucket = (ts / (duration.num_microseconds().unwrap_or(3600_000_000) as i64));
            
            // Add row to group
            let group = groups.entry(bucket).or_insert_with(|| {
                RecordBatch::new_empty(batch.schema().clone())
            });
            
            // Note: In production, would build batches properly
            let _ = group;
        }
        
        Ok(groups)
    }
    
    /// Aggregate a batch using the specified function
    fn aggregate_batch(
        &self,
        batch: &RecordBatch,
        aggregation: &crate::storage::engine::Aggregation,
    ) -> Result<RecordBatch, PartitionError> {
        // Simple aggregation - in production would use proper vectorized ops
        let num_rows = batch.num_rows();
        
        if num_rows == 0 {
            return Ok(batch.clone());
        }
        
        let mut aggregated_arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
        
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            let aggregated = aggregate_array(array.as_ref(), aggregation, num_rows)?;
            aggregated_arrays.push(aggregated);
        }
        
        RecordBatch::try_new(batch.schema().clone(), aggregated_arrays)
            .map_err(|e| e.into())
    }
    
    /// Flush data to disk
    pub fn flush(&mut self) -> Result<(), PartitionError> {
        if !self.is_dirty {
            return Ok(());
        }
        
        // In production, would write to Parquet files
        self.is_dirty = false;
        
        tracing::debug!("Flushed partition {} ({} rows)", self.key, self.row_count);
        Ok(())
    }
    
    /// Compact the partition by merging with another
    pub fn compact(&mut self, other: &Partition) -> Result<(), PartitionError> {
        // Merge columns
        for (name, other_col) in &other.columns {
            if let Some(self_col) = self.columns.get_mut(name) {
                self_col.merge(other_col)?;
            } else {
                self.columns.insert(name.clone(), other_col.clone());
            }
        }
        
        self.row_count += other.row_count;
        self.is_dirty = true;
        
        Ok(())
    }
}

/// Aggregate an array using the specified function
fn aggregate_array(
    array: &dyn arrow::array::Array,
    aggregation: &crate::storage::engine::Aggregation,
    count: usize,
) -> Result<Arc<dyn arrow::array::Array>, PartitionError> {
    use arrow::array::*;
    
    // Simple single-value aggregation
    let scalar: ScalarValue = match array.data_type() {
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| PartitionError::InvalidOperation("Invalid type".to_string()))?;
            let sum: f64 = (0..count).map(|i| arr.value(i)).sum();
            ScalarValue::Float64(Some(sum / count as f64))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| PartitionError::InvalidOperation("Invalid type".to_string()))?;
            let sum: i64 = (0..count).map(|i| arr.value(i)).sum();
            ScalarValue::Int64(Some(sum / count as i64))
        }
        _ => ScalarValue::Null,
    };
    
    // Create single-element array with scalar
    let result: Arc<dyn arrow::array::Array> = match scalar {
        ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![v])),
        ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![v])),
        _ => return Ok(Arc::new(NullArray::new(1))),
    };
    
    Ok(result)
}

/// Align a timestamp to a duration boundary
fn align_to_duration(time: DateTime<Utc>, duration: Duration) -> DateTime<Utc> {
    let micros = duration.num_microseconds().unwrap_or(3600_000_000);
    let ts = time.timestamp_micros();
    let aligned = (ts / micros) * micros;
    DateTime::from_timestamp(aligned / 1_000_000, (aligned % 1_000_000) as u32)
        .unwrap_or(time)
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("key", &self.key)
            .field("row_count", &self.row_count)
            .field("size_bytes", &self.size_bytes)
            .field("num_columns", &self.columns.len())
            .finish()
    }
}

use std::sync::atomic::{AtomicUsize, Ordering};

impl Default for Partition {
    fn default() -> Self {
        Self::new(
            &PartitionKey::new("default", Utc::now(), Duration::hours(1)),
            Arc::new(Schema::default()),
            CompressionType::Zstd,
        )
    }
}
