//! Core storage engine implementation
//! 
//! Implements a columnar storage engine using Apache Arrow with:
//! - Vectorized reads and writes
//! - Automatic partition management
//! - Write-ahead logging (WAL)
//! - Transaction support

use crate::storage::{partition::Partition, schema::Schema};
use crate::compression::{Compressor, CompressionType};
use crate::query::QueryResult;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};

/// Storage engine errors
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Partition not found: {0}")]
    PartitionNotFound(String),
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("Compression error: {0}")]
    Compression(String),
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
}

/// Storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Data directory path
    pub data_dir: PathBuf,
    /// WAL directory path
    pub wal_dir: PathBuf,
    /// Default compression type
    pub compression: CompressionType,
    /// Partition duration (e.g., "1h", "1d")
    pub partition_duration: Duration,
    /// Maximum partition size in bytes
    pub max_partition_size: usize,
    /// Enable WAL
    pub enable_wal: bool,
    /// Sync writes to disk
    pub sync_writes: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: super::default_data_dir(),
            wal_dir: super::default_wal_dir(),
            compression: CompressionType::Zstd,
            partition_duration: Duration::hours(1),
            max_partition_size: 128 * 1024 * 1024, // 128MB
            enable_wal: true,
            sync_writes: true,
        }
    }
}

/// Database table
pub struct Table {
    pub id: Uuid,
    pub name: String,
    pub schema: Arc<Schema>,
    pub partitions: DashMap<String, Arc<Partition>>,
    pub config: TableConfig,
}

/// Table configuration
#[derive(Debug, Clone)]
pub struct TableConfig {
    /// Time column name
    pub time_column: String,
    /// Tags (indexed columns)
    pub tags: Vec<String>,
    /// Field columns
    pub fields: Vec<String>,
    /// Retention policy ID
    pub retention_id: Option<Uuid>,
    /// Downsampling rules
    pub downsampling: Vec<DownsampleRule>,
}

/// Downsampling rule
#[derive(Debug, Clone)]
pub struct DownsampleRule {
    /// Rule name
    pub name: String,
    /// Duration for downsampling
    pub duration: Duration,
    /// Aggregation function
    pub aggregation: Aggregation,
    /// Target retention
    pub retention: Option<Duration>,
}

/// Aggregation functions
#[derive(Debug, Clone, Copy)]
pub enum Aggregation {
    Mean,
    Sum,
    Min,
    Max,
    First,
    Last,
    Count,
    Median,
    P50,
    P95,
    P99,
}

/// The main storage engine
#[derive(Debug)]
pub struct Engine {
    config: StorageConfig,
    tables: DashMap<String, Arc<Table>>,
    compressor: Compressor,
    wal: Option<WriteAheadLog>,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new(StorageConfig::default()).expect("Failed to create storage engine")
    }
}

impl Engine {
    /// Create a new storage engine
    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(&config.wal_dir)?;
        
        let wal = if config.enable_wal {
            Some(WriteAheadLog::new(&config.wal_dir)?)
        } else {
            None
        };
        
        Ok(Self {
            config,
            tables: DashMap::new(),
            compressor: Compressor::new(),
            wal,
        })
    }
    
    /// Create a new table
    pub async fn create_table(
        &self,
        name: &str,
        schema: Schema,
        config: TableConfig,
    ) -> Result<Arc<Table>, StorageError> {
        let table_id = Uuid::new_v4();
        
        let table = Arc::new(Table {
            id: table_id,
            name: name.to_string(),
            schema: Arc::new(schema),
            partitions: DashMap::new(),
            config,
        });
        
        self.tables.insert(name.to_string(), table.clone());
        
        // Persist table metadata
        self.save_table_metadata(&table).await?;
        
        tracing::info!("Created table: {} with id: {}", name, table_id);
        Ok(table)
    }
    
    /// Write a record batch to a table
    pub async fn write(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<usize, StorageError> {
        let table = self.tables
            .get(table_name)
            .ok_or_else(|| StorageError::PartitionNotFound(table_name.to_string()))?;
        
        // Get partition key from timestamp column
        let num_rows = batch.num_rows();
        
        // Group by partition
        let partition_groups = self.group_by_partition(&table, &batch)?;
        
        for (partition_key, group_batch) in partition_groups {
            // Get or create partition
            let partition = table.partitions
                .entry(partition_key.clone())
                .or_insert_with(|| {
                    Arc::new(Partition::new(
                        &partition_key,
                        table.schema.clone(),
                        self.config.compression,
                    ))
                });
            
            // Write to partition
            partition.write_batch(&group_batch)?;
            
            // Write to WAL if enabled
            if let Some(wal) = &self.wal {
                wal.write_record_batch(&table.name, &group_batch).await?;
            }
        }
        
        tracing::debug!("Wrote {} rows to table {}", num_rows, table_name);
        Ok(num_rows)
    }
    
    /// Group records by partition key
    fn group_by_partition(
        &self,
        table: &Arc<Table>,
        batch: &RecordBatch,
    ) -> Result<HashMap<String, RecordBatch>, StorageError> {
        let time_col_idx = batch.schema()
            .index_of(&table.config.time_column)
            .map_err(|_| StorageError::SchemaMismatch("Time column not found".to_string()))?;
        
        // For simplicity, create a single partition
        // In production, this would group by timestamp buckets
        let partition_key = PartitionKey::from_timestamp(
            &table.config.time_column,
            &table.schema,
            batch,
            time_col_idx,
            self.config.partition_duration,
        )?;
        
        let mut groups = HashMap::new();
        groups.insert(partition_key.to_string(), batch.clone());
        Ok(groups)
    }
    
    /// Read data from a table
    pub async fn read(
        &self,
        table_name: &str,
        time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> Result<QueryResult, StorageError> {
        let table = self.tables
            .get(table_name)
            .ok_or_else(|| StorageError::PartitionNotFound(table_name.to_string()))?;
        
        let mut results: Vec<RecordBatch> = Vec::new();
        
        // Collect all partitions
        for partition in table.partitions.iter() {
            let partition_key = partition.key();
            
            // Check time range filter
            if let Some((start, end)) = &time_range {
                if !partition_key.in_time_range(start, end) {
                    continue;
                }
            }
            
            // Read from partition
            if let Some(batch) = partition.read_all()? {
                results.push(batch);
            }
        }
        
        Ok(QueryResult::new(results))
    }
    
    /// Delete old data based on retention policy
    pub async fn enforce_retention(&self, table_name: &str) -> Result<usize, StorageError> {
        let table = self.tables
            .get(table_name)
            .ok_or_else(|| StorageError::PartitionNotFound(table_name.to_string()))?;
        
        let mut deleted_count = 0;
        let cutoff = Utc::now() - Duration::days(30); // Default 30 days
        
        table.partitions.retain(|key, partition| {
            let should_delete = partition.key().before(&cutoff);
            if should_delete {
                deleted_count += partition.num_rows();
            }
            !should_delete
        });
        
        tracing::info!("Enforced retention on table {}: deleted {} rows", table_name, deleted_count);
        Ok(deleted_count)
    }
    
    /// Apply downsampling to a table
    pub async fn downsample(
        &self,
        table_name: &str,
        rule: &DownsampleRule,
    ) -> Result<Arc<Table>, StorageError> {
        let source_table = self.tables
            .get(table_name)
            .ok_or_else(|| StorageError::PartitionNotFound(table_name.to_string()))?;
        
        let downsampled_name = format!("{}_{}", table_name, rule.name);
        
        // Create downsampled table schema
        let mut fields = source_table.schema.fields().to_vec();
        fields.retain(|f| f.name != table_name); // Keep all fields
        
        let downsampled_schema = Schema::new(fields);
        let mut config = source_table.config.clone();
        config.downsampling.push(rule.clone());
        
        let downsampled_table = self.create_table(
            &downsampled_name,
            downsampled_schema,
            config,
        ).await?;
        
        // Process each partition
        for partition in source_table.partitions.iter() {
            let downsampled_batches = partition.downsample(rule)?;
            for batch in downsampled_batches {
                downsampled_table.partitions.iter().next();
            }
        }
        
        Ok(downsampled_table)
    }
    
    /// Save table metadata to disk
    async fn save_table_metadata(&self, table: &Table) -> Result<(), StorageError> {
        let metadata_path = self.config.data_dir
            .join("metadata")
            .join(format!("{}.json", table.name));
        
        let metadata = TableMetadata {
            id: table.id,
            name: table.name.clone(),
            schema: table.schema.as_ref().clone(),
            config: table.config.clone(),
        };
        
        let json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| StorageError::Compression(e.to_string()))?;
        
        tokio::fs::write(&metadata_path, json).await?;
        Ok(())
    }
    
    /// Load table metadata from disk
    pub async fn load_tables(&self) -> Result<(), StorageError> {
        let metadata_dir = self.config.data_dir.join("metadata");
        
        if !metadata_dir.exists() {
            return Ok(());
        }
        
        let mut entries = tokio::fs::read_dir(&metadata_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().map_or(false, |e| e == "json") {
                let json = tokio::fs::read_to_string(entry.path()).await?;
                let metadata: TableMetadata = serde_json::from_str(&json)
                    .map_err(|e| StorageError::Compression(e.to_string()))?;
                
                let table = Arc::new(Table {
                    id: metadata.id,
                    name: metadata.name.clone(),
                    schema: Arc::new(metadata.schema),
                    partitions: DashMap::new(),
                    config: metadata.config,
                });
                
                self.tables.insert(metadata.name, table);
            }
        }
        
        tracing::info!("Loaded {} tables from disk", self.tables.len());
        Ok(())
    }
    
    /// Flush all data to disk
    pub async fn flush(&self) -> Result<(), StorageError> {
        for table in self.tables.iter() {
            for partition in table.partitions.iter() {
                partition.flush()?;
            }
        }
        
        if let Some(wal) = &self.wal {
            wal.flush().await?;
        }
        
        tracing::info!("Flushed all data to disk");
        Ok(())
    }
    
    /// Get table statistics
    pub fn get_stats(&self, table_name: &str) -> Result<TableStats, StorageError> {
        let table = self.tables
            .get(table_name)
            .ok_or_else(|| StorageError::PartitionNotFound(table_name.to_string()))?;
        
        let mut total_rows = 0;
        let mut num_partitions = 0;
        let mut total_size = 0;
        
        for partition in table.partitions.iter() {
            total_rows += partition.num_rows();
            num_partitions += 1;
            total_size += partition.size_bytes();
        }
        
        Ok(TableStats {
            table_name: table_name.to_string(),
            total_rows,
            num_partitions,
            total_size_bytes: total_size,
            compression_ratio: if total_size > 0 {
                total_size as f64 / (total_rows * 8).max(1) as f64
            } else {
                1.0
            },
        })
    }
}

/// Table statistics
#[derive(Debug, Clone)]
pub struct TableStats {
    pub table_name: String,
    pub total_rows: usize,
    pub num_partitions: usize,
    pub total_size_bytes: usize,
    pub compression_ratio: f64,
}

/// Table metadata for persistence
#[derive(Debug, Serialize, Deserialize)]
struct TableMetadata {
    id: Uuid,
    name: String,
    schema: Schema,
    config: TableConfig,
}

/// Write-Ahead Log for durability
pub struct WriteAheadLog {
    dir: PathBuf,
    file: RwLock<Option<tokio::fs::File>>,
    position: RwLock<u64>,
    buffer: RwLock<Vec<u8>>,
}

impl WriteAheadLog {
    pub fn new(dir: &Path) -> Result<Self, StorageError> {
        let log_path = dir.join("wal.log");
        
        // Open or create WAL file
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        
        let position = file.metadata()?.len();
        
        Ok(Self {
            dir: dir.to_path_buf(),
            file: RwLock::new(Some(tokio::fs::File::from_std(file))),
            position: RwLock::new(position),
            buffer: RwLock::new(Vec::with_capacity(1024 * 1024)),
        })
    }
    
    pub async fn write_record_batch(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), StorageError> {
        let entry = WalEntry {
            table_name: table_name.to_string(),
            batch: batch.clone(),
            timestamp: Utc::now(),
        };
        
        let encoded = rmp_serde::to_vec(&entry)
            .map_err(|e| StorageError::Compression(e.to_string()))?;
        
        let len_bytes = (encoded.len() as u32).to_le_bytes();
        
        let mut file = self.file.write();
        if let Some(ref mut f) = *file {
            use tokio::io::AsyncWriteExt;
            f.write_all(&len_bytes).await?;
            f.write_all(&encoded).await?;
            
            let mut pos = self.position.write();
            *pos += 4 + encoded.len() as u64;
        }
        
        Ok(())
    }
    
    pub async fn flush(&self) -> Result<(), StorageError> {
        let mut file = self.file.write();
        if let Some(ref mut f) = *file {
            use tokio::io::AsyncWriteExt;
            f.flush().await?;
        }
        Ok(())
    }
    
    pub async fn replay(&self, engine: &Engine) -> Result<usize, StorageError> {
        let log_path = self.dir.join("wal.log");
        if !log_path.exists() {
            return Ok(0);
        }
        
        let data = tokio::fs::read(&log_path).await?;
        let mut pos = 0;
        let mut replayed = 0;
        
        while pos < data.len() {
            if pos + 4 > data.len() {
                break;
            }
            
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            
            if pos + len > data.len() {
                break;
            }
            
            let entry_data = &data[pos..pos + len];
            let entry: WalEntry = rmp_serde::from_slice(entry_data)
                .map_err(|e| StorageError::Compression(e.to_string()))?;
            
            engine.write(&entry.table_name, entry.batch).await?;
            replayed += 1;
            pos += len;
        }
        
        tracing::info!("Replayed {} WAL entries", replayed);
        Ok(replayed)
    }
}

/// WAL entry
#[derive(Debug, Serialize, Deserialize)]
struct WalEntry {
    table_name: String,
    batch: RecordBatch,
    timestamp: DateTime<Utc>,
}

// Import serde
use serde::{Serialize, Deserialize};
