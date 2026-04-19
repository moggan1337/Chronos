//! Storage engine module
//! 
//! Provides columnar storage using Apache Arrow with:
//! - Column-oriented data layout for efficient time-range queries
//! - Automatic partitioning by time buckets
//! - Retention policy management
//! - Downsampling and aggregation

pub mod engine;
pub mod partition;
pub mod column;
pub mod schema;
pub mod retention;

pub use engine::{Engine, StorageConfig, StorageError};
pub use partition::{Partition, PartitionKey};
pub use column::{Column, ColumnType};
pub use schema::{Schema, Field};
pub use retention::{RetentionPolicy, DownsampleRule};

use std::path::PathBuf;

/// Default data directory
pub fn default_data_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("chronos")
        .join("data")
}

/// Default WAL directory
pub fn default_wal_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("chronos")
        .join("wal")
}
