//! Chronos - Time-Series Database with Embedded ML Inference
//! 
//! A high-performance time-series database built on Apache Arrow with:
//! - Columnar storage with vectorized operations
//! - Built-in ML models for anomaly detection, forecasting, and classification
//! - Streaming ingestion (Kafka-compatible)
//! - Advanced compression (Gorilla, CHIMP)
//! - SQL-like query language with time-series functions

pub mod storage;
pub mod query;
pub mod ml;
pub mod compression;
pub mod streaming;
pub mod server;
pub mod cli;

pub use storage::{Engine, StorageConfig};
pub use query::{QueryEngine, Query, QueryResult};
pub use ml::{ModelRegistry, AnomalyDetector, Forecaster, Classifier};
pub use compression::{Compressor, Decompressor, CompressionType};

use once_cell::sync::Lazy;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize logging with tracing
pub fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// Default engine instance
pub static ENGINE: Lazy<Engine> = Lazy::new(Engine::default);

/// Chronos version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
