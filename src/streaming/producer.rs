//! Stream producer implementation
//! 
//! Kafka-compatible producer for time-series data output.

use crate::streaming::{Message, StreamingError, IngestionStats};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProducerError {
    #[error("Producer error: {0}")]
    Error(String),
    #[error("Not connected")]
    NotConnected,
    #[error("Send error: {0}")]
    SendError(String),
    #[error("Buffer full")]
    BufferFull,
}

/// Producer configuration
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub bootstrap_servers: Vec<String>,
    pub topic: String,
    pub acks: Acks,
    pub compression: ProducerCompression,
    pub batch_size: usize,
    pub linger_ms: u64,
    pub max_in_flight: usize,
    pub retry_backoff_ms: u64,
    pub max_retries: u32,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            topic: String::new(),
            acks: Acks::All,
            compression: ProducerCompression::None,
            batch_size: 16384,
            linger_ms: 5,
            max_in_flight: 5,
            retry_backoff_ms: 100,
            max_retries: 3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Acks {
    /// No acknowledgments
    None,
    /// Leader acknowledgment
    Leader,
    /// All replicas acknowledgment
    All,
}

#[derive(Debug, Clone, Copy)]
pub enum ProducerCompression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// Record to produce
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub timestamp: i64,
    pub headers: std::collections::HashMap<String, Vec<u8>>,
}

impl ProducerRecord {
    pub fn new(topic: &str, value: Vec<u8>) -> Self {
        Self {
            topic: topic.to_string(),
            partition: None,
            key: None,
            value,
            timestamp: chrono::Utc::now().timestamp_millis(),
            headers: std::collections::HashMap::new(),
        }
    }
    
    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }
    
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }
    
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }
    
    pub fn header(mut self, name: &str, value: Vec<u8>) -> Self {
        self.headers.insert(name.to_string(), value);
        self
    }
    
    pub fn into_message(self) -> Message {
        Message {
            key: self.key,
            value: self.value,
            headers: self.headers,
            timestamp: self.timestamp,
            partition: self.partition.unwrap_or(0),
            offset: 0,
            topic: self.topic,
        }
    }
}

/// Kafka-compatible stream producer
pub struct StreamProducer {
    config: ProducerConfig,
    buffer: Arc<RwLock<VecDeque<ProducerRecord>>>,
    stats: Arc<RwLock<IngestionStats>>,
    running: Arc<RwLock<bool>>,
}

impl StreamProducer {
    pub fn new(config: ProducerConfig) -> Self {
        Self {
            config,
            buffer: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(IngestionStats::default())),
            running: Arc::new(RwLock::new(false)),
        }
    }
    
    /// Send a record
    pub async fn send(&self, record: ProducerRecord) -> Result<i64, ProducerError> {
        if record.topic.is_empty() {
            return Err(ProducerError::Error("Topic not set".to_string()));
        }
        
        // Buffer the record
        let mut buffer = self.buffer.write().await;
        buffer.push_back(record);
        
        // Update stats
        let mut stats = self.stats.write().await;
        stats.messages_written += 1;
        stats.bytes_written += record.value.len() as u64;
        
        Ok(stats.messages_written as i64)
    }
    
    /// Send a record and wait for acknowledgment
    pub async fn send_and_wait(&self, record: ProducerRecord) -> Result<(), ProducerError> {
        self.send(record).await?;
        // In real implementation, would wait for ack
        Ok(())
    }
    
    /// Flush buffered records
    pub async fn flush(&self) -> Result<(), ProducerError> {
        let mut buffer = self.buffer.write().await;
        let count = buffer.len();
        buffer.clear();
        tracing::debug!("Flushed {} records", count);
        Ok(())
    }
    
    /// Get producer statistics
    pub async fn stats(&self) -> IngestionStats {
        self.stats.read().await.clone()
    }
    
    /// Get buffer size
    pub async fn buffer_size(&self) -> usize {
        self.buffer.read().await.len()
    }
    
    /// Start producer
    pub async fn start(&self) -> Result<(), ProducerError> {
        *self.running.write().await = true;
        tracing::info!("Producer started");
        Ok(())
    }
    
    /// Stop producer
    pub async fn stop(&self) -> Result<(), ProducerError> {
        *self.running.write().await = false;
        self.flush().await?;
        tracing::info!("Producer stopped");
        Ok(())
    }
    
    /// Check if running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }
}

impl Default for StreamProducer {
    fn default() -> Self {
        Self::new(ProducerConfig::default())
    }
}

/// Mock producer for testing
pub struct MockProducer {
    records: Vec<ProducerRecord>,
    config: ProducerConfig,
}

impl MockProducer {
    pub fn new(config: ProducerConfig) -> Self {
        Self {
            records: Vec::new(),
            config,
        }
    }
    
    pub fn send(&mut self, record: ProducerRecord) -> Result<(), ProducerError> {
        self.records.push(record);
        Ok(())
    }
    
    pub fn records(&self) -> &[ProducerRecord] {
        &self.records
    }
    
    pub fn clear(&mut self) {
        self.records.clear();
    }
    
    pub fn len(&self) -> usize {
        self.records.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}
