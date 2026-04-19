//! Streaming module
//! 
//! Provides Kafka-compatible streaming ingestion:
//! - Consumer and producer implementations
//! - Message queue integration
//! - Backpressure handling
//! - Exactly-once semantics

pub mod consumer;
pub mod producer;
pub mod codec;
pub mod partitioning;

pub use consumer::{StreamConsumer, ConsumerConfig, StreamMessage};
pub use producer::{StreamProducer, ProducerConfig, ProducerRecord};
pub use codec::{Codec, JsonCodec, CsvCodec, ProtobufCodec};
pub use partitioning::PartitionStrategy;

use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamingError {
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Queue error: {0}")]
    Queue(String),
    #[error("Timeout: {0}")]
    Timeout(String),
}

/// Stream message envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: HashMap<String, Vec<u8>>,
    pub timestamp: i64,
    pub partition: i32,
    pub offset: i64,
    pub topic: String,
}

impl Message {
    pub fn new(key: Option<Vec<u8>>, value: Vec<u8>, topic: &str) -> Self {
        Self {
            key,
            value,
            headers: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            partition: 0,
            offset: 0,
            topic: topic.to_string(),
        }
    }
    
    pub fn with_headers(mut self, headers: HashMap<String, Vec<u8>>) -> Self {
        self.headers = headers;
        self
    }
    
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }
}

use std::collections::HashMap;

/// Batch of messages
#[derive(Debug, Clone)]
pub struct MessageBatch {
    pub messages: Vec<Message>,
    pub topic: String,
    pub partition: i32,
}

impl MessageBatch {
    pub fn new(topic: &str, partition: i32) -> Self {
        Self {
            messages: Vec::new(),
            topic: topic.to_string(),
            partition,
        }
    }
    
    pub fn push(&mut self, message: Message) {
        self.messages.push(message);
    }
    
    pub fn len(&self) -> usize {
        self.messages.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
    
    pub fn size_bytes(&self) -> usize {
        self.messages.iter()
            .map(|m| m.key.as_ref().map_or(0, |k| k.len()) + m.value.len())
            .sum()
    }
}

/// Ingestion statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionStats {
    pub messages_received: u64,
    pub messages_written: u64,
    pub bytes_received: u64,
    pub bytes_written: u64,
    pub errors: u64,
    pub lag_ms: f64,
    pub throughput_mbps: f64,
}

impl Default for IngestionStats {
    fn default() -> Self {
        Self {
            messages_received: 0,
            messages_written: 0,
            bytes_received: 0,
            bytes_written: 0,
            errors: 0,
            lag_ms: 0.0,
            throughput_mbps: 0.0,
        }
    }
}

impl IngestionStats {
    pub fn record_message(&mut self, size: usize) {
        self.messages_received += 1;
        self.bytes_received += size as u64;
    }
    
    pub fn record_write(&mut self, size: usize) {
        self.messages_written += 1;
        self.bytes_written += size as u64;
    }
    
    pub fn record_error(&mut self) {
        self.errors += 1;
    }
    
    pub fn update_throughput(&mut self, elapsed_secs: f64) {
        if elapsed_secs > 0.0 {
            self.throughput_mbps = (self.bytes_written as f64 / 1_000_000.0) / elapsed_secs;
        }
    }
}

/// Stream processor trait
pub trait StreamProcessor: Send + Sync {
    /// Process a batch of messages
    fn process_batch(&self, batch: MessageBatch) -> Result<(), StreamingError>;
    
    /// Get processor name
    fn name(&self) -> &str;
}

/// Simple in-memory stream queue
pub struct StreamQueue {
    sender: mpsc::Sender<Message>,
    receiver: mpsc::Receiver<Message>,
    buffer_size: usize,
}

impl StreamQueue {
    pub fn new(buffer_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(buffer_size);
        Self {
            sender,
            receiver,
            buffer_size,
        }
    }
    
    pub async fn send(&self, message: Message) -> Result<(), StreamingError> {
        self.sender.send(message).await
            .map_err(|_| StreamingError::Queue("Channel closed".to_string()))
    }
    
    pub async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }
    
    pub fn sender(&self) -> mpsc::Sender<Message> {
        self.sender.clone()
    }
    
    pub fn try_recv(&mut self) -> Option<Message> {
        self.receiver.try_recv().ok()
    }
}

/// Backpressure configuration
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum queue size before backpressure
    pub max_queue_size: usize,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,
    /// Enable backpressure
    pub enabled: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10000,
            max_batch_size: 1000,
            batch_timeout_ms: 100,
            enabled: true,
        }
    }
}

/// Stream configuration
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub brokers: Vec<String>,
    pub group_id: String,
    pub topic: String,
    pub buffer_size: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub backpressure: BackpressureConfig,
}

impl StreamConfig {
    pub fn new(brokers: Vec<String>, topic: &str, group_id: &str) -> Self {
        Self {
            brokers,
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            buffer_size: 10000,
            batch_size: 100,
            batch_timeout_ms: 100,
            backpressure: BackpressureConfig::default(),
        }
    }
}
