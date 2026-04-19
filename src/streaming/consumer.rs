//! Stream consumer implementation
//! 
//! Kafka-compatible consumer for time-series data ingestion.

use crate::streaming::{StreamConfig, Message, MessageBatch, StreamingError, IngestionStats};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConsumerError {
    #[error("Consumer error: {0}")]
    Error(String),
    #[error("Not connected")]
    NotConnected,
    #[error("Subscription error: {0}")]
    Subscription(String),
}

/// Consumer configuration
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub bootstrap_servers: Vec<String>,
    pub group_id: String,
    pub topics: Vec<String>,
    pub auto_offset_reset: AutoOffsetReset,
    pub enable_auto_commit: bool,
    pub max_poll_records: usize,
    pub session_timeout_ms: u64,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            group_id: "chronos-consumer".to_string(),
            topics: Vec::new(),
            auto_offset_reset: AutoOffsetReset::Earliest,
            enable_auto_commit: true,
            max_poll_records: 500,
            session_timeout_ms: 30000,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
}

/// Stream message with parsed data
#[derive(Debug, Clone)]
pub struct StreamMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: std::collections::HashMap<String, Vec<u8>>,
}

impl From<Message> for StreamMessage {
    fn from(msg: Message) -> Self {
        Self {
            topic: msg.topic,
            partition: msg.partition,
            offset: msg.offset,
            timestamp: msg.timestamp,
            key: msg.key,
            value: msg.value,
            headers: msg.headers,
        }
    }
}

/// Kafka-compatible stream consumer
pub struct StreamConsumer {
    config: ConsumerConfig,
    offset: Arc<RwLock<i64>>,
    messages: Arc<RwLock<VecDeque<Message>>>,
    stats: Arc<RwLock<IngestionStats>>,
    running: Arc<RwLock<bool>>,
}

impl StreamConsumer {
    pub fn new(config: ConsumerConfig) -> Self {
        Self {
            config,
            offset: Arc::new(RwLock::new(0)),
            messages: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(IngestionStats::default())),
            running: Arc::new(RwLock::new(false)),
        }
    }
    
    /// Subscribe to topics
    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), ConsumerError> {
        if topics.is_empty() {
            return Err(ConsumerError::Subscription("No topics specified".to_string()));
        }
        
        self.config.topics = topics;
        tracing::info!("Subscribed to topics: {:?}", self.config.topics);
        Ok(())
    }
    
    /// Poll for messages
    pub async fn poll(&self, timeout_ms: u64) -> Result<Vec<StreamMessage>, ConsumerError> {
        if !self.config.topitles.iter().any(|t| !t.is_empty()) {
            return Err(ConsumerError::NotConnected);
        }
        
        let messages = self.messages.read().await;
        let msgs: Vec<StreamMessage> = messages
            .iter()
            .take(self.config.max_poll_records)
            .cloned()
            .map(StreamMessage::from)
            .collect();
        
        Ok(msgs)
    }
    
    /// Commit offsets
    pub async fn commit(&self) -> Result<(), ConsumerError> {
        let mut offset = self.offset.write().await;
        *offset += 1;
        tracing::debug!("Committed offset: {}", *offset);
        Ok(())
    }
    
    /// Get consumer statistics
    pub async fn stats(&self) -> IngestionStats {
        self.stats.read().await.clone()
    }
    
    /// Start consuming in background
    pub async fn start(&self) -> Result<(), ConsumerError> {
        *self.running.write().await = true;
        tracing::info!("Consumer started");
        Ok(())
    }
    
    /// Stop consuming
    pub async fn stop(&self) -> Result<(), ConsumerError> {
        *self.running.write().await = false;
        tracing::info!("Consumer stopped");
        Ok(())
    }
    
    /// Check if running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }
}

/// Mock consumer for testing
pub struct MockConsumer {
    messages: Vec<Message>,
    index: usize,
    config: ConsumerConfig,
}

impl MockConsumer {
    pub fn new(config: ConsumerConfig) -> Self {
        Self {
            messages: Vec::new(),
            index: 0,
            config,
        }
    }
    
    pub fn add_message(&mut self, message: Message) {
        self.messages.push(message);
    }
    
    pub fn add_messages(&mut self, messages: Vec<Message>) {
        self.messages.extend(messages);
    }
    
    pub fn poll(&mut self) -> Vec<StreamMessage> {
        let end = (self.index + self.config.max_poll_records).min(self.messages.len());
        let batch: Vec<StreamMessage> = self.messages[self.index..end]
            .iter()
            .cloned()
            .map(StreamMessage::from)
            .collect();
        self.index = end;
        batch
    }
    
    pub fn reset(&mut self) {
        self.index = 0;
    }
    
    pub fn messages_remaining(&self) -> usize {
        self.messages.len() - self.index
    }
}

use chrono::{DateTime, Utc};

/// Message builder for testing
pub struct MessageBuilder {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
    headers: std::collections::HashMap<String, Vec<u8>>,
    timestamp: i64,
    topic: String,
    partition: i32,
}

impl MessageBuilder {
    pub fn new(topic: &str, value: Vec<u8>) -> Self {
        Self {
            key: None,
            value,
            headers: std::collections::HashMap::new(),
            timestamp: Utc::now().timestamp_millis(),
            topic: topic.to_string(),
            partition: 0,
        }
    }
    
    pub fn key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }
    
    pub fn header(mut self, name: &str, value: Vec<u8>) -> Self {
        self.headers.insert(name.to_string(), value);
        self
    }
    
    pub fn timestamp(mut self, ts: i64) -> Self {
        self.timestamp = ts;
        self
    }
    
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }
    
    pub fn build(self) -> Message {
        Message {
            key: self.key,
            value: self.value,
            headers: self.headers,
            timestamp: self.timestamp,
            partition: self.partition,
            offset: 0,
            topic: self.topic,
        }
    }
}

impl Default for StreamConsumer {
    fn default() -> Self {
        Self::new(ConsumerConfig::default())
    }
}
