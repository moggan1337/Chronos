//! Message partitioning strategies
//! 
//! Implements various partitioning strategies for distributing messages across partitions.

use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PartitioningError {
    #[error("Invalid partition key: {0}")]
    InvalidKey(String),
    #[error("No partitions available")]
    NoPartitions,
    #[error("Partition not found: {0}")]
    PartitionNotFound(i32),
}

/// Strategy for partitioning messages
#[derive(Debug, Clone, Copy)]
pub enum PartitionStrategy {
    /// Hash-based partitioning
    Hash,
    /// Round-robin partitioning
    RoundRobin,
    /// Range-based partitioning
    Range,
    /// Time-based partitioning
    Time,
    /// Random partitioning
    Random,
}

/// Partition assignment
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    pub partitions: Vec<i32>,
    pub version: i32,
}

/// Consistent hash ring for partitioning
pub struct ConsistentHash {
    ring: Vec<(u64, i32)>,
    partitions: i32,
    virtual_nodes: usize,
}

impl ConsistentHash {
    pub fn new(partitions: i32, virtual_nodes: usize) -> Self {
        Self {
            ring: Vec::new(),
            partitions,
            virtual_nodes,
        }
    }
    
    pub fn add_node(&mut self, node_id: &str) {
        for i in 0..self.virtual_nodes {
            let key = format!("{}-vn{}", node_id, i);
            let hash = self.hash(&key);
            self.ring.push((hash, self.ring.len() as i32));
        }
        self.ring.sort_by_key(|(hash, _)| *hash);
    }
    
    pub fn get_partition(&self, key: &[u8]) -> i32 {
        if self.ring.is_empty() {
            return 0;
        }
        
        let hash = self.hash(key);
        
        // Binary search for the first node with hash >= key hash
        let idx = match self.ring.binary_search_by_key(&hash, |(h, _)| *h) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        
        if idx >= self.ring.len() {
            self.ring[0].1
        } else {
            self.ring[idx].1
        }
    }
    
    fn hash(&self, key: &str) -> u64 {
        // Simple hash function (in production, use a better one like MurmurHash)
        let mut hash: u64 = 5381;
        for byte in key.as_bytes() {
            hash = hash.wrapping_mul(33).wrapping_add(*byte as u64);
        }
        hash
    }
}

/// Partition router
pub struct PartitionRouter {
    strategy: PartitionStrategy,
    partitions: i32,
    consistent_hash: Option<ConsistentHash>,
}

impl PartitionRouter {
    pub fn new(strategy: PartitionStrategy, partitions: i32) -> Self {
        let consistent_hash = if matches!(strategy, PartitionStrategy::Hash) {
            let mut ch = ConsistentHash::new(partitions, 100);
            for i in 0..partitions {
                ch.add_node(&format!("partition-{}", i));
            }
            Some(ch)
        } else {
            None
        };
        
        Self {
            strategy,
            partitions,
            consistent_hash,
        }
    }
    
    /// Route a message to a partition
    pub fn route(&self, key: Option<&[u8]>, timestamp: Option<i64>) -> i32 {
        match self.strategy {
            PartitionStrategy::Hash => {
                if let Some(k) = key {
                    if let Some(ref ch) = self.consistent_hash {
                        ch.get_partition(k) % self.partitions
                    } else {
                        self.hash_key(k) % self.partitions
                    }
                } else {
                    0
                }
            }
            PartitionStrategy::RoundRobin => {
                // This would need state tracking
                0
            }
            PartitionStrategy::Range => {
                if let Some(k) = key {
                    self.hash_key(k) % self.partitions
                } else {
                    0
                }
            }
            PartitionStrategy::Time => {
                self.time_partition(timestamp)
            }
            PartitionStrategy::Random => {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                rng.gen_range(0..self.partitions)
            }
        }
    }
    
    fn hash_key(&self, key: &[u8]) -> i32 {
        let mut hash: u64 = 5381;
        for &byte in key {
            hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
        }
        (hash % self.partitions as u64) as i32
    }
    
    fn time_partition(&self, timestamp: Option<i64>) -> i32 {
        match timestamp {
            Some(ts) => {
                // Partition by hour of day
                let datetime = DateTime::from_timestamp_millis(ts)
                    .unwrap_or_else(Utc::now);
                (datetime.hour() as i32) % self.partitions
            }
            None => {
                let now = Utc::now();
                (now.hour() as i32) % self.partitions
            }
        }
    }
    
    /// Get all partitions
    pub fn all_partitions(&self) -> Vec<i32> {
        (0..self.partitions).collect()
    }
}

/// Topic partition metadata
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: Option<String>,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub lag: i64,
    pub status: PartitionStatus,
}

#[derive(Debug, Clone, Copy)]
pub enum PartitionStatus {
    Active,
    Inactive,
    Offline,
}

impl Default for PartitionInfo {
    fn default() -> Self {
        Self {
            id: 0,
            leader: None,
            replicas: Vec::new(),
            isr: Vec::new(),
            lag: 0,
            status: PartitionStatus::Active,
        }
    }
}

/// Partition manager
pub struct PartitionManager {
    partitions: HashMap<String, Vec<PartitionInfo>>,
    router: PartitionRouter,
}

impl PartitionManager {
    pub fn new(num_partitions: i32, strategy: PartitionStrategy) -> Self {
        Self {
            partitions: HashMap::new(),
            router: PartitionRouter::new(strategy, num_partitions),
        }
    }
    
    /// Create topic partitions
    pub fn create_topic(&mut self, topic: &str, num_partitions: i32) {
        let partitions: Vec<PartitionInfo> = (0..num_partitions)
            .map(|i| PartitionInfo {
                id: i,
                leader: Some(format!("broker-{}", i % 3)),
                replicas: vec![i],
                isr: vec![i],
                lag: 0,
                status: PartitionStatus::Active,
            })
            .collect();
        
        self.partitions.insert(topic.to_string(), partitions);
    }
    
    /// Get partition for a message
    pub fn get_partition(&self, topic: &str, key: Option<&[u8]>, timestamp: Option<i64>) 
        -> Result<i32, PartitioningError> 
    {
        let partitions = self.partitions.get(topic)
            .ok_or_else(|| PartitioningError::PartitionNotFound(0))?;
        
        let partition_id = self.router.route(key, timestamp);
        
        if partition_id >= partitions.len() as i32 {
            return Err(PartitioningError::PartitionNotFound(partition_id));
        }
        
        Ok(partition_id)
    }
    
    /// Get all partitions for a topic
    pub fn get_partitions(&self, topic: &str) -> Option<&Vec<PartitionInfo>> {
        self.partitions.get(topic)
    }
    
    /// Get partition info
    pub fn get_partition_info(&self, topic: &str, partition: i32) 
        -> Option<&PartitionInfo> 
    {
        self.partitions.get(topic)
            .and_then(|p| p.get(partition as usize))
    }
    
    /// Update partition lag
    pub fn update_lag(&mut self, topic: &str, partition: i32, lag: i64) {
        if let Some(partitions) = self.partitions.get_mut(topic) {
            if let Some(p) = partitions.get_mut(partition as usize) {
                p.lag = lag;
            }
        }
    }
}

impl Default for PartitionManager {
    fn default() -> Self {
        Self::new(3, PartitionStrategy::Hash)
    }
}

impl chrono::Timelike for DateTime<Utc> {
    fn hour(&self) -> u32 {
        use chrono::Datelike;
        let hour = (self.timestamp() / 3600) % 24;
        hour as u32
    }
}
