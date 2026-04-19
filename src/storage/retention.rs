//! Retention policy management
//! 
//! Handles data retention, downsampling, and tiered storage.

use chrono::{DateTime, Utc, Duration};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::sync::Arc;
use dashmap::DashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RetentionError {
    #[error("Invalid duration: {0}")]
    InvalidDuration(String),
    #[error("Policy not found: {0}")]
    PolicyNotFound(String),
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

/// Duration with time unit support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurationSpec {
    pub value: i64,
    pub unit: DurationUnit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DurationUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
    Months,
}

impl DurationSpec {
    pub fn to_duration(&self) -> Duration {
        match self.unit {
            DurationUnit::Seconds => Duration::seconds(self.value),
            DurationUnit::Minutes => Duration::minutes(self.value),
            DurationUnit::Hours => Duration::hours(self.value),
            DurationUnit::Days => Duration::days(self.value),
            DurationUnit::Weeks => Duration::weeks(self.value),
            DurationUnit::Months => Duration::days(self.value * 30), // Approximate
        }
    }
    
    pub fn from_string(s: &str) -> Result<Self, RetentionError> {
        let s = s.trim().to_lowercase();
        
        let (value, unit_str) = if let Some(n) = s.find(|c: char| !c.is_ascii_digit()) {
            let v: i64 = s[..n].parse().map_err(|_| RetentionError::InvalidDuration(s.clone()))?;
            (v, &s[n..])
        } else {
            return Err(RetentionError::InvalidDuration(s));
        };
        
        let unit = match unit_str {
            "s" | "sec" | "seconds" => DurationUnit::Seconds,
            "m" | "min" | "minutes" => DurationUnit::Minutes,
            "h" | "hr" | "hours" => DurationUnit::Hours,
            "d" | "day" | "days" => DurationUnit::Days,
            "w" | "wk" | "weeks" => DurationUnit::Weeks,
            "mo" | "month" | "months" => DurationUnit::Months,
            _ => return Err(RetentionError::InvalidDuration(s)),
        };
        
        Ok(Self { value, unit })
    }
}

impl std::fmt::Display for DurationSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.value, self.unit)
    }
}

/// Retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub id: Uuid,
    pub name: String,
    pub duration: DurationSpec,
    pub tier: StorageTier,
    pub downsampling: Vec<DownsampleRule>,
}

impl RetentionPolicy {
    /// Create a new retention policy
    pub fn new(name: &str, duration: DurationSpec, tier: StorageTier) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            duration,
            tier,
            downsampling: Vec::new(),
        }
    }
    
    /// Get the cutoff time for this policy
    pub fn cutoff_time(&self) -> DateTime<Utc> {
        Utc::now() - self.duration.to_duration()
    }
    
    /// Check if a timestamp is before the cutoff
    pub fn is_expired(&self, timestamp: DateTime<Utc>) -> bool {
        timestamp < self.cutoff_time()
    }
    
    /// Add a downsampling rule
    pub fn add_downsampling(&mut self, rule: DownsampleRule) {
        self.downsampling.push(rule);
    }
}

/// Storage tier
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum StorageTier {
    /// Hot storage - fast SSD
    Hot,
    /// Warm storage - standard disk
    Warm,
    /// Cold storage - archive
    Cold,
    /// Frozen storage - deep archive
    Frozen,
}

impl StorageTier {
    pub fn compression_level(&self) -> i32 {
        match self {
            StorageTier::Hot => 1,
            StorageTier::Warm => 3,
            StorageTier::Cold => 5,
            StorageTier::Frozen => 7,
        }
    }
    
    pub fn block_size(&self) -> usize {
        match self {
            StorageTier::Hot => 64 * 1024,
            StorageTier::Warm => 256 * 1024,
            StorageTier::Cold => 1024 * 1024,
            StorageTier::Frozen => 4 * 1024 * 1024,
        }
    }
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTier::Hot => write!(f, "hot"),
            StorageTier::Warm => write!(f, "warm"),
            StorageTier::Cold => write!(f, "cold"),
            StorageTier::Frozen => write!(f, "frozen"),
        }
    }
}

/// Downsampling rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownsampleRule {
    pub id: Uuid,
    pub name: String,
    pub duration: DurationSpec,
    pub aggregation: AggregationFunction,
    pub target_retention: Option<DurationSpec>,
}

impl DownsampleRule {
    /// Create a new downsampling rule
    pub fn new(name: &str, duration: DurationSpec, aggregation: AggregationFunction) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            duration,
            aggregation,
            target_retention: None,
        }
    }
    
    /// Set target retention
    pub fn with_retention(mut self, retention: DurationSpec) -> Self {
        self.target_retention = Some(retention);
        self
    }
}

/// Aggregation functions for downsampling
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AggregationFunction {
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
    P999,
    StdDev,
    Variance,
}

impl std::fmt::Display for AggregationFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregationFunction::Mean => write!(f, "mean"),
            AggregationFunction::Sum => write!(f, "sum"),
            AggregationFunction::Min => write!(f, "min"),
            AggregationFunction::Max => write!(f, "max"),
            AggregationFunction::First => write!(f, "first"),
            AggregationFunction::Last => write!(f, "last"),
            AggregationFunction::Count => write!(f, "count"),
            AggregationFunction::Median => write!(f, "median"),
            AggregationFunction::P50 => write!(f, "p50"),
            AggregationFunction::P95 => write!(f, "p95"),
            AggregationFunction::P99 => write!(f, "p99"),
            AggregationFunction::P999 => write!(f, "p999"),
            AggregationFunction::StdDev => write!(f, "stddev"),
            AggregationFunction::Variance => write!(f, "variance"),
        }
    }
}

impl AggregationFunction {
    /// Parse from string
    pub fn from_string(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "mean" | "avg" | "average" => Some(AggregationFunction::Mean),
            "sum" => Some(AggregationFunction::Sum),
            "min" => Some(AggregationFunction::Min),
            "max" => Some(AggregationFunction::Max),
            "first" => Some(AggregationFunction::First),
            "last" => Some(AggregationFunction::Last),
            "count" => Some(AggregationFunction::Count),
            "median" => Some(AggregationFunction::Median),
            "p50" => Some(AggregationFunction::P50),
            "p95" => Some(AggregationFunction::P95),
            "p99" => Some(AggregationFunction::P99),
            "p999" => Some(AggregationFunction::P999),
            "stddev" | "std_dev" => Some(AggregationFunction::StdDev),
            "variance" | "var" => Some(AggregationFunction::Variance),
            _ => None,
        }
    }
}

/// Retention policy manager
pub struct RetentionManager {
    policies: DashMap<Uuid, Arc<RetentionPolicy>>,
    default_policy: Option<Uuid>,
}

impl RetentionManager {
    /// Create a new retention manager
    pub fn new() -> Self {
        Self {
            policies: DashMap::new(),
            default_policy: None,
        }
    }
    
    /// Create with default policies
    pub fn with_defaults() -> Self {
        let mut manager = Self::new();
        
        // Add default policies
        let hot = RetentionPolicy::new(
            "hot",
            DurationSpec { value: 7, unit: DurationUnit::Days },
            StorageTier::Hot,
        );
        manager.add_policy(hot.clone());
        manager.set_default(hot.id);
        
        let warm = RetentionPolicy::new(
            "warm",
            DurationSpec { value: 30, unit: DurationUnit::Days },
            StorageTier::Warm,
        );
        manager.add_policy(warm);
        
        let cold = RetentionPolicy::new(
            "cold",
            DurationSpec { value: 365, unit: DurationUnit::Days },
            StorageTier::Cold,
        );
        manager.add_policy(cold);
        
        manager
    }
    
    /// Add a retention policy
    pub fn add_policy(&self, policy: RetentionPolicy) {
        self.policies.insert(policy.id, Arc::new(policy));
    }
    
    /// Get a policy by ID
    pub fn get(&self, id: Uuid) -> Option<Arc<RetentionPolicy>> {
        self.policies.get(&id).map(|r| r.clone())
    }
    
    /// Get a policy by name
    pub fn get_by_name(&self, name: &str) -> Option<Arc<RetentionPolicy>> {
        self.policies
            .values()
            .find(|p| p.name == name)
            .cloned()
    }
    
    /// Get the default policy
    pub fn default_policy(&self) -> Option<Arc<RetentionPolicy>> {
        self.default_policy.and_then(|id| self.get(id))
    }
    
    /// Set the default policy
    pub fn set_default(&mut self, id: Uuid) {
        if self.policies.contains_key(&id) {
            self.default_policy = Some(id);
        }
    }
    
    /// Remove a policy
    pub fn remove(&self, id: Uuid) -> bool {
        if self.default_policy == Some(id) {
            self.default_policy = None;
        }
        self.policies.remove(&id).is_some()
    }
    
    /// List all policies
    pub fn list(&self) -> Vec<Arc<RetentionPolicy>> {
        self.policies.values().cloned().collect()
    }
    
    /// Check if data should be expired
    pub fn should_expire(&self, timestamp: DateTime<Utc>) -> bool {
        if let Some(policy) = self.default_policy() {
            return policy.is_expired(timestamp);
        }
        false
    }
    
    /// Get storage tier for a timestamp
    pub fn tier_for(&self, timestamp: DateTime<Utc>) -> StorageTier {
        for policy in self.policies.values() {
            if !policy.is_expired(timestamp) {
                return policy.tier;
            }
        }
        StorageTier::Cold
    }
}

impl Default for RetentionManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}
