//! Anomaly detection models
//! 
//! Implements various anomaly detection algorithms:
//! - Statistical (Z-score, IQR)
//! - Isolation Forest
//! - Exponential Smoothing based

use crate::ml::{MLError, ModelTrait, ModelType, ModelRegistry};
use crate::query::functions::TimeSeriesFunctions;
use serde::{Serialize, Deserialize};
use std::collections::VecDeque;
use rand::Rng;
use ndarray::{Array1, Array2};

/// Anomaly detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyResult {
    pub scores: Vec<f64>,
    pub anomalies: Vec<AnomalyPoint>,
    pub threshold: f64,
    pub model_name: String,
}

/// Single anomaly point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyPoint {
    pub index: usize,
    pub value: f64,
    pub score: f64,
    pub timestamp: Option<i64>,
}

/// Anomaly detection model type
#[derive(Debug, Clone, Copy)]
pub enum AnomalyModel {
    /// Statistical Z-score based
    ZScore,
    /// Interquartile range based
    IQR,
    /// Isolation Forest
    IsolationForest,
    /// Exponential smoothing based
    ExponentialSmoothing,
}

/// Z-Score anomaly detector
#[derive(Debug, Clone)]
pub struct ZScoreDetector {
    threshold: f64,
    mean: f64,
    stddev: f64,
    window_size: usize,
}

impl ZScoreDetector {
    pub fn new(threshold: f64, window_size: usize) -> Self {
        Self {
            threshold,
            mean: 0.0,
            stddev: 1.0,
            window_size,
        }
    }
    
    fn calculate_stats(&mut self, values: &[f64]) {
        if values.is_empty() {
            return;
        }
        
        self.mean = values.iter().sum::<f64>() / values.len() as f64;
        
        if values.len() > 1 {
            let variance: f64 = values.iter()
                .map(|v| (v - self.mean).powi(2))
                .sum::<f64>() / (values.len() - 1) as f64;
            self.stddev = variance.sqrt();
        }
        
        if self.stddev < 1e-10 {
            self.stddev = 1.0;
        }
    }
    
    pub fn detect(&mut self, values: &[f64]) -> AnomalyResult {
        // Update statistics
        self.calculate_stats(values);
        
        let mut scores = Vec::with_capacity(values.len());
        let mut anomalies = Vec::new();
        
        for (i, &value) in values.iter().enumerate() {
            let z_score = (value - self.mean) / self.stddev;
            let score = z_score.abs();
            scores.push(score);
            
            if score > self.threshold {
                anomalies.push(AnomalyPoint {
                    index: i,
                    value,
                    score,
                    timestamp: None,
                });
            }
        }
        
        AnomalyResult {
            scores,
            anomalies,
            threshold: self.threshold,
            model_name: "ZScore".to_string(),
        }
    }
}

/// IQR-based anomaly detector
#[derive(Debug, Clone)]
pub struct IQRDetector {
    multiplier: f64,
    q1: f64,
    q3: f64,
    iqr: f64,
    lower_bound: f64,
    upper_bound: f64,
}

impl IQRDetector {
    pub fn new(multiplier: f64) -> Self {
        Self {
            multiplier,
            q1: 0.0,
            q3: 0.0,
            iqr: 0.0,
            lower_bound: f64::NEG_INFINITY,
            upper_bound: f64::INFINITY,
        }
    }
    
    fn calculate_bounds(&mut self, values: &[f64]) {
        if values.is_empty() {
            return;
        }
        
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let n = sorted.len();
        self.q1 = Self::percentile(&sorted, 25.0);
        self.q3 = Self::percentile(&sorted, 75.0);
        self.iqr = self.q3 - self.q1;
        
        self.lower_bound = self.q1 - self.multiplier * self.iqr;
        self.upper_bound = self.q3 + self.multiplier * self.iqr;
    }
    
    fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        
        let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
    
    pub fn detect(&mut self, values: &[f64]) -> AnomalyResult {
        self.calculate_bounds(values);
        
        let mut scores = Vec::with_capacity(values.len());
        let mut anomalies = Vec::new();
        
        for (i, &value) in values.iter().enumerate() {
            let score = if value < self.lower_bound {
                (self.lower_bound - value).abs()
            } else if value > self.upper_bound {
                value - self.upper_bound
            } else {
                0.0
            };
            
            scores.push(score);
            
            if score > 0.0 {
                anomalies.push(AnomalyPoint {
                    index: i,
                    value,
                    score,
                    timestamp: None,
                });
            }
        }
        
        AnomalyResult {
            scores,
            anomalies,
            threshold: self.multiplier,
            model_name: "IQR".to_string(),
        }
    }
}

/// Isolation Forest implementation
#[derive(Debug, Clone)]
pub struct IsolationForestDetector {
    num_trees: usize,
    sample_size: usize,
    max_depth: usize,
    trees: Vec<IsolationTree>,
}

#[derive(Debug, Clone)]
struct IsolationTree {
    depth: usize,
    left: Option<Box<IsolationTree>>,
    right: Option<Box<IsolationTree>>,
    feature_index: usize,
    split_value: f64,
    size: usize,
}

impl IsolationForestDetector {
    pub fn new(num_trees: usize, sample_size: usize) -> Self {
        Self {
            num_trees,
            sample_size,
            max_depth: (sample_size as f64).log2() as usize,
            trees: Vec::new(),
        }
    }
    
    pub fn train(&mut self, data: &[f64]) -> Result<(), MLError> {
        let data: Vec<f64> = if data.len() > self.sample_size {
            let mut rng = rand::thread_rng();
            let mut indices: Vec<usize> = (0..data.len()).collect();
            indices.shuffle(&mut rng);
            indices.truncate(self.sample_size);
            indices.iter().map(|&i| data[i]).collect()
        } else {
            data.to_vec()
        };
        
        self.trees.clear();
        for _ in 0..self.num_trees {
            let tree = IsolationTree::build(&data, 0, self.max_depth);
            self.trees.push(tree);
        }
        
        tracing::info!("Trained Isolation Forest with {} trees", self.num_trees);
        Ok(())
    }
    
    pub fn detect(&self, values: &[f64]) -> AnomalyResult {
        let mut scores = Vec::with_capacity(values.len());
        let mut anomalies = Vec::new();
        
        // Calculate average path length
        let c = Self::c(self.sample_size);
        
        for (i, &value) in values.iter().enumerate() {
            let mut path_lengths: Vec<f64> = self.trees.iter()
                .map(|tree| tree.path_length(value, 0) as f64)
                .collect();
            
            let avg_path_length: f64 = path_lengths.iter().sum::<f64>() / path_lengths.len() as f64;
            
            // Anomaly score: higher means more anomalous
            let score = (-avg_path_length / c).exp();
            scores.push(score);
        }
        
        // Determine threshold using average + standard deviation
        let mean: f64 = scores.iter().sum::<f64>() / scores.len() as f64;
        let variance: f64 = scores.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / scores.len() as f64;
        let stddev = variance.sqrt();
        let threshold = mean + 2.0 * stddev;
        
        for (i, &score) in scores.iter().enumerate() {
            if score > threshold {
                anomalies.push(AnomalyPoint {
                    index: i,
                    value: values[i],
                    score,
                    timestamp: None,
                });
            }
        }
        
        AnomalyResult {
            scores,
            anomalies,
            threshold,
            model_name: "IsolationForest".to_string(),
        }
    }
    
    fn c(n: usize) -> f64 {
        if n <= 1 {
            return 1.0;
        }
        2.0 * (n as f64 - 1.0).ln() - 2.0 * (n as f64 - 1.0) / (n as f64)
    }
}

impl IsolationTree {
    fn build(data: &[f64], depth: usize, max_depth: usize) -> Self {
        let size = data.len();
        
        if depth >= max_depth || size <= 1 {
            return IsolationTree {
                depth,
                left: None,
                right: None,
                feature_index: 0,
                split_value: data.first().copied().unwrap_or(0.0),
                size,
            };
        }
        
        // Random split
        let mut rng = rand::thread_rng();
        let min = data.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = data.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        
        let split_value = if (max - min).abs() < 1e-10 {
            return IsolationTree {
                depth,
                left: None,
                right: None,
                feature_index: 0,
                split_value: min,
                size,
            };
        } else {
            min + rng.gen::<f64>() * (max - min)
        };
        
        let left: Vec<f64> = data.iter().filter(|&&v| v < split_value).copied().collect();
        let right: Vec<f64> = data.iter().filter(|&&v| v >= split_value).copied().collect();
        
        IsolationTree {
            depth,
            left: if left.is_empty() { None } else { Some(Box::new(Self::build(&left, depth + 1, max_depth))) },
            right: if right.is_empty() { None } else { Some(Box::new(Self::build(&right, depth + 1, max_depth))) },
            feature_index: 0,
            split_value,
            size,
        }
    }
    
    fn path_length(&self, value: f64, depth: usize) -> usize {
        if self.left.is_none() && self.right.is_none() {
            return depth;
        }
        
        if value < self.split_value {
            if let Some(ref left) = self.left {
                left.path_length(value, depth + 1)
            } else {
                depth + 1
            }
        } else {
            if let Some(ref right) = self.right {
                right.path_length(value, depth + 1)
            } else {
                depth + 1
            }
        }
    }
}

/// Unified anomaly detector interface
pub struct AnomalyDetector {
    detectors: Vec<Box<dyn AnomalyDetectorTrait>>,
}

trait AnomalyDetectorTrait: Send + Sync {
    fn detect(&self, values: &[f64]) -> AnomalyResult;
}

impl AnomalyDetector {
    pub fn new() -> Self {
        Self {
            detectors: Vec::new(),
        }
    }
    
    pub fn with_zscore(mut self, threshold: f64, window_size: usize) -> Self {
        self.detectors.push(Box::new(ZScoreDetector::new(threshold, window_size)));
        self
    }
    
    pub fn with_iqr(mut self, multiplier: f64) -> Self {
        self.detectors.push(Box::new(IQRDetector::new(multiplier)));
        self
    }
    
    pub fn with_isolation_forest(mut self, num_trees: usize, sample_size: usize) -> Self {
        let mut detector = IsolationForestDetector::new(num_trees, sample_size);
        self.detectors.push(Box::new(detector));
        self
    }
    
    pub fn detect(&self, values: &[f64]) -> Vec<AnomalyResult> {
        self.detectors.iter()
            .map(|detector| detector.detect(values))
            .collect()
    }
    
    pub fn detect_ensemble(&self, values: &[f64]) -> AnomalyResult {
        let results = self.detect(values);
        
        if results.is_empty() {
            return AnomalyResult {
                scores: vec![],
                anomalies: vec![],
                threshold: 0.0,
                model_name: "Ensemble".to_string(),
            };
        }
        
        // Average scores
        let num_values = results[0].scores.len();
        let mut avg_scores = vec![0.0; num_values];
        
        for result in &results {
            for (i, &score) in result.scores.iter().enumerate() {
                avg_scores[i] += score;
            }
        }
        
        for score in &mut avg_scores {
            *score /= results.len() as f64;
        }
        
        // Use average threshold
        let threshold: f64 = results.iter().map(|r| r.threshold).sum::<f64>() / results.len() as f64;
        
        // Find anomalies
        let mut anomalies = Vec::new();
        for (i, &score) in avg_scores.iter().enumerate() {
            if score > threshold {
                anomalies.push(AnomalyPoint {
                    index: i,
                    value: values[i],
                    score,
                    timestamp: None,
                });
            }
        }
        
        AnomalyResult {
            scores: avg_scores,
            anomalies,
            threshold,
            model_name: "Ensemble".to_string(),
        }
    }
}

impl Default for AnomalyDetector {
    fn default() -> Self {
        Self::new()
            .with_zscore(3.0, 100)
            .with_iqr(1.5)
    }
}
