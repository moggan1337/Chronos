//! Time-series functions
//! 
//! Built-in functions for time-series analysis:
//! - Aggregation functions
//! - Windowing functions
//! - Rate and delta calculations
//! - Statistical functions

use chrono::{Duration, DateTime, Utc};
use ndarray::{Array1, Array2};
use std::collections::HashMap;

/// Time-series function registry
pub struct TimeSeriesFunctions;

impl TimeSeriesFunctions {
    /// Get all available function names
    pub fn list_functions() -> Vec<(&'static str, &'static str)> {
        vec![
            // Aggregation
            ("MEAN", "Calculate the mean of values"),
            ("SUM", "Calculate the sum of values"),
            ("MIN", "Find the minimum value"),
            ("MAX", "Find the maximum value"),
            ("COUNT", "Count non-null values"),
            ("FIRST", "Get the first value"),
            ("LAST", "Get the last value"),
            ("MEDIAN", "Calculate the median"),
            ("STDDEV", "Calculate standard deviation"),
            ("VARIANCE", "Calculate variance"),
            ("PERCENTILE", "Calculate percentile"),
            
            // Time-series specific
            ("RATE", "Calculate rate of change"),
            ("DELTA", "Calculate difference between values"),
            ("MOVING_AVG", "Calculate moving average"),
            ("EMA", "Exponential moving average"),
            ("BUCKET", "Group by time bucket"),
            ("FILL", "Fill missing values"),
            ("INTERPOLATE", "Interpolate missing values"),
            
            // Statistical
            ("TREND", "Detect trend direction"),
            ("ANOMALY_SCORE", "Calculate anomaly score"),
            ("CORRELATION", "Calculate correlation"),
            ("COVARIANCE", "Calculate covariance"),
            
            // Forecasting
            ("HOLT_WINTERS", "Holt-Winters forecasting"),
            ("LINEAR_REGRESSION", "Simple linear regression"),
        ]
    }
    
    /// Calculate rate of change
    pub fn rate(values: &[f64], timestamps: &[i64]) -> Vec<f64> {
        if values.len() < 2 {
            return vec![0.0; values.len()];
        }
        
        let mut rates = Vec::with_capacity(values.len());
        rates.push(0.0); // First value has no rate
        
        for i in 1..values.len() {
            let delta_value = values[i] - values[i - 1];
            let delta_time = (timestamps[i] - timestamps[i - 1]) as f64 / 1_000_000.0; // microseconds to seconds
            
            if delta_time > 0.0 {
                rates.push(delta_value / delta_time);
            } else {
                rates.push(0.0);
            }
        }
        
        rates
    }
    
    /// Calculate delta (difference between consecutive values)
    pub fn delta(values: &[f64]) -> Vec<f64> {
        if values.len() < 2 {
            return vec![0.0; values.len()];
        }
        
        let mut deltas = Vec::with_capacity(values.len());
        deltas.push(0.0);
        
        for i in 1..values.len() {
            deltas.push(values[i] - values[i - 1]);
        }
        
        deltas
    }
    
    /// Calculate moving average
    pub fn moving_avg(values: &[f64], window: usize) -> Vec<f64> {
        if values.is_empty() || window == 0 {
            return Vec::new();
        }
        
        let window = window.min(values.len());
        let mut result = Vec::with_capacity(values.len());
        
        for i in 0..values.len() {
            let start = i.saturating_sub(window - 1);
            let slice = &values[start..=i];
            let sum: f64 = slice.iter().sum();
            result.push(sum / slice.len() as f64);
        }
        
        result
    }
    
    /// Calculate exponential moving average
    pub fn ema(values: &[f64], alpha: f64) -> Vec<f64> {
        if values.is_empty() {
            return Vec::new();
        }
        
        let alpha = alpha.clamp(0.0, 1.0);
        let mut result = Vec::with_capacity(values.len());
        result.push(values[0]);
        
        for i in 1..values.len() {
            let ema_prev = result[i - 1];
            let ema = alpha * values[i] + (1.0 - alpha) * ema_prev;
            result.push(ema);
        }
        
        result
    }
    
    /// Fill missing values using forward fill
    pub fn forward_fill(values: &[Option<f64>]) -> Vec<f64> {
        let mut result = Vec::with_capacity(values.len());
        let mut last_value: Option<f64> = None;
        
        for &opt in values {
            match opt {
                Some(v) => {
                    last_value = Some(v);
                    result.push(v);
                }
                None => {
                    if let Some(v) = last_value {
                        result.push(v);
                    } else {
                        result.push(0.0);
                    }
                }
            }
        }
        
        result
    }
    
    /// Interpolate missing values
    pub fn interpolate_linear(values: &[Option<f64>]) -> Vec<f64> {
        let mut result = Vec::with_capacity(values.len());
        
        let mut i = 0;
        while i < values.len() {
            match values[i] {
                Some(v) => {
                    result.push(v);
                    i += 1;
                }
                None => {
                    // Find the next non-None value
                    let start_idx = i;
                    let mut j = i + 1;
                    while j < values.len() && values[j].is_none() {
                        j += 1;
                    }
                    
                    // Get endpoints
                    let start_val = if start_idx > 0 {
                        values[start_idx - 1]
                    } else if j < values.len() {
                        values[j]
                    } else {
                        None
                    };
                    
                    let end_val = if j < values.len() {
                        values[j]
                    } else if start_idx > 0 {
                        values[start_idx - 1]
                    } else {
                        None
                    };
                    
                    match (start_val, end_val) {
                        (Some(start), Some(end)) => {
                            let count = j - start_idx;
                            for k in 0..count {
                                let t = k as f64 / count as f64;
                                let interpolated = start + t * (end - start);
                                result.push(interpolated);
                            }
                        }
                        (Some(v), None) | (None, Some(v)) => {
                            for _ in start_idx..j {
                                result.push(v);
                            }
                        }
                        (None, None) => {
                            for _ in start_idx..j {
                                result.push(0.0);
                            }
                        }
                    }
                    
                    i = j;
                }
            }
        }
        
        result
    }
    
    /// Calculate standard deviation
    pub fn stddev(values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        
        let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
        let variance: f64 = values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / (values.len() - 1) as f64;
        
        variance.sqrt()
    }
    
    /// Calculate percentile
    pub fn percentile(values: &[f64], p: f64) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let p = p.clamp(0.0, 100.0);
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
    
    /// Detect trend (returns 1 for up, -1 for down, 0 for flat)
    pub fn trend(values: &[f64], threshold: f64) -> i32 {
        if values.len() < 2 {
            return 0;
        }
        
        let diffs: Vec<f64> = values.windows(2)
            .map(|w| w[1] - w[0])
            .collect();
        
        let avg_diff: f64 = diffs.iter().sum::<f64>() / diffs.len() as f64;
        
        if avg_diff > threshold {
            1
        } else if avg_diff < -threshold {
            -1
        } else {
            0
        }
    }
    
    /// Calculate simple linear regression
    pub fn linear_regression(x: &[f64], y: &[f64]) -> Option<(f64, f64)> {
        if x.len() != y.len() || x.len() < 2 {
            return None;
        }
        
        let n = x.len() as f64;
        let sum_x: f64 = x.iter().sum();
        let sum_y: f64 = y.iter().sum();
        let sum_xy: f64 = x.iter().zip(y.iter()).map(|(a, b)| a * b).sum();
        let sum_xx: f64 = x.iter().map(|v| v * v).sum();
        
        let denom = n * sum_xx - sum_x * sum_x;
        if denom.abs() < 1e-10 {
            return None;
        }
        
        let slope = (n * sum_xy - sum_x * sum_y) / denom;
        let intercept = (sum_y - slope * sum_x) / n;
        
        Some((slope, intercept))
    }
    
    /// Calculate moving standard deviation
    pub fn moving_stddev(values: &[f64], window: usize) -> Vec<f64> {
        if values.is_empty() || window < 2 {
            return Vec::new();
        }
        
        let mut result = Vec::with_capacity(values.len());
        
        for i in 0..values.len() {
            let start = i.saturating_sub(window - 1);
            let slice = &values[start..=i];
            result.push(Self::stddev(slice));
        }
        
        result
    }
    
    /// Calculate Z-score for anomaly detection
    pub fn zscore(value: f64, mean: f64, stddev: f64) -> f64 {
        if stddev.abs() < 1e-10 {
            0.0
        } else {
            (value - mean) / stddev
        }
    }
    
    /// Calculate anomaly score using moving average and standard deviation
    pub fn anomaly_score(values: &[f64], window: usize, threshold: f64) -> Vec<f64> {
        if values.len() < window {
            return vec![0.0; values.len()];
        }
        
        let mut scores = Vec::with_capacity(values.len());
        let mut moving_std = Self::moving_stddev(values, window);
        
        for i in 0..values.len() {
            if i < window - 1 {
                scores.push(0.0);
            } else {
                let mean = moving_avg(&values[i.saturating_sub(window - 1)..=i], window);
                let mean_val = mean.last().copied().unwrap_or(0.0);
                let std_val = moving_std[i];
                
                let score = Self::zscore(values[i], mean_val, std_val);
                
                // Mark as anomaly if score exceeds threshold
                if score.abs() > threshold {
                    scores.push(score.abs());
                } else {
                    scores.push(0.0);
                }
            }
        }
        
        scores
    }
    
    /// Group values into time buckets
    pub fn bucket<T: Clone>(
        timestamps: &[i64],
        values: &[T],
        bucket_duration: Duration,
    ) -> HashMap<i64, Vec<(i64, T)>> 
    {
        let mut buckets: HashMap<i64, Vec<(i64, T)>> = HashMap::new();
        let duration_ms = bucket_duration.num_microseconds().unwrap_or(3600_000_000);
        
        for i in 0..timestamps.len().min(values.len()) {
            let bucket_key = (timestamps[i] / duration_ms) * duration_ms;
            buckets.entry(bucket_key)
                .or_insert_with(Vec::new)
                .push((timestamps[i], values[i].clone()));
        }
        
        buckets
    }
    
    /// Apply aggregation to bucketed data
    pub fn aggregate_bucket<F>(bucket: &[(i64, f64)], agg: AggregationType) -> f64 {
        let values: Vec<f64> = bucket.iter().map(|(_, v)| *v).collect();
        
        match agg {
            AggregationType::Mean => values.iter().sum::<f64>() / values.len() as f64,
            AggregationType::Sum => values.iter().sum(),
            AggregationType::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
            AggregationType::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            AggregationType::Count => values.len() as f64,
            AggregationType::First => values.first().copied().unwrap_or(0.0),
            AggregationType::Last => values.last().copied().unwrap_or(0.0),
            AggregationType::Median => Self::percentile(&values, 50.0),
            AggregationType::StdDev => Self::stddev(&values),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AggregationType {
    Mean,
    Sum,
    Min,
    Max,
    Count,
    First,
    Last,
    Median,
    StdDev,
}
