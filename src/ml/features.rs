//! Feature extraction for time-series
//! 
//! Extracts features from time-series data for ML model training:
//! - Statistical features
//! - Temporal features
//! - Trend features
//! - Frequency domain features

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Extracted time-series features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesFeatures {
    pub statistical: StatisticalFeatures,
    pub temporal: TemporalFeatures,
    pub trend: TrendFeatures,
    pub spectral: SpectralFeatures,
    pub custom: HashMap<String, f64>,
}

/// Statistical features
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatisticalFeatures {
    pub mean: f64,
    pub median: f64,
    pub std_dev: f64,
    pub variance: f64,
    pub min: f64,
    pub max: f64,
    pub range: f64,
    pub q1: f64,
    pub q3: f64,
    pub iqr: f64,
    pub skewness: f64,
    pub kurtosis: f64,
    pub entropy: f64,
    pub coefficient_of_variation: f64,
    pub energy: f64,
}

/// Temporal features
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TemporalFeatures {
    pub zero_crossings: usize,
    pub mean_crossings: usize,
    pub first_acf: f64,
    pub peaks: usize,
    pub troughs: usize,
    pub stationarity: f64,
    pub trend_strength: f64,
    pub seasonality_strength: f64,
}

/// Trend features
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrendFeatures {
    pub slope: f64,
    pub intercept: f64,
    pub residual_variance: f64,
    pub trend_direction: i32, // -1 down, 0 flat, 1 up
    pub change_points: Vec<usize>,
}

/// Spectral features
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpectralFeatures {
    pub dominant_frequency: f64,
    pub spectral_centroid: f64,
    pub spectral_rolloff: f64,
    pub spectral_entropy: f64,
    pub spectral_energy: f64,
}

/// Feature extractor
pub struct FeatureExtractor {
    include_statistical: bool,
    include_temporal: bool,
    include_trend: bool,
    include_spectral: bool,
}

impl FeatureExtractor {
    pub fn new() -> Self {
        Self {
            include_statistical: true,
            include_temporal: true,
            include_trend: true,
            include_spectral: true,
        }
    }
    
    pub fn with_statistical(mut self, include: bool) -> Self {
        self.include_statistical = include;
        self
    }
    
    pub fn with_temporal(mut self, include: bool) -> Self {
        self.include_temporal = include;
        self
    }
    
    pub fn with_trend(mut self, include: bool) -> Self {
        self.include_trend = include;
        self
    }
    
    pub fn with_spectral(mut self, include: bool) -> Self {
        self.include_spectral = include;
        self
    }
    
    /// Extract all features from a time-series
    pub fn extract(&self, values: &[f64]) -> TimeSeriesFeatures {
        let statistical = if self.include_statistical {
            self.statistical_features(values)
        } else {
            StatisticalFeatures::default()
        };
        
        let temporal = if self.include_temporal {
            self.temporal_features(values)
        } else {
            TemporalFeatures::default()
        };
        
        let trend = if self.include_trend {
            self.trend_features(values)
        } else {
            TrendFeatures::default()
        };
        
        let spectral = if self.include_spectral {
            self.spectral_features(values)
        } else {
            SpectralFeatures::default()
        };
        
        TimeSeriesFeatures {
            statistical,
            temporal,
            trend,
            spectral,
            custom: HashMap::new(),
        }
    }
    
    /// Extract statistical features
    pub fn statistical_features(&self, values: &[f64]) -> StatisticalFeatures {
        if values.is_empty() {
            return StatisticalFeatures::default();
        }
        
        let n = values.len() as f64;
        
        // Basic statistics
        let sum: f64 = values.iter().sum();
        let mean = sum / n;
        
        let variance: f64 = values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / (n - 1.0).max(1.0);
        
        let std_dev = variance.sqrt();
        
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let min = sorted.first().copied().unwrap_or(0.0);
        let max = sorted.last().copied().unwrap_or(0.0);
        let range = max - min;
        
        let q1 = Self::percentile(&sorted, 25.0);
        let q3 = Self::percentile(&sorted, 75.0);
        let iqr = q3 - q1;
        
        let median = Self::percentile(&sorted, 50.0);
        
        // Higher moments
        let skewness = if std_dev > 1e-10 {
            let m3: f64 = values.iter()
                .map(|v| (v - mean).powi(3))
                .sum::<f64>() / n;
            m3 / std_dev.powi(3)
        } else {
            0.0
        };
        
        let kurtosis = if std_dev > 1e-10 {
            let m4: f64 = values.iter()
                .map(|v| (v - mean).powi(4))
                .sum::<f64>() / n;
            m4 / std_dev.powi(4) - 3.0
        } else {
            0.0
        };
        
        // Entropy
        let entropy = Self::entropy(values);
        
        // Coefficient of variation
        let coefficient_of_variation = if mean.abs() > 1e-10 {
            std_dev / mean.abs()
        } else {
            0.0
        };
        
        // Energy
        let energy: f64 = values.iter().map(|v| v * v).sum();
        
        StatisticalFeatures {
            mean,
            median,
            std_dev,
            variance,
            min,
            max,
            range,
            q1,
            q3,
            iqr,
            skewness,
            kurtosis,
            entropy,
            coefficient_of_variation,
            energy,
        }
    }
    
    /// Extract temporal features
    pub fn temporal_features(&self, values: &[f64]) -> TemporalFeatures {
        if values.is_empty() {
            return TemporalFeatures::default();
        }
        
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        
        // Zero crossings
        let zero_crossings = values.windows(2)
            .filter(|w| w[0] * w[1] < 0.0)
            .count();
        
        // Mean crossings
        let mean_crossings = values.windows(2)
            .filter(|w| (w[0] - mean) * (w[1] - mean) < 0.0)
            .count();
        
        // First autocorrelation
        let first_acf = self.autocorrelation(values, 1);
        
        // Peaks and troughs
        let (peaks, troughs) = self.count_peaks_troughs(values);
        
        // Stationarity (simplified)
        let stationarity = self.approx_stationarity(values);
        
        // Trend and seasonality strength
        let (trend_strength, seasonality_strength) = self.decompose_strength(values);
        
        TemporalFeatures {
            zero_crossings,
            mean_crossings,
            first_acf,
            peaks,
            troughs,
            stationarity,
            trend_strength,
            seasonality_strength,
        }
    }
    
    /// Extract trend features
    pub fn trend_features(&self, values: &[f64]) -> TrendFeatures {
        if values.len() < 2 {
            return TrendFeatures::default();
        }
        
        // Linear regression
        let x: Vec<f64> = (0..values.len()).map(|i| i as f64).collect();
        let (slope, intercept) = self.linear_regression(&x, values);
        
        // Calculate residuals
        let residuals: Vec<f64> = values.iter()
            .enumerate()
            .map(|(i, &v)| v - (slope * i as f64 + intercept))
            .collect();
        
        let residual_variance: f64 = residuals.iter()
            .map(|r| r * r)
            .sum::<f64>() / residuals.len() as f64;
        
        // Trend direction
        let trend_direction = if slope > 0.01 {
            1
        } else if slope < -0.01 {
            -1
        } else {
            0
        };
        
        // Change points (simplified)
        let change_points = self.find_change_points(values);
        
        TrendFeatures {
            slope,
            intercept,
            residual_variance,
            trend_direction,
            change_points,
        }
    }
    
    /// Extract spectral features
    pub fn spectral_features(&self, values: &[f64]) -> SpectralFeatures {
        // Simplified spectral analysis using DFT
        let n = values.len();
        let frequencies: Vec<f64> = (0..n / 2)
            .map(|k| k as f64 / n as f64)
            .collect();
        
        let magnitudes: Vec<f64> = self.dft_magnitudes(values);
        
        if magnitudes.is_empty() {
            return SpectralFeatures::default();
        }
        
        let total_energy: f64 = magnitudes.iter().map(|m| m * m).sum();
        
        // Dominant frequency
        let (dominant_idx, dominant_magnitude) = magnitudes.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or((0, &0.0));
        
        let dominant_frequency = if dominant_idx < frequencies.len() {
            frequencies[dominant_idx]
        } else {
            0.0
        };
        
        // Spectral centroid
        let spectral_centroid = if total_energy > 0.0 {
            frequencies.iter()
                .zip(magnitudes.iter())
                .map(|(f, m)| f * m * m)
                .sum::<f64>() / total_energy
        } else {
            0.0
        };
        
        // Spectral rolloff (frequency below which 85% of energy is contained)
        let threshold = 0.85 * total_energy;
        let mut cumulative = 0.0;
        let mut spectral_rolloff = 0.0;
        
        for (i, m) in magnitudes.iter().enumerate() {
            cumulative += m * m;
            if cumulative >= threshold {
                spectral_rolloff = frequencies.get(i).copied().unwrap_or(0.0);
                break;
            }
        }
        
        // Spectral entropy
        let spectral_entropy = Self::entropy(&magnitudes);
        
        SpectralFeatures {
            dominant_frequency,
            spectral_centroid,
            spectral_rolloff,
            spectral_entropy,
            spectral_energy: total_energy,
        }
    }
    
    /// Convert features to a flat vector for ML models
    pub fn to_vector(&self, features: &TimeSeriesFeatures) -> Vec<f64> {
        let mut vec = Vec::new();
        
        // Statistical
        vec.push(features.statistical.mean);
        vec.push(features.statistical.median);
        vec.push(features.statistical.std_dev);
        vec.push(features.statistical.variance);
        vec.push(features.statistical.min);
        vec.push(features.statistical.max);
        vec.push(features.statistical.range);
        vec.push(features.statistical.q1);
        vec.push(features.statistical.q3);
        vec.push(features.statistical.iqr);
        vec.push(features.statistical.skewness);
        vec.push(features.statistical.kurtosis);
        vec.push(features.statistical.entropy);
        vec.push(features.statistical.coefficient_of_variation);
        vec.push(features.statistical.energy);
        
        // Temporal
        vec.push(features.temporal.zero_crossings as f64);
        vec.push(features.temporal.mean_crossings as f64);
        vec.push(features.temporal.first_acf);
        vec.push(features.temporal.peaks as f64);
        vec.push(features.temporal.troughs as f64);
        vec.push(features.temporal.stationarity);
        vec.push(features.temporal.trend_strength);
        vec.push(features.temporal.seasonality_strength);
        
        // Trend
        vec.push(features.trend.slope);
        vec.push(features.trend.intercept);
        vec.push(features.trend.residual_variance);
        vec.push(features.trend.trend_direction as f64);
        vec.push(features.trend.change_points.len() as f64);
        
        // Spectral
        vec.push(features.spectral.dominant_frequency);
        vec.push(features.spectral.spectral_centroid);
        vec.push(features.spectral.spectral_rolloff);
        vec.push(features.spectral.spectral_entropy);
        vec.push(features.spectral.spectral_energy);
        
        // Custom
        for (_, v) in &features.custom {
            vec.push(*v);
        }
        
        vec
    }
    
    // Helper functions
    
    fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        
        let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
    
    fn entropy(values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        
        let sum: f64 = values.iter().map(|v| v.abs()).sum();
        if sum < 1e-10 {
            return 0.0;
        }
        
        let probs: Vec<f64> = values.iter()
            .map(|v| v.abs() / sum)
            .filter(|&p| p > 0.0)
            .collect();
        
        -probs.iter()
            .map(|p| p * p.log2())
            .sum::<f64>()
    }
    
    fn autocorrelation(values: &[f64], lag: usize) -> f64 {
        if values.len() <= lag {
            return 0.0;
        }
        
        let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
        let n = values.len() - lag;
        
        let c0: f64 = values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        
        if c0 < 1e-10 {
            return 0.0;
        }
        
        let ck: f64 = values.iter()
            .skip(lag)
            .zip(values.iter())
            .map(|(v1, v2)| (v1 - mean) * (v2 - mean))
            .sum::<f64>() / values.len() as f64;
        
        ck / c0
    }
    
    fn count_peaks_troughs(&self, values: &[f64]) -> (usize, usize) {
        if values.len() < 3 {
            return (0, 0);
        }
        
        let mut peaks = 0;
        let mut troughs = 0;
        
        for i in 1..values.len() - 1 {
            if values[i] > values[i - 1] && values[i] > values[i + 1] {
                peaks += 1;
            } else if values[i] < values[i - 1] && values[i] < values[i + 1] {
                troughs += 1;
            }
        }
        
        (peaks, troughs)
    }
    
    fn approx_stationarity(&self, values: &[f64]) -> f64 {
        // Simplified: compare variance of first and second half
        if values.len() < 4 {
            return 1.0;
        }
        
        let mid = values.len() / 2;
        let first_half = &values[..mid];
        let second_half = &values[mid..];
        
        let mean1 = first_half.iter().sum::<f64>() / first_half.len() as f64;
        let mean2 = second_half.iter().sum::<f64>() / second_half.len() as f64;
        
        let var1: f64 = first_half.iter().map(|v| (v - mean1).powi(2)).sum::<f64>() / first_half.len() as f64;
        let var2: f64 = second_half.iter().map(|v| (v - mean2).powi(2)).sum::<f64>() / second_half.len() as f64;
        
        let ratio = var1.min(var2) / var1.max(var2);
        ratio.max(0.0).min(1.0)
    }
    
    fn decompose_strength(&self, values: &[f64]) -> (f64, f64) {
        // Simplified decomposition strength estimation
        let trend_strength = 0.5; // Placeholder
        let seasonality_strength = 0.3; // Placeholder
        (trend_strength, seasonality_strength)
    }
    
    fn linear_regression(&self, x: &[f64], y: &[f64]) -> (f64, f64) {
        let n = x.len().min(y.len()) as f64;
        if n < 2.0 {
            return (0.0, 0.0);
        }
        
        let sum_x: f64 = x.iter().sum();
        let sum_y: f64 = y.iter().sum();
        let sum_xy: f64 = x.iter().zip(y.iter()).map(|(a, b)| a * b).sum();
        let sum_xx: f64 = x.iter().map(|v| v * v).sum();
        
        let denom = n * sum_xx - sum_x * sum_x;
        if denom.abs() < 1e-10 {
            return (0.0, sum_y / n);
        }
        
        let slope = (n * sum_xy - sum_x * sum_y) / denom;
        let intercept = (sum_y - slope * sum_x) / n;
        
        (slope, intercept)
    }
    
    fn find_change_points(&self, values: &[f64]) -> Vec<usize> {
        // Simplified change point detection
        let threshold = 2.0;
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let std_dev = values.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>()
            .sqrt() / values.len().max(1) as f64;
        
        let mut change_points = Vec::new();
        let mut prev_diff = 0.0;
        
        for i in 1..values.len() {
            let diff = values[i] - values[i - 1];
            if prev_diff * diff < 0.0 && diff.abs() > threshold * std_dev {
                change_points.push(i);
            }
            prev_diff = diff;
        }
        
        change_points
    }
    
    fn dft_magnitudes(&self, values: &[f64]) -> Vec<f64> {
        let n = values.len();
        let mut magnitudes = Vec::with_capacity(n / 2);
        
        for k in 0..n / 2 {
            let mut real = 0.0;
            let mut imag = 0.0;
            
            for (t, &value) in values.iter().enumerate() {
                let angle = 2.0 * std::f64::consts::PI * k as f64 * t as f64 / n as f64;
                real += value * angle.cos();
                imag += value * angle.sin();
            }
            
            magnitudes.push((real * real + imag * imag).sqrt());
        }
        
        magnitudes
    }
}

impl Default for FeatureExtractor {
    fn default() -> Self {
        Self::new()
    }
}
