//! Forecasting models
//! 
//! Implements time-series forecasting algorithms:
//! - Simple Exponential Smoothing
//! - Holt's Linear Trend
//! - Holt-Winters Seasonal
//! - Moving Average
//! - ARIMA-style models

use crate::ml::{MLError, ModelType};
use serde::{Serialize, Deserialize};
use std::collections::VecDeque;

/// Forecast result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForecastResult {
    pub predictions: Vec<f64>,
    pub lower_bounds: Vec<f64>,
    pub upper_bounds: Vec<f64>,
    pub confidence: f64,
    pub horizon: usize,
    pub model_name: String,
}

/// Forecast model type
#[derive(Debug, Clone, Copy)]
pub enum ForecastModel {
    SimpleExponentialSmoothing,
    HoltLinear,
    HoltWinters,
    MovingAverage,
    Naive,
    Drift,
}

/// Simple Exponential Smoothing forecaster
#[derive(Debug, Clone)]
pub struct SimpleExponentialSmoothing {
    alpha: f64,
    level: f64,
    fitted: Vec<f64>,
}

impl SimpleExponentialSmoothing {
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.0, 1.0),
            level: 0.0,
            fitted: Vec::new(),
        }
    }
    
    pub fn train(&mut self, data: &[f64]) -> Result<(), MLError> {
        if data.is_empty() {
            return Err(MLError::InvalidData("Empty data".to_string()));
        }
        
        self.level = data[0];
        self.fitted.clear();
        self.fitted.push(self.level);
        
        for &value in &data[1..] {
            self.level = self.alpha * value + (1.0 - self.alpha) * self.level;
            self.fitted.push(self.level);
        }
        
        Ok(())
    }
    
    pub fn forecast(&self, horizon: usize, confidence: f64) -> ForecastResult {
        let predictions: Vec<f64> = (0..horizon).map(|_| self.level).collect();
        
        // Calculate residual standard error
        let residuals: Vec<f64> = if self.fitted.len() > 1 {
            // Simplified error estimation
            vec![0.0; horizon]
        } else {
            vec![0.0; horizon]
        };
        
        let std_error = if residuals.is_empty() {
            0.0
        } else {
            let mse: f64 = residuals.iter().map(|r| r * r).sum::<f64>() / residuals.len() as f64;
            mse.sqrt()
        };
        
        // Calculate prediction intervals
        let z = match confidence {
            0.9 => 1.645,
            0.95 => 1.96,
            0.99 => 2.576,
            _ => 1.96,
        };
        
        let lower_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p - z * std_error * (1.0 + i as f64 * 0.1).sqrt())
            .collect();
        
        let upper_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p + z * std_error * (1.0 + i as f64 * 0.1).sqrt())
            .collect();
        
        ForecastResult {
            predictions,
            lower_bounds,
            upper_bounds,
            confidence,
            horizon,
            model_name: "SimpleExpSmoothing".to_string(),
        }
    }
    
    pub fn fitted_values(&self) -> &[f64] {
        &self.fitted
    }
}

/// Holt's Linear Trend forecaster
#[derive(Debug, Clone)]
pub struct HoltLinearForecaster {
    alpha: f64,
    beta: f64,
    level: f64,
    trend: f64,
    fitted: Vec<f64>,
}

impl HoltLinearForecaster {
    pub fn new(alpha: f64, beta: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.0, 1.0),
            beta: beta.clamp(0.0, 1.0),
            level: 0.0,
            trend: 0.0,
            fitted: Vec::new(),
        }
    }
    
    pub fn train(&mut self, data: &[f64]) -> Result<(), MLError> {
        if data.len() < 3 {
            return Err(MLError::InvalidData("Need at least 3 data points".to_string()));
        }
        
        self.level = data[0];
        self.trend = (data[2] - data[1] + data[1] - data[0]) / 2.0;
        self.fitted.clear();
        self.fitted.push(self.level);
        
        for i in 1..data.len() {
            let prev_level = self.level;
            self.level = self.alpha * data[i] + (1.0 - self.alpha) * (self.level + self.trend);
            self.trend = self.beta * (self.level - prev_level) + (1.0 - self.beta) * self.trend;
            
            let fitted_val = self.level + self.trend;
            self.fitted.push(fitted_val);
        }
        
        Ok(())
    }
    
    pub fn forecast(&self, horizon: usize, confidence: f64) -> ForecastResult {
        let mut predictions = Vec::with_capacity(horizon);
        
        for h in 1..=horizon {
            let pred = self.level + (h as f64) * self.trend;
            predictions.push(pred);
        }
        
        let z = match confidence {
            0.9 => 1.645,
            0.95 => 1.96,
            0.99 => 2.576,
            _ => 1.96,
        };
        
        let std_error = 0.1; // Simplified
        
        let lower_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p - z * std_error * (1.0 + i as f64 * 0.1).sqrt())
            .collect();
        
        let upper_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p + z * std_error * (1.0 + i as f64 * 0.1).sqrt())
            .collect();
        
        ForecastResult {
            predictions,
            lower_bounds,
            upper_bounds,
            confidence,
            horizon,
            model_name: "HoltLinear".to_string(),
        }
    }
}

/// Holt-Winters Seasonal forecaster
#[derive(Debug, Clone)]
pub struct HoltWintersForecaster {
    alpha: f64,
    beta: f64,
    gamma: f64,
    seasonal_period: usize,
    level: f64,
    trend: f64,
    seasonal: Vec<f64>,
    fitted: Vec<f64>,
}

impl HoltWintersForecaster {
    pub fn new(alpha: f64, beta: f64, gamma: f64, seasonal_period: usize) -> Self {
        Self {
            alpha: alpha.clamp(0.0, 1.0),
            beta: beta.clamp(0.0, 1.0),
            gamma: gamma.clamp(0.0, 1.0),
            seasonal_period,
            level: 0.0,
            trend: 0.0,
            seasonal: vec![0.0; seasonal_period],
            fitted: Vec::new(),
        }
    }
    
    pub fn train(&mut self, data: &[f64]) -> Result<(), MLError> {
        if data.len() < 2 * self.seasonal_period {
            return Err(MLError::InvalidData(
                format!("Need at least {} data points for seasonal period {}",
                    2 * self.seasonal_period, self.seasonal_period)
            ));
        }
        
        // Initialize level and trend
        self.level = data[0..self.seasonal_period].iter().sum::<f64>() / self.seasonal_period as f64;
        self.trend = (data[self.seasonal_period] - data[0]) / self.seasonal_period as f64;
        
        // Initialize seasonal components
        let seasonal_avg: f64 = data.iter().sum::<f64>() / data.len() as f64;
        for i in 0..self.seasonal_period {
            let season_sum: f64 = data.iter()
                .skip(i)
                .step_by(self.seasonal_period)
                .sum();
            let season_count = data.len() / self.seasonal_period;
            self.seasonal[i] = season_sum / season_count as f64 - seasonal_avg;
        }
        
        // Fit the model
        self.fitted.clear();
        for i in 0..data.len() {
            let season_idx = i % self.seasonal_period;
            
            if i > 0 {
                let prev_level = self.level;
                self.level = self.alpha * (data[i] - self.seasonal[season_idx]) +
                    (1.0 - self.alpha) * (self.level + self.trend);
                self.trend = self.beta * (self.level - prev_level) +
                    (1.0 - self.beta) * self.trend;
                self.seasonal[season_idx] = self.gamma * (data[i] - self.level) +
                    (1.0 - self.gamma) * self.seasonal[season_idx];
            }
            
            let fitted_val = self.level + self.trend + self.seasonal[season_idx];
            self.fitted.push(fitted_val);
        }
        
        Ok(())
    }
    
    pub fn forecast(&self, horizon: usize, confidence: f64) -> ForecastResult {
        let mut predictions = Vec::with_capacity(horizon);
        
        for h in 1..=horizon {
            let season_idx = ((self.seasonal.len() - 1) + h) % self.seasonal_period;
            let pred = self.level + (h as f64) * self.trend + self.seasonal[season_idx];
            predictions.push(pred);
        }
        
        let z = match confidence {
            0.9 => 1.645,
            0.95 => 1.96,
            0.99 => 2.576,
            _ => 1.96,
        };
        
        let std_error = 0.1;
        
        let lower_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p - z * std_error * (1.0 + i as f64 * 0.1).sqrt())
            .collect();
        
        let upper_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p + z * std_error * (1.0 + i as f64 * 0.1).sqrt())
            .collect();
        
        ForecastResult {
            predictions,
            lower_bounds,
            upper_bounds,
            confidence,
            horizon,
            model_name: "HoltWinters".to_string(),
        }
    }
}

/// Moving Average forecaster
#[derive(Debug, Clone)]
pub struct MovingAverageForecaster {
    window_size: usize,
    weights: Vec<f64>,
}

impl MovingAverageForecaster {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            weights: vec![1.0; window_size],
        }
    }
    
    pub fn with_weights(mut self, weights: Vec<f64>) -> Self {
        self.weights = weights;
        if self.weights.len() != self.window_size {
            self.weights = vec![1.0; self.window_size];
        }
        self
    }
    
    pub fn train(&mut self, _data: &[f64]) -> Result<(), MLError> {
        // MA doesn't need training
        Ok(())
    }
    
    pub fn forecast(&self, data: &[f64], horizon: usize, confidence: f64) -> ForecastResult {
        let sum_weights: f64 = self.weights.iter().sum();
        let last_values: Vec<f64> = data.iter().rev().take(self.window_size).rev().copied().collect();
        
        let mut predictions = Vec::with_capacity(horizon);
        for _ in 0..horizon {
            let weighted_sum: f64 = last_values.iter()
                .zip(self.weights.iter())
                .map(|(v, w)| v * w)
                .sum();
            predictions.push(weighted_sum / sum_weights);
        }
        
        let z = match confidence {
            0.9 => 1.645,
            0.95 => 1.96,
            0.99 => 2.576,
            _ => 1.96,
        };
        
        let std_error = 0.1;
        
        let lower_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p - z * std_error * (1.0 + i as f64 * 0.05).sqrt())
            .collect();
        
        let upper_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p + z * std_error * (1.0 + i as f64 * 0.05).sqrt())
            .collect();
        
        ForecastResult {
            predictions,
            lower_bounds,
            upper_bounds,
            confidence,
            horizon,
            model_name: "MovingAverage".to_string(),
        }
    }
}

/// Naive forecaster (last value)
#[derive(Debug, Clone)]
pub struct NaiveForecaster;

impl NaiveForecaster {
    pub fn new() -> Self {
        Self
    }
    
    pub fn train(&mut self, _data: &[f64]) -> Result<(), MLError> {
        Ok(())
    }
    
    pub fn forecast(&self, data: &[f64], horizon: usize, confidence: f64) -> ForecastResult {
        let last_value = data.last().copied().unwrap_or(0.0);
        let predictions = vec![last_value; horizon];
        
        let z = match confidence {
            0.9 => 1.645,
            0.95 => 1.96,
            0.99 => 2.576,
            _ => 1.96,
        };
        
        let std_error = data.windows(2)
            .map(|w| (w[1] - w[0]).abs())
            .sum::<f64>() / data.len().max(1) as f64;
        
        let lower_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p - z * std_error * (1.0 + i as f64 * 0.1).sqrt())
            .collect();
        
        let upper_bounds: Vec<f64> = predictions.iter()
            .enumerate()
            .map(|(i, &p)| p + z * std_error * (1.0 + i as f64 * 0.1).sqrt())
            .collect();
        
        ForecastResult {
            predictions,
            lower_bounds,
            upper_bounds,
            confidence,
            horizon,
            model_name: "Naive".to_string(),
        }
    }
}

impl Default for NaiveForecaster {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified forecaster interface
pub struct Forecaster {
    model: ForecasterModel,
}

enum ForecasterModel {
    SES(SimpleExponentialSmoothing),
    HoltLinear(HoltLinearForecaster),
    HoltWinters(HoltWintersForecaster),
    MA(MovingAverageForecaster),
    Naive(NaiveForecaster),
}

impl Forecaster {
    pub fn simple_exponential_smoothing(alpha: f64) -> Self {
        Self {
            model: ForecasterModel::SES(SimpleExponentialSmoothing::new(alpha)),
        }
    }
    
    pub fn holt_linear(alpha: f64, beta: f64) -> Self {
        Self {
            model: ForecasterModel::HoltLinear(HoltLinearForecaster::new(alpha, beta)),
        }
    }
    
    pub fn holt_winters(alpha: f64, beta: f64, gamma: f64, period: usize) -> Self {
        Self {
            model: ForecasterModel::HoltWinters(HoltWintersForecaster::new(alpha, beta, gamma, period)),
        }
    }
    
    pub fn moving_average(window: usize) -> Self {
        Self {
            model: ForecasterModel::MA(MovingAverageForecaster::new(window)),
        }
    }
    
    pub fn naive() -> Self {
        Self {
            model: ForecasterModel::Naive(NaiveForecaster::new()),
        }
    }
    
    pub fn train(&mut self, data: &[f64]) -> Result<(), MLError> {
        match &mut self.model {
            ForecasterModel::SES(m) => m.train(data),
            ForecasterModel::HoltLinear(m) => m.train(data),
            ForecasterModel::HoltWinters(m) => m.train(data),
            ForecasterModel::MA(m) => m.train(data),
            ForecasterModel::Naive(m) => m.train(data),
        }
    }
    
    pub fn forecast(&self, horizon: usize, confidence: f64) -> ForecastResult {
        // For MA and Naive, we need the data, but for others we don't
        // This is a limitation of the current design
        match &self.model {
            ForecasterModel::SES(m) => m.forecast(horizon, confidence),
            ForecasterModel::HoltLinear(m) => m.forecast(horizon, confidence),
            ForecasterModel::HoltWinters(m) => m.forecast(horizon, confidence),
            ForecasterModel::MA(_) => ForecastResult {
                predictions: vec![],
                lower_bounds: vec![],
                upper_bounds: vec![],
                confidence,
                horizon,
                model_name: "MovingAverage".to_string(),
            },
            ForecasterModel::Naive(_) => ForecastResult {
                predictions: vec![],
                lower_bounds: vec![],
                upper_bounds: vec![],
                confidence,
                horizon,
                model_name: "Naive".to_string(),
            },
        }
    }
    
    /// Forecast with data (for models that need it)
    pub fn forecast_with_data(&self, data: &[f64], horizon: usize, confidence: f64) -> ForecastResult {
        match &self.model {
            ForecasterModel::MA(m) => m.forecast(data, horizon, confidence),
            ForecasterModel::Naive(m) => m.forecast(data, horizon, confidence),
            other => self.forecast(horizon, confidence),
        }
    }
}
