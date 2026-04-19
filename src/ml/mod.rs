//! Machine Learning module
//! 
//! Provides embedded ML models for time-series analysis:
//! - Anomaly detection (Isolation Forest, Statistical, LSTM-based)
//! - Forecasting (ARIMA, Exponential Smoothing, Prophet-style)
//! - Classification (Decision Trees, Random Forests)
//! - Feature extraction and engineering

pub mod anomaly;
pub mod forecasting;
pub mod classification;
pub mod features;

pub use anomaly::{AnomalyDetector, AnomalyResult, AnomalyModel};
pub use forecasting::{Forecaster, ForecastResult, ForecastModel};
pub use classification::{Classifier, ClassificationResult, ClassificationModel};
pub use features::{FeatureExtractor, TimeSeriesFeatures};

use serde::{Serialize, Deserialize};
use std::sync::Arc;
use dashmap::DashMap;
use uuid::Uuid;

/// ML model metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    pub id: Uuid,
    pub name: String,
    pub model_type: ModelType,
    pub version: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metrics: Option<ModelMetrics>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ModelType {
    AnomalyDetector,
    Forecaster,
    Classifier,
}

/// Model performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetrics {
    pub accuracy: Option<f64>,
    pub precision: Option<f64>,
    pub recall: Option<f64>,
    pub f1_score: Option<f64>,
    pub auc_roc: Option<f64>,
    pub mse: Option<f64>,
    pub rmse: Option<f64>,
    pub mae: Option<f64>,
}

/// Model registry for managing ML models
pub struct ModelRegistry {
    models: DashMap<Uuid, Arc<dyn ModelTrait>>,
    metadata: DashMap<Uuid, ModelMetadata>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: DashMap::new(),
            metadata: DashMap::new(),
        }
    }
    
    /// Register a model
    pub fn register<M: ModelTrait + 'static>(&self, name: &str, model: M) -> Uuid {
        let id = Uuid::new_v4();
        let metadata = ModelMetadata {
            id,
            name: name.to_string(),
            model_type: M::model_type(),
            version: "1.0.0".to_string(),
            created_at: chrono::Utc::now(),
            metrics: None,
        };
        
        self.models.insert(id, Arc::new(model));
        self.metadata.insert(id, metadata);
        
        tracing::info!("Registered model: {} ({})", name, id);
        id
    }
    
    /// Get a model by ID
    pub fn get(&self, id: Uuid) -> Option<Arc<dyn ModelTrait>> {
        self.models.get(&id).map(|r| r.clone())
    }
    
    /// Get model metadata
    pub fn get_metadata(&self, id: Uuid) -> Option<ModelMetadata> {
        self.metadata.get(&id).map(|r| r.clone())
    }
    
    /// List all models
    pub fn list(&self) -> Vec<ModelMetadata> {
        self.metadata.iter().map(|r| r.clone()).collect()
    }
    
    /// Remove a model
    pub fn remove(&self, id: Uuid) -> bool {
        self.models.remove(&id);
        self.metadata.remove(&id);
        true
    }
    
    /// Update model metrics
    pub fn update_metrics(&self, id: Uuid, metrics: ModelMetrics) {
        if let Some(mut meta) = self.metadata.get_mut(&id) {
            meta.metrics = Some(metrics);
        }
    }
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for ML models
pub trait ModelTrait: Send + Sync {
    /// Get the model type
    fn model_type() -> ModelType
    where
        Self: Sized;
    
    /// Get model name
    fn name(&self) -> &str;
    
    /// Train the model
    fn train(&mut self, data: &[f64], labels: Option<&[f64]>) -> Result<(), MLError>;
    
    /// Predict
    fn predict(&self, data: &[f64]) -> Result<Vec<f64>, MLError>;
    
    /// Save model to bytes
    fn save(&self) -> Result<Vec<u8>, MLError>;
    
    /// Load model from bytes
    fn load(data: &[u8]) -> Result<Self, MLError>
    where
        Self: Sized;
}

#[derive(Debug, thiserror::Error)]
pub enum MLError {
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("Training failed: {0}")]
    TrainingFailed(String),
    #[error("Prediction failed: {0}")]
    PredictionFailed(String),
    #[error("Model error: {0}")]
    ModelError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
}
