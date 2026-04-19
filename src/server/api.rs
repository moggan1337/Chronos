//! REST API server
//! 
//! HTTP endpoints for Chronos operations.

use serde::{Serialize, Deserialize};
use std::sync::Arc;
use crate::storage::Engine;
use crate::query::{QueryExecutor, QueryParser};
use crate::ml::ModelRegistry;
use crate::streaming::{StreamProducer, ProducerConfig};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<crate::query::QueryError> for ApiError {
    fn from(e: crate::query::QueryError) -> Self {
        ApiError::Internal(e.to_string())
    }
}

impl From<crate::storage::StorageError> for ApiError {
    fn from(e: crate::storage::StorageError) -> Self {
        ApiError::Internal(e.to_string())
    }
}

/// API response wrapper
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }
    
    pub fn error(msg: &str) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

/// HTTP API handler
pub struct ApiHandler {
    engine: Arc<Engine>,
    query_executor: QueryExecutor,
    query_parser: QueryParser,
    model_registry: ModelRegistry,
    producer: Option<StreamProducer>,
}

impl ApiHandler {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine: engine.clone(),
            query_executor: QueryExecutor::new(engine),
            query_parser: QueryParser::new(),
            model_registry: ModelRegistry::new(),
            producer: None,
        }
    }
    
    /// Execute SQL query
    pub async fn query(&self, sql: &str) -> Result<ApiResponse<QueryResponse>, ApiError> {
        let query = self.query_parser.parse(sql)?;
        let result = self.query_executor.execute(&query).await?;
        
        Ok(ApiResponse::success(QueryResponse {
            num_rows: result.num_rows(),
            execution_time_ms: result.execution_time_ms,
            batches: result.batches.len(),
        }))
    }
    
    /// Create a table
    pub async fn create_table(&self, req: CreateTableRequest) -> Result<ApiResponse<TableInfo>, ApiError> {
        let schema = crate::storage::schema::Schema::parse(&req.schema)
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        
        let config = crate::storage::engine::TableConfig {
            time_column: req.time_column.unwrap_or_else(|| "time".to_string()),
            tags: req.tags.unwrap_or_default(),
            fields: req.fields.unwrap_or_default(),
            retention_id: None,
            downsampling: Vec::new(),
        };
        
        let table = self.engine.create_table(&req.name, schema, config).await?;
        
        Ok(ApiResponse::success(TableInfo {
            name: table.name.clone(),
            id: table.id.to_string(),
            num_partitions: 0,
            num_rows: 0,
        }))
    }
    
    /// Write data to a table
    pub async fn write(&self, req: WriteRequest) -> Result<ApiResponse<WriteResponse>, ApiError> {
        // Convert JSON to Arrow batch
        // Simplified - in production would use proper conversion
        let num_rows = req.values.len();
        
        Ok(ApiResponse::success(WriteResponse {
            rows_written: num_rows,
        }))
    }
    
    /// Get table info
    pub async fn get_table(&self, name: &str) -> Result<ApiResponse<TableInfo>, ApiError> {
        let stats = self.engine.get_stats(name)?;
        
        Ok(ApiResponse::success(TableInfo {
            name: name.to_string(),
            id: String::new(),
            num_partitions: stats.num_partitions,
            num_rows: stats.total_rows,
        }))
    }
    
    /// List tables
    pub async fn list_tables(&self) -> Result<ApiResponse<Vec<String>>, ApiError> {
        let tables: Vec<String> = self.engine.tables.iter().map(|t| t.name.clone()).collect();
        Ok(ApiResponse::success(tables))
    }
    
    /// Detect anomalies
    pub async fn detect_anomalies(&self, req: AnomalyRequest) -> Result<ApiResponse<AnomalyResponse>, ApiError> {
        let detector = crate::ml::anomaly::AnomalyDetector::default()
            .with_zscore(req.threshold.unwrap_or(3.0), req.window.unwrap_or(100) as usize);
        
        let values: Vec<f64> = req.values;
        let results = detector.detect(&values);
        
        Ok(ApiResponse::success(AnomalyResponse {
            scores: results[0].scores.clone(),
            anomalies: results[0].anomalies.iter().map(|a| a.index).collect(),
        }))
    }
    
    /// Generate forecast
    pub async fn forecast(&self, req: ForecastRequest) -> Result<ApiResponse<ForecastResponse>, ApiError> {
        let mut forecaster = crate::ml::forecasting::Forecaster::simple_exponential_smoothing(
            req.alpha.unwrap_or(0.3)
        );
        
        forecaster.train(&req.values)?;
        let result = forecaster.forecast(req.horizon.unwrap_or(10), 0.95);
        
        Ok(ApiResponse::success(ForecastResponse {
            predictions: result.predictions,
            lower_bounds: result.lower_bounds,
            upper_bounds: result.upper_bounds,
        }))
    }
    
    /// Get server health
    pub async fn health(&self) -> ApiResponse<HealthStatus> {
        ApiResponse::success(HealthStatus {
            status: "healthy".to_string(),
            version: crate::VERSION.to_string(),
        })
    }
}

// Request/Response types

#[derive(Debug, Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub schema: String,
    pub time_column: Option<String>,
    pub tags: Option<Vec<String>>,
    pub fields: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub name: String,
    pub id: String,
    pub num_partitions: usize,
    pub num_rows: usize,
}

#[derive(Debug, Deserialize)]
pub struct WriteRequest {
    pub table: String,
    pub values: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct WriteResponse {
    pub rows_written: usize,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub num_rows: usize,
    pub execution_time_ms: u64,
    pub batches: usize,
}

#[derive(Debug, Deserialize)]
pub struct AnomalyRequest {
    pub values: Vec<f64>,
    pub threshold: Option<f64>,
    pub window: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct AnomalyResponse {
    pub scores: Vec<f64>,
    pub anomalies: Vec<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ForecastRequest {
    pub values: Vec<f64>,
    pub horizon: Option<usize>,
    pub alpha: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct ForecastResponse {
    pub predictions: Vec<f64>,
    pub lower_bounds: Vec<f64>,
    pub upper_bounds: Vec<f64>,
}

#[derive(Debug, Serialize)]
pub struct HealthStatus {
    pub status: String,
    pub version: String,
}

/// REST API server
pub struct ApiServer {
    handler: Arc<ApiHandler>,
    config: ServerConfig,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
        }
    }
}

impl ApiServer {
    pub fn new(engine: Arc<Engine>, config: ServerConfig) -> Self {
        Self {
            handler: Arc::new(ApiHandler::new(engine)),
            config,
        }
    }
    
    pub async fn start(&self) {
        tracing::info!("API server starting on {}:{}", self.config.host, self.config.port);
    }
    
    pub fn handler(&self) -> Arc<ApiHandler> {
        self.handler.clone()
    }
}
