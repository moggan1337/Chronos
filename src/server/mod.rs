//! Chronos server
//! 
//! HTTP/REST and gRPC server for the time-series database.

pub mod api;
pub mod rpc;

pub use api::ApiServer;
pub use rpc::RpcServer;

use crate::storage::Engine;
use crate::query::{QueryExecutor, QueryParser};
use crate::ml::ModelRegistry;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Server error: {0}")]
    Error(String),
    #[error("Bind error: {0}")]
    BindError(String),
    #[error("Request error: {0}")]
    RequestError(String),
}

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub grpc_port: u16,
    pub max_connections: usize,
    pub request_timeout_ms: u64,
    pub enable_tls: bool,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            grpc_port: 50051,
            max_connections: 1000,
            request_timeout_ms: 30000,
            enable_tls: false,
            tls_cert: None,
            tls_key: None,
        }
    }
}

/// Chronos server
pub struct ChronosServer {
    config: ServerConfig,
    engine: Arc<Engine>,
    query_parser: QueryParser,
    query_executor: QueryExecutor,
    model_registry: ModelRegistry,
}

impl ChronosServer {
    pub fn new(config: ServerConfig, engine: Arc<Engine>) -> Self {
        let query_executor = QueryExecutor::new(engine.clone());
        
        Self {
            config,
            engine,
            query_parser: QueryParser::new(),
            query_executor,
            model_registry: ModelRegistry::new(),
        }
    }
    
    /// Start the server
    pub async fn start(&self) -> Result<(), ServerError> {
        tracing::info!("Starting Chronos server on {}:{}", self.config.host, self.config.port);
        
        // Start HTTP server
        self.start_http_server().await?;
        
        // Start gRPC server
        self.start_grpc_server().await?;
        
        Ok(())
    }
    
    async fn start_http_server(&self) -> Result<(), ServerError> {
        tracing::info!("HTTP API available at http://{}:{}/api/v1", self.config.host, self.config.port);
        // In production, would start actual HTTP server
        Ok(())
    }
    
    async fn start_grpc_server(&self) -> Result<(), ServerError> {
        tracing::info!("gRPC server available at {}:{}", self.config.host, self.config.grpc_port);
        // In production, would start actual gRPC server
        Ok(())
    }
    
    /// Stop the server
    pub async fn stop(&self) -> Result<(), ServerError> {
        tracing::info!("Stopping Chronos server");
        Ok(())
    }
    
    /// Execute a query
    pub async fn execute_query(&self, sql: &str) -> Result<crate::query::QueryResult, crate::query::QueryError> {
        let query = self.query_parser.parse(sql)?;
        self.query_executor.execute(&query).await
    }
    
    /// Get server status
    pub async fn status(&self) -> ServerStatus {
        ServerStatus {
            version: crate::VERSION.to_string(),
            uptime_seconds: 0,
            num_tables: self.engine.tables.iter().count(),
            total_rows: 0,
            memory_usage_mb: 0,
        }
    }
}

/// Server status
#[derive(Debug, Clone)]
pub struct ServerStatus {
    pub version: String,
    pub uptime_seconds: u64,
    pub num_tables: usize,
    pub total_rows: usize,
    pub memory_usage_mb: u64,
}

impl Default for ChronosServer {
    fn default() -> Self {
        Self::new(ServerConfig::default(), Arc::new(Engine::default()))
    }
}
