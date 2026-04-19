//! Chronos Server Binary

use chronos::{init_logging, Engine, storage::StorageConfig};
use std::sync::Arc;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    init_logging();
    
    println!(r#"
  _____      _            _             _ 
 | ____|_  _| |_ _ __ ___| |_ ___ _ __ | |
 |  _| \ \/ / __| '__/ _ \ __/ _ \ '_ \| |
 | |___ >  <| |_| | | (_) | ||  __/ | | |_|
 |_____/_/\_\\__|_|  \___/\__\___|_| |_(_)
 
 Time-Series Database with Embedded ML Inference
"#);

    println!("Version: {}", chronos::VERSION);
    println!();

    // Load configuration
    let config = StorageConfig::default();
    
    // Create storage engine
    println!("Initializing storage engine...");
    let engine = Arc::new(Engine::new(config)?);
    
    // Load existing tables
    engine.load_tables().await?;
    
    // Create example table
    let schema = chronos::storage::schema::Schema::parse(
        "time TIMESTAMP, value FLOAT64, sensor_id STRING"
    )?;
    
    let table_config = chronos::storage::engine::TableConfig {
        time_column: "time".to_string(),
        tags: vec!["sensor_id".to_string()],
        fields: vec!["value".to_string()],
        retention_id: None,
        downsampling: vec![],
    };
    
    let table = engine.create_table("metrics", schema, table_config).await?;
    println!("Created table: {}", table.name);

    // Start HTTP API server
    println!("\nStarting HTTP API server on http://0.0.0.0:8080...");
    println!("gRPC server on 0.0.0.0:50051");
    
    // In production, would start actual servers
    println!("\nPress Ctrl+C to stop...\n");

    // Keep running
    tokio::signal::ctrl_c().await?;
    
    println!("\nShutting down...");
    engine.flush().await?;
    
    Ok(())
}
