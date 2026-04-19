//! Chronos CLI Binary

use chronos::{init_logging, Engine};
use std::sync::Arc;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "chronos-cli")]
#[command(about = "Chronos CLI - Time-Series Database with Embedded ML", long_about = None)]
struct Cli {
    #[arg(short, long, default_value = "localhost")]
    host: String,
    
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
    
    #[arg(short, long)]
    user: Option<String>,
    
    #[arg(short, long)]
    password: Option<String>,
    
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute SQL query
    Query {
        #[arg(required = true)]
        sql: String,
    },
    /// List all tables
    Tables,
    /// Create a new table
    Create {
        #[arg(required = true)]
        name: String,
        schema: String,
    },
    /// Insert data
    Insert {
        #[arg(required = true)]
        table: String,
        values: String,
    },
    /// Show statistics
    Stats {
        table: Option<String>,
    },
    /// Run anomaly detection
    Anomaly {
        values: Vec<f64>,
        #[arg(long, default_value_t = 3.0)]
        threshold: f64,
    },
    /// Generate forecast
    Forecast {
        values: Vec<f64>,
        #[arg(long, default_value_t = 10)]
        horizon: usize,
    },
    /// Import data from file
    Import {
        file: String,
        #[arg(long)]
        table: Option<String>,
    },
    /// Export data to file
    Export {
        table: String,
        #[arg(long)]
        file: Option<String>,
    },
    /// Start interactive REPL
    Repl,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    init_logging();
    
    let cli = Cli::parse();
    
    // Create engine (in-memory for CLI)
    let config = chronos::storage::StorageConfig {
        data_dir: std::path::PathBuf::from("/tmp/chronos"),
        ..Default::default()
    };
    
    let engine = Arc::new(Engine::new(config)?);
    
    // Load existing tables
    engine.load_tables().await.ok();
    
    // Execute command or start REPL
    match cli.command {
        Some(Commands::Query { sql }) => {
            println!("Executing: {}", sql);
            // In production, would execute query
        }
        Some(Commands::Tables) => {
            println!("Tables:");
            for table in engine.tables.iter() {
                println!("  {}", table.name);
            }
        }
        Some(Commands::Create { name, schema }) => {
            println!("Creating table: {} with schema: {}", name, schema);
            let parsed_schema = chronos::storage::schema::Schema::parse(&schema)?;
            let table_config = chronos::storage::engine::TableConfig {
                time_column: "time".to_string(),
                tags: vec![],
                fields: vec![],
                retention_id: None,
                downsampling: vec![],
            };
            let table = engine.create_table(&name, parsed_schema, table_config).await?;
            println!("Created table: {}", table.name);
        }
        Some(Commands::Insert { table, values }) => {
            println!("Inserting into {}: {}", table, values);
        }
        Some(Commands::Stats { table }) => {
            if let Some(name) = table {
                match engine.get_stats(&name) {
                    Ok(stats) => {
                        println!("Statistics for {}:", name);
                        println!("  Rows: {}", stats.total_rows);
                        println!("  Partitions: {}", stats.num_partitions);
                        println!("  Size: {} bytes", stats.total_size_bytes);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                }
            } else {
                println!("Total tables: {}", engine.tables.len());
            }
        }
        Some(Commands::Anomaly { values, threshold }) => {
            println!("Anomaly detection on {} values (threshold={})", values.len(), threshold);
            
            let mut detector = chronos::ml::anomaly::AnomalyDetector::default()
                .with_zscore(threshold, 100);
            
            let results = detector.detect_ensemble(&values);
            
            println!("\nDetected {} anomalies:", results.anomalies.len());
            for anomaly in results.anomalies.iter().take(10) {
                println!("  Index {}: value={}, score={:.3}", 
                    anomaly.index, anomaly.value, anomaly.score);
            }
        }
        Some(Commands::Forecast { values, horizon }) => {
            println!("Forecasting {} values (horizon={})", values.len(), horizon);
            
            let mut forecaster = chronos::ml::forecasting::Forecaster::simple_exponential_smoothing(0.3);
            
            if let Err(e) = forecaster.train(&values) {
                eprintln!("Training error: {}", e);
                return Ok(());
            }
            
            let result = forecaster.forecast(horizon, 0.95);
            
            println!("\nForecasts:");
            for (i, pred) in result.predictions.iter().enumerate() {
                println!("  t+{}: {:.4} (range: {:.4} - {:.4})", 
                    i + 1, pred, result.lower_bounds[i], result.upper_bounds[i]);
            }
        }
        Some(Commands::Import { file, table }) => {
            println!("Importing from: {} to table: {:?}", file, table);
        }
        Some(Commands::Export { table, file }) => {
            println!("Exporting {} to file: {:?}", table, file);
        }
        Some(Commands::Repl) | None => {
            // Start interactive REPL
            let mut repl = chronos::cli::repl::Repl::new(engine.clone());
            repl.run();
        }
    }
    
    Ok(())
}
