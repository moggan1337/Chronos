//! Interactive REPL for Chronos

use std::sync::Arc;
use std::io::{self, Write};
use crate::storage::Engine;
use crate::query::{QueryParser, QueryExecutor};
use crate::ml::{ModelRegistry, anomaly::AnomalyDetector};
use crate::cli::commands::CliContext;

/// Interactive REPL
pub struct Repl {
    context: CliContext,
    running: bool,
    history: Vec<String>,
}

impl Repl {
    pub fn new(engine: Arc<Engine>) -> Self {
        let query_parser = QueryParser::new();
        let query_executor = QueryExecutor::new(engine.clone());
        
        Self {
            context: CliContext {
                engine,
                query_parser,
                query_executor,
            },
            running: false,
            history: Vec::new(),
        }
    }
    
    /// Run the REPL
    pub fn run(&mut self) {
        self.running = true;
        self.print_banner();
        
        loop {
            print!("chronos> ");
            io::stdout().flush().unwrap();
            
            let mut input = String::new();
            if io::stdin().read_line(&mut input).is_err() {
                break;
            }
            
            let input = input.trim();
            if input.is_empty() {
                continue;
            }
            
            self.history.push(input.to_string());
            
            if !self.process_input(input) {
                break;
            }
        }
        
        println!("Goodbye!");
        self.running = false;
    }
    
    fn print_banner(&self) {
        println!(r#"
  _____      _            _             _ 
 | ____|_  _| |_ _ __ ___| |_ ___ _ __ | |
 |  _| \ \/ / __| '__/ _ \ __/ _ \ '_ \| |
 | |___ >  <| |_| | | (_) | ||  __/ | | |_|
 |_____/_/\_\\__|_|  \___/\__\___|_| |_(_)
 
 Time-Series Database with Embedded ML Inference
 Version: {}
"#, crate::VERSION);
    }
    
    fn process_input(&mut self, input: &str) -> bool {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            return true;
        }
        
        match parts[0].to_lowercase().as_str() {
            "help" | "?" => self.cmd_help(),
            "exit" | "quit" | "q" => return false,
            "tables" | "list" | "ls" => self.cmd_tables(),
            "query" | "select" | "sql" => self.cmd_query(&input[parts[0].len()..]),
            "create" => self.cmd_create(&parts[1..]),
            "insert" | "write" => self.cmd_insert(&parts[1..]),
            "delete" | "drop" => self.cmd_delete(&parts[1..]),
            "stats" | "stat" => self.cmd_stats(&parts[1..]),
            "anomaly" | "detect" => self.cmd_anomaly(&parts[1..]),
            "forecast" | "predict" => self.cmd_forecast(&parts[1..]),
            "import" => self.cmd_import(&parts[1..]),
            "export" => self.cmd_export(&parts[1..]),
            "clear" | "cls" => print!("\x1B[2J\x1B[H"),
            "history" | "hist" => self.cmd_history(),
            "ping" => println!("PONG"),
            _ => {
                // Try as SQL query
                self.cmd_query(input);
            }
        }
        
        true
    }
    
    fn cmd_help(&self) {
        println!(r#"
Available commands:
  help, ?          Show this help message
  exit, quit, q    Exit the CLI
  tables, ls       List all tables
  query, sql       Execute SQL query
  create           Create a new table
  insert, write    Insert data into a table
  delete, drop     Delete data from a table
  stats            Show table/database statistics
  anomaly, detect  Run anomaly detection
  forecast, predict Generate forecast
  import           Import data from file
  export           Export data to file
  clear, cls       Clear the screen
  history, hist    Show command history
  
Examples:
  CREATE TABLE sensor_data (time TIMESTAMP, value FLOAT64, sensor_id STRING)
  INSERT INTO sensor_data VALUES (NOW(), 23.5, 'sensor-1')
  SELECT * FROM sensor_data WHERE time > NOW() - 1h
  ANOMALY VALUES 1,2,3,100,4,5
  FORECAST VALUES 1,2,3,4,5 HORIZON 10
"#);
    }
    
    fn cmd_tables(&self) {
        println!("┌────────────────────────────────────┐");
        println!("│ Tables                             │");
        println!("├────────────────────────────────────┤");
        
        let tables: Vec<String> = self.context.engine.tables.iter()
            .map(|t| t.name.clone())
            .collect();
        
        if tables.is_empty() {
            println!("│ No tables found                    │");
        } else {
            for table in tables {
                println!("│ {:<34} │", table);
            }
        }
        
        println!("└────────────────────────────────────┘");
    }
    
    fn cmd_query(&self, sql: &str) {
        let sql = sql.trim();
        if sql.is_empty() {
            println!("Usage: query <sql>");
            return;
        }
        
        println!("Executing: {}", sql);
        
        match self.context.query_parser.parse(sql) {
            Ok(query) => {
                println!("Query parsed successfully:");
                println!("  Table: {}", query.table);
                println!("  Columns: {:?}", query.columns);
                if let Some(range) = &query.time_range {
                    println!("  Time range: {} - {}", range.start, range.end);
                }
            }
            Err(e) => {
                println!("Parse error: {}", e);
            }
        }
    }
    
    fn cmd_create(&self, args: &[&str]) {
        if args.is_empty() {
            println!("Usage: CREATE TABLE <name> (schema)");
            return;
        }
        
        let table_def = args.join(" ");
        println!("Creating table: {}", table_def);
        println!("(Table creation not implemented in demo mode)");
    }
    
    fn cmd_insert(&self, args: &[&str]) {
        if args.is_empty() {
            println!("Usage: INSERT INTO <table> VALUES (...)");
            return;
        }
        
        let data = args.join(" ");
        println!("Inserting: {}", data);
        println!("(Insert not implemented in demo mode)");
    }
    
    fn cmd_delete(&self, args: &[&str]) {
        if args.is_empty() {
            println!("Usage: DELETE FROM <table> [WHERE ...]");
            return;
        }
        
        println!("(Delete not implemented in demo mode)");
    }
    
    fn cmd_stats(&self, args: &[&str]) {
        if args.is_empty() {
            println!("Database Statistics:");
            println!("  Tables: {}", self.context.engine.tables.len());
            println!("  Version: {}", crate::VERSION);
        } else {
            let table_name = args[0];
            match self.context.engine.get_stats(table_name) {
                Ok(stats) => {
                    println!("Statistics for '{}':", table_name);
                    println!("  Rows: {}", stats.total_rows);
                    println!("  Partitions: {}", stats.num_partitions);
                    println!("  Size: {:.2} KB", stats.total_size_bytes as f64 / 1024.0);
                    println!("  Compression: {:.2}x", stats.compression_ratio);
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        }
    }
    
    fn cmd_anomaly(&self, args: &[&str]) {
        println!("Anomaly Detection:");
        
        // Parse values
        let values: Vec<f64> = args.iter()
            .filter_map(|s| s.trim_end_matches(',').parse().ok())
            .collect();
        
        if values.is_empty() {
            println!("Usage: ANOMALY VALUES 1,2,3,100,4,5");
            return;
        }
        
        let mut detector = AnomalyDetector::default()
            .with_zscore(3.0, 100);
        
        let results = detector.detect(&values);
        let ensemble = detector.detect_ensemble(&values);
        
        println!("\nResults:");
        for result in &results {
            println!("  {} detector: {} anomalies", result.model_name, result.anomalies.len());
        }
        
        println!("  Ensemble: {} anomalies", ensemble.anomalies.len());
        
        if !ensemble.anomalies.is_empty() {
            println!("\nDetected anomalies at indices:");
            for anomaly in &ensemble.anomalies {
                println!("  Index {}: value={}, score={:.3}", 
                    anomaly.index, anomaly.value, anomaly.score);
            }
        }
    }
    
    fn cmd_forecast(&self, args: &[&str]) {
        println!("Forecasting:");
        
        // Parse values
        let values: Vec<f64> = args.iter()
            .filter_map(|s| s.trim_end_matches(',').parse().ok())
            .take_while(|s| *s != 0.0 || s.is_nan() == false)
            .collect();
        
        if values.len() < 5 {
            println!("Usage: FORECAST VALUES 1,2,3,4,5,6,7,8,9,10 [HORIZON 5]");
            return;
        }
        
        let horizon = args.iter()
            .skip_while(|s| s.to_lowercase() != "horizon")
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        
        let mut forecaster = crate::ml::forecasting::Forecaster::simple_exponential_smoothing(0.3);
        if let Err(e) = forecaster.train(&values) {
            println!("Training error: {}", e);
            return;
        }
        
        let result = forecaster.forecast(horizon, 0.95);
        
        println!("\nForecasts (horizon={}):", horizon);
        for (i, pred) in result.predictions.iter().enumerate() {
            println!("  t+{}: {:.4} (range: {:.4} - {:.4})", 
                i + 1, pred, result.lower_bounds[i], result.upper_bounds[i]);
        }
    }
    
    fn cmd_import(&self, args: &[&str]) {
        if args.is_empty() {
            println!("Usage: IMPORT <file> [INTO <table>]");
            return;
        }
        
        let file = args[0];
        println!("Importing from: {}", file);
        println!("(Import not implemented in demo mode)");
    }
    
    fn cmd_export(&self, args: &[&str]) {
        if args.is_empty() {
            println!("Usage: EXPORT <table> [TO <file>]");
            return;
        }
        
        println!("(Export not implemented in demo mode)");
    }
    
    fn cmd_history(&self) {
        println!("Command history:");
        for (i, cmd) in self.history.iter().enumerate() {
            println!("  {}: {}", i + 1, cmd);
        }
    }
}
