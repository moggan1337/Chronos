//! Chronos CLI
//! 
//! Command-line interface for interacting with Chronos.

pub mod commands;
pub mod repl;

pub use commands::CliCommand;
pub use repl::Repl;

use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Command error: {0}")]
    Command(String),
    #[error("Connection error: {0}")]
    Connection(String),
}

/// CLI configuration
#[derive(Debug, Clone)]
pub struct CliConfig {
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub config_file: Option<PathBuf>,
    pub output_format: OutputFormat,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 8080,
            user: None,
            password: None,
            database: None,
            config_file: None,
            output_format: OutputFormat::Table,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Table,
    Csv,
    Json,
    Tsv,
}

impl CliConfig {
    pub fn from_args(matches: &clap::ArgMatches) -> Self {
        Self {
            host: matches.get_one::<String>("host").cloned().unwrap_or_else(|| "localhost".to_string()),
            port: *matches.get_one::<u16>("port").unwrap_or(&8080),
            user: matches.get_one::<String>("user").cloned(),
            password: matches.get_one::<String>("password").cloned(),
            database: matches.get_one::<String>("database").cloned(),
            config_file: matches.get_one::<String>("config").map(PathBuf::from),
            output_format: OutputFormat::Table,
        }
    }
}

/// CLI context
pub struct CliContext {
    pub config: CliConfig,
    pub history: Vec<String>,
}

impl CliContext {
    pub fn new(config: CliConfig) -> Self {
        Self {
            config,
            history: Vec::new(),
        }
    }
    
    pub fn add_history(&mut self, command: String) {
        self.history.push(command);
    }
}

/// Print formatted output
pub fn print_output<T: std::fmt::Display>(
    data: &[T],
    format: OutputFormat,
    headers: Option<&[String]>,
) {
    match format {
        OutputFormat::Table => print_table(data, headers),
        OutputFormat::Csv => print_csv(data),
        OutputFormat::Json => print_json(data),
        OutputFormat::Tsv => print_tsv(data),
    }
}

fn print_table<T: std::fmt::Display>(data: &[T], headers: Option<&[String]>) {
    // Simple table formatting
    if let Some(h) = headers {
        println!("{}", h.join(" | "));
        println!("{}", h.iter().map(|s| "-".repeat(s.len())).collect::<Vec<_>>().join("-+-"));
    }
    
    for row in data {
        println!("{}", row);
    }
}

fn print_csv<T: std::fmt::Display>(data: &[T]) {
    for row in data {
        println!("{}", row);
    }
}

fn print_json<T: serde::Serialize>(data: &[T]) {
    println!("{}", serde_json::to_string_pretty(data).unwrap_or_else(|_| "[]".to_string()));
}

fn print_tsv<T: std::fmt::Display>(data: &[T]) {
    for row in data {
        println!("{}", row);
    }
}

/// Interactive REPL
pub struct Repl {
    context: CliContext,
    running: bool,
}

impl Repl {
    pub fn new(config: CliConfig) -> Self {
        Self {
            context: CliContext::new(config),
            running: false,
        }
    }
    
    pub fn run(&mut self) {
        self.running = true;
        println!("Chronos CLI v{}\nType 'help' for commands, 'exit' to quit.\n", crate::VERSION);
        
        loop {
            print!("chronos> ");
            std::io::Write::flush(&mut std::io::stdout()).ok();
            
            let mut input = String::new();
            if std::io::stdin().read_line(&mut input).is_err() {
                break;
            }
            
            let input = input.trim();
            if input.is_empty() {
                continue;
            }
            
            self.context.add_history(input.to_string());
            
            if input == "exit" || input == "quit" {
                break;
            }
            
            self.execute_command(input);
        }
        
        self.running = false;
    }
    
    fn execute_command(&self, input: &str) {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            return;
        }
        
        match parts[0].to_lowercase().as_str() {
            "help" => self.print_help(),
            "tables" | "list" => self.list_tables(),
            "query" | "sql" => {
                if parts.len() > 1 {
                    let sql = input[parts[0].len()..].trim();
                    self.run_query(sql);
                } else {
                    println!("Usage: query <sql>");
                }
            }
            "create" => self.handle_create(input),
            "insert" => self.handle_insert(input),
            "anomaly" => self.handle_anomaly(input),
            "forecast" => self.handle_forecast(input),
            "clear" => println!("\x1B[2J\x1B[H"),
            "history" => self.print_history(),
            "stats" => self.print_stats(),
            _ => println!("Unknown command: {}. Type 'help' for available commands.", parts[0]),
        }
    }
    
    fn print_help(&self) {
        println!(r#"
Chronos CLI Commands:
  help              Show this help message
  exit, quit        Exit the CLI
  tables            List all tables
  query <sql>       Execute SQL query
  create <table>    Create a new table
  insert <data>     Insert data into a table
  anomaly <values>  Run anomaly detection
  forecast <data>   Generate forecast
  stats             Show database statistics
  clear             Clear the screen
  history           Show command history
"#);
    }
    
    fn list_tables(&self) {
        println!("Tables:");
        println!("  (none)");
    }
    
    fn run_query(&self, _sql: &str) {
        println!("Query execution not implemented in demo mode");
    }
    
    fn handle_create(&self, input: &str) {
        let args = input.split_whitespace().collect::<Vec<_>>();
        if args.len() > 1 {
            println!("Creating table: {}", args[1]);
        } else {
            println!("Usage: create <table_name>");
        }
    }
    
    fn handle_insert(&self, input: &str) {
        let args = input.split_whitespace().collect::<Vec<_>>();
        if args.len() > 1 {
            println!("Inserting into: {}", args[1]);
        } else {
            println!("Usage: insert <table_name>");
        }
    }
    
    fn handle_anomaly(&self, input: &str) {
        println!("Anomaly detection: {}", input);
    }
    
    fn handle_forecast(&self, input: &str) {
        println!("Forecasting: {}", input);
    }
    
    fn print_history(&self) {
        for (i, cmd) in self.context.history.iter().enumerate() {
            println!("{}: {}", i + 1, cmd);
        }
    }
    
    fn print_stats(&self) {
        println!("Chronos Database Statistics:");
        println!("  Version: {}", crate::VERSION);
        println!("  Tables: 0");
        println!("  Total rows: 0");
    }
}
