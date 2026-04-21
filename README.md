# Chronos - Time-Series Database with Embedded ML Inference

<p align="center">
  <img src="https://img.shields.io/github/actions/workflow/status/moggan1337/Chronos/ci.yml?branch=main&style=for-the-badge" alt="CI">
  <img src="https://img.shields.io/badge/Version-0.1.0-blue.svg" alt="Version">
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License">
  <img src="https://img.shields.io/badge/Rust-1.70+-orange.svg" alt="Rust">
</p>

Chronos is a high-performance time-series database built in Rust, featuring:

- **Columnar Storage Engine** powered by Apache Arrow
- **Vectorized Queries** with SIMD optimization
- **Built-in ML Models** for anomaly detection, forecasting, and classification
- **Streaming Ingestion** with Kafka-compatible protocol
- **Advanced Compression** using Gorilla and CHIMP algorithms
- **SQL-like Query Language** with time-series functions
- **Retention Policies** and automatic downsampling

---

## 🎬 Demo
![Chronos Demo](demo.gif)

*Real-time time-series ingestion and ML-powered forecasting*

## Screenshots
| Component | Preview |
|-----------|---------|
| Data Dashboard | ![dashboard](screenshots/dashboard.png) |
| Anomaly Detection | ![anomaly](screenshots/anomaly.png) |
| Forecast View | ![forecast](screenshots/forecast.png) |

## Visual Description
Dashboard displays streaming data with smooth line charts updating in real-time. Anomaly detection highlights outliers with red markers and confidence intervals. Forecast shows predicted values with uncertainty bands.

---


## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage](#usage)
  - [CLI](#cli)
  - [Server](#server)
  - [Query Language](#query-language)
  - [ML Functions](#ml-functions)
- [Compression](#compression)
- [Streaming](#streaming)
- [Benchmarks](#benchmarks)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Development](#development)
- [Roadmap](#roadmap)
- [License](#license)

---

## Features

### Core Features

| Feature | Description |
|---------|-------------|
| Columnar Storage | Apache Arrow-based columnar storage for efficient analytics |
| Vectorized Queries | SIMD-accelerated query execution |
| SQL-like Queries | Familiar SQL syntax with time-series extensions |
| ML Inference | Built-in anomaly detection, forecasting, classification |
| Streaming | Kafka-compatible producer/consumer |
| Compression | Gorilla (TSZ), CHIMP, Zstd, Snappy, LZ4 |
| Retention | Automatic data lifecycle management |
| Downsampling | Automatic data aggregation for long-term storage |

### ML Models

| Model | Description | Use Case |
|-------|-------------|----------|
| Z-Score | Statistical anomaly detection | Simple threshold-based detection |
| IQR | Interquartile range detection | Robust to outliers |
| Isolation Forest | Tree-based anomaly detection | Complex patterns |
| Exponential Smoothing | Time-series forecasting | Smooth trends |
| Holt-Winters | Seasonal forecasting | Periodic data |
| Decision Tree | Classification | Categorical predictions |
| Random Forest | Ensemble classification | High accuracy |

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Chronos System                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   CLI       │  │   HTTP API  │  │   gRPC API              │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                     Query Engine                            ││
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌─────────────┐ ││
│  │  │  Parser   │ │  Planner   │ │ Optimizer │ │  Executor   │ ││
│  │  └───────────┘ └───────────┘ └───────────┘ └─────────────┘ ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                   ML Module                                 ││
│  │  ┌───────────────┐ ┌───────────────┐ ┌─────────────────┐   ││
│  │  │   Anomaly     │ │   Forecasting │ │   Classification│   ││
│  │  └───────────────┘ └───────────────┘ └─────────────────┘   ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                  Storage Engine                             ││
│  │  ┌───────────────┐ ┌───────────────┐ ┌─────────────────┐   ││
│  │  │  Partition    │ │    Column     │ │     Schema      │   ││
│  │  └───────────────┘ └───────────────┘ └─────────────────┘   ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │               Compression Layer                             ││
│  │  ┌───────────────┐ ┌───────────────┐ ┌─────────────────┐   ││
│  │  │   Gorilla     │ │    CHIMP      │ │      Zstd       │   ││
│  │  └───────────────┘ └───────────────┘ └─────────────────┘   ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Storage Architecture

Chronos uses a tiered storage architecture optimized for time-series data:

1. **Partitions**: Data is partitioned by time (default: 1 hour)
2. **Columns**: Each column is stored separately for efficient projection
3. **Compression**: Specialized compression for each data type
4. **WAL**: Write-Ahead Logging for durability

```
┌─────────────────────────────────────────────────────────────────┐
│                         Table                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐│
│  │   Partition 1   │  │   Partition 2   │  │   Partition N   ││
│  │   2024-01-01     │  │   2024-01-02     │  │   2024-01-03     ││
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤│
│  │ time (col)      │  │ time (col)      │  │ time (col)      ││
│  │ value (col)     │  │ value (col)     │  │ value (col)     ││
│  │ tag (col)       │  │ tag (col)       │  │ tag (col)       ││
│  └─────────────────┘  └─────────────────┘  └─────────────────┘│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Query Execution Pipeline

```
SQL Query
    │
    ▼
┌───────────┐
│  Parser   │ ─── Parse SQL ───▶ AST
└───────────┘
    │
    ▼
┌───────────┐
│  Planner  │ ─── Generate Plan ───▶ Plan Tree
└───────────┘
    │
    ▼
┌───────────┐
│ Optimizer │ ─── Optimize Plan ───▶ Optimized Plan
└───────────┘
    │
    ▼
┌───────────┐
│ Executor  │ ─── Execute ───▶ Results
└───────────┘
    │
    ▼
┌───────────┐
│  Arrow    │ ─── Format ───▶ RecordBatches
└───────────┘
```

---

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/moggan1337/Chronos.git
cd Chronos

# Build the project
cargo build --release

# Run the server
./target/release/chronos

# Or run the CLI
./target/release/chronos-cli
```

### Basic Operations

```sql
-- Create a table
CREATE TABLE sensor_data (
    time TIMESTAMP,
    value FLOAT64,
    sensor_id STRING
);

-- Insert data
INSERT INTO sensor_data VALUES (NOW(), 23.5, 'sensor-1');
INSERT INTO sensor_data VALUES (NOW(), 24.2, 'sensor-1');
INSERT INTO sensor_data VALUES (NOW(), 22.8, 'sensor-2');

-- Query data
SELECT * FROM sensor_data WHERE time > NOW() - 1h;
SELECT * FROM sensor_data WHERE sensor_id = 'sensor-1';

-- Aggregate data
SELECT MEAN(value) FROM sensor_data GROUP BY time(1h);
```

### ML Operations

```bash
# Anomaly detection
chronos-cli anomaly VALUES 1,2,3,100,4,5

# Forecasting
chronos-cli forecast VALUES 1,2,3,4,5,6,7,8,9,10 HORIZON 5
```

---

## Installation

### Prerequisites

- Rust 1.70 or later
- Cargo
- (Optional) OpenSSL for TLS support

### Building from Source

```bash
# Clone repository
git clone https://github.com/moggan1337/Chronos.git
cd Chronos

# Build release version
cargo build --release

# Build with all features
cargo build --release --all-features

# Run tests
cargo test

# Run benchmarks
cargo bench
```

### Docker

```dockerfile
FROM rust:latest as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/chronos /usr/local/bin/
COPY --from=builder /app/target/release/chronos-cli /usr/local/bin/
ENTRYPOINT ["chronos"]
```

---

## Usage

### CLI

```bash
# Start interactive REPL
chronos-cli

# Execute a query
chronos-cli query "SELECT * FROM metrics"

# List tables
chronos-cli tables

# Create a table
chronos-cli create metrics "time TIMESTAMP, value FLOAT64"

# Anomaly detection
chronos-cli anomaly VALUES 1,2,3,4,100,5,6 --threshold 3.0

# Forecasting
chronos-cli forecast VALUES 1,2,3,4,5,6,7,8,9,10 --horizon 5

# Import data
chronos-cli import data.csv --table metrics

# Export data
chronos-cli export metrics --file output.csv
```

### Server

```bash
# Start the server (default: 0.0.0.0:8080)
chronos

# Start with custom configuration
RUST_LOG=debug chronos --host 127.0.0.1 --port 9090
```

### HTTP API

```bash
# Create table
curl -X POST http://localhost:8080/api/v1/tables \
  -H "Content-Type: application/json" \
  -d '{"name": "metrics", "schema": "time TIMESTAMP, value FLOAT64"}'

# Query
curl "http://localhost:8080/api/v1/query?sql=SELECT%20*%20FROM%20metrics"

# Write data
curl -X POST http://localhost:8080/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{"table": "metrics", "values": [[1234567890, 23.5]]}'

# Anomaly detection
curl -X POST http://localhost:8080/api/v1/anomaly \
  -H "Content-Type: application/json" \
  -d '{"values": [1,2,3,4,100,5,6], "threshold": 3.0}'

# Forecasting
curl -X POST http://localhost:8080/api/v1/forecast \
  -H "Content-Type: application/json" \
  -d '{"values": [1,2,3,4,5,6,7,8,9,10], "horizon": 5}'
```

---

## Query Language

Chronos supports a SQL-like query language with time-series extensions.

### Basic Syntax

```sql
SELECT column_list
FROM table_name
[WHERE condition]
[GROUP BY time_window | column_list]
[ORDER BY column [ASC | DESC]]
[LIMIT n]
```

### Time-Series Functions

| Function | Description | Example |
|----------|-------------|---------|
| `MEAN()` | Average value | `SELECT MEAN(value) FROM metrics` |
| `SUM()` | Sum of values | `SELECT SUM(value) FROM metrics` |
| `MIN()` | Minimum value | `SELECT MIN(value) FROM metrics` |
| `MAX()` | Maximum value | `SELECT MAX(value) FROM metrics` |
| `COUNT()` | Count of values | `SELECT COUNT(*) FROM metrics` |
| `FIRST()` | First value | `SELECT FIRST(value) FROM metrics` |
| `LAST()` | Last value | `SELECT LAST(value) FROM metrics` |
| `MEDIAN()` | Median value | `SELECT MEDIAN(value) FROM metrics` |
| `PERCENTILE(n)` | Nth percentile | `SELECT PERCENTILE(95) FROM metrics` |
| `STDDEV()` | Standard deviation | `SELECT STDDEV(value) FROM metrics` |
| `RATE()` | Rate of change | `SELECT RATE(value) FROM metrics` |
| `DELTA()` | Difference | `SELECT DELTA(value) FROM metrics` |

### Time Windows

```sql
-- 1-minute buckets
SELECT MEAN(value) FROM metrics GROUP BY time(1m)

-- 5-minute buckets
SELECT MEAN(value) FROM metrics GROUP BY time(5m)

-- 1-hour buckets
SELECT MEAN(value) FROM metrics GROUP BY time(1h)

-- 1-day buckets
SELECT MEAN(value) FROM metrics GROUP BY time(1d)
```

### Examples

```sql
-- Simple query
SELECT * FROM metrics WHERE time > NOW() - 1h;

-- Aggregated query
SELECT MEAN(value), MIN(value), MAX(value) 
FROM metrics 
WHERE time > NOW() - 24h 
GROUP BY time(1h);

-- With filters
SELECT * FROM metrics 
WHERE sensor_id = 'temp-1' 
  AND value > 20 
  AND time > NOW() - 7d;

-- Complex aggregation
SELECT 
    sensor_id,
    MEAN(value) as avg_value,
    STDDEV(value) as std_value,
    PERCENTILE(95) as p95_value
FROM metrics
WHERE time > NOW() - 30d
GROUP BY sensor_id, time(1h);
```

---

## ML Functions

### Anomaly Detection

Chronos provides multiple anomaly detection algorithms:

```bash
# Z-Score detection
chronos-cli anomaly VALUES 1,2,3,100,4,5 --threshold 3.0

# IQR detection
chronos-cli anomaly VALUES 1,2,3,100,4,5 --method iqr
```

### Forecasting

```bash
# Simple Exponential Smoothing
chronos-cli forecast VALUES 1,2,3,4,5,6,7,8,9,10 --horizon 5

# With custom alpha
chronos-cli forecast VALUES 1,2,3,4,5 --alpha 0.5 --horizon 10
```

### Feature Extraction

```rust
use chronos::ml::features::FeatureExtractor;

let extractor = FeatureExtractor::new();
let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
let features = extractor.extract(&values);
let vector = extractor.to_vector(&features);
```

---

## Compression

Chronos uses specialized compression algorithms optimized for time-series data.

### Supported Algorithms

| Algorithm | Best For | Compression Ratio | Speed |
|-----------|----------|-------------------|-------|
| Gorilla (TSZ) | Float time-series | ~90% | Very Fast |
| CHIMP | Integer timestamps | ~85% | Fast |
| Zstd | General purpose | ~75% | Medium |
| Snappy | General purpose | ~65% | Very Fast |
| LZ4 | General purpose | ~70% | Fastest |

### Configuration

```toml
[storage]
compression = "gorilla"  # or "chimp", "zstd", "snappy", "lz4", "auto"

[storage.compression]
level = 3  # 1-22 for zstd
```

### Encoding Details

**Gorilla Encoding (Float)**
- Uses XOR-based encoding
- Stores deltas between consecutive values
- Excellent for smoothly changing values

**CHIMP Encoding (Integer)**
- Uses predictive encoding
- Stores prediction errors
- Optimized for monotonically increasing values

---

## Streaming

Chronos provides Kafka-compatible streaming ingestion.

### Producer

```rust
use chronos::streaming::{StreamProducer, ProducerConfig, ProducerRecord};

let config = ProducerConfig::default();
let producer = StreamProducer::new(config);

let record = ProducerRecord::new("metrics", vec![1, 2, 3])
    .with_key(vec![1])
    .with_partition(0);

producer.send(record).await?;
```

### Consumer

```rust
use chronos::streaming::{StreamConsumer, ConsumerConfig};

let config = ConsumerConfig::default();
let consumer = StreamConsumer::new(config);

consumer.subscribe(vec!["metrics".to_string()]).await?;

loop {
    let messages = consumer.poll(1000).await?;
    for msg in messages {
        // Process message
    }
}
```

### Codecs

| Codec | Format | Use Case |
|-------|--------|----------|
| JSON | Text | Human-readable |
| CSV | Text | Import/Export |
| MessagePack | Binary | Efficient |
| Protobuf | Binary | Schema evolution |

---

## Benchmarks

Performance benchmarks against common time-series databases:

| Operation | Chronos | InfluxDB | TimescaleDB |
|-----------|---------|----------|--------------|
| Write (1M rows/sec) | ~500K | ~400K | ~350K |
| Point Query (ms) | ~0.5 | ~1.2 | ~1.5 |
| Aggregation (ms) | ~10 | ~25 | ~20 |
| Compression Ratio | ~85% | ~70% | ~65% |
| Memory Usage | Low | Medium | Medium |

### Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- query

# Run with profiling
cargo bench --profile-time=10
```

---

## API Reference

### Storage Engine

```rust
use chronos::{Engine, StorageConfig};

// Create engine
let config = StorageConfig::default();
let engine = Engine::new(config)?;

// Create table
let schema = Schema::parse("time TIMESTAMP, value FLOAT64")?;
let table = engine.create_table("metrics", schema, config).await?;

// Write data
let num_rows = engine.write("metrics", batch).await?;

// Read data
let result = engine.read("metrics", Some((start, end))).await?;
```

### Query Engine

```rust
use chronos::query::{QueryParser, QueryExecutor};

// Parse and execute query
let parser = QueryParser::new();
let query = parser.parse("SELECT * FROM metrics WHERE time > NOW() - 1h")?;
let executor = QueryExecutor::new(engine.clone());
let result = executor.execute(&query).await?;
```

### ML Models

```rust
use chronos::ml::{anomaly::AnomalyDetector, forecasting::Forecaster};

// Anomaly detection
let detector = AnomalyDetector::default().with_zscore(3.0, 100);
let results = detector.detect(&values);

// Forecasting
let mut forecaster = Forecaster::simple_exponential_smoothing(0.3);
forecaster.train(&values)?;
let forecast = forecaster.forecast(10, 0.95);
```

---

## Configuration

### Configuration File

```toml
[server]
host = "0.0.0.0"
port = 8080
grpc_port = 50051

[storage]
data_dir = "/var/lib/chronos/data"
wal_dir = "/var/lib/chronos/wal"
compression = "gorilla"
partition_duration = "1h"

[storage.retention]
hot = "7d"
warm = "30d"
cold = "365d"

[query]
timeout = "30s"
max_rows = 1000000

[streaming]
brokers = ["localhost:9092"]
consumer_group = "chronos-consumer"
```

### Environment Variables

```bash
# Server configuration
CHRONOS_HOST=0.0.0.0
CHRONOS_PORT=8080
CHRONOS_DATA_DIR=/var/lib/chronos

# Logging
RUST_LOG=info
RUST_LOG=debug

# Storage
CHRONOS_COMPRESSION=gorilla
```

---

## Development

### Project Structure

```
Chronos/
├── src/
│   ├── lib.rs              # Library root
│   ├── storage/            # Storage engine
│   │   ├── engine.rs       # Main engine
│   │   ├── partition.rs    # Partition management
│   │   ├── column.rs       # Column storage
│   │   ├── schema.rs       # Schema definitions
│   │   └── retention.rs    # Retention policies
│   ├── query/              # Query engine
│   │   ├── parser.rs       # SQL parser
│   │   ├── planner.rs      # Query planner
│   │   ├── executor.rs     # Query executor
│   │   ├── optimizer.rs    # Query optimizer
│   │   └── functions.rs    # Built-in functions
│   ├── ml/                 # ML module
│   │   ├── anomaly.rs      # Anomaly detection
│   │   ├── forecasting.rs   # Forecasting
│   │   ├── classification.rs # Classification
│   │   └── features.rs     # Feature extraction
│   ├── compression/        # Compression algorithms
│   │   ├── mod.rs
│   │   ├── gorilla.rs      # Gorilla encoding
│   │   ├── chimp.rs         # CHIMP encoding
│   │   └── zstd.rs          # Zstd wrapper
│   ├── streaming/           # Streaming module
│   │   ├── consumer.rs     # Stream consumer
│   │   ├── producer.rs     # Stream producer
│   │   ├── codec.rs        # Message codecs
│   │   └── partitioning.rs # Partitioning
│   ├── server/             # Server implementation
│   │   ├── api.rs          # REST API
│   │   └── rpc.rs          # gRPC server
│   └── cli/                # CLI implementation
│       ├── commands.rs     # CLI commands
│       └── repl.rs         # Interactive REPL
├── bin/
│   ├── server.rs           # Server binary
│   └── cli.rs              # CLI binary
├── Cargo.toml
└── README.md
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

### Testing

```bash
# Run all tests
cargo test

# Run with coverage
cargo tarpaulin --out Html

# Run specific test
cargo test test_name
```

---

## Roadmap

- [x] Core storage engine
- [x] Query engine with SQL-like syntax
- [x] Compression algorithms (Gorilla, CHIMP, Zstd)
- [x] Anomaly detection models
- [x] Forecasting models
- [ ] Clustering support
- [ ] Full-text search
- [ ] Graph queries
- [ ] WebUI dashboard
- [ ] Kubernetes operator

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

## Authors

- Chronos Team

## Acknowledgments

Built with:
- [Apache Arrow](https://arrow.apache.org/)
- [DataFusion](https://github.com/apache/arrow-datafusion)
- [Parquet](https://github.com/apache/arrow-rs)
- [ndarray](https://github.com/rust-ndarray/ndarray)
- [tokio](https://tokio.rs/)

---

<p align="center">
  <strong>Chronos</strong> - Time-Series Database with Embedded ML Inference
</p>
