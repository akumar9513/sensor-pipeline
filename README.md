# Real-Time Sensor Data Pipeline

A production-grade Python pipeline that monitors a directory for incoming IoT sensor CSV files, validates and transforms the data, then stores it in MySQL — all in real time with fault tolerance and automatic retries.

---

## Architecture Overview

```
data/ folder
    │
    ▼  (watchdog FileSystemObserver – fires on every new .csv)
pipeline.py  ──►  processor.py
                      │
                      ├─► validator.py      (null checks, type checks, range checks)
                      │       │
                      │       ├── valid rows ──► transformer.py  (normalize, parse timestamps)
                      │       │                       │
                      │       │                       ├──► db.insert_readings()     → sensor_readings table
                      │       │                       └──► db.insert_aggregates()   → aggregated_metrics table
                      │       │
                      │       └── invalid rows ──► quarantine/   +  error_log table
                      │
                      └─► processed/   (file archived after success)
```

### Key components

| File | Responsibility |
|---|---|
| `pipeline.py` | Entry point; starts the folder watcher; calls `init_db()` on startup |
| `processor.py` | Orchestrates one file end-to-end; handles all errors gracefully |
| `validator.py` | Row-level validation (nulls, types, sensor value ranges) |
| `transformer.py` | Timestamp parsing, min-max normalization, metadata tagging, aggregate computation |
| `db.py` | MySQL connection, schema creation (`CREATE TABLE IF NOT EXISTS`), bulk inserts with retry |
| `generate_sample_data.py` | Creates synthetic test data (including intentionally bad rows) |

---

## Database Schema

### `sensor_readings`  — raw data
| Column | Type | Description |
|---|---|---|
| id | BIGINT PK | Auto-increment |
| sensor_id | VARCHAR(100) | Device identifier |
| timestamp | DATETIME | Reading time (UTC, tz-stripped for MySQL) |
| location | VARCHAR(200) | Physical location |
| temperature | FLOAT | Normalized 0–1 |
| humidity | FLOAT | Normalized 0–1 |
| pressure | FLOAT | Normalized 0–1 |
| source_file | VARCHAR(255) | Origin filename |
| data_source | VARCHAR(100) | Inferred source (Kaggle / UCI / AWS / unknown) |
| ingested_at | DATETIME | Pipeline insert time |

### `aggregated_metrics`  — per-file stats
| Column | Type | Description |
|---|---|---|
| id | BIGINT PK | Auto-increment |
| source_file | VARCHAR(255) | Origin filename |
| data_source | VARCHAR(100) | Inferred source |
| sensor_type | VARCHAR(50) | temperature / humidity / pressure |
| sensor_id | VARCHAR(100) | NULL = all sensors combined |
| min_value | FLOAT | Minimum reading |
| max_value | FLOAT | Maximum reading |
| avg_value | FLOAT | Mean reading |
| std_value | FLOAT | Standard deviation |
| record_count | INT | Number of rows in the group |
| aggregated_at | DATETIME | When aggregate was computed |

### `error_log`  — validation & pipeline errors
| Column | Type | Description |
|---|---|---|
| id | BIGINT PK | Auto-increment |
| source_file | VARCHAR(255) | File that caused the error |
| error_type | VARCHAR(100) | SCHEMA_ERROR / VALIDATION_ERROR / DB_ERROR … |
| error_message | TEXT | Human-readable reason |
| row_number | INT | CSV row (NULL for file-level errors) |
| logged_at | DATETIME | When the error was logged |

---

## Setup & Running

### 1. Prerequisites
- Python 3.10+
- MySQL 8.x running locally

### 2. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/sensor-pipeline.git
cd sensor-pipeline
```

### 3. Create a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 4. Install dependencies
```bash
pip install -r requirements.txt
```

### 5. Configure credentials
```bash
cp .env.example .env
# Open .env and set DB_PASSWORD to your MySQL root password
```

### 6. Generate sample data (optional)
```bash
python generate_sample_data.py
```
This creates three CSV files in `data/` — 80 clean rows + 5 deliberately bad rows each — so you can test quarantine and validation immediately.

### 7. Run the pipeline
```bash
python pipeline.py
```

The pipeline will:
1. Create the `sensor_pipeline` database and all tables automatically.
2. Process any CSV files already sitting in `data/`.
3. Watch `data/` continuously — drop a new CSV file there and it processes within ~1 second.

To stop: press **Ctrl + C**.

---

## Fault Tolerance

| Failure scenario | Behaviour |
|---|---|
| Unreadable / empty CSV | Moved to `quarantine/`, error logged |
| Missing required columns | Whole file quarantined, error logged |
| Individual bad rows | Saved to `quarantine/<file>_invalid.csv`, good rows continue |
| DB temporarily unavailable | Automatic retry × 3 with exponential back-off (2 s, 4 s) |
| Pipeline crash | Unprocessed files remain in `data/` and are re-picked on next startup |
| Duplicate filename in archive | Auto-renamed with counter (`file_1.csv`, `file_2.csv` …) |

---

## Scaling to Production

For millions of files per day, replace or augment each layer:

### Ingestion layer
- **Apache Kafka** – producers write sensor events to topics; the pipeline becomes a consumer group, enabling horizontal scaling by adding consumer instances.
- **AWS Kinesis / Google Pub-Sub** – fully managed alternatives; zero infrastructure to operate.

### Processing layer
- **Apache Spark Structured Streaming** – replace pandas with Spark DataFrames for distributed, in-memory processing across a cluster.
- **AWS Lambda** – trigger a Lambda function on each S3 `PUT` event; auto-scales to millions of concurrent invocations.

### Storage layer
- **Amazon RDS Aurora (MySQL-compatible)** – managed, auto-scaling relational database with read replicas.
- **Apache Parquet on S3 + AWS Athena** – store aggregates as columnar Parquet files; query with serverless SQL.
- **ClickHouse** – column-oriented OLAP database; ideal for time-series analytics at extreme scale.

### Orchestration
- **Apache Airflow** – schedule and monitor pipeline DAGs with retries, alerting, and a web UI.
- **Kubernetes** – containerise the pipeline (`Dockerfile`) and run replicas managed by a Deployment.

### Optimisations
- Batch micro-commits (accumulate 1 000 rows, then INSERT in a single statement).
- Partition `sensor_readings` by month (`PARTITION BY RANGE` on `timestamp`).
- Add a composite index on `(sensor_id, timestamp)` for range queries.
- Use connection pooling (e.g., `sqlalchemy` with `pool_size=20`) to avoid reconnect overhead.

---

## Data Ethics & Compliance
- Only publicly available datasets (Kaggle / UCI / AWS Open Data) are used.
- No PII is collected or stored.
- Credentials are stored in `.env` (excluded from version control via `.gitignore`).
