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

### Key Components

| File | Responsibility |
|---|---|
| `pipeline.py` | Entry point; starts the folder watcher; calls `init_db()` on startup |
| `processor.py` | Orchestrates one file end-to-end; handles all errors gracefully |
| `validator.py` | Row-level validation (nulls, types, sensor value ranges) |
| `transformer.py` | Timestamp parsing to MySQL-compatible format, min-max normalization, metadata tagging, aggregate computation |
| `db.py` | MySQL connection, schema creation, bulk inserts with retry |
| `generate_sample_data.py` | Creates synthetic test data (including intentionally bad rows) |
| `prepare_kaggle_data.py` | Splits the Kaggle CSV into 1000-row chunks for pipeline testing |

---

## Database Schema

### `sensor_readings` — raw data
| Column | Type | Description |
|---|---|---|
| id | BIGINT PK | Auto-increment |
| sensor_id | VARCHAR(100) | Device identifier |
| timestamp | DATETIME | Reading time in YYYY-MM-DD HH:MM:SS format |
| location | VARCHAR(200) | Physical location |
| temperature | FLOAT | Min-max normalized to 0-1 |
| humidity | FLOAT | Min-max normalized to 0-1 |
| pressure | FLOAT | Min-max normalized to 0-1 (if available) |
| source_file | VARCHAR(255) | Origin filename |
| data_source | VARCHAR(100) | Inferred source (Kaggle / UCI / AWS / unknown) |
| ingested_at | DATETIME | Pipeline insert time |

### `aggregated_metrics` — per-file stats
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

### `error_log` — validation & pipeline errors
| Column | Type | Description |
|---|---|---|
| id | BIGINT PK | Auto-increment |
| source_file | VARCHAR(255) | File that caused the error |
| error_type | VARCHAR(100) | SCHEMA_ERROR / VALIDATION_ERROR / DB_ERROR |
| error_message | TEXT | Human-readable reason |
| row_num | INT | CSV row number (NULL for file-level errors) |
| logged_at | DATETIME | When the error was logged |

---

## Validation Rules

| Check | Rule |
|---|---|
| Required fields | `sensor_id` and `timestamp` must not be null |
| Timestamp format | Must be parseable as a valid datetime |
| Numeric types | `temperature`, `humidity`, `pressure` must be numeric |
| Temperature range | Must be between -50 and 50 |
| Humidity range | Must be between 0 and 100 |
| Pressure range | Must be between 800 and 1100 hPa |

Rows that fail any check are saved to `quarantine/<filename>_invalid.csv` and logged in the `error_log` table with the reason for failure.

---

## Data Transformation

After validation, each row is transformed as follows:

- **Timestamps** — parsed and converted to `YYYY-MM-DD HH:MM:SS` format compatible with MySQL DATETIME
- **Sensor values** — cast to float and min-max normalized to a 0-1 range per file
- **Metadata** — `source_file` and `data_source` (inferred from filename: Kaggle / UCI / AWS) added to each row

---

## Setup & Running

### 1. Prerequisites
- Python 3.10+
- MySQL 8.x or later running locally

### 2. Clone the repo
```bash
git clone https://github.com/akumar9513/sensor-pipeline.git
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
Create a `.env` file in the project root:
```
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=sensor_pipeline
```

### 6. Download the real dataset (recommended)
Download the IoT sensor dataset from Kaggle:
- Go to: https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k
- Download and extract the zip file
- Copy `iot_telemetry_data.csv` into the project root folder
- Run the splitter script to prepare the data:
```bash
python3 prepare_kaggle_data.py
```

### 7. Or use the sample data already in data/ (quick test)
The `data/` folder already contains 3 sample CSV files from the Kaggle dataset:
- `kaggle_iot_batch_0001.csv` — 1000 real IoT sensor rows
- `kaggle_iot_batch_0002.csv` — 1000 real IoT sensor rows
- `kaggle_iot_batch_0003.csv` — 1000 real IoT sensor rows

### 8. Run the pipeline
```bash
python pipeline.py
```

The pipeline will:
1. Automatically create the `sensor_pipeline` database and all tables.
2. Process any CSV files already in `data/`.
3. Watch `data/` continuously — drop a new CSV file and it processes within ~1 second.

To stop: press **Ctrl + C**.

---

## Folder Structure

```
sensor_pipeline/
├── pipeline.py                    # Entry point / folder watcher
├── processor.py                   # File lifecycle orchestrator
├── validator.py                   # Row-level validation rules
├── transformer.py                 # Data cleaning and normalization
├── db.py                          # MySQL schema and insert helpers
├── generate_sample_data.py        # Synthetic test data generator
├── prepare_kaggle_data.py         # Splits Kaggle CSV into chunks
├── requirements.txt               # Python dependencies
├── .env                           # Your credentials (never commit this!)
├── .gitignore                     # Excludes .env, venv, logs/ etc.
├── data/                          # Drop CSV files here to trigger pipeline
│   ├── kaggle_iot_batch_0001.csv  # Sample Kaggle IoT data (1000 rows)
│   ├── kaggle_iot_batch_0002.csv  # Sample Kaggle IoT data (1000 rows)
│   └── kaggle_iot_batch_0003.csv  # Sample Kaggle IoT data (1000 rows)
├── processed/                     # Successfully processed files (auto-created)
├── quarantine/                    # Invalid files and bad rows (auto-created)
└── logs/
    └── pipeline.log               # Full pipeline activity log
```

---

## Fault Tolerance

| Failure scenario | Behaviour |
|---|---|
| Unreadable / empty CSV | Moved to `quarantine/`, error logged |
| Missing required columns | Whole file quarantined, error logged |
| Individual bad rows | Saved to `quarantine/<file>_invalid.csv`, good rows continue |
| DB temporarily unavailable | Automatic retry x3 with exponential back-off (2s, 4s) |
| Pipeline crash | Unprocessed files remain in `data/` and are re-picked on next startup |
| Duplicate filename in archive | Auto-renamed with counter (file_1.csv, file_2.csv) |

---

## Scaling to Production

### Ingestion layer
- **Apache Kafka** — producers write sensor events to topics; the pipeline becomes a consumer group, enabling horizontal scaling by adding consumer instances.
- **AWS Kinesis / Google Pub-Sub** — fully managed alternatives with zero infrastructure to operate.

### Processing layer
- **Apache Spark Structured Streaming** — replace pandas with Spark DataFrames for distributed processing across a cluster.
- **AWS Lambda** — trigger a function on each S3 PUT event; auto-scales to millions of concurrent invocations.

### Storage layer
- **Amazon RDS Aurora (MySQL-compatible)** — managed, auto-scaling relational database with read replicas.
- **Apache Parquet on S3 + AWS Athena** — columnar storage for aggregates; query with serverless SQL.
- **ClickHouse** — column-oriented OLAP database ideal for time-series analytics at extreme scale.

### Orchestration
- **Apache Airflow** — schedule and monitor pipeline DAGs with retries, alerting, and a web UI.
- **Kubernetes** — containerise the pipeline and run replicas managed by a Deployment.

### Optimisations
- Batch micro-commits (accumulate 1,000 rows then INSERT in a single statement).
- Partition `sensor_readings` by month using PARTITION BY RANGE on timestamp.
- Add a composite index on (sensor_id, timestamp) for range queries.
- Use connection pooling to avoid reconnect overhead.

---

## Data Ethics & Compliance
- Only publicly available datasets (Kaggle / UCI / AWS Open Data) are used.
- No PII is collected or stored.
- Credentials are stored in `.env` (excluded from version control via `.gitignore`).

---

## Credits & Sources

### Dataset
- **Kaggle IoT Environmental Sensor Data** by Gary Stafford
  https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k

### Tools & Libraries Used
- **Python** — core programming language
- **pandas** — data manipulation and transformation
- **watchdog** — real-time file system monitoring
- **mysql-connector-python** — MySQL database connectivity
- **python-dotenv** — environment variable management

### AI Assistance
- This project was built with guidance from **Claude (Anthropic)** for code structure,
  debugging, and architecture decisions. All code was reviewed, understood, and
  run locally by the author.