"""
db.py – Database layer (MySQL)
Handles connection, table creation, and all INSERT operations.
"""

import os
import logging
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()          # reads .env file for credentials
logger = logging.getLogger(__name__)

# ── Connection helper ──────────────────────────────────────────────────────────
def get_connection():
    """Return a fresh MySQL connection using .env credentials."""
    return mysql.connector.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = int(os.getenv("DB_PORT", "3306")),
        user     = os.getenv("DB_USER",     "root"),
        password = os.getenv("DB_PASSWORD", ""),
        database = os.getenv("DB_NAME",     "sensor_pipeline"),
    )


# ── Schema creation ────────────────────────────────────────────────────────────
def init_db():
    """
    Create the database (if missing) and all required tables.
    Safe to call on every startup – uses IF NOT EXISTS.
    """
    # First connect WITHOUT specifying a database so we can create it
    try:
        conn = mysql.connector.connect(
            host     = os.getenv("DB_HOST",     "localhost"),
            port     = int(os.getenv("DB_PORT", "3306")),
            user     = os.getenv("DB_USER",     "root"),
            password = os.getenv("DB_PASSWORD", ""),
        )
        cur = conn.cursor()
        db_name = os.getenv("DB_NAME", "sensor_pipeline")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Database '{db_name}' ready.")
    except Error as e:
        logger.error(f"Could not create database: {e}")
        raise

    # Now connect to the database and create tables
    conn = get_connection()
    cur  = conn.cursor()

    # ── Table 1: raw sensor readings ───────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id            BIGINT       AUTO_INCREMENT PRIMARY KEY,
            sensor_id     VARCHAR(100) NOT NULL,
            timestamp     DATETIME     NOT NULL,
            location      VARCHAR(200),
            temperature   FLOAT,
            humidity      FLOAT,
            pressure      FLOAT,
            source_file   VARCHAR(255) NOT NULL,
            data_source   VARCHAR(100),
            ingested_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_sensor_id  (sensor_id),
            INDEX idx_timestamp  (timestamp),
            INDEX idx_source_file(source_file)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # ── Table 2: per-file aggregated metrics ───────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aggregated_metrics (
            id              BIGINT       AUTO_INCREMENT PRIMARY KEY,
            source_file     VARCHAR(255) NOT NULL,
            data_source     VARCHAR(100),
            sensor_type     VARCHAR(50)  NOT NULL,   -- e.g. temperature / humidity / pressure
            sensor_id       VARCHAR(100),            -- NULL means "all sensors in file"
            min_value       FLOAT,
            max_value       FLOAT,
            avg_value       FLOAT,
            std_value       FLOAT,
            record_count    INT,
            aggregated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_agg_file  (source_file),
            INDEX idx_agg_sensor(sensor_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # ── Table 3: pipeline error / quarantine log ───────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS error_log (
            id            BIGINT       AUTO_INCREMENT PRIMARY KEY,
            source_file   VARCHAR(255) NOT NULL,
            error_type    VARCHAR(100) NOT NULL,
            error_message TEXT,
            row_num    INT,
            logged_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_err_file(source_file)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("All tables verified / created.")


# ── Insert helpers ─────────────────────────────────────────────────────────────
def insert_readings(rows: list[dict]):
    """Bulk-insert validated sensor rows into sensor_readings."""
    if not rows:
        return
    sql = """
        INSERT INTO sensor_readings
            (sensor_id, timestamp, location, temperature, humidity, pressure,
             source_file, data_source)
        VALUES
            (%(sensor_id)s, %(timestamp)s, %(location)s, %(temperature)s,
             %(humidity)s, %(pressure)s, %(source_file)s, %(data_source)s)
    """
    _bulk_execute(sql, rows)
    logger.info(f"Inserted {len(rows)} raw readings.")


def insert_aggregates(rows: list[dict]):
    """Bulk-insert aggregated metrics."""
    if not rows:
        return
    sql = """
        INSERT INTO aggregated_metrics
            (source_file, data_source, sensor_type, sensor_id,
             min_value, max_value, avg_value, std_value, record_count)
        VALUES
            (%(source_file)s, %(data_source)s, %(sensor_type)s, %(sensor_id)s,
             %(min_value)s, %(max_value)s, %(avg_value)s, %(std_value)s, %(record_count)s)
    """
    _bulk_execute(sql, rows)
    logger.info(f"Inserted {len(rows)} aggregate rows.")


def log_error(source_file: str, error_type: str, message: str, row_number: int = None):
    """Record a validation or processing error in error_log."""
    sql = """
        INSERT INTO error_log (source_file, error_type, error_message, row_num)
        VALUES (%s, %s, %s, %s)
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(sql, (source_file, error_type, message, row_number))
        conn.commit()
        cur.close()
        conn.close()
    except Error as e:
        logger.error(f"Failed to write error log: {e}")


def _bulk_execute(sql: str, rows: list[dict], retries: int = 3):
    """Execute a bulk INSERT with automatic retry on transient failures."""
    import time
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection()
            cur  = conn.cursor()
            cur.executemany(sql, rows)
            conn.commit()
            cur.close()
            conn.close()
            return
        except Error as e:
            logger.warning(f"DB insert attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)   # exponential back-off: 2s, 4s
            else:
                logger.error("All DB insert retries exhausted.")
                raise
