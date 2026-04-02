"""
processor.py – Orchestrates the full lifecycle of one CSV file:
  1. Read
  2. Validate  →  quarantine bad rows
  3. Transform
  4. Compute aggregates
  5. Store in MySQL
  6. Move file to processed/
"""

import os
import shutil
import logging
import pandas as pd

from validator   import validate_dataframe
from transformer import transform, compute_aggregates
from db          import insert_readings, insert_aggregates, log_error

logger = logging.getLogger(__name__)

QUARANTINE_DIR = "quarantine"
PROCESSED_DIR  = "processed"


def process_file(filepath: str):
    """End-to-end processing for a single CSV file."""
    fname = os.path.basename(filepath)
    logger.info(f"── Processing: {fname} ──────────────────────────")

    # ── Step 1: Read ───────────────────────────────────────────────────────────
    try:
        df = pd.read_csv(filepath)
        logger.info(f"  Read {len(df)} rows, {len(df.columns)} columns.")
    except Exception as e:
        logger.error(f"  Cannot read file '{fname}': {e}")
        log_error(fname, "READ_ERROR", str(e))
        _move(filepath, QUARANTINE_DIR)
        return

    if df.empty:
        logger.warning(f"  '{fname}' is empty – skipping.")
        _move(filepath, QUARANTINE_DIR)
        return

    # ── Step 2: Validate ───────────────────────────────────────────────────────
    try:
        valid_df, invalid_df = validate_dataframe(df, fname)
    except ValueError as e:
        # Missing required columns → whole file is bad
        logger.error(f"  Schema error in '{fname}': {e}")
        log_error(fname, "SCHEMA_ERROR", str(e))
        _move(filepath, QUARANTINE_DIR)
        return

    # Quarantine invalid rows: save them to quarantine/<fname>_invalid.csv
    if not invalid_df.empty:
        qpath = os.path.join(QUARANTINE_DIR, fname.replace(".csv", "_invalid.csv"))
        invalid_df.to_csv(qpath, index=False)
        logger.warning(f"  {len(invalid_df)} invalid rows quarantined → {qpath}")

        # Log each bad row in the DB
        for _, row in invalid_df.iterrows():
            log_error(
                source_file = fname,
                error_type  = "VALIDATION_ERROR",
                message     = row.get("error", "unknown"),
                row_number=int(row.name) + 2,   # +2 for header + 0-index offset
            )

    if valid_df.empty:
        logger.warning(f"  No valid rows in '{fname}' – nothing to store.")
        _move(filepath, QUARANTINE_DIR)
        return

    # ── Step 3: Transform ──────────────────────────────────────────────────────
    try:
        clean_df = transform(valid_df, filepath)
    except Exception as e:
        logger.error(f"  Transform failed for '{fname}': {e}")
        log_error(fname, "TRANSFORM_ERROR", str(e))
        return

    # ── Step 4: Compute aggregates ─────────────────────────────────────────────
    try:
        agg_rows = compute_aggregates(clean_df, filepath)
    except Exception as e:
        logger.error(f"  Aggregation failed for '{fname}': {e}")
        log_error(fname, "AGGREGATION_ERROR", str(e))
        return

    # ── Step 5: Store in DB ────────────────────────────────────────────────────
    try:
        # Build list-of-dicts for raw readings
        reading_rows = clean_df[[
            "sensor_id", "timestamp", "location",
            "temperature", "humidity", "pressure",
            "source_file", "data_source",
        ]].where(pd.notnull(clean_df), None).to_dict(orient="records")

        insert_readings(reading_rows)
        insert_aggregates(agg_rows)
    except Exception as e:
        logger.error(f"  DB insert failed for '{fname}': {e}")
        log_error(fname, "DB_ERROR", str(e))
        return   # leave file in place so it can be retried

    # ── Step 6: Archive processed file ────────────────────────────────────────
    _move(filepath, PROCESSED_DIR)
    logger.info(f"  ✓ '{fname}' processed successfully.")


# ── Helper ─────────────────────────────────────────────────────────────────────
def _move(src: str, dest_dir: str):
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, os.path.basename(src))
    # Avoid overwrite collisions by appending a counter
    if os.path.exists(dest):
        base, ext = os.path.splitext(os.path.basename(src))
        i = 1
        while os.path.exists(dest):
            dest = os.path.join(dest_dir, f"{base}_{i}{ext}")
            i += 1
    shutil.move(src, dest)
    logger.info(f"  Moved '{os.path.basename(src)}' → {dest_dir}/")
