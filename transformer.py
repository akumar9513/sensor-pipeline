import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)
SENSOR_COLS = ["temperature", "humidity", "pressure"]

def _infer_source(filepath):
    name = os.path.basename(filepath).lower()
    if "kaggle" in name: return "Kaggle"
    if "uci" in name: return "UCI"
    if "aws" in name: return "AWS"
    return "unknown"

def _agg_row(series, sensor_type, sensor_id, source_file, data_source):
    s = series.dropna()
    return {
        "source_file": source_file, "data_source": data_source,
        "sensor_type": sensor_type, "sensor_id": sensor_id,
        "min_value": float(s.min()), "max_value": float(s.max()),
        "avg_value": float(s.mean()),
        "std_value": float(s.std()) if len(s) > 1 else 0.0,
        "record_count": int(len(s)),
    }

def transform(df, source_file):
    df = df.copy()
    parsed_ts = pd.to_datetime(df["timestamp"], errors="coerce")
    df["timestamp"] = parsed_ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    for col in SENSOR_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in SENSOR_COLS:
        if col in df.columns and df[col].notna().any():
            col_min, col_max = df[col].min(), df[col].max()
            df[col] = (df[col] - col_min) / (col_max - col_min) if col_max != col_min else 0.0
    df["source_file"] = os.path.basename(source_file)
    df["data_source"] = _infer_source(source_file)
    if "location" not in df.columns:
        df["location"] = None
    logger.info(f"Transformed {len(df)} rows from {os.path.basename(source_file)}.")
    return df

def compute_aggregates(df, source_file):
    records = []
    fname = os.path.basename(source_file)
    dsource = _infer_source(source_file)
    for col in SENSOR_COLS:
        if col not in df.columns or df[col].isna().all():
            continue
        records.append(_agg_row(df[col], col, None, fname, dsource))
        if "sensor_id" in df.columns:
            for sid, group in df.groupby("sensor_id"):
                if group[col].notna().any():
                    records.append(_agg_row(group[col], col, str(sid), fname, dsource))
    logger.info(f"Computed {len(records)} aggregate rows for {os.path.basename(source_file)}.")
    return records
