"""
validator.py – Data validation rules for incoming sensor CSV rows.

Each function returns (is_valid: bool, reason: str).
The main entry point is validate_row().
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ── Acceptable value ranges ────────────────────────────────────────────────────
RANGES = {
    "temperature": (-50.0,  50.0),
    "humidity":    (  0.0, 100.0),
    "pressure":    (800.0, 1100.0),
}

# Required columns that must exist and must not be null
REQUIRED_COLUMNS = ["sensor_id", "timestamp"]


def validate_dataframe(df: pd.DataFrame, source_file: str):
    """
    Validate an entire DataFrame.

    Returns
    -------
    valid_df   : pd.DataFrame  – rows that passed all checks
    invalid_df : pd.DataFrame  – rows that failed (with an extra 'error' column)
    """
    # ── 1. Column presence ─────────────────────────────────────────────────────
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    errors = []          # list of (row_index, reason) tuples
    valid_mask = pd.Series(True, index=df.index)

    # ── 2. Null checks on required fields ──────────────────────────────────────
    for col in REQUIRED_COLUMNS:
        null_rows = df[col].isnull()
        for idx in df[null_rows].index:
            errors.append((idx, f"Null value in required column '{col}'"))
        valid_mask &= ~null_rows

    # ── 3. Timestamp parseable ─────────────────────────────────────────────────
    ts_bad = pd.to_datetime(df["timestamp"], errors="coerce").isnull()
    for idx in df[ts_bad & valid_mask].index:
        errors.append((idx, f"Unparseable timestamp: {df.at[idx, 'timestamp']}"))
    valid_mask &= ~ts_bad

    # ── 4. Numeric types & range checks ───────────────────────────────────────
    for col, (lo, hi) in RANGES.items():
        if col not in df.columns:
            continue  # column is optional

        # Convert to numeric (non-numeric → NaN)
        numeric = pd.to_numeric(df[col], errors="coerce")
        non_numeric = numeric.isnull() & df[col].notnull()
        for idx in df[non_numeric & valid_mask].index:
            errors.append((idx, f"Non-numeric value in '{col}': {df.at[idx, col]}"))
        valid_mask &= ~non_numeric

        # Range check
        out_of_range = numeric.notnull() & ((numeric < lo) | (numeric > hi))
        for idx in df[out_of_range & valid_mask].index:
            errors.append((
                idx,
                f"'{col}' value {df.at[idx, col]} out of range [{lo}, {hi}]"
            ))
        valid_mask &= ~out_of_range

    # ── Build output DataFrames ────────────────────────────────────────────────
    valid_df   = df[valid_mask].copy()
    invalid_df = df[~valid_mask].copy()

    # Attach error messages to the invalid rows
    error_map = {}
    for idx, reason in errors:
        error_map.setdefault(idx, []).append(reason)
    invalid_df["error"] = invalid_df.index.map(
        lambda i: "; ".join(error_map.get(i, ["unknown"]))
    )

    logger.info(
        f"{source_file}: {len(valid_df)} valid rows, "
        f"{len(invalid_df)} invalid rows."
    )
    return valid_df, invalid_df
