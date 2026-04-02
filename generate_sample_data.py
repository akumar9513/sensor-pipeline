"""
generate_sample_data.py
Generates realistic IoT sensor CSV files and drops them into data/
so you can test the pipeline without downloading a real dataset.

Run once:  python generate_sample_data.py
It creates 3 CSV files with ~100 rows each, including some deliberately
bad rows (nulls, out-of-range values) to demo quarantine logic.
"""

import os
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

SENSORS    = [f"SENSOR_{i:03d}" for i in range(1, 6)]
LOCATIONS  = ["Lab-A", "Lab-B", "Warehouse", "Rooftop", "Basement"]
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def make_clean_rows(n: int = 80) -> list[dict]:
    base_time = datetime(2024, 1, 1, 8, 0, 0)
    rows = []
    for i in range(n):
        rows.append({
            "sensor_id":   random.choice(SENSORS),
            "timestamp":   (base_time + timedelta(minutes=i * 5)).isoformat(),
            "location":    random.choice(LOCATIONS),
            "temperature": round(random.uniform(-10, 40), 2),
            "humidity":    round(random.uniform(10, 90), 2),
            "pressure":    round(random.uniform(950, 1050), 2),
        })
    return rows


def make_bad_rows() -> list[dict]:
    """A handful of intentionally broken records."""
    return [
        # Missing sensor_id
        {"sensor_id": None,        "timestamp": "2024-01-01T10:00:00",
         "location": "Lab-A",      "temperature": 22.1, "humidity": 55.0, "pressure": 1010.0},
        # Missing timestamp
        {"sensor_id": "SENSOR_001","timestamp": None,
         "location": "Lab-B",      "temperature": 19.5, "humidity": 60.0, "pressure": 1005.0},
        # Temperature out of range
        {"sensor_id": "SENSOR_002","timestamp": "2024-01-01T10:05:00",
         "location": "Rooftop",    "temperature": 999.9, "humidity": 45.0, "pressure": 1000.0},
        # Non-numeric humidity
        {"sensor_id": "SENSOR_003","timestamp": "2024-01-01T10:10:00",
         "location": "Basement",   "temperature": 20.0, "humidity": "N/A", "pressure": 990.0},
        # Pressure out of range
        {"sensor_id": "SENSOR_004","timestamp": "2024-01-01T10:15:00",
         "location": "Warehouse",  "temperature": 18.0, "humidity": 70.0, "pressure": 5000.0},
    ]


for file_num in range(1, 4):
    rows = make_clean_rows(80) + make_bad_rows()
    random.shuffle(rows)
    df   = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, f"sensor_data_batch_{file_num:02d}.csv")
    df.to_csv(path, index=False)
    print(f"Created: {path}  ({len(df)} rows, {len(make_bad_rows())} deliberately bad)")

print("\nDone! Drop the pipeline now:  python pipeline.py")
