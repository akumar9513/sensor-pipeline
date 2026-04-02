"""
Real-Time Sensor Data Pipeline
Main entry point - monitors a folder and processes CSV files automatically.
"""

import time
import os
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from processor import process_file
from db import init_db

# ── Logging setup ──────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(),          # also print to terminal
    ],
)
logger = logging.getLogger(__name__)

# ── Folders ────────────────────────────────────────────────────────────────────
DATA_DIR       = "data"
QUARANTINE_DIR = "quarantine"
PROCESSED_DIR  = "processed"

for d in [DATA_DIR, QUARANTINE_DIR, PROCESSED_DIR]:
    os.makedirs(d, exist_ok=True)


# ── File-system event handler ──────────────────────────────────────────────────
class CSVHandler(FileSystemEventHandler):
    """Triggered every time a new file lands in the data/ folder."""

    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".csv"):
            logger.info(f"New file detected: {event.src_path}")
            # Small sleep so the file has time to be fully written to disk
            time.sleep(1)
            process_file(event.src_path)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    logger.info("Starting Sensor Data Pipeline…")
    init_db()           # create tables if they don't exist yet

    # Process any CSV files that were already sitting in data/ before we started
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            process_file(os.path.join(DATA_DIR, filename))

    # Start the real-time watcher
    observer = Observer()
    observer.schedule(CSVHandler(), path=DATA_DIR, recursive=False)
    observer.start()
    logger.info(f"Watching '{DATA_DIR}/' for new CSV files. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Pipeline stopped.")
    observer.join()


if __name__ == "__main__":
    main()
