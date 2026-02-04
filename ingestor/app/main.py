import os
import re
import sys
import time
import uuid
import hashlib
import logging
import pandas as pd
import psycopg2
import yaml
from typing import Dict, List, Any

from pipeline import run_csv_pipeline


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

# Add parent directory to path for api_fetcher imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Phase 1 Refactoring: Import new pipeline modules

PIPELINE_ENABLED = True


WATCH_DIR = "/data/incoming"
PROCESSED_DIR = "/data/processed"
REJECTED_DIR = "/data/rejected"
MAPPINGS_DIR = "/app/mappings"

POLL_INTERVAL = 10
STABLE_SECONDS = 3

# ============================================================================
# Connection Management
# ============================================================================

def get_conn():
    """Get database connection."""
    return psycopg2.connect(os.environ["DB_DSN"])

# ============================================================================
# File Operations
# ============================================================================

def file_sha256(path: str) -> str:
    """Calculate SHA256 hash of file for deduplication."""
    sha256_hash = hashlib.sha256()
    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def is_stable(path: str) -> bool:
    """Check if file size is stable (not being written to)."""
    s1 = os.path.getsize(path)
    time.sleep(STABLE_SECONDS)
    return s1 == os.path.getsize(path)


def generate_device_id(location: str) -> str:
    """Generate device_id from location string."""
    if location is None or pd.isna(location):
        location = "unknown"
    location = str(location).strip()
    if not location or location.lower() == "nan":
        location = "unknown"
    return f"device_{hashlib.md5(location.encode()).hexdigest()[:8]}"


def detect_dataset_from_content(path: str) -> Dict[str, Any]:
    """
    Detect dataset type, granularity, date range from file content.
    Returns: {mapping, device_id, start_date, end_date, granularity}
    """
    fname = os.path.basename(path)
    ext = os.path.splitext(fname)[1].lower()

    # Read header + sample rows
    read_fn = pd.read_excel if ext == '.xlsx' else pd.read_csv
    df = read_fn(path, nrows=5)
    cols = {c.lower() for c in df.columns}

    # Detect type by column signatures
    if cols & {'pm10', 'pm2p5', 'humidity', 'temperature', 'atm_pressure'} or \
       any('µg/m' in c or 'hpa' in c.lower() for c in df.columns):
        mapping, dtype = 'environmental_metrics', 'environmental'
    elif cols & {'nr. animals', 'feed efficiency', 'rumination'} or \
         any('production' in c and 'cow' in c for c in cols):
        mapping, dtype = 'dairy_production', 'dairy'
    elif {'date and time'} & cols and len(df.columns) == 2:
        # Energy file: 2 columns (Date and Time, Hourly/Daily)
        second_col = [c for c in df.columns if c.lower() != 'date and time'][0]
        mapping = 'energy_hourly' if 'hourly' in second_col.lower() else 'energy_daily'
        dtype = 'energy'
    else:
        raise ValueError(f"Cannot detect dataset type for {fname}")

    # Extract device_id only for energy (from filename pattern: UUID-type-...)
    device_id = None
    if dtype == 'energy':
        m = re.match(r'^([a-f0-9]{20,})-', fname)
        device_id = m.group(1) if m else None

    # Get date range and granularity from full data
    df_full = read_fn(path)
    ts_col = next((c for c in df_full.columns if any(k in c.lower() for k in ['date', 'time', 'timestamp'])), None)

    start_date = end_date = granularity = None
    if ts_col and len(df_full) > 0:
        # Parse timestamps
        ts_series = pd.to_datetime(df_full[ts_col], errors='coerce')
        ts_series = ts_series.dropna()
        if len(ts_series) > 0:
            start_date = ts_series.min().date()
            end_date = ts_series.max().date()
            # Detect granularity from median time delta
            if len(ts_series) > 1:
                deltas = ts_series.sort_values().diff().dropna()
                median_delta = deltas.median().total_seconds()
                granularity = 'hourly' if median_delta < 7200 else 'daily' if median_delta < 172800 else 'raw'
            else:
                granularity = 'daily'

    return {
        'mapping': f"{MAPPINGS_DIR}/{mapping}.yaml",
        'device_id': device_id,
        'start_date': start_date,
        'end_date': end_date,
        'granularity': granularity or ('hourly' if 'hourly' in mapping else 'daily'),
        'dataset_type': dtype
    }


# ============================================================================
# Database Operations - New Modular (for ingest_csv_file)
# ============================================================================

def check_duplicate(cursor, table: str, primary_key_cols: List[str],
                   values: Dict[str, Any]) -> bool:
    """
    Check if record exists using primary key columns.
    Returns True if duplicate found, False otherwise.
    FIXED: Use SELECT 1 instead of SELECT id
    """
    try:
        pk_values = [values[col] for col in primary_key_cols]
        pk_where = ' AND '.join([f"{col} = %s" for col in primary_key_cols])

        cursor.execute(
            f"SELECT 1 FROM {table} WHERE {pk_where} LIMIT 1",
            pk_values
        )

        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking duplicate in {table}: {str(e)}")
        return False

# ============================================================================
# New Pipeline Integration (Phase 1 Refactoring)
# ============================================================================

def ingest_file_with_pipeline(file_path: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ingest file using new modular pipeline.
    Args: file_path, meta={mapping, device_id, start_date, end_date, granularity}
    """
    if not PIPELINE_ENABLED:
        return {'error': 'Pipeline not enabled'}

    fname = os.path.basename(file_path)
    sha = file_sha256(file_path)
    device_id = meta.get('device_id')

    with open(meta['mapping']) as f:
        mapping = yaml.safe_load(f)

    connection = get_conn()
    try:
        cursor = connection.cursor()
        # Resolve device_id to PK for energy datasets
        if device_id:
            cursor.execute("SELECT id FROM device WHERE device_id = %s", (device_id,))
            row = cursor.fetchone()
            device_id = row[0] if row else None

        # Check duplicates
        cursor.execute("SELECT file_id FROM ingest_file WHERE sha256 = %s", (sha,))
        if cursor.fetchone():
            logger.info(f"File {fname} already processed (duplicate SHA256)")
            return {'status': 'skipped', 'reason': 'duplicate', 'file': fname}

        file_id = str(uuid.uuid4())
        granularity = meta.get('granularity') or mapping.get('granularity', 'unknown')

        cursor.execute("""
            INSERT INTO ingest_file
                (file_id, file_name, device_id, granularity, start_date, end_date, sha256, pipeline_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, '2.0')
            RETURNING file_id
        """, (file_id, fname, device_id, granularity, meta.get('start_date'), meta.get('end_date'), sha))
        connection.commit()

        # Run pipeline
        metrics = run_csv_pipeline(
            file_path=file_path,
            connection=connection,
            mapping=mapping,
            source_file_id=uuid.UUID(file_id),
            device_id=device_id
        )

        # Update ingest_file with results
        cursor.execute("""
            UPDATE ingest_file
            SET
                execution_time_ms = %s,
                validation_status = %s,
                quality_score = %s
            WHERE file_id = %s
        """, (
            int(metrics.total_duration * 1000),
            'passed' if metrics.invalid_records == 0 else 'partial',
            round((metrics.valid_records / metrics.extract_records * 100)
                  if metrics.extract_records > 0 else 0, 2),
            file_id
        ))
        connection.commit()

        logger.info(
            f"✓ Pipeline completed for {fname}: "
            f"{metrics.load_records} loaded, "
            f"{metrics.invalid_records} invalid, "
            f"{metrics.skipped_records} skipped "
            f"({metrics.total_duration:.2f}s)"
        )

        return {
            'status': 'success',
            'file': fname,
            'file_id': file_id,
            'metrics': metrics.to_dict()
        }

    except Exception as e:
        logger.error(f"Pipeline failed for {fname}: {e}", exc_info=True)
        connection.rollback()
        return {'status': 'failed', 'file': fname, 'error': str(e)}
    finally:
        connection.close()


# ============================================================================
# Main Event Loop
# ============================================================================

def main():
    """Main application entry point."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(REJECTED_DIR, exist_ok=True)

    logger.info("="*60)
    logger.info("Starting CEI-InOE Ingestor")
    logger.info(f"Pipeline Mode: {'NEW (Phase 1)' if PIPELINE_ENABLED else 'LEGACY'}")
    logger.info(f"File watcher: {WATCH_DIR}")
    logger.info("="*60)

    while True:
        try:
            for f in os.listdir(WATCH_DIR):
                path = f"{WATCH_DIR}/{f}"
                if (f.endswith(".xlsx") or f.endswith(".csv")) and is_stable(path):
                    try:
                        if PIPELINE_ENABLED:
                            process_file_pipeline(path)
                        else:
                            logger.info(f"Processing (legacy) file: {f}")
                            continue
                    except Exception as e:
                        logger.exception(f"Error processing {f}: {e}")
                        os.rename(path, f"{REJECTED_DIR}/{f}")
        except Exception as e:
            logger.exception(f"Error in main loop: {e}")
        
        time.sleep(POLL_INTERVAL)


def process_file_pipeline(path: str):
    """Process file using content-based detection."""
    fname = os.path.basename(path)
    logger.info(f"[PIPELINE] Processing {fname}")
    
    try:
        meta = detect_dataset_from_content(path)
        logger.info(f"[PIPELINE] Detected: {meta['dataset_type']} ({meta['granularity']})")
        result = ingest_file_with_pipeline(path, meta)
    except Exception as e:
        logger.error(f"[PIPELINE] Detection/ingestion failed: {e}")
        result = {'status': 'failed', 'error': str(e)}

    if result.get('status') == 'success':
        dest = f"{PROCESSED_DIR}/{fname}"
        os.rename(path, dest)
        logger.info(f"[PIPELINE] ✓ Moved {fname} → processed/")
    elif result.get('status') == 'skipped':
        # Still move to processed if duplicate
        dest = f"{PROCESSED_DIR}/{fname}"
        os.rename(path, dest)
        logger.info(f"[PIPELINE] ⊘ Skipped {fname} (duplicate) → processed/")
    else:
        # Failed - move to rejected
        dest = f"{REJECTED_DIR}/{fname}"
        os.rename(path, dest)
        logger.error(f"[PIPELINE] ✗ Failed {fname} → rejected/")


if __name__ == "__main__":
    main()