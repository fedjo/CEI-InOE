import csv
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
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch
from typing import Dict, List, Tuple, Any
from threading import Thread
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

# Add parent directory to path for api_fetcher imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Phase 1 Refactoring: Import new pipeline modules
try:
    from app.pipeline import run_csv_pipeline, run_api_pipeline
    from app.validation import DataValidator
    from app.transformation import DataTransformer, LegacyTransformationAdapter
    from app.staging import StagingManager
    PIPELINE_ENABLED = True
    logger.info("✓ New pipeline modules loaded successfully")
except ImportError as e:
    PIPELINE_ENABLED = False
    logger.warning(f"Pipeline modules not available, using legacy mode: {e}")

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


def parse_csv_filename(fname: str) -> Tuple[str, Any, Any]:
    """
    Parse CSV filename to extract device_id, start_date, end_date.
    Handles multiple formats:
    - Old format: metrics_1236_10122025_18122025.csv
    - New format: 68f1fb6546b86e0009bf15bb-hourly-07112025-18122025.csv
    Returns: (device_id, start_date, end_date)
    """
    try:
        # New format: UUID-type-DDMMYYYY-DDMMYYYY.csv
        # Example: 68f1fb6546b86e0009bf15bb-hourly-07112025-18122025.csv
        match = re.search(r'^([a-f0-9]+)-(hourly|daily)-(\d{8})-(\d{8})\.csv$', fname)
        if match:
            device_id = match.group(1)  # UUID
            start_date = datetime.strptime(match.group(3), '%d%m%Y').date()
            end_date = datetime.strptime(match.group(4), '%d%m%Y').date()
            return device_id, start_date, end_date

        # Old format: metrics_1236_10122025_18122025.csv
        parts = fname.replace('.csv', '').split('_')
        if len(parts) >= 4:
            device_id = f"{parts[0]}_{parts[1]}"  # e.g., "metrics_1236"
            try:
                start_date = datetime.strptime(parts[2], '%d%m%Y').date()
                end_date = datetime.strptime(parts[3], '%d%m%Y').date()
                return device_id, start_date, end_date
            except (ValueError, IndexError):
                pass

    except Exception as e:
        logger.warning(f"Error parsing filename {fname}: {e}")
    
    return "unknown", None, None


def generate_device_id(location: str) -> str:
    """Generate device_id from location string."""
    # FIXED: Handle NaN and convert to string safely
    if location is None or pd.isna(location):
        location = "unknown"
    location = str(location).strip()
    
    if not location or location.lower() == "nan":
        location = "unknown"
    
    return f"device_{hashlib.md5(location.encode()).hexdigest()[:8]}"

# ============================================================================
# Configuration & Mapping
# ============================================================================

def load_mapping(mapping_file: str) -> Dict[str, Any]:
    """Load YAML mapping configuration."""
    with open(mapping_file, 'r') as f:
        return yaml.safe_load(f)


def detect_dataset_type(df: pd.DataFrame, fname: str) -> str:
    """
    Detect dataset type from dataframe columns and filename.
    Returns mapping file path.
    """
    columns_lower = [col.lower() for col in df.columns]

    # Check for dairy production indicators
    if any('production' in col for col in columns_lower):
        return f"{MAPPINGS_DIR}/dairy_production.yaml"

    # Check for environmental metrics (BEFORE energy - more specific)
    if any('pm10' in col or 'temperature' in col or 'humidity' in col for col in columns_lower):
        return f"{MAPPINGS_DIR}/environmental_metrics.yaml"

    # Check for energy indicators
    # Look for: 'kwh', 'energy', 'hourly', or just numeric column with time
    if any('kwh' in col or 'energy' in col or 'hourly' in col for col in columns_lower):
        if 'hourly' in fname.lower():
            return f"{MAPPINGS_DIR}/energy_hourly.yaml"
        else:
            return f"{MAPPINGS_DIR}/energy_daily.yaml"
    
    # Also check filename for hourly/daily pattern if columns don't help
    if 'hourly' in fname.lower():
        return f"{MAPPINGS_DIR}/energy_hourly.yaml"
    elif 'daily' in fname.lower():
        return f"{MAPPINGS_DIR}/energy_daily.yaml"

    raise Exception(f"Cannot detect dataset type for {fname}")

# ============================================================================
# Data Coercion
# ============================================================================

def coerce(value: Any, target_type: str) -> Any:
    """
    Coerce value to target type. Handles string, numeric, and date conversions.
    Original working version - used by process_file for all datasets.
    """
    if value is None or pd.isna(value) or value == "":
        return None

    try:
        if target_type == "float":
            if isinstance(value, str):
                value = value.replace(",", ".")
            return float(value)
        elif target_type == "int":
            if isinstance(value, str):
                value = value.strip()
            return int(float(value))
        elif target_type == "datetime":
            return pd.to_datetime(value)
        elif target_type == "date":
            return pd.to_datetime(value).date()
        elif target_type == "string":
            return str(value).strip()
        else:
            return value
    except (ValueError, TypeError):
        return None


def coerce_value(value: str, coercion_type: str) -> Any:
    """
    Coerce CSV string value to appropriate type.
    Used by new ingest_csv_file for modular datasets.
    Returns None if value is empty or conversion fails.
    """
    if not value or not value.strip():
        return None

    try:
        if coercion_type == 'datetime':
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        elif coercion_type == 'date':
            return datetime.strptime(value, '%Y-%m-%d').date()
        elif coercion_type == 'float':
            return float(value)
        elif coercion_type == 'int':
            return int(value)
        else:
            return value
    except (ValueError, TypeError):
        return None

# ============================================================================
# Data Transformation
# ============================================================================

def transform_row(csv_row: Dict[str, str], mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform CSV row into database column values using mapping and coercions.
    Uses coerce_value for new modular datasets.
    """
    values = {}
    
    for csv_col, db_col in mapping['columns'].items():
        if csv_col in csv_row:
            coercion_type = mapping['coercions'][db_col]
            values[db_col] = coerce_value(csv_row[csv_col], coercion_type)
        else:
            values[db_col] = None

    return values


def transform_row_legacy(row: pd.Series, mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform pandas DataFrame row using original coerce logic.
    Used by process_file for backwards compatibility.
    FIXED: Correct pandas Series access
    """
    canonical = {}
    
    for csv_col, canonical_col in mapping["columns"].items():
        # FIXED: Use proper pandas access, not .get()
        raw_val = row[csv_col] if csv_col in row.index else None
        target_type = mapping["coercions"].get(canonical_col)
        try:
            canonical[canonical_col] = coerce(raw_val, target_type)
        except Exception as e:
            logger.warning(f"Failed to coerce {csv_col}={raw_val} to {target_type}: {e}")
            canonical[canonical_col] = None
    
    return canonical

# ============================================================================
# Database Operations - Legacy (for process_file)
# ============================================================================

def insert_dairy_production_batch(cur, rows: List[Tuple], file_id: str):
    """Insert dairy production rows with conflict handling."""
    execute_batch(cur, """
        INSERT INTO dairy_production 
        (production_date, day_production_per_cow_kg,
         number_of_animals, average_lactation_days, fed_per_cow_total_kg,
         fed_per_cow_water_kg, feed_efficiency, rumination_minutes, source_file)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (production_date) DO UPDATE SET
            day_production_per_cow_kg = EXCLUDED.day_production_per_cow_kg,
            number_of_animals = EXCLUDED.number_of_animals,
            average_lactation_days = EXCLUDED.average_lactation_days,
            fed_per_cow_total_kg = EXCLUDED.fed_per_cow_total_kg,
            fed_per_cow_water_kg = EXCLUDED.fed_per_cow_water_kg,
            feed_efficiency = EXCLUDED.feed_efficiency,
            rumination_minutes = EXCLUDED.rumination_minutes
    """, rows, page_size=500)


def insert_energy_hourly_batch(cur, rows: List[Tuple]):
    """Insert energy hourly rows."""
    execute_batch(cur, """
        INSERT INTO fact_energy_hourly (device_id, ts, energy_kwh, source_file)
        VALUES (%s,%s,%s,%s)
    """, rows, page_size=500)


def insert_energy_daily_batch(cur, rows: List[Tuple]):
    """Insert energy daily rows."""
    execute_batch(cur, """
        INSERT INTO fact_energy_daily (device_id, ts, energy_kwh, source_file)
        VALUES (%s,%s,%s,%s)
    """, rows, page_size=500)


def insert_environmental_metrics_batch(cur, rows: List[Tuple]):
    """Insert environmental metrics rows with conflict handling."""
    execute_batch(cur, """
        INSERT INTO environmental_metrics
        (timestamp, atm_pressure, noise_level_db, temperature, 
         humidity, pm10, wind_speed, wind_direction_sectors, wind_angle, pm2p5, source_file)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (timestamp) DO UPDATE SET
            atm_pressure = EXCLUDED.atm_pressure,
            noise_level_db = EXCLUDED.noise_level_db,
            temperature = EXCLUDED.temperature,
            humidity = EXCLUDED.humidity,
            pm10 = EXCLUDED.pm10,
            wind_speed = EXCLUDED.wind_speed,
            wind_direction_sectors = EXCLUDED.wind_direction_sectors,
            wind_angle = EXCLUDED.wind_angle,
            pm2p5 = EXCLUDED.pm2p5
    """, rows, page_size=500)

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


def insert_record(cursor, table: str, values: Dict[str, Any], 
                 source_file_id: str = None) -> Tuple[bool, str]:
    """
    Insert a record into database table.
    Returns (success: bool, error_msg: str)
    FIXED: Better error handling and return error message
    """
    try:
        if source_file_id:
            values['source_file'] = source_file_id
        
        columns = ', '.join(values.keys())
        placeholders = ', '.join(['%s'] * len(values))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        cursor.execute(query, tuple(values.values()))
        return True, None
    except Exception as e:
        error_msg = f"Error inserting into {table}: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def log_ingestion_legacy(cur, file_id: str, fname: str, device_id: str, 
                        mapping: Dict[str, Any], start_date: Any, 
                        end_date: Any, sha: str):
    """
    Register file in ingest_file table using legacy schema.
    Used by process_file for backwards compatibility.
    """
    cur.execute("""
        INSERT INTO ingest_file
        (file_id, file_name, device_id, granularity, start_date, end_date, sha256)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (file_id, fname, device_id, mapping["granularity"], start_date, end_date, sha))


def log_ingestion(cursor, dataset: str, filename: str, 
                 ingestion_date: str, inserted: int, 
                 skipped: int, errors: int, sha: str = None) -> str:
    """
    Log file ingestion to ingest_file table.
    Uses only columns that exist in the legacy schema.
    Returns the file_id UUID.
    """
    file_id = str(uuid.uuid4())
    device_id = "environmental_metrics"  # Use dataset name as device_id for environmental metrics
    granularity = "raw"  # environmental metrics are raw/unprocessed data
    
    try:
        cursor.execute(
            """INSERT INTO ingest_file 
               (file_id, file_name, device_id, granularity, sha256)
               VALUES (%s, %s, %s, %s, %s)""",
            (file_id, filename, device_id, granularity, sha)
        )
    except psycopg2.Error as e:
        logger.error(f"Error logger ingestion: {str(e)}")
        raise
    
    return file_id

# ============================================================================
# Main Ingestion Logic - New Modular
# ============================================================================

def ingest_csv_file(file_path: str, db_connection, table: str,
                   mapping_file: str) -> Dict[str, Any]:
    """
    Main ingestion function for CSV files (new modular approach).
    Handles transformation, deduplication, and logging.
    FIXED: Better error handling in insert_record call
    """
    mapping = load_mapping(mapping_file)
    filename = file_path.split('/')[-1]
    sha = file_sha256(file_path)
    
    inserted = 0
    skipped = 0
    errors = 0
    cursor = db_connection.cursor()
    source_file_id = None
    ingestion_date = None

    try:
        # Extract ingestion date based on filename pattern
        if re.search(r'_(\d{8})_(\d{8})\.csv$', filename):
            match = re.search(r'_(\d{8})_(\d{8})\.csv$', filename)
            end_date_str = match.group(2)
            ingestion_date = datetime.strptime(end_date_str, '%d%m%Y').date()
        else:
            ingestion_date = datetime.now().date()

        # Check for duplicate file
        cursor.execute("SELECT 1 FROM ingest_file WHERE sha256=%s", (sha,))
        if cursor.fetchone():
            logger.info(f"File already ingested: {filename}")
            return {
                'dataset': mapping['dataset'],
                'file': filename,
                'file_id': None,
                'inserted': 0,
                'skipped': 0,
                'errors': 0,
                'status': 'duplicate'
            }

        # Log ingestion FIRST to get source_file_id
        source_file_id = log_ingestion(
            cursor,
            mapping['dataset'],
            filename,
            ingestion_date,
            0,
            0,
            0,
            sha
        )
        db_connection.commit()

        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, csv_row in enumerate(reader, start=2):
                try:
                    # Transform row
                    values = transform_row(csv_row, mapping)
                    
                    # Check for duplicates
                    if check_duplicate(cursor, table, mapping['primary_key'], values):
                        skipped += 1
                        continue

                    # Insert record with source_file_id - FIXED: handle error tuple
                    success, error_msg = insert_record(cursor, table, values, source_file_id)
                    if success:
                        inserted += 1
                    else:
                        errors += 1
                        logger.error(f"Row {row_num}: {error_msg}")
                
                except Exception as e:
                    errors += 1
                    logger.warning(f"Error processing row {row_num}: {str(e)}")
        
        # Commit all inserts
        db_connection.commit()

        logger.info(f"Ingestion complete: {filename} - Inserted: {inserted}, Skipped: {skipped}, Errors: {errors}")

        return {
            'dataset': mapping['dataset'],
            'file': filename,
            'file_id': source_file_id,
            'inserted': inserted,
            'skipped': skipped,
            'errors': errors,
            'ingestion_date': str(ingestion_date),
            'status': 'success'
        }

    except Exception as e:
        db_connection.rollback()
        logger.error(f"Fatal error ingesting {file_path}: {str(e)}")
        raise
    finally:
        cursor.close()

# ============================================================================
# Dataset-Specific Wrappers (New Modular)
# ============================================================================

def ingest_environmental_metrics(file_path: str, db_connection) -> Dict[str, Any]:
    """Ingest environmental metrics CSV file."""
    return ingest_csv_file(
        file_path,
        db_connection,
        'environmental_metrics',
        f'{MAPPINGS_DIR}/environmental_metrics.yaml'
    )


def ingest_dairy_production_modular(file_path: str, db_connection) -> Dict[str, Any]:
    """Ingest dairy production CSV file (modular version)."""
    return ingest_csv_file(
        file_path,
        db_connection,
        'dairy_production',
        f'{MAPPINGS_DIR}/dairy_production.yaml'
    )


def ingest_energy_daily(file_path: str, db_connection) -> Dict[str, Any]:
    """Ingest energy daily CSV file."""
    return ingest_csv_file(
        file_path,
        db_connection,
        'fact_energy_daily',
        f'{MAPPINGS_DIR}/energy_daily.yaml'
    )


def ingest_energy_hourly(file_path: str, db_connection) -> Dict[str, Any]:
    """Ingest energy hourly CSV file."""
    return ingest_csv_file(
        file_path,
        db_connection,
        'fact_energy_hourly',
        f'{MAPPINGS_DIR}/energy_hourly.yaml'
    )

# ============================================================================
# New Pipeline Integration (Phase 1 Refactoring)
# ============================================================================

def ingest_file_with_pipeline(file_path: str, mapping_path: str) -> Dict[str, Any]:
    """
    Ingest file using new modular pipeline.
    Uses: extract → validate → transform → load stages with full observability.
    
    Args:
        file_path: Path to CSV file
        mapping_path: Path to YAML mapping file
    
    Returns:
        Dictionary with ingestion results
    """
    if not PIPELINE_ENABLED:
        logger.warning("Pipeline not available, falling back to legacy ingestion")
        return {'error': 'Pipeline not enabled'}
    
    fname = os.path.basename(file_path)
    device_id, start_date, end_date = parse_csv_filename(fname)
    sha = file_sha256(file_path)

    # Load mapping
    with open(mapping_path) as f:
        mapping = yaml.safe_load(f)
    
    connection = get_conn()
    
    try:
        cursor = connection.cursor()
        # Get device id pk is exists
        if device_id:
            cursor.execute("SELECT id FROM device WHERE device_id = %s", (device_id,))
            row = cursor.fetchone()
            if row:
                device_id = row[0]
            else:
                logger.warning(f"Device {device_id} not found")

        # Check for duplicates
        cursor.execute(
            "SELECT file_id FROM ingest_file WHERE sha256 = %s",
            (sha,)
        )
        if cursor.fetchone():
            logger.info(f"File {fname} already processed (duplicate SHA256)")
            print('File already processed (duplicate SHA256)')
            return {
                'status': 'skipped',
                'reason': 'duplicate',
                'file': fname
            }

        # Log file ingestion
        file_id = str(uuid.uuid4())
        # Extract granularity from filename or use default
        match = re.search(r'-(hourly|daily)-', fname)
        granularity = match.group(1) if match else mapping.get('granularity', 'unknown')
        
        cursor.execute("""
            INSERT INTO ingest_file
                (file_id, file_name, device_id, granularity, start_date, end_date, sha256, pipeline_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, '2.0')
            RETURNING file_id
        """, (file_id, fname, device_id, granularity, start_date, end_date, sha))
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
        return {
            'status': 'failed',
            'file': fname,
            'error': str(e)
        }
    finally:
        connection.close()


def ingest_environmental_metrics_pipeline(file_path: str) -> Dict[str, Any]:
    """Ingest environmental metrics using new pipeline."""
    return ingest_file_with_pipeline(
        file_path,
        f'{MAPPINGS_DIR}/environmental_metrics.yaml'
    )


def ingest_dairy_production_pipeline(file_path: str) -> Dict[str, Any]:
    """Ingest dairy production using new pipeline."""
    return ingest_file_with_pipeline(
        file_path,
        f'{MAPPINGS_DIR}/dairy_production.yaml'
    )


def ingest_energy_daily_pipeline(file_path: str) -> Dict[str, Any]:
    """Ingest energy daily using new pipeline."""
    return ingest_file_with_pipeline(
        file_path,
        f'{MAPPINGS_DIR}/energy_daily.yaml'
    )


def ingest_energy_hourly_pipeline(file_path: str) -> Dict[str, Any]:
    """Ingest energy hourly using new pipeline."""
    return ingest_file_with_pipeline(
        file_path,
        f'{MAPPINGS_DIR}/energy_hourly.yaml'
    )

# ============================================================================
# Legacy Processing (Original process_file - UNCHANGED LOGIC)
# ============================================================================

def process_file(path: str):
    """
    Process a single file (legacy version - original working logic).
    Handles both .xlsx and .csv files.
    Supports dairy_production_daily, energy_hourly, energy_daily, and environmental_metrics datasets.
    """
    fname = os.path.basename(path)
    logger.info(f"Processing file: {fname}")

    sha = file_sha256(path)
    file_id = str(uuid.uuid4())

    try:
        # Determine file type and read
        if fname.endswith('.xlsx'):
            df = pd.read_excel(path)
            # FIXED: Safer extraction of Location
            if len(df) > 0:
                location = df.iloc[0].get("Location")
                if location is None or pd.isna(location):
                    location = "unknown"
                location = str(location).strip()
            else:
                location = "unknown"
            
            device_id = generate_device_id(location)
            
            # Extract date range from data
            date_col = "Date"
            if date_col in df.columns:
                dates = pd.to_datetime(df[date_col], format='mixed', errors='coerce').dropna()
                if len(dates) > 0:
                    start_date = dates.min().date()
                    end_date = dates.max().date()
                else:
                    start_date = None
                    end_date = None
            else:
                start_date = None
                end_date = None

        elif fname.endswith('.csv'):
            df = pd.read_csv(path)
            # Parse device_id from CSV filename
            device_id, start_date, end_date = parse_csv_filename(fname)
            
        else:
            raise Exception(f"Unsupported file format: {fname}")
        
        dataset_type_path = detect_dataset_type(df, fname)
        mapping = load_mapping(dataset_type_path)
        dataset_type = os.path.basename(dataset_type_path).replace('.yaml', '')

        logger.info(f"Detected dataset type: {dataset_type}")
        logger.info(f"Generated/parsed device_id: {device_id}")

    except Exception as e:
        logger.error(f"Failed to read file {fname}: {e}")
        try:
            os.rename(path, f"{REJECTED_DIR}/{fname}")
        except Exception as rename_error:
            logger.error(f"Failed to move file to rejected: {rename_error}")
        return

    logger.info(f"File info - device_id: {device_id}, start_date: {start_date}, end_date: {end_date}, dataset: {dataset_type}")
    
    with get_conn() as conn:
        with conn.cursor() as cur:

            # File deduplication
            cur.execute("SELECT 1 FROM ingest_file WHERE sha256=%s", (sha,))
            if cur.fetchone():
                logger.info("File already ingested")
                try:
                    os.rename(path, f"{PROCESSED_DIR}/{fname}")
                except Exception as rename_error:
                    logger.error(f"Failed to move file to processed: {rename_error}")
                return

            # Register file with device_id in ingest_file table
            try:
                log_ingestion_legacy(cur, file_id, fname, device_id, mapping, start_date, end_date, sha)
            except Exception as e:
                logger.error(f"Failed to log ingestion: {e}")
                conn.rollback()
                return

            # Get device.id - SKIP for environmental_metrics
            device_pk_id = None
            if dataset_type in ("energy_daily", "energy_hourly"):
                cur.execute("SELECT id FROM device WHERE device_id=%s", (device_id,))
                result = cur.fetchone()
                if result:
                    device_pk_id = result[0]
                    logger.info(f"Found device with id: {device_pk_id}")
                else:
                    logger.warning(f"Device {device_id} not found in device table")

            rows = []

            for idx, r in df.iterrows():
                try:
                    canonical = transform_row_legacy(r, mapping)

                    # Skip rows based on dataset type
                    if dataset_type == "dairy_production_daily":
                        if not canonical.get("production_date"):
                            continue
                        rows.append((
                            canonical.get("production_date"),
                            canonical.get("day_production_per_cow_kg"),
                            canonical.get("number_of_animals"),
                            canonical.get("average_lactation_days"),
                            canonical.get("fed_per_cow_total_kg"),
                            canonical.get("fed_per_cow_water_kg"),
                            canonical.get("feed_efficiency"),
                            canonical.get("rumination_minutes"),
                            file_id
                        ))
                        
                    elif dataset_type == "energy_hourly" or dataset_type == "energy_daily":
                        if not canonical.get("ts"):
                            continue
                        rows.append((
                            device_pk_id,
                            canonical.get("ts"),
                            canonical.get("energy_kwh"),
                            file_id
                        ))

                    elif dataset_type == "environmental_metrics":
                        if not canonical.get("timestamp"):
                            continue
                        rows.append((
                            canonical.get("timestamp"),
                            canonical.get("atm_pressure"),
                            canonical.get("noise_level_db"),
                            canonical.get("temperature"),
                            canonical.get("humidity"),
                            canonical.get("pm10"),
                            canonical.get("wind_speed"),
                            canonical.get("wind_direction_sectors"),
                            canonical.get("wind_angle"),
                            canonical.get("pm2p5"),
                            file_id
                        ))
                
                except Exception as row_error:
                    logger.warning(f"Error processing row {idx}: {row_error}")
                    continue

            # Insert into appropriate table
            try:
                if dataset_type == "dairy_production_daily" and rows:
                    insert_dairy_production_batch(cur, rows, file_id)
                    
                elif dataset_type == "energy_hourly" and rows:
                    insert_energy_hourly_batch(cur, rows)
                    
                elif dataset_type == "energy_daily" and rows:
                    insert_energy_daily_batch(cur, rows)

                elif dataset_type == "environmental_metrics" and rows:
                    insert_environmental_metrics_batch(cur, rows)

                conn.commit()
                logger.info(f"Inserted {len(rows)} rows from {fname}")
                
            except Exception as insert_error:
                logger.error(f"Failed to insert rows: {insert_error}")
                conn.rollback()
                return

    try:
        os.rename(path, f"{PROCESSED_DIR}/{fname}")
    except Exception as rename_error:
        logger.error(f"Failed to move file to processed: {rename_error}")
    
    logger.info(f"Finished processing {fname}")

# ============================================================================
# API Fetching Tasks
# ============================================================================

def fetch_airbeld_readings():
    """
    Fetch previous day's hourly readings from Airbeld API.
    Runs daily at 00:01.
    """
    try:
        from api_fetcher import AirbeldFetcher
        
        db_dsn = os.environ.get('DB_DSN')
        api_token = os.environ.get('AIRBELD_API_TOKEN')
        device_id = int(os.environ.get('AIRBELD_DEVICE_ID', '1236'))
        
        fetcher = AirbeldFetcher(db_dsn, api_token)
        yesterday = datetime.now().date() - timedelta(days=1)
        
        logger.info(f"Fetching Airbeld hourly readings for device {device_id} on {yesterday}")
        
        readings = fetcher.fetch_readings(
            device_id=device_id,
            start_date=yesterday,
            end_date=yesterday,
            period='hour'
        )
        
        source_ref = f"airbeld-api-{device_id}-{yesterday.isoformat()}"
        fetcher.save_readings(readings, source_ref)
        
        logger.info(f"Successfully fetched and saved {len(readings)} Airbeld readings")
        
    except Exception as e:
        logger.error(f"Failed to fetch Airbeld readings: {str(e)}", exc_info=True)


def fetch_airbeld_devices():
    """
    Fetch and update Airbeld devices list.
    Runs daily at 00:05.
    """
    try:
        from api_fetcher import AirbeldFetcher
        
        db_dsn = os.environ.get('DB_DSN')
        api_token = os.environ.get('AIRBELD_API_TOKEN')
        
        fetcher = AirbeldFetcher(db_dsn, api_token)
        
        logger.info("Fetching Airbeld devices")
        devices = fetcher.fetch_devices()
        fetcher.save_devices(devices)
        
        logger.info(f"Successfully fetched and saved {len(devices)} Airbeld devices")
        
    except Exception as e:
        logger.error(f"Failed to fetch Airbeld devices: {str(e)}", exc_info=True)


def setup_api_scheduler():
    """
    Setup background scheduler for API fetching tasks.
    Returns scheduler instance that runs in background thread.
    """
    scheduler = BackgroundScheduler()
    
    # Schedule Airbeld hourly readings fetch - daily at 00:01
    scheduler.add_job(
        fetch_airbeld_readings,
        trigger=CronTrigger(hour=0, minute=1),
        id='airbeld_hourly_readings',
        name='Fetch Airbeld hourly readings',
        replace_existing=True
    )
    
    # Schedule Airbeld devices update - daily at 00:05
    scheduler.add_job(
        fetch_airbeld_devices,
        trigger=CronTrigger(hour=0, minute=5),
        id='airbeld_devices_update',
        name='Update Airbeld devices',
        replace_existing=True
    )
    
    # Add more API fetching jobs here as needed
    # Example:
    # scheduler.add_job(
    #     fetch_energy_api_data,
    #     trigger=CronTrigger(hour=1, minute=0),
    #     id='energy_api_fetch'
    # )
    
    return scheduler

# ============================================================================
# Main Event Loop
# ============================================================================

def main():
    """
    Main application entry point.
    Runs file watching loop and API fetching scheduler in parallel.
    """
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(REJECTED_DIR, exist_ok=True)

    logger.info("="*60)
    logger.info("Starting CEI-InOE Ingestor")
    logger.info(f"Pipeline Mode: {'NEW (Phase 1)' if PIPELINE_ENABLED else 'LEGACY'}")
    logger.info(f"File watcher: {WATCH_DIR}")
    logger.info("="*60)
    
    # Start API fetching scheduler in background
    try:
        scheduler = setup_api_scheduler()
        scheduler.start()
        logger.info("API Scheduler started")
        logger.info("Scheduled jobs:")
        for job in scheduler.get_jobs():
            logger.info(f"  - {job.name} (ID: {job.id})")
    except Exception as e:
        logger.warning(f"Failed to start API scheduler: {e}")
        logger.info("Continuing with file watching only...")
    
    logger.info("="*60)
    
    # File watching loop (main thread)
    while True:
        try:
            for f in os.listdir(WATCH_DIR):
                path = f"{WATCH_DIR}/{f}"
                if (f.endswith(".xlsx") or f.endswith(".csv")) and is_stable(path):
                    try:
                        # Use new pipeline if available, otherwise fallback to legacy
                        if PIPELINE_ENABLED:
                            process_file_pipeline(path)
                        else:
                            process_file(path)
                    except Exception as e:
                        logger.exception(f"Error processing {f}: {e}")
                        os.rename(path, f"{REJECTED_DIR}/{f}")
        except Exception as e:
            logger.exception(f"Error in main loop: {e}")
        
        time.sleep(POLL_INTERVAL)


def process_file_pipeline(path: str):
    """
    Process file using new pipeline (Phase 1 refactoring).
    Auto-detects dataset type and routes to appropriate pipeline.
    """
    fname = os.path.basename(path)
    logger.info(f"[PIPELINE] Processing {fname}")
    
    # Detect dataset type from filename patterns
    fname_lower = fname.lower()
    
    if 'hourly' in fname_lower:
        result = ingest_energy_hourly_pipeline(path)
    elif 'daily' in fname_lower:
        result = ingest_energy_daily_pipeline(path)
    elif 'environmental' in fname_lower or 'metrics' in fname_lower or 'airbeld' in fname_lower:
        result = ingest_environmental_metrics_pipeline(path)
    elif any(keyword in fname_lower for keyword in ['dairy', 'feeding', 'feedtype', 'milk', 'production']):
        result = ingest_dairy_production_pipeline(path)
    else:
        # Try to detect from file content (fallback to legacy)
        logger.warning(f"Cannot determine dataset type from filename: {fname}, using legacy detection")
        process_file(path)
        return
    
    # Handle result
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