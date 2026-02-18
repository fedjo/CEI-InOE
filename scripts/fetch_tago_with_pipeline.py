#!/usr/bin/env python3
"""
Tago.io Energy Data Fetcher with Full Pipeline
===============================================
Fetches hourly and daily energy consumption data from Tago.io API
and processes it through the full ingestor pipeline, filling all
database tables:
  - ingest_file: File/API fetch metadata
  - staging_energy_hourly/daily: Staging data
  - fact_energy_hourly/daily: Final energy data
  - pipeline_execution: Pipeline stage tracking
  - data_quality_checks: Validation results
  - api_fetch_cursor: Cursor for incremental fetches

Usage:
    # Fetch hourly data for specific devices
    python fetch_tago_with_pipeline.py --devices 68f1fb6550bcae000b4c2501 \
                                       --start-date 2025-01-01 --end-date 2025-01-31

    # Fetch data for ALL devices from database
    python fetch_tago_with_pipeline.py --all-devices --start-date 2025-01-01 --end-date 2025-01-31

    # Fetch both hourly and daily data
    python fetch_tago_with_pipeline.py --all-devices --start-date 2025-01-01 --end-date 2025-01-31 \
                                       --granularity both

    # Dry run (fetch but don't save)
    python fetch_tago_with_pipeline.py --all-devices --start-date 2025-01-01 --end-date 2025-01-31 --dry-run
"""

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import requests
from dotenv import load_dotenv

# Setup paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'ingestor', 'app'))

# Load environment
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# Import pipeline components after path setup
from connectors.base import InputEnvelope
from pipeline_runner import PipelineRunner, DuplicateInputError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

TAGO_API_URL = os.environ.get('TAGO_API_URL', 'https://api.tago.io')
DB_DSN = os.environ.get('DB_DSN', '')
MAPPINGS_DIR = os.path.join(PROJECT_ROOT, 'ingestor', 'app', 'mappings')

# Variable name mapping
VARIABLE_MAP = {
    'hourly': 'hourly_consumption',
    'daily': 'diffday',
}

MAPPING_MAP = {
    'hourly': 'api_energy_hourly.yaml',
    'daily': 'api_energy_daily.yaml',
}


# =============================================================================
# Database Functions
# =============================================================================

def get_db_connection():
    """Get database connection."""
    if not DB_DSN:
        raise ValueError("DB_DSN environment variable not set")
    return psycopg2.connect(DB_DSN)


def get_all_energy_devices(conn) -> List[Dict[str, Any]]:
    """Get all energy devices with tokens from database."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                device_id,
                alias,
                client,
                metadata->>'device_token' as device_token,
                metadata
            FROM generic_device
            WHERE device_type = 'energy'
            AND status = 'online'
            AND metadata->>'device_token' IS NOT NULL
        """)
        rows = cur.fetchall()
    
    return [
        {
            'device_id': row[0],
            'alias': row[1],
            'client': row[2],
            'device_token': row[3],
            'metadata': row[4],
        }
        for row in rows
    ]


def get_device_by_id(conn, device_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific device by device_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                device_id,
                alias,
                client,
                metadata->>'device_token' as device_token,
                metadata
            FROM generic_device
            WHERE device_id = %s
            AND device_type = 'energy'
        """, (device_id,))
        row = cur.fetchone()
    
    if not row:
        return None
    
    return {
        'device_id': row[0],
        'alias': row[1],
        'client': row[2],
        'device_token': row[3],
        'metadata': row[4],
    }


def update_cursor(conn, device_id: str, endpoint_id: str, last_timestamp: datetime):
    """Update the API fetch cursor after successful pipeline run."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO api_fetch_cursor 
                (connector_id, endpoint_id, device_id, last_fetch_timestamp, fetch_count)
            VALUES ('tago_energy', %s, %s, %s, 1)
            ON CONFLICT (connector_id, endpoint_id, device_id)
            DO UPDATE SET
                last_fetch_timestamp = EXCLUDED.last_fetch_timestamp,
                last_fetch_success = NOW(),
                fetch_count = api_fetch_cursor.fetch_count + 1,
                updated_at = NOW()
        """, (endpoint_id, device_id, last_timestamp))
    conn.commit()


# =============================================================================
# API Functions
# =============================================================================

def fetch_tago_data(
    device_token: str,
    variable: str,
    start_date: datetime,
    end_date: datetime,
    qty: int = 10000
) -> List[Dict]:
    """
    Fetch data from Tago.io API.
    
    Returns:
        List of raw records from API
    """
    url = f"{TAGO_API_URL}/data"
    headers = {
        'device-token': device_token,
        'Content-Type': 'application/json',
    }
    params = {
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'qty': qty,
        'variables': variable,
    }
    
    logger.debug(f"Fetching from Tago: {url} with params {params}")
    
    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    
    if not data.get('status'):
        logger.warning(f"Tago API returned status=false: {data}")
        return []
    
    return data.get('result', [])


def transform_tago_records(raw_records: List[Dict]) -> List[Dict]:
    """
    Transform Tago.io API response to pipeline-ready records.
    
    Input from API:
        [{"id": "...", "time": "2025-12-08T23:00:00.000Z", "value": 3.24, ...}, ...]
    
    Output:
        [{"ts": "2025-12-08T23:00:00.000Z", "energy_kwh": 3.24}, ...]
    """
    transformed = []
    
    for record in raw_records:
        ts = record.get('time')
        value = record.get('value')
        
        if ts is None or value is None:
            continue
        
        transformed.append({
            'ts': ts,
            'energy_kwh': float(value),
        })
    
    # Sort by timestamp (oldest first)
    transformed.sort(key=lambda r: r['ts'])
    
    return transformed


def compute_sha256(device_id: str, records: List[Dict], start_date: datetime, end_date: datetime) -> str:
    """Compute SHA256 hash for deduplication."""
    content = f"{device_id}:{len(records)}:{start_date.isoformat()}:{end_date.isoformat()}"
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


# =============================================================================
# Pipeline Processing
# =============================================================================

def process_with_pipeline(
    device: Dict[str, Any],
    records: List[Dict],
    granularity: str,
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Process records through the full pipeline.
    
    Returns:
        Dictionary with pipeline execution results
    """
    device_id = device['device_id']
    variable = VARIABLE_MAP.get(granularity, 'hourly_consumption')
    endpoint_id = f"{device_id}:{variable}"
    mapping_file = os.path.join(MAPPINGS_DIR, MAPPING_MAP.get(granularity, 'api_energy_hourly.yaml'))
    
    if dry_run:
        logger.info(f"[DRY RUN] Would process {len(records)} records through pipeline")
        return {
            'device_id': device_id,
            'records': len(records),
            'loaded': 0,
            'dry_run': True,
        }
    
    # Compute SHA256 for deduplication
    sha256 = compute_sha256(device_id, records, start_date, end_date)
    
    # Build InputEnvelope for pipeline
    envelope = InputEnvelope(
        connector_id='tago_energy_script',
        input_id=f"tago_{device_id}_{granularity}_{start_date.date()}_{end_date.date()}",
        source_uri=f"tago://api.tago.io/data/{variable}",
        content=records,
        content_type='json',
        hint_mapping=mapping_file,
        hint_device_id=device_id,
        hint_granularity=granularity,
        metadata={
            'source_type': 'api',
            'file_name': f"tago_{device_id}_{granularity}_{start_date.date()}_to_{end_date.date()}",
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'sha256': sha256,
            'device_token': device.get('device_token'),
            'variable': variable,
        }
    )
    
    # Run through pipeline
    try:
        runner = PipelineRunner(DB_DSN)
        metrics = runner.run(envelope)
        
        # Update cursor after successful pipeline run
        if records:
            last_ts = records[-1]['ts']
            if isinstance(last_ts, str):
                last_ts = datetime.fromisoformat(last_ts.replace('Z', '+00:00'))
            
            conn = get_db_connection()
            # try:
            #     update_cursor(conn, device_id, endpoint_id, last_ts)
            # finally:
            #     conn.close()
        
        return {
            'device_id': device_id,
            'alias': device.get('alias'),
            'granularity': granularity,
            'records': metrics.extract_records,
            'valid': metrics.valid_records,
            'invalid': metrics.invalid_records,
            'loaded': metrics.load_records,
            'skipped': metrics.skipped_records,
            'duration': metrics.total_duration,
        }
        
    except DuplicateInputError:
        logger.info(f"Data already processed for {device_id} (duplicate)")
        return {
            'device_id': device_id,
            'alias': device.get('alias'),
            'granularity': granularity,
            'records': len(records),
            'loaded': 0,
            'skipped': 'duplicate',
        }
    except Exception as e:
        logger.error(f"Pipeline error for {device_id}: {e}", exc_info=True)
        return {
            'device_id': device_id,
            'alias': device.get('alias'),
            'granularity': granularity,
            'error': str(e),
            'records': len(records),
            'loaded': 0,
        }


# =============================================================================
# Main Fetch Logic
# =============================================================================

def fetch_device_data(
    device: Dict[str, Any],
    start_date: datetime,
    end_date: datetime,
    granularity: str,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Fetch and process data for a single device through the pipeline.
    
    Returns:
        Dictionary with fetch and pipeline statistics
    """
    device_id = device['device_id']
    device_token = device.get('device_token')
    
    if not device_token:
        return {'device_id': device_id, 'error': 'No device token', 'records': 0, 'loaded': 0}
    
    variable = VARIABLE_MAP.get(granularity)
    if not variable:
        return {'device_id': device_id, 'error': f'Invalid granularity: {granularity}', 'records': 0, 'loaded': 0}
    
    logger.info(f"Fetching {granularity} data for {device_id} ({device.get('alias', 'N/A')}) "
                f"from {start_date.date()} to {end_date.date()}")
    
    try:
        # Fetch from API
        raw_records = fetch_tago_data(device_token, variable, start_date, end_date)
        logger.info(f"  -> Received {len(raw_records)} raw records")
        
        if not raw_records:
            return {'device_id': device_id, 'alias': device.get('alias'), 
                    'granularity': granularity, 'records': 0, 'loaded': 0}
        
        # Transform records
        records = transform_tago_records(raw_records)
        logger.info(f"  -> Transformed to {len(records)} records")
        
        # Process through pipeline
        result = process_with_pipeline(device, records, granularity, start_date, end_date, dry_run)
        logger.info(f"  -> Pipeline: {result.get('loaded', 0)} loaded, {result.get('invalid', 0)} invalid")
        
        return result
        
    except requests.RequestException as e:
        logger.error(f"API error for {device_id}: {e}")
        return {'device_id': device_id, 'alias': device.get('alias'), 
                'granularity': granularity, 'error': str(e), 'records': 0, 'loaded': 0}
    except Exception as e:
        logger.error(f"Error processing {device_id}: {e}", exc_info=True)
        return {'device_id': device_id, 'alias': device.get('alias'),
                'granularity': granularity, 'error': str(e), 'records': 0, 'loaded': 0}


def main():
    parser = argparse.ArgumentParser(
        description='Fetch energy data from Tago.io API and process through full pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Device selection
    device_group = parser.add_mutually_exclusive_group(required=True)
    device_group.add_argument(
        '--devices', '-d',
        nargs='+',
        help='Device IDs to fetch (space-separated)'
    )
    device_group.add_argument(
        '--all-devices', '-a',
        action='store_true',
        help='Fetch data for all energy devices from database'
    )
    
    # Date range
    parser.add_argument(
        '--start-date', '-s',
        required=True,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date', '-e',
        required=True,
        help='End date (YYYY-MM-DD)'
    )
    
    # Options
    parser.add_argument(
        '--granularity', '-g',
        choices=['hourly', 'daily', 'both'],
        default='hourly',
        help='Data granularity (default: hourly)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Fetch data but do not save to database'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)
    
    if start_date > end_date:
        logger.error("Start date must be before end date")
        sys.exit(1)
    
    # Connect to database
    try:
        conn = get_db_connection()
        logger.info("Connected to database")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)
    
    # Get devices
    devices = []
    if args.all_devices:
        devices = get_all_energy_devices(conn)
        logger.info(f"Found {len(devices)} energy devices in database")
    else:
        for device_id in args.devices:
            device = get_device_by_id(conn, device_id)
            if device:
                devices.append(device)
            else:
                logger.warning(f"Device not found: {device_id}")
    
    conn.close()
    
    if not devices:
        logger.error("No valid devices to process")
        sys.exit(1)
    
    # Determine granularities to fetch
    granularities = ['hourly', 'daily'] if args.granularity == 'both' else [args.granularity]
    
    # Fetch data for each device and granularity
    results = []
    for granularity in granularities:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {granularity.upper()} data through pipeline")
        logger.info(f"{'='*60}")
        
        for device in devices:
            result = fetch_device_data(device, start_date, end_date, granularity, args.dry_run)
            results.append(result)
    
    # Summary
    print("\n" + "="*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Tables populated: ingest_file, staging_energy_*, fact_energy_*, pipeline_execution, data_quality_checks, api_fetch_cursor")
    print("-"*60)
    
    total_records = 0
    total_loaded = 0
    total_invalid = 0
    errors = 0
    
    for r in results:
        status = "✓" if 'error' not in r else "✗"
        records = r.get('records', 0)
        loaded = r.get('loaded', 0)
        invalid = r.get('invalid', 0)
        skipped = r.get('skipped', 0)
        error_msg = r.get('error', '')
        
        print(f"{status} {r.get('device_id', 'N/A')} ({r.get('granularity', 'N/A')}): "
              f"{records} records -> {loaded} loaded, {invalid} invalid, {skipped} skipped"
              f"{' - ' + error_msg if error_msg else ''}")
        
        total_records += records
        total_loaded += loaded
        total_invalid += invalid
        if 'error' in r:
            errors += 1
    
    print("-"*60)
    print(f"Total: {total_records} records fetched, {total_loaded} loaded, {total_invalid} invalid, {errors} errors")
    
    if args.dry_run:
        print("\n[DRY RUN - No data was saved to database]")


if __name__ == '__main__':
    main()
