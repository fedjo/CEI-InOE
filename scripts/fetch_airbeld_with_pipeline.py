#!/usr/bin/env python3
"""
Airbeld Environmental Data Fetcher with Full Pipeline
======================================================
Fetches hourly and daily environmental/weather data from Airbeld API
and processes it through the full ingestor pipeline, filling all
database tables:
  - ingest_file: File/API fetch metadata
  - staging_environmental_metrics: Staging data
  - environmental_metrics: Final environmental data
  - pipeline_execution: Pipeline stage tracking
  - data_quality_checks: Validation results
  - api_fetch_cursor: Cursor for incremental fetches

Usage:
    # Fetch hourly data for specific devices
    python fetch_airbeld_with_pipeline.py --devices device_001 device_002 \
                                          --start-date 2025-01-01 --end-date 2025-01-31

    # Fetch data for ALL weather devices from database
    python fetch_airbeld_with_pipeline.py --all-devices --start-date 2025-01-01 --end-date 2025-01-31

    # Fetch daily aggregated data
    python fetch_airbeld_with_pipeline.py --all-devices --start-date 2025-01-01 --end-date 2025-01-31 \
                                          --granularity daily

    # Fetch both hourly and daily
    python fetch_airbeld_with_pipeline.py --all-devices --start-date 2025-01-01 --end-date 2025-01-31 \
                                          --granularity both

    # Dry run (fetch but don't save)
    python fetch_airbeld_with_pipeline.py --all-devices --start-date 2025-01-01 --end-date 2025-01-31 --dry-run
"""

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
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

AIRBELD_API_URL = os.environ.get('AIRBELD_API_URL', 'https://api.airbeld.com/api/v1')
AIRBELD_EMAIL = os.environ.get('AIRBELD_EMAIL', '')
AIRBELD_PASSWORD = os.environ.get('AIRBELD_PASSWORD', '')
DB_DSN = os.environ.get('DB_DSN', '')
MAPPINGS_DIR = os.path.join(PROJECT_ROOT, 'ingestor', 'app', 'mappings')

# Sensor name mapping: API name → DB column
SENSOR_MAP = {
    'atm_pressure': 'atm_pressure',
    'noise_level_db': 'noise_level_db',
    'temperature': 'temperature',
    'humidity': 'humidity',
    'pm10': 'pm10',
    'wind_speed': 'wind_speed',
    'wind_direction': 'wind_direction_sectors',
    'wind_angle': 'wind_angle',
    'pm2p5': 'pm2p5',
}


# =============================================================================
# Database Functions
# =============================================================================

def get_db_connection():
    """Get database connection."""
    print(DB_DSN)
    if not DB_DSN:
        raise ValueError("DB_DSN environment variable not set")
    return psycopg2.connect(DB_DSN)


def get_all_weather_devices(conn) -> List[Dict[str, Any]]:
    """Get all weather devices from database."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                device_id,
                alias,
                client,
                metadata->>'external_id' as external_id,
                metadata
            FROM generic_device
            WHERE device_type = 'weather'
            AND status = 'online'
        """)
        rows = cur.fetchall()
    
    return [
        {
            'device_id': row[0],
            'alias': row[1],
            'client': row[2],
            'external_id': row[3],
            'metadata': row[4],
        }
        for row in rows
    ]


def get_device_by_id(conn, device_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific weather device by device_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                device_id,
                alias,
                client,
                metadata->>'external_id' as external_id,
                metadata
            FROM generic_device
            WHERE device_id = %s
            AND device_type = 'weather'
        """, (device_id,))
        row = cur.fetchone()
    
    if not row:
        return None
    
    return {
        'device_id': row[0],
        'alias': row[1],
        'client': row[2],
        'external_id': row[3],
        'metadata': row[4],
    }


def update_cursor(conn, device_id: str, endpoint_id: str, last_timestamp: datetime):
    """Update the API fetch cursor after successful pipeline run."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO api_fetch_cursor 
                (connector_id, endpoint_id, device_id, last_fetch_timestamp, fetch_count)
            VALUES ('airbeld_environmental', %s, %s, %s, 1)
            ON CONFLICT (connector_id, endpoint_id, device_id)
            DO UPDATE SET
                last_fetch_timestamp = EXCLUDED.last_fetch_timestamp,
                last_fetch_success = NOW(),
                fetch_count = api_fetch_cursor.fetch_count + 1,
                updated_at = NOW()
        """, (endpoint_id, device_id, last_timestamp))
    conn.commit()


# =============================================================================
# API Client
# =============================================================================

class AirbeldClient:
    """Client for Airbeld API with token management."""
    
    def __init__(self, base_url: str, email: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.email = email
        self.password = password
        self._session = requests.Session()
        self._access_token = None
        self._token_expires = None
    
    def _ensure_authenticated(self):
        """Ensure we have a valid access token."""
        now = datetime.now()
        
        if self._access_token and self._token_expires and now < self._token_expires:
            return
        
        logger.info("Authenticating with Airbeld API...")
        
        response = self._session.post(
            f"{self.base_url}/auth/token/",
            json={
                'email': self.email,
                'password': self.password,
            },
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        self._access_token = data.get('accessToken') or data.get('access_token')
        self._token_expires = now + timedelta(hours=1)
        
        logger.info("Authentication successful")
    
    def fetch_device_data(
        self,
        external_id: str,
        start_date: datetime,
        end_date: datetime,
        period: str = 'hour'
    ) -> Dict[str, Any]:
        """
        Fetch environmental data for a device.
        
        Args:
            external_id: Airbeld device ID
            start_date: Start date
            end_date: End date
            period: 'hour' or 'day'
        
        Returns:
            API response with sensor data
        """
        self._ensure_authenticated()
        
        url = f"{self.base_url}/devices/{external_id}/readings_by_date/"
        headers = {
            'Authorization': f"Bearer {self._access_token}",
        }
        params = {
            'start-date': start_date.strftime('%Y-%m-%d'),
            'end-date': end_date.strftime('%Y-%m-%d'),
            'period': period,
        }
        
        logger.debug(f"Fetching from Airbeld: {url} with params {params}")
        
        response = self._session.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        
        return response.json()


# =============================================================================
# Data Transformation
# =============================================================================

def transform_airbeld_data(api_response: Dict[str, Any]) -> List[Dict]:
    """
    Transform column-oriented sensor data to row-oriented records.
    
    Input:
        {'sensors': {'atm_pressure': {'values': [{'timestamp': '...', 'value': 99.5}, ...]}, ...}}
    
    Output:
        [{'timestamp': '...', 'atm_pressure': 99.5, 'temperature': 22.5, ...}, ...]
    """
    sensors = api_response.get('sensors', {})
    
    if not sensors:
        return []
    
    # Collect all values by timestamp
    by_timestamp: Dict[str, Dict[str, Any]] = {}
    
    for sensor_name, sensor_data in sensors.items():
        # Map API sensor name to our internal name
        db_column = SENSOR_MAP.get(sensor_name)
        if not db_column:
            logger.debug(f"Unknown sensor: {sensor_name}")
            continue
        
        values = sensor_data.get('values', [])
        for item in values:
            ts = item.get('timestamp')
            value = item.get('value')
            
            if ts and value is not None:
                if ts not in by_timestamp:
                    by_timestamp[ts] = {'timestamp': ts}
                by_timestamp[ts][sensor_name] = value
    
    # Convert to list, sorted by timestamp
    result = list(by_timestamp.values())
    result.sort(key=lambda r: r['timestamp'])
    
    return result


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
    endpoint_id = f"{device_id}:{granularity}"
    mapping_file = os.path.join(MAPPINGS_DIR, 'api_environmental_metrics.yaml')
    
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
        connector_id='airbeld_environmental_script',
        input_id=f"airbeld_{device_id}_{granularity}_{start_date.date()}_{end_date.date()}",
        source_uri=f"airbeld://api.airbeld.com/devices/{device.get('external_id')}/readings",
        content=records,
        content_type='json',
        hint_mapping=mapping_file,
        hint_device_id=device_id,
        hint_granularity=granularity,
        metadata={
            'source_type': 'api',
            'file_name': f"airbeld_{device_id}_{granularity}_{start_date.date()}_to_{end_date.date()}",
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'sha256': sha256,
            'external_id': device.get('external_id'),
        }
    )
    
    # Run through pipeline
    try:
        runner = PipelineRunner(DB_DSN)
        metrics = runner.run(envelope)
        
        # Update cursor after successful pipeline run
        if records:
            last_ts = records[-1]['timestamp']
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
            'external_id': device.get('external_id'),
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
            'external_id': device.get('external_id'),
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
            'external_id': device.get('external_id'),
            'granularity': granularity,
            'error': str(e),
            'records': len(records),
            'loaded': 0,
        }


# =============================================================================
# Main Fetch Logic
# =============================================================================

def fetch_device_data(
    client: AirbeldClient,
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
    external_id = device.get('external_id')
    
    if not external_id:
        return {'device_id': device_id, 'error': 'No external_id', 'records': 0, 'loaded': 0}
    
    period = 'hour' if granularity == 'hourly' else 'day'
    
    logger.info(f"Fetching {granularity} data for {device_id} ({device.get('alias', 'N/A')}) "
                f"from {start_date.date()} to {end_date.date()}")
    
    try:
        # Fetch from API
        api_response = client.fetch_device_data(external_id, start_date, end_date, period)
        
        # Transform records
        records = transform_airbeld_data(api_response)
        logger.info(f"  -> Transformed to {len(records)} records")
        
        if not records:
            return {'device_id': device_id, 'alias': device.get('alias'),
                    'external_id': external_id, 'granularity': granularity,
                    'records': 0, 'loaded': 0}
        
        # Process through pipeline
        result = process_with_pipeline(device, records, granularity, start_date, end_date, dry_run)
        logger.info(f"  -> Pipeline: {result.get('loaded', 0)} loaded, {result.get('invalid', 0)} invalid")
        
        return result
        
    except requests.RequestException as e:
        logger.error(f"API error for {device_id}: {e}")
        return {'device_id': device_id, 'alias': device.get('alias'),
                'external_id': external_id, 'granularity': granularity,
                'error': str(e), 'records': 0, 'loaded': 0}
    except Exception as e:
        logger.error(f"Error processing {device_id}: {e}", exc_info=True)
        return {'device_id': device_id, 'alias': device.get('alias'),
                'external_id': external_id, 'granularity': granularity,
                'error': str(e), 'records': 0, 'loaded': 0}


def main():
    parser = argparse.ArgumentParser(
        description='Fetch environmental data from Airbeld API and process through full pipeline',
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
        help='Fetch data for all weather devices from database'
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
    
    # Check Airbeld credentials
    if not AIRBELD_EMAIL or not AIRBELD_PASSWORD:
        logger.error("AIRBELD_EMAIL and AIRBELD_PASSWORD environment variables required")
        sys.exit(1)
    
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
        devices = get_all_weather_devices(conn)
        logger.info(f"Found {len(devices)} weather devices in database")
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
    
    # Initialize API client
    client = AirbeldClient(AIRBELD_API_URL, AIRBELD_EMAIL, AIRBELD_PASSWORD)
    
    # Determine granularities to fetch
    granularities = ['hourly', 'daily'] if args.granularity == 'both' else [args.granularity]
    
    # Fetch data for each device and granularity
    results = []
    for granularity in granularities:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {granularity.upper()} data through pipeline")
        logger.info(f"{'='*60}")
        
        for device in devices:
            result = fetch_device_data(client, device, start_date, end_date, granularity, args.dry_run)
            results.append(result)
    
    # Summary
    print("\n" + "="*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Tables populated: ingest_file, staging_environmental_metrics, environmental_metrics, pipeline_execution, data_quality_checks, api_fetch_cursor")
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
