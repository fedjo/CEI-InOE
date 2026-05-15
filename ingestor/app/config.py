"""
CEI-InOE Ingestor - Configuration
Centralized application configuration loaded from environment variables.
"""

import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

# Load environment variables from .env file
# Look for .env in the project root (two levels up from this file)
_env_path = Path(__file__).resolve().parent.parent.parent / '.env'
load_dotenv(_env_path)


# ============================================================================
# Database Configuration
# ============================================================================

DB_DSN = os.environ.get('DB_DSN', '')


# ============================================================================
# Worker Configuration
# ============================================================================

NUM_WORKERS = int(os.environ.get('NUM_WORKERS', '2'))
QUEUE_MAX_SIZE = int(os.environ.get('QUEUE_MAX_SIZE', '100'))


# ============================================================================
# Logging Configuration
# ============================================================================

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


# ============================================================================
# File Connector Configuration
# ============================================================================

WATCH_DIR = os.environ.get('WATCH_DIR', '/data/incoming')
PROCESSED_DIR = os.environ.get('PROCESSED_DIR', '/data/processed')
REJECTED_DIR = os.environ.get('REJECTED_DIR', '/data/rejected')
MAPPINGS_DIR = os.environ.get('MAPPINGS_DIR', '/app/mappings')
FILE_POLL_INTERVAL = int(os.environ.get('FILE_POLL_INTERVAL', '5'))
FILE_STABLE_SECONDS = int(os.environ.get('FILE_STABLE_SECONDS', '3'))


# ============================================================================
# HTTP Connector Configuration (Example)
# ============================================================================

ENERGY_API_URL = os.environ.get('ENERGY_API_URL', 'https://api.example.com')
ENERGY_API_KEY = os.environ.get('ENERGY_API_KEY', '')

# ============================================================================
# Airbeld API Configuration
# ============================================================================

AIRBELD_API_URL = os.environ.get('AIRBELD_API_URL', '')
AIRBELD_EMAIL = os.environ.get('AIRBELD_EMAIL', '')
AIRBELD_PASSWORD = os.environ.get('AIRBELD_PASSWORD', '')
AIRBELD_POLL_INTERVAL = int(os.environ.get('AIRBELD_POLL_INTERVAL', '43200'))  # (43200) 12-hours in seconds
AIRBELD_LOOKBACK_DAYS = int(os.environ.get('AIRBELD_LOOKBACK_DAYS', '7'))


# ============================================================================
# Tago.io Energy API Configuration
# ============================================================================

TAGO_API_URL = os.environ.get('TAGO_API_URL', 'https://api.tago.io')
TAGO_POLL_INTERVAL = int(os.environ.get('TAGO_POLL_INTERVAL', '3600'))  # 1-hour in seconds
TAGO_LOOKBACK_DAYS = int(os.environ.get('TAGO_LOOKBACK_DAYS', '7'))
TAGO_ENABLED = os.environ.get('TAGO_ENABLED', 'true').lower() == 'true'


# ============================================================================
# FusionSolar API Configuration
# ============================================================================

FUSIONSOLAR_API_URL = os.environ.get('FUSIONSOLAR_API_URL', 'https://intl.fusionsolar.huawei.com/thirdData')
FUSIONSOLAR_USER = os.environ.get('FUSIONSOLAR_USER', '')
FUSIONSOLAR_SYSTEM_CODE = os.environ.get('FUSIONSOLAR_SYSTEM_CODE', '')
FUSIONSOLAR_POLL_INTERVAL = int(os.environ.get('FUSIONSOLAR_POLL_INTERVAL', '3600'))
FUSIONSOLAR_LOOKBACK_DAYS = int(os.environ.get('FUSIONSOLAR_LOOKBACK_DAYS', '30'))
FUSIONSOLAR_ENABLED = os.environ.get('FUSIONSOLAR_ENABLED', 'true').lower() == 'true'


# ============================================================================
# Open-Meteo Forecast API Configuration
# ============================================================================

# No API key required for non-commercial use (<10,000 calls/day).
# Coordinates are resolved from the site table at startup; these env vars
# are only used as a fallback when the DB has no site row yet.
OPEN_METEO_LATITUDE = os.environ.get('OPEN_METEO_LATITUDE', '')
OPEN_METEO_LONGITUDE = os.environ.get('OPEN_METEO_LONGITUDE', '')
OPEN_METEO_PANEL_TILT = float(os.environ.get('OPEN_METEO_PANEL_TILT', '30'))
OPEN_METEO_PANEL_AZIMUTH = float(os.environ.get('OPEN_METEO_PANEL_AZIMUTH', '0'))
OPEN_METEO_FORECAST_DAYS = int(os.environ.get('OPEN_METEO_FORECAST_DAYS', '7'))
OPEN_METEO_POLL_INTERVAL = int(os.environ.get('OPEN_METEO_POLL_INTERVAL', '3600'))  # 1 hour
OPEN_METEO_MODEL = os.environ.get('OPEN_METEO_MODEL', 'best_match')
OPEN_METEO_ENABLED = os.environ.get('OPEN_METEO_ENABLED', 'true').lower() == 'true'


# ============================================================================
# Site Configuration
# ============================================================================

# Directory containing site_config.yaml and datasources.yaml
CONF_DIR = os.environ.get('CONF_DIR', '/app/conf')


# ============================================================================
# Connector Configurations
# ============================================================================

CONNECTOR_CONFIGS: Dict[str, Dict[str, Any]] = {
    'file_watcher': {
        'type': 'file',
        'watch_dir': WATCH_DIR,
        'processed_dir': PROCESSED_DIR,
        'rejected_dir': REJECTED_DIR,
        'mappings_dir': MAPPINGS_DIR,
        'schedule_seconds': FILE_POLL_INTERVAL,
        'stable_seconds': FILE_STABLE_SECONDS,
    },

    # Airbeld Environmental API
    'airbeld_environmental': {
        'type': 'airbeld',
        'base_url': AIRBELD_API_URL,
        'email': AIRBELD_EMAIL,
        'password': AIRBELD_PASSWORD,
        'schedule_seconds': AIRBELD_POLL_INTERVAL,
        'lookback_days': AIRBELD_LOOKBACK_DAYS,
        'timeout': 30,
        'mappings_dir': MAPPINGS_DIR,
        'enabled': bool(AIRBELD_EMAIL and AIRBELD_PASSWORD),
    },

    # Tago.io Energy API (hourly consumption)
    'tago_energy': {
        'type': 'tago',
        'base_url': TAGO_API_URL,
        'schedule_seconds': TAGO_POLL_INTERVAL,
        'lookback_days': TAGO_LOOKBACK_DAYS,
        'default_qty': 1000,
        'timeout': 30,
        'max_retries': 3,
        'mappings_dir': MAPPINGS_DIR,
        'enabled': TAGO_ENABLED,
    },

    # FusionSolar Station KPIs
    'fusionsolar_station': {
        'type': 'fusionsolar',
        'base_url': FUSIONSOLAR_API_URL,
        'user_name': FUSIONSOLAR_USER,
        'system_code': FUSIONSOLAR_SYSTEM_CODE,
        'schedule_seconds': FUSIONSOLAR_POLL_INTERVAL,
        'lookback_days': FUSIONSOLAR_LOOKBACK_DAYS,
        'timeout': 30,
        'max_retries': 3,
        'mappings_dir': MAPPINGS_DIR,
        'granularities': ['hourly', 'daily', 'monthly'],
        'enabled': bool(FUSIONSOLAR_USER and FUSIONSOLAR_SYSTEM_CODE) and FUSIONSOLAR_ENABLED,
    },

    # Open-Meteo Weather Forecast
    'openmeteo_forecast': {
        'type': 'openmeteo',
        # lat/lon resolved from site table at startup; fallback to env vars
        'latitude': float(OPEN_METEO_LATITUDE) if OPEN_METEO_LATITUDE else None,
        'longitude': float(OPEN_METEO_LONGITUDE) if OPEN_METEO_LONGITUDE else None,
        'panel_tilt': OPEN_METEO_PANEL_TILT,
        'panel_azimuth': OPEN_METEO_PANEL_AZIMUTH,
        'forecast_days': OPEN_METEO_FORECAST_DAYS,
        'model': OPEN_METEO_MODEL,
        'schedule_seconds': OPEN_METEO_POLL_INTERVAL,
        'timeout': 30,
        'max_retries': 3,
        'mappings_dir': MAPPINGS_DIR,
        'enabled': OPEN_METEO_ENABLED,
    },
}

    
