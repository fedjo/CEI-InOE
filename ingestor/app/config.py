"""
CEI-InOE Ingestor - Configuration
Centralized application configuration loaded from environment variables.
"""

import os
from typing import Any, Dict


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

AIRBELD_API_URL = os.environ.get('AIRBELD_API_URL', 'https://api.airbeld.com/api/v1')
AIRBELD_EMAIL = os.environ.get('AIRBELD_EMAIL', 'ymar@greensupplychain.eu')
AIRBELD_PASSWORD = os.environ.get('AIRBELD_PASSWORD', 'hH5j~hy#L]K2q;E')
AIRBELD_POLL_INTERVAL = int(os.environ.get('AIRBELD_POLL_INTERVAL', '3600'))  # (21600) 6-hours in seconds
AIRBELD_LOOKBACK_DAYS = int(os.environ.get('AIRBELD_LOOKBACK_DAYS', '1'))


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
}
    
    # Example HTTP connector (uncomment and configure)
    # 'energy_api': {
    #     'type': 'http',
    #     'base_url': ENERGY_API_URL,
    #     'schedule_seconds': 300,
    #     'auth': {
    #         'type': 'api_key',
    #         'api_key_name': 'X-API-Key',
    #         'api_key_value': ENERGY_API_KEY,
    #     },
    #     'endpoints': [
    #         {
    #             'id': 'energy_hourly',
    #             'path': '/v1/readings',
    #             'data_path': 'data.readings',
    #             'mapping': f'{MAPPINGS_DIR}/energy_hourly.yaml',
    #             'device_id': 'energy_meter_1',
    #             'granularity': 'hourly',
    #             'use_time_cursor': True,
    #             'time_param': 'since',
    #             'timestamp_field': 'timestamp',
    #         },
    #     ],
    # },
