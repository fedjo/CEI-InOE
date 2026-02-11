"""
Airbeld Environmental API connector.
Extends HttpConnector with Airbeld-specific authentication and data transformation.
"""

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from .http_connector import HttpConnector, EndpointConfig, AuthConfig, HttpConnectorConfig

logger = logging.getLogger(__name__)


class AirbeldConnectorConfig(BaseModel):
    """Configuration for Airbeld connector."""
    type: str = "airbeld"
    base_url: str = "https://api.airbeld.com/api/v1"
    email: str = ""
    password: str = ""
    schedule_seconds: int = 3600
    lookback_days: int = 7
    timeout: int = 30
    max_retries: int = 3
    enabled: bool = True
    mappings_dir: str = "/app/mappings"


class AirbeldConnector(HttpConnector):
    """
    Connector for Airbeld environmental sensor API.
    
    Features:
    - OAuth-style email/password authentication
    - Per-device dynamic endpoints (devices loaded from DB)
    - Sensor data pivot transformation (column → row oriented)
    - Date range queries with cursor tracking
    """
    
    # Sensor name mapping: API name → DB column
    SENSOR_MAP = {
        'atm_pressure': 'atm_pressure',
        'noise_level_db': 'noise_level_db',
        'temperature': 'temperature',
        'humidity': 'humidity',
        'pm10': 'pm10',
        'wind_speed': 'wind_speed',
        'wind_direction': 'wind_direction',
        'wind_angle': 'wind_angle',
        'pm2p5': 'pm2p5',
    }
    
    def __init__(self, connector_id: str, config: Dict[str, Any], db_connection=None):
        # Parse Airbeld-specific config
        self.airbeld_cfg = AirbeldConnectorConfig(**config)
        
        # Build base HttpConnector config
        http_config = {
            'type': 'http',
            'base_url': self.airbeld_cfg.base_url,
            'endpoints': [],  # Will be populated dynamically
            'auth': {
                'type': 'oauth',
                'token_url': f"{self.airbeld_cfg.base_url}/auth/token/",
                'username': self.airbeld_cfg.email,
                'password': self.airbeld_cfg.password,
                'token_expires_in': 3600,
            },
            'timeout': self.airbeld_cfg.timeout,
            'max_retries': self.airbeld_cfg.max_retries,
            'schedule_seconds': self.airbeld_cfg.schedule_seconds,
            'enabled': self.airbeld_cfg.enabled,
        }
        
        super().__init__(connector_id, http_config, db_connection)
        self._devices: List[Dict[str, Any]] = []
    
    def start(self) -> None:
        """Initialize and load devices from DB."""
        super().start()
        self._load_devices()
        logger.info(f"[{self.connector_id}] Loaded {len(self._devices)} weather devices")
    
    def discover(self) -> List[str]:
        """Return device IDs (not endpoint IDs)."""
        return [d['device_id'] for d in self._devices if d.get('external_id')]
    
    def fetch(self, item_id: str) -> Optional[Any]:
        """Fetch environmental data for a device."""
        device = self._get_device(item_id)
        if not device:
            logger.error(f"[{self.connector_id}] Unknown device: {item_id}")
            return None
        
        external_id = device.get('external_id')
        if not external_id:
            logger.error(f"[{self.connector_id}] No external_id for device {item_id}")
            return None
        
        # Create dynamic endpoint for this device
        start_date, end_date = self._get_date_range(item_id)
        
        endpoint = EndpointConfig(
            id=item_id,
            path=f"/devices/{external_id}/readings_by_date/",
            method="GET",
            params={
                'start-date': start_date.strftime('%Y-%m-%d'),
                'end-date': end_date.strftime('%Y-%m-%d'),
                'period': 'hour',
            },
            data_path='sensors',
            mapping=f"{self.airbeld_cfg.mappings_dir}/api_environmental_metrics.yaml",
            device_id=item_id,
            granularity='hourly',
            use_time_cursor=True,
            timestamp_field='timestamp',
            enabled=True,
        )
        
        # Temporarily add endpoint for base class fetch
        self.cfg.endpoints = [endpoint]
        
        # Store date range for metadata
        self._current_start_date = start_date
        self._current_end_date = end_date
        self._current_external_id = external_id
        
        return super().fetch(item_id)
    
    # -------------------------------------------------------------------------
    # Override: OAuth Authentication
    # -------------------------------------------------------------------------
    
    def _refresh_oauth_token(self) -> None:
        """Refresh token using Airbeld email/password auth."""
        logger.info(f"[{self.connector_id}] Authenticating with Airbeld API...")
        
        response = self._session.post(  # type: ignore
            f"{self.airbeld_cfg.base_url}/auth/token/",
            json={
                'email': self.airbeld_cfg.email,
                'password': self.airbeld_cfg.password,
            },
            timeout=self.airbeld_cfg.timeout
        )
        response.raise_for_status()
        
        data = response.json()
        self._access_token = data.get('accessToken') or data.get('access_token')
        self._token_expires = datetime.now() + timedelta(hours=1)
        
        logger.info(f"[{self.connector_id}] Authentication successful")
    
    # -------------------------------------------------------------------------
    # Override: Data Transformation
    # -------------------------------------------------------------------------
    
    def _extract_records(self, data: Any, endpoint: EndpointConfig) -> List[Dict]:
        """Extract sensors dict from response."""
        if isinstance(data, dict) and 'sensors' in data:
            return [data['sensors']]  # Return as single item for transformation
        return [data] if isinstance(data, dict) else []
    
    def _transform_records(
        self, 
        records: List[Dict], 
        endpoint: EndpointConfig
    ) -> List[Dict]:
        """
        Transform column-oriented sensor data to row-oriented records.
        
        Input:
            {'atm_pressure': {'values': [{'timestamp': '...', 'value': 99.5}, ...]}, ...}
        
        Output:
            [{'timestamp': '...', 'atm_pressure': 99.5, 'temperature': 22.5, ...}, ...]
        """
        if not records:
            return []
        
        sensors = records[0]  # We passed sensors dict as single record
        
        # Collect all values by timestamp
        by_timestamp: Dict[str, Dict[str, Any]] = {}
        
        for sensor_name, sensor_data in sensors.items():
            db_column = self.SENSOR_MAP.get(sensor_name)
            if not db_column:
                logger.debug(f"[{self.connector_id}] Unknown sensor: {sensor_name}")
                continue
            
            values = sensor_data.get('values', [])
            for item in values:
                ts = item.get('timestamp')
                value = item.get('value')
                
                if ts and value is not None:
                    if ts not in by_timestamp:
                        by_timestamp[ts] = {'timestamp': ts}
                    by_timestamp[ts][db_column] = value
        
        # Convert to list, sorted by timestamp
        result = list(by_timestamp.values())
        result.sort(key=lambda r: r['timestamp'])
        
        return result
    
    def _build_metadata(
        self, 
        item_id: str, 
        records: List[Dict],
        endpoint: EndpointConfig
    ) -> Dict[str, Any]:
        """Build envelope metadata with Airbeld-specific info."""
        return {
            'endpoint_id': item_id,
            'device_id': item_id,
            'external_id': getattr(self, '_current_external_id', None),
            'start_date': getattr(self, '_current_start_date', datetime.now()).isoformat(),
            'end_date': getattr(self, '_current_end_date', datetime.now()).isoformat(),
            'record_count': len(records),
            'cursor': self._cursors.get(item_id),
            'source_type': 'api',
            'sha256': self._file_sha256(self._compute_input_id(item_id, records)),
        }


    
    # -------------------------------------------------------------------------
    # Device Management
    # -------------------------------------------------------------------------
    
    def _load_devices(self) -> None:
        """Load weather devices from database."""
        if not self._db_connection:
            logger.warning(f"[{self.connector_id}] No DB connection, using empty device list")
            self._devices = []
            return
        
        try:
            cursor = self._db_connection.cursor()
            cursor.execute("""
                SELECT 
                    device_id,
                    alias,
                    client,
                    metadata->>'external_id' as external_id,
                    metadata
                FROM generic_device
                WHERE device_type = 'weather'
                AND status = 'online'
            """) # `device_type` = 'weather' selects Airbeld weather stations
            
            self._devices = []
            for row in cursor.fetchall():
                self._devices.append({
                    'device_id': row[0],
                    'alias': row[1],
                    'client': row[2],
                    'external_id': row[3],
                    'metadata': row[4],
                })
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"[{self.connector_id}] Failed to load devices: {e}")
            self._devices = []
    
    def _get_device(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Find device by ID."""
        for d in self._devices:
            if d['device_id'] == device_id:
                return d
        return None
    
    # -------------------------------------------------------------------------
    # Helpers / Date Range / Cursor
    # -------------------------------------------------------------------------
    
    def _get_date_range(self, device_id: str) -> tuple[datetime, datetime]:
        """Get start and end dates for fetch."""
        end_date = datetime.now()

        # Try to get last cursor from database
        start_date = self._get_cursor_from_db(device_id)
        
        if not start_date:
            # Fallback: query max timestamp from data table
            start_date = self._get_max_timestamp_from_data(device_id)
        
        if not start_date:
            # Default: use lookback
            start_date = end_date - timedelta(days=self.airbeld_cfg.lookback_days)
        else:
            # Start from last fetch + 1 hour to avoid duplicates
            start_date = start_date + timedelta(hours=1)
        
        return start_date, end_date
    
    def _get_cursor_from_db(self, device_id: str) -> Optional[datetime]:
        """Get last fetch timestamp from cursor table."""
        if not self._db_connection:
            return None
        
        try:
            cursor = self._db_connection.cursor()
            cursor.execute("""
                SELECT last_fetch_timestamp
                FROM api_fetch_cursor
                WHERE connector_id = %s
                AND endpoint_id = %s
                AND device_id = %s
            """, (self.connector_id, device_id, device_id))
            
            row = cursor.fetchone()
            cursor.close()
            
            return row[0] if row else None
            
        except Exception as e:
            logger.debug(f"[{self.connector_id}] Cursor table query failed: {e}")
            return None
    
    def _get_max_timestamp_from_data(self, device_id: str) -> Optional[datetime]:
        """Fallback: get max timestamp from environmental_metrics table."""
        if not self._db_connection:
            return None
        
        try:
            cursor = self._db_connection.cursor()
            cursor.execute("""
                SELECT MAX(timestamp)
                FROM environmental_metrics
                WHERE source_device_id = %s
            """, (device_id,))
            
            row = cursor.fetchone()
            cursor.close()
            
            return row[0] if row and row[0] else None
            
        except Exception as e:
            logger.debug(f"[{self.connector_id}] Data table query failed: {e}")
            return None

    def _file_sha256(self, path: str) -> str:
        """Calculate SHA256 hash."""
        sha256 = hashlib.sha256()
        sha256.update(path.encode('utf-8'))
        return sha256.hexdigest()