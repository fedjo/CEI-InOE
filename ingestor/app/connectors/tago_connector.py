"""
Tago.io Energy API connector.
Fetches energy consumption data (hourly/daily) from Tago.io API.
Extends HttpConnector with Tago-specific device token handling and data transformation.
"""

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from .http_connector import HttpConnector, EndpointConfig, HttpConnectorConfig

logger = logging.getLogger(__name__)


class TagoConnectorConfig(BaseModel):
    """Configuration for Tago.io connector."""
    type: str = "tago"
    base_url: str = "https://api.tago.io"
    schedule_seconds: int = 3600  # Default: fetch every hour
    lookback_days: int = 7  # Default lookback if no cursor exists
    default_qty: int = 1000  # Max records per request
    timeout: int = 30
    max_retries: int = 3
    enabled: bool = True
    mappings_dir: str = "/app/mappings"


class TagoConnector(HttpConnector):
    """
    Connector for Tago.io energy API.
    
    Features:
    - Per-device token authentication (token from DB metadata)
    - Per-device dynamic endpoints (devices loaded from DB)
    - Incremental fetching with date cursors
    - Transforms Tago.io response to energy records
    
    API Format:
        GET https://api.tago.io/data?start_date=...&end_date=...&qty=1000&variables=hourly_consumption
        Header: device-token: <token>
    
    Response:
        {
            "status": true,
            "result": [
                {"id": "...", "time": "...", "value": 3.24, "variable": "hourly_consumption", "device": "..."},
                ...
            ]
        }
    """
    
    # Variable name mapping: API variable → internal endpoint id
    VARIABLE_MAP = {
        'hourly_consumption': 'energy_hourly',
        'diffday': 'energy_daily',
    }
    
    def __init__(self, connector_id: str, config: Dict[str, Any], db_connection=None):
        # Parse Tago-specific config
        self.tago_cfg = TagoConnectorConfig(**config)
        
        # Build base HttpConnector config
        http_config = {
            'type': 'http',
            'base_url': self.tago_cfg.base_url,
            'endpoints': [],  # Will be populated dynamically per device
            'auth': {'type': 'none'},  # Auth via device-token header
            'timeout': self.tago_cfg.timeout,
            'max_retries': self.tago_cfg.max_retries,
            'schedule_seconds': self.tago_cfg.schedule_seconds,
            'enabled': self.tago_cfg.enabled,
        }
        
        super().__init__(connector_id, http_config, db_connection)
        self._devices: List[Dict[str, Any]] = []
        self._current_device: Optional[Dict[str, Any]] = None
    
    def start(self) -> None:
        """Initialize and load energy devices from DB."""
        super().start()
        self._load_devices()
        logger.info(f"[{self.connector_id}] Loaded {len(self._devices)} energy devices")
    
    def discover(self) -> List[str]:
        """
        Return device endpoint IDs for hourly consumption.
        Format: {device_id}:hourly_consumption
        """
        endpoints = []
        for device in self._devices:
            device_token = device.get('device_token')
            if device_token:
                endpoints.append(f"{device['device_id']}:hourly_consumption")
                # Uncomment to enable daily consumption as well:
                # endpoints.append(f"{device['device_id']}:diffday")
        return endpoints
    
    def fetch(self, item_id: str) -> Optional[Any]:
        """
        Fetch energy data for a device/variable combination.
        
        Args:
            item_id: Format "{device_id}:{variable_name}" e.g. "68f1fb6550bcae000b4c2501:hourly_consumption"
        """
        # Parse item_id
        parts = item_id.split(':')
        if len(parts) != 2:
            logger.error(f"[{self.connector_id}] Invalid item_id format: {item_id}")
            return None
        
        device_id, variable_name = parts
        
        device = self._get_device(device_id)
        if not device:
            logger.error(f"[{self.connector_id}] Unknown device: {device_id}")
            return None

        device_token = device.get('device_token')
        if not device_token:
            logger.error(f"[{self.connector_id}] No device_token for device {device_id}")
            return None
        
        self._current_device = device
        
        # Get date range for fetch
        start_date, end_date = self._get_date_range(device_id, variable_name)
        
        # Determine mapping based on variable
        mapping_name = self.VARIABLE_MAP.get(variable_name, 'energy_hourly')
        granularity = 'hourly' if variable_name == 'hourly_consumption' else 'daily'
        
        # Create dynamic endpoint for this device/variable
        endpoint = EndpointConfig(
            id=item_id,
            path='/data',
            method='GET',
            params={
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'qty': self.tago_cfg.default_qty,
                'variables': variable_name,
            },
            headers={
                'device-token': device_token,
            },
            data_path='result',  # Extract from response.result
            mapping=f"{self.tago_cfg.mappings_dir}/api_{mapping_name}.yaml",
            device_id=device_id,
            granularity=granularity,
            use_time_cursor=True,
            timestamp_field='ts',
            enabled=True,
        )
        
        # Temporarily add endpoint for base class fetch
        self.cfg.endpoints = [endpoint]
        
        # Store context for metadata
        self._current_start_date = start_date
        self._current_end_date = end_date
        self._current_variable = variable_name
        
        return super().fetch(item_id)
    
    # -------------------------------------------------------------------------
    # Override: Data Transformation
    # -------------------------------------------------------------------------
    
    def _transform_records(
        self, 
        records: List[Dict], 
        endpoint: EndpointConfig
    ) -> List[Dict]:
        """
        Transform Tago.io API response to energy records.
        
        Input from API:
            [
                {"id": "...", "time": "2025-12-08T23:00:00.000Z", "value": 3.24, "variable": "hourly_consumption", "device": "..."},
                ...
            ]
        
        Output:
            [
                {"ts": "2025-12-08T23:00:00.000Z", "energy_kwh": 3.24},
                ...
            ]
        """
        transformed = []
        
        for record in records:
            ts = record.get('time')
            value = record.get('value')
            
            if ts is None or value is None:
                logger.debug(f"[{self.connector_id}] Skipping record without time/value: {record}")
                continue
            
            transformed.append({
                'ts': ts,
                'energy_kwh': float(value),
            })
        
        # Sort by timestamp (oldest first for proper cursor tracking)
        transformed.sort(key=lambda r: r['ts'])
        
        return transformed
    
    def _build_metadata(
        self, 
        item_id: str, 
        records: List[Dict],
        endpoint: EndpointConfig
    ) -> Dict[str, Any]:
        """Build envelope metadata with Tago-specific info."""
        device_id = item_id.split(':')[0] if ':' in item_id else item_id
        
        return {
            'endpoint_id': item_id,
            'device_id': device_id,
            'device_token': self._current_device.get('device_token') if self._current_device else None,
            'variable': getattr(self, '_current_variable', None),
            'start_date': getattr(self, '_current_start_date', datetime.now()).isoformat(),
            'end_date': getattr(self, '_current_end_date', datetime.now()).isoformat(),
            'record_count': len(records),
            'cursor': self._cursors.get(item_id),
            'source_type': 'api',
            'sha256': self._compute_sha256(item_id, records),
        }
    
    # -------------------------------------------------------------------------
    # Device Management
    # -------------------------------------------------------------------------
    
    def _load_devices(self) -> None:
        """Load energy devices from database using DAO."""
        if not self._db_connection:
            logger.warning(f"[{self.connector_id}] No DB connection, using empty device list")
            self._devices = []
            return
        
        try:
            from dao import DeviceDAO
            device_dao = DeviceDAO(self._db_connection)
            self._devices = device_dao.get_energy_devices_with_token()
            logger.debug(f"[{self.connector_id}] Loaded devices: {[d['device_id'] for d in self._devices]}")
            
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
    # Date Range / Cursor Management
    # -------------------------------------------------------------------------
    
    def _get_date_range(self, device_id: str, variable_name: str) -> tuple[datetime, datetime]:
        """
        Get start and end dates for fetch.
        
        Strategy:
        1. Try cursor table (last successful fetch timestamp)
        2. Fallback: query max timestamp from target data table
        3. Default: use lookback_days config
        
        end_date is always current timestamp.
        """
        end_date = datetime.now(timezone.utc)
        
        # Build endpoint_id for cursor lookup
        endpoint_id = f"{device_id}:{variable_name}"
        
        # Try to get last cursor from database
        start_date = self._get_cursor_from_db(device_id, endpoint_id)
        
        if not start_date:
            # Fallback: query max timestamp from data table
            granularity = 'hourly' if variable_name == 'hourly_consumption' else 'daily'
            start_date = self._get_max_timestamp_from_data(device_id, granularity)
        
        if not start_date:
            # Default: use lookback
            start_date = end_date - timedelta(days=self.tago_cfg.lookback_days)
            logger.info(f"[{self.connector_id}] No cursor for {endpoint_id}, using {self.tago_cfg.lookback_days} day lookback")
        else:
            # Start from last fetch + 1 hour to avoid duplicates
            start_date = start_date + timedelta(hours=1)
            logger.debug(f"[{self.connector_id}] Resuming from cursor: {start_date}")
        
        return start_date, end_date
    
    def _get_cursor_from_db(self, device_id: str, endpoint_id: str) -> Optional[datetime]:
        """Get last fetch timestamp from cursor table using DAO."""
        if not self._db_connection:
            return None
        
        try:
            from dao import CursorDAO
            cursor_dao = CursorDAO(self._db_connection)
            return cursor_dao.get_cursor(
                connector_id=self.connector_id,
                endpoint_id=endpoint_id,
                device_id=device_id
            )
            
        except Exception as e:
            logger.debug(f"[{self.connector_id}] Cursor table query failed: {e}")
            return None
    
    def _get_max_timestamp_from_data(self, device_id: str, granularity: str) -> Optional[datetime]:
        """Fallback: get max timestamp from energy fact table using DAO."""
        if not self._db_connection:
            return None
        
        try:
            from dao.cursor_dao import EnergyMetricsDAO
            energy_dao = EnergyMetricsDAO(self._db_connection)
            return energy_dao.get_max_timestamp(device_id, granularity)
            
        except Exception as e:
            logger.debug(f"[{self.connector_id}] Energy table query failed: {e}")
            return None
    
    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    
    def _compute_sha256(self, item_id: str, records: List[Dict]) -> str:
        """Compute SHA256 hash of input for deduplication."""
        content = f"{item_id}:{len(records)}:{datetime.now().isoformat()}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
