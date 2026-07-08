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
    base_url: str = "http://api.example.com"  # Placeholder, not used directly since endpoints are dynamic
    schedule_seconds: int = 3600  # Default: fetch every hour
    lookback_days: int = 180  # Default lookback if no cursor exists
    default_qty: int = 100000  # Max records per request
    timeout: int = 30
    max_retries: int = 3
    enabled: bool = True
    mappings_dir: str = "/app/mappings"


class TagoConnector(HttpConnector):
    """
    Connector for Tago.io energy API.
    
    Features:
    - Per-datasource token authentication (token from DB metadata)
    - Per-datasource dynamic endpoints (datasources loaded from DB)
    - Incremental fetching with date cursors
    - Transforms Tago.io response to energy records
    
    API Format:
        GET https://api.tago.io/data?start_date=...&end_date=...&qty=100000&variables=hourly_consumption
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
    
    def __init__(self, connector_id: str, config: Dict[str, Any]):
        # Parse Tago-specific config
        self.tago_cfg = TagoConnectorConfig(**config)
        
        # Build base HttpConnector config
        http_config = {
            'type': 'http',
            'base_url': self.tago_cfg.base_url,
            'endpoints': [],  # Will be populated dynamically per datasource
            'auth': {'type': 'none'},  # Auth via device-token header
            'timeout': self.tago_cfg.timeout,
            'max_retries': self.tago_cfg.max_retries,
            'schedule_seconds': self.tago_cfg.schedule_seconds,
            'enabled': self.tago_cfg.enabled,
        }
        
        super().__init__(connector_id, http_config)
        self._datasources: List[Dict[str, Any]] = []
        self._current_datasource: Optional[Dict[str, Any]] = None
    
    def start(self) -> None:
        """Initialize and load energy datasources from DB."""
        super().start()
        self._load_datasources()
        logger.info(f"[{self.connector_id}] Loaded {len(self._datasources)} energy devices")

    def discover(self) -> List[str]:
        """
        Return device endpoint IDs for hourly consumption.
        Format: {device_id}:hourly_consumption

        Datasource list is refreshed from the DB on every call so that
        additions / status changes made via the API take effect at the
        next scheduled poll without restarting the ingestor.
        """
        self._load_datasources()
        endpoints = []
        for ds in self._datasources:
            device_token = ds.get('device_token')
            if device_token:
                endpoints.append(f"{ds['external_id']}:hourly_consumption")
                # Uncomment to enable daily consumption as well:
                # endpoints.append(f"{ds['external_id']}:diffday")
        return endpoints

    def fetch(self, ext_id: str) -> Optional[Any]:
        """
        Fetch energy data for a device/variable combination.
        
        Args:
            item_id: Format "{device_id}:{variable_name}" e.g. "68f1fb6550bcae000b4c2501:hourly_consumption"
        """
        # Parse item_id
        parts = ext_id.split(':')
        if len(parts) != 2:
            logger.error(f"[{self.connector_id}] Invalid item_id format: {ext_id}")
            return None
        
        datasource_ext_id, variable_name = parts
        
        datasource = self._get_datasource(datasource_ext_id)
        if not datasource:
            logger.error(f"[{self.connector_id}] Unknown device: {datasource_ext_id}")
            return None

        device_token = datasource.get('device_token')
        if not device_token:
            logger.error(f"[{self.connector_id}] No device_token for device {datasource_ext_id}")
            return None

        ds_id = datasource.get('id')
        if not ds_id:
            logger.error(f"[{self.connector_id}] No internal ID for datasource {datasource_ext_id}")
            return None

        self._current_datasource = datasource
        
        # Get date range for fetch
        start_date, end_date = self._get_date_range(ds_id, datasource_ext_id, variable_name)
        
        # Determine mapping based on variable
        mapping_name = self.VARIABLE_MAP.get(variable_name, 'energy_hourly')
        granularity = 'hourly' if variable_name == 'hourly_consumption' else 'daily'

        # Create dynamic endpoint for this device/variable
        endpoint = EndpointConfig(
            id=ext_id,
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
            datasource_id=ds_id,
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
        
        return super().fetch(ext_id)
    
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
            'device_token': self._current_datasource.get('device_token') if self._current_datasource else None,
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
    
    def _load_datasources(self) -> None:
        """Load energy datasources from database using DAO."""
        try:
            from shared import get_connection
            from dao import DatasourceDAO

            with get_connection() as conn:
                datasource_dao = DatasourceDAO(conn)
                datasources = datasource_dao.get_energy_datasources_with_token()
                # Map to datasource-like format for backward compatibility
                self._datasources = [
                    {
                        'id': ds['id'],
                        'external_id': ds['external_id'],
                        'alias': ds.get('alias'),
                        'client': ds.get('client'),
                        'device_token': ds.get('device_token'),
                        'metadata': ds.get('metadata', {}),
                    }
                    for ds in datasources
                ]
                logger.debug(f"[{self.connector_id}] Loaded datasources: {[d['external_id'] for d in self._datasources]}")

        except Exception as e:
            logger.error(f"[{self.connector_id}] Failed to load datasources: {e}")
            self._datasources = []

    def _get_datasource(self, external_id: str) -> Optional[Dict[str, Any]]:
        """Find datasource by external ID."""
        for d in self._datasources:
            if d['external_id'] == external_id:
                return d
        return None

    def reload_datasources(self) -> None:
        """Force an immediate refresh of the in-memory datasource list."""
        self._load_datasources()

    # -------------------------------------------------------------------------
    # Date Range / Cursor Management
    # -------------------------------------------------------------------------
    
    def _get_date_range(self, datasource_id: int, external_id: str, variable_name: str) -> tuple[datetime, datetime]:
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
        endpoint_id = f"{external_id}:{variable_name}"

        # Try to get last cursor from database
        start_date = self._get_cursor_from_db(datasource_id, endpoint_id)
        
        if not start_date:
            # Fallback: query max timestamp from data table
            granularity = 'hourly' if variable_name == 'hourly_consumption' else 'daily'
            start_date = self._get_max_timestamp_from_data(datasource_id, granularity)
        
        if not start_date:
            # Default: use lookback
            start_date = end_date - timedelta(days=self.tago_cfg.lookback_days)
            logger.info(f"[{self.connector_id}] No cursor for {endpoint_id}, using {self.tago_cfg.lookback_days} day lookback")
        else:
            # Start from last fetch + 1 hour to avoid duplicates
            start_date = start_date + timedelta(hours=1)
            logger.debug(f"[{self.connector_id}] Resuming from cursor: {start_date}")

        return start_date, end_date
    
    def _get_cursor_from_db(self, datasource_id: int, endpoint_id: str) -> Optional[datetime]:
        """Get last fetch timestamp from cursor table using DAO."""
        try:
            from shared import get_connection
            from dao import CursorDAO
            
            with get_connection() as conn:
                cursor_dao = CursorDAO(conn)
                return cursor_dao.get_cursor(
                    connector_id=self.connector_id,
                    endpoint_id=endpoint_id,
                    datasource_id=datasource_id
                )
            
        except Exception as e:
            logger.debug(f"[{self.connector_id}] Cursor table query failed: {e}")
            return None
    
    def _get_max_timestamp_from_data(self, datasource_id: int, granularity: str) -> Optional[datetime]:
        """Fallback: get max timestamp from energy fact table using DAO."""
        try:
            from shared import get_connection
            from dao import CursorDAO
            
            with get_connection() as conn:
                cursor_dao = CursorDAO(conn)
                return cursor_dao.get_max_energy_timestamp(datasource_id, granularity)
            
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
