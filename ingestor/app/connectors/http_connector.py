"""
HTTP API connector for fetching data from external APIs.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseConnector, ConnectorStatus, InputEnvelope

logger = logging.getLogger(__name__)


class PaginationConfig(BaseModel):
    """Pagination configuration."""
    type: Optional[str] = None  # offset, cursor, page
    page_size: int = 100
    offset_param: str = "offset"
    limit_param: str = "limit"
    cursor_param: str = "cursor"
    next_cursor_path: str = "next_cursor"
    page_param: str = "page"


class EndpointConfig(BaseModel):
    """Configuration for an API endpoint."""
    id: str
    path: str
    method: str = "GET"
    params: Dict[str, Any] = {}
    headers: Dict[str, str] = {}
    body: Optional[Dict[str, Any]] = None
    data_path: str = ""
    mapping: Optional[str] = None
    device_id: Optional[str] = None
    granularity: Optional[str] = None
    pagination: PaginationConfig = PaginationConfig()
    use_time_cursor: bool = False
    time_param: str = "since"
    timestamp_field: Optional[str] = None
    cursor_field: Optional[str] = None
    enabled: bool = True
    test: bool = False


class AuthConfig(BaseModel):
    """Authentication configuration."""
    type: str = "none"  # none, bearer, api_key, basic
    token: Optional[str] = None
    api_key_name: str = "X-API-Key"
    api_key_value: Optional[str] = None
    api_key_location: str = "header"  # header, query
    username: Optional[str] = None
    password: Optional[str] = None


class HttpConnectorConfig(BaseModel):
    """Configuration for HTTP connector."""
    type: str = "http"
    base_url: str
    endpoints: List[EndpointConfig] = []
    auth: AuthConfig = AuthConfig()
    rate_limit: float = 10.0
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.5
    schedule_seconds: int = 300
    cursor_file: Optional[str] = None
    enabled: bool = True


class HttpConnector(BaseConnector):
    """
    Generic HTTP connector for REST APIs.
    """
    
    def __init__(self, connector_id: str, config: Dict[str, Any]):
        super().__init__(connector_id, config)
        
        # Parse config
        self.cfg = HttpConnectorConfig(**config)
        
        # State
        self._cursors: Dict[str, Any] = {}
        self._last_request_time: float = 0
        self._session: Optional[requests.Session] = None
    
    def start(self) -> None:
        """Initialize HTTP session."""
        self._session = requests.Session()
        
        # Configure retries
        retry = Retry(
            total=self.cfg.max_retries,
            backoff_factor=self.cfg.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        
        # Default headers
        self._session.headers.update({
            'User-Agent': f'CEI-InOE/{self.connector_id}',
            'Accept': 'application/json',
        })
        
        self._apply_auth()
        self._load_cursors()
        
        self.status = ConnectorStatus.RUNNING
        logger.info(f"[{self.connector_id}] Started ({len(self.cfg.endpoints)} endpoints)")
    
    def stop(self) -> None:
        """Close session and save cursors."""
        self._save_cursors()
        if self._session:
            self._session.close()
            self._session = None
        self.status = ConnectorStatus.STOPPED
        logger.info(f"[{self.connector_id}] Stopped")
    
    def discover(self) -> List[str]:
        """Return enabled endpoint IDs."""
        return [ep.id for ep in self.cfg.endpoints if ep.enabled]
    
    def fetch(self, item_id: str) -> Optional[InputEnvelope]:
        """Fetch data from endpoint."""
        endpoint = self._get_endpoint(item_id)
        if not endpoint:
            logger.error(f"[{self.connector_id}] Unknown endpoint: {item_id}")
            return None
        
        try:
            self._rate_limit()
            
            url = urljoin(self.cfg.base_url.rstrip('/') + '/', endpoint.path.lstrip('/'))
            params = self._build_params(endpoint)
            
            logger.debug(f"[{self.connector_id}] {endpoint.method} {url}")
            
            if endpoint.method.upper() == 'POST':
                response = self._session.post( # type: ignore
                    url,
                    params=params,
                    json=endpoint.body,
                    headers=endpoint.headers,
                    timeout=self.cfg.timeout
                )
            else:
                response = self._session.get( # type: ignore
                    url,
                    params=params,
                    headers=endpoint.headers,
                    timeout=self.cfg.timeout
                )
            
            response.raise_for_status()
            data = response.json()
            records = self._extract_records(data, endpoint)
            
            if not records:
                logger.debug(f"[{self.connector_id}] No records from {item_id}")
                return None
            
            self._update_cursor(item_id, data, records, endpoint)
            
            input_id = self._compute_input_id(item_id, records)
            
            envelope = InputEnvelope(
                connector_id=self.connector_id,
                input_id=input_id,
                source_uri=url,
                received_at=datetime.now(),
                content=records,
                content_type='json',
                hint_mapping=endpoint.mapping,
                hint_device_id=endpoint.device_id,
                hint_granularity=endpoint.granularity,
                metadata={
                    'endpoint_id': item_id,
                    'record_count': len(records),
                    'cursor': self._cursors.get(item_id),
                }
            )
            
            logger.info(f"[{self.connector_id}] Fetched {len(records)} records from {item_id}")
            return envelope
            
        except requests.exceptions.HTTPError as e:
            self._last_error = f"HTTP {e.response.status_code}"
            logger.error(f"[{self.connector_id}] HTTP error {item_id}: {e}")
            return None
        except Exception as e:
            self._last_error = str(e)
            logger.error(f"[{self.connector_id}] Fetch error {item_id}: {e}")
            return None
    
    def ack(self, envelope: InputEnvelope) -> None:
        """Persist cursor after success."""
        self._save_cursors()
        logger.debug(f"[{self.connector_id}] Acked {envelope.input_id}")
    
    def fail(self, envelope: InputEnvelope, error: str) -> None:
        """Log failure; cursor NOT advanced."""
        logger.error(f"[{self.connector_id}] Failed {envelope.metadata.get('endpoint_id')}: {error}")
    
    def health(self) -> Dict[str, Any]:
        """Return health with cursor info."""
        base = super().health()
        base['endpoints'] = len(self.cfg.endpoints)
        base['cursors'] = dict(self._cursors)
        return base
    
    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    
    def _apply_auth(self) -> None:
        """Apply authentication."""
        auth = self.cfg.auth
        
        if auth.type == 'bearer' and auth.token:
            self._session.headers['Authorization'] = f'Bearer {auth.token}' # type: ignore
        elif auth.type == 'api_key' and auth.api_key_value:
            if auth.api_key_location == 'header':
                self._session.headers[auth.api_key_name] = auth.api_key_value # type: ignore
        elif auth.type == 'basic' and auth.username:
            self._session.auth = (auth.username, auth.password or '') # type: ignore
    
    def _get_endpoint(self, endpoint_id: str) -> Optional[EndpointConfig]:
        """Find endpoint by ID."""
        for ep in self.cfg.endpoints:
            if ep.id == endpoint_id:
                return ep
        return None
    
    def _build_params(self, endpoint: EndpointConfig) -> Dict[str, Any]:
        """Build query parameters."""
        params = dict(endpoint.params)
        
        # API key in query
        if self.cfg.auth.type == 'api_key' and self.cfg.auth.api_key_location == 'query':
            params[self.cfg.auth.api_key_name] = self.cfg.auth.api_key_value
        
        # Pagination
        pag = endpoint.pagination
        cursor = self._cursors.get(endpoint.id)
        
        if pag.type == 'offset' and cursor:
            params[pag.offset_param] = cursor
            params[pag.limit_param] = pag.page_size
        elif pag.type == 'cursor' and cursor:
            params[pag.cursor_param] = cursor
        elif pag.type == 'page':
            params[pag.page_param] = cursor or 1
        
        # Time-based cursor
        if endpoint.use_time_cursor:
            if cursor:
                params[endpoint.time_param] = cursor
            else:
                default_start = (datetime.now() - timedelta(hours=24)).isoformat()
                params[endpoint.time_param] = default_start
        
        return params
    
    def _extract_records(self, data: Any, endpoint: EndpointConfig) -> List[Dict]:
        """Extract records using data_path."""
        if not endpoint.data_path:
            return data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        
        result = data
        for key in endpoint.data_path.split('.'):
            if isinstance(result, dict):
                result = result.get(key, [])
            else:
                return []
        
        return result if isinstance(result, list) else [result] if isinstance(result, dict) else []
    
    def _update_cursor(self, endpoint_id: str, data: Any, records: List[Dict], endpoint: EndpointConfig) -> None:
        """Update cursor for next fetch."""
        pag = endpoint.pagination
        
        if pag.type == 'offset':
            current = self._cursors.get(endpoint_id, 0)
            if len(records) >= pag.page_size:
                self._cursors[endpoint_id] = current + len(records)
            else:
                self._cursors[endpoint_id] = 0
        elif pag.type == 'cursor':
            next_cursor = self._get_nested(data, pag.next_cursor_path)
            if next_cursor:
                self._cursors[endpoint_id] = next_cursor
        elif pag.type == 'page':
            current = self._cursors.get(endpoint_id, 1)
            if len(records) >= pag.page_size:
                self._cursors[endpoint_id] = current + 1
            else:
                self._cursors[endpoint_id] = 1
        
        # Time-based cursor
        if endpoint.timestamp_field and records:
            timestamps = [r.get(endpoint.timestamp_field) for r in records if r.get(endpoint.timestamp_field)]
            if timestamps:
                self._cursors[endpoint_id] = max(timestamps) # type: ignore
    
    def _get_nested(self, data: Any, path: str) -> Any:
        """Get nested value."""
        if not path:
            return None
        result = data
        for key in path.split('.'):
            if isinstance(result, dict):
                result = result.get(key)
            else:
                return None
        return result
    
    def _compute_input_id(self, endpoint_id: str, records: List[Dict]) -> str:
        """Compute idempotency key."""
        content_hash = hashlib.sha256(
            json.dumps(records, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"{self.connector_id}:{endpoint_id}:{timestamp}:{content_hash}"
    
    def _rate_limit(self) -> None:
        """Apply rate limiting."""
        if self.cfg.rate_limit <= 0:
            return
        min_interval = 1.0 / self.cfg.rate_limit
        elapsed = time.time() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()
    
    def _load_cursors(self) -> None:
        """Load persisted cursors."""
        cursor_file = self.cfg.cursor_file or f'/tmp/{self.connector_id}_cursors.json'
        try:
            if os.path.exists(cursor_file):
                with open(cursor_file) as f:
                    self._cursors = json.load(f)
        except Exception as e:
            logger.warning(f"[{self.connector_id}] Failed to load cursors: {e}")
    
    def _save_cursors(self) -> None:
        """Persist cursors."""
        cursor_file = self.cfg.cursor_file or f'/tmp/{self.connector_id}_cursors.json'
        try:
            with open(cursor_file, 'w') as f:
                json.dump(self._cursors, f)
        except Exception as e:
            logger.warning(f"[{self.connector_id}] Failed to save cursors: {e}")
