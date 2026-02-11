"""
HTTP API connector for fetching data from external APIs.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

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
    type: str = "none"  # none, bearer, api_key, basic, oauth
    token: Optional[str] = None
    api_key_name: str = "X-API-Key"
    api_key_value: Optional[str] = None
    api_key_location: str = "header"  # header, query
    username: Optional[str] = None
    password: Optional[str] = None
    # OAuth fields
    token_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token_expires_in: int = 3600  # seconds


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
    Subclass this for API-specific implementations.
    """
    
    def __init__(self, connector_id: str, config: Dict[str, Any], db_connection=None):
        super().__init__(connector_id, config)
        self.cfg = HttpConnectorConfig(**config)
        self._db_connection = db_connection
        self._cursors: Dict[str, Any] = {}
        self._last_request_time: float = 0
        self._session: Optional[requests.Session] = None
        self._access_token: Optional[str] = None
        self._token_expires: Optional[datetime] = None
    
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
            'Content-Type': 'application/json',
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
            self._ensure_authenticated()

            url = self._build_url(endpoint)
            params = self._build_params(endpoint)
            headers = self._build_headers(endpoint)

            logger.debug(f"[{self.connector_id}] {endpoint.method} {url}")

            response = self._make_request(endpoint, url, params, headers)
            response.raise_for_status()
            
            data = response.json()
            records = self._extract_records(data, endpoint)

            if not records:
                logger.debug(f"[{self.connector_id}] No records from {item_id}")
                return None

            # Transform records if needed (subclasses override)
            records = self._transform_records(records, endpoint)

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
                metadata=self._build_metadata(item_id, records, endpoint)
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
        self._save_cursor_to_db(envelope)
        logger.debug(f"[{self.connector_id}] Acked {envelope.input_id}")

    def fail(self, envelope: InputEnvelope, error: str) -> None:
        """Log failure; cursor NOT advanced."""
        logger.error(f"[{self.connector_id}] Failed {envelope.metadata.get('endpoint_id')}: {error}")

    def health(self) -> Dict[str, Any]:
        """Return health with cursor info."""
        base = super().health()
        base['endpoints'] = len(self.cfg.endpoints)
        base['cursors'] = dict(self._cursors)
        base['authenticated'] = self._access_token is not None
        return base

    # -------------------------------------------------------------------------
    # Authentication (can be overridden by subclasses)
    # -------------------------------------------------------------------------
    
    def _apply_auth(self) -> None:
        """Apply static authentication."""
        auth = self.cfg.auth

        if auth.type == 'bearer' and auth.token:
            self._session.headers['Authorization'] = f'Bearer {auth.token}'  # type: ignore
        elif auth.type == 'api_key' and auth.api_key_value:
            if auth.api_key_location == 'header':
                self._session.headers[auth.api_key_name] = auth.api_key_value  # type: ignore
        elif auth.type == 'basic' and auth.username:
            self._session.auth = (auth.username, auth.password or '')  # type: ignore
    
    def _ensure_authenticated(self) -> None:
        """Ensure we have valid auth. Override for OAuth flows."""
        if self.cfg.auth.type != 'oauth':
            return
        
        if self._access_token and self._token_expires and datetime.now() < self._token_expires:
            return
        
        self._refresh_oauth_token()
    
    def _refresh_oauth_token(self) -> None:
        """Refresh OAuth token. Override for custom OAuth implementations."""
        auth = self.cfg.auth
        if not auth.token_url:
            raise ValueError("OAuth requires token_url")
        
        response = self._session.post( # type: ignore
            auth.token_url,
            json={
                'username': auth.username,
                'password': auth.password,
            },
            timeout=self.cfg.timeout
        )
        response.raise_for_status()
        
        data = response.json()
        self._access_token = data.get('accessToken') or data.get('access_token')
        self._token_expires = datetime.now() + timedelta(seconds=auth.token_expires_in)
        
        logger.info(f"[{self.connector_id}] OAuth token refreshed")

    # -------------------------------------------------------------------------
    # Request building (can be overridden by subclasses)
    # -------------------------------------------------------------------------
    
    def _build_url(self, endpoint: EndpointConfig) -> str:
        """Build full URL. Override for dynamic path parameters."""
        base = self.cfg.base_url.rstrip('/')
        path = endpoint.path.lstrip('/')
        return f"{base}/{path}"
    
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
        elif pag.type == 'cursor' and cursor:
            params[pag.cursor_param] = cursor
        elif pag.type == 'page' and cursor:
            params[pag.page_param] = cursor

        # Time-based cursor
        if endpoint.use_time_cursor and cursor:
            params[endpoint.time_param] = cursor

        return params
    
    def _build_headers(self, endpoint: EndpointConfig) -> Dict[str, str]:
        """Build request headers."""
        headers = dict(endpoint.headers)
        
        if self._access_token:
            headers['Authorization'] = f'Bearer {self._access_token}'
        
        return headers
    
    def _make_request(
        self, 
        endpoint: EndpointConfig, 
        url: str, 
        params: Dict[str, Any],
        headers: Dict[str, str]
    ) -> requests.Response:
        """Make HTTP request. Override for custom request logic."""
        if endpoint.method.upper() == 'POST':
            return self._session.post(  # type: ignore
                url,
                params=params,
                json=endpoint.body,
                headers=headers,
                timeout=self.cfg.timeout
            )
        else:
            return self._session.get(  # type: ignore
                url,
                params=params,
                headers=headers,
                timeout=self.cfg.timeout
            )

    # -------------------------------------------------------------------------
    # Response processing (can be overridden by subclasses)
    # -------------------------------------------------------------------------
    
    def _extract_records(self, data: Any, endpoint: EndpointConfig) -> List[Dict]:
        """Extract records from response using data_path."""
        if not endpoint.data_path:
            if isinstance(data, list):
                return data
            return [data] if isinstance(data, dict) else []
        
        # Navigate nested path
        result = data
        for key in endpoint.data_path.split('.'):
            if isinstance(result, dict):
                result = result.get(key, [])
            else:
                return []
        
        return result if isinstance(result, list) else [result]
    
    def _transform_records(
        self, 
        records: List[Dict], 
        endpoint: EndpointConfig
    ) -> List[Dict]:
        """Transform records. Override for API-specific transformations."""
        return records
    
    def _build_metadata(
        self, 
        item_id: str, 
        records: List[Dict], 
        endpoint: EndpointConfig
    ) -> Dict[str, Any]:
        """Build envelope metadata."""
        return {
            'endpoint_id': item_id,
            'record_count': len(records),
            'cursor': self._cursors.get(item_id),
        }

    # -------------------------------------------------------------------------
    # Cursor management
    # -------------------------------------------------------------------------
    
    def _get_endpoint(self, endpoint_id: str) -> Optional[EndpointConfig]:
        """Find endpoint by ID."""
        for ep in self.cfg.endpoints:
            if ep.id == endpoint_id:
                return ep
        return None
    
    def _rate_limit(self) -> None:
        """Enforce rate limiting."""
        if self.cfg.rate_limit <= 0:
            return
        min_interval = 1.0 / self.cfg.rate_limit
        elapsed = time.time() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()
    
    def _update_cursor(
        self, 
        item_id: str, 
        data: Any, 
        records: List[Dict], 
        endpoint: EndpointConfig
    ) -> None:
        """Update cursor based on response."""
        if endpoint.pagination.type == 'cursor':
            # Extract next cursor from response
            next_cursor = data
            for key in endpoint.pagination.next_cursor_path.split('.'):
                if isinstance(next_cursor, dict):
                    next_cursor = next_cursor.get(key)
            if next_cursor:
                self._cursors[item_id] = next_cursor
                
        elif endpoint.pagination.type == 'offset':
            current = self._cursors.get(item_id, 0)
            self._cursors[item_id] = current + len(records)
            
        elif endpoint.use_time_cursor and records and endpoint.timestamp_field:
            # Get max timestamp from records
            timestamps = [r.get(endpoint.timestamp_field) for r in records if r.get(endpoint.timestamp_field)]
            if timestamps:
                self._cursors[item_id] = max(timestamps) # type: ignore
    
    def _load_cursors(self) -> None:
        """Load cursors from file."""
        if not self.cfg.cursor_file:
            return
        try:
            if os.path.exists(self.cfg.cursor_file):
                with open(self.cfg.cursor_file, 'r') as f:
                    self._cursors = json.load(f)
                logger.debug(f"[{self.connector_id}] Loaded cursors: {self._cursors}")
        except Exception as e:
            logger.warning(f"[{self.connector_id}] Failed to load cursors: {e}")
    
    def _save_cursors(self) -> None:
        """Save cursors to file."""
        if not self.cfg.cursor_file:
            return
        try:
            os.makedirs(os.path.dirname(self.cfg.cursor_file), exist_ok=True)
            with open(self.cfg.cursor_file, 'w') as f:
                json.dump(self._cursors, f)
        except Exception as e:
            logger.warning(f"[{self.connector_id}] Failed to save cursors: {e}")
    
    def _save_cursor_to_db(self, envelope: InputEnvelope) -> None:
        """Save cursor to database."""
        if not self._db_connection:
            return
        
        endpoint_id = envelope.metadata.get('endpoint_id')
        device_id = envelope.hint_device_id
        cursor_value = envelope.metadata.get('cursor')
        
        if not all([endpoint_id, device_id, cursor_value]):
            return
        
        try:
            cursor = self._db_connection.cursor()
            cursor.execute("""
                INSERT INTO api_fetch_cursor 
                    (connector_id, endpoint_id, device_id, last_fetch_timestamp, fetch_count)
                VALUES (%s, %s, %s, %s, 1)
                ON CONFLICT (connector_id, endpoint_id, device_id)
                DO UPDATE SET
                    last_fetch_timestamp = EXCLUDED.last_fetch_timestamp,
                    last_fetch_success = NOW(),
                    fetch_count = api_fetch_cursor.fetch_count + 1,
                    updated_at = NOW()
            """, (self.connector_id, endpoint_id, device_id, cursor_value))
            self._db_connection.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"[{self.connector_id}] Failed to save cursor to DB: {e}")
    
    def _compute_input_id(self, item_id: str, records: List[Dict]) -> str:
        """Compute unique input ID."""
        ts = datetime.now().isoformat()
        return f"{self.connector_id}:{item_id}:{ts}:{len(records)}"
