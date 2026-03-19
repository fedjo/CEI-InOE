"""
FusionSolar API connector.
Fetches station KPIs (hourly/daily/monthly) from Huawei FusionSolar.
"""

import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from pydantic import BaseModel

from .http_connector import HttpConnector, EndpointConfig, HttpConnectorConfig
from .base import BaseConnector, ConnectorStatus, InputEnvelope

logger = logging.getLogger(__name__)

# FusionSolar API field → DB column mapping
FIELD_MAP = {
    'PVYield': 'pv_yield_kwh',
    'inverterYield': 'inverter_yield_kwh',
    'inverter_power': 'inverter_power_kw',
    'ongrid_power': 'ongrid_power_kwh',
    'buyPower': 'buy_power_kwh',
    'use_power': 'use_power_kwh',
    'selfUsePower': 'self_use_power_kwh',
    'selfProvide': 'self_provide_pct',
    'perpower_ratio': 'perpower_ratio',
    'installed_capacity': 'installed_capacity_kwp',
    'power_profit': 'power_profit',
    'reduction_total_co2': 'reduction_total_co2',
    'reduction_total_coal': 'reduction_total_coal',
    'reduction_total_tree': 'reduction_total_tree',
}

GRANULARITY_MAP = {
    'hourly': ('getKpiStationHour', 'solar_hourly'),
    'daily': ('getKpiStationDay', 'solar_daily'),
    'monthly': ('getKpiStationMonth', 'solar_monthly'),
}


class FusionSolarConnectorConfig(BaseModel):
    type: str = "fusionsolar"
    base_url: str = "https://intl.fusionsolar.huawei.com/thirdData"
    user_name: str = ""
    system_code: str = ""
    schedule_seconds: int = 3600
    lookback_days: int = 7
    timeout: int = 30
    max_retries: int = 3
    enabled: bool = True
    mappings_dir: str = "/app/mappings"
    granularities: List[str] = ["daily"]


class FusionSolarConnector(BaseConnector):
    """
    Connector for Huawei FusionSolar Northbound API.

    Uses session-based auth (login → XSRF-TOKEN cookie).
    Discovers station codes, then fetches KPIs per granularity.

    Rate-limiting: all API calls are throttled to respect FusionSolar's
    ~10 s per-interface limit.  Station codes are batched into a single
    call per granularity and cached so that multiple datasources don't
    trigger extra requests.
    """

    _MIN_REQUEST_INTERVAL = 15.0  # seconds between API calls

    def __init__(self, connector_id: str, config: Dict[str, Any]):
        super().__init__(connector_id, config)
        self.cfg = FusionSolarConnectorConfig(**config)
        self._session: Optional[requests.Session] = None
        self._station_codes: List[str] = []
        self._datasources: List[Dict[str, Any]] = []
        self._api_cache: Dict[str, List[Dict]] = {}   # granularity → raw data points
        self._last_request_at: float = 0.0

    # ── Lifecycle ─────────────────────────────────────────────────

    def start(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
        })
        # Lazy login — first _ensure_logged_in() will handle it
        self._load_datasources()
        self.status = ConnectorStatus.RUNNING
        logger.info(f"[{self.connector_id}] Started with {len(self._datasources)} solar datasources")

    def stop(self) -> None:
        if self._session:
            self._session.close()
            self._session = None
        self.status = ConnectorStatus.STOPPED

    # ── Throttle ──────────────────────────────────────────────────

    def _throttle(self) -> None:
        """Enforce minimum interval between consecutive API requests."""
        elapsed = time.monotonic() - self._last_request_at
        if self._last_request_at and elapsed < self._MIN_REQUEST_INTERVAL:
            delay = self._MIN_REQUEST_INTERVAL - elapsed
            logger.debug(f"[{self.connector_id}] Throttling {delay:.1f}s")
            time.sleep(delay)

    def _post(self, path: str, payload: dict) -> requests.Response:
        """Throttled POST to the FusionSolar API."""
        self._throttle()
        resp = self._session.post(
            f"{self.cfg.base_url}/{path}",
            json=payload,
            timeout=self.cfg.timeout,
        )
        self._last_request_at = time.monotonic()
        resp.raise_for_status()
        return resp

    # ── Discover / Fetch ──────────────────────────────────────────

    def discover(self) -> List[str]:
        """
        Return work items as {external_id}:{granularity}.
        Clears the API cache so each cycle makes fresh requests.
        """
        self._api_cache = {}
        items = []
        for ds in self._datasources:
            if not ds.get('station_code'):
                continue
            for gran in self.cfg.granularities:
                items.append(f"{ds['external_id']}:{gran}")
        return items

    def _fetch_granularity_data(self, granularity: str) -> List[Dict]:
        """
        Fetch KPI data for ALL stations at once for *granularity*.
        The result is cached so that the second datasource for the same
        granularity does not hit the API again.
        """
        if granularity in self._api_cache:
            return self._api_cache[granularity]

        api_function, _ = GRANULARITY_MAP[granularity]

        # Batch every station code into one comma-separated value
        station_codes = ','.join(
            ds['station_code'] for ds in self._datasources
            if ds.get('station_code')
        )
        if not station_codes:
            self._api_cache[granularity] = []
            return []

        try:
            self._ensure_logged_in()
            collect_time = int(datetime.now(timezone.utc).timestamp()) * 1000

            resp = self._post(api_function, {
                'stationCodes': station_codes,
                'collectTime': collect_time,
            })
            body = resp.json()
            print(body)  # debug

            if not body.get('success', False):
                fail_code = body.get('failCode')
                if fail_code in (305, 306, 307):
                    self._login()
                    resp = self._post(api_function, {
                        'stationCodes': station_codes,
                        'collectTime': collect_time,
                    })
                    body = resp.json()
                if not body.get('success', False):
                    logger.warning(f"[{self.connector_id}] API error for {granularity}: {body}")
                    self._api_cache[granularity] = []
                    return []

            points = body.get('data') or []
            self._api_cache[granularity] = points
            logger.info(f"[{self.connector_id}] Fetched {len(points)} data points for {granularity}")
            return points

        except requests.RequestException as e:
            logger.error(f"[{self.connector_id}] API request failed for {granularity}: {e}")
            self._api_cache[granularity] = []
            return []

    def fetch(self, ext_id: str) -> Optional[InputEnvelope]:
        parts = ext_id.split(':')
        if len(parts) != 2:
            logger.error(f"[{self.connector_id}] Invalid item_id: {ext_id}")
            return None

        datasource_ext_id, granularity = parts
        if granularity not in GRANULARITY_MAP:
            logger.error(f"[{self.connector_id}] Unknown granularity: {granularity}")
            return None

        datasource = self._get_datasource(datasource_ext_id)
        if not datasource:
            logger.error(f"[{self.connector_id}] Unknown datasource: {datasource_ext_id}")
            return None

        station_code = datasource.get('station_code')
        ds_id = datasource.get('id')
        api_function, mapping_name = GRANULARITY_MAP[granularity]

        start_date, end_date = self._get_date_range(ds_id, ext_id, granularity)

        # One batched + cached API call per granularity
        all_points = self._fetch_granularity_data(granularity)

        # Filter to THIS station and date range
        start_ms = int(start_date.timestamp()) * 1000
        end_ms = int(end_date.timestamp()) * 1000
        all_records = []
        for point in all_points:
            if point.get('stationCode') and point['stationCode'] != station_code:
                continue
            record = self._transform_point(point)
            if record and start_ms <= record['ts'] <= end_ms:
                all_records.append(record)

        if not all_records:
            logger.info(f"[{self.connector_id}] No records from {ext_id}")
            return None

        all_records.sort(key=lambda r: r['ts'])

        sha = self._compute_sha256(ext_id, all_records)
        mapping_path = f"{self.cfg.mappings_dir}/api_{mapping_name}.yaml"

        envelope = InputEnvelope(
            connector_id=self.connector_id,
            input_id=sha,
            source_uri=f"{self.cfg.base_url}/{api_function}",
            received_at=datetime.now(timezone.utc),
            content=all_records,
            content_type="json",
            hint_mapping=mapping_path,
            hint_datasource_id=ds_id,
            hint_data_type='solar',
            hint_granularity=granularity,
            metadata={
                'endpoint_id': ext_id,
                'station_code': station_code,
                'granularity': granularity,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'record_count': len(all_records),
                'source_type': 'api',
                'sha256': sha,
            },
        )

        logger.info(f"[{self.connector_id}] {len(all_records)} {granularity} records for {datasource_ext_id}")
        return envelope

    # ── Ack / Fail ────────────────────────────────────────────────

    def ack(self, envelope: InputEnvelope) -> None:
        """Update cursor after successful processing."""
        ext_id = envelope.metadata.get('endpoint_id', '')
        ds_id = envelope.hint_datasource_id
        end_date_str = envelope.metadata.get('end_date')

        if ds_id and end_date_str:
            try:
                from shared import get_connection
                from dao import CursorDAO

                ts = datetime.fromisoformat(end_date_str)
                with get_connection() as conn:
                    CursorDAO(conn).update_cursor(
                        connector_id=self.connector_id,
                        endpoint_id=ext_id,
                        datasource_id=ds_id,
                        timestamp=ts,           # was last_fetch_timestamp — wrong kwarg
                    )
                    conn.commit()
            except Exception as e:
                logger.error(f"[{self.connector_id}] Cursor update failed: {e}", exc_info=True)

    def fail(self, envelope: InputEnvelope, error: Exception) -> None:
        logger.error(f"[{self.connector_id}] Processing failed for {envelope.input_id}: {error}")

    def health(self) -> Dict[str, Any]:
        return {
            'status': self.status.value,
            'datasources': len(self._datasources),
            'granularities': self.cfg.granularities,
        }

    # ── Auth ──────────────────────────────────────────────────────

    def _login(self) -> None:
        resp = self._post('login', {
            'userName': self.cfg.user_name,
            'systemCode': self.cfg.system_code,
        })
        body = resp.json()
        if not body.get('success', False):
            raise RuntimeError(f"FusionSolar login failed: {body}")
        xsrf = resp.cookies.get('XSRF-TOKEN')
        if xsrf:
            self._session.headers['XSRF-TOKEN'] = xsrf
        self._token_expiry = datetime.now(timezone.utc) + timedelta(minutes=20)
        logger.debug(f"[{self.connector_id}] Logged in")

    def _ensure_logged_in(self) -> None:
        if not hasattr(self, '_token_expiry') or datetime.now(timezone.utc) >= self._token_expiry:
            self._login()

    # ── Datasources ───────────────────────────────────────────────

    def _load_datasources(self) -> None:
        try:
            from shared import get_connection
            from dao import DatasourceDAO

            with get_connection() as conn:
                dao = DatasourceDAO(conn)
                rows = dao.get_solar_datasources()
                self._datasources = [
                    {
                        'id': r['id'],
                        'external_id': r['external_id'],
                        'station_code': r.get('station_code'),
                        'metadata': r.get('metadata', {}),
                    }
                    for r in rows
                ]
        except Exception as e:
            logger.error(f"[{self.connector_id}] Failed to load datasources: {e}")
            self._datasources = []

    def _get_datasource(self, external_id: str) -> Optional[Dict[str, Any]]:
        for d in self._datasources:
            if d['external_id'] == external_id:
                return d
        return None

    # ── Transform ─────────────────────────────────────────────────

    def _transform_point(self, point: Dict) -> Optional[Dict[str, Any]]:
        """Transform a single API data point to a flat record."""
        collect_time = point.get('collectTime')
        data = point.get('dataItemMap', {})
        if not collect_time or not data:
            return None

        record = {'ts': collect_time}  # epoch ms — coerced by mapping
        for api_field, db_col in FIELD_MAP.items():
            val = data.get(api_field)
            if val is not None:
                try:
                    record[db_col] = float(val)
                except (ValueError, TypeError):
                    pass
        return record

    # ── Cursor ────────────────────────────────────────────────────

    def _get_date_range(self, datasource_id: int, endpoint_id: str, granularity: str):
        end_date = datetime.now(timezone.utc)
        start_date = self._get_cursor_from_db(datasource_id, endpoint_id)

        if not start_date:
            start_date = end_date - timedelta(days=self.cfg.lookback_days)
            logger.info(f"[{self.connector_id}] No cursor for {endpoint_id}, using {self.cfg.lookback_days}d lookback")
        else:
            start_date = start_date + timedelta(hours=1)

        return start_date, end_date

    def _get_cursor_from_db(self, datasource_id: int, endpoint_id: str) -> Optional[datetime]:
        try:
            from shared import get_connection
            from dao import CursorDAO

            with get_connection() as conn:
                return CursorDAO(conn).get_cursor(
                    connector_id=self.connector_id,
                    endpoint_id=endpoint_id,
                    datasource_id=datasource_id,
                )
        except Exception as e:
            logger.error(f"[{self.connector_id}] Cursor query failed: {e}")
            return None

    # ── Helpers ────────────────────────────────────────────────────

    def _compute_sha256(self, item_id: str, records: List[Dict]) -> str:
        content = f"{item_id}:{len(records)}:{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
