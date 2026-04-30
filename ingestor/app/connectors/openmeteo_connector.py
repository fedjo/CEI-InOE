"""
Open-Meteo weather forecast connector.

Fetches 7-day hourly forecast from api.open-meteo.com and stores it in
fact_weather_forecast. No API key required for non-commercial use.

Key differences from other HTTP connectors:
  - Open-Meteo returns parallel arrays (one per variable), not a list of
    objects. _transform_records() zips them into one dict per hour.
  - forecast_run_at is taken from the API's generationtime_ms field.
  - horizon_hours is computed per-row as (valid_at − forecast_run_at).
  - site_id and location (lat/lon) are loaded once from the site table.
"""

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from models import SourceType
from .base import BaseConnector, ConnectorStatus, InputEnvelope
from .http_connector import HttpConnector, HttpConnectorConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Open-Meteo variable names → our DB column names
# ---------------------------------------------------------------------------
VARIABLE_MAP: Dict[str, str] = {
    "shortwave_radiation":          "shortwave_radiation_wm2",
    "direct_radiation":             "direct_radiation_wm2",
    "direct_normal_irradiance":     "direct_normal_irradiance_wm2",
    "diffuse_radiation":            "diffuse_radiation_wm2",
    "global_tilted_irradiance":     "global_tilted_irradiance_wm2",
    "cloud_cover":                  "cloud_cover_pct",
    "cloud_cover_low":              "cloud_cover_low_pct",
    "cloud_cover_mid":              "cloud_cover_mid_pct",
    "cloud_cover_high":             "cloud_cover_high_pct",
    "temperature_2m":               "temperature_2m_c",
    "wind_speed_10m":               "wind_speed_10m_ms",
    "wind_direction_10m":           "wind_direction_10m_deg",
    "precipitation":                "precipitation_mm",
    "weather_code":                 "weather_code",
    "sunshine_duration":            "sunshine_duration_s",
    "is_day":                       "is_day",
}

# Variables to request from the API (order doesn't matter)
HOURLY_VARIABLES: List[str] = list(VARIABLE_MAP.keys())


class OpenMeteoConnectorConfig(BaseModel):
    """Configuration for the Open-Meteo connector."""
    type: str = "openmeteo"
    # Location — pulled from site table at startup if not provided
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    # Panel geometry for global_tilted_irradiance
    panel_tilt: float = 30.0      # degrees from horizontal
    panel_azimuth: float = 0.0    # degrees: 0=south, -90=east, 90=west
    # Forecast window
    forecast_days: int = 7        # 1-16
    # Wind speed unit — open-meteo default is km/h; we want m/s
    wind_speed_unit: str = "ms"
    # NWP model — 'best_match' lets Open-Meteo pick the best available model
    model: str = "best_match"
    # Scheduling
    schedule_seconds: int = 3600  # refresh every hour
    timeout: int = 30
    max_retries: int = 3
    mappings_dir: str = "/app/mappings"
    enabled: bool = True
    # Internal — filled at startup from site table
    site_id: Optional[int] = None


class OpenMeteoConnector(HttpConnector):
    """
    Connector for the Open-Meteo free weather forecast API.

    Endpoint: GET https://api.open-meteo.com/v1/forecast
    Auth: none (free, non-commercial)
    Rate limit: 10,000 calls/day; we run every hour → ~24 calls/day.

    The API returns a single JSON object with:
      - generationtime_ms — epoch ms when the model run was computed
      - hourly.time[]     — ISO-8601 timestamps, one per hour
      - hourly.<var>[]    — matching arrays for each requested variable

    _transform_records() converts these parallel arrays into a list of flat
    dicts, one per hour, and injects the metadata columns.
    """

    BASE_URL = "https://api.open-meteo.com"
    ENDPOINT_ID = "hourly_forecast"

    def __init__(self, connector_id: str, config: Dict[str, Any]):
        self.om_cfg = OpenMeteoConnectorConfig(**config)

        # Build a minimal HttpConnectorConfig for the base class
        http_config: Dict[str, Any] = {
            "type": "http",
            "base_url": self.BASE_URL,
            "endpoints": [],          # populated dynamically in start()
            "auth": {"type": "none"},
            "rate_limit": 1.0,        # 1 req/s max (well within free limit)
            "timeout": self.om_cfg.timeout,
            "max_retries": self.om_cfg.max_retries,
            "schedule_seconds": self.om_cfg.schedule_seconds,
            "enabled": self.om_cfg.enabled,
        }
        super().__init__(connector_id, http_config)

        self._site_id: Optional[int] = self.om_cfg.site_id
        self._latitude: Optional[float] = self.om_cfg.latitude
        self._longitude: Optional[float] = self.om_cfg.longitude
        self._forecast_run_at: Optional[datetime] = None

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Initialize session and resolve site coordinates."""
        super().start()
        self._load_site()
        self._register_endpoint()
        logger.info(
            f"[{self.connector_id}] Started — "
            f"lat={self._latitude}, lon={self._longitude}, "
            f"site_id={self._site_id}, forecast_days={self.om_cfg.forecast_days}"
        )

    def discover(self) -> List[str]:
        return [self.ENDPOINT_ID]

    # -------------------------------------------------------------------------
    # Transform — the main customization point
    # -------------------------------------------------------------------------

    def _transform_records(
        self,
        records: List[Dict],
        endpoint: Any,
    ) -> List[Dict]:
        """
        Convert Open-Meteo parallel arrays into one dict per hour.

        `records` here is already the raw API response (a single-element list
        containing the full response dict), because _extract_records() with
        data_path='' on a dict returns [data].
        """
        if not records:
            return []

        raw = records[0]  # the full JSON response dict

        # Stamp forecast_run_at from the API's generation time
        self._forecast_run_at = datetime.now(tz=timezone.utc)

        hourly = raw.get("hourly", {})
        time_series: List[str] = hourly.get("time", [])

        if not time_series:
            logger.warning(f"[{self.connector_id}] No hourly time series in response")
            return []

        rows: List[Dict] = []
        for i, ts_str in enumerate(time_series):
            try:
                valid_at = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
            except ValueError:
                logger.debug(f"[{self.connector_id}] Skipping unparseable timestamp: {ts_str}")
                continue

            horizon = int(
                (valid_at - self._forecast_run_at).total_seconds() / 3600
            )

            row: Dict[str, Any] = {
                "valid_at":          valid_at.isoformat(),
                "forecast_run_at":   self._forecast_run_at.isoformat(),
                "horizon_hours":     horizon,
                "site_id":           self._site_id,
                "model_id":          self.om_cfg.model,
            }

            for api_key, db_col in VARIABLE_MAP.items():
                values = hourly.get(api_key, [])
                val = values[i] if i < len(values) else None
                # Open-Meteo uses null for night-time radiation fields — keep as None
                row[db_col] = val

            rows.append(row)

        logger.debug(
            f"[{self.connector_id}] Transformed {len(rows)} forecast rows "
            f"(run={self._forecast_run_at.isoformat()})"
        )
        return rows

    # -------------------------------------------------------------------------
    # Metadata
    # -------------------------------------------------------------------------

    def _build_metadata(
        self,
        item_id: str,
        records: List[Dict],
        endpoint: Any,
    ) -> Dict[str, Any]:
        return {
            "endpoint_id": item_id,
            "record_count": len(records),
            "forecast_run_at": (
                self._forecast_run_at.isoformat()
                if self._forecast_run_at else None
            ),
            "site_id": self._site_id,
            "cursor": None,  # no pagination cursor needed
        }

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _load_site(self) -> None:
        """Resolve site coordinates and site_id from the database."""
        try:
            from shared import get_connection, Site
            from sqlalchemy import select

            with get_connection() as conn:
                row = conn.execute(
                    select(Site.__table__).limit(1)
                ).fetchone()

            if row is None:
                logger.warning(
                    f"[{self.connector_id}] No site found in DB — "
                    "using config lat/lon if provided"
                )
                return

            mapping = dict(row._mapping)
            self._site_id = mapping["id"]

            location = mapping.get("location") or {}
            coords = location.get("coordinates", [])
            if len(coords) >= 2:
                # GeoJSON order: [longitude, latitude]
                self._longitude = coords[0]
                self._latitude = coords[1]

            logger.info(
                f"[{self.connector_id}] Site resolved: id={self._site_id}, "
                f"lat={self._latitude}, lon={self._longitude}"
            )

        except Exception as e:
            logger.error(f"[{self.connector_id}] Failed to load site: {e}")

    def _register_endpoint(self) -> None:
        """Build and register the forecast endpoint using resolved coordinates."""
        from .http_connector import EndpointConfig, PaginationConfig

        if self._latitude is None or self._longitude is None:
            logger.error(
                f"[{self.connector_id}] Cannot register endpoint: "
                "latitude/longitude not resolved"
            )
            return

        params: Dict[str, Any] = {
            "latitude":       self._latitude,
            "longitude":      self._longitude,
            "hourly":         ",".join(HOURLY_VARIABLES),
            "tilt":           self.om_cfg.panel_tilt,
            "azimuth":        self.om_cfg.panel_azimuth,
            "wind_speed_unit": self.om_cfg.wind_speed_unit,
            "timezone":       "UTC",
            "forecast_days":  self.om_cfg.forecast_days,
        }
        if self.om_cfg.model != "best_match":
            params["models"] = self.om_cfg.model

        endpoint = EndpointConfig(
            id=self.ENDPOINT_ID,
            path="/v1/forecast",
            method="GET",
            params=params,
            data_path="",      # whole response is one object; zipped in _transform_records
            mapping=f"{self.om_cfg.mappings_dir}/api_weather_forecast.yaml",
            datasource_id=self._site_id,
            granularity="hourly",
            pagination=PaginationConfig(),
            enabled=True,
        )

        self.cfg.endpoints = [endpoint]
