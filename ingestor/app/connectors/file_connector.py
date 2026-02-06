"""
File-based connector for CSV and Excel files.
"""

import hashlib
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel

from .base import BaseConnector, ConnectorStatus, InputEnvelope

logger = logging.getLogger(__name__)


class FileConnectorConfig(BaseModel):
    """Configuration for file connector."""
    type: str = "file"
    watch_dir: str = "/data/incoming"
    processed_dir: str = "/data/processed"
    rejected_dir: str = "/data/rejected"
    mappings_dir: str = "/app/mappings"
    stable_seconds: int = 3
    schedule_seconds: int = 10
    enabled: bool = True


class FileConnector(BaseConnector):
    """
    Connector for CSV and Excel files.
    Polls directory and emits envelopes.
    """
    
    SUPPORTED_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
    
    def __init__(self, connector_id: str, config: Dict[str, Any]):
        super().__init__(connector_id, config)
        
        # Parse config with defaults
        cfg = FileConnectorConfig(**config)
        self.watch_dir = cfg.watch_dir
        self.processed_dir = cfg.processed_dir
        self.rejected_dir = cfg.rejected_dir
        self.mappings_dir = cfg.mappings_dir
        self.stable_seconds = cfg.stable_seconds
        
        self._in_progress: set = set()
    
    def start(self) -> None:
        """Ensure directories exist."""
        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.rejected_dir, exist_ok=True)
        self.status = ConnectorStatus.RUNNING
        logger.info(f"[{self.connector_id}] Watching {self.watch_dir}")
    
    def stop(self) -> None:
        """Stop connector."""
        self.status = ConnectorStatus.STOPPED
        logger.info(f"[{self.connector_id}] Stopped")
    
    def discover(self) -> List[str]:
        """Find stable files ready for processing."""
        discovered = []
        
        try:
            for fname in os.listdir(self.watch_dir):
                path = os.path.join(self.watch_dir, fname)
                ext = os.path.splitext(fname)[1].lower()
                
                if ext not in self.SUPPORTED_EXTENSIONS:
                    continue
                if path in self._in_progress:
                    continue
                if not os.path.isfile(path):
                    continue
                if self._is_stable(path):
                    discovered.append(path)
                    
        except Exception as e:
            logger.error(f"[{self.connector_id}] Discovery error: {e}")
            self._last_error = str(e)

        return discovered

    def fetch(self, item_id: str) -> Optional[InputEnvelope]:
        """Read file and create InputEnvelope."""
        path = item_id
        fname = os.path.basename(path)

        try:
            self._in_progress.add(path)
            
            # Compute idempotency key
            file_hash = self._file_sha256(path)
            input_id = f"{fname}:{file_hash}"
            
            # Detect content type
            ext = os.path.splitext(fname)[1].lower()
            content_type = "excel" if ext in {'.xlsx', '.xls'} else "csv"
            
            # Read content
            content = self._read_file(path, content_type)
            if content is None:
                self._in_progress.discard(path)
                return None
            
            # Detect hints
            hints = self._detect_hints(path, content_type)
            
            envelope = InputEnvelope(
                connector_id=self.connector_id,
                input_id=input_id,
                source_uri=path,
                received_at=datetime.now(),
                content=content,
                content_type=content_type,
                hint_mapping=hints.get('mapping'),
                hint_device_id=hints.get('device_id'),
                hint_granularity=hints.get('granularity'),
                metadata={
                    'file_name': fname,
                    'file_size': os.path.getsize(path),
                    'sha256': file_hash,
                    'start_date': str(hints.get('start_date')) if hints.get('start_date') else None,
                    'end_date': str(hints.get('end_date')) if hints.get('end_date') else None,
                }
            )

            logger.info(f"[{self.connector_id}] Fetched {fname} ({len(content)} records)")
            return envelope
            
        except Exception as e:
            logger.error(f"[{self.connector_id}] Fetch error {fname}: {e}")
            self._last_error = str(e)
            self._in_progress.discard(path)
            return None
    
    def ack(self, envelope: InputEnvelope) -> None:
        """Move file to processed directory."""
        path = envelope.source_uri
        fname = os.path.basename(path)
        dest = os.path.join(self.processed_dir, fname)
        
        try:
            if os.path.exists(path):
                os.rename(path, dest)
                logger.info(f"[{self.connector_id}] ✓ {fname} → processed/")
        except Exception as e:
            logger.error(f"[{self.connector_id}] Ack error: {e}")
        finally:
            self._in_progress.discard(path)
    
    def fail(self, envelope: InputEnvelope, error: str) -> None:
        """Move file to rejected directory."""
        path = envelope.source_uri
        fname = os.path.basename(path)
        dest = os.path.join(self.rejected_dir, fname)
        
        try:
            if os.path.exists(path):
                os.rename(path, dest)
                logger.error(f"[{self.connector_id}] ✗ {fname} → rejected/ ({error})")
        except Exception as e:
            logger.error(f"[{self.connector_id}] Fail error: {e}")
        finally:
            self._in_progress.discard(path)
    
    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    
    def _is_stable(self, path: str) -> bool:
        """Check file size stability."""
        try:
            s1 = os.path.getsize(path)
            time.sleep(self.stable_seconds)
            return s1 == os.path.getsize(path)
        except OSError:
            return False
    
    def _file_sha256(self, path: str) -> str:
        """Calculate SHA256 hash."""
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def _read_file(self, path: str, content_type: str) -> Optional[List[Dict]]:
        """Read file as list of dicts."""
        try:
            if content_type == "excel":
                df = pd.read_excel(path)
            else:
                df = pd.read_csv(path, encoding='utf-8-sig')
            
            df = df.dropna(how='all')
            df = df.replace({pd.NA: None, float('nan'): None})
            
            # Remove summary rows
            if 'Date' in df.columns:
                df = df[~df['Date'].astype(str).str.upper().isin(
                    ['AVG', 'SUM', 'TOTAL', 'AVERAGE']
                )]
            
            # Convert timestamps
            for col in df.select_dtypes(include=['datetime64']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

            # 4. Convert all datetime objects in the DataFrame to ISO strings for JSON serialization
            for col in df.columns:
                df[col] = df[col].apply(
                    lambda x: x.isoformat() if isinstance(x, (datetime, pd.Timestamp)) else x
                )
            
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"[{self.connector_id}] Read error: {e}")
            return None
    
    def _detect_hints(self, path: str, content_type: str) -> Dict[str, Any]:
        """Detect dataset type and metadata."""
        fname = os.path.basename(path)
        
        try:
            if content_type == "excel":
                df = pd.read_excel(path, nrows=5)
            else:
                df = pd.read_csv(path, nrows=5, encoding='utf-8-sig')
            
            cols = {c.lower() for c in df.columns}
            
            # Detect by columns
            if (cols & {'pm10', 'pm2p5', 'humidity', 'temperature', 'atm_pressure'} or \
                any ('µg/m' in c or 'hpa' in c.lower() for c in df.columns)):
                mapping, device_id = 'environmental_metrics', 'testweather2'
            elif (cols & {'nr. animals', 'feed efficiency', 'rumination'} or \
                any('production' in c and 'cow' in c for c in cols)):
                mapping, device_id = 'dairy_production', 'lelyna'
            elif {'date and time'} & cols and len(df.columns) == 2:
                second_col = [c for c in df.columns if c.lower() != 'date and time'][0]
                mapping = 'energy_hourly' if 'hourly' in second_col.lower() else 'energy_daily'
                m = re.match(r'^([a-f0-9]{20,})-', fname)
                device_id = m.group(1) if m else None
            else:
                mapping, device_id = None, None
            
            granularity, start_date, end_date = self._detect_time_info(path, content_type)
            
            return {
                'mapping': f"{self.mappings_dir}/{mapping}.yaml" if mapping else None,
                'device_id': device_id,
                'granularity': granularity or 'daily',
                'start_date': start_date,
                'end_date': end_date,
            }
            
        except Exception as e:
            logger.warning(f"[{self.connector_id}] Hint detection failed: {e}")
            return {}
    
    def _detect_time_info(self, path: str, content_type: str):
        """Detect granularity and date range."""
        try:
            if content_type == "excel":
                df = pd.read_excel(path)
            else:
                df = pd.read_csv(path, encoding='utf-8-sig')
            
            ts_col = next(
                (c for c in df.columns if any(k in c.lower() for k in ['date', 'time', 'timestamp'])),
                None
            )
            
            if not ts_col or len(df) == 0:
                return None, None, None
            
            ts = pd.to_datetime(df[ts_col], errors='coerce').dropna()
            if len(ts) == 0:
                return None, None, None
            
            start_date = ts.min().date()
            end_date = ts.max().date()
            
            if len(ts) > 1:
                median_delta = ts.sort_values().diff().dropna().median().total_seconds()
                granularity = 'hourly' if median_delta < 7200 else 'daily'
            else:
                granularity = 'daily'
            
            return granularity, start_date, end_date
            
        except Exception:
            return None, None, None
