"""
Pipeline runner that processes InputEnvelopes.
"""

import logging
import uuid
from typing import Optional

import psycopg2
import yaml

from connectors.base import InputEnvelope
from pipeline import DataPipeline, PipelineMetrics
from dao import DAOFactory

logger = logging.getLogger(__name__)


class DuplicateInputError(Exception):
    """Raised when input was already processed."""
    pass


class PipelineRunner:
    """Processes InputEnvelopes through the pipeline."""
    
    def __init__(self, db_dsn: str):
        self.db_dsn = db_dsn
    
    def run(self, envelope: InputEnvelope) -> PipelineMetrics:
        """Execute pipeline for envelope."""
        conn = psycopg2.connect(self.db_dsn)
        dao = DAOFactory(conn)

        try:
            # Check duplicates
            sha256 = envelope.metadata.get('sha256')
            if sha256 and dao.ingest_file.exists_by_sha256(sha256):
                raise DuplicateInputError(envelope.input_id)
            
            # Load mapping
            mapping = self._load_mapping(envelope.hint_mapping)
            if not mapping:
                raise ValueError(f"No mapping for {envelope.source_uri}")
            
            # Resolve device
            device_id = dao.device.get_by_device_id(envelope.hint_device_id or 'unknown')
            
            # Register input
            file_id = dao.ingest_file.register(
                file_name=envelope.metadata.get('file_name', envelope.source_uri),
                device_id=device_id,
                granularity=envelope.hint_granularity,
                start_date=envelope.metadata.get('start_date'),
                end_date=envelope.metadata.get('end_date'),
                sha256=sha256,
            )
            dao.commit()
            
            # Build context
            source_context = {
                'source_type': envelope.metadata.get('source_type'),
                'source_file': file_id,
                'source_api_endpoint': envelope.source_uri,
                'device_id': device_id,
                'ingestion_method': envelope.content_type,
            }

            # Run pipeline
            pipeline = DataPipeline(conn, mapping, source_context)
            metrics = pipeline.execute(envelope.content)
            
            # Update record with metrics
            quality = (
                round(metrics.valid_records / metrics.extract_records * 100, 2)
                if metrics.extract_records > 0 else 0
            )
            dao.ingest_file.update_metrics(
                file_id=file_id,
                execution_time_ms=int(metrics.total_duration * 1000),
                validation_status='passed' if metrics.invalid_records == 0 else 'partial',
                quality_score=quality,
            )
            dao.commit()
            
            return metrics
            
        except DuplicateInputError:
            raise
        except Exception:
            dao.rollback()
            raise
        finally:
            conn.close()
    
    def _load_mapping(self, path: Optional[str]) -> Optional[dict]:
        """Load YAML mapping."""
        if not path:
            return None
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load mapping {path}: {e}")
            return None
