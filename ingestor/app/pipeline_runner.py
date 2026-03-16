"""
Pipeline runner that processes InputEnvelopes.
"""

import logging
import uuid
from typing import Optional

import yaml

from shared import get_connection
from connectors.base import InputEnvelope
from pipeline import DataPipeline, PipelineMetrics
from dao import DAOFactory

logger = logging.getLogger(__name__)


class DuplicateInputError(Exception):
    """Raised when input was already processed."""
    pass


class PipelineRunner:
    """Processes InputEnvelopes through the pipeline."""
    
    def __init__(self, db_dsn: str = None):
        """
        Initialize pipeline runner.
        
        Args:
            db_dsn: Database DSN (deprecated, uses shared.get_connection)
        """
        # db_dsn is kept for backward compatibility but not used
        self.db_dsn = db_dsn
    
    def run(self, envelope: InputEnvelope) -> PipelineMetrics:
        """Execute pipeline for envelope."""
        with get_connection() as conn:
            dao = DAOFactory(conn)

            try:
                # Check duplicates
                sha256 = envelope.metadata.get('sha256')
                if sha256 and dao.ingest_batch.exists_by_sha256(sha256):
                    raise DuplicateInputError(envelope.input_id)
                
                # Load mapping
                mapping = self._load_mapping(envelope.hint_mapping)
                if not mapping:
                    raise ValueError(f"No mapping for {envelope.source_uri}")
                
                # Resolve datasource (external_id -> internal id)
                datasource_id = dao.datasource.resolve_id(envelope.hint_datasource_id or 'unknown')

                # Register batch
                batch_id = dao.ingest_batch.register(
                    source_type=envelope.content_type,
                    source_name=envelope.metadata.get('file_name', envelope.source_uri),
                    datasource_id=datasource_id,
                    granularity=envelope.hint_granularity,
                    date_range_start=envelope.metadata.get('start_date'),
                    date_range_end=envelope.metadata.get('end_date'),
                    file_sha256=sha256,
                )
                dao.commit()

                # Build context
                source_context = {
                    'source_type': envelope.content_type,
                    'source_batch_id': batch_id,
                    'source_api_endpoint': envelope.source_uri,
                    'datasource_id': datasource_id,
                    'ingestion_method': envelope.content_type,
                }

                # Run pipeline
                pipeline = DataPipeline(conn, mapping, source_context)
                metrics = pipeline.execute(envelope.content)

                # Update batch with metrics and status
                dao.ingest_batch.update_status(batch_id, 'completed', metrics.load_records, metrics.invalid_records)
                quality = (
                    round(metrics.valid_records / metrics.extract_records * 100, 2)
                    if metrics.extract_records > 0 else 0
                )
                dao.ingest_batch.update_metrics(
                    batch_id=batch_id,
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
