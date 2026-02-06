"""
CEI-InOE Ingestor - Main Entry Point
Connector-based architecture with APScheduler.
"""

import logging
import os
import signal
import sys
from queue import Empty, Queue
from threading import Event, Thread
from typing import Dict, List

from apscheduler.schedulers.background import BackgroundScheduler

from connectors import BaseConnector, InputEnvelope, create_connector
from pipeline_runner import DuplicateInputError, PipelineRunner

# ============================================================================
# Logging
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

DB_DSN = os.environ.get('DB_DSN', '')

CONNECTOR_CONFIGS = {
    'file_watcher': {
        'type': 'file',
        'watch_dir': os.environ.get('WATCH_DIR', '/data/incoming'),
        'processed_dir': os.environ.get('PROCESSED_DIR', '/data/processed'),
        'rejected_dir': os.environ.get('REJECTED_DIR', '/data/rejected'),
        'mappings_dir': os.environ.get('MAPPINGS_DIR', '/app/mappings'),
        'schedule_seconds': int(os.environ.get('FILE_POLL_INTERVAL', '5')),
        'stable_seconds': 3,
    },
    
    # Example HTTP connector (uncomment and configure)
    # 'energy_api': {
    #     'type': 'http',
    #     'base_url': os.environ.get('ENERGY_API_URL', 'https://api.example.com'),
    #     'schedule_seconds': 300,
    #     'auth': {
    #         'type': 'api_key',
    #         'api_key_name': 'X-API-Key',
    #         'api_key_value': os.environ.get('ENERGY_API_KEY', ''),
    #     },
    #     'endpoints': [
    #         {
    #             'id': 'energy_hourly',
    #             'path': '/v1/readings',
    #             'data_path': 'data.readings',
    #             'mapping': '/app/mappings/energy_hourly.yaml',
    #             'device_id': 'energy_meter_1',
    #             'granularity': 'hourly',
    #             'use_time_cursor': True,
    #             'time_param': 'since',
    #             'timestamp_field': 'timestamp',
    #         },
    #     ],
    # },
}

# ============================================================================
# Worker Pool
# ============================================================================

class WorkerPool:
    """Pool of workers processing InputEnvelopes."""
    
    def __init__(self, num_workers: int, queue: Queue, runner: PipelineRunner,
                 connectors: Dict[str, BaseConnector]):
        self.queue = queue
        self.runner = runner
        self.connectors = connectors
        self.shutdown_event = Event()
        self.workers: List[Thread] = [
            Thread(target=self._worker_loop, name=f"worker-{i}", daemon=True)
            for i in range(num_workers)
        ]
    
    def start(self):
        """Start workers."""
        for t in self.workers:
            t.start()
        logger.info(f"Started {len(self.workers)} workers")
    
    def stop(self):
        """Stop workers."""
        self.shutdown_event.set()
        for t in self.workers:
            t.join(timeout=5)
        logger.info("Workers stopped")
    
    def _worker_loop(self):
        """Worker main loop."""
        while not self.shutdown_event.is_set():
            try:
                envelope: InputEnvelope = self.queue.get(timeout=1)
            except Empty:
                continue
            
            connector = self.connectors.get(envelope.connector_id)
            if not connector:
                logger.error(f"Unknown connector: {envelope.connector_id}")
                continue
            
            try:
                metrics = self.runner.run(envelope)
                connector.ack(envelope)
                logger.info(
                    f"✓ {envelope.source_uri}: "
                    f"{metrics.load_records} loaded, {metrics.invalid_records} invalid"
                )
            except DuplicateInputError:
                connector.ack(envelope)
                logger.info(f"⊘ Duplicate: {envelope.source_uri}")
            except Exception as e:
                connector.fail(envelope, str(e))
                logger.error(f"✗ {envelope.source_uri}: {e}")
            finally:
                self.queue.task_done()


# ============================================================================
# Scheduler
# ============================================================================

def make_discover_job(connector: BaseConnector, queue: Queue):
    """Create discover job for connector."""
    def job():
        try:
            for item_id in connector.discover():
                envelope = connector.fetch(item_id)
                if envelope:
                    queue.put(envelope)
        except Exception as e:
            logger.error(f"[{connector.connector_id}] Discover error: {e}")
    return job


# ============================================================================
# Application
# ============================================================================

class IngestorApp:
    """Main application."""
    
    def __init__(self):
        self.connectors: Dict[str, BaseConnector] = {}
        self.queue: Queue = Queue(maxsize=100)
        self.scheduler = BackgroundScheduler()
        self.runner = PipelineRunner(DB_DSN)
        self.worker_pool: WorkerPool = None # type: ignore
        self.shutdown_event = Event()
    
    def setup(self):
        """Initialize components."""
        logger.info("=" * 60)
        logger.info("CEI-InOE Ingestor")
        logger.info("=" * 60)
        
        # Create connectors
        for conn_id, config in CONNECTOR_CONFIGS.items():
            if not config.get('enabled', True):
                continue
            
            connector = create_connector(conn_id, config)
            connector.start()
            self.connectors[conn_id] = connector
            
            # Schedule discovery
            interval = config.get('schedule_seconds', 60)
            self.scheduler.add_job(
                make_discover_job(connector, self.queue),
                'interval',
                seconds=interval,
                id=f"discover_{conn_id}",
                max_instances=1,
            )
            logger.info(f"Registered: {conn_id} (every {interval}s)")
        
        # Create workers
        num_workers = int(os.environ.get('NUM_WORKERS', '2'))
        self.worker_pool = WorkerPool(num_workers, self.queue, self.runner, self.connectors)
    
    def run(self):
        """Start application."""
        self.setup()
        
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        self.scheduler.start()
        self.worker_pool.start()
        
        logger.info("Running. Ctrl+C to stop.")
        self.shutdown_event.wait()
        self._shutdown()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Signal {signum}, shutting down...")
        self.shutdown_event.set()
    
    def _shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down...")
        self.scheduler.shutdown(wait=False)
        self.queue.join()
        self.worker_pool.stop()
        for c in self.connectors.values():
            c.stop()
        logger.info("Done.")


def main():
    """Entry point."""
    app = IngestorApp()
    app.run()


if __name__ == "__main__":
    main()