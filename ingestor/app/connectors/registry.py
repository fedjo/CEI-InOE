"""
Connector registry - maps types to classes.
"""

from typing import Dict, Type

from .base import BaseConnector
from .file_connector import FileConnector
from .http_connector import HttpConnector

CONNECTOR_TYPES: Dict[str, Type[BaseConnector]] = {
    'file': FileConnector,
    'csv': FileConnector,
    'excel': FileConnector,
    'http': HttpConnector,
    'api': HttpConnector,
    'rest': HttpConnector,
}


def create_connector(connector_id: str, config: Dict) -> BaseConnector:
    """Create connector from config."""
    connector_type = config.get('type', 'file')
    
    if connector_type not in CONNECTOR_TYPES:
        raise ValueError(f"Unknown connector type: {connector_type}")
    
    return CONNECTOR_TYPES[connector_type](connector_id, config)


def register_connector(type_name: str, cls: Type[BaseConnector]) -> None:
    """Register new connector type."""
    CONNECTOR_TYPES[type_name] = cls
