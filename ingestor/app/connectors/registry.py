"""
Connector registry for dynamic connector instantiation.
"""

from typing import Any, Dict, Optional, Type

from .base import BaseConnector
from .file_connector import FileConnector
from .http_connector import HttpConnector
from .airbeld_connector import AirbeldConnector


# Registry of connector types
CONNECTOR_TYPES: Dict[str, Type[BaseConnector]] = {
    'file': FileConnector,
    'http': HttpConnector,
    'airbeld': AirbeldConnector,
    # Future connectors:
    # 'cyric': CyricConnector,
    # 'mqtt': MqttConnector,
}


def create_connector(
    connector_id: str, 
    config: Dict[str, Any],
    db_connection=None
) -> Optional[BaseConnector]:
    """
    Create a connector instance based on config type.
    
    Args:
        connector_id: Unique identifier for the connector
        config: Connector configuration dict (must include 'type')
        db_connection: Optional database connection for API connectors
    
    Returns:
        Connector instance or None if type unknown
    """
    connector_type = config.get('type', 'file')
    
    connector_class = CONNECTOR_TYPES.get(connector_type)
    if not connector_class:
        return None
    
    # Pass db_connection to connectors that support it
    if connector_type in ('http', 'airbeld'):
        return connector_class(connector_id, config, db_connection)
    
    return connector_class(connector_id, config)


def list_connector_types() -> list[str]:
    """Return available connector types."""
    return list(CONNECTOR_TYPES.keys())
