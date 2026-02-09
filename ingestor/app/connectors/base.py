"""
Base connector interface and InputEnvelope definition.
All connectors must implement this interface.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ConnectorStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"


class InputEnvelope(BaseModel):
    """
    Lightweight envelope for data flowing through the pipeline.
    Connectors emit these; the pipeline consumes them.
    """
    connector_id: str
    input_id: str  # Idempotency key
    source_uri: str
    received_at: datetime = Field(default_factory=datetime.now)
    content: Any  # Raw data: List[Dict], bytes
    content_type: str  # "csv", "excel", "json"
    
    # Pipeline hints
    hint_mapping: Optional[str] = None
    hint_device_id: Optional[str] = None
    hint_granularity: Optional[str] = None
    
    # Provenance
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        arbitrary_types_allowed = True


class ConnectorConfig(BaseModel):
    """Base configuration for connectors."""
    type: str
    enabled: bool = True
    schedule_seconds: int = 60
    
    class Config:
        extra = "allow"  # Allow subclass-specific fields


class BaseConnector(ABC):
    """
    Abstract base class for all connectors.
    Responsible for: discover → fetch → emit envelopes.
    Does NOT validate or transform business data.
    """
    
    def __init__(self, connector_id: str, config: Dict[str, Any]):
        self.connector_id = connector_id
        self.config = config
        self.status = ConnectorStatus.IDLE
        self._last_error: Optional[str] = None
    
    @abstractmethod
    def start(self) -> None:
        """Initialize resources."""
        pass
    
    @abstractmethod
    def stop(self) -> None:
        """Cleanup resources."""
        pass
    
    @abstractmethod
    def discover(self) -> List[str]:
        """Find available work items."""
        pass
    
    @abstractmethod
    def fetch(self, item_id: str) -> Optional[InputEnvelope]:
        """Fetch and wrap item into InputEnvelope."""
        pass
    
    @abstractmethod
    def ack(self, envelope: InputEnvelope) -> None:
        """Mark item as successfully processed."""
        pass
    
    @abstractmethod
    def fail(self, envelope: InputEnvelope, error: str) -> None:
        """Mark item as failed."""
        pass
    
    def health(self) -> Dict[str, Any]:
        """Return connector health status."""
        return {
            "connector_id": self.connector_id,
            "status": self.status.value,
            "last_error": self._last_error,
        }
