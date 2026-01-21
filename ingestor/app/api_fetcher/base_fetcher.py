"""
Base API Fetcher Module

Provides abstract base classes for implementing API data fetchers.
Each API source should extend BaseFetcher and implement the required methods.
"""

import os
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime, date
import psycopg2
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """Abstract base class for API data fetchers."""
    
    def __init__(self, db_dsn: str):
        """
        Initialize fetcher with database connection.
        
        Args:
            db_dsn: PostgreSQL connection string
        """
        self.db_dsn = db_dsn
        self.session = requests.Session()
    
    def get_conn(self):
        """Get database connection."""
        return psycopg2.connect(self.db_dsn)
    
    @abstractmethod
    def get_base_url(self) -> str:
        """Return the base URL for the API."""
        pass
    
    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        """Return headers for API requests."""
        pass
    
    def make_request(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """
        Make API request with error handling.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            Response JSON data
        """
        url = f"{self.get_base_url()}{endpoint}"
        headers = self.get_headers()
        
        try:
            response = self.session.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API request failed: {url} - {str(e)}")
            raise
    
    @abstractmethod
    def fetch_devices(self) -> List[Dict[str, Any]]:
        """Fetch and return list of devices from API."""
        pass
    
    @abstractmethod
    def save_devices(self, devices: List[Dict[str, Any]]):
        """Save devices to database."""
        pass
    
    @abstractmethod
    def fetch_readings(self, device_id: int, start_date: date, end_date: date, period: str) -> List[Dict[str, Any]]:
        """
        Fetch readings for a device.
        
        Args:
            device_id: Device identifier
            start_date: Start date for readings
            end_date: End date for readings
            period: Period type (hour, day, etc.)
            
        Returns:
            List of readings
        """
        pass
    
    @abstractmethod
    def save_readings(self, readings: List[Dict[str, Any]], source_ref: str):
        """
        Save readings to appropriate database table.
        
        Args:
            readings: List of reading data
            source_ref: Source file reference for tracking
        """
        pass


class DataTransformer:
    """Utility class for transforming API data to database format."""
    
    @staticmethod
    def parse_timestamp(timestamp_str: str) -> datetime:
        """
        Parse timestamp from various formats.
        
        Args:
            timestamp_str: Timestamp string
            
        Returns:
            Datetime object
        """
        # Try common formats
        formats = [
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d %H:%M:%S',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        
        # Fallback to ISO format parser
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    
    @staticmethod
    def safe_float(value: Any) -> Optional[float]:
        """
        Safely convert value to float.
        
        Args:
            value: Value to convert
            
        Returns:
            Float value or None
        """
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
