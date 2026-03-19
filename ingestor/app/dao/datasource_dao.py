"""
Datasource DAO using SQLAlchemy Core.

Replaces the old DeviceDAO.
"""

import logging
from typing import Any

from sqlalchemy import select, insert, update

from shared import Datasource
from . import BaseCoreDAO

logger = logging.getLogger(__name__)


class DatasourceDAO(BaseCoreDAO):
    """Data access for datasource table using SQLAlchemy Core."""

    def __init__(self, connection):
        super().__init__(connection)
        # Access the Core Table from the ORM model
        self.table = Datasource.__table__

    def get_by_id(self, datasource_id: int) -> int | None:
        """Get datasource by internal ID."""
        stmt = select(self.table).where(self.table.c.id == datasource_id)
        row = self.fetch_one(stmt)
        return dict(row._mapping) if row else None

    def get_by_external_id(self, external_id: str) -> dict | None:
        """
        Get datasource by external ID.

        Args:
            external_id: External device identifier

        Returns:
            Datasource dict or None
        """
        stmt = select(self.table).where(self.table.c.external_id == external_id)
        row = self.fetch_one(stmt)
        return dict(row._mapping) if row else None

    def resolve_id(self, external_id: str) -> int | None:
        """
        Resolve external ID to internal ID.

        Args:
            external_id: External device identifier

        Returns:
            Internal database ID or None
        """
        stmt = select(self.table.c.id).where(self.table.c.external_id == external_id)
        return self.scalar(stmt)

    def get_by_type(self, data_type: str, status: str = 'active') -> list[dict]:
        """
        Get all datasources of a specific type.

        Args:
            data_type: Type of data (e.g., 'weather', 'energy')
            status: Status filter

        Returns:
            List of datasource dictionaries
        """
        stmt = select(self.table).where(
            self.table.c.data_type == data_type,
            self.table.c.status == status
        )
        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]

    def get_weather_datasources(self) -> list[dict]:
        """Get all active weather datasources."""
        return self.get_by_type('weather', 'online')

    def get_energy_datasources(self) -> list[dict]:
        """Get all active energy datasources."""
        return self.get_by_type('energy', 'online')

    def get_energy_datasources_with_token(self) -> list[dict]:
        """
        Get all energy datasources with device tokens.
        Required for Tago.io API authentication.

        Returns:
            List of datasource dicts with device_token from metadata
        """
        stmt = select(
            self.table.c.id,
            self.table.c.external_id,
            self.table.c.alias,
            self.table.c.client,
            self.table.c.metadata['device_token'].astext.label('device_token'),
            self.table.c.metadata
        ).where(
            self.table.c.data_type == 'energy',
            self.table.c.status == 'online',
            self.table.c.metadata['device_token'].astext.isnot(None)
        )

        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]

    def get_solar_datasources(self) -> list[dict]:
        """
        Get all solar datasources with station_code in metadata.
        Required for FusionSolar API.
        """
        stmt = select(
            self.table.c.id,
            self.table.c.external_id,
            self.table.c.alias,
            self.table.c.client,
            self.table.c.metadata['station_code'].astext.label('station_code'),
            self.table.c.metadata
        ).where(
            self.table.c.data_type == 'solar',
            self.table.c.status == 'online',
            self.table.c.metadata['station_code'].astext.isnot(None)
        )

        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]

    def create(self, **kwargs) -> int:
        """Create a new datasource."""
        stmt = insert(self.table).values(**kwargs).returning(self.table.c.id)
        return self.scalar(stmt)

    def update_status(self, datasource_id: int, status: str):
        """Update datasource status."""
        stmt = update(self.table).where(
            self.table.c.id == datasource_id
        ).values(status=status)
        self.execute(stmt)
