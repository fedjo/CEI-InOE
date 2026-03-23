"""Add fact_solar_hourly, fact_solar_daily, fact_solar_monthly tables

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-19 10:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _solar_columns():
    """Return fresh Column objects so each table gets its own FK constraints."""
    return [
        sa.Column('pv_yield_kwh', sa.Float(), comment='PV generation (kWh)'),
        sa.Column('inverter_yield_kwh', sa.Float(), comment='Inverter yield (kWh)'),
        sa.Column('inverter_power_kw', sa.Float(), comment='Inverter power (kW)'),
        sa.Column('ongrid_power_kwh', sa.Float(), comment='Exported to grid (kWh)'),
        sa.Column('buy_power_kwh', sa.Float(), comment='Imported from grid (kWh)'),
        sa.Column('use_power_kwh', sa.Float(), comment='Site consumption (kWh)'),
        sa.Column('self_use_power_kwh', sa.Float(), comment='PV self-consumed (kWh)'),
        sa.Column('self_provide_pct', sa.Float(), comment='Self-sufficiency (%)'),
        sa.Column('perpower_ratio', sa.Float(), comment='Specific yield (kWh/kWp)'),
        sa.Column('installed_capacity_kwp', sa.Float(), comment='Installed capacity (kWp)'),
        sa.Column('power_profit', sa.Float(), comment='Revenue / savings'),
        sa.Column('reduction_total_co2', sa.Float(), comment='CO2 avoided'),
        sa.Column('reduction_total_coal', sa.Float(), comment='Coal avoided'),
        sa.Column('reduction_total_tree', sa.Float(), comment='Equivalent trees'),
        sa.Column('source_type', sa.String(32), server_default='api'),
        sa.Column('source_batch_id', postgresql.UUID(as_uuid=True),
                  sa.ForeignKey('ingest_batch.batch_id')),
        sa.Column('source_api_endpoint', sa.Text()),
        sa.Column('source_device_id', sa.String(64)),
        sa.Column('ingested_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    ]


def _create_solar_table(name, ts_column, uq_name):
    op.create_table(
        name,
        sa.Column('id', sa.Integer(), primary_key=True),
        ts_column,
        *_solar_columns(),
        sa.UniqueConstraint('source_batch_id', 'ts', name=uq_name),
    )
    short = name.replace('fact_', '')
    op.create_index(f'idx_{short}_ts', name, ['ts'])
    op.create_index(f'idx_{short}_source', name, ['source_type', 'source_device_id'])
    op.create_index(f'idx_{short}_batch', name, ['source_batch_id'])


def _drop_solar_table(name):
    short = name.replace('fact_', '')
    op.drop_index(f'idx_{short}_batch', table_name=name)
    op.drop_index(f'idx_{short}_source', table_name=name)
    op.drop_index(f'idx_{short}_ts', table_name=name)
    op.drop_table(name)


def _create_staging_table(name):
    op.create_table(
        name,
        sa.Column('staging_id', sa.Integer(), primary_key=True),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True),
                  sa.ForeignKey('ingest_batch.batch_id')),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('granularity', sa.String(16), nullable=False, comment='hourly | daily | monthly'),
        sa.Column('raw_data', postgresql.JSONB(), nullable=False),
        sa.Column('transformed_data', postgresql.JSONB()),
        sa.Column('validation_errors', postgresql.JSONB()),
        sa.Column('is_valid', sa.Boolean(), server_default='false'),
        sa.Column('loaded_to_final', sa.Boolean(), server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index(f'idx_{name}_batch', name, ['batch_id'])
    op.create_index(f'idx_{name}_valid', name, ['is_valid', 'loaded_to_final'])
    op.create_index(f'idx_{name}_granularity', name, ['granularity'])


def upgrade() -> None:
    _create_solar_table(
        'fact_solar_hourly',
        sa.Column('ts', sa.DateTime(timezone=True), nullable=False),
        'uq_solar_hourly_batch_ts',
    )
    _create_solar_table(
        'fact_solar_daily',
        sa.Column('ts', sa.Date(), nullable=False),
        'uq_solar_daily_batch_ts',
    )
    _create_solar_table(
        'fact_solar_monthly',
        sa.Column('ts', sa.Date(), nullable=False),
        'uq_solar_monthly_batch_ts',
    )

    _create_staging_table('staging_solar_kpi')


def downgrade() -> None:
    op.drop_index('idx_staging_solar_kpi_granularity', table_name='staging_solar_kpi')
    op.drop_index('idx_staging_solar_kpi_valid', table_name='staging_solar_kpi')
    op.drop_index('idx_staging_solar_kpi_batch', table_name='staging_solar_kpi')
    op.drop_table('staging_solar_kpi')

    _drop_solar_table('fact_solar_monthly')
    _drop_solar_table('fact_solar_daily')
    _drop_solar_table('fact_solar_hourly')
