"""Add composite unique constraints for fact tables

Revision ID: 8f2c4d5e6a7b
Revises: ad5ec9320389
Create Date: 2026-03-16 17:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8f2c4d5e6a7b'
down_revision: Union[str, None] = 'ad5ec9320389'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop old unique constraints on timestamp columns alone
    op.drop_constraint('fact_energy_hourly_ts_key', 'fact_energy_hourly', type_='unique')
    op.drop_constraint('fact_energy_daily_ts_key', 'fact_energy_daily', type_='unique')
    op.drop_constraint('environmental_metrics_timestamp_key', 'environmental_metrics', type_='unique')
    op.drop_constraint('dairy_production_production_date_key', 'dairy_production', type_='unique')
    
    # Add composite unique constraints on (source_batch_id, timestamp)
    op.create_unique_constraint(
        'uq_energy_hourly_batch_ts', 
        'fact_energy_hourly', 
        ['source_batch_id', 'ts']
    )
    op.create_unique_constraint(
        'uq_energy_daily_batch_ts', 
        'fact_energy_daily', 
        ['source_batch_id', 'ts']
    )
    op.create_unique_constraint(
        'uq_env_metrics_batch_ts', 
        'environmental_metrics', 
        ['source_batch_id', 'timestamp']
    )
    op.create_unique_constraint(
        'uq_dairy_batch_date', 
        'dairy_production', 
        ['source_batch_id', 'production_date']
    )
    
    # Add indexes on timestamp columns for query performance
    op.create_index('idx_energy_hourly_ts', 'fact_energy_hourly', ['ts'])
    op.create_index('idx_energy_daily_ts', 'fact_energy_daily', ['ts'])


def downgrade() -> None:
    # Drop indexes
    op.drop_index('idx_energy_daily_ts', table_name='fact_energy_daily')
    op.drop_index('idx_energy_hourly_ts', table_name='fact_energy_hourly')
    
    # Drop composite unique constraints
    op.drop_constraint('uq_dairy_batch_date', 'dairy_production', type_='unique')
    op.drop_constraint('uq_env_metrics_batch_ts', 'environmental_metrics', type_='unique')
    op.drop_constraint('uq_energy_daily_batch_ts', 'fact_energy_daily', type_='unique')
    op.drop_constraint('uq_energy_hourly_batch_ts', 'fact_energy_hourly', type_='unique')
    
    # Restore old unique constraints on timestamp columns alone
    op.create_unique_constraint('dairy_production_production_date_key', 'dairy_production', ['production_date'])
    op.create_unique_constraint('environmental_metrics_timestamp_key', 'environmental_metrics', ['timestamp'])
    op.create_unique_constraint('fact_energy_daily_ts_key', 'fact_energy_daily', ['ts'])
    op.create_unique_constraint('fact_energy_hourly_ts_key', 'fact_energy_hourly', ['ts'])
