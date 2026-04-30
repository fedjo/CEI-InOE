"""Add datasource_id FK to all fact/metric tables

Extends fact_energy_hourly, fact_energy_daily, environmental_metrics,
dairy_production, fact_solar_hourly, fact_solar_daily, and fact_solar_monthly
with a datasource_id foreign key so that deduplication can be keyed on a
stable business identity (datasource + time grain) rather than batch_id.

The new datasource-scoped unique constraints are added alongside the existing
batch-scoped constraints.  The old batch-scoped constraints are intentionally
left in place here and will be dropped in a later cleanup migration (Phase 5/6)
after existing rows have been backfilled and duplicate data has been resolved.

Revision ID: c4d5e6f7a8b9
Revises: b2c3d4e5f6a7
Create Date: 2026-04-26 12:00:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c4d5e6f7a8b9'
down_revision: Union[str, None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── fact_energy_hourly ────────────────────────────────────────────────
    op.add_column(
        'fact_energy_hourly',
        sa.Column('datasource_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_energy_hourly_datasource',
        'fact_energy_hourly', 'datasource',
        ['datasource_id'], ['id'],
    )
    op.create_index(
        'idx_energy_hourly_datasource',
        'fact_energy_hourly', ['datasource_id'],
    )
    op.create_unique_constraint(
        'uq_energy_hourly_datasource_ts',
        'fact_energy_hourly', ['datasource_id', 'ts'],
    )

    # ── fact_energy_daily ─────────────────────────────────────────────────
    op.add_column(
        'fact_energy_daily',
        sa.Column('datasource_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_energy_daily_datasource',
        'fact_energy_daily', 'datasource',
        ['datasource_id'], ['id'],
    )
    op.create_index(
        'idx_energy_daily_datasource',
        'fact_energy_daily', ['datasource_id'],
    )
    op.create_unique_constraint(
        'uq_energy_daily_datasource_ts',
        'fact_energy_daily', ['datasource_id', 'ts'],
    )

    # ── environmental_metrics ─────────────────────────────────────────────
    op.add_column(
        'environmental_metrics',
        sa.Column('datasource_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_env_metrics_datasource',
        'environmental_metrics', 'datasource',
        ['datasource_id'], ['id'],
    )
    op.create_index(
        'idx_env_metrics_datasource',
        'environmental_metrics', ['datasource_id'],
    )
    op.create_unique_constraint(
        'uq_env_metrics_datasource_ts',
        'environmental_metrics', ['datasource_id', 'timestamp'],
    )

    # ── dairy_production ──────────────────────────────────────────────────
    op.add_column(
        'dairy_production',
        sa.Column('datasource_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_dairy_datasource',
        'dairy_production', 'datasource',
        ['datasource_id'], ['id'],
    )
    op.create_index(
        'idx_dairy_datasource',
        'dairy_production', ['datasource_id'],
    )
    op.create_unique_constraint(
        'uq_dairy_datasource_date',
        'dairy_production', ['datasource_id', 'production_date'],
    )

    # ── fact_solar_hourly ─────────────────────────────────────────────────
    op.add_column(
        'fact_solar_hourly',
        sa.Column('datasource_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_solar_hourly_datasource',
        'fact_solar_hourly', 'datasource',
        ['datasource_id'], ['id'],
    )
    op.create_index(
        'idx_solar_hourly_datasource',
        'fact_solar_hourly', ['datasource_id'],
    )
    op.create_unique_constraint(
        'uq_solar_hourly_datasource_ts',
        'fact_solar_hourly', ['datasource_id', 'ts'],
    )

    # ── fact_solar_daily ──────────────────────────────────────────────────
    op.add_column(
        'fact_solar_daily',
        sa.Column('datasource_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_solar_daily_datasource',
        'fact_solar_daily', 'datasource',
        ['datasource_id'], ['id'],
    )
    op.create_index(
        'idx_solar_daily_datasource',
        'fact_solar_daily', ['datasource_id'],
    )
    op.create_unique_constraint(
        'uq_solar_daily_datasource_ts',
        'fact_solar_daily', ['datasource_id', 'ts'],
    )

    # ── fact_solar_monthly ────────────────────────────────────────────────
    op.add_column(
        'fact_solar_monthly',
        sa.Column('datasource_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_solar_monthly_datasource',
        'fact_solar_monthly', 'datasource',
        ['datasource_id'], ['id'],
    )
    op.create_index(
        'idx_solar_monthly_datasource',
        'fact_solar_monthly', ['datasource_id'],
    )
    op.create_unique_constraint(
        'uq_solar_monthly_datasource_ts',
        'fact_solar_monthly', ['datasource_id', 'ts'],
    )


def downgrade() -> None:
    # ── fact_solar_monthly ────────────────────────────────────────────────
    op.drop_constraint('uq_solar_monthly_datasource_ts', 'fact_solar_monthly', type_='unique')
    op.drop_index('idx_solar_monthly_datasource', table_name='fact_solar_monthly')
    op.drop_constraint('fk_solar_monthly_datasource', 'fact_solar_monthly', type_='foreignkey')
    op.drop_column('fact_solar_monthly', 'datasource_id')

    # ── fact_solar_daily ──────────────────────────────────────────────────
    op.drop_constraint('uq_solar_daily_datasource_ts', 'fact_solar_daily', type_='unique')
    op.drop_index('idx_solar_daily_datasource', table_name='fact_solar_daily')
    op.drop_constraint('fk_solar_daily_datasource', 'fact_solar_daily', type_='foreignkey')
    op.drop_column('fact_solar_daily', 'datasource_id')

    # ── fact_solar_hourly ─────────────────────────────────────────────────
    op.drop_constraint('uq_solar_hourly_datasource_ts', 'fact_solar_hourly', type_='unique')
    op.drop_index('idx_solar_hourly_datasource', table_name='fact_solar_hourly')
    op.drop_constraint('fk_solar_hourly_datasource', 'fact_solar_hourly', type_='foreignkey')
    op.drop_column('fact_solar_hourly', 'datasource_id')

    # ── dairy_production ──────────────────────────────────────────────────
    op.drop_constraint('uq_dairy_datasource_date', 'dairy_production', type_='unique')
    op.drop_index('idx_dairy_datasource', table_name='dairy_production')
    op.drop_constraint('fk_dairy_datasource', 'dairy_production', type_='foreignkey')
    op.drop_column('dairy_production', 'datasource_id')

    # ── environmental_metrics ─────────────────────────────────────────────
    op.drop_constraint('uq_env_metrics_datasource_ts', 'environmental_metrics', type_='unique')
    op.drop_index('idx_env_metrics_datasource', table_name='environmental_metrics')
    op.drop_constraint('fk_env_metrics_datasource', 'environmental_metrics', type_='foreignkey')
    op.drop_column('environmental_metrics', 'datasource_id')

    # ── fact_energy_daily ─────────────────────────────────────────────────
    op.drop_constraint('uq_energy_daily_datasource_ts', 'fact_energy_daily', type_='unique')
    op.drop_index('idx_energy_daily_datasource', table_name='fact_energy_daily')
    op.drop_constraint('fk_energy_daily_datasource', 'fact_energy_daily', type_='foreignkey')
    op.drop_column('fact_energy_daily', 'datasource_id')

    # ── fact_energy_hourly ────────────────────────────────────────────────
    op.drop_constraint('uq_energy_hourly_datasource_ts', 'fact_energy_hourly', type_='unique')
    op.drop_index('idx_energy_hourly_datasource', table_name='fact_energy_hourly')
    op.drop_constraint('fk_energy_hourly_datasource', 'fact_energy_hourly', type_='foreignkey')
    op.drop_column('fact_energy_hourly', 'datasource_id')
