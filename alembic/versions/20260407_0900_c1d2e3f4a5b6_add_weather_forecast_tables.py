"""Add fact_weather_forecast and staging_weather_forecast tables

Revision ID: c1d2e3f4a5b6
Revises: b2c3d4e5f6a7
Create Date: 2026-04-07 09:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'c1d2e3f4a5b6'
down_revision: Union[str, None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # -------------------------------------------------------------------------
    # fact_weather_forecast
    # -------------------------------------------------------------------------
    op.create_table(
        'fact_weather_forecast',
        sa.Column('id', sa.Integer(), primary_key=True),

        # Temporal keys
        sa.Column('forecast_run_at', sa.DateTime(timezone=True), nullable=False,
                  comment='When the NWP model run was issued'),
        sa.Column('valid_at', sa.DateTime(timezone=True), nullable=False,
                  comment='The hour being forecasted'),
        sa.Column('horizon_hours', sa.SmallInteger(), nullable=False,
                  comment='valid_at − forecast_run_at in whole hours'),

        # Location
        sa.Column('site_id', sa.Integer(), sa.ForeignKey('site.id'), nullable=False),

        # Solar radiation
        sa.Column('shortwave_radiation_wm2', sa.Float(),
                  comment='Global Horizontal Irradiance (W/m²)'),
        sa.Column('direct_radiation_wm2', sa.Float(),
                  comment='Direct horizontal irradiance (W/m²)'),
        sa.Column('direct_normal_irradiance_wm2', sa.Float(),
                  comment='Direct Normal Irradiance perpendicular to sun (W/m²)'),
        sa.Column('diffuse_radiation_wm2', sa.Float(),
                  comment='Diffuse horizontal irradiance (W/m²)'),
        sa.Column('global_tilted_irradiance_wm2', sa.Float(),
                  comment='GTI on panel tilt/azimuth (W/m²)'),

        # Cloud cover
        sa.Column('cloud_cover_pct', sa.Float(),
                  comment='Total cloud cover (%)'),
        sa.Column('cloud_cover_low_pct', sa.Float(),
                  comment='Low clouds 0–3 km (%)'),
        sa.Column('cloud_cover_mid_pct', sa.Float(),
                  comment='Mid clouds 3–8 km (%)'),
        sa.Column('cloud_cover_high_pct', sa.Float(),
                  comment='High clouds >8 km (%)'),

        # Supporting weather
        sa.Column('temperature_2m_c', sa.Float(),
                  comment='Air temperature 2 m (°C)'),
        sa.Column('wind_speed_10m_ms', sa.Float(),
                  comment='Wind speed 10 m (m/s)'),
        sa.Column('wind_direction_10m_deg', sa.Float(),
                  comment='Wind direction 10 m (°)'),
        sa.Column('precipitation_mm', sa.Float(),
                  comment='Precipitation preceding hour (mm)'),
        sa.Column('weather_code', sa.SmallInteger(),
                  comment='WMO weather interpretation code'),

        # Derived
        sa.Column('sunshine_duration_s', sa.Float(),
                  comment='Seconds of sunshine in the hour'),
        sa.Column('is_day', sa.Boolean(),
                  comment='1 if daylight, 0 if night'),

        # Model metadata
        sa.Column('model_id', sa.String(32), server_default='best_match',
                  comment='NWP model: best_match, icon, ifs, etc.'),

        # Standard source tracking
        sa.Column('source_type', sa.String(32), server_default='api'),
        sa.Column('source_batch_id', postgresql.UUID(as_uuid=True),
                  sa.ForeignKey('ingest_batch.batch_id')),
        sa.Column('source_api_endpoint', sa.Text()),
        sa.Column('source_device_id', sa.String(64)),
        sa.Column('ingested_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),

        sa.UniqueConstraint('forecast_run_at', 'valid_at', 'site_id',
                            name='uq_forecast_run_valid_site'),
    )

    op.create_index('idx_forecast_valid_at', 'fact_weather_forecast', ['valid_at'])
    op.create_index('idx_forecast_run', 'fact_weather_forecast', ['forecast_run_at'])
    op.create_index('idx_forecast_site', 'fact_weather_forecast', ['site_id'])
    op.create_index('idx_forecast_horizon', 'fact_weather_forecast', ['horizon_hours'])
    op.create_index('idx_forecast_batch', 'fact_weather_forecast', ['source_batch_id'])

    # -------------------------------------------------------------------------
    # staging_weather_forecast
    # -------------------------------------------------------------------------
    op.create_table(
        'staging_weather_forecast',
        sa.Column('staging_id', sa.Integer(), primary_key=True),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True),
                  sa.ForeignKey('ingest_batch.batch_id')),
        sa.Column('row_number', sa.Integer(), nullable=False),
        sa.Column('raw_data', postgresql.JSONB(), nullable=False),
        sa.Column('transformed_data', postgresql.JSONB()),
        sa.Column('validation_errors', postgresql.JSONB()),
        sa.Column('is_valid', sa.Boolean(), server_default='false'),
        sa.Column('loaded_to_final', sa.Boolean(), server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_index('idx_staging_forecast_batch', 'staging_weather_forecast', ['batch_id'])
    op.create_index('idx_staging_forecast_valid', 'staging_weather_forecast',
                    ['is_valid', 'loaded_to_final'])


def downgrade() -> None:
    op.drop_index('idx_staging_forecast_valid', table_name='staging_weather_forecast')
    op.drop_index('idx_staging_forecast_batch', table_name='staging_weather_forecast')
    op.drop_table('staging_weather_forecast')

    op.drop_index('idx_forecast_batch', table_name='fact_weather_forecast')
    op.drop_index('idx_forecast_horizon', table_name='fact_weather_forecast')
    op.drop_index('idx_forecast_site', table_name='fact_weather_forecast')
    op.drop_index('idx_forecast_run', table_name='fact_weather_forecast')
    op.drop_index('idx_forecast_valid_at', table_name='fact_weather_forecast')
    op.drop_table('fact_weather_forecast')
