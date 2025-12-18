-- Environmental Metrics Schema
CREATE TABLE IF NOT EXISTS environmental_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    atm_pressure DECIMAL(8, 2),
    noise_level_db DECIMAL(6, 2),
    temperature DECIMAL(6, 2),
    humidity DECIMAL(6, 2),
    pm10 DECIMAL(8, 2),
    wind_speed DECIMAL(6, 2),
    wind_direction_sectors DECIMAL(6, 2),
    wind_angle DECIMAL(6, 2),
    pm2p5 DECIMAL(8, 2),
    source_file UUID REFERENCES ingest_file(file_id),
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(timestamp)
);

CREATE INDEX idx_environmental_metrics_timestamp ON environmental_metrics(timestamp);
CREATE INDEX idx_environmental_metrics_created_at ON environmental_metrics(created_at);

-- Environmental Metrics Daily View
CREATE VIEW v_environmental_metrics_daily AS
SELECT
    DATE(em.timestamp) as measurement_date,
    ROUND(AVG(em.atm_pressure)::numeric, 2) as avg_atm_pressure,
    ROUND(MIN(em.atm_pressure)::numeric, 2) as min_atm_pressure,
    ROUND(MAX(em.atm_pressure)::numeric, 2) as max_atm_pressure,
    ROUND(AVG(em.noise_level_db)::numeric, 2) as avg_noise_level_db,
    ROUND(MAX(em.noise_level_db)::numeric, 2) as max_noise_level_db,
    ROUND(AVG(em.temperature)::numeric, 2) as avg_temperature,
    ROUND(MIN(em.temperature)::numeric, 2) as min_temperature,
    ROUND(MAX(em.temperature)::numeric, 2) as max_temperature,
    ROUND(AVG(em.humidity)::numeric, 2) as avg_humidity,
    ROUND(AVG(em.pm10)::numeric, 2) as avg_pm10,
    ROUND(MAX(em.pm10)::numeric, 2) as max_pm10,
    ROUND(AVG(em.wind_speed)::numeric, 2) as avg_wind_speed,
    ROUND(MAX(em.wind_speed)::numeric, 2) as max_wind_speed,
    ROUND(AVG(em.pm2p5)::numeric, 2) as avg_pm2p5,
    ROUND(MAX(em.pm2p5)::numeric, 2) as max_pm2p5,
    COUNT(*) as record_count
FROM environmental_metrics em
GROUP BY DATE(em.timestamp)
ORDER BY measurement_date DESC;

-- Air Quality Analysis View
CREATE VIEW v_air_quality_analysis AS
SELECT
    DATE(em.timestamp) as measurement_date,
    ROUND(AVG(em.pm10)::numeric, 2) as avg_pm10,
    ROUND(MAX(em.pm10)::numeric, 2) as max_pm10,
    ROUND(AVG(em.pm2p5)::numeric, 2) as avg_pm2p5,
    ROUND(MAX(em.pm2p5)::numeric, 2) as max_pm2p5,
    CASE
        WHEN ROUND(AVG(em.pm10)::numeric, 2) <= 40 THEN 'Good'
        WHEN ROUND(AVG(em.pm10)::numeric, 2) <= 80 THEN 'Fair'
        WHEN ROUND(AVG(em.pm10)::numeric, 2) <= 120 THEN 'Moderate'
        ELSE 'Poor'
    END as pm10_quality,
    CASE
        WHEN ROUND(AVG(em.pm2p5)::numeric, 2) <= 12 THEN 'Good'
        WHEN ROUND(AVG(em.pm2p5)::numeric, 2) <= 35.4 THEN 'Moderate'
        ELSE 'Poor'
    END as pm2p5_quality
FROM environmental_metrics em
GROUP BY DATE(em.timestamp)
ORDER BY measurement_date DESC;

-- Environmental Metrics Summary View (Monthly Aggregates)
CREATE VIEW v_environmental_summary AS
SELECT
    DATE_TRUNC('month', em.timestamp)::date AS month,
    COUNT(*) AS record_count,
    ROUND(AVG(em.atm_pressure)::numeric, 2) AS avg_atm_pressure,
    ROUND(AVG(em.noise_level_db)::numeric, 2) AS avg_noise_db,
    ROUND(AVG(em.temperature)::numeric, 2) AS avg_temperature,
    ROUND(AVG(em.humidity)::numeric, 2) AS avg_humidity,
    ROUND(AVG(em.pm10)::numeric, 2) AS avg_pm10,
    ROUND(MAX(em.pm10)::numeric, 2) AS max_pm10,
    ROUND(AVG(em.wind_speed)::numeric, 2) AS avg_wind_speed,
    ROUND(AVG(em.pm2p5)::numeric, 2) AS avg_pm2p5,
    ROUND(MAX(em.pm2p5)::numeric, 2) AS max_pm2p5
FROM environmental_metrics em
GROUP BY DATE_TRUNC('month', em.timestamp)
ORDER BY month DESC;