-- Dairy Production Schema
CREATE TABLE IF NOT EXISTS dairy_production (
    id SERIAL PRIMARY KEY,
    production_date DATE NOT NULL,
    day_production_per_cow_kg DECIMAL(10, 2),
    number_of_animals INTEGER,
    average_lactation_days INTEGER,
    fed_per_cow_total_kg DECIMAL(10, 2),
    fed_per_cow_water_kg DECIMAL(10, 2),
    feed_efficiency DECIMAL(10, 4),
    rumination_minutes INTEGER,
    source_file UUID REFERENCES ingest_file(file_id),
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(production_date)
);

CREATE INDEX idx_dairy_production_date ON dairy_production(production_date);

-- Dairy Production Daily View
CREATE VIEW v_dairy_production_daily AS
SELECT
    dp.production_date,
    dp.day_production_per_cow_kg,
    dp.number_of_animals,
    dp.average_lactation_days,
    dp.fed_per_cow_total_kg,
    dp.fed_per_cow_water_kg,
    dp.feed_efficiency,
    dp.rumination_minutes,
    (dp.day_production_per_cow_kg * dp.number_of_animals) AS total_daily_production_kg,
    CASE
        WHEN dp.fed_per_cow_total_kg > 0
        THEN ROUND((dp.day_production_per_cow_kg / dp.fed_per_cow_total_kg)::numeric, 4)
        ELSE NULL 
    END AS calculated_feed_efficiency
FROM dairy_production dp
ORDER BY dp.production_date DESC;

-- Feed Efficiency Analysis View
CREATE VIEW v_feed_efficiency_analysis AS
SELECT
    dp.production_date,
    dp.feed_efficiency,
    dp.fed_per_cow_total_kg,
    dp.day_production_per_cow_kg,
    dp.rumination_minutes,
    dp.number_of_animals,
    dp.average_lactation_days
FROM dairy_production dp
WHERE dp.feed_efficiency > 0
ORDER BY dp.production_date DESC;

-- Production Summary View (Monthly Aggregates)
CREATE VIEW v_production_summary AS
SELECT
    DATE_TRUNC('month', dp.production_date)::date AS month,
    COUNT(*) AS days_recorded,
    ROUND(AVG(dp.day_production_per_cow_kg)::numeric, 2) AS avg_production_per_cow_kg,
    ROUND(MAX(dp.day_production_per_cow_kg)::numeric, 2) AS max_production_per_cow_kg,
    ROUND(MIN(dp.day_production_per_cow_kg)::numeric, 2) AS min_production_per_cow_kg,
    ROUND(AVG(dp.number_of_animals)::numeric, 0) AS avg_animals,
    ROUND(AVG(dp.average_lactation_days)::numeric, 1) AS avg_lactation_days,
    ROUND(AVG(dp.feed_efficiency)::numeric, 4) AS avg_feed_efficiency,
    ROUND(AVG(dp.rumination_minutes)::numeric, 1) AS avg_rumination_minutes
FROM dairy_production dp
GROUP BY DATE_TRUNC('month', dp.production_date)
ORDER BY month DESC;