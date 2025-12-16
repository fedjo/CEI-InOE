CREATE VIEW v_energy_hourly AS
SELECT
    d.device_token,
    d.client,
    d.alias,
    feh.ts,
    feh.energy_kwh,
    'hourly' as granularity
FROM fact_energy_hourly feh
JOIN device d ON feh.device_id = d.id;

CREATE VIEW v_energy_daily AS
SELECT 
    d.device_token,
    d.client,
    d.alias,
    feh.ts::timestamp,
    feh.energy_kwh,
    'daily' as granularity
FROM fact_energy_daily feh
JOIN device d ON feh.device_id = d.id;

CREATE VIEW v_energy_monthly AS
SELECT
    d.device_token,
    d.client,
    d.alias,
    DATE_TRUNC('month', feh.ts)::date as month,
    SUM(feh.energy_kwh) as total_energy_kwh,
    COUNT(*) as data_points
FROM fact_energy_daily feh
JOIN device d ON feh.device_id = d.id
GROUP BY d.device_id, d.device_token, d.client, d.alias, DATE_TRUNC('month', feh.ts);
