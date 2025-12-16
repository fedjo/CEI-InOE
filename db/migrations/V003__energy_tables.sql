CREATE TABLE fact_energy_hourly (
    energy_id SERIAL PRIMARY KEY,
    device_id INTEGER NOT NULL REFERENCES device(id),
    ts TIMESTAMP NOT NULL,
    energy_kwh FLOAT NOT NULL,
    source_file UUID REFERENCES ingest_file(file_id),
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(device_id, ts)
);

CREATE INDEX idx_fact_energy_hourly_device_ts ON fact_energy_hourly(device_id, ts);

CREATE TABLE fact_energy_daily (
    energy_id SERIAL PRIMARY KEY,
    device_id INTEGER NOT NULL REFERENCES device(id),
    ts DATE NOT NULL,
    energy_kwh FLOAT NOT NULL,
    source_file UUID REFERENCES ingest_file(file_id),
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(device_id, ts)
);

CREATE INDEX idx_fact_energy_daily_device_ts ON fact_energy_daily(device_id, ts);