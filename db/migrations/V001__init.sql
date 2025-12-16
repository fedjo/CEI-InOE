CREATE TABLE ingest_file (
    file_id UUID PRIMARY KEY,
    file_name TEXT NOT NULL,
    device_id TEXT NOT NULL,
    granularity TEXT NOT NULL,
    start_date DATE,
    end_date DATE,
    sha256 TEXT UNIQUE NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now()
);
