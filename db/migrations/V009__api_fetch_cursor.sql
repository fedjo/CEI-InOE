-- API fetch cursors for tracking last successful fetch per device/endpoint
CREATE TABLE IF NOT EXISTS api_fetch_cursor (
    id SERIAL PRIMARY KEY,
    connector_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    last_fetch_timestamp TIMESTAMPTZ NOT NULL,
    last_fetch_success TIMESTAMPTZ DEFAULT NOW(),
    fetch_count INT DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(connector_id, endpoint_id, device_id)
);

CREATE INDEX idx_api_cursor_lookup ON api_fetch_cursor(connector_id, endpoint_id, device_id);

COMMENT ON TABLE api_fetch_cursor IS 'Tracks last successful API fetch timestamp per device for incremental data fetching';