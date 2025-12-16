CREATE TABLE device (
    id SERIAL PRIMARY KEY,
    device_id TEXT UNIQUE NOT NULL,
    device_token TEXT UNIQUE NOT NULL,
    device_name TEXT,
    client TEXT,
    alias TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
