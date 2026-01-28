-- ============================================================================
-- V008: Refactor device table to support multiple device types
-- ============================================================================

-- Create new unified device table
CREATE TABLE generic_device (
    id              SERIAL PRIMARY KEY,
    device_id      VARCHAR(64) NOT NULL UNIQUE,  -- External unique identifier
    device_type     VARCHAR(32) NOT NULL,
    alias          VARCHAR(255),
    client          VARCHAR(255) NOT NULL,
    description     TEXT,
    status          VARCHAR(32) DEFAULT 'unknown',  -- online, offline, unknown
    timezone        VARCHAR(64) DEFAULT 'UTC',
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX idx_generic_device_id ON generic_device(device_id);
CREATE INDEX idx_generic_device_type ON generic_device(device_type);
CREATE INDEX idx_generic_device_client ON generic_device(client);
CREATE INDEX idx_generic_device_status ON generic_device(status);
CREATE INDEX idx_generic_device_metadata ON generic_device USING GIN(metadata);

-- Migrate existing energy devices from old device table
INSERT INTO generic_device (device_id, device_type, alias, client, metadata)
SELECT 
    device_id,
    'energy',
    COALESCE(alias, ''),
    COALESCE(client, 'Unknown'),
    jsonb_build_object(
        'name', device_name,
        'device_token', device_token
    )
FROM device
ON CONFLICT (device_id) DO NOTHING;

-- Create view for backward compatibility with energy devices
CREATE OR REPLACE VIEW device_energy AS
SELECT 
    id,
    device_id,
    alias AS display_name,
    client,
    metadata->>'name' AS name,
    metadata->>'device_token' AS device_token
FROM generic_device
WHERE device_type = 'energy';

-- Create view for weather/environmental devices
CREATE OR REPLACE VIEW device_weather AS
SELECT 
    id,
    device_id,
    alias AS display_name,
    client,
    description,
    status,
    timezone,
    metadata->>'location' AS location
FROM generic_device
WHERE device_type = 'weather';


-- Add comment documentation
COMMENT ON TABLE generic_device IS 'Unified device table supporting multiple device types';
COMMENT ON COLUMN generic_device.device_id IS 'External unique identifier (e.g., API ID, serial number)';
COMMENT ON COLUMN generic_device.device_type IS 'Type: energy, weather, pv, dairy, other';
COMMENT ON COLUMN generic_device.metadata IS 'Type-specific metadata as JSONB';

-- Update foreign key references in energy tables to use new device table
-- (keeping old device table for now, can be dropped in future migration)

-- Add trigger for updated_at
CREATE OR REPLACE FUNCTION update_generic_device_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_generic_device_updated
    BEFORE UPDATE ON generic_device
    FOR EACH ROW
    EXECUTE FUNCTION update_generic_device_timestamp();