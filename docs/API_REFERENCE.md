# CEI-InOE API Reference

## Base URL

```
Development: http://localhost:8000
Production:  https://api.cei-inoe.example.com
```

## Authentication

**Phase 1 (Current):** No authentication (internal use only)

**Phase 2 (Planned):** Auth0 JWT tokens
```
Authorization: Bearer <token>
```

---

## Common Patterns

### Paginated Response

All list endpoints return paginated responses:

```json
{
  "data": [...],
  "total": 1250,
  "page": 1,
  "page_size": 100,
  "total_pages": 13
}
```

### Error Response

```json
{
  "detail": "Error message description"
}
```

### Common Query Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `start_date` | date | Filter from date (inclusive) | — |
| `end_date` | date | Filter to date (inclusive) | — |
| `page` | int | Page number (1-indexed) | 1 |
| `page_size` | int | Records per page (max 1000) | 100 |

---

## Health & Metadata

### `GET /health`

Health check endpoint for container orchestration.

**Response:**
```json
{
  "status": "healthy",
  "database": "connected",
  "timestamp": "2026-02-19T10:30:00Z"
}
```

### `GET /stats`

Overview statistics for dashboard widgets.

**Response:**
```json
{
  "environmental_count": 125000,
  "energy_hourly_count": 85000,
  "energy_daily_count": 3500,
  "dairy_count": 450,
  "devices_count": 12,
  "latest_environmental": "2026-02-19T10:00:00Z",
  "latest_energy": "2026-02-19T09:00:00Z",
  "latest_dairy": "2026-02-18"
}
```

---

## Energy Endpoints

### `GET /api/v1/energy/hourly`

Retrieve hourly energy consumption data.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | date | Yes | Start date |
| `end_date` | date | Yes | End date |
| `device_id` | int | No | Filter by device |
| `page` | int | No | Page number |
| `page_size` | int | No | Records per page |

**Example Request:**
```
GET /api/v1/energy/hourly?start_date=2026-02-01&end_date=2026-02-07&device_id=1
```

**Response:**
```json
{
  "data": [
    {
      "id": 12345,
      "device_id": 1,
      "ts": "2026-02-07T23:00:00Z",
      "kwh": 5.234,
      "source_type": "api",
      "source_file": null,
      "created_at": "2026-02-07T23:05:00Z"
    }
  ],
  "total": 168,
  "page": 1,
  "page_size": 100,
  "total_pages": 2
}
```

### `GET /api/v1/energy/daily`

Retrieve daily energy consumption data.

**Query Parameters:** Same as hourly.

**Response:**
```json
{
  "data": [
    {
      "id": 456,
      "device_id": 1,
      "day": "2026-02-07",
      "kwh": 125.45,
      "source_type": "csv",
      "source_file": "550e8400-e29b-41d4-a716-446655440000",
      "created_at": "2026-02-08T00:00:00Z"
    }
  ],
  "total": 30,
  "page": 1,
  "page_size": 100,
  "total_pages": 1
}
```

### `GET /api/v1/energy/latest`

Get the most recent hourly energy reading.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `device_id` | int | No | Filter by device |

**Response:**
```json
{
  "id": 12350,
  "device_id": 1,
  "ts": "2026-02-19T10:00:00Z",
  "kwh": 4.891
}
```

### `GET /api/v1/energy/stats`

Get energy data statistics.

**Response:**
```json
{
  "hourly_count": 85000,
  "daily_count": 3500,
  "hourly_first": "2025-06-01T00:00:00Z",
  "hourly_last": "2026-02-19T10:00:00Z",
  "daily_first": "2025-06-01",
  "daily_last": "2026-02-18",
  "hourly_devices": 8,
  "daily_devices": 8
}
```

---

## Environmental Endpoints

### `GET /api/v1/environmental/hourly`

Retrieve environmental metrics records.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | date | Yes | Start date |
| `end_date` | date | Yes | End date |
| `page` | int | No | Page number |
| `page_size` | int | No | Records per page |

**Response:**
```json
{
  "data": [
    {
      "id": 78901,
      "timestamp": "2026-02-07T14:00:00Z",
      "temperature": 22.5,
      "humidity": 65.3,
      "atm_pressure": 1013.25,
      "pm10": 18.2,
      "pm2p5": 12.1,
      "noise_level_db": 42.5,
      "wind_speed": 3.2,
      "wind_angle": 180.0,
      "wind_direction_sectors": 4.0,
      "source_type": "api",
      "source_file": null,
      "created_at": "2026-02-07T14:05:00Z"
    }
  ],
  "total": 500,
  "page": 1,
  "page_size": 100,
  "total_pages": 5
}
```

### `GET /api/v1/environmental/latest`

Get the most recent environmental reading.

**Response:**
```json
{
  "id": 78950,
  "timestamp": "2026-02-19T10:30:00Z",
  "temperature": 19.8,
  "humidity": 62.1,
  "atm_pressure": 1015.50,
  "pm10": 15.4,
  "pm2p5": 9.8,
  "noise_level_db": 38.2,
  "wind_speed": 2.1,
  "wind_angle": 225.0,
  "wind_direction_sectors": 5.0
}
```

### `GET /api/v1/environmental/stats`

Get environmental data statistics.

**Response:**
```json
{
  "total_count": 125000,
  "first_record": "2025-06-01T00:00:00Z",
  "last_record": "2026-02-19T10:30:00Z",
  "avg_temperature": 18.5,
  "avg_humidity": 65.2,
  "avg_pm10": 22.4,
  "avg_pm2p5": 15.1,
  "days_with_data": 264
}
```

---

## Dairy Endpoints

### `GET /api/v1/dairy/daily`

Get dairy production records.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | date | No | Start date |
| `end_date` | date | No | End date |
| `page` | int | No | Page number |
| `page_size` | int | No | Records per page |

**Response:**
```json
{
  "data": [
    {
      "id": 234,
      "production_date": "2026-02-07",
      "day_production_per_cow_kg": 28.5,
      "number_of_animals": 85,
      "average_lactation_days": 165,
      "fed_per_cow_total_kg": 22.3,
      "fed_per_cow_water_kg": 85.0,
      "feed_efficiency": 1.278,
      "rumination_minutes": 485,
      "source_type": "csv",
      "source_file": "550e8400-e29b-41d4-a716-446655440001",
      "ingested_at": "2026-02-08T06:00:00Z"
    }
  ],
  "total": 450,
  "page": 1,
  "page_size": 100,
  "total_pages": 5
}
```

### `GET /api/v1/dairy/latest`

Get the most recent dairy production record.

**Response:**
```json
{
  "id": 450,
  "production_date": "2026-02-18",
  "day_production_per_cow_kg": 29.2,
  "number_of_animals": 86,
  "average_lactation_days": 162,
  "fed_per_cow_total_kg": 21.8,
  "fed_per_cow_water_kg": 88.0,
  "feed_efficiency": 1.339,
  "rumination_minutes": 492
}
```

### `GET /api/v1/dairy/stats`

Get dairy data statistics.

**Response:**
```json
{
  "total_count": 450,
  "first_record": "2025-06-01",
  "last_record": "2026-02-18",
  "avg_production_per_cow": 27.8,
  "avg_animals": 84,
  "avg_feed_efficiency": 1.285,
  "avg_rumination_minutes": 478
}
```

---

## Device Endpoints

### `GET /api/v1/devices`

Get devices with optional filtering.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `device_type` | string | No | Filter by type (energy, weather, etc.) |
| `status` | string | No | Filter by status (online, offline, unknown) |
| `page` | int | No | Page number |
| `page_size` | int | No | Records per page |

**Response:**
```json
{
  "data": [
    {
      "id": 1,
      "device_id": "68f1fb6546b86e0009bf15bb",
      "device_type": "energy",
      "alias": "Main Building",
      "client": "FarmCo",
      "description": "Main building energy meter",
      "status": "online",
      "timezone": "Europe/Athens",
      "metadata": {
        "name": "Energy Meter 1",
        "device_token": "abc123"
      },
      "created_at": "2025-06-01T00:00:00Z",
      "updated_at": "2026-02-19T10:00:00Z"
    }
  ],
  "total": 12,
  "page": 1,
  "page_size": 100,
  "total_pages": 1
}
```

### `GET /api/v1/devices/types`

Get device types with counts.

**Response:**
```json
[
  {"device_type": "energy", "count": 8},
  {"device_type": "weather", "count": 2},
  {"device_type": "dairy", "count": 1},
  {"device_type": "pv", "count": 1}
]
```

### `GET /api/v1/devices/{device_id}`

Get a specific device by ID.

**Response:**
```json
{
  "id": 1,
  "device_id": "68f1fb6546b86e0009bf15bb",
  "device_type": "energy",
  "alias": "Main Building",
  "client": "FarmCo",
  "description": "Main building energy meter",
  "status": "online",
  "timezone": "Europe/Athens",
  "metadata": {
    "name": "Energy Meter 1",
    "device_token": "abc123"
  },
  "created_at": "2025-06-01T00:00:00Z",
  "updated_at": "2026-02-19T10:00:00Z"
}
```

---

## HTTP Status Codes

| Code | Meaning | Example |
|------|---------|---------|
| 200 | Success | Data returned |
| 400 | Bad Request | Invalid date format, start_date > end_date |
| 404 | Not Found | No data for filters, device not found |
| 422 | Validation Error | Missing required parameter |
| 500 | Server Error | Database connection failed |

## Interactive Documentation

- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **OpenAPI JSON:** http://localhost:8000/openapi.json
