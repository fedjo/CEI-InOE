[[Home]] | [[API-Architecture]] | [[API-Reference]] | [[Ingestor-Architecture]] | [[Ingestor-Database-Schema]] | [[Schema-Change-Guide]] | [[Migration-Guide]]
---

# CEI-InOE API Reference

## Base URL

```
Development: http://localhost:8000
Production:  https://api.cei-inoe.example.com
```

## Authentication

All data endpoints require an API key passed via the `X-API-Key` header.

```
X-API-Key: cei-inoe-dev-key-2026
```

The key is configured server-side via the `API_KEY` environment variable. Requests missing a valid key receive a `403 Forbidden` response.

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

## Health Endpoints

Health endpoints are public (no API key required).

### `GET /health`

Health check for container orchestration.

**Response:**
```json
{
  "status": "healthy",
  "database": true,
  "timestamp": "2026-02-19T10:30:00Z"
}
```

`status` is `"healthy"` when the database is reachable, otherwise `"degraded"`.

### `GET /ready`

Readiness probe. Returns `200` when the service is ready to accept traffic, or `200` with a failure reason if not ready.

**Response (ready):**
```json
{"status": "ready"}
```

**Response (not ready):**
```json
{"status": "not ready", "reason": "database connection failed"}
```

---

## Energy Endpoints

All endpoints require `X-API-Key`.

### `GET /api/v1/energy/hourly`

Retrieve hourly energy consumption data.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | date | Yes | Start date (inclusive) |
| `end_date` | date | Yes | End date (inclusive) |
| `datasource_id` | int | No | Filter by datasource ID |
| `page` | int | No | Page number (default: 1) |
| `page_size` | int | No | Records per page (default: 100, max: 1000) |

**Example Request:**
```
GET /api/v1/energy/hourly?start_date=2026-02-01&end_date=2026-02-07&datasource_id=3
X-API-Key: cei-inoe-dev-key-2026
```

**Response:**
```json
{
  "data": [
    {
      "energy_id": 12345,
      "ts": "2026-02-07T23:00:00Z",
      "energy_kwh": 5.234,
      "source_type": "api",
      "source_batch_id": "550e8400-e29b-41d4-a716-446655440000",
      "source_device_id": "68f1fb6546b86e0009bf15bb",
      "ingested_at": "2026-02-07T23:05:00Z"
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

**Query Parameters:** Same as `/hourly`.

**Response:**
```json
{
  "data": [
    {
      "energy_id": 456,
      "ts": "2026-02-07",
      "energy_kwh": 125.45,
      "source_type": "csv",
      "source_batch_id": "550e8400-e29b-41d4-a716-446655440000",
      "source_device_id": "68f1fb6546b86e0009bf15bb",
      "ingested_at": "2026-02-08T00:00:00Z"
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
| `datasource_id` | int | No | Filter by datasource ID |

**Response:**
```json
{
  "energy_id": 12350,
  "ts": "2026-02-19T10:00:00Z",
  "energy_kwh": 4.891,
  "source_type": "api",
  "source_batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "source_device_id": "68f1fb6546b86e0009bf15bb",
  "ingested_at": "2026-02-19T10:05:00Z"
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
  "hourly_datasources": 8,
  "daily_datasources": 8
}
```

---

## Environmental Endpoints

All endpoints require `X-API-Key`.

### `GET /api/v1/environmental/hourly`

Retrieve environmental metrics records.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | date | Yes | Start date (inclusive) |
| `end_date` | date | Yes | End date (inclusive) |
| `source_device_id` | string | No | Filter by originating device ID |
| `page` | int | No | Page number (default: 1) |
| `page_size` | int | No | Records per page (default: 100, max: 1000) |

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
      "source_batch_id": "550e8400-e29b-41d4-a716-446655440002",
      "source_device_id": "airbeld_1236",
      "ingested_at": "2026-02-07T14:05:00Z"
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
  "wind_direction_sectors": 5.0,
  "source_type": "api",
  "source_batch_id": "550e8400-e29b-41d4-a716-446655440002",
  "source_device_id": "airbeld_1236",
  "ingested_at": "2026-02-19T10:35:00Z"
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

All endpoints require `X-API-Key`.

### `GET /api/v1/dairy/daily`

Get dairy production records.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | date | No | Start date (inclusive) |
| `end_date` | date | No | End date (inclusive) |
| `page` | int | No | Page number (default: 1) |
| `page_size` | int | No | Records per page (default: 100, max: 1000) |

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
      "source_batch_id": "550e8400-e29b-41d4-a716-446655440001",
      "source_device_id": null,
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
  "rumination_minutes": 492,
  "source_type": "csv",
  "source_batch_id": "550e8400-e29b-41d4-a716-446655440001",
  "source_device_id": null,
  "ingested_at": "2026-02-19T06:00:00Z"
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

## Datasource Endpoints

All endpoints require `X-API-Key`. Datasources are the unified registry for all data sources (energy meters, weather stations, file uploads, APIs).

### `GET /api/v1/datasources`

Get datasources with optional filtering.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `data_type` | string | No | Filter by type: `energy`, `weather`, `dairy`, `pv` |
| `source_category` | string | No | Filter by category: `device`, `file`, `api`, `manual` |
| `status` | string | No | Filter by status: `active`, `inactive`, `deprecated` |
| `page` | int | No | Page number (default: 1) |
| `page_size` | int | No | Records per page (default: 100, max: 1000) |

**Response:**
```json
{
  "data": [
    {
      "id": 1,
      "external_id": "68f1fb6546b86e0009bf15bb",
      "source_category": "device",
      "data_type": "energy",
      "name": "Blue Farm Fan Motor",
      "alias": "BF Fan",
      "client": "Blue Farm Dairy",
      "description": "Energy meter on ventilation fan motor",
      "status": "active",
      "timezone": "Europe/Athens",
      "metadata": {
        "device_token": "abc123"
      },
      "site_id": 1,
      "created_at": "2025-06-01T00:00:00Z",
      "updated_at": "2026-02-19T10:00:00Z"
    }
  ],
  "total": 27,
  "page": 1,
  "page_size": 100,
  "total_pages": 1
}
```

### `GET /api/v1/datasources/types`

Get data types with counts.

**Response:**
```json
[
  {"data_type": "energy", "count": 18},
  {"data_type": "weather", "count": 2},
  {"data_type": "dairy", "count": 2},
  {"data_type": "pv", "count": 1}
]
```

### `GET /api/v1/datasources/categories`

Get source categories with counts.

**Response:**
```json
[
  {"source_category": "device", "count": 23},
  {"source_category": "file", "count": 2},
  {"source_category": "api", "count": 2}
]
```

### `GET /api/v1/datasources/{datasource_id}`

Get a specific datasource by its integer ID.

**Response:** Single `DatasourceRead` object (see shape above).

### `GET /api/v1/datasources/external/{external_id}`

Get a datasource by its external identifier (e.g. Tago.io device ID, Airbeld station ID).

**Response:** Single `DatasourceRead` object.

---

## Batch Endpoints

All endpoints require `X-API-Key`. Batches represent individual ingestion operations (file uploads, scheduled API fetches).

### `GET /api/v1/batches`

Get ingest batches with optional filtering.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_type` | string | No | Filter by type: `file`, `api`, `stream`, `manual` |
| `status` | string | No | Filter by status: `pending`, `processing`, `completed`, `failed` |
| `datasource_id` | int | No | Filter by datasource ID |
| `page` | int | No | Page number (default: 1) |
| `page_size` | int | No | Records per page (default: 100, max: 1000) |

**Response:**
```json
{
  "data": [
    {
      "batch_id": "550e8400-e29b-41d4-a716-446655440000",
      "source_type": "file",
      "source_name": "energy_hourly_2026-02.csv",
      "datasource_id": 3,
      "granularity": "hourly",
      "date_range_start": "2026-02-01",
      "date_range_end": "2026-02-28",
      "file_sha256": "a3f5b2...",
      "status": "completed",
      "records_loaded": 672,
      "records_failed": 0,
      "execution_time_ms": 1420,
      "validation_status": "passed",
      "quality_score": 100.0,
      "pipeline_version": "1.0",
      "started_at": "2026-03-01T06:00:00Z",
      "completed_at": "2026-03-01T06:00:01Z"
    }
  ],
  "total": 42,
  "page": 1,
  "page_size": 100,
  "total_pages": 1
}
```

### `GET /api/v1/batches/summary`

Get summary statistics across all batches.

**Response:**
```json
{
  "total_batches": 42,
  "completed": 39,
  "failed": 3,
  "total_records_loaded": 95420,
  "total_records_failed": 128
}
```

### `GET /api/v1/batches/{batch_id}`

Get a specific batch by UUID.

**Response:** Single `IngestBatchRead` object (see shape above).

---

## HTTP Status Codes

| Code | Meaning | Example |
|------|---------|---------|
| 200 | Success | Data returned |
| 400 | Bad Request | Invalid date format, `start_date` > `end_date` |
| 403 | Forbidden | Missing or invalid `X-API-Key` |
| 404 | Not Found | No record for given ID or filters |
| 422 | Validation Error | Missing required parameter |
| 500 | Server Error | Database connection failed |

## Interactive Documentation

- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **OpenAPI JSON:** http://localhost:8000/openapi.json
