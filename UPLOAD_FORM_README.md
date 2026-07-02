# Data Upload Form - Implementation Guide

## Overview

A skeleton implementation for uploading data files (Excel/CSV) with placeholder fields to the CEI-InOE platform. This provides the foundation for future data update workflows.

## Components

### 1. API Endpoint
- **URL**: `POST /api/v1/upload/data`
- **Authentication**: API key required
- **Content-Type**: `multipart/form-data`
- **Parameters**:
  - `file` (required): Excel (.xlsx, .xls) or CSV file
  - `field1` (optional): Placeholder text field
  - `field2` (optional): Placeholder text field

### 2. HTML Form
- **URL**: `GET /upload-form`
- **Authentication**: None (public access for easy development)
- **Features**:
  - Modern, responsive UI
  - File upload with drag-and-drop support
  - Real-time validation
  - Success/error messaging
  - Loading states

### 3. Database Storage
- Creates an `IngestBatch` record with:
  - `source_type`: "manual"
  - `source_name`: uploaded filename
  - `file_sha256`: SHA-256 hash for deduplication
  - `status`: "uploaded" (custom status for unprocessed files)
  - `pipeline_version`: "skeleton-1.0"

## Usage

### Using the HTML Form

1. Start the API server:
   ```bash
   cd api
   uvicorn app.main:app --reload
   ```

2. Open your browser to: `http://localhost:8000/upload-form`

3. Fill in the form:
   - Select an Excel or CSV file
   - Optionally fill in placeholder fields
   - Click "Upload Data"

4. Check the response:
   - Success: Shows batch ID and file details
   - Error: Shows error message

### Using the API Directly

```bash
curl -X POST "http://localhost:8000/api/v1/upload/data" \
  -H "X-API-Key: your-api-key-here" \
  -F "file=@/path/to/your/data.xlsx" \
  -F "field1=some value" \
  -F "field2=another value"
```

### Using Python

```python
import requests

url = "http://localhost:8000/api/v1/upload/data"
headers = {"X-API-Key": "your-api-key-here"}

files = {"file": open("data.xlsx", "rb")}
data = {"field1": "some value", "field2": "another value"}

response = requests.post(url, headers=headers, files=files, data=data)
print(response.json())
```

## Response Format

### Success Response
```json
{
  "success": true,
  "message": "File uploaded successfully. Batch ID: 123e4567-e89b-12d3-a456-426614174000",
  "batch_id": "123e4567-e89b-12d3-a456-426614174000",
  "filename": "data.xlsx",
  "file_size_bytes": 45678,
  "fields": {
    "field1": "some value",
    "field2": "another value"
  }
}
```

### Error Response
```json
{
  "detail": "Invalid file type. Only Excel (.xlsx, .xls) and CSV files are supported"
}
```

## Features Implemented

✅ File upload with multipart support  
✅ SHA-256 hash deduplication  
✅ Database record creation  
✅ Placeholder fields for future expansion  
✅ Modern HTML form UI  
✅ Success/error handling  
✅ File type validation  

## Future Enhancements (TODO)

🔲 Store file content to filesystem or S3  
🔲 Parse Excel/CSV files  
🔲 Extract and validate data from files  
🔲 Update specific database tables based on data type  
🔲 Add progress tracking for large files  
🔲 Add data preview before final submission  
🔲 Replace placeholder fields with actual data fields  
🔲 Add batch processing status page  
🔲 Add file format templates/examples  

## Files Changed/Created

- ✅ `api/requirements.txt` - Added python-multipart
- ✅ `api/app/routers/upload.py` - New upload router
- ✅ `api/app/templates/upload_form.html` - HTML form
- ✅ `api/app/main.py` - Wired upload router and form route
- ✅ `shared/src/shared/schemas.py` - Added DataUploadRequest and DataUploadResponse
- ✅ `shared/src/shared/__init__.py` - Exported new schemas

## Testing

### Manual Testing

1. Install dependencies:
   ```bash
   cd api
   pip install -r requirements.txt
   ```

2. Start the server:
   ```bash
   uvicorn app.main:app --reload
   ```

3. Test the form:
   - Navigate to `http://localhost:8000/upload-form`
   - Upload a test Excel/CSV file
   - Check the database for the new `ingest_batch` record

4. Verify in database:
   ```sql
   SELECT * FROM ingest_batch 
   WHERE source_type = 'manual' 
   ORDER BY started_at DESC 
   LIMIT 5;
   ```

### API Documentation

The upload endpoint is automatically documented in FastAPI's interactive docs:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Notes

- The HTML form route (`/upload-form`) is currently public for easy development access
- The API endpoint requires an API key for security
- File content is read for hashing but not yet persisted to storage
- Placeholder fields can be replaced with actual business fields as requirements are clarified
- The form uses pure JavaScript (no framework dependencies)

## Security Considerations

- API key authentication required for the upload endpoint
- File type validation prevents arbitrary file uploads
- File size limits should be added in production
- Consider adding rate limiting
- Consider adding CSRF protection for the form
- Store uploaded files in a secure location with access controls
