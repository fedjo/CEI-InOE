"""
Data upload endpoints.

Provides endpoints for uploading data files (Excel, CSV) and updating database records.
This is a skeleton implementation with placeholder fields.
"""

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional
import hashlib

from app.db.session import get_db
from shared import DataUploadResponse, IngestBatchCreate
from shared.models import IngestBatch

router = APIRouter()


@router.post("/data", response_model=DataUploadResponse)
async def upload_data(
    file: UploadFile = File(..., description="Excel file to upload"),
    field1: Optional[str] = Form(None, description="Placeholder field 1"),
    field2: Optional[str] = Form(None, description="Placeholder field 2"),
    db: Session = Depends(get_db),
):
    """
    Upload data file (Excel) with placeholder fields.
    
    This is a skeleton implementation that:
    - Accepts an Excel file upload
    - Captures two placeholder text fields
    - Creates a minimal IngestBatch record in the database
    - Returns success response with batch ID
    
    The actual parsing and data processing will be added later.
    """
    
    # Validate file type
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    if not file.filename.lower().endswith(('.xlsx', '.xls', '.csv')):
        raise HTTPException(
            status_code=400, 
            detail="Invalid file type. Only Excel (.xlsx, .xls) and CSV files are supported"
        )
    
    # Read file content and compute hash
    file_content = await file.read()
    file_size = len(file_content)
    file_hash = hashlib.sha256(file_content).hexdigest()
    
    # Reset file pointer (in case we need it later)
    await file.seek(0)
    
    # Check if file was already uploaded (based on hash)
    existing_batch = db.query(IngestBatch).filter(
        IngestBatch.file_sha256 == file_hash
    ).first()
    
    if existing_batch:
        return DataUploadResponse(
            success=True,
            message="File already uploaded (duplicate detected)",
            batch_id=existing_batch.batch_id,
            filename=file.filename,
            file_size_bytes=file_size,
            fields={"field1": field1, "field2": field2}
        )
    
    # Create IngestBatch record as placeholder
    batch_data = IngestBatchCreate(
        source_type="manual",
        source_name=file.filename,
        datasource_id=None,  # Placeholder - to be filled when datasource is known
        granularity=None,
        date_range_start=None,
        date_range_end=None,
        file_sha256=file_hash,
        pipeline_version="skeleton-1.0"
    )
    
    # Create the batch record
    batch = IngestBatch(
        source_type=batch_data.source_type,
        source_name=batch_data.source_name,
        datasource_id=batch_data.datasource_id,
        granularity=batch_data.granularity,
        date_range_start=batch_data.date_range_start,
        date_range_end=batch_data.date_range_end,
        file_sha256=batch_data.file_sha256,
        pipeline_version=batch_data.pipeline_version,
        status="uploaded",  # Custom status for uploaded but not processed files
        records_loaded=0,
        records_failed=0,
        started_at=datetime.utcnow(),
    )
    
    db.add(batch)
    db.commit()
    db.refresh(batch)
    
    # TODO: Store the actual file content somewhere (local filesystem, S3, etc.)
    # TODO: Parse the Excel file and extract data
    # TODO: Update the appropriate database tables based on the data
    
    return DataUploadResponse(
        success=True,
        message=f"File uploaded successfully. Batch ID: {batch.batch_id}",
        batch_id=batch.batch_id,
        filename=file.filename,
        file_size_bytes=file_size,
        fields={"field1": field1, "field2": field2}
    )
