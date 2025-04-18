import os
import shutil
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, status
from fastapi.responses import JSONResponse
import pandas as pd

from app.core.config import settings
from app.core.security import get_api_key
from app.services.parser import CSVParser

router = APIRouter()

@router.post("/input", summary="Upload input CSV file")
async def upload_input_file(
    file: UploadFile = File(...),
    overwrite: bool = Form(False),
    api_key: str = Depends(get_api_key)
):
    """
    Upload an input CSV file for report generation.
    
    - **file**: CSV file to upload
    - **overwrite**: Whether to overwrite existing file with same name
    """
    return await _handle_file_upload(file, "input", overwrite)


@router.post("/reference", summary="Upload reference CSV file")
async def upload_reference_file(
    file: UploadFile = File(...),
    overwrite: bool = Form(False),
    api_key: str = Depends(get_api_key)
):
    """
    Upload a reference CSV file for joining with input data.
    
    - **file**: CSV file to upload
    - **overwrite**: Whether to overwrite existing file with same name
    """
    return await _handle_file_upload(file, "reference", overwrite)


@router.get("/list", summary="List uploaded files")
async def list_uploaded_files(
    file_type: Optional[str] = None,
    api_key: str = Depends(get_api_key)
):
    """
    List all uploaded files.
    
    - **file_type**: Filter by file type (input, reference)
    """
    os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
    
    files = []
    
    for filename in os.listdir(settings.UPLOAD_FOLDER):
        file_path = os.path.join(settings.UPLOAD_FOLDER, filename)
        
        if os.path.isfile(file_path):
            current_file_type = None
            if filename.startswith("input_"):
                current_file_type = "input"
            elif filename.startswith("reference_"):
                current_file_type = "reference"
            
            if file_type and current_file_type != file_type:
                continue
            
            file_stats = os.stat(file_path)
            
            columns = []
            if file_path.lower().endswith('.csv'):
                columns = CSVParser.get_columns(file_path)
            
            files.append({
                "name": filename,
                "size_bytes": file_stats.st_size,
                "created_at": file_stats.st_ctime,
                "file_type": current_file_type,
                "columns": columns
            })
    
    files.sort(key=lambda x: x["created_at"], reverse=True)
    
    return {"files": files}


@router.get("/sample/{filename}", summary="Get sample data from a file")
async def get_sample_data(
    filename: str,
    rows: int = 5,
    api_key: str = Depends(get_api_key)
):
    """
    Get sample data from a CSV file.
    
    - **filename**: Name of the file
    - **rows**: Number of rows to sample
    """
    file_path = os.path.join(settings.UPLOAD_FOLDER, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File not found: {filename}"
        )
    
    if not file_path.lower().endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File is not a CSV"
        )
    
    try:
        # Get sample data
        sample_data = CSVParser.get_sample_data(file_path, rows)
        
        return {
            "filename": filename,
            "sample_rows": sample_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading sample data: {str(e)}"
        )


@router.delete("/{filename}", summary="Delete an uploaded file")
async def delete_file(
    filename: str,
    api_key: str = Depends(get_api_key)
):
    """
    Delete an uploaded file.
    
    - **filename**: Name of the file to delete
    """
    file_path = os.path.join(settings.UPLOAD_FOLDER, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File not found: {filename}"
        )
    
    try:
        os.remove(file_path)
        return {"message": f"File deleted: {filename}"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting file: {str(e)}"
        )


async def _handle_file_upload(file: UploadFile, file_prefix: str, overwrite: bool) -> Dict[str, Any]:
    """
    Handle file upload with common logic.
    
    Args:
        file: File to upload
        file_prefix: Prefix for the file (input, reference)
        overwrite: Whether to overwrite existing file
        
    Returns:
        Response with file details
    """
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are supported"
        )
    
    os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
    
    target_filename = f"{file_prefix}_{file.filename}"
    file_path = os.path.join(settings.UPLOAD_FOLDER, target_filename)
    
    if os.path.exists(file_path) and not overwrite:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"File {target_filename} already exists. Use overwrite=true to replace it."
        )
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        valid, error = CSVParser.validate_csv(file_path)
        if not valid:
            os.remove(file_path)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid CSV file: {error}"
            )
        
        columns = CSVParser.get_columns(file_path)
        
        return {
            "filename": target_filename,
            "original_filename": file.filename,
            "size_bytes": os.path.getsize(file_path),
            "columns": columns,
            "file_type": file_prefix
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading file: {str(e)}"
        )