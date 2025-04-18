import os
import json
import yaml
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, HTTPException, Depends, status, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.core.config import settings
from app.core.security import get_api_key
from app.services.transformer import RuleEngine

router = APIRouter()

class RuleRequest(BaseModel):
    """Request model for creating or updating transformation rules."""
    rules: Dict[str, str] = Field(..., description="Dictionary of transformation rules")
    filename: str = Field(..., description="Filename to save rules")

class RuleValidationRequest(BaseModel):
    """Request model for validating rules against a CSV file."""
    rules: Dict[str, str] = Field(..., description="Dictionary of transformation rules")
    input_file: str = Field(..., description="Name of the input CSV file to validate against")
    reference_file: Optional[str] = Field(None, description="Name of the reference CSV file")

@router.get("/list", summary="List available rule files")
async def list_rules(
    api_key: str = Depends(get_api_key)
):
    """
    List all available rule files.
    """
    # Create upload folder if it doesn't exist
    os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
    
    rules_files = []
    
    for filename in os.listdir(settings.UPLOAD_FOLDER):
        file_path = os.path.join(settings.UPLOAD_FOLDER, filename)
        
        # Only include JSON and YAML files that likely contain rules
        if os.path.isfile(file_path) and filename.lower().endswith(('.json', '.yaml', '.yml')):
            # Get file stats
            file_stats = os.stat(file_path)
            
            # Try to load the file as rules
            try:
                rule_engine = RuleEngine(rules_file=file_path)
                rules_count = len(rule_engine.rules)
                
                rules_files.append({
                    "name": filename,
                    "size_bytes": file_stats.st_size,
                    "created_at": file_stats.st_ctime,
                    "rules_count": rules_count,
                    "format": os.path.splitext(filename)[1][1:]  # Remove the leading dot
                })
            except Exception:
                # Skip files that can't be loaded as rules
                pass
    
    # Sort files by creation time (newest first)
    rules_files.sort(key=lambda x: x["created_at"], reverse=True)
    
    return {"rules_files": rules_files}


@router.get("/get/{filename}", summary="Get rules from a file")
async def get_rules(
    filename: str,
    api_key: str = Depends(get_api_key)
):
    """
    Get transformation rules from a file.
    
    - **filename**: Name of the rules file
    """
    file_path = os.path.join(settings.UPLOAD_FOLDER, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rules file not found: {filename}"
        )
    
    try:
        rule_engine = RuleEngine(rules_file=file_path)
        return {"rules": rule_engine.rules}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error loading rules: {str(e)}"
        )


@router.post("/create", summary="Create or update rules")
async def create_rules(
    request: RuleRequest,
    api_key: str = Depends(get_api_key)
):
    """
    Create or update transformation rules.
    
    - **rules**: Dictionary of transformation rules
    - **filename**: Filename to save rules
    """
    # Determine file format from extension
    file_ext = os.path.splitext(request.filename)[1].lower()
    if file_ext not in ['.json', '.yaml', '.yml']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file extension. Supported formats: .json, .yaml, .yml"
        )
    
    # Create the rules file
    file_path = os.path.join(settings.UPLOAD_FOLDER, request.filename)
    
    try:
        # Create rule engine with the provided rules
        rule_engine = RuleEngine(rules_dict=request.rules)
        
        # Save rules to file
        rule_engine.save_rules_to_file(file_path)
        
        return {
            "message": f"Rules saved to {request.filename}",
            "rules_count": len(request.rules),
            "filename": request.filename
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error saving rules: {str(e)}"
        )


@router.post("/upload", summary="Upload a rules file")
async def upload_rules_file(
    file: UploadFile = File(...),
    overwrite: bool = Form(False),
    api_key: str = Depends(get_api_key)
):
    """
    Upload a rules file (JSON or YAML).
    
    - **file**: Rules file to upload
    - **overwrite**: Whether to overwrite existing file with same name
    """
    # Check file extension
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in ['.json', '.yaml', '.yml']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file extension. Supported formats: .json, .yaml, .yml"
        )
    
    # Create upload directory if it doesn't exist
    os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
    
    # Set target filename
    target_filename = file.filename
    file_path = os.path.join(settings.UPLOAD_FOLDER, target_filename)
    
    # Check if file already exists
    if os.path.exists(file_path) and not overwrite:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"File {target_filename} already exists. Use overwrite=true to replace it."
        )
    
    try:
        # Save the file
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Validate the file contains valid rules
        try:
            rule_engine = RuleEngine(rules_file=file_path)
            rules_count = len(rule_engine.rules)
        except Exception as e:
            # Remove the invalid file
            os.remove(file_path)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid rules file: {str(e)}"
            )
        
        return {
            "filename": target_filename,
            "size_bytes": os.path.getsize(file_path),
            "rules_count": rules_count,
            "format": file_ext[1:]  # Remove the leading dot
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading rules file: {str(e)}"
        )


@router.post("/validate", summary="Validate rules against a CSV file")
async def validate_rules(
    request: RuleValidationRequest,
    api_key: str = Depends(get_api_key)
):
    """
    Validate that rules can be applied to a CSV file.
    
    - **rules**: Dictionary of transformation rules
    - **input_file**: Name of the input CSV file to validate against
    - **reference_file**: Name of the reference CSV file (optional)
    """
    # Import parser
    from app.services.parser import CSVParser
    
    # Construct file paths
    input_path = os.path.join(settings.UPLOAD_FOLDER, request.input_file)
    
    if not os.path.exists(input_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Input file not found: {request.input_file}"
        )
    
    reference_path = None
    if request.reference_file:
        reference_path = os.path.join(settings.UPLOAD_FOLDER, request.reference_file)
        if not os.path.exists(reference_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Reference file not found: {request.reference_file}"
            )
    
    try:
        # Create rule engine
        rule_engine = RuleEngine(rules_dict=request.rules)
        
        # Load sample data from input and reference files
        if reference_path:
            # Get join keys from the rules
            # This is a simple approach - just use columns that are in both files
            input_columns = set(CSVParser.get_columns(input_path))
            ref_columns = set(CSVParser.get_columns(reference_path))
            common_columns = input_columns.intersection(ref_columns)
            
            if not common_columns:
                return {
                    "valid": False,
                    "message": "No common columns found for joining input and reference files",
                    "rule_validations": {}
                }
            
            # Create join keys dictionary (use same column name for both sides)
            join_keys = {col: col for col in common_columns}
            
            # Load joined data sample
            sample_df = CSVParser.process_in_chunks(
                input_file=input_path,
                reference_file=reference_path,
                join_keys=join_keys,
                chunk_size=10  # Only need a small sample
            )
        else:
            # Load input data sample
            sample_df = pd.read_csv(input_path, nrows=10)
        
        # Validate rules against the sample data
        validation_results = rule_engine.validate_rules(sample_df)
        
        # Check overall validity
        all_valid = all(validation_results.values())
        
        return {
            "valid": all_valid,
            "message": "All rules are valid" if all_valid else "Some rules are invalid",
            "rule_validations": validation_results
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error validating rules: {str(e)}"
        )


@router.delete("/{filename}", summary="Delete a rules file")
async def delete_rules_file(
    filename: str,
    api_key: str = Depends(get_api_key)
):
    """
    Delete a rules file.
    
    - **filename**: Name of the rules file to delete
    """
    file_path = os.path.join(settings.UPLOAD_FOLDER, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rules file not found: {filename}"
        )
    
    try:
        os.remove(file_path)
        return {"message": f"Rules file deleted: {filename}"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting rules file: {str(e)}"
        )