import os
from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, status, BackgroundTasks, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from app.core.config import settings
from app.core.security import get_api_key
from app.services.report_generator import ReportGenerator, ReportManager

router = APIRouter()

class ReportGenerationRequest(BaseModel):
    """Request model for report generation."""
    input_file: str = Field(..., description="Name of the input CSV file")
    rules_file: str = Field(..., description="Name of the rules file")
    reference_file: Optional[str] = Field(None, description="Name of the reference CSV file")
    join_keys: Optional[Dict[str, str]] = Field(None, description="Dictionary mapping input keys to reference keys")
    output_format: str = Field("csv", description="Output format (csv, xlsx, json)")

class ScheduledReportRequest(BaseModel):
    """Request model for scheduled report generation."""
    job_id: str = Field(..., description="Unique ID for the scheduled job")
    cron_expression: str = Field(..., description="Cron expression for scheduling")
    input_file: str = Field(..., description="Name of the input CSV file")
    rules_file: str = Field(..., description="Name of the rules file")
    reference_file: Optional[str] = Field(None, description="Name of the reference CSV file")
    join_keys: Optional[Dict[str, str]] = Field(None, description="Dictionary mapping input keys to reference keys")
    output_format: str = Field("csv", description="Output format (csv, xlsx, json)")

@router.post("/generate", summary="Generate a report")
async def generate_report(
    request: ReportGenerationRequest,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(get_api_key)
):
    """
    Generate a report by applying transformation rules to input data.
    
    - **input_file**: Name of the input CSV file
    - **rules_file**: Name of the rules file
    - **reference_file**: Name of the reference CSV file (optional)
    - **join_keys**: Dictionary mapping input keys to reference keys
    - **output_format**: Output format (csv, xlsx, json)
    """
    # Construct full file paths
    input_path = os.path.join(settings.UPLOAD_FOLDER, request.input_file)
    rules_path = os.path.join(settings.UPLOAD_FOLDER, request.rules_file)
    reference_path = None
    
    if request.reference_file:
        reference_path = os.path.join(settings.UPLOAD_FOLDER, request.reference_file)
    
    # Validate files exist
    if not os.path.exists(input_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Input file not found: {request.input_file}"
        )
    
    if not os.path.exists(rules_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rules file not found: {request.rules_file}"
        )
    
    if reference_path and not os.path.exists(reference_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Reference file not found: {request.reference_file}"
        )
    
    # Validate output format
    if request.output_format not in settings.SUPPORTED_FORMATS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported output format: {request.output_format}. "
                   f"Supported formats: {', '.join(settings.SUPPORTED_FORMATS)}"
        )
    
    try:
        # Create report generator
        report_generator = ReportGenerator(
            input_file=input_path,
            rules_file=rules_path,
            reference_file=reference_path,
            join_keys=request.join_keys,
            output_format=request.output_format
        )
        
        # For large reports, generate in background
        def generate_in_background():
            try:
                report_generator.generate_report()
            except Exception as e:
                # Log the error
                print(f"Error generating report: {str(e)}")
        
        # Start generation in the background
        background_tasks.add_task(generate_in_background)
        
        return {
            "status": "processing",
            "message": "Report generation started in the background",
            "input_file": request.input_file,
            "reference_file": request.reference_file,
            "rules_file": request.rules_file,
            "output_format": request.output_format
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating report: {str(e)}"
        )


@router.get("/list", summary="List available reports")
async def list_reports(
    api_key: str = Depends(get_api_key)
):
    """
    List all available reports.
    """
    reports = ReportManager.list_reports()
    return {"reports": reports}


@router.get("/{report_id}", summary="Download a report")
async def download_report(
    report_id: str,
    api_key: str = Depends(get_api_key)
):
    """
    Download a report by ID.
    
    - **report_id**: ID of the report to download
    """
    report_path = ReportManager.get_report_path(report_id)
    
    if not report_path or not os.path.exists(report_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report not found: {report_id}"
        )
    
    # Determine content type
    content_type = "text/csv"
    filename = os.path.basename(report_path)
    
    if filename.endswith(".xlsx"):
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif filename.endswith(".json"):
        content_type = "application/json"
    
    return FileResponse(
        path=report_path,
        filename=filename,
        media_type=content_type
    )


@router.delete("/{report_id}", summary="Delete a report")
async def delete_report(
    report_id: str,
    api_key: str = Depends(get_api_key)
):
    """
    Delete a report by ID.
    
    - **report_id**: ID of the report to delete
    """
    success = ReportManager.delete_report(report_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report not found or could not be deleted: {report_id}"
        )
    
    return {"message": f"Report deleted: {report_id}"}


@router.post("/schedule", summary="Schedule a report generation")
async def schedule_report(
    request: ScheduledReportRequest,
    api_key: str = Depends(get_api_key)
):
    """
    Schedule a report to be generated on a recurring basis.
    
    - **job_id**: Unique ID for the scheduled job
    - **cron_expression**: Cron expression for scheduling
    - **input_file**: Name of the input CSV file
    - **rules_file**: Name of the rules file
    - **reference_file**: Name of the reference CSV file (optional)
    - **join_keys**: Dictionary mapping input keys to reference keys
    - **output_format**: Output format (csv, xlsx, json)
    """
    # Import here to avoid circular imports
    from app.services.scheduler import add_scheduled_job, save_schedule_config
    
    # Construct full file paths
    input_path = os.path.join(settings.UPLOAD_FOLDER, request.input_file)
    rules_path = os.path.join(settings.UPLOAD_FOLDER, request.rules_file)
    reference_path = None
    
    if request.reference_file:
        reference_path = os.path.join(settings.UPLOAD_FOLDER, request.reference_file)
    
    # Validate files exist
    if not os.path.exists(input_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Input file not found: {request.input_file}"
        )
    
    if not os.path.exists(rules_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rules file not found: {request.rules_file}"
        )
    
    if reference_path and not os.path.exists(reference_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Reference file not found: {request.reference_file}"
        )
    
    # Add the scheduled job
    success = add_scheduled_job(
        job_id=request.job_id,
        cron_expression=request.cron_expression,
        input_file=input_path,
        rules_file=rules_path,
        reference_file=reference_path,
        join_keys=request.join_keys,
        output_format=request.output_format
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to schedule the report generation"
        )
    
    # Save the schedule configuration
    save_schedule_config()
    
    return {
        "status": "scheduled",
        "job_id": request.job_id,
        "cron_expression": request.cron_expression,
        "input_file": request.input_file,
        "rules_file": request.rules_file,
        "reference_file": request.reference_file,
        "output_format": request.output_format
    }


@router.get("/schedule/list", summary="List scheduled report generations")
async def list_scheduled_reports(
    api_key: str = Depends(get_api_key)
):
    """
    List all scheduled report generations.
    """
    # Import here to avoid circular imports
    from app.services.scheduler import list_scheduled_jobs
    
    jobs = list_scheduled_jobs()
    return {"scheduled_jobs": jobs}


@router.delete("/schedule/{job_id}", summary="Delete a scheduled report generation")
async def delete_scheduled_report(
    job_id: str,
    api_key: str = Depends(get_api_key)
):
    """
    Delete a scheduled report generation by job ID.
    
    - **job_id**: ID of the scheduled job to delete
    """
    # Import here to avoid circular imports
    from app.services.scheduler import remove_scheduled_job, save_schedule_config
    
    success = remove_scheduled_job(job_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scheduled job not found or could not be deleted: {job_id}"
        )
    
    # Save the updated schedule configuration
    save_schedule_config()
    
    return {"message": f"Scheduled job deleted: {job_id}"}