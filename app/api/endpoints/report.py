import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, status, BackgroundTasks, Query
from fastapi.responses import FileResponse, JSONResponse
import logging

logger = logging.getLogger(__name__)

from app.core.config import settings
from app.core.security import get_api_key
from app.services.report_generator import ReportGenerator, ReportManager
from app.schemas.schemas import ReportRequest, ScheduledJobRequest

router = APIRouter()

@router.post("/generate", summary="Generate a report")
async def generate_report(
    request: ReportRequest,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(get_api_key)
):
    input_path = os.path.join(settings.UPLOAD_FOLDER, request.input_file)
    rules_path = os.path.join(settings.RULES_FOLDER, request.rules_file)
    reference_path = None
    if request.reference_file:
        reference_path = os.path.join(settings.UPLOAD_FOLDER, request.reference_file)
    if not os.path.exists(input_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Input file not found: {request.input_file}")
    if not os.path.exists(rules_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Rules file not found: {request.rules_file}")
    if reference_path and not os.path.exists(reference_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reference file not found: {request.reference_file}")
    if request.output_format not in settings.SUPPORTED_FORMATS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported output format: {request.output_format}. Supported formats: {', '.join(settings.SUPPORTED_FORMATS)}"
        )
    try:
        report_generator = ReportGenerator(
            input_file=input_path,
            rules_file=rules_path,
            reference_file=reference_path,
            join_keys=request.join_keys,
            output_format=request.output_format
        )
        async def generate_in_background(generator: ReportGenerator):
            try:
                report_path, report_id = generator.generate_report()
                print(f"Background task finished for report {report_id} at {report_path}")
            except Exception as e:
                print(f"Error in background report generation task: {str(e)}")
        background_tasks.add_task(generate_in_background, report_generator)
        return {
            "status": "processing",
            "message": "Report generation started in the background",
            "input_file": request.input_file,
            "reference_file": request.reference_file,
            "rules_file": request.rules_file,
            "output_format": request.output_format
        }
    except FileNotFoundError as fnf_e:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(fnf_e))
    except ValueError as val_e:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_e))
    except HTTPException:
         raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error initiating report generation: {str(e)}")

@router.get("/list", summary="List available reports")
async def list_reports(api_key: str = Depends(get_api_key)):
    reports = ReportManager.list_reports()
    return {"reports": reports}

@router.get("/{report_id}", summary="Download a report")
async def download_report(report_id: str, api_key: str = Depends(get_api_key)):
    """
    Download a report by ID.
    
    Args:
        report_id: ID of the report to download
        api_key: API key for authentication
        
    Returns:
        File response with the report content
    """
    try:
        # Get report path
        report_path = ReportManager.get_report_path(report_id)
        
        # Check if report exists
        if not report_path:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Report not found with ID: {report_id}"
            )
            
        if not os.path.exists(report_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Report file not found on disk: {report_id}"
            )
            
        # Determine content type
        filename = os.path.basename(report_path)
        file_extension = Path(filename).suffix.lower()
        content_type = "application/octet-stream"
        
        if file_extension == ".csv":
            content_type = "text/csv"
        elif file_extension == ".xlsx":
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif file_extension == ".json":
            content_type = "application/json"
            
        # Return file
        return FileResponse(
            path=report_path, 
            filename=filename, 
            media_type=content_type
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        # Log unexpected errors and return 500
        logger.error(f"Error downloading report {report_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error downloading report: {str(e)}"
        )

@router.delete("/{report_id}", summary="Delete a report")
async def delete_report(report_id: str, api_key: str = Depends(get_api_key)):
    success = ReportManager.delete_report(report_id)
    if not success:
        report_exists = ReportManager.get_report_path(report_id) is not None
        if report_exists:
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Report found but could not be deleted: {report_id}")
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Report not found: {report_id}")
    return {"message": f"Report deleted: {report_id}"}

@router.post("/schedule", summary="Schedule a report generation")
async def schedule_report(
    request: ScheduledJobRequest,
    api_key: str = Depends(get_api_key)
):
    from app.services.scheduler import add_scheduled_job, save_schedule_config
    input_path_check = os.path.join(settings.UPLOAD_FOLDER, request.input_file)
    rules_path_check = os.path.join(settings.RULES_FOLDER, request.rules_file)
    reference_path_check = os.path.join(settings.UPLOAD_FOLDER, request.reference_file) if request.reference_file else None
    if not os.path.exists(input_path_check):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Input file not found: {request.input_file}")
    if not os.path.exists(rules_path_check):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Rules file not found: {request.rules_file}")
    if reference_path_check and not os.path.exists(reference_path_check):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reference file not found: {request.reference_file}")
    if request.output_format not in settings.SUPPORTED_FORMATS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported output format: {request.output_format}. Supported formats: {', '.join(settings.SUPPORTED_FORMATS)}")
    try:
        success = add_scheduled_job(
            job_id=request.job_id,
            cron_expression=request.cron_expression,
            input_file=request.input_file,
            rules_file=request.rules_file,
            reference_file=request.reference_file,
            join_keys=request.join_keys,
            output_format=request.output_format
        )
        if not success:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to schedule the report generation job: {request.job_id}")
        save_schedule_config()
        return {
            "status": "scheduled",
            "job_id": request.job_id,
            "cron_expression": request.cron_expression,
            "input_file": request.input_file,
            "rules_file": request.rules_file,
            "reference_file": request.reference_file,
            "output_format": request.output_format,
            "message": f"Report generation job '{request.job_id}' scheduled."
        }
    except ValueError as val_e:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid scheduling request: {val_e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error scheduling report generation: {str(e)}")

@router.get("/schedule/list", summary="List scheduled report generations")
async def list_scheduled_reports(api_key: str = Depends(get_api_key)):
    from app.services.scheduler import list_scheduled_jobs
    jobs = list_scheduled_jobs()
    return {"scheduled_jobs": jobs}

@router.delete("/schedule/{job_id}", summary="Delete a scheduled report generation")
async def delete_scheduled_report(job_id: str, api_key: str = Depends(get_api_key)):
    from app.services.scheduler import remove_scheduled_job, save_schedule_config
    success = remove_scheduled_job(job_id)
    if not success:
        from app.services.scheduler import list_scheduled_jobs
        existing_jobs = list_scheduled_jobs()
        job_found = any(job['id'] == job_id for job in existing_jobs)
        if job_found:
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Scheduled job '{job_id}' found but could not be deleted.")
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Scheduled job not found: {job_id}")
    save_schedule_config()
    return {"message": f"Scheduled job deleted: {job_id}"}