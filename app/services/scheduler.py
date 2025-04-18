import logging
import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI

from app.core.config import settings
from app.services.report_generator import ReportGenerator

logger = logging.getLogger(__name__)

scheduler = None

def setup_scheduler(app: FastAPI) -> None:
    """
    Set up the APScheduler for scheduled report generation.
    
    Args:
        app: FastAPI instance
    """
    global scheduler
    
    jobstores = {
        'default': SQLAlchemyJobStore(url=settings.SQLALCHEMY_DATABASE_URI)
    }
    
    scheduler = AsyncIOScheduler(jobstores=jobstores)
    
    schedule_config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'rules', 'schedules.json')
    if os.path.exists(schedule_config_path):
        try:
            with open(schedule_config_path, 'r') as f:
                schedules = json.load(f)
            
            for job_id, job_config in schedules.items():
                add_scheduled_job(
                    job_id=job_id,
                    cron_expression=job_config.get('cron'),
                    input_file=job_config.get('input_file'),
                    rules_file=job_config.get('rules_file'),
                    reference_file=job_config.get('reference_file'),
                    join_keys=job_config.get('join_keys'),
                    output_format=job_config.get('output_format', 'csv')
                )
        except Exception as e:
            logger.error(f"Error loading scheduled jobs: {str(e)}")
    
    scheduler.start()
    
    @app.on_event("shutdown")
    async def shutdown_scheduler():
        if scheduler:
            scheduler.shutdown()


def add_scheduled_job(
    job_id: str,
    cron_expression: str,
    input_file: str,
    rules_file: str,
    reference_file: Optional[str] = None,
    join_keys: Optional[Dict[str, str]] = None,
    output_format: str = "csv"
) -> bool:
    """
    Add a scheduled job for report generation.
    
    Args:
        job_id: Unique ID for the job
        cron_expression: Cron expression for scheduling
        input_file: Path to input CSV file
        rules_file: Path to rules file
        reference_file: Path to reference CSV file (optional)
        join_keys: Dictionary mapping input keys to reference keys
        output_format: Output format (csv, xlsx, json)
        
    Returns:
        True if job was added successfully, False otherwise
    """
    if not scheduler:
        logger.error("Scheduler not initialized")
        return False
    
    try:
        trigger = CronTrigger.from_crontab(cron_expression)
        
        scheduler.add_job(
            generate_scheduled_report,
            trigger=trigger,
            id=job_id,
            kwargs={
                'input_file': input_file,
                'rules_file': rules_file,
                'reference_file': reference_file,
                'join_keys': join_keys,
                'output_format': output_format,
                'job_id': job_id
            },
            replace_existing=True
        )
        
        logger.info(f"Added scheduled job {job_id} with cron: {cron_expression}")
        return True
    except Exception as e:
        logger.error(f"Error adding scheduled job {job_id}: {str(e)}")
        return False


def remove_scheduled_job(job_id: str) -> bool:
    """
    Remove a scheduled job.
    
    Args:
        job_id: ID of the job to remove
        
    Returns:
        True if job was removed, False otherwise
    """
    if not scheduler:
        logger.error("Scheduler not initialized")
        return False
    
    try:
        scheduler.remove_job(job_id)
        logger.info(f"Removed scheduled job: {job_id}")
        return True
    except Exception as e:
        logger.error(f"Error removing scheduled job {job_id}: {str(e)}")
        return False


def list_scheduled_jobs() -> List[Dict[str, Any]]:
    """
    List all scheduled jobs.
    
    Returns:
        List of job dictionaries
    """
    if not scheduler:
        logger.error("Scheduler not initialized")
        return []
    
    jobs = []
    
    for job in scheduler.get_jobs():
        try:
            # Extract job details
            jobs.append({
                'id': job.id,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'cron_trigger': str(job.trigger),
                'input_file': job.kwargs.get('input_file'),
                'rules_file': job.kwargs.get('rules_file'),
                'reference_file': job.kwargs.get('reference_file'),
                'output_format': job.kwargs.get('output_format')
            })
        except Exception as e:
            logger.error(f"Error extracting job details: {str(e)}")
    
    return jobs


async def generate_scheduled_report(
    input_file: str,
    rules_file: str,
    reference_file: Optional[str] = None,
    join_keys: Optional[Dict[str, str]] = None,
    output_format: str = "csv",
    job_id: str = ""
) -> None:
    """
    Generate a report based on a scheduled job.
    
    Args:
        input_file: Path to input CSV file
        rules_file: Path to rules file
        reference_file: Path to reference CSV file (optional)
        join_keys: Dictionary mapping input keys to reference keys
        output_format: Output format (csv, xlsx, json)
        job_id: ID of the scheduled job
    """
    logger.info(f"Executing scheduled report generation job: {job_id}")
    
    try:
        generator = ReportGenerator(
            input_file=input_file,
            rules_file=rules_file,
            reference_file=reference_file,
            join_keys=join_keys,
            output_format=output_format
        )
        
        report_path, report_id = generator.generate_report()
        
        logger.info(f"Scheduled job {job_id} completed, generated report: {report_id}")
    except Exception as e:
        logger.error(f"Error in scheduled job {job_id}: {str(e)}")


def save_schedule_config() -> bool:
    """
    Save current schedule configuration to file.
    
    Returns:
        True if saved successfully, False otherwise
    """
    if not scheduler:
        logger.error("Scheduler not initialized")
        return False
    
    try:
        schedules = {}
        
        for job in scheduler.get_jobs():
            schedules[job.id] = {
                'cron': str(job.trigger),
                'input_file': job.kwargs.get('input_file'),
                'rules_file': job.kwargs.get('rules_file'),
                'reference_file': job.kwargs.get('reference_file'),
                'join_keys': job.kwargs.get('join_keys'),
                'output_format': job.kwargs.get('output_format', 'csv')
            }
        
        rules_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'rules')
        os.makedirs(rules_dir, exist_ok=True)
        
        schedule_config_path = os.path.join(rules_dir, 'schedules.json')
        with open(schedule_config_path, 'w') as f:
            json.dump(schedules, f, indent=2)
        
        logger.info(f"Saved {len(schedules)} scheduled jobs to {schedule_config_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving schedule configuration: {str(e)}")
        return False