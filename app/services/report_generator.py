import os
import uuid
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any, Generator
from datetime import datetime
from pathlib import Path
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
import re

from app.core.config import settings
from app.services.parser import CSVParser
from app.services.transformer import RuleEngine

logger = logging.getLogger(__name__)

class ReportGenerator:
    """
    Service for generating reports by applying transformation rules to input data.
    """
    
    def __init__(self, 
                 input_file: str,
                 rules_file: str,
                 reference_file: Optional[str] = None,
                 join_keys: Optional[Dict[str, str]] = None,
                 output_format: str = "csv"):
        """
        Initialize the report generator.
        
        Args:
            input_file: Absolute path to the input CSV file
            rules_file: Absolute path to the rules file
            reference_file: Absolute path to the reference CSV file (optional)
            join_keys: Dictionary mapping input file keys to reference file keys
            output_format: Output format (csv, xlsx, json)
        """
        self.input_file = input_file
        self.rules_file = rules_file
        self.reference_file = reference_file
        self.join_keys = join_keys
        self.output_format = output_format
        
        if self.output_format not in settings.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported output format: {output_format}. "
                            f"Supported formats: {', '.join(settings.SUPPORTED_FORMATS)}")

    def generate_report(self) -> Tuple[str, str]:
        """
        Generate a report by processing input data with transformation rules.
        
        Returns:
            Tuple of (report_file_path, report_id)
        """
        report_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"report_{timestamp}_{report_id}.{self.output_format}"
        report_path = os.path.join(settings.REPORT_FOLDER, report_filename)
        
        logger.info(f"Starting report generation {report_id} to {report_path}")
        
        try:
            self._validate_input_files()
            try:
                rules_engine = RuleEngine(rules_file=self.rules_file)
                rules_dict = rules_engine.rules
                if not rules_dict:
                     logger.warning(f"No rules loaded from {self.rules_file}. Report will contain only input/joined data.")
            except Exception as e:
                logger.error(f"Failed to load rules from {self.rules_file}: {str(e)}")
                raise
            chunk_iterator = CSVParser.process_in_chunks(
                input_file=self.input_file,
                reference_file=self.reference_file,
                join_keys=self.join_keys,
                chunk_size=settings.CSV_CHUNK_SIZE
            )
            all_processed_chunks = self._apply_rules_in_parallel(chunk_iterator, rules_dict)
            if not all_processed_chunks:
                 logger.warning(f"No data chunks processed for report {report_id}. Result will be empty.")
                 final_df = pd.DataFrame()
            else:
                 final_df = pd.concat(all_processed_chunks, ignore_index=True)
            self._save_output(final_df, report_path)
            logger.info(f"Report generated successfully: {report_path}")
            return report_path, report_id
        except FileNotFoundError as fnf_e:
             logger.error(f"Report generation failed due to file not found: {fnf_e}")
             from fastapi import HTTPException, status
             raise HTTPException(
                 status_code=status.HTTP_404_NOT_FOUND,
                 detail=f"Required file not found: {fnf_e}"
             )
        except ValueError as val_e:
             logger.error(f"Report generation failed due to configuration/data error: {val_e}")
             from fastapi import HTTPException, status
             raise HTTPException(
                 status_code=status.HTTP_400_BAD_REQUEST,
                 detail=f"Configuration or data error: {val_e}"
             )
        except Exception as e:
            logger.error(f"Unexpected error generating report {report_id}: {str(e)}", exc_info=True)
            from fastapi import HTTPException, status
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error generating report: {str(e)}"
            )
    
    def _validate_input_files(self) -> None:
        """
        Validate that all required input files exist and have correct format.
        Uses absolute paths provided during initialization.
        """
        valid, error = CSVParser.validate_csv(self.input_file)
        if not valid:
            raise FileNotFoundError(f"Invalid input file '{os.path.basename(self.input_file)}': {error}")
        if self.reference_file:
            valid, error = CSVParser.validate_csv(self.reference_file)
            if not valid:
                raise FileNotFoundError(f"Invalid reference file '{os.path.basename(self.reference_file)}': {error}")
            if self.join_keys:
                ref_columns = CSVParser.get_columns(self.reference_file)
                missing_ref_keys = [ref_key for ref_key in self.join_keys.values() if ref_key not in ref_columns]
                if missing_ref_keys:
                    raise ValueError(f"Reference file '{os.path.basename(self.reference_file)}' is missing required join columns: {missing_ref_keys}")

    def _apply_rules_in_parallel(self, chunk_iterator: Generator[pd.DataFrame, None, None], rules_dict: Dict[str, str]) -> List[pd.DataFrame]:
         """
         Applies rules to DataFrame chunks in parallel using multiprocessing.
         """
         logger.info(f"Applying rules to data chunks using {settings.MAX_WORKERS} workers...")
         all_processed_chunks = []
         with ProcessPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
             futures = []
             try:
                for i, chunk in enumerate(chunk_iterator):
                    if chunk.empty:
                         logger.debug(f"Skipping empty chunk {i+1}")
                         continue
                    future = executor.submit(ReportGenerator._process_chunk_static, chunk, rules_dict)
                    futures.append(future)
                    logger.debug(f"Submitted chunk {i+1} for processing.")
                for i, future in enumerate(as_completed(futures)):
                    try:
                        processed_chunk = future.result()
                        if processed_chunk is not None and not processed_chunk.empty:
                            all_processed_chunks.append(processed_chunk)
                            logger.debug(f"Collected processed chunk {i+1}/{len(futures)}.")
                        elif processed_chunk is not None:
                             logger.debug(f"Collected empty processed chunk {i+1}/{len(futures)}.")
                        else:
                             logger.warning(f"Collected None result for chunk {i+1}/{len(futures)}.")
                    except Exception as e:
                        logger.error(f"Error processing a chunk in worker: {str(e)}", exc_info=True)
             except Exception as e:
                  logger.error(f"Error during chunk iteration or submission: {str(e)}", exc_info=True)
         logger.info(f"Finished processing chunks. Collected {len(all_processed_chunks)} non-empty processed chunks.")
         return all_processed_chunks

    @staticmethod
    def _process_chunk_static(chunk: pd.DataFrame, rules_dict: Dict[str, str]) -> pd.DataFrame:
        chunk_rule_engine = RuleEngine(rules_dict=rules_dict)
        return chunk_rule_engine.apply_rules(chunk)
    
    def _save_output(self, df: pd.DataFrame, output_path: str) -> None:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        try:
            if self.output_format == "csv":
                df.to_csv(output_path, index=False)
            elif self.output_format == "xlsx":
                df.to_excel(output_path, index=False)
            elif self.output_format == "json":
                df.to_json(output_path, orient="records", indent=2)
            else:
                raise ValueError(f"Unsupported output format: {self.output_format}")
            logger.info(f"Saved output to {output_path} in {self.output_format} format")
        except Exception as e:
            logger.error(f"Error saving output to {output_path}: {str(e)}")
            raise

class ReportManager:
    """
    Manager class for handling report metadata and retrieval.
    """
    @staticmethod
    def list_reports() -> List[Dict[str, Any]]:
        reports = []
        try:
            if not os.path.exists(settings.REPORT_FOLDER):
                return reports
            for entry_name in os.listdir(settings.REPORT_FOLDER):
                file_path = os.path.join(settings.REPORT_FOLDER, entry_name)
                if os.path.isfile(file_path):
                    filename = os.path.basename(file_path)
                    stem = Path(filename).stem
                    suffix = Path(filename).suffix[1:]
                    parts = stem.split('_')
                    if len(parts) >= 3 and parts[0] == "report":
                        timestamp_str = f"{parts[1]}_{parts[2]}"
                        report_id = '_'.join(parts[3:])
                        if not report_id:
                             continue
                        try:
                            timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                            file_stats = os.stat(file_path)
                            file_size = file_stats.st_size
                            reports.append({
                                "id": report_id,
                                "filename": filename,
                                "created_at": timestamp.isoformat(),
                                "size_bytes": file_size,
                                "format": suffix
                            })
                        except ValueError:
                             logger.debug(f"Skipping file '{filename}' in report folder: Timestamp parsing failed.")
                             pass
                        except Exception as e:
                            logger.warning(f"Error getting file stats or metadata for {filename}: {str(e)}")
                            pass
            reports.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        except FileNotFoundError:
             pass
        except Exception as e:
            logger.error(f"Error listing reports in {settings.REPORT_FOLDER}: {str(e)}")
            return reports
        return reports
    
    @staticmethod
    def get_report_path(report_id: str) -> Optional[str]:
        """
        Get the file path for a report by ID.
        
        Args:
            report_id: ID of the report to find
            
        Returns:
            Full path to the report file, or None if not found
        """
        if not os.path.exists(settings.REPORT_FOLDER):
            logger.warning(f"Report folder does not exist: {settings.REPORT_FOLDER}")
            return None
        
        try:
            expected_filename_pattern = f"*_{report_id}.csv"
            
            for entry_name in os.listdir(settings.REPORT_FOLDER):
                if entry_name.endswith(f"_{report_id}.csv"):
                    file_path = os.path.join(settings.REPORT_FOLDER, entry_name)
                    if os.path.isfile(file_path):
                        return file_path
            
            for ext in ['.xlsx', '.json']:
                for entry_name in os.listdir(settings.REPORT_FOLDER):
                    if entry_name.endswith(f"_{report_id}{ext}"):
                        file_path = os.path.join(settings.REPORT_FOLDER, entry_name)
                        if os.path.isfile(file_path):
                            return file_path
                            
            return None
            
        except Exception as e:
            logger.error(f"Error searching for report ID {report_id}: {str(e)}")
            return None
    
    @staticmethod
    def delete_report(report_id: str) -> bool:
        report_path = ReportManager.get_report_path(report_id)
        if report_path and os.path.exists(report_path):
            try:
                os.remove(report_path)
                logger.info(f"Deleted report: {report_path}")
                return True
            except Exception as e:
                logger.error(f"Error deleting report {report_id} at {report_path}: {str(e)}")
                return False
        logger.warning(f"Attempted to delete report ID {report_id}, but file not found at expected location.")
        return False