import os
import uuid
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from pathlib import Path
import json
from concurrent.futures import ProcessPoolExecutor

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
            input_file: Path to the input CSV file
            rules_file: Path to the rules file
            reference_file: Path to the reference CSV file (optional)
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
        
        # Initialize rule engine
        self.rule_engine = RuleEngine(rules_file=rules_file)
    
    def generate_report(self) -> Tuple[str, str]:
        """
        Generate a report by processing input data with transformation rules.
        
        Returns:
            Tuple of (report_file_path, report_id)
        """
        # Generate a unique ID for this report
        report_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"report_{timestamp}_{report_id}.{self.output_format}"
        report_path = os.path.join(settings.REPORT_FOLDER, report_filename)
        
        logger.info(f"Generating report {report_id} to {report_path}")
        
        try:
            # Validate input files
            self._validate_input_files()
            
            # Process data in chunks with multiprocessing
            result_df = self._process_data()
            
            # Save the output in the requested format
            self._save_output(result_df, report_path)
            
            logger.info(f"Report generated successfully: {report_path}")
            return report_path, report_id
            
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise
    
    def _validate_input_files(self) -> None:
        """
        Validate that all required input files exist and have correct format.
        
        Raises:
            FileNotFoundError: If a required file is not found
            ValueError: If a file has invalid format
        """
        # Validate input file
        valid, error = CSVParser.validate_csv(self.input_file)
        if not valid:
            raise ValueError(f"Invalid input file: {error}")
        
        # Validate reference file if provided
        if self.reference_file:
            valid, error = CSVParser.validate_csv(self.reference_file)
            if not valid:
                raise ValueError(f"Invalid reference file: {error}")
    
    def _process_data(self) -> pd.DataFrame:
        """
        Process the input data with transformation rules.
        
        Returns:
            DataFrame with transformed data
        """
        logger.info(f"Processing data from {self.input_file}")
        
        # Get data (with optional join)
        data = CSVParser.process_in_chunks(
            input_file=self.input_file,
            reference_file=self.reference_file,
            join_keys=self.join_keys,
            chunk_size=settings.CSV_CHUNK_SIZE
        )
        
        # If processing in chunks, handle each chunk with multiprocessing
        if isinstance(data, pd.io.parsers.TextFileReader):  # This is a chunk iterator
            chunks = []
            
            # Process chunks in parallel using ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
                # Create a RuleEngine instance for each worker
                futures = []
                
                for chunk in data:
                    # Submit chunk for processing
                    futures.append(executor.submit(self._process_chunk, chunk))
                
                # Collect results
                for future in futures:
                    chunk_result = future.result()
                    if chunk_result is not None and not chunk_result.empty:
                        chunks.append(chunk_result)
            
            # Combine all processed chunks
            if not chunks:
                return pd.DataFrame()
            
            return pd.concat(chunks, ignore_index=True)
        else:
            # Single chunk processing
            return self.rule_engine.apply_rules(data)
    
    def _process_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Process a single chunk of data with transformation rules.
        
        Args:
            chunk: DataFrame chunk to process
            
        Returns:
            Processed DataFrame chunk
        """
        # Create a new RuleEngine instance to ensure thread safety
        chunk_rule_engine = RuleEngine(rules_file=self.rules_file)
        return chunk_rule_engine.apply_rules(chunk)
    
    def _save_output(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Save the output DataFrame to the specified format.
        
        Args:
            df: DataFrame to save
            output_path: Path to save the output
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        if self.output_format == "csv":
            df.to_csv(output_path, index=False)
        elif self.output_format == "xlsx":
            df.to_excel(output_path, index=False)
        elif self.output_format == "json":
            df.to_json(output_path, orient="records", indent=2)
        else:
            raise ValueError(f"Unsupported output format: {self.output_format}")
        
        logger.info(f"Saved output to {output_path} in {self.output_format} format")


class ReportManager:
    """
    Manager class for handling report metadata and retrieval.
    """
    
    @staticmethod
    def list_reports() -> List[Dict[str, Any]]:
        """
        List all available reports.
        
        Returns:
            List of report metadata dictionaries
        """
        reports = []
        
        try:
            # Check if the report folder exists
            if not os.path.exists(settings.REPORT_FOLDER):
                return reports
            
            # List all files in the report folder
            for filename in os.listdir(settings.REPORT_FOLDER):
                file_path = os.path.join(settings.REPORT_FOLDER, filename)
                
                # Only include files (not directories)
                if os.path.isfile(file_path):
                    # Parse report ID and timestamp from filename
                    parts = Path(filename).stem.split('_')
                    
                    if len(parts) >= 3 and parts[0] == "report":
                        timestamp_str = parts[1]
                        report_id = parts[2]
                        
                        try:
                            # Try to parse timestamp
                            timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                            
                            # Get file info
                            file_stats = os.stat(file_path)
                            file_size = file_stats.st_size
                            
                            reports.append({
                                "id": report_id,
                                "filename": filename,
                                "created_at": timestamp.isoformat(),
                                "size_bytes": file_size,
                                "format": Path(filename).suffix[1:]  # Remove the leading dot
                            })
                        except Exception as e:
                            logger.warning(f"Error parsing report metadata for {filename}: {str(e)}")
            
            # Sort by creation time (newest first)
            reports.sort(key=lambda x: x["created_at"], reverse=True)
            
        except Exception as e:
            logger.error(f"Error listing reports: {str(e)}")
        
        return reports
    
    @staticmethod
    def get_report_path(report_id: str) -> Optional[str]:
        """
        Get the file path for a report by ID.
        
        Args:
            report_id: ID of the report
            
        Returns:
            Path to the report file, or None if not found
        """
        all_reports = ReportManager.list_reports()
        
        for report in all_reports:
            if report["id"] == report_id:
                return os.path.join(settings.REPORT_FOLDER, report["filename"])
        
        return None
    
    @staticmethod
    def delete_report(report_id: str) -> bool:
        """
        Delete a report by ID.
        
        Args:
            report_id: ID of the report
            
        Returns:
            True if the report was deleted, False otherwise
        """
        report_path = ReportManager.get_report_path(report_id)
        
        if report_path and os.path.exists(report_path):
            try:
                os.remove(report_path)
                logger.info(f"Deleted report: {report_path}")
                return True
            except Exception as e:
                logger.error(f"Error deleting report {report_id}: {str(e)}")
                return False
        
        return False