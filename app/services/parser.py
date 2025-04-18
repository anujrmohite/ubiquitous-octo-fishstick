import os
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

from app.core.config import settings

logger = logging.getLogger(__name__)

class CSVParser:
    """
    Parser for handling CSV files with efficient chunking for large files.
    """
    
    @staticmethod
    def validate_csv(file_path: str, required_columns: Optional[List[str]] = None) -> Tuple[bool, str]:
        """
        Validate that a CSV file exists and has the required columns.
        
        Args:
            file_path: Path to the CSV file
            required_columns: List of column names that must be present
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not os.path.exists(file_path):
            return False, f"File not found: {file_path}"
        
        try:
            # Just read the header to validate columns
            df = pd.read_csv(file_path, nrows=0)
            
            if required_columns:
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    return False, f"Missing required columns: {', '.join(missing_columns)}"
            
            return True, ""
        except Exception as e:
            return False, f"Error validating CSV: {str(e)}"
    
    @staticmethod
    def get_columns(file_path: str) -> List[str]:
        """
        Get the column names from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List of column names
        """
        try:
            df = pd.read_csv(file_path, nrows=0)
            return df.columns.tolist()
        except Exception as e:
            logger.error(f"Error reading columns from {file_path}: {str(e)}")
            return []
    
    @staticmethod
    def process_in_chunks(
        input_file: str,
        reference_file: Optional[str] = None,
        join_keys: Optional[Dict[str, str]] = None,
        chunk_size: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Process a large CSV file in chunks, optionally joining with a reference file.
        
        Args:
            input_file: Path to the input CSV file
            reference_file: Path to the reference CSV file (optional)
            join_keys: Dictionary mapping input file keys to reference file keys
            chunk_size: Size of chunks to process
            
        Returns:
            DataFrame with joined data
        """
        if chunk_size is None:
            chunk_size = settings.CSV_CHUNK_SIZE
            
        # If no reference file, just return the input file as chunks
        if not reference_file or not join_keys:
            return pd.read_csv(input_file, chunksize=chunk_size)
        
        # Load reference data
        try:
            ref_df = pd.read_csv(reference_file)
            logger.info(f"Loaded reference data: {len(ref_df)} rows")
        except Exception as e:
            logger.error(f"Error loading reference data: {str(e)}")
            raise
        
        # Process input file in chunks and join with reference data
        chunks = []
        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            # Prepare joining keys mapping between input and reference
            # Example: join_keys = {"refkey1": "refkey1", "refkey2": "refkey2"}
            # This means join input[refkey1] with reference[refkey1]
            for input_key, ref_key in join_keys.items():
                chunk_with_ref = pd.merge(
                    chunk,
                    ref_df,
                    left_on=input_key,
                    right_on=ref_key,
                    how="left"
                )
                chunks.append(chunk_with_ref)
                
        # Combine all chunks
        if not chunks:
            return pd.DataFrame()
        
        return pd.concat(chunks, ignore_index=True)
    
    @staticmethod
    def get_sample_data(file_path: str, nrows: int = 5) -> List[Dict[str, Any]]:
        """
        Get sample data from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            nrows: Number of rows to sample
            
        Returns:
            List of dictionaries with sample data
        """
        try:
            df = pd.read_csv(file_path, nrows=nrows)
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"Error getting sample data from {file_path}: {str(e)}")
            return []