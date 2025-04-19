import os
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any, Generator
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
            df = pd.read_csv(file_path, nrows=0)
            
            if required_columns:
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    return False, f"Missing required columns: {', '.join(missing_columns)}"
            
            return True, ""
        except pd.errors.EmptyDataError:
             return False, "CSV file is empty."
        except pd.errors.ParserError as pe:
             return False, f"CSV parsing error: {pe}"
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
        except (FileNotFoundError, pd.errors.EmptyDataError, pd.errors.ParserError):
            return []
        except Exception as e:
            logger.error(f"Error reading columns from {file_path}: {str(e)}")
            return []
    
    @staticmethod
    def process_in_chunks(
        input_file: str,
        reference_file: Optional[str] = None,
        join_keys: Optional[Dict[str, str]] = None,
        chunk_size: Optional[int] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Processes a large CSV file in chunks, optionally joining with a reference file,
        and yields DataFrames (either input chunks or joined chunks).
        
        Args:
            input_file: Path to the input CSV file
            reference_file: Path to the reference CSV file (optional)
            join_keys: Dictionary mapping input file keys to reference file keys
            chunk_size: Size of chunks to process (defaults to settings.CSV_CHUNK_SIZE)
            
        Yields:
            DataFrame representing a chunk of input or joined data.
        """
        if chunk_size is None:
             chunk_size = settings.CSV_CHUNK_SIZE

        ref_df = None
        if reference_file:
            try:
                ref_df = pd.read_csv(reference_file)
                logger.info(f"Loaded reference data: {len(ref_df)} rows from {reference_file}")

                if join_keys:
                    ref_cols = set(ref_df.columns)
                    missing_ref_keys = [ref_key for ref_key in join_keys.values() if ref_key not in ref_cols]
                    if missing_ref_keys:
                         raise ValueError(f"Reference file '{reference_file}' missing required join keys: {missing_ref_keys}")

            except FileNotFoundError:
                logger.error(f"Reference file not found: {reference_file}")
                raise
            except pd.errors.EmptyDataError:
                 logger.error(f"Reference file is empty: {reference_file}")
                 raise ValueError(f"Reference file is empty: {reference_file}")
            except Exception as e:
                logger.error(f"Error loading reference data from {reference_file}: {str(e)}")
                raise


        try:
            input_chunk_iterator = pd.read_csv(input_file, chunksize=chunk_size)

            for i, input_chunk in enumerate(input_chunk_iterator):
                if input_chunk.empty:
                    logger.warning(f"Skipping empty input chunk {i+1} from {input_file}")
                    continue

                if ref_df is not None and join_keys:
                    input_cols = set(input_chunk.columns)
                    missing_input_join_keys = [k for k in join_keys.keys() if k not in input_cols]
                    if missing_input_join_keys:
                         logger.error(f"Input chunk {i+1} missing required join keys: {missing_input_join_keys}. Skipping merge for this chunk.")
                         yield input_chunk
                         continue

                    try:
                        merged_chunk = pd.merge(
                            input_chunk,
                            ref_df,
                            left_on=list(join_keys.keys()),
                            right_on=list(join_keys.values()),
                            how="left"
                        )
                        yield merged_chunk
                    except Exception as merge_e:
                        logger.error(f"Error merging input chunk {i+1} with reference data: {str(merge_e)}")
                        continue
                else:
                    yield input_chunk

        except pd.errors.EmptyDataError:
            logger.warning(f"Input file is empty or has no data: {input_file}")
            pass

        except FileNotFoundError:
            logger.error(f"Input file not found during chunk processing: {input_file}")
            raise

        except Exception as e:
            logger.error(f"Error processing input file {input_file} in chunks: {str(e)}")
            raise
    
    @staticmethod
    def get_sample_data(file_path: str, nrows: int = 10) -> List[Dict[str, Any]]:
        """
        Get sample data from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            nrows: Number of rows to sample
            
        Returns:
            List of dictionaries with sample data, or empty list if file not found or error.
        """
        if not os.path.exists(file_path):
            logger.warning(f"Sample file not found: {file_path}")
            return []
            
        try:
            df = pd.read_csv(file_path, nrows=nrows, low_memory=False)
            return df.to_dict(orient='records')
        except pd.errors.EmptyDataError:
             logger.warning(f"Sample file is empty: {file_path}")
             return []
        except pd.errors.ParserError as pe:
             logger.error(f"CSV parsing error getting sample from {file_path}: {pe}")
             return []
        except Exception as e:
            logger.error(f"Error getting sample data from {file_path}: {str(e)}")
            return []