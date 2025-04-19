import os
import pytest
import pandas as pd
import tempfile

from app.services.parser import CSVParser
from app.core.config import settings

def test_validate_csv_success(sample_input_csv_path):
    """Test CSV validation with a valid file."""
    file_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)

    valid, error = CSVParser.validate_csv(file_path)
    assert valid is True
    assert error == ""

    valid, error = CSVParser.validate_csv(file_path, required_columns=["order_id", "quantity", "unit_price"])
    assert valid is True
    assert error == ""

def test_validate_csv_file_not_found():
    """Test CSV validation with a non-existent file."""
    valid, error = CSVParser.validate_csv("nonexistent.csv")
    assert valid is False
    assert "File not found" in error

def test_validate_csv_missing_columns(sample_input_csv_path):
    """Test CSV validation when required columns are missing."""
    file_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)
    valid, error = CSVParser.validate_csv(file_path, required_columns=["order_id", "nonexistent_col_1", "nonexistent_col_2"])
    assert valid is False
    assert "Missing required columns" in error
    assert "nonexistent_col_1" in error
    assert "nonexistent_col_2" in error

def test_validate_csv_invalid_format(temp_dir):
    """Test CSV validation with a file that's not a valid CSV."""
    invalid_file_path = os.path.join(settings.UPLOAD_FOLDER, "invalid.csv")
    with open(invalid_file_path, "w") as f:
        f.write("col1,col2\nvalue1,value2\ninvalid row without comma\n") # Malformed CSV

    valid, error = CSVParser.validate_csv(invalid_file_path)
    assert valid is False
    assert any(err_substr in error for err_substr in ["Error validating CSV", "ParserError", "expected"])


def test_get_columns_success(sample_input_csv_path):
    """Test getting columns from a valid CSV."""
    file_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)
    columns = CSVParser.get_columns(file_path)
    expected_columns = ["order_id", "product_id", "quantity", "unit_price", "customer_id", "order_date", "region"]
    assert sorted(columns) == sorted(expected_columns)
    
def test_get_columns_file_not_found():
    """Test getting columns from a non-existent file."""
    columns = CSVParser.get_columns("nonexistent.csv")
    assert columns == []

def test_get_columns_invalid_format(temp_dir):
    """Test getting columns from a file that's not readable as CSV."""
    invalid_header_path = os.path.join(settings.UPLOAD_FOLDER, "bad_encoding.csv")
    with open(invalid_header_path, "wb") as f:
         f.write(b'\xff\xfeHello')

    columns = CSVParser.get_columns(invalid_header_path)
    assert columns == []


def test_process_in_chunks_input_only(sample_input_csv_path):
    """Test process_in_chunks with only an input file."""
    input_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)

    chunk_size = 3
    result_df = CSVParser.process_in_chunks(
        input_file=input_path,
        chunk_size=chunk_size
    )

    assert isinstance(result_df, pd.DataFrame)

    assert len(result_df) == 10

    expected_columns = ["order_id", "product_id", "quantity", "unit_price", "customer_id", "order_date", "region"]
    assert sorted(result_df.columns.tolist()) == sorted(expected_columns)


def test_process_in_chunks_with_join(sample_input_csv_path, sample_reference_csv_path, sample_reference_data):
    """Test process_in_chunks with input and reference files with join."""
    input_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)
    reference_path = os.path.join(settings.UPLOAD_FOLDER, sample_reference_csv_path)

    join_keys = {"product_id": "product_id"}

    chunk_size = 4
    result_df = CSVParser.process_in_chunks(
        input_file=input_path,
        reference_file=reference_path,
        join_keys=join_keys,
        chunk_size=chunk_size
    )

    assert isinstance(result_df, pd.DataFrame)

    assert len(result_df) == 10
    
    input_cols = ["order_id", "product_id", "quantity", "unit_price", "customer_id", "order_date", "region"]
    ref_cols = ["product_id", "product_name", "category", "cost_price", "supplier_id", "weight_kg", "stock_level"]

    expected_cols = list(set(input_cols + ref_cols))
    assert sorted(result_df.columns.tolist()) == sorted(expected_cols) # Use sorted for robust comparison

    ref_data_df = sample_reference_data # Get the reference data DataFrame from fixture
    p100_cost = ref_data_df[ref_data_df['product_id'] == 'P100']['cost_price'].iloc[0]
    assert result_df[result_df['order_id'] == 1001]['cost_price'].iloc[0] == p100_cost

    p300_cat = ref_data_df[ref_data_df['product_id'] == 'P300']['category'].iloc[0]
    assert result_df[result_df['order_id'] == 1003]['category'].iloc[0] == p300_cat

def test_process_in_chunks_file_not_found(sample_input_csv_path):
    """Test process_in_chunks with non-existent input or reference file."""
    with pytest.raises((FileNotFoundError, pd.errors.EmptyDataError)):
        CSVParser.process_in_chunks("nonexistent_input.csv")

    input_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)
    join_keys = {"product_id": "product_id"}
    with pytest.raises(FileNotFoundError):
        CSVParser.process_in_chunks(
            input_file=input_path,
            reference_file="nonexistent_ref.csv",
            join_keys=join_keys
        )

def test_process_in_chunks_join_keys_missing_in_ref(sample_input_csv_path, sample_reference_csv_path):
    """Test process_in_chunks when join keys are missing in the reference file."""
    input_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)
    reference_path = os.path.join(settings.UPLOAD_FOLDER, sample_reference_csv_path)

    invalid_join_keys = {"product_id": "nonexistent_ref_id"}

    with pytest.raises(ValueError, match="Reference file missing join keys"):
        CSVParser.process_in_chunks(
            input_file=input_path,
            reference_file=reference_path,
            join_keys=invalid_join_keys
        )


def test_get_sample_data_success(sample_input_csv_path):
    """Test getting sample data from a valid CSV."""
    file_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)

    sample = CSVParser.get_sample_data(file_path)
    assert isinstance(sample, list)
    assert len(sample) == 5
    assert isinstance(sample[0], dict)
    assert "order_id" in sample[0]

    sample_3 = CSVParser.get_sample_data(file_path, nrows=3)
    assert len(sample_3) == 3

    sample_all = CSVParser.get_sample_data(file_path, nrows=100)
    assert len(sample_all) == 10

def test_get_sample_data_file_not_found():
    """Test getting sample data from a non-existent file."""
    sample = CSVParser.get_sample_data("nonexistent.csv")
    assert sample == []

def test_get_sample_data_invalid_format(temp_dir):
    """Test getting sample data from a file that's not readable as CSV."""
    invalid_file_path = os.path.join(settings.UPLOAD_FOLDER, "bad_data.csv")
    with open(invalid_file_path, "w") as f:
        f.write("col1,col2\n")
        f.write("value1,value2\n")
        f.write("value3\n")

    sample = CSVParser.get_sample_data(invalid_file_path, nrows=10)
    assert sample == []