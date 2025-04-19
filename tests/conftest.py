import os
import pytest
import tempfile
import pandas as pd
import json
import shutil

from fastapi.testclient import TestClient

from app.main import app
from app.core.config import settings

@pytest.fixture(scope="function")

def temp_dir():
    """
    Create a temporary directory for test files and override settings.
    Ensures a clean state for each test function that uses it.
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        old_upload_folder = settings.UPLOAD_FOLDER
        old_report_folder = settings.REPORT_FOLDER
        old_sqlalchemy_uri = settings.SQLALCHEMY_DATABASE_URI

        test_uploads_dir = os.path.join(tmpdirname, "uploads")
        test_reports_dir = os.path.join(tmpdirname, "reports")
        test_db_path = os.path.join(tmpdirname, "test_scheduler.db")
        test_db_uri = f"sqlite:///{test_db_path}"

        test_app_dir = os.path.join(tmpdirname, "app")
        test_app_rules_dir = os.path.join(test_app_dir, "rules")

        settings.UPLOAD_FOLDER = test_uploads_dir
        settings.REPORT_FOLDER = test_reports_dir
        settings.SQLALCHEMY_DATABASE_URI = test_db_uri

        os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
        os.makedirs(settings.REPORT_FOLDER, exist_ok=True)
        os.makedirs(test_app_rules_dir, exist_ok=True)

        dummy_schedules_path = os.path.join(test_app_rules_dir, 'schedules.json')
        with open(dummy_schedules_path, 'w') as f:
            json.dump({}, f)

        yield tmpdirname

        settings.UPLOAD_FOLDER = old_upload_folder
        settings.REPORT_FOLDER = old_report_folder
        settings.SQLALCHEMY_DATABASE_URI = old_sqlalchemy_uri

@pytest.fixture
def sample_input_csv_path(temp_dir):
    """Copy the 'input_sales.csv' sample input file into the temp upload directory."""
    source_path = os.path.join(os.path.dirname(__file__), "data", "input_sales.csv")
    filename = os.path.basename(source_path)
    target_path = os.path.join(settings.UPLOAD_FOLDER, filename)
    shutil.copy(source_path, target_path)
    return filename


@pytest.fixture
def sample_reference_csv_path(temp_dir):
    """Copy the 'reference_products.csv' sample reference file into the temp upload directory."""
    source_path = os.path.join(os.path.dirname(__file__), "data", "reference_products.csv")
    filename = os.path.basename(source_path)
    target_path = os.path.join(settings.UPLOAD_FOLDER, filename)
    shutil.copy(source_path, target_path)
    return filename

@pytest.fixture
def sample_input_data(sample_input_csv_path):
    """Load the sample input data CSV into a pandas DataFrame."""
    full_path = os.path.join(settings.UPLOAD_FOLDER, sample_input_csv_path)
    return pd.read_csv(full_path)

@pytest.fixture
def sample_reference_data(sample_reference_csv_path):
    """Load the sample reference data CSV into a pandas DataFrame."""
    full_path = os.path.join(settings.UPLOAD_FOLDER, sample_reference_csv_path)
    return pd.read_csv(full_path)

@pytest.fixture
def sales_joined_data_df(sample_input_data, sample_reference_data):
    """Load input_sales and reference_products, join them, and return the DataFrame."""
    join_keys = {"product_id": "product_id"}

    input_df = sample_input_data
    ref_df = sample_reference_data

    joined_df = pd.merge(
        input_df,
        ref_df,
        left_on=list(join_keys.keys()),
        right_on=list(join_keys.values()),
        how='left'
    )
    return joined_df

@pytest.fixture
def sample_rules_json_path(temp_dir):
    """Copy the 'rules.json' sample rules file into the temp upload directory."""
    source_path = os.path.join(os.path.dirname(__file__), "data", "rules.json")
    filename = os.path.basename(source_path)
    target_path = os.path.join(settings.UPLOAD_FOLDER, filename)
    shutil.copy(source_path, target_path)
    return filename

@pytest.fixture
def sample_rules_dict(sample_rules_json_path):
    """Load the sample rules JSON file into a dictionary."""
    full_path = os.path.join(settings.UPLOAD_FOLDER, sample_rules_json_path)
    with open(full_path, 'r') as f:
        return json.load(f)

@pytest.fixture(scope="session")
def client():
    """
    Create a test client for the FastAPI app.
    Uses 'with' statement for proper startup/shutdown events (like scheduler).
    """
    with TestClient(app) as c:
        c.headers.update({"X-API-Key": "dev_api_key"})
        yield c