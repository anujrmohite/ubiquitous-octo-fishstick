# Report Generator Microservice

A high-performance microservice for transforming CSV data and generating reports with flexible transformation rules.

## 📋 Overview

The Report Generator Microservice is designed to handle large CSV files, apply customizable transformation rules, join with reference data, and generate downloadable reports in various formats. It's built with FastAPI for high performance and excellent developer experience.

### Features

- **CSV Processing**: Stream and process large CSV files efficiently using chunking
- **Rule-based Transformations**: Apply custom transformation rules defined in JSON/YAML
- **Data Joining**: Join input data with reference data
- **Multiple Output Formats**: Generate reports in CSV, XLSX, or JSON formats
- **Scheduled Reports**: Schedule reports using cron expressions
- **API Authentication**: Secure API with API key authentication
- **Monitoring**: Built-in Prometheus and Grafana monitoring
- **Performance**: Optimized for large datasets with multiprocessing

### Using Docker Compose (Recommended)

```bash

# Clone the repository
git clone https://github.com/yourusername/report-generator.git
cd report-generator

# Start the services
docker-compose up -d

# The API will be available at http://localhost:8000/api/v1/docs
```

### Manual Setup

```bash
git clone https://github.com/yourusername/report-generator.git
cd report-generator

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

pip install -r requirements.txt

uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Once the service is running, you can access the Swagger UI at:
- http://localhost:8000/api/v1/docs

### API Endpoints

#### File Upload

- `POST /api/v1/upload/input` - Upload an input CSV file
- `POST /api/v1/upload/reference` - Upload a reference CSV file
- `GET /api/v1/upload/list` - List uploaded files
- `GET /api/v1/upload/sample/{filename}` - Get sample data from a file
- `DELETE /api/v1/upload/{filename}` - Delete an uploaded file

#### Rules Management

- `GET /api/v1/rules/list` - List available rule files
- `GET /api/v1/rules/get/{filename}` - Get rules from a file
- `POST /api/v1/rules/create` - Create or update rules
- `POST /api/v1/rules/upload` - Upload a rules file
- `POST /api/v1/rules/validate` - Validate rules against a CSV file
- `DELETE /api/v1/rules/{filename}` - Delete a rules file

#### Report Generation

- `POST /api/v1/report/generate` - Generate a report
- `GET /api/v1/report/list` - List available reports
- `GET /api/v1/report/{report_id}` - Download a report
- `DELETE /api/v1/report/{report_id}` - Delete a report
- `POST /api/v1/report/schedule` - Schedule a report generation
- `GET /api/v1/report/schedule/list` - List scheduled report generations
- `DELETE /api/v1/report/schedule/{job_id}` - Delete a scheduled report generation

## 📊 Transformation Rules

Rules are defined in JSON or YAML format and specify how to transform input data into output fields.

### Supported Operations

- **Arithmetic**: `+`, `-`, `*`, `/`
- **Functions**: `max`, `min`, `sum`, `abs`, `round`
- **Column References**: Any column name from input or reference data

### Create Rules

```bash
curl -X 'POST' \
  'http://localhost:8000/api/v1/rules/create' \
  -H 'X-API-Key: dev_api_key' \
  -H 'Content-Type: application/json' \
  -d '{
  "rules": {
    "total": "price * quantity",
    "discount": "max(0, total * 0.1)",
    "final_price": "total - discount"
  },
  "filename": "pricing_rules.json"
}'
```

### Generate a Report

```bash
curl -X 'POST' \
  'http://localhost:8000/api/v1/report/generate' \
  -H 'X-API-Key: dev_api_key' \
  -H 'Content-Type: application/json' \
  -d '{
  "input_file": "input_sales.csv",
  "rules_file": "pricing_rules.json",
  "output_format": "xlsx"
}'
```

### Schedule a Report

```bash
curl -X 'POST' \
  'http://localhost:8000/api/v1/report/schedule' \
  -H 'X-API-Key: dev_api_key' \
  -H 'Content-Type: application/json' \
  -d '{
  "job_id": "daily_sales_report",
  "cron_expression": "0 8 * * *",
  "input_file": "input_sales.csv",
  "rules_file": "pricing_rules.json",
  "output_format": "xlsx"
}'
```

## 🧪 Testing

```bash
pytest

pytest --cov=app tests/

pytest tests/test_parser.py
```