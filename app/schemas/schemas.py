from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator

class FileInfo(BaseModel):
    """File information schema."""
    name: str
    size_bytes: int
    created_at: float
    file_type: Optional[str] = None
    columns: Optional[List[str]] = None

class RulesFileInfo(BaseModel):
    """Rules file information schema."""
    name: str
    size_bytes: int
    created_at: float
    rules_count: int
    format: str

class ReportInfo(BaseModel):
    """Report information schema."""
    id: str
    filename: str
    created_at: str
    size_bytes: int
    format: str

class Rule(BaseModel):
    """Single transformation rule schema."""
    output_field: str
    expression: str
    
    class Config:
        schema_extra = {
            "example": {
                "output_field": "total_amount",
                "expression": "price * quantity"
            }
        }

class RuleSet(BaseModel):
    """Set of transformation rules schema."""
    rules: Dict[str, str]
    
    class Config:
        schema_extra = {
            "example": {
                "rules": {
                    "outfield1": "field1 + field2",
                    "outfield2": "refdata1",
                    "outfield3": "refdata2 + refdata3",
                    "outfield4": "field3 * max(field5, refdata4)",
                    "outfield5": "max(field5, refdata4)"
                }
            }
        }

class ScheduledJobInfo(BaseModel):
    """Scheduled job information schema."""
    id: str
    next_run_time: Optional[str] = None
    cron_trigger: str
    input_file: str
    rules_file: str
    reference_file: Optional[str] = None
    output_format: str

class ScheduledJobRequest(BaseModel):
    """Request to create a scheduled job."""
    job_id: str = Field(..., description="Unique ID for the scheduled job")
    cron_expression: str = Field(..., description="Cron expression for scheduling")
    input_file: str = Field(..., description="Name of the input CSV file")
    rules_file: str = Field(..., description="Name of the rules file")
    reference_file: Optional[str] = Field(None, description="Name of the reference CSV file")
    join_keys: Optional[Dict[str, str]] = Field(None, description="Dictionary mapping input keys to reference keys")
    output_format: str = Field("csv", description="Output format (csv, xlsx, json)")
    
    @validator('cron_expression')
    def validate_cron(cls, v):
        parts = v.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts: minute, hour, day of month, month, day of week")
        return v

class ReportRequest(BaseModel):
    """Request to generate a report."""
    input_file: str = Field(..., description="Name of the input CSV file")
    rules_file: str = Field(..., description="Name of the rules file")
    reference_file: Optional[str] = Field(None, description="Name of the reference CSV file")
    join_keys: Optional[Dict[str, str]] = Field(None, description="Dictionary mapping input keys to reference keys")
    output_format: str = Field("csv", description="Output format (csv, xlsx, json)")

class RuleValidationRequest(BaseModel):
    """Request model for validating rules against a CSV file."""
    rules: Dict[str, str] = Field(..., description="Dictionary of transformation rules")
    input_file: str = Field(..., description="Name of the input CSV file to validate against")
    reference_file: Optional[str] = Field(None, description="Name of the reference CSV file")
    join_keys: Optional[Dict[str, str]] = Field(None, description="Dictionary mapping input keys to reference keys for validation join")


class RuleValidationResult(BaseModel):
    """Result of rule validation."""
    valid: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    rule_validations: Dict[str, bool]