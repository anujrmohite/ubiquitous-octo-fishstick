import os
import secrets
from typing import Dict, List, Optional, Any, Union
from pydantic import AnyHttpUrl, BaseSettings, PostgresDsn, validator

class Settings(BaseSettings):
    PROJECT_NAME: str = "Report Generator"
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    
    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)
    
    POSTGRES_SERVER: str = os.getenv("POSTGRES_SERVER", "db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "password")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "reportdb")
    SQLALCHEMY_DATABASE_URI: Optional[str] = None
    
    @validator("SQLALCHEMY_DATABASE_URI", pre=True, always=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if v is not None:
            return v
        database_url_env = os.getenv("DATABASE_URL")
        if database_url_env:
            return database_url_env
        else:
            base_dir_local = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            default_sqlite_path = os.path.join(base_dir_local, "app", "scheduler.db")
            os.makedirs(os.path.dirname(default_sqlite_path), exist_ok=True)
            return f"sqlite:///{default_sqlite_path}"

    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    UPLOAD_FOLDER: str = os.getenv("UPLOAD_FOLDER", os.path.join(BASE_DIR, "data", "uploads"))
    REPORT_FOLDER: str = os.getenv("REPORT_FOLDER", os.path.join(BASE_DIR, "data", "reports"))
    RULES_FOLDER: str = os.getenv("RULES_FOLDER", os.path.join(BASE_DIR, "data", "rules"))

    CSV_CHUNK_SIZE: int = int(os.getenv("CSV_CHUNK_SIZE", "100000"))
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))
    SUPPORTED_FORMATS: List[str] = ["csv", "xlsx", "json"]
    API_KEY: str = os.getenv("API_KEY", "local_dev_api_key")

    class Config:
        case_sensitive = True