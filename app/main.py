import os
import logging
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse

from app.api.endpoints import upload, report, rules
from app.core.config import settings

from app.core.security import get_api_key

from app.services.scheduler import setup_scheduler

os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
os.makedirs(settings.REPORT_FOLDER, exist_ok=True)
os.makedirs(settings.RULES_FOLDER, exist_ok=True)


app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    docs_url=f"{settings.API_V1_STR}/docs",
    redoc_url=f"{settings.API_V1_STR}/redoc",
    version="0.1.0"
)

if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

router_dependencies = [Depends(get_api_key)]

app.include_router(
    upload.router,
    prefix=f"{settings.API_V1_STR}/upload",
    tags=["upload"],
    dependencies=router_dependencies,
)

app.include_router(
    report.router,
    prefix=f"{settings.API_V1_STR}/report",
    tags=["report"],
    dependencies=router_dependencies,
)

app.include_router(
    rules.router,
    prefix=f"{settings.API_V1_STR}/rules",
    tags=["rules"],
    dependencies=router_dependencies,
)

@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to API docs."""
    return RedirectResponse(url=f"{settings.API_V1_STR}/docs")

setup_scheduler(app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)