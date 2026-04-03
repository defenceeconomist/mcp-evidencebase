"""FastAPI application for bucket and document ingestion operations."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from mcp_evidencebase.api_modules.deps import get_bucket_service, get_ingestion_service
from mcp_evidencebase.api_modules.routers.buckets import router as buckets_router
from mcp_evidencebase.api_modules.routers.collections import router as collections_router
from mcp_evidencebase.api_modules.routers.gpt import router as gpt_router
from mcp_evidencebase.runtime_diagnostics import (
    collect_runtime_health,
    log_runtime_health,
    raise_for_failed_required_checks,
)
from mcp_evidencebase.tasks import (
    chunk_minio_object,
    meta_minio_object,
    partition_minio_object,
    scan_minio_objects,
    section_minio_object,
    upsert_minio_object,
)

logger = logging.getLogger(__name__)


def validate_runtime_dependencies_on_startup() -> None:
    """Log and validate required runtime dependencies before serving requests."""
    report = collect_runtime_health()
    log_runtime_health(logger, report=report, component_name="api")
    raise_for_failed_required_checks(report, component_name="api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Validate runtime dependencies before the app starts serving."""
    del app
    validate_runtime_dependencies_on_startup()
    yield

app = FastAPI(title="mcp-evidencebase API", version="0.1.0", lifespan=lifespan)


@app.get("/livez")
def livez() -> dict[str, str]:
    """Return process liveness status."""
    return {"status": "ok"}


@app.get("/healthz")
@app.get("/readyz")
def readyz() -> JSONResponse:
    """Return API readiness based on external dependency reachability."""
    report = collect_runtime_health()
    return JSONResponse(
        status_code=200 if bool(report.get("ready")) else 503,
        content=report,
    )


app.include_router(gpt_router)
app.include_router(buckets_router)
app.include_router(collections_router)

__all__ = [
    "app",
    "chunk_minio_object",
    "get_bucket_service",
    "get_ingestion_service",
    "meta_minio_object",
    "partition_minio_object",
    "scan_minio_objects",
    "section_minio_object",
    "upsert_minio_object",
]
