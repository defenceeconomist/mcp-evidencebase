"""FastAPI application for bucket and document ingestion operations."""

from __future__ import annotations

from fastapi import FastAPI

from mcp_evidencebase.api_modules.deps import get_bucket_service, get_ingestion_service
from mcp_evidencebase.api_modules.routers.buckets import router as buckets_router
from mcp_evidencebase.api_modules.routers.collections import router as collections_router
from mcp_evidencebase.api_modules.routers.gpt import router as gpt_router
from mcp_evidencebase.tasks import (
    chunk_minio_object,
    meta_minio_object,
    partition_minio_object,
    scan_minio_objects,
    section_minio_object,
    upsert_minio_object,
)

app = FastAPI(title="mcp-evidencebase API", version="0.1.0")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Return API readiness status."""
    return {"status": "ok"}


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
