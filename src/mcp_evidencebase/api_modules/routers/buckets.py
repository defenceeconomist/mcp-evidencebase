"""Bucket management API routes."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from minio.error import S3Error

from mcp_evidencebase.api_modules.deps import get_bucket_service, get_ingestion_service
from mcp_evidencebase.api_modules.errors import raise_bucket_http_error
from mcp_evidencebase.api_modules.models import BucketCreateRequest
from mcp_evidencebase.bucket_service import BucketService
from mcp_evidencebase.ingestion import IngestionService

router = APIRouter(tags=["buckets"])


@router.get("/buckets")
def get_buckets(
    service: Annotated[BucketService, Depends(get_bucket_service)],
) -> dict[str, list[str]]:
    """List all MinIO buckets."""
    try:
        buckets = service.list_buckets()
    except S3Error as exc:
        raise_bucket_http_error(exc)
    return {"buckets": buckets}


@router.post("/buckets")
def create_bucket(
    payload: BucketCreateRequest,
    service: Annotated[BucketService, Depends(get_bucket_service)],
) -> dict[str, bool | str]:
    """Create a bucket without coupling bucket lifecycle to Qdrant."""
    normalized_bucket_name = payload.bucket_name.strip()
    try:
        created = service.create_bucket(payload.bucket_name)
    except (ValueError, S3Error) as exc:
        raise_bucket_http_error(exc)

    return {
        "bucket_name": normalized_bucket_name,
        "created": created,
    }


@router.delete("/buckets/{bucket_name}")
def delete_bucket(
    bucket_name: str,
    service: Annotated[BucketService, Depends(get_bucket_service)],
    ingestion_service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, bool | str]:
    """Delete one logical collection and its related storage metadata."""
    normalized_bucket_name = bucket_name.strip()
    try:
        removed = ingestion_service.delete_collection(bucket_name=bucket_name)
        if not removed:
            removed = service.delete_bucket(bucket_name)
    except (ValueError, S3Error) as exc:
        raise_bucket_http_error(exc)

    return {
        "bucket_name": normalized_bucket_name,
        "removed": removed,
    }
