"""HTTP API for MinIO bucket management used by the UI."""

from __future__ import annotations

from typing import Annotated, NoReturn

from fastapi import Depends, FastAPI, HTTPException
from minio.error import S3Error
from pydantic import BaseModel

from mcp_evidencebase.bucket_service import BucketService
from mcp_evidencebase.minio_settings import MinioSettings, build_minio_settings

app = FastAPI(title="mcp-evidencebase API", version="0.1.0")


class BucketCreateRequest(BaseModel):
    """Payload for creating a MinIO bucket."""

    bucket_name: str


def get_minio_settings() -> MinioSettings:
    """Dependency that resolves MinIO settings."""
    return build_minio_settings()


def get_bucket_service(
    settings: Annotated[MinioSettings, Depends(get_minio_settings)],
) -> BucketService:
    """Dependency that resolves the bucket service."""
    return BucketService(settings=settings)


def _format_minio_error(exc: S3Error) -> str:
    """Create a concise API-safe message from MinIO SDK errors."""
    return f"{exc.code}: {exc.message}"


def _raise_bucket_http_error(exc: ValueError | S3Error) -> NoReturn:
    """Raise a client error for bucket operation failures."""
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail=_format_minio_error(exc)) from exc


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Return a lightweight readiness response."""
    return {"status": "ok"}


@app.get("/buckets")
def get_buckets(
    service: Annotated[BucketService, Depends(get_bucket_service)],
) -> dict[str, list[str]]:
    """Return all bucket names."""
    try:
        buckets = service.list_buckets()
    except S3Error as exc:
        _raise_bucket_http_error(exc)

    return {"buckets": buckets}


@app.post("/buckets")
def create_bucket(
    payload: BucketCreateRequest,
    service: Annotated[BucketService, Depends(get_bucket_service)],
) -> dict[str, bool | str]:
    """Create a bucket when missing."""
    try:
        created = service.create_bucket(payload.bucket_name)
    except (ValueError, S3Error) as exc:
        _raise_bucket_http_error(exc)

    return {"bucket_name": payload.bucket_name.strip(), "created": created}


@app.delete("/buckets/{bucket_name}")
def delete_bucket(
    bucket_name: str,
    service: Annotated[BucketService, Depends(get_bucket_service)],
) -> dict[str, bool | str]:
    """Remove a bucket when it exists."""
    try:
        removed = service.delete_bucket(bucket_name)
    except (ValueError, S3Error) as exc:
        _raise_bucket_http_error(exc)

    return {"bucket_name": bucket_name.strip(), "removed": removed}
