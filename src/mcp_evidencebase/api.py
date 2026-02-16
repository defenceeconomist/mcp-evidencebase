"""FastAPI application for bucket and document ingestion operations.

The module exposes endpoints used by the dashboard and command-line workflows.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, NoReturn

from fastapi import Depends, FastAPI, HTTPException, Request
from minio.error import S3Error
from pydantic import BaseModel, Field

from mcp_evidencebase.bucket_service import BucketService
from mcp_evidencebase.ingestion import IngestionService, build_ingestion_service
from mcp_evidencebase.minio_settings import MinioSettings, build_minio_settings
from mcp_evidencebase.tasks import process_minio_object, scan_minio_objects

app = FastAPI(title="mcp-evidencebase API", version="0.1.0")
logger = logging.getLogger(__name__)


class BucketCreateRequest(BaseModel):
    """Payload for creating a MinIO bucket."""

    bucket_name: str = Field(description="Name of the bucket to create.")


class MetadataUpdateRequest(BaseModel):
    """Payload for updating document metadata fields."""

    metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Subset of normalized metadata fields to update.",
    )


def get_minio_settings() -> MinioSettings:
    """Resolve MinIO connection settings.

    Returns:
        MinIO settings built from environment variables.
    """
    return build_minio_settings()


def get_bucket_service(
    settings: Annotated[MinioSettings, Depends(get_minio_settings)],
) -> BucketService:
    """Resolve bucket service dependency.

    Args:
        settings: Resolved MinIO settings dependency.

    Returns:
        Bucket service configured with the current MinIO settings.
    """
    return BucketService(settings=settings)


def get_ingestion_service() -> IngestionService:
    """Resolve document ingestion service dependency.

    Returns:
        Fully configured ingestion service.
    """
    return build_ingestion_service()


def _format_minio_error(exc: S3Error) -> str:
    """Create a concise API-safe message from MinIO SDK errors."""
    return f"{exc.code}: {exc.message}"


def _raise_bucket_http_error(exc: ValueError | S3Error) -> NoReturn:
    """Map bucket-related exceptions to a client-facing HTTP error."""
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail=_format_minio_error(exc)) from exc


def _raise_document_http_error(exc: Exception) -> NoReturn:
    """Map ingestion exceptions to API-safe HTTP errors."""
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, FileNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, S3Error):
        raise HTTPException(status_code=400, detail=_format_minio_error(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Return API readiness status.

    Returns:
        Dictionary with a static ``ok`` status string.
    """
    return {"status": "ok"}


@app.get("/buckets")
def get_buckets(
    service: Annotated[BucketService, Depends(get_bucket_service)],
) -> dict[str, list[str]]:
    """List all MinIO buckets.

    Args:
        service: Bucket service dependency.

    Returns:
        Response containing bucket names.
    """
    try:
        buckets = service.list_buckets()
    except S3Error as exc:
        _raise_bucket_http_error(exc)

    return {"buckets": buckets}


@app.post("/buckets")
def create_bucket(
    payload: BucketCreateRequest,
    service: Annotated[BucketService, Depends(get_bucket_service)],
    ingestion_service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, bool | str]:
    """Create a bucket and ensure its paired Qdrant collection exists.

    Args:
        payload: Request body containing bucket name.
        service: Bucket service dependency.
        ingestion_service: Ingestion service dependency.

    Returns:
        Bucket name and creation/sync flags.

    Raises:
        HTTPException: ``400`` for invalid bucket input or MinIO errors.
        HTTPException: ``502`` when Qdrant collection sync fails.
    """
    normalized_bucket_name = payload.bucket_name.strip()
    try:
        created = service.create_bucket(payload.bucket_name)
    except (ValueError, S3Error) as exc:
        _raise_bucket_http_error(exc)

    try:
        qdrant_collection_created = ingestion_service.ensure_bucket_qdrant_collection(
            normalized_bucket_name
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Qdrant sync failed while creating collection: {exc}",
        ) from exc

    return {
        "bucket_name": normalized_bucket_name,
        "created": created,
        "qdrant_collection_created": qdrant_collection_created,
    }


@app.delete("/buckets/{bucket_name}")
def delete_bucket(
    bucket_name: str,
    service: Annotated[BucketService, Depends(get_bucket_service)],
    ingestion_service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, bool | str]:
    """Delete a bucket and remove its paired Qdrant collection.

    Args:
        bucket_name: Bucket path parameter.
        service: Bucket service dependency.
        ingestion_service: Ingestion service dependency.

    Returns:
        Bucket name and removal/sync flags.

    Raises:
        HTTPException: ``400`` for invalid bucket input or MinIO errors.
        HTTPException: ``502`` when Qdrant collection sync fails.
    """
    normalized_bucket_name = bucket_name.strip()
    try:
        removed = service.delete_bucket(bucket_name)
    except (ValueError, S3Error) as exc:
        _raise_bucket_http_error(exc)

    try:
        qdrant_collection_removed = ingestion_service.delete_bucket_qdrant_collection(
            normalized_bucket_name
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Qdrant sync failed while deleting collection: {exc}",
        ) from exc

    return {
        "bucket_name": normalized_bucket_name,
        "removed": removed,
        "qdrant_collection_removed": qdrant_collection_removed,
    }


@app.get("/collections/{bucket_name}/documents")
def get_documents(
    bucket_name: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """List document records for one bucket.

    Args:
        bucket_name: Bucket path parameter.
        service: Ingestion service dependency.

    Returns:
        Bucket name and document records.
    """
    try:
        documents = service.list_documents(bucket_name.strip())
    except Exception as exc:
        _raise_document_http_error(exc)
    return {"bucket_name": bucket_name.strip(), "documents": documents}


@app.post("/collections/{bucket_name}/documents/upload")
async def upload_document(
    bucket_name: str,
    file_name: str,
    request: Request,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Upload one document and enqueue background processing.

    Args:
        bucket_name: Bucket path parameter.
        file_name: Object name query parameter.
        request: Request body containing raw file bytes.
        service: Ingestion service dependency.

    Returns:
        Upload result including queue status and optional Celery task ID.
    """
    normalized_bucket_name = bucket_name.strip()
    normalized_file_name = file_name.strip()
    if not normalized_file_name:
        raise HTTPException(status_code=400, detail="file name must not be empty.")

    payload = await request.body()
    content_type = request.headers.get("content-type")
    try:
        object_name = service.upload_document(
            bucket_name=normalized_bucket_name,
            object_name=normalized_file_name,
            payload=payload,
            content_type=content_type,
        )
    except Exception as exc:
        _raise_document_http_error(exc)

    queued = True
    task_id: str | None = None
    queue_error = ""
    try:
        task = process_minio_object.delay(normalized_bucket_name, object_name, None)
        task_id = task.id
    except Exception as exc:
        queued = False
        queue_error = str(exc)
        logger.exception("Failed to enqueue process_minio_object task.")

    return {
        "bucket_name": normalized_bucket_name,
        "object_name": object_name,
        "queued": queued,
        "task_id": task_id,
        "queue_error": queue_error,
    }


@app.post("/collections/{bucket_name}/scan")
def trigger_bucket_scan(
    bucket_name: str,
) -> dict[str, Any]:
    """Enqueue an on-demand bucket scan task.

    Args:
        bucket_name: Bucket path parameter.

    Returns:
        Queue status, optional task ID, and queue error details.
    """
    normalized_bucket_name = bucket_name.strip()
    try:
        task = scan_minio_objects.delay(normalized_bucket_name)
    except Exception as exc:
        logger.exception("Failed to enqueue scan_minio_objects task.")
        return {
            "bucket_name": normalized_bucket_name,
            "queued": False,
            "task_id": None,
            "queue_error": str(exc),
        }

    return {
        "bucket_name": normalized_bucket_name,
        "queued": True,
        "task_id": task.id,
        "queue_error": "",
    }


@app.put("/collections/{bucket_name}/documents/{document_id}/metadata")
def update_document_metadata(
    bucket_name: str,
    document_id: str,
    payload: MetadataUpdateRequest,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Update normalized metadata for a document.

    Args:
        bucket_name: Bucket path parameter.
        document_id: Canonical document hash.
        payload: Metadata update payload.
        service: Ingestion service dependency.

    Returns:
        Updated metadata payload.
    """
    try:
        metadata = service.update_metadata(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
            metadata=payload.metadata,
        )
    except Exception as exc:
        _raise_document_http_error(exc)

    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        "metadata": metadata,
    }


@app.delete("/collections/{bucket_name}/documents/{document_id}")
def delete_document(
    bucket_name: str,
    document_id: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Delete a document while retaining Redis partition payload.

    Args:
        bucket_name: Bucket path parameter.
        document_id: Canonical document hash.
        service: Ingestion service dependency.

    Returns:
        Removal status and retention behavior flags.
    """
    try:
        removed = service.delete_document(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
            keep_partitions=True,
        )
    except Exception as exc:
        _raise_document_http_error(exc)

    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        "removed": removed,
        "partitions_retained": True,
    }
