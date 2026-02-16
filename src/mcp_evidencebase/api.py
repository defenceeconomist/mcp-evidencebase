"""HTTP API for MinIO bucket management used by the UI."""

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

    bucket_name: str


class MetadataUpdateRequest(BaseModel):
    """Payload for updating document metadata fields."""

    metadata: dict[str, str] = Field(default_factory=dict)


def get_minio_settings() -> MinioSettings:
    """Dependency that resolves MinIO settings."""
    return build_minio_settings()


def get_bucket_service(
    settings: Annotated[MinioSettings, Depends(get_minio_settings)],
) -> BucketService:
    """Dependency that resolves the bucket service."""
    return BucketService(settings=settings)


def get_ingestion_service() -> IngestionService:
    """Dependency that resolves the document ingestion service."""
    return build_ingestion_service()


def _format_minio_error(exc: S3Error) -> str:
    """Create a concise API-safe message from MinIO SDK errors."""
    return f"{exc.code}: {exc.message}"


def _raise_bucket_http_error(exc: ValueError | S3Error) -> NoReturn:
    """Raise a client error for bucket operation failures."""
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
    ingestion_service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, bool | str]:
    """Create a bucket when missing."""
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
    """Remove a bucket when it exists."""
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
    """Return all document records for one collection bucket."""
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
    """Upload a file to MinIO and enqueue partition/chunk processing."""
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
    """Enqueue an on-demand scan for newly dropped bucket files."""
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
    """Update BibTeX metadata fields in Redis for a document."""
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
    """Remove MinIO object, metadata, chunks, and vectors while retaining partitions."""
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
