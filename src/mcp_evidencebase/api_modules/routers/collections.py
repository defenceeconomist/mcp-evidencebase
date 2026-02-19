"""Collection/document API routes."""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from mcp_evidencebase.api_modules.deps import get_ingestion_service
from mcp_evidencebase.api_modules.errors import raise_document_http_error
from mcp_evidencebase.api_modules.models import MetadataUpdateRequest
from mcp_evidencebase.api_modules.services import perform_collection_search
from mcp_evidencebase.api_modules.task_dispatch import enqueue_partition_task, enqueue_scan_task
from mcp_evidencebase.citation_schema import get_citation_schema
from mcp_evidencebase.ingestion import IngestionService

logger = logging.getLogger(__name__)
router = APIRouter(tags=["collections"])


@router.get("/metadata/schema")
def get_metadata_schema() -> dict[str, Any]:
    """Return shared citation/author schema used by backend and frontend."""
    return get_citation_schema()


@router.get("/collections/{bucket_name}/documents")
def get_documents(
    bucket_name: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """List document records for one bucket."""
    try:
        documents = service.list_documents(bucket_name.strip())
    except Exception as exc:
        raise_document_http_error(exc)
    return {"bucket_name": bucket_name.strip(), "documents": documents}


@router.get("/collections/{bucket_name}/search")
def search_collection(
    bucket_name: str,
    query: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
    limit: int = 10,
    mode: str = "hybrid",
    rrf_k: int = 60,
) -> dict[str, Any]:
    """Search one collection using semantic, keyword, or hybrid retrieval."""
    return perform_collection_search(
        bucket_name=bucket_name,
        query=query,
        limit=limit,
        mode=mode,
        rrf_k=rrf_k,
        service=service,
    )


@router.get("/collections/{bucket_name}/documents/resolve")
def resolve_document(
    bucket_name: str,
    file_path: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> Response:
    """Return one stored object for the PDF.js resolver view."""
    normalized_bucket_name = bucket_name.strip()
    normalized_file_path = file_path.strip().lstrip("/")
    if not normalized_file_path:
        raise HTTPException(status_code=400, detail="file_path must not be empty.")

    try:
        payload, content_type = service.resolve_document_object(
            bucket_name=normalized_bucket_name,
            object_name=normalized_file_path,
        )
    except Exception as exc:
        raise_document_http_error(exc)

    file_name = PurePosixPath(normalized_file_path).name or "document"
    quoted_file_name = file_name.replace('"', "")
    return Response(
        content=payload,
        media_type=content_type,
        headers={
            "Content-Disposition": f'inline; filename="{quoted_file_name}"',
            "Cache-Control": "no-store",
        },
    )


@router.get("/collections/{bucket_name}/documents/{document_id}/sections/{section_id}")
def get_document_section(
    bucket_name: str,
    document_id: str,
    section_id: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Return one section record from Redis section mapping for a document."""
    try:
        section = service.get_document_section(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
            section_id=section_id.strip(),
        )
    except Exception as exc:
        raise_document_http_error(exc)
    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        "section_id": section_id.strip(),
        "section": section,
    }


@router.get("/collections/{bucket_name}/documents/{document_id}/sections")
def list_document_sections(
    bucket_name: str,
    document_id: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Return all section records from Redis section mapping for a document."""
    try:
        sections = service.list_document_sections(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
        )
    except Exception as exc:
        raise_document_http_error(exc)
    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        "sections": sections,
    }


@router.post("/collections/{bucket_name}/sections/rebuild")
def rebuild_sections(
    bucket_name: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
    document_id: str | None = None,
) -> dict[str, Any]:
    """Rebuild Redis section mappings from stored partitions without re-ingesting files."""
    try:
        if document_id and document_id.strip():
            return service.rebuild_document_section_mapping(
                bucket_name=bucket_name.strip(),
                document_id=document_id.strip(),
            )
        return service.rebuild_bucket_section_mappings(bucket_name=bucket_name.strip())
    except Exception as exc:
        raise_document_http_error(exc)


@router.post("/collections/{bucket_name}/documents/upload")
async def upload_document(
    bucket_name: str,
    file_name: str,
    request: Request,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Upload one document and enqueue background processing."""
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
        raise_document_http_error(exc)

    queued = True
    task_id: str | None = None
    queue_error = ""
    try:
        task = enqueue_partition_task(normalized_bucket_name, object_name)
        task_id = task.id
    except Exception as exc:
        queued = False
        queue_error = str(exc)
        logger.exception("Failed to enqueue partition_minio_object task.")

    return {
        "bucket_name": normalized_bucket_name,
        "object_name": object_name,
        "queued": queued,
        "task_id": task_id,
        "queue_error": queue_error,
    }


@router.post("/collections/{bucket_name}/scan")
def trigger_bucket_scan(bucket_name: str) -> dict[str, Any]:
    """Enqueue an on-demand bucket scan task."""
    normalized_bucket_name = bucket_name.strip()
    try:
        task = enqueue_scan_task(normalized_bucket_name)
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


@router.put("/collections/{bucket_name}/documents/{document_id}/metadata")
def update_document_metadata(
    bucket_name: str,
    document_id: str,
    payload: MetadataUpdateRequest,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Update normalized metadata for a document."""
    try:
        metadata = service.update_metadata(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
            metadata=payload.metadata,
        )
    except Exception as exc:
        raise_document_http_error(exc)

    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        "metadata": metadata,
    }


@router.post("/collections/{bucket_name}/documents/{document_id}/metadata/fetch")
def fetch_document_metadata_from_crossref(
    bucket_name: str,
    document_id: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Fetch metadata from Crossref using DOI/ISBN/ISSN/title fallbacks."""
    try:
        result = service.fetch_metadata_from_crossref(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
        )
    except Exception as exc:
        raise_document_http_error(exc)

    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        "lookup_field": result.get("lookup_field", ""),
        "confidence": result.get("confidence", 0.0),
        "metadata": result.get("metadata", {}),
    }


@router.delete("/collections/{bucket_name}/documents/{document_id}")
def delete_document(
    bucket_name: str,
    document_id: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Delete a document while retaining Redis partition payload."""
    try:
        removed = service.delete_document(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
            keep_partitions=True,
        )
    except Exception as exc:
        raise_document_http_error(exc)

    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        "removed": removed,
        "partitions_retained": True,
    }
