"""Collection/document API routes."""

from __future__ import annotations

import json
import logging
from pathlib import PurePosixPath
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from mcp_evidencebase.api_modules.deps import get_ingestion_service
from mcp_evidencebase.api_modules.errors import raise_document_http_error
from mcp_evidencebase.api_modules.models import MetadataUpdateRequest
from mcp_evidencebase.api_modules.services import (
    build_collection_bibtex,
    perform_collection_search,
)
from mcp_evidencebase.api_modules.task_dispatch import (
    enqueue_partition_task,
    enqueue_scan_task,
    enqueue_upsert_task,
)
from mcp_evidencebase.citation_schema import get_citation_schema
from mcp_evidencebase.ingestion import IngestionService
from mcp_evidencebase.ingestion_modules.metadata import (
    METADATA_FIELDS,
    extract_pdf_metadata_seed,
)
from mcp_evidencebase.pdf_split import (
    MAX_SPLIT_HEADING_LEVEL,
    build_pdf_split_plan,
    load_pdf_reader,
    render_pdf_split_segment,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["collections"])


def _parse_split_metadata_headers(request: Request) -> dict[str, str]:
    """Read split metadata overrides from request headers."""
    raw_metadata = request.headers.get("X-Evidencebase-Split-Metadata", "")
    if not raw_metadata:
        return {}

    try:
        payload = json.loads(raw_metadata)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid split metadata JSON: {exc.msg}.",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Split metadata payload must be a JSON object.")

    return {
        field_name: str(payload.get(field_name, "")).strip()
        for field_name in METADATA_FIELDS
        if str(payload.get(field_name, "")).strip()
    }


def _parse_split_selected_files(request: Request) -> set[str] | None:
    """Read the selected split output files from request headers."""
    raw_selected_files = request.headers.get("X-Evidencebase-Split-Selected-Files", "")
    if not raw_selected_files:
        return None
    return {
        file_name.strip()
        for file_name in raw_selected_files.split(",")
        if file_name.strip()
    }


def _format_split_page_range(page_start: int, page_end: int) -> str:
    """Return a BibTeX-friendly page range for one split segment."""
    if page_start <= 0 or page_end <= 0:
        return ""
    if page_start == page_end:
        return str(page_start)
    return f"{page_start}-{page_end}"


@router.get("/metadata/schema")
def get_metadata_schema() -> dict[str, Any]:
    """Return shared citation/author schema used by backend and frontend."""
    return get_citation_schema()


@router.get("/collections/{bucket_name}/documents")
def get_documents(
    bucket_name: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
    include_debug: bool = True,
    include_locations: bool = True,
) -> dict[str, Any]:
    """List document records for one bucket."""
    try:
        documents = service.list_documents(
            bucket_name.strip(),
            include_debug=include_debug,
            include_locations=include_locations,
        )
    except Exception as exc:
        raise_document_http_error(exc)
    return {"bucket_name": bucket_name.strip(), "documents": documents}


@router.get("/collections/{bucket_name}/bibliography.bib")
def download_collection_bibliography(
    bucket_name: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> Response:
    """Return collection metadata as a downloadable BibTeX bibliography file."""
    normalized_bucket_name = bucket_name.strip()
    if not normalized_bucket_name:
        raise HTTPException(status_code=400, detail="bucket_name must not be empty.")

    try:
        documents = service.list_documents(normalized_bucket_name)
    except Exception as exc:
        raise_document_http_error(exc)

    bibtex_payload, entry_count = build_collection_bibtex(documents=documents)
    safe_bucket_name = "".join(
        character if (character.isalnum() or character in {"-", "_", "."}) else "-"
        for character in normalized_bucket_name
    ).strip("-")
    if not safe_bucket_name:
        safe_bucket_name = "collection"
    download_file_name = f"{safe_bucket_name}-bibliography.bib"
    return Response(
        content=bibtex_payload,
        media_type="application/x-bibtex",
        headers={
            "Content-Disposition": f'attachment; filename="{download_file_name}"',
            "Cache-Control": "no-store",
            "X-BibTeX-Entry-Count": str(entry_count),
        },
    )


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


@router.get("/collections/{bucket_name}/documents/{document_id}/debug")
def get_document_debug_payload(
    bucket_name: str,
    document_id: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Return one document's partitions/chunks payload on demand."""
    try:
        payload = service.get_document_debug_payload(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
        )
    except Exception as exc:
        raise_document_http_error(exc)
    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        **payload,
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


@router.post("/collections/{bucket_name}/documents/split/preview")
async def preview_document_split(
    bucket_name: str,
    file_name: str,
    request: Request,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Preview how a PDF would be split by outline heading level."""
    normalized_bucket_name = bucket_name.strip()
    normalized_file_name = file_name.strip()
    if not normalized_file_name:
        raise HTTPException(status_code=400, detail="file name must not be empty.")

    payload = await request.body()
    try:
        reader = load_pdf_reader(payload)
        plan = build_pdf_split_plan(reader, normalized_file_name)
    except Exception as exc:
        raise_document_http_error(exc)

    metadata_seed = extract_pdf_metadata_seed(payload)
    if "title" not in metadata_seed:
        metadata_seed["title"] = plan.pdf_title
    try:
        crossref_result = service.lookup_metadata_seed_from_crossref(metadata=metadata_seed)
    except Exception:
        crossref_result = {}
    crossref_metadata = crossref_result.get("metadata")
    if isinstance(crossref_metadata, dict):
        for field_name, field_value in crossref_metadata.items():
            if field_value in ("", None, []):
                continue
            metadata_seed[field_name] = field_value

    return {
        "bucket_name": normalized_bucket_name,
        "file_name": normalized_file_name,
        "metadata_seed": metadata_seed,
        **plan.to_dict(),
    }


@router.post("/collections/{bucket_name}/documents/split/upload")
async def upload_split_document(
    bucket_name: str,
    file_name: str,
    heading_level: int,
    request: Request,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
    folder_name: str | None = None,
    book_title: str | None = None,
    author: str | None = None,
) -> dict[str, Any]:
    """Split one PDF by outline heading level, upload parts, and queue processing."""
    normalized_bucket_name = bucket_name.strip()
    normalized_file_name = file_name.strip()
    if not normalized_file_name:
        raise HTTPException(status_code=400, detail="file name must not be empty.")
    if heading_level < 1 or heading_level > MAX_SPLIT_HEADING_LEVEL:
        raise HTTPException(
            status_code=400,
            detail=(f"heading_level must be between 1 and {MAX_SPLIT_HEADING_LEVEL}."),
        )

    payload = await request.body()
    metadata_overrides = _parse_split_metadata_headers(request)
    selected_output_files = _parse_split_selected_files(request)
    normalized_legacy_book_title = (book_title or "").strip()
    normalized_legacy_author = (author or "").strip()
    if normalized_legacy_book_title and "booktitle" not in metadata_overrides:
        metadata_overrides["booktitle"] = normalized_legacy_book_title
    if normalized_legacy_author and "author" not in metadata_overrides:
        metadata_overrides["author"] = normalized_legacy_author
    try:
        reader = load_pdf_reader(payload)
        plan = build_pdf_split_plan(
            reader,
            normalized_file_name,
            folder_name_override=folder_name,
            pdf_title_override=(
                normalized_legacy_book_title
                or metadata_overrides.get("booktitle", "")
            ),
        )
        level_preview = plan.get_level(heading_level)
    except Exception as exc:
        raise_document_http_error(exc)

    if not level_preview.available or not level_preview.splits:
        raise HTTPException(
            status_code=400,
            detail=f"No level {heading_level} outline headings were found in this PDF.",
        )
    split_segments = list(level_preview.splits)
    if selected_output_files is not None:
        split_segments = [
            split_segment
            for split_segment in split_segments
            if split_segment.file_name in selected_output_files
        ]
        if not split_segments:
            raise HTTPException(
                status_code=400,
                detail="Select at least one split output file.",
            )

    uploaded: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for split_segment in split_segments:
        split_metadata_overrides = dict(metadata_overrides)
        if "pages" not in split_metadata_overrides:
            split_page_range = _format_split_page_range(
                split_segment.page_start,
                split_segment.page_end,
            )
            if split_page_range:
                split_metadata_overrides["pages"] = split_page_range
        try:
            split_payload = render_pdf_split_segment(
                reader,
                split_segment,
                book_title=metadata_overrides.get("booktitle", plan.pdf_title),
                author=metadata_overrides.get("author", normalized_legacy_author),
            )
            object_name = service.upload_document(
                bucket_name=normalized_bucket_name,
                object_name=split_segment.object_name,
                payload=split_payload,
                content_type="application/pdf",
            )
        except Exception as exc:
            failures.append(
                {
                    "chapter_title": split_segment.chapter_title,
                    "object_name": split_segment.object_name,
                    "error": str(exc),
                }
            )
            continue

        queued = True
        task_id: str | None = None
        queue_error = ""
        try:
            task = enqueue_partition_task(
                normalized_bucket_name,
                object_name,
                metadata_overrides=split_metadata_overrides,
            )
            task_id = task.id
        except Exception as exc:
            queued = False
            queue_error = str(exc)
            logger.exception("Failed to enqueue partition_minio_object task for split PDF.")

        uploaded.append(
            {
                "chapter_title": split_segment.chapter_title,
                "object_name": object_name,
                "file_name": split_segment.file_name,
                "page_start": split_segment.page_start,
                "page_end": split_segment.page_end,
                "page_count": split_segment.page_count,
                "pages": split_metadata_overrides.get("pages", ""),
                "queued": queued,
                "task_id": task_id,
                "queue_error": queue_error,
            }
        )

    return {
        "bucket_name": normalized_bucket_name,
        "file_name": normalized_file_name,
        "pdf_title": plan.pdf_title,
        "folder_name": plan.folder_name,
        "heading_level": heading_level,
        "uploaded_count": len(uploaded),
        "failure_count": len(failures),
        "uploaded": uploaded,
        "failures": failures,
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


@router.post("/collections/{bucket_name}/documents/{document_id}/reindex")
def reindex_document(
    bucket_name: str,
    document_id: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Queue reindex/upsert work for one existing document."""
    normalized_bucket_name = bucket_name.strip()
    normalized_document_id = document_id.strip()
    try:
        stage_payload = service.build_document_reindex_payload(
            bucket_name=normalized_bucket_name,
            document_id=normalized_document_id,
        )
    except Exception as exc:
        raise_document_http_error(exc)

    queued = True
    task_id: str | None = None
    queue_error = ""
    try:
        task = enqueue_upsert_task(stage_payload)
        task_id = task.id
    except Exception as exc:
        queued = False
        queue_error = str(exc)
        logger.exception("Failed to enqueue upsert_minio_object task for reindex.")

    return {
        "bucket_name": normalized_bucket_name,
        "document_id": normalized_document_id,
        "object_name": stage_payload.get("object_name", ""),
        "queued": queued,
        "task_id": task_id,
        "queue_error": queue_error,
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
