"""FastAPI application for bucket and document ingestion operations.

The module exposes endpoints used by the dashboard and command-line workflows.
"""

from __future__ import annotations

from datetime import UTC, datetime
import logging
import os
from pathlib import PurePosixPath
import secrets
from typing import Annotated, Any, NoReturn
from urllib.parse import urlsplit, urlunsplit

from fastapi import Depends, FastAPI, HTTPException, Header, Request, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from minio.error import S3Error
from pydantic import BaseModel, Field

from mcp_evidencebase.bucket_service import BucketService
from mcp_evidencebase.ingestion import SEARCH_MODES, IngestionService, build_ingestion_service
from mcp_evidencebase.minio_settings import MinioSettings, build_minio_settings
from mcp_evidencebase.tasks import partition_minio_object, scan_minio_objects

app = FastAPI(title="mcp-evidencebase API", version="0.1.0")
logger = logging.getLogger(__name__)
gpt_basic_auth = HTTPBasic(auto_error=False)


class BucketCreateRequest(BaseModel):
    """Payload for creating a MinIO bucket."""

    bucket_name: str = Field(description="Name of the bucket to create.")


class MetadataUpdateRequest(BaseModel):
    """Payload for updating document metadata fields."""

    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Subset of normalized metadata fields to update.",
    )


class GptSearchRequest(BaseModel):
    """Payload for GPT action search requests."""

    bucket_name: str | None = Field(
        default=None,
        description=(
            "Optional bucket/collection name to search. "
            "If omitted and exactly one bucket exists, it is selected automatically."
        ),
    )
    query: str = Field(description="Natural language query.")
    limit: int = Field(default=10, description="Maximum number of results to return.")
    mode: str = Field(
        default="hybrid",
        description=f"Search mode. One of: {', '.join(SEARCH_MODES)}.",
    )
    rrf_k: int = Field(
        default=60,
        description="Reciprocal Rank Fusion parameter for hybrid mode.",
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


def _unauthorized_basic_auth_error() -> HTTPException:
    """Create a standard Basic Auth challenge response."""
    return HTTPException(
        status_code=401,
        detail="Invalid API credentials.",
        headers={"WWW-Authenticate": 'Basic realm="gpt-actions"'},
    )


def _matches_api_key(candidate: str | None, expected_api_key: str) -> bool:
    """Return true when a candidate token matches the configured API key."""
    if not candidate:
        return False
    normalized_candidate = candidate.strip()
    if not normalized_candidate:
        return False
    return secrets.compare_digest(normalized_candidate, expected_api_key)


def require_gpt_basic_auth(
    request: Request,
    credentials: Annotated[HTTPBasicCredentials | None, Depends(gpt_basic_auth)],
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> str:
    """Validate API key auth for GPT action endpoints."""
    expected_api_key = os.getenv("GPT_ACTIONS_API_KEY", "").strip()
    if not expected_api_key:
        raise HTTPException(
            status_code=503,
            detail="GPT API key is not configured on the server.",
        )

    if credentials is not None:
        # Keep Basic-compatible behavior for manual/legacy clients.
        if _matches_api_key(credentials.username, expected_api_key) or _matches_api_key(
            credentials.password, expected_api_key
        ):
            return credentials.username

    if _matches_api_key(x_api_key, expected_api_key):
        return "x-api-key"

    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        bearer_token = authorization[7:]
        if _matches_api_key(bearer_token, expected_api_key):
            return "bearer"

    raise _unauthorized_basic_auth_error()


def _perform_collection_search(
    *,
    bucket_name: str,
    query: str,
    limit: int,
    mode: str,
    rrf_k: int,
    service: IngestionService,
) -> dict[str, Any]:
    """Execute collection search with shared validation and response shape."""
    normalized_mode = mode.strip().lower()
    if normalized_mode not in SEARCH_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"mode must be one of: {', '.join(SEARCH_MODES)}",
        )

    normalized_bucket_name = bucket_name.strip()
    normalized_query = query.strip()
    try:
        results = service.search_documents(
            bucket_name=normalized_bucket_name,
            query=normalized_query,
            limit=limit,
            mode=normalized_mode,
            rrf_k=rrf_k,
        )
    except Exception as exc:
        _raise_document_http_error(exc)

    return {
        "bucket_name": normalized_bucket_name,
        "query": normalized_query,
        "mode": normalized_mode,
        "limit": max(1, min(int(limit), 100)),
        "rrf_k": max(1, int(rrf_k)),
        "results": results,
    }


def _resolve_gpt_search_bucket_name(
    *,
    bucket_name: str | None,
    service: IngestionService,
) -> str:
    """Resolve bucket name for GPT search requests with single-bucket fallback."""
    normalized_bucket_name = (bucket_name or "").strip()
    if normalized_bucket_name:
        return normalized_bucket_name

    try:
        available_buckets = service.list_buckets()
    except Exception as exc:
        _raise_document_http_error(exc)

    if not available_buckets:
        raise HTTPException(status_code=404, detail="No buckets are available to search.")

    if len(available_buckets) == 1:
        return available_buckets[0]

    preview = ", ".join(available_buckets[:10])
    raise HTTPException(
        status_code=400,
        detail=f"bucket_name is required when multiple buckets exist. Available buckets: {preview}",
    )


def _normalize_public_base_url(value: str) -> str:
    """Return a normalized absolute base URL without query, fragment, or trailing slash."""
    normalized = value.strip()
    if not normalized:
        return ""
    if "://" not in normalized:
        normalized = f"https://{normalized.lstrip('/')}"
    parsed = urlsplit(normalized)
    if not parsed.netloc:
        return ""
    scheme = parsed.scheme or "https"
    normalized_path = parsed.path.rstrip("/")
    return urlunsplit((scheme, parsed.netloc, normalized_path, "", ""))


def _resolve_gpt_links_base_url(request: Request) -> str:
    """Resolve the base URL used to build clickable links in GPT search responses."""
    configured_base_url = _normalize_public_base_url(
        os.getenv("GPT_ACTIONS_LINK_BASE_URL", "")
    )
    if configured_base_url:
        return configured_base_url
    return _normalize_public_base_url(str(request.base_url))


def _absolutize_http_url(base_url: str, value: str) -> str:
    """Convert site-relative URLs to absolute URLs against ``base_url``."""
    normalized_value = value.strip()
    if not normalized_value:
        return ""
    parsed = urlsplit(normalized_value)
    if parsed.scheme and parsed.netloc:
        return normalized_value
    if not normalized_value.startswith("/"):
        return normalized_value
    if not base_url:
        return normalized_value
    return f"{base_url.rstrip('/')}{normalized_value}"


def _prepare_gpt_search_result(item: dict[str, Any], *, links_base_url: str) -> dict[str, Any]:
    """Normalize GPT search result links for external clients."""
    normalized_result = dict(item)
    source_material_url = _absolutize_http_url(
        links_base_url,
        str(item.get("source_material_url", "")),
    )
    resolver_link_url = _absolutize_http_url(
        links_base_url,
        str(item.get("resolver_link_url", "")),
    )
    resolver_url = str(item.get("resolver_url", "")).strip()

    if source_material_url:
        normalized_result["source_material_url"] = source_material_url
    if resolver_link_url:
        normalized_result["resolver_link_url"] = resolver_link_url

    if resolver_url.startswith("docs://"):
        normalized_result["resolver_reference"] = resolver_url
        if resolver_link_url:
            normalized_result["resolver_url"] = resolver_link_url
    elif resolver_url:
        normalized_result["resolver_url"] = _absolutize_http_url(
            links_base_url,
            resolver_url,
        )
    elif resolver_link_url:
        normalized_result["resolver_url"] = resolver_link_url

    return normalized_result


def _prepare_gpt_search_response(
    payload: dict[str, Any], *, links_base_url: str
) -> dict[str, Any]:
    """Return GPT search response payload with normalized link fields."""
    normalized_payload = dict(payload)
    raw_results = payload.get("results", [])
    if not isinstance(raw_results, list):
        return normalized_payload

    normalized_payload["results"] = [
        _prepare_gpt_search_result(item, links_base_url=links_base_url)
        if isinstance(item, dict)
        else item
        for item in raw_results
    ]
    return normalized_payload


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Return API readiness status.

    Returns:
        Dictionary with a static ``ok`` status string.
    """
    return {"status": "ok"}


@app.get("/gpt/ping")
def gpt_ping(
    message: str = "ping",
    authenticated_user: Annotated[str, Depends(require_gpt_basic_auth)] = "",
) -> dict[str, str]:
    """Return a Basic-auth protected ping response for GPT Actions."""
    del authenticated_user
    normalized_message = message.strip() or "ping"
    return {
        "status": "ok",
        "reply": "pong",
        "echo": normalized_message,
        "timestamp_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


@app.post("/gpt/search")
def gpt_search(
    request: Request,
    payload: GptSearchRequest,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
    authenticated_user: Annotated[str, Depends(require_gpt_basic_auth)] = "",
) -> dict[str, Any]:
    """GPT Actions wrapper for collection search."""
    del authenticated_user
    resolved_bucket_name = _resolve_gpt_search_bucket_name(
        bucket_name=payload.bucket_name,
        service=service,
    )
    search_payload = _perform_collection_search(
        bucket_name=resolved_bucket_name,
        query=payload.query,
        limit=payload.limit,
        mode=payload.mode,
        rrf_k=payload.rrf_k,
        service=service,
    )
    links_base_url = _resolve_gpt_links_base_url(request)
    return _prepare_gpt_search_response(
        search_payload,
        links_base_url=links_base_url,
    )


@app.get("/gpt/openapi.json", include_in_schema=False)
def gpt_openapi() -> dict[str, Any]:
    """Return a minimal OpenAPI document for the GPT ping action."""
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "Evidence Base GPT Ping API",
            "version": "1.0.0",
            "description": "Minimal API-key-over-Basic API for ChatGPT custom GPT actions.",
        },
        "servers": [{"url": "https://open.heley.uk/api"}],
        "components": {
            "schemas": {
                "GptSearchRequest": {
                    "type": "object",
                    "required": ["query"],
                    "properties": {
                        "bucket_name": {
                            "type": "string",
                            "description": (
                                "Optional bucket/collection name to search. "
                                "If omitted and exactly one bucket exists, it is selected automatically."
                            ),
                        },
                        "query": {
                            "type": "string",
                            "description": "Natural language query text.",
                        },
                        "limit": {
                            "type": "integer",
                            "default": 10,
                            "description": "Maximum number of results to return.",
                        },
                        "mode": {
                            "type": "string",
                            "default": "hybrid",
                            "description": f"Search mode. One of: {', '.join(SEARCH_MODES)}.",
                        },
                        "rrf_k": {
                            "type": "integer",
                            "default": 60,
                            "description": "Reciprocal Rank Fusion parameter for hybrid mode.",
                        },
                    },
                },
                "GptSearchResponse": {
                    "type": "object",
                    "required": ["bucket_name", "query", "mode", "limit", "rrf_k", "results"],
                    "properties": {
                        "bucket_name": {"type": "string"},
                        "query": {"type": "string"},
                        "mode": {"type": "string"},
                        "limit": {"type": "integer"},
                        "rrf_k": {"type": "integer"},
                        "results": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/GptSearchResult"},
                        },
                    },
                },
                "GptSearchResult": {
                    "type": "object",
                    "required": ["id", "score", "text"],
                    "properties": {
                        "id": {"type": ["string", "integer"]},
                        "score": {"type": "number"},
                        "document_id": {"type": "string"},
                        "bucket_name": {"type": "string"},
                        "file_path": {"type": "string"},
                        "source_material_url": {
                            "type": "string",
                            "description": (
                                "HTTP link to retrieve the source document from this API."
                            ),
                        },
                        "resolver_link_url": {
                            "type": "string",
                            "description": (
                                "HTTP link to open resolver.html with the resolved page anchor."
                            ),
                        },
                        "resolver_url": {
                            "type": "string",
                            "description": (
                                "Web-accessible resolver link. "
                                "Use this for clickable links."
                            ),
                        },
                        "resolver_reference": {
                            "type": "string",
                            "description": (
                                "Internal docs:// resolver reference retained for compatibility."
                            ),
                        },
                        "page_start": {"type": "integer"},
                        "page_end": {"type": "integer"},
                        "text": {"type": "string"},
                        "minio_location": {"type": "string"},
                    },
                    "additionalProperties": True,
                },
            },
            "securitySchemes": {
                "BearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "API Key",
                    "description": "ChatGPT Actions: Authentication type API key, Auth Type Bearer.",
                }
            },
        },
        "paths": {
            "/gpt/ping": {
                "get": {
                    "operationId": "ping",
                    "summary": "Ping endpoint",
                    "description": "Returns a pong response to confirm API connectivity.",
                    "security": [{"BearerAuth": []}],
                    "parameters": [
                        {
                            "name": "message",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "string", "default": "ping"},
                            "description": "Optional text to echo back in the response.",
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Successful ping response.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "required": [
                                            "status",
                                            "reply",
                                            "echo",
                                            "timestamp_utc",
                                        ],
                                        "properties": {
                                            "status": {"type": "string"},
                                            "reply": {"type": "string"},
                                            "echo": {"type": "string"},
                                            "timestamp_utc": {
                                                "type": "string",
                                                "format": "date-time",
                                            },
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            },
            "/gpt/search": {
                "post": {
                    "operationId": "searchCollection",
                    "summary": "Search collection",
                    "description": "Search one collection using semantic, keyword, or hybrid retrieval.",
                    "security": [{"BearerAuth": []}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/GptSearchRequest"}
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Search results.",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/GptSearchResponse"}
                                }
                            },
                        }
                    },
                }
            },
        },
    }


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


@app.get("/collections/{bucket_name}/search")
def search_collection(
    bucket_name: str,
    query: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
    limit: int = 10,
    mode: str = "hybrid",
    rrf_k: int = 60,
) -> dict[str, Any]:
    """Search one collection using semantic, keyword, or hybrid retrieval.

    Args:
        bucket_name: Bucket path parameter.
        query: User query text.
        limit: Maximum number of chunks to return.
        mode: Retrieval mode (semantic, keyword, hybrid).
        rrf_k: Rank constant used by Reciprocal Rank Fusion in hybrid mode.
        service: Ingestion service dependency.

    Returns:
        Search metadata and ranked chunk hits.
    """
    return _perform_collection_search(
        bucket_name=bucket_name,
        query=query,
        limit=limit,
        mode=mode,
        rrf_k=rrf_k,
        service=service,
    )


@app.get("/collections/{bucket_name}/documents/resolve")
def resolve_document(
    bucket_name: str,
    file_path: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> Response:
    """Return one stored object for the PDF.js resolver view.

    Args:
        bucket_name: Bucket path parameter.
        file_path: Object path query parameter.
        service: Ingestion service dependency.

    Returns:
        Raw document bytes with inline content disposition.
    """
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
        _raise_document_http_error(exc)

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
        task = partition_minio_object.delay(
            normalized_bucket_name,
            object_name,
            None,
            True,
        )
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


@app.post("/collections/{bucket_name}/documents/{document_id}/metadata/fetch")
def fetch_document_metadata_from_crossref(
    bucket_name: str,
    document_id: str,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
) -> dict[str, Any]:
    """Fetch metadata from Crossref using DOI/ISBN/ISSN/title fallbacks.

    Args:
        bucket_name: Bucket path parameter.
        document_id: Canonical document hash.
        service: Ingestion service dependency.

    Returns:
        Match source, confidence, and merged metadata payload.
    """
    try:
        result = service.fetch_metadata_from_crossref(
            bucket_name=bucket_name.strip(),
            document_id=document_id.strip(),
        )
    except Exception as exc:
        _raise_document_http_error(exc)

    return {
        "bucket_name": bucket_name.strip(),
        "document_id": document_id.strip(),
        "lookup_field": result.get("lookup_field", ""),
        "confidence": result.get("confidence", 0.0),
        "metadata": result.get("metadata", {}),
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
