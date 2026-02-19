"""GPT-related API routes."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request

from mcp_evidencebase.api_modules.deps import get_ingestion_service, require_gpt_basic_auth
from mcp_evidencebase.api_modules.models import GptSearchRequest
from mcp_evidencebase.api_modules.services import (
    build_gpt_openapi_document,
    perform_collection_search,
    prepare_gpt_search_response,
    resolve_gpt_links_base_url,
    resolve_gpt_search_bucket_name,
)
from mcp_evidencebase.ingestion import IngestionService

router = APIRouter(tags=["gpt"])


@router.get("/gpt/ping")
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


@router.post("/gpt/search")
def gpt_search(
    request: Request,
    payload: GptSearchRequest,
    service: Annotated[IngestionService, Depends(get_ingestion_service)],
    authenticated_user: Annotated[str, Depends(require_gpt_basic_auth)] = "",
) -> dict[str, Any]:
    """GPT Actions wrapper for collection search."""
    del authenticated_user
    resolved_bucket_name = resolve_gpt_search_bucket_name(
        bucket_name=payload.bucket_name,
        service=service,
    )
    search_payload = perform_collection_search(
        bucket_name=resolved_bucket_name,
        query=payload.query,
        limit=payload.limit,
        mode=payload.mode,
        rrf_k=payload.rrf_k,
        service=service,
    )
    links_base_url = resolve_gpt_links_base_url(request)
    return prepare_gpt_search_response(search_payload, links_base_url=links_base_url)


@router.get("/gpt/openapi.json", include_in_schema=False)
def gpt_openapi() -> dict[str, Any]:
    """Return a minimal OpenAPI document for GPT endpoints."""
    return build_gpt_openapi_document()
