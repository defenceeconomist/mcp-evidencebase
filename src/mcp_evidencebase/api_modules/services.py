"""Shared service-layer helpers for API routes."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from fastapi import HTTPException, Request

from mcp_evidencebase.api_modules.errors import raise_document_http_error
from mcp_evidencebase.ingestion import SEARCH_MODES, IngestionService


def perform_collection_search(
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
        raise_document_http_error(exc)

    return {
        "bucket_name": normalized_bucket_name,
        "query": normalized_query,
        "mode": normalized_mode,
        "limit": max(1, min(int(limit), 100)),
        "rrf_k": max(1, int(rrf_k)),
        "results": results,
    }


def resolve_gpt_search_bucket_name(*, bucket_name: str | None, service: IngestionService) -> str:
    """Resolve bucket name for GPT search requests with single-bucket fallback."""
    normalized_bucket_name = (bucket_name or "").strip()
    if normalized_bucket_name:
        return normalized_bucket_name

    try:
        available_buckets = service.list_buckets()
    except Exception as exc:
        raise_document_http_error(exc)

    if not available_buckets:
        raise HTTPException(status_code=404, detail="No buckets are available to search.")

    if len(available_buckets) == 1:
        return available_buckets[0]

    preview = ", ".join(available_buckets[:10])
    raise HTTPException(
        status_code=400,
        detail=f"bucket_name is required when multiple buckets exist. Available buckets: {preview}",
    )


def normalize_public_base_url(value: str) -> str:
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


def resolve_gpt_links_base_url(request: Request) -> str:
    """Resolve the base URL used to build clickable links in GPT search responses."""
    configured_base_url = normalize_public_base_url(os.getenv("GPT_ACTIONS_LINK_BASE_URL", ""))
    if configured_base_url:
        return configured_base_url
    return normalize_public_base_url(str(request.base_url))


def absolutize_http_url(base_url: str, value: str) -> str:
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


def prepare_gpt_search_result(item: dict[str, Any], *, links_base_url: str) -> dict[str, Any]:
    """Normalize GPT search result links for external clients."""
    normalized_result = dict(item)
    source_material_url = absolutize_http_url(
        links_base_url,
        str(item.get("source_material_url", "")),
    )
    resolver_link_url = absolutize_http_url(
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
        normalized_result["resolver_url"] = absolutize_http_url(
            links_base_url,
            resolver_url,
        )
    elif resolver_link_url:
        normalized_result["resolver_url"] = resolver_link_url

    return normalized_result


def prepare_gpt_search_response(
    payload: dict[str, Any], *, links_base_url: str
) -> dict[str, Any]:
    """Return GPT search response payload with normalized link fields."""
    normalized_payload = dict(payload)
    raw_results = payload.get("results", [])
    if not isinstance(raw_results, list):
        return normalized_payload

    normalized_payload["results"] = [
        prepare_gpt_search_result(item, links_base_url=links_base_url)
        if isinstance(item, dict)
        else item
        for item in raw_results
    ]
    return normalized_payload


def build_gpt_openapi_document() -> dict[str, Any]:
    """Return minimal OpenAPI schema for GPT actions."""
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
                            "description": "HTTP link to retrieve the source document from this API.",
                        },
                        "resolver_link_url": {
                            "type": "string",
                            "description": "HTTP link to open resolver.html with the resolved page anchor.",
                        },
                        "resolver_url": {
                            "type": "string",
                            "description": "Web-accessible resolver link. Use this for clickable links.",
                        },
                        "resolver_reference": {
                            "type": "string",
                            "description": "Internal docs:// resolver reference retained for compatibility.",
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
                                        "required": ["status", "reply", "echo", "timestamp_utc"],
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
