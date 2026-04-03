"""Dependency providers and auth guards for API routes."""

from __future__ import annotations

import os
import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from mcp_evidencebase.api_modules.errors import unauthorized_basic_auth_error
from mcp_evidencebase.bucket_service import BucketService
from mcp_evidencebase.ingestion import IngestionService, build_ingestion_service
from mcp_evidencebase.ingestion_modules.service import DependencyConfigurationError
from mcp_evidencebase.minio_settings import MinioSettings, build_minio_settings

gpt_basic_auth = HTTPBasic(auto_error=False)


def get_minio_settings() -> MinioSettings:
    """Resolve MinIO connection settings."""
    return build_minio_settings()


def get_bucket_service(
    settings: Annotated[MinioSettings, Depends(get_minio_settings)],
) -> BucketService:
    """Resolve bucket service dependency."""
    return BucketService(settings=settings)


def get_ingestion_service() -> IngestionService:
    """Resolve document ingestion service dependency."""
    try:
        return build_ingestion_service()
    except DependencyConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def matches_api_key(candidate: str | None, expected_api_key: str) -> bool:
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
        if matches_api_key(credentials.username, expected_api_key) or matches_api_key(
            credentials.password, expected_api_key
        ):
            return credentials.username

    if matches_api_key(x_api_key, expected_api_key):
        return "x-api-key"

    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        bearer_token = authorization[7:]
        if matches_api_key(bearer_token, expected_api_key):
            return "bearer"

    raise unauthorized_basic_auth_error()
