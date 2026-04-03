"""Shared API error mapping utilities."""

from __future__ import annotations

from typing import NoReturn

from fastapi import HTTPException
from minio.error import S3Error

from mcp_evidencebase.ingestion_modules.service import (
    DependencyConfigurationError,
    DependencyDisabledError,
)


def format_minio_error(exc: S3Error) -> str:
    """Create a concise API-safe message from MinIO SDK errors."""
    return f"{exc.code}: {exc.message}"


def raise_bucket_http_error(exc: ValueError | S3Error) -> NoReturn:
    """Map bucket-related exceptions to a client-facing HTTP error."""
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail=format_minio_error(exc)) from exc


def raise_document_http_error(exc: Exception) -> NoReturn:
    """Map ingestion exceptions to API-safe HTTP errors."""
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, (DependencyConfigurationError, DependencyDisabledError)):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if isinstance(exc, FileNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, S3Error):
        raise HTTPException(status_code=400, detail=format_minio_error(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


def unauthorized_basic_auth_error() -> HTTPException:
    """Create a standard Basic Auth challenge response."""
    return HTTPException(
        status_code=401,
        detail="Invalid API credentials.",
        headers={"WWW-Authenticate": 'Basic realm="gpt-actions"'},
    )
