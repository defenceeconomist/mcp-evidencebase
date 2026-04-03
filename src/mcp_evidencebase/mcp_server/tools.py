"""Tool implementations for the Evidence Base MCP server."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from minio.error import S3Error

from mcp_evidencebase.bucket_service import BucketService
from mcp_evidencebase.citation_schema import get_citation_schema
from mcp_evidencebase.ingestion import IngestionService, build_ingestion_service
from mcp_evidencebase.ingestion_modules.service import (
    DependencyConfigurationError,
    DependencyDisabledError,
)
from mcp_evidencebase.mcp_server.serialization import (
    JSONValue,
    normalize_positive_int,
    normalize_required_text,
    normalize_search_mode,
    to_jsonable,
)
from mcp_evidencebase.minio_settings import build_minio_settings
from mcp_evidencebase.runtime_diagnostics import collect_runtime_health

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvidenceBaseMcpTools:
    """Thin MCP-facing adapter over the existing service layer."""

    bucket_service_factory: Callable[[], BucketService] = lambda: BucketService(
        settings=build_minio_settings()
    )
    ingestion_service_factory: Callable[[], IngestionService] = build_ingestion_service
    runtime_health_collector: Callable[[], dict[str, Any]] = collect_runtime_health

    def _handle_call(
        self,
        *,
        tool_name: str,
        operation: Callable[[], Any],
    ) -> JSONValue:
        try:
            return to_jsonable(operation())
        except (
            DependencyConfigurationError,
            DependencyDisabledError,
            S3Error,
            ValueError,
        ) as exc:
            raise ValueError(str(exc)) from exc
        except Exception as exc:  # pragma: no cover - defensive error normalization
            logger.exception("Unexpected error while running MCP tool '%s'.", tool_name)
            raise ValueError(
                f"Internal error while running '{tool_name}'. Check server logs for details."
            ) from exc

    def healthcheck(self) -> dict[str, JSONValue]:
        """Return dependency-aware runtime health."""
        return {
            "health": self._handle_call(
                tool_name="healthcheck",
                operation=self.runtime_health_collector,
            )
        }

    def list_buckets(self) -> dict[str, JSONValue]:
        """Return all bucket names."""
        return {
            "buckets": self._handle_call(
                tool_name="list_buckets",
                operation=lambda: self.bucket_service_factory().list_buckets(),
            )
        }

    def list_documents(self, bucket_name: str) -> dict[str, JSONValue]:
        """Return normalized document records for one bucket."""
        normalized_bucket_name = normalize_required_text(bucket_name, field_name="bucket_name")
        return {
            "bucket_name": normalized_bucket_name,
            "documents": self._handle_call(
                tool_name="list_documents",
                operation=lambda: self.ingestion_service_factory().list_documents(
                    normalized_bucket_name
                ),
            ),
        }

    def search_collection(
        self,
        bucket_name: str,
        query: str,
        limit: int = 10,
        mode: str = "hybrid",
        rrf_k: int = 60,
    ) -> dict[str, JSONValue]:
        """Search one collection using the current ingestion search behavior."""
        normalized_bucket_name = normalize_required_text(bucket_name, field_name="bucket_name")
        normalized_query = normalize_required_text(query, field_name="query")
        normalized_limit = normalize_positive_int(limit, field_name="limit")
        normalized_mode = normalize_search_mode(mode)
        normalized_rrf_k = normalize_positive_int(rrf_k, field_name="rrf_k")
        return {
            "bucket_name": normalized_bucket_name,
            "query": normalized_query,
            "limit": normalized_limit,
            "mode": normalized_mode,
            "rrf_k": normalized_rrf_k,
            "results": self._handle_call(
                tool_name="search_collection",
                operation=lambda: self.ingestion_service_factory().search_documents(
                    bucket_name=normalized_bucket_name,
                    query=normalized_query,
                    limit=normalized_limit,
                    mode=normalized_mode,
                    rrf_k=normalized_rrf_k,
                ),
            ),
        }

    def list_document_sections(
        self,
        bucket_name: str,
        document_id: str,
    ) -> dict[str, JSONValue]:
        """Return all sections for one document."""
        normalized_bucket_name = normalize_required_text(bucket_name, field_name="bucket_name")
        normalized_document_id = normalize_required_text(document_id, field_name="document_id")
        return {
            "bucket_name": normalized_bucket_name,
            "document_id": normalized_document_id,
            "sections": self._handle_call(
                tool_name="list_document_sections",
                operation=lambda: self.ingestion_service_factory().list_document_sections(
                    bucket_name=normalized_bucket_name,
                    document_id=normalized_document_id,
                ),
            ),
        }

    def get_document_section(
        self,
        bucket_name: str,
        document_id: str,
        section_id: str,
    ) -> dict[str, JSONValue]:
        """Return one section record for one document."""
        normalized_bucket_name = normalize_required_text(bucket_name, field_name="bucket_name")
        normalized_document_id = normalize_required_text(document_id, field_name="document_id")
        normalized_section_id = normalize_required_text(section_id, field_name="section_id")
        return {
            "bucket_name": normalized_bucket_name,
            "document_id": normalized_document_id,
            "section_id": normalized_section_id,
            "section": self._handle_call(
                tool_name="get_document_section",
                operation=lambda: self.ingestion_service_factory().get_document_section(
                    bucket_name=normalized_bucket_name,
                    document_id=normalized_document_id,
                    section_id=normalized_section_id,
                ),
            ),
        }

    def get_metadata_schema(self) -> dict[str, JSONValue]:
        """Return the shared metadata schema."""
        return {
            "metadata_schema": self._handle_call(
                tool_name="get_metadata_schema",
                operation=get_citation_schema,
            )
        }
