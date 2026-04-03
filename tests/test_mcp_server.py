from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any

import pytest

from mcp_evidencebase.ingestion_modules.service import (
    DependencyConfigurationError,
    DependencyDisabledError,
)
from mcp_evidencebase.mcp_server.server import build_server
from mcp_evidencebase.mcp_server.tools import EvidenceBaseMcpTools

pytestmark = pytest.mark.area_mcp


class FakeBucketService:
    def __init__(
        self,
        *,
        bucket_names: list[str] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.bucket_names = list(bucket_names or [])
        self.error = error

    def list_buckets(self) -> list[str]:
        if self.error is not None:
            raise self.error
        return list(self.bucket_names)


class FakeIngestionService:
    def __init__(
        self,
        *,
        documents: list[dict[str, Any]] | None = None,
        sections: list[dict[str, Any]] | None = None,
        section: dict[str, Any] | None = None,
        search_results: list[dict[str, Any]] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.documents = list(documents or [])
        self.sections = list(sections or [])
        self.section = dict(section or {"section_id": "sec-1", "section_title": "Methods"})
        self.search_results = list(search_results or [])
        self.error = error
        self.search_calls: list[dict[str, Any]] = []

    def list_documents(self, bucket_name: str) -> list[dict[str, Any]]:
        if self.error is not None:
            raise self.error
        return [{"bucket_name": bucket_name, **document} for document in self.documents]

    def search_documents(
        self,
        *,
        bucket_name: str,
        query: str,
        limit: int = 10,
        mode: str = "hybrid",
        rrf_k: int = 60,
    ) -> list[dict[str, Any]]:
        if self.error is not None:
            raise self.error
        self.search_calls.append(
            {
                "bucket_name": bucket_name,
                "query": query,
                "limit": limit,
                "mode": mode,
                "rrf_k": rrf_k,
            }
        )
        return list(self.search_results)

    def list_document_sections(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> list[dict[str, Any]]:
        if self.error is not None:
            raise self.error
        return [
            {
                "bucket_name": bucket_name,
                "document_id": document_id,
                **section,
            }
            for section in self.sections
        ]

    def get_document_section(
        self,
        *,
        bucket_name: str,
        document_id: str,
        section_id: str,
    ) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        return {
            "bucket_name": bucket_name,
            "document_id": document_id,
            "section_id": section_id,
            **self.section,
        }


def build_tools(
    *,
    bucket_service: FakeBucketService | None = None,
    ingestion_service: FakeIngestionService | None = None,
    health_payload: dict[str, Any] | None = None,
    health_error: Exception | None = None,
) -> EvidenceBaseMcpTools:
    def collect_health() -> dict[str, Any]:
        if health_error is not None:
            raise health_error
        return dict(health_payload or {"status": "ok", "ready": True})

    return EvidenceBaseMcpTools(
        bucket_service_factory=lambda: bucket_service
        or FakeBucketService(bucket_names=["offsets"]),
        ingestion_service_factory=lambda: ingestion_service
        or FakeIngestionService(
            documents=[{"document_id": "doc-1"}],
            sections=[{"section_id": "sec-1"}],
            search_results=[{"id": "chunk-1"}],
        ),
        runtime_health_collector=collect_health,
    )


def test_healthcheck_returns_runtime_report() -> None:
    tools = build_tools(health_payload={"status": "ok", "ready": True, "checks": {"redis": "ok"}})

    result = tools.healthcheck()

    assert result["health"] == {"status": "ok", "ready": True, "checks": {"redis": "ok"}}


def test_list_buckets_returns_bucket_names() -> None:
    tools = build_tools(bucket_service=FakeBucketService(bucket_names=["beta", "alpha"]))

    result = tools.list_buckets()

    assert result == {"buckets": ["beta", "alpha"]}


def test_list_documents_requires_non_empty_bucket_name() -> None:
    tools = build_tools()

    with pytest.raises(ValueError, match="bucket_name must not be empty"):
        tools.list_documents("   ")


def test_list_documents_returns_bucket_payload() -> None:
    tools = build_tools(
        ingestion_service=FakeIngestionService(documents=[{"document_id": "doc-7"}])
    )

    result = tools.list_documents("evaluation")

    assert result == {
        "bucket_name": "evaluation",
        "documents": [{"bucket_name": "evaluation", "document_id": "doc-7"}],
    }


def test_search_collection_preserves_current_defaults() -> None:
    service = FakeIngestionService(search_results=[{"id": "chunk-1", "score": 0.9}])
    tools = build_tools(ingestion_service=service)

    result = tools.search_collection(bucket_name="evaluation", query="matching")

    assert result["mode"] == "hybrid"
    assert result["limit"] == 10
    assert result["rrf_k"] == 60
    assert result["results"] == [{"id": "chunk-1", "score": 0.9}]
    assert service.search_calls == [
        {
            "bucket_name": "evaluation",
            "query": "matching",
            "limit": 10,
            "mode": "hybrid",
            "rrf_k": 60,
        }
    ]


def test_search_collection_rejects_invalid_mode() -> None:
    tools = build_tools()

    with pytest.raises(ValueError, match="mode must be one of"):
        tools.search_collection(bucket_name="evaluation", query="matching", mode="dense")


def test_search_collection_surfaces_dependency_disabled_errors() -> None:
    tools = build_tools(
        ingestion_service=FakeIngestionService(
            error=DependencyDisabledError(
                component="Qdrant",
                feature="search",
                hint="Enable Qdrant to use search.",
            )
        )
    )

    with pytest.raises(ValueError, match="Qdrant is disabled for search"):
        tools.search_collection(bucket_name="evaluation", query="matching")


def test_list_document_sections_returns_sections() -> None:
    tools = build_tools(
        ingestion_service=FakeIngestionService(
            sections=[{"section_id": "sec-1", "section_title": "Intro"}]
        )
    )

    result = tools.list_document_sections(bucket_name="evaluation", document_id="doc-1")

    assert result == {
        "bucket_name": "evaluation",
        "document_id": "doc-1",
        "sections": [
            {
                "bucket_name": "evaluation",
                "document_id": "doc-1",
                "section_id": "sec-1",
                "section_title": "Intro",
            }
        ],
    }


def test_get_document_section_returns_one_section() -> None:
    tools = build_tools(
        ingestion_service=FakeIngestionService(section={"section_title": "Methods", "page": 4})
    )

    result = tools.get_document_section(
        bucket_name="evaluation",
        document_id="doc-1",
        section_id="sec-9",
    )

    assert result == {
        "bucket_name": "evaluation",
        "document_id": "doc-1",
        "section_id": "sec-9",
        "section": {
            "bucket_name": "evaluation",
            "document_id": "doc-1",
            "section_id": "sec-9",
            "section_title": "Methods",
            "page": 4,
        },
    }


def test_get_metadata_schema_returns_wrapped_schema() -> None:
    tools = build_tools()

    result = tools.get_metadata_schema()

    assert "metadata_schema" in result
    assert isinstance(result["metadata_schema"], dict)
    assert "document_types" in result["metadata_schema"]


def test_dependency_configuration_error_is_user_facing() -> None:
    tools = build_tools(
        ingestion_service=FakeIngestionService(
            error=DependencyConfigurationError("REDIS_URL is required.")
        )
    )

    with pytest.raises(ValueError, match="REDIS_URL is required"):
        tools.list_documents("evaluation")


def test_unexpected_errors_are_normalized() -> None:
    tools = build_tools(ingestion_service=FakeIngestionService(error=RuntimeError("boom")))

    with pytest.raises(ValueError, match="Internal error while running 'list_documents'"):
        tools.list_documents("evaluation")


def test_stdio_server_advertises_expected_tools() -> None:
    pytest.importorskip("mcp")
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    repo_root = Path(__file__).resolve().parents[1]
    python_bin = repo_root / ".venv" / "bin" / "python"
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo_root / "src")
    env["MCP_EVIDENCEBASE_REQUIRE_MINIO"] = "false"
    env["MCP_EVIDENCEBASE_REQUIRE_REDIS"] = "false"
    env["MCP_EVIDENCEBASE_REQUIRE_QDRANT"] = "false"
    env["MCP_EVIDENCEBASE_REQUIRE_CELERY_BROKER"] = "false"
    env["MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND"] = "false"

    async def run() -> list[str]:
        server_params = StdioServerParameters(
            command=str(python_bin),
            args=["-m", "mcp_evidencebase.mcp_server"],
            env=env,
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_response = await session.list_tools()
                return [tool.name for tool in tools_response.tools]

    tool_names = asyncio.run(run())

    assert tool_names == [
        "healthcheck",
        "list_buckets",
        "list_documents",
        "search_collection",
        "list_document_sections",
        "get_document_section",
        "get_metadata_schema",
    ]


def test_build_server_returns_fastmcp_instance() -> None:
    server = build_server(build_tools())

    assert server.name == "mcp-evidencebase"
    assert sys.modules["mcp_evidencebase.mcp_server.server"]
