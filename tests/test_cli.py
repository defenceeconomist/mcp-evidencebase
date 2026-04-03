import sys
from typing import Any

import pytest

from mcp_evidencebase import __version__, cli
from mcp_evidencebase.ingestion_modules.service import DependencyDisabledError

pytestmark = pytest.mark.area_cli


def test_version_is_non_empty() -> None:
    """Check package metadata exports a non-empty version string."""
    assert __version__


def test_main_purge_datastores_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ensure ``--purge-datastores`` runs purge and prints the summary payload."""

    class FakeService:
        def purge_datastores(self) -> dict[str, int]:
            return {"redis_deleted_keys": 3, "qdrant_deleted_collections": 2}

    monkeypatch.setattr(cli, "build_ingestion_service", lambda: FakeService())
    monkeypatch.setattr(sys, "argv", ["mcp-evidencebase", "--purge-datastores"])

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "redis_deleted_keys" in captured.out
    assert "qdrant_deleted_collections" in captured.out


def test_main_healthcheck_exits_non_zero_when_runtime_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ensure ``--healthcheck`` reflects dependency readiness in its exit code."""
    monkeypatch.setattr(cli, "healthcheck", lambda: "error")
    monkeypatch.setattr(sys, "argv", ["mcp-evidencebase", "--healthcheck"])

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out.strip() == "error"


def test_main_doctor_prints_report_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ensure ``--doctor`` prints the structured preflight report."""
    monkeypatch.setattr(
        cli,
        "collect_runtime_health",
        lambda: {
            "status": "ok",
            "ready": True,
            "summary": "All required runtime dependencies are reachable.",
            "failed_required_checks": [],
            "contract": {
                "celery_result_backend": {
                    "required": True,
                    "env_var": "MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND",
                }
            },
            "checks": {
                "celery_result_backend": {
                    "required": True,
                    "configured": True,
                    "status": "ok",
                    "target": "redis://redis:6379/1",
                    "detail": "reachable",
                }
            },
        },
    )
    monkeypatch.setattr(sys, "argv", ["mcp-evidencebase", "--doctor"])

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"ready": true' in captured.out
    assert '"status": "ok"' in captured.out
    assert '"celery_result_backend"' in captured.out


def test_main_search_prints_results_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ensure search flags call ingestion search and print serialized result payload."""

    class FakeService:
        def search_documents(
            self,
            *,
            bucket_name: str,
            query: str,
            limit: int = 10,
            mode: str = "hybrid",
            rrf_k: int = 60,
        ) -> list[dict[str, str]]:
            assert bucket_name == "research-raw"
            assert query == "causal inference"
            assert limit == 3
            assert mode == "hybrid"
            assert rrf_k == 80
            return [{"id": "chunk-1", "document_id": "doc-1", "text": "Causal chunk"}]

    monkeypatch.setattr(cli, "build_ingestion_service", lambda: FakeService())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mcp-evidencebase",
            "--search-bucket",
            "research-raw",
            "--search-query",
            "causal inference",
            "--search-limit",
            "3",
            "--search-mode",
            "hybrid",
            "--search-rrf-k",
            "80",
        ],
    )

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"bucket_name": "research-raw"' in captured.out
    assert '"query": "causal inference"' in captured.out
    assert '"rrf_k": 80' in captured.out
    assert (
        '"results": [{"document_id": "doc-1", "id": "chunk-1", "text": "Causal chunk"}]'
        in captured.out
    )


def test_main_search_returns_non_zero_when_qdrant_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Search should fail cleanly with a dependency-disabled message."""

    class FakeService:
        def search_documents(self, **kwargs: Any) -> list[dict[str, str]]:
            del kwargs
            raise DependencyDisabledError(
                component="Qdrant",
                feature="search",
                hint="Enable Qdrant to use search.",
            )

    monkeypatch.setattr(cli, "build_ingestion_service", lambda: FakeService())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mcp-evidencebase",
            "--search-bucket",
            "research-raw",
            "--search-query",
            "causal inference",
        ],
    )

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Qdrant is disabled for search." in captured.out
