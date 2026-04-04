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


def test_main_migrate_qdrant_to_shared_collection_prints_summary_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ensure migration flag calls the Qdrant backfill workflow and prints JSON."""

    class FakeService:
        def migrate_legacy_qdrant_collections(self, *, dry_run: bool = True) -> dict[str, Any]:
            assert dry_run is False
            return {
                "shared_collection_name": "evidence-base",
                "legacy_collections_seen": 2,
                "legacy_points_migrated": 14,
            }

    monkeypatch.setattr(cli, "build_ingestion_service", lambda: FakeService())
    monkeypatch.setattr(
        sys,
        "argv",
        ["mcp-evidencebase", "--migrate-qdrant-to-shared-collection", "--apply"],
    )

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"shared_collection_name": "evidence-base"' in captured.out
    assert '"legacy_collections_seen": 2' in captured.out
    assert '"legacy_points_migrated": 14' in captured.out


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


def test_main_relocate_prefix_to_root_prints_summary_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Ensure relocation flags call maintenance workflow and print serialized summary."""

    class FakeService:
        def relocate_prefix_to_bucket_root(
            self,
            *,
            bucket_name: str,
            source_prefix: str = "articles/",
            dry_run: bool = True,
        ) -> dict[str, Any]:
            assert bucket_name == "offsets"
            assert source_prefix == "articles/"
            assert dry_run is True
            return {"bucket_name": bucket_name, "would_relocate": 3, "relocated": 0}

    monkeypatch.setattr(cli, "build_ingestion_service", lambda: FakeService())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mcp-evidencebase",
            "--relocate-prefix-to-root",
            "--bucket",
            "offsets",
        ],
    )

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert '"bucket_name": "offsets"' in captured.out
    assert '"would_relocate": 3' in captured.out
    assert '"relocated": 0' in captured.out


def test_main_relocate_prefix_to_root_returns_non_zero_on_dependency_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Relocation should fail cleanly when a required dependency is disabled."""

    class FakeService:
        def relocate_prefix_to_bucket_root(self, **kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise DependencyDisabledError(
                component="Qdrant",
                feature="repository relocation",
                hint="Enable Qdrant to relocate indexed documents.",
            )

    monkeypatch.setattr(cli, "build_ingestion_service", lambda: FakeService())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mcp-evidencebase",
            "--relocate-prefix-to-root",
            "--bucket",
            "offsets",
            "--apply",
        ],
    )

    exit_code = cli.main()
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Qdrant is disabled for repository relocation." in captured.out


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
