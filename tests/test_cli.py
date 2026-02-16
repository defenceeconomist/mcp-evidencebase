import sys

import pytest

from mcp_evidencebase import __version__, cli

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
