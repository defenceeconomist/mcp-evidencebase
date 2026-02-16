import sys

import pytest

from mcp_evidencebase import __version__
from mcp_evidencebase import cli

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
