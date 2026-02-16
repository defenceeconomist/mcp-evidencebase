import pytest

from mcp_evidencebase import __version__

pytestmark = pytest.mark.area_cli


def test_version_is_non_empty() -> None:
    """Check package metadata exports a non-empty version string."""
    assert __version__
