from mcp_evidencebase.core import healthcheck


def test_healthcheck() -> None:
    assert healthcheck() == "ok"
