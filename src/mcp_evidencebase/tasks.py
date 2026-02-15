"""Example Celery tasks."""

from __future__ import annotations

from mcp_evidencebase.celery_app import app


@app.task(name="mcp_evidencebase.ping")
def ping() -> str:
    """Health task for verifying worker execution."""
    return "pong"
