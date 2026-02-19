"""Task dispatch helpers that preserve monkeypatch points in `mcp_evidencebase.api`."""

from __future__ import annotations

from typing import Any


def enqueue_partition_task(bucket_name: str, object_name: str) -> Any:
    """Queue partition task using the symbols exposed on ``mcp_evidencebase.api``."""
    import mcp_evidencebase.api as api_module

    return api_module.partition_minio_object.delay(bucket_name, object_name, None, True)


def enqueue_scan_task(bucket_name: str) -> Any:
    """Queue scan task using the symbols exposed on ``mcp_evidencebase.api``."""
    import mcp_evidencebase.api as api_module

    return api_module.scan_minio_objects.delay(bucket_name)
