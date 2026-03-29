"""Task dispatch helpers that preserve monkeypatch points in `mcp_evidencebase.api`."""

from __future__ import annotations

from typing import Any


def enqueue_partition_task(
    bucket_name: str,
    object_name: str,
    *,
    update_meta: bool = True,
    metadata_overrides: dict[str, Any] | None = None,
) -> Any:
    """Queue partition task using the symbols exposed on ``mcp_evidencebase.api``."""
    import mcp_evidencebase.api as api_module

    if metadata_overrides:
        return api_module.partition_minio_object.delay(
            bucket_name,
            object_name,
            None,
            update_meta,
            metadata_overrides,
        )
    return api_module.partition_minio_object.delay(bucket_name, object_name, None, update_meta)


def enqueue_scan_task(bucket_name: str) -> Any:
    """Queue scan task using the symbols exposed on ``mcp_evidencebase.api``."""
    import mcp_evidencebase.api as api_module

    return api_module.scan_minio_objects.delay(bucket_name)


def enqueue_upsert_task(stage_payload: dict[str, str]) -> Any:
    """Queue upsert task using the symbols exposed on ``mcp_evidencebase.api``."""
    import mcp_evidencebase.api as api_module

    return api_module.upsert_minio_object.delay(stage_payload)
