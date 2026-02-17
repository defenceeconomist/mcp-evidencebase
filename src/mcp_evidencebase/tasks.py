"""Celery tasks for asynchronous ingestion workflow.

Task summary:

- ``mcp_evidencebase.ping``: worker health probe.
- ``mcp_evidencebase.scan_minio_objects``: scans buckets and enqueues changed objects.
- ``mcp_evidencebase.partition_minio_object``: partition + metadata stage.
- ``mcp_evidencebase.chunk_minio_object``: chunk + vector upsert stage.
- ``mcp_evidencebase.process_minio_object``: backward-compatible full pipeline wrapper.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from mcp_evidencebase.celery_app import app
from mcp_evidencebase.ingestion import build_ingestion_service

logger = logging.getLogger(__name__)


def _update_metadata_from_crossref(
    *,
    service: Any,
    bucket_name: str,
    document_id: str,
) -> None:
    """Attempt Crossref metadata enrichment without failing ingestion tasks."""
    try:
        service.fetch_metadata_from_crossref(
            bucket_name=bucket_name,
            document_id=document_id,
        )
    except Exception as exc:
        logger.info(
            "Crossref metadata update skipped for %s/%s: %s",
            bucket_name,
            document_id,
            exc,
        )


@app.task(name="mcp_evidencebase.ping")
def ping() -> str:
    """Health task for verifying worker execution."""
    return "pong"


@app.task(name="mcp_evidencebase.scan_minio_objects")
def scan_minio_objects(
    bucket_name: str | None = None,
    update_meta: bool = True,
) -> dict[str, int]:
    """Scan MinIO buckets and enqueue processing for new or changed objects.

    Args:
        bucket_name: Optional bucket to scan. When omitted, all buckets are scanned.
        update_meta: Forward metadata update behavior to partition tasks.

    Returns:
        Scan summary with number of buckets scanned and objects queued.
    """
    service = build_ingestion_service()
    bucket_names = [bucket_name] if bucket_name else service.list_buckets()
    queued = 0
    scanned = 0

    for current_bucket_name in bucket_names:
        scanned += 1
        for object_name, etag in service.list_bucket_objects(current_bucket_name):
            if not service.object_requires_processing(
                bucket_name=current_bucket_name,
                object_name=object_name,
                etag=etag,
            ):
                continue
            partition_minio_object.delay(
                current_bucket_name,
                object_name,
                etag,
                update_meta,
            )
            queued += 1

    return {"scanned": scanned, "queued": queued}


@app.task(
    name="mcp_evidencebase.partition_minio_object",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def partition_minio_object(
    bucket_name: str,
    object_name: str,
    etag: str | None = None,
    update_meta: bool = False,
) -> dict[str, str]:
    """Run partitioning and metadata extraction, then enqueue chunk/index stage.

    Args:
        bucket_name: Source MinIO bucket.
        object_name: Source object path.
        etag: Optional object etag from scanner context.
        update_meta: When ``True``, attempt Crossref metadata enrichment.

    Returns:
        Mapping containing stage payload with deterministic IDs and keys.
    """
    service = build_ingestion_service()
    stage_payload = service.partition_object(
        bucket_name=bucket_name,
        object_name=object_name,
        etag=etag,
    )
    if update_meta:
        _update_metadata_from_crossref(
            service=service,
            bucket_name=bucket_name,
            document_id=stage_payload["document_id"],
        )
    chunk_minio_object.delay(stage_payload)
    return stage_payload


@app.task(
    name="mcp_evidencebase.chunk_minio_object",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def chunk_minio_object(partition_payload: Mapping[str, str]) -> dict[str, str]:
    """Run chunking and vector upsert for a previously partitioned document.

    Args:
        partition_payload: Output payload produced by ``partition_minio_object``.

    Returns:
        Mapping containing the processed bucket/object and deterministic ``document_id``.
    """
    bucket_name = str(partition_payload.get("bucket_name", "")).strip()
    object_name = str(partition_payload.get("object_name", "")).strip()
    document_id = str(partition_payload.get("document_id", "")).strip()
    if not bucket_name or not object_name or not document_id:
        raise ValueError("chunk_minio_object requires bucket_name, object_name, and document_id.")

    service = build_ingestion_service()
    stage_payload = service.chunk_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
    )
    return stage_payload


@app.task(
    name="mcp_evidencebase.process_minio_object",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def process_minio_object(
    bucket_name: str,
    object_name: str,
    etag: str | None = None,
    update_meta: bool = False,
) -> dict[str, str]:
    """Backward-compatible wrapper that runs both ingestion stages inline."""
    service = build_ingestion_service()
    stage_payload = service.partition_object(
        bucket_name=bucket_name,
        object_name=object_name,
        etag=etag,
    )
    if update_meta:
        _update_metadata_from_crossref(
            service=service,
            bucket_name=bucket_name,
            document_id=stage_payload["document_id"],
        )
    service.chunk_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=stage_payload["document_id"],
    )
    return {
        "bucket_name": bucket_name,
        "object_name": object_name,
        "document_id": stage_payload["document_id"],
    }
