"""Celery tasks for asynchronous ingestion workflow.

Task summary:

- ``mcp_evidencebase.ping``: worker health probe.
- ``mcp_evidencebase.scan_minio_objects``: scans buckets and enqueues changed objects.
- ``mcp_evidencebase.partition_minio_object``: partition stage.
- ``mcp_evidencebase.meta_minio_object``: metadata stage (+ optional Crossref update).
- ``mcp_evidencebase.section_minio_object``: section mapping stage.
- ``mcp_evidencebase.chunk_minio_object``: chunk stage.
- ``mcp_evidencebase.upsert_minio_object``: vector upsert stage.
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


def _resolve_stage_payload(
    payload: Mapping[str, Any],
    *,
    task_name: str,
) -> tuple[str, str, str, str]:
    """Resolve canonical stage payload fields from one mapping."""
    bucket_name = str(payload.get("bucket_name", "")).strip()
    object_name = str(payload.get("object_name", "")).strip()
    document_id = str(payload.get("document_id", "")).strip()
    etag = str(payload.get("etag", "")).strip()
    if not bucket_name or not object_name or not document_id:
        raise ValueError(
            f"{task_name} requires bucket_name, object_name, and document_id."
        )
    return bucket_name, object_name, document_id, etag


@app.task(name="mcp_evidencebase.ping")  # type: ignore[untyped-decorator]
def ping() -> str:
    """Health task for verifying worker execution."""
    return "pong"


@app.task(name="mcp_evidencebase.scan_minio_objects")  # type: ignore[untyped-decorator]
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


@app.task(  # type: ignore[untyped-decorator]
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
    metadata_overrides: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Run partition stage and enqueue metadata stage.

    Args:
        bucket_name: Source MinIO bucket.
        object_name: Source object path.
        etag: Optional object etag from scanner context.
        update_meta: When ``True``, request Crossref enrichment during metadata stage.

    Returns:
        Mapping containing stage payload with deterministic IDs and keys.
    """
    service = build_ingestion_service()
    stage_payload = service.partition_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        etag=etag,
    )
    meta_payload: dict[str, Any] = {
        "bucket_name": stage_payload.get("bucket_name", bucket_name),
        "object_name": stage_payload.get("object_name", object_name),
        "document_id": stage_payload.get("document_id", ""),
        "etag": stage_payload.get("etag", ""),
        "partition_key": stage_payload.get("partition_key", ""),
        "meta_key": stage_payload.get("meta_key", ""),
        "update_meta": bool(update_meta),
    }
    if metadata_overrides:
        meta_payload["metadata_overrides"] = dict(metadata_overrides)
    meta_minio_object.delay(meta_payload)
    return stage_payload


@app.task(  # type: ignore[untyped-decorator]
    name="mcp_evidencebase.meta_minio_object",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def meta_minio_object(partition_payload: Mapping[str, Any]) -> dict[str, str]:
    """Run metadata stage, optionally enrich from Crossref, then enqueue section stage."""
    bucket_name, object_name, document_id, etag = _resolve_stage_payload(
        partition_payload,
        task_name="meta_minio_object",
    )
    update_meta = bool(partition_payload.get("update_meta", False))
    metadata_overrides = partition_payload.get("metadata_overrides")
    service = build_ingestion_service()
    stage_payload = service.meta_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag=etag,
    )
    if update_meta:
        _update_metadata_from_crossref(
            service=service,
            bucket_name=bucket_name,
            document_id=document_id,
        )
    if isinstance(metadata_overrides, Mapping) and metadata_overrides:
        service.update_metadata(
            bucket_name=bucket_name,
            document_id=document_id,
            metadata=metadata_overrides,
            refresh_vectors=False,
        )
    section_minio_object.delay(stage_payload)
    return stage_payload


@app.task(  # type: ignore[untyped-decorator]
    name="mcp_evidencebase.section_minio_object",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def section_minio_object(meta_payload: Mapping[str, Any]) -> dict[str, str]:
    """Run section mapping stage and enqueue chunk stage."""
    bucket_name, object_name, document_id, etag = _resolve_stage_payload(
        meta_payload,
        task_name="section_minio_object",
    )
    service = build_ingestion_service()
    stage_payload = service.section_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag=etag,
    )
    chunk_minio_object.delay(stage_payload)
    return stage_payload


@app.task(  # type: ignore[untyped-decorator]
    name="mcp_evidencebase.chunk_minio_object",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def chunk_minio_object(section_payload: Mapping[str, Any]) -> dict[str, str]:
    """Run chunk stage and enqueue vector upsert stage.

    Args:
        section_payload: Output payload produced by ``section_minio_object``.

    Returns:
        Mapping containing the processed bucket/object and deterministic ``document_id``.
    """
    bucket_name, object_name, document_id, etag = _resolve_stage_payload(
        section_payload,
        task_name="chunk_minio_object",
    )
    service = build_ingestion_service()
    stage_payload = service.chunk_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag=etag,
    )
    upsert_minio_object.delay(stage_payload)
    return stage_payload


@app.task(  # type: ignore[untyped-decorator]
    name="mcp_evidencebase.upsert_minio_object",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def upsert_minio_object(chunk_payload: Mapping[str, Any]) -> dict[str, str]:
    """Run vector upsert stage for a previously chunked document."""
    bucket_name, object_name, document_id, etag = _resolve_stage_payload(
        chunk_payload,
        task_name="upsert_minio_object",
    )
    service = build_ingestion_service()
    stage_payload = service.upsert_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag=etag,
    )
    return stage_payload


@app.task(  # type: ignore[untyped-decorator]
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
    """Backward-compatible wrapper that runs all ingestion stages inline."""
    service = build_ingestion_service()
    partition_payload = service.partition_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        etag=etag,
    )
    meta_payload = service.meta_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=partition_payload["document_id"],
        etag=partition_payload.get("etag", ""),
    )
    if update_meta:
        _update_metadata_from_crossref(
            service=service,
            bucket_name=bucket_name,
            document_id=partition_payload["document_id"],
        )
    service.section_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=partition_payload["document_id"],
        etag=meta_payload.get("etag", ""),
    )
    service.chunk_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=partition_payload["document_id"],
        etag=meta_payload.get("etag", ""),
    )
    service.upsert_stage_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=partition_payload["document_id"],
        etag=meta_payload.get("etag", ""),
    )
    return {
        "bucket_name": bucket_name,
        "object_name": object_name,
        "document_id": partition_payload["document_id"],
    }
