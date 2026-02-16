"""Celery tasks for document ingestion."""

from __future__ import annotations

from mcp_evidencebase.celery_app import app
from mcp_evidencebase.ingestion import build_ingestion_service


@app.task(name="mcp_evidencebase.ping")
def ping() -> str:
    """Health task for verifying worker execution."""
    return "pong"


@app.task(name="mcp_evidencebase.scan_minio_objects")
def scan_minio_objects(bucket_name: str | None = None) -> dict[str, int]:
    """Scan MinIO buckets and enqueue processing for new or changed objects."""
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
            process_minio_object.delay(current_bucket_name, object_name, etag)
            queued += 1

    return {"scanned": scanned, "queued": queued}


@app.task(
    name="mcp_evidencebase.process_minio_object",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def process_minio_object(
    self: object,
    bucket_name: str,
    object_name: str,
    etag: str | None = None,
) -> dict[str, str]:
    """Process a single MinIO object into partitions/chunks/vectors."""
    service = build_ingestion_service()
    document_id = service.process_object(
        bucket_name=bucket_name,
        object_name=object_name,
        etag=etag,
    )
    return {
        "bucket_name": bucket_name,
        "object_name": object_name,
        "document_id": document_id,
    }
