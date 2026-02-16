"""Celery application configuration."""

from __future__ import annotations

import os

from celery import Celery

broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
_raw_scan_interval_seconds = os.getenv("MINIO_SCAN_INTERVAL_SECONDS", "15")
try:
    minio_scan_interval_seconds = int(_raw_scan_interval_seconds)
except ValueError:
    minio_scan_interval_seconds = 15

app = Celery("mcp_evidencebase", broker=broker_url, backend=result_backend)
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        "scan-minio-buckets": {
            "task": "mcp_evidencebase.scan_minio_objects",
            "schedule": max(5, minio_scan_interval_seconds),
        }
    },
)
app.autodiscover_tasks(["mcp_evidencebase"])
