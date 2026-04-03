"""Celery application configuration."""

from __future__ import annotations

import logging
import os

from celery import Celery  # type: ignore[import-untyped]
from celery.signals import beat_init, worker_init  # type: ignore[import-untyped]

from mcp_evidencebase.runtime_diagnostics import (
    collect_runtime_health,
    log_runtime_health,
    raise_for_failed_required_checks,
)

logger = logging.getLogger(__name__)

broker_url = os.getenv("CELERY_BROKER_URL", "").strip()
result_backend = os.getenv("CELERY_RESULT_BACKEND", "").strip()
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


def _validate_runtime_dependencies(component_name: str) -> None:
    """Log and validate runtime dependencies for worker-style processes."""
    report = collect_runtime_health()
    log_runtime_health(logger, report=report, component_name=component_name)
    raise_for_failed_required_checks(report, component_name=component_name)


@worker_init.connect  # type: ignore[untyped-decorator]
def validate_worker_runtime_dependencies(*args: object, **kwargs: object) -> None:
    """Validate runtime dependencies when the Celery worker boots."""
    del args, kwargs
    _validate_runtime_dependencies("celery-worker")


@beat_init.connect  # type: ignore[untyped-decorator]
def validate_beat_runtime_dependencies(*args: object, **kwargs: object) -> None:
    """Validate runtime dependencies when the Celery beat process boots."""
    del args, kwargs
    _validate_runtime_dependencies("celery-beat")
