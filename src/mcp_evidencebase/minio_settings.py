"""MinIO API settings helpers."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class MinioSettings:
    """Connection settings for the MinIO client."""

    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    region: str | None


def to_bool(value: str | None) -> bool:
    """Convert common truthy strings to ``True``."""
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def build_minio_settings(env: Mapping[str, str] | None = None) -> MinioSettings:
    """Build MinIO settings from an environment mapping.

    Args:
        env: Optional mapping of environment variables. Defaults to ``os.environ``.

    Returns:
        A ``MinioSettings`` instance with sane defaults.
    """
    source = os.environ if env is None else env
    region = source.get("MINIO_REGION")
    return MinioSettings(
        endpoint=source.get("MINIO_ENDPOINT", "minio:9000"),
        access_key=source.get("MINIO_ROOT_USER", "minioadmin"),
        secret_key=source.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=to_bool(source.get("MINIO_SECURE")),
        region=region if region else None,
    )
