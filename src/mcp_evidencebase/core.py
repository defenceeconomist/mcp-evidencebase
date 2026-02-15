"""Core helpers for mcp_evidencebase."""

from __future__ import annotations

import importlib
from typing import Protocol


class BucketSummaryLike(Protocol):
    """Bucket data needed for UI/API listing."""

    name: str


class MinioClientLike(Protocol):
    """Subset of MinIO client behavior required by core bucket helpers."""

    def bucket_exists(self, bucket_name: str) -> bool:
        """Return whether ``bucket_name`` exists."""
        ...

    def make_bucket(self, bucket_name: str, location: str | None = None) -> None:
        """Create ``bucket_name`` in an optional ``location``."""
        ...

    def remove_bucket(self, bucket_name: str) -> None:
        """Remove ``bucket_name``."""
        ...

    def list_buckets(self) -> list[BucketSummaryLike]:
        """List available buckets."""
        ...


def healthcheck() -> str:
    """Return a simple status message."""
    return "ok"


def _normalize_bucket_name(bucket_name: str) -> str:
    """Normalize and validate a bucket name input."""
    normalized_bucket_name = bucket_name.strip()
    if not normalized_bucket_name:
        raise ValueError("bucket_name must not be empty.")
    return normalized_bucket_name


def _resolve_minio_client(
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
    secure: bool,
    region: str | None,
    client: MinioClientLike | None,
) -> MinioClientLike:
    """Return a provided client or construct one from connection settings."""
    if client is not None:
        return client

    try:
        minio_module = importlib.import_module("minio")
    except ImportError as exc:
        raise RuntimeError(
            "MinIO SDK is required to manage buckets. Install dependency 'minio'."
        ) from exc

    return minio_module.Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        region=region,
    )


def add_minio_bucket(
    bucket_name: str,
    *,
    endpoint: str = "minio:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    secure: bool = False,
    region: str | None = None,
    client: MinioClientLike | None = None,
) -> bool:
    """Create a MinIO bucket when it does not already exist.

    Args:
        bucket_name: Name of the MinIO bucket to create.
        endpoint: MinIO endpoint in ``host:port`` format.
        access_key: MinIO access key.
        secret_key: MinIO secret key.
        secure: Use HTTPS when ``True``.
        region: Optional region/location value for bucket creation.
        client: Optional pre-configured MinIO-compatible client.

    Returns:
        ``True`` if the bucket was created, or ``False`` when it already existed.

    Raises:
        ValueError: If ``bucket_name`` is blank.
        RuntimeError: If the MinIO SDK cannot be imported when ``client`` is omitted.
    """
    normalized_bucket_name = _normalize_bucket_name(bucket_name)
    minio_client = _resolve_minio_client(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        region=region,
        client=client,
    )

    if minio_client.bucket_exists(normalized_bucket_name):
        return False

    minio_client.make_bucket(normalized_bucket_name, location=region)
    return True


def remove_minio_bucket(
    bucket_name: str,
    *,
    endpoint: str = "minio:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    secure: bool = False,
    region: str | None = None,
    client: MinioClientLike | None = None,
) -> bool:
    """Remove a MinIO bucket if it exists.

    Args:
        bucket_name: Name of the MinIO bucket to remove.
        endpoint: MinIO endpoint in ``host:port`` format.
        access_key: MinIO access key.
        secret_key: MinIO secret key.
        secure: Use HTTPS when ``True``.
        region: Optional region/location value used by the client.
        client: Optional pre-configured MinIO-compatible client.

    Returns:
        ``True`` if the bucket was removed, or ``False`` when it did not exist.
    """
    normalized_bucket_name = _normalize_bucket_name(bucket_name)
    minio_client = _resolve_minio_client(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        region=region,
        client=client,
    )

    if not minio_client.bucket_exists(normalized_bucket_name):
        return False

    minio_client.remove_bucket(normalized_bucket_name)
    return True


def list_minio_buckets(
    *,
    endpoint: str = "minio:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    secure: bool = False,
    region: str | None = None,
    client: MinioClientLike | None = None,
) -> list[str]:
    """Return all bucket names available on the MinIO server."""
    minio_client = _resolve_minio_client(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        region=region,
        client=client,
    )
    bucket_names = [bucket.name for bucket in minio_client.list_buckets()]
    bucket_names.sort()
    return bucket_names
