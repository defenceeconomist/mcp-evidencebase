"""Service layer for bucket management operations."""

from __future__ import annotations

from dataclasses import dataclass

from mcp_evidencebase.core import add_minio_bucket, list_minio_buckets, remove_minio_bucket
from mcp_evidencebase.minio_settings import MinioSettings


@dataclass(frozen=True)
class BucketService:
    """Service for listing, adding, and removing MinIO buckets."""

    settings: MinioSettings

    def list_buckets(self) -> list[str]:
        """Return all bucket names."""
        return list_minio_buckets(
            endpoint=self.settings.endpoint,
            access_key=self.settings.access_key,
            secret_key=self.settings.secret_key,
            secure=self.settings.secure,
            region=self.settings.region,
        )

    def create_bucket(self, bucket_name: str) -> bool:
        """Create ``bucket_name`` if missing."""
        return add_minio_bucket(
            bucket_name,
            endpoint=self.settings.endpoint,
            access_key=self.settings.access_key,
            secret_key=self.settings.secret_key,
            secure=self.settings.secure,
            region=self.settings.region,
        )

    def delete_bucket(self, bucket_name: str) -> bool:
        """Delete ``bucket_name`` if it exists."""
        return remove_minio_bucket(
            bucket_name,
            endpoint=self.settings.endpoint,
            access_key=self.settings.access_key,
            secret_key=self.settings.secret_key,
            secure=self.settings.secure,
            region=self.settings.region,
        )
