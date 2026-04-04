"""Service layer for logical collection management operations."""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any

from minio import Minio

from mcp_evidencebase.minio_settings import MinioSettings
from mcp_evidencebase.storage_layout import (
    build_collection_marker_object_name,
    collect_storage_collection_names,
    marker_payload,
    normalize_collection_name,
)


@dataclass(frozen=True)
class BucketService:
    """Service for listing, creating, and removing logical collections."""

    settings: MinioSettings

    def _client(self) -> Minio:
        return Minio(
            self.settings.endpoint,
            access_key=self.settings.access_key,
            secret_key=self.settings.secret_key,
            secure=self.settings.secure,
            region=self.settings.region,
        )

    def _bucket_exists(self, client: Any, bucket_name: str) -> bool:
        if hasattr(client, "bucket_exists"):
            return bool(client.bucket_exists(bucket_name))
        bucket_names = [str(bucket.name) for bucket in client.list_buckets()]
        return bucket_name in bucket_names

    def _ensure_storage_bucket(self, client: Any) -> None:
        if self._bucket_exists(client, self.settings.storage_bucket_name):
            return
        client.make_bucket(self.settings.storage_bucket_name, location=self.settings.region)

    def _list_storage_collection_names(self, client: Any) -> list[str]:
        if not self._bucket_exists(client, self.settings.storage_bucket_name):
            return []
        object_names = [
            str(getattr(item, "object_name", "")).strip()
            for item in client.list_objects(self.settings.storage_bucket_name, recursive=True)
            if str(getattr(item, "object_name", "")).strip()
        ]
        return collect_storage_collection_names(object_names)

    def _delete_storage_collection_prefix(self, client: Any, collection_name: str) -> bool:
        if not self._bucket_exists(client, self.settings.storage_bucket_name):
            return False
        normalized_collection_name = normalize_collection_name(collection_name)
        prefix = f"{normalized_collection_name}/"
        removed = False
        for item in client.list_objects(self.settings.storage_bucket_name, recursive=True):
            object_name = str(getattr(item, "object_name", "")).strip()
            if not object_name.startswith(prefix):
                continue
            client.remove_object(self.settings.storage_bucket_name, object_name)
            removed = True
        return removed

    def list_buckets(self) -> list[str]:
        """Return all logical collection names."""
        client = self._client()
        collection_names = set(self._list_storage_collection_names(client))
        for bucket in client.list_buckets():
            bucket_name = str(bucket.name).strip()
            if not bucket_name or bucket_name == self.settings.storage_bucket_name:
                continue
            collection_names.add(bucket_name)
        return sorted(collection_names)

    def create_bucket(self, bucket_name: str) -> bool:
        """Create one logical collection marker in the shared storage bucket."""
        normalized_bucket_name = normalize_collection_name(bucket_name)
        client = self._client()
        existing_collections = set(self.list_buckets())
        if normalized_bucket_name in existing_collections:
            return False

        self._ensure_storage_bucket(client)
        marker_object_name = build_collection_marker_object_name(normalized_bucket_name)
        payload = marker_payload(normalized_bucket_name)
        client.put_object(
            self.settings.storage_bucket_name,
            marker_object_name,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        return True

    def delete_bucket(self, bucket_name: str) -> bool:
        """Delete one logical collection prefix or fall back to legacy bucket deletion."""
        normalized_bucket_name = normalize_collection_name(bucket_name)
        client = self._client()
        removed_storage_prefix = self._delete_storage_collection_prefix(client, normalized_bucket_name)
        if removed_storage_prefix:
            return True
        if not self._bucket_exists(client, normalized_bucket_name):
            return False
        client.remove_bucket(normalized_bucket_name)
        return True
