"""Helpers for mapping logical collections onto physical MinIO storage."""

from __future__ import annotations

import json
from collections.abc import Iterable

COLLECTION_MARKER_FILENAME = ".evidencebase-folder.json"
DEFAULT_STORAGE_BUCKET_NAME = "evidence-base"


def normalize_collection_name(collection_name: str) -> str:
    """Normalize and validate a logical collection name."""
    normalized = str(collection_name).strip().strip("/")
    if not normalized:
        raise ValueError("bucket_name must not be empty.")
    if "/" in normalized:
        raise ValueError("bucket_name must not contain '/'.")
    return normalized


def normalize_object_name(object_name: str) -> str:
    """Normalize and validate a collection-relative object path."""
    normalized = str(object_name).strip().lstrip("/")
    if not normalized:
        raise ValueError("object_name must not be empty.")
    return normalized


def build_storage_object_name(collection_name: str, object_name: str) -> str:
    """Build one physical object name inside the shared storage bucket."""
    normalized_collection = normalize_collection_name(collection_name)
    normalized_object = normalize_object_name(object_name)
    return f"{normalized_collection}/{normalized_object}"


def build_collection_marker_object_name(collection_name: str) -> str:
    """Return the marker-object path for one logical collection."""
    normalized_collection = normalize_collection_name(collection_name)
    return f"{normalized_collection}/{COLLECTION_MARKER_FILENAME}"


def is_collection_marker_object_name(object_name: str) -> bool:
    """Return whether one storage object path is the collection marker."""
    normalized = str(object_name).strip().lstrip("/")
    return normalized.endswith(f"/{COLLECTION_MARKER_FILENAME}")


def split_storage_object_name(storage_object_name: str) -> tuple[str, str]:
    """Split ``collection/path`` into logical collection and relative object name."""
    normalized = str(storage_object_name).strip().lstrip("/")
    if "/" not in normalized:
        raise ValueError("storage_object_name must include a collection prefix.")
    collection_name, object_name = normalized.split("/", 1)
    normalized_collection = normalize_collection_name(collection_name)
    normalized_object = normalize_object_name(object_name)
    return normalized_collection, normalized_object


def extract_collection_name_from_storage_object_name(storage_object_name: str) -> str:
    """Return the logical collection for one shared-bucket object path."""
    try:
        collection_name, _ = split_storage_object_name(storage_object_name)
    except ValueError:
        return ""
    return collection_name


def marker_payload(collection_name: str) -> bytes:
    """Build the marker-object payload for one logical collection."""
    normalized_collection = normalize_collection_name(collection_name)
    payload = {
        "collection_name": normalized_collection,
        "kind": "collection-marker",
    }
    return json.dumps(payload, sort_keys=True).encode("utf-8")


def collect_storage_collection_names(object_names: Iterable[str]) -> list[str]:
    """Return sorted unique logical collections from shared-bucket object paths."""
    collection_names: set[str] = set()
    for object_name in object_names:
        collection_name = extract_collection_name_from_storage_object_name(object_name)
        if collection_name:
            collection_names.add(collection_name)
    return sorted(collection_names)
