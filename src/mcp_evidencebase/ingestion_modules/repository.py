"""Redis persistence for document/source metadata and partition state."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from mcp_evidencebase.ingestion_modules.metadata import (
    AUTHORS_METADATA_FIELD,
    BIBTEX_FIELDS,
    METADATA_FIELDS,
    _normalize_author_entries,
    _serialize_author_entries,
    build_default_citation_key,
    build_default_metadata,
    build_resolver_url,
    canonical_json,
    compute_partition_key,
    normalize_etag,
    normalize_metadata,
    utc_now_iso,
)

class RedisDocumentRepository:
    """Persistence layer for document, partition, and metadata state in Redis."""

    def __init__(self, redis_client: Any, *, key_prefix: str) -> None:
        """Initialize repository with a Redis client."""
        self._redis = redis_client
        self._prefix = key_prefix.strip() or "evidencebase"

    def _document_key(self, document_id: str) -> str:
        return f"{self._prefix}:document:{document_id}"

    def _document_sources_key(self, document_id: str) -> str:
        return f"{self._prefix}:document:{document_id}:sources"

    def _document_partition_payload_key(self, document_id: str) -> str:
        return f"{self._prefix}:document:{document_id}:partition"

    def _document_sections_payload_key(self, document_id: str) -> str:
        return f"{self._prefix}:document:{document_id}:sections"

    def _source_mapping_key(self, bucket_name: str, object_name: str) -> str:
        return f"{self._prefix}:source:{self._location_reference(bucket_name, object_name)}"

    def _source_meta_key(self, bucket_name: str, object_name: str) -> str:
        return f"{self._source_mapping_key(bucket_name, object_name)}:meta"

    def _source_meta_key_from_meta_key(self, meta_key: str) -> str | None:
        split_location = self._split_location_reference(meta_key)
        if not split_location:
            return None
        bucket_name, object_name = split_location
        return self._source_meta_key(bucket_name, object_name)

    def _source_bucket_key(self, bucket_name: str) -> str:
        return f"{self._prefix}:source:bucket:{bucket_name}"

    @staticmethod
    def _location_reference(bucket_name: str, object_name: str) -> str:
        return f"{bucket_name}/{object_name}"

    @staticmethod
    def _split_location_reference(location_reference: str) -> tuple[str, str] | None:
        if "/" not in location_reference:
            return None
        bucket_name, object_name = location_reference.split("/", 1)
        if not bucket_name or not object_name:
            return None
        return bucket_name, object_name

    @staticmethod
    def _safe_int(value: str | None, default: int = 0) -> int:
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    def _sorted_set_members(self, key: str) -> list[str]:
        """Return deterministic sorted set members as strings."""
        values = [str(value) for value in self._redis.smembers(key)]
        values.sort()
        return values

    def _build_source_mapping_payload(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str,
    ) -> dict[str, str]:
        """Build the canonical source mapping payload."""
        location_reference = self._location_reference(bucket_name, object_name)
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "location": location_reference,
            "document_id": document_id,
            "etag": normalize_etag(etag),
            "resolver_url": build_resolver_url(bucket_name, object_name),
            "updated_at": utc_now_iso(),
        }

    def add_document(self, bucket_name: str, document_id: str) -> None:
        """Ensure a document hash exists."""
        del bucket_name
        self._redis.hset(
            self._document_key(document_id),
            mapping={"updated_at": utc_now_iso()},
        )

    def list_document_ids(self, bucket_name: str) -> list[str]:
        """Return sorted document IDs visible in one bucket from source mappings."""
        document_ids: set[str] = set()
        bucket_sources_key = self._source_bucket_key(bucket_name)
        for location_reference in self._sorted_set_members(bucket_sources_key):
            split_location = self._split_location_reference(location_reference)
            if not split_location:
                self._redis.srem(bucket_sources_key, location_reference)
                continue
            source_mapping = self.get_object_mapping(split_location[0], split_location[1])
            document_id = source_mapping.get("document_id", "")
            if document_id:
                document_ids.add(document_id)
            else:
                self._redis.srem(bucket_sources_key, location_reference)
        return sorted(document_ids)

    def mark_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str,
    ) -> None:
        """Save source mapping and reverse document/source references."""
        mapping_key = self._source_mapping_key(bucket_name, object_name)
        previous_mapping = self.get_object_mapping(bucket_name, object_name)
        location_reference = self._location_reference(bucket_name, object_name)

        previous_document_id = previous_mapping.get("document_id", "")
        if previous_document_id and previous_document_id != document_id:
            self._redis.srem(
                self._document_sources_key(previous_document_id),
                location_reference,
            )
            # Metadata is source-scoped, so remapping a source replaces its metadata.
            self._redis.delete(self._source_meta_key(bucket_name, object_name))

        payload = self._build_source_mapping_payload(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=document_id,
            etag=etag,
        )
        self._redis.hset(mapping_key, mapping=payload)
        self._redis.sadd(
            self._document_sources_key(document_id),
            location_reference,
        )
        self._redis.sadd(self._source_bucket_key(bucket_name), location_reference)

    def get_object_mapping(self, bucket_name: str, object_name: str) -> dict[str, str]:
        """Return source mapping payload."""
        raw_mapping = self._redis.hgetall(self._source_mapping_key(bucket_name, object_name))
        return {str(key): str(value) for key, value in raw_mapping.items()}

    def get_document_object_names(self, bucket_name: str, document_id: str) -> list[str]:
        """Return all object names associated with a bucket/document pair."""
        object_names: list[str] = []
        for location in self.get_document_locations(document_id, bucket_name):
            split_location = self._split_location_reference(location)
            if split_location:
                object_names.append(split_location[1])
        object_names.sort()
        return object_names

    def get_document_locations(self, document_id: str, bucket_name: str | None = None) -> list[str]:
        """Return location references for one document hash."""
        locations = self._sorted_set_members(self._document_sources_key(document_id))
        if bucket_name is not None:
            filtered_locations: list[str] = []
            for location in locations:
                split_location = self._split_location_reference(location)
                if split_location and split_location[0] == bucket_name:
                    filtered_locations.append(location)
            return filtered_locations
        return locations

    def set_state(self, document_id: str, values: Mapping[str, Any]) -> None:
        """Update document processing state fields."""
        payload = {str(key): str(value) for key, value in values.items() if value is not None}
        payload["updated_at"] = utc_now_iso()
        self._redis.hset(self._document_key(document_id), mapping=payload)

    def get_state(self, document_id: str) -> dict[str, str]:
        """Read state hash for a document."""
        raw_mapping = self._redis.hgetall(self._document_key(document_id))
        return {str(key): str(value) for key, value in raw_mapping.items()}

    def _store_metadata_payload(
        self,
        *,
        meta_key: str,
        document_id: str,
        metadata: Mapping[str, Any],
    ) -> None:
        """Store normalized metadata payload for a source-scoped metadata key."""
        source_meta_key = self._source_meta_key_from_meta_key(meta_key)
        if source_meta_key is None:
            return
        normalized = normalize_metadata(metadata)
        payload = {field_name: normalized.get(field_name, "") for field_name in METADATA_FIELDS}
        payload["document_id"] = document_id
        payload["source"] = meta_key
        payload["updated_at"] = utc_now_iso()
        self._redis.hset(source_meta_key, mapping=payload)

    def get_metadata_by_key(self, meta_key: str) -> dict[str, str]:
        """Return metadata payload by metadata hash key."""
        if not meta_key:
            return {field_name: "" for field_name in METADATA_FIELDS}
        source_meta_key = self._source_meta_key_from_meta_key(meta_key)
        if source_meta_key is None:
            return {field_name: "" for field_name in METADATA_FIELDS}
        raw_mapping = self._redis.hgetall(source_meta_key)
        metadata = {field_name: "" for field_name in METADATA_FIELDS}
        for key, value in raw_mapping.items():
            key_name = str(key)
            if key_name in metadata:
                metadata[key_name] = str(value)
        metadata["document_type"] = metadata.get("document_type", "") or "misc"
        return metadata

    def set_metadata_for_location(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        metadata: Mapping[str, Any],
    ) -> str:
        """Store metadata for one source location and return its metadata key."""
        normalized = normalize_metadata(metadata)
        if not normalized.get("citation_key"):
            normalized["citation_key"] = build_default_citation_key(
                metadata=normalized,
                file_path=object_name,
                document_id=document_id,
            )

        meta_key = self._location_reference(bucket_name, object_name)
        self._store_metadata_payload(
            meta_key=meta_key,
            document_id=document_id,
            metadata=normalized,
        )
        return meta_key

    def get_document_metadata(
        self,
        bucket_name: str,
        document_id: str,
    ) -> tuple[str, dict[str, str]]:
        """Return source-scoped metadata key and payload for one bucket/document pair."""
        object_names = self.get_document_object_names(bucket_name, document_id)
        for object_name in object_names:
            meta_key = self._location_reference(bucket_name, object_name)
            if self._redis.hlen(self._source_meta_key(bucket_name, object_name)) <= 0:
                continue
            return meta_key, self.get_metadata_by_key(meta_key)

        state = self.get_state(document_id)
        state_meta_key = state.get("meta_key", "")
        state_meta_redis_key = self._source_meta_key_from_meta_key(state_meta_key)
        if state_meta_redis_key and self._redis.hlen(state_meta_redis_key) > 0:
            return state_meta_key, self.get_metadata_by_key(state_meta_key)

        file_path = object_names[0] if object_names else state.get("file_path", document_id)
        return "", build_default_metadata(file_path, document_id)

    def set_default_metadata_if_missing(
        self,
        bucket_name: str,
        document_id: str,
        *,
        file_path: str,
    ) -> str:
        """Populate metadata when no values were persisted yet."""
        meta_key = self._location_reference(bucket_name, file_path)
        existing_metadata = self.get_metadata_by_key(meta_key)
        if any(
            existing_metadata.get(field_name, "")
            for field_name in ("title", "author", "doi", "isbn", "issn")
        ):
            return meta_key

        default_metadata = build_default_metadata(file_path, document_id)
        return self.set_metadata_for_location(
            bucket_name=bucket_name,
            object_name=file_path,
            document_id=document_id,
            metadata=default_metadata,
        )

    def update_document_metadata(
        self,
        *,
        bucket_name: str,
        document_id: str,
        metadata: Mapping[str, Any],
    ) -> dict[str, str]:
        """Update metadata for all locations in a bucket/document pair."""
        _, existing = self.get_document_metadata(bucket_name, document_id)
        merged = normalize_metadata(existing)
        for key, value in metadata.items():
            field_name = str(key).strip().lower()
            if field_name in METADATA_FIELDS:
                if field_name == AUTHORS_METADATA_FIELD:
                    merged[field_name] = _serialize_author_entries(value)
                else:
                    merged[field_name] = str(value).strip()
        merged["document_type"] = merged.get("document_type", "") or "misc"

        object_names = self.get_document_object_names(bucket_name, document_id)
        file_path = object_names[0] if object_names else self.get_state(document_id).get("file_path", "")
        if not merged.get("citation_key"):
            merged["citation_key"] = build_default_citation_key(
                metadata=merged,
                file_path=file_path or document_id,
                document_id=document_id,
            )
        if not object_names:
            self.set_state(document_id, {"meta_key": ""})
            return merged

        last_meta_key = ""
        for object_name in object_names:
            last_meta_key = self.set_metadata_for_location(
                bucket_name=bucket_name,
                object_name=object_name,
                document_id=document_id,
                metadata=merged,
            )
        self.set_state(document_id, {"meta_key": last_meta_key})
        return merged

    def set_partitions(
        self,
        bucket_name: str,
        document_id: str,
        partitions: list[dict[str, Any]],
    ) -> str:
        """Persist partition payload under the partition hash key."""
        del bucket_name
        partition_key = compute_partition_key(partitions)
        self._redis.set(self._document_partition_payload_key(document_id), canonical_json(partitions))
        self.set_state(
            document_id,
            {
                "partition_key": partition_key,
                "partitions_count": len(partitions),
            },
        )
        return partition_key

    def set_document_sections(
        self,
        *,
        document_id: str,
        partition_key: str,
        sections: list[dict[str, Any]],
        chunk_sections: list[dict[str, Any]],
    ) -> None:
        """Persist section/chunk mapping payload for one document."""
        payload = {
            "document_id": document_id,
            "partition_key": partition_key,
            "sections_count": len(sections),
            "chunk_sections_count": len(chunk_sections),
            "sections": sections,
            "chunk_sections": chunk_sections,
            "updated_at": utc_now_iso(),
        }
        self._redis.set(
            self._document_sections_payload_key(document_id),
            canonical_json(payload),
        )
        self.set_state(
            document_id,
            {
                "sections_count": len(sections),
            },
        )

    @staticmethod
    def _parse_document_sections_payload(raw_value: str | None) -> dict[str, Any]:
        """Parse section/chunk mapping payload stored for one document."""
        if not isinstance(raw_value, str) or not raw_value:
            return {}
        parsed = json.loads(raw_value)
        if not isinstance(parsed, dict):
            return {}
        return {str(key): value for key, value in parsed.items()}

    def get_document_sections_payload(self, document_id: str) -> dict[str, Any]:
        """Return section/chunk mapping payload for one document."""
        return self._parse_document_sections_payload(
            self._redis.get(self._document_sections_payload_key(document_id))
        )

    def get_document_sections(self, document_id: str) -> list[dict[str, Any]]:
        """Return section records for one document."""
        payload = self.get_document_sections_payload(document_id)
        raw_sections = payload.get("sections")
        if not isinstance(raw_sections, list):
            return []
        sections: list[dict[str, Any]] = []
        for raw_section in raw_sections:
            if not isinstance(raw_section, Mapping):
                continue
            sections.append({str(key): value for key, value in raw_section.items()})
        return sections

    def get_document_section(
        self,
        document_id: str,
        section_id: str,
    ) -> dict[str, Any] | None:
        """Return one section record for a document by section ID."""
        normalized_section_id = section_id.strip()
        if not normalized_section_id:
            return None
        for section in self.get_document_sections(document_id):
            if str(section.get("section_id", "")).strip() == normalized_section_id:
                return section
        return None

    @staticmethod
    def _parse_partitions_payload(raw_value: str | None) -> list[dict[str, Any]]:
        """Parse a Redis string value as a partition payload."""
        if not isinstance(raw_value, str) or not raw_value:
            return []
        parsed = json.loads(raw_value)
        if isinstance(parsed, list):
            return [entry for entry in parsed if isinstance(entry, dict)]
        return []

    def _get_partitions_for_document(self, document_id: str) -> list[dict[str, Any]]:
        """Read partition payload for one document hash via its partition key."""
        state = self.get_state(document_id)
        partition_key = state.get("partition_key", "")
        return self.get_partitions_by_key(partition_key, document_id=document_id)

    def _iter_document_ids_for_partition_lookup(self) -> list[str]:
        """Return candidate document IDs for partition-key lookups."""
        document_ids: set[str] = set()
        key_prefix = f"{self._prefix}:document:"
        scan_iter = getattr(self._redis, "scan_iter", None)
        keys: Iterable[Any]
        if callable(scan_iter):
            keys = scan_iter(match=f"{key_prefix}*")
        else:
            keys = self._fallback_iter_keys_for_tests()

        for key in keys:
            key_text = str(key)
            if not key_text.startswith(key_prefix):
                continue
            suffix = key_text[len(key_prefix) :]
            if not suffix or ":" in suffix:
                continue
            document_ids.add(suffix)
        return sorted(document_ids)

    def get_partitions_by_key(
        self,
        partition_key: str,
        *,
        document_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read partition payload from a partition hash key."""
        if not partition_key:
            return []

        if document_id:
            state = self.get_state(document_id)
            if state.get("partition_key", "") != partition_key:
                return []
            key = self._document_partition_payload_key(document_id)
            return self._parse_partitions_payload(self._redis.get(key))

        for candidate_document_id in self._iter_document_ids_for_partition_lookup():
            state = self.get_state(candidate_document_id)
            if state.get("partition_key", "") != partition_key:
                continue
            key = self._document_partition_payload_key(candidate_document_id)
            return self._parse_partitions_payload(self._redis.get(key))
        return []

    def get_partitions(self, bucket_name: str, document_id: str) -> list[dict[str, Any]]:
        """Return partitions for one bucket/document pair."""
        del bucket_name
        return self._get_partitions_for_document(document_id)

    def set_chunks(self, bucket_name: str, document_id: str, chunks: list[dict[str, Any]]) -> None:
        """Deprecated no-op: chunks are persisted in Qdrant, not Redis."""
        del bucket_name, document_id, chunks

    def get_chunks(self, bucket_name: str, document_id: str) -> list[dict[str, Any]]:
        """Deprecated no-op: chunks are persisted in Qdrant, not Redis."""
        del bucket_name, document_id
        return []

    def remove_document(
        self,
        bucket_name: str,
        document_id: str,
        *,
        keep_partitions: bool,
    ) -> bool:
        """Delete source/meta entries for a bucket/document and keep document state."""
        object_names = self.get_document_object_names(bucket_name, document_id)
        state = self.get_state(document_id)
        partition_key = state.get("partition_key", "")

        for object_name in object_names:
            location_reference = self._location_reference(bucket_name, object_name)

            self._redis.delete(self._source_mapping_key(bucket_name, object_name))
            self._redis.delete(self._source_meta_key(bucket_name, object_name))
            self._redis.srem(self._source_bucket_key(bucket_name), location_reference)
            self._redis.srem(self._document_sources_key(document_id), location_reference)

        remaining_locations = self._redis.smembers(self._document_sources_key(document_id))
        if not remaining_locations:
            self._redis.delete(self._document_sources_key(document_id))

        self.set_state(document_id, {"meta_key": ""})

        if not keep_partitions:
            if partition_key:
                self._redis.delete(self._document_partition_payload_key(document_id))
            self.set_state(
                document_id,
                {
                    "partition_key": "",
                    "partitions_count": 0,
                },
            )
        self._redis.delete(self._document_sections_payload_key(document_id))
        self.set_state(
            document_id,
            {
                "sections_count": 0,
            },
        )

        return True

    def purge_prefix_data(self) -> int:
        """Delete every Redis key stored under the configured prefix."""
        pattern = f"{self._prefix}:*"
        deleted = 0
        scan_iter = getattr(self._redis, "scan_iter", None)
        if callable(scan_iter):
            for key in scan_iter(match=pattern):
                deleted += int(self._redis.delete(str(key)))
            return deleted

        # Test doubles may not implement SCAN iteration.
        for key in self._fallback_iter_keys_for_tests():
            deleted += int(self._redis.delete(key))
        return deleted

    def _fallback_iter_keys_for_tests(self) -> list[str]:
        """Return prefixed keys for in-memory test doubles without scan_iter."""
        prefixed_keys: set[str] = set()
        prefix = f"{self._prefix}:"
        for store_name in ("_sets", "_hashes", "_strings"):
            store = getattr(self._redis, store_name, None)
            if isinstance(store, dict):
                for key in store:
                    key_text = str(key)
                    if key_text.startswith(prefix):
                        prefixed_keys.add(key_text)
        return sorted(prefixed_keys)

    def object_requires_processing(
        self,
        bucket_name: str,
        object_name: str,
        etag: str | None,
    ) -> bool:
        """Return whether object state indicates processing is required."""
        mapping = self.get_object_mapping(bucket_name, object_name)
        if not mapping:
            return True

        previous_etag = normalize_etag(mapping.get("etag"))
        if previous_etag != normalize_etag(etag):
            return True

        document_id = mapping.get("document_id")
        if not document_id:
            return True

        state = self.get_state(document_id).get("processing_state", "")
        return state not in {"processing", "processed"}

    def list_documents(self, bucket_name: str) -> list[dict[str, Any]]:
        """Read UI-facing document records for one bucket."""
        records: list[dict[str, Any]] = []
        for document_id in self.list_document_ids(bucket_name):
            state = self.get_state(document_id)
            partition_key = state.get("partition_key", "")
            partitions = self._get_partitions_for_document(document_id)
            meta_key, metadata = self.get_document_metadata(bucket_name, document_id)

            object_names = self.get_document_object_names(bucket_name, document_id)
            file_path = object_names[0] if object_names else state.get("file_path", "")
            records.append(
                self._build_document_record(
                    document_id=document_id,
                    file_path=file_path,
                    object_names=object_names,
                    state=state,
                    partition_key=partition_key,
                    partitions=partitions,
                    meta_key=meta_key,
                    metadata=metadata,
                )
            )

        records.sort(key=lambda record: str(record.get("file_path", "")).lower())
        return records

    def _build_document_record(
        self,
        *,
        document_id: str,
        file_path: str,
        object_names: list[str],
        state: Mapping[str, str],
        partition_key: str,
        partitions: list[dict[str, Any]],
        meta_key: str,
        metadata: Mapping[str, str],
    ) -> dict[str, Any]:
        """Build one UI-facing document record."""
        normalized_authors = _normalize_author_entries(metadata.get(AUTHORS_METADATA_FIELD, ""))
        record: dict[str, Any] = {
            "id": document_id,
            "document_id": document_id,
            "partition_key": partition_key,
            "meta_key": meta_key,
            "file_path": file_path,
            "locations": object_names,
            "processing_state": state.get("processing_state", "processed"),
            "processing_progress": self._safe_int(state.get("processing_progress"), 100),
            "partitions_count": self._safe_int(state.get("partitions_count"), len(partitions)),
            "chunks_count": self._safe_int(state.get("chunks_count"), 0),
            "partitions_tree": {"partitions": partitions},
            "chunks_tree": {"chunks": []},
            "error": state.get("error", ""),
            "document_type": metadata.get("document_type", "misc") or "misc",
            "citation_key": metadata.get("citation_key", ""),
            "authors": normalized_authors,
            "bibtex_fields": {
                field_name: metadata.get(field_name, "")
                for field_name in BIBTEX_FIELDS
            },
        }
        for field_name in BIBTEX_FIELDS:
            record[field_name] = metadata.get(field_name, "")
        return record

