"""High-level ingestion service orchestration."""

from __future__ import annotations

import io
import threading
import time
from collections import defaultdict
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any, ClassVar, Protocol, cast
from urllib.parse import quote

from minio.error import S3Error

from mcp_evidencebase.ingestion_modules.chunking import build_partition_chunks
from mcp_evidencebase.ingestion_modules.crossref import (
    _crossref_enrichment_score,
    _crossref_map_item_to_metadata,
    _crossref_score_item,
    _metadata_update_changes,
    _normalize_doi_lookup_value,
    _normalize_isbn_value,
    _normalize_issn_value,
)
from mcp_evidencebase.ingestion_modules.metadata import (
    AUTHORS_METADATA_FIELD,
    CROSSREF_API_BASE_URL,
    CROSSREF_CONFIDENCE_THRESHOLD,
    CROSSREF_LIST_QUERY_MAX_REQUESTS_PER_SECOND,
    CROSSREF_MAX_RESULTS,
    CROSSREF_PUBLIC_POOL_MAX_CONCURRENT_REQUESTS,
    CROSSREF_PUBLIC_POOL_MAX_REQUESTS_PER_SECOND,
    CROSSREF_SINGLE_RECORD_MAX_REQUESTS_PER_SECOND,
    CROSSREF_TIMEOUT_SECONDS,
    METADATA_FIELDS,
    _serialize_author_entries,
    compute_document_id,
    extract_metadata_from_partitions,
    extract_pdf_title_author,
    infer_content_type,
    normalize_etag,
    utc_now_iso,
)
from mcp_evidencebase.storage_layout import (
    COLLECTION_MARKER_FILENAME,
    DEFAULT_STORAGE_BUCKET_NAME,
    build_collection_marker_object_name,
    build_storage_object_name,
    collect_storage_collection_names,
    marker_payload,
    normalize_collection_name,
    normalize_object_name,
)


class DependencyConfigurationError(RuntimeError):
    """Raised when a required external dependency is misconfigured."""


class DependencyDisabledError(RuntimeError):
    """Raised when code attempts to use an explicitly disabled dependency."""

    def __init__(self, *, component: str, feature: str, hint: str) -> None:
        self.component = component
        self.feature = feature
        super().__init__(f"{component} is disabled for {feature}. {hint}")


class DisabledRedisDocumentRepository:
    """Explicitly disabled Redis repository used for reduced-capability mode."""

    is_disabled = True

    @staticmethod
    def _raise(method_name: str) -> None:
        raise DependencyDisabledError(
            component="Redis",
            feature=f"repository method '{method_name}'",
            hint=(
                "Set REDIS_URL and enable MCP_EVIDENCEBASE_REQUIRE_REDIS=true "
                "to use Redis-backed document state, metadata, and section features."
            ),
        )

    def purge_prefix_data(self) -> int:
        """Return zero deleted keys when Redis is disabled."""
        return 0

    def __getattr__(self, name: str) -> Any:
        def _disabled(*args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            self._raise(name)

        return _disabled


class DisabledQdrantIndexer:
    """Explicitly disabled Qdrant adapter used for reduced-capability mode."""

    is_disabled = True

    @staticmethod
    def _raise(method_name: str) -> None:
        raise DependencyDisabledError(
            component="Qdrant",
            feature=f"index method '{method_name}'",
            hint=(
                "Set QDRANT_URL and enable MCP_EVIDENCEBASE_REQUIRE_QDRANT=true "
                "to use vector indexing and search features."
            ),
        )

    def ensure_bucket_collection(self, bucket_name: str) -> bool:
        del bucket_name
        return False

    def delete_bucket_collection(self, bucket_name: str) -> bool:
        del bucket_name
        return False

    def purge_prefixed_collections(self) -> int:
        return 0

    def delete_document(self, bucket_name: str, document_id: str) -> None:
        del bucket_name, document_id

    def upsert_document_chunks(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        self._raise("upsert_document_chunks")

    def rewrite_document_source_paths(self, *args: Any, **kwargs: Any) -> int:
        del args, kwargs
        self._raise("rewrite_document_source_paths")
        raise AssertionError("unreachable")

    def rewrite_collection_storage_metadata(self, *args: Any, **kwargs: Any) -> int:
        del args, kwargs
        self._raise("rewrite_collection_storage_metadata")
        raise AssertionError("unreachable")

    def search_chunks(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        del args, kwargs
        self._raise("search_chunks")
        raise AssertionError("unreachable")

    def migrate_legacy_collections_to_shared_collection(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        del args, kwargs
        self._raise("migrate_legacy_collections_to_shared_collection")
        raise AssertionError("unreachable")


class MinioBucketLike(Protocol):
    """Subset of MinIO bucket summary fields used by ingestion operations."""

    name: str


class MinioObjectResponseLike(Protocol):
    """Subset of streamed object response methods used by ingestion operations."""

    def read(self) -> bytes:
        """Read object bytes."""
        ...

    def close(self) -> None:
        """Close the underlying response object."""
        ...

    def release_conn(self) -> None:
        """Release the connection back to the pool."""
        ...


class MinioObjectStatLike(Protocol):
    """Subset of object stat fields used by ingestion operations."""

    etag: str | None
    content_type: str | None


class MinioObjectLike(Protocol):
    """Subset of object listing fields used by ingestion operations."""

    object_name: str
    etag: str | None
    is_dir: bool


class MinioClientLike(Protocol):
    """Structural MinIO client interface used by ``IngestionService``."""

    def get_object(self, bucket_name: str, object_name: str) -> MinioObjectResponseLike:
        """Return a readable object response."""
        ...

    def stat_object(self, bucket_name: str, object_name: str) -> MinioObjectStatLike:
        """Return object stat metadata."""
        ...

    def remove_object(self, bucket_name: str, object_name: str) -> None:
        """Delete one object."""
        ...

    def list_buckets(self) -> list[MinioBucketLike] | tuple[MinioBucketLike, ...]:
        """List bucket summaries."""
        ...

    def bucket_exists(self, bucket_name: str) -> bool:
        """Return whether one bucket exists."""
        ...

    def make_bucket(self, bucket_name: str, location: str | None = None) -> None:
        """Create one bucket when missing."""
        ...

    def remove_bucket(self, bucket_name: str) -> None:
        """Remove one bucket."""
        ...

    def list_objects(
        self,
        bucket_name: str,
        recursive: bool = False,
    ) -> list[MinioObjectLike] | tuple[MinioObjectLike, ...]:
        """List objects inside one bucket."""
        ...

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        *,
        data: Any,
        length: int,
        content_type: str,
    ) -> Any:
        """Upload one object."""
        ...

    def copy_object(
        self,
        bucket_name: str,
        object_name: str,
        source: Any,
    ) -> Any:
        """Copy one object from an existing bucket/object source."""
        ...


class PartitionClientLike(Protocol):
    """Structural partition client interface used by ``IngestionService``."""

    def partition_file(
        self,
        *,
        file_name: str,
        file_bytes: bytes,
        content_type: str,
    ) -> list[dict[str, Any]]:
        """Partition one file into structured elements."""
        ...


class UnstructuredPartitionClient:
    """HTTP client for Unstructured partition API."""

    def __init__(
        self,
        *,
        api_url: str,
        api_key: str | None,
        strategy: str,
        timeout_seconds: float = 120.0,
    ) -> None:
        """Initialize API client settings."""
        self._api_url = api_url
        self._api_key = api_key
        self._strategy = strategy
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def partition_file(
        self,
        *,
        file_name: str,
        file_bytes: bytes,
        content_type: str,
    ) -> list[dict[str, Any]]:
        """Send file bytes to Unstructured and return parsed partitions."""
        import httpx

        headers: dict[str, str] = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
            headers["unstructured-api-key"] = self._api_key

        payload = {"strategy": self._strategy}
        files = {"files": (file_name, file_bytes, content_type)}
        request_timeout = httpx.Timeout(
            timeout=self._timeout_seconds,
            connect=min(10.0, self._timeout_seconds),
            read=self._timeout_seconds,
            write=self._timeout_seconds,
        )
        try:
            with httpx.Client(timeout=request_timeout) as client:
                response = client.post(
                    self._api_url,
                    data=payload,
                    files=files,
                    headers=headers,
                )
        except httpx.ReadTimeout as exc:
            raise TimeoutError(
                "Unstructured API read timeout after "
                f"{self._timeout_seconds:g}s while partitioning '{file_name}'. "
                "Increase UNSTRUCTURED_TIMEOUT_SECONDS or choose a faster UNSTRUCTURED_STRATEGY."
            ) from exc
        response.raise_for_status()

        body = response.json()
        if isinstance(body, list):
            return [entry for entry in body if isinstance(entry, dict)]
        if isinstance(body, dict):
            elements = body.get("elements")
            if isinstance(elements, list):
                return [entry for entry in elements if isinstance(entry, dict)]
        raise ValueError("Unexpected Unstructured API response payload.")


class IngestionService:
    """High-level ingestion operations shared by API handlers and Celery tasks.

    The service coordinates MinIO object storage, Unstructured partitioning,
    Redis state/metadata persistence, and Qdrant vector indexing. Atomic
    stage methods are provided for partition, metadata, section mapping,
    chunking, and vector upsert workflows.
    """

    _crossref_rate_lock = threading.Lock()
    _crossref_next_allowed_global_ts = 0.0
    _crossref_next_allowed_single_ts = 0.0
    _crossref_next_allowed_list_ts = 0.0
    _stage_progress_bounds: ClassVar[dict[str, tuple[int, int]]] = {
        "partition": (0, 20),
        "meta": (20, 40),
        "section": (40, 60),
        "chunk": (60, 80),
        "upsert": (80, 100),
    }

    def __init__(
        self,
        *,
        minio_client: Any,
        repository: Any,
        partition_client: PartitionClientLike,
        qdrant_indexer: Any,
        chunk_size_chars: int,
        chunk_overlap_chars: int,
        chunk_exclude_element_types: tuple[str, ...] | None = None,
        chunking_strategy: str = "by_title",
        chunk_new_after_n_chars: int = 1500,
        chunk_combine_text_under_n_chars: int = 500,
        chunk_include_title_text: bool = False,
        chunk_image_text_mode: str = "placeholder",
        chunk_paragraph_break_strategy: str = "text",
        chunk_preserve_page_breaks: bool = True,
        storage_bucket_name: str = DEFAULT_STORAGE_BUCKET_NAME,
    ) -> None:
        """Construct ingestion service dependencies.

        Args:
            minio_client: Configured MinIO-compatible client.
            repository: Redis-backed document repository.
            partition_client: Partition client used for partition extraction.
            qdrant_indexer: Qdrant index adapter for chunk embeddings.
            chunk_size_chars: Maximum chunk size for text chunking.
            chunk_overlap_chars: Overlap size for adjacent chunks.
            chunk_exclude_element_types: Optional deny-list of raw element types.
            chunking_strategy: Chunking strategy (by_title, none).
            chunk_new_after_n_chars: Soft chunk split target size.
            chunk_combine_text_under_n_chars: Threshold for merging tiny chunks.
            chunk_include_title_text: Include title elements in chunk text.
            chunk_image_text_mode: Image text mode (placeholder, ocr, exclude).
            chunk_paragraph_break_strategy: Paragraph strategy (text, coordinates).
            chunk_preserve_page_breaks: Preserve paragraph breaks on page changes.
        """
        self._minio_client = minio_client
        self._repository = repository
        self._partition_client = partition_client
        self._qdrant_indexer = qdrant_indexer
        self._storage_bucket_name = (
            str(storage_bucket_name or DEFAULT_STORAGE_BUCKET_NAME).strip()
            or DEFAULT_STORAGE_BUCKET_NAME
        )
        self._chunk_size_chars = chunk_size_chars
        self._chunk_overlap_chars = chunk_overlap_chars
        self._chunk_exclude_element_types = (
            tuple(chunk_exclude_element_types) if chunk_exclude_element_types else None
        )
        self._chunking_strategy = str(chunking_strategy or "by_title").strip().lower() or "by_title"
        self._chunk_new_after_n_chars = max(1, int(chunk_new_after_n_chars))
        self._chunk_combine_text_under_n_chars = max(0, int(chunk_combine_text_under_n_chars))
        self._chunk_include_title_text = bool(chunk_include_title_text)
        self._chunk_image_text_mode = str(chunk_image_text_mode or "placeholder").strip().lower()
        self._chunk_paragraph_break_strategy = (
            str(chunk_paragraph_break_strategy or "text").strip().lower() or "text"
        )
        self._chunk_preserve_page_breaks = bool(chunk_preserve_page_breaks)

    def _bucket_exists(self, bucket_name: str) -> bool:
        normalized_bucket_name = str(bucket_name).strip()
        if not normalized_bucket_name:
            return False
        if hasattr(self._minio_client, "bucket_exists"):
            return bool(self._minio_client.bucket_exists(normalized_bucket_name))
        bucket_names = [str(bucket.name).strip() for bucket in self._minio_client.list_buckets()]
        return normalized_bucket_name in bucket_names

    def _storage_bucket_exists(self) -> bool:
        return self._bucket_exists(self._storage_bucket_name)

    def _ensure_storage_bucket(self) -> None:
        if self._storage_bucket_exists():
            return
        self._minio_client.make_bucket(self._storage_bucket_name)

    def _list_storage_collection_names(self) -> list[str]:
        if not self._storage_bucket_exists():
            return []
        object_names = [
            str(getattr(item, "object_name", "")).strip()
            for item in self._minio_client.list_objects(self._storage_bucket_name, recursive=True)
            if str(getattr(item, "object_name", "")).strip()
        ]
        return collect_storage_collection_names(object_names)

    def _collection_exists_in_storage(self, bucket_name: str) -> bool:
        normalized_bucket_name = str(bucket_name).strip()
        if not normalized_bucket_name or not self._storage_bucket_exists():
            return False
        prefix = f"{normalized_bucket_name}/"
        for item in self._minio_client.list_objects(self._storage_bucket_name, recursive=True):
            object_name = str(getattr(item, "object_name", "")).strip()
            if object_name.startswith(prefix):
                return True
        return False

    def _resolve_physical_location(
        self,
        *,
        bucket_name: str,
        object_name: str,
        prefer_storage: bool = False,
    ) -> tuple[str, str]:
        normalized_bucket_name = normalize_collection_name(bucket_name)
        normalized_object_name = normalize_object_name(object_name)
        if prefer_storage or self._collection_exists_in_storage(normalized_bucket_name):
            return (
                self._storage_bucket_name,
                build_storage_object_name(normalized_bucket_name, normalized_object_name),
            )
        return normalized_bucket_name, normalized_object_name

    def _create_collection_marker(self, bucket_name: str) -> None:
        normalized_bucket_name = normalize_collection_name(bucket_name)
        self._ensure_storage_bucket()
        marker_object_name = build_collection_marker_object_name(normalized_bucket_name)
        payload = marker_payload(normalized_bucket_name)
        self._minio_client.put_object(
            self._storage_bucket_name,
            marker_object_name,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )

    def _set_state(self, document_id: str, **values: Any) -> None:
        """Convenience wrapper for repository state updates."""
        self._repository.set_state(document_id, values)

    @classmethod
    def _resolve_processing_progress(cls, stage: str, stage_progress: int) -> int:
        """Project stage-local progress onto global processing progress."""
        if stage == "processed":
            return 100
        if stage == "failed":
            return 100
        bounds = cls._stage_progress_bounds.get(stage)
        if bounds is None:
            return 0
        start, end = bounds
        width = max(0, end - start)
        bounded_stage_progress = max(0, min(100, int(stage_progress)))
        return round(start + (width * bounded_stage_progress / 100))

    def _set_stage_state(
        self,
        document_id: str,
        *,
        stage: str,
        stage_progress: int,
        processing_state: str = "processing",
        **values: Any,
    ) -> None:
        """Set state with both stage-local and global progress fields."""
        bounded_stage_progress = max(0, min(100, int(stage_progress)))
        self._set_state(
            document_id,
            processing_state=processing_state,
            processing_stage=stage,
            processing_stage_progress=bounded_stage_progress,
            processing_progress=self._resolve_processing_progress(stage, bounded_stage_progress),
            **values,
        )

    def _set_stage_failed(
        self,
        document_id: str,
        *,
        stage: str,
        error: Exception,
        **values: Any,
    ) -> None:
        """Mark one stage as failed and persist the error message."""
        try:
            self._set_state(
                document_id,
                processing_state="failed",
                processing_stage=stage,
                processing_stage_progress=100,
                processing_progress=100,
                error=str(error),
                **values,
            )
        except DependencyDisabledError:
            return

    @staticmethod
    def _build_stage_payload(
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str,
        partition_key: str = "",
        meta_key: str = "",
    ) -> dict[str, str]:
        """Build canonical stage payload used across Celery task boundaries."""
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "document_id": document_id,
            "etag": etag,
            "partition_key": partition_key,
            "meta_key": meta_key,
        }

    def _load_partition_context(
        self,
        *,
        document_id: str,
    ) -> tuple[dict[str, str], str, list[dict[str, Any]]]:
        """Load state, partition key, and partition payload for one document."""
        state = self._repository.get_state(document_id)
        partition_key = str(state.get("partition_key", "")).strip()
        if not partition_key:
            raise ValueError(
                f"Document '{document_id}' has no partition_key; run partition stage first."
            )
        partitions = self._repository.get_partitions_by_key(
            partition_key,
            document_id=document_id,
        )
        if not partitions:
            raise ValueError(
                f"No partitions were found for document '{document_id}' and key '{partition_key}'."
            )
        return state, partition_key, partitions

    def _resolve_metadata_context(
        self,
        *,
        bucket_name: str,
        document_id: str,
        state: Mapping[str, str],
    ) -> tuple[str, dict[str, str]]:
        """Resolve persisted metadata key and payload for one document."""
        meta_key = str(state.get("meta_key", "")).strip()
        resolved_meta_key, metadata = self._repository.get_document_metadata(
            bucket_name,
            document_id,
        )
        if not meta_key:
            meta_key = resolved_meta_key
        return meta_key, metadata

    def _build_partition_chunks(
        self,
        partitions: list[dict[str, Any]] | list[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build chunk records with configured chunking options."""
        return build_partition_chunks(
            partitions,
            chunk_size_chars=self._chunk_size_chars,
            chunk_overlap_chars=self._chunk_overlap_chars,
            chunking_strategy=self._chunking_strategy,
            chunk_new_after_n_chars=self._chunk_new_after_n_chars,
            chunk_combine_text_under_n_chars=self._chunk_combine_text_under_n_chars,
            exclude_element_types=self._chunk_exclude_element_types,
            include_title_text=self._chunk_include_title_text,
            image_text_mode=self._chunk_image_text_mode,
            paragraph_break_strategy=self._chunk_paragraph_break_strategy,
            preserve_page_breaks=self._chunk_preserve_page_breaks,
        )

    @staticmethod
    def _safe_positive_int(value: Any) -> int | None:
        """Convert value to positive integer when possible."""
        try:
            resolved = int(value)
        except (TypeError, ValueError):
            return None
        if resolved < 0:
            return None
        return resolved

    def _build_document_section_payload(
        self,
        *,
        partition_key: str,
        chunks: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Build per-document section/chunk mapping payload from chunk metadata."""
        sections_by_id: dict[str, dict[str, Any]] = {}
        chunk_sections: list[dict[str, Any]] = []

        for chunk in chunks:
            raw_metadata = chunk.get("metadata")
            metadata = raw_metadata if isinstance(raw_metadata, Mapping) else {}

            section_id = str(metadata.get("parent_section_id", "")).strip()
            if not section_id:
                continue

            section_title = str(
                metadata.get("parent_section_title", metadata.get("section_title", ""))
            ).strip()
            section_markdown = str(
                metadata.get("parent_section_markdown", metadata.get("parent_section_text", ""))
            ).strip()
            section_index = self._safe_positive_int(metadata.get("parent_section_index"))

            chunk_index = self._safe_positive_int(chunk.get("chunk_index"))
            chunk_id = str(chunk.get("chunk_id", "")).strip()
            chunk_page_start = self._safe_positive_int(metadata.get("page_start"))
            chunk_page_end = self._safe_positive_int(metadata.get("page_end"))
            if chunk_page_start is None and chunk_page_end is not None:
                chunk_page_start = chunk_page_end
            if chunk_page_end is None and chunk_page_start is not None:
                chunk_page_end = chunk_page_start

            section = sections_by_id.get(section_id)
            if section is None:
                section = {
                    "section_id": section_id,
                    "section_index": section_index
                    if section_index is not None
                    else len(sections_by_id),
                    "section_title": section_title,
                    "section_text": section_markdown,
                    "section_markdown": section_markdown,
                    "page_start": chunk_page_start,
                    "page_end": chunk_page_end,
                    "chunk_indexes": [],
                    "chunk_ids": [],
                    "partition_key": partition_key,
                }
                sections_by_id[section_id] = section
            else:
                if not str(section.get("section_title", "")).strip() and section_title:
                    section["section_title"] = section_title
                if not str(section.get("section_text", "")).strip() and section_markdown:
                    section["section_text"] = section_markdown
                    section["section_markdown"] = section_markdown
                current_page_start = self._safe_positive_int(section.get("page_start"))
                current_page_end = self._safe_positive_int(section.get("page_end"))
                if chunk_page_start is not None:
                    section["page_start"] = (
                        chunk_page_start
                        if current_page_start is None
                        else min(current_page_start, chunk_page_start)
                    )
                if chunk_page_end is not None:
                    section["page_end"] = (
                        chunk_page_end
                        if current_page_end is None
                        else max(current_page_end, chunk_page_end)
                    )

            if chunk_index is not None and chunk_index not in section["chunk_indexes"]:
                section["chunk_indexes"].append(chunk_index)
            if chunk_id and chunk_id not in section["chunk_ids"]:
                section["chunk_ids"].append(chunk_id)

            chunk_sections.append(
                {
                    "chunk_index": chunk_index,
                    "chunk_id": chunk_id,
                    "section_id": section_id,
                }
            )

        sections = list(sections_by_id.values())
        sections.sort(key=lambda item: self._safe_positive_int(item.get("section_index")) or 0)
        for section in sections:
            section["chunk_indexes"] = sorted(
                index for index in section.get("chunk_indexes", []) if isinstance(index, int)
            )
        chunk_sections.sort(
            key=lambda item: (
                self._safe_positive_int(item.get("chunk_index")) or 0,
                str(item.get("chunk_id", "")),
            )
        )
        return sections, chunk_sections

    def _hydrate_search_results_with_sections(
        self,
        *,
        results: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Attach section text/title/index from Redis section mappings."""
        if not results:
            return results

        sections_cache: dict[str, dict[str, dict[str, Any]]] = {}

        def section_map_for_document(document_id: str) -> dict[str, dict[str, Any]]:
            cached = sections_cache.get(document_id)
            if cached is not None:
                return cached
            section_map: dict[str, dict[str, Any]] = {}
            for section in self._repository.get_document_sections(document_id):
                section_id = str(section.get("section_id", "")).strip()
                if section_id:
                    section_map[section_id] = section
            sections_cache[document_id] = section_map
            return section_map

        for result in results:
            document_id = str(result.get("document_id", "")).strip()
            section_id = (
                str(result.get("section_id", "")).strip()
                or str(result.get("parent_section_id", "")).strip()
            )
            if not document_id or not section_id:
                continue

            section = section_map_for_document(document_id).get(section_id)
            if section is None:
                continue

            section_title = str(section.get("section_title", "")).strip()
            section_text = str(
                section.get("section_markdown", section.get("section_text", ""))
            ).strip()
            section_index = self._safe_positive_int(section.get("section_index"))

            result["section_id"] = section_id
            result["parent_section_id"] = section_id
            result["parent_section_index"] = section_index
            result["parent_section_title"] = section_title
            result["parent_section_text"] = section_text
            result["parent_section_markdown"] = section_text
            if section_title:
                result["section_title"] = section_title

        return results

    def _hydrate_search_results_with_metadata(
        self,
        *,
        bucket_name: str,
        results: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Attach title/author/year/citation metadata from Redis by document ID."""
        if not results:
            return results

        metadata_cache: dict[str, dict[str, str]] = {}
        for result in results:
            document_id = str(result.get("document_id", "")).strip()
            if not document_id:
                continue

            metadata = metadata_cache.get(document_id)
            if metadata is None:
                _, resolved_metadata = self._repository.get_document_metadata(
                    bucket_name,
                    document_id,
                )
                metadata = {str(key): str(value) for key, value in resolved_metadata.items()}
                metadata_cache[document_id] = metadata

            for field_name in ("title", "author", "citation_key", "year"):
                redis_value = str(metadata.get(field_name, "")).strip()
                if redis_value:
                    result[field_name] = redis_value
                    continue
                result[field_name] = str(result.get(field_name, "")).strip()

        return results

    def _read_object_bytes(self, bucket_name: str, object_name: str) -> bytes:
        """Read and return object bytes from MinIO."""
        physical_bucket_name, physical_object_name = self._resolve_physical_location(
            bucket_name=bucket_name,
            object_name=object_name,
        )
        object_response = self._minio_client.get_object(physical_bucket_name, physical_object_name)
        try:
            return cast(bytes, object_response.read())
        finally:
            object_response.close()
            object_response.release_conn()

    def _resolve_document_identity(
        self,
        *,
        bucket_name: str,
        object_name: str,
        etag: str | None,
        file_bytes: bytes,
    ) -> tuple[str, str]:
        """Resolve and persist deterministic document identity and source mapping."""
        physical_bucket_name, physical_object_name = self._resolve_physical_location(
            bucket_name=bucket_name,
            object_name=object_name,
        )
        stat_info = self._minio_client.stat_object(physical_bucket_name, physical_object_name)
        resolved_etag = normalize_etag(etag) or normalize_etag(getattr(stat_info, "etag", ""))
        document_id = compute_document_id(file_bytes)

        self._repository.add_document(bucket_name, document_id)
        self._repository.mark_object(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=document_id,
            etag=resolved_etag,
            storage_bucket_name=physical_bucket_name,
            storage_object_name=physical_object_name,
        )
        return document_id, resolved_etag

    def _remove_object_if_exists(self, bucket_name: str, object_name: str) -> None:
        """Delete an object and ignore not-found responses."""
        try:
            physical_bucket_name, physical_object_name = self._resolve_physical_location(
                bucket_name=bucket_name,
                object_name=object_name,
            )
            self._minio_client.remove_object(physical_bucket_name, physical_object_name)
        except S3Error as exc:
            if exc.code not in {"NoSuchKey", "NoSuchObject"}:
                raise

    def relocate_prefix_to_bucket_root(
        self,
        *,
        bucket_name: str,
        source_prefix: str = "articles/",
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Relocate one bucket prefix to the bucket root without reindexing."""
        from minio.commonconfig import CopySource

        normalized_bucket_name = bucket_name.strip()
        normalized_source_prefix = source_prefix.strip().lstrip("/")
        if not normalized_bucket_name:
            raise ValueError("bucket_name must not be empty.")
        if not normalized_source_prefix:
            raise ValueError("source_prefix must not be empty.")
        if not normalized_source_prefix.endswith("/"):
            normalized_source_prefix = f"{normalized_source_prefix}/"

        bucket_objects = self.list_bucket_objects(normalized_bucket_name)
        object_etags = {object_name: etag for object_name, etag in bucket_objects}
        source_objects = sorted(
            object_name
            for object_name, _etag in bucket_objects
            if object_name.startswith(normalized_source_prefix)
        )
        target_name_sources: dict[str, list[str]] = defaultdict(list)
        for object_name in source_objects:
            target_name_sources[PurePosixPath(object_name).name].append(object_name)

        report_items: list[dict[str, Any]] = []
        report: dict[str, Any] = {
            "bucket_name": normalized_bucket_name,
            "source_prefix": normalized_source_prefix,
            "dry_run": bool(dry_run),
            "candidates_seen": len(source_objects),
            "relocated": 0,
            "would_relocate": 0,
            "skipped_missing_mapping": 0,
            "skipped_missing_document_id": 0,
            "skipped_existing_target": 0,
            "skipped_existing_target_mapping": 0,
            "skipped_multiple_source_locations": 0,
            "skipped_duplicate_target_name": 0,
            "failed": 0,
            "items": report_items,
        }

        for old_object_name in source_objects:
            new_object_name = PurePosixPath(old_object_name).name
            item_report: dict[str, Any] = {
                "old_object_name": old_object_name,
                "new_object_name": new_object_name,
            }
            report_items.append(item_report)

            old_mapping = self._repository.get_object_mapping(
                normalized_bucket_name,
                old_object_name,
            )
            if not old_mapping:
                item_report["status"] = "skipped_missing_mapping"
                report["skipped_missing_mapping"] += 1
                continue

            document_id = str(old_mapping.get("document_id", "")).strip()
            item_report["document_id"] = document_id
            if not document_id:
                item_report["status"] = "skipped_missing_document_id"
                report["skipped_missing_document_id"] += 1
                continue

            prefixed_document_locations = [
                location
                for location in self._repository.get_document_locations(
                    document_id,
                    normalized_bucket_name,
                )
                if location.startswith(f"{normalized_bucket_name}/{normalized_source_prefix}")
            ]
            if len(prefixed_document_locations) > 1:
                item_report["status"] = "skipped_multiple_source_locations"
                item_report["locations"] = prefixed_document_locations
                report["skipped_multiple_source_locations"] += 1
                continue

            if len(target_name_sources.get(new_object_name, [])) > 1:
                item_report["status"] = "skipped_duplicate_target_name"
                item_report["conflicting_sources"] = list(target_name_sources[new_object_name])
                report["skipped_duplicate_target_name"] += 1
                continue

            if new_object_name in object_etags:
                item_report["status"] = "skipped_existing_target"
                report["skipped_existing_target"] += 1
                continue

            if self._repository.get_object_mapping(normalized_bucket_name, new_object_name):
                item_report["status"] = "skipped_existing_target_mapping"
                report["skipped_existing_target_mapping"] += 1
                continue

            if dry_run:
                item_report["status"] = "dry_run"
                report["would_relocate"] += 1
                continue

            old_etag = normalize_etag(old_mapping.get("etag", "")) or object_etags.get(
                old_object_name,
                "",
            )
            copied_object = False
            redis_relocated = False
            qdrant_rewritten = False
            destination_bucket_name = normalized_bucket_name
            destination_object_name = new_object_name
            source_bucket_name = normalized_bucket_name
            source_object_name = old_object_name
            if self._collection_exists_in_storage(normalized_bucket_name):
                destination_bucket_name = self._storage_bucket_name
                destination_object_name = build_storage_object_name(
                    normalized_bucket_name,
                    new_object_name,
                )
                source_bucket_name = self._storage_bucket_name
                source_object_name = build_storage_object_name(
                    normalized_bucket_name,
                    old_object_name,
                )
            try:
                self._minio_client.copy_object(
                    destination_bucket_name,
                    destination_object_name,
                    CopySource(source_bucket_name, source_object_name),
                )
                copied_object = True
                stat_info = self._minio_client.stat_object(
                    destination_bucket_name,
                    destination_object_name,
                )
                new_etag = normalize_etag(getattr(stat_info, "etag", ""))
                self._repository.relocate_source_location(
                    bucket_name=normalized_bucket_name,
                    document_id=document_id,
                    old_object_name=old_object_name,
                    new_object_name=new_object_name,
                    etag=new_etag,
                    storage_bucket_name=destination_bucket_name,
                    storage_object_name=destination_object_name,
                )
                redis_relocated = True
                updated_points = cast(
                    int,
                    self._qdrant_indexer.rewrite_document_source_paths(
                        bucket_name=normalized_bucket_name,
                        document_id=document_id,
                        old_object_name=old_object_name,
                        new_object_name=new_object_name,
                        storage_bucket_name=(
                            destination_bucket_name
                            if destination_bucket_name != normalized_bucket_name
                            else None
                        ),
                    ),
                )
                qdrant_rewritten = True
                self._remove_object_if_exists(normalized_bucket_name, old_object_name)
                object_etags.pop(old_object_name, None)
                object_etags[new_object_name] = new_etag
                item_report["status"] = "relocated"
                item_report["updated_points"] = updated_points
                report["relocated"] += 1
            except Exception as exc:
                if qdrant_rewritten:
                    try:
                        self._qdrant_indexer.rewrite_document_source_paths(
                            bucket_name=normalized_bucket_name,
                            document_id=document_id,
                            old_object_name=new_object_name,
                            new_object_name=old_object_name,
                            storage_bucket_name=(
                                destination_bucket_name
                                if destination_bucket_name != normalized_bucket_name
                                else None
                            ),
                        )
                    except Exception:
                        pass
                if redis_relocated:
                    try:
                        self._repository.relocate_source_location(
                            bucket_name=normalized_bucket_name,
                            document_id=document_id,
                            old_object_name=new_object_name,
                            new_object_name=old_object_name,
                            etag=old_etag,
                            storage_bucket_name=source_bucket_name,
                            storage_object_name=source_object_name,
                        )
                    except Exception:
                        pass
                if copied_object:
                    try:
                        self._remove_object_if_exists(normalized_bucket_name, new_object_name)
                    except Exception:
                        pass
                item_report["status"] = "failed"
                item_report["error"] = str(exc)
                report["failed"] += 1

        return report

    def list_documents(self, bucket_name: str) -> list[dict[str, Any]]:
        """Return UI-facing document records for a bucket.

        Args:
            bucket_name: Bucket to query.

        Returns:
            List of document records for the bucket.
        """
        records = cast(list[dict[str, Any]], self._repository.list_documents(bucket_name))
        for record in records:
            processing_state = str(record.get("processing_state", "")).strip().lower()
            if processing_state != "processed":
                continue

            raw_partitions_tree = record.get("partitions_tree")
            if not isinstance(raw_partitions_tree, Mapping):
                continue
            raw_partitions = raw_partitions_tree.get("partitions")
            if not isinstance(raw_partitions, list):
                continue

            partitions = [
                partition for partition in raw_partitions if isinstance(partition, Mapping)
            ]
            if not partitions:
                record["chunks_tree"] = {"chunks": []}
                continue

            normalized_partitions = [
                {str(key): value for key, value in partition.items()} for partition in partitions
            ]
            chunks = self._build_partition_chunks(normalized_partitions)
            record["chunks_tree"] = {"chunks": chunks}
        return records

    def list_buckets(self) -> list[str]:
        """Return sorted logical collection names.

        Returns:
            Sorted list of logical collection names.
        """
        bucket_names = set(self._list_storage_collection_names())
        for bucket in self._minio_client.list_buckets():
            current_bucket_name = str(bucket.name).strip()
            if not current_bucket_name or current_bucket_name == self._storage_bucket_name:
                continue
            bucket_names.add(current_bucket_name)
        return sorted(bucket_names)

    def ensure_bucket_qdrant_collection(self, bucket_name: str) -> bool:
        """Ensure the bucket-level Qdrant collection exists.

        Args:
            bucket_name: Bucket whose collection should exist.

        Returns:
            ``True`` when a collection is created, otherwise ``False``.
        """
        return cast(bool, self._qdrant_indexer.ensure_bucket_collection(bucket_name))

    def delete_bucket_qdrant_collection(self, bucket_name: str) -> bool:
        """Delete the bucket's Qdrant points from the shared collection when present.

        Args:
            bucket_name: Bucket whose collection should be removed.

        Returns:
            ``True`` when the shared collection exists and a delete is issued.
        """
        return cast(bool, self._qdrant_indexer.delete_bucket_collection(bucket_name))

    def migrate_legacy_qdrant_collections(self, *, dry_run: bool = True) -> dict[str, Any]:
        """Backfill legacy per-bucket Qdrant collections into the shared collection."""
        return cast(
            dict[str, Any],
            self._qdrant_indexer.migrate_legacy_collections_to_shared_collection(
                dry_run=dry_run
            ),
        )

    def list_bucket_objects(self, bucket_name: str) -> list[tuple[str, str]]:
        """Return non-directory object names and ETags for one logical collection.

        Args:
            bucket_name: Bucket to scan.

        Returns:
            List of ``(object_name, etag)`` tuples.
        """
        normalized_bucket_name = normalize_collection_name(bucket_name)
        objects: list[tuple[str, str]] = []
        if self._collection_exists_in_storage(normalized_bucket_name):
            prefix = f"{normalized_bucket_name}/"
            object_iter = self._minio_client.list_objects(self._storage_bucket_name, recursive=True)
            for item in object_iter:
                storage_object_name = str(getattr(item, "object_name", "")).strip()
                if not storage_object_name.startswith(prefix):
                    continue
                if bool(getattr(item, "is_dir", False)):
                    continue
                object_name = storage_object_name[len(prefix) :]
                if not object_name or object_name == COLLECTION_MARKER_FILENAME:
                    continue
                etag = normalize_etag(getattr(item, "etag", ""))
                objects.append((object_name, etag))
            return objects

        object_iter = self._minio_client.list_objects(normalized_bucket_name, recursive=True)
        for item in object_iter:
            object_name = str(getattr(item, "object_name", "")).strip()
            if not object_name:
                continue
            if bool(getattr(item, "is_dir", False)):
                continue
            etag = normalize_etag(getattr(item, "etag", ""))
            objects.append((object_name, etag))
        return objects

    def object_requires_processing(
        self,
        *,
        bucket_name: str,
        object_name: str,
        etag: str | None,
    ) -> bool:
        """Return whether an object should be processed again.

        Args:
            bucket_name: Bucket containing the object.
            object_name: Object path within the bucket.
            etag: Optional current ETag from object listing/stat.

        Returns:
            ``True`` when processing is required.
        """
        return cast(
            bool,
            self._repository.object_requires_processing(bucket_name, object_name, etag),
        )

    def upload_document(
        self,
        *,
        bucket_name: str,
        object_name: str,
        payload: bytes,
        content_type: str | None = None,
    ) -> str:
        """Upload file bytes to MinIO and initialize queued processing state.

        Args:
            bucket_name: Destination bucket.
            object_name: Destination object name.
            payload: File bytes to upload.
            content_type: Optional MIME type override.

        Returns:
            Normalized object name that was uploaded.

        Raises:
            ValueError: If payload is empty or object name is blank.
        """
        if not payload:
            raise ValueError("uploaded file is empty.")
        normalized_bucket_name = normalize_collection_name(bucket_name)
        normalized_object_name = normalize_object_name(object_name)

        prefer_storage = self._collection_exists_in_storage(normalized_bucket_name) or not (
            self._bucket_exists(normalized_bucket_name)
        )
        if prefer_storage:
            self._ensure_storage_bucket()
        physical_bucket_name, physical_object_name = self._resolve_physical_location(
            bucket_name=normalized_bucket_name,
            object_name=normalized_object_name,
            prefer_storage=prefer_storage,
        )

        guessed_content_type = content_type or infer_content_type(normalized_object_name)
        self._minio_client.put_object(
            physical_bucket_name,
            physical_object_name,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type=guessed_content_type,
        )
        stat_info = self._minio_client.stat_object(physical_bucket_name, physical_object_name)
        etag = normalize_etag(getattr(stat_info, "etag", ""))

        document_id = compute_document_id(payload)
        self._repository.add_document(normalized_bucket_name, document_id)
        self._repository.mark_object(
            bucket_name=normalized_bucket_name,
            object_name=normalized_object_name,
            document_id=document_id,
            etag=etag,
            storage_bucket_name=physical_bucket_name,
            storage_object_name=physical_object_name,
        )
        meta_key = self._repository.set_default_metadata_if_missing(
            normalized_bucket_name,
            document_id,
            file_path=normalized_object_name,
        )
        self._set_state(
            document_id,
            file_path=normalized_object_name,
            processing_state="processing",
            processing_progress=0,
            processing_stage="queued",
            processing_stage_progress=0,
            partition_key="",
            meta_key=meta_key,
            partitions_count=0,
            chunks_count=0,
            error="",
            created_at=utc_now_iso(),
        )
        return normalized_object_name

    def process_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        etag: str | None = None,
    ) -> str:
        """Process one MinIO object through partition and chunk stages.

        This compatibility wrapper runs ``partition_object()`` then
        ``chunk_object()`` in sequence.

        Args:
            bucket_name: Source bucket.
            object_name: Source object path.
            etag: Optional object ETag from scanner context.

        Returns:
            Canonical document ID (SHA-256 hash of object bytes).

        Raises:
            ValueError: If the object payload is empty.
            Exception: Any downstream partitioning or indexing failure.
        """
        stage = self.partition_object(
            bucket_name=bucket_name,
            object_name=object_name,
            etag=etag,
        )
        self.chunk_object(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=stage["document_id"],
        )
        return stage["document_id"]

    def partition_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        """Run only Unstructured partition extraction for one object."""
        file_bytes = self._read_object_bytes(bucket_name, object_name)
        if not file_bytes:
            raise ValueError(f"Object '{object_name}' in bucket '{bucket_name}' is empty.")

        document_id, resolved_etag = self._resolve_document_identity(
            bucket_name=bucket_name,
            object_name=object_name,
            etag=etag,
            file_bytes=file_bytes,
        )
        self._set_stage_state(
            document_id,
            stage="partition",
            stage_progress=0,
            file_path=object_name,
            error="",
        )
        try:
            partitions = self._partition_client.partition_file(
                file_name=PurePosixPath(object_name).name,
                file_bytes=file_bytes,
                content_type=infer_content_type(object_name),
            )
            partition_key = self._repository.set_partitions(bucket_name, document_id, partitions)
            self._set_stage_state(
                document_id,
                stage="partition",
                stage_progress=100,
                partition_key=partition_key,
                partitions_count=len(partitions),
                error="",
            )
            return self._build_stage_payload(
                bucket_name=bucket_name,
                object_name=object_name,
                document_id=document_id,
                etag=resolved_etag,
                partition_key=partition_key,
            )
        except Exception as exc:
            self._set_stage_failed(document_id, stage="partition", error=exc)
            raise

    def meta_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        """Run only metadata extraction/persistence for a partitioned document."""
        try:
            _, partition_key, partitions = self._load_partition_context(document_id=document_id)
            self._set_stage_state(
                document_id,
                stage="meta",
                stage_progress=0,
                partition_key=partition_key,
                partitions_count=len(partitions),
                error="",
            )

            file_bytes = self._read_object_bytes(bucket_name, object_name)
            if not file_bytes:
                raise ValueError(f"Object '{object_name}' in bucket '{bucket_name}' is empty.")

            extracted_metadata = extract_metadata_from_partitions(
                partitions=partitions,
                file_path=object_name,
                document_id=document_id,
                pdf_metadata=extract_pdf_title_author(file_bytes),
            )
            meta_key = self._repository.set_metadata_for_location(
                bucket_name=bucket_name,
                object_name=object_name,
                document_id=document_id,
                metadata=extracted_metadata,
            )
            self._set_stage_state(
                document_id,
                stage="meta",
                stage_progress=100,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                error="",
            )
            return self._build_stage_payload(
                bucket_name=bucket_name,
                object_name=object_name,
                document_id=document_id,
                etag=str(etag or "").strip(),
                partition_key=partition_key,
                meta_key=meta_key,
            )
        except Exception as exc:
            self._set_stage_failed(document_id, stage="meta", error=exc)
            raise

    def section_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        """Build and persist section/chunk-section mapping for one document."""
        try:
            state, partition_key, partitions = self._load_partition_context(document_id=document_id)
            meta_key, _ = self._resolve_metadata_context(
                bucket_name=bucket_name,
                document_id=document_id,
                state=state,
            )
            self._set_stage_state(
                document_id,
                stage="section",
                stage_progress=0,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                error="",
            )

            chunks = self._build_partition_chunks(partitions)
            sections, chunk_sections = self._build_document_section_payload(
                partition_key=partition_key,
                chunks=chunks,
            )
            self._repository.set_document_sections(
                document_id=document_id,
                partition_key=partition_key,
                sections=sections,
                chunk_sections=chunk_sections,
            )
            self._set_stage_state(
                document_id,
                stage="section",
                stage_progress=100,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                chunks_count=len(chunks),
                sections_count=len(sections),
                error="",
            )
            return self._build_stage_payload(
                bucket_name=bucket_name,
                object_name=object_name,
                document_id=document_id,
                etag=str(etag or "").strip(),
                partition_key=partition_key,
                meta_key=meta_key,
            )
        except Exception as exc:
            self._set_stage_failed(document_id, stage="section", error=exc)
            raise

    def chunk_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        """Build chunk payload for one document and persist chunk-level progress."""
        try:
            state, partition_key, partitions = self._load_partition_context(document_id=document_id)
            meta_key, _ = self._resolve_metadata_context(
                bucket_name=bucket_name,
                document_id=document_id,
                state=state,
            )
            self._set_stage_state(
                document_id,
                stage="chunk",
                stage_progress=0,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                error="",
            )

            chunks = self._build_partition_chunks(partitions)
            existing_sections_count = self._safe_positive_int(state.get("sections_count"))
            if existing_sections_count is None:
                existing_sections_count = len(self._repository.get_document_sections(document_id))
            self._set_stage_state(
                document_id,
                stage="chunk",
                stage_progress=100,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                chunks_count=len(chunks),
                sections_count=existing_sections_count,
                error="",
            )
            return self._build_stage_payload(
                bucket_name=bucket_name,
                object_name=object_name,
                document_id=document_id,
                etag=str(etag or "").strip(),
                partition_key=partition_key,
                meta_key=meta_key,
            )
        except Exception as exc:
            self._set_stage_failed(document_id, stage="chunk", error=exc)
            raise

    def upsert_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        """Embed and upsert chunks for one document into Qdrant."""
        try:
            state, partition_key, partitions = self._load_partition_context(document_id=document_id)
            meta_key, metadata = self._resolve_metadata_context(
                bucket_name=bucket_name,
                document_id=document_id,
                state=state,
            )
            self._set_stage_state(
                document_id,
                stage="upsert",
                stage_progress=0,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                error="",
            )

            document_year = str(metadata.get("year", "")).strip()
            chunks = self._build_partition_chunks(partitions)
            self._qdrant_indexer.upsert_document_chunks(
                bucket_name=bucket_name,
                document_id=document_id,
                file_path=object_name,
                chunks=chunks,
                partition_key=partition_key,
                meta_key=meta_key,
                document_year=document_year,
                storage_bucket_name=(
                    self._storage_bucket_name
                    if self._collection_exists_in_storage(bucket_name)
                    else None
                ),
            )
            sections_count = self._safe_positive_int(state.get("sections_count"))
            if sections_count is None:
                sections_count = len(self._repository.get_document_sections(document_id))
            self._set_stage_state(
                document_id,
                stage="processed",
                stage_progress=100,
                processing_state="processed",
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                chunks_count=len(chunks),
                sections_count=sections_count,
                error="",
            )
            return self._build_stage_payload(
                bucket_name=bucket_name,
                object_name=object_name,
                document_id=document_id,
                etag=str(etag or "").strip(),
                partition_key=partition_key,
                meta_key=meta_key,
            )
        except Exception as exc:
            self._set_stage_failed(document_id, stage="upsert", error=exc)
            raise

    def partition_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        """Run partitioning and metadata extraction for one MinIO object.

        Args:
            bucket_name: Source bucket.
            object_name: Source object path.
            etag: Optional object ETag from scanner context.

        Returns:
            Stage payload containing bucket/object IDs and partition metadata keys.

        Raises:
            ValueError: If the object payload is empty.
            Exception: Any downstream partitioning failure.
        """
        partition_payload = self.partition_stage_object(
            bucket_name=bucket_name,
            object_name=object_name,
            etag=etag,
        )
        meta_payload = self.meta_stage_object(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=partition_payload["document_id"],
            etag=partition_payload.get("etag", ""),
        )
        return {
            "bucket_name": partition_payload["bucket_name"],
            "object_name": partition_payload["object_name"],
            "document_id": partition_payload["document_id"],
            "etag": partition_payload.get("etag", ""),
            "partition_key": partition_payload.get("partition_key", ""),
            "meta_key": meta_payload.get("meta_key", ""),
        }

    def chunk_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
    ) -> dict[str, str]:
        """Run section, chunk, and upsert stages for a partitioned document.

        Args:
            bucket_name: Source bucket.
            object_name: Source object path.
            document_id: Canonical document hash to chunk/index.

        Returns:
            Stage payload containing bucket/object IDs and persisted key references.

        Raises:
            ValueError: If no partition payload is available for the document.
            Exception: Any downstream section/chunk/upsert failure.
        """
        section_payload = self.section_stage_object(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=document_id,
        )
        chunk_payload = self.chunk_stage_object(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=document_id,
            etag=section_payload.get("etag", ""),
        )
        upsert_payload = self.upsert_stage_object(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=document_id,
            etag=chunk_payload.get("etag", ""),
        )
        return {
            "bucket_name": upsert_payload["bucket_name"],
            "object_name": upsert_payload["object_name"],
            "document_id": upsert_payload["document_id"],
            "partition_key": upsert_payload.get("partition_key", ""),
            "meta_key": upsert_payload.get("meta_key", ""),
        }

    def get_document_section(
        self,
        *,
        bucket_name: str,
        document_id: str,
        section_id: str,
    ) -> dict[str, Any]:
        """Return one section mapping record for a document."""
        normalized_bucket_name = bucket_name.strip()
        normalized_document_id = document_id.strip()
        normalized_section_id = section_id.strip()
        if not normalized_bucket_name:
            raise ValueError("bucket_name must not be empty.")
        if not normalized_document_id:
            raise ValueError("document_id must not be empty.")
        if not normalized_section_id:
            raise ValueError("section_id must not be empty.")

        if not self._repository.get_document_object_names(
            normalized_bucket_name,
            normalized_document_id,
        ):
            raise ValueError(
                "Document "
                f"'{normalized_document_id}' was not found in bucket "
                f"'{normalized_bucket_name}'."
            )

        section = self._repository.get_document_section(
            normalized_document_id, normalized_section_id
        )
        if section is None:
            raise ValueError(
                f"Section '{normalized_section_id}' was not found for document "
                f"'{normalized_document_id}'."
            )
        return cast(dict[str, Any], section)

    def list_document_sections(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> list[dict[str, Any]]:
        """Return all section mapping records for a document."""
        normalized_bucket_name = bucket_name.strip()
        normalized_document_id = document_id.strip()
        if not normalized_bucket_name:
            raise ValueError("bucket_name must not be empty.")
        if not normalized_document_id:
            raise ValueError("document_id must not be empty.")

        if not self._repository.get_document_object_names(
            normalized_bucket_name,
            normalized_document_id,
        ):
            raise ValueError(
                "Document "
                f"'{normalized_document_id}' was not found in bucket "
                f"'{normalized_bucket_name}'."
            )

        sections = cast(
            list[dict[str, Any]],
            self._repository.get_document_sections(normalized_document_id),
        )
        sections.sort(
            key=lambda section: (
                self._safe_positive_int(section.get("section_index")) or 0,
                str(section.get("section_id", "")),
            )
        )
        return sections

    def rebuild_document_section_mapping(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> dict[str, Any]:
        """Rebuild and persist section mapping for one document from stored partitions."""
        normalized_bucket_name = bucket_name.strip()
        normalized_document_id = document_id.strip()
        if not normalized_bucket_name:
            raise ValueError("bucket_name must not be empty.")
        if not normalized_document_id:
            raise ValueError("document_id must not be empty.")

        if not self._repository.get_document_object_names(
            normalized_bucket_name,
            normalized_document_id,
        ):
            raise ValueError(
                "Document "
                f"'{normalized_document_id}' was not found in bucket "
                f"'{normalized_bucket_name}'."
            )

        state = self._repository.get_state(normalized_document_id)
        partition_key = state.get("partition_key", "")
        if not partition_key:
            raise ValueError(
                f"Document '{normalized_document_id}' has no partition_key; "
                "run partition stage first."
            )

        partitions = self._repository.get_partitions_by_key(
            partition_key,
            document_id=normalized_document_id,
        )
        if not partitions:
            raise ValueError(
                "No partitions were found for document "
                f"'{normalized_document_id}' and key '{partition_key}'."
            )

        chunks = self._build_partition_chunks(partitions)
        sections, chunk_sections = self._build_document_section_payload(
            partition_key=partition_key,
            chunks=chunks,
        )
        self._repository.set_document_sections(
            document_id=normalized_document_id,
            partition_key=partition_key,
            sections=sections,
            chunk_sections=chunk_sections,
        )
        self._set_state(
            normalized_document_id,
            partition_key=partition_key,
            partitions_count=len(partitions),
            chunks_count=len(chunks),
            sections_count=len(sections),
            error="",
        )
        return {
            "bucket_name": normalized_bucket_name,
            "document_id": normalized_document_id,
            "partition_key": partition_key,
            "sections_count": len(sections),
            "chunk_sections_count": len(chunk_sections),
        }

    def rebuild_bucket_section_mappings(self, *, bucket_name: str) -> dict[str, Any]:
        """Rebuild section mappings for all documents in one bucket."""
        normalized_bucket_name = bucket_name.strip()
        if not normalized_bucket_name:
            raise ValueError("bucket_name must not be empty.")

        document_ids = self._repository.list_document_ids(normalized_bucket_name)
        rebuilt = 0
        failed = 0
        errors: list[str] = []

        for document_id in document_ids:
            try:
                self.rebuild_document_section_mapping(
                    bucket_name=normalized_bucket_name,
                    document_id=document_id,
                )
                rebuilt += 1
            except Exception as exc:
                failed += 1
                errors.append(f"{document_id}: {exc}")

        return {
            "bucket_name": normalized_bucket_name,
            "documents_seen": len(document_ids),
            "rebuilt": rebuilt,
            "failed": failed,
            "errors": errors,
        }

    def build_document_reindex_payload(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> dict[str, str]:
        """Build a queueable upsert payload for one existing document."""
        normalized_bucket_name = bucket_name.strip()
        normalized_document_id = document_id.strip()
        if not normalized_bucket_name:
            raise ValueError("bucket_name must not be empty.")
        if not normalized_document_id:
            raise ValueError("document_id must not be empty.")

        state, partition_key, _ = self._load_partition_context(document_id=normalized_document_id)
        meta_key, _ = self._resolve_metadata_context(
            bucket_name=normalized_bucket_name,
            document_id=normalized_document_id,
            state=state,
        )

        object_name = str(state.get("file_path", "")).strip()
        if not object_name:
            object_names = self._repository.get_document_object_names(
                normalized_bucket_name,
                normalized_document_id,
            )
            if object_names:
                object_name = object_names[0]
        if not object_name:
            raise ValueError(
                f"Document '{normalized_document_id}' has no stored object path in bucket "
                f"'{normalized_bucket_name}'."
            )

        return self._build_stage_payload(
            bucket_name=normalized_bucket_name,
            object_name=object_name,
            document_id=normalized_document_id,
            etag=str(state.get("etag", "")).strip(),
            partition_key=partition_key,
            meta_key=meta_key,
        )

    def update_metadata(
        self,
        *,
        bucket_name: str,
        document_id: str,
        metadata: Mapping[str, Any],
        refresh_vectors: bool = True,
    ) -> dict[str, str]:
        """Upsert BibTeX-style metadata for a document.

        Args:
            bucket_name: Bucket containing document sources.
            document_id: Canonical document hash.
            metadata: Partial metadata update payload.

        Returns:
            Normalized metadata after merge.
        """
        normalized: dict[str, str] = {}
        for key, value in metadata.items():
            field_name = key.strip().lower()
            if field_name in METADATA_FIELDS:
                if field_name == AUTHORS_METADATA_FIELD:
                    normalized[field_name] = _serialize_author_entries(value)
                else:
                    normalized[field_name] = str(value).strip()

        if "document_type" in normalized:
            normalized["document_type"] = normalized["document_type"] or "misc"

        _, existing_metadata = self._repository.get_document_metadata(bucket_name, document_id)

        merged = cast(
            dict[str, str],
            self._repository.update_document_metadata(
                bucket_name=bucket_name,
                document_id=document_id,
                metadata=normalized,
            ),
        )
        should_refresh_vectors = (
            refresh_vectors
            and not bool(getattr(self._qdrant_indexer, "is_disabled", False))
            and self._indexed_metadata_has_changed(
                previous=existing_metadata,
                current=merged,
            )
        )
        if should_refresh_vectors:
            self._refresh_document_chunk_vectors(
                bucket_name=bucket_name,
                document_id=document_id,
            )
        else:
            self._normalize_state_after_metadata_only_update(document_id=document_id)
        return merged

    @staticmethod
    def _indexed_metadata_has_changed(
        *,
        previous: Mapping[str, Any],
        current: Mapping[str, Any],
    ) -> bool:
        """Return whether Qdrant-indexed metadata fields changed."""
        indexed_fields = ("year",)
        for field_name in indexed_fields:
            if (
                str(previous.get(field_name, "")).strip()
                != str(current.get(field_name, "")).strip()
            ):
                return True
        return False

    def _refresh_document_chunk_vectors(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> None:
        """Re-upsert one document's vectors when metadata affecting payload changes."""
        if bool(getattr(self._qdrant_indexer, "is_disabled", False)):
            return

        state = self._repository.get_state(document_id)
        partition_key = str(state.get("partition_key", "")).strip()
        if not partition_key:
            return
        partitions = self._repository.get_partitions_by_key(
            partition_key,
            document_id=document_id,
        )
        if not partitions:
            return

        object_name = str(state.get("file_path", "")).strip()
        if not object_name:
            object_names = self._repository.get_document_object_names(bucket_name, document_id)
            if object_names:
                object_name = object_names[0]
        if not object_name:
            return

        self.upsert_stage_object(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=document_id,
            etag=state.get("etag", ""),
        )

    def _normalize_state_after_metadata_only_update(self, *, document_id: str) -> None:
        """Clear stale upsert progress when metadata changes do not queue new work."""
        state = self._repository.get_state(document_id)
        processing_state = str(state.get("processing_state", "")).strip().lower()
        processing_stage = str(state.get("processing_stage", "")).strip().lower()
        processing_progress = self._safe_positive_int(state.get("processing_progress")) or 0
        stage_progress = self._safe_positive_int(state.get("processing_stage_progress")) or 0

        should_mark_processed = False
        if processing_state == "processed":
            should_mark_processed = (
                processing_stage != "processed"
                or processing_progress < 100
                or stage_progress < 100
            )
        elif (
            processing_state == "processing"
            and processing_stage == "upsert"
            and processing_progress >= 80
        ):
            should_mark_processed = True

        if not should_mark_processed:
            return

        self._set_state(
            document_id,
            processing_state="processed",
            processing_stage="processed",
            processing_stage_progress=100,
            processing_progress=100,
            error="",
        )

    def resolve_document_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
    ) -> tuple[bytes, str]:
        """Read one object from MinIO for resolver delivery.

        Args:
            bucket_name: Source bucket name.
            object_name: Object path within the bucket.

        Returns:
            Tuple of object bytes and detected content type.
        """
        normalized_bucket_name = bucket_name.strip()
        normalized_object_name = object_name.strip().lstrip("/")
        if not normalized_bucket_name:
            raise ValueError("bucket_name must not be empty.")
        if not normalized_object_name:
            raise ValueError("file_path must not be empty.")

        physical_bucket_name, physical_object_name = self._resolve_physical_location(
            bucket_name=normalized_bucket_name,
            object_name=normalized_object_name,
        )
        stat_info = self._minio_client.stat_object(physical_bucket_name, physical_object_name)
        content_type = str(getattr(stat_info, "content_type", "") or "").strip()
        if not content_type:
            content_type = infer_content_type(normalized_object_name)
        payload = self._read_object_bytes(normalized_bucket_name, normalized_object_name)
        return payload, content_type

    def _crossref_get_json(
        self,
        *,
        path: str,
        params: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any]:
        """Fetch a Crossref API payload and return decoded JSON mapping."""
        try:
            import httpx
        except ImportError as exc:
            raise RuntimeError("httpx is required for Crossref metadata fetching.") from exc

        request_timeout = httpx.Timeout(
            timeout=CROSSREF_TIMEOUT_SECONDS,
            connect=min(5.0, CROSSREF_TIMEOUT_SECONDS),
            read=CROSSREF_TIMEOUT_SECONDS,
            write=CROSSREF_TIMEOUT_SECONDS,
        )
        request_kind = self._crossref_request_kind(path)
        with httpx.Client(timeout=request_timeout, follow_redirects=True) as client:
            # Crossref "public" pool guidance: 1 concurrent request.
            with self.__class__._crossref_rate_lock:
                self.__class__._crossref_enforce_rate_limit_locked(request_kind)
                response = client.get(
                    f"{CROSSREF_API_BASE_URL}{path}",
                    params=dict(params or {}),
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "mcp-evidencebase/0.1 (+https://evidencebase.heley.uk)",
                    },
                )

        if response.status_code == 404:
            return {}
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, Mapping):
            return payload
        return {}

    @staticmethod
    def _crossref_request_kind(path: str) -> str:
        """Classify Crossref request shape as ``single`` or ``list``."""
        normalized_path = str(path).strip()
        if normalized_path.startswith("/works/") and len(normalized_path) > len("/works/"):
            return "single"
        return "list"

    @classmethod
    def _crossref_enforce_rate_limit_locked(cls, request_kind: str) -> None:
        """Delay request start to respect Crossref public and query-type limits."""
        if CROSSREF_PUBLIC_POOL_MAX_CONCURRENT_REQUESTS != 1:
            raise ValueError("CROSSREF_PUBLIC_POOL_MAX_CONCURRENT_REQUESTS must be 1.")

        global_min_interval = 1.0 / CROSSREF_PUBLIC_POOL_MAX_REQUESTS_PER_SECOND
        if request_kind == "single":
            request_min_interval = 1.0 / CROSSREF_SINGLE_RECORD_MAX_REQUESTS_PER_SECOND
            next_allowed_for_kind = cls._crossref_next_allowed_single_ts
        else:
            request_min_interval = 1.0 / CROSSREF_LIST_QUERY_MAX_REQUESTS_PER_SECOND
            next_allowed_for_kind = cls._crossref_next_allowed_list_ts

        now = time.monotonic()
        next_allowed_start = max(cls._crossref_next_allowed_global_ts, next_allowed_for_kind)
        sleep_seconds = next_allowed_start - now
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
            now = time.monotonic()

        cls._crossref_next_allowed_global_ts = (
            max(cls._crossref_next_allowed_global_ts, now) + global_min_interval
        )
        if request_kind == "single":
            cls._crossref_next_allowed_single_ts = (
                max(cls._crossref_next_allowed_single_ts, now) + request_min_interval
            )
            return
        cls._crossref_next_allowed_list_ts = (
            max(cls._crossref_next_allowed_list_ts, now) + request_min_interval
        )

    @staticmethod
    def _crossref_extract_single_item(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        """Extract one work item from a Crossref API payload."""
        message = payload.get("message")
        if isinstance(message, Mapping):
            return message
        return None

    @staticmethod
    def _crossref_extract_items(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        """Extract work-item lists from a Crossref search payload."""
        message = payload.get("message")
        if not isinstance(message, Mapping):
            return []
        raw_items = message.get("items")
        if not isinstance(raw_items, list):
            return []
        return [item for item in raw_items if isinstance(item, Mapping)]

    def _crossref_select_best_item(
        self,
        *,
        items: list[Mapping[str, Any]],
        lookup_field: str,
        expected_doi: str,
        expected_isbn: str,
        expected_issn: str,
        expected_title: str,
        expected_year: str,
    ) -> tuple[Mapping[str, Any] | None, float]:
        """Return the highest-scoring Crossref item for the current lookup mode."""
        best_item: Mapping[str, Any] | None = None
        best_score = 0.0

        for item in items:
            score = _crossref_score_item(
                item,
                lookup_field=lookup_field,
                expected_doi=expected_doi,
                expected_isbn=expected_isbn,
                expected_issn=expected_issn,
                expected_title=expected_title,
                expected_year=expected_year,
            )
            if score > best_score:
                best_item = item
                best_score = score

        return best_item, best_score

    def _crossref_rank_items(
        self,
        *,
        items: list[Mapping[str, Any]],
        lookup_field: str,
        expected_doi: str,
        expected_isbn: str,
        expected_issn: str,
        expected_title: str,
        expected_year: str,
    ) -> list[tuple[Mapping[str, Any], float]]:
        """Return scored candidates ordered by confidence then metadata richness."""
        scored: list[tuple[Mapping[str, Any], float]] = []
        for item in items:
            confidence = _crossref_score_item(
                item,
                lookup_field=lookup_field,
                expected_doi=expected_doi,
                expected_isbn=expected_isbn,
                expected_issn=expected_issn,
                expected_title=expected_title,
                expected_year=expected_year,
            )
            if confidence <= 0:
                continue
            scored.append((item, confidence))

        scored.sort(
            key=lambda candidate: (
                candidate[1],
                _crossref_enrichment_score(candidate[0]),
            ),
            reverse=True,
        )
        return scored

    def _lookup_metadata_from_crossref(
        self,
        *,
        existing_metadata: Mapping[str, Any],
        require_changes: bool,
    ) -> dict[str, Any]:
        """Return one accepted Crossref metadata payload for existing seed metadata."""
        expected_doi = _normalize_doi_lookup_value(existing_metadata.get("doi", ""))
        expected_isbn = _normalize_isbn_value(existing_metadata.get("isbn", ""))
        expected_issn = _normalize_issn_value(existing_metadata.get("issn", ""))
        expected_title = str(existing_metadata.get("title", "")).strip()
        expected_year = str(existing_metadata.get("year", "")).strip()

        if not any([expected_doi, expected_isbn, expected_issn, expected_title]):
            raise ValueError("No DOI, ISBN, ISSN, or title is available for Crossref lookup.")

        lookup_plan: list[tuple[str, str, Mapping[str, str]]] = []
        if expected_doi:
            lookup_plan.append(("doi", f"/works/{quote(expected_doi, safe='')}", {}))
        if expected_isbn:
            isbn_params: dict[str, str] = {
                "filter": f"isbn:{expected_isbn}",
                "rows": str(CROSSREF_MAX_RESULTS),
            }
            if expected_title:
                isbn_params["query.title"] = expected_title
            lookup_plan.append(("isbn", "/works", isbn_params))
        if expected_issn:
            issn_params: dict[str, str] = {
                "filter": f"issn:{expected_issn}",
                "rows": str(CROSSREF_MAX_RESULTS),
            }
            if expected_title:
                issn_params["query.title"] = expected_title
            lookup_plan.append(("issn", "/works", issn_params))
        if expected_title:
            lookup_plan.append(
                (
                    "title",
                    "/works",
                    {
                        "query.title": expected_title,
                        "rows": str(CROSSREF_MAX_RESULTS),
                    },
                )
            )

        best_attempt_field = ""
        best_attempt_score = 0.0
        for lookup_field, path, params in lookup_plan:
            payload = self._crossref_get_json(path=path, params=params)
            if not payload:
                continue

            if lookup_field == "doi":
                single_item = self._crossref_extract_single_item(payload)
                if not single_item:
                    continue
                candidate_item = single_item
                confidence = _crossref_score_item(
                    candidate_item,
                    lookup_field=lookup_field,
                    expected_doi=expected_doi,
                    expected_isbn=expected_isbn,
                    expected_issn=expected_issn,
                    expected_title=expected_title,
                    expected_year=expected_year,
                )
                if confidence > best_attempt_score:
                    best_attempt_score = confidence
                    best_attempt_field = lookup_field
                if confidence < CROSSREF_CONFIDENCE_THRESHOLD:
                    continue

                fetched_metadata = _crossref_map_item_to_metadata(candidate_item)
                if require_changes and not _metadata_update_changes(
                    existing_metadata,
                    fetched_metadata,
                ):
                    continue
                return {
                    "lookup_field": lookup_field,
                    "confidence": round(confidence, 4),
                    "metadata": fetched_metadata,
                }

            items = self._crossref_extract_items(payload)
            ranked_candidates = self._crossref_rank_items(
                items=items,
                lookup_field=lookup_field,
                expected_doi=expected_doi,
                expected_isbn=expected_isbn,
                expected_issn=expected_issn,
                expected_title=expected_title,
                expected_year=expected_year,
            )
            if not ranked_candidates:
                continue

            for candidate_item, confidence in ranked_candidates:
                if confidence > best_attempt_score:
                    best_attempt_score = confidence
                    best_attempt_field = lookup_field
                if confidence < CROSSREF_CONFIDENCE_THRESHOLD:
                    break

                fetched_metadata = _crossref_map_item_to_metadata(candidate_item)
                if require_changes and not _metadata_update_changes(
                    existing_metadata,
                    fetched_metadata,
                ):
                    continue
                return {
                    "lookup_field": lookup_field,
                    "confidence": round(confidence, 4),
                    "metadata": fetched_metadata,
                }

        if best_attempt_field:
            raise ValueError(
                "No high-confidence Crossref match was accepted. "
                f"Best attempt used {best_attempt_field.upper()} with confidence "
                f"{best_attempt_score:.2f}."
            )
        raise ValueError("No Crossref results were found for DOI/ISBN/ISSN/title lookups.")

    def lookup_metadata_seed_from_crossref(
        self,
        *,
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Look up Crossref metadata for a transient metadata seed without persisting it."""
        return self._lookup_metadata_from_crossref(
            existing_metadata=metadata,
            require_changes=False,
        )

    def fetch_metadata_from_crossref(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> dict[str, Any]:
        """Fetch and persist document metadata from Crossref with confidence gating."""
        _, existing_metadata = self._repository.get_document_metadata(bucket_name, document_id)
        result = self._lookup_metadata_from_crossref(
            existing_metadata=existing_metadata,
            require_changes=True,
        )
        merged = self.update_metadata(
            bucket_name=bucket_name,
            document_id=document_id,
            metadata=result["metadata"],
        )
        return {
            "lookup_field": result["lookup_field"],
            "confidence": result["confidence"],
            "metadata": merged,
        }

    def delete_document(
        self,
        *,
        bucket_name: str,
        document_id: str,
        keep_partitions: bool = True,
    ) -> bool:
        """Delete a document from MinIO, Qdrant, and selected Redis data.

        Args:
            bucket_name: Bucket containing document sources.
            document_id: Canonical document hash.
            keep_partitions: Keep partition payload in Redis when ``True``.

        Returns:
            ``True`` when deletion bookkeeping completes.
        """
        object_names = self._repository.get_document_object_names(bucket_name, document_id)
        for object_name in object_names:
            self._remove_object_if_exists(bucket_name, object_name)

        self._qdrant_indexer.delete_document(bucket_name, document_id)

        return cast(
            bool,
            self._repository.remove_document(
                bucket_name=bucket_name,
                document_id=document_id,
                keep_partitions=keep_partitions,
            ),
        )

    def delete_collection(self, *, bucket_name: str, keep_partitions: bool = True) -> bool:
        """Delete one logical collection and all related MinIO, Redis, and Qdrant state."""
        normalized_bucket_name = normalize_collection_name(bucket_name)
        collection_exists_in_storage = self._collection_exists_in_storage(normalized_bucket_name)
        legacy_bucket_exists = self._bucket_exists(normalized_bucket_name)
        if not collection_exists_in_storage and not legacy_bucket_exists:
            return False

        document_ids = self._repository.list_document_ids(normalized_bucket_name)
        for document_id in document_ids:
            self.delete_document(
                bucket_name=normalized_bucket_name,
                document_id=document_id,
                keep_partitions=keep_partitions,
            )

        if collection_exists_in_storage:
            prefix = f"{normalized_bucket_name}/"
            for item in self._minio_client.list_objects(self._storage_bucket_name, recursive=True):
                object_name = str(getattr(item, "object_name", "")).strip()
                if not object_name.startswith(prefix):
                    continue
                self._minio_client.remove_object(self._storage_bucket_name, object_name)
        elif legacy_bucket_exists:
            for object_name, _etag in self.list_bucket_objects(normalized_bucket_name):
                self._remove_object_if_exists(normalized_bucket_name, object_name)
            self._minio_client.remove_bucket(normalized_bucket_name)

        self.delete_bucket_qdrant_collection(normalized_bucket_name)
        return True

    def merge_buckets_into_storage(
        self,
        *,
        source_bucket_names: list[str],
        target_bucket_name: str | None = None,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Merge legacy physical buckets into the shared storage bucket."""
        normalized_target_bucket_name = (
            str(target_bucket_name or self._storage_bucket_name).strip()
            or self._storage_bucket_name
        )
        normalized_source_bucket_names: list[str] = []
        seen_bucket_names: set[str] = set()
        for bucket_name in source_bucket_names:
            normalized_bucket_name = normalize_collection_name(bucket_name)
            if normalized_bucket_name == normalized_target_bucket_name:
                continue
            if normalized_bucket_name in seen_bucket_names:
                continue
            normalized_source_bucket_names.append(normalized_bucket_name)
            seen_bucket_names.add(normalized_bucket_name)

        if not normalized_source_bucket_names:
            raise ValueError("At least one source bucket is required.")

        existing_target_objects: set[str] = set()
        if self._bucket_exists(normalized_target_bucket_name):
            for item in self._minio_client.list_objects(
                normalized_target_bucket_name, recursive=True
            ):
                object_name = str(getattr(item, "object_name", "")).strip()
                if object_name:
                    existing_target_objects.add(object_name)

        report: dict[str, Any] = {
            "target_bucket_name": normalized_target_bucket_name,
            "source_bucket_names": normalized_source_bucket_names,
            "dry_run": bool(dry_run),
            "warnings": [
                "Stop Celery workers and bucket scans before applying this migration."
            ],
            "buckets_seen": 0,
            "source_buckets_found": 0,
            "source_buckets_missing": 0,
            "objects_seen": 0,
            "objects_copied": 0,
            "objects_deleted": 0,
            "markers_created": 0,
            "redis_mappings_patched": 0,
            "redis_mappings_missing": 0,
            "qdrant_collections_patched": 0,
            "qdrant_points_patched": 0,
            "conflicts": 0,
            "removed_source_buckets": 0,
            "items": [],
        }

        for source_bucket_name in normalized_source_bucket_names:
            bucket_report: dict[str, Any] = {
                "bucket_name": source_bucket_name,
                "found": False,
                "objects_seen": 0,
                "objects_copied": 0,
                "objects_deleted": 0,
                "redis_mappings_patched": 0,
                "redis_mappings_missing": 0,
                "qdrant_points_patched": 0,
                "conflicts": [],
            }
            report["items"].append(bucket_report)
            report["buckets_seen"] += 1

            if not self._bucket_exists(source_bucket_name):
                report["source_buckets_missing"] += 1
                continue

            bucket_report["found"] = True
            report["source_buckets_found"] += 1
            source_objects = [
                (
                    str(getattr(item, "object_name", "")).strip(),
                    normalize_etag(getattr(item, "etag", "")),
                )
                for item in self._minio_client.list_objects(source_bucket_name, recursive=True)
                if str(getattr(item, "object_name", "")).strip()
            ]
            source_objects.sort(key=lambda item: item[0])
            bucket_report["objects_seen"] = len(source_objects)
            report["objects_seen"] += len(source_objects)

            if dry_run:
                if not self._collection_exists_in_storage(source_bucket_name):
                    report["markers_created"] += 1
                for object_name, _etag in source_objects:
                    target_object_name = build_storage_object_name(source_bucket_name, object_name)
                    if target_object_name in existing_target_objects:
                        bucket_report["conflicts"].append(object_name)
                        report["conflicts"] += 1
                continue

            self._ensure_storage_bucket()
            if not self._collection_exists_in_storage(source_bucket_name):
                self._create_collection_marker(source_bucket_name)
                report["markers_created"] += 1

            for object_name, _etag in source_objects:
                target_object_name = build_storage_object_name(source_bucket_name, object_name)
                if target_object_name in existing_target_objects:
                    bucket_report["conflicts"].append(object_name)
                    report["conflicts"] += 1
                    continue
                from minio.commonconfig import CopySource

                self._minio_client.copy_object(
                    normalized_target_bucket_name,
                    target_object_name,
                    CopySource(source_bucket_name, object_name),
                )
                existing_target_objects.add(target_object_name)
                bucket_report["objects_copied"] += 1
                report["objects_copied"] += 1
                try:
                    self._repository.update_storage_location(
                        bucket_name=source_bucket_name,
                        object_name=object_name,
                        storage_bucket_name=normalized_target_bucket_name,
                        storage_object_name=target_object_name,
                    )
                    bucket_report["redis_mappings_patched"] += 1
                    report["redis_mappings_patched"] += 1
                except ValueError:
                    bucket_report["redis_mappings_missing"] += 1
                    report["redis_mappings_missing"] += 1

            rewritten_points = cast(
                int,
                self._qdrant_indexer.rewrite_collection_storage_metadata(
                    bucket_name=source_bucket_name,
                    storage_bucket_name=normalized_target_bucket_name,
                ),
            )
            if rewritten_points > 0:
                report["qdrant_collections_patched"] += 1
                report["qdrant_points_patched"] += rewritten_points
                bucket_report["qdrant_points_patched"] = rewritten_points

            for object_name, _etag in source_objects:
                target_object_name = build_storage_object_name(source_bucket_name, object_name)
                if target_object_name not in existing_target_objects:
                    continue
                self._minio_client.remove_object(source_bucket_name, object_name)
                bucket_report["objects_deleted"] += 1
                report["objects_deleted"] += 1

            self._minio_client.remove_bucket(source_bucket_name)
            report["removed_source_buckets"] += 1

        return report

    def purge_datastores(self) -> dict[str, int]:
        """Purge Redis and Qdrant data for this application prefix."""
        redis_deleted_keys = self._repository.purge_prefix_data()
        qdrant_deleted_collections = self._qdrant_indexer.purge_prefixed_collections()
        return {
            "redis_deleted_keys": redis_deleted_keys,
            "qdrant_deleted_collections": qdrant_deleted_collections,
        }

    def search_documents(
        self,
        *,
        bucket_name: str,
        query: str,
        limit: int = 10,
        mode: str = "hybrid",
        rrf_k: int = 60,
    ) -> list[dict[str, Any]]:
        """Search indexed chunks for one bucket using semantic/keyword/hybrid retrieval."""
        results = self._qdrant_indexer.search_chunks(
            bucket_name=bucket_name,
            query=query,
            limit=limit,
            mode=mode,
            rrf_k=rrf_k,
        )
        results = self._hydrate_search_results_with_metadata(
            bucket_name=bucket_name,
            results=results,
        )
        return self._hydrate_search_results_with_sections(results=results)
