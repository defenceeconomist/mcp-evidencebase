"""High-level ingestion service orchestration."""

from __future__ import annotations

import io
import threading
import time
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote

from minio import Minio
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
    SEARCH_MODES,
    _serialize_author_entries,
    compute_document_id,
    extract_metadata_from_partitions,
    extract_pdf_title_author,
    infer_content_type,
    normalize_etag,
    utc_now_iso,
)
from mcp_evidencebase.ingestion_modules.qdrant import QdrantIndexer
from mcp_evidencebase.ingestion_modules.repository import RedisDocumentRepository

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
    Redis state/metadata persistence, and Qdrant vector indexing. Partitioning
    and chunking/indexing are exposed as separate stage methods so workflows can
    rerun chunking independently from partition extraction.
    """

    _crossref_rate_lock = threading.Lock()
    _crossref_next_allowed_global_ts = 0.0
    _crossref_next_allowed_single_ts = 0.0
    _crossref_next_allowed_list_ts = 0.0

    def __init__(
        self,
        *,
        minio_client: Minio,
        repository: RedisDocumentRepository,
        partition_client: UnstructuredPartitionClient,
        qdrant_indexer: QdrantIndexer,
        chunk_size_chars: int,
        chunk_overlap_chars: int,
    ) -> None:
        """Construct ingestion service dependencies.

        Args:
            minio_client: Configured MinIO SDK client.
            repository: Redis-backed document repository.
            partition_client: Unstructured API client used for partition extraction.
            qdrant_indexer: Qdrant index adapter for chunk embeddings.
            chunk_size_chars: Maximum chunk size for text chunking.
            chunk_overlap_chars: Overlap size for adjacent chunks.
        """
        self._minio_client = minio_client
        self._repository = repository
        self._partition_client = partition_client
        self._qdrant_indexer = qdrant_indexer
        self._chunk_size_chars = chunk_size_chars
        self._chunk_overlap_chars = chunk_overlap_chars

    def _set_state(self, document_id: str, **values: Any) -> None:
        """Convenience wrapper for repository state updates."""
        self._repository.set_state(document_id, values)

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
                    "section_index": section_index if section_index is not None else len(sections_by_id),
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
            section_id = str(result.get("section_id", "")).strip() or str(
                result.get("parent_section_id", "")
            ).strip()
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

    def _read_object_bytes(self, bucket_name: str, object_name: str) -> bytes:
        """Read and return object bytes from MinIO."""
        object_response = self._minio_client.get_object(bucket_name, object_name)
        try:
            return object_response.read()
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
        stat_info = self._minio_client.stat_object(bucket_name, object_name)
        resolved_etag = normalize_etag(etag) or normalize_etag(getattr(stat_info, "etag", ""))
        document_id = compute_document_id(file_bytes)

        self._repository.add_document(bucket_name, document_id)
        self._repository.mark_object(
            bucket_name=bucket_name,
            object_name=object_name,
            document_id=document_id,
            etag=resolved_etag,
        )
        return document_id, resolved_etag

    def _remove_object_if_exists(self, bucket_name: str, object_name: str) -> None:
        """Delete an object and ignore not-found responses."""
        try:
            self._minio_client.remove_object(bucket_name, object_name)
        except S3Error as exc:
            if exc.code not in {"NoSuchKey", "NoSuchObject"}:
                raise

    def list_documents(self, bucket_name: str) -> list[dict[str, Any]]:
        """Return UI-facing document records for a bucket.

        Args:
            bucket_name: Bucket to query.

        Returns:
            List of document records for the bucket.
        """
        records = self._repository.list_documents(bucket_name)
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
            chunks = build_partition_chunks(
                normalized_partitions,
                chunk_size_chars=self._chunk_size_chars,
                chunk_overlap_chars=self._chunk_overlap_chars,
            )
            record["chunks_tree"] = {"chunks": chunks}
        return records

    def list_buckets(self) -> list[str]:
        """Return sorted bucket names from MinIO.

        Returns:
            Sorted list of bucket names.
        """
        bucket_names = [bucket.name for bucket in self._minio_client.list_buckets()]
        bucket_names.sort()
        return bucket_names

    def ensure_bucket_qdrant_collection(self, bucket_name: str) -> bool:
        """Ensure the bucket-level Qdrant collection exists.

        Args:
            bucket_name: Bucket whose collection should exist.

        Returns:
            ``True`` when a collection is created, otherwise ``False``.
        """
        return self._qdrant_indexer.ensure_bucket_collection(bucket_name)

    def delete_bucket_qdrant_collection(self, bucket_name: str) -> bool:
        """Delete the bucket-level Qdrant collection when present.

        Args:
            bucket_name: Bucket whose collection should be removed.

        Returns:
            ``True`` when a collection is removed, otherwise ``False``.
        """
        return self._qdrant_indexer.delete_bucket_collection(bucket_name)

    def list_bucket_objects(self, bucket_name: str) -> list[tuple[str, str]]:
        """Return non-directory object names and ETags for one bucket.

        Args:
            bucket_name: Bucket to scan.

        Returns:
            List of ``(object_name, etag)`` tuples.
        """
        objects: list[tuple[str, str]] = []
        object_iter = self._minio_client.list_objects(bucket_name, recursive=True)
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
        return self._repository.object_requires_processing(bucket_name, object_name, etag)

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
        normalized_object_name = object_name.strip()
        if not normalized_object_name:
            raise ValueError("object_name must not be empty.")

        guessed_content_type = content_type or infer_content_type(normalized_object_name)
        self._minio_client.put_object(
            bucket_name,
            normalized_object_name,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type=guessed_content_type,
        )
        stat_info = self._minio_client.stat_object(bucket_name, normalized_object_name)
        etag = normalize_etag(getattr(stat_info, "etag", ""))

        document_id = compute_document_id(payload)
        self._repository.add_document(bucket_name, document_id)
        self._repository.mark_object(
            bucket_name=bucket_name,
            object_name=normalized_object_name,
            document_id=document_id,
            etag=etag,
        )
        meta_key = self._repository.set_default_metadata_if_missing(
            bucket_name,
            document_id,
            file_path=normalized_object_name,
        )
        self._set_state(
            document_id,
            file_path=normalized_object_name,
            processing_state="processing",
            processing_progress=0,
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
        file_bytes = self._read_object_bytes(bucket_name, object_name)
        if not file_bytes:
            raise ValueError(f"Object '{object_name}' in bucket '{bucket_name}' is empty.")

        document_id, resolved_etag = self._resolve_document_identity(
            bucket_name=bucket_name,
            object_name=object_name,
            etag=etag,
            file_bytes=file_bytes,
        )

        self._set_state(
            document_id,
            file_path=object_name,
            processing_state="processing",
            processing_progress=10,
            error="",
        )

        try:
            partitions = self._partition_client.partition_file(
                file_name=PurePosixPath(object_name).name,
                file_bytes=file_bytes,
                content_type=infer_content_type(object_name),
            )
            partition_key = self._repository.set_partitions(bucket_name, document_id, partitions)
            self._set_state(
                document_id,
                processing_state="processing",
                processing_progress=50,
                partition_key=partition_key,
                partitions_count=len(partitions),
                error="",
            )

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
            self._set_state(
                document_id,
                processing_state="processing",
                processing_progress=60,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                error="",
            )
            return {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "document_id": document_id,
                "etag": resolved_etag,
                "partition_key": partition_key,
                "meta_key": meta_key,
            }
        except Exception as exc:
            self._set_state(
                document_id,
                processing_state="failed",
                processing_progress=100,
                error=str(exc),
            )
            raise

    def chunk_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
    ) -> dict[str, str]:
        """Run chunking and vector indexing for an already partitioned document.

        Args:
            bucket_name: Source bucket.
            object_name: Source object path.
            document_id: Canonical document hash to chunk/index.

        Returns:
            Stage payload containing bucket/object IDs and persisted key references.

        Raises:
            ValueError: If no partition payload is available for the document.
            Exception: Any downstream chunking or indexing failure.
        """
        try:
            state = self._repository.get_state(document_id)
            partition_key = state.get("partition_key", "")
            if not partition_key:
                raise ValueError(
                    f"Document '{document_id}' has no partition_key; run partition stage first."
                )

            partitions = self._repository.get_partitions_by_key(partition_key)
            if not partitions:
                raise ValueError(
                    "No partitions were found for document "
                    f"'{document_id}' and key '{partition_key}'."
                )

            meta_key = state.get("meta_key", "")
            resolved_meta_key, metadata = self._repository.get_document_metadata(
                bucket_name, document_id
            )
            if not meta_key:
                meta_key = resolved_meta_key
            document_title = str(metadata.get("title", "")).strip()
            document_author = str(metadata.get("author", "")).strip()
            document_year = str(metadata.get("year", "")).strip()

            self._set_state(
                document_id,
                processing_state="processing",
                processing_progress=75,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                error="",
            )

            chunks = build_partition_chunks(
                partitions,
                chunk_size_chars=self._chunk_size_chars,
                chunk_overlap_chars=self._chunk_overlap_chars,
            )
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
            self._set_state(
                document_id,
                processing_state="processing",
                processing_progress=90,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                chunks_count=len(chunks),
                sections_count=len(sections),
                error="",
            )

            self._qdrant_indexer.upsert_document_chunks(
                bucket_name=bucket_name,
                document_id=document_id,
                file_path=object_name,
                chunks=chunks,
                partition_key=partition_key,
                meta_key=meta_key,
                document_title=document_title,
                document_author=document_author,
                document_year=document_year,
            )
            self._set_state(
                document_id,
                processing_state="processed",
                processing_progress=100,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                chunks_count=len(chunks),
                sections_count=len(sections),
                error="",
            )
            return {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "document_id": document_id,
                "partition_key": partition_key,
                "meta_key": meta_key,
            }
        except Exception as exc:
            self._set_state(
                document_id,
                processing_state="failed",
                processing_progress=100,
                error=str(exc),
            )
            raise

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
                f"Document '{normalized_document_id}' was not found in bucket '{normalized_bucket_name}'."
            )

        section = self._repository.get_document_section(normalized_document_id, normalized_section_id)
        if section is None:
            raise ValueError(
                f"Section '{normalized_section_id}' was not found for document '{normalized_document_id}'."
            )
        return section

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
                f"Document '{normalized_document_id}' was not found in bucket '{normalized_bucket_name}'."
            )

        sections = self._repository.get_document_sections(normalized_document_id)
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
                f"Document '{normalized_document_id}' was not found in bucket '{normalized_bucket_name}'."
            )

        state = self._repository.get_state(normalized_document_id)
        partition_key = state.get("partition_key", "")
        if not partition_key:
            raise ValueError(
                f"Document '{normalized_document_id}' has no partition_key; run partition stage first."
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

        chunks = build_partition_chunks(
            partitions,
            chunk_size_chars=self._chunk_size_chars,
            chunk_overlap_chars=self._chunk_overlap_chars,
        )
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

    def update_metadata(
        self,
        *,
        bucket_name: str,
        document_id: str,
        metadata: Mapping[str, Any],
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

        return self._repository.update_document_metadata(
            bucket_name=bucket_name,
            document_id=document_id,
            metadata=normalized,
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

        stat_info = self._minio_client.stat_object(normalized_bucket_name, normalized_object_name)
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

    def fetch_metadata_from_crossref(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> dict[str, Any]:
        """Fetch and persist document metadata from Crossref with confidence gating."""
        _, existing_metadata = self._repository.get_document_metadata(bucket_name, document_id)

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

                fetched_metadata = _crossref_map_item_to_metadata(
                    candidate_item,
                )
                if not _metadata_update_changes(existing_metadata, fetched_metadata):
                    continue
                merged = self.update_metadata(
                    bucket_name=bucket_name,
                    document_id=document_id,
                    metadata=fetched_metadata,
                )
                return {
                    "lookup_field": lookup_field,
                    "confidence": round(confidence, 4),
                    "metadata": merged,
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

                fetched_metadata = _crossref_map_item_to_metadata(
                    candidate_item,
                )
                if not _metadata_update_changes(existing_metadata, fetched_metadata):
                    continue

                merged = self.update_metadata(
                    bucket_name=bucket_name,
                    document_id=document_id,
                    metadata=fetched_metadata,
                )
                return {
                    "lookup_field": lookup_field,
                    "confidence": round(confidence, 4),
                    "metadata": merged,
                }

        if best_attempt_field:
            raise ValueError(
                "No high-confidence Crossref match was accepted. "
                f"Best attempt used {best_attempt_field.upper()} with confidence "
                f"{best_attempt_score:.2f}."
            )
        raise ValueError("No Crossref results were found for DOI/ISBN/ISSN/title lookups.")

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

        return self._repository.remove_document(
            bucket_name=bucket_name,
            document_id=document_id,
            keep_partitions=keep_partitions,
        )

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
        return self._hydrate_search_results_with_sections(results=results)
