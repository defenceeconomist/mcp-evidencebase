"""Document ingestion services for MinIO, Redis, Unstructured, and Qdrant."""

from __future__ import annotations

import hashlib
import io
import json
import mimetypes
import os
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Protocol

from minio import Minio
from minio.error import S3Error

from mcp_evidencebase.minio_settings import MinioSettings, build_minio_settings

BIBTEX_FIELDS: tuple[str, ...] = (
    "address",
    "annote",
    "author",
    "booktitle",
    "chapter",
    "crossref",
    "doi",
    "edition",
    "editor",
    "email",
    "howpublished",
    "institution",
    "isbn",
    "journal",
    "month",
    "note",
    "number",
    "organization",
    "pages",
    "publisher",
    "school",
    "series",
    "title",
    "type",
    "volume",
    "year",
)

METADATA_FIELDS: tuple[str, ...] = (*BIBTEX_FIELDS, "document_type", "citation_key")

DOI_PATTERN = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)
ISBN_PATTERN = re.compile(r"\b(?:97[89][-\s]?)?(?:\d[-\s]?){9}[\dXx]\b")
SEARCH_MODES: tuple[str, ...] = ("semantic", "keyword", "hybrid")


class MinioObjectLike(Protocol):
    """Subset of MinIO object summary data used by the scanner."""

    object_name: str
    etag: str | None
    is_dir: bool


@dataclass(frozen=True)
class IngestionSettings:
    """Runtime settings for document ingestion components."""

    minio: MinioSettings
    redis_url: str
    redis_prefix: str
    qdrant_url: str
    qdrant_api_key: str | None
    qdrant_collection_prefix: str
    unstructured_api_url: str
    unstructured_api_key: str | None
    unstructured_strategy: str
    unstructured_timeout_seconds: float
    fastembed_model: str
    fastembed_keyword_model: str
    chunk_size_chars: int
    chunk_overlap_chars: int
    scan_interval_seconds: int


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def normalize_etag(value: str | None) -> str:
    """Normalize MinIO ETag values by trimming whitespace and quotes."""
    if value is None:
        return ""
    return value.strip().strip('"')


def slugify(value: str) -> str:
    """Create a lowercase safe slug from free text."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return slug or "document"


def infer_content_type(object_name: str) -> str:
    """Infer content type from object name; fallback to binary stream."""
    guessed_type, _ = mimetypes.guess_type(object_name)
    if guessed_type:
        return guessed_type
    return "application/octet-stream"


def canonical_json(value: Any) -> str:
    """Serialize values into a deterministic JSON representation."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def compute_hash_for_value(value: Any) -> str:
    """Compute SHA-256 for deterministic JSON-serializable values."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def compute_document_id(document_bytes: bytes) -> str:
    """Return a deterministic document identifier from file bytes."""
    return hashlib.sha256(document_bytes).hexdigest()


def compute_partition_key(partitions: list[dict[str, Any]]) -> str:
    """Return a deterministic partition key for serialized partition payload."""
    return compute_hash_for_value(partitions)


def compute_metadata_key(metadata: Mapping[str, str]) -> str:
    """Return a deterministic metadata key for normalized metadata values."""
    payload = {field_name: str(metadata.get(field_name, "")) for field_name in METADATA_FIELDS}
    return compute_hash_for_value(payload)


def build_resolver_url(bucket_name: str, object_name: str) -> str:
    """Build a resolver URL placeholder for future deep link implementation."""
    normalized_bucket = bucket_name.strip()
    normalized_object = object_name.strip().lstrip("/")
    return f"docs://{normalized_bucket}/{normalized_object}?page="


def compute_chunk_point_id(
    *,
    bucket_name: str,
    document_id: str,
    chunk_index: int,
) -> str:
    """Return a deterministic UUID point ID for a chunk."""
    value = f"{bucket_name}:{document_id}:{chunk_index}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, value))


def build_default_metadata(file_path: str, document_id: str) -> dict[str, str]:
    """Construct default BibTeX metadata values for a document."""
    file_name = PurePosixPath(file_path).name
    file_stem = PurePosixPath(file_name).stem
    citation_key = slugify(file_stem) or document_id[:12]
    title = re.sub(r"[-_]+", " ", file_stem).strip() or file_name or document_id[:12]

    metadata = {field_name: "" for field_name in BIBTEX_FIELDS}
    metadata["title"] = title
    metadata["document_type"] = "misc"
    metadata["citation_key"] = citation_key
    return metadata


def normalize_metadata(metadata: Mapping[str, str]) -> dict[str, str]:
    """Normalize metadata into the Redis schema with fixed field names."""
    normalized = {field_name: "" for field_name in METADATA_FIELDS}
    for key, value in metadata.items():
        field_name = str(key).strip().lower()
        if field_name in normalized:
            normalized[field_name] = str(value).strip()

    normalized["document_type"] = normalized.get("document_type", "") or "misc"
    return normalized


def extract_partition_text(partition: Mapping[str, Any]) -> str:
    """Extract display text from a partition payload entry."""
    text_value = partition.get("text")
    if isinstance(text_value, str):
        text = text_value.strip()
        if text:
            return text
    return ""


def extract_partition_page_number(partition: Mapping[str, Any]) -> int | None:
    """Read the page number from partition metadata when available."""
    metadata = partition.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    raw_page_number = metadata.get("page_number")
    if raw_page_number is None:
        return None
    try:
        page_number = int(raw_page_number)
    except (TypeError, ValueError):
        return None
    if page_number < 1:
        return None
    return page_number


def _normalize_coordinate_points(points: Any) -> list[list[float]]:
    """Normalize a coordinates points payload to ``[[x, y], ...]``."""
    if not isinstance(points, list):
        return []

    normalized: list[list[float]] = []
    for pair in points:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        try:
            x = float(pair[0])
            y = float(pair[1])
        except (TypeError, ValueError):
            continue
        normalized.append([x, y])
    return normalized


def extract_partition_bounding_box(partition: Mapping[str, Any]) -> dict[str, Any] | None:
    """Read normalized bounding-box coordinates from partition payload metadata."""
    coordinates: Mapping[str, Any] | None = None

    metadata = partition.get("metadata")
    if isinstance(metadata, Mapping):
        raw_coordinates = metadata.get("coordinates")
        if isinstance(raw_coordinates, Mapping):
            coordinates = raw_coordinates

    if coordinates is None:
        raw_coordinates = partition.get("coordinates")
        if isinstance(raw_coordinates, Mapping):
            coordinates = raw_coordinates

    if coordinates is None:
        return None

    points = _normalize_coordinate_points(coordinates.get("points"))
    if not points:
        return None

    payload: dict[str, Any] = {"points": points}
    raw_layout_width = coordinates.get("layout_width")
    if isinstance(raw_layout_width, (int, float)):
        payload["layout_width"] = float(raw_layout_width)
    raw_layout_height = coordinates.get("layout_height")
    if isinstance(raw_layout_height, (int, float)):
        payload["layout_height"] = float(raw_layout_height)
    raw_system = coordinates.get("system")
    if isinstance(raw_system, str):
        normalized_system = raw_system.strip()
        if normalized_system:
            payload["system"] = normalized_system
    return payload


def build_partition_chunks(
    partitions: Iterable[Mapping[str, Any]],
    *,
    chunk_size_chars: int,
    chunk_overlap_chars: int,
) -> list[dict[str, Any]]:
    """Create overlapping chunks with page and bounding-box metadata."""
    entries: list[dict[str, Any]] = []
    for partition in partitions:
        text = extract_partition_text(partition)
        if not text:
            continue
        entries.append(
            {
                "text": text,
                "page_number": extract_partition_page_number(partition),
                "bounding_box": extract_partition_bounding_box(partition),
            }
        )

    if not entries:
        return []

    delimiter = "\n\n"
    joined = delimiter.join(str(entry["text"]) for entry in entries)
    size = max(128, chunk_size_chars)
    overlap = max(0, min(chunk_overlap_chars, size - 1))

    windows: list[tuple[int, int]] = []
    if len(joined) <= size:
        windows.append((0, len(joined)))
    else:
        step = size - overlap
        cursor = 0
        while cursor < len(joined):
            windows.append((cursor, min(cursor + size, len(joined))))
            cursor += step

    spans: list[tuple[int, int, dict[str, Any]]] = []
    cursor = 0
    for index, entry in enumerate(entries):
        text = str(entry["text"])
        start = cursor
        end = start + len(text)
        spans.append((start, end, entry))
        cursor = end
        if index < len(entries) - 1:
            cursor += len(delimiter)

    chunks: list[dict[str, Any]] = []
    for window_start, window_end in windows:
        chunk_text = joined[window_start:window_end].strip()
        if not chunk_text:
            continue

        page_numbers: list[int] = []
        page_numbers_seen: set[int] = set()
        bounding_boxes: list[dict[str, Any]] = []
        bounding_boxes_seen: set[str] = set()

        for partition_start, partition_end, entry in spans:
            if partition_end <= window_start or partition_start >= window_end:
                continue

            raw_page_number = entry.get("page_number")
            page_number = raw_page_number if isinstance(raw_page_number, int) else None
            if page_number is not None and page_number > 0 and page_number not in page_numbers_seen:
                page_numbers.append(page_number)
                page_numbers_seen.add(page_number)

            raw_bounding_box = entry.get("bounding_box")
            if not isinstance(raw_bounding_box, Mapping):
                continue

            bounding_box_payload = {
                str(key): value for key, value in raw_bounding_box.items() if key != "page_number"
            }
            if page_number is not None and page_number > 0:
                bounding_box_payload["page_number"] = page_number

            dedupe_key = canonical_json(bounding_box_payload)
            if dedupe_key in bounding_boxes_seen:
                continue
            bounding_boxes_seen.add(dedupe_key)
            bounding_boxes.append(bounding_box_payload)

        chunks.append(
            {
                "chunk_index": len(chunks),
                "text": chunk_text,
                "page_numbers": page_numbers,
                "bounding_boxes": bounding_boxes,
            }
        )

    return chunks


def chunk_partition_texts(
    partitions: Iterable[Mapping[str, Any]],
    *,
    chunk_size_chars: int,
    chunk_overlap_chars: int,
) -> list[str]:
    """Create overlapping text chunks from partition text."""
    chunks = build_partition_chunks(
        partitions,
        chunk_size_chars=chunk_size_chars,
        chunk_overlap_chars=chunk_overlap_chars,
    )
    return [str(chunk.get("text", "")) for chunk in chunks if str(chunk.get("text", ""))]


def _extract_first_page_partitions(partitions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return partitions likely to belong to the first page/front matter."""
    with_page_numbers: list[tuple[int, dict[str, Any]]] = []
    for partition in partitions:
        page_number = extract_partition_page_number(partition)
        if page_number is None:
            continue
        with_page_numbers.append((page_number, partition))

    if with_page_numbers:
        first_page = [partition for page, partition in with_page_numbers if page == 1]
        if first_page:
            return first_page

    return partitions[:8]


def _normalize_doi(value: str) -> str:
    """Normalize DOI values by stripping punctuation noise."""
    return value.strip().rstrip(".);,")


def _extract_isbn(text: str) -> str:
    """Extract the first plausible ISBN string from text."""
    for match in ISBN_PATTERN.finditer(text):
        raw = match.group(0)
        normalized = re.sub(r"[^0-9Xx]", "", raw)
        if len(normalized) in {10, 13}:
            return normalized.upper()
    return ""


def _normalize_pdf_metadata_value(value: Any) -> str:
    """Normalize one PDF metadata field value to a non-empty string."""
    if value is None:
        return ""
    return str(value).strip()


def extract_pdf_title_author(document_bytes: bytes) -> dict[str, str]:
    """Extract title/author values from embedded PDF metadata when available.

    Args:
        document_bytes: Raw PDF file bytes.

    Returns:
        Dictionary containing optional ``title`` and ``author`` keys.
    """
    if not document_bytes:
        return {}

    try:
        from pypdf import PdfReader
    except ImportError:
        return {}

    try:
        reader = PdfReader(io.BytesIO(document_bytes))
    except Exception:
        return {}

    raw_metadata = getattr(reader, "metadata", None)
    if raw_metadata is None:
        return {}

    title = _normalize_pdf_metadata_value(getattr(raw_metadata, "title", None))
    author = _normalize_pdf_metadata_value(getattr(raw_metadata, "author", None))

    if isinstance(raw_metadata, Mapping):
        if not title:
            title = _normalize_pdf_metadata_value(raw_metadata.get("/Title"))
        if not author:
            author = _normalize_pdf_metadata_value(raw_metadata.get("/Author"))

    extracted: dict[str, str] = {}
    if title:
        extracted["title"] = title
    if author:
        extracted["author"] = author
    return extracted


def extract_metadata_from_partitions(
    *,
    partitions: list[dict[str, Any]],
    file_path: str,
    document_id: str,
    pdf_metadata: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Extract metadata using PDF metadata + first-page text identifiers.

    Title and author are read from PDF metadata when available. DOI and ISBN
    are extracted from first-page partitions only.
    """
    metadata = build_default_metadata(file_path, document_id)
    if pdf_metadata:
        pdf_title = str(pdf_metadata.get("title", "")).strip()
        pdf_author = str(pdf_metadata.get("author", "")).strip()
        if pdf_title:
            metadata["title"] = pdf_title
        if pdf_author:
            metadata["author"] = pdf_author

    first_page_partitions = _extract_first_page_partitions(partitions)

    first_page_text = "\n".join(
        text
        for text in (
            extract_partition_text(partition) for partition in first_page_partitions
        )
        if text
    )

    doi_match = DOI_PATTERN.search(first_page_text)
    if doi_match:
        metadata["doi"] = _normalize_doi(doi_match.group(0))

    extracted_isbn = _extract_isbn(first_page_text)
    if extracted_isbn:
        metadata["isbn"] = extracted_isbn

    return metadata


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

    def _partition_payload_key(self, partition_key: str) -> str:
        return f"{self._prefix}:partition:{partition_key}"

    def _metadata_payload_key(self, meta_key: str) -> str:
        return f"{self._prefix}:meta:{meta_key}"

    def _source_mapping_key(self, bucket_name: str, object_name: str) -> str:
        return f"{self._prefix}:source:{self._location_reference(bucket_name, object_name)}"

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
            self._redis.delete(self._metadata_payload_key(location_reference))

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
        metadata: Mapping[str, str],
    ) -> None:
        """Store normalized metadata payload for a source-scoped metadata key."""
        normalized = normalize_metadata(metadata)
        payload = {field_name: normalized.get(field_name, "") for field_name in METADATA_FIELDS}
        payload["document_id"] = document_id
        payload["source"] = meta_key
        payload["updated_at"] = utc_now_iso()
        self._redis.hset(self._metadata_payload_key(meta_key), mapping=payload)

    def get_metadata_by_key(self, meta_key: str) -> dict[str, str]:
        """Return metadata payload by metadata hash key."""
        if not meta_key:
            return {field_name: "" for field_name in METADATA_FIELDS}
        raw_mapping = self._redis.hgetall(self._metadata_payload_key(meta_key))
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
        metadata: Mapping[str, str],
    ) -> str:
        """Store metadata for one source location and return its metadata key."""
        normalized = normalize_metadata(metadata)
        if not normalized.get("citation_key"):
            normalized["citation_key"] = (
                slugify(PurePosixPath(object_name).stem) or document_id[:12]
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
            if self._redis.hlen(self._metadata_payload_key(meta_key)) <= 0:
                continue
            return meta_key, self.get_metadata_by_key(meta_key)

        state = self.get_state(document_id)
        state_meta_key = state.get("meta_key", "")
        if state_meta_key and self._redis.hlen(self._metadata_payload_key(state_meta_key)) > 0:
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
            for field_name in ("title", "author", "doi", "isbn")
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
        metadata: Mapping[str, str],
    ) -> dict[str, str]:
        """Update metadata for all locations in a bucket/document pair."""
        _, existing = self.get_document_metadata(bucket_name, document_id)
        merged = normalize_metadata(existing)
        for key, value in metadata.items():
            field_name = str(key).strip().lower()
            if field_name in METADATA_FIELDS:
                merged[field_name] = str(value).strip()
        merged["document_type"] = merged.get("document_type", "") or "misc"

        object_names = self.get_document_object_names(bucket_name, document_id)
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
        self._redis.set(self._partition_payload_key(partition_key), canonical_json(partitions))
        self.set_state(
            document_id,
            {
                "partition_key": partition_key,
                "partitions_count": len(partitions),
            },
        )
        return partition_key

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
        return self.get_partitions_by_key(partition_key)

    def get_partitions_by_key(self, partition_key: str) -> list[dict[str, Any]]:
        """Read partition payload from a partition hash key."""
        if not partition_key:
            return []
        key = self._partition_payload_key(partition_key)
        return self._parse_partitions_payload(self._redis.get(key))

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
            self._redis.delete(self._metadata_payload_key(location_reference))
            self._redis.srem(self._source_bucket_key(bucket_name), location_reference)
            self._redis.srem(self._document_sources_key(document_id), location_reference)

        remaining_locations = self._redis.smembers(self._document_sources_key(document_id))
        if not remaining_locations:
            self._redis.delete(self._document_sources_key(document_id))

        self.set_state(document_id, {"meta_key": ""})

        if not keep_partitions:
            if partition_key:
                self._redis.delete(self._partition_payload_key(partition_key))
            self.set_state(
                document_id,
                {
                    "partition_key": "",
                    "partitions_count": 0,
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
            "bibtex_fields": {
                field_name: metadata.get(field_name, "")
                for field_name in BIBTEX_FIELDS
            },
        }
        for field_name in BIBTEX_FIELDS:
            record[field_name] = metadata.get(field_name, "")
        return record


class QdrantIndexer:
    """Qdrant upsert/search/delete operations with dense and keyword vectors."""

    def __init__(
        self,
        *,
        qdrant_client: Any,
        fastembed_model: str,
        fastembed_keyword_model: str,
        collection_prefix: str,
    ) -> None:
        """Initialize indexer with a Qdrant client and embedding model name."""
        self._qdrant_client = qdrant_client
        self._fastembed_model = fastembed_model
        self._fastembed_keyword_model = fastembed_keyword_model
        self._collection_prefix = collection_prefix
        self._embedder: Any | None = None
        self._keyword_embedder: Any | None = None
        self._dense_vector_name = "dense"
        self._keyword_vector_name = "keyword"

    def _collection_name(self, bucket_name: str) -> str:
        normalized_bucket = re.sub(r"[^a-zA-Z0-9_]+", "_", bucket_name).strip("_").lower()
        if not normalized_bucket:
            normalized_bucket = "default"
        return f"{self._collection_prefix}_{normalized_bucket}"

    def _get_embedder(self) -> Any:
        if self._embedder is None:
            from fastembed import TextEmbedding

            self._embedder = TextEmbedding(model_name=self._fastembed_model)
        return self._embedder

    def _get_keyword_embedder(self) -> Any:
        if self._keyword_embedder is None:
            from fastembed import SparseTextEmbedding

            self._keyword_embedder = SparseTextEmbedding(model_name=self._fastembed_keyword_model)
        return self._keyword_embedder

    @staticmethod
    def _coerce_sparse_embedding(embedding: Any) -> tuple[list[int], list[float]]:
        """Normalize sparse embedding payload into aligned index/value lists."""
        raw_indices: Any = None
        raw_values: Any = None

        if isinstance(embedding, Mapping):
            raw_indices = embedding.get("indices")
            raw_values = embedding.get("values")
        else:
            raw_indices = getattr(embedding, "indices", None)
            raw_values = getattr(embedding, "values", None)

        if raw_indices is None or raw_values is None:
            return [], []

        try:
            indices = [int(value) for value in raw_indices]
            values = [float(value) for value in raw_values]
        except (TypeError, ValueError):
            return [], []

        size = min(len(indices), len(values))
        if size <= 0:
            return [], []
        return indices[:size], values[:size]

    def _build_sparse_vector(self, *, indices: list[int], values: list[float]) -> Any:
        """Create a Qdrant sparse vector payload, with compatibility fallback."""
        from qdrant_client import models as qdrant_models

        sparse_vector_cls = getattr(qdrant_models, "SparseVector", None)
        if sparse_vector_cls is None:
            return {"indices": indices, "values": values}
        return sparse_vector_cls(indices=indices, values=values)

    def _ensure_collection(self, collection_name: str, vector_size: int) -> None:
        from qdrant_client import models as qdrant_models

        collection_names = {
            str(collection.name) for collection in self._qdrant_client.get_collections().collections
        }
        if collection_name in collection_names:
            return

        dense_vectors_config = {
            self._dense_vector_name: qdrant_models.VectorParams(
                size=vector_size,
                distance=qdrant_models.Distance.COSINE,
            )
        }

        sparse_vectors_config: dict[str, Any] | None = None
        sparse_vector_params_cls = getattr(qdrant_models, "SparseVectorParams", None)
        if sparse_vector_params_cls is not None:
            sparse_vectors_config = {
                self._keyword_vector_name: sparse_vector_params_cls(),
            }

        try:
            if sparse_vectors_config is None:
                self._qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=dense_vectors_config,
                )
            else:
                self._qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=dense_vectors_config,
                    sparse_vectors_config=sparse_vectors_config,
                )
        except TypeError:
            # Older qdrant-client versions do not support sparse config kwargs.
            self._qdrant_client.create_collection(
                collection_name=collection_name,
                vectors_config=dense_vectors_config,
            )

    def _collection_exists(self, collection_name: str) -> bool:
        collection_names = {
            str(collection.name) for collection in self._qdrant_client.get_collections().collections
        }
        return collection_name in collection_names

    def ensure_bucket_collection(self, bucket_name: str) -> bool:
        """Ensure a Qdrant collection exists for a bucket."""
        collection_name = self._collection_name(bucket_name)
        if self._collection_exists(collection_name):
            return False

        embedder = self._get_embedder()
        embeddings = list(embedder.embed(["collection-dimension-probe"]))
        if not embeddings:
            raise RuntimeError("Could not determine FastEmbed vector dimension.")
        vector_size = len(embeddings[0])
        self._ensure_collection(collection_name, vector_size)
        return True

    def delete_bucket_collection(self, bucket_name: str) -> bool:
        """Delete the Qdrant collection for a bucket when it exists."""
        collection_name = self._collection_name(bucket_name)
        if not self._collection_exists(collection_name):
            return False
        self._qdrant_client.delete_collection(collection_name=collection_name)
        return True

    def purge_prefixed_collections(self) -> int:
        """Delete every collection that belongs to this application's prefix."""
        prefix = f"{self._collection_prefix}_"
        deleted = 0
        for collection in self._qdrant_client.get_collections().collections:
            collection_name = str(getattr(collection, "name", "")).strip()
            if not collection_name.startswith(prefix):
                continue
            self._qdrant_client.delete_collection(collection_name=collection_name)
            deleted += 1
        return deleted

    def delete_document(self, bucket_name: str, document_id: str) -> None:
        """Remove all chunk vectors for one document from Qdrant."""
        from qdrant_client import models as qdrant_models

        collection_name = self._collection_name(bucket_name)
        if not self._collection_exists(collection_name):
            return

        self._qdrant_client.delete(
            collection_name=collection_name,
            points_selector=qdrant_models.FilterSelector(
                filter=qdrant_models.Filter(
                    must=[
                        qdrant_models.FieldCondition(
                            key="document_id",
                            match=qdrant_models.MatchValue(value=document_id),
                        )
                    ]
                )
            ),
        )

    @staticmethod
    def _extract_query_points(raw_response: Any) -> list[Any]:
        """Normalize query/search responses to a list of scored points."""
        if isinstance(raw_response, list):
            return list(raw_response)
        points = getattr(raw_response, "points", None)
        if isinstance(points, list):
            return points
        result = getattr(raw_response, "result", None)
        if isinstance(result, list):
            return result
        return []

    def _search_dense(
        self,
        *,
        collection_name: str,
        query_vector: list[float],
        limit: int,
    ) -> list[Any]:
        """Run a dense-vector search query against Qdrant."""
        if hasattr(self._qdrant_client, "query_points"):
            try:
                response = self._qdrant_client.query_points(
                    collection_name=collection_name,
                    query=query_vector,
                    using=self._dense_vector_name,
                    limit=limit,
                    with_payload=True,
                    with_vectors=False,
                )
                return self._extract_query_points(response)
            except TypeError:
                pass

        if hasattr(self._qdrant_client, "search"):
            response = self._qdrant_client.search(
                collection_name=collection_name,
                query_vector=(self._dense_vector_name, query_vector),
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            return self._extract_query_points(response)

        raise RuntimeError("The configured Qdrant client does not support search operations.")

    def _search_keyword(
        self,
        *,
        collection_name: str,
        sparse_query: Any,
        limit: int,
    ) -> list[Any]:
        """Run a sparse keyword-vector search query against Qdrant."""
        if hasattr(self._qdrant_client, "query_points"):
            try:
                response = self._qdrant_client.query_points(
                    collection_name=collection_name,
                    query=sparse_query,
                    using=self._keyword_vector_name,
                    limit=limit,
                    with_payload=True,
                    with_vectors=False,
                )
                return self._extract_query_points(response)
            except TypeError:
                pass

        if hasattr(self._qdrant_client, "search"):
            response = self._qdrant_client.search(
                collection_name=collection_name,
                query_vector=(self._keyword_vector_name, sparse_query),
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            return self._extract_query_points(response)

        raise RuntimeError("The configured Qdrant client does not support search operations.")

    @staticmethod
    def _normalize_point_id(point: Any, fallback_index: int) -> str:
        point_id = getattr(point, "id", None)
        if point_id is None:
            return f"point-{fallback_index}"
        return str(point_id)

    @staticmethod
    def _normalize_payload(value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            return {}
        return {str(key): payload_value for key, payload_value in value.items()}

    def _format_result_payload(
        self,
        *,
        point_id: str,
        payload: Mapping[str, Any],
        raw_score: Any,
    ) -> dict[str, Any]:
        try:
            score = float(raw_score)
        except (TypeError, ValueError):
            score = 0.0
        return {
            "id": point_id,
            "score": score,
            "document_id": str(payload.get("document_id", "")),
            "file_path": str(payload.get("file_path", "")),
            "chunk_index": int(payload.get("chunk_index", 0) or 0),
            "text": str(payload.get("text", "")),
            "page_numbers": payload.get("page_numbers", []),
            "resolver_url": str(payload.get("resolver_url", "")),
            "minio_location": str(payload.get("minio_location", "")),
            "meta_key": str(payload.get("meta_key", "")),
            "partition_key": str(payload.get("partition_key", "")),
        }

    def _format_result_point(self, point: Any, *, fallback_rank: int) -> dict[str, Any]:
        return self._format_result_payload(
            point_id=self._normalize_point_id(point, fallback_rank),
            payload=self._normalize_payload(getattr(point, "payload", {})),
            raw_score=getattr(point, "score", 0.0),
        )

    def _rrf(
        self,
        *,
        semantic_points: list[Any],
        keyword_points: list[Any],
        rrf_k: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Merge two ranked result sets using reciprocal rank fusion (RRF)."""
        rank_constant = float(max(1, int(rrf_k)))
        merged: dict[str, dict[str, Any]] = {}

        for rank, point in enumerate(semantic_points):
            point_id = self._normalize_point_id(point, rank)
            payload = self._normalize_payload(getattr(point, "payload", {}))
            entry = merged.setdefault(
                point_id,
                {
                    "point": point,
                    "payload": payload,
                    "score": 0.0,
                },
            )
            entry["score"] += 1.0 / (rank_constant + rank + 1.0)

        for rank, point in enumerate(keyword_points):
            point_id = self._normalize_point_id(point, rank)
            payload = self._normalize_payload(getattr(point, "payload", {}))
            entry = merged.setdefault(
                point_id,
                {
                    "point": point,
                    "payload": payload,
                    "score": 0.0,
                },
            )
            entry["score"] += 1.0 / (rank_constant + rank + 1.0)

        ranked_entries = sorted(
            merged.values(),
            key=lambda item: float(item["score"]),
            reverse=True,
        )
        results: list[dict[str, Any]] = []
        for index, entry in enumerate(ranked_entries[:limit]):
            point = entry["point"]
            results.append(
                self._format_result_payload(
                    point_id=self._normalize_point_id(point, index),
                    payload=self._normalize_payload(entry["payload"]),
                    raw_score=float(entry["score"]),
                )
            )
        return results

    def search_chunks(
        self,
        *,
        bucket_name: str,
        query: str,
        limit: int,
        mode: str,
        rrf_k: int = 60,
    ) -> list[dict[str, Any]]:
        """Search chunk payloads in Qdrant using semantic, keyword, or hybrid mode."""
        normalized_query = query.strip()
        if not normalized_query:
            raise ValueError("query must not be empty.")

        normalized_mode = mode.strip().lower()
        if normalized_mode not in SEARCH_MODES:
            raise ValueError(f"mode must be one of: {', '.join(SEARCH_MODES)}")

        resolved_limit = max(1, min(int(limit), 100))
        collection_name = self._collection_name(bucket_name)
        if not self._collection_exists(collection_name):
            return []

        dense_embedder = self._get_embedder()
        dense_embeddings = list(dense_embedder.embed([normalized_query]))
        if not dense_embeddings:
            return []
        dense_query = [float(value) for value in dense_embeddings[0]]

        keyword_embedder = self._get_keyword_embedder()
        keyword_embeddings = list(keyword_embedder.embed([normalized_query]))
        if not keyword_embeddings:
            return []
        sparse_indices, sparse_values = self._coerce_sparse_embedding(keyword_embeddings[0])
        has_sparse_query = bool(sparse_indices and sparse_values)
        if normalized_mode == "keyword" and not has_sparse_query:
            return []
        sparse_query = self._build_sparse_vector(indices=sparse_indices, values=sparse_values)

        if normalized_mode == "semantic":
            points = self._search_dense(
                collection_name=collection_name,
                query_vector=dense_query,
                limit=resolved_limit,
            )
            return [
                self._format_result_point(point, fallback_rank=index)
                for index, point in enumerate(points)
            ]

        if normalized_mode == "keyword":
            points = self._search_keyword(
                collection_name=collection_name,
                sparse_query=sparse_query,
                limit=resolved_limit,
            )
            return [
                self._format_result_point(point, fallback_rank=index)
                for index, point in enumerate(points)
            ]

        if not has_sparse_query:
            semantic_points = self._search_dense(
                collection_name=collection_name,
                query_vector=dense_query,
                limit=resolved_limit,
            )
            return [
                self._format_result_point(point, fallback_rank=index)
                for index, point in enumerate(semantic_points)
            ]

        prefetch_limit = min(200, max(10, resolved_limit * 4))
        semantic_points = self._search_dense(
            collection_name=collection_name,
            query_vector=dense_query,
            limit=prefetch_limit,
        )
        keyword_points = self._search_keyword(
            collection_name=collection_name,
            sparse_query=sparse_query,
            limit=prefetch_limit,
        )
        return self._rrf(
            semantic_points=semantic_points,
            keyword_points=keyword_points,
            rrf_k=rrf_k,
            limit=resolved_limit,
        )

    def upsert_document_chunks(
        self,
        *,
        bucket_name: str,
        document_id: str,
        file_path: str,
        chunks: list[dict[str, Any]],
        partition_key: str,
        meta_key: str,
    ) -> None:
        """Embed chunk text and upsert vectors into Qdrant."""
        if not chunks:
            return

        texts = [str(chunk.get("text", "")) for chunk in chunks]
        if not any(texts):
            return

        embedder = self._get_embedder()
        embeddings = list(embedder.embed(texts))
        if not embeddings:
            return
        keyword_embedder = self._get_keyword_embedder()
        sparse_embeddings = list(keyword_embedder.embed(texts))
        if sparse_embeddings and len(sparse_embeddings) != len(embeddings):
            sparse_embeddings = sparse_embeddings[: len(embeddings)]

        first_vector = embeddings[0]
        vector_size = len(first_vector)
        collection_name = self._collection_name(bucket_name)
        self._ensure_collection(collection_name, vector_size)
        self.delete_document(bucket_name, document_id)

        from qdrant_client import models as qdrant_models

        resolver_url = build_resolver_url(bucket_name, file_path)
        minio_location = f"{bucket_name}/{file_path}"

        points: list[qdrant_models.PointStruct] = []
        for index, (chunk, embedding) in enumerate(zip(chunks, embeddings, strict=False)):
            chunk_index = int(chunk.get("chunk_index", len(points)))
            raw_page_numbers = chunk.get("page_numbers", [])
            page_numbers: list[int] = []
            if isinstance(raw_page_numbers, list):
                for value in raw_page_numbers:
                    try:
                        page_number = int(value)
                    except (TypeError, ValueError):
                        continue
                    if page_number > 0 and page_number not in page_numbers:
                        page_numbers.append(page_number)

            raw_bounding_boxes = chunk.get("bounding_boxes", [])
            bounding_boxes: list[dict[str, Any]] = []
            if isinstance(raw_bounding_boxes, list):
                for value in raw_bounding_boxes:
                    if not isinstance(value, Mapping):
                        continue
                    points_payload = _normalize_coordinate_points(value.get("points"))
                    if not points_payload:
                        continue
                    payload_box: dict[str, Any] = {"points": points_payload}

                    raw_page_number = value.get("page_number")
                    if raw_page_number is not None:
                        bbox_page_number: int | None
                        try:
                            bbox_page_number = int(raw_page_number)
                        except (TypeError, ValueError):
                            bbox_page_number = None
                        if bbox_page_number is not None and bbox_page_number > 0:
                            payload_box["page_number"] = bbox_page_number

                    raw_layout_width = value.get("layout_width")
                    if isinstance(raw_layout_width, (int, float)):
                        payload_box["layout_width"] = float(raw_layout_width)
                    raw_layout_height = value.get("layout_height")
                    if isinstance(raw_layout_height, (int, float)):
                        payload_box["layout_height"] = float(raw_layout_height)

                    raw_system = value.get("system")
                    if isinstance(raw_system, str):
                        normalized_system = raw_system.strip()
                        if normalized_system:
                            payload_box["system"] = normalized_system

                    bounding_boxes.append(payload_box)

            point_id = compute_chunk_point_id(
                bucket_name=bucket_name,
                document_id=document_id,
                chunk_index=chunk_index,
            )
            vector = [float(value) for value in embedding]
            named_vector_payload: dict[str, Any] = {self._dense_vector_name: vector}
            if index < len(sparse_embeddings):
                sparse_indices, sparse_values = self._coerce_sparse_embedding(
                    sparse_embeddings[index]
                )
                if sparse_indices and sparse_values:
                    named_vector_payload[self._keyword_vector_name] = self._build_sparse_vector(
                        indices=sparse_indices,
                        values=sparse_values,
                    )
            payload = {
                "bucket_name": bucket_name,
                "document_id": document_id,
                "partition_key": partition_key,
                "meta_key": meta_key,
                "file_path": file_path,
                "minio_location": minio_location,
                "resolver_url": resolver_url,
                "chunk_index": chunk_index,
                "text": str(chunk.get("text", "")),
                "page_numbers": page_numbers,
                "bounding_boxes": bounding_boxes,
            }
            points.append(
                qdrant_models.PointStruct(
                    id=point_id,
                    vector=named_vector_payload,
                    payload=payload,
                )
            )

        if points:
            self._qdrant_client.upsert(collection_name=collection_name, points=points, wait=True)


class IngestionService:
    """High-level ingestion operations shared by API handlers and Celery tasks.

    The service coordinates MinIO object storage, Unstructured partitioning,
    Redis state/metadata persistence, and Qdrant vector indexing. Partitioning
    and chunking/indexing are exposed as separate stage methods so workflows can
    rerun chunking independently from partition extraction.
    """

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
        return self._repository.list_documents(bucket_name)

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
            if not meta_key:
                resolved_meta_key, _ = self._repository.get_document_metadata(
                    bucket_name, document_id
                )
                meta_key = resolved_meta_key

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
            self._set_state(
                document_id,
                processing_state="processing",
                processing_progress=90,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                chunks_count=len(chunks),
                error="",
            )

            self._qdrant_indexer.upsert_document_chunks(
                bucket_name=bucket_name,
                document_id=document_id,
                file_path=object_name,
                chunks=chunks,
                partition_key=partition_key,
                meta_key=meta_key,
            )
            self._set_state(
                document_id,
                processing_state="processed",
                processing_progress=100,
                partition_key=partition_key,
                meta_key=meta_key,
                partitions_count=len(partitions),
                chunks_count=len(chunks),
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

    def update_metadata(
        self,
        *,
        bucket_name: str,
        document_id: str,
        metadata: Mapping[str, str],
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
                normalized[field_name] = str(value).strip()

        if "document_type" in normalized:
            normalized["document_type"] = normalized["document_type"] or "misc"

        return self._repository.update_document_metadata(
            bucket_name=bucket_name,
            document_id=document_id,
            metadata=normalized,
        )

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
        return self._qdrant_indexer.search_chunks(
            bucket_name=bucket_name,
            query=query,
            limit=limit,
            mode=mode,
            rrf_k=rrf_k,
        )


def build_ingestion_settings(env: Mapping[str, str] | None = None) -> IngestionSettings:
    """Build ingestion settings from environment variables.

    Args:
        env: Optional environment mapping. Defaults to ``os.environ``.

    Returns:
        Parsed ingestion settings with defaults for unset variables.
    """
    source = os.environ if env is None else env
    minio_settings = build_minio_settings(source)

    def _safe_int(name: str, default: int) -> int:
        raw_value = source.get(name, str(default))
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return default

    def _safe_float(name: str, default: float) -> float:
        raw_value = source.get(name, str(default))
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return default

    return IngestionSettings(
        minio=minio_settings,
        redis_url=source.get("REDIS_URL", "redis://localhost:6379/2"),
        redis_prefix=source.get("REDIS_PREFIX", "evidencebase"),
        qdrant_url=source.get("QDRANT_URL", "http://localhost:6333"),
        qdrant_api_key=source.get("QDRANT_API_KEY") or None,
        qdrant_collection_prefix=source.get("QDRANT_COLLECTION_PREFIX", "evidencebase"),
        unstructured_api_url=source.get(
            "UNSTRUCTURED_API_URL", "https://api.unstructuredapp.io/general/v0/general"
        ),
        unstructured_api_key=source.get("UNSTRUCTURED_API_KEY") or None,
        unstructured_strategy=source.get("UNSTRUCTURED_STRATEGY", "auto"),
        unstructured_timeout_seconds=max(
            5.0, _safe_float("UNSTRUCTURED_TIMEOUT_SECONDS", 300.0)
        ),
        fastembed_model=source.get("FASTEMBED_MODEL", "BAAI/bge-small-en-v1.5"),
        fastembed_keyword_model=source.get("FASTEMBED_KEYWORD_MODEL", "Qdrant/bm25"),
        chunk_size_chars=_safe_int("CHUNK_SIZE_CHARS", 1200),
        chunk_overlap_chars=_safe_int("CHUNK_OVERLAP_CHARS", 150),
        scan_interval_seconds=max(5, _safe_int("MINIO_SCAN_INTERVAL_SECONDS", 15)),
    )


def build_ingestion_service(settings: IngestionSettings | None = None) -> IngestionService:
    """Build a fully configured ingestion service.

    Args:
        settings: Optional precomputed settings object.

    Returns:
        Ready-to-use ingestion service with MinIO, Redis, and Qdrant clients.
    """
    resolved_settings = settings or build_ingestion_settings()

    minio_client = Minio(
        resolved_settings.minio.endpoint,
        access_key=resolved_settings.minio.access_key,
        secret_key=resolved_settings.minio.secret_key,
        secure=resolved_settings.minio.secure,
        region=resolved_settings.minio.region,
    )

    import redis
    from qdrant_client import QdrantClient

    redis_client = redis.Redis.from_url(resolved_settings.redis_url, decode_responses=True)
    repository = RedisDocumentRepository(
        redis_client=redis_client,
        key_prefix=resolved_settings.redis_prefix,
    )
    partition_client = UnstructuredPartitionClient(
        api_url=resolved_settings.unstructured_api_url,
        api_key=resolved_settings.unstructured_api_key,
        strategy=resolved_settings.unstructured_strategy,
        timeout_seconds=resolved_settings.unstructured_timeout_seconds,
    )
    qdrant_client = QdrantClient(
        url=resolved_settings.qdrant_url,
        api_key=resolved_settings.qdrant_api_key,
    )
    qdrant_indexer = QdrantIndexer(
        qdrant_client=qdrant_client,
        fastembed_model=resolved_settings.fastembed_model,
        fastembed_keyword_model=resolved_settings.fastembed_keyword_model,
        collection_prefix=resolved_settings.qdrant_collection_prefix,
    )
    return IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=partition_client,
        qdrant_indexer=qdrant_indexer,
        chunk_size_chars=resolved_settings.chunk_size_chars,
        chunk_overlap_chars=resolved_settings.chunk_overlap_chars,
    )
