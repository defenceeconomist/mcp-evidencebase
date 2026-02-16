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
from difflib import SequenceMatcher
from pathlib import PurePosixPath
from typing import Any, Protocol
from urllib.parse import quote

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
    "issn",
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

AUTHORS_METADATA_FIELD = "authors"
METADATA_FIELDS: tuple[str, ...] = (
    *BIBTEX_FIELDS,
    "document_type",
    "citation_key",
    AUTHORS_METADATA_FIELD,
)

DOI_PATTERN = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)
ISBN_PATTERN = re.compile(r"\b(?:97[89][-\s]?)?(?:\d[-\s]?){9}[\dXx]\b")
ISSN_PATTERN = re.compile(
    r"\b(?:e[-\s]?issn|p[-\s]?issn|issn)\s*[:#]?\s*(\d{4}[-\s]?\d{3}[\dXx])\b",
    re.IGNORECASE,
)
SEARCH_MODES: tuple[str, ...] = ("semantic", "keyword", "hybrid")
CROSSREF_API_BASE_URL = "https://api.crossref.org"
CROSSREF_CONFIDENCE_THRESHOLD = 0.92
CROSSREF_MAX_RESULTS = 8
CROSSREF_TIMEOUT_SECONDS = 12.0
MONTH_ABBREVIATIONS: tuple[str, ...] = (
    "",
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
)
CROSSREF_TYPE_TO_DOCUMENT_TYPE: dict[str, str] = {
    "journal-article": "article",
    "journal-volume": "article",
    "journal-issue": "article",
    "book": "book",
    "edited-book": "book",
    "reference-book": "book",
    "monograph": "book",
    "book-chapter": "incollection",
    "book-part": "incollection",
    "book-section": "incollection",
    "reference-entry": "incollection",
    "proceedings-article": "inproceedings",
    "proceedings": "proceedings",
    "report": "techreport",
    "report-component": "techreport",
    "dissertation": "phdthesis",
    "standard": "manual",
    "posted-content": "unpublished",
}


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


def _normalize_author_entries(value: Any) -> list[dict[str, str]]:
    """Normalize structured author entries to ``[{first_name,last_name,suffix}, ...]``."""
    payload: Any = value
    if isinstance(payload, str):
        stripped = payload.strip()
        if not stripped:
            return []
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            return []

    if not isinstance(payload, list):
        return []

    normalized: list[dict[str, str]] = []
    for entry in payload:
        if not isinstance(entry, Mapping):
            continue
        first_name = str(
            entry.get("first_name")
            or entry.get("firstName")
            or entry.get("first")
            or entry.get("given")
            or ""
        ).strip()
        last_name = str(
            entry.get("last_name")
            or entry.get("lastName")
            or entry.get("last")
            or entry.get("family")
            or ""
        ).strip()
        suffix = str(entry.get("suffix") or entry.get("suffix_name") or "").strip()
        if not first_name and not last_name and not suffix:
            continue
        normalized.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "suffix": suffix,
            }
        )
    return normalized


def _serialize_author_entries(value: Any) -> str:
    """Serialize normalized author entries into deterministic JSON text."""
    normalized = _normalize_author_entries(value)
    if not normalized:
        return ""
    return canonical_json(normalized)


def compute_metadata_key(metadata: Mapping[str, Any]) -> str:
    """Return a deterministic metadata key for normalized metadata values."""
    payload: dict[str, str] = {}
    for field_name in METADATA_FIELDS:
        if field_name == AUTHORS_METADATA_FIELD:
            payload[field_name] = _serialize_author_entries(metadata.get(field_name, ""))
        else:
            payload[field_name] = str(metadata.get(field_name, ""))
    return compute_hash_for_value(payload)


def build_resolver_url(
    bucket_name: str,
    object_name: str,
    *,
    page_start: int | None = None,
) -> str:
    """Build a resolver URL with an optional page number deep link."""
    normalized_bucket = bucket_name.strip()
    normalized_object = object_name.strip().lstrip("/")
    resolved_page: int | None = None
    if page_start is not None:
        try:
            candidate_page = int(page_start)
        except (TypeError, ValueError):
            candidate_page = 0
        if candidate_page > 0:
            resolved_page = candidate_page
    page_value = str(resolved_page) if resolved_page is not None else ""
    return f"docs://{normalized_bucket}/{normalized_object}?page={page_value}"


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
    metadata[AUTHORS_METADATA_FIELD] = ""
    return metadata


def normalize_metadata(metadata: Mapping[str, Any]) -> dict[str, str]:
    """Normalize metadata into the Redis schema with fixed field names."""
    normalized = {field_name: "" for field_name in METADATA_FIELDS}
    for key, value in metadata.items():
        field_name = str(key).strip().lower()
        if field_name in normalized:
            if field_name == AUTHORS_METADATA_FIELD:
                normalized[field_name] = _serialize_author_entries(value)
            else:
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


@dataclass(frozen=True)
class _NormalizedElement:
    """Normalized Unstructured element used by the chunking pipeline."""

    element_id: str
    text: str
    raw_type: str
    kind: str
    page_number: int | None
    coordinates: dict[str, Any] | None
    filename: str | None


def _safe_int(value: Any) -> int | None:
    """Convert value to positive integer when possible."""
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return None
    if resolved < 1:
        return None
    return resolved


def _safe_non_empty_string(value: Any) -> str | None:
    """Convert value to stripped string and return ``None`` when empty."""
    if value is None:
        return None
    resolved = str(value).strip()
    return resolved or None


def _normalize_element_type(element: Mapping[str, Any]) -> tuple[str, str]:
    """Return the raw element type and normalized chunking kind."""
    raw_type = _safe_non_empty_string(element.get("type")) or _safe_non_empty_string(
        element.get("category")
    )
    resolved_type = raw_type or "Text"
    lowered = resolved_type.lower()
    if lowered in {
        "header",
        "footer",
        "pageheader",
        "pagefooter",
        "page-header",
        "page-footer",
    }:
        return resolved_type, "excluded"
    if "image" in lowered:
        return resolved_type, "excluded"
    if lowered in {
        "uncategorizedtext",
        "uncategorized_text",
    }:
        return resolved_type, "excluded"
    if lowered == "title":
        return resolved_type, "title"
    if "table" in lowered:
        return resolved_type, "table"
    return resolved_type, "text"


def _stable_fallback_element_id(
    *,
    raw_type: str,
    page_number: int | None,
    text: str,
) -> str:
    """Build deterministic element ID fallback for records missing ``element_id``."""
    page_marker = str(page_number or 0)
    payload = f"{raw_type}|{page_marker}|{text[:200]}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def _normalize_chunking_elements(
    elements: Iterable[Mapping[str, Any]],
) -> list[_NormalizedElement]:
    """Normalize raw Unstructured elements into deterministic internal records."""
    normalized: list[_NormalizedElement] = []
    for raw_element in elements:
        text_value = raw_element.get("text")
        if not isinstance(text_value, str):
            continue
        text = text_value.strip()
        if not text:
            continue

        raw_type, kind = _normalize_element_type(raw_element)
        if kind == "excluded":
            continue
        metadata = raw_element.get("metadata")
        metadata_mapping = metadata if isinstance(metadata, Mapping) else {}

        page_number = _safe_int(metadata_mapping.get("page_number"))
        if page_number is None:
            page_number = _safe_int(raw_element.get("page_number"))

        element_id = _safe_non_empty_string(raw_element.get("element_id"))
        if element_id is None:
            element_id = _stable_fallback_element_id(
                raw_type=raw_type,
                page_number=page_number,
                text=text,
            )

        filename = _safe_non_empty_string(metadata_mapping.get("filename")) or (
            _safe_non_empty_string(raw_element.get("filename"))
        )
        coordinates = extract_partition_bounding_box(raw_element)

        normalized.append(
            _NormalizedElement(
                element_id=element_id,
                text=text,
                raw_type=raw_type,
                kind=kind,
                page_number=page_number,
                coordinates=coordinates,
                filename=filename,
            )
        )
    return normalized


def _build_trace_record(
    element: _NormalizedElement,
    *,
    text_len: int,
) -> dict[str, Any]:
    """Build minimal orig-element trace metadata for one chunk contribution."""
    trace: dict[str, Any] = {
        "element_id": element.element_id,
        "type": element.raw_type,
        "category": element.raw_type,
        "page_number": element.page_number,
        "text_len": int(text_len),
    }
    if element.coordinates is not None:
        trace["coordinates"] = dict(element.coordinates)
    return trace


def _coerce_chunk_page_numbers(orig_elements: list[dict[str, Any]]) -> list[int]:
    """Collect ordered, de-duplicated page numbers from trace records."""
    page_numbers: list[int] = []
    seen: set[int] = set()
    for record in orig_elements:
        page_number = _safe_int(record.get("page_number"))
        if page_number is None or page_number in seen:
            continue
        seen.add(page_number)
        page_numbers.append(page_number)
    return page_numbers


def _coerce_chunk_bounding_boxes(orig_elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collect ordered, de-duplicated normalized bounding boxes from trace records."""
    bounding_boxes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in orig_elements:
        raw_coordinates = record.get("coordinates")
        if not isinstance(raw_coordinates, Mapping):
            continue
        payload = {str(key): value for key, value in raw_coordinates.items()}
        page_number = _safe_int(record.get("page_number"))
        if page_number is not None:
            payload["page_number"] = page_number
        dedupe_key = canonical_json(payload)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        bounding_boxes.append(payload)
    return bounding_boxes


def _emit_chunk(
    *,
    chunk_type: str,
    text: str,
    section_title: str | None,
    contributions: list[tuple[_NormalizedElement, int]],
) -> dict[str, Any] | None:
    """Build one chunk payload from normalized element contributions."""
    resolved_text = text.strip()
    if not resolved_text:
        return None

    orig_elements = [
        _build_trace_record(element, text_len=text_len)
        for element, text_len in contributions
    ]
    if not orig_elements:
        return None

    filename: str | None = None
    for element, _ in contributions:
        if filename is None and element.filename:
            filename = element.filename
        if filename is not None:
            break

    page_numbers = _coerce_chunk_page_numbers(orig_elements)
    page_start = min(page_numbers) if page_numbers else None
    page_end = max(page_numbers) if page_numbers else None
    metadata: dict[str, Any] = {
        "page_start": page_start,
        "page_end": page_end,
        "section_title": section_title,
    }
    if filename is not None:
        metadata["filename"] = filename

    return {
        "chunk_id": "",
        "chunk_index": 0,
        "text": resolved_text,
        "type": chunk_type,
        "metadata": metadata,
        "orig_elements": orig_elements,
        "page_numbers": page_numbers,
        "bounding_boxes": _coerce_chunk_bounding_boxes(orig_elements),
    }


def _find_split_boundary(text: str, *, start: int, hard_end: int) -> int:
    """Find best split boundary before ``hard_end`` preferring paragraph/sentence breaks."""
    if hard_end >= len(text):
        return len(text)

    search_start = start + max(1, (hard_end - start) // 2)
    for delimiter in ("\n\n", "\n", ". ", "? ", "! ", "; ", ": ", ", ", " "):
        index = text.rfind(delimiter, search_start, hard_end)
        if index < 0:
            continue
        if delimiter in {". ", "? ", "! ", "; ", ": ", ", ", " "}:
            return index + 1
        return index + len(delimiter)
    return hard_end


def _split_oversized_text(
    *,
    text: str,
    max_characters: int,
    overlap_chars: int,
) -> list[str]:
    """Split oversized element text with optional overlap between internal slices."""
    if len(text) <= max_characters:
        return [text]

    chunks: list[str] = []
    cursor = 0
    limit = len(text)
    while cursor < limit:
        hard_end = min(cursor + max_characters, limit)
        end = _find_split_boundary(text, start=cursor, hard_end=hard_end)
        if end <= cursor:
            end = hard_end

        piece = text[cursor:end].strip()
        if piece:
            chunks.append(piece)
        if end >= limit:
            break
        cursor = max(end - overlap_chars, cursor + 1)
    return chunks


def _merge_text_chunks(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    max_characters: int,
) -> dict[str, Any] | None:
    """Merge two text chunks when safe under size constraints."""
    if left.get("type") != "text" or right.get("type") != "text":
        return None
    joined_text = f"{left.get('text', '').strip()}\n\n{right.get('text', '').strip()}".strip()
    if len(joined_text) > max_characters:
        return None

    left_metadata = left.get("metadata")
    right_metadata = right.get("metadata")
    left_meta = left_metadata if isinstance(left_metadata, Mapping) else {}
    right_meta = right_metadata if isinstance(right_metadata, Mapping) else {}
    merged_orig = [*left.get("orig_elements", []), *right.get("orig_elements", [])]
    orig_elements = [record for record in merged_orig if isinstance(record, Mapping)]
    resolved_orig = [{str(key): value for key, value in record.items()} for record in orig_elements]
    page_numbers = _coerce_chunk_page_numbers(resolved_orig)

    metadata: dict[str, Any] = {
        "page_start": min(page_numbers) if page_numbers else None,
        "page_end": max(page_numbers) if page_numbers else None,
        "section_title": left_meta.get("section_title", right_meta.get("section_title")),
    }
    filename = _safe_non_empty_string(left_meta.get("filename")) or _safe_non_empty_string(
        right_meta.get("filename")
    )
    if filename is not None:
        metadata["filename"] = filename

    merged_boxes = [
        box
        for box in [*left.get("bounding_boxes", []), *right.get("bounding_boxes", [])]
        if isinstance(box, Mapping)
    ]
    deduped_boxes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for box in merged_boxes:
        payload = {str(key): value for key, value in box.items()}
        dedupe_key = canonical_json(payload)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped_boxes.append(payload)

    return {
        "chunk_id": "",
        "chunk_index": 0,
        "text": joined_text,
        "type": "text",
        "metadata": metadata,
        "orig_elements": resolved_orig,
        "page_numbers": page_numbers,
        "bounding_boxes": deduped_boxes,
    }


def _consolidate_small_chunks(
    chunks: list[dict[str, Any]],
    *,
    combine_under_n_chars: int,
    max_characters: int,
) -> list[dict[str, Any]]:
    """Merge undersized neighboring text chunks while preserving table isolation."""
    if combine_under_n_chars <= 0:
        return chunks

    consolidated = list(chunks)
    index = 0
    while index < len(consolidated):
        chunk = consolidated[index]
        chunk_text = str(chunk.get("text", ""))
        if chunk.get("type") != "text" or len(chunk_text) >= combine_under_n_chars:
            index += 1
            continue

        chunk_metadata = chunk.get("metadata")
        metadata = chunk_metadata if isinstance(chunk_metadata, Mapping) else {}
        section_title = metadata.get("section_title")

        merged = False
        # Prefer same-section adjacency first (previous, then next for determinism).
        if index > 0:
            previous = consolidated[index - 1]
            previous_metadata = previous.get("metadata")
            previous_meta = previous_metadata if isinstance(previous_metadata, Mapping) else {}
            if (
                previous.get("type") == "text"
                and previous_meta.get("section_title") == section_title
            ):
                candidate = _merge_text_chunks(
                    previous,
                    chunk,
                    max_characters=max_characters,
                )
                if candidate is not None:
                    consolidated[index - 1] = candidate
                    del consolidated[index]
                    index = max(0, index - 1)
                    merged = True
        if merged:
            continue

        if index + 1 < len(consolidated):
            following = consolidated[index + 1]
            following_metadata = following.get("metadata")
            following_meta = following_metadata if isinstance(following_metadata, Mapping) else {}
            if (
                following.get("type") == "text"
                and following_meta.get("section_title") == section_title
            ):
                candidate = _merge_text_chunks(
                    chunk,
                    following,
                    max_characters=max_characters,
                )
                if candidate is not None:
                    consolidated[index] = candidate
                    del consolidated[index + 1]
                    merged = True
        if merged:
            continue

        # Fallback to next text chunk regardless of section.
        if index + 1 < len(consolidated):
            following = consolidated[index + 1]
            if following.get("type") == "text":
                candidate = _merge_text_chunks(
                    chunk,
                    following,
                    max_characters=max_characters,
                )
                if candidate is not None:
                    consolidated[index] = candidate
                    del consolidated[index + 1]
                    continue

        index += 1

    return consolidated


def _finalize_chunk_ids(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Assign deterministic chunk indexes and chunk IDs."""
    for index, chunk in enumerate(chunks):
        metadata = chunk.get("metadata")
        resolved_metadata = metadata if isinstance(metadata, Mapping) else {}
        source_marker = _safe_non_empty_string(resolved_metadata.get("filename"))
        orig_elements = chunk.get("orig_elements", [])
        first_element_id = ""
        last_element_id = ""
        if isinstance(orig_elements, list) and orig_elements:
            first = orig_elements[0]
            last = orig_elements[-1]
            if isinstance(first, Mapping):
                first_element_id = str(first.get("element_id", ""))
            if isinstance(last, Mapping):
                last_element_id = str(last.get("element_id", ""))
        section_title = _safe_non_empty_string(resolved_metadata.get("section_title")) or ""

        id_payload = "|".join(
            [
                source_marker or "",
                first_element_id,
                last_element_id,
                section_title,
                str(index),
            ]
        )
        chunk["chunk_index"] = index
        chunk["chunk_id"] = hashlib.sha256(id_payload.encode("utf-8")).hexdigest()[:24]
    return chunks


def chunk_unstructured_elements(
    elements: list[dict[str, Any]],
    **params: Any,
) -> list[dict[str, Any]]:
    """Chunk Unstructured JSON elements into deterministic, traceable records.

    Args:
        elements: Ordered Unstructured element dictionaries. Expected keys include
            ``text``, ``type`` or ``category``, optional ``element_id``, and optional
            ``metadata`` with ``page_number``/``coordinates``/``filename`` fields.
        **params: Optional chunking controls.
            ``max_characters`` (default ``2500``): Hard text cap per chunk.
            ``new_after_n_chars`` (default ``1500``): Soft target to start a new
                chunk when appending full elements.
            ``combine_under_n_chars`` (default ``500``): Post-pass merge threshold
                for undersized text chunks.
            ``overlap_chars`` (default ``120``): Overlap size used only when splitting
                one oversized element (text or table) internally.

    Returns:
        Deterministic chunk dictionaries containing:
            ``chunk_id``, ``text``, ``type`` (``text``/``table``), ``metadata``
            (including ``page_start``, ``page_end``, ``section_title``), plus
            ``orig_elements`` trace records.

    Guarantees:
        - Preserves source order and semantic element boundaries whenever possible.
        - Excludes header/footer/image elements from chunk text and trace records.
        - Uses Title elements as hard section boundaries.
        - Keeps table elements isolated from narrative text.
        - Avoids inter-chunk overlap except for oversized single-element splits.
        - Produces deterministic IDs and metadata for stable re-ingestion.
    """
    max_characters = max(1, int(params.get("max_characters", 2500)))
    new_after_n_chars = max(1, min(int(params.get("new_after_n_chars", 1500)), max_characters))
    combine_under_n_chars = max(0, int(params.get("combine_under_n_chars", 500)))
    overlap_chars = max(0, min(int(params.get("overlap_chars", 120)), max_characters - 1))

    normalized_elements = _normalize_chunking_elements(elements)
    if not normalized_elements:
        return []

    chunks: list[dict[str, Any]] = []
    current_elements: list[_NormalizedElement] = []
    current_length = 0
    section_title: str | None = None

    def flush_current_text_chunk() -> None:
        nonlocal current_elements
        nonlocal current_length
        if not current_elements:
            return
        text = "\n\n".join(element.text for element in current_elements)
        chunk = _emit_chunk(
            chunk_type="text",
            text=text,
            section_title=section_title,
            contributions=[(element, len(element.text)) for element in current_elements],
        )
        if chunk is not None:
            chunks.append(chunk)
        current_elements = []
        current_length = 0

    def push_text_element(element: _NormalizedElement) -> None:
        nonlocal current_elements
        nonlocal current_length
        element_length = len(element.text)

        if element_length > max_characters:
            flush_current_text_chunk()
            parts = _split_oversized_text(
                text=element.text,
                max_characters=max_characters,
                overlap_chars=overlap_chars,
            )
            for part in parts:
                chunk = _emit_chunk(
                    chunk_type="text",
                    text=part,
                    section_title=section_title,
                    contributions=[(element, len(part))],
                )
                if chunk is not None:
                    chunks.append(chunk)
            return

        candidate_length = element_length
        if current_elements:
            candidate_length += current_length + 2
            if current_length >= new_after_n_chars or candidate_length > max_characters:
                flush_current_text_chunk()

        current_elements.append(element)
        current_length = len("\n\n".join(item.text for item in current_elements))

    for element in normalized_elements:
        if element.kind == "title":
            flush_current_text_chunk()
            section_title = element.text
            continue

        if element.kind == "table":
            flush_current_text_chunk()
            table_text = element.text
            if len(table_text) <= max_characters:
                chunk = _emit_chunk(
                    chunk_type="table",
                    text=table_text,
                    section_title=section_title,
                    contributions=[(element, len(table_text))],
                )
                if chunk is not None:
                    chunks.append(chunk)
                continue

            table_parts = _split_oversized_text(
                text=table_text,
                max_characters=max_characters,
                overlap_chars=overlap_chars,
            )
            for part in table_parts:
                chunk = _emit_chunk(
                    chunk_type="table",
                    text=part,
                    section_title=section_title,
                    contributions=[(element, len(part))],
                )
                if chunk is not None:
                    chunks.append(chunk)
            continue

        push_text_element(element)

    flush_current_text_chunk()
    consolidated = _consolidate_small_chunks(
        chunks,
        combine_under_n_chars=combine_under_n_chars,
        max_characters=max_characters,
    )
    return _finalize_chunk_ids(consolidated)


def build_partition_chunks(
    partitions: Iterable[Mapping[str, Any]],
    *,
    chunk_size_chars: int,
    chunk_overlap_chars: int,
) -> list[dict[str, Any]]:
    """Backwards-compatible wrapper around ``chunk_unstructured_elements``."""
    normalized: list[dict[str, Any]] = []
    for partition in partitions:
        if not isinstance(partition, Mapping):
            continue
        normalized.append({str(key): value for key, value in partition.items()})
    return chunk_unstructured_elements(
        normalized,
        max_characters=max(1, int(chunk_size_chars)),
        overlap_chars=max(0, int(chunk_overlap_chars)),
    )


def chunk_partition_texts(
    partitions: Iterable[Mapping[str, Any]],
    *,
    chunk_size_chars: int,
    chunk_overlap_chars: int,
) -> list[str]:
    """Create text chunks from partition payload entries."""
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


def _extract_issn(text: str) -> str:
    """Extract the first labeled ISSN value from text."""
    for match in ISSN_PATTERN.finditer(text):
        raw = match.group(1)
        normalized = re.sub(r"[^0-9Xx]", "", raw).upper()
        if len(normalized) == 8:
            return f"{normalized[:4]}-{normalized[4:]}"
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

    Title and author are read from PDF metadata when available. DOI, ISBN,
    and ISSN are extracted from first-page partitions only.
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

    extracted_issn = _extract_issn(first_page_text)
    if extracted_issn:
        metadata["issn"] = extracted_issn

    return metadata


def _normalize_doi_lookup_value(value: str) -> str:
    """Normalize DOI metadata values for Crossref lookup and comparisons."""
    normalized = str(value).strip()
    if not normalized:
        return ""
    normalized = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"^doi:\s*", "", normalized, flags=re.IGNORECASE)
    matched_doi = DOI_PATTERN.search(normalized)
    if matched_doi:
        return _normalize_doi(matched_doi.group(0))
    return _normalize_doi(normalized)


def _normalize_isbn_value(value: str) -> str:
    """Normalize ISBN values to compact uppercase text."""
    normalized = re.sub(r"[^0-9Xx]", "", str(value)).upper()
    if len(normalized) in {10, 13}:
        return normalized
    return ""


def _normalize_issn_value(value: str) -> str:
    """Normalize ISSN values to ``NNNN-NNNN`` format."""
    normalized = re.sub(r"[^0-9Xx]", "", str(value)).upper()
    if len(normalized) == 8:
        return f"{normalized[:4]}-{normalized[4:]}"
    return ""


def _normalize_title_for_match(value: str) -> str:
    """Normalize title text for fuzzy comparisons."""
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def _title_similarity(left: str, right: str) -> float:
    """Return title similarity ratio in ``[0, 1]``."""
    normalized_left = _normalize_title_for_match(left)
    normalized_right = _normalize_title_for_match(right)
    if not normalized_left or not normalized_right:
        return 0.0
    return float(SequenceMatcher(None, normalized_left, normalized_right).ratio())


def _crossref_first_text(value: Any) -> str:
    """Extract the first non-empty text value from list-or-string payloads."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                normalized = item.strip()
                if normalized:
                    return normalized
    return ""


def _crossref_extract_year_month(item: Mapping[str, Any]) -> tuple[str, str]:
    """Extract publication year/month from Crossref date-parts."""
    raw_issued = item.get("issued")
    if not isinstance(raw_issued, Mapping):
        return "", ""
    raw_date_parts = raw_issued.get("date-parts")
    if not isinstance(raw_date_parts, list) or not raw_date_parts:
        return "", ""
    first_row = raw_date_parts[0]
    if not isinstance(first_row, list) or not first_row:
        return "", ""

    year = ""
    month = ""
    try:
        parsed_year = int(first_row[0])
        if parsed_year > 0:
            year = str(parsed_year)
    except (TypeError, ValueError):
        year = ""

    if len(first_row) >= 2:
        try:
            parsed_month = int(first_row[1])
            if 1 <= parsed_month <= 12:
                month = MONTH_ABBREVIATIONS[parsed_month]
        except (TypeError, ValueError):
            month = ""

    return year, month


def _crossref_extract_author_entries(item: Mapping[str, Any]) -> list[dict[str, str]]:
    """Extract normalized author entries from a Crossref work item."""
    raw_authors = item.get("author")
    if not isinstance(raw_authors, list):
        return []

    normalized_authors: list[dict[str, str]] = []
    for raw_author in raw_authors:
        if not isinstance(raw_author, Mapping):
            continue
        first_name = str(raw_author.get("given") or "").strip()
        last_name = str(raw_author.get("family") or "").strip()
        suffix = str(raw_author.get("suffix") or "").strip()
        if not first_name and not last_name and not suffix:
            continue
        normalized_authors.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "suffix": suffix,
            }
        )
    return normalized_authors


def _author_initials(first_name: str) -> str:
    """Convert first-name tokens to dotted initials."""
    initials: list[str] = []
    for token in str(first_name).strip().split():
        for char in token:
            if char.isalpha():
                initials.append(f"{char.upper()}.")
                break
    return " ".join(initials)


def _format_author_harvard(author_entry: Mapping[str, Any]) -> str:
    """Format one author entry as ``Last, F.`` with optional suffix."""
    first_name = str(author_entry.get("first_name") or "").strip()
    last_name = str(author_entry.get("last_name") or "").strip()
    suffix = str(author_entry.get("suffix") or "").strip()
    initials = _author_initials(first_name)

    if last_name and initials:
        formatted = f"{last_name}, {initials}"
    elif last_name:
        formatted = last_name
    elif first_name:
        formatted = first_name
    else:
        formatted = ""

    if not formatted:
        return ""
    if suffix:
        return f"{formatted}, {suffix}"
    return formatted


def _format_authors_harvard(author_entries: list[dict[str, str]]) -> str:
    """Format author entries using the same display style as the dashboard."""
    formatted = [
        value
        for value in (_format_author_harvard(author_entry) for author_entry in author_entries)
        if value
    ]
    if not formatted:
        return ""
    if len(formatted) == 1:
        return formatted[0]
    if len(formatted) == 2:
        return f"{formatted[0]} & {formatted[1]}"
    return f"{', '.join(formatted[:-1])} & {formatted[-1]}"


def _map_crossref_document_type(value: str) -> str:
    """Map Crossref work type values to supported BibTeX entry types."""
    normalized = str(value).strip().lower()
    if not normalized:
        return "misc"
    return CROSSREF_TYPE_TO_DOCUMENT_TYPE.get(normalized, "misc")


def _crossref_extract_item_title(item: Mapping[str, Any]) -> str:
    """Return the first title-like value from a Crossref work item."""
    title = _crossref_first_text(item.get("title"))
    if title:
        return title
    return _crossref_first_text(item.get("short-title"))


def _crossref_score_item(
    item: Mapping[str, Any],
    *,
    lookup_field: str,
    expected_doi: str,
    expected_isbn: str,
    expected_issn: str,
    expected_title: str,
    expected_year: str,
) -> float:
    """Score one Crossref candidate in ``[0, 1]`` for acceptance."""
    item_title = _crossref_extract_item_title(item)
    title_score = _title_similarity(expected_title, item_title) if expected_title else 0.0

    item_year, _ = _crossref_extract_year_month(item)
    year_score = 0.0
    if expected_year and item_year:
        try:
            difference = abs(int(expected_year) - int(item_year))
            if difference == 0:
                year_score = 1.0
            elif difference == 1:
                year_score = 0.5
        except ValueError:
            year_score = 0.0

    if lookup_field == "doi":
        item_doi = _normalize_doi_lookup_value(str(item.get("DOI") or ""))
        if expected_doi and item_doi and item_doi.lower() == expected_doi.lower():
            return 1.0
        return 0.0

    if lookup_field == "isbn":
        raw_isbns = item.get("ISBN")
        isbn_candidates = raw_isbns if isinstance(raw_isbns, list) else []
        normalized_candidates = {
            normalized
            for normalized in (
                _normalize_isbn_value(str(raw_isbn)) for raw_isbn in isbn_candidates
            )
            if normalized
        }
        identifier_score = 1.0 if expected_isbn and expected_isbn in normalized_candidates else 0.0
        return min(1.0, 0.96 * identifier_score + (0.03 * title_score) + (0.01 * year_score))

    if lookup_field == "issn":
        raw_issns = item.get("ISSN")
        issn_candidates = raw_issns if isinstance(raw_issns, list) else []
        normalized_candidates = {
            normalized
            for normalized in (
                _normalize_issn_value(str(raw_issn)) for raw_issn in issn_candidates
            )
            if normalized
        }
        identifier_score = 1.0 if expected_issn and expected_issn in normalized_candidates else 0.0
        if not expected_title:
            return 0.55 * identifier_score
        return min(1.0, (0.45 * identifier_score) + (0.5 * title_score) + (0.05 * year_score))

    if lookup_field == "title":
        if not expected_title:
            return 0.0
        return min(1.0, (0.93 * title_score) + (0.07 * year_score))

    return 0.0


def _crossref_map_item_to_metadata(
    item: Mapping[str, Any],
) -> dict[str, Any]:
    """Map one accepted Crossref item to internal metadata fields."""
    metadata: dict[str, Any] = {}

    title = _crossref_extract_item_title(item)
    if title:
        metadata["title"] = title

    doi = _normalize_doi_lookup_value(str(item.get("DOI") or ""))
    if doi:
        metadata["doi"] = doi

    raw_isbns = item.get("ISBN")
    if isinstance(raw_isbns, list):
        for raw_isbn in raw_isbns:
            normalized_isbn = _normalize_isbn_value(str(raw_isbn))
            if normalized_isbn:
                metadata["isbn"] = normalized_isbn
                break

    raw_issns = item.get("ISSN")
    if isinstance(raw_issns, list):
        for raw_issn in raw_issns:
            normalized_issn = _normalize_issn_value(str(raw_issn))
            if normalized_issn:
                metadata["issn"] = normalized_issn
                break

    document_type = _map_crossref_document_type(str(item.get("type") or ""))
    metadata["document_type"] = document_type or "misc"

    raw_crossref_type = str(item.get("type") or "").strip()
    if raw_crossref_type:
        metadata["type"] = raw_crossref_type

    container_title = _crossref_first_text(item.get("container-title"))
    if container_title:
        if metadata["document_type"] in {"inproceedings", "incollection", "inbook"}:
            metadata["booktitle"] = container_title
        else:
            metadata["journal"] = container_title

    publisher = str(item.get("publisher") or "").strip()
    if publisher:
        metadata["publisher"] = publisher

    page_range = str(item.get("page") or "").strip()
    if page_range:
        metadata["pages"] = page_range

    volume = str(item.get("volume") or "").strip()
    if volume:
        metadata["volume"] = volume

    issue = str(item.get("issue") or "").strip()
    if issue:
        metadata["number"] = issue

    year, month = _crossref_extract_year_month(item)
    if year:
        metadata["year"] = year
    if month:
        metadata["month"] = month

    authors = _crossref_extract_author_entries(item)
    if authors:
        metadata[AUTHORS_METADATA_FIELD] = authors
        metadata["author"] = _format_authors_harvard(authors)

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
        metadata: Mapping[str, Any],
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
        metadata: Mapping[str, Any],
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

    @staticmethod
    def _normalize_search_result_text(value: Any) -> str:
        """Normalize chunk text returned by search into a single display line."""
        raw = str(value or "")
        # Collapse paragraph/line breaks and repeated whitespace in API search text.
        return re.sub(r"\s+", " ", raw).strip()

    @staticmethod
    def _extract_file_path_from_minio_location(value: Any) -> str:
        """Extract object path from ``bucket/object`` style minio location."""
        location = str(value or "").strip().lstrip("/")
        if not location or "/" not in location:
            return ""
        _, object_path = location.split("/", 1)
        return object_path.strip()

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
        minio_location = str(payload.get("minio_location", ""))
        file_path = str(payload.get("file_path", "")).strip()
        if not file_path:
            file_path = self._extract_file_path_from_minio_location(minio_location)
        return {
            "id": point_id,
            "score": score,
            "document_id": str(payload.get("document_id", "")),
            "title": str(payload.get("title", "")),
            "author": str(payload.get("author", "")),
            "year": str(payload.get("year", "")),
            "file_path": file_path,
            "chunk_index": int(payload.get("chunk_index", 0) or 0),
            "section_title": str(payload.get("section_title", "")),
            "text": self._normalize_search_result_text(payload.get("text", "")),
            "page_start": _safe_int(payload.get("page_start")),
            "page_end": _safe_int(payload.get("page_end")),
            "resolver_url": str(payload.get("resolver_url", "")),
            "minio_location": minio_location,
            "qdrant_payload": dict(payload),
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
        document_title: str | None = None,
        document_author: str | None = None,
        document_year: str | None = None,
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

        minio_location = f"{bucket_name}/{file_path}"
        resolved_document_title = str(document_title or "").strip()
        resolved_document_author = str(document_author or "").strip()
        resolved_document_year = str(document_year or "").strip()

        points: list[qdrant_models.PointStruct] = []
        for index, (chunk, embedding) in enumerate(zip(chunks, embeddings, strict=False)):
            chunk_index = int(chunk.get("chunk_index", len(points)))
            raw_metadata = chunk.get("metadata")
            chunk_metadata = raw_metadata if isinstance(raw_metadata, Mapping) else {}
            raw_orig_elements = chunk.get("orig_elements", [])
            orig_elements: list[dict[str, Any]] = []
            if isinstance(raw_orig_elements, list):
                for value in raw_orig_elements:
                    if not isinstance(value, Mapping):
                        continue
                    orig_elements.append({str(key): field for key, field in value.items()})

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
            if not page_numbers and orig_elements:
                page_numbers = _coerce_chunk_page_numbers(orig_elements)
            if not page_numbers:
                page_start = _safe_int(chunk_metadata.get("page_start"))
                page_end = _safe_int(chunk_metadata.get("page_end"))
                if page_start is not None and page_end is not None:
                    for page_number in range(page_start, page_end + 1):
                        if page_number not in page_numbers:
                            page_numbers.append(page_number)
                elif page_start is not None:
                    page_numbers.append(page_start)
                elif page_end is not None:
                    page_numbers.append(page_end)
            chunk_page_start = _safe_int(chunk_metadata.get("page_start"))
            chunk_page_end = _safe_int(chunk_metadata.get("page_end"))
            if chunk_page_start is None and page_numbers:
                chunk_page_start = min(page_numbers)
            if chunk_page_end is None and page_numbers:
                chunk_page_end = max(page_numbers)
            if chunk_page_start is None and chunk_page_end is not None:
                chunk_page_start = chunk_page_end

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
            if not bounding_boxes and orig_elements:
                bounding_boxes = _coerce_chunk_bounding_boxes(orig_elements)

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
            resolver_url = build_resolver_url(
                bucket_name,
                file_path,
                page_start=chunk_page_start,
            )
            payload = {
                "document_id": document_id,
                "title": resolved_document_title,
                "author": resolved_document_author,
                "year": resolved_document_year,
                "partition_key": partition_key,
                "minio_location": minio_location,
                "resolver_url": resolver_url,
                "chunk_index": chunk_index,
                "chunk_id": str(chunk.get("chunk_id", "")),
                "chunk_type": str(chunk.get("type", "text")),
                "section_title": chunk_metadata.get("section_title"),
                "page_start": chunk_page_start,
                "page_end": chunk_page_end,
                "filename": chunk_metadata.get("filename"),
                "text": str(chunk.get("text", "")),
                "bounding_boxes": bounding_boxes,
                "orig_elements": orig_elements,
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
        with httpx.Client(timeout=request_timeout, follow_redirects=True) as client:
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
            else:
                items = self._crossref_extract_items(payload)
                candidate_item, confidence = self._crossref_select_best_item(
                    items=items,
                    lookup_field=lookup_field,
                    expected_doi=expected_doi,
                    expected_isbn=expected_isbn,
                    expected_issn=expected_issn,
                    expected_title=expected_title,
                    expected_year=expected_year,
                )
                if not candidate_item:
                    continue

            if confidence > best_attempt_score:
                best_attempt_score = confidence
                best_attempt_field = lookup_field

            if confidence < CROSSREF_CONFIDENCE_THRESHOLD:
                continue

            fetched_metadata = _crossref_map_item_to_metadata(
                candidate_item,
            )
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
