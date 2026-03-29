"""Document ingestion services for MinIO, Redis, Unstructured, and Qdrant."""

from __future__ import annotations

import hashlib
import io
import json
import mimetypes
import re
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from mcp_evidencebase.citation_schema import BIBTEX_FIELDS as SHARED_BIBTEX_FIELDS

BIBTEX_FIELDS: tuple[str, ...] = SHARED_BIBTEX_FIELDS

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
CHAPTER_TITLE_PATTERN = re.compile(
    r"\bchapter\s*([0-9]+|[ivxlcdm]+)\b(?:\s*[-_:,.;)]*\s*([A-Za-z0-9]+))?",
    re.IGNORECASE,
)
SEARCH_MODES: tuple[str, ...] = ("semantic", "keyword", "hybrid")
CROSSREF_API_BASE_URL = "https://api.crossref.org"
CROSSREF_CONFIDENCE_THRESHOLD = 0.92
CROSSREF_MAX_RESULTS = 8
CROSSREF_TIMEOUT_SECONDS = 12.0
CROSSREF_PUBLIC_POOL_MAX_REQUESTS_PER_SECOND = 5.0
CROSSREF_PUBLIC_POOL_MAX_CONCURRENT_REQUESTS = 1
CROSSREF_SINGLE_RECORD_MAX_REQUESTS_PER_SECOND = 5.0
CROSSREF_LIST_QUERY_MAX_REQUESTS_PER_SECOND = 1.0
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


def _citation_token(value: str) -> str:
    """Normalize one citation-key token to lowercase alphanumeric text."""
    return re.sub(r"[^a-zA-Z0-9]+", "", str(value)).lower()


def _strip_outer_braces(value: str) -> str:
    """Remove balanced outer braces often used by BibTeX title/author fields."""
    normalized = str(value).strip()
    while normalized.startswith("{") and normalized.endswith("}") and len(normalized) >= 2:
        normalized = normalized[1:-1].strip()
    return normalized


def _extract_first_author_last_name_from_text(value: str) -> str:
    """Extract the first author last name from BibTeX-style free-text author values."""
    normalized = _strip_outer_braces(value)
    if not normalized:
        return ""

    first_author = re.split(
        r"\s+and\s+|\s*&\s*|\s*;\s*",
        normalized,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    first_author = _strip_outer_braces(first_author)
    if not first_author:
        return ""

    if "," in first_author:
        return _strip_outer_braces(first_author.split(",", 1)[0])

    tokens = [_strip_outer_braces(token) for token in first_author.split() if token.strip()]
    if not tokens:
        return ""
    return tokens[-1]


def _extract_first_author_last_name(metadata: Mapping[str, Any]) -> str:
    """Extract the first author family name from structured or text metadata."""
    author_entries = _normalize_author_entries(metadata.get(AUTHORS_METADATA_FIELD, ""))
    if author_entries:
        first_entry = author_entries[0]
        last_name = str(first_entry.get("last_name") or "").strip()
        if last_name:
            return last_name
        return str(first_entry.get("first_name") or "").strip()

    return _extract_first_author_last_name_from_text(str(metadata.get("author", "")))


def _extract_year_token(value: str) -> str:
    """Extract a four-digit year token for citation-key generation."""
    normalized = str(value).strip()
    if not normalized:
        return ""

    matched_year = re.search(r"(?:19|20)\d{2}", normalized)
    if matched_year:
        return matched_year.group(0)

    fallback_year = re.search(r"\d{4}", normalized)
    if fallback_year:
        return fallback_year.group(0)
    return ""


def _extract_first_title_word(value: str) -> str:
    """Extract the first alphanumeric title word for citation-key generation."""
    normalized = _strip_outer_braces(value)
    if not normalized:
        return ""
    matched_word = re.search(r"[A-Za-z0-9]+", normalized)
    if matched_word:
        return matched_word.group(0)
    return ""


def _extract_chapter_title_token(value: str) -> str:
    """Extract ``chN<word>`` token when a title contains an explicit chapter marker."""
    normalized = _strip_outer_braces(value)
    if not normalized:
        return ""
    chapter_match = CHAPTER_TITLE_PATTERN.search(normalized)
    if not chapter_match:
        return ""
    chapter_number = _citation_token(chapter_match.group(1))
    trailing_word = _citation_token(chapter_match.group(2) or "")
    chapter_token = f"ch{chapter_number}{trailing_word}"
    return "" if chapter_token == "ch" else chapter_token


def build_default_citation_key(
    *,
    metadata: Mapping[str, Any],
    file_path: str,
    document_id: str,
) -> str:
    """Build default citation key as ``firstAuthorLastName + year + firstTitleWord``."""
    author_token = _citation_token(_extract_first_author_last_name(metadata))
    year_token = _extract_year_token(str(metadata.get("year", "")))
    file_stem = PurePosixPath(file_path).stem
    file_title_token = _extract_chapter_title_token(file_stem)
    metadata_title_token = _citation_token(
        _extract_first_title_word(str(metadata.get("title", "")))
    )
    title_token = file_title_token or metadata_title_token

    candidate = f"{author_token}{year_token}{title_token}"
    if candidate:
        return candidate

    return slugify(file_stem) or document_id[:12]


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
    title = re.sub(r"[-_]+", " ", file_stem).strip() or file_name or document_id[:12]

    metadata = {field_name: "" for field_name in BIBTEX_FIELDS}
    metadata["title"] = title
    metadata["document_type"] = "misc"
    metadata[AUTHORS_METADATA_FIELD] = ""
    metadata["citation_key"] = build_default_citation_key(
        metadata=metadata,
        file_path=file_path,
        document_id=document_id,
    )
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


def extract_pdf_metadata_seed(document_bytes: bytes) -> dict[str, str]:
    """Extract preview metadata using the same PDF/title identifier approach as ingestion."""
    extracted = extract_pdf_title_author(document_bytes)
    if not document_bytes:
        return extracted

    try:
        from pypdf import PdfReader
    except ImportError:
        return extracted

    try:
        reader = PdfReader(io.BytesIO(document_bytes))
    except Exception:
        return extracted

    if not reader.pages:
        return extracted

    try:
        first_page_text = str(reader.pages[0].extract_text() or "").strip()
    except Exception:
        first_page_text = ""

    if not first_page_text:
        return extracted

    doi_match = DOI_PATTERN.search(first_page_text)
    if doi_match:
        extracted["doi"] = _normalize_doi(doi_match.group(0))

    extracted_isbn = _extract_isbn(first_page_text)
    if extracted_isbn:
        extracted["isbn"] = extracted_isbn

    extracted_issn = _extract_issn(first_page_text)
    if extracted_issn:
        extracted["issn"] = extracted_issn

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
