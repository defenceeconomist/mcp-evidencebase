"""Crossref lookup and mapping helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from difflib import SequenceMatcher
from typing import Any

from mcp_evidencebase.ingestion_modules.metadata import (
    AUTHORS_METADATA_FIELD,
    CROSSREF_TYPE_TO_DOCUMENT_TYPE,
    DOI_PATTERN,
    METADATA_FIELDS,
    MONTH_ABBREVIATIONS,
    _normalize_doi,
    normalize_metadata,
)


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


def _crossref_parse_person_name(value: Any) -> tuple[str, str]:
    """Parse a free-text person name into ``(first_name, last_name)``."""
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return "", ""

    if "," in normalized_value:
        comma_parts = [part.strip() for part in normalized_value.split(",") if part.strip()]
        if len(comma_parts) >= 2:
            return comma_parts[1], comma_parts[0]
        return "", comma_parts[0]

    tokens = [token.strip() for token in normalized_value.split() if token.strip()]
    if not tokens:
        return "", ""
    if len(tokens) == 1:
        return "", tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def _crossref_extract_contributor_entries(
    item: Mapping[str, Any],
    field_name: str,
) -> list[dict[str, str]]:
    """Extract normalized person entries from one Crossref contributor field."""
    raw_people = item.get(field_name)
    if not isinstance(raw_people, list):
        return []

    normalized_people: list[dict[str, str]] = []
    for raw_person in raw_people:
        if not isinstance(raw_person, Mapping):
            continue
        first_name = str(raw_person.get("given") or "").strip()
        last_name = str(raw_person.get("family") or "").strip()
        suffix = str(raw_person.get("suffix") or "").strip()
        if not first_name and not last_name:
            parsed_first_name, parsed_last_name = _crossref_parse_person_name(
                raw_person.get("name") or raw_person.get("literal") or ""
            )
            first_name = parsed_first_name
            last_name = parsed_last_name
        if not first_name and not last_name and not suffix:
            continue
        normalized_people.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "suffix": suffix,
            }
        )
    return normalized_people


def _crossref_extract_author_entries(item: Mapping[str, Any]) -> list[dict[str, str]]:
    """Extract normalized author entries from a Crossref work item."""
    return _crossref_extract_contributor_entries(item, "author")


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


def _crossref_enrichment_score(item: Mapping[str, Any]) -> int:
    """Return a coarse metadata richness score for tie-breaking candidate selection."""
    score = 0
    if _crossref_extract_item_title(item):
        score += 1
    if _normalize_doi_lookup_value(str(item.get("DOI") or "")):
        score += 2
    if _crossref_extract_author_entries(item):
        score += 4
    if _crossref_extract_contributor_entries(item, "editor"):
        score += 2
    year, _ = _crossref_extract_year_month(item)
    if year:
        score += 1
    if _crossref_first_text(item.get("container-title")):
        score += 1
    return score


def _metadata_update_changes(existing: Mapping[str, Any], update: Mapping[str, Any]) -> bool:
    """Return whether applying ``update`` would change any normalized metadata field."""
    existing_normalized = normalize_metadata(existing)
    update_normalized = normalize_metadata(update)

    for field_name in METADATA_FIELDS:
        if field_name not in update_normalized:
            continue
        if update_normalized.get(field_name, "") != existing_normalized.get(field_name, ""):
            return True
    return False


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
    editors = _crossref_extract_contributor_entries(item, "editor")
    if editors:
        metadata["editor"] = _format_authors_harvard(editors)
    if authors:
        metadata[AUTHORS_METADATA_FIELD] = authors
        metadata["author"] = _format_authors_harvard(authors)
    elif editors:
        # Some Crossref records provide contributor names under editor only.
        metadata["author"] = _format_authors_harvard(editors)

    return metadata


