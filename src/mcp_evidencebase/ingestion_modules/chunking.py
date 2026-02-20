"""Chunking utilities for Unstructured element payloads."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from mcp_evidencebase.ingestion_modules.metadata import (
    canonical_json,
    extract_partition_bounding_box,
)

@dataclass(frozen=True)
class _NormalizedElement:
    """Normalized Unstructured element used by the chunking pipeline."""

    element_id: str
    text: str
    render_markdown: str
    raw_type: str
    kind: str
    page_number: int | None
    coordinates: dict[str, Any] | None
    filename: str | None


_MAX_INLINE_IMAGE_BASE64_CHARS = 200_000
_NO_LEADING_SPACE_PUNCTUATION = {",", ".", ";", ":", "!", "?", ")", "]", "}", "%"}
_DEFAULT_EXCLUDED_ELEMENT_TYPE_KEYS = {
    "header",
    "footer",
    "pageheader",
    "pagefooter",
    "uncategorizedtext",
}
_DEFAULT_IMAGE_TEXT_MODE = "placeholder"
_DEFAULT_PARAGRAPH_BREAK_STRATEGY = "text"
_DEFAULT_CHUNKING_STRATEGY = "by_title"
_IMAGE_MARKDOWN_ALT_TEXT = "Image"
_IMAGE_PLACEHOLDER_MARKDOWN = "**Image unavailable**"


def _safe_int(value: Any) -> int | None:
    """Convert value to positive integer when possible."""
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return None
    if resolved < 1:
        return None
    return resolved


def _safe_non_negative_int(value: Any) -> int | None:
    """Convert value to non-negative integer when possible."""
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        return None
    if resolved < 0:
        return None
    return resolved


def _safe_non_empty_string(value: Any) -> str | None:
    """Convert value to stripped string and return ``None`` when empty."""
    if value is None:
        return None
    resolved = str(value).strip()
    return resolved or None


def _to_bool(value: Any, *, default: bool = False) -> bool:
    """Convert common truthy/falsey values to ``bool``."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if not normalized:
        return default
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _normalize_element_type_key(value: Any) -> str:
    """Normalize an element-type label into an alphanumeric key."""
    resolved = _safe_non_empty_string(value) or ""
    return "".join(character for character in resolved.lower() if character.isalnum())


def _parse_element_type_filter(value: Any, *, default: set[str] | None = None) -> set[str]:
    """Parse optional element-type filter from string/list input."""
    if value is None:
        return set(default or set())

    raw_items: list[Any]
    if isinstance(value, str):
        raw_items = value.split(",")
    elif isinstance(value, Iterable):
        raw_items = list(value)
    else:
        raw_items = [value]

    parsed: set[str] = set()
    for item in raw_items:
        normalized = _normalize_element_type_key(item)
        if normalized:
            parsed.add(normalized)
    return parsed


def _normalize_element_type(
    element: Mapping[str, Any],
    *,
    excluded_type_keys: set[str],
) -> tuple[str, str, str]:
    """Return the raw element type and normalized chunking kind."""
    raw_type = _safe_non_empty_string(element.get("type")) or _safe_non_empty_string(
        element.get("category")
    )
    resolved_type = raw_type or "Text"
    type_key = _normalize_element_type_key(resolved_type)
    if type_key in excluded_type_keys:
        return resolved_type, "excluded", type_key
    if type_key == "title":
        return resolved_type, "title", type_key
    if "table" in type_key:
        return resolved_type, "table", type_key
    if "image" in type_key:
        return resolved_type, "image", type_key
    return resolved_type, "text", type_key


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


def _resolve_table_markdown(
    raw_element: Mapping[str, Any],
    *,
    fallback_text: str,
    metadata_mapping: Mapping[str, Any],
) -> str:
    """Return render-markdown for a table element, preferring HTML when provided."""
    table_html = _safe_non_empty_string(metadata_mapping.get("text_as_html")) or _safe_non_empty_string(
        raw_element.get("text_as_html")
    )
    if table_html and "<table" in table_html.lower():
        return table_html
    return fallback_text


def _resolve_image_markdown(
    raw_element: Mapping[str, Any],
    *,
    metadata_mapping: Mapping[str, Any],
) -> str | None:
    """Return render-markdown for an image element."""
    image_source = (
        _safe_non_empty_string(metadata_mapping.get("image_url"))
        or _safe_non_empty_string(metadata_mapping.get("url"))
        or _safe_non_empty_string(raw_element.get("image_url"))
        or _safe_non_empty_string(raw_element.get("url"))
        or _safe_non_empty_string(metadata_mapping.get("image_path"))
        or _safe_non_empty_string(raw_element.get("image_path"))
    )
    if not image_source:
        image_base64 = _safe_non_empty_string(
            metadata_mapping.get("image_base64") or raw_element.get("image_base64")
        )
        if image_base64:
            normalized_base64 = "".join(image_base64.split())
            if len(normalized_base64) <= _MAX_INLINE_IMAGE_BASE64_CHARS:
                mime_type = (
                    _safe_non_empty_string(metadata_mapping.get("image_mime_type"))
                    or _safe_non_empty_string(raw_element.get("image_mime_type"))
                    or "image/png"
                )
                if "/" not in mime_type:
                    mime_type = "image/png"
                image_source = f"data:{mime_type};base64,{normalized_base64}"

    if image_source:
        return f"![{_IMAGE_MARKDOWN_ALT_TEXT}]({image_source})"
    return _IMAGE_PLACEHOLDER_MARKDOWN


def _normalize_chunking_elements(
    elements: Iterable[Mapping[str, Any]],
    *,
    exclude_element_type_keys: set[str],
    image_text_mode: str,
) -> list[_NormalizedElement]:
    """Normalize raw Unstructured elements into deterministic internal records."""
    normalized: list[_NormalizedElement] = []
    normalized_image_text_mode = str(image_text_mode or _DEFAULT_IMAGE_TEXT_MODE).strip().lower()
    if normalized_image_text_mode not in {"placeholder", "ocr", "exclude"}:
        normalized_image_text_mode = _DEFAULT_IMAGE_TEXT_MODE

    for raw_element in elements:
        metadata = raw_element.get("metadata")
        metadata_mapping = metadata if isinstance(metadata, Mapping) else {}
        text = _safe_non_empty_string(raw_element.get("text")) or ""

        raw_type, kind, type_key = _normalize_element_type(
            raw_element,
            excluded_type_keys=exclude_element_type_keys,
        )
        if kind == "excluded":
            continue

        render_markdown = text
        index_text = text
        if kind == "table":
            render_markdown = _resolve_table_markdown(
                raw_element,
                fallback_text=text,
                metadata_mapping=metadata_mapping,
            )
        elif kind == "image":
            if normalized_image_text_mode == "exclude":
                continue
            render_markdown = (
                _resolve_image_markdown(
                    raw_element,
                    metadata_mapping=metadata_mapping,
                )
                or ""
            )
            if normalized_image_text_mode == "ocr":
                index_text = text or "Image"
            else:
                index_text = "[Image]"

        if kind != "image" and not index_text:
            continue
        if not render_markdown:
            continue

        page_number = _safe_int(metadata_mapping.get("page_number"))
        if page_number is None:
            page_number = _safe_int(raw_element.get("page_number"))

        element_id = _safe_non_empty_string(raw_element.get("element_id"))
        if element_id is None:
            element_id = _stable_fallback_element_id(
                raw_type=raw_type,
                page_number=page_number,
                text=index_text or render_markdown,
            )

        filename = _safe_non_empty_string(metadata_mapping.get("filename")) or (
            _safe_non_empty_string(raw_element.get("filename"))
        )
        coordinates = extract_partition_bounding_box(raw_element)

        normalized.append(
            _NormalizedElement(
                element_id=element_id,
                text=index_text,
                render_markdown=render_markdown,
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


def _first_non_whitespace_char(value: str) -> str:
    """Return first non-whitespace character from ``value`` or empty string."""
    for character in value:
        if not character.isspace():
            return character
    return ""


def _extract_coordinate_bounds(
    coordinates: Mapping[str, Any] | None,
) -> tuple[float, float, float, float, float | None] | None:
    """Return ``(left, top, right, bottom, layout_width)`` for coordinate payloads."""
    if not isinstance(coordinates, Mapping):
        return None
    points = coordinates.get("points")
    if not isinstance(points, list) or not points:
        return None

    xs: list[float] = []
    ys: list[float] = []
    for point in points:
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            continue
        try:
            x = float(point[0])
            y = float(point[1])
        except (TypeError, ValueError):
            continue
        xs.append(x)
        ys.append(y)
    if not xs or not ys:
        return None

    raw_layout_width = coordinates.get("layout_width")
    layout_width: float | None = None
    if isinstance(raw_layout_width, (int, float)):
        resolved_layout_width = float(raw_layout_width)
        if resolved_layout_width > 0:
            layout_width = resolved_layout_width

    return (min(xs), min(ys), max(xs), max(ys), layout_width)


def _is_likely_paragraph_break(
    *,
    left_page: int | None,
    right_page: int | None,
    left_coordinates: Mapping[str, Any] | None,
    right_coordinates: Mapping[str, Any] | None,
) -> bool:
    """Return ``True`` when two adjacent text fragments likely start a new paragraph."""
    if left_page is not None and right_page is not None and left_page != right_page:
        return True

    left_bounds = _extract_coordinate_bounds(left_coordinates)
    right_bounds = _extract_coordinate_bounds(right_coordinates)
    if left_bounds is None or right_bounds is None:
        return False

    left_x, left_top, _, left_bottom, left_layout_width = left_bounds
    right_x, right_top, _, right_bottom, _ = right_bounds

    line_height = max(1.0, left_bottom - left_top, right_bottom - right_top)
    vertical_gap = right_top - left_bottom
    if vertical_gap > line_height * 0.9:
        return True

    indent_threshold = 12.0
    if left_layout_width is not None:
        indent_threshold = max(indent_threshold, left_layout_width * 0.025)
    if (right_x - left_x) > indent_threshold and vertical_gap > -(line_height * 0.3):
        return True

    return False


def _resolve_text_join_separator(
    *,
    right_text: str,
    left_page: int | None,
    right_page: int | None,
    left_coordinates: Mapping[str, Any] | None,
    right_coordinates: Mapping[str, Any] | None,
    paragraph_break_strategy: str,
    preserve_page_breaks: bool,
) -> str:
    """Resolve join separator between adjacent text fragments."""
    right_first_char = _first_non_whitespace_char(right_text)
    if right_first_char and right_first_char in _NO_LEADING_SPACE_PUNCTUATION:
        return ""

    if preserve_page_breaks and left_page is not None and right_page is not None and left_page != right_page:
        return "\n\n"

    normalized_strategy = str(paragraph_break_strategy or _DEFAULT_PARAGRAPH_BREAK_STRATEGY).strip().lower()
    if normalized_strategy == "coordinates" and _is_likely_paragraph_break(
        left_page=left_page,
        right_page=right_page,
        left_coordinates=left_coordinates,
        right_coordinates=right_coordinates,
    ):
        return "\n\n"

    return " "


def _join_two_text_fragments(
    left_text: str,
    right_text: str,
    *,
    left_page: int | None = None,
    right_page: int | None = None,
    left_coordinates: Mapping[str, Any] | None = None,
    right_coordinates: Mapping[str, Any] | None = None,
    paragraph_break_strategy: str = _DEFAULT_PARAGRAPH_BREAK_STRATEGY,
    preserve_page_breaks: bool = True,
) -> str:
    """Join adjacent text fragments while preserving likely paragraph boundaries."""
    left = str(left_text or "").rstrip()
    right = str(right_text or "").lstrip()
    if not left:
        return right.strip()
    if not right:
        return left.strip()

    if left.endswith("-") and right[:1].islower():
        return f"{left[:-1].rstrip()}{right}"

    separator = _resolve_text_join_separator(
        right_text=right,
        left_page=left_page,
        right_page=right_page,
        left_coordinates=left_coordinates,
        right_coordinates=right_coordinates,
        paragraph_break_strategy=paragraph_break_strategy,
        preserve_page_breaks=preserve_page_breaks,
    )
    if separator:
        return f"{left}{separator}{right}"
    return f"{left}{right}"


def _join_element_text_fragments(
    elements: list[_NormalizedElement],
    *,
    paragraph_break_strategy: str = _DEFAULT_PARAGRAPH_BREAK_STRATEGY,
    preserve_page_breaks: bool = True,
) -> str:
    """Join normalized element text into one block with paragraph-aware boundaries."""
    joined = ""
    previous_page: int | None = None
    previous_coordinates: Mapping[str, Any] | None = None

    for element in elements:
        text = _safe_non_empty_string(element.text)
        if text is None:
            continue
        if not joined:
            joined = text
        else:
            joined = _join_two_text_fragments(
                joined,
                text,
                left_page=previous_page,
                right_page=element.page_number,
                left_coordinates=previous_coordinates,
                right_coordinates=element.coordinates,
                paragraph_break_strategy=paragraph_break_strategy,
                preserve_page_breaks=preserve_page_breaks,
            )
        previous_page = element.page_number
        previous_coordinates = element.coordinates

    return joined.strip()


def _join_element_render_fragments(
    elements: list[_NormalizedElement],
    *,
    paragraph_break_strategy: str = _DEFAULT_PARAGRAPH_BREAK_STRATEGY,
    preserve_page_breaks: bool = True,
) -> str:
    """Join normalized element render-markdown into one section text block."""
    joined = ""
    previous_page: int | None = None
    previous_coordinates: Mapping[str, Any] | None = None

    for element in elements:
        text = _safe_non_empty_string(element.render_markdown)
        if text is None:
            continue
        if not joined:
            joined = text
        else:
            joined = _join_two_text_fragments(
                joined,
                text,
                left_page=previous_page,
                right_page=element.page_number,
                left_coordinates=previous_coordinates,
                right_coordinates=element.coordinates,
                paragraph_break_strategy=paragraph_break_strategy,
                preserve_page_breaks=preserve_page_breaks,
            )
        previous_page = element.page_number
        previous_coordinates = element.coordinates

    return joined.strip()


def _derive_section_indexes(
    elements: list[_NormalizedElement],
    *,
    split_on_titles: bool,
) -> tuple[list[int], dict[int, str | None]]:
    """Assign each element a deterministic section index and title."""
    section_indexes: list[int] = []
    section_titles: dict[int, str | None] = {0: None}
    current_section_index = 0
    current_section_has_title = False
    current_section_has_non_title = False

    for element in elements:
        if element.kind == "title":
            if split_on_titles:
                if current_section_has_non_title or current_section_has_title:
                    current_section_index += 1
                    current_section_has_non_title = False
                    current_section_has_title = False
                section_titles[current_section_index] = element.text
                current_section_has_title = True
            else:
                if not current_section_has_title:
                    section_titles[current_section_index] = element.text
                    current_section_has_title = True
            section_indexes.append(current_section_index)
            continue

        current_section_has_non_title = True
        section_indexes.append(current_section_index)

    return section_indexes, section_titles


def _build_section_payloads_from_raw_elements(
    *,
    elements: list[_NormalizedElement],
    section_indexes: list[int],
    section_titles: dict[int, str | None],
    paragraph_break_strategy: str,
    preserve_page_breaks: bool,
) -> dict[int, dict[str, Any]]:
    """Build parent-section payloads from raw normalized elements."""
    section_elements: dict[int, list[_NormalizedElement]] = {}
    for element, section_index in zip(elements, section_indexes, strict=False):
        # Section display text should exclude title headings.
        if element.kind == "title":
            continue
        section_elements.setdefault(section_index, []).append(element)

    payloads: dict[int, dict[str, Any]] = {}
    for section_index in sorted(set(section_indexes)):
        run_elements = section_elements.get(section_index, [])
        section_text = _join_element_render_fragments(
            run_elements,
            paragraph_break_strategy=paragraph_break_strategy,
            preserve_page_breaks=preserve_page_breaks,
        )

        filename = ""
        for element in run_elements:
            if element.filename:
                filename = element.filename
                break
        first_element_id = run_elements[0].element_id if run_elements else ""
        last_element_id = run_elements[-1].element_id if run_elements else ""
        section_title = section_titles.get(section_index)
        parent_payload = "|".join(
            [
                filename,
                section_title or "",
                first_element_id,
                last_element_id,
                str(section_index),
            ]
        )
        payloads[section_index] = {
            "parent_section_id": hashlib.sha256(parent_payload.encode("utf-8")).hexdigest()[:24],
            "parent_section_index": section_index,
            "parent_section_title": section_title,
            "parent_section_text": section_text,
            "parent_section_markdown": section_text,
        }
    return payloads


def _resolve_chunk_boundary_context(
    chunk: Mapping[str, Any],
    *,
    first: bool,
) -> tuple[int | None, dict[str, Any] | None]:
    """Return ``(page_number, coordinates)`` for the first/last chunk orig-element."""
    orig_elements = chunk.get("orig_elements")
    if not isinstance(orig_elements, list) or not orig_elements:
        return None, None

    boundary = orig_elements[0] if first else orig_elements[-1]
    if not isinstance(boundary, Mapping):
        return None, None

    page_number = _safe_int(boundary.get("page_number"))
    raw_coordinates = boundary.get("coordinates")
    coordinates: dict[str, Any] | None = None
    if isinstance(raw_coordinates, Mapping):
        coordinates = {str(key): value for key, value in raw_coordinates.items()}
    return page_number, coordinates


def _emit_chunk(
    *,
    chunk_type: str,
    text: str,
    section_title: str | None,
    section_index: int | None,
    contributions: list[tuple[_NormalizedElement, int]],
    render_markdown: str | None = None,
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
        "section_index": section_index,
    }
    if filename is not None:
        metadata["filename"] = filename
    resolved_render_markdown = _safe_non_empty_string(render_markdown)
    if resolved_render_markdown:
        metadata["render_markdown"] = resolved_render_markdown

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
    paragraph_break_strategy: str,
    preserve_page_breaks: bool,
) -> dict[str, Any] | None:
    """Merge two text chunks when safe under size constraints."""
    if left.get("type") != "text" or right.get("type") != "text":
        return None
    left_section_index = _resolve_chunk_section_index(left)
    right_section_index = _resolve_chunk_section_index(right)
    if left_section_index != right_section_index:
        return None
    left_boundary_page, left_boundary_coordinates = _resolve_chunk_boundary_context(
        left,
        first=False,
    )
    right_boundary_page, right_boundary_coordinates = _resolve_chunk_boundary_context(
        right,
        first=True,
    )
    joined_text = _join_two_text_fragments(
        str(left.get("text", "")),
        str(right.get("text", "")),
        left_page=left_boundary_page,
        right_page=right_boundary_page,
        left_coordinates=left_boundary_coordinates,
        right_coordinates=right_boundary_coordinates,
        paragraph_break_strategy=paragraph_break_strategy,
        preserve_page_breaks=preserve_page_breaks,
    )
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
        "section_index": left_section_index,
    }
    left_render_markdown = _safe_non_empty_string(left_meta.get("render_markdown")) or str(
        left.get("text", "")
    ).strip()
    right_render_markdown = _safe_non_empty_string(right_meta.get("render_markdown")) or str(
        right.get("text", "")
    ).strip()
    merged_render_markdown = _join_two_text_fragments(
        left_render_markdown,
        right_render_markdown,
        left_page=left_boundary_page,
        right_page=right_boundary_page,
        left_coordinates=left_boundary_coordinates,
        right_coordinates=right_boundary_coordinates,
        paragraph_break_strategy=paragraph_break_strategy,
        preserve_page_breaks=preserve_page_breaks,
    )
    if merged_render_markdown:
        metadata["render_markdown"] = merged_render_markdown
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
    paragraph_break_strategy: str,
    preserve_page_breaks: bool,
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
                    paragraph_break_strategy=paragraph_break_strategy,
                    preserve_page_breaks=preserve_page_breaks,
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
                    paragraph_break_strategy=paragraph_break_strategy,
                    preserve_page_breaks=preserve_page_breaks,
                )
                if candidate is not None:
                    consolidated[index] = candidate
                    del consolidated[index + 1]
                    merged = True
        if merged:
            continue

        index += 1

    return consolidated


def _resolve_chunk_metadata_mapping(chunk: Mapping[str, Any]) -> dict[str, Any]:
    """Return mutable chunk metadata mapping."""
    metadata = chunk.get("metadata")
    if not isinstance(metadata, Mapping):
        return {}
    return {str(key): value for key, value in metadata.items()}


def _resolve_chunk_section_title(chunk: Mapping[str, Any]) -> str | None:
    """Return normalized section title from chunk metadata."""
    metadata = _resolve_chunk_metadata_mapping(chunk)
    return _safe_non_empty_string(metadata.get("section_title"))


def _resolve_chunk_section_index(chunk: Mapping[str, Any]) -> int | None:
    """Return normalized section index from chunk metadata."""
    metadata = _resolve_chunk_metadata_mapping(chunk)
    return _safe_non_negative_int(metadata.get("section_index"))


def _resolve_chunk_first_element_id(chunk: Mapping[str, Any]) -> str:
    """Return first orig-element ID for a chunk, or empty string."""
    orig_elements = chunk.get("orig_elements", [])
    if not isinstance(orig_elements, list) or not orig_elements:
        return ""
    first = orig_elements[0]
    if not isinstance(first, Mapping):
        return ""
    return str(first.get("element_id", ""))


def _resolve_chunk_last_element_id(chunk: Mapping[str, Any]) -> str:
    """Return last orig-element ID for a chunk, or empty string."""
    orig_elements = chunk.get("orig_elements", [])
    if not isinstance(orig_elements, list) or not orig_elements:
        return ""
    last = orig_elements[-1]
    if not isinstance(last, Mapping):
        return ""
    return str(last.get("element_id", ""))


def _assemble_parent_section_text(
    run_chunks: list[dict[str, Any]],
    *,
    paragraph_break_strategy: str,
    preserve_page_breaks: bool,
) -> str:
    """Build section text from chunk render payloads with paragraph-aware joins."""
    section_text = ""
    previous_chunk_type: str | None = None
    previous_page: int | None = None
    previous_coordinates: dict[str, Any] | None = None

    for chunk in run_chunks:
        metadata = _resolve_chunk_metadata_mapping(chunk)
        rendered_part = _safe_non_empty_string(metadata.get("render_markdown")) or _safe_non_empty_string(
            chunk.get("text")
        )
        if rendered_part is None:
            continue

        chunk_type = str(chunk.get("type", "")).strip().lower() or "text"
        current_page, current_coordinates = _resolve_chunk_boundary_context(chunk, first=True)
        last_page, last_coordinates = _resolve_chunk_boundary_context(chunk, first=False)
        if not section_text:
            section_text = rendered_part
        elif previous_chunk_type == "text" and chunk_type == "text":
            section_text = _join_two_text_fragments(
                section_text,
                rendered_part,
                left_page=previous_page,
                right_page=current_page,
                left_coordinates=previous_coordinates,
                right_coordinates=current_coordinates,
                paragraph_break_strategy=paragraph_break_strategy,
                preserve_page_breaks=preserve_page_breaks,
            )
        else:
            section_text = f"{section_text.rstrip()}\n\n{rendered_part.lstrip()}"

        previous_chunk_type = chunk_type
        previous_page = last_page if last_page is not None else current_page
        previous_coordinates = last_coordinates if last_coordinates is not None else current_coordinates

    return section_text.strip()


def _annotate_parent_sections(
    chunks: list[dict[str, Any]],
    *,
    paragraph_break_strategy: str,
    preserve_page_breaks: bool,
) -> list[dict[str, Any]]:
    """Annotate chunks with deterministic parent-section metadata."""
    if not chunks:
        return chunks

    section_index = 0
    run_start = 0
    run_section_title = _resolve_chunk_section_title(chunks[0])

    for index in range(1, len(chunks) + 1):
        if index < len(chunks):
            next_section_title = _resolve_chunk_section_title(chunks[index])
            if next_section_title == run_section_title:
                continue

        run_chunks = chunks[run_start:index]
        section_text = _assemble_parent_section_text(
            run_chunks,
            paragraph_break_strategy=paragraph_break_strategy,
            preserve_page_breaks=preserve_page_breaks,
        )

        filename = ""
        for chunk in run_chunks:
            metadata = _resolve_chunk_metadata_mapping(chunk)
            candidate_filename = _safe_non_empty_string(metadata.get("filename")) or ""
            if candidate_filename:
                filename = candidate_filename
                break

        first_element_id = ""
        for chunk in run_chunks:
            candidate_first = _resolve_chunk_first_element_id(chunk)
            if candidate_first:
                first_element_id = candidate_first
                break

        last_element_id = ""
        for chunk in reversed(run_chunks):
            candidate_last = _resolve_chunk_last_element_id(chunk)
            if candidate_last:
                last_element_id = candidate_last
                break

        parent_payload = "|".join(
            [
                filename,
                run_section_title or "",
                first_element_id,
                last_element_id,
                str(section_index),
            ]
        )
        parent_section_id = hashlib.sha256(parent_payload.encode("utf-8")).hexdigest()[:24]

        for chunk in run_chunks:
            metadata = _resolve_chunk_metadata_mapping(chunk)
            metadata["parent_section_id"] = parent_section_id
            metadata["parent_section_index"] = section_index
            metadata["parent_section_title"] = run_section_title
            metadata["parent_section_text"] = section_text
            metadata["parent_section_markdown"] = section_text
            chunk["metadata"] = metadata

        section_index += 1
        run_start = index
        if index < len(chunks):
            run_section_title = _resolve_chunk_section_title(chunks[index])

    return chunks


def _attach_parent_sections_from_raw_elements(
    chunks: list[dict[str, Any]],
    *,
    section_payloads: Mapping[int, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Attach precomputed parent-section payloads to chunks."""
    for chunk in chunks:
        metadata = _resolve_chunk_metadata_mapping(chunk)
        section_index = _safe_non_negative_int(metadata.get("section_index"))
        if section_index is None:
            chunk["metadata"] = metadata
            continue
        payload = section_payloads.get(section_index)
        if payload is None:
            chunk["metadata"] = metadata
            continue
        metadata["parent_section_id"] = str(payload.get("parent_section_id", "")).strip()
        metadata["parent_section_index"] = section_index
        metadata["parent_section_title"] = payload.get("parent_section_title")
        metadata["parent_section_text"] = str(payload.get("parent_section_text", "")).strip()
        metadata["parent_section_markdown"] = str(payload.get("parent_section_markdown", "")).strip()
        chunk["metadata"] = metadata
    return chunks


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
            ``overlap_chars`` (default ``0``): Overlap size used only when splitting
                one oversized element (text or table) internally.
            ``chunking_strategy`` (default ``by_title``): One of ``by_title`` | ``none``.
            ``exclude_element_types`` (default header/footer/uncategorized): Optional
                comma/list filter of element types to exclude.
            ``include_title_text`` (default ``False``): Include Title text as chunked text.
            ``image_text_mode`` (default ``placeholder``): One of
                ``placeholder`` | ``ocr`` | ``exclude``.
            ``paragraph_break_strategy`` (default ``text``): One of
                ``text`` | ``coordinates``.
            ``preserve_page_breaks`` (default ``True``): Insert paragraph breaks on page changes.

    Returns:
        Deterministic chunk dictionaries containing:
            ``chunk_id``, ``text``, ``type`` (``text``/``table``), ``metadata``
            (including ``page_start``, ``page_end``, ``section_title``), plus
            ``orig_elements`` trace records.

    Guarantees:
        - Preserves source order and semantic element boundaries whenever possible.
        - Excludes header/footer elements and parser-noise blocks from chunk text.
        - Uses Title elements as hard section boundaries.
        - Keeps table elements isolated from narrative text.
        - Preserves image/table render markdown for section-level display.
        - Avoids inter-chunk overlap except for oversized single-element splits.
        - Produces deterministic IDs and metadata for stable re-ingestion.
    """
    max_characters = max(1, int(params.get("max_characters", 2500)))
    new_after_n_chars = max(1, min(int(params.get("new_after_n_chars", 1500)), max_characters))
    combine_under_n_chars = max(0, int(params.get("combine_under_n_chars", 500)))
    overlap_chars = max(0, min(int(params.get("overlap_chars", 0)), max_characters - 1))
    chunking_strategy = str(params.get("chunking_strategy", _DEFAULT_CHUNKING_STRATEGY)).strip().lower()
    if chunking_strategy not in {"by_title", "none"}:
        chunking_strategy = _DEFAULT_CHUNKING_STRATEGY
    split_on_titles = chunking_strategy == "by_title"
    exclude_element_type_keys = _parse_element_type_filter(
        params.get("exclude_element_types"),
        default=_DEFAULT_EXCLUDED_ELEMENT_TYPE_KEYS,
    )
    include_title_text = _to_bool(params.get("include_title_text"), default=False)
    image_text_mode = str(params.get("image_text_mode", _DEFAULT_IMAGE_TEXT_MODE)).strip().lower()
    paragraph_break_strategy = str(
        params.get("paragraph_break_strategy", _DEFAULT_PARAGRAPH_BREAK_STRATEGY)
    ).strip().lower() or _DEFAULT_PARAGRAPH_BREAK_STRATEGY
    if paragraph_break_strategy not in {"text", "coordinates"}:
        paragraph_break_strategy = _DEFAULT_PARAGRAPH_BREAK_STRATEGY
    preserve_page_breaks = _to_bool(params.get("preserve_page_breaks"), default=True)

    normalized_elements = _normalize_chunking_elements(
        elements,
        exclude_element_type_keys=exclude_element_type_keys,
        image_text_mode=image_text_mode,
    )
    if not normalized_elements:
        return []

    section_indexes, section_titles = _derive_section_indexes(
        normalized_elements,
        split_on_titles=split_on_titles,
    )
    section_payloads = _build_section_payloads_from_raw_elements(
        elements=normalized_elements,
        section_indexes=section_indexes,
        section_titles=section_titles,
        paragraph_break_strategy=paragraph_break_strategy,
        preserve_page_breaks=preserve_page_breaks,
    )

    chunks: list[dict[str, Any]] = []
    current_elements: list[_NormalizedElement] = []
    current_length = 0
    current_section_index = section_indexes[0] if section_indexes else 0
    section_title = section_titles.get(current_section_index)

    def flush_current_text_chunk() -> None:
        nonlocal current_elements
        nonlocal current_length
        if not current_elements:
            return
        text = _join_element_text_fragments(
            current_elements,
            paragraph_break_strategy=paragraph_break_strategy,
            preserve_page_breaks=preserve_page_breaks,
        )
        chunk = _emit_chunk(
            chunk_type="text",
            text=text,
            section_title=section_title,
            section_index=current_section_index,
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
                    section_index=current_section_index,
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
        current_length = len(
            _join_element_text_fragments(
                current_elements,
                paragraph_break_strategy=paragraph_break_strategy,
                preserve_page_breaks=preserve_page_breaks,
            )
        )

    for element_index, element in enumerate(normalized_elements):
        element_section_index = section_indexes[element_index]
        if element_section_index != current_section_index:
            flush_current_text_chunk()
            current_section_index = element_section_index
            section_title = section_titles.get(current_section_index)

        if element.kind == "title":
            if include_title_text and element.text:
                chunk = _emit_chunk(
                    chunk_type="text",
                    text=element.text,
                    section_title=section_title,
                    section_index=current_section_index,
                    contributions=[(element, len(element.text))],
                )
                if chunk is not None:
                    chunks.append(chunk)
            continue

        if element.kind == "table":
            flush_current_text_chunk()
            table_text = element.text
            if len(table_text) <= max_characters:
                chunk = _emit_chunk(
                    chunk_type="table",
                    text=table_text,
                    section_title=section_title,
                    section_index=current_section_index,
                    contributions=[(element, len(table_text))],
                    render_markdown=element.render_markdown,
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
                    section_index=current_section_index,
                    contributions=[(element, len(part))],
                    render_markdown=part,
                )
                if chunk is not None:
                    chunks.append(chunk)
            continue

        if element.kind == "image":
            flush_current_text_chunk()
            chunk = _emit_chunk(
                chunk_type="image",
                text=element.text,
                section_title=section_title,
                section_index=current_section_index,
                contributions=[(element, 0)],
                render_markdown=element.render_markdown,
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
        paragraph_break_strategy=paragraph_break_strategy,
        preserve_page_breaks=preserve_page_breaks,
    )
    parent_annotated = _attach_parent_sections_from_raw_elements(
        consolidated,
        section_payloads=section_payloads,
    )
    return _finalize_chunk_ids(parent_annotated)


def build_partition_chunks(
    partitions: Iterable[Mapping[str, Any]],
    *,
    chunk_size_chars: int,
    chunk_overlap_chars: int,
    chunking_strategy: str = _DEFAULT_CHUNKING_STRATEGY,
    chunk_new_after_n_chars: int = 1500,
    chunk_combine_text_under_n_chars: int = 500,
    exclude_element_types: Iterable[str] | str | None = None,
    include_title_text: bool = False,
    image_text_mode: str = _DEFAULT_IMAGE_TEXT_MODE,
    paragraph_break_strategy: str = _DEFAULT_PARAGRAPH_BREAK_STRATEGY,
    preserve_page_breaks: bool = True,
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
        new_after_n_chars=max(1, int(chunk_new_after_n_chars)),
        combine_under_n_chars=max(0, int(chunk_combine_text_under_n_chars)),
        overlap_chars=max(0, int(chunk_overlap_chars)),
        chunking_strategy=chunking_strategy,
        exclude_element_types=exclude_element_types,
        include_title_text=include_title_text,
        image_text_mode=image_text_mode,
        paragraph_break_strategy=paragraph_break_strategy,
        preserve_page_breaks=preserve_page_breaks,
    )


def chunk_partition_texts(
    partitions: Iterable[Mapping[str, Any]],
    *,
    chunk_size_chars: int,
    chunk_overlap_chars: int,
    chunking_strategy: str = _DEFAULT_CHUNKING_STRATEGY,
    chunk_new_after_n_chars: int = 1500,
    chunk_combine_text_under_n_chars: int = 500,
    exclude_element_types: Iterable[str] | str | None = None,
    include_title_text: bool = False,
    image_text_mode: str = _DEFAULT_IMAGE_TEXT_MODE,
    paragraph_break_strategy: str = _DEFAULT_PARAGRAPH_BREAK_STRATEGY,
    preserve_page_breaks: bool = True,
) -> list[str]:
    """Create text chunks from partition payload entries."""
    chunks = build_partition_chunks(
        partitions,
        chunk_size_chars=chunk_size_chars,
        chunk_overlap_chars=chunk_overlap_chars,
        chunking_strategy=chunking_strategy,
        chunk_new_after_n_chars=chunk_new_after_n_chars,
        chunk_combine_text_under_n_chars=chunk_combine_text_under_n_chars,
        exclude_element_types=exclude_element_types,
        include_title_text=include_title_text,
        image_text_mode=image_text_mode,
        paragraph_break_strategy=paragraph_break_strategy,
        preserve_page_breaks=preserve_page_breaks,
    )
    return [str(chunk.get("text", "")) for chunk in chunks if str(chunk.get("text", ""))]
