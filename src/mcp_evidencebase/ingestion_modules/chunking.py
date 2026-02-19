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
        return resolved_type, "image"
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


def _escape_markdown_alt_text(value: str) -> str:
    """Escape markdown image-alt control characters."""
    return value.replace("[", "\\[").replace("]", "\\]").replace("\n", " ").strip()


def _resolve_image_markdown(
    raw_element: Mapping[str, Any],
    *,
    fallback_text: str,
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

    alt_text = (
        _safe_non_empty_string(metadata_mapping.get("image_alt_text"))
        or _safe_non_empty_string(raw_element.get("image_alt_text"))
        or _safe_non_empty_string(fallback_text)
        or "Image"
    )
    escaped_alt = _escape_markdown_alt_text(alt_text) or "Image"
    if image_source:
        return f"![{escaped_alt}]({image_source})"

    page_number = _safe_int(metadata_mapping.get("page_number"))
    if page_number is not None:
        return f"**Image (page {page_number})**: {escaped_alt}"
    return f"**Image**: {escaped_alt}"


def _normalize_chunking_elements(
    elements: Iterable[Mapping[str, Any]],
) -> list[_NormalizedElement]:
    """Normalize raw Unstructured elements into deterministic internal records."""
    normalized: list[_NormalizedElement] = []
    for raw_element in elements:
        metadata = raw_element.get("metadata")
        metadata_mapping = metadata if isinstance(metadata, Mapping) else {}
        text = _safe_non_empty_string(raw_element.get("text")) or ""

        raw_type, kind = _normalize_element_type(raw_element)
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
            render_markdown = (
                _resolve_image_markdown(
                    raw_element,
                    fallback_text=text,
                    metadata_mapping=metadata_mapping,
                )
                or ""
            )
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


def _emit_chunk(
    *,
    chunk_type: str,
    text: str,
    section_title: str | None,
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
    left_render_markdown = _safe_non_empty_string(left_meta.get("render_markdown")) or str(
        left.get("text", "")
    ).strip()
    right_render_markdown = _safe_non_empty_string(right_meta.get("render_markdown")) or str(
        right.get("text", "")
    ).strip()
    merged_render_markdown = (
        f"{left_render_markdown}\n\n{right_render_markdown}".strip()
        if left_render_markdown or right_render_markdown
        else ""
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


def _annotate_parent_sections(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        section_text_parts: list[str] = []
        for chunk in run_chunks:
            metadata = _resolve_chunk_metadata_mapping(chunk)
            rendered_part = _safe_non_empty_string(metadata.get("render_markdown")) or str(
                chunk.get("text", "")
            ).strip()
            if rendered_part:
                section_text_parts.append(rendered_part)
        section_text = "\n\n".join(part for part in section_text_parts if part).strip()

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
    )
    parent_annotated = _annotate_parent_sections(consolidated)
    return _finalize_chunk_ids(parent_annotated)


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
