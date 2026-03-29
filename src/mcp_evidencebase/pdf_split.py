"""Utilities for previewing and splitting PDFs by outline heading level."""

from __future__ import annotations

import re
from dataclasses import dataclass
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any

from pypdf import PdfReader, PdfWriter

MAX_SPLIT_HEADING_LEVEL = 3

_CONTROL_AND_PATH_PATTERN = re.compile(r"[\x00-\x1f\x7f/\\]+")
_WHITESPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True)
class PdfOutlineHeading:
    """One outline heading extracted from the PDF bookmark tree."""

    title: str
    level: int
    page_index: int


@dataclass(frozen=True)
class PdfSplitSegment:
    """One chapter-sized PDF split derived from an outline heading."""

    level: int
    chapter_title: str
    file_name: str
    object_name: str
    page_start: int
    page_end: int
    page_count: int
    page_start_index: int
    page_end_index: int
    heading_page_start: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to an API-safe preview payload."""
        return {
            "level": self.level,
            "chapter_title": self.chapter_title,
            "file_name": self.file_name,
            "object_name": self.object_name,
            "page_start": self.page_start,
            "page_end": self.page_end,
            "page_count": self.page_count,
            "heading_page_start": self.heading_page_start,
        }


@dataclass(frozen=True)
class PdfSplitLevel:
    """All split candidates for one outline heading level."""

    level: int
    available: bool
    split_count: int
    splits: tuple[PdfSplitSegment, ...]

    def to_dict(self) -> dict[str, Any]:
        """Convert to an API-safe preview payload."""
        return {
            "level": self.level,
            "available": self.available,
            "split_count": self.split_count,
            "splits": [segment.to_dict() for segment in self.splits],
        }


@dataclass(frozen=True)
class PdfSplitPlan:
    """Preview data for a split-ready PDF."""

    pdf_title: str
    folder_name: str
    page_count: int
    default_heading_level: int
    levels: tuple[PdfSplitLevel, ...]

    def get_level(self, heading_level: int) -> PdfSplitLevel:
        """Return split preview data for one supported heading level."""
        for level_preview in self.levels:
            if level_preview.level == heading_level:
                return level_preview
        raise ValueError(f"Heading level {heading_level} is not supported.")

    def to_dict(self) -> dict[str, Any]:
        """Convert to an API-safe preview payload."""
        return {
            "pdf_title": self.pdf_title,
            "folder_name": self.folder_name,
            "page_count": self.page_count,
            "default_heading_level": self.default_heading_level,
            "levels": [level.to_dict() for level in self.levels],
        }


def load_pdf_reader(document_bytes: bytes) -> Any:
    """Load a PDF reader from raw bytes and map parser failures to ValueError."""
    if not document_bytes:
        raise ValueError("PDF body must not be empty.")
    try:
        return PdfReader(BytesIO(document_bytes))
    except Exception as exc:  # pragma: no cover - depends on parser internals
        raise ValueError(f"Could not parse PDF: {exc}") from exc


def build_pdf_split_plan(reader: Any, file_name: str) -> PdfSplitPlan:
    """Create preview data for heading-based PDF splitting."""
    normalized_file_name = _normalize_text(file_name)
    file_stem = PurePosixPath(normalized_file_name or "document.pdf").stem or "document"
    pdf_title = _derive_pdf_title(reader, fallback_title=file_stem)
    folder_name = _sanitize_object_segment(pdf_title, fallback=file_stem)
    page_count = len(reader.pages)
    outline_headings = _extract_outline_headings(reader)

    levels: list[PdfSplitLevel] = []
    for level in range(1, MAX_SPLIT_HEADING_LEVEL + 1):
        segments = _build_level_segments(
            outline_headings=outline_headings,
            heading_level=level,
            page_count=page_count,
            folder_name=folder_name,
        )
        levels.append(
            PdfSplitLevel(
                level=level,
                available=bool(segments),
                split_count=len(segments),
                splits=tuple(segments),
            )
        )

    available_levels = [level.level for level in levels if level.available]
    if not available_levels:
        raise ValueError("This PDF does not expose outline headings at levels 1, 2, or 3.")

    return PdfSplitPlan(
        pdf_title=pdf_title,
        folder_name=folder_name,
        page_count=page_count,
        default_heading_level=available_levels[0],
        levels=tuple(levels),
    )


def render_pdf_split_segment(reader: Any, segment: PdfSplitSegment) -> bytes:
    """Render one split segment as a standalone PDF."""
    writer = PdfWriter()
    for page_index in range(segment.page_start_index, segment.page_end_index + 1):
        writer.add_page(reader.pages[page_index])
    writer.add_metadata({"/Title": segment.chapter_title})
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def _normalize_text(value: Any) -> str:
    return _WHITESPACE_PATTERN.sub(" ", str(value or "")).strip()


def _sanitize_object_segment(value: str, *, fallback: str) -> str:
    cleaned = _CONTROL_AND_PATH_PATTERN.sub(" ", _normalize_text(value)).strip(" .")
    cleaned = _WHITESPACE_PATTERN.sub(" ", cleaned)
    if cleaned:
        return cleaned
    fallback_cleaned = _CONTROL_AND_PATH_PATTERN.sub(" ", _normalize_text(fallback)).strip(" .")
    fallback_cleaned = _WHITESPACE_PATTERN.sub(" ", fallback_cleaned)
    return fallback_cleaned or "document"


def _extract_outline_headings(reader: Any) -> list[PdfOutlineHeading]:
    headings: list[PdfOutlineHeading] = []
    try:
        outline = reader.outline
    except Exception as exc:  # pragma: no cover - depends on parser internals
        raise ValueError(f"Could not read PDF outline: {exc}") from exc
    if not isinstance(outline, list):
        return headings
    _walk_outline(reader=reader, items=outline, level=1, headings=headings)
    return headings


def _walk_outline(
    *,
    reader: Any,
    items: list[Any],
    level: int,
    headings: list[PdfOutlineHeading],
) -> None:
    for item in items:
        if isinstance(item, list):
            if level < MAX_SPLIT_HEADING_LEVEL:
                _walk_outline(reader=reader, items=item, level=level + 1, headings=headings)
            continue
        title = _extract_outline_title(item)
        if not title or level > MAX_SPLIT_HEADING_LEVEL:
            continue
        try:
            page_index = int(reader.get_destination_page_number(item))
        except Exception:
            continue
        if page_index < 0:
            continue
        headings.append(PdfOutlineHeading(title=title, level=level, page_index=page_index))


def _extract_outline_title(outline_item: Any) -> str:
    title = _normalize_text(getattr(outline_item, "title", ""))
    if title:
        return title
    if isinstance(outline_item, dict):
        return _normalize_text(outline_item.get("/Title", ""))
    return ""


def _derive_pdf_title(reader: Any, *, fallback_title: str) -> str:
    metadata = getattr(reader, "metadata", None)
    title = _normalize_text(getattr(metadata, "title", ""))
    if not title and isinstance(metadata, dict):
        title = _normalize_text(metadata.get("/Title", ""))
    return title or _normalize_text(fallback_title) or "document"


def _build_level_segments(
    *,
    outline_headings: list[PdfOutlineHeading],
    heading_level: int,
    page_count: int,
    folder_name: str,
) -> list[PdfSplitSegment]:
    level_headings = [
        heading
        for heading in outline_headings
        if heading.level == heading_level and heading.page_index < page_count
    ]
    if not level_headings:
        return []

    collapsed_headings: list[PdfOutlineHeading] = []
    seen_start_pages: set[int] = set()
    for heading in level_headings:
        if heading.page_index in seen_start_pages:
            continue
        seen_start_pages.add(heading.page_index)
        collapsed_headings.append(heading)

    segments: list[PdfSplitSegment] = []
    used_file_stems: set[str] = set()
    for index, heading in enumerate(collapsed_headings):
        page_start_index = 0 if index == 0 else heading.page_index
        next_heading_page = (
            collapsed_headings[index + 1].page_index
            if index + 1 < len(collapsed_headings)
            else page_count
        )
        page_end_index = max(page_start_index, next_heading_page - 1)
        file_stem = _dedupe_file_stem(
            _sanitize_object_segment(heading.title, fallback=f"chapter {index + 1}"),
            used_file_stems,
        )
        file_name = f"{file_stem}.pdf"
        segments.append(
            PdfSplitSegment(
                level=heading_level,
                chapter_title=heading.title,
                file_name=file_name,
                object_name=f"{folder_name}/{file_name}",
                page_start=page_start_index + 1,
                page_end=page_end_index + 1,
                page_count=(page_end_index - page_start_index) + 1,
                page_start_index=page_start_index,
                page_end_index=page_end_index,
                heading_page_start=heading.page_index + 1,
            )
        )
    return segments


def _dedupe_file_stem(file_stem: str, used_file_stems: set[str]) -> str:
    normalized_stem = _normalize_text(file_stem) or "chapter"
    candidate = normalized_stem
    suffix = 2
    while candidate.casefold() in used_file_stems:
        candidate = f"{normalized_stem} {suffix}"
        suffix += 1
    used_file_stems.add(candidate.casefold())
    return candidate
