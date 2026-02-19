from __future__ import annotations

from typing import Any

import pytest

from mcp_evidencebase.ingestion import chunk_unstructured_elements

pytestmark = pytest.mark.area_ingestion


def _element(
    *,
    text: str,
    element_id: str | None = None,
    element_type: str = "NarrativeText",
    page_number: int = 1,
    filename: str = "paper.pdf",
    document_id: str = "doc-1",
    coordinates: dict[str, Any] | None = None,
    metadata_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "page_number": page_number,
        "filename": filename,
        "document_id": document_id,
    }
    if coordinates is not None:
        metadata["coordinates"] = coordinates
    if metadata_extra:
        metadata.update(metadata_extra)
    payload: dict[str, Any] = {"text": text, "type": element_type, "metadata": metadata}
    if element_id is not None:
        payload["element_id"] = element_id
    return payload


def _shared_boundary_chars(left: str, right: str, max_chars: int) -> int:
    for size in range(max_chars, 0, -1):
        if left.endswith(right[:size]):
            return size
    return 0


def test_chunking_title_creates_section_boundary() -> None:
    elements = [
        _element(text="Introduction", element_id="t1", element_type="Title", page_number=1),
        _element(text="A" * 620, element_id="a1", page_number=1),
        _element(text="B" * 620, element_id="a2", page_number=1),
        _element(text="Methods", element_id="t2", element_type="Title", page_number=2),
        _element(text="C" * 620, element_id="m1", page_number=2),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=1000,
        new_after_n_chars=700,
        combine_under_n_chars=0,
    )

    assert chunks
    assert all(
        not {
            "a2",
            "m1",
        }.issubset({str(item["element_id"]) for item in chunk["orig_elements"]})
        for chunk in chunks
    )
    methods_chunk = next(
        chunk
        for chunk in chunks
        if "m1" in {str(item["element_id"]) for item in chunk["orig_elements"]}
    )
    assert methods_chunk["metadata"]["section_title"] == "Methods"


def test_chunking_annotates_parent_section_metadata() -> None:
    elements = [
        _element(text="Methods", element_id="title-methods", element_type="Title", page_number=2),
        _element(
            text="Methods paragraph one with unique token ALPHA.",
            element_id="m1",
            page_number=2,
        ),
        _element(
            text="Methods paragraph two with unique token BETA.",
            element_id="m2",
            page_number=3,
        ),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=70,
        new_after_n_chars=60,
        combine_under_n_chars=0,
    )

    methods_chunks = [
        chunk for chunk in chunks if chunk.get("metadata", {}).get("section_title") == "Methods"
    ]
    assert len(methods_chunks) >= 2

    parent_ids = {
        str(chunk["metadata"].get("parent_section_id", ""))
        for chunk in methods_chunks
        if chunk.get("metadata", {}).get("parent_section_id")
    }
    assert len(parent_ids) == 1

    parent_titles = {
        str(chunk["metadata"].get("parent_section_title", ""))
        for chunk in methods_chunks
    }
    assert parent_titles == {"Methods"}

    parent_indices = {
        int(chunk["metadata"].get("parent_section_index", -1))
        for chunk in methods_chunks
    }
    assert parent_indices == {0}

    parent_text = str(methods_chunks[0]["metadata"].get("parent_section_text", ""))
    assert "unique token ALPHA" in parent_text
    assert "unique token BETA" in parent_text


def test_chunking_size_limits_respected() -> None:
    elements = [
        _element(text="D" * 700, element_id="d1", page_number=1),
        _element(text="E" * 700, element_id="d2", page_number=1),
        _element(text="F" * 2900, element_id="big", page_number=2),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=1000,
        new_after_n_chars=800,
        combine_under_n_chars=0,
        overlap_chars=80,
    )

    assert chunks
    assert all(len(str(chunk["text"])) <= 1000 for chunk in chunks)
    assert sum(
        1
        for chunk in chunks
        if "big" in {str(item["element_id"]) for item in chunk["orig_elements"]}
    ) >= 2


def test_chunking_tables_are_isolated() -> None:
    elements = [
        _element(text="Narrative before table", element_id="n1", page_number=1),
        _element(
            text="Col1 | Col2\n10 | 20",
            element_id="tbl",
            element_type="Table",
            page_number=1,
        ),
        _element(text="Narrative after table", element_id="n2", page_number=1),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=1000,
        new_after_n_chars=800,
        combine_under_n_chars=0,
    )

    assert [chunk["type"] for chunk in chunks] == ["text", "table", "text"]
    assert [item["element_id"] for item in chunks[1]["orig_elements"]] == ["tbl"]


def test_chunking_small_chunk_merging() -> None:
    elements = [
        _element(text="G" * 850, element_id="g1", page_number=1),
        _element(text="H" * 850, element_id="g2", page_number=1),
        _element(text="tiny", element_id="g3", page_number=1),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=1000,
        new_after_n_chars=800,
        combine_under_n_chars=500,
    )

    assert len(chunks) == 2
    assert [item["element_id"] for item in chunks[1]["orig_elements"]] == ["g2", "g3"]
    assert len(chunks[1]["text"]) <= 1000


def test_chunking_chunk_id_is_deterministic() -> None:
    elements = [
        _element(text="Deterministic chunking payload one.", page_number=1),
        _element(text="Deterministic chunking payload two.", page_number=1),
    ]

    first = chunk_unstructured_elements(elements)
    second = chunk_unstructured_elements(elements)

    assert [chunk["chunk_id"] for chunk in first] == [chunk["chunk_id"] for chunk in second]
    assert first[0]["orig_elements"][0]["element_id"] == second[0]["orig_elements"][0]["element_id"]


def test_chunking_oversized_split_overlap_is_internal_only() -> None:
    long_text = " ".join(f"Sentence {index:04d} with context." for index in range(1, 260))
    elements = [
        _element(text=long_text, element_id="big", page_number=1),
        _element(text="UNIQUE NEXT BLOCK", element_id="next", page_number=2),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=500,
        new_after_n_chars=400,
        combine_under_n_chars=0,
        overlap_chars=50,
    )

    big_chunks = [
        chunk
        for chunk in chunks
        if "big" in {str(item["element_id"]) for item in chunk["orig_elements"]}
    ]
    assert len(big_chunks) >= 2
    internal_overlap = _shared_boundary_chars(big_chunks[0]["text"], big_chunks[1]["text"], 80)
    assert 1 <= internal_overlap <= 50

    next_chunk_index = next(
        index
        for index, chunk in enumerate(chunks)
        if "next" in {str(item["element_id"]) for item in chunk["orig_elements"]}
    )
    assert next_chunk_index > 0
    transition_overlap = _shared_boundary_chars(
        chunks[next_chunk_index - 1]["text"],
        chunks[next_chunk_index]["text"],
        80,
    )
    assert transition_overlap == 0


def test_chunking_page_range_metadata_is_correct() -> None:
    elements = [
        _element(text="Page one content", element_id="p1", page_number=1),
        _element(
            text="Page two content",
            element_id="p2",
            page_number=2,
            coordinates={"points": [[1, 1], [2, 1], [2, 2], [1, 2]]},
        ),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=5000,
        new_after_n_chars=5000,
        combine_under_n_chars=0,
    )

    assert len(chunks) == 1
    chunk = chunks[0]
    assert chunk["metadata"]["page_start"] == 1
    assert chunk["metadata"]["page_end"] == 2
    assert [item["page_number"] for item in chunk["orig_elements"]] == [1, 2]
    assert chunk["orig_elements"][1]["coordinates"]["points"] == [
        [1.0, 1.0],
        [2.0, 1.0],
        [2.0, 2.0],
        [1.0, 2.0],
    ]


def test_chunking_excludes_headers_footers_and_parser_noise() -> None:
    elements = [
        _element(
            text="Company Confidential",
            element_id="h1",
            element_type="Header",
            page_number=1,
        ),
        _element(text="Overview", element_id="t1", element_type="Title", page_number=1),
        _element(text="Body text that should be indexed.", element_id="b1", page_number=1),
        _element(
            text="Image OCR text should be excluded from indexed text.",
            element_id="i1",
            element_type="Image",
            metadata_extra={"image_url": "https://example.com/image.png"},
        ),
        _element(text="Page 1", element_id="f1", element_type="Footer", page_number=1),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=2000,
        new_after_n_chars=1500,
        combine_under_n_chars=0,
    )

    assert chunks
    all_element_ids = {
        str(item["element_id"]) for chunk in chunks for item in chunk.get("orig_elements", [])
    }
    assert "t1" not in all_element_ids
    assert "b1" in all_element_ids
    assert "h1" not in all_element_ids
    assert "i1" in all_element_ids
    assert "f1" not in all_element_ids

    merged_text = "\n".join(str(chunk["text"]) for chunk in chunks)
    assert "Overview" not in merged_text
    assert "Company Confidential" not in merged_text
    assert "Image OCR text should be excluded from indexed text." not in merged_text
    assert "Page 1" not in merged_text
    assert any(chunk["type"] == "image" for chunk in chunks)
    image_chunk = next(chunk for chunk in chunks if chunk["type"] == "image")
    image_markdown = str(image_chunk.get("metadata", {}).get("render_markdown", ""))
    assert image_markdown.startswith("![")


def test_chunking_parent_section_text_prefers_table_html_and_image_markdown() -> None:
    elements = [
        _element(text="Methods", element_id="title-methods", element_type="Title", page_number=1),
        _element(text="Narrative context line.", element_id="m1", page_number=1),
        _element(
            text="A B\n1 2",
            element_id="tbl1",
            element_type="Table",
            page_number=1,
            metadata_extra={"text_as_html": "<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>"},
        ),
        _element(
            text="Microscope image",
            element_id="img1",
            element_type="Image",
            page_number=1,
            metadata_extra={"image_url": "https://example.com/figure.png"},
        ),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=1000,
        new_after_n_chars=800,
        combine_under_n_chars=0,
    )

    methods_chunks = [
        chunk for chunk in chunks if chunk.get("metadata", {}).get("section_title") == "Methods"
    ]
    assert methods_chunks

    parent_text = str(methods_chunks[0]["metadata"].get("parent_section_text", ""))
    assert "<table>" in parent_text
    assert "![Microscope image](https://example.com/figure.png)" in parent_text


def test_chunking_excludes_uncategorized_text() -> None:
    elements = [
        _element(
            text="Parser noise that should not be indexed.",
            element_id="u1",
            element_type="UncategorizedText",
            page_number=1,
        ),
        _element(text="Keep this narrative block.", element_id="n1", page_number=1),
    ]

    chunks = chunk_unstructured_elements(
        elements,
        max_characters=2000,
        new_after_n_chars=1500,
        combine_under_n_chars=0,
    )

    assert chunks
    all_element_ids = {
        str(item["element_id"]) for chunk in chunks for item in chunk.get("orig_elements", [])
    }
    assert "u1" not in all_element_ids
    assert "n1" in all_element_ids
    merged_text = "\n".join(str(chunk["text"]) for chunk in chunks)
    assert "Parser noise that should not be indexed." not in merged_text
    assert "Keep this narrative block." in merged_text
