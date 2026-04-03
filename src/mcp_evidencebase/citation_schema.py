"""Shared citation and author schema for backend and frontend."""

from __future__ import annotations

from typing import Any

DOCUMENT_TYPES: tuple[str, ...] = (
    "article",
    "book",
    "booklet",
    "conference",
    "inbook",
    "incollection",
    "inproceedings",
    "manual",
    "mastersthesis",
    "misc",
    "phdthesis",
    "proceedings",
    "techreport",
    "unpublished",
)

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

BIBTEX_TYPE_RULES: dict[str, dict[str, list[str]]] = {
    "article": {
        "required": ["author", "title", "journal", "year"],
        "recommended": ["volume", "number", "pages", "month", "note", "doi", "issn"],
    },
    "book": {
        "required": ["title", "author", "year", "publisher", "address"],
        "recommended": [
            "editor",
            "volume",
            "number",
            "series",
            "edition",
            "month",
            "note",
            "doi",
            "isbn",
        ],
    },
    "booklet": {
        "required": ["title"],
        "recommended": ["author", "howpublished", "address", "month", "year", "note", "isbn"],
    },
    "conference": {
        "required": ["author", "title", "booktitle", "year"],
        "recommended": [
            "editor",
            "volume",
            "number",
            "series",
            "pages",
            "address",
            "month",
            "organization",
            "publisher",
            "note",
        ],
    },
    "inbook": {
        "required": ["author", "title", "booktitle", "publisher", "year"],
        "recommended": [
            "editor",
            "chapter",
            "pages",
            "volume",
            "number",
            "series",
            "address",
            "edition",
            "month",
            "note",
            "isbn",
        ],
    },
    "incollection": {
        "required": ["author", "title", "booktitle", "publisher", "year"],
        "recommended": [
            "editor",
            "volume",
            "number",
            "series",
            "chapter",
            "pages",
            "address",
            "edition",
            "month",
            "organization",
            "note",
            "isbn",
        ],
    },
    "inproceedings": {
        "required": ["author", "title", "booktitle", "year"],
        "recommended": [
            "editor",
            "volume",
            "number",
            "series",
            "pages",
            "address",
            "month",
            "organization",
            "publisher",
            "note",
        ],
    },
    "manual": {
        "required": ["title"],
        "recommended": [
            "author",
            "organization",
            "address",
            "edition",
            "month",
            "year",
            "note",
            "isbn",
        ],
    },
    "mastersthesis": {
        "required": ["author", "title", "school", "year"],
        "recommended": ["type", "address", "month", "note"],
    },
    "misc": {
        "required": [],
        "recommended": ["author", "title", "howpublished", "month", "year", "note"],
    },
    "phdthesis": {
        "required": ["author", "title", "school", "year"],
        "recommended": ["type", "address", "month", "note"],
    },
    "proceedings": {
        "required": ["title", "year"],
        "recommended": [
            "editor",
            "volume",
            "number",
            "series",
            "address",
            "month",
            "publisher",
            "organization",
            "note",
            "isbn",
        ],
    },
    "techreport": {
        "required": ["author", "title", "institution", "year"],
        "recommended": ["type", "number", "address", "month", "note"],
    },
    "unpublished": {
        "required": ["author", "title", "note"],
        "recommended": ["month", "year"],
    },
}

KNOWN_AUTHOR_SUFFIXES: tuple[str, ...] = (
    "jr",
    "jr.",
    "sr",
    "sr.",
    "ii",
    "iii",
    "iv",
    "v",
    "phd",
    "m.d.",
    "md",
)

CROSSREF_LOOKUP_SEED_FIELDS: tuple[str, ...] = ("doi", "issn", "isbn", "title")
MINIMAL_METADATA_IDENTITY_FIELDS: tuple[str, ...] = ("doi", "issn", "isbn", "title", "author")


def get_citation_schema() -> dict[str, Any]:
    """Return JSON-serializable schema payload for citation and author rules."""
    return {
        "document_types": list(DOCUMENT_TYPES),
        "bibtex_fields": list(BIBTEX_FIELDS),
        "bibtex_type_rules": {
            key: {
                "required": list(value.get("required", [])),
                "recommended": list(value.get("recommended", [])),
            }
            for key, value in BIBTEX_TYPE_RULES.items()
        },
        "known_author_suffixes": list(KNOWN_AUTHOR_SUFFIXES),
        "crossref_lookup_seed_fields": list(CROSSREF_LOOKUP_SEED_FIELDS),
        "minimal_metadata_identity_fields": list(MINIMAL_METADATA_IDENTITY_FIELDS),
    }
