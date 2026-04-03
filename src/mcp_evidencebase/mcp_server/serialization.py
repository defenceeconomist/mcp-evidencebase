"""Shared validation and JSON-serialization helpers for the MCP server."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from mcp_evidencebase.ingestion import SEARCH_MODES

JSONScalar = str | int | float | bool | None
JSONValue = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]


def normalize_required_text(value: str, *, field_name: str) -> str:
    """Return one required non-empty text field."""
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty.")
    return normalized


def normalize_positive_int(
    value: int,
    *,
    field_name: str,
    minimum: int = 1,
) -> int:
    """Return one validated positive integer."""
    if value < minimum:
        raise ValueError(f"{field_name} must be greater than or equal to {minimum}.")
    return value


def normalize_search_mode(value: str) -> str:
    """Return one validated search mode."""
    normalized = value.strip().lower()
    if normalized not in SEARCH_MODES:
        valid_modes = ", ".join(SEARCH_MODES)
        raise ValueError(f"mode must be one of: {valid_modes}.")
    return normalized


def to_jsonable(value: Any) -> JSONValue:
    """Convert nested results to JSON-serializable Python primitives."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [to_jsonable(item) for item in value]
    return str(value)
