"""Pydantic models for API request payloads."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from mcp_evidencebase.ingestion import SEARCH_MODES


class BucketCreateRequest(BaseModel):
    """Payload for creating a MinIO bucket."""

    bucket_name: str = Field(description="Name of the bucket to create.")


class MetadataUpdateRequest(BaseModel):
    """Payload for updating document metadata fields."""

    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Subset of normalized metadata fields to update.",
    )


class GptSearchRequest(BaseModel):
    """Payload for GPT action search requests."""

    bucket_name: str | None = Field(
        default=None,
        description=(
            "Optional bucket/collection name to search. "
            "If omitted and exactly one bucket exists, it is selected automatically."
        ),
    )
    query: str = Field(description="Natural language query.")
    limit: int = Field(default=10, description="Maximum number of results to return.")
    mode: str = Field(
        default="hybrid",
        description=f"Search mode. One of: {', '.join(SEARCH_MODES)}.",
    )
    rrf_k: int = Field(
        default=60,
        description="Reciprocal Rank Fusion parameter for hybrid mode.",
    )
    use_staged_retrieval: bool = Field(
        default=True,
        description=(
            "Enable multi-stage retrieval (query variants, wide chunk recall, section shortlist, "
            "and section rerank with citations)."
        ),
    )
    query_variant_limit: int = Field(
        default=6,
        description="Target number of query variants to generate (clamped to 3-8).",
    )
    wide_limit_per_variant: int = Field(
        default=75,
        description=(
            "Top chunks retrieved per query variant during wide recall "
            "(clamped to 50-100)."
        ),
    )
    section_shortlist_limit: int = Field(
        default=20,
        description="Section groups kept after shortlist scoring (clamped to 10-30).",
    )
    max_section_text_chars: int = Field(
        default=2500,
        description="Maximum characters of section text returned per result.",
    )
    minimal_response: bool = Field(
        default=True,
        description=(
            "Return a compact GPT-oriented response shape to reduce token usage. "
            "Set false to include full retrieval diagnostics and rich section fields."
        ),
    )
    minimal_result_text_chars: int = Field(
        default=500,
        description=(
            "Maximum characters for each result text snippet when minimal_response=true "
            "(clamped to 25-2000)."
        ),
    )
