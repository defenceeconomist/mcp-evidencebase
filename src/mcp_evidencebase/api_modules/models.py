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
