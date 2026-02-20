"""Ingestion service settings and dependency wiring."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from minio import Minio

from mcp_evidencebase.ingestion_modules.service import IngestionService, UnstructuredPartitionClient
from mcp_evidencebase.ingestion_modules.qdrant import QdrantIndexer
from mcp_evidencebase.ingestion_modules.repository import RedisDocumentRepository
from mcp_evidencebase.minio_settings import MinioSettings, build_minio_settings


class MinioObjectLike(Protocol):
    """Subset of MinIO object summary data used by the scanner."""

    object_name: str
    etag: str | None
    is_dir: bool


@dataclass(frozen=True)
class IngestionSettings:
    """Runtime settings for document ingestion components."""

    minio: MinioSettings
    redis_url: str
    redis_prefix: str
    qdrant_url: str
    qdrant_api_key: str | None
    qdrant_collection_prefix: str
    unstructured_api_url: str
    unstructured_api_key: str | None
    unstructured_strategy: str
    unstructured_timeout_seconds: float
    fastembed_model: str
    fastembed_keyword_model: str
    chunk_size_chars: int
    chunk_overlap_chars: int
    chunk_exclude_element_types: tuple[str, ...] | None
    chunking_strategy: str
    chunk_new_after_n_chars: int
    chunk_combine_text_under_n_chars: int
    chunk_include_title_text: bool
    chunk_image_text_mode: str
    chunk_paragraph_break_strategy: str
    chunk_preserve_page_breaks: bool
    scan_interval_seconds: int

def build_ingestion_settings(env: Mapping[str, str] | None = None) -> IngestionSettings:
    """Build ingestion settings from environment variables.

    Args:
        env: Optional environment mapping. Defaults to ``os.environ``.

    Returns:
        Parsed ingestion settings with defaults for unset variables.
    """
    source = os.environ if env is None else env
    minio_settings = build_minio_settings(source)

    def _safe_int(name: str, default: int) -> int:
        raw_value = source.get(name, str(default))
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return default

    def _safe_float(name: str, default: float) -> float:
        raw_value = source.get(name, str(default))
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return default

    def _safe_bool(name: str, default: bool) -> bool:
        raw_value = source.get(name)
        if raw_value is None:
            return default
        normalized = str(raw_value).strip().lower()
        if not normalized:
            return default
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        return default

    def _safe_csv_tuple(name: str) -> tuple[str, ...] | None:
        raw_value = source.get(name)
        if raw_value is None:
            return None
        values = tuple(item.strip() for item in str(raw_value).split(",") if item.strip())
        if not values:
            return None
        return values

    return IngestionSettings(
        minio=minio_settings,
        redis_url=source.get("REDIS_URL", "redis://localhost:6379/2"),
        redis_prefix=source.get("REDIS_PREFIX", "evidencebase"),
        qdrant_url=source.get("QDRANT_URL", "http://localhost:6333"),
        qdrant_api_key=source.get("QDRANT_API_KEY") or None,
        qdrant_collection_prefix=source.get("QDRANT_COLLECTION_PREFIX", "evidencebase"),
        unstructured_api_url=source.get(
            "UNSTRUCTURED_API_URL", "https://api.unstructuredapp.io/general/v0/general"
        ),
        unstructured_api_key=source.get("UNSTRUCTURED_API_KEY") or None,
        unstructured_strategy=source.get("UNSTRUCTURED_STRATEGY", "hi_res"),
        unstructured_timeout_seconds=max(
            5.0, _safe_float("UNSTRUCTURED_TIMEOUT_SECONDS", 300.0)
        ),
        fastembed_model=source.get("FASTEMBED_MODEL", "BAAI/bge-small-en-v1.5"),
        fastembed_keyword_model=source.get("FASTEMBED_KEYWORD_MODEL", "Qdrant/bm25"),
        chunk_size_chars=_safe_int("CHUNK_SIZE_CHARS", 1200),
        chunk_overlap_chars=_safe_int("CHUNK_OVERLAP_CHARS", 0),
        chunk_exclude_element_types=_safe_csv_tuple("CHUNK_EXCLUDE_ELEMENT_TYPES"),
        chunking_strategy=source.get("CHUNKING_STRATEGY", "by_title"),
        chunk_new_after_n_chars=_safe_int("CHUNK_NEW_AFTER_N_CHARS", 1500),
        chunk_combine_text_under_n_chars=_safe_int("CHUNK_COMBINE_TEXT_UNDER_N_CHARS", 500),
        chunk_include_title_text=_safe_bool("CHUNK_INCLUDE_TITLE_TEXT", False),
        chunk_image_text_mode=source.get("CHUNK_IMAGE_TEXT_MODE", "placeholder"),
        chunk_paragraph_break_strategy=source.get("CHUNK_PARAGRAPH_BREAK_STRATEGY", "text"),
        chunk_preserve_page_breaks=_safe_bool("CHUNK_PRESERVE_PAGE_BREAKS", True),
        scan_interval_seconds=max(5, _safe_int("MINIO_SCAN_INTERVAL_SECONDS", 15)),
    )


def build_ingestion_service(settings: IngestionSettings | None = None) -> IngestionService:
    """Build a fully configured ingestion service.

    Args:
        settings: Optional precomputed settings object.

    Returns:
        Ready-to-use ingestion service with MinIO, Redis, and Qdrant clients.
    """
    resolved_settings = settings or build_ingestion_settings()

    minio_client = Minio(
        resolved_settings.minio.endpoint,
        access_key=resolved_settings.minio.access_key,
        secret_key=resolved_settings.minio.secret_key,
        secure=resolved_settings.minio.secure,
        region=resolved_settings.minio.region,
    )

    import redis
    from qdrant_client import QdrantClient

    redis_client = redis.Redis.from_url(resolved_settings.redis_url, decode_responses=True)
    repository = RedisDocumentRepository(
        redis_client=redis_client,
        key_prefix=resolved_settings.redis_prefix,
    )
    partition_client = UnstructuredPartitionClient(
        api_url=resolved_settings.unstructured_api_url,
        api_key=resolved_settings.unstructured_api_key,
        strategy=resolved_settings.unstructured_strategy,
        timeout_seconds=resolved_settings.unstructured_timeout_seconds,
    )
    qdrant_client = QdrantClient(
        url=resolved_settings.qdrant_url,
        api_key=resolved_settings.qdrant_api_key,
    )
    qdrant_indexer = QdrantIndexer(
        qdrant_client=qdrant_client,
        fastembed_model=resolved_settings.fastembed_model,
        fastembed_keyword_model=resolved_settings.fastembed_keyword_model,
        collection_prefix=resolved_settings.qdrant_collection_prefix,
    )
    return IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=partition_client,
        qdrant_indexer=qdrant_indexer,
        chunk_size_chars=resolved_settings.chunk_size_chars,
        chunk_overlap_chars=resolved_settings.chunk_overlap_chars,
        chunk_exclude_element_types=resolved_settings.chunk_exclude_element_types,
        chunking_strategy=resolved_settings.chunking_strategy,
        chunk_new_after_n_chars=resolved_settings.chunk_new_after_n_chars,
        chunk_combine_text_under_n_chars=resolved_settings.chunk_combine_text_under_n_chars,
        chunk_include_title_text=resolved_settings.chunk_include_title_text,
        chunk_image_text_mode=resolved_settings.chunk_image_text_mode,
        chunk_paragraph_break_strategy=resolved_settings.chunk_paragraph_break_strategy,
        chunk_preserve_page_breaks=resolved_settings.chunk_preserve_page_breaks,
    )
