from __future__ import annotations

import io
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

try:
    import redis
except ModuleNotFoundError:
    redis = None  # type: ignore[assignment]

try:
    from minio import Minio
    from minio.error import S3Error
except ModuleNotFoundError:
    Minio = Any  # type: ignore[assignment,misc]
    S3Error = Exception  # type: ignore[assignment,misc]

try:
    from qdrant_client import QdrantClient
    from qdrant_client import models as qdrant_models
except ModuleNotFoundError:
    QdrantClient = Any  # type: ignore[assignment,misc]
    qdrant_models = None  # type: ignore[assignment]

from mcp_evidencebase.ingestion import IngestionService, QdrantIndexer, RedisDocumentRepository
from mcp_evidencebase.minio_settings import to_bool

pytestmark = [pytest.mark.area_ingestion, pytest.mark.integration_live]

LIVE_TEST_FLAG = "MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION"


class _StaticPartitionClient:
    def __init__(self, partitions: list[dict[str, Any]]) -> None:
        self._partitions = partitions
        self.calls: list[dict[str, Any]] = []

    def partition_file(
        self,
        *,
        file_name: str,
        file_bytes: bytes,
        content_type: str,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            {
                "file_name": file_name,
                "bytes_size": len(file_bytes),
                "content_type": content_type,
            }
        )
        return [
            {str(key): value for key, value in partition.items()}
            for partition in self._partitions
        ]


@dataclass
class _StaticSparseEmbedding:
    indices: list[int]
    values: list[float]


class _StaticDenseEmbedder:
    def embed(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            token_count = max(1, len(text.split()))
            checksum = sum(ord(char) for char in text[:200]) % 997
            vectors.append(
                [
                    float(token_count),
                    float(len(text) % 389),
                    float(checksum),
                    float((token_count * 13) % 211),
                ]
            )
        return vectors


class _StaticSparseEmbedder:
    def embed(self, texts: list[str]) -> list[_StaticSparseEmbedding]:
        embeddings: list[_StaticSparseEmbedding] = []
        for text in texts:
            token_count = max(1, len(text.split()))
            embeddings.append(
                _StaticSparseEmbedding(
                    indices=[1, 3, 7],
                    values=[1.0, float(token_count % 5 + 1), float(len(text) % 17 + 1)],
                )
            )
        return embeddings


class _LiveQdrantIndexer(QdrantIndexer):
    def _get_embedder(self) -> Any:
        return _StaticDenseEmbedder()

    def _get_keyword_embedder(self) -> Any:
        return _StaticSparseEmbedder()


@dataclass
class _LiveStack:
    service: IngestionService
    repository: RedisDocumentRepository
    minio_client: Minio
    qdrant_client: QdrantClient
    qdrant_indexer: _LiveQdrantIndexer
    bucket_name: str
    redis_prefix: str


def _env_enabled(name: str) -> bool:
    return to_bool(os.getenv(name, ""))


def _resolve_minio_endpoint(raw_value: str) -> str:
    value = raw_value.strip()
    if "://" not in value:
        return value
    parsed = urlsplit(value)
    return parsed.netloc or value


def _get_live_env(name: str, fallback_name: str, default_value: str) -> str:
    candidate = os.getenv(name)
    if candidate is not None and candidate.strip():
        return candidate.strip()
    fallback = os.getenv(fallback_name)
    if fallback is not None and fallback.strip():
        return fallback.strip()
    return default_value


def _put_object(
    *,
    minio_client: Minio,
    bucket_name: str,
    object_name: str,
    payload: bytes,
    content_type: str = "application/pdf",
) -> str:
    minio_client.put_object(
        bucket_name,
        object_name,
        data=io.BytesIO(payload),
        length=len(payload),
        content_type=content_type,
    )
    stat_info = minio_client.stat_object(bucket_name, object_name)
    return str(getattr(stat_info, "etag", "")).strip('"')


def _collection_name(stack: _LiveStack) -> str:
    return stack.qdrant_indexer._collection_name(stack.bucket_name)


def _list_collection_names(qdrant_client: QdrantClient) -> set[str]:
    return {str(collection.name) for collection in qdrant_client.get_collections().collections}


def _extract_scroll_points(raw_response: Any) -> list[Any]:
    if isinstance(raw_response, tuple) and raw_response:
        first = raw_response[0]
        if isinstance(first, list):
            return first
    points = getattr(raw_response, "points", None)
    if isinstance(points, list):
        return points
    if isinstance(raw_response, list):
        return raw_response
    return []


def _scroll_document_points(stack: _LiveStack, document_id: str) -> list[Any]:
    collection_name = _collection_name(stack)
    if collection_name not in _list_collection_names(stack.qdrant_client):
        return []
    response = stack.qdrant_client.scroll(
        collection_name=collection_name,
        scroll_filter=qdrant_models.Filter(
            must=[
                qdrant_models.FieldCondition(
                    key="document_id",
                    match=qdrant_models.MatchValue(value=document_id),
                )
            ]
        ),
        limit=100,
        with_payload=True,
        with_vectors=False,
    )
    return _extract_scroll_points(response)


def _wait_for_empty_qdrant_document_points(stack: _LiveStack, document_id: str) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if not _scroll_document_points(stack, document_id):
            return
        time.sleep(0.1)
    pytest.fail("Timed out waiting for Qdrant points to be removed.")


@pytest.fixture
def live_stack() -> Any:
    if redis is None or qdrant_models is None:
        pytest.skip("Install redis, minio, and qdrant-client to run live integration tests.")

    if not _env_enabled(LIVE_TEST_FLAG):
        pytest.skip(
            f"{LIVE_TEST_FLAG}=1 is required to run live MinIO/Redis/Qdrant integration tests."
        )

    minio_endpoint = _resolve_minio_endpoint(
        _get_live_env("MCP_EVIDENCEBASE_LIVE_MINIO_ENDPOINT", "MINIO_ENDPOINT", "localhost:9000")
    )
    minio_access_key = _get_live_env(
        "MCP_EVIDENCEBASE_LIVE_MINIO_ROOT_USER",
        "MINIO_ROOT_USER",
        "minioadmin",
    )
    minio_secret_key = _get_live_env(
        "MCP_EVIDENCEBASE_LIVE_MINIO_ROOT_PASSWORD",
        "MINIO_ROOT_PASSWORD",
        "minioadmin",
    )
    minio_secure = to_bool(_get_live_env("MCP_EVIDENCEBASE_LIVE_MINIO_SECURE", "MINIO_SECURE", ""))
    redis_url = _get_live_env(
        "MCP_EVIDENCEBASE_LIVE_REDIS_URL",
        "REDIS_URL",
        "redis://localhost:6379/2",
    )
    qdrant_url = _get_live_env(
        "MCP_EVIDENCEBASE_LIVE_QDRANT_URL",
        "QDRANT_URL",
        "http://localhost:6333",
    )
    qdrant_api_key = (
        _get_live_env("MCP_EVIDENCEBASE_LIVE_QDRANT_API_KEY", "QDRANT_API_KEY", "") or None
    )

    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure,
    )
    redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
    qdrant_client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)

    try:
        minio_client.list_buckets()
    except Exception as exc:  # pragma: no cover - skip path depends on external runtime
        pytest.skip(f"MinIO is not reachable at '{minio_endpoint}': {exc}")
    try:
        redis_client.ping()
    except Exception as exc:  # pragma: no cover - skip path depends on external runtime
        pytest.skip(f"Redis is not reachable at '{redis_url}': {exc}")
    try:
        qdrant_client.get_collections()
    except Exception as exc:  # pragma: no cover - skip path depends on external runtime
        pytest.skip(f"Qdrant is not reachable at '{qdrant_url}': {exc}")

    run_id = uuid4().hex[:12]
    bucket_name = f"it-live-{run_id}"
    redis_prefix = f"it_live_{run_id}"
    collection_prefix = f"it_live_{run_id}"

    minio_client.make_bucket(bucket_name)

    partition_client = _StaticPartitionClient(
        partitions=[
            {
                "type": "Title",
                "text": "Defense offsets and industrial capability development",
                "metadata": {
                    "page_number": 1,
                    "coordinates": {
                        "points": [[5, 5], [600, 5], [600, 50], [5, 50]],
                        "layout_width": 612,
                        "layout_height": 792,
                        "system": "PixelSpace",
                    },
                },
            },
            {
                "type": "NarrativeText",
                "text": (
                    "This integration test validates ingestion state in Redis "
                    "and vector indexing in Qdrant using live datastore services."
                ),
                "metadata": {
                    "page_number": 2,
                    "coordinates": {
                        "points": [[10, 80], [580, 80], [580, 230], [10, 230]],
                        "layout_width": 612,
                        "layout_height": 792,
                        "system": "PixelSpace",
                    },
                },
            },
        ]
    )
    repository = RedisDocumentRepository(redis_client=redis_client, key_prefix=redis_prefix)
    qdrant_indexer = _LiveQdrantIndexer(
        qdrant_client=qdrant_client,
        fastembed_model="static-dense",
        fastembed_keyword_model="static-keyword",
        collection_prefix=collection_prefix,
    )
    service = IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=partition_client,
        qdrant_indexer=qdrant_indexer,
        chunk_size_chars=128,
        chunk_overlap_chars=32,
    )
    stack = _LiveStack(
        service=service,
        repository=repository,
        minio_client=minio_client,
        qdrant_client=qdrant_client,
        qdrant_indexer=qdrant_indexer,
        bucket_name=bucket_name,
        redis_prefix=redis_prefix,
    )
    try:
        yield stack
    finally:
        try:
            service.purge_datastores()
        except Exception:
            pass
        try:
            for item in minio_client.list_objects(bucket_name, recursive=True):
                object_name = str(getattr(item, "object_name", "")).strip()
                if not object_name:
                    continue
                if bool(getattr(item, "is_dir", False)):
                    continue
                minio_client.remove_object(bucket_name, object_name)
            minio_client.remove_bucket(bucket_name)
        except Exception:
            pass
        redis_client.close()
        qdrant_client.close()


def test_live_partition_chunk_and_search_round_trip(live_stack: _LiveStack) -> None:
    """Verify partition/chunk flow persists state and returns Qdrant-backed search hits."""
    object_name = "docs/live-round-trip.pdf"
    payload = (
        b"%PDF-1.7\n"
        b"Integration content for defense offsets and procurement policy retrieval.\n"
    )

    uploaded_name = live_stack.service.upload_document(
        bucket_name=live_stack.bucket_name,
        object_name=object_name,
        payload=payload,
        content_type="application/pdf",
    )
    stage_partition = live_stack.service.partition_object(
        bucket_name=live_stack.bucket_name,
        object_name=uploaded_name,
    )
    stage_chunk = live_stack.service.chunk_object(
        bucket_name=live_stack.bucket_name,
        object_name=uploaded_name,
        document_id=stage_partition["document_id"],
    )

    document_id = stage_partition["document_id"]
    assert stage_chunk["document_id"] == document_id
    assert stage_partition["partition_key"]
    assert stage_partition["meta_key"]

    state = live_stack.repository.get_state(document_id)
    assert state["processing_state"] == "processed"
    assert state["partition_key"] == stage_partition["partition_key"]
    assert state["meta_key"] == stage_partition["meta_key"]
    assert int(state["chunks_count"]) >= 1

    indexed_points = _scroll_document_points(live_stack, document_id)
    assert indexed_points
    first_payload = getattr(indexed_points[0], "payload", {})
    assert isinstance(first_payload, dict)
    assert first_payload["document_id"] == document_id
    assert first_payload["minio_location"] == f"{live_stack.bucket_name}/{object_name}"
    assert str(first_payload.get("resolver_url", "")).startswith(
        f"docs://{live_stack.bucket_name}/{object_name}"
    )

    results = live_stack.service.search_documents(
        bucket_name=live_stack.bucket_name,
        query="defense offsets retrieval",
        limit=5,
        mode="hybrid",
    )
    assert results
    assert results[0]["document_id"] == document_id


def test_live_etag_changes_require_reprocessing(live_stack: _LiveStack) -> None:
    """Confirm Redis mapping state detects ETag changes from MinIO updates."""
    object_name = "docs/live-etag.pdf"
    original_payload = b"%PDF-1.7\nInitial content for ETag tracking.\n"

    uploaded_name = live_stack.service.upload_document(
        bucket_name=live_stack.bucket_name,
        object_name=object_name,
        payload=original_payload,
        content_type="application/pdf",
    )
    initial_etag = str(
        getattr(
            live_stack.minio_client.stat_object(live_stack.bucket_name, uploaded_name),
            "etag",
            "",
        )
    ).strip('"')
    stage_partition = live_stack.service.partition_object(
        bucket_name=live_stack.bucket_name,
        object_name=uploaded_name,
        etag=initial_etag,
    )
    live_stack.service.chunk_object(
        bucket_name=live_stack.bucket_name,
        object_name=uploaded_name,
        document_id=stage_partition["document_id"],
    )

    assert (
        live_stack.service.object_requires_processing(
            bucket_name=live_stack.bucket_name,
            object_name=uploaded_name,
            etag=initial_etag,
        )
        is False
    )

    updated_etag = _put_object(
        minio_client=live_stack.minio_client,
        bucket_name=live_stack.bucket_name,
        object_name=uploaded_name,
        payload=b"%PDF-1.7\nUpdated object content forcing reprocessing.\n",
    )
    assert updated_etag != initial_etag
    assert (
        live_stack.service.object_requires_processing(
            bucket_name=live_stack.bucket_name,
            object_name=uploaded_name,
            etag=updated_etag,
        )
        is True
    )


def test_live_delete_document_cleans_minio_redis_and_qdrant(live_stack: _LiveStack) -> None:
    """Ensure delete_document removes source bytes plus Redis/Qdrant references for one document."""
    object_name = "docs/live-delete.pdf"
    payload = b"%PDF-1.7\nDelete-path integration payload.\n"

    uploaded_name = live_stack.service.upload_document(
        bucket_name=live_stack.bucket_name,
        object_name=object_name,
        payload=payload,
        content_type="application/pdf",
    )
    stage_partition = live_stack.service.partition_object(
        bucket_name=live_stack.bucket_name,
        object_name=uploaded_name,
    )
    live_stack.service.chunk_object(
        bucket_name=live_stack.bucket_name,
        object_name=uploaded_name,
        document_id=stage_partition["document_id"],
    )

    document_id = stage_partition["document_id"]
    assert _scroll_document_points(live_stack, document_id)
    assert (
        live_stack.service.delete_document(
            bucket_name=live_stack.bucket_name,
            document_id=document_id,
            keep_partitions=False,
        )
        is True
    )

    with pytest.raises(S3Error):
        live_stack.minio_client.stat_object(live_stack.bucket_name, uploaded_name)
    assert live_stack.repository.get_object_mapping(live_stack.bucket_name, uploaded_name) == {}
    state = live_stack.repository.get_state(document_id)
    assert state.get("partition_key", "") == ""
    assert state.get("partitions_count", "0") == "0"
    _wait_for_empty_qdrant_document_points(live_stack, document_id)
