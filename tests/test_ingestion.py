from __future__ import annotations

import io
import json
import sys
import types
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any, Literal

import pytest

import mcp_evidencebase.ingestion as ingestion_module
import mcp_evidencebase.ingestion_modules.wiring as wiring_module
from mcp_evidencebase.ingestion import (
    IngestionService,
    QdrantIndexer,
    RedisDocumentRepository,
    UnstructuredPartitionClient,
    build_ingestion_service,
    build_ingestion_settings,
    build_partition_chunks,
    chunk_partition_texts,
    compute_chunk_point_id,
    compute_document_id,
    extract_metadata_from_partitions,
    extract_partition_bounding_box,
    extract_pdf_title_author,
)
from mcp_evidencebase.ingestion_modules.service import DependencyConfigurationError
from mcp_evidencebase.perf import reset as reset_perf_stats
from mcp_evidencebase.perf import snapshot as perf_snapshot

pytestmark = pytest.mark.area_ingestion


class FakeRedis:
    def __init__(self) -> None:
        self._sets: dict[str, set[str]] = defaultdict(set)
        self._hashes: dict[str, dict[str, str]] = defaultdict(dict)
        self._strings: dict[str, str] = {}

    def sadd(self, key: str, *values: str) -> int:
        before = len(self._sets[key])
        self._sets[key].update(values)
        return len(self._sets[key]) - before

    def srem(self, key: str, *values: str) -> int:
        removed = 0
        target = self._sets.get(key, set())
        for value in values:
            if value in target:
                target.remove(value)
                removed += 1
        return removed

    def smembers(self, key: str) -> set[str]:
        return set(self._sets.get(key, set()))

    def hset(self, key: str, mapping: dict[str, str]) -> int:
        self._hashes[key].update(mapping)
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hashes.get(key, {}))

    def hlen(self, key: str) -> int:
        return len(self._hashes.get(key, {}))

    def set(self, key: str, value: str) -> bool:
        self._strings[key] = value
        return True

    def get(self, key: str) -> str | None:
        return self._strings.get(key)

    def delete(self, key: str) -> int:
        removed = 0
        if key in self._sets:
            del self._sets[key]
            removed += 1
        if key in self._hashes:
            del self._hashes[key]
            removed += 1
        if key in self._strings:
            del self._strings[key]
            removed += 1
        return removed

    def scan_iter(self, match: str | None = None) -> Any:
        keys = set(self._sets) | set(self._hashes) | set(self._strings)
        for key in sorted(keys):
            if match is None or fnmatch(key, match):
                yield key


@dataclass
class FakeCollection:
    name: str


@dataclass
class FakeCollectionsResponse:
    collections: list[FakeCollection]


class FakeQdrantClient:
    def __init__(self) -> None:
        self.collection_names: set[str] = set()
        self.collection_points: dict[str, list[Any]] = defaultdict(list)
        self.upsert_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []
        self.set_payload_calls: list[dict[str, Any]] = []
        self.search_calls: list[dict[str, Any]] = []
        self.search_results: dict[str, list[Any]] = {"dense": [], "keyword": []}
        self.scroll_points: list[Any] = []

    @staticmethod
    def _match_payload_condition(payload: Mapping[str, Any], condition: Any) -> bool:
        key = str(getattr(condition, "key", "")).strip()
        match = getattr(condition, "match", None)
        value = str(getattr(match, "value", "")).strip()
        return str(payload.get(key, "")).strip() == value

    @classmethod
    def _matches_filter(cls, payload: Mapping[str, Any], filter_value: Any) -> bool:
        if filter_value is None:
            return True
        must_conditions = getattr(filter_value, "must", None)
        should_conditions = getattr(filter_value, "should", None)
        if isinstance(must_conditions, list) and any(
            not cls._match_payload_condition(payload, condition) for condition in must_conditions
        ):
            return False
        if isinstance(should_conditions, list) and should_conditions:
            return any(
                cls._match_payload_condition(payload, condition)
                for condition in should_conditions
            )
        return True

    def _get_collection_points(self, collection_name: str) -> list[Any]:
        points = self.collection_points.get(collection_name)
        if points:
            return list(points)
        return list(self.scroll_points)

    def get_collections(self) -> FakeCollectionsResponse:
        return FakeCollectionsResponse(
            collections=[FakeCollection(name=name) for name in sorted(self.collection_names)]
        )

    def create_collection(
        self,
        collection_name: str,
        vectors_config: Any,
        sparse_vectors_config: Any | None = None,
    ) -> None:
        del vectors_config, sparse_vectors_config
        self.collection_names.add(collection_name)

    def delete_collection(self, collection_name: str) -> None:
        self.collection_names.discard(collection_name)

    def upsert(self, *, collection_name: str, points: list[Any], wait: bool) -> None:
        self.collection_names.add(collection_name)
        self.upsert_calls.append(
            {
                "collection_name": collection_name,
                "points": points,
                "wait": wait,
            }
        )
        current_points = {
            str(getattr(point, "id", "")): point
            for point in self.collection_points.get(collection_name, [])
            if getattr(point, "id", None) is not None
        }
        for point in points:
            point_id = getattr(point, "id", None)
            if point_id is None:
                continue
            current_points[str(point_id)] = point
        self.collection_points[collection_name] = list(current_points.values())

    def scroll(
        self,
        *,
        collection_name: str,
        scroll_filter: Any,
        limit: int,
        with_payload: bool,
        with_vectors: bool,
        offset: Any = None,
    ) -> tuple[list[Any], None]:
        del limit, with_payload, with_vectors, offset
        points = []
        for point in self._get_collection_points(collection_name):
            payload = getattr(point, "payload", {})
            if not isinstance(payload, Mapping):
                continue
            if self._matches_filter(payload, scroll_filter):
                points.append(point)
        return points, None

    def set_payload(
        self,
        *,
        collection_name: str,
        payload: dict[str, Any],
        points: list[Any],
        wait: bool,
        **kwargs: Any,
    ) -> None:
        del kwargs
        self.set_payload_calls.append(
            {
                "collection_name": collection_name,
                "payload": dict(payload),
                "points": list(points),
                "wait": wait,
            }
        )
        point_ids = {str(point_id) for point_id in points}
        for point in self._get_collection_points(collection_name):
            point_id = getattr(point, "id", None)
            if point_id is None or str(point_id) not in point_ids:
                continue
            current_payload = getattr(point, "payload", {})
            if isinstance(current_payload, dict):
                current_payload.update(payload)

    def search(
        self,
        *,
        collection_name: str,
        query_vector: Any,
        limit: int,
        with_payload: bool,
        with_vectors: bool,
        query_filter: Any = None,
        filter: Any = None,
    ) -> list[Any]:
        del collection_name, with_payload, with_vectors
        vector_name = str(query_vector[0]) if isinstance(query_vector, tuple) else "dense"
        resolved_filter = query_filter if query_filter is not None else filter
        self.search_calls.append(
            {
                "vector_name": vector_name,
                "limit": limit,
                "query_filter": resolved_filter,
            }
        )
        points: list[Any] = []
        for point in self.search_results.get(vector_name, []):
            payload = getattr(point, "payload", {})
            if not isinstance(payload, Mapping):
                continue
            if self._matches_filter(payload, resolved_filter):
                points.append(point)
        return points[:limit]

    def delete(self, *, collection_name: str, points_selector: Any) -> None:
        self.delete_calls.append(
            {
                "collection_name": collection_name,
                "points_selector": points_selector,
            }
        )
        filter_value = getattr(points_selector, "filter", None)
        existing_points = self._get_collection_points(collection_name)
        retained_points = []
        for point in existing_points:
            payload = getattr(point, "payload", {})
            if not isinstance(payload, Mapping) or not self._matches_filter(payload, filter_value):
                retained_points.append(point)
        self.collection_points[collection_name] = retained_points


class FakeMinioObjectResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def close(self) -> None:
        return None

    def release_conn(self) -> None:
        return None


class FakeMinioClient:
    def __init__(self, payload: bytes, *, etag: str = "etag-1") -> None:
        self._payload = payload
        self._etag = etag
        self._buckets: dict[str, dict[str, dict[str, Any]]] = {"research-raw": {}}
        self._etag_counter = 1
        self.get_object_calls: list[tuple[str, str]] = []
        self.stat_object_calls: list[tuple[str, str]] = []
        self.remove_object_calls: list[tuple[str, str]] = []
        self.copy_object_calls: list[tuple[str, str, str]] = []

    def bucket_exists(self, bucket_name: str) -> bool:
        return bucket_name in self._buckets

    def make_bucket(self, bucket_name: str, location: str | None = None) -> None:
        del location
        self._buckets.setdefault(bucket_name, {})

    def remove_bucket(self, bucket_name: str) -> None:
        self._buckets.pop(bucket_name, None)

    def get_object(self, bucket_name: str, object_name: str) -> FakeMinioObjectResponse:
        self.get_object_calls.append((bucket_name, object_name))
        payload = self._buckets.get(bucket_name, {}).get(object_name, {}).get(
            "payload", self._payload
        )
        return FakeMinioObjectResponse(payload)

    def stat_object(self, bucket_name: str, object_name: str) -> Any:
        self.stat_object_calls.append((bucket_name, object_name))
        stored_object = self._buckets.get(bucket_name, {}).get(object_name)
        if stored_object is None:
            return types.SimpleNamespace(etag=self._etag, content_type="application/pdf")
        return types.SimpleNamespace(
            etag=stored_object.get("etag", self._etag),
            content_type=stored_object.get("content_type", "application/pdf"),
        )

    def remove_object(self, bucket_name: str, object_name: str) -> None:
        self.remove_object_calls.append((bucket_name, object_name))
        self._buckets.setdefault(bucket_name, {}).pop(object_name, None)

    def list_buckets(self) -> list[Any]:
        return [types.SimpleNamespace(name=name) for name in sorted(self._buckets)]

    def list_objects(
        self,
        bucket_name: str,
        recursive: bool = False,
    ) -> list[Any]:
        del recursive
        return [
            types.SimpleNamespace(
                object_name=object_name,
                etag=stored_object.get("etag", self._etag),
                is_dir=False,
            )
            for object_name, stored_object in sorted(self._buckets.get(bucket_name, {}).items())
        ]

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        *,
        data: Any,
        length: int,
        content_type: str,
    ) -> Any:
        del length
        self._buckets.setdefault(bucket_name, {})
        payload = bytes(data.read())
        self._buckets[bucket_name][object_name] = {
            "payload": payload,
            "etag": f"etag-{self._etag_counter}",
            "content_type": content_type,
        }
        self._etag_counter += 1
        return types.SimpleNamespace(object_name=object_name)

    def copy_object(self, bucket_name: str, object_name: str, source: Any) -> Any:
        self._buckets.setdefault(bucket_name, {})
        source_bucket_name = str(getattr(source, "bucket_name", bucket_name)).strip() or bucket_name
        source_object_name = str(getattr(source, "object_name", "")).strip()
        self.copy_object_calls.append((bucket_name, object_name, source_object_name))
        stored_source = self._buckets.get(source_bucket_name, {}).get(source_object_name)
        if stored_source is None:
            stored_source = {
                "payload": self._payload,
                "etag": self._etag,
                "content_type": "application/pdf",
            }
        self._buckets[bucket_name][object_name] = dict(stored_source)
        return types.SimpleNamespace(object_name=object_name)


class FakePartitionClient:
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
                "file_bytes": file_bytes,
                "content_type": content_type,
            }
        )
        return list(self._partitions)


class RecordingQdrantIndexer:
    def __init__(self) -> None:
        self.upsert_calls: list[dict[str, Any]] = []
        self.rewrite_calls: list[dict[str, Any]] = []
        self.search_calls: list[dict[str, Any]] = []
        self.migration_calls: list[dict[str, Any]] = []

    def upsert_document_chunks(
        self,
        *,
        bucket_name: str,
        document_id: str,
        file_path: str,
        chunks: list[dict[str, Any]],
        partition_key: str,
        meta_key: str,
        document_year: str | None = None,
        storage_bucket_name: str | None = None,
    ) -> None:
        self.upsert_calls.append(
            {
                "bucket_name": bucket_name,
                "document_id": document_id,
                "file_path": file_path,
                "chunks": chunks,
                "partition_key": partition_key,
                "meta_key": meta_key,
                "document_year": document_year,
                "storage_bucket_name": storage_bucket_name,
            }
        )

    def ensure_bucket_collection(self, bucket_name: str) -> bool:
        del bucket_name
        return False

    def delete_bucket_collection(self, bucket_name: str) -> bool:
        del bucket_name
        return False

    def purge_prefixed_collections(self) -> int:
        return 0

    def delete_document(self, bucket_name: str, document_id: str) -> None:
        del bucket_name, document_id

    def rewrite_document_source_paths(
        self,
        *,
        bucket_name: str,
        document_id: str,
        old_object_name: str,
        new_object_name: str,
        storage_bucket_name: str | None = None,
    ) -> int:
        self.rewrite_calls.append(
            {
                "bucket_name": bucket_name,
                "document_id": document_id,
                "old_object_name": old_object_name,
                "new_object_name": new_object_name,
                "storage_bucket_name": storage_bucket_name,
            }
        )
        return 1

    def rewrite_collection_storage_metadata(
        self,
        *,
        bucket_name: str,
        storage_bucket_name: str,
    ) -> int:
        self.rewrite_calls.append(
            {
                "bucket_name": bucket_name,
                "storage_bucket_name": storage_bucket_name,
                "scope": "collection",
            }
        )
        return 0

    def search_chunks(
        self,
        *,
        bucket_name: str,
        query: str,
        limit: int,
        mode: str,
        rrf_k: int = 60,
    ) -> list[dict[str, Any]]:
        self.search_calls.append(
            {
                "bucket_name": bucket_name,
                "query": query,
                "limit": limit,
                "mode": mode,
                "rrf_k": rrf_k,
            }
        )
        return []

    def migrate_legacy_collections_to_shared_collection(
        self,
        *,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        self.migration_calls.append({"dry_run": dry_run})
        return {
            "shared_collection_name": "evidence-base",
            "dry_run": dry_run,
            "legacy_collections_seen": 0,
            "legacy_collections_migrated": 0,
            "legacy_collections_deleted": 0,
            "legacy_points_migrated": 0,
            "items": [],
        }


class FakeEmbedder:
    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[0.1, 0.2, 0.3] for _ in texts]


@dataclass
class FakeSparseEmbedding:
    indices: list[int]
    values: list[float]


class FakeSparseEmbedder:
    def embed(self, texts: list[str]) -> list[FakeSparseEmbedding]:
        return [FakeSparseEmbedding(indices=[1, 3], values=[0.8, 0.2]) for _ in texts]


class StubQdrantIndexer(QdrantIndexer):
    def _get_embedder(self) -> Any:
        return FakeEmbedder()

    def _get_keyword_embedder(self) -> Any:
        return FakeSparseEmbedder()


@dataclass
class StubVectorParams:
    size: int
    distance: str


@dataclass
class StubMatchValue:
    value: str


@dataclass
class StubFieldCondition:
    key: str
    match: StubMatchValue


@dataclass
class StubFilter:
    should: list[StubFieldCondition] | None = None
    must: list[StubFieldCondition] | None = None


@dataclass
class StubFilterSelector:
    filter: StubFilter


@dataclass
class StubPointStruct:
    id: str
    vector: dict[str, Any]
    payload: dict[str, Any]


@dataclass
class StubSparseVector:
    indices: list[int]
    values: list[float]


@dataclass
class StubSparseVectorParams:
    modifier: str | None = None


def install_qdrant_client_stub() -> None:
    models = types.SimpleNamespace(
        Distance=types.SimpleNamespace(COSINE="cosine"),
        VectorParams=StubVectorParams,
        MatchValue=StubMatchValue,
        FieldCondition=StubFieldCondition,
        Filter=StubFilter,
        FilterSelector=StubFilterSelector,
        PointStruct=StubPointStruct,
        SparseVector=StubSparseVector,
        SparseVectorParams=StubSparseVectorParams,
    )
    stub_module = types.ModuleType("qdrant_client")
    stub_module.models = models  # type: ignore[attr-defined]
    sys.modules["qdrant_client"] = stub_module


def test_compute_document_id_is_deterministic() -> None:
    """Verify document IDs are deterministic SHA-256 hashes of the same bytes."""
    payload = b"sample-document-bytes"
    first = compute_document_id(payload)
    second = compute_document_id(payload)

    assert first == second
    assert len(first) == 64


def test_build_ingestion_settings_supports_unstructured_timeout_override() -> None:
    """Confirm UNSTRUCTURED_TIMEOUT_SECONDS is parsed and minimum-clamped."""
    settings = build_ingestion_settings({"UNSTRUCTURED_TIMEOUT_SECONDS": "420"})
    assert settings.unstructured_timeout_seconds == 420.0

    min_clamped = build_ingestion_settings({"UNSTRUCTURED_TIMEOUT_SECONDS": "1"})
    assert min_clamped.unstructured_timeout_seconds == 5.0

    fallback_default = build_ingestion_settings({"UNSTRUCTURED_TIMEOUT_SECONDS": "invalid"})
    assert fallback_default.unstructured_timeout_seconds == 900.0


def test_build_ingestion_settings_supports_qdrant_timeout_override() -> None:
    """Confirm QDRANT_TIMEOUT_SECONDS is parsed and minimum-clamped."""
    settings = build_ingestion_settings({"QDRANT_TIMEOUT_SECONDS": "45"})
    assert settings.qdrant_timeout_seconds == 45.0

    min_clamped = build_ingestion_settings({"QDRANT_TIMEOUT_SECONDS": "0"})
    assert min_clamped.qdrant_timeout_seconds == 1.0

    fallback_default = build_ingestion_settings({"QDRANT_TIMEOUT_SECONDS": "invalid"})
    assert fallback_default.qdrant_timeout_seconds == 30.0


def test_build_ingestion_settings_supports_qdrant_collection_name_override() -> None:
    """Confirm shared Qdrant collection name is configurable and defaults correctly."""
    settings = build_ingestion_settings({})
    assert settings.qdrant_collection_name == "evidence-base"

    overridden = build_ingestion_settings({"QDRANT_COLLECTION_NAME": "custom-evidence-base"})
    assert overridden.qdrant_collection_name == "custom-evidence-base"


def test_build_ingestion_settings_do_not_default_redis_or_qdrant_urls() -> None:
    """Redis and Qdrant targets should be empty until explicitly configured."""
    settings = build_ingestion_settings({})

    assert settings.redis_url == ""
    assert settings.qdrant_url == ""


def test_build_ingestion_service_rejects_missing_required_redis_url() -> None:
    """Required Redis should fail fast before route/task execution."""
    with pytest.raises(DependencyConfigurationError, match="REDIS_URL"):
        build_ingestion_service(
            settings=build_ingestion_settings(
                {
                    "QDRANT_URL": "http://qdrant:6333",
                }
            ),
            env={
                "MCP_EVIDENCEBASE_REQUIRE_REDIS": "true",
                "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "false",
            },
        )


def test_build_ingestion_service_rejects_missing_required_qdrant_url() -> None:
    """Required Qdrant should fail fast before route/task execution."""
    with pytest.raises(DependencyConfigurationError, match="QDRANT_URL"):
        build_ingestion_service(
            settings=build_ingestion_settings(
                {
                    "REDIS_URL": "redis://redis:6379/2",
                }
            ),
            env={
                "MCP_EVIDENCEBASE_REQUIRE_REDIS": "false",
                "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "true",
            },
        )


def test_build_ingestion_service_allows_disabled_redis_and_qdrant() -> None:
    """Reduced-capability mode should build without Redis and Qdrant clients."""
    service = build_ingestion_service(
        settings=build_ingestion_settings({}),
        env={
            "MCP_EVIDENCEBASE_REQUIRE_REDIS": "false",
            "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "false",
        },
    )

    assert getattr(service._repository, "is_disabled", False) is True
    assert getattr(service._qdrant_indexer, "is_disabled", False) is True


def test_get_cached_ingestion_service_reuses_instance_until_reset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Process-local cached ingestion service should be reused until explicitly reset."""
    wiring_module.reset_cached_ingestion_service()
    built_services: list[object] = []

    def fake_build_ingestion_service(*args: Any, **kwargs: Any) -> object:
        del args, kwargs
        service = object()
        built_services.append(service)
        return service

    monkeypatch.setattr(wiring_module, "build_ingestion_service", fake_build_ingestion_service)

    first = wiring_module.get_cached_ingestion_service()
    second = wiring_module.get_cached_ingestion_service()
    wiring_module.reset_cached_ingestion_service()
    third = wiring_module.get_cached_ingestion_service()

    assert first is second
    assert first is not third
    assert len(built_services) == 2


def test_reset_cached_ingestion_service_closes_redis_and_qdrant_clients() -> None:
    """Reset should dispose of shared network clients before clearing the cache."""
    wiring_module.reset_cached_ingestion_service()
    closed_clients: list[str] = []

    class ClosableClient:
        def __init__(self, label: str) -> None:
            self.label = label

        def close(self) -> None:
            closed_clients.append(self.label)

    fake_service = types.SimpleNamespace(
        _repository=types.SimpleNamespace(_redis=ClosableClient("redis")),
        _qdrant_indexer=types.SimpleNamespace(_qdrant_client=ClosableClient("qdrant")),
    )

    wiring_module._CACHED_INGESTION_SERVICE = fake_service  # type: ignore[assignment]

    wiring_module.reset_cached_ingestion_service()

    assert wiring_module._CACHED_INGESTION_SERVICE is None
    assert closed_clients == ["redis", "qdrant"]


def test_build_ingestion_settings_supports_chunking_element_overrides() -> None:
    """Confirm chunking element filters and text controls are parsed from env."""
    settings = build_ingestion_settings(
        {
            "CHUNK_EXCLUDE_ELEMENT_TYPES": "header,footer",
            "CHUNKING_STRATEGY": "by_title",
            "CHUNK_NEW_AFTER_N_CHARS": "2000",
            "CHUNK_COMBINE_TEXT_UNDER_N_CHARS": "500",
            "CHUNK_INCLUDE_TITLE_TEXT": "true",
            "CHUNK_IMAGE_TEXT_MODE": "ocr",
            "CHUNK_PARAGRAPH_BREAK_STRATEGY": "text",
            "CHUNK_PRESERVE_PAGE_BREAKS": "false",
        }
    )

    assert settings.chunk_exclude_element_types == ("header", "footer")
    assert settings.chunking_strategy == "by_title"
    assert settings.chunk_new_after_n_chars == 2000
    assert settings.chunk_combine_text_under_n_chars == 500
    assert settings.chunk_include_title_text is True
    assert settings.chunk_image_text_mode == "ocr"
    assert settings.chunk_paragraph_break_strategy == "text"
    assert settings.chunk_preserve_page_breaks is False


def test_unstructured_partition_client_wraps_read_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure read timeouts raise a clear TimeoutError with tuning guidance."""

    class StubReadTimeout(Exception):
        pass

    class StubClient:
        def __init__(self, timeout: Any) -> None:
            self.timeout = timeout

        def __enter__(self) -> StubClient:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Literal[False]:
            del exc_type, exc, tb
            return False

        def post(self, *args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise StubReadTimeout("The read operation timed out")

    class StubTimeout:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    monkeypatch.setitem(
        sys.modules,
        "httpx",
        types.SimpleNamespace(
            Client=StubClient,
            Timeout=StubTimeout,
            ReadTimeout=StubReadTimeout,
        ),
    )

    client = UnstructuredPartitionClient(
        api_url="https://example.invalid/partition",
        api_key="token",
        strategy="auto",
        timeout_seconds=42.0,
    )

    with pytest.raises(TimeoutError) as exc_info:
        client.partition_file(
            file_name="paper.pdf",
            file_bytes=b"%PDF-1.7 fake",
            content_type="application/pdf",
        )

    message = str(exc_info.value)
    assert "42" in message
    assert "UNSTRUCTURED_TIMEOUT_SECONDS" in message


def test_compute_chunk_point_id_is_deterministic_uuid() -> None:
    """Ensure chunk point IDs are deterministic UUIDs and change by chunk index."""
    first = compute_chunk_point_id(
        bucket_name="research-raw",
        document_id="doc-123",
        chunk_index=0,
    )
    second = compute_chunk_point_id(
        bucket_name="research-raw",
        document_id="doc-123",
        chunk_index=0,
    )
    third = compute_chunk_point_id(
        bucket_name="research-raw",
        document_id="doc-123",
        chunk_index=1,
    )

    assert first == second
    assert first != third
    assert len(first) == 36


def test_chunk_partition_texts_preserves_element_boundaries_without_global_overlap() -> None:
    """Check chunking keeps whole-element boundaries and avoids global overlap."""
    partitions = [{"text": "a" * 60}, {"text": "b" * 60}, {"text": "c" * 60}]

    chunks = chunk_partition_texts(
        partitions,
        chunk_size_chars=100,
        chunk_overlap_chars=20,
    )

    assert len(chunks) == 3
    assert chunks[0] == "a" * 60
    assert chunks[1] == "b" * 60
    assert chunks[2] == "c" * 60


def test_extract_partition_bounding_box_reads_unstructured_coordinates() -> None:
    """Ensure partition coordinate metadata is normalized into a bounding-box payload."""
    bounding_box = extract_partition_bounding_box(
        {
            "metadata": {
                "coordinates": {
                    "points": [[10, 20], [30, 20], [30, 40], [10, 40]],
                    "layout_width": 612,
                    "layout_height": 792,
                    "system": "PixelSpace",
                }
            }
        }
    )

    assert bounding_box == {
        "points": [[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]],
        "layout_width": 612.0,
        "layout_height": 792.0,
        "system": "PixelSpace",
    }


def test_build_partition_chunks_attaches_page_numbers_and_bounding_boxes() -> None:
    """Verify chunk records include source page numbers and bounding-box metadata."""
    partitions = [
        {
            "text": "A" * 90,
            "metadata": {
                "page_number": 1,
                "coordinates": {"points": [[0, 0], [10, 0], [10, 10], [0, 10]]},
            },
        },
        {
            "text": "B" * 90,
            "metadata": {
                "page_number": 2,
                "coordinates": {"points": [[1, 1], [11, 1], [11, 11], [1, 11]]},
            },
        },
    ]

    chunks = build_partition_chunks(
        partitions,
        chunk_size_chars=128,
        chunk_overlap_chars=32,
    )

    assert len(chunks) >= 2
    assert chunks[0]["text"]
    assert 1 in chunks[0]["page_numbers"]
    assert chunks[0]["bounding_boxes"]
    assert "points" in chunks[0]["bounding_boxes"][0]
    assert chunks[0]["bounding_boxes"][0]["page_number"] == 1


def test_extract_metadata_limits_doi_extraction_to_first_page() -> None:
    """Confirm title/author come from PDF metadata and DOI from first-page only."""
    partitions = [
        {
            "text": "Causal Inference in Medicine\nAlice Smith, Bob Jones",
            "metadata": {"page_number": 1},
        },
        {
            "text": "References\n10.9999/doi-from-references",
            "metadata": {"page_number": 12},
        },
    ]

    metadata = extract_metadata_from_partitions(
        partitions=partitions,
        file_path="paper.pdf",
        document_id="doc-1",
        pdf_metadata={"title": "Metadata Title", "author": "Metadata Author"},
    )

    assert metadata["title"] == "Metadata Title"
    assert metadata["author"] == "Metadata Author"
    assert metadata["doi"] == ""


def test_extract_metadata_can_parse_first_page_identifiers() -> None:
    """Verify DOI/ISBN/ISSN are extracted from first-page text only."""
    partitions = [
        {
            "text": (
                "Different Page Title\nDifferent Author\nDOI: 10.1000/xyz123\n"
                "ISBN 978-0-393-04002-9\nISSN 2049-3630"
            ),
            "metadata": {"page_number": 1},
        }
    ]

    metadata = extract_metadata_from_partitions(
        partitions=partitions,
        file_path="study.pdf",
        document_id="doc-2",
        pdf_metadata={"title": "PDF Embedded Title", "author": "PDF Embedded Author"},
    )

    assert metadata["title"] == "PDF Embedded Title"
    assert metadata["author"] == "PDF Embedded Author"
    assert metadata["doi"] == "10.1000/xyz123"
    assert metadata["isbn"] == "9780393040029"
    assert metadata["issn"] == "2049-3630"


def test_fetch_metadata_from_crossref_prefers_doi_and_updates_authors_and_entry_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure DOI lookup is preferred and updates document_type/authors fields."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "research-raw"
    document_id = "doc-crossref-doi"
    object_name = "paper.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={"title": "Local Title", "doi": "10.5555/example-doi"},
    )

    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )
    called_paths: list[tuple[str, dict[str, str]]] = []

    def fake_crossref_get_json(
        *, path: str, params: Mapping[str, str] | None = None
    ) -> Mapping[str, Any]:
        called_paths.append((path, dict(params or {})))
        return {
            "message": {
                "DOI": "10.5555/example-doi",
                "title": ["Crossref Accepted Title"],
                "type": "journal-article",
                "author": [
                    {"given": "Jane", "family": "Doe"},
                    {"given": "John", "family": "Smith"},
                ],
                "issued": {"date-parts": [[2025, 4, 18]]},
                "container-title": ["Journal of Causal Inference"],
            }
        }

    monkeypatch.setattr(service, "_crossref_get_json", fake_crossref_get_json)

    result = service.fetch_metadata_from_crossref(
        bucket_name=bucket_name,
        document_id=document_id,
    )

    assert result["lookup_field"] == "doi"
    assert result["confidence"] == 1.0
    assert called_paths == [("/works/10.5555%2Fexample-doi", {})]

    _, metadata = repository.get_document_metadata(bucket_name, document_id)
    assert metadata["title"] == "Crossref Accepted Title"
    assert metadata["document_type"] == "article"
    assert metadata["journal"] == "Journal of Causal Inference"
    assert metadata["year"] == "2025"
    assert metadata["month"] == "apr"
    assert metadata["author"] == "Doe, J. & Smith, J."
    assert json.loads(metadata["authors"]) == [
        {"first_name": "Jane", "last_name": "Doe", "suffix": ""},
        {"first_name": "John", "last_name": "Smith", "suffix": ""},
    ]


def test_fetch_metadata_from_crossref_prefers_isbn_before_issn_and_title(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify lookup order checks ISBN before ISSN/title when DOI is unavailable."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "research-raw"
    document_id = "doc-crossref-isbn"
    object_name = "book.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={
            "title": "Practical Causal Inference",
            "isbn": "978-0-393-04002-9",
            "issn": "2049-3630",
        },
    )

    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )
    called_queries: list[dict[str, str]] = []

    def fake_crossref_get_json(
        *, path: str, params: Mapping[str, str] | None = None
    ) -> Mapping[str, Any]:
        del path
        query_params = dict(params or {})
        called_queries.append(query_params)
        return {
            "message": {
                "items": [
                    {
                        "DOI": "10.1234/isbn-match",
                        "title": ["Practical Causal Inference"],
                        "type": "book",
                        "ISBN": ["9780393040029"],
                        "issued": {"date-parts": [[2024, 1, 5]]},
                        "publisher": "Example Press",
                    }
                ]
            }
        }

    monkeypatch.setattr(service, "_crossref_get_json", fake_crossref_get_json)

    result = service.fetch_metadata_from_crossref(
        bucket_name=bucket_name,
        document_id=document_id,
    )

    assert result["lookup_field"] == "isbn"
    assert len(called_queries) == 1
    assert called_queries[0]["filter"] == "isbn:9780393040029"


def test_fetch_metadata_from_crossref_rejects_low_confidence_title_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure low-confidence title matches are not accepted."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "research-raw"
    document_id = "doc-crossref-title"
    object_name = "note.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={"title": "Causal Graphs in Medicine"},
    )

    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    def fake_crossref_get_json(
        *, path: str, params: Mapping[str, str] | None = None
    ) -> Mapping[str, Any]:
        del path, params
        return {
            "message": {
                "items": [
                    {
                        "DOI": "10.9999/unrelated",
                        "title": ["Completely Different Topic"],
                        "type": "journal-article",
                    }
                ]
            }
        }

    monkeypatch.setattr(service, "_crossref_get_json", fake_crossref_get_json)

    with pytest.raises(ValueError) as exc_info:
        service.fetch_metadata_from_crossref(
            bucket_name=bucket_name,
            document_id=document_id,
        )

    assert "No high-confidence Crossref match was accepted" in str(exc_info.value)


def test_fetch_metadata_from_crossref_parses_name_only_author_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure title lookups accept Crossref author entries that only expose ``name``."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "research-raw"
    document_id = "doc-crossref-name-only-author"
    object_name = "chapter.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={"title": "Mandatory defense offsets-conceptual foundations"},
    )

    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    def fake_crossref_get_json(
        *, path: str, params: Mapping[str, str] | None = None
    ) -> Mapping[str, Any]:
        del path, params
        return {
            "message": {
                "items": [
                    {
                        "DOI": "10.4324/9780203392300-12",
                        "title": ["Mandatory defense offsets-conceptual foundations"],
                        "type": "book-chapter",
                        "author": [{"name": "Ron Matthews"}],
                        "container-title": ["Arms Trade and Economic Development"],
                    }
                ]
            }
        }

    monkeypatch.setattr(service, "_crossref_get_json", fake_crossref_get_json)

    result = service.fetch_metadata_from_crossref(
        bucket_name=bucket_name,
        document_id=document_id,
    )

    assert result["lookup_field"] == "title"
    _, metadata = repository.get_document_metadata(bucket_name, document_id)
    assert metadata["author"] == "Matthews, R."
    assert json.loads(metadata["authors"]) == [
        {"first_name": "Ron", "last_name": "Matthews", "suffix": ""}
    ]


def test_fetch_metadata_from_crossref_falls_back_to_editors_when_author_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure editor-only Crossref records still populate displayable creator fields."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "research-raw"
    document_id = "doc-crossref-editor-only"
    object_name = "volume.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={"title": "Offsets and Industrial Cooperation"},
    )

    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    def fake_crossref_get_json(
        *, path: str, params: Mapping[str, str] | None = None
    ) -> Mapping[str, Any]:
        del path, params
        return {
            "message": {
                "items": [
                    {
                        "DOI": "10.5555/editor-only",
                        "title": ["Offsets and Industrial Cooperation"],
                        "type": "edited-book",
                        "editor": [
                            {"given": "Ron", "family": "Matthews"},
                            {"given": "Keith", "family": "Hartley"},
                        ],
                        "publisher": "Routledge",
                    }
                ]
            }
        }

    monkeypatch.setattr(service, "_crossref_get_json", fake_crossref_get_json)

    result = service.fetch_metadata_from_crossref(
        bucket_name=bucket_name,
        document_id=document_id,
    )

    assert result["lookup_field"] == "title"
    _, metadata = repository.get_document_metadata(bucket_name, document_id)
    assert metadata["author"] == "Matthews, R. & Hartley, K."
    assert metadata["editor"] == "Matthews, R. & Hartley, K."
    assert metadata["authors"] == ""


def test_fetch_metadata_from_crossref_tries_next_candidate_when_first_has_no_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure title lookups continue when the top-scoring candidate adds no new fields."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "research-raw"
    document_id = "doc-crossref-title-next-candidate"
    object_name = "offsets.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={"title": "Using procurement offsets as an economic development strategy"},
    )

    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    def fake_crossref_get_json(
        *, path: str, params: Mapping[str, str] | None = None
    ) -> Mapping[str, Any]:
        del path, params
        return {
            "message": {
                "items": [
                    {
                        "DOI": "10.9999/no-op",
                        "title": ["Using procurement offsets as an economic development strategy"],
                        "type": "book-chapter",
                    },
                    {
                        "DOI": "10.4324/9780203392300-9",
                        "title": ["Using procurement offsets as an economic development strategy"],
                        "type": "book-chapter",
                        "author": [{"given": "Travis", "family": "Taylor"}],
                        "container-title": ["Arms Trade and Economic Development"],
                    },
                ]
            }
        }

    monkeypatch.setattr(service, "_crossref_get_json", fake_crossref_get_json)

    result = service.fetch_metadata_from_crossref(
        bucket_name=bucket_name,
        document_id=document_id,
    )

    assert result["lookup_field"] == "title"
    _, metadata = repository.get_document_metadata(bucket_name, document_id)
    assert metadata["author"] == "Taylor, T."
    assert json.loads(metadata["authors"]) == [
        {"first_name": "Travis", "last_name": "Taylor", "suffix": ""}
    ]


def test_crossref_request_kind_detects_single_and_list_requests() -> None:
    """Ensure Crossref request paths are classified correctly."""
    assert IngestionService._crossref_request_kind("/works/10.5555%2Fexample-doi") == "single"
    assert IngestionService._crossref_request_kind("/works") == "list"


def test_crossref_rate_limit_enforces_list_and_single_intervals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify list requests wait ~1s and single-record requests wait ~0.2s."""
    now = {"value": 100.0}
    sleep_calls: list[float] = []

    def fake_monotonic() -> float:
        return float(now["value"])

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        now["value"] = float(now["value"]) + seconds

    monkeypatch.setattr(ingestion_module.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(ingestion_module.time, "sleep", fake_sleep)

    IngestionService._crossref_next_allowed_global_ts = 0.0
    IngestionService._crossref_next_allowed_single_ts = 0.0
    IngestionService._crossref_next_allowed_list_ts = 0.0

    with IngestionService._crossref_rate_lock:
        IngestionService._crossref_enforce_rate_limit_locked("list")
    with IngestionService._crossref_rate_lock:
        IngestionService._crossref_enforce_rate_limit_locked("list")

    assert sleep_calls == [pytest.approx(1.0, abs=1e-6)]

    sleep_calls.clear()
    IngestionService._crossref_next_allowed_global_ts = 0.0
    IngestionService._crossref_next_allowed_single_ts = 0.0
    IngestionService._crossref_next_allowed_list_ts = 0.0
    now["value"] = 250.0

    with IngestionService._crossref_rate_lock:
        IngestionService._crossref_enforce_rate_limit_locked("single")
    with IngestionService._crossref_rate_lock:
        IngestionService._crossref_enforce_rate_limit_locked("single")

    assert sleep_calls == [pytest.approx(0.2, abs=1e-6)]


def test_partition_object_persists_partitions_and_metadata_without_chunking() -> None:
    """Ensure partition stage stores partitions/metadata and does not upsert vectors."""
    payload = b"%PDF-1.7 fake"
    partitions = [
        {
            "text": "Page one text DOI 10.1000/abc123 ISBN 978-0-393-04002-9",
            "metadata": {"page_number": 1},
        }
    ]
    minio_client = FakeMinioClient(payload, etag="etag-remote")
    repository = RedisDocumentRepository(FakeRedis(), key_prefix="test")
    partition_client = FakePartitionClient(partitions)
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=partition_client,
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    result = service.partition_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        etag=None,
    )

    expected_document_id = compute_document_id(payload)
    assert result["document_id"] == expected_document_id
    assert result["partition_key"]
    assert result["meta_key"]
    assert repository.get_partitions_by_key(result["partition_key"]) == partitions
    assert repository.get_metadata_by_key(result["meta_key"])["doi"] == "10.1000/abc123"
    assert qdrant_indexer.upsert_calls == []
    assert len(partition_client.calls) == 1

    state = repository.get_state(expected_document_id)
    assert state["processing_state"] == "processing"
    assert state["processing_stage"] == "meta"
    assert state["processing_stage_progress"] == "100"
    assert state["processing_progress"] == "40"
    assert state["partitions_count"] == "1"
    assert state["meta_key"] == result["meta_key"]


def test_chunk_object_reads_persisted_partitions_and_marks_processed() -> None:
    """Verify chunk stage reads stored partition payload and writes final processed state."""
    payload = b"%PDF-1.7 fake"
    partitions = [{"text": "A" * 200, "metadata": {"page_number": 1}}]
    minio_client = FakeMinioClient(payload, etag="etag-remote")
    repository = RedisDocumentRepository(FakeRedis(), key_prefix="test")
    partition_client = FakePartitionClient(partitions)
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=partition_client,
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=128,
        chunk_overlap_chars=32,
    )

    partition_stage = service.partition_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        etag="etag-incoming",
    )
    chunk_stage = service.chunk_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id=partition_stage["document_id"],
    )

    assert chunk_stage["document_id"] == partition_stage["document_id"]
    assert len(partition_client.calls) == 1
    assert len(qdrant_indexer.upsert_calls) == 1
    upsert_call = qdrant_indexer.upsert_calls[0]
    assert upsert_call["partition_key"] == partition_stage["partition_key"]
    assert upsert_call["meta_key"] == partition_stage["meta_key"]
    _, metadata = repository.get_document_metadata("research-raw", partition_stage["document_id"])
    assert upsert_call["document_year"] == metadata.get("year", "")
    assert upsert_call["chunks"]

    state = repository.get_state(partition_stage["document_id"])
    assert state["processing_state"] == "processed"
    assert state["processing_stage"] == "processed"
    assert state["processing_stage_progress"] == "100"
    assert state["processing_progress"] == "100"
    assert int(state["chunks_count"]) >= 1


def test_list_documents_populates_chunks_tree_for_processed_documents() -> None:
    """Ensure list_documents returns computed chunk payloads for modal chunk viewing."""
    payload = b"%PDF-1.7 fake"
    partitions = [{"text": "A" * 220, "metadata": {"page_number": 1}}]
    minio_client = FakeMinioClient(payload, etag="etag-remote")
    repository = RedisDocumentRepository(FakeRedis(), key_prefix="test")
    partition_client = FakePartitionClient(partitions)
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=partition_client,
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=128,
        chunk_overlap_chars=32,
    )

    partition_stage = service.partition_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        etag="etag-incoming",
    )
    service.chunk_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id=partition_stage["document_id"],
    )

    documents = service.list_documents("research-raw")

    assert len(documents) == 1
    chunks_tree = documents[0]["chunks_tree"]
    assert isinstance(chunks_tree, dict)
    chunks = chunks_tree["chunks"]
    assert isinstance(chunks, list)
    assert len(chunks) >= 1
    assert chunks[0]["text"]
    assert "chunk_id" in chunks[0]


def test_list_documents_can_skip_debug_payloads_for_summary_views() -> None:
    """Ensure summary document listing omits partitions/chunks payloads when requested."""
    payload = b"%PDF-1.7 fake"
    partitions = [{"text": "A" * 220, "metadata": {"page_number": 1}}]
    minio_client = FakeMinioClient(payload, etag="etag-remote")
    repository = RedisDocumentRepository(FakeRedis(), key_prefix="test")
    partition_client = FakePartitionClient(partitions)
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=partition_client,
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=128,
        chunk_overlap_chars=32,
    )

    partition_stage = service.partition_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        etag="etag-incoming",
    )
    service.chunk_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id=partition_stage["document_id"],
    )

    documents = service.list_documents(
        "research-raw",
        include_debug=False,
        include_locations=False,
    )

    assert len(documents) == 1
    assert "partitions_tree" not in documents[0]
    assert "chunks_tree" not in documents[0]
    assert "locations" not in documents[0]
    assert documents[0]["document_id"] == partition_stage["document_id"]


def test_get_document_debug_payload_returns_partitions_and_chunks() -> None:
    """Ensure on-demand debug payload returns one document's partitions and computed chunks."""
    payload = b"%PDF-1.7 fake"
    partitions = [{"text": "A" * 220, "metadata": {"page_number": 1}}]
    minio_client = FakeMinioClient(payload, etag="etag-remote")
    repository = RedisDocumentRepository(FakeRedis(), key_prefix="test")
    partition_client = FakePartitionClient(partitions)
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=partition_client,
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=128,
        chunk_overlap_chars=32,
    )

    partition_stage = service.partition_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        etag="etag-incoming",
    )
    service.chunk_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id=partition_stage["document_id"],
    )

    payload = service.get_document_debug_payload(
        bucket_name="research-raw",
        document_id=partition_stage["document_id"],
    )

    assert payload["document_id"] == partition_stage["document_id"]
    assert payload["partitions_tree"]["partitions"]
    assert payload["chunks_tree"]["chunks"]


def test_extract_pdf_title_author_reads_pdf_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify PDF title/author extraction reads embedded metadata values."""

    class FakePdfMetadata:
        title = "Embedded Title"
        author = "Embedded Author"

    class FakePdfReader:
        def __init__(self, stream: Any) -> None:
            del stream
            self.metadata = FakePdfMetadata()

    monkeypatch.setitem(sys.modules, "pypdf", types.SimpleNamespace(PdfReader=FakePdfReader))

    extracted = extract_pdf_title_author(b"%PDF-1.7 fake")

    assert extracted == {"title": "Embedded Title", "author": "Embedded Author"}


def test_repository_maps_document_to_multiple_minio_locations() -> None:
    """Ensure one document can map to multiple MinIO object locations deterministically."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    repository.add_document("research-raw", "doc-123")
    repository.mark_object(
        bucket_name="research-raw",
        object_name="folder/paper.pdf",
        document_id="doc-123",
        etag="etag-1",
    )
    repository.mark_object(
        bucket_name="research-raw",
        object_name="paper-copy.pdf",
        document_id="doc-123",
        etag="etag-2",
    )

    assert repository.get_document_object_names("research-raw", "doc-123") == [
        "folder/paper.pdf",
        "paper-copy.pdf",
    ]
    assert repository.get_document_locations("doc-123", "research-raw") == [
        "research-raw/folder/paper.pdf",
        "research-raw/paper-copy.pdf",
    ]


def test_repository_persists_isbn_metadata_field() -> None:
    """Ensure ISBN is stored and returned through metadata payload fields."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    meta_key = repository.set_metadata_for_location(
        bucket_name="research-raw",
        object_name="book.pdf",
        document_id="doc-isbn",
        metadata={"title": "Book Title", "isbn": "9780393040029"},
    )

    metadata = repository.get_metadata_by_key(meta_key)
    assert metadata["title"] == "Book Title"
    assert metadata["isbn"] == "9780393040029"


def test_repository_persists_issn_metadata_field() -> None:
    """Ensure ISSN is stored and returned through metadata payload fields."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    meta_key = repository.set_metadata_for_location(
        bucket_name="research-raw",
        object_name="journal.pdf",
        document_id="doc-issn",
        metadata={"title": "Journal Title", "issn": "2049-3630"},
    )

    metadata = repository.get_metadata_by_key(meta_key)
    assert metadata["title"] == "Journal Title"
    assert metadata["issn"] == "2049-3630"


def test_repository_defaults_citation_key_from_author_year_and_title() -> None:
    """Ensure missing citation keys default to ``firstAuthorLastName + year + firstTitleWord``."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    meta_key = repository.set_metadata_for_location(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id="doc-citekey",
        metadata={
            "author": "Doe, Jane and Smith, John",
            "title": "Causal Inference in Practice",
            "year": "2024",
        },
    )

    metadata = repository.get_metadata_by_key(meta_key)
    assert metadata["citation_key"] == "doe2024causal"


def test_repository_defaults_citation_key_from_structured_authors() -> None:
    """Ensure structured authors are preferred when deriving the citation key."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    meta_key = repository.set_metadata_for_location(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id="doc-citekey-structured",
        metadata={
            "authors": [{"first_name": "Jane", "last_name": "van Rossum", "suffix": ""}],
            "title": "{The} Causal Frontier",
            "year": "2024a",
        },
    )

    metadata = repository.get_metadata_by_key(meta_key)
    assert metadata["citation_key"] == "vanrossum2024the"


def test_repository_defaults_citation_key_from_chapter_filename_token() -> None:
    """Ensure chapter-marked filenames become ``chN<word>`` citation title tokens."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    meta_key = repository.set_metadata_for_location(
        bucket_name="research-raw",
        object_name="Gertler 2010 Chapter 1 Introduction.pdf",
        document_id="doc-citekey-chapter",
        metadata={
            "author": "Gertler, Paul",
            "title": "Introduction",
            "year": "2010",
        },
    )

    metadata = repository.get_metadata_by_key(meta_key)
    assert metadata["citation_key"] == "gertler2010ch1introduction"


@pytest.mark.parametrize(
    "metadata_update",
    [
        {"citation_key": "newkey2024paper"},
        {"title": "Retitled Paper"},
        {"author": "Smith, J."},
    ],
)
def test_update_metadata_does_not_reindex_when_non_year_index_fields_change(
    metadata_update: dict[str, str],
) -> None:
    """Ensure non-year metadata changes do not force Qdrant re-upsert."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )
    bucket_name = "research-raw"
    document_id = "doc-citation-refresh"
    object_name = "paper.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    partition_key = repository.set_partitions(
        bucket_name,
        document_id,
        [{"text": "Chunk text for metadata refresh.", "metadata": {"page_number": 1}}],
    )
    meta_key = repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={
            "title": "Paper Title",
            "author": "Doe, J.",
            "year": "2024",
            "citation_key": "oldkey2024paper",
        },
    )
    repository.set_state(
        document_id,
        {
            "file_path": object_name,
            "etag": "etag-1",
            "partition_key": partition_key,
            "meta_key": meta_key,
            "processing_state": "processed",
            "processing_stage": "processed",
            "processing_stage_progress": 100,
            "processing_progress": 100,
            "partitions_count": 1,
            "chunks_count": 1,
        },
    )

    updated = service.update_metadata(
        bucket_name=bucket_name,
        document_id=document_id,
        metadata=metadata_update,
    )

    for field_name, field_value in metadata_update.items():
        assert updated[field_name] == field_value
    assert qdrant_indexer.upsert_calls == []


def test_update_metadata_reindexes_vectors_when_year_changes() -> None:
    """Ensure year updates refresh Qdrant payloads for existing chunks."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )
    bucket_name = "research-raw"
    document_id = "doc-citation-stable"
    object_name = "paper.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    partition_key = repository.set_partitions(
        bucket_name,
        document_id,
        [{"text": "Chunk text for stable citation key.", "metadata": {"page_number": 1}}],
    )
    meta_key = repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={"title": "Paper Title", "citation_key": "stablekey2024paper"},
    )
    repository.set_state(
        document_id,
        {
            "file_path": object_name,
            "etag": "etag-1",
            "partition_key": partition_key,
            "meta_key": meta_key,
            "processing_state": "processed",
            "processing_stage": "processed",
            "processing_stage_progress": 100,
            "processing_progress": 100,
            "partitions_count": 1,
            "chunks_count": 1,
        },
    )

    updated = service.update_metadata(
        bucket_name=bucket_name,
        document_id=document_id,
        metadata={"year": "2025"},
    )

    assert updated["year"] == "2025"
    assert len(qdrant_indexer.upsert_calls) == 1
    call = qdrant_indexer.upsert_calls[0]
    assert call["document_id"] == document_id
    assert call["file_path"] == object_name
    assert call["document_year"] == "2025"


def test_update_metadata_does_not_reindex_when_non_indexed_metadata_changes() -> None:
    """Ensure fields not persisted in Qdrant chunk payload don't force re-upsert."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )
    bucket_name = "research-raw"
    document_id = "doc-nonindexed-stable"
    object_name = "paper.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    partition_key = repository.set_partitions(
        bucket_name,
        document_id,
        [{"text": "Chunk text for stable indexed metadata.", "metadata": {"page_number": 1}}],
    )
    meta_key = repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={
            "title": "Paper Title",
            "author": "Doe, J.",
            "year": "2024",
            "citation_key": "stablekey2024paper",
        },
    )
    repository.set_state(
        document_id,
        {
            "file_path": object_name,
            "etag": "etag-1",
            "partition_key": partition_key,
            "meta_key": meta_key,
            "processing_state": "processed",
            "processing_stage": "processed",
            "processing_stage_progress": 100,
            "processing_progress": 100,
            "partitions_count": 1,
            "chunks_count": 1,
        },
    )

    updated = service.update_metadata(
        bucket_name=bucket_name,
        document_id=document_id,
        metadata={"journal": "Journal of Causal Methods"},
    )

    assert updated["journal"] == "Journal of Causal Methods"
    assert qdrant_indexer.upsert_calls == []
    state = repository.get_state(document_id)
    assert state["processing_state"] == "processed"
    assert state["processing_stage"] == "processed"
    assert state["processing_progress"] == "100"
    assert state["processing_stage_progress"] == "100"


def test_update_metadata_clears_stale_upsert_state_when_no_reindex_is_needed() -> None:
    """Ensure citation-key-only updates do not leave documents parked at upsert 80%."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )
    bucket_name = "research-raw"
    document_id = "doc-citation-upsert-stale"
    object_name = "paper.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    partition_key = repository.set_partitions(
        bucket_name,
        document_id,
        [{"text": "Chunk text for stale upsert state.", "metadata": {"page_number": 1}}],
    )
    meta_key = repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={
            "title": "Paper Title",
            "author": "Doe, J.",
            "year": "2024",
            "citation_key": "oldkey2024paper",
        },
    )
    repository.set_state(
        document_id,
        {
            "file_path": object_name,
            "etag": "etag-1",
            "partition_key": partition_key,
            "meta_key": meta_key,
            "processing_state": "processing",
            "processing_stage": "upsert",
            "processing_stage_progress": 0,
            "processing_progress": 80,
            "partitions_count": 1,
            "chunks_count": 1,
            "sections_count": 1,
        },
    )

    updated = service.update_metadata(
        bucket_name=bucket_name,
        document_id=document_id,
        metadata={"citation_key": "doe2024paper"},
    )

    assert updated["citation_key"] == "doe2024paper"
    assert qdrant_indexer.upsert_calls == []
    state = repository.get_state(document_id)
    assert state["processing_state"] == "processed"
    assert state["processing_stage"] == "processed"
    assert state["processing_progress"] == "100"
    assert state["processing_stage_progress"] == "100"


def test_repository_persists_structured_authors_metadata_field() -> None:
    """Ensure structured author entries are serialized and returned from metadata payload."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    structured_authors = [
        {"first_name": "Jane", "last_name": "Doe", "suffix": ""},
        {"first_name": "John", "last_name": "Smith", "suffix": "Jr."},
    ]
    meta_key = repository.set_metadata_for_location(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id="doc-authors",
        metadata={"title": "Paper Title", "authors": structured_authors},
    )

    metadata = repository.get_metadata_by_key(meta_key)
    assert metadata["title"] == "Paper Title"
    assert json.loads(metadata["authors"]) == structured_authors


def test_list_documents_includes_structured_authors_field() -> None:
    """Verify list_documents surfaces normalized structured author entries."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    service = IngestionService(
        minio_client=FakeMinioClient(b"%PDF-1.7 fake"),
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )
    bucket_name = "research-raw"
    document_id = "doc-authors"
    object_name = "paper.pdf"
    structured_authors = [
        {"first_name": "Jane", "last_name": "Doe", "suffix": ""},
        {"first_name": "John", "last_name": "Smith", "suffix": "Jr."},
    ]

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        etag="etag-1",
    )
    meta_key = repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id=document_id,
        metadata={
            "title": "Paper Title",
            "author": "Doe, J. & Smith, J., Jr.",
            "authors": structured_authors,
        },
    )
    repository.set_state(
        document_id,
        {
            "file_path": object_name,
            "meta_key": meta_key,
            "partition_key": "",
            "processing_state": "processed",
            "processing_progress": 100,
            "partitions_count": 0,
            "chunks_count": 0,
        },
    )

    documents = service.list_documents(bucket_name)
    assert len(documents) == 1
    assert documents[0]["authors"] == structured_authors
    assert documents[0]["author"] == "Doe, J. & Smith, J., Jr."


def test_repository_relocate_source_location_updates_mapping_meta_and_state() -> None:
    """Relocation should move source-scoped Redis state and preserve other aliases."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "offsets"
    document_id = "doc-relocate"
    old_object_name = "articles/relocate-me.pdf"
    new_object_name = "relocate-me.pdf"
    alias_object_name = "00-relocate-title.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=old_object_name,
        document_id=document_id,
        etag="etag-old",
    )
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=alias_object_name,
        document_id=document_id,
        etag="etag-alias",
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=old_object_name,
        document_id=document_id,
        metadata={"title": "Relocation test", "year": "2025"},
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=alias_object_name,
        document_id=document_id,
        metadata={"title": "Alias title", "year": "2024"},
    )
    repository.set_state(
        document_id,
        {
            "file_path": old_object_name,
            "meta_key": f"{bucket_name}/{old_object_name}",
            "partition_key": "partition-1",
            "sections_count": 3,
            "chunks_count": 4,
        },
    )

    result = repository.relocate_source_location(
        bucket_name=bucket_name,
        document_id=document_id,
        old_object_name=old_object_name,
        new_object_name=new_object_name,
        etag="etag-new",
    )

    assert result["new_object_name"] == new_object_name
    assert repository.get_object_mapping(bucket_name, old_object_name) == {}
    new_mapping = repository.get_object_mapping(bucket_name, new_object_name)
    assert new_mapping["document_id"] == document_id
    assert new_mapping["object_name"] == new_object_name
    assert new_mapping["etag"] == "etag-new"
    assert (
        repository.get_metadata_by_key(f"{bucket_name}/{new_object_name}")["title"]
        == "Relocation test"
    )
    assert repository.get_metadata_by_key(f"{bucket_name}/{old_object_name}")["title"] == ""
    assert (
        repository.get_object_mapping(bucket_name, alias_object_name)["document_id"] == document_id
    )
    state = repository.get_state(document_id)
    assert state["file_path"] == new_object_name
    assert state["meta_key"] == f"{bucket_name}/{new_object_name}"
    assert state["partition_key"] == "partition-1"
    assert state["sections_count"] == "3"


def test_repository_prefers_state_backed_file_path_and_meta_key_for_document_reads() -> None:
    """Canonical reads should follow state-backed file path/meta key over sorted aliases."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "offsets"
    document_id = "doc-canonical"
    preferred_object_name = "article-root.pdf"
    alias_object_name = "00-title-alias.pdf"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=preferred_object_name,
        document_id=document_id,
        etag="etag-preferred",
    )
    repository.mark_object(
        bucket_name=bucket_name,
        object_name=alias_object_name,
        document_id=document_id,
        etag="etag-alias",
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=preferred_object_name,
        document_id=document_id,
        metadata={"title": "Preferred title", "year": "2025"},
    )
    repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=alias_object_name,
        document_id=document_id,
        metadata={"title": "Alias title", "year": "2024"},
    )
    repository.set_state(
        document_id,
        {
            "file_path": preferred_object_name,
            "meta_key": f"{bucket_name}/{preferred_object_name}",
            "processing_state": "processed",
            "processing_stage": "processed",
            "processing_progress": 100,
            "processing_stage_progress": 100,
        },
    )

    meta_key, metadata = repository.get_document_metadata(bucket_name, document_id)
    assert meta_key == f"{bucket_name}/{preferred_object_name}"
    assert metadata["title"] == "Preferred title"

    records = repository.list_documents(bucket_name)
    assert len(records) == 1
    assert records[0]["file_path"] == preferred_object_name
    assert records[0]["meta_key"] == f"{bucket_name}/{preferred_object_name}"
    assert records[0]["title"] == "Preferred title"


def test_remove_document_keeps_partitions_but_removes_other_redis_data() -> None:
    """Check document removal keeps partitions but clears source/meta mappings."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "research-raw"
    document_id = "doc-123"

    repository.add_document(bucket_name, document_id)
    repository.mark_object(
        bucket_name=bucket_name,
        object_name="paper.pdf",
        document_id=document_id,
        etag="etag-1",
    )
    partition_key = repository.set_partitions(
        bucket_name,
        document_id,
        [{"text": "partition text"}],
    )
    meta_key = repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name="paper.pdf",
        document_id=document_id,
        metadata={"title": "Paper"},
    )
    repository.set_state(
        document_id,
        {
            "processing_state": "processed",
            "partition_key": partition_key,
            "meta_key": meta_key,
            "chunks_count": 3,
        },
    )

    removed = repository.remove_document(
        bucket_name=bucket_name,
        document_id=document_id,
        keep_partitions=True,
    )

    assert removed is True
    assert repository.list_document_ids(bucket_name) == []
    state = repository.get_state(document_id)
    assert state["processing_state"] == "processed"
    assert state["partition_key"] == partition_key
    assert state["meta_key"] == ""
    assert repository.get_partitions_by_key(partition_key) == [{"text": "partition text"}]
    assert repository.get_metadata_by_key(meta_key)["title"] == ""
    assert repository.get_chunks(bucket_name, document_id) == []
    assert repository.get_object_mapping(bucket_name, "paper.pdf") == {}


def test_set_partitions_stores_payload_under_document_partition_key() -> None:
    """Ensure partition payloads are nested under the owning document key."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    document_id = "doc-789"

    partition_key = repository.set_partitions(
        "research-raw",
        document_id,
        [{"text": "partition text"}],
    )

    assert repository.get_partitions_by_key(partition_key) == [{"text": "partition text"}]
    assert redis_client.get(f"test:document:{document_id}:partition") is not None
    assert redis_client.get(f"test:partition:{partition_key}") is None


def test_set_metadata_for_location_stores_payload_under_source_meta_key() -> None:
    """Ensure metadata payloads are nested under source keys."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    bucket_name = "research-raw"
    object_name = "paper.pdf"

    meta_key = repository.set_metadata_for_location(
        bucket_name=bucket_name,
        object_name=object_name,
        document_id="doc-meta",
        metadata={"title": "Paper"},
    )

    assert meta_key == "research-raw/paper.pdf"
    assert (
        redis_client.hgetall(f"test:source:{bucket_name}/{object_name}:meta").get("title", "")
        == "Paper"
    )
    assert redis_client.hgetall(f"test:meta:{bucket_name}/{object_name}") == {}


def test_get_partitions_by_key_does_not_read_legacy_storage() -> None:
    """Verify legacy reverse-index/document partition keys are ignored."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    document_id = "doc-legacy"
    partition_key = "legacy-partition-key"

    redis_client.set(
        f"test:document:{document_id}:partitions",
        '[{"text":"legacy partition"}]',
    )
    redis_client.set(f"test:document:partition:{partition_key}", document_id)
    repository.set_state(document_id, {"partition_key": partition_key})

    partitions = repository.get_partitions_by_key(partition_key)

    assert partitions == []
    assert redis_client.get(f"test:partition:{partition_key}") is None
    assert redis_client.get(f"test:document:{document_id}:partition") is None
    assert redis_client.get(f"test:document:partition:{partition_key}") == document_id
    assert redis_client.get(f"test:document:{document_id}:partitions") is not None


def test_repository_purge_prefix_data_deletes_only_prefixed_keys() -> None:
    """Ensure purge removes only keys under the configured Redis prefix."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    redis_client.set("test:one", "1")
    redis_client.hset("test:two", mapping={"field": "value"})
    redis_client.sadd("test:three", "member")
    redis_client.set("other:one", "2")

    deleted = repository.purge_prefix_data()

    assert deleted == 3
    assert redis_client.get("test:one") is None
    assert redis_client.hgetall("test:two") == {}
    assert redis_client.smembers("test:three") == set()
    assert redis_client.get("other:one") == "2"


def test_qdrant_indexer_purge_prefixed_collections() -> None:
    """Validate only collections with the configured prefix are deleted."""
    qdrant = FakeQdrantClient()
    qdrant.collection_names = {
        "evidence-base",
        "evidencebase_research_raw",
        "evidencebase_other",
        "unrelated_collection",
    }
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    deleted = indexer.purge_prefixed_collections()

    assert deleted == 3
    assert "evidence-base" not in qdrant.collection_names
    assert "unrelated_collection" in qdrant.collection_names
    assert "evidencebase_research_raw" not in qdrant.collection_names
    assert "evidencebase_other" not in qdrant.collection_names


def test_qdrant_rewrite_document_source_paths_updates_payload_only() -> None:
    """Relocation should patch only payload path fields for existing points."""
    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    qdrant.collection_names = {"evidence-base"}
    qdrant.collection_points["evidence-base"] = [
        types.SimpleNamespace(
            id="point-1",
            vector={"dense": [0.1, 0.2, 0.3]},
            payload={
                "evidence_base_collection": "offsets",
                "document_id": "doc-relocate",
                "minio_location": "offsets/articles/relocate-me.pdf",
                "resolver_url": "docs://offsets/articles/relocate-me.pdf?page=2",
                "file_path": "articles/relocate-me.pdf",
                "page_start": 2,
            },
        ),
        types.SimpleNamespace(
            id="point-2",
            vector={"dense": [0.1, 0.2, 0.3]},
            payload={
                "evidence_base_collection": "offsets",
                "document_id": "doc-relocate",
                "minio_location": "offsets/articles/relocate-me.pdf",
                "resolver_url": "docs://offsets/articles/relocate-me.pdf?page=5",
                "page_start": 5,
            },
        ),
        types.SimpleNamespace(
            id="point-3",
            vector={"dense": [0.1, 0.2, 0.3]},
            payload={
                "evidence_base_collection": "other-bucket",
                "document_id": "doc-relocate",
                "minio_location": "other-bucket/articles/leave-alone.pdf",
                "resolver_url": "docs://other-bucket/articles/leave-alone.pdf?page=3",
                "file_path": "articles/leave-alone.pdf",
                "page_start": 3,
            },
        ),
    ]
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    updated_count = indexer.rewrite_document_source_paths(
        bucket_name="offsets",
        document_id="doc-relocate",
        old_object_name="articles/relocate-me.pdf",
        new_object_name="relocate-me.pdf",
    )

    assert updated_count == 2
    assert qdrant.upsert_calls == []
    assert qdrant.delete_calls == []
    assert len(qdrant.set_payload_calls) == 3
    payloads = [call["payload"] for call in qdrant.set_payload_calls]
    assert {
        "minio_location": "offsets/relocate-me.pdf",
        "file_path": "relocate-me.pdf",
        "evidence_base_collection": "offsets",
        "collection_name": "offsets",
    } in payloads
    assert {"resolver_url": "docs://offsets/relocate-me.pdf?page=2"} in payloads
    assert {"resolver_url": "docs://offsets/relocate-me.pdf?page=5"} in payloads
    for point in qdrant.collection_points["evidence-base"][:2]:
        assert point.payload["minio_location"] == "offsets/relocate-me.pdf"
        assert point.payload["file_path"] == "relocate-me.pdf"
        assert str(point.payload["resolver_url"]).startswith("docs://offsets/relocate-me.pdf")
    untouched_point = qdrant.collection_points["evidence-base"][2]
    assert untouched_point.payload["minio_location"] == "other-bucket/articles/leave-alone.pdf"
    assert untouched_point.payload["file_path"] == "articles/leave-alone.pdf"


def test_qdrant_delete_document_filters_by_document_and_collection() -> None:
    """Deleting one document should not remove same-id points from other folders."""
    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    qdrant.collection_names = {"evidence-base"}
    qdrant.collection_points["evidence-base"] = [
        types.SimpleNamespace(
            id="point-1",
            vector={"dense": [0.1, 0.2, 0.3]},
            payload={"document_id": "doc-1", "evidence_base_collection": "offsets"},
        ),
        types.SimpleNamespace(
            id="point-2",
            vector={"dense": [0.1, 0.2, 0.3]},
            payload={"document_id": "doc-1", "evidence_base_collection": "other"},
        ),
    ]
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    indexer.delete_document("offsets", "doc-1")

    remaining_points = qdrant.collection_points["evidence-base"]
    assert [getattr(point, "id", "") for point in remaining_points] == ["point-2"]


def test_qdrant_indexer_migrates_legacy_collections_into_shared_collection() -> None:
    """Legacy per-bucket collections should backfill into the shared collection."""
    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    qdrant.collection_names = {"evidencebase_offsets", "evidencebase_other"}
    qdrant.collection_points["evidencebase_offsets"] = [
        types.SimpleNamespace(
            id="point-1",
            vector={
                "dense": [0.1, 0.2, 0.3],
                "keyword": StubSparseVector(indices=[1], values=[1.0]),
            },
            payload={"document_id": "doc-1", "collection_name": "offsets", "file_path": "a.pdf"},
        )
    ]
    qdrant.collection_points["evidencebase_other"] = [
        types.SimpleNamespace(
            id="point-2",
            vector={
                "dense": [0.4, 0.5, 0.6],
                "keyword": StubSparseVector(indices=[2], values=[0.5]),
            },
            payload={"document_id": "doc-2", "collection_name": "other", "file_path": "b.pdf"},
        )
    ]
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    report = indexer.migrate_legacy_collections_to_shared_collection(dry_run=False)

    assert report["shared_collection_name"] == "evidence-base"
    assert report["legacy_collections_seen"] == 2
    assert report["legacy_collections_migrated"] == 2
    assert report["legacy_collections_deleted"] == 2
    assert report["legacy_points_migrated"] == 2
    assert qdrant.collection_names == {"evidence-base"}
    migrated_points = qdrant.collection_points["evidence-base"]
    assert len(migrated_points) == 2
    assert {point.payload["evidence_base_collection"] for point in migrated_points} == {
        "offsets",
        "other",
    }
    assert {point.payload["collection_name"] for point in migrated_points} == {"offsets", "other"}

    rerun_report = indexer.migrate_legacy_collections_to_shared_collection(dry_run=False)
    assert rerun_report["legacy_collections_seen"] == 0
    assert rerun_report["legacy_points_migrated"] == 0


def test_ingestion_service_purge_datastores_returns_deleted_counts() -> None:
    """Ensure service purge reports deleted Redis keys and Qdrant collections."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    redis_client.set("test:sample", "value")
    qdrant = FakeQdrantClient()
    qdrant.collection_names = {"evidencebase_research_raw", "external_collection"}
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )
    service = IngestionService(
        minio_client=types.SimpleNamespace(),
        repository=repository,
        partition_client=types.SimpleNamespace(),
        qdrant_indexer=indexer,
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    summary = service.purge_datastores()

    assert summary == {"redis_deleted_keys": 1, "qdrant_deleted_collections": 1}


def test_relocate_prefix_to_bucket_root_dry_run_reports_candidates_without_mutation() -> None:
    """Dry-run relocation should report candidates while leaving stores untouched."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    minio_client = FakeMinioClient(b"%PDF-1.7 fake")
    minio_client.put_object(
        "offsets",
        "articles/relocate-me.pdf",
        data=io.BytesIO(b"%PDF-1.7 fake"),
        length=len(b"%PDF-1.7 fake"),
        content_type="application/pdf",
    )
    minio_client.put_object(
        "offsets",
        "articles/existing-target.pdf",
        data=io.BytesIO(b"%PDF-1.7 fake"),
        length=len(b"%PDF-1.7 fake"),
        content_type="application/pdf",
    )
    minio_client.put_object(
        "offsets",
        "existing-target.pdf",
        data=io.BytesIO(b"%PDF-1.7 fake"),
        length=len(b"%PDF-1.7 fake"),
        content_type="application/pdf",
    )
    repository.add_document("offsets", "doc-relocate")
    repository.mark_object(
        bucket_name="offsets",
        object_name="articles/relocate-me.pdf",
        document_id="doc-relocate",
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name="offsets",
        object_name="articles/relocate-me.pdf",
        document_id="doc-relocate",
        metadata={"title": "Relocate me"},
    )
    repository.set_state(
        "doc-relocate",
        {
            "file_path": "articles/relocate-me.pdf",
            "meta_key": "offsets/articles/relocate-me.pdf",
        },
    )
    repository.add_document("offsets", "doc-conflict")
    repository.mark_object(
        bucket_name="offsets",
        object_name="articles/existing-target.pdf",
        document_id="doc-conflict",
        etag="etag-2",
    )
    repository.set_state(
        "doc-conflict",
        {
            "file_path": "articles/existing-target.pdf",
            "meta_key": "offsets/articles/existing-target.pdf",
        },
    )
    service = IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    summary = service.relocate_prefix_to_bucket_root(bucket_name="offsets", dry_run=True)

    assert summary["candidates_seen"] == 2
    assert summary["would_relocate"] == 1
    assert summary["skipped_existing_target"] == 1
    assert summary["relocated"] == 0
    assert repository.get_object_mapping("offsets", "articles/relocate-me.pdf")["document_id"] == (
        "doc-relocate"
    )
    assert repository.get_object_mapping("offsets", "relocate-me.pdf") == {}


def test_relocate_prefix_to_bucket_root_moves_one_document_without_reindexing() -> None:
    """Apply relocation should move MinIO/Redis state and patch Qdrant paths only."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    minio_client = FakeMinioClient(b"%PDF-1.7 fake")
    minio_client.put_object(
        "offsets",
        "articles/relocate-me.pdf",
        data=io.BytesIO(b"%PDF-1.7 fake"),
        length=len(b"%PDF-1.7 fake"),
        content_type="application/pdf",
    )
    repository.add_document("offsets", "doc-relocate")
    repository.mark_object(
        bucket_name="offsets",
        object_name="articles/relocate-me.pdf",
        document_id="doc-relocate",
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name="offsets",
        object_name="articles/relocate-me.pdf",
        document_id="doc-relocate",
        metadata={"title": "Relocate me", "year": "2025"},
    )
    partition_key = repository.set_partitions(
        "offsets",
        "doc-relocate",
        [{"text": "Offset relocation test.", "metadata": {"page_number": 1}}],
    )
    repository.set_state(
        "doc-relocate",
        {
            "file_path": "articles/relocate-me.pdf",
            "meta_key": "offsets/articles/relocate-me.pdf",
            "partition_key": partition_key,
            "sections_count": 2,
            "chunks_count": 3,
            "processing_state": "processed",
            "processing_stage": "processed",
            "processing_progress": 100,
            "processing_stage_progress": 100,
        },
    )
    qdrant_indexer = RecordingQdrantIndexer()
    service = IngestionService(
        minio_client=minio_client,
        repository=repository,
        partition_client=FakePartitionClient([]),
        qdrant_indexer=qdrant_indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    summary = service.relocate_prefix_to_bucket_root(bucket_name="offsets", dry_run=False)

    assert summary["relocated"] == 1
    assert summary["failed"] == 0
    assert repository.get_object_mapping("offsets", "articles/relocate-me.pdf") == {}
    assert repository.get_object_mapping("offsets", "relocate-me.pdf")["document_id"] == (
        "doc-relocate"
    )
    assert repository.get_metadata_by_key("offsets/relocate-me.pdf")["title"] == "Relocate me"
    state = repository.get_state("doc-relocate")
    assert state["file_path"] == "relocate-me.pdf"
    assert state["meta_key"] == "offsets/relocate-me.pdf"
    assert state["partition_key"] == partition_key
    assert state["sections_count"] == "2"
    assert state["chunks_count"] == "3"
    assert (
        "offsets",
        "relocate-me.pdf",
        "articles/relocate-me.pdf",
    ) in minio_client.copy_object_calls
    assert repository.get_object_mapping("offsets", "relocate-me.pdf")["etag"].startswith("etag-")
    assert qdrant_indexer.upsert_calls == []
    assert qdrant_indexer.rewrite_calls == [
        {
            "bucket_name": "offsets",
            "document_id": "doc-relocate",
            "old_object_name": "articles/relocate-me.pdf",
            "new_object_name": "relocate-me.pdf",
            "storage_bucket_name": None,
        }
    ]


def test_remove_document_can_remove_partitions() -> None:
    """Verify partition payloads are deleted when ``keep_partitions`` is ``False``."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    repository.add_document("research-raw", "doc-456")
    partition_key = repository.set_partitions(
        "research-raw",
        "doc-456",
        [{"text": "partition text"}],
    )

    repository.remove_document(
        bucket_name="research-raw",
        document_id="doc-456",
        keep_partitions=False,
    )

    assert repository.get_partitions_by_key(partition_key) == []


def test_qdrant_payload_contains_document_partition_meta_and_resolver_keys() -> None:
    """Validate Qdrant payload contains resolver, partition, and spatial metadata keys."""
    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    indexer.upsert_document_chunks(
        bucket_name="research-raw",
        document_id="abc123",
        file_path="paper.pdf",
        chunks=[
            {
                "chunk_index": 0,
                "text": "hello world",
                "metadata": {
                    "section_title": "Methods",
                    "parent_section_id": "section-123",
                    "parent_section_index": 4,
                    "parent_section_title": "Methods",
                    "parent_section_text": "Methods section full text.",
                },
                "page_numbers": [1],
                "bounding_boxes": [
                    {
                        "page_number": 1,
                        "points": [[10, 20], [30, 20], [30, 40], [10, 40]],
                    }
                ],
            }
        ],
        partition_key="partition-hash",
        meta_key="meta-hash",
        document_year="2024",
    )

    assert len(qdrant.upsert_calls) == 1
    upsert_call = qdrant.upsert_calls[0]
    assert upsert_call["collection_name"] == "evidence-base"
    assert upsert_call["wait"] is True

    point = upsert_call["points"][0]
    assert len(str(point.id)) == 36
    assert set(point.vector.keys()) == {"dense", "keyword"}
    assert point.vector["dense"] == [0.1, 0.2, 0.3]
    assert point.vector["keyword"] == StubSparseVector(indices=[1, 3], values=[0.8, 0.2])
    assert point.payload["document_id"] == "abc123"
    assert point.payload["year"] == "2024"
    assert "document_key" not in point.payload
    assert point.payload["partition_key"] == "partition-hash"
    assert "meta_key" not in point.payload
    assert "bucket_name" not in point.payload
    assert point.payload["evidence_base_collection"] == "research-raw"
    assert point.payload["collection_name"] == "research-raw"
    assert point.payload["file_path"] == "paper.pdf"
    assert point.payload["resolver_url"] == "docs://research-raw/paper.pdf?page=1"
    assert point.payload["minio_location"] == "research-raw/paper.pdf"
    assert point.payload["section_title"] == "Methods"
    assert point.payload["section_id"] == "section-123"
    assert "parent_section_id" not in point.payload
    assert "parent_section_index" not in point.payload
    assert "parent_section_title" not in point.payload
    assert "parent_section_text" not in point.payload
    assert point.payload["page_start"] == 1
    assert point.payload["page_end"] == 1
    assert "page_numbers" not in point.payload
    assert point.payload["bounding_boxes"] == [
        {
            "page_number": 1,
            "points": [[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]],
        }
    ]


def test_qdrant_upsert_skips_image_chunks_for_embedding() -> None:
    """Ensure image chunks are excluded from embedding/index payload writes."""
    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    indexer.upsert_document_chunks(
        bucket_name="research-raw",
        document_id="abc123",
        file_path="paper.pdf",
        chunks=[
            {
                "chunk_index": 0,
                "type": "text",
                "text": "indexed narrative text",
                "metadata": {"section_title": "Methods"},
            },
            {
                "chunk_index": 1,
                "type": "image",
                "text": "[Image]",
                "metadata": {
                    "section_title": "Methods",
                    "render_markdown": "![Figure](https://example.com/figure.png)",
                },
            },
        ],
        partition_key="partition-hash",
        meta_key="meta-hash",
        document_year="2024",
    )

    assert len(qdrant.upsert_calls) == 1
    upsert_call = qdrant.upsert_calls[0]
    assert len(upsert_call["points"]) == 1
    point = upsert_call["points"][0]
    assert point.payload["chunk_type"] == "text"
    assert point.payload["chunk_index"] == 0
    assert point.payload["text"] == "indexed narrative text"


def test_qdrant_upsert_batches_large_documents_into_fixed_size_writes() -> None:
    """Large document upserts should be split into deterministic Qdrant batches."""
    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    indexer.upsert_document_chunks(
        bucket_name="research-raw",
        document_id="abc123",
        file_path="paper.pdf",
        chunks=[
            {
                "chunk_index": chunk_index,
                "text": f"chunk text {chunk_index}",
                "metadata": {"section_title": "Methods"},
            }
            for chunk_index in range(260)
        ],
        partition_key="partition-hash",
        meta_key="meta-hash",
        document_year="2024",
    )

    assert [len(call["points"]) for call in qdrant.upsert_calls] == [128, 128, 4]
    assert all(call["wait"] is True for call in qdrant.upsert_calls)


def test_qdrant_indexer_hybrid_search_merges_dense_and_keyword_results() -> None:
    """Verify hybrid search merges dense and keyword ranks with RRF."""
    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    qdrant.collection_names = {"evidence-base"}
    qdrant.search_results["dense"] = [
        types.SimpleNamespace(
            id="chunk-a",
            score=0.91,
            payload={
                "evidence_base_collection": "research-raw",
                "document_id": "doc-a",
                "title": "Document A",
                "author": "Doe, J.",
                "year": "2023",
                "minio_location": "research-raw/a.pdf",
                "section_title": "Methods",
                "section_id": "section-a",
                "page_start": 2,
                "page_end": 3,
                "text": "dense\n\nmatch",
            },
        ),
        types.SimpleNamespace(
            id="chunk-other",
            score=0.9,
            payload={
                "evidence_base_collection": "other-bucket",
                "document_id": "doc-z",
                "title": "Document Z",
                "author": "Someone Else",
                "year": "2020",
                "minio_location": "other-bucket/z.pdf",
                "section_title": "Other",
                "section_id": "section-z",
                "page_start": 1,
                "page_end": 1,
                "text": "other bucket match",
            },
        ),
    ]
    qdrant.search_results["keyword"] = [
        types.SimpleNamespace(
            id="chunk-b",
            score=0.88,
            payload={
                "evidence_base_collection": "research-raw",
                "document_id": "doc-b",
                "title": "Document B",
                "author": "Smith, R.",
                "year": "2021",
                "minio_location": "research-raw/b.pdf",
                "section_title": "Findings",
                "section_id": "section-b",
                "page_start": 5,
                "page_end": 5,
                "text": "keyword\nmatch",
            },
        ),
        types.SimpleNamespace(
            id="chunk-a",
            score=0.7,
            payload={
                "evidence_base_collection": "research-raw",
                "document_id": "doc-a",
                "title": "Document A",
                "author": "Doe, J.",
                "year": "2023",
                "minio_location": "research-raw/a.pdf",
                "section_title": "Methods",
                "section_id": "section-a",
                "page_start": 2,
                "page_end": 3,
                "text": "dense\n\nmatch",
            },
        ),
    ]
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    results = indexer.search_chunks(
        bucket_name="research-raw",
        query="causal inference",
        limit=2,
        mode="hybrid",
        rrf_k=60,
    )

    assert [item["id"] for item in results] == ["chunk-a", "chunk-b"]
    assert results[0]["document_id"] == "doc-a"
    assert results[1]["document_id"] == "doc-b"
    assert results[0]["title"] == "Document A"
    assert results[1]["title"] == "Document B"
    assert results[0]["author"] == "Doe, J."
    assert results[1]["author"] == "Smith, R."
    assert results[0]["year"] == "2023"
    assert results[1]["year"] == "2021"
    assert results[0]["file_path"] == "a.pdf"
    assert results[1]["file_path"] == "b.pdf"
    assert results[0]["section_title"] == "Methods"
    assert results[1]["section_title"] == "Findings"
    assert results[0]["section_id"] == "section-a"
    assert results[1]["section_id"] == "section-b"
    assert results[0]["parent_section_id"] == "section-a"
    assert results[1]["parent_section_id"] == "section-b"
    assert results[0]["parent_section_index"] is None
    assert results[1]["parent_section_index"] is None
    assert results[0]["parent_section_title"] == ""
    assert results[1]["parent_section_title"] == ""
    assert results[0]["parent_section_text"] == ""
    assert results[1]["parent_section_text"] == ""
    assert results[0]["text"] == "dense match"
    assert results[1]["text"] == "keyword match"
    assert results[0]["page_start"] == 2
    assert results[0]["page_end"] == 3
    assert results[1]["page_start"] == 5
    assert results[1]["page_end"] == 5
    assert results[0]["qdrant_payload"]["text"] == "dense\n\nmatch"
    assert results[1]["qdrant_payload"]["text"] == "keyword\nmatch"
    assert results[0]["qdrant_payload"]["document_id"] == "doc-a"
    assert results[1]["qdrant_payload"]["document_id"] == "doc-b"
    assert (
        results[0]["source_material_url"]
        == "/api/collections/research-raw/documents/resolve?file_path=a.pdf"
    )
    assert (
        results[0]["resolver_link_url"]
        == "/resolver.html?bucket=research-raw&file_path=a.pdf&page=2"
    )
    assert (
        results[1]["source_material_url"]
        == "/api/collections/research-raw/documents/resolve?file_path=b.pdf"
    )
    assert (
        results[1]["resolver_link_url"]
        == "/resolver.html?bucket=research-raw&file_path=b.pdf&page=5"
    )
    assert [call["vector_name"] for call in qdrant.search_calls] == ["dense", "keyword"]
    for call in qdrant.search_calls:
        must_conditions = getattr(call["query_filter"], "must", None)
        assert isinstance(must_conditions, list)
        assert any(
            getattr(condition, "key", "") == "evidence_base_collection"
            and getattr(getattr(condition, "match", None), "value", "") == "research-raw"
            for condition in must_conditions
        )


def test_qdrant_variant_search_embeds_queries_once_per_embedder() -> None:
    """Variant search should batch dense and sparse embedding work per request."""

    class CountingDenseEmbedder:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def embed(self, texts: list[str]) -> list[list[float]]:
            self.calls.append(list(texts))
            return [[0.1, 0.2, 0.3] for _ in texts]

    class CountingSparseEmbedder:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def embed(self, texts: list[str]) -> list[FakeSparseEmbedding]:
            self.calls.append(list(texts))
            return [FakeSparseEmbedding(indices=[1, 3], values=[0.8, 0.2]) for _ in texts]

    class CountingQdrantIndexer(QdrantIndexer):
        def __init__(self, *, dense_embedder: Any, sparse_embedder: Any, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self._dense_embedder = dense_embedder
            self._sparse_embedder = sparse_embedder

        def _get_embedder(self) -> Any:
            return self._dense_embedder

        def _get_keyword_embedder(self) -> Any:
            return self._sparse_embedder

    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    qdrant.collection_names = {"evidence-base"}
    qdrant.search_results["dense"] = [
        types.SimpleNamespace(
            id="chunk-a",
            score=0.9,
            payload={
                "evidence_base_collection": "research-raw",
                "document_id": "doc-a",
                "minio_location": "research-raw/a.pdf",
                "text": "dense match",
            },
        )
    ]
    qdrant.search_results["keyword"] = [
        types.SimpleNamespace(
            id="chunk-a",
            score=0.8,
            payload={
                "evidence_base_collection": "research-raw",
                "document_id": "doc-a",
                "minio_location": "research-raw/a.pdf",
                "text": "keyword match",
            },
        )
    ]
    dense_embedder = CountingDenseEmbedder()
    sparse_embedder = CountingSparseEmbedder()
    indexer = CountingQdrantIndexer(
        qdrant_client=qdrant,
        dense_embedder=dense_embedder,
        sparse_embedder=sparse_embedder,
        fastembed_model="ignored",
        fastembed_keyword_model="ignored-keyword",
        collection_prefix="evidencebase",
    )

    results = indexer.search_chunk_variants(
        bucket_name="research-raw",
        queries=["causal inference", "industrial participation"],
        limit=5,
        mode="hybrid",
        rrf_k=60,
    )

    assert dense_embedder.calls == [["causal inference", "industrial participation"]]
    assert sparse_embedder.calls == [["causal inference", "industrial participation"]]
    assert set(results) == {"causal inference", "industrial participation"}
    assert len(qdrant.search_calls) == 4


def test_ingestion_service_search_documents_delegates_to_qdrant_indexer() -> None:
    """Ensure service search delegates to Qdrant indexer with unchanged parameters."""

    class SearchRecordingIndexer(RecordingQdrantIndexer):
        def search_chunks(
            self,
            *,
            bucket_name: str,
            query: str,
            limit: int,
            mode: str,
            rrf_k: int = 60,
        ) -> list[dict[str, Any]]:
            super().search_chunks(
                bucket_name=bucket_name,
                query=query,
                limit=limit,
                mode=mode,
                rrf_k=rrf_k,
            )
            return [{"id": "chunk-1", "document_id": "doc-1"}]

    indexer = SearchRecordingIndexer()
    repository = RedisDocumentRepository(FakeRedis(), key_prefix="test")
    repository.add_document("research-raw", "doc-1")
    repository.mark_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id="doc-1",
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id="doc-1",
        metadata={
            "title": "Paper Title",
            "author": "Doe, J.",
            "year": "2024",
            "citation_key": "doe2024paper",
        },
    )
    service = IngestionService(
        minio_client=types.SimpleNamespace(),
        repository=repository,
        partition_client=types.SimpleNamespace(),
        qdrant_indexer=indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    results = service.search_documents(
        bucket_name="research-raw",
        query="causal effects",
        limit=7,
        mode="hybrid",
        rrf_k=80,
    )

    assert results == [
        {
            "id": "chunk-1",
            "document_id": "doc-1",
            "title": "Paper Title",
            "author": "Doe, J.",
            "year": "2024",
            "citation_key": "doe2024paper",
        }
    ]
    assert indexer.search_calls == [
        {
            "bucket_name": "research-raw",
            "query": "causal effects",
            "limit": 7,
            "mode": "hybrid",
            "rrf_k": 80,
        }
    ]


def test_ingestion_service_search_documents_hydrates_parent_section_from_redis() -> None:
    """Ensure search results are enriched from Redis metadata and section mappings."""

    class SearchRecordingIndexer(RecordingQdrantIndexer):
        def search_chunks(
            self,
            *,
            bucket_name: str,
            query: str,
            limit: int,
            mode: str,
            rrf_k: int = 60,
        ) -> list[dict[str, Any]]:
            super().search_chunks(
                bucket_name=bucket_name,
                query=query,
                limit=limit,
                mode=mode,
                rrf_k=rrf_k,
            )
            return [
                {
                    "id": "chunk-1",
                    "document_id": "doc-1",
                    "section_id": "section-42",
                    "section_title": "",
                    "parent_section_id": "section-42",
                    "parent_section_title": "",
                    "parent_section_index": None,
                    "parent_section_text": "",
                }
            ]

    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    repository.add_document("research-raw", "doc-1")
    repository.mark_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id="doc-1",
        etag="etag-1",
    )
    repository.set_metadata_for_location(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id="doc-1",
        metadata={
            "title": "Paper Title",
            "author": "Doe, J.",
            "year": "2024",
            "citation_key": "doe2024paper",
        },
    )
    repository.set_document_sections(
        document_id="doc-1",
        partition_key="partition-1",
        sections=[
            {
                "section_id": "section-42",
                "section_index": 5,
                "section_title": "Methods",
                "section_text": "Full section text from Redis mapping.",
            }
        ],
        chunk_sections=[
            {"chunk_index": 0, "chunk_id": "chunk-1", "section_id": "section-42"},
        ],
    )
    indexer = SearchRecordingIndexer()
    service = IngestionService(
        minio_client=types.SimpleNamespace(),
        repository=repository,
        partition_client=types.SimpleNamespace(),
        qdrant_indexer=indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    results = service.search_documents(
        bucket_name="research-raw",
        query="causal effects",
        limit=5,
        mode="semantic",
        rrf_k=60,
    )

    assert results[0]["section_id"] == "section-42"
    assert results[0]["parent_section_id"] == "section-42"
    assert results[0]["parent_section_index"] == 5
    assert results[0]["parent_section_title"] == "Methods"
    assert results[0]["parent_section_text"] == "Full section text from Redis mapping."
    assert results[0]["section_title"] == "Methods"
    assert results[0]["title"] == "Paper Title"
    assert results[0]["author"] == "Doe, J."
    assert results[0]["year"] == "2024"
    assert results[0]["citation_key"] == "doe2024paper"


def test_perf_stats_record_search_documents_invocations() -> None:
    """Hot-path search instrumentation should record invocation counts."""

    class SearchRecordingIndexer(RecordingQdrantIndexer):
        def search_chunks(
            self,
            *,
            bucket_name: str,
            query: str,
            limit: int,
            mode: str,
            rrf_k: int = 60,
        ) -> list[dict[str, Any]]:
            super().search_chunks(
                bucket_name=bucket_name,
                query=query,
                limit=limit,
                mode=mode,
                rrf_k=rrf_k,
            )
            return []

    reset_perf_stats()
    repository = RedisDocumentRepository(FakeRedis(), key_prefix="test")
    indexer = SearchRecordingIndexer()
    service = IngestionService(
        minio_client=types.SimpleNamespace(),
        repository=repository,
        partition_client=types.SimpleNamespace(),
        qdrant_indexer=indexer,  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    service.search_documents(
        bucket_name="research-raw",
        query="causal effects",
        limit=3,
        mode="semantic",
        rrf_k=60,
    )

    stats = perf_snapshot()
    assert stats["search_documents"]["count"] >= 1


def test_ingestion_service_rebuild_document_section_mapping_from_partitions() -> None:
    """Ensure section mappings can be rebuilt independently from stored partitions."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")
    repository.add_document("research-raw", "doc-1")
    repository.mark_object(
        bucket_name="research-raw",
        object_name="paper.pdf",
        document_id="doc-1",
        etag="etag-1",
    )
    partition_key = repository.set_partitions(
        "research-raw",
        "doc-1",
        [
            {
                "type": "Title",
                "text": "Methods",
                "metadata": {"page_number": 1, "filename": "paper.pdf"},
            },
            {
                "type": "NarrativeText",
                "text": "Methods section full body text.",
                "metadata": {"page_number": 1, "filename": "paper.pdf"},
            },
        ],
    )

    service = IngestionService(
        minio_client=types.SimpleNamespace(),
        repository=repository,
        partition_client=types.SimpleNamespace(),
        qdrant_indexer=RecordingQdrantIndexer(),  # type: ignore[arg-type]
        chunk_size_chars=1200,
        chunk_overlap_chars=150,
    )

    result = service.rebuild_document_section_mapping(
        bucket_name="research-raw",
        document_id="doc-1",
    )

    assert result["partition_key"] == partition_key
    assert result["sections_count"] >= 1
    sections = repository.get_document_sections("doc-1")
    assert sections
    assert sections[0]["section_id"]
    assert sections[0]["section_text"]
