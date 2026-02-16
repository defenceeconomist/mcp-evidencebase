from __future__ import annotations

import sys
import types
from collections import defaultdict
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any

import pytest

from mcp_evidencebase.ingestion import (
    IngestionService,
    QdrantIndexer,
    RedisDocumentRepository,
    build_partition_chunks,
    chunk_partition_texts,
    compute_chunk_point_id,
    compute_document_id,
    extract_metadata_from_partitions,
    extract_partition_bounding_box,
    extract_pdf_title_author,
)

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
        self.upsert_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []

    def get_collections(self) -> FakeCollectionsResponse:
        return FakeCollectionsResponse(
            collections=[FakeCollection(name=name) for name in sorted(self.collection_names)]
        )

    def create_collection(self, collection_name: str, vectors_config: Any) -> None:
        del vectors_config
        self.collection_names.add(collection_name)

    def delete_collection(self, collection_name: str) -> None:
        self.collection_names.discard(collection_name)

    def delete(self, *, collection_name: str, points_selector: Any) -> None:
        self.delete_calls.append(
            {
                "collection_name": collection_name,
                "points_selector": points_selector,
            }
        )

    def upsert(self, *, collection_name: str, points: list[Any], wait: bool) -> None:
        self.upsert_calls.append(
            {
                "collection_name": collection_name,
                "points": points,
                "wait": wait,
            }
        )


class FakeEmbedder:
    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[0.1, 0.2, 0.3] for _ in texts]


class StubQdrantIndexer(QdrantIndexer):
    def _get_embedder(self) -> Any:
        return FakeEmbedder()


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
    vector: list[float]
    payload: dict[str, Any]


def install_qdrant_client_stub() -> None:
    models = types.SimpleNamespace(
        Distance=types.SimpleNamespace(COSINE="cosine"),
        VectorParams=StubVectorParams,
        MatchValue=StubMatchValue,
        FieldCondition=StubFieldCondition,
        Filter=StubFilter,
        FilterSelector=StubFilterSelector,
        PointStruct=StubPointStruct,
    )
    sys.modules["qdrant_client"] = types.SimpleNamespace(models=models)


def test_compute_document_id_is_deterministic() -> None:
    """Verify document IDs are deterministic SHA-256 hashes of the same bytes."""
    payload = b"sample-document-bytes"
    first = compute_document_id(payload)
    second = compute_document_id(payload)

    assert first == second
    assert len(first) == 64


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


def test_chunk_partition_texts_returns_overlapping_chunks() -> None:
    """Check chunking applies overlap so adjacent chunks share boundary context."""
    partitions = [{"text": "a" * 60}, {"text": "b" * 60}, {"text": "c" * 60}]

    chunks = chunk_partition_texts(
        partitions,
        chunk_size_chars=100,
        chunk_overlap_chars=20,
    )

    assert len(chunks) >= 2
    assert chunks[0]
    assert chunks[1]
    assert chunks[0][-20:] in chunks[1]


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
    """Verify DOI/ISBN are extracted from first-page text only."""
    partitions = [
        {
            "text": (
                "Different Page Title\nDifferent Author\nDOI: 10.1000/xyz123\n"
                "ISBN 978-0-393-04002-9"
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


def test_set_partitions_stores_payload_under_partition_hash_key() -> None:
    """Ensure partition payloads use direct hash keys and not reverse-index keys."""
    redis_client = FakeRedis()
    repository = RedisDocumentRepository(redis_client, key_prefix="test")

    partition_key = repository.set_partitions(
        "research-raw",
        "doc-789",
        [{"text": "partition text"}],
    )

    assert repository.get_partitions_by_key(partition_key) == [{"text": "partition text"}]
    assert redis_client.get(f"test:partition:{partition_key}") is not None
    assert redis_client.get(f"test:document:partition:{partition_key}") is None


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
        "evidencebase_research_raw",
        "evidencebase_other",
        "unrelated_collection",
    }
    indexer = StubQdrantIndexer(
        qdrant_client=qdrant,
        fastembed_model="ignored",
        collection_prefix="evidencebase",
    )

    deleted = indexer.purge_prefixed_collections()

    assert deleted == 2
    assert "unrelated_collection" in qdrant.collection_names
    assert "evidencebase_research_raw" not in qdrant.collection_names
    assert "evidencebase_other" not in qdrant.collection_names


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
    )

    assert len(qdrant.upsert_calls) == 1
    upsert_call = qdrant.upsert_calls[0]
    assert upsert_call["collection_name"] == "evidencebase_research_raw"
    assert upsert_call["wait"] is True

    point = upsert_call["points"][0]
    assert len(str(point.id)) == 36
    assert point.payload["document_id"] == "abc123"
    assert "document_key" not in point.payload
    assert point.payload["partition_key"] == "partition-hash"
    assert point.payload["meta_key"] == "meta-hash"
    assert point.payload["resolver_url"] == "docs://research-raw/paper.pdf?page="
    assert point.payload["minio_location"] == "research-raw/paper.pdf"
    assert point.payload["page_numbers"] == [1]
    assert point.payload["bounding_boxes"] == [
        {
            "page_number": 1,
            "points": [[10.0, 20.0], [30.0, 20.0], [30.0, 40.0], [10.0, 40.0]],
        }
    ]
