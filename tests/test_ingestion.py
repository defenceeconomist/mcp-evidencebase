from __future__ import annotations

import sys
import types
import json
from collections import defaultdict
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any, Mapping

import pytest

import mcp_evidencebase.ingestion as ingestion_module
from mcp_evidencebase.ingestion import (
    IngestionService,
    QdrantIndexer,
    RedisDocumentRepository,
    UnstructuredPartitionClient,
    build_ingestion_settings,
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
        self.search_calls: list[dict[str, Any]] = []
        self.search_results: dict[str, list[Any]] = {"dense": [], "keyword": []}

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

    def search(
        self,
        *,
        collection_name: str,
        query_vector: Any,
        limit: int,
        with_payload: bool,
        with_vectors: bool,
    ) -> list[Any]:
        del collection_name, with_payload, with_vectors
        vector_name = str(query_vector[0]) if isinstance(query_vector, tuple) else "dense"
        self.search_calls.append({"vector_name": vector_name, "limit": limit})
        return list(self.search_results.get(vector_name, []))[:limit]


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
        self.get_object_calls: list[tuple[str, str]] = []
        self.stat_object_calls: list[tuple[str, str]] = []

    def get_object(self, bucket_name: str, object_name: str) -> FakeMinioObjectResponse:
        self.get_object_calls.append((bucket_name, object_name))
        return FakeMinioObjectResponse(self._payload)

    def stat_object(self, bucket_name: str, object_name: str) -> Any:
        self.stat_object_calls.append((bucket_name, object_name))
        return types.SimpleNamespace(etag=self._etag)


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
        self.search_calls: list[dict[str, Any]] = []

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
    sys.modules["qdrant_client"] = types.SimpleNamespace(models=models)


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

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
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
    assert upsert_call["collection_name"] == "evidencebase_research_raw"
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
    assert "file_path" not in point.payload
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


def test_qdrant_indexer_hybrid_search_merges_dense_and_keyword_results() -> None:
    """Verify hybrid search merges dense and keyword ranks with RRF."""
    install_qdrant_client_stub()
    qdrant = FakeQdrantClient()
    qdrant.collection_names = {"evidencebase_research_raw"}
    qdrant.search_results["dense"] = [
        types.SimpleNamespace(
            id="chunk-a",
            score=0.91,
            payload={
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
        )
    ]
    qdrant.search_results["keyword"] = [
        types.SimpleNamespace(
            id="chunk-b",
            score=0.88,
            payload={
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
