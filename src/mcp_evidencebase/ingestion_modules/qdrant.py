"""Qdrant indexing and hybrid retrieval logic."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

from mcp_evidencebase.ingestion_modules.chunking import (
    _coerce_chunk_bounding_boxes,
    _coerce_chunk_page_numbers,
    _safe_int,
)
from mcp_evidencebase.ingestion_modules.metadata import (
    SEARCH_MODES,
    _normalize_coordinate_points,
    build_resolver_url,
    compute_chunk_point_id,
)


class QdrantIndexer:
    """Qdrant upsert/search/delete operations with dense and keyword vectors."""

    def __init__(
        self,
        *,
        qdrant_client: Any,
        fastembed_model: str,
        fastembed_keyword_model: str,
        collection_prefix: str,
    ) -> None:
        """Initialize indexer with a Qdrant client and embedding model name."""
        self._qdrant_client = qdrant_client
        self._fastembed_model = fastembed_model
        self._fastembed_keyword_model = fastembed_keyword_model
        self._collection_prefix = collection_prefix
        self._embedder: Any | None = None
        self._keyword_embedder: Any | None = None
        self._dense_vector_name = "dense"
        self._keyword_vector_name = "keyword"

    def _collection_name(self, bucket_name: str) -> str:
        normalized_bucket = re.sub(r"[^a-zA-Z0-9_]+", "_", bucket_name).strip("_").lower()
        if not normalized_bucket:
            normalized_bucket = "default"
        return f"{self._collection_prefix}_{normalized_bucket}"

    def _get_embedder(self) -> Any:
        if self._embedder is None:
            from fastembed import TextEmbedding

            self._embedder = TextEmbedding(model_name=self._fastembed_model)
        return self._embedder

    def _get_keyword_embedder(self) -> Any:
        if self._keyword_embedder is None:
            from fastembed import SparseTextEmbedding

            self._keyword_embedder = SparseTextEmbedding(model_name=self._fastembed_keyword_model)
        return self._keyword_embedder

    @staticmethod
    def _coerce_sparse_embedding(embedding: Any) -> tuple[list[int], list[float]]:
        """Normalize sparse embedding payload into aligned index/value lists."""
        raw_indices: Any = None
        raw_values: Any = None

        if isinstance(embedding, Mapping):
            raw_indices = embedding.get("indices")
            raw_values = embedding.get("values")
        else:
            raw_indices = getattr(embedding, "indices", None)
            raw_values = getattr(embedding, "values", None)

        if raw_indices is None or raw_values is None:
            return [], []

        try:
            indices = [int(value) for value in raw_indices]
            values = [float(value) for value in raw_values]
        except (TypeError, ValueError):
            return [], []

        size = min(len(indices), len(values))
        if size <= 0:
            return [], []
        return indices[:size], values[:size]

    def _build_sparse_vector(self, *, indices: list[int], values: list[float]) -> Any:
        """Create a Qdrant sparse vector payload, with compatibility fallback."""
        from qdrant_client import models as qdrant_models

        sparse_vector_cls = getattr(qdrant_models, "SparseVector", None)
        if sparse_vector_cls is None:
            return {"indices": indices, "values": values}
        return sparse_vector_cls(indices=indices, values=values)

    def _ensure_collection(self, collection_name: str, vector_size: int) -> None:
        from qdrant_client import models as qdrant_models

        collection_names = {
            str(collection.name) for collection in self._qdrant_client.get_collections().collections
        }
        if collection_name in collection_names:
            return

        dense_vectors_config = {
            self._dense_vector_name: qdrant_models.VectorParams(
                size=vector_size,
                distance=qdrant_models.Distance.COSINE,
            )
        }

        sparse_vectors_config: dict[str, Any] | None = None
        sparse_vector_params_cls = getattr(qdrant_models, "SparseVectorParams", None)
        if sparse_vector_params_cls is not None:
            sparse_vectors_config = {
                self._keyword_vector_name: sparse_vector_params_cls(),
            }

        try:
            if sparse_vectors_config is None:
                self._qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=dense_vectors_config,
                )
            else:
                self._qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=dense_vectors_config,
                    sparse_vectors_config=sparse_vectors_config,
                )
        except TypeError:
            # Older qdrant-client versions do not support sparse config kwargs.
            self._qdrant_client.create_collection(
                collection_name=collection_name,
                vectors_config=dense_vectors_config,
            )

    def _collection_exists(self, collection_name: str) -> bool:
        collection_names = {
            str(collection.name) for collection in self._qdrant_client.get_collections().collections
        }
        return collection_name in collection_names

    def ensure_bucket_collection(self, bucket_name: str) -> bool:
        """Ensure a Qdrant collection exists for a bucket."""
        collection_name = self._collection_name(bucket_name)
        if self._collection_exists(collection_name):
            return False

        embedder = self._get_embedder()
        embeddings = list(embedder.embed(["collection-dimension-probe"]))
        if not embeddings:
            raise RuntimeError("Could not determine FastEmbed vector dimension.")
        vector_size = len(embeddings[0])
        self._ensure_collection(collection_name, vector_size)
        return True

    def delete_bucket_collection(self, bucket_name: str) -> bool:
        """Delete the Qdrant collection for a bucket when it exists."""
        collection_name = self._collection_name(bucket_name)
        if not self._collection_exists(collection_name):
            return False
        self._qdrant_client.delete_collection(collection_name=collection_name)
        return True

    def purge_prefixed_collections(self) -> int:
        """Delete every collection that belongs to this application's prefix."""
        prefix = f"{self._collection_prefix}_"
        deleted = 0
        for collection in self._qdrant_client.get_collections().collections:
            collection_name = str(getattr(collection, "name", "")).strip()
            if not collection_name.startswith(prefix):
                continue
            self._qdrant_client.delete_collection(collection_name=collection_name)
            deleted += 1
        return deleted

    def delete_document(self, bucket_name: str, document_id: str) -> None:
        """Remove all chunk vectors for one document from Qdrant."""
        from qdrant_client import models as qdrant_models

        collection_name = self._collection_name(bucket_name)
        if not self._collection_exists(collection_name):
            return

        self._qdrant_client.delete(
            collection_name=collection_name,
            points_selector=qdrant_models.FilterSelector(
                filter=qdrant_models.Filter(
                    must=[
                        qdrant_models.FieldCondition(
                            key="document_id",
                            match=qdrant_models.MatchValue(value=document_id),
                        )
                    ]
                )
            ),
        )

    @staticmethod
    def _extract_query_points(raw_response: Any) -> list[Any]:
        """Normalize query/search responses to a list of scored points."""
        if isinstance(raw_response, list):
            return list(raw_response)
        points = getattr(raw_response, "points", None)
        if isinstance(points, list):
            return points
        result = getattr(raw_response, "result", None)
        if isinstance(result, list):
            return result
        return []

    def _search_dense(
        self,
        *,
        collection_name: str,
        query_vector: list[float],
        limit: int,
    ) -> list[Any]:
        """Run a dense-vector search query against Qdrant."""
        if hasattr(self._qdrant_client, "query_points"):
            try:
                response = self._qdrant_client.query_points(
                    collection_name=collection_name,
                    query=query_vector,
                    using=self._dense_vector_name,
                    limit=limit,
                    with_payload=True,
                    with_vectors=False,
                )
                return self._extract_query_points(response)
            except TypeError:
                pass

        if hasattr(self._qdrant_client, "search"):
            response = self._qdrant_client.search(
                collection_name=collection_name,
                query_vector=(self._dense_vector_name, query_vector),
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            return self._extract_query_points(response)

        raise RuntimeError("The configured Qdrant client does not support search operations.")

    def _search_keyword(
        self,
        *,
        collection_name: str,
        sparse_query: Any,
        limit: int,
    ) -> list[Any]:
        """Run a sparse keyword-vector search query against Qdrant."""
        if hasattr(self._qdrant_client, "query_points"):
            try:
                response = self._qdrant_client.query_points(
                    collection_name=collection_name,
                    query=sparse_query,
                    using=self._keyword_vector_name,
                    limit=limit,
                    with_payload=True,
                    with_vectors=False,
                )
                return self._extract_query_points(response)
            except TypeError:
                pass

        if hasattr(self._qdrant_client, "search"):
            response = self._qdrant_client.search(
                collection_name=collection_name,
                query_vector=(self._keyword_vector_name, sparse_query),
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            return self._extract_query_points(response)

        raise RuntimeError("The configured Qdrant client does not support search operations.")

    @staticmethod
    def _normalize_point_id(point: Any, fallback_index: int) -> str:
        point_id = getattr(point, "id", None)
        if point_id is None:
            return f"point-{fallback_index}"
        return str(point_id)

    @staticmethod
    def _normalize_payload(value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            return {}
        return {str(key): payload_value for key, payload_value in value.items()}

    @staticmethod
    def _normalize_search_result_text(value: Any) -> str:
        """Normalize chunk text returned by search into a single display line."""
        raw = str(value or "")
        # Collapse paragraph/line breaks and repeated whitespace in API search text.
        return re.sub(r"\s+", " ", raw).strip()

    @staticmethod
    def _extract_file_path_from_minio_location(value: Any) -> str:
        """Extract object path from ``bucket/object`` style minio location."""
        location = str(value or "").strip().lstrip("/")
        if not location or "/" not in location:
            return ""
        _, object_path = location.split("/", 1)
        return object_path.strip()

    @staticmethod
    def _extract_bucket_from_minio_location(value: Any) -> str:
        """Extract bucket name from ``bucket/object`` style minio location."""
        location = str(value or "").strip().lstrip("/")
        if not location or "/" not in location:
            return ""
        bucket_name, _ = location.split("/", 1)
        return bucket_name.strip()

    @staticmethod
    def _extract_bucket_and_path_from_resolver_url(value: Any) -> tuple[str, str]:
        """Extract bucket/object path from ``docs://bucket/object?page=N`` resolver URLs."""
        resolver = str(value or "").strip()
        prefix = "docs://"
        if not resolver.startswith(prefix):
            return "", ""
        path_with_query = resolver[len(prefix) :].lstrip("/")
        path = path_with_query.split("?", 1)[0].strip()
        if not path or "/" not in path:
            return "", ""
        bucket_name, object_path = path.split("/", 1)
        return bucket_name.strip(), object_path.strip()

    @staticmethod
    def _build_source_material_url(bucket_name: str, file_path: str) -> str:
        """Build API URL that serves the underlying source document bytes."""
        normalized_bucket = bucket_name.strip()
        normalized_path = file_path.strip().lstrip("/")
        if not normalized_bucket or not normalized_path:
            return ""
        encoded_bucket = quote(normalized_bucket, safe="")
        encoded_file_path = quote(normalized_path, safe="")
        return f"/api/collections/{encoded_bucket}/documents/resolve?file_path={encoded_file_path}"

    @staticmethod
    def _build_resolver_link_url(bucket_name: str, file_path: str, page_start: Any) -> str:
        """Build resolver page URL for source deep links."""
        normalized_bucket = bucket_name.strip()
        normalized_path = file_path.strip().lstrip("/")
        if not normalized_bucket or not normalized_path:
            return ""
        encoded_bucket = quote(normalized_bucket, safe="")
        encoded_file_path = quote(normalized_path, safe="")
        page_value = _safe_int(page_start)
        if page_value is None:
            return f"/resolver.html?bucket={encoded_bucket}&file_path={encoded_file_path}"
        return (
            f"/resolver.html?bucket={encoded_bucket}&file_path={encoded_file_path}"
            f"&page={int(page_value)}"
        )

    def _format_result_payload(
        self,
        *,
        point_id: str,
        payload: Mapping[str, Any],
        raw_score: Any,
    ) -> dict[str, Any]:
        try:
            score = float(raw_score)
        except (TypeError, ValueError):
            score = 0.0
        minio_location = str(payload.get("minio_location", ""))
        bucket_name = self._extract_bucket_from_minio_location(minio_location)
        file_path = str(payload.get("file_path", "")).strip()
        if not file_path:
            file_path = self._extract_file_path_from_minio_location(minio_location)
        resolver_url = str(payload.get("resolver_url", ""))
        if not bucket_name or not file_path:
            resolver_bucket_name, resolver_file_path = (
                self._extract_bucket_and_path_from_resolver_url(resolver_url)
            )
            if not bucket_name:
                bucket_name = resolver_bucket_name
            if not file_path:
                file_path = resolver_file_path
        source_material_url = self._build_source_material_url(bucket_name, file_path)
        resolver_link_url = self._build_resolver_link_url(
            bucket_name,
            file_path,
            payload.get("page_start"),
        )
        section_id = str(payload.get("section_id", "") or payload.get("parent_section_id", ""))
        section_title = str(payload.get("section_title", "") or "")
        return {
            "id": point_id,
            "score": score,
            "document_id": str(payload.get("document_id", "")),
            "title": str(payload.get("title", "")),
            "author": str(payload.get("author", "")),
            "year": str(payload.get("year", "")),
            "citation_key": str(payload.get("citation_key", "")),
            "file_path": file_path,
            "chunk_index": int(payload.get("chunk_index", 0) or 0),
            "section_id": section_id,
            "section_title": section_title,
            "parent_section_id": section_id,
            "parent_section_index": None,
            "parent_section_title": "",
            "parent_section_text": "",
            "text": self._normalize_search_result_text(payload.get("text", "")),
            "page_start": _safe_int(payload.get("page_start")),
            "page_end": _safe_int(payload.get("page_end")),
            "resolver_url": resolver_url,
            "resolver_link_url": resolver_link_url,
            "source_material_url": source_material_url,
            "minio_location": minio_location,
            "qdrant_payload": dict(payload),
        }

    def _format_result_point(self, point: Any, *, fallback_rank: int) -> dict[str, Any]:
        return self._format_result_payload(
            point_id=self._normalize_point_id(point, fallback_rank),
            payload=self._normalize_payload(getattr(point, "payload", {})),
            raw_score=getattr(point, "score", 0.0),
        )

    def _rrf(
        self,
        *,
        semantic_points: list[Any],
        keyword_points: list[Any],
        rrf_k: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Merge two ranked result sets using reciprocal rank fusion (RRF)."""
        rank_constant = float(max(1, int(rrf_k)))
        merged: dict[str, dict[str, Any]] = {}

        for rank, point in enumerate(semantic_points):
            point_id = self._normalize_point_id(point, rank)
            payload = self._normalize_payload(getattr(point, "payload", {}))
            entry = merged.setdefault(
                point_id,
                {
                    "point": point,
                    "payload": payload,
                    "score": 0.0,
                },
            )
            entry["score"] += 1.0 / (rank_constant + rank + 1.0)

        for rank, point in enumerate(keyword_points):
            point_id = self._normalize_point_id(point, rank)
            payload = self._normalize_payload(getattr(point, "payload", {}))
            entry = merged.setdefault(
                point_id,
                {
                    "point": point,
                    "payload": payload,
                    "score": 0.0,
                },
            )
            entry["score"] += 1.0 / (rank_constant + rank + 1.0)

        ranked_entries = sorted(
            merged.values(),
            key=lambda item: float(item["score"]),
            reverse=True,
        )
        results: list[dict[str, Any]] = []
        for index, entry in enumerate(ranked_entries[:limit]):
            point = entry["point"]
            results.append(
                self._format_result_payload(
                    point_id=self._normalize_point_id(point, index),
                    payload=self._normalize_payload(entry["payload"]),
                    raw_score=float(entry["score"]),
                )
            )
        return results

    def search_chunks(
        self,
        *,
        bucket_name: str,
        query: str,
        limit: int,
        mode: str,
        rrf_k: int = 60,
    ) -> list[dict[str, Any]]:
        """Search chunk payloads in Qdrant using semantic, keyword, or hybrid mode."""
        normalized_query = query.strip()
        if not normalized_query:
            raise ValueError("query must not be empty.")

        normalized_mode = mode.strip().lower()
        if normalized_mode not in SEARCH_MODES:
            raise ValueError(f"mode must be one of: {', '.join(SEARCH_MODES)}")

        resolved_limit = max(1, min(int(limit), 100))
        collection_name = self._collection_name(bucket_name)
        if not self._collection_exists(collection_name):
            return []

        dense_embedder = self._get_embedder()
        dense_embeddings = list(dense_embedder.embed([normalized_query]))
        if not dense_embeddings:
            return []
        dense_query = [float(value) for value in dense_embeddings[0]]

        keyword_embedder = self._get_keyword_embedder()
        keyword_embeddings = list(keyword_embedder.embed([normalized_query]))
        if not keyword_embeddings:
            return []
        sparse_indices, sparse_values = self._coerce_sparse_embedding(keyword_embeddings[0])
        has_sparse_query = bool(sparse_indices and sparse_values)
        if normalized_mode == "keyword" and not has_sparse_query:
            return []
        sparse_query = self._build_sparse_vector(indices=sparse_indices, values=sparse_values)

        if normalized_mode == "semantic":
            points = self._search_dense(
                collection_name=collection_name,
                query_vector=dense_query,
                limit=resolved_limit,
            )
            return [
                self._format_result_point(point, fallback_rank=index)
                for index, point in enumerate(points)
            ]

        if normalized_mode == "keyword":
            points = self._search_keyword(
                collection_name=collection_name,
                sparse_query=sparse_query,
                limit=resolved_limit,
            )
            return [
                self._format_result_point(point, fallback_rank=index)
                for index, point in enumerate(points)
            ]

        if not has_sparse_query:
            semantic_points = self._search_dense(
                collection_name=collection_name,
                query_vector=dense_query,
                limit=resolved_limit,
            )
            return [
                self._format_result_point(point, fallback_rank=index)
                for index, point in enumerate(semantic_points)
            ]

        prefetch_limit = min(200, max(10, resolved_limit * 4))
        semantic_points = self._search_dense(
            collection_name=collection_name,
            query_vector=dense_query,
            limit=prefetch_limit,
        )
        keyword_points = self._search_keyword(
            collection_name=collection_name,
            sparse_query=sparse_query,
            limit=prefetch_limit,
        )
        return self._rrf(
            semantic_points=semantic_points,
            keyword_points=keyword_points,
            rrf_k=rrf_k,
            limit=resolved_limit,
        )

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
        """Embed chunk text and upsert vectors into Qdrant."""
        if not chunks:
            return

        indexable_chunks: list[dict[str, Any]] = []
        texts: list[str] = []
        for chunk in chunks:
            chunk_type = str(chunk.get("type", "text")).strip().lower()
            if chunk_type == "image":
                continue
            text_value = str(chunk.get("text", "")).strip()
            if not text_value:
                continue
            indexable_chunks.append(chunk)
            texts.append(text_value)
        if not indexable_chunks:
            self.delete_document(bucket_name, document_id)
            return

        embedder = self._get_embedder()
        embeddings = list(embedder.embed(texts))
        if not embeddings:
            self.delete_document(bucket_name, document_id)
            return
        keyword_embedder = self._get_keyword_embedder()
        sparse_embeddings = list(keyword_embedder.embed(texts))
        if sparse_embeddings and len(sparse_embeddings) != len(embeddings):
            sparse_embeddings = sparse_embeddings[: len(embeddings)]

        first_vector = embeddings[0]
        vector_size = len(first_vector)
        collection_name = self._collection_name(bucket_name)
        self._ensure_collection(collection_name, vector_size)
        self.delete_document(bucket_name, document_id)

        from qdrant_client import models as qdrant_models

        minio_location = f"{bucket_name}/{file_path}"
        resolved_document_year = str(document_year or "").strip()
        del meta_key

        points: list[qdrant_models.PointStruct] = []
        for index, (chunk, embedding) in enumerate(zip(indexable_chunks, embeddings, strict=False)):
            chunk_index = int(chunk.get("chunk_index", len(points)))
            raw_metadata = chunk.get("metadata")
            chunk_metadata = raw_metadata if isinstance(raw_metadata, Mapping) else {}
            section_title = str(chunk_metadata.get("section_title", "") or "")
            section_id = str(
                chunk_metadata.get("parent_section_id", "") or chunk_metadata.get("section_id", "")
            ).strip()
            raw_orig_elements = chunk.get("orig_elements", [])
            orig_elements: list[dict[str, Any]] = []
            if isinstance(raw_orig_elements, list):
                for value in raw_orig_elements:
                    if not isinstance(value, Mapping):
                        continue
                    orig_elements.append({str(key): field for key, field in value.items()})

            raw_page_numbers = chunk.get("page_numbers", [])
            page_numbers: list[int] = []
            if isinstance(raw_page_numbers, list):
                for value in raw_page_numbers:
                    try:
                        page_number = int(value)
                    except (TypeError, ValueError):
                        continue
                    if page_number > 0 and page_number not in page_numbers:
                        page_numbers.append(page_number)
            if not page_numbers and orig_elements:
                page_numbers = _coerce_chunk_page_numbers(orig_elements)
            if not page_numbers:
                page_start = _safe_int(chunk_metadata.get("page_start"))
                page_end = _safe_int(chunk_metadata.get("page_end"))
                if page_start is not None and page_end is not None:
                    for page_number in range(page_start, page_end + 1):
                        if page_number not in page_numbers:
                            page_numbers.append(page_number)
                elif page_start is not None:
                    page_numbers.append(page_start)
                elif page_end is not None:
                    page_numbers.append(page_end)
            chunk_page_start = _safe_int(chunk_metadata.get("page_start"))
            chunk_page_end = _safe_int(chunk_metadata.get("page_end"))
            if chunk_page_start is None and page_numbers:
                chunk_page_start = min(page_numbers)
            if chunk_page_end is None and page_numbers:
                chunk_page_end = max(page_numbers)
            if chunk_page_start is None and chunk_page_end is not None:
                chunk_page_start = chunk_page_end

            raw_bounding_boxes = chunk.get("bounding_boxes", [])
            bounding_boxes: list[dict[str, Any]] = []
            if isinstance(raw_bounding_boxes, list):
                for value in raw_bounding_boxes:
                    if not isinstance(value, Mapping):
                        continue
                    points_payload = _normalize_coordinate_points(value.get("points"))
                    if not points_payload:
                        continue
                    payload_box: dict[str, Any] = {"points": points_payload}

                    raw_page_number = value.get("page_number")
                    if raw_page_number is not None:
                        bbox_page_number: int | None
                        try:
                            bbox_page_number = int(raw_page_number)
                        except (TypeError, ValueError):
                            bbox_page_number = None
                        if bbox_page_number is not None and bbox_page_number > 0:
                            payload_box["page_number"] = bbox_page_number

                    raw_layout_width = value.get("layout_width")
                    if isinstance(raw_layout_width, (int, float)):
                        payload_box["layout_width"] = float(raw_layout_width)
                    raw_layout_height = value.get("layout_height")
                    if isinstance(raw_layout_height, (int, float)):
                        payload_box["layout_height"] = float(raw_layout_height)

                    raw_system = value.get("system")
                    if isinstance(raw_system, str):
                        normalized_system = raw_system.strip()
                        if normalized_system:
                            payload_box["system"] = normalized_system

                    bounding_boxes.append(payload_box)
            if not bounding_boxes and orig_elements:
                bounding_boxes = _coerce_chunk_bounding_boxes(orig_elements)

            point_id = compute_chunk_point_id(
                bucket_name=bucket_name,
                document_id=document_id,
                chunk_index=chunk_index,
            )
            vector = [float(value) for value in embedding]
            named_vector_payload: dict[str, Any] = {self._dense_vector_name: vector}
            if index < len(sparse_embeddings):
                sparse_indices, sparse_values = self._coerce_sparse_embedding(
                    sparse_embeddings[index]
                )
                if sparse_indices and sparse_values:
                    named_vector_payload[self._keyword_vector_name] = self._build_sparse_vector(
                        indices=sparse_indices,
                        values=sparse_values,
                    )
            resolver_url = build_resolver_url(
                bucket_name,
                file_path,
                page_start=chunk_page_start,
            )
            payload = {
                "document_id": document_id,
                "year": resolved_document_year,
                "partition_key": partition_key,
                "minio_location": minio_location,
                "resolver_url": resolver_url,
                "chunk_index": chunk_index,
                "chunk_id": str(chunk.get("chunk_id", "")),
                "chunk_type": str(chunk.get("type", "text")),
                "section_id": section_id,
                "section_title": section_title,
                "page_start": chunk_page_start,
                "page_end": chunk_page_end,
                "filename": chunk_metadata.get("filename"),
                "text": str(chunk.get("text", "")),
                "bounding_boxes": bounding_boxes,
                "orig_elements": orig_elements,
            }
            points.append(
                qdrant_models.PointStruct(
                    id=point_id,
                    vector=named_vector_payload,
                    payload=payload,
                )
            )

        if points:
            self._qdrant_client.upsert(collection_name=collection_name, points=points, wait=True)
