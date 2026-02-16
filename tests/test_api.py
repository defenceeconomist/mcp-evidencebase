from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from fastapi.testclient import TestClient
from minio.error import S3Error

from mcp_evidencebase.api import app, get_bucket_service, get_ingestion_service

pytestmark = pytest.mark.area_api


@dataclass
class FakeBucketService:
    bucket_names: list[str] = field(default_factory=list)
    created: bool = True
    removed: bool = True
    list_error: Exception | None = None
    create_error: Exception | None = None
    delete_error: Exception | None = None

    def list_buckets(self) -> list[str]:
        if self.list_error is not None:
            raise self.list_error
        return self.bucket_names

    def create_bucket(self, bucket_name: str) -> bool:
        if self.create_error is not None:
            raise self.create_error
        return self.created

    def delete_bucket(self, bucket_name: str) -> bool:
        if self.delete_error is not None:
            raise self.delete_error
        return self.removed


@dataclass
class FakeIngestionService:
    documents: list[dict[str, Any]] = field(default_factory=list)
    uploaded: list[tuple[str, str, bytes]] = field(default_factory=list)
    deleted: list[tuple[str, str, bool]] = field(default_factory=list)
    metadata_updates: list[tuple[str, str, dict[str, str]]] = field(default_factory=list)
    upload_error: Exception | None = None
    delete_error: Exception | None = None
    metadata_error: Exception | None = None
    qdrant_create_result: bool = True
    qdrant_delete_result: bool = True
    qdrant_create_error: Exception | None = None
    qdrant_delete_error: Exception | None = None

    def list_documents(self, bucket_name: str) -> list[dict[str, Any]]:
        return self.documents

    def upload_document(
        self,
        *,
        bucket_name: str,
        object_name: str,
        payload: bytes,
        content_type: str | None = None,
    ) -> str:
        if self.upload_error is not None:
            raise self.upload_error
        del content_type
        self.uploaded.append((bucket_name, object_name, payload))
        return object_name

    def delete_document(
        self,
        *,
        bucket_name: str,
        document_id: str,
        keep_partitions: bool = True,
    ) -> bool:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append((bucket_name, document_id, keep_partitions))
        return True

    def update_metadata(
        self,
        *,
        bucket_name: str,
        document_id: str,
        metadata: dict[str, str],
    ) -> dict[str, str]:
        if self.metadata_error is not None:
            raise self.metadata_error
        self.metadata_updates.append((bucket_name, document_id, metadata))
        return metadata

    def ensure_bucket_qdrant_collection(self, bucket_name: str) -> bool:
        if self.qdrant_create_error is not None:
            raise self.qdrant_create_error
        self.metadata_updates.append((bucket_name, "__qdrant_create__", {}))
        return self.qdrant_create_result

    def delete_bucket_qdrant_collection(self, bucket_name: str) -> bool:
        if self.qdrant_delete_error is not None:
            raise self.qdrant_delete_error
        self.metadata_updates.append((bucket_name, "__qdrant_delete__", {}))
        return self.qdrant_delete_result


@pytest.fixture
def client() -> TestClient:
    app.dependency_overrides[get_ingestion_service] = lambda: FakeIngestionService()
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def _override_bucket_service(service: FakeBucketService) -> None:
    app.dependency_overrides[get_bucket_service] = lambda: service


def _override_ingestion_service(service: FakeIngestionService) -> None:
    app.dependency_overrides[get_ingestion_service] = lambda: service


@dataclass
class FakeTaskResult:
    id: str


class FakeTask:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.calls: list[tuple[Any, ...]] = []

    def delay(self, *args: Any) -> FakeTaskResult:
        self.calls.append(args)
        return FakeTaskResult(id=self.task_id)


class FailingTask:
    def __init__(self, message: str) -> None:
        self.message = message

    def delay(self, *args: Any) -> FakeTaskResult:
        del args
        raise RuntimeError(self.message)


def _make_s3_error() -> S3Error:
    return S3Error(
        None,
        "AccessDenied",
        "Denied",
        None,
        None,
        None,
    )


def test_healthz_returns_ok(client: TestClient) -> None:
    """Assert ``/healthz`` returns HTTP 200 with ``{\"status\": \"ok\"}``."""
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_buckets_returns_bucket_names(client: TestClient) -> None:
    """Ensure ``GET /buckets`` returns bucket names from the bucket service."""
    _override_bucket_service(FakeBucketService(bucket_names=["alpha", "beta"]))

    response = client.get("/buckets")

    assert response.status_code == 200
    assert response.json() == {"buckets": ["alpha", "beta"]}


def test_get_buckets_maps_s3_errors_to_bad_request(client: TestClient) -> None:
    """Verify MinIO ``S3Error`` values are mapped to HTTP 400 responses."""
    _override_bucket_service(FakeBucketService(list_error=_make_s3_error()))

    response = client.get("/buckets")

    assert response.status_code == 400
    assert response.json() == {"detail": "AccessDenied: Denied"}


def test_create_bucket_returns_created_result(client: TestClient) -> None:
    """Check bucket creation normalizes whitespace and reports Qdrant sync status."""
    _override_bucket_service(FakeBucketService(created=True))
    _override_ingestion_service(FakeIngestionService(qdrant_create_result=True))

    response = client.post("/buckets", json={"bucket_name": "  research-raw  "})

    assert response.status_code == 200
    assert response.json() == {
        "bucket_name": "research-raw",
        "created": True,
        "qdrant_collection_created": True,
    }


def test_create_bucket_maps_value_error_to_bad_request(client: TestClient) -> None:
    """Confirm invalid bucket input is surfaced as HTTP 400 from ``POST /buckets``."""
    _override_bucket_service(
        FakeBucketService(create_error=ValueError("bucket_name must not be empty."))
    )

    response = client.post("/buckets", json={"bucket_name": "   "})

    assert response.status_code == 400
    assert response.json() == {"detail": "bucket_name must not be empty."}


def test_delete_bucket_returns_removed_result(client: TestClient) -> None:
    """Ensure successful deletion returns both bucket and Qdrant removal flags."""
    _override_bucket_service(FakeBucketService(removed=True))
    _override_ingestion_service(FakeIngestionService(qdrant_delete_result=True))

    response = client.delete("/buckets/research-raw")

    assert response.status_code == 200
    assert response.json() == {
        "bucket_name": "research-raw",
        "removed": True,
        "qdrant_collection_removed": True,
    }


def test_delete_bucket_maps_s3_errors_to_bad_request(client: TestClient) -> None:
    """Verify ``DELETE /buckets/{bucket}`` maps MinIO errors to HTTP 400."""
    _override_bucket_service(FakeBucketService(delete_error=_make_s3_error()))

    response = client.delete("/buckets/research-raw")

    assert response.status_code == 400
    assert response.json() == {"detail": "AccessDenied: Denied"}


def test_create_bucket_returns_bad_gateway_when_qdrant_sync_fails(client: TestClient) -> None:
    """Check Qdrant sync failures during create map to HTTP 502."""
    _override_bucket_service(FakeBucketService(created=True))
    _override_ingestion_service(
        FakeIngestionService(qdrant_create_error=RuntimeError("qdrant unavailable"))
    )

    response = client.post("/buckets", json={"bucket_name": "research-raw"})

    assert response.status_code == 502
    assert "Qdrant sync failed while creating collection" in response.json()["detail"]


def test_delete_bucket_returns_bad_gateway_when_qdrant_sync_fails(client: TestClient) -> None:
    """Check Qdrant sync failures during delete map to HTTP 502."""
    _override_bucket_service(FakeBucketService(removed=True))
    _override_ingestion_service(
        FakeIngestionService(qdrant_delete_error=RuntimeError("qdrant unavailable"))
    )

    response = client.delete("/buckets/research-raw")

    assert response.status_code == 502
    assert "Qdrant sync failed while deleting collection" in response.json()["detail"]


def test_get_documents_returns_documents(client: TestClient) -> None:
    """Assert ``GET /collections/{bucket}/documents`` returns service records."""
    service = FakeIngestionService(
        documents=[
            {
                "id": "doc-1",
                "document_id": "doc-1",
                "file_path": "paper.pdf",
                "processing_state": "processed",
                "processing_progress": 100,
                "partitions_count": 2,
                "chunks_count": 3,
            }
        ]
    )
    _override_ingestion_service(service)

    response = client.get("/collections/research-raw/documents")

    assert response.status_code == 200
    assert response.json()["bucket_name"] == "research-raw"
    assert response.json()["documents"][0]["document_id"] == "doc-1"


def test_upload_document_queues_processing_task(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify uploads enqueue ``partition_minio_object`` and return task metadata."""
    service = FakeIngestionService()
    _override_ingestion_service(service)
    fake_task = FakeTask("task-upload-1")
    monkeypatch.setattr("mcp_evidencebase.api.partition_minio_object", fake_task)

    response = client.post(
        "/collections/research-raw/documents/upload?file_name=paper.pdf",
        content=b"pdf-bytes",
        headers={"Content-Type": "application/pdf"},
    )

    assert response.status_code == 200
    assert response.json()["queued"] is True
    assert response.json()["task_id"] == "task-upload-1"
    assert response.json()["queue_error"] == ""
    assert service.uploaded == [("research-raw", "paper.pdf", b"pdf-bytes")]
    assert fake_task.calls == [("research-raw", "paper.pdf", None)]


def test_trigger_bucket_scan_queues_task(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure manual scan endpoint queues a task and returns ``queued=true``."""
    fake_task = FakeTask("task-scan-1")
    monkeypatch.setattr("mcp_evidencebase.api.scan_minio_objects", fake_task)

    response = client.post("/collections/research-raw/scan")

    assert response.status_code == 200
    assert response.json() == {
        "bucket_name": "research-raw",
        "queued": True,
        "task_id": "task-scan-1",
        "queue_error": "",
    }
    assert fake_task.calls == [("research-raw",)]


def test_upload_document_returns_queued_false_when_broker_is_unavailable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirm broker failures keep upload successful but report ``queued=false``."""
    service = FakeIngestionService()
    _override_ingestion_service(service)
    monkeypatch.setattr(
        "mcp_evidencebase.api.partition_minio_object",
        FailingTask("retry limit exceeded while trying to reconnect"),
    )

    response = client.post(
        "/collections/research-raw/documents/upload?file_name=paper.pdf",
        content=b"pdf-bytes",
        headers={"Content-Type": "application/pdf"},
    )

    assert response.status_code == 200
    assert response.json()["queued"] is False
    assert response.json()["task_id"] is None
    assert "retry limit exceeded" in response.json()["queue_error"]
    assert service.uploaded == [("research-raw", "paper.pdf", b"pdf-bytes")]


def test_trigger_bucket_scan_returns_queued_false_when_broker_is_unavailable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirm scan queue failures return ``queued=false`` with queue error details."""
    monkeypatch.setattr(
        "mcp_evidencebase.api.scan_minio_objects",
        FailingTask("retry limit exceeded while trying to reconnect"),
    )

    response = client.post("/collections/research-raw/scan")

    assert response.status_code == 200
    assert response.json()["bucket_name"] == "research-raw"
    assert response.json()["queued"] is False
    assert response.json()["task_id"] is None
    assert "retry limit exceeded" in response.json()["queue_error"]


def test_update_document_metadata_returns_payload(client: TestClient) -> None:
    """Check metadata update echoes normalized payload and service call arguments."""
    service = FakeIngestionService()
    _override_ingestion_service(service)

    response = client.put(
        "/collections/research-raw/documents/doc-1/metadata",
        json={"metadata": {"title": "A Paper", "year": "2024"}},
    )

    assert response.status_code == 200
    assert response.json() == {
        "bucket_name": "research-raw",
        "document_id": "doc-1",
        "metadata": {"title": "A Paper", "year": "2024"},
    }
    assert service.metadata_updates == [
        ("research-raw", "doc-1", {"title": "A Paper", "year": "2024"})
    ]


def test_delete_document_keeps_partitions(client: TestClient) -> None:
    """Ensure delete endpoint preserves partitions by forcing ``keep_partitions=True``."""
    service = FakeIngestionService()
    _override_ingestion_service(service)

    response = client.delete("/collections/research-raw/documents/doc-1")

    assert response.status_code == 200
    assert response.json() == {
        "bucket_name": "research-raw",
        "document_id": "doc-1",
        "removed": True,
        "partitions_retained": True,
    }
    assert service.deleted == [("research-raw", "doc-1", True)]
