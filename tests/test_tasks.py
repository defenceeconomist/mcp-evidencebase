from __future__ import annotations

from typing import Any

import pytest

from mcp_evidencebase import tasks as task_module

pytestmark = pytest.mark.area_ingestion


class FakeDelayTask:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def delay(self, *args: Any) -> Any:
        self.calls.append(args)
        return None


class FakeScanService:
    def list_buckets(self) -> list[str]:
        return ["research-raw"]

    def list_bucket_objects(self, bucket_name: str) -> list[tuple[str, str]]:
        assert bucket_name == "research-raw"
        return [("paper.pdf", "etag-1"), ("skip.pdf", "etag-2")]

    def object_requires_processing(
        self,
        *,
        bucket_name: str,
        object_name: str,
        etag: str | None,
    ) -> bool:
        assert bucket_name == "research-raw"
        assert etag is not None
        return object_name != "skip.pdf"


class FakePartitionService:
    def __init__(
        self,
        stage_payload: dict[str, str],
        crossref_error: Exception | None = None,
    ) -> None:
        self.stage_payload = stage_payload
        self.calls: list[tuple[str, str, str | None]] = []
        self.crossref_calls: list[tuple[str, str]] = []
        self.crossref_error = crossref_error

    def partition_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        self.calls.append((bucket_name, object_name, etag))
        return self.stage_payload

    def fetch_metadata_from_crossref(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> dict[str, Any]:
        self.crossref_calls.append((bucket_name, document_id))
        if self.crossref_error is not None:
            raise self.crossref_error
        return {
            "lookup_field": "doi",
            "confidence": 1.0,
            "metadata": {"title": "Fetched Title"},
        }


class FakeChunkService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def chunk_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
    ) -> dict[str, str]:
        self.calls.append((bucket_name, object_name, document_id))
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "document_id": document_id,
            "partition_key": "partition-hash",
            "meta_key": "meta-hash",
        }


class FakeProcessService:
    def __init__(self, crossref_error: Exception | None = None) -> None:
        self.partition_calls: list[tuple[str, str, str | None]] = []
        self.crossref_calls: list[tuple[str, str]] = []
        self.chunk_calls: list[tuple[str, str, str]] = []
        self.crossref_error = crossref_error

    def partition_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        self.partition_calls.append((bucket_name, object_name, etag))
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "document_id": "doc-123",
            "etag": etag or "",
            "partition_key": "partition-hash",
            "meta_key": "meta-hash",
        }

    def fetch_metadata_from_crossref(
        self,
        *,
        bucket_name: str,
        document_id: str,
    ) -> dict[str, Any]:
        self.crossref_calls.append((bucket_name, document_id))
        if self.crossref_error is not None:
            raise self.crossref_error
        return {
            "lookup_field": "doi",
            "confidence": 1.0,
            "metadata": {"title": "Fetched Title"},
        }

    def chunk_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
    ) -> dict[str, str]:
        self.chunk_calls.append((bucket_name, object_name, document_id))
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "document_id": document_id,
            "partition_key": "partition-hash",
            "meta_key": "meta-hash",
        }


def test_scan_minio_objects_enqueues_partition_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure scan task queues only changed objects into the partition stage."""
    fake_partition_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: FakeScanService())
    monkeypatch.setattr(task_module, "partition_minio_object", fake_partition_task)

    summary = task_module.scan_minio_objects.run("research-raw")

    assert summary == {"scanned": 1, "queued": 1}
    assert fake_partition_task.calls == [("research-raw", "paper.pdf", "etag-1", True)]


def test_partition_task_enqueues_chunk_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify partition stage task schedules chunk stage with returned payload."""
    stage_payload = {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
        "etag": "etag-1",
        "partition_key": "partition-hash",
        "meta_key": "meta-hash",
    }
    fake_service = FakePartitionService(stage_payload)
    fake_chunk_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)
    monkeypatch.setattr(task_module, "chunk_minio_object", fake_chunk_task)

    result = task_module.partition_minio_object.run("research-raw", "paper.pdf", "etag-1")

    assert result == stage_payload
    assert fake_service.calls == [("research-raw", "paper.pdf", "etag-1")]
    assert fake_service.crossref_calls == []
    assert fake_chunk_task.calls == [(stage_payload,)]


def test_partition_task_update_meta_fetches_crossref(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify partition stage can enrich metadata before enqueueing chunk stage."""
    stage_payload = {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
        "etag": "etag-1",
        "partition_key": "partition-hash",
        "meta_key": "meta-hash",
    }
    fake_service = FakePartitionService(stage_payload)
    fake_chunk_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)
    monkeypatch.setattr(task_module, "chunk_minio_object", fake_chunk_task)

    result = task_module.partition_minio_object.run("research-raw", "paper.pdf", "etag-1", True)

    assert result == stage_payload
    assert fake_service.crossref_calls == [("research-raw", "doc-123")]
    assert fake_chunk_task.calls == [(stage_payload,)]


def test_partition_task_update_meta_ignores_crossref_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure Crossref failures do not block chunk stage execution."""
    stage_payload = {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
        "etag": "etag-1",
        "partition_key": "partition-hash",
        "meta_key": "meta-hash",
    }
    fake_service = FakePartitionService(stage_payload, crossref_error=ValueError("no doi"))
    fake_chunk_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)
    monkeypatch.setattr(task_module, "chunk_minio_object", fake_chunk_task)

    result = task_module.partition_minio_object.run("research-raw", "paper.pdf", "etag-1", True)

    assert result == stage_payload
    assert fake_service.crossref_calls == [("research-raw", "doc-123")]
    assert fake_chunk_task.calls == [(stage_payload,)]


def test_chunk_task_calls_chunk_stage_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure chunk task resolves payload fields and calls ``chunk_object``."""
    fake_service = FakeChunkService()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)

    result = task_module.chunk_minio_object.run(
        {
            "bucket_name": "research-raw",
            "object_name": "paper.pdf",
            "document_id": "doc-123",
        }
    )

    assert result["document_id"] == "doc-123"
    assert fake_service.calls == [("research-raw", "paper.pdf", "doc-123")]


def test_process_task_update_meta_fetches_crossref_before_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify compatibility wrapper supports optional Crossref enrichment."""
    fake_service = FakeProcessService()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)

    result = task_module.process_minio_object.run("research-raw", "paper.pdf", "etag-1", True)

    assert result == {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
    }
    assert fake_service.partition_calls == [("research-raw", "paper.pdf", "etag-1")]
    assert fake_service.crossref_calls == [("research-raw", "doc-123")]
    assert fake_service.chunk_calls == [("research-raw", "paper.pdf", "doc-123")]
