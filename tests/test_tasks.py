from __future__ import annotations

from typing import Any

import pytest

from mcp_evidencebase import tasks as task_module
from mcp_evidencebase.ingestion_modules.service import DependencyDisabledError

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


class FakeStageService:
    def __init__(
        self,
        crossref_error: Exception | None = None,
        *,
        section_error: Exception | None = None,
        upsert_error: Exception | None = None,
    ) -> None:
        self.partition_calls: list[tuple[str, str, str | None]] = []
        self.meta_calls: list[tuple[str, str, str, str | None]] = []
        self.section_calls: list[tuple[str, str, str, str | None]] = []
        self.chunk_calls: list[tuple[str, str, str, str | None]] = []
        self.upsert_calls: list[tuple[str, str, str, str | None]] = []
        self.crossref_calls: list[tuple[str, str]] = []
        self.crossref_error = crossref_error
        self.section_error = section_error
        self.upsert_error = upsert_error

    def partition_stage_object(
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
            "meta_key": "",
        }

    def meta_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        self.meta_calls.append((bucket_name, object_name, document_id, etag))
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "document_id": document_id,
            "etag": etag or "",
            "partition_key": "partition-hash",
            "meta_key": "meta-hash",
        }

    def section_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        if self.section_error is not None:
            raise self.section_error
        self.section_calls.append((bucket_name, object_name, document_id, etag))
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "document_id": document_id,
            "etag": etag or "",
            "partition_key": "partition-hash",
            "meta_key": "meta-hash",
        }

    def chunk_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        self.chunk_calls.append((bucket_name, object_name, document_id, etag))
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "document_id": document_id,
            "etag": etag or "",
            "partition_key": "partition-hash",
            "meta_key": "meta-hash",
        }

    def upsert_stage_object(
        self,
        *,
        bucket_name: str,
        object_name: str,
        document_id: str,
        etag: str | None = None,
    ) -> dict[str, str]:
        if self.upsert_error is not None:
            raise self.upsert_error
        self.upsert_calls.append((bucket_name, object_name, document_id, etag))
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "document_id": document_id,
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


def test_scan_minio_objects_enqueues_partition_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure scan task queues only changed objects into the partition stage."""
    fake_partition_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: FakeScanService())
    monkeypatch.setattr(task_module, "partition_minio_object", fake_partition_task)

    summary = task_module.scan_minio_objects.run("research-raw")

    assert summary == {"scanned": 1, "queued": 1}
    assert fake_partition_task.calls == [("research-raw", "paper.pdf", "etag-1", True)]


def test_partition_task_enqueues_meta_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify partition task runs partition stage and schedules metadata stage."""
    fake_service = FakeStageService()
    fake_meta_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)
    monkeypatch.setattr(task_module, "meta_minio_object", fake_meta_task)

    result = task_module.partition_minio_object.run("research-raw", "paper.pdf", "etag-1", True)

    assert result["document_id"] == "doc-123"
    assert fake_service.partition_calls == [("research-raw", "paper.pdf", "etag-1")]
    assert fake_meta_task.calls == [
        (
            {
                "bucket_name": "research-raw",
                "object_name": "paper.pdf",
                "document_id": "doc-123",
                "etag": "etag-1",
                "partition_key": "partition-hash",
                "meta_key": "",
                "update_meta": True,
            },
        )
    ]


def test_meta_task_update_meta_fetches_crossref(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify metadata task can enrich from Crossref before queueing sections."""
    fake_service = FakeStageService()
    fake_section_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)
    monkeypatch.setattr(task_module, "section_minio_object", fake_section_task)

    payload = {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
        "etag": "etag-1",
        "partition_key": "partition-hash",
        "update_meta": True,
    }
    result = task_module.meta_minio_object.run(payload)

    assert result["meta_key"] == "meta-hash"
    assert fake_service.meta_calls == [("research-raw", "paper.pdf", "doc-123", "etag-1")]
    assert fake_service.crossref_calls == [("research-raw", "doc-123")]
    assert fake_section_task.calls == [(result,)]


def test_meta_task_update_meta_ignores_crossref_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure Crossref failures do not block downstream ingestion stages."""
    fake_service = FakeStageService(crossref_error=ValueError("no doi"))
    fake_section_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)
    monkeypatch.setattr(task_module, "section_minio_object", fake_section_task)

    payload = {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
        "etag": "etag-1",
        "partition_key": "partition-hash",
        "update_meta": True,
    }
    result = task_module.meta_minio_object.run(payload)

    assert result["document_id"] == "doc-123"
    assert fake_service.crossref_calls == [("research-raw", "doc-123")]
    assert fake_section_task.calls == [(result,)]


def test_section_task_enqueues_chunk_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure section task runs section stage and queues chunk stage."""
    fake_service = FakeStageService()
    fake_chunk_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)
    monkeypatch.setattr(task_module, "chunk_minio_object", fake_chunk_task)

    payload = {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
        "etag": "etag-1",
    }
    result = task_module.section_minio_object.run(payload)

    assert fake_service.section_calls == [("research-raw", "paper.pdf", "doc-123", "etag-1")]
    assert fake_chunk_task.calls == [(result,)]


def test_chunk_task_enqueues_upsert_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure chunk task runs chunk stage and queues upsert stage."""
    fake_service = FakeStageService()
    fake_upsert_task = FakeDelayTask()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)
    monkeypatch.setattr(task_module, "upsert_minio_object", fake_upsert_task)

    payload = {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
        "etag": "etag-1",
    }
    result = task_module.chunk_minio_object.run(payload)

    assert fake_service.chunk_calls == [("research-raw", "paper.pdf", "doc-123", "etag-1")]
    assert fake_upsert_task.calls == [(result,)]


def test_upsert_task_calls_upsert_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure upsert task runs upsert stage directly."""
    fake_service = FakeStageService()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)

    result = task_module.upsert_minio_object.run(
        {
            "bucket_name": "research-raw",
            "object_name": "paper.pdf",
            "document_id": "doc-123",
            "etag": "etag-1",
        }
    )

    assert result["document_id"] == "doc-123"
    assert fake_service.upsert_calls == [("research-raw", "paper.pdf", "doc-123", "etag-1")]


def test_section_task_propagates_disabled_redis_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis-disabled section rebuilds should fail explicitly in worker tasks."""
    fake_service = FakeStageService(
        section_error=DependencyDisabledError(
            component="Redis",
            feature="section mapping",
            hint="Enable Redis to store section mappings.",
        )
    )
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)

    with pytest.raises(
        DependencyDisabledError,
        match=r"Redis is disabled for section mapping\.",
    ):
        task_module.section_minio_object.run(
            {
                "bucket_name": "research-raw",
                "object_name": "paper.pdf",
                "document_id": "doc-123",
                "etag": "etag-1",
            }
        )


def test_upsert_task_propagates_disabled_qdrant_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Qdrant-disabled upserts should fail explicitly in worker tasks."""
    fake_service = FakeStageService(
        upsert_error=DependencyDisabledError(
            component="Qdrant",
            feature="vector upsert",
            hint="Enable Qdrant to store vectors.",
        )
    )
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)

    with pytest.raises(
        DependencyDisabledError,
        match=r"Qdrant is disabled for vector upsert\.",
    ):
        task_module.upsert_minio_object.run(
            {
                "bucket_name": "research-raw",
                "object_name": "paper.pdf",
                "document_id": "doc-123",
                "etag": "etag-1",
            }
        )


def test_process_task_update_meta_fetches_crossref_before_tail_stages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify compatibility wrapper still executes all stages inline."""
    fake_service = FakeStageService()
    monkeypatch.setattr(task_module, "build_ingestion_service", lambda: fake_service)

    result = task_module.process_minio_object.run("research-raw", "paper.pdf", "etag-1", True)

    assert result == {
        "bucket_name": "research-raw",
        "object_name": "paper.pdf",
        "document_id": "doc-123",
    }
    assert fake_service.partition_calls == [("research-raw", "paper.pdf", "etag-1")]
    assert fake_service.meta_calls == [("research-raw", "paper.pdf", "doc-123", "etag-1")]
    assert fake_service.crossref_calls == [("research-raw", "doc-123")]
    assert fake_service.section_calls == [("research-raw", "paper.pdf", "doc-123", "etag-1")]
    assert fake_service.chunk_calls == [("research-raw", "paper.pdf", "doc-123", "etag-1")]
    assert fake_service.upsert_calls == [("research-raw", "paper.pdf", "doc-123", "etag-1")]
