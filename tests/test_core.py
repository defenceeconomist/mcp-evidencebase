import pytest

import mcp_evidencebase.core as core
from mcp_evidencebase.core import (
    add_minio_bucket,
    healthcheck,
    list_minio_buckets,
    remove_minio_bucket,
)

pytestmark = pytest.mark.area_core


def test_healthcheck_returns_ok_when_runtime_is_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the healthcheck helper reports ``ok`` when required checks pass."""
    monkeypatch.setattr(core, "collect_runtime_health", lambda: {"ready": True})
    assert healthcheck() == "ok"


def test_healthcheck_returns_error_when_runtime_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the healthcheck helper reports ``error`` when required checks fail."""
    monkeypatch.setattr(core, "collect_runtime_health", lambda: {"ready": False})
    assert healthcheck() == "error"


class FakeBucketSummary:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeMinioClient:
    def __init__(self, initial_bucket_names: set[str] | None = None) -> None:
        self.bucket_names = set(initial_bucket_names or set())
        self.created_bucket_names: list[str] = []
        self.created_locations: list[str | None] = []
        self.removed_bucket_names: list[str] = []

    def bucket_exists(self, bucket_name: str) -> bool:
        return bucket_name in self.bucket_names

    def make_bucket(self, bucket_name: str, location: str | None = None) -> None:
        self.bucket_names.add(bucket_name)
        self.created_bucket_names.append(bucket_name)
        self.created_locations.append(location)

    def remove_bucket(self, bucket_name: str) -> None:
        self.bucket_names.remove(bucket_name)
        self.removed_bucket_names.append(bucket_name)

    def list_buckets(self) -> tuple[FakeBucketSummary, ...]:
        return tuple(FakeBucketSummary(name) for name in self.bucket_names)


def test_add_minio_bucket_creates_when_missing() -> None:
    """Confirm missing buckets are created and propagate the requested region."""
    client = FakeMinioClient()

    created = add_minio_bucket("new-bucket", client=client, region="us-east-1")

    assert created is True
    assert client.created_bucket_names == ["new-bucket"]
    assert client.created_locations == ["us-east-1"]


def test_add_minio_bucket_returns_false_when_exists() -> None:
    """Ensure creating an existing bucket is a no-op that returns ``False``."""
    client = FakeMinioClient({"existing-bucket"})

    created = add_minio_bucket("existing-bucket", client=client)

    assert created is False
    assert client.created_bucket_names == []


def test_add_minio_bucket_rejects_empty_bucket_name() -> None:
    """Validate blank bucket names are rejected with a ``ValueError``."""
    client = FakeMinioClient()

    with pytest.raises(ValueError, match=r"bucket_name must not be empty\."):
        add_minio_bucket("   ", client=client)


def test_remove_minio_bucket_removes_when_exists() -> None:
    """Confirm existing buckets are removed and tracked by the client stub."""
    client = FakeMinioClient({"existing-bucket"})

    removed = remove_minio_bucket("existing-bucket", client=client)

    assert removed is True
    assert client.removed_bucket_names == ["existing-bucket"]
    assert "existing-bucket" not in client.bucket_names


def test_remove_minio_bucket_returns_false_when_missing() -> None:
    """Ensure removing a missing bucket returns ``False`` without side effects."""
    client = FakeMinioClient()

    removed = remove_minio_bucket("missing-bucket", client=client)

    assert removed is False
    assert client.removed_bucket_names == []


def test_list_minio_buckets_returns_sorted_names() -> None:
    """Verify bucket listing is returned in deterministic alphabetical order."""
    client = FakeMinioClient({"zeta", "alpha", "beta"})

    bucket_names = list_minio_buckets(client=client)

    assert bucket_names == ["alpha", "beta", "zeta"]
