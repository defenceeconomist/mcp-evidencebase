import pytest

from mcp_evidencebase.core import (
    add_minio_bucket,
    healthcheck,
    list_minio_buckets,
    remove_minio_bucket,
)


def test_healthcheck() -> None:
    assert healthcheck() == "ok"


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

    def list_buckets(self) -> list[FakeBucketSummary]:
        return [FakeBucketSummary(name) for name in self.bucket_names]


def test_add_minio_bucket_creates_when_missing() -> None:
    client = FakeMinioClient()

    created = add_minio_bucket("new-bucket", client=client, region="us-east-1")

    assert created is True
    assert client.created_bucket_names == ["new-bucket"]
    assert client.created_locations == ["us-east-1"]


def test_add_minio_bucket_returns_false_when_exists() -> None:
    client = FakeMinioClient({"existing-bucket"})

    created = add_minio_bucket("existing-bucket", client=client)

    assert created is False
    assert client.created_bucket_names == []


def test_add_minio_bucket_rejects_empty_bucket_name() -> None:
    client = FakeMinioClient()

    with pytest.raises(ValueError, match="bucket_name must not be empty."):
        add_minio_bucket("   ", client=client)


def test_remove_minio_bucket_removes_when_exists() -> None:
    client = FakeMinioClient({"existing-bucket"})

    removed = remove_minio_bucket("existing-bucket", client=client)

    assert removed is True
    assert client.removed_bucket_names == ["existing-bucket"]
    assert "existing-bucket" not in client.bucket_names


def test_remove_minio_bucket_returns_false_when_missing() -> None:
    client = FakeMinioClient()

    removed = remove_minio_bucket("missing-bucket", client=client)

    assert removed is False
    assert client.removed_bucket_names == []


def test_list_minio_buckets_returns_sorted_names() -> None:
    client = FakeMinioClient({"zeta", "alpha", "beta"})

    bucket_names = list_minio_buckets(client=client)

    assert bucket_names == ["alpha", "beta", "zeta"]
