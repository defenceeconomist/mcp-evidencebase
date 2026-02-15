from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from fastapi.testclient import TestClient
from minio.error import S3Error

from mcp_evidencebase.api import app, get_bucket_service


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


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def _override_bucket_service(service: FakeBucketService) -> None:
    app.dependency_overrides[get_bucket_service] = lambda: service


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
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_buckets_returns_bucket_names(client: TestClient) -> None:
    _override_bucket_service(FakeBucketService(bucket_names=["alpha", "beta"]))

    response = client.get("/buckets")

    assert response.status_code == 200
    assert response.json() == {"buckets": ["alpha", "beta"]}


def test_get_buckets_maps_s3_errors_to_bad_request(client: TestClient) -> None:
    _override_bucket_service(FakeBucketService(list_error=_make_s3_error()))

    response = client.get("/buckets")

    assert response.status_code == 400
    assert response.json() == {"detail": "AccessDenied: Denied"}


def test_create_bucket_returns_created_result(client: TestClient) -> None:
    _override_bucket_service(FakeBucketService(created=True))

    response = client.post("/buckets", json={"bucket_name": "  research-raw  "})

    assert response.status_code == 200
    assert response.json() == {"bucket_name": "research-raw", "created": True}


def test_create_bucket_maps_value_error_to_bad_request(client: TestClient) -> None:
    _override_bucket_service(
        FakeBucketService(create_error=ValueError("bucket_name must not be empty."))
    )

    response = client.post("/buckets", json={"bucket_name": "   "})

    assert response.status_code == 400
    assert response.json() == {"detail": "bucket_name must not be empty."}


def test_delete_bucket_returns_removed_result(client: TestClient) -> None:
    _override_bucket_service(FakeBucketService(removed=True))

    response = client.delete("/buckets/research-raw")

    assert response.status_code == 200
    assert response.json() == {"bucket_name": "research-raw", "removed": True}


def test_delete_bucket_maps_s3_errors_to_bad_request(client: TestClient) -> None:
    _override_bucket_service(FakeBucketService(delete_error=_make_s3_error()))

    response = client.delete("/buckets/research-raw")

    assert response.status_code == 400
    assert response.json() == {"detail": "AccessDenied: Denied"}
