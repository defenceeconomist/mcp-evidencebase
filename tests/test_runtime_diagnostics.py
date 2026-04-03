from __future__ import annotations

import logging

import pytest

from mcp_evidencebase.runtime_diagnostics import (
    build_runtime_contract,
    collect_runtime_health,
    log_runtime_health,
    raise_for_failed_required_checks,
)

pytestmark = pytest.mark.area_core


def _base_env() -> dict[str, str]:
    return {
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": "minioadmin",
        "REDIS_URL": "redis://redis:6379/2",
        "QDRANT_URL": "http://qdrant:6333",
        "CELERY_BROKER_URL": "redis://redis:6379/0",
        "CELERY_RESULT_BACKEND": "redis://redis:6379/1",
    }


def test_build_runtime_contract_reads_explicit_flags() -> None:
    """Verify runtime dependency requirement flags are loaded from environment values."""
    contract = build_runtime_contract(
        {
            "MCP_EVIDENCEBASE_REQUIRE_MINIO": "true",
            "MCP_EVIDENCEBASE_REQUIRE_REDIS": "false",
            "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "false",
            "MCP_EVIDENCEBASE_REQUIRE_CELERY_BROKER": "true",
            "MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND": "true",
        }
    )

    assert contract.minio.required is True
    assert contract.redis.required is False
    assert contract.qdrant.required is False
    assert contract.celery_broker.required is True
    assert contract.celery_result_backend.required is True


def test_collect_runtime_health_returns_ready_when_optional_qdrant_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optional dependency failures should not make the overall runtime unready."""
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_minio", lambda settings: None)
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_redis", lambda redis_url: None)
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_qdrant",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("qdrant unavailable")),
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_broker",
        lambda broker_url: None,
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_result_backend",
        lambda result_backend_url: None,
    )

    report = collect_runtime_health(
        {
            **_base_env(),
            "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "false",
        }
    )

    assert report["ready"] is True
    assert report["status"] == "ok"
    assert report["checks"]["qdrant"]["status"] == "error"
    assert report["failed_required_checks"] == []


def test_collect_runtime_health_returns_unready_when_required_qdrant_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Required dependency failures should make the readiness report fail."""
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_minio", lambda settings: None)
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_redis", lambda redis_url: None)
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_qdrant",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("qdrant unavailable")),
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_broker",
        lambda broker_url: None,
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_result_backend",
        lambda result_backend_url: None,
    )

    report = collect_runtime_health(
        {
            **_base_env(),
            "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "true",
        }
    )

    assert report["ready"] is False
    assert report["status"] == "error"
    assert report["failed_required_checks"] == ["qdrant"]


def test_collect_runtime_health_marks_optional_missing_dependency_as_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optional dependencies with no configured target should be marked disabled."""
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_minio", lambda settings: None)
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_redis", lambda redis_url: None)
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_broker",
        lambda broker_url: None,
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_result_backend",
        lambda result_backend_url: None,
    )

    report = collect_runtime_health(
        {
            **_base_env(),
            "QDRANT_URL": "",
            "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "false",
        }
    )

    assert report["ready"] is True
    assert report["checks"]["qdrant"]["status"] == "disabled"
    assert report["checks"]["qdrant"]["configured"] is False


def test_collect_runtime_health_marks_missing_required_redis_as_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing required targets should fail readiness without localhost fallbacks."""
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_minio", lambda settings: None)
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_broker",
        lambda broker_url: None,
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_result_backend",
        lambda result_backend_url: None,
    )

    report = collect_runtime_health(
        {
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
            "CELERY_BROKER_URL": "redis://redis:6379/0",
            "CELERY_RESULT_BACKEND": "redis://redis:6379/1",
            "MCP_EVIDENCEBASE_REQUIRE_REDIS": "true",
            "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "false",
        }
    )

    assert report["ready"] is False
    assert report["checks"]["redis"]["configured"] is False
    assert report["checks"]["redis"]["status"] == "error"
    assert report["failed_required_checks"] == ["redis"]


def test_collect_runtime_health_marks_missing_required_qdrant_as_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing required Qdrant config should fail readiness without fallbacks."""
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_minio", lambda settings: None)
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_redis", lambda redis_url: None)
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_broker",
        lambda broker_url: None,
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_result_backend",
        lambda result_backend_url: None,
    )

    report = collect_runtime_health(
        {
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_ROOT_USER": "minioadmin",
            "MINIO_ROOT_PASSWORD": "minioadmin",
            "REDIS_URL": "redis://redis:6379/2",
            "CELERY_BROKER_URL": "redis://redis:6379/0",
            "CELERY_RESULT_BACKEND": "redis://redis:6379/1",
            "MCP_EVIDENCEBASE_REQUIRE_QDRANT": "true",
        }
    )

    assert report["ready"] is False
    assert report["checks"]["qdrant"]["configured"] is False
    assert report["checks"]["qdrant"]["status"] == "error"
    assert report["failed_required_checks"] == ["qdrant"]


def test_collect_runtime_health_marks_missing_required_celery_broker_as_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing required broker config should fail readiness immediately."""
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_minio", lambda settings: None)
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_redis", lambda redis_url: None)
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_qdrant",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_result_backend",
        lambda result_backend_url: None,
    )

    report = collect_runtime_health(
        {
            **_base_env(),
            "CELERY_BROKER_URL": "",
            "MCP_EVIDENCEBASE_REQUIRE_CELERY_BROKER": "true",
        }
    )

    assert report["ready"] is False
    assert report["checks"]["celery_broker"]["configured"] is False
    assert report["checks"]["celery_broker"]["status"] == "error"
    assert report["failed_required_checks"] == ["celery_broker"]


def test_collect_runtime_health_marks_missing_required_celery_result_backend_as_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing required result backend config should fail readiness immediately."""
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_minio", lambda settings: None)
    monkeypatch.setattr("mcp_evidencebase.runtime_diagnostics.probe_redis", lambda redis_url: None)
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_qdrant",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "mcp_evidencebase.runtime_diagnostics.probe_celery_broker",
        lambda broker_url: None,
    )

    report = collect_runtime_health(
        {
            **_base_env(),
            "CELERY_RESULT_BACKEND": "",
            "MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND": "true",
        }
    )

    assert report["ready"] is False
    assert report["checks"]["celery_result_backend"]["configured"] is False
    assert report["checks"]["celery_result_backend"]["status"] == "error"
    assert report["failed_required_checks"] == ["celery_result_backend"]


def test_log_runtime_health_includes_celery_result_backend(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Startup contract logging should include the result backend fragment."""
    logger = logging.getLogger("tests.runtime_diagnostics")

    with caplog.at_level(logging.INFO, logger=logger.name):
        log_runtime_health(
            logger,
            component_name="celery-worker",
            report={
                "checks": {
                    "minio": {"required": True, "status": "ok", "target": "minio:9000"},
                    "redis": {"required": True, "status": "ok", "target": "redis://redis:6379/2"},
                    "qdrant": {"required": True, "status": "ok", "target": "http://qdrant:6333"},
                    "celery_broker": {
                        "required": True,
                        "status": "ok",
                        "target": "redis://redis:6379/0",
                    },
                    "celery_result_backend": {
                        "required": True,
                        "status": "ok",
                        "target": "redis://redis:6379/1",
                    },
                }
            },
        )

    assert "celery_result_backend=ok (required, target=redis://redis:6379/1)" in caplog.text


def test_raise_for_failed_required_checks_includes_component_details() -> None:
    """Startup failures should include the failing dependency and target."""
    with pytest.raises(RuntimeError, match="api startup blocked"):
        raise_for_failed_required_checks(
            {
                "failed_required_checks": ["qdrant"],
                "checks": {
                    "qdrant": {
                        "target": "http://qdrant:6333",
                        "detail": "ConnectionError: unavailable",
                    }
                },
            },
            component_name="api",
        )
