from __future__ import annotations

import pytest

from mcp_evidencebase import celery_app

pytestmark = pytest.mark.area_core


def test_validate_worker_runtime_dependencies_uses_worker_component_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker init hook should validate runtime dependencies as celery-worker."""
    component_names: list[str] = []
    monkeypatch.setattr(
        celery_app,
        "_validate_runtime_dependencies",
        lambda component_name: component_names.append(component_name),
    )

    celery_app.validate_worker_runtime_dependencies()

    assert component_names == ["celery-worker"]


def test_validate_beat_runtime_dependencies_uses_beat_component_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Beat init hook should validate runtime dependencies as celery-beat."""
    component_names: list[str] = []
    monkeypatch.setattr(
        celery_app,
        "_validate_runtime_dependencies",
        lambda component_name: component_names.append(component_name),
    )

    celery_app.validate_beat_runtime_dependencies()

    assert component_names == ["celery-beat"]


def test_validate_runtime_dependencies_raises_for_failed_required_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Celery startup validation should raise with the Celery component label."""
    monkeypatch.setattr(
        celery_app,
        "collect_runtime_health",
        lambda: {
            "ready": False,
            "failed_required_checks": ["celery_result_backend"],
            "checks": {
                "celery_result_backend": {
                    "required": True,
                    "status": "error",
                    "target": "redis://redis:6379/1",
                    "detail": "ConnectionError: unavailable",
                }
            },
        },
    )

    with pytest.raises(RuntimeError, match="celery-worker startup blocked"):
        celery_app._validate_runtime_dependencies("celery-worker")
