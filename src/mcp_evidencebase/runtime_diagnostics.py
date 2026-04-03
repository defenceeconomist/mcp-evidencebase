"""Runtime dependency diagnostics for API readiness and CLI preflight checks."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from minio import Minio

from mcp_evidencebase.minio_settings import MinioSettings, to_bool


@dataclass(frozen=True)
class DependencyRequirement:
    """Runtime requirement policy for one external component."""

    required: bool
    env_var: str


@dataclass(frozen=True)
class RuntimeContract:
    """Explicit dependency contract for the running process."""

    minio: DependencyRequirement
    redis: DependencyRequirement
    qdrant: DependencyRequirement
    celery_broker: DependencyRequirement
    celery_result_backend: DependencyRequirement


@dataclass(frozen=True)
class ComponentStatus:
    """Serializable health status for one runtime component."""

    required: bool
    configured: bool
    status: str
    target: str
    detail: str


def _read_target(source: Mapping[str, str], name: str) -> str:
    """Return one normalized dependency target from the environment."""
    return str(source.get(name, "")).strip()


def _read_required_flag(
    source: Mapping[str, str],
    name: str,
    *,
    default: bool,
) -> DependencyRequirement:
    raw_value = source.get(name)
    if raw_value is None:
        return DependencyRequirement(required=default, env_var=name)
    return DependencyRequirement(required=to_bool(raw_value), env_var=name)


def build_runtime_contract(env: Mapping[str, str] | None = None) -> RuntimeContract:
    """Build the runtime dependency contract from environment variables."""
    source = os.environ if env is None else env
    return RuntimeContract(
        minio=_read_required_flag(source, "MCP_EVIDENCEBASE_REQUIRE_MINIO", default=True),
        redis=_read_required_flag(source, "MCP_EVIDENCEBASE_REQUIRE_REDIS", default=True),
        qdrant=_read_required_flag(source, "MCP_EVIDENCEBASE_REQUIRE_QDRANT", default=True),
        celery_broker=_read_required_flag(
            source,
            "MCP_EVIDENCEBASE_REQUIRE_CELERY_BROKER",
            default=True,
        ),
        celery_result_backend=_read_required_flag(
            source,
            "MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND",
            default=True,
        ),
    )


def probe_minio(settings: MinioSettings) -> None:
    """Verify MinIO is reachable using the configured credentials."""
    client = Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
        region=settings.region,
    )
    client.list_buckets()


def probe_redis(redis_url: str) -> None:
    """Verify Redis is reachable."""
    import redis

    client = redis.Redis.from_url(redis_url, decode_responses=True)
    try:
        client.ping()
    finally:
        client.close()


def probe_qdrant(
    *,
    qdrant_url: str,
    api_key: str | None,
    timeout_seconds: float,
) -> None:
    """Verify Qdrant is reachable."""
    from qdrant_client import QdrantClient

    client = QdrantClient(
        url=qdrant_url,
        api_key=api_key,
        timeout=max(1, int(timeout_seconds)),
    )
    try:
        client.get_collections()
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def probe_celery_broker(broker_url: str) -> None:
    """Verify the Celery broker is reachable."""
    from kombu import Connection  # type: ignore[import-untyped]

    connection = Connection(broker_url, connect_timeout=5)
    try:
        connection.connect()
    finally:
        connection.release()


def probe_celery_result_backend(result_backend_url: str) -> None:
    """Verify the Celery result backend is reachable."""
    from kombu import Connection

    connection = Connection(result_backend_url, connect_timeout=5)
    try:
        connection.connect()
    finally:
        connection.release()


def _component_status(
    *,
    target: str,
    requirement: DependencyRequirement,
    probe: Any,
) -> ComponentStatus:
    normalized_target = str(target).strip()
    configured = bool(normalized_target)
    if not configured:
        status = "error" if requirement.required else "disabled"
        detail = (
            f"Missing configuration for required dependency ({requirement.env_var})."
            if requirement.required
            else "Dependency disabled because no target is configured."
        )
        return ComponentStatus(
            required=requirement.required,
            configured=configured,
            status=status,
            target=normalized_target,
            detail=detail,
        )

    try:
        probe()
    except Exception as exc:
        return ComponentStatus(
            required=requirement.required,
            configured=configured,
            status="error",
            target=normalized_target,
            detail=f"{type(exc).__name__}: {exc}",
        )

    return ComponentStatus(
        required=requirement.required,
        configured=configured,
        status="ok",
        target=normalized_target,
        detail="reachable",
    )


def collect_runtime_health(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Collect component health for external dependencies."""
    from mcp_evidencebase.ingestion_modules.wiring import build_ingestion_settings

    source = os.environ if env is None else env
    settings = build_ingestion_settings(source)
    contract = build_runtime_contract(source)
    broker_url = _read_target(source, "CELERY_BROKER_URL")
    result_backend_url = _read_target(source, "CELERY_RESULT_BACKEND")

    checks = {
        "minio": asdict(
            _component_status(
                target=settings.minio.endpoint,
                requirement=contract.minio,
                probe=lambda: probe_minio(settings.minio),
            )
        ),
        "redis": asdict(
            _component_status(
                target=settings.redis_url,
                requirement=contract.redis,
                probe=lambda: probe_redis(settings.redis_url),
            )
        ),
        "qdrant": asdict(
            _component_status(
                target=settings.qdrant_url,
                requirement=contract.qdrant,
                probe=lambda: probe_qdrant(
                    qdrant_url=settings.qdrant_url,
                    api_key=settings.qdrant_api_key,
                    timeout_seconds=settings.qdrant_timeout_seconds,
                ),
            )
        ),
        "celery_broker": asdict(
            _component_status(
                target=broker_url,
                requirement=contract.celery_broker,
                probe=lambda: probe_celery_broker(broker_url),
            )
        ),
        "celery_result_backend": asdict(
            _component_status(
                target=result_backend_url,
                requirement=contract.celery_result_backend,
                probe=lambda: probe_celery_result_backend(result_backend_url),
            )
        ),
    }

    failed_required_checks = [
        component_name
        for component_name, payload in checks.items()
        if payload["required"] and payload["status"] != "ok"
    ]
    ready = not failed_required_checks
    status = "ok" if ready else "error"
    summary = (
        "All required runtime dependencies are reachable."
        if ready
        else "Required runtime dependencies are unavailable."
    )
    return {
        "status": status,
        "ready": ready,
        "summary": summary,
        "failed_required_checks": failed_required_checks,
        "contract": {
            "minio": asdict(contract.minio),
            "redis": asdict(contract.redis),
            "qdrant": asdict(contract.qdrant),
            "celery_broker": asdict(contract.celery_broker),
            "celery_result_backend": asdict(contract.celery_result_backend),
        },
        "checks": checks,
    }


def log_runtime_health(
    logger: logging.Logger,
    *,
    report: Mapping[str, Any],
    component_name: str,
) -> None:
    """Log the resolved runtime dependency contract for one long-running process."""
    checks = report.get("checks", {})
    if not isinstance(checks, Mapping):
        logger.info("%s runtime dependency report is unavailable.", component_name)
        return

    fragments: list[str] = []
    for dependency_name in (
        "minio",
        "redis",
        "qdrant",
        "celery_broker",
        "celery_result_backend",
    ):
        payload = checks.get(dependency_name)
        if not isinstance(payload, Mapping):
            continue
        status = str(payload.get("status", "unknown")).strip() or "unknown"
        target = str(payload.get("target", "")).strip() or "<unset>"
        required = bool(payload.get("required"))
        contract_label = "required" if required else "optional"
        fragments.append(f"{dependency_name}={status} ({contract_label}, target={target})")

    logger.info("%s runtime dependency contract: %s", component_name, "; ".join(fragments))


def raise_for_failed_required_checks(
    report: Mapping[str, Any],
    *,
    component_name: str,
) -> None:
    """Raise when required dependencies are unavailable for process startup."""
    failed_required_checks = report.get("failed_required_checks", [])
    if not isinstance(failed_required_checks, list) or not failed_required_checks:
        return

    checks = report.get("checks", {})
    details: list[str] = []
    if isinstance(checks, Mapping):
        for dependency_name in failed_required_checks:
            payload = checks.get(str(dependency_name))
            if not isinstance(payload, Mapping):
                details.append(str(dependency_name))
                continue
            target = str(payload.get("target", "")).strip() or "<unset>"
            detail = str(payload.get("detail", "")).strip() or "unavailable"
            details.append(f"{dependency_name} [{target}]: {detail}")

    suffix = (
        "; ".join(details)
        if details
        else ", ".join(str(name) for name in failed_required_checks)
    )
    raise RuntimeError(
        f"{component_name} startup blocked by required runtime dependency failures: {suffix}"
    )
