# Redis/Qdrant Decoupling Remediation Backlog

Updated: 2026-04-03

This document reflects the current branch state after reviewing the runtime wiring, diagnostics surface, test coverage, CI, and generated docs. The earlier backlog overstated how much decoupling work was still open: the runtime contract, reduced-capability behavior, startup fail-fast, CI, and docs cleanup are now implemented.

## Completed On This Branch

### 1. Dependency-aware readiness and preflight now exist

- Shipped:
  - `GET /livez` for process liveness.
  - `GET /healthz` and `GET /readyz` for dependency-aware readiness.
  - `collect_runtime_health()` as the shared probe surface.
  - `python -m mcp_evidencebase --doctor` for structured runtime diagnostics.
  - `--healthcheck` now exits non-zero when required dependencies are unavailable.
- Evidence:
  - `src/mcp_evidencebase/api.py`
  - `src/mcp_evidencebase/core.py`
  - `src/mcp_evidencebase/cli.py`
  - `src/mcp_evidencebase/runtime_diagnostics.py`
  - `tests/test_api.py`
  - `tests/test_cli.py`
  - `tests/test_core.py`
  - `tests/test_runtime_diagnostics.py`
- Status: complete enough to remove from the active backlog.

### 2. Runtime dependency policy is now explicit in configuration and wiring

- Shipped:
  - `MCP_EVIDENCEBASE_REQUIRE_MINIO`
  - `MCP_EVIDENCEBASE_REQUIRE_REDIS`
  - `MCP_EVIDENCEBASE_REQUIRE_QDRANT`
  - `MCP_EVIDENCEBASE_REQUIRE_CELERY_BROKER`
  - `MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND`
  - Redis and Qdrant URLs no longer silently default to localhost in ingestion settings.
  - Missing required Redis/Qdrant targets now raise explicit configuration errors.
  - Readiness and startup logging now treat the Celery result backend as part of the explicit runtime contract.
- Evidence:
  - `src/mcp_evidencebase/ingestion_modules/wiring.py`
  - `src/mcp_evidencebase/runtime_diagnostics.py`
  - `tests/test_ingestion.py`
  - `.env.example`
  - `docker-compose.yml`
  - `docker-compose.images.yml`
- Status: baseline complete for Redis/Qdrant contract enforcement.

### 3. Reduced-capability Redis/Qdrant mode now affects runtime behavior

- Shipped:
  - `build_ingestion_service()` now allows disabled Redis/Qdrant integrations when the runtime contract marks them optional.
  - Disabled adapters raise explicit `DependencyDisabledError` messages instead of raw client or constructor failures.
  - API error mapping returns HTTP 503 for disabled/configuration-gated document features.
  - CLI search exits non-zero with an explicit dependency-disabled message.
- Evidence:
  - `src/mcp_evidencebase/ingestion_modules/wiring.py`
  - `src/mcp_evidencebase/ingestion_modules/service.py`
  - `src/mcp_evidencebase/api_modules/errors.py`
  - `src/mcp_evidencebase/api_modules/deps.py`
  - `tests/test_api.py`
  - `tests/test_cli.py`
  - `tests/test_ingestion.py`
- Status: complete enough to remove from the active backlog.

### 4. API and Celery now fail fast on required dependency startup failures

- Shipped:
  - API startup logs the resolved dependency contract and raises before serving requests when required checks fail.
  - Celery worker and beat perform the same runtime validation on boot.
  - Celery broker/backend URLs no longer silently default to localhost.
- Evidence:
  - `src/mcp_evidencebase/api.py`
  - `src/mcp_evidencebase/celery_app.py`
  - `src/mcp_evidencebase/runtime_diagnostics.py`
  - `tests/test_api.py`
  - `tests/test_celery_app.py`
- Status: complete enough to remove from the active backlog.

### 5. Bucket CRUD is no longer hard-coupled to Qdrant collection lifecycle

- Shipped:
  - Bucket create/delete now return MinIO outcomes only.
  - Qdrant provisioning failures no longer turn bucket CRUD into HTTP 502 responses.
- Evidence:
  - `src/mcp_evidencebase/api_modules/routers/buckets.py`
  - `tests/test_api.py`
- Status: complete for the current decoupling goal.

### 6. Diagnostic workflow now uses the supported probe surface

- Shipped:
  - `scripts/run_diagnostic_checklist.sh` now checks `/api/readyz` and `python -m mcp_evidencebase --doctor` instead of maintaining a separate readiness implementation.
  - The script still keeps extra operator checks that sit outside runtime probes, such as compose rendering and external network presence.
- Evidence:
  - `scripts/run_diagnostic_checklist.sh`
  - `src/mcp_evidencebase/cli.py`
  - `src/mcp_evidencebase/runtime_diagnostics.py`
- Status: complete enough to remove from the active backlog.

### 7. Compose and environment setup now match the external datastore model more closely

- Shipped:
  - Repo-owned Redis/Qdrant services removed from compose files.
  - Shared datastore network is explicitly modeled.
  - Datastore URLs are required in compose instead of silently defaulting there.
  - `.env.example` documents the expected external topology.
  - README setup steps now reference the shared datastore stack and doctor command.
- Evidence:
  - `docker-compose.yml`
  - `docker-compose.images.yml`
  - `.env.example`
  - `README.md`
- Status: baseline complete.

### 8. Runtime-contract coverage now includes Celery hooks and more disabled-dependency paths

- Shipped:
  - Added Celery worker/beat startup-hook tests.
  - Added missing-required configuration tests for Qdrant, Celery broker, and Celery result backend.
  - Added additional API/task coverage for Redis/Qdrant disabled behavior outside the search path.
- Evidence:
  - `tests/test_celery_app.py`
  - `tests/test_runtime_diagnostics.py`
  - `tests/test_ingestion.py`
  - `tests/test_api.py`
  - `tests/test_tasks.py`
- Status: complete enough to remove from the active backlog.

### 9. CI now validates the decoupled deployment path

- Shipped:
  - Added GitHub Actions workflow for lint, typecheck, non-live pytest, and docs build.
  - Added scheduled/manual live integration against disposable MinIO, Redis, and Qdrant containers.
- Evidence:
  - `.github/workflows/ci.yml`
  - `tests/test_live_datastores_integration.py`
- Status: baseline complete.

### 10. Documentation cleanup across generated and secondary docs is complete

- Shipped:
  - Removed README file-path links that produced MyST cross-reference warnings.
  - Added an explicit top-level heading for the docs-side README include so the generated document no longer starts at `H2`.
  - Regenerated the published docs after cleanup.
- Evidence:
  - `README.md`
  - `docs/source/readme.md`
  - `docs/site/`
- Status: complete enough to remove from the active backlog.

### 11. FastAPI startup validation now uses lifespan handlers

- Shipped:
  - Replaced deprecated startup events with a FastAPI lifespan handler.
  - Preserved the same runtime contract logging and fail-fast startup validation behavior.
- Evidence:
  - `src/mcp_evidencebase/api.py`
  - `tests/test_api.py`
- Status: complete enough to remove from the active backlog.

## Open Backlog

No open remediation items remain on this branch for the Redis/Qdrant decoupling work.

## Recommended Next Steps

No further remediation work is required for this backlog. Future work can be tracked as new feature or maintenance items rather than decoupling remediation.
