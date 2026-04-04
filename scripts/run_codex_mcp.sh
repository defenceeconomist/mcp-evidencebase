#!/bin/zsh

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
container_name="${MCP_EVIDENCEBASE_CONTAINER:-evidencebase-api}"

# Prefer the running API container so the stdio MCP process shares the same
# Docker network and runtime that already serves the healthy local API.
if docker ps --format '{{.Names}}' 2>/dev/null | grep -Fxq "${container_name}"; then
  exec docker exec -i "${container_name}" python -m mcp_evidencebase.mcp_server
fi

# Codex launches stdio MCP servers on the host, so use host-reachable datastore
# endpoints instead of the Docker-only service names in `.env`.
export PYTHONPATH="${repo_root}/src${PYTHONPATH:+:${PYTHONPATH}}"
export MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
export MINIO_SECURE="${MINIO_SECURE:-false}"
export MINIO_REGION="${MINIO_REGION:-}"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
export MINIO_SERVER_URL="${MINIO_SERVER_URL:-http://localhost:9000}"
export MINIO_BROWSER_REDIRECT_URL="${MINIO_BROWSER_REDIRECT_URL:-http://localhost:52180/minio-console}"
export CELERY_BROKER_URL="${CELERY_BROKER_URL:-redis://localhost:6379/0}"
export CELERY_RESULT_BACKEND="${CELERY_RESULT_BACKEND:-redis://localhost:6379/1}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/2}"
export REDIS_PREFIX="${REDIS_PREFIX:-evidencebase}"
export QDRANT_URL="${QDRANT_URL:-http://localhost:6333}"
export QDRANT_API_KEY="${QDRANT_API_KEY:-}"
export QDRANT_COLLECTION_PREFIX="${QDRANT_COLLECTION_PREFIX:-evidencebase}"
export QDRANT_TIMEOUT_SECONDS="${QDRANT_TIMEOUT_SECONDS:-30}"
export MCP_EVIDENCEBASE_REQUIRE_MINIO="${MCP_EVIDENCEBASE_REQUIRE_MINIO:-true}"
export MCP_EVIDENCEBASE_REQUIRE_REDIS="${MCP_EVIDENCEBASE_REQUIRE_REDIS:-true}"
export MCP_EVIDENCEBASE_REQUIRE_QDRANT="${MCP_EVIDENCEBASE_REQUIRE_QDRANT:-true}"
export MCP_EVIDENCEBASE_REQUIRE_CELERY_BROKER="${MCP_EVIDENCEBASE_REQUIRE_CELERY_BROKER:-true}"
export MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND="${MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND:-true}"

exec "${repo_root}/.venv/bin/python" -m mcp_evidencebase.mcp_server
