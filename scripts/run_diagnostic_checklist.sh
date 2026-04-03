#!/usr/bin/env bash
set -u
set -o pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

START_STACK=0
WITH_LIVE=0
WITH_E2E=0
VERBOSE=0

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/evidencebase-diagnostics.XXXXXX")"
PYTHON_BIN=""
PROXY_PORT="${PROXY_PORT:-52180}"
SHARED_NETWORK_NAME="${SHARED_DATASTORE_NETWORK_NAME:-shared-datastores}"

CHECK_NAMES=()
CHECK_STATUSES=()
CHECK_HINTS=()
CHECK_LOGS=()

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

usage() {
  cat <<'EOF'
Usage: scripts/run_diagnostic_checklist.sh [options]

Runs the Evidence Base decoupling diagnostics checklist.

Options:
  --start-stack   Start `minio`, `api`, `celery`, and `proxy` if runtime smoke checks need them.
  --with-live     Run live MinIO/Redis/Qdrant integration tests.
  --with-e2e      Run Playwright E2E tests.
  --verbose       Print full failing command logs instead of the tail.
  --help          Show this help text.

Behavior:
  - Static repo and Python checks always run.
  - Runtime smoke checks run against an already-running stack by default.
  - Live integration and Playwright E2E checks are opt-in.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --start-stack)
      START_STACK=1
      ;;
    --with-live)
      WITH_LIVE=1
      ;;
    --with-e2e)
      WITH_E2E=1
      ;;
    --verbose)
      VERBOSE=1
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

section() {
  printf '\n== %s ==\n' "$1"
}

record_result() {
  CHECK_NAMES+=("$1")
  CHECK_STATUSES+=("$2")
  CHECK_HINTS+=("$3")
  CHECK_LOGS+=("$4")
}

print_log_excerpt() {
  local log_path="$1"
  if [ ! -s "$log_path" ]; then
    return
  fi
  if [ "$VERBOSE" -eq 1 ]; then
    cat "$log_path"
    return
  fi
  tail -n 20 "$log_path"
}

pass_check() {
  printf '[PASS] %s\n' "$1"
  record_result "$1" "PASS" "" "$2"
}

fail_check() {
  printf '[FAIL] %s\n' "$1"
  if [ -n "$3" ]; then
    printf '       %s\n' "$3"
  fi
  print_log_excerpt "$2"
  record_result "$1" "FAIL" "$3" "$2"
}

skip_check() {
  printf '[SKIP] %s\n' "$1"
  if [ -n "$2" ]; then
    printf '       %s\n' "$2"
  fi
  record_result "$1" "SKIP" "$2" ""
}

run_check() {
  local name="$1"
  local hint="$2"
  local fn_name="$3"
  local log_path="$TMP_DIR/$(printf '%02d' "${#CHECK_NAMES[@]}")-$(printf '%s' "$name" | tr ' /' '__').log"
  if "$fn_name" >"$log_path" 2>&1; then
    pass_check "$name" "$log_path"
  else
    fail_check "$name" "$log_path" "$hint"
  fi
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

resolve_python() {
  if [ -n "$PYTHON_BIN" ]; then
    return 0
  fi
  if [ -x "$ROOT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
    return 0
  fi
  if have_cmd python3; then
    PYTHON_BIN="$(command -v python3)"
    return 0
  fi
  if have_cmd python; then
    PYTHON_BIN="$(command -v python)"
    return 0
  fi
  return 1
}

check_python_available() {
  resolve_python
}

check_python_dependencies() {
  "$PYTHON_BIN" - <<'PY'
import importlib

required = [
    "celery",
    "fastapi",
    "minio",
    "qdrant_client",
    "redis",
]
for module_name in required:
    importlib.import_module(module_name)
PY
}

check_docker_compose_available() {
  have_cmd docker && docker compose version >/dev/null
}

check_compose_config_renders() {
  docker compose config >/dev/null
}

check_shared_network_present() {
  docker network inspect "$SHARED_NETWORK_NAME" >/dev/null
}

check_ruff() {
  ruff check .
}

check_mypy() {
  mypy .
}

check_pytest_non_live() {
  PYTHONPATH="${PYTHONPATH:-src}" "$PYTHON_BIN" -m pytest -m "not integration_live"
}

check_frontend_unit() {
  npm --prefix frontend run test:unit
}

check_frontend_e2e() {
  npm --prefix frontend run test:e2e
}

stack_is_reachable() {
  curl -fsS "http://127.0.0.1:${PROXY_PORT}/api/readyz" >/dev/null
}

start_stack_if_requested() {
  if stack_is_reachable; then
    return 0
  fi
  if [ "$START_STACK" -ne 1 ]; then
    return 1
  fi
  docker compose up -d --build minio api celery proxy
  stack_is_reachable
}

check_runtime_healthz() {
  local response
  response="$(curl -fsS "http://127.0.0.1:${PROXY_PORT}/api/readyz")"
  EVIDENCEBASE_READYZ_RESPONSE="$response" "$PYTHON_BIN" - <<'PY'
import json
import os

payload = json.loads(os.environ["EVIDENCEBASE_READYZ_RESPONSE"])
if payload.get("ready") is not True:
    raise SystemExit(1)
if "checks" not in payload:
    raise SystemExit(1)
PY
}

check_runtime_cli_doctor() {
  local output
  output="$(docker compose exec -T api python -m mcp_evidencebase --doctor)"
  EVIDENCEBASE_DOCTOR_OUTPUT="$output" "$PYTHON_BIN" - <<'PY'
import json
import os

payload = json.loads(os.environ["EVIDENCEBASE_DOCTOR_OUTPUT"])
if payload.get("ready") is not True:
    raise SystemExit(1)
if "checks" not in payload:
    raise SystemExit(1)
PY
}

check_runtime_bucket_list() {
  local response
  response="$(curl -fsS "http://127.0.0.1:${PROXY_PORT}/api/buckets")"
  EVIDENCEBASE_BUCKETS_RESPONSE="$response" "$PYTHON_BIN" - <<'PY'
import json
import os

payload = json.loads(os.environ["EVIDENCEBASE_BUCKETS_RESPONSE"])
if "buckets" not in payload:
    raise SystemExit(1)
PY
}

check_live_integration() {
  MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION=1 \
    PYTHONPATH="${PYTHONPATH:-src}" \
    "$PYTHON_BIN" -m pytest -m integration_live tests/test_live_datastores_integration.py
}

print_summary() {
  local pass_count=0
  local fail_count=0
  local skip_count=0
  local index

  printf '\n== Summary ==\n'
  for index in "${!CHECK_NAMES[@]}"; do
    case "${CHECK_STATUSES[$index]}" in
      PASS) pass_count=$((pass_count + 1)) ;;
      FAIL) fail_count=$((fail_count + 1)) ;;
      SKIP) skip_count=$((skip_count + 1)) ;;
    esac
    printf '%-5s %s\n' "${CHECK_STATUSES[$index]}" "${CHECK_NAMES[$index]}"
  done

  printf '\nPass: %d  Fail: %d  Skip: %d\n' "$pass_count" "$fail_count" "$skip_count"

  if [ "$fail_count" -gt 0 ]; then
    printf '\nRemediation starting points:\n'
    for index in "${!CHECK_NAMES[@]}"; do
      if [ "${CHECK_STATUSES[$index]}" = "FAIL" ] && [ -n "${CHECK_HINTS[$index]}" ]; then
        printf -- '- %s: %s\n' "${CHECK_NAMES[$index]}" "${CHECK_HINTS[$index]}"
      fi
    done
    printf -- '- Backlog: docs/redis-qdrant-decoupling-remediation-backlog.md\n'
  fi

  [ "$fail_count" -eq 0 ]
}

section "Repo And Tooling"

run_check \
  "Python interpreter is available" \
  "Create a virtualenv or install Python 3.10+ before running diagnostics." \
  check_python_available

if [ -n "$PYTHON_BIN" ]; then
  run_check \
    "Python runtime dependencies import cleanly" \
    "Install the project dependencies, for example: python -m pip install -e \".[dev]\"." \
    check_python_dependencies
else
  skip_check \
    "Python runtime dependencies import cleanly" \
    "Skipped because no Python interpreter was found."
fi

if check_docker_compose_available >/dev/null 2>&1; then
  run_check \
    "Docker Compose configuration renders" \
    'Copy `.env.example` to `.env` and fill in the required external datastore variables until `docker compose config` succeeds.' \
    check_compose_config_renders
  run_check \
    "Shared datastore network exists" \
    "Create or rename the external network, or update SHARED_DATASTORE_NETWORK_NAME." \
    check_shared_network_present
else
  skip_check \
    "Docker Compose configuration renders" \
    "Skipped because Docker Compose is not available."
  skip_check \
    "Shared datastore network exists" \
    "Skipped because Docker Compose is not available."
fi

section "Python Test Suite"

if have_cmd ruff; then
  run_check \
    "Ruff lint passes" \
    "Fix lint violations before investigating runtime coupling issues." \
    check_ruff
else
  skip_check \
    "Ruff lint passes" \
    'Skipped because `ruff` is not installed.'
fi

if have_cmd mypy; then
  run_check \
    "Mypy type checks pass" \
    "Fix typing regressions in the decoupling changes before moving on to runtime diagnostics." \
    check_mypy
else
  skip_check \
    "Mypy type checks pass" \
    'Skipped because `mypy` is not installed.'
fi

if [ -n "$PYTHON_BIN" ]; then
  run_check \
    "Pytest passes without live integration markers" \
    "Fix unit and API regressions first, then rerun the diagnostics." \
    check_pytest_non_live
else
  skip_check \
    "Pytest passes without live integration markers" \
    "Skipped because no Python interpreter was found."
fi

section "Frontend"

if have_cmd npm; then
  if [ -d frontend/node_modules ]; then
    run_check \
      "Frontend unit tests pass" \
      "Fix frontend test regressions or API contract drift in the mocked UI layer." \
      check_frontend_unit
  else
    skip_check \
      "Frontend unit tests pass" \
      'Skipped because frontend dependencies are not installed. Run `npm install --prefix frontend`.'
  fi
else
  skip_check \
    "Frontend unit tests pass" \
    'Skipped because `npm` is not installed.'
fi

if [ "$WITH_E2E" -eq 1 ]; then
  if have_cmd npm && [ -d frontend/node_modules ]; then
    run_check \
      "Playwright E2E tests pass" \
      "Fix static UI regressions or install the Playwright browser bundle before rerunning." \
      check_frontend_e2e
  else
    skip_check \
      "Playwright E2E tests pass" \
      "Skipped because frontend dependencies are not installed."
  fi
else
  skip_check \
    "Playwright E2E tests pass" \
    'Opt in with `--with-e2e`.'
fi

section "Runtime Smoke"

if have_cmd curl && [ -n "$PYTHON_BIN" ]; then
  if start_stack_if_requested >/dev/null 2>&1; then
    run_check \
      "Proxy /readyz responds" \
      "Inspect api/proxy startup logs and dependency wiring until the readiness endpoint responds." \
      check_runtime_healthz
    run_check \
      "CLI doctor reports ready runtime" \
      'Fix runtime dependency wiring until `python -m mcp_evidencebase --doctor` returns a ready report.' \
      check_runtime_cli_doctor
    run_check \
      "API bucket listing responds" \
      "Check MinIO connectivity and API service dependency wiring." \
      check_runtime_bucket_list
  else
    skip_check \
      "Proxy /readyz responds" \
      'Skipped because the stack is not reachable. Start it first or rerun with `--start-stack`.'
    skip_check \
      "CLI doctor reports ready runtime" \
      "Skipped because runtime smoke checks were not started."
    skip_check \
      "API bucket listing responds" \
      "Skipped because runtime smoke checks were not started."
  fi
else
  skip_check \
    "Proxy /readyz responds" \
    'Skipped because `curl` or Python is unavailable.'
  skip_check \
    "CLI doctor reports ready runtime" \
    'Skipped because `curl` or Python is unavailable.'
  skip_check \
    "API bucket listing responds" \
    'Skipped because `curl` or Python is unavailable.'
fi

section "Live Integration"

if [ "$WITH_LIVE" -eq 1 ]; then
  if [ -n "$PYTHON_BIN" ]; then
    run_check \
      "Live MinIO/Redis/Qdrant integration tests pass" \
      "Validate external datastore credentials, network routing, and collection/key lifecycle until the live suite passes." \
      check_live_integration
  else
    skip_check \
      "Live MinIO/Redis/Qdrant integration tests pass" \
      "Skipped because no Python interpreter was found."
  fi
else
  skip_check \
    "Live MinIO/Redis/Qdrant integration tests pass" \
    'Opt in with `--with-live`.'
fi

print_summary
