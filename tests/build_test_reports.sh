#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

REPORT_DIR="${TEST_REPORT_DIR:-build/test-reports}"

if [ -n "${PYTHON:-}" ]; then
  PYTHON_BIN="$PYTHON"
elif [ -x "$ROOT_DIR/.venv/bin/python" ]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

if ! "$PYTHON_BIN" -m pytest --help | grep -q -- '--html-output'; then
  echo "pytest-html-plus is required: missing --html-output option" >&2
  exit 1
fi

rm -rf "$REPORT_DIR"
mkdir -p "$REPORT_DIR"

PYTHONPATH="${PYTHONPATH:-src}" "$PYTHON_BIN" -m pytest \
  --cov-report=term-missing \
  --cov-report=xml:"$REPORT_DIR/coverage.xml" \
  --html-output="$REPORT_DIR" \
  --should-open-report=never

PYTHONPATH="${PYTHONPATH:-src}" "$PYTHON_BIN" docs/scripts/render_test_summary.py \
  --report-json "$REPORT_DIR/final_report.json" \
  --coverage-xml "$REPORT_DIR/coverage.xml" \
  --tests-dir tests \
  --output "$REPORT_DIR/summary.md"
