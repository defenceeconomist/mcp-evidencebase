#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p docs/source/_static/tests

if pytest --help | grep -q -- '--html-output'; then
  PYTHONPATH="${PYTHONPATH:-src}" pytest --html-output=docs/source/_static/tests
elif pytest --help | grep -q -- '--html='; then
  PYTHONPATH="${PYTHONPATH:-src}" pytest --html=docs/source/_static/tests/index.html --self-contained-html
else
  echo "pytest-html-plus/pytest-html is required: missing --html-output/--html option" >&2
  exit 1
fi

PYTHONPATH="${PYTHONPATH:-src}" python docs/scripts/render_test_summary.py

sphinx-build -b html docs/source docs/site
