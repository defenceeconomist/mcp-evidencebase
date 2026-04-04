#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -n "${PYTHON:-}" ]; then
  PYTHON_BIN="$PYTHON"
elif [ -x "$ROOT_DIR/.venv/bin/python" ]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

FAKE_BACKED_EXPR='cached_ingestion_service or qdrant_upsert_batches_large_documents_into_fixed_size_writes or qdrant_variant_search_embeds_queries_once_per_embedder or perf_stats_record_search_documents_invocations or qdrant_indexer_hybrid_search_merges_dense_and_keyword_results or qdrant_upsert_skips_image_chunks_for_embedding or qdrant_payload_contains_document_partition_meta_and_resolver_keys or ingestion_service_search_documents'

API_EXPR='staged_retrieval_returns_section_citations'

echo "== Fake-backed runtime perf regression slice =="
"$PYTHON_BIN" -m pytest -q tests/test_tasks.py
"$PYTHON_BIN" -m pytest -q tests/test_api.py -k "$API_EXPR"
"$PYTHON_BIN" -m pytest -q tests/test_ingestion.py -k "$FAKE_BACKED_EXPR"

if [ "${MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION:-0}" = "1" ]; then
  echo "== Live runtime perf smoke slice =="
  "$PYTHON_BIN" -m pytest -q -s tests/test_live_datastores_integration.py -k 'perf_smoke'
else
  echo "Skipping live runtime perf smoke slice. Set MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION=1 to enable it."
fi
