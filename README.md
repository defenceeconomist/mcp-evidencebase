<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">Evidence Base</h3>

  <p align="center">
    Evidence ingestion, metadata management, and semantic retrieval over MinIO with externally managed Redis and Qdrant.
    <br />
    <a href="https://github.com/defenceeconomist/mcp-evidencebase"><strong>Explore the repo »</strong></a>
    <br />
    <br />
    <a href="https://github.com/defenceeconomist/mcp-evidencebase/issues/new?labels=bug">Report Bug</a>
    ·
    <a href="https://github.com/defenceeconomist/mcp-evidencebase/issues/new?labels=enhancement">Request Feature</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#project-structure">Project Structure</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

## About The Project

Evidence Base is a Python-first stack for:

- bucket and document management through a FastAPI service,
- asynchronous ingestion and processing via Celery,
- partition and metadata state in an external Redis deployment,
- embedding/chunk retrieval in an external Qdrant deployment,
- unified operations UI/docs behind an NGINX proxy.

The repository includes local Docker orchestration, Sphinx documentation, API/CLI examples, and standalone test reporting with grouped summaries and coverage.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

- [Python](https://www.python.org/)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Celery](https://docs.celeryq.dev/)
- [MinIO](https://min.io/)
- [Redis](https://redis.io/)
- [Qdrant](https://qdrant.tech/)
- [Sphinx](https://www.sphinx-doc.org/)
- [Docker Compose](https://docs.docker.com/compose/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Getting Started

### Prerequisites

- Python 3.10+
- Docker + Docker Compose
- Optional: `jq` for easier API response filtering

### Installation

1. Clone the repo.
   ```bash
   git clone https://github.com/defenceeconomist/mcp-evidencebase.git
   cd mcp-evidencebase
   ```
2. Create a virtual environment.
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
3. Install development dependencies.
   ```bash
   python -m pip install -e ".[dev]"
   ```
4. Copy the environment template and set the external datastore endpoints.
   ```bash
   cp .env.example .env
   ```
5. Verify local tooling.
   ```bash
   ruff check .
   mypy .
   pytest
   ```

### UI E2E Test (Playwright)

Run the frontend Playwright test:

```bash
cd frontend
npm install
npx playwright install chromium
npm run test:e2e
```

The test serves the static UI and mocks `/api/*` responses, so it does not require the full Docker stack.

Optional live datastore integration tests (MinIO/Redis/Qdrant):

```bash
MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION=1 \
pytest -m integration_live tests/test_live_datastores_integration.py
```

Configure live endpoints with `MCP_EVIDENCEBASE_LIVE_*` variables (or fall back to
`MINIO_ENDPOINT`, `REDIS_URL`, and `QDRANT_URL`).

Runtime perf harness:

```bash
./scripts/run_runtime_perf_harness.sh
```

This runs the fake-backed runtime regression slice by default and, when
`MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION=1` is set, also runs the live staged-search
and document-processing perf smoke tests against MinIO/Redis/Qdrant.

Wave 1 stabilization workflow:

```bash
# 1) Keep the full suite green.
pytest -q

# 2) Run the targeted runtime regression harness.
./scripts/run_runtime_perf_harness.sh

# 3) Capture live baseline timings when the shared datastores are available.
MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION=1 ./scripts/run_runtime_perf_harness.sh
```

The live perf slice runs with `pytest -s` and prints machine-readable `RUNTIME_PERF`
lines. Keep the first successful live run for a branch as the acceptance baseline for
future Wave 1 comparisons.

Run live integration tests inside Docker Compose (recommended, same network as
MinIO/Redis/Qdrant):

```bash
# 1) Start the shared datastores stack in a separate folder.
cd /Users/lukeheley/Developer/shared-datastores
docker compose up -d

# 2) Run pytest in a one-off api container on the compose network.
#    Use the shared datastore hostnames for MinIO, Redis, and Qdrant.
cd /Users/lukeheley/Developer/mcp-evidencebase
docker compose run --rm --no-deps \
  -v "$PWD:/app" \
  -e MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION=1 \
  -e MCP_EVIDENCEBASE_LIVE_MINIO_ENDPOINT=minio:9000 \
  -e MCP_EVIDENCEBASE_LIVE_REDIS_URL=redis://redis:6379/2 \
  -e MCP_EVIDENCEBASE_LIVE_QDRANT_URL=http://qdrant:6333 \
  api sh -lc 'python -m pip install -e ".[dev]" && pytest -m integration_live tests/test_live_datastores_integration.py'
```

This uses shared `minio`, `redis`, and `qdrant` services from the external
`shared-datastores` network without requiring repo-owned datastore containers.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Usage

### CLI

Run the package healthcheck:

```bash
python -m mcp_evidencebase --healthcheck
# ok
```

Run the full dependency preflight:

```bash
python -m mcp_evidencebase --doctor
```

`--healthcheck` returns only `ok` or `error`. `--doctor` prints structured
component status for MinIO, Redis, Qdrant, the Celery broker, and the Celery
result backend.

Run hybrid search from the CLI:

```bash
python -m mcp_evidencebase \
  --search-bucket offsets \
  --search-query "definition of offsets" \
  --search-mode hybrid \
  --search-limit 5 \
  --search-rrf-k 80
```

If you run this on your host, the configured datastore endpoints must also be host
reachable. When your `.env` uses Docker-only hostnames such as `minio`, `redis`, or
`qdrant`, run the CLI inside the Compose network instead.

Run CLI search inside the Compose network:

```bash
# Start shared datastores once.
cd /Users/lukeheley/Developer/shared-datastores
docker compose up -d

# Run CLI in a one-off api container on the same Docker network.
cd /Users/lukeheley/Developer/mcp-evidencebase
docker compose run --rm --no-deps \
  -v "$PWD:/app" \
  api python -m mcp_evidencebase \
    --search-bucket offsets \
    --search-query "definition of offsets" \
    --search-mode hybrid \
    --search-limit 5 \
    --search-rrf-k 80
```

Run CLI search on host (alternative):

```bash
export QDRANT_URL=http://localhost:52180
python -m mcp_evidencebase \
  --search-bucket offsets \
  --search-query "definition of offsets" \
  --search-mode hybrid \
  --search-limit 5 \
  --search-rrf-k 80
```

The host-based command above works through the NGINX proxy and does not require
direct `localhost:6333` access.

### MCP Server

Evidence Base also ships a local stdio MCP server for direct attachment from
Codex and VS Code.

Install the package into the repo virtualenv first:

```bash
python -m pip install -e ".[dev]"
```

Run the server directly:

```bash
mcp-evidencebase-mcp
```

Module fallback:

```bash
python -m mcp_evidencebase.mcp_server
```

The v1 MCP surface is intentionally read-only and exposes:

- `healthcheck`
- `list_buckets`
- `list_documents`
- `search_collection`
- `list_document_sections`
- `get_document_section`
- `get_metadata_schema`

Codex attachment:

```bash
codex mcp add evidencebase -- mcp-evidencebase-mcp
```

If you want to launch it from the repo virtualenv without relying on the console
script being on `PATH`, use:

```bash
codex mcp add evidencebase -- /Users/lukeheley/Developer/mcp-evidencebase/.venv/bin/python -m mcp_evidencebase.mcp_server
```

If Codex is running on the host while MinIO, Redis, and Qdrant stay on the
Docker bridge, register the host-aware launcher instead:

```bash
codex mcp add evidencebase -- /Users/lukeheley/Developer/mcp-evidencebase/scripts/run_codex_mcp.sh
```

That launcher prefers the running `evidencebase-api` container when available,
and falls back to host-reachable `localhost` datastore endpoints otherwise.

VS Code attachment is preconfigured in `.vscode/mcp.json`.

The MCP server uses the same environment-variable contract as the CLI and API:

- `MINIO_ENDPOINT`
- `REDIS_URL`
- `QDRANT_URL`
- related `MCP_EVIDENCEBASE_REQUIRE_*` flags

When your datastore endpoints are only reachable on the Docker network, a
host-launched stdio MCP server will not be able to connect. In that case either
expose host-reachable endpoints or launch the server from an environment that
shares the datastore network.

### Docker Compose Stack

Start the full local stack:

```bash
# Copy the template and set the external datastore endpoints.
cp .env.example .env

# Start shared MinIO, Redis, RedisInsight, and Qdrant first.
cd /Users/lukeheley/Developer/shared-datastores
docker compose up -d

# Then start the Evidence Base application stack.
cd /Users/lukeheley/Developer/mcp-evidencebase
docker compose up -d --build
```

The repository now uses a single `docker-compose.yml` file. It includes both
the default image tags and the local `build:` directives, so you do not need
compose overrides for local development.

Services include:

- NGINX reverse proxy (`52180` default)
- API service
- Celery worker
- Dashboard and documentation routes served by the proxy

Optional profile:

- Cloudflare Tunnel (`cloudflared`, `--profile tunnel`) if you temporarily need
  public ingress again

External dependencies consumed from `shared-datastores`:

- MinIO + MinIO Console
- Redis + RedisInsight
- Qdrant

Readiness endpoints:

- `GET /livez`: process liveness only
- `GET /healthz`: dependency-aware readiness
- `GET /readyz`: dependency-aware readiness alias

Start stack:

```bash
# Shared datastores must already be running.
docker compose up -d
```

If you explicitly enable the legacy tunnel profile:

```bash
docker compose --profile tunnel up -d
```

`CLOUDFLARE_TUNNEL_TOKEN` is only required when starting the optional
`cloudflared` profile.

### Prebuilt Image Stack

Use the same `docker-compose.yml` when deploying with prebuilt images. Set the
image tags in the environment and start the stack without `--build`:

```text
APP_IMAGE=ghcr.io/your-org/mcp-evidencebase-app:latest
PROXY_IMAGE=ghcr.io/your-org/mcp-evidencebase-proxy:latest
SHARED_DATASTORE_NETWORK_NAME=shared-datastores
```

Set those variables in your deployment environment before starting the stack:

```bash
docker compose up -d
```

### Service URLs

All services are proxied through one host port (`${PROXY_PORT:-52180}`):

```text
http://localhost:52180
http://localhost:52180/docs/
http://localhost:52180/docs/readme.html
http://localhost:52180/docs/reference.html
http://localhost:52180/minio/
http://localhost:52180/minio-console/
http://localhost:52180/redisinsight/
http://localhost:52180/dashboard/
http://localhost:52180/api/gpt/openapi.json
http://localhost:52180/api/gpt/ping?message=hello
```

`/minio/`, `/minio-console/`, `/redisinsight/`, and `/dashboard/` require the
shared datastore stack to be running and reachable on the external
`shared-datastores` network.

Meshnet equivalents for your own devices:

```text
http://<meshnet-hostname-or-ip>:52180
http://<meshnet-hostname-or-ip>:52180/docs/
http://<meshnet-hostname-or-ip>:52180/docs/readme.html
http://<meshnet-hostname-or-ip>:52180/docs/reference.html
http://<meshnet-hostname-or-ip>:52180/minio/
http://<meshnet-hostname-or-ip>:52180/minio-console/
http://<meshnet-hostname-or-ip>:52180/redisinsight/
http://<meshnet-hostname-or-ip>:52180/dashboard/
http://<meshnet-hostname-or-ip>:52180/api/gpt/openapi.json
http://<meshnet-hostname-or-ip>:52180/api/gpt/ping?message=hello
```

Replace `<meshnet-hostname-or-ip>` with the NordVPN Meshnet hostname or Meshnet
IP of the machine running the stack.

### NordVPN Meshnet Access

Current remote access is private-only: the public `evidencebase.heley.uk`
deployment has been retired in favour of NordVPN Meshnet.

1. Join the host machine and your client devices to the same NordVPN Meshnet.
2. Start the shared datastores and local Evidence Base stack on the host.
3. Reach the proxy from another device at `http://<meshnet-hostname-or-ip>:52180`.
4. Set `GPT_ACTIONS_LINK_BASE_URL=http://<meshnet-hostname-or-ip>:52180` in
   `.env` if you want generated resolver/source links to point at the Meshnet
   host instead of `localhost`.
5. Keep in mind that hosted ChatGPT Actions cannot reach Meshnet-only or other
   private addresses. Use browsers/scripts on your own Meshnet devices, or
   reintroduce a public HTTPS endpoint if you need cloud-hosted ChatGPT access.

Example private GPT/API base over Meshnet:

```text
http://<meshnet-hostname-or-ip>:52180/api/gpt/openapi.json
http://<meshnet-hostname-or-ip>:52180/api/gpt/ping?message=hello
```

### API Workflow Examples

Set base URL:

```bash
BASE_URL="http://localhost:52180/api"
```

List buckets:

```bash
curl -sS "$BASE_URL/buckets"
```

Create bucket:

```bash
curl -sS -X POST "$BASE_URL/buckets" \
  -H "Content-Type: application/json" \
  -d '{"bucket_name":"research-raw"}'
```

Upload document and queue processing:

```bash
curl -sS -X POST \
  "$BASE_URL/collections/research-raw/documents/upload?file_name=paper.pdf" \
  -H "Content-Type: application/pdf" \
  --data-binary "@paper.pdf"
```

This upload path enqueues `partition_minio_object(..., update_meta=True)`, which kicks off the
full atomic chain (`partition -> meta -> section -> chunk -> upsert`). Crossref metadata enrichment
is attempted during the metadata stage when a high-confidence match is found.

Trigger on-demand scan:

```bash
curl -sS -X POST "$BASE_URL/collections/research-raw/scan"
```

Update document metadata:

```bash
curl -sS -X PUT "$BASE_URL/collections/research-raw/documents/<document_id>/metadata" \
  -H "Content-Type: application/json" \
  -d '{"metadata":{"title":"My Paper","author":"A. Author","year":"2025","document_type":"article"}}'
```

Fetch metadata from Crossref (DOI -> ISBN -> ISSN -> title, high-confidence only):

```bash
curl -sS -X POST "$BASE_URL/collections/research-raw/documents/<document_id>/metadata/fetch"
```

Reindex a document explicitly:

```bash
curl -sS -X POST "$BASE_URL/collections/research-raw/documents/<document_id>/reindex"
```

Recover documents stuck at `processing/upsert` with `80%` progress:

```bash
docker compose up -d --build api celery
curl -sS -X POST "$BASE_URL/collections/<bucket>/documents/<document_id>/reindex"
```

If you need to requeue several stuck documents at once:

```bash
for document_id in <doc1> <doc2> <doc3>; do
  curl -sS -X POST "$BASE_URL/collections/<bucket>/documents/$document_id/reindex"
done
```

The reindex endpoint rebuilds the stored upsert payload for the existing document and queues
`upsert_minio_object` directly. This is the preferred recovery path when metadata edits or a slow
Qdrant write leave a document stuck in the final ingestion stage.

Run hybrid search:

```bash
curl -sS "$BASE_URL/collections/research-raw/search?query=causal%20inference&mode=hybrid&limit=5&rrf_k=80"
```

Delete document:

```bash
curl -sS -X DELETE "$BASE_URL/collections/research-raw/documents/<document_id>"
```

Delete bucket:

```bash
curl -sS -X DELETE "$BASE_URL/buckets/research-raw"
```

### Private GPT/API Access (API Key + Bearer)

Use this for GPT-facing endpoints from your own Meshnet-connected devices.

1. Set API key and link base in `.env`:
   ```bash
   GPT_ACTIONS_API_KEY=<your-api-key>
   GPT_ACTIONS_LINK_BASE_URL=http://<meshnet-hostname-or-ip>:52180
   ```
2. Start the local stack:
   ```bash
   docker compose up -d
   ```
3. Verify ping from another Meshnet device:
   ```bash
   curl -sS -H "Authorization: Bearer <your-api-key>" \
     "http://<meshnet-hostname-or-ip>:52180/api/gpt/ping?message=hello"
   ```
4. Fetch the schema from the same private base if needed:
   ```bash
   curl -sS -H "Authorization: Bearer <your-api-key>" \
     "http://<meshnet-hostname-or-ip>:52180/api/gpt/openapi.json"
   ```

The GPT-facing ping endpoint is `GET /api/gpt/ping` and requires API key over
Bearer auth.

Hosted ChatGPT custom Actions require a public HTTPS URL and will not be able to
reach a Meshnet-only/private endpoint.

Search wrapper for private GPT/API clients:

```bash
curl -sS -X POST "http://<meshnet-hostname-or-ip>:52180/api/gpt/search" \
  -H "Authorization: Bearer <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{"bucket_name":"research-raw","query":"causal inference","mode":"hybrid","limit":5,"rrf_k":80}'
```

`bucket_name` is optional for GPT search. If exactly one bucket exists, it is auto-selected.
`minimal_response` defaults to `true` to keep GPT payloads small; set `minimal_response=false`
when you need full retrieval diagnostics and expanded section fields.

#### GPT Search: Staged Retrieval Behaviour

`POST /api/gpt/search` uses staged retrieval by default (`use_staged_retrieval=true`).

When enabled, retrieval runs as:

1. Query variant expansion (`query_variant_limit`, default `6`, clamped to `3..8`)
2. Wide chunk recall per variant (`wide_limit_per_variant`, default `75`, clamped to `50..100`)
3. Section grouping and shortlist (`section_shortlist_limit`, default `20`, clamped to `10..30`)
4. Section-level rerank with hard-filter boosts (country/year/programme signals extracted from query)
5. Final top `limit` section-centric results with citation anchors (`chunk_ids_used`)

When disabled (`use_staged_retrieval=false`), the endpoint falls back to one-pass
collection search (semantic/keyword/hybrid) without variant expansion, shortlist, or
section-level reranking.

Tunable staged retrieval parameters:

- `query_variant_limit`: number of generated query variants (`3..8`)
- `wide_limit_per_variant`: chunk recall depth per variant (`50..100`)
- `section_shortlist_limit`: number of section groups kept for deep scoring (`10..30`)
- `max_section_text_chars`: per-result section text cap in full response mode (`250..12000`)

Response behavior:

- `minimal_response=true` (default): compact payload for lower token usage
- `minimal_response=false`: includes `query_variants`, `hard_filters`, `stage_stats`, and `citations`
- `minimal_result_text_chars` controls snippet truncation for minimal mode (default `500`, clamped to `25..2000`)

Example with staged retrieval enabled (default):

```bash
curl -sS -X POST "http://<meshnet-hostname-or-ip>:52180/api/gpt/search" \
  -H "Authorization: Bearer <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "research-raw",
    "query": "UK offsets programme 2020",
    "mode": "hybrid",
    "limit": 5,
    "minimal_response": false
  }'
```

Example fallback to one-pass retrieval:

```bash
curl -sS -X POST "http://<meshnet-hostname-or-ip>:52180/api/gpt/search" \
  -H "Authorization: Bearer <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "research-raw",
    "query": "UK offsets programme 2020",
    "mode": "hybrid",
    "limit": 5,
    "use_staged_retrieval": false
  }'
```

### Ingestion Pipeline: Atomic Stages

The ingestion flow now uses five explicit stages that run sequentially:

1. `partition_minio_object` -> `IngestionService.partition_stage_object()`
2. `meta_minio_object` -> `IngestionService.meta_stage_object()`
3. `section_minio_object` -> `IngestionService.section_stage_object()`
4. `chunk_minio_object` -> `IngestionService.chunk_stage_object()`
5. `upsert_minio_object` -> `IngestionService.upsert_stage_object()`

`process_minio_object` is retained as a backward-compatible wrapper that runs all stages inline.

Stage details:

1. `partition`: read bytes from MinIO, compute deterministic `document_id` (SHA-256), run Unstructured partitioning, persist partition JSON in Redis (`document:<document_hash>:partition`), and persist `partition_key` in document state.
2. `meta`: extract metadata (embedded PDF title/author + DOI/ISBN/ISSN/title heuristics from partitions), persist source-scoped metadata (`meta_key`), and optionally run Crossref enrichment when `update_meta=True`.
3. `section`: derive deterministic section/chunk-section mappings from partitions and persist Redis section payload (`document:<document_hash>:sections`).
4. `chunk`: build deterministic chunk payload and persist chunk/section counters in document state.
5. `upsert`: embed chunk text and upsert vectors into bucket-specific Qdrant collections (dense + sparse vectors).

The document state now exposes both global progress (`processing_progress`) and stage-local progress (`processing_stage`, `processing_stage_progress`) so UI progress bars can reflect which stage each document is currently in.

### Celery Tasks

Defined in `src/mcp_evidencebase/tasks.py`:

- `mcp_evidencebase.ping`: worker health probe (`pong`).
- `mcp_evidencebase.scan_minio_objects(bucket_name=None, update_meta=True)`: scan one/all buckets
  and enqueue `partition_minio_object` for changed objects.
- `mcp_evidencebase.partition_minio_object(bucket_name, object_name, etag=None, update_meta=False)`:
  partition stage, then enqueues `meta_minio_object`.
- `mcp_evidencebase.meta_minio_object(partition_payload)`: metadata stage, optional Crossref metadata update, then enqueues `section_minio_object`.
- `mcp_evidencebase.section_minio_object(meta_payload)`: section mapping stage, then enqueues `chunk_minio_object`.
- `mcp_evidencebase.chunk_minio_object(section_payload)`: chunk stage, then enqueues `upsert_minio_object`.
- `mcp_evidencebase.upsert_minio_object(chunk_payload)`: vector upsert stage.
- `mcp_evidencebase.process_minio_object(bucket_name, object_name, etag=None, update_meta=False)`:
  backward-compatible wrapper that runs all five stages inline.

Beat schedule is configured in `src/mcp_evidencebase/celery_app.py`. To execute scheduled scans,
run a beat process (or worker with beat enabled). The current API endpoint
`POST /collections/{bucket}/scan` can always trigger scans on demand.

Operational guidance:

- Re-run partitioning when source bytes/ETag change.
- Re-run metadata enrichment independently when metadata heuristics or Crossref behavior changes.
- Re-run section/chunk/upsert stages when chunking strategy changes and partition payload is still valid.
- Keep stage payloads deterministic so retries remain idempotent.

### Documentation

Build docs locally:

```bash
docs/build_docs.sh
```

### Test Reporting

Generate standalone test artifacts locally:

```bash
tests/build_test_reports.sh
```

The test report bundle includes:

- grouped tests by key areas,
- per-test commentary (what is validated and expected result),
- module and total line coverage,
- full HTML report in `build/test-reports/report.html`,
- summary in `build/test-reports/summary.md`.

### Environment Overrides

```bash
PROXY_PORT=52180
SHARED_DATASTORE_NETWORK_NAME=shared-datastores
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=minio:9000
MINIO_SECURE=false
MINIO_REGION=
# External datastore URLs are required in `.env` or the shell environment.
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1
REDIS_URL=redis://redis:6379/2
REDIS_PREFIX=evidencebase
QDRANT_URL=http://qdrant:6333
QDRANT_API_KEY=
QDRANT_COLLECTION_PREFIX=evidencebase
QDRANT_TIMEOUT_SECONDS=30
MCP_EVIDENCEBASE_REQUIRE_MINIO=true
MCP_EVIDENCEBASE_REQUIRE_REDIS=true
MCP_EVIDENCEBASE_REQUIRE_QDRANT=true
MCP_EVIDENCEBASE_REQUIRE_CELERY_BROKER=true
MCP_EVIDENCEBASE_REQUIRE_CELERY_RESULT_BACKEND=true
UNSTRUCTURED_API_URL=https://api.unstructuredapp.io/general/v0/general
UNSTRUCTURED_API_KEY=<your-unstructured-api-key>
UNSTRUCTURED_STRATEGY=hi_res
# For very large PDFs, raise this further (for example 1200-1800) or set UNSTRUCTURED_STRATEGY=fast.
UNSTRUCTURED_TIMEOUT_SECONDS=900
FASTEMBED_MODEL=BAAI/bge-small-en-v1.5
FASTEMBED_KEYWORD_MODEL=Qdrant/bm25
HF_HOME=/model-cache/huggingface
FASTEMBED_CACHE_PATH=/model-cache/fastembed
CHUNK_SIZE_CHARS=3000
CHUNK_OVERLAP_CHARS=0
CHUNKING_STRATEGY=by_title
CHUNK_NEW_AFTER_N_CHARS=2000
CHUNK_COMBINE_TEXT_UNDER_N_CHARS=500
CHUNK_EXCLUDE_ELEMENT_TYPES=header,footer,pageheader,pagefooter,page-header,page-footer,uncategorizedtext,uncategorized_text
CHUNK_INCLUDE_TITLE_TEXT=false
CHUNK_IMAGE_TEXT_MODE=placeholder
CHUNK_PARAGRAPH_BREAK_STRATEGY=text
CHUNK_PRESERVE_PAGE_BREAKS=true
MINIO_SCAN_INTERVAL_SECONDS=15
# Only required when explicitly starting docker compose with --profile tunnel.
CLOUDFLARE_TUNNEL_TOKEN=
GPT_ACTIONS_API_KEY=<your-api-key>
GPT_ACTIONS_LINK_BASE_URL=http://localhost:52180
```

The checked-in template is `.env.example`. If you use a different
external Docker network or hostnames, update `SHARED_DATASTORE_NETWORK_NAME`,
`MINIO_ENDPOINT`, `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND`, `REDIS_URL`,
and `QDRANT_URL` together. Configure `MINIO_SERVER_URL` and
`MINIO_BROWSER_REDIRECT_URL` in the shared datastore stack instead.

### Data Model Snapshot (Redis + Qdrant)

`document_id` is the SHA-256 hash of file bytes.

Redis keys (prefix default: `evidencebase`):

- `document:<document_hash>` processing state/progress hash
- `document:<document_hash>:sources` source locations set (`bucket/object`)
- `document:<document_hash>:partition` partition payload JSON (`partition_key` stored in state hash)
- `document:<document_hash>:sections` section/chunk mapping payload (`sections`, `chunk_sections`, `partition_key`)
- `source:<bucket>/<object>` source mapping (`document_id`, `etag`, `resolver_url`)
- `source:<bucket>/<object>:meta` source-scoped normalized metadata
- `source:bucket:<bucket>` set of source locations in bucket

Qdrant point payload fields include:

- `document_id`
- `partition_key`
- `title`, `author`, `year`
- `resolver_url` (`docs://bucket/object.ext?page=`)
- `minio_location`, `chunk_index`, `chunk_id`, `chunk_type`, `text`
- `section_id`, `section_title`, `page_start`, `page_end`, `filename`
- `bounding_boxes` (chunk source coordinates + page references)
- `orig_elements` (trace records for PDF deep-linking/bbox follow-up)

Search results are hydrated from Redis section mappings (when available) to include:
`parent_section_id`, `parent_section_index`, `parent_section_title`, and `parent_section_text`.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Project Structure

```text
.
├── deploy/
│   └── nginx/
├── docker/
├── docs/
│   ├── build_docs.sh
│   └── source/
├── frontend/
├── src/
│   └── mcp_evidencebase/
└── tests/
    └── build_test_reports.sh
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Roadmap

- [x] Unified proxy-based local stack for UI/API/docs/ops services
- [x] End-to-end ingestion flow (upload -> partition -> metadata -> chunks -> vectors)
- [x] Sphinx docs with API reference and CLI vignette
- [x] Standalone test reporting with grouped areas and coverage summary
- [ ] Add automated deployment workflow for docs and services
- [ ] Implement parent-child context retrieval
- [ ] Add prompt rewriting for query refinement
- [x] Expand integration tests for live MinIO/Redis/Qdrant interactions

See open issues for additional planning: [open issues][issues-url].

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Contributing

Contributions are welcome.

1. Fork the project
2. Create your feature branch (`git checkout -b feature/my-change`)
3. Commit your changes (`git commit -m 'Add some feature'`)
4. Push to the branch (`git push origin feature/my-change`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## License

No license file is currently included in this repository.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Contact

Project repository: [https://github.com/defenceeconomist/mcp-evidencebase](https://github.com/defenceeconomist/mcp-evidencebase)

Issues: [https://github.com/defenceeconomist/mcp-evidencebase/issues](https://github.com/defenceeconomist/mcp-evidencebase/issues)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Acknowledgments

- [Best-README-Template](https://github.com/othneildrew/Best-README-Template)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Celery](https://docs.celeryq.dev/)
- [Qdrant](https://qdrant.tech/)
- [MinIO](https://min.io/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
[contributors-shield]: https://img.shields.io/github/contributors/defenceeconomist/mcp-evidencebase.svg?style=for-the-badge
[contributors-url]: https://github.com/defenceeconomist/mcp-evidencebase/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/defenceeconomist/mcp-evidencebase.svg?style=for-the-badge
[forks-url]: https://github.com/defenceeconomist/mcp-evidencebase/network/members
[stars-shield]: https://img.shields.io/github/stars/defenceeconomist/mcp-evidencebase.svg?style=for-the-badge
[stars-url]: https://github.com/defenceeconomist/mcp-evidencebase/stargazers
[issues-shield]: https://img.shields.io/github/issues/defenceeconomist/mcp-evidencebase.svg?style=for-the-badge
[issues-url]: https://github.com/defenceeconomist/mcp-evidencebase/issues
