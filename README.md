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
    Evidence ingestion, metadata management, and semantic retrieval over MinIO, Redis, and Qdrant.
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
- partition and metadata state in Redis,
- embedding/chunk retrieval in Qdrant,
- unified operations UI/docs behind an NGINX proxy.

The repository includes local Docker orchestration, Sphinx documentation, API/CLI examples, and automated test reporting with grouped summaries and coverage.

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
4. Verify local tooling.
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

Run live integration tests inside Docker Compose (recommended, same network as
MinIO/Redis/Qdrant):

```bash
# 1) Start only required datastores (avoids cloudflared dependency).
docker compose up -d minio redis qdrant

# 2) Run pytest in a one-off api container on the compose network.
#    Use compose service hostnames for live endpoints.
docker compose run --rm --no-deps \
  -v "$PWD:/app" \
  -e MCP_EVIDENCEBASE_RUN_LIVE_INTEGRATION=1 \
  -e MCP_EVIDENCEBASE_LIVE_MINIO_ENDPOINT=minio:9000 \
  -e MCP_EVIDENCEBASE_LIVE_REDIS_URL=redis://redis:6379/2 \
  -e MCP_EVIDENCEBASE_LIVE_QDRANT_URL=http://qdrant:6333 \
  api sh -lc 'python -m pip install -e ".[dev]" && pytest -m integration_live tests/test_live_datastores_integration.py'
```

This uses service-hostnames from `docker-compose.yml` (`minio`, `redis`, `qdrant`)
without exposing datastore ports to localhost.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Usage

### CLI

Run the package healthcheck:

```bash
python -m mcp_evidencebase --healthcheck
# ok
```

Run hybrid search from the CLI:

```bash
python -m mcp_evidencebase \
  --search-bucket offsets \
  --search-query "definition of offsets" \
  --search-mode hybrid \
  --search-limit 5 \
  --search-rrf-k 80
```

If you run this on your host while using Docker Compose defaults, `connection refused`
is expected unless your datastore endpoints are reachable from the host. Compose uses
internal hostnames (`minio`, `redis`, `qdrant`) for container-to-container traffic.

Run CLI search inside the Compose network:

```bash
# Start only required services.
docker compose up -d minio redis qdrant

# Run CLI in a one-off api container on the same Docker network.
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

### Docker Compose Stack

Start the full local stack:

```bash
docker compose up -d --build
```

Services include:

- NGINX reverse proxy (`52180` default)
- Frontend dashboard
- API service
- Documentation site
- MinIO + MinIO Console
- Redis + RedisInsight
- Qdrant
- Celery worker + Flower
- Cloudflare Tunnel (`cloudflared`)

Start stack (including Cloudflare Tunnel):

```bash
docker compose up -d
```

`CLOUDFLARE_TUNNEL_TOKEN` is required because `cloudflared` always starts.

### Service URLs

All services are proxied through one host port (`${PROXY_PORT:-52180}`):

```text
http://localhost:52180
http://localhost:52180/docs/
http://localhost:52180/docs/readme.html
http://localhost:52180/docs/reference.html
http://localhost:52180/docs/tests.html
http://localhost:52180/minio-console/
http://localhost:52180/redisinsight/
http://localhost:52180/dashboard/
http://localhost:52180/flower/
http://localhost:52180/api/gpt/openapi.json
http://localhost:52180/api/gpt/ping?message=hello
```

Public equivalents currently configured:

```text
https://evidencebase.heley.uk
https://evidencebase.heley.uk/docs/
https://evidencebase.heley.uk/docs/readme.html
https://evidencebase.heley.uk/docs/reference.html
https://evidencebase.heley.uk/docs/tests.html
https://evidencebase.heley.uk/minio-console/
https://evidencebase.heley.uk/redisinsight/
https://evidencebase.heley.uk/dashboard/
https://evidencebase.heley.uk/flower/
https://evidencebase.heley.uk/api/gpt/openapi.json
https://evidencebase.heley.uk/api/gpt/ping?message=hello
```

Recommended public GPT-only hostname:

```text
https://open.heley.uk/api/gpt/openapi.json
https://open.heley.uk/api/gpt/ping?message=hello
```

### Cloudflare Split: Protected UI + Open GPT API

To keep `evidencebase.heley.uk` behind Cloudflare Access while exposing only the GPT action API:

1. In Cloudflare Zero Trust -> Networks -> Tunnels -> your named tunnel -> Public hostnames, add:
   - Hostname: `open.heley.uk`
   - Service: `http://proxy:80`
2. Keep `evidencebase.heley.uk` mapped to `http://proxy:80` as-is.
3. In Cloudflare Access -> Applications, ensure policy scope protects only `evidencebase.heley.uk/*` (not `*.heley.uk`).
4. Do not attach Access protection to `open.heley.uk`.

NGINX already restricts `open.heley.uk` to `/api/gpt/*` and returns `404` for all other paths.

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

This upload path enqueues `partition_minio_object(..., update_meta=True)`, so metadata
enrichment from Crossref is attempted before chunking when a high-confidence match is found.

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

### Custom GPT Ping Action (API Key + Bearer)

Use this when creating an Action in the ChatGPT UI.

1. Start named tunnel:
   ```bash
   docker compose up -d cloudflared
   ```
2. Set API key in `.env`:
   ```bash
   GPT_ACTIONS_API_KEY=<your-api-key>
   GPT_ACTIONS_LINK_BASE_URL=https://evidencebase.heley.uk
   ```
3. Verify ping from the `open.heley.uk` hostname:
   ```bash
   curl -sS -H "Authorization: Bearer <your-api-key>" \
     "https://open.heley.uk/api/gpt/ping?message=hello"
   ```
4. In ChatGPT -> Custom GPT -> Actions:
   - Authentication type: `API key`
   - Auth Type: `Bearer`
   - API key value: same value as `GPT_ACTIONS_API_KEY`
   - OpenAPI schema URL: `https://open.heley.uk/api/gpt/openapi.json`

The ping action exposed to ChatGPT is `GET /api/gpt/ping` and requires API key over Bearer auth.

Search wrapper exposed to ChatGPT Actions:

```bash
curl -sS -X POST "https://open.heley.uk/api/gpt/search" \
  -H "Authorization: Bearer <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{"bucket_name":"research-raw","query":"causal inference","mode":"hybrid","limit":5,"rrf_k":80}'
```

`bucket_name` is optional for GPT search. If exactly one bucket exists, it is auto-selected.

### Ingestion Pipeline: Partitioning And Chunking

The ingestion flow now uses two explicit stages that run sequentially:

1. `partition_minio_object` runs `IngestionService.partition_object()`.
2. `chunk_minio_object` runs `IngestionService.chunk_object()` after partitioning succeeds.

`process_minio_object` is retained as a backward-compatible wrapper that runs both stages inline.

Stage details:

1. Read bytes from MinIO and compute deterministic `document_id` (SHA-256).
2. Partition document with Unstructured API and store raw partition JSON in Redis under
   `document:<document_hash>:partition` (with the computed `partition_key` tracked in document state).
3. Extract metadata:
   - title/author from embedded PDF metadata (when available),
   - DOI/ISBN/ISSN from first-page partition text.
4. Optionally enrich metadata from Crossref when `update_meta=True` is provided to the task
   (upload and scan task paths enable this by default). Crossref requests use the existing shared
   limiter:
   public pool concurrency `1`, global `5 req/s`, single-record `5 req/s`, list-query `1 req/s`.
5. Chunk partitions with structure-aware deterministic chunking (`chunk_unstructured_elements`):
   - title-aware: `Title` elements create hard section boundaries and update `section_title`,
   - element-first: full elements are appended in order and joined with `\n\n` (semantic boundaries kept),
   - table-safe: `Table` elements are emitted as standalone `type="table"` chunks,
   - size controls:
     - hard cap: `max_characters` (wired from `CHUNK_SIZE_CHARS`),
     - soft split target: `new_after_n_chars` (defaults to 1500, clamped to hard cap),
     - undersized merge threshold: `combine_under_n_chars` (default 500),
     - overlap: `overlap_chars` (wired from `CHUNK_OVERLAP_CHARS`) only for internal splits of one oversized element,
   - deterministic IDs and traces:
     - each chunk has deterministic `chunk_id`,
     - each chunk keeps `metadata` (`page_start`, `page_end`, `section_title`, plus `source_id`/`filename` when present),
     - each chunk keeps `orig_elements` trace records (`element_id`, `type/category`, `page_number`,
       `coordinates`, `text_len`),
     - compatibility fields `chunk_index`, `page_numbers`, and `bounding_boxes` are retained.
6. Embed chunks and upsert vectors into bucket-specific Qdrant collection:
   - dense semantic vector (`FASTEMBED_MODEL`),
   - sparse keyword vector (`FASTEMBED_KEYWORD_MODEL`, default `Qdrant/bm25`).
7. Hybrid search fuses semantic and keyword ranks using weighted reciprocal rank fusion.

Chunking is internal to `IngestionService` and does not call Unstructured APIs. This allows
future chunking strategy changes without changing the partition stage.

### Celery Tasks

Defined in `src/mcp_evidencebase/tasks.py`:

- `mcp_evidencebase.ping`: worker health probe (`pong`).
- `mcp_evidencebase.scan_minio_objects(bucket_name=None, update_meta=True)`: scan one/all buckets
  and enqueue `partition_minio_object` for changed objects.
- `mcp_evidencebase.partition_minio_object(bucket_name, object_name, etag=None, update_meta=False)`:
  partition + metadata stage, optional Crossref metadata update, then enqueues `chunk_minio_object`.
- `mcp_evidencebase.chunk_minio_object(partition_payload)`: chunk + vector upsert stage.
- `mcp_evidencebase.process_minio_object(bucket_name, object_name, etag=None, update_meta=False)`:
  backward-compatible wrapper that runs both stages inline.

Beat schedule is configured in `src/mcp_evidencebase/celery_app.py`. To execute scheduled scans,
run a beat process (or worker with beat enabled). The current API endpoint
`POST /collections/{bucket}/scan` can always trigger scans on demand.

Operational guidance:

- Re-run partitioning when source bytes/ETag change.
- Re-run chunking/indexing when only chunking strategy changes and partition payload is still valid.
- Keep partition and chunk stage payloads deterministic so retries remain idempotent.

### Documentation And Test Reporting

Build docs locally (includes full test run, grouped summary, and coverage):

```bash
docs/build_docs.sh
```

The docs test page includes:

- grouped tests by key areas,
- per-test commentary (what is validated and expected result),
- module and total line coverage,
- full HTML test report embed.

### Environment Overrides

```bash
PROXY_PORT=52180
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_SERVER_URL=http://127.0.0.1:9000
MINIO_BROWSER_REDIRECT_URL=http://localhost:52180/minio-console
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1
REDIS_URL=redis://redis:6379/2
REDIS_PREFIX=evidencebase
QDRANT_URL=http://qdrant:6333
QDRANT_COLLECTION_PREFIX=evidencebase
UNSTRUCTURED_API_URL=https://api.unstructuredapp.io/general/v0/general
UNSTRUCTURED_API_KEY=<your-unstructured-api-key>
UNSTRUCTURED_STRATEGY=auto
UNSTRUCTURED_TIMEOUT_SECONDS=300
FASTEMBED_MODEL=BAAI/bge-small-en-v1.5
FASTEMBED_KEYWORD_MODEL=Qdrant/bm25
HF_HOME=/model-cache/huggingface
FASTEMBED_CACHE_PATH=/model-cache/fastembed
CHUNK_SIZE_CHARS=1200
CHUNK_OVERLAP_CHARS=150
MINIO_SCAN_INTERVAL_SECONDS=15
# Required because cloudflared starts with docker compose up.
CLOUDFLARE_TUNNEL_TOKEN=<your-cloudflare-tunnel-token>
GPT_ACTIONS_API_KEY=<your-api-key>
GPT_ACTIONS_LINK_BASE_URL=https://evidencebase.heley.uk
```

### Data Model Snapshot (Redis + Qdrant)

`document_id` is the SHA-256 hash of file bytes.

Redis keys (prefix default: `evidencebase`):

- `document:<document_hash>` processing state/progress hash
- `document:<document_hash>:sources` source locations set (`bucket/object`)
- `document:<document_hash>:partition` partition payload JSON (`partition_key` stored in state hash)
- `source:<bucket>/<object>` source mapping (`document_id`, `etag`, `resolver_url`)
- `source:<bucket>/<object>:meta` source-scoped normalized metadata
- `source:bucket:<bucket>` set of source locations in bucket

Qdrant point payload fields include:

- `document_id`
- `partition_key`
- `meta_key`
- `resolver_url` (`docs://bucket/object.ext?page=`)
- `minio_location`, `chunk_index`, `chunk_id`, `chunk_type`, `text`
- `section_title`, `page_start`, `page_end`
- `source_id`, `filename`
- `page_numbers` (chunk source pages)
- `bounding_boxes` (chunk source coordinates + page references)
- `orig_elements` (trace records for PDF deep-linking/bbox follow-up)

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
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Roadmap

- [x] Unified proxy-based local stack for UI/API/docs/ops services
- [x] End-to-end ingestion flow (upload -> partition -> metadata -> chunks -> vectors)
- [x] Sphinx docs with API reference and CLI vignette
- [x] Test reporting with grouped areas and coverage summary in docs
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
