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
  --search-bucket research-raw \
  --search-query "causal inference in healthcare" \
  --search-mode hybrid \
  --search-limit 5 \
  --search-rrf-k 80
```

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
- Optional Cloudflare Tunnel profile

Enable Cloudflare Tunnel:

```bash
docker compose --profile cloudflare up -d cloudflared
```

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

### Ingestion Pipeline: Partitioning And Chunking

The ingestion flow now uses two explicit stages that run sequentially:

1. `partition_minio_object` runs `IngestionService.partition_object()`.
2. `chunk_minio_object` runs `IngestionService.chunk_object()` after partitioning succeeds.

`process_minio_object` is retained as a backward-compatible wrapper that runs both stages inline.

Stage details:

1. Read bytes from MinIO and compute deterministic `document_id` (SHA-256).
2. Partition document with Unstructured API and store raw partition JSON in Redis under
   `partition:<partition_hash>`.
3. Extract metadata:
   - title/author from embedded PDF metadata (when available),
   - DOI/ISBN from first-page partition text.
4. Chunk partitions with structure-aware deterministic chunking (`chunk_unstructured_elements`):
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
5. Embed chunks and upsert vectors into bucket-specific Qdrant collection:
   - dense semantic vector (`FASTEMBED_MODEL`),
   - sparse keyword vector (`FASTEMBED_KEYWORD_MODEL`, default `Qdrant/bm25`).
6. Hybrid search fuses semantic and keyword ranks using weighted reciprocal rank fusion.

Chunking is internal to `IngestionService` and does not call Unstructured APIs. This allows
future chunking strategy changes without changing the partition stage.

### Celery Tasks

Defined in `src/mcp_evidencebase/tasks.py`:

- `mcp_evidencebase.ping`: worker health probe (`pong`).
- `mcp_evidencebase.scan_minio_objects(bucket_name=None)`: scan one/all buckets and enqueue
  `partition_minio_object` for changed objects.
- `mcp_evidencebase.partition_minio_object(bucket_name, object_name, etag=None)`: partition +
  metadata stage, then enqueues `chunk_minio_object`.
- `mcp_evidencebase.chunk_minio_object(partition_payload)`: chunk + vector upsert stage.
- `mcp_evidencebase.process_minio_object(bucket_name, object_name, etag=None)`: backward-compatible
  wrapper that runs both stages inline.

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
CLOUDFLARE_TUNNEL_TOKEN=<your-cloudflare-tunnel-token>
```

### Data Model Snapshot (Redis + Qdrant)

`document_id` is the SHA-256 hash of file bytes.

Redis keys (prefix default: `evidencebase`):

- `document:<document_hash>` processing state/progress hash
- `document:<document_hash>:sources` source locations set (`bucket/object`)
- `partition:<partition_hash>` partition payload JSON
- `source:<bucket>/<object>` source mapping (`document_id`, `etag`, `resolver_url`)
- `source:bucket:<bucket>` set of source locations in bucket
- `meta:<bucket>/<object>` source-scoped normalized metadata

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
- [ ] Expand integration tests for live MinIO/Redis/Qdrant interactions

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
