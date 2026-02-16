# Evidence Base

Manage evidence base metadata, semantic search, and GPT summaries.

## Project layout

```text
.
├── deploy/
│   └── nginx/
│       └── default.conf
├── docker-compose.yml
├── frontend/
│   ├── Dockerfile
│   ├── index.html
│   └── package.json
├── pyproject.toml
├── src/
│   └── mcp_evidencebase/
│       ├── __init__.py
│       ├── __main__.py
│       ├── cli.py
│       └── core.py
└── tests/
    ├── test_cli.py
    └── test_core.py
```

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

Run checks:

```bash
ruff check .
mypy .
pytest
```

Run the CLI:

```bash
python -m mcp_evidencebase --healthcheck
# ok
```

## Docker compose stack

Starts:
- NGINX reverse proxy (`52180` by default)
- Frontend dashboard (via proxy)
- Bucket API service (via proxy)
- Documentation site (via proxy)
- MinIO (via proxy)
- MinIO Console (via proxy)
- Redis (internal only)
- RedisInsight (via proxy)
- Qdrant HTTP API (via proxy)
- Celery worker
- Flower (via proxy)
- Cloudflare Tunnel (optional `cloudflare` profile)

```bash
docker compose up -d --build
```

Enable Cloudflare Tunnel:

```bash
docker compose --profile cloudflare up -d cloudflared
```

The tunnel token is read from `CLOUDFLARE_TUNNEL_TOKEN` in `.env`.
If your Cloudflare public hostname currently points to `http://localhost:52180`,
set the tunnel service target to `http://proxy:80` for this compose stack.

All web services are exposed through one host port (`${PROXY_PORT:-52180}`) using
path-based routing on the main host (`localhost`) and via Cloudflare (`evidencebase.heley.uk`):

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

## Documentation

Build docs locally (includes test output from `pytest-html-plus`):

```bash
docs/build_docs.sh
```

Optional environment overrides:

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
FASTEMBED_MODEL=BAAI/bge-small-en-v1.5
CHUNK_SIZE_CHARS=1200
CHUNK_OVERLAP_CHARS=150
MINIO_SCAN_INTERVAL_SECONDS=15
CLOUDFLARE_TUNNEL_TOKEN=<your-cloudflare-tunnel-token>
```

## Bucket API command-line examples

The UI calls the same API shown below. These are copy/paste commands.

Set the API base URL:

```bash
BASE_URL="http://localhost:52180/api"
```

List buckets:

```bash
curl -sS "$BASE_URL/buckets"
```

Add bucket `research-raw`:

```bash
curl -sS -X POST "$BASE_URL/buckets" \
  -H "Content-Type: application/json" \
  -d '{"bucket_name":"research-raw"}'
```

Expected response when created:

```json
{"bucket_name":"research-raw","created":true,"qdrant_collection_created":true}
```

Expected response when it already exists:

```json
{"bucket_name":"research-raw","created":false,"qdrant_collection_created":false}
```

Remove bucket `research-raw`:

```bash
curl -sS -X DELETE "$BASE_URL/buckets/research-raw"
```

Expected response when removed:

```json
{"bucket_name":"research-raw","removed":true,"qdrant_collection_removed":true}
```

Expected response when it does not exist:

```json
{"bucket_name":"research-raw","removed":false,"qdrant_collection_removed":false}
```

## Document ingestion API command-line examples

List documents for bucket `research-raw`:

```bash
curl -sS "$BASE_URL/collections/research-raw/documents"
```

Upload and queue processing (`document_id` is the SHA-256 hash of bytes):

```bash
curl -sS -X POST \
  "$BASE_URL/collections/research-raw/documents/upload?file_name=paper.pdf" \
  -H "Content-Type: application/pdf" \
  --data-binary "@paper.pdf"
```

Queue an on-demand bucket scan (for files dropped directly into MinIO):

```bash
curl -sS -X POST "$BASE_URL/collections/research-raw/scan"
```

Update BibTeX metadata in Redis:

```bash
curl -sS -X PUT "$BASE_URL/collections/research-raw/documents/<document_id>/metadata" \
  -H "Content-Type: application/json" \
  -d '{"metadata":{"title":"My Paper","author":"A. Author","year":"2025","document_type":"article"}}'
```

Delete a document (keeps partition payload in Redis, removes everything else):

```bash
curl -sS -X DELETE "$BASE_URL/collections/research-raw/documents/<document_id>"
```

## Redis and Qdrant Schema

`document_id` is the SHA-256 hash of file bytes and is the canonical document key.

Redis keys (prefix defaults to `evidencebase`):
- `document:<document_hash>` hash for processing state/progress (`processing_state`, `processing_progress`, `partition_key`, `meta_key`, etc.).
- `document:<document_hash>:sources` set of `bucket/object` source locations mapped to the document hash.
- `document:<document_hash>:partitions` Unstructured partition JSON payload.
- `document:partition:<partition_hash>` reverse index mapping partition hash -> document hash.
- `source:<bucket>/<object>` source mapping hash (`document_id`, `etag`, `resolver_url`, location fields).
- `source:bucket:<bucket>` set of source locations in a bucket.
- `meta:<bucket>/<object>` source-scoped normalized BibTeX metadata payload with `document_id` and `source`.

Notes:
- Redis stores partitions and metadata, but **does not store chunks**.
- Deleting a document keeps `document:<document_hash>` state and partition payload by default, and removes source/meta mappings.
- Metadata extraction attempts title/authors/DOI/ISBN; DOI extraction is intentionally limited to first-page/front-matter partitions.

Qdrant stores chunk text + embeddings. Each point payload includes:
- `document_id` (document hash)
- `partition_key` (partition hash)
- `meta_key` (metadata hash)
- `resolver_url` in the form `docs://bucket/object.ext?page=`
- `minio_location`, `chunk_index`, and `text`
