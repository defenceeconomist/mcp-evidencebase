# mcp-evidencebase

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
- MinIO (via proxy)
- MinIO Console (via proxy)
- Redis (internal only)
- RedisInsight (via proxy)
- Qdrant HTTP API (via proxy)
- Celery worker
- Flower (via proxy)

```bash
docker compose up -d --build
```

All web services are exposed through one host port (`${PROXY_PORT:-52180}`) using
path-based routing on the main host:

```text
http://evidencebase.localhost:52180
http://evidencebase.localhost:52180/minio-console/
http://evidencebase.localhost:52180/redisinsight/
http://evidencebase.localhost:52180/dashboard/
http://evidencebase.localhost:52180/flower/
```

External machine access:
- `.localhost` only resolves on the local machine.
- From another device, use the Docker host name or IP directly with the same paths.

```text
http://luke-mac-mini.local:52180/minio-console/
http://luke-mac-mini.local:52180/redisinsight/
http://luke-mac-mini.local:52180/dashboard/
http://luke-mac-mini.local:52180/flower/
```

Optional environment overrides:

```bash
PROXY_PORT=52180
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_SERVER_URL=http://evidencebase.localhost:52180/minio
MINIO_BROWSER_REDIRECT_URL=http://evidencebase.localhost:52180/minio-console
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1
```
