# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade pip && \
    python -c "import pathlib, tomllib; data = tomllib.loads(pathlib.Path('pyproject.toml').read_text()); reqs = [*data['build-system']['requires'], *data['project']['dependencies']]; pathlib.Path('/tmp/requirements.txt').write_text('\n'.join(dict.fromkeys(reqs)))" && \
    python -m pip install -r /tmp/requirements.txt

COPY src /app/src

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --no-deps -e .

CMD ["celery", "-A", "mcp_evidencebase.celery_app", "worker", "--loglevel=info"]
