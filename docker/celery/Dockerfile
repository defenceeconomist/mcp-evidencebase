FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src

RUN python -m pip install --upgrade pip && \
    python -m pip install -e . && \
    python -m pip install "flower>=2.0.1"

CMD ["celery", "-A", "mcp_evidencebase.celery_app", "worker", "--loglevel=info"]
