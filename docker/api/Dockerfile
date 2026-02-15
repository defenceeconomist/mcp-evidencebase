FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src

RUN python -m pip install --upgrade pip && \
    python -m pip install -e .

CMD ["uvicorn", "mcp_evidencebase.api:app", "--host", "0.0.0.0", "--port", "8000"]
