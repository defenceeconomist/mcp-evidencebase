FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/

RUN python -m pip install --upgrade pip && \
    python -c "import pathlib, tomllib; data = tomllib.loads(pathlib.Path('pyproject.toml').read_text()); reqs = [*data['build-system']['requires'], *data['project']['dependencies']]; pathlib.Path('/tmp/requirements.txt').write_text('\n'.join(dict.fromkeys(reqs)))" && \
    python -m pip install -r /tmp/requirements.txt

COPY src /app/src

RUN python -m pip install --no-deps -e .

CMD ["uvicorn", "mcp_evidencebase.api:app", "--host", "0.0.0.0", "--port", "8000"]
