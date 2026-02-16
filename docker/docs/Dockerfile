# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder

WORKDIR /app
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1

COPY pyproject.toml README.md ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    python -c "import pathlib, tomllib; data = tomllib.loads(pathlib.Path('pyproject.toml').read_text()); reqs = [*data['build-system']['requires'], *data['project']['dependencies'], *data['project']['optional-dependencies']['dev'], *data['project']['optional-dependencies']['docs']]; pathlib.Path('/tmp/requirements.txt').write_text('\n'.join(dict.fromkeys(reqs)))" && \
    pip install -r /tmp/requirements.txt

COPY src ./src
COPY tests ./tests
COPY docs ./docs

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-deps -e .
RUN ./docs/build_docs.sh

FROM nginx:1.27-alpine

COPY --from=builder /app/docs/site/ /usr/share/nginx/html/
