FROM python:3.12-slim AS builder

WORKDIR /app
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1

COPY pyproject.toml README.md ./
COPY src ./src
COPY tests ./tests
COPY docs ./docs

RUN pip install --no-cache-dir -e ".[dev,docs]"
RUN ./docs/build_docs.sh

FROM nginx:1.27-alpine

COPY --from=builder /app/docs/site/ /usr/share/nginx/html/
