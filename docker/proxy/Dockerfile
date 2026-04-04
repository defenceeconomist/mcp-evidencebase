FROM python:3.12-slim AS docs-builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/

RUN python -m pip install --upgrade pip && \
    python -c "import pathlib, tomllib; data = tomllib.loads(pathlib.Path('pyproject.toml').read_text()); reqs = [*data['build-system']['requires'], *data['project']['dependencies'], *data['project']['optional-dependencies']['docs']]; pathlib.Path('/tmp/requirements.txt').write_text('\n'.join(dict.fromkeys(reqs)))" && \
    python -m pip install -r /tmp/requirements.txt

COPY src /app/src
COPY tests /app/tests
COPY docs /app/docs

RUN python -m pip install --no-deps -e .

RUN ./docs/build_docs.sh

FROM nginx:1.27-alpine

RUN apk add --no-cache apache2-utils && \
    sed -i 's|application/javascript[[:space:]]*js;|application/javascript js mjs;|' /etc/nginx/mime.types

COPY deploy/nginx/default.conf /etc/nginx/templates/default.conf.template
COPY docker/proxy/04-configure-app-basic-auth.envsh /docker-entrypoint.d/04-configure-app-basic-auth.envsh
COPY frontend/index.html /usr/share/nginx/html/frontend/index.html
COPY frontend/index.css /usr/share/nginx/html/frontend/index.css
COPY frontend/index.js /usr/share/nginx/html/frontend/index.js
COPY frontend/resolver.html /usr/share/nginx/html/frontend/resolver.html
COPY frontend/resolver.css /usr/share/nginx/html/frontend/resolver.css
COPY frontend/resolver.js /usr/share/nginx/html/frontend/resolver.js
COPY frontend/js /usr/share/nginx/html/frontend/js
COPY --from=docs-builder /app/docs/site /usr/share/nginx/html/docs

RUN chmod +x /docker-entrypoint.d/04-configure-app-basic-auth.envsh
