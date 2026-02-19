FROM nginx:1.27-alpine

# Serve ES modules with a JavaScript MIME type.
RUN sed -i 's|application/javascript[[:space:]]*js;|application/javascript js mjs;|' /etc/nginx/mime.types

COPY index.html /usr/share/nginx/html/index.html
COPY index.css /usr/share/nginx/html/index.css
COPY index.js /usr/share/nginx/html/index.js
COPY js /usr/share/nginx/html/js
COPY resolver.html /usr/share/nginx/html/resolver.html
COPY resolver.css /usr/share/nginx/html/resolver.css
COPY resolver.js /usr/share/nginx/html/resolver.js
