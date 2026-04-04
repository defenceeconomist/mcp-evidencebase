from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.area_core


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_proxy_config_enables_basic_auth_and_health_exceptions() -> None:
    """Default proxy config should protect app routes while leaving health probes open."""
    config_text = (_repo_root() / "deploy/nginx/default.conf").read_text(encoding="utf-8")

    assert "auth_basic ${APP_PROXY_BASIC_AUTH_REALM};" in config_text
    assert "auth_basic_user_file /etc/nginx/auth/app-users.htpasswd;" in config_text
    assert "location = /livez" in config_text
    assert "location = /healthz" in config_text
    assert "location = /readyz" in config_text
    assert config_text.count("auth_basic off;") == 3


def test_proxy_config_limits_main_api_and_removes_admin_surfaces() -> None:
    """Default proxy config should rate-limit the app API and stop exposing admin passthroughs."""
    config_text = (_repo_root() / "deploy/nginx/default.conf").read_text(encoding="utf-8")

    assert (
        "limit_req_zone $binary_remote_addr zone=app_api_read_per_ip:10m rate=120r/m;"
        in config_text
    )
    assert (
        "limit_req_zone $binary_remote_addr zone=app_api_write_per_ip:10m rate=20r/m;"
        in config_text
    )
    assert "location /api/" in config_text
    assert "/minio/" not in config_text
    assert "/minio-console/" not in config_text
    assert "/redisinsight/" not in config_text
    assert "/dashboard/" not in config_text
    assert "qdrant_upstream" not in config_text


def test_frontend_navigation_no_longer_links_to_admin_tools() -> None:
    """The shared frontend should no longer advertise proxied datastore dashboards."""
    index_text = (_repo_root() / "frontend/index.html").read_text(encoding="utf-8")

    assert "Developer Tools" not in index_text
    assert "/minio-console/" not in index_text
    assert "/redisinsight/" not in index_text
    assert "/dashboard/" not in index_text


def test_app_images_drop_root_privileges() -> None:
    """API and worker images should run as a dedicated non-root user."""
    app_dockerfile = (_repo_root() / "docker/app/Dockerfile").read_text(encoding="utf-8")
    api_dockerfile = (_repo_root() / "docker/api/Dockerfile").read_text(encoding="utf-8")

    assert "USER evidencebase" in app_dockerfile
    assert "USER evidencebase" in api_dockerfile


def test_proxy_image_uses_template_and_auth_bootstrap_envsh() -> None:
    """The proxy image should render config from a template and source auth bootstrap env."""
    proxy_dockerfile = (_repo_root() / "docker/proxy/Dockerfile").read_text(encoding="utf-8")
    envsh_text = (
        _repo_root() / "docker/proxy/04-configure-app-basic-auth.envsh"
    ).read_text(encoding="utf-8")

    assert "/etc/nginx/templates/default.conf.template" in proxy_dockerfile
    assert "04-configure-app-basic-auth.envsh" in proxy_dockerfile
    assert 'export APP_PROXY_BASIC_AUTH_REALM=\'off\'' in envsh_text
