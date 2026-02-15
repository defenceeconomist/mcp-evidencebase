from __future__ import annotations

from mcp_evidencebase.minio_settings import build_minio_settings, to_bool


def test_to_bool_accepts_common_truthy_values() -> None:
    assert to_bool("true") is True
    assert to_bool("Yes") is True
    assert to_bool(" 1 ") is True


def test_to_bool_returns_false_for_none_or_unknown_values() -> None:
    assert to_bool(None) is False
    assert to_bool("nope") is False


def test_build_minio_settings_uses_defaults() -> None:
    settings = build_minio_settings({})

    assert settings.endpoint == "minio:9000"
    assert settings.access_key == "minioadmin"
    assert settings.secret_key == "minioadmin"
    assert settings.secure is False
    assert settings.region is None


def test_build_minio_settings_reads_env_values() -> None:
    settings = build_minio_settings(
        {
            "MINIO_ENDPOINT": "localhost:9001",
            "MINIO_ROOT_USER": "user1",
            "MINIO_ROOT_PASSWORD": "pass1",
            "MINIO_SECURE": "true",
            "MINIO_REGION": "us-east-1",
        }
    )

    assert settings.endpoint == "localhost:9001"
    assert settings.access_key == "user1"
    assert settings.secret_key == "pass1"
    assert settings.secure is True
    assert settings.region == "us-east-1"
