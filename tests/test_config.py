"""Tests for configuration."""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from terminal_proxy.config import Settings, StorageMode


def test_settings_defaults():
    settings = Settings()

    assert settings.proxy_host == "0.0.0.0"
    assert settings.proxy_port == 8000
    assert settings.namespace == "default"
    assert settings.storage_mode == StorageMode.NONE
    assert settings.storage_retain_pvc is False
    assert settings.storage_pvc_retention_ttl_seconds == 0
    assert settings.max_concurrent_pods == 100
    assert settings.pod_idle_timeout_seconds == 3600
    assert settings.terminal_ephemeral_storage_request == "5Gi"
    assert settings.terminal_ephemeral_storage_limit == "5Gi"
    assert settings.terminal_shm_size == "512Mi"


def test_settings_from_env():
    with patch.dict(
        os.environ,
        {
            "PROXY_PORT": "9000",
            "MAX_CONCURRENT_PODS": "50",
            "STORAGE_MODE": "shared",
        },
    ):
        settings = Settings()

        assert settings.proxy_port == 9000
        assert settings.max_concurrent_pods == 50
        assert settings.storage_mode == StorageMode.SHARED


def test_cors_origins_parsing():
    settings = Settings()

    assert settings.cors_origins == ["*"]

    with patch.dict(os.environ, {"CORS_ALLOWED_ORIGINS": "http://localhost,http://example.com"}):
        settings = Settings()
        assert settings.cors_origins == ["http://localhost", "http://example.com"]


@pytest.mark.parametrize("bad", ["", "0", "0Mi", "0.0Gi", "abc"])
def test_shm_size_rejects_empty_and_zero(bad):
    """Empty/0 would mount /dev/shm as an unbounded memory tmpfs."""
    with pytest.raises(ValidationError, match="terminalShmSize"):
        Settings(terminal_shm_size=bad)


def test_shm_size_accepts_positive_quantities():
    assert Settings(terminal_shm_size="1Gi").terminal_shm_size == "1Gi"
    assert Settings(terminal_shm_size="0.5Gi").terminal_shm_size == "0.5Gi"
    assert Settings(terminal_shm_size="256Mi").terminal_shm_size == "256Mi"


def test_settings_none_mode():
    with patch.dict(os.environ, {"STORAGE_MODE": "none"}):
        settings = Settings()
        assert settings.storage_mode == StorageMode.NONE


def test_settings_retain_pvc_from_env():
    with patch.dict(os.environ, {"STORAGE_RETAIN_PVC": "true"}):
        settings = Settings()
        assert settings.storage_retain_pvc is True


def test_settings_pvc_retention_ttl_from_env():
    with patch.dict(
        os.environ,
        {"STORAGE_RETAIN_PVC": "true", "STORAGE_PVC_RETENTION_TTL_SECONDS": "2592000"},
    ):
        settings = Settings()
        assert settings.storage_pvc_retention_ttl_seconds == 2592000
