"""Tests for configuration."""

import os
from unittest.mock import patch

from terminal_proxy.config import Settings, StorageMode


def test_settings_defaults():
    settings = Settings()

    assert settings.proxy_host == "0.0.0.0"
    assert settings.proxy_port == 8000
    assert settings.namespace == "default"
    assert settings.storage_mode == StorageMode.NONE
    assert settings.max_concurrent_pods == 100
    assert settings.pod_idle_timeout_seconds == 3600
    assert settings.terminal_ephemeral_storage_request == "5Gi"
    assert settings.terminal_ephemeral_storage_limit == "5Gi"


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


def test_settings_none_mode():
    with patch.dict(os.environ, {"STORAGE_MODE": "none"}):
        settings = Settings()
        assert settings.storage_mode == StorageMode.NONE
