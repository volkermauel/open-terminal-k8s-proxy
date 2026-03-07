"""Pytest configuration and fixtures."""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def mock_settings(monkeypatch):
    """Mock settings for testing."""
    monkeypatch.setenv("PROXY_API_KEY", "test-api-key-12345")
    monkeypatch.setenv("NAMESPACE", "test-namespace")
    monkeypatch.setenv("MAX_CONCURRENT_PODS", "10")
