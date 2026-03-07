"""Tests for main FastAPI application."""

import pytest
from fastapi.testclient import TestClient

from terminal_proxy.main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_status_requires_auth(client):
    response = client.get("/api/status")
    assert response.status_code == 401


def test_files_endpoint_requires_user_id(client):
    response = client.get(
        "/files/list",
        headers={"Authorization": "Bearer invalid-key"},
    )
    assert response.status_code == 400


def test_websocket_missing_user_id(client):
    with client.websocket_connect("/api/terminals/test-session") as websocket:
        websocket.close(code=4002)
