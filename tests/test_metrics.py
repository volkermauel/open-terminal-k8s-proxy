"""Tests for Prometheus metrics (capability-token masking)."""

import pytest
from fastapi.testclient import TestClient

from terminal_proxy import metrics
from terminal_proxy.main import REQUEST_BODY_MAX_SIZE, app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_registry():
    metrics._request_latencies.clear()
    metrics._error_counts.clear()
    yield
    metrics._request_latencies.clear()
    metrics._error_counts.clear()


def test_capability_token_never_reaches_metrics(client):
    """Normal and 413 recording branches must mask the token segment."""
    token = "t" * 40

    # Normal branch (404: unknown browser session)
    client.get(f"/browser/view/sess/{token}/runtime.js")
    # 413 branch (payload too large)
    client.post(
        f"/browser/view/sess/{token}/frame/https/x",
        content=b"x" * (REQUEST_BODY_MAX_SIZE + 1),
    )

    keys = "\n".join(list(metrics._request_latencies) + list(metrics._error_counts))
    assert token not in keys
    assert any("/***/" in key for key in metrics._request_latencies)
    assert any("/***/" in key for key in metrics._error_counts)

    metrics_text = client.get("/metrics").text
    assert token not in metrics_text
