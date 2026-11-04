"""Tests for HTTP proxy header handling."""

import httpx
import pytest

from terminal_proxy.proxy.http import HttpProxy


class _FakeRequest:
    """Minimal request stub for HttpProxy.proxy_request."""

    method = "GET"
    query_params: dict[str, str] = {}

    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

    async def body(self) -> bytes:
        return b""


@pytest.mark.asyncio
async def test_capability_forward_omits_authorization_header():
    """Capability URLs authenticate by their path token upstream: the pod key
    must NOT be injected there, while normal API paths keep carrying it."""
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(200, content=b"ok")

    proxy = HttpProxy()
    proxy._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=False
    )

    await proxy.proxy_request(
        "http://pod.test:8000/browser/view/sess1/tok/runtime.js", _FakeRequest(), "pod-key"
    )
    await proxy.proxy_request(
        "http://pod.test:8000/execute/abc/status", _FakeRequest(), "pod-key"
    )
    await proxy.close()

    assert seen[0] is None
    assert seen[1] == "Bearer pod-key"
