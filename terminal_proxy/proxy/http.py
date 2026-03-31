"""HTTP request proxying to terminal pods."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

import httpx
from fastapi import Request
from fastapi.responses import Response, StreamingResponse

from terminal_proxy.circuit_breaker import circuit_breaker_registry

logger = logging.getLogger(__name__)

STRIPPED_REQUEST_HEADERS = frozenset((
    "host", "content-length", "transfer-encoding", "connection",
    "authorization",
))

STRIPPED_RESPONSE_HEADERS = frozenset((
    "content-encoding", "content-length", "transfer-encoding", "connection",
))

STREAMING_CONTENT_TYPES = ("application/octet-stream", "image/", "application/pdf", "video/", "audio/")


class HttpProxy:
    """HTTP proxy for forwarding requests to terminal pods."""

    def __init__(self) -> None:
        """Initialize the HTTP proxy."""
        self._client: httpx.AsyncClient | None = None

    async def get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(300.0, connect=10.0),
                follow_redirects=False,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def proxy_request(
        self,
        target_url: str,
        request: Request,
        terminal_api_key: str,
        pod_key: str | None = None,
    ) -> Response:
        """Proxy an HTTP request to a terminal pod with circuit breaker support."""
        if pod_key:
            circuit_breaker = circuit_breaker_registry.get(pod_key)
            if not await circuit_breaker.can_execute():
                return Response(
                    content=b'{"error": "Circuit breaker open", "detail": "Terminal pod is temporarily unavailable"}',
                    status_code=503,
                    media_type="application/json",
                )

        client = await self.get_client()

        headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in STRIPPED_REQUEST_HEADERS
        }
        headers["Authorization"] = f"Bearer {terminal_api_key}"

        body = await request.body()

        try:
            response = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=body or None,
                params=dict(request.query_params),
            )

            if pod_key:
                circuit_breaker = circuit_breaker_registry.get(pod_key)
                await circuit_breaker.record_success()

        except httpx.ConnectError:
            if pod_key:
                circuit_breaker = circuit_breaker_registry.get(pod_key)
                await circuit_breaker.record_failure()
            return Response(
                content=b'{"error": "Terminal pod unavailable"}',
                status_code=503,
                media_type="application/json",
            )
        except httpx.TimeoutException:
            if pod_key:
                circuit_breaker = circuit_breaker_registry.get(pod_key)
                await circuit_breaker.record_failure()
            return Response(
                content=b'{"error": "Terminal pod timeout"}',
                status_code=504,
                media_type="application/json",
            )

        response_headers = {
            k: v for k, v in response.headers.items()
            if k.lower() not in STRIPPED_RESPONSE_HEADERS
        }

        content_type = response.headers.get("content-type", "")
        if any(ct in content_type for ct in STREAMING_CONTENT_TYPES):

            async def stream() -> AsyncIterator[bytes]:
                async for chunk in response.aiter_bytes():
                    yield chunk

            return StreamingResponse(
                stream(),
                status_code=response.status_code,
                headers=response_headers,
                media_type=content_type,
            )

        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=response_headers,
            media_type=content_type.split(";")[0] if content_type else "application/json",
        )


http_proxy = HttpProxy()
