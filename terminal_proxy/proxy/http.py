"""HTTP request proxying to terminal pods."""

from __future__ import annotations

import logging
from typing import Optional

import httpx
from fastapi import Request
from fastapi.responses import Response, StreamingResponse

from terminal_proxy.circuit_breaker import circuit_breaker_registry
from terminal_proxy.config import settings

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
    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(300.0, connect=10.0),
                follow_redirects=False,
            )
        return self._client

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def proxy_request(
        self,
        target_url: str,
        request: Request,
        terminal_api_key: str,
        pod_key: Optional[str] = None,
    ) -> Response:
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
                
        except httpx.ConnectError as e:
            if pod_key:
                circuit_breaker = circuit_breaker_registry.get(pod_key)
                await circuit_breaker.record_failure()
            return Response(
                content=b'{"error": "Terminal pod unavailable"}',
                status_code=503,
                media_type="application/json",
            )
        except httpx.TimeoutException as e:
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

            async def stream():
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
        )


http_proxy = HttpProxy()
