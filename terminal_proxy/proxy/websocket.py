"""WebSocket proxying to terminal pods."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
from typing import Any

import aiohttp
from fastapi import WebSocket, WebSocketDisconnect

from terminal_proxy.models import TerminalPod

logger = logging.getLogger(__name__)

# Capability URLs embed a per-session token; keep it out of logs.
_TOKEN_IN_PATH_RE = re.compile(r"(browser/view/[^/]+)/[^/]+(/?)")

RELAY_WEBSOCKET_TYPES = (
    aiohttp.WSMsgType.CLOSE,
    aiohttp.WSMsgType.CLOSING,
    aiohttp.WSMsgType.CLOSED,
    aiohttp.WSMsgType.ERROR,
)


def safe_ws_path(path: str) -> str:
    """Mask browser capability tokens so they never reach logs."""
    return _TOKEN_IN_PATH_RE.sub(r"\1/***/", path)


class WebSocketProxy:
    """WebSocket proxy for terminal sessions."""

    def __init__(self) -> None:
        """Initialize the WebSocket proxy."""
        self._session: aiohttp.ClientSession | None = None

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp client session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Close the aiohttp client session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def proxy_websocket(
        self,
        client_ws: WebSocket,
        terminal: TerminalPod,
        path: str,
        *,
        send_auth: bool = True,
        auth_payload: dict[str, Any] | None = None,
    ) -> None:
        """Proxy a WebSocket connection to a terminal pod.

        send_auth=False for capability-URL routes (/browser/view/{sid}/{token}/...):
        upstream authenticates those by the path token and treats our first
        message as protocol payload, so none may be injected. The client's auth
        payload (extra keys like chat_id) is relayed upstream with the pod key
        substituted for the proxy key.
        """
        session = await self.get_session()

        ws_url = f"ws://{terminal.service_name}:8000{path}"
        log_url = f"ws://{terminal.service_name}:8000{safe_ws_path(path)}"
        logger.info(f"Connecting to upstream WebSocket: {log_url}")

        connect_kwargs: dict[str, Any] = {}
        requested = client_ws.headers.get("sec-websocket-protocol")
        if requested:
            connect_kwargs["protocols"] = [
                p.strip() for p in requested.split(",") if p.strip()
            ]

        try:
            async with session.ws_connect(ws_url, **connect_kwargs) as upstream_ws:
                logger.info(f"Upstream WebSocket connected: {log_url}")

                if send_auth:
                    payload = dict(auth_payload) if isinstance(auth_payload, dict) else {}
                    payload["type"] = "auth"
                    payload["token"] = terminal.api_key
                    await upstream_ws.send_str(json.dumps(payload))

                async def client_to_upstream() -> None:
                    try:
                        while True:
                            msg = await client_ws.receive()
                            logger.debug(f"Client message: {msg.get('type')}")
                            if msg["type"] == "websocket.disconnect":
                                logger.info("Client sent disconnect")
                                return
                            elif msg["type"] == "websocket.receive":
                                if "bytes" in msg and msg.get("bytes"):
                                    await upstream_ws.send_bytes(msg["bytes"])
                                elif "text" in msg and msg.get("text"):
                                    await upstream_ws.send_str(msg["text"])
                    except WebSocketDisconnect:
                        logger.debug("Client disconnected")
                    except Exception as e:
                        logger.warning(f"Client to upstream error: {e}")

                async def upstream_to_client() -> None:
                    try:
                        async for msg in upstream_ws:
                            if msg.type == aiohttp.WSMsgType.BINARY:
                                await client_ws.send_bytes(msg.data)
                            elif msg.type == aiohttp.WSMsgType.TEXT:
                                await client_ws.send_text(msg.data)
                            elif msg.type in RELAY_WEBSOCKET_TYPES:
                                return
                    except Exception as e:
                        logger.warning(f"Upstream to client error: {e}")

                # Whichever side finishes first tears down the other and both
                # connections; waiting for both would keep a live upstream pinned
                # to a dead client (and the pod unevictable).
                tasks = [
                    asyncio.create_task(client_to_upstream()),
                    asyncio.create_task(upstream_to_client()),
                ]
                _done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                # retrieve exceptions so asyncio does not warn about them
                for task in _done:
                    task.exception()

        except aiohttp.ClientError as e:
            logger.warning(f"WebSocket connection error to {log_url}: {e}")
            await client_ws.close(code=1011, reason="Terminal unavailable")
        except Exception as e:
            logger.error(f"WebSocket proxy error: {e}")
            await client_ws.close(code=1011, reason="Internal error")
        finally:
            with contextlib.suppress(Exception):
                await client_ws.close()


ws_proxy = WebSocketProxy()
