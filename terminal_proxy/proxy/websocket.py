"""WebSocket proxying to terminal pods."""

from __future__ import annotations

import asyncio
import contextlib
import logging

import aiohttp
from fastapi import WebSocket, WebSocketDisconnect

from terminal_proxy.models import TerminalPod

logger = logging.getLogger(__name__)


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
    ) -> None:
        """Proxy a WebSocket connection to a terminal pod."""
        await client_ws.accept()

        session = await self.get_session()

        ws_url = f"ws://{terminal.pod_ip}:8000{path}"

        try:
            async with session.ws_connect(
                ws_url,
                headers={"Authorization": f"Bearer {terminal.api_key}"},
            ) as upstream_ws:

                async def client_to_upstream() -> None:
                    try:
                        while True:
                            msg = await client_ws.receive()
                            if msg["type"] == "websocket.disconnect":
                                break
                            elif "bytes" in msg and msg["bytes"]:
                                await upstream_ws.send_bytes(msg["bytes"])
                            elif "text" in msg and msg["text"]:
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
                            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                                break
                    except Exception as e:
                        logger.warning(f"Upstream to client error: {e}")

                await asyncio.gather(
                    client_to_upstream(),
                    upstream_to_client(),
                    return_exceptions=True,
                )

        except aiohttp.ClientError as e:
            logger.warning(f"WebSocket connection error to {ws_url}: {e}")
            await client_ws.close(code=1011, reason="Terminal unavailable")
        except Exception as e:
            logger.error(f"WebSocket proxy error: {e}")
            await client_ws.close(code=1011, reason="Internal error")
        finally:
            with contextlib.suppress(Exception):
                await client_ws.close()


ws_proxy = WebSocketProxy()
