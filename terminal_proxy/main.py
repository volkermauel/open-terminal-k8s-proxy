"""Main FastAPI application for terminal proxy."""

from __future__ import annotations

import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from terminal_proxy import __version__
from terminal_proxy.config import settings
from terminal_proxy.models import HealthStatus, PodState
from terminal_proxy.pod_manager import pod_manager
from terminal_proxy.proxy.http import http_proxy
from terminal_proxy.proxy.websocket import ws_proxy

logger = logging.getLogger(__name__)

bearer_scheme = HTTPBearer(auto_error=False)


def get_or_create_proxy_api_key() -> str:
    if settings.proxy_api_key:
        return settings.proxy_api_key
    key = secrets.token_urlsafe(32)
    logger.info(f"Generated proxy API key: {key[:8]}...")
    return key


PROXY_API_KEY = get_or_create_proxy_api_key()


async def verify_api_key(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> str:
    if not PROXY_API_KEY:
        return "anonymous"
    if not credentials or credentials.credentials != PROXY_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return credentials.credentials


def extract_user_id(request: Request) -> str:
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-Id header required")
    return user_id


@asynccontextmanager
async def lifespan(app: FastAPI):
    from terminal_proxy.k8s.client import k8s_client
    from terminal_proxy.storage import storage_manager

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    k8s_client.init()
    logger.info("Kubernetes client initialized")

    if settings.storage_mode in ("shared", "sharedRWO"):
        storage_manager.ensure_shared_pvc()

    await pod_manager.start()

    yield

    await pod_manager.stop()
    await http_proxy.close()
    await ws_proxy.close()


app = FastAPI(
    title="Open Terminal K8s Proxy",
    description="Kubernetes orchestrator for per-user open-terminal instances",
    version=__version__,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok"}


@app.get(
    "/api/config",
    include_in_schema=False,
    dependencies=[Depends(verify_api_key)],
)
async def get_config():
    return {
        "features": {
            "terminal": True,
            "notebooks": True,
        },
    }


@app.get(
    "/api/status",
    dependencies=[Depends(verify_api_key)],
)
async def get_status():
    return pod_manager.get_stats()


@app.get(
    "/files/cwd",
    include_in_schema=False,
    dependencies=[Depends(verify_api_key)],
)
async def get_cwd(request: Request, user_id: str = Depends(extract_user_id)):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/files/cwd"
    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.post(
    "/files/cwd",
    include_in_schema=False,
    dependencies=[Depends(verify_api_key)],
)
async def set_cwd(request: Request, user_id: str = Depends(extract_user_id)):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/files/cwd"
    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


@app.api_route("/files/{path:path}", methods=PROXY_METHODS)
async def proxy_files(
    path: str,
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/files/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/execute", methods=["GET", "POST"])
async def proxy_execute(
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/execute"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/execute/{process_id}/{path:path}", methods=PROXY_METHODS)
async def proxy_execute_process(
    process_id: str,
    path: str,
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/execute/{process_id}/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/ports", methods=["GET"])
async def proxy_ports(
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/ports"
    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/proxy/{port}/{path:path}", methods=PROXY_METHODS)
async def proxy_port_forward(
    port: int,
    path: str,
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/proxy/{port}/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/api/terminals", methods=["GET", "POST"])
async def proxy_terminals(
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/api/terminals"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/api/terminals/{session_id}", methods=["GET", "DELETE"])
async def proxy_terminal_session(
    session_id: str,
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
):
    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        raise HTTPException(status_code=503, detail="Terminal not ready")

    target_url = f"http://{terminal.pod_ip}:8000/api/terminals/{session_id}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.websocket("/api/terminals/{session_id}")
async def websocket_terminal(client_ws: WebSocket, session_id: str):
    await client_ws.accept()

    if PROXY_API_KEY:
        try:
            import asyncio
            import json

            raw = await asyncio.wait_for(client_ws.receive_text(), timeout=10.0)
            payload = json.loads(raw)
            if payload.get("type") != "auth" or payload.get("token") != PROXY_API_KEY:
                await client_ws.close(code=4001, reason="Invalid API key")
                return
        except (asyncio.TimeoutError, json.JSONDecodeError, Exception):
            await client_ws.close(code=4001, reason="Auth timeout or invalid payload")
            return

    user_id = client_ws.query_params.get("user_id")
    if not user_id:
        await client_ws.close(code=4002, reason="user_id query param required")
        return

    terminal = await pod_manager.get_or_create(user_id)
    if terminal.state != PodState.RUNNING:
        await client_ws.close(code=5031, reason="Terminal not ready")
        return

    await ws_proxy.proxy_websocket(
        client_ws=client_ws,
        terminal=terminal,
        path=f"/api/terminals/{session_id}",
    )


def main():
    import uvicorn

    uvicorn.run(
        "terminal_proxy.main:app",
        host=settings.proxy_host,
        port=settings.proxy_port,
        reload=False,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
