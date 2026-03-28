"""Main FastAPI application for terminal proxy."""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from terminal_proxy import __version__
from terminal_proxy.config import settings
from terminal_proxy.metrics import format_prometheus_metrics, record_request, update_pod_states
from terminal_proxy.models import K8sUnavailableError, PodState, TerminalPod
from terminal_proxy.pod_manager import pod_manager
from terminal_proxy.proxy.http import http_proxy
from terminal_proxy.proxy.websocket import ws_proxy

logger = logging.getLogger(__name__)

bearer_scheme = HTTPBearer(auto_error=False)

REQUESTS_PER_MINUTE = 300
REQUEST_BODY_MAX_SIZE = 100 * 1024 * 1024
request_counts: dict[str, list[float]] = defaultdict(list)


class WriteFileRequest(BaseModel):
    path: str = Field(
        description="Absolute or relative path to write to. Parent directories are created automatically."
    )
    content: str = Field(description="Text content to write to the file. Overwrites if the file already exists.")


class ReplacementChunk(BaseModel):
    target: str = Field(
        description="Exact string to find. Must match precisely, including whitespace."
    )
    replacement: str = Field(description="Content to replace the target with.")
    start_line: int | None = Field(
        None, description="Narrow the search to lines at or after this (1-indexed)."
    )
    end_line: int | None = Field(
        None, description="Narrow the search to lines at or before this (1-indexed)."
    )
    allow_multiple: bool = Field(False, description="If true, replaces all occurrences.")


class ReplaceFileRequest(BaseModel):
    path: str = Field(description="Path to the file to modify.")
    replacements: list[ReplacementChunk] = Field(
        description="List of find-and-replace operations to apply sequentially."
    )


class ExecRequest(BaseModel):
    command: str = Field(
        description="Shell command to execute. Supports chaining (&&, ||, ;), pipes (|), and redirections.",
        examples=["echo hello", "ls -la && whoami", "rm /home/user/file.txt"],
    )
    cwd: str | None = Field(
        None,
        description="Working directory for the command. Defaults to the server's current directory if not set.",
    )
    env: dict[str, str] | None = Field(
        None,
        description="Extra environment variables merged into the subprocess environment.",
    )


def get_or_create_proxy_api_key() -> str:
    """Get or create the proxy API key."""
    if settings.proxy_api_key:
        return settings.proxy_api_key
    key = secrets.token_urlsafe(32)
    logger.info("Generated proxy API key")
    return key


PROXY_API_KEY = get_or_create_proxy_api_key()


async def verify_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> str:
    """Verify the API key from the Authorization header."""
    if not PROXY_API_KEY:
        return "anonymous"
    if not credentials or credentials.credentials != PROXY_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return credentials.credentials


def extract_user_id(request: Request) -> str:
    """Extract user ID from request headers."""
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-Id header required")
    return user_id


def ensure_k8s_available() -> None:
    """Ensure Kubernetes API is available, raise K8sUnavailableError if not."""
    from terminal_proxy.k8s.client import k8s_client

    if not k8s_client._initialized:
        try:
            k8s_client.init()
        except Exception as e:
            raise K8sUnavailableError(f"Kubernetes API initialization failed: {e}") from e


async def get_terminal_for_user(user_id: str) -> TerminalPod:
    """Get or create terminal pod with graceful error handling."""
    from kubernetes.client.rest import ApiException

    ensure_k8s_available()

    try:
        terminal = await pod_manager.get_or_create(user_id)
        if terminal.state != PodState.RUNNING:
            raise HTTPException(status_code=503, detail="Terminal not ready")
        return terminal
    except ApiException as e:
        logger.error(f"K8s API error getting terminal for user {user_id}: {e}")
        raise K8sUnavailableError(f"Kubernetes API error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting terminal for user {user_id}: {e}")
        raise K8sUnavailableError(f"Failed to get terminal: {e}") from e


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application lifespan for startup and shutdown."""
    from terminal_proxy.k8s.client import k8s_client
    from terminal_proxy.logging_config import setup_logging
    from terminal_proxy.storage import storage_manager

    logger.debug("Starting application lifespan")
    setup_logging()
    logger.debug("Logging configured")

    try:
        logger.debug("Initializing Kubernetes client")
        k8s_client.init()
        logger.info("Kubernetes client initialized")
    except Exception as e:
        logger.warning(f"Kubernetes client initialization failed: {e}. Will retry on demand.")

    try:
        if settings.storage_mode in ("shared", "sharedRWO"):
            logger.debug(f"Ensuring shared PVC for storage mode: {settings.storage_mode}")
            storage_manager.ensure_shared_pvc()
    except Exception as e:
        logger.warning(f"Failed to ensure shared PVC: {e}")

    logger.debug("Starting pod manager")
    await pod_manager.start()
    logger.info("Application started successfully")

    yield

    logger.debug("Shutting down application")
    await pod_manager.stop()
    await http_proxy.close()
    await ws_proxy.close()
    logger.debug("Application shutdown complete")


app = FastAPI(
    title="Open Terminal K8s Proxy",
    description="Kubernetes orchestrator for per-user open-terminal instances",
    version=__version__,
    lifespan=lifespan,
)


@app.exception_handler(K8sUnavailableError)
async def k8s_unavailable_handler(request: Request, exc: K8sUnavailableError) -> JSONResponse:
    """Handle K8s API unavailability gracefully."""
    logger.error(f"K8s API unavailable: {exc}")
    return JSONResponse(
        status_code=503,
        content={
            "error": "Service temporarily unavailable",
            "detail": "Kubernetes API is unavailable",
        },
    )


@app.middleware("http")
async def rate_limit_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Rate limit middleware to prevent abuse."""
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()

    request_counts[client_ip] = [ts for ts in request_counts[client_ip] if now - ts < 60]

    if len(request_counts[client_ip]) >= REQUESTS_PER_MINUTE:
        latency = time.time() - now
        record_request(request.method, request.url.path, latency, 429)
        return JSONResponse(
            status_code=429,
            content={"error": "Rate limit exceeded", "detail": "Too many requests"},
        )

    request_counts[client_ip].append(now)

    start_time = time.time()
    response = await call_next(request)
    latency = time.time() - start_time
    record_request(request.method, request.url.path, latency, response.status_code)

    return response


@app.middleware("http")
async def request_size_limit_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Middleware to limit request body size."""
    if request.method in ("POST", "PUT", "PATCH"):
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > REQUEST_BODY_MAX_SIZE:
            record_request(request.method, request.url.path, 0, 413)
            return JSONResponse(
                status_code=413,
                content={
                    "error": "Payload too large",
                    "detail": f"Maximum size is {REQUEST_BODY_MAX_SIZE} bytes",
                },
            )

    return await call_next(request)


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", include_in_schema=False, response_model=None)
async def health() -> dict[str, str | int] | JSONResponse:
    """Health check endpoint."""
    from terminal_proxy.k8s.client import k8s_client

    if not k8s_client._initialized:
        return {"status": "ok", "k8s": "not_initialized"}

    k8s_healthy = False
    try:
        k8s_client.core_v1.list_namespaced_pod(k8s_client.namespace, limit=1)
        k8s_healthy = True
    except Exception as e:
        logger.warning(f"K8s health check failed: {e}")

    stats = pod_manager.get_stats()
    update_pod_states({h: (t, t.state) for h, t in pod_manager._pods.items()})

    if not k8s_healthy:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "k8s": "disconnected",
                "active_pods": stats["active_pods"],
                "max_pods": stats["max_pods"],
            },
        )

    return {
        "status": "ok",
        "k8s": "connected",
        "active_pods": stats["active_pods"],
        "max_pods": stats["max_pods"],
        "storage_mode": settings.storage_mode.value,
    }


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    stats = pod_manager.get_stats()
    update_pod_states({h: (t, t.state) for h, t in pod_manager._pods.items()})
    metrics_text = format_prometheus_metrics(
        active_pods=stats["active_pods"],
        max_pods=stats["max_pods"],
        storage_mode=settings.storage_mode.value,
    )
    return Response(content=metrics_text, media_type="text/plain; charset=utf-8")


@app.get(
    "/api/config",
    include_in_schema=False,
    dependencies=[Depends(verify_api_key)],
)
async def get_config() -> dict[str, dict[str, bool]]:
    """Get proxy configuration."""
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
async def get_status() -> dict[str, Any]:
    """Get proxy status and statistics."""
    return pod_manager.get_stats()


@app.get(
    "/files/cwd",
    include_in_schema=False,
    dependencies=[Depends(verify_api_key)],
)
async def get_cwd(request: Request, user_id: str = Depends(extract_user_id)) -> Response:
    """Get current working directory."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/files/cwd"
    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.post(
    "/files/cwd",
    include_in_schema=False,
    dependencies=[Depends(verify_api_key)],
)
async def set_cwd(request: Request, user_id: str = Depends(extract_user_id)) -> Response:
    """Set current working directory."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/files/cwd"
    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


@app.get("/files/list", dependencies=[Depends(verify_api_key)])
async def proxy_files_list(
    request: Request,
    user_id: str = Depends(extract_user_id),
    directory: str | None = Query(None, description="Directory path to list. Defaults to current directory."),
) -> Response:
    """Return a structured listing of files and directories at the given path."""
    terminal = await get_terminal_for_user(user_id)
    return await http_proxy.proxy_request(
        f"{terminal.endpoint}/files/list", request, terminal.api_key, pod_key=terminal.user_hash
    )


@app.get("/files/read", dependencies=[Depends(verify_api_key)])
async def proxy_files_read(
    request: Request,
    user_id: str = Depends(extract_user_id),
    path: str = Query(..., description="Path to the file to read."),
    start_line: int | None = Query(
        None, description="First line to return (1-indexed, inclusive). Defaults to the beginning of the file."
    ),
    end_line: int | None = Query(
        None, description="Last line to return (1-indexed, inclusive). Defaults to the end of the file."
    ),
) -> Response:
    """Read a file and return its contents. Supports text files and images (PNG, JPEG, WebP, etc.). For text files you can optionally request a specific line range. Images are returned as binary so you can view and analyze them directly. Use display to show a file to the user."""
    terminal = await get_terminal_for_user(user_id)
    return await http_proxy.proxy_request(
        f"{terminal.endpoint}/files/read", request, terminal.api_key, pod_key=terminal.user_hash
    )


@app.get("/files/display", dependencies=[Depends(verify_api_key)])
async def proxy_files_display(
    request: Request,
    user_id: str = Depends(extract_user_id),
    path: str = Query(..., description="Path to the file to display."),
) -> Response:
    """Open a file in the user's file viewer so they can see it. Use this when the user wants to view or look at a file. This does not return file content to you — use read if you need to read the content yourself."""
    terminal = await get_terminal_for_user(user_id)
    return await http_proxy.proxy_request(
        f"{terminal.endpoint}/files/display", request, terminal.api_key, pod_key=terminal.user_hash
    )


@app.post("/files/write", dependencies=[Depends(verify_api_key)])
async def proxy_files_write(
    request: Request,
    body: WriteFileRequest,
    user_id: str = Depends(extract_user_id),
) -> Response:
    """Write text content to a file. Creates parent directories automatically. Overwrites if the file already exists."""
    terminal = await get_terminal_for_user(user_id)
    return await http_proxy.proxy_request(
        f"{terminal.endpoint}/files/write", request, terminal.api_key, pod_key=terminal.user_hash
    )


@app.post("/files/replace", dependencies=[Depends(verify_api_key)])
async def proxy_files_replace(
    request: Request,
    body: ReplaceFileRequest,
    user_id: str = Depends(extract_user_id),
) -> Response:
    """Find and replace exact strings in a file. Supports multiple replacements in one call with optional line range narrowing."""
    terminal = await get_terminal_for_user(user_id)
    return await http_proxy.proxy_request(
        f"{terminal.endpoint}/files/replace", request, terminal.api_key, pod_key=terminal.user_hash
    )


@app.get("/files/grep", dependencies=[Depends(verify_api_key)])
async def proxy_files_grep(
    request: Request,
    user_id: str = Depends(extract_user_id),
    query: str = Query(..., description="Text or regex pattern to search for."),
    path: str | None = Query(None, description="Directory or file to search in. Defaults to current directory."),
    regex: bool | None = Query(None, description="Treat query as a regex pattern."),
    case_insensitive: bool | None = Query(None, description="Perform case-insensitive matching."),
    include: str | None = Query(None, description="Glob patterns to filter files (e.g. '*.py'). Files must match at least one pattern."),
    match_per_line: bool | None = Query(None, description="If true, return each matching line with line numbers. If false, return only the names of matching files."),
    max_results: int | None = Query(None, description="Maximum number of matches to return."),
) -> Response:
    """Search for a text pattern across files in a directory. Returns structured matches with file paths, line numbers, and matching lines. Skips binary files."""
    terminal = await get_terminal_for_user(user_id)
    return await http_proxy.proxy_request(
        f"{terminal.endpoint}/files/grep", request, terminal.api_key, pod_key=terminal.user_hash
    )


@app.get("/files/glob", dependencies=[Depends(verify_api_key)])
async def proxy_files_glob(
    request: Request,
    user_id: str = Depends(extract_user_id),
    pattern: str = Query(..., description="Glob pattern to search for (e.g. '*.py')."),
    path: str | None = Query(None, description="Directory to search within. Defaults to current directory."),
    exclude: str | None = Query(None, description="Glob patterns to exclude from search results."),
    type: str | None = Query(None, description="Type filter: 'file', 'directory', or 'any'."),
    max_results: int | None = Query(None, description="Maximum number of matches to return."),
) -> Response:
    """Search for files and subdirectories by name within a specified directory using glob patterns. Results will include the relative path, type, size, and modification time."""
    terminal = await get_terminal_for_user(user_id)
    return await http_proxy.proxy_request(
        f"{terminal.endpoint}/files/glob", request, terminal.api_key, pod_key=terminal.user_hash
    )


@app.api_route(
    "/files/{path:path}",
    methods=PROXY_METHODS,
    include_in_schema=False,
)
async def proxy_files(
    path: str,
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
) -> Response:
    """Proxy file operations to terminal pod."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/files/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(
        target_url, request, terminal.api_key, pod_key=terminal.user_hash
    )


@app.get("/execute", dependencies=[Depends(verify_api_key)])
async def proxy_execute_list(
    request: Request,
    user_id: str = Depends(extract_user_id),
) -> Response:
    """List running commands. Returns a list of all tracked background processes, including running, done, and killed."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/execute"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.post("/execute", dependencies=[Depends(verify_api_key)])
async def proxy_execute(
    request: Request,
    body: ExecRequest,
    user_id: str = Depends(extract_user_id),
    wait: float | None = Query(
        None,
        description="Seconds to wait for the command to finish before returning. If the command completes in time, output is included inline. Null to return immediately.",
    ),
    tail: int | None = Query(
        None,
        description="Return only the last N output entries. Useful to limit response size when only recent output matters.",
    ),
) -> Response:
    """Execute a shell command. Run a command in the background and return a command ID. This gives you full Linux shell access: you can run any command including file operations (rm, cp, mv, mkdir, cat, grep, find, etc.), install packages, run scripts, and chain commands with &&, ||, ;, and pipes. Use this for operations not covered by dedicated file endpoints, such as deleting files (rm), moving/renaming (mv), or any other shell task."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/execute"
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
) -> Response:
    """Manage a running command process. Use GET /execute/{process_id}/status to poll output and check if finished. Use POST /execute/{process_id}/input to send stdin. Use DELETE /execute/{process_id} to kill (SIGTERM, or force=true for SIGKILL)."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/execute/{process_id}/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/ports", methods=["GET"])
async def proxy_ports(
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
) -> Response:
    """Proxy port listing requests."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/ports"
    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/proxy/{port}/{path:path}", methods=PROXY_METHODS)
async def proxy_port_forward(
    port: int,
    path: str,
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
) -> Response:
    """Proxy port forwarding requests."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/proxy/{port}/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/api/terminals", methods=["GET", "POST"])
async def proxy_terminals(
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
) -> Response:
    """Proxy terminal session management."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/api/terminals"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.api_route("/api/terminals/{session_id}", methods=["GET", "DELETE"])
async def proxy_terminal_session(
    session_id: str,
    request: Request,
    user_id: str = Depends(extract_user_id),
    _: str = Depends(verify_api_key),
) -> Response:
    """Proxy terminal session operations."""
    terminal = await get_terminal_for_user(user_id)

    target_url = f"{terminal.endpoint}/api/terminals/{session_id}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    return await http_proxy.proxy_request(target_url, request, terminal.api_key)


@app.websocket("/api/terminals/{session_id}")
async def websocket_terminal(client_ws: WebSocket, session_id: str) -> None:
    """WebSocket endpoint for terminal sessions."""
    import json

    await client_ws.accept()

    if PROXY_API_KEY:
        try:
            raw = await asyncio.wait_for(client_ws.receive_text(), timeout=10.0)
            payload = json.loads(raw)
            if payload.get("type") != "auth" or payload.get("token") != PROXY_API_KEY:
                await client_ws.close(code=1008, reason="Policy Violation: Invalid API key")
                return
        except (TimeoutError, json.JSONDecodeError, Exception):
            await client_ws.close(
                code=1008, reason="Policy Violation: Auth timeout or invalid payload"
            )
            return

    user_id = client_ws.query_params.get("user_id")
    if not user_id:
        await client_ws.close(code=1008, reason="Policy Violation: user_id query param required")
        return

    try:
        terminal = await get_terminal_for_user(user_id)
    except K8sUnavailableError as e:
        await client_ws.close(code=1011, reason=f"Internal Error: {e}")
        return
    except HTTPException as e:
        await client_ws.close(code=1011, reason=f"Internal Error: {e.detail}")
        return

    await ws_proxy.proxy_websocket(
        client_ws=client_ws,
        terminal=terminal,
        path=f"/api/terminals/{session_id}",
    )


def main() -> None:
    """Start the server."""
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
