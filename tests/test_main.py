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
    data = response.json()
    assert data["status"] == "ok"
    assert "k8s" in data


def test_status_requires_auth(client):
    response = client.get("/api/status")
    assert response.status_code == 401


def test_files_endpoint_requires_auth(client):
    response = client.get(
        "/files/list",
        headers={"Authorization": "Bearer invalid-key"},
    )
    assert response.status_code == 401


def test_files_endpoint_requires_user_id(client):
    from terminal_proxy.main import PROXY_API_KEY

    response = client.get(
        "/files/list",
        headers={"Authorization": f"Bearer {PROXY_API_KEY}"},
    )
    assert response.status_code == 400


def test_websocket_missing_user_id(client):
    with client.websocket_connect("/api/terminals/test-session") as websocket:
        websocket.close(code=4002)


def _get_openapi(client):
    return client.get("/openapi.json").json()


def _resolve_schema_ref(spec, schema):
    ref = schema.get("$ref", None)
    if ref and ref.startswith("#/components/schemas/"):
        name = ref.split("/")[-1]
        if name in spec.get("components", {}).get("schemas", {}):
            return spec["components"]["schemas"][name]
    return None


def _get_query_params(operation):
    result = {}
    for p in operation.get("parameters", []):
        if p.get("in") == "query":
            result[p["name"]] = p.get("required", False)
    return result


def _get_required_body_fields(spec, operation):
    request_body = operation.get("requestBody")
    if not request_body:
        return set()
    schema = request_body.get("content", {}).get("application/json", {}).get("schema", {})
    if not schema:
        return set()
    resolved = _resolve_schema_ref(spec, schema)
    if resolved:
        schema = resolved
    return set(schema.get("required", []))


def test_openapi_exposes_explicit_file_routes(client):
    spec = _get_openapi(client)
    paths = spec["paths"]

    for route in [
        "/files/list",
        "/files/read",
        "/files/write",
        "/files/replace",
        "/files/display",
        "/files/grep",
        "/files/glob",
    ]:
        assert route in paths, f"Missing explicit route: {route}"


def test_openapi_file_get_query_params(client):
    spec = _get_openapi(client)
    paths = spec["paths"]

    params = _get_query_params(paths["/files/list"]["get"])
    assert "directory" in params
    assert params["directory"] is False

    params = _get_query_params(paths["/files/read"]["get"])
    assert "path" in params
    assert params["path"] is True
    assert "start_line" in params
    assert params["start_line"] is False
    assert "end_line" in params
    assert params["end_line"] is False

    params = _get_query_params(paths["/files/display"]["get"])
    assert "path" in params
    assert params["path"] is True

    params = _get_query_params(paths["/files/grep"]["get"])
    assert "query" in params
    assert params["query"] is True
    assert "path" in params
    assert params["path"] is False
    assert "regex" in params
    assert "include" in params
    assert "case_insensitive" in params
    assert "match_per_line" in params
    assert "max_results" in params

    params = _get_query_params(paths["/files/glob"]["get"])
    assert "pattern" in params
    assert params["pattern"] is True
    assert "path" in params
    assert params["path"] is False
    assert "exclude" in params
    assert "type" in params
    assert "max_results" in params


def test_openapi_file_post_body_params(client):
    spec = _get_openapi(client)
    paths = spec["paths"]

    required = _get_required_body_fields(spec, paths["/files/write"]["post"])
    assert required == {"path", "content"}

    required = _get_required_body_fields(spec, paths["/files/replace"]["post"])
    assert required == {"path", "replacements"}

    body_props = paths["/files/replace"]["post"]["requestBody"]["content"]["application/json"][
        "schema"
    ]
    resolved = _resolve_schema_ref(spec, body_props)
    if resolved:
        body_props = resolved
    properties = set(body_props.get("properties", {}).keys())
    assert "replacements" in properties
    replacement_schema = body_props["properties"]["replacements"]
    items_ref = replacement_schema.get("items", {}).get("$ref", "")
    if items_ref:
        chunk_name = items_ref.split("/")[-1]
        chunk_schema = spec["components"]["schemas"][chunk_name]
        chunk_props = set(chunk_schema.get("properties", {}).keys())
        assert "target" in chunk_props
        assert "replacement" in chunk_props
        assert chunk_schema.get("required", []) == ["target", "replacement"]


def test_openapi_hides_desktop_routes(client):
    """Virtual desktop endpoints are hidden from the OpenAPI schema (not maintained)."""
    spec = _get_openapi(client)
    paths = spec["paths"]

    for route in [
        "/desktop",
        "/desktop/start",
        "/desktop/stop",
        "/desktop/screenshot",
        "/desktop/click",
        "/desktop/mouse_move",
        "/desktop/drag",
        "/desktop/type",
        "/desktop/key",
        "/desktop/scroll",
    ]:
        assert route not in paths, f"Desktop route {route} should be hidden from schema"


def test_desktop_routes_require_auth(client):
    routes = [
        ("/desktop", "get"),
        ("/desktop/start", "post"),
        ("/desktop/stop", "post"),
        ("/desktop/screenshot", "post"),
        ("/desktop/click", "post"),
        ("/desktop/mouse_move", "post"),
        ("/desktop/drag", "post"),
        ("/desktop/type", "post"),
        ("/desktop/key", "post"),
        ("/desktop/scroll", "post"),
    ]
    for path, method in routes:
        response = getattr(client, method)(path)
        assert response.status_code == 401, f"{method.upper()} {path} should require auth"


def test_desktop_routes_require_user_id(client):
    from terminal_proxy.main import PROXY_API_KEY

    response = client.get(
        "/desktop",
        headers={"Authorization": f"Bearer {PROXY_API_KEY}"},
    )
    assert response.status_code == 400


def test_openapi_exposes_info_and_execute_process_routes(client):
    spec = _get_openapi(client)
    paths = spec["paths"]

    exposed = [
        "/info",
        "/execute/{process_id}/status",
        "/execute/{process_id}/input",
        "/execute/{process_id}",
    ]
    for route in exposed:
        assert route in paths, f"Missing exposed route: {route}"


def test_openapi_hides_system_notebooks_and_execute_catchall(client):
    spec = _get_openapi(client)
    paths = spec["paths"]

    hidden = [
        "/system",
        "/notebooks",
        "/notebooks/{session_id}",
        "/execute/{process_id}/{path:path}",
    ]
    for route in hidden:
        assert route not in paths, f"Route should be hidden: {route}"


def test_new_forwarder_routes_require_auth(client):
    routes = [
        ("/info", "get"),
        ("/system", "get"),
        ("/execute/abc/status", "get"),
        ("/execute/abc/input", "post"),
        ("/execute/abc", "delete"),
        ("/notebooks", "post"),
        ("/notebooks/abc", "get"),
    ]
    for path, method in routes:
        response = getattr(client, method)(path, headers={"X-User-Id": "u1"})
        assert response.status_code == 401, f"{method.upper()} {path} should require auth"


def test_new_forwarder_routes_require_user_id(client):
    from terminal_proxy.main import PROXY_API_KEY

    headers = {"Authorization": f"Bearer {PROXY_API_KEY}"}
    for path, method in [("/info", "get"), ("/execute/abc/status", "get"), ("/notebooks", "post")]:
        response = getattr(client, method)(path, headers=headers)
        assert response.status_code == 400, f"{method.upper()} {path} should require X-User-Id"


def test_new_forwarder_routes_target_the_pod(client):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, patch

    from fastapi import Response

    from terminal_proxy.main import PROXY_API_KEY

    terminal = SimpleNamespace(endpoint="http://pod.test:8000", api_key="pod-key", user_hash="h")
    headers = {"Authorization": f"Bearer {PROXY_API_KEY}", "X-User-Id": "user-1"}
    captured = []

    async def fake_proxy(target_url, request, terminal_api_key, pod_key=None):
        captured.append((target_url, terminal_api_key))
        return Response(content=b'{"ok": 1}', media_type="application/json", status_code=200)

    with (
        patch("terminal_proxy.main.get_terminal_for_user", new=AsyncMock(return_value=terminal)),
        patch("terminal_proxy.main.http_proxy.proxy_request", new=fake_proxy),
    ):
        assert client.get("/info", headers=headers).status_code == 200
        assert client.get("/system", headers=headers).status_code == 200
        assert client.get("/execute/abc/status?seq=5", headers=headers).status_code == 200
        assert (
            client.post("/execute/abc/input", json={"text": "hi"}, headers=headers).status_code
            == 200
        )
        assert client.delete("/execute/abc?force=true", headers=headers).status_code == 200
        assert (
            client.post("/notebooks", json={"kernel": "python3"}, headers=headers).status_code
            == 200
        )
        assert (
            client.post("/notebooks/s1/execute", json={"code": "x"}, headers=headers).status_code
            == 200
        )

    targets = [url for url, _ in captured]
    assert "http://pod.test:8000/info" in targets
    assert "http://pod.test:8000/system" in targets
    assert "http://pod.test:8000/execute/abc/status?seq=5" in targets
    assert "http://pod.test:8000/execute/abc/input" in targets
    assert "http://pod.test:8000/execute/abc?force=true" in targets
    assert "http://pod.test:8000/notebooks" in targets
    assert "http://pod.test:8000/notebooks/s1/execute" in targets
    assert all(key == "pod-key" for _, key in captured)


def test_openapi_catch_all_files_route_hidden(client):
    spec = _get_openapi(client)
    paths = spec["paths"]
    catch_all = [
        p
        for p in paths
        if p.startswith("/files/")
        and "{" not in p
        and p
        not in (
            "/files/list",
            "/files/read",
            "/files/write",
            "/files/replace",
            "/files/display",
            "/files/grep",
            "/files/glob",
            "/files/cwd",
        )
    ]
    assert len(catch_all) == 0, (
        f"Catch-all /files/{{path}} should be hidden from schema. Found: {catch_all}"
    )


@pytest.mark.asyncio
async def test_create_terminal_for_user_bootstraps_chat_dir() -> None:
    """Every terminal resolution (POST /api/terminals and each perUser read) must seed
    the per-chat cwd so the session cwd is bound before the PTY spawns."""
    from unittest.mock import ANY, AsyncMock, patch

    from terminal_proxy.main import _create_terminal_for_user
    from terminal_proxy.models import PodState, TerminalPod

    terminal = TerminalPod.create("user-1", "key")
    terminal.state = PodState.RUNNING

    with (
        patch("terminal_proxy.main.ensure_k8s_available"),
        patch(
            "terminal_proxy.main.pod_manager.get_or_create",
            new=AsyncMock(return_value=terminal),
        ),
        patch("terminal_proxy.main.ensure_chat_dir", new=AsyncMock()) as bootstrap,
    ):
        result = await _create_terminal_for_user("user-1", "chat-9")

    assert result is terminal
    # Reads use the cached bootstrap: ensure_chat_dir(terminal, chat_id, settings, force=False)
    bootstrap.assert_awaited_once_with(terminal, "chat-9", ANY, force=False)


@pytest.mark.asyncio
async def test_create_terminal_for_user_forces_chat_dir_on_spawn() -> None:
    """Terminal creation (POST) forces a fresh bootstrap so a chat dir deleted
    mid-session is recreated before the PTY spawns."""
    from unittest.mock import ANY, AsyncMock, patch

    from terminal_proxy.main import _create_terminal_for_user
    from terminal_proxy.models import PodState, TerminalPod

    terminal = TerminalPod.create("user-1", "key")
    terminal.state = PodState.RUNNING

    with (
        patch("terminal_proxy.main.ensure_k8s_available"),
        patch(
            "terminal_proxy.main.pod_manager.get_or_create",
            new=AsyncMock(return_value=terminal),
        ),
        patch("terminal_proxy.main.ensure_chat_dir", new=AsyncMock()) as bootstrap,
    ):
        await _create_terminal_for_user("user-1", "chat-9", force_chat_dir=True)

    # force=True bypasses the cache (recreates a deleted dir) before the PTY spawns
    bootstrap.assert_awaited_once_with(terminal, "chat-9", ANY, force=True)


# --- /api/config capability discovery ----------------------------------------


def _config_headers() -> dict[str, str]:
    from terminal_proxy.main import PROXY_API_KEY

    return {"Authorization": f"Bearer {PROXY_API_KEY}", "X-User-Id": "user-1"}


def _terminal_stub() -> object:
    from types import SimpleNamespace

    return SimpleNamespace(endpoint="http://pod.test:8000", api_key="pod-key", user_hash="h")


class _Stub:
    """Attribute bag that keeps constructor kwargs (test helper)."""

    def __init__(self, **kw: object) -> None:
        self.__dict__.update(kw)


def test_api_config_requires_auth(client):
    assert client.get("/api/config").status_code == 401


def test_api_config_forwards_pod_features(client):
    from unittest.mock import AsyncMock, patch

    terminal = _terminal_stub()
    pod_response = _Stub(
        raise_for_status=lambda: None,
        json=lambda: {
            "features": {
                "terminal": True,
                "notebooks": True,
                "system": True,
                "git": True,
                "events": True,
                "browser": True,
                "desktop": True,
            }
        },
    )
    http_client = _Stub(get=AsyncMock(return_value=pod_response))

    with (
        patch(
            "terminal_proxy.main.pod_manager.lookup",
            new=AsyncMock(return_value=terminal),
        ),
        patch(
            "terminal_proxy.main.http_proxy.get_client",
            new=AsyncMock(return_value=http_client),
        ),
    ):
        response = client.get("/api/config", headers=_config_headers())

    assert response.status_code == 200
    features = response.json()["features"]
    # The pod's own flags win verbatim (desktop computed by the pod itself)
    assert features["desktop"] is True
    assert features["browser"] is True
    assert features["git"] is True
    # The pod is fetched with its own API key
    assert http_client.get.await_args.args[0] == "http://pod.test:8000/api/config"
    assert http_client.get.await_args.kwargs["headers"]["Authorization"] == "Bearer pod-key"


def test_api_config_falls_back_to_configured_flags_when_pod_unreachable(
    client, monkeypatch
):
    from unittest.mock import AsyncMock, patch

    from terminal_proxy import config as config_module

    monkeypatch.setattr(config_module.settings, "terminal_enable_desktop", False)
    monkeypatch.setattr(config_module.settings, "terminal_enable_browser", True)

    http_client = _Stub(get=AsyncMock(side_effect=RuntimeError("pod down")))

    with (
        patch(
            "terminal_proxy.main.pod_manager.lookup",
            new=AsyncMock(return_value=_terminal_stub()),
        ),
        patch(
            "terminal_proxy.main.http_proxy.get_client",
            new=AsyncMock(return_value=http_client),
        ),
    ):
        response = client.get("/api/config", headers=_config_headers())

    assert response.status_code == 200
    features = response.json()["features"]
    assert features["desktop"] is False
    assert features["browser"] is True
    # git is unknowable without a pod: never advertise it in the fallback
    assert features["git"] is False
    assert features["events"] is True


def test_api_config_does_not_create_pod_in_per_user_mode(client, monkeypatch):
    """Capability polling (/api/config) must be lookup-only in perUser mode:
    it must never get-or-create a pod nor bootstrap a chat dir."""
    from unittest.mock import AsyncMock, patch

    from terminal_proxy import config as config_module
    from terminal_proxy.config import PodMode

    monkeypatch.setattr(config_module.settings, "pod_mode", PodMode.PER_USER)

    with (
        patch(
            "terminal_proxy.main.pod_manager.lookup",
            new=AsyncMock(return_value=None),
        ) as lookup,
        patch(
            "terminal_proxy.main.pod_manager.get_or_create", new=AsyncMock()
        ) as get_or_create,
        patch("terminal_proxy.main.ensure_chat_dir", new=AsyncMock()) as bootstrap,
    ):
        response = client.get("/api/config", headers=_config_headers())

    assert response.status_code == 200
    lookup.assert_awaited_once()
    get_or_create.assert_not_awaited()
    bootstrap.assert_not_awaited()
    # no running pod: the configured fallback flags apply
    features = response.json()["features"]
    assert features["terminal"] is True
    assert features["git"] is False


def test_api_config_without_user_id_uses_fallback(client):
    from terminal_proxy.main import PROXY_API_KEY

    response = client.get("/api/config", headers={"Authorization": f"Bearer {PROXY_API_KEY}"})
    assert response.status_code == 200
    assert "features" in response.json()


# --- catch-all pass-through ---------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_browser_sessions():
    from terminal_proxy.main import _browser_sessions

    _browser_sessions.clear()
    yield
    _browser_sessions.clear()


def _capture_proxy():
    """Patch pod resolution and HTTP forwarding; return the capture list."""
    from fastapi import Response

    captured: list[dict[str, object]] = []

    async def fake_proxy(target_url, request, terminal_api_key, pod_key=None):
        captured.append(
            {
                "url": target_url,
                "api_key": terminal_api_key,
                "pod_key": pod_key,
                "query": str(request.query_params),
            }
        )
        return Response(content=b'{"id": "sess-from-response", "token": "t"}', status_code=200)

    return captured, fake_proxy


def _patch_terminal():
    from unittest.mock import AsyncMock, patch

    return patch(
        "terminal_proxy.main.get_terminal_for_user",
        new=AsyncMock(return_value=_terminal_stub()),
    )


def test_catch_all_requires_auth(client):
    assert client.get("/git/status").status_code == 401
    assert client.post("/desktop/action").status_code == 401


def test_catch_all_requires_user_id(client):
    from terminal_proxy.main import PROXY_API_KEY

    response = client.get("/git/status", headers={"Authorization": f"Bearer {PROXY_API_KEY}"})
    assert response.status_code == 400


def test_catch_all_forwards_path_query_and_pod_key(client):
    captured, fake_proxy = _capture_proxy()
    from unittest.mock import patch

    with (
        _patch_terminal() as _p1,
        patch("terminal_proxy.main.http_proxy.proxy_request", new=fake_proxy),
    ):
        response = client.get(
            "/git/log?since=2026-01-01&max=5", headers=_config_headers()
        )

    assert response.status_code == 200
    assert captured[0]["url"] == "http://pod.test:8000/git/log"
    assert captured[0]["query"] == "since=2026-01-01&max=5"
    assert captured[0]["api_key"] == "pod-key"


def test_catch_all_distinguishes_explicit_routes(client):
    # explicit routes still win over the catch-all (registration order)
    from terminal_proxy.main import PROXY_API_KEY

    # /desktop is explicit and requires X-User-Id (400, not forwarded blindly)
    response = client.get("/desktop", headers={"Authorization": f"Bearer {PROXY_API_KEY}"})
    assert response.status_code == 400


def test_browser_capability_url_bypasses_api_key(client):
    from terminal_proxy.main import _browser_sessions

    _browser_sessions["sess1"] = _terminal_stub()
    captured, fake_proxy = _capture_proxy()
    from unittest.mock import patch

    with patch("terminal_proxy.main.http_proxy.proxy_request", new=fake_proxy):
        # no Authorization / X-User-Id headers at all: the token in the path IS the auth
        response = client.get("/browser/view/sess1/tok/runtime.js")

    assert response.status_code == 200
    assert captured[0]["url"] == "http://pod.test:8000/browser/view/sess1/tok/runtime.js"
    assert captured[0]["api_key"] == "pod-key"


def test_browser_capability_unknown_session_is_404(client):
    response = client.get("/browser/view/unknown-sid/tok/runtime.js")
    assert response.status_code == 404


def test_catch_all_learns_session_from_create_response(client):
    captured, fake_proxy = _capture_proxy()
    from unittest.mock import patch

    from terminal_proxy.main import _browser_sessions

    with (
        _patch_terminal(),
        patch("terminal_proxy.main.http_proxy.proxy_request", new=fake_proxy),
    ):
        created = client.post("/browser/sessions", json={"url": ""}, headers=_config_headers())
        assert created.status_code == 200
        assert "sess-from-response" in _browser_sessions
        # a browser can now load capability URLs without any headers
        loaded = client.get("/browser/view/sess-from-response/tok/frame/https/example.com/index")

    assert loaded.status_code == 200
    assert (
        captured[-1]["url"]
        == "http://pod.test:8000/browser/view/sess-from-response/tok/frame/https/example.com/index"
    )


def test_catch_all_forgets_session_on_delete(client):
    from terminal_proxy.main import _browser_sessions

    _browser_sessions["sess1"] = _terminal_stub()
    _, fake_proxy = _capture_proxy()
    from unittest.mock import patch

    with (
        _patch_terminal(),
        patch("terminal_proxy.main.http_proxy.proxy_request", new=fake_proxy),
    ):
        response = client.delete("/browser/sessions/sess1", headers=_config_headers())

    assert response.status_code == 200
    assert "sess1" not in _browser_sessions


def test_catch_all_registers_sessions_from_list_response(client):
    from unittest.mock import patch

    from fastapi import Response

    from terminal_proxy.main import _browser_sessions

    async def fake_proxy(target_url, request, terminal_api_key, pod_key=None):
        return Response(
            content=b'{"current_session_id": "a", "sessions": [{"id": "a"}, {"id": "b"}]}',
            status_code=200,
        )

    with (
        _patch_terminal(),
        patch("terminal_proxy.main.http_proxy.proxy_request", new=fake_proxy),
    ):
        assert client.get("/browser/sessions", headers=_config_headers()).status_code == 200

    assert "a" in _browser_sessions and "b" in _browser_sessions


# --- catch-all WebSocket ------------------------------------------------------


def test_catch_all_ws_registers_active_connection(client):
    from unittest.mock import AsyncMock, patch

    from terminal_proxy.main import PROXY_API_KEY
    from terminal_proxy.models import TerminalPod

    terminal = TerminalPod.create("user-1", "pod-key")

    async def fake_proxy(client_ws, terminal, path, **kwargs):
        assert path == "/desktop/stream"
        assert terminal.active_connections == 1  # registered while in use
        await client_ws.send_text("upstream-ready")

    with (
        patch("terminal_proxy.main.get_terminal_for_user", new=AsyncMock(return_value=terminal)),
        patch("terminal_proxy.main.ws_proxy.proxy_websocket", new=fake_proxy),
        client.websocket_connect("/desktop/stream?user_id=user-1") as ws,
    ):
        ws.send_json({"type": "auth", "token": PROXY_API_KEY})
        assert ws.receive_text() == "upstream-ready"

    assert terminal.active_connections == 0  # deregistered after close


def test_catch_all_ws_rejects_bad_auth(client):
    with client.websocket_connect("/events?user_id=user-1") as ws:
        ws.send_json({"type": "auth", "token": "wrong"})
        frame = ws.receive()
    assert frame["type"] == "websocket.close"
    assert frame["code"] == 1008


def test_capability_ws_skips_auth_and_registers(client):
    from unittest.mock import patch

    from terminal_proxy.main import _browser_sessions
    from terminal_proxy.models import TerminalPod

    terminal = TerminalPod.create("user-1", "pod-key")
    _browser_sessions["s1"] = terminal
    seen: dict[str, object] = {}

    async def fake_proxy(client_ws, terminal, path, **kwargs):
        seen["path"] = path
        seen["send_auth"] = kwargs.get("send_auth")
        assert terminal.active_connections == 1

    with (
        patch("terminal_proxy.main.ws_proxy.proxy_websocket", new=fake_proxy),
        # NO auth message: capability sockets must connect directly
        client.websocket_connect("/browser/view/s1/tok/ws"),
    ):
        pass

    assert seen["path"] == "/browser/view/s1/tok/ws"
    assert seen["send_auth"] is False
    assert terminal.active_connections == 0


def test_capability_ws_unknown_session_closed(client):
    with client.websocket_connect("/browser/view/nope/tok/ws") as ws:
        frame = ws.receive()
    assert frame["type"] == "websocket.close"
    assert frame["code"] == 4004
