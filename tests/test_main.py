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

    body_props = paths["/files/replace"]["post"]["requestBody"]["content"]["application/json"]["schema"]
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


def test_openapi_catch_all_files_route_hidden(client):
    spec = _get_openapi(client)
    paths = spec["paths"]
    catch_all = [
        p for p in paths
        if p.startswith("/files/") and "{" not in p and p not in (
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
        "Catch-all /files/{path} should be hidden from schema. "
        f"Found: {catch_all}"
    )
