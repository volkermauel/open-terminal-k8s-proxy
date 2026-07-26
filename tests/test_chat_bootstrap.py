"""Tests for per-chat working-directory bootstrap."""

from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from terminal_proxy.chat_bootstrap import ensure_chat_dir
from terminal_proxy.config import Settings, StorageMode
from terminal_proxy.models import PodState, TerminalPod


def _terminal() -> TerminalPod:
    return TerminalPod(
        user_id="u",
        user_hash="h",
        pod_name="terminal-h",
        service_name="terminal-h",
        secret_name="terminal-secret-h",
        pvc_name=None,
        api_key="key",
        state=PodState.RUNNING,
        created_at=datetime.utcnow(),
        last_active_at=datetime.utcnow(),
        pod_ip="10.0.0.1",
    )


def _resp(status: int = 200, payload: dict[str, Any] | None = None) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload or {}
    r.text = ""
    return r


def _patch_client() -> tuple[MagicMock, Any]:
    """Patch httpx.AsyncClient; return (client_mock, patch_context_manager)."""
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    client.post = AsyncMock(return_value=_resp(200))
    ctx = patch("terminal_proxy.chat_bootstrap.httpx.AsyncClient", return_value=client)
    return client, ctx


@pytest.mark.asyncio
async def test_ensure_chat_dir_mkdir_then_setcwd_per_session() -> None:
    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.PER_USER)
    client, ctx = _patch_client()
    with ctx:
        await ensure_chat_dir(_terminal(), "chat-42", cfg)

    # mkdir then set_cwd (with X-Session-Id header); no GET needed -- home is known
    assert client.get.call_count == 0
    assert client.post.call_count == 2
    mkdir, setcwd = client.post.call_args_list
    assert mkdir.args[0].endswith("/files/mkdir")
    assert mkdir.kwargs["json"]["path"] == "/data/chat-42"
    assert setcwd.args[0].endswith("/files/cwd")
    assert setcwd.kwargs["headers"].get("X-Session-Id") == "chat-42"
    assert setcwd.kwargs["json"]["path"] == "/data/chat-42"


@pytest.mark.asyncio
async def test_ensure_chat_dir_disabled_is_noop() -> None:
    cfg = Settings(
        proxy_api_key="k", storage_mode=StorageMode.PER_USER, per_chat_dirs_enabled=False
    )
    with patch("terminal_proxy.chat_bootstrap.httpx.AsyncClient") as mc:
        await ensure_chat_dir(_terminal(), "chat-1", cfg)
        mc.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_chat_dir_no_chat_id_is_noop() -> None:
    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.PER_USER)
    with patch("terminal_proxy.chat_bootstrap.httpx.AsyncClient") as mc:
        await ensure_chat_dir(_terminal(), "", cfg)
        mc.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_chat_dir_sanitizes_path() -> None:
    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.PER_USER)
    client, ctx = _patch_client()
    with ctx:
        await ensure_chat_dir(_terminal(), "../evil?x=1", cfg)
    path = client.post.call_args_list[0].kwargs["json"]["path"]
    assert path.startswith("/data/")
    rest = path[len("/data/") :]
    assert "/" not in rest and rest not in (".", "..")


@pytest.mark.asyncio
async def test_ensure_chat_dir_is_idempotent_and_best_effort() -> None:
    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.PER_USER)
    client, ctx = _patch_client()
    # mkdir already-exists (400) should not abort; set_cwd still runs
    client.post = AsyncMock(side_effect=[_resp(400), _resp(200)])
    with ctx:
        await ensure_chat_dir(_terminal(), "chat-1", cfg)
    assert client.post.call_count == 2


@pytest.mark.asyncio
async def test_ensure_chat_dir_cached_skips_repeat_call() -> None:
    """Second call for the same (pod, chat) hits the cache and skips the HTTP calls."""
    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.PER_USER)
    client, ctx = _patch_client()
    terminal = _terminal()
    with ctx:
        await ensure_chat_dir(terminal, "chat-1", cfg)
        await ensure_chat_dir(terminal, "chat-1", cfg)  # cached -> skipped
    assert client.post.call_count == 2  # only the first call hit the pod
    assert "chat-1" in terminal.bootstrapped_chats


@pytest.mark.asyncio
async def test_ensure_chat_dir_cache_is_per_chat() -> None:
    """Each distinct chat_id is bootstrapped once; repeats are served from cache."""
    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.PER_USER)
    client, ctx = _patch_client()
    terminal = _terminal()
    with ctx:
        await ensure_chat_dir(terminal, "chat-a", cfg)
        await ensure_chat_dir(terminal, "chat-b", cfg)
        await ensure_chat_dir(terminal, "chat-a", cfg)  # cached
    assert client.post.call_count == 4  # 2 calls per distinct chat
    assert {"chat-a", "chat-b"} <= set(terminal.bootstrapped_chats)


@pytest.mark.asyncio
async def test_ensure_chat_dir_perchat_mode_is_noop() -> None:
    """perChat pods are launched in their chat dir; bootstrap must not call the pod."""
    from terminal_proxy.config import PodMode

    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.SHARED, pod_mode=PodMode.PER_CHAT)
    with patch("terminal_proxy.chat_bootstrap.httpx.AsyncClient") as mc:
        await ensure_chat_dir(_terminal(), "chat-1", cfg)
        mc.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_chat_dir_failed_setcwd_is_not_cached() -> None:
    """A failed set_cwd must not be cached so the next request retries."""
    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.PER_USER)
    client, ctx = _patch_client()
    # call 1: mkdir 200, set_cwd 500 -> not cached; call 2: mkdir 200, set_cwd 200 -> cached
    client.post = AsyncMock(side_effect=[_resp(200), _resp(500), _resp(200), _resp(200)])
    terminal = _terminal()
    with ctx:
        await ensure_chat_dir(terminal, "chat-1", cfg)
        await ensure_chat_dir(terminal, "chat-1", cfg)  # retries: first call did not cache
    assert client.post.call_count == 4
    assert "chat-1" in terminal.bootstrapped_chats


@pytest.mark.asyncio
async def test_ensure_chat_dir_force_bypasses_cache() -> None:
    """force=True re-runs mkdir+set_cwd even when the chat is cached, recreating a
    chat dir deleted mid-session before the PTY spawns."""
    cfg = Settings(proxy_api_key="k", storage_mode=StorageMode.PER_USER)
    client, ctx = _patch_client()
    terminal = _terminal()
    with ctx:
        await ensure_chat_dir(terminal, "chat-1", cfg)              # bootstrap + cache
        await ensure_chat_dir(terminal, "chat-1", cfg)              # cached -> skipped
        await ensure_chat_dir(terminal, "chat-1", cfg, force=True)  # forced -> re-runs
    assert client.post.call_count == 4  # 2 (first) + 0 (cached) + 2 (forced)
    assert "chat-1" in terminal.bootstrapped_chats
