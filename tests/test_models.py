"""Tests for data models."""

from datetime import datetime

from terminal_proxy.models import (
    PodState,
    TerminalPod,
    sanitize_k8s_name,
    user_id_to_hash,
)


def test_user_id_to_hash_consistent():
    user_id = "test-user-123"
    hash1 = user_id_to_hash(user_id)
    hash2 = user_id_to_hash(user_id)
    assert hash1 == hash2
    assert len(hash1) == 12


def test_user_id_to_hash_different():
    hash1 = user_id_to_hash("user1")
    hash2 = user_id_to_hash("user2")
    assert hash1 != hash2


def test_sanitize_k8s_name():
    assert sanitize_k8s_name("My-App-Name") == "my-app-name"
    assert sanitize_k8s_name("test___user") == "test-user"
    assert sanitize_k8s_name("-leading-trailing-") == "leading-trailing"
    assert sanitize_k8s_name("a" * 100) == "a" * 63


def test_terminal_pod_create():
    terminal = TerminalPod.create("user-123", "api-key-456")

    assert terminal.user_id == "user-123"
    assert terminal.api_key == "api-key-456"
    assert terminal.state == PodState.CREATING
    assert terminal.pod_name.startswith("terminal-")
    assert terminal.service_name.startswith("terminal-")
    assert terminal.secret_name.startswith("terminal-secret-")
    assert terminal.pvc_name.startswith("pvc-")
    assert isinstance(terminal.created_at, datetime)
    assert isinstance(terminal.last_active_at, datetime)


def test_terminal_pod_endpoint():
    terminal = TerminalPod(
        user_id="test",
        user_hash="abc123",
        pod_name="terminal-abc123",
        service_name="terminal-abc123",
        secret_name="terminal-secret-abc123",
        pvc_name="pvc-abc123",
        api_key="key",
        state=PodState.RUNNING,
        created_at=datetime.utcnow(),
        last_active_at=datetime.utcnow(),
        pod_ip="10.0.0.1",
    )

    assert terminal.endpoint == "http://terminal-abc123:8000"

    terminal_no_ip = TerminalPod(
        user_id="test",
        user_hash="abc123",
        pod_name="terminal-abc123",
        service_name="terminal-abc123",
        secret_name="terminal-secret-abc123",
        pvc_name="pvc-abc123",
        api_key="key",
        state=PodState.RUNNING,
        created_at=datetime.utcnow(),
        last_active_at=datetime.utcnow(),
    )

    assert terminal_no_ip.endpoint == "http://terminal-abc123:8000"


def test_chat_id_to_hash():
    from terminal_proxy.models import chat_id_to_hash

    h = chat_id_to_hash("chat-1")
    assert len(h) == 12
    assert h != chat_id_to_hash("chat-2")


def test_sanitize_chat_id_safe_path_component():
    from terminal_proxy.models import sanitize_chat_id

    assert sanitize_chat_id("chat-42") == "chat-42"
    assert sanitize_chat_id("../x") == "x"
    assert sanitize_chat_id("a/b?c=1") == "a-b-c-1"
    all_dots = sanitize_chat_id("....")
    assert all_dots and all_dots not in (".", "..") and "/" not in all_dots
    assert "/" not in sanitize_chat_id("../etc/passwd")
    assert sanitize_chat_id("")  # non-empty hash fallback
    assert len(sanitize_chat_id("x" * 200)) <= 64


def test_terminal_pod_create_chat_mode():
    t = TerminalPod.create("user-1", "api-key", "chat-1")
    assert t.is_chat_pod
    assert t.chat_hash and t.chat_id == "chat-1"
    assert t.pvc_name is None
    assert len(t.pod_name) <= 63

    user_only = TerminalPod.create("user-1", "api-key")
    assert not user_only.is_chat_pod
    assert user_only.pvc_name and user_only.chat_hash is None
