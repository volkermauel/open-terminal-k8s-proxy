"""Tests for data models."""

from datetime import datetime

from terminal_proxy.models import (
    PodState,
    TerminalPod,
    user_id_to_hash,
    sanitize_k8s_name,
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
    assert terminal.pvc_name.startswith("pvc-")
    assert isinstance(terminal.created_at, datetime)
    assert isinstance(terminal.last_active_at, datetime)


def test_terminal_pod_endpoint():
    terminal = TerminalPod(
        user_id="test",
        user_hash="abc123",
        pod_name="terminal-abc123",
        pvc_name="pvc-abc123",
        api_key="key",
        state=PodState.RUNNING,
        created_at=datetime.utcnow(),
        last_active_at=datetime.utcnow(),
        pod_ip="10.0.0.1",
    )
    
    assert terminal.endpoint == "http://10.0.0.1:8000"
    
    terminal_no_ip = TerminalPod(
        user_id="test",
        user_hash="abc123",
        pod_name="terminal-abc123",
        pvc_name="pvc-abc123",
        api_key="key",
        state=PodState.RUNNING,
        created_at=datetime.utcnow(),
        last_active_at=datetime.utcnow(),
    )
    
    assert "terminal-abc123" in terminal_no_ip.endpoint
