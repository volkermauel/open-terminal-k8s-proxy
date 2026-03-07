"""Tests for pod manager."""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from terminal_proxy.config import Settings, StorageMode
from terminal_proxy.models import PodState, TerminalPod
from terminal_proxy.pod_manager import PodManager


@pytest.fixture
def settings():
    return Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        max_concurrent_pods=10,
        pod_idle_timeout_seconds=300,
        pod_startup_timeout_seconds=60,
        storage_mode=StorageMode.PER_USER,
    )


@pytest.fixture
def pod_manager(settings):
    return PodManager(settings)


@pytest.fixture
def mock_k8s_client():
    with patch("terminal_proxy.pod_manager.k8s_client") as mock:
        mock.list_terminal_pods.return_value = MagicMock(items=[])
        mock.create_pod.return_value = MagicMock(metadata=MagicMock(name="terminal-test"))
        mock.wait_for_pod_ready = AsyncMock(return_value=(True, "10.0.0.1"))
        yield mock


@pytest.fixture
def mock_storage_manager():
    with patch("terminal_proxy.pod_manager.storage_manager") as mock:
        mock.create_user_pvc.return_value = True
        mock.delete_user_pvc.return_value = None
        yield mock


@pytest.mark.asyncio
async def test_start_reconciles_existing_pods(pod_manager, mock_k8s_client):
    mock_pod = MagicMock()
    mock_pod.metadata.labels = {"user-id-hash": "abc123"}
    mock_pod.metadata.name = "terminal-abc123"
    mock_pod.metadata.creation_timestamp = datetime.utcnow()
    mock_pod.status.phase = "Running"
    mock_pod.status.pod_ip = "10.0.0.1"
    
    mock_k8s_client.list_terminal_pods.return_value = MagicMock(items=[mock_pod])
    
    await pod_manager.start()
    
    assert "abc123" in pod_manager._pods
    assert pod_manager._pods["abc123"].state == PodState.RUNNING
    assert pod_manager._pods["abc123"].pod_ip == "10.0.0.1"


@pytest.mark.asyncio
async def test_get_or_create_returns_existing(pod_manager):
    existing = TerminalPod.create("user-123", "api-key")
    existing.state = PodState.RUNNING
    existing.pod_ip = "10.0.0.1"
    pod_manager._pods[existing.user_hash] = existing
    
    result = await pod_manager.get_or_create("user-123")
    
    assert result == existing


@pytest.mark.asyncio
async def test_get_or_create_creates_new(pod_manager, mock_k8s_client, mock_storage_manager):
    result = await pod_manager.get_or_create("new-user")
    
    assert result.user_hash in pod_manager._pods
    mock_k8s_client.create_pod.assert_called_once()


@pytest.mark.asyncio
async def test_get_or_create_enforces_max_pods(pod_manager, mock_k8s_client, mock_storage_manager):
    pod_manager.cfg.max_concurrent_pods = 2
    
    for i in range(3):
        mock_k8s_client.wait_for_pod_ready.return_value = (True, f"10.0.0.{i}")
        await pod_manager.get_or_create(f"user-{i}")
    
    assert len(pod_manager._pods) == 2


@pytest.mark.asyncio
async def test_cleanup_idle_pods(pod_manager, mock_k8s_client):
    old_pod = TerminalPod.create("old-user", "key")
    old_pod.last_active_at = datetime.utcnow() - timedelta(seconds=400)
    pod_manager._pods[old_pod.user_hash] = old_pod
    
    recent_pod = TerminalPod.create("recent-user", "key")
    recent_pod.last_active_at = datetime.utcnow() - timedelta(seconds=100)
    pod_manager._pods[recent_pod.user_hash] = recent_pod
    
    await pod_manager._cleanup_idle_pods()
    
    assert old_pod.user_hash not in pod_manager._pods
    assert recent_pod.user_hash in pod_manager._pods
    mock_k8s_client.delete_pod.assert_called_once()


def test_get_stats(pod_manager):
    pod_manager._pods["user1"] = TerminalPod.create("user1", "key")
    pod_manager._pods["user2"] = TerminalPod.create("user2", "key")
    
    stats = pod_manager.get_stats()
    
    assert stats["active_pods"] == 2
    assert stats["max_pods"] == pod_manager.cfg.max_concurrent_pods
    assert len(stats["pods"]) == 2
