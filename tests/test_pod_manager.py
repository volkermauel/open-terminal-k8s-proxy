"""Tests for pod manager."""

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
        mock.touch_pvc.return_value = None
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
async def test_get_or_create_returns_existing(pod_manager, mock_storage_manager):
    existing = TerminalPod.create("user-123", "api-key")
    existing.state = PodState.RUNNING
    existing.pod_ip = "10.0.0.1"
    pod_manager._pods[existing.user_hash] = existing

    result = await pod_manager.get_or_create("user-123")

    assert result == existing
    mock_storage_manager.touch_pvc.assert_called_once_with(existing.pvc_name)


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


@pytest.mark.asyncio
async def test_get_or_create_none_mode_no_pvc(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.NONE,
    )
    pm = PodManager(cfg)

    result = await pm.get_or_create("none-user")

    assert result.pvc_name is None
    mock_storage_manager.create_user_pvc.assert_not_called()
    mock_k8s_client.create_pod.assert_called_once()

    # No volumes should be present
    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    volumes = pod_manifest["spec"]["volumes"]
    assert volumes == []


@pytest.mark.asyncio
async def test_terminal_pod_gets_tolerations(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.NONE,
        terminal_tolerations=[{"key": "foo", "value": "bar", "effect": "baz"}],
        terminal_node_selector={"kubernetes.io/hostname": "foobar"},
    )
    pm = PodManager(cfg)

    await pm.get_or_create("tol-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    assert pod_manifest["spec"]["tolerations"] == [{"key": "foo", "value": "bar", "effect": "baz"}]
    assert pod_manifest["spec"]["nodeSelector"] == {"kubernetes.io/hostname": "foobar"}


@pytest.mark.asyncio
async def test_terminal_pod_no_tolerations_by_default(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.NONE,
    )
    pm = PodManager(cfg)

    await pm.get_or_create("notol-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    assert "tolerations" not in pod_manifest["spec"]
    assert "nodeSelector" not in pod_manifest["spec"]


@pytest.mark.asyncio
async def test_ephemeral_storage_in_container_resources(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.NONE,
        terminal_ephemeral_storage_request="5Gi",
        terminal_ephemeral_storage_limit="5Gi",
    )
    pm = PodManager(cfg)

    await pm.get_or_create("eph-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    container = pod_manifest["spec"]["containers"][0]
    assert container["resources"]["requests"]["ephemeral-storage"] == "5Gi"
    assert container["resources"]["limits"]["ephemeral-storage"] == "5Gi"


@pytest.mark.asyncio
async def test_ephemeral_storage_disabled_when_empty(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.NONE,
        terminal_ephemeral_storage_request="",
        terminal_ephemeral_storage_limit="",
    )
    pm = PodManager(cfg)

    await pm.get_or_create("noeph-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    container = pod_manifest["spec"]["containers"][0]
    assert "ephemeral-storage" not in container["resources"]["requests"]
    assert "ephemeral-storage" not in container["resources"]["limits"]


@pytest.mark.asyncio
async def test_ephemeral_storage_with_pvc_mode(mock_k8s_client, mock_storage_manager):
    """Ephemeral-storage limits should apply regardless of storage mode."""
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.PER_USER,
        terminal_ephemeral_storage_request="3Gi",
        terminal_ephemeral_storage_limit="6Gi",
    )
    pm = PodManager(cfg)

    await pm.get_or_create("pvc-eph-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    container = pod_manifest["spec"]["containers"][0]
    # PVC volume should be present
    volumes = pod_manifest["spec"]["volumes"]
    assert any("persistentVolumeClaim" in v for v in volumes)
    # AND ephemeral-storage limits should also be present
    assert container["resources"]["requests"]["ephemeral-storage"] == "3Gi"
    assert container["resources"]["limits"]["ephemeral-storage"] == "6Gi"


@pytest.mark.asyncio
async def test_delete_pod_retains_pvc_when_configured(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.PER_USER,
        storage_retain_pvc=True,
    )
    pm = PodManager(cfg)
    terminal = TerminalPod.create("retain-user", "key")
    terminal.state = PodState.RUNNING
    pm._pods[terminal.user_hash] = terminal

    await pm._delete_pod(terminal.user_hash)

    mock_storage_manager.delete_user_pvc.assert_not_called()
    mock_storage_manager.touch_pvc.assert_called_once_with(terminal.pvc_name)
    mock_k8s_client.delete_pod.assert_called_once()
    mock_k8s_client.delete_service.assert_called_once()
    mock_k8s_client.delete_secret.assert_called_once()


@pytest.mark.asyncio
async def test_delete_pod_deletes_pvc_when_not_retained(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.PER_USER,
        storage_retain_pvc=False,
    )
    pm = PodManager(cfg)
    terminal = TerminalPod.create("delete-user", "key")
    terminal.state = PodState.RUNNING
    pm._pods[terminal.user_hash] = terminal

    await pm._delete_pod(terminal.user_hash)

    mock_storage_manager.delete_user_pvc.assert_called_once_with(terminal.pvc_name)


@pytest.mark.asyncio
async def test_cleanup_idle_pods_retains_pvc(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.PER_USER,
        pod_idle_timeout_seconds=300,
        storage_retain_pvc=True,
    )
    pm = PodManager(cfg)
    old_pod = TerminalPod.create("idle-user", "key")
    old_pod.last_active_at = datetime.utcnow() - timedelta(seconds=400)
    pm._pods[old_pod.user_hash] = old_pod

    await pm._cleanup_idle_pods()

    assert old_pod.user_hash not in pm._pods
    mock_k8s_client.delete_pod.assert_called_once()
    mock_storage_manager.delete_user_pvc.assert_not_called()


@pytest.mark.asyncio
async def test_get_or_create_retains_pvc_on_recycle(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.PER_USER,
        storage_retain_pvc=True,
    )
    pm = PodManager(cfg)
    failed = TerminalPod.create("recycle-user", "old-key")
    failed.state = PodState.FAILED
    pm._pods[failed.user_hash] = failed

    result = await pm.get_or_create("recycle-user")

    mock_storage_manager.delete_user_pvc.assert_not_called()
    mock_storage_manager.touch_pvc.assert_called_with(failed.pvc_name)
    assert result.pvc_name is not None
    mock_k8s_client.create_pod.assert_called_once()


@pytest.mark.asyncio
async def test_home_env_set_when_pvc_mounted(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.PER_USER,
    )
    pm = PodManager(cfg)

    await pm.get_or_create("home-pvc-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    env = pod_manifest["spec"]["containers"][0]["env"]
    env_names = {e["name"] for e in env}
    assert "HOME" in env_names
    assert next(e for e in env if e["name"] == "HOME")["value"] == "/data"


@pytest.mark.asyncio
async def test_home_env_not_set_without_pvc(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.NONE,
    )
    pm = PodManager(cfg)

    await pm.get_or_create("no-home-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    env = pod_manifest["spec"]["containers"][0]["env"]
    env_names = {e["name"] for e in env}
    assert "HOME" not in env_names


@pytest.mark.asyncio
async def test_security_context_set_when_pvc_mounted(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.PER_USER,
    )
    pm = PodManager(cfg)

    await pm.get_or_create("fsgroup-pvc-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    sec_ctx = pod_manifest["spec"]["securityContext"]
    assert sec_ctx["fsGroup"] == 1000
    assert sec_ctx["fsGroupChangePolicy"] == "Always"


@pytest.mark.asyncio
async def test_security_context_not_set_without_pvc(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.NONE,
    )
    pm = PodManager(cfg)

    await pm.get_or_create("no-fsgroup-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    assert "securityContext" not in pod_manifest["spec"]
