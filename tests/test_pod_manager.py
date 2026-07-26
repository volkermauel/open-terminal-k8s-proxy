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
        # self-healing create path defaults (fresh create; override per-test)
        mock.create_or_get_secret.return_value = (MagicMock(), True)
        mock.create_or_get_service.return_value = MagicMock()
        mock.get_pod.return_value = None
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
async def test_home_and_cwd_set_when_pvc_mounted(mock_k8s_client, mock_storage_manager):
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
    args = pod_manifest["spec"]["containers"][0].get("args", [])
    assert args == ["run", "--cwd", "/data"]


async def test_no_home_no_cwd_without_pvc(mock_k8s_client, mock_storage_manager):
    cfg = Settings(
        proxy_api_key="test-key",
        namespace="test-ns",
        storage_mode=StorageMode.NONE,
    )
    pm = PodManager(cfg)

    await pm.get_or_create("no-home-user")

    pod_manifest = mock_k8s_client.create_pod.call_args[0][0]
    container = pod_manifest["spec"]["containers"][0]
    env_names = {e["name"] for e in container["env"]}
    # none mode: open-terminal uses its image default; no --cwd / HOME override
    assert "HOME" not in env_names
    assert "args" not in container


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


@pytest.mark.asyncio
async def test_get_or_create_perchat_provisions_one_pod_per_chat(
    mock_k8s_client, mock_storage_manager
):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        max_concurrent_pods=10,
        storage_mode=StorageMode.SHARED,
        pod_mode=PodMode.PER_CHAT,
    )
    pm = PodManager(cfg)
    a = await pm.get_or_create("user-1", "chat-a")
    b = await pm.get_or_create("user-1", "chat-b")
    assert a.pod_name != b.pod_name
    assert a.is_chat_pod and b.is_chat_pod
    assert len(pm._pods) == 2


@pytest.mark.asyncio
async def test_perchat_evicts_oldest_user_pod_at_per_user_cap(
    mock_k8s_client, mock_storage_manager
):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        max_concurrent_pods=100,
        storage_mode=StorageMode.SHARED,
        pod_mode=PodMode.PER_CHAT,
        max_pods_per_user=2,
    )
    pm = PodManager(cfg)
    a = await pm.get_or_create("user-1", "chat-a")
    b = await pm.get_or_create("user-1", "chat-b")
    assert len(pm._pods) == 2
    # mark 'a' older so it is the one evicted when the cap is hit
    a_key = f"{a.user_hash}-{a.chat_hash}"
    pm._pods[a_key].last_active_at = datetime.utcnow() - timedelta(seconds=1000)
    # third chat -> per-user cap (2) reached -> oldest ('a') evicted
    c = await pm.get_or_create("user-1", "chat-c")
    assert len(pm._pods) == 2
    assert a_key not in pm._pods
    assert c.pod_name not in (a.pod_name, b.pod_name)


@pytest.mark.asyncio
async def test_perchat_per_user_cap_does_not_touch_other_users(
    mock_k8s_client, mock_storage_manager
):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        max_concurrent_pods=100,
        storage_mode=StorageMode.SHARED,
        pod_mode=PodMode.PER_CHAT,
        max_pods_per_user=1,
    )
    pm = PodManager(cfg)
    await pm.get_or_create("user-1", "chat-a")
    u2 = await pm.get_or_create("user-2", "chat-a")
    # user-1 hitting their cap (1) must not evict user-2's pod
    await pm.get_or_create("user-1", "chat-b")
    u2_key = f"{u2.user_hash}-{u2.chat_hash}"
    assert u2_key in pm._pods
    assert len([t for t in pm._pods.values() if t.user_hash == u2.user_hash]) == 1


@pytest.mark.asyncio
async def test_get_or_create_peruser_ignores_chat_id(mock_k8s_client, mock_storage_manager):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        max_concurrent_pods=10,
        storage_mode=StorageMode.PER_USER,
        pod_mode=PodMode.PER_USER,
    )
    pm = PodManager(cfg)
    await pm.get_or_create("user-1", "chat-a")
    await pm.get_or_create("user-1", "chat-b")
    assert len(pm._pods) == 1  # same user pod regardless of chat


@pytest.mark.asyncio
async def test_perchat_falls_back_to_user_pod_when_no_session(
    mock_k8s_client, mock_storage_manager
):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        max_concurrent_pods=10,
        storage_mode=StorageMode.SHARED,
        pod_mode=PodMode.PER_CHAT,
    )
    pm = PodManager(cfg)
    # no chat_id -> per_chat False -> keyed by user hash (a single pod)
    t = await pm.get_or_create("user-1", None)
    assert not t.is_chat_pod
    assert len(pm._pods) == 1


@pytest.mark.asyncio
async def test_reconcile_rebuilds_chat_pod(mock_k8s_client):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        storage_mode=StorageMode.SHARED,
        pod_mode=PodMode.PER_CHAT,
    )
    pm = PodManager(cfg)
    mock_pod = MagicMock()
    mock_pod.metadata.labels = {"user-id-hash": "abc123", "chat-id-hash": "def456"}
    mock_pod.metadata.annotations = {"chat-slug": "chat-x"}
    mock_pod.metadata.name = "terminal-abc123-def456"
    mock_pod.metadata.creation_timestamp = datetime.utcnow()
    mock_pod.status.phase = "Running"
    mock_pod.status.pod_ip = "10.0.0.1"
    mock_k8s_client.list_terminal_pods.return_value = MagicMock(items=[mock_pod])

    await pm._reconcile_existing_pods()

    assert "abc123-def456" in pm._pods
    t = pm._pods["abc123-def456"]
    assert t.is_chat_pod and t.chat_id == "chat-x"


@pytest.mark.asyncio
async def test_cleanup_idle_uses_chat_timeout_for_chat_pods(mock_k8s_client):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        storage_mode=StorageMode.SHARED,
        pod_mode=PodMode.PER_CHAT,
        pod_idle_timeout_seconds=9999,
        chat_pod_idle_timeout_seconds=10,
        pod_cleanup_interval_seconds=9999,
    )
    pm = PodManager(cfg)
    chat = TerminalPod.create("u", "k", "chat-1")
    chat.state = PodState.RUNNING
    chat.last_active_at = datetime.utcnow() - timedelta(seconds=100)
    user = TerminalPod.create("u", "k")
    user.state = PodState.RUNNING
    user.last_active_at = datetime.utcnow() - timedelta(seconds=100)
    chat_key = f"{chat.user_hash}-{chat.chat_hash}"
    pm._pods[chat_key] = chat
    pm._pods[user.user_hash] = user

    await pm._cleanup_idle_pods()

    assert chat_key not in pm._pods  # evicted (over short chat timeout)
    assert user.user_hash in pm._pods  # retained (under long user timeout)


# ---------------------------------------------------------------------------
# Self-healing resource creation (secret reuse, pod adoption, dead-leftover cleanup)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_reuses_leftover_secret_and_adopts_key(mock_k8s_client, mock_storage_manager):
    cfg = Settings(proxy_api_key="k", namespace="ns", storage_mode=StorageMode.PER_USER)
    pm = PodManager(cfg)
    pm._get_api_key_from_secret = MagicMock(return_value="adopted-key")
    mock_k8s_client.create_or_get_secret.return_value = (MagicMock(), False)  # leftover secret
    mock_k8s_client.get_pod.return_value = None

    t = await pm.get_or_create("user-1")

    assert t.api_key == "adopted-key"  # adopted the existing secret's key
    mock_k8s_client.create_pod.assert_called_once()  # fresh pod still created


@pytest.mark.asyncio
async def test_create_adopts_live_pod_and_skips_create(mock_k8s_client, mock_storage_manager):
    cfg = Settings(proxy_api_key="k", namespace="ns", storage_mode=StorageMode.PER_USER)
    pm = PodManager(cfg)
    live = MagicMock()
    live.status.phase = "Running"
    mock_k8s_client.get_pod.return_value = live

    await pm.get_or_create("user-1")

    mock_k8s_client.create_pod.assert_not_called()  # adopted the live leftover


@pytest.mark.asyncio
async def test_create_removes_dead_leftover_then_creates(mock_k8s_client, mock_storage_manager):
    cfg = Settings(proxy_api_key="k", namespace="ns", storage_mode=StorageMode.PER_USER)
    pm = PodManager(cfg)
    dead = MagicMock()
    dead.status.phase = "Failed"
    mock_k8s_client.get_pod.return_value = dead

    await pm.get_or_create("user-1")

    mock_k8s_client.delete_pod.assert_called_once()  # removed the dead leftover
    mock_k8s_client.create_pod.assert_called_once()  # then created a fresh pod


# ---------------------------------------------------------------------------
# perUserPerChat mode: per-chat pod + dedicated per-user RWX PVC
# ---------------------------------------------------------------------------


def test_peruserperchat_requires_peruser_storage():
    from pydantic import ValidationError

    from terminal_proxy.config import PodMode

    with pytest.raises(ValidationError):
        Settings(
            proxy_api_key="k",
            pod_mode=PodMode.PER_USER_PER_CHAT,
            storage_mode=StorageMode.SHARED,
        )


@pytest.mark.asyncio
async def test_peruserperchat_creates_per_chat_pod_with_per_user_pvc(mock_k8s_client, mock_storage_manager):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        max_concurrent_pods=100,
        storage_mode=StorageMode.PER_USER,
        pod_mode=PodMode.PER_USER_PER_CHAT,
    )
    pm = PodManager(cfg)
    a = await pm.get_or_create("user-1", "chat-a")
    b = await pm.get_or_create("user-1", "chat-b")

    assert a.is_chat_pod and b.is_chat_pod and a.pod_name != b.pod_name
    # both chat pods share ONE per-user PVC
    assert a.pvc_name == b.pvc_name == f"pvc-{a.user_hash}"
    assert len(pm._pods) == 2
    mock_storage_manager.create_user_pvc.assert_called_with(f"pvc-{a.user_hash}", a.user_hash)


@pytest.mark.asyncio
async def test_peruserperchat_pvc_refcount_keeps_pvc_until_last_chat(
    mock_k8s_client, mock_storage_manager
):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        storage_mode=StorageMode.PER_USER,
        pod_mode=PodMode.PER_USER_PER_CHAT,
        storage_retain_pvc=False,  # delete PVC on removal
    )
    pm = PodManager(cfg)
    a = await pm.get_or_create("user-1", "chat-a")
    b = await pm.get_or_create("user-1", "chat-b")
    a_key = f"{a.user_hash}-{a.chat_hash}"
    b_key = f"{b.user_hash}-{b.chat_hash}"

    # Removing one chat pod must keep the shared per-user PVC (sibling still uses it).
    await pm._delete_pod(a_key)
    mock_storage_manager.delete_user_pvc.assert_not_called()
    mock_storage_manager.touch_pvc.assert_called()

    # Removing the last chat pod now deletes the PVC.
    await pm._delete_pod(b_key)
    mock_storage_manager.delete_user_pvc.assert_called_once_with(f"pvc-{b.user_hash}")


@pytest.mark.asyncio
async def test_reconcile_peruserperchat_pvc_is_per_user(mock_k8s_client):
    from terminal_proxy.config import PodMode

    cfg = Settings(
        proxy_api_key="k",
        namespace="ns",
        storage_mode=StorageMode.PER_USER,
        pod_mode=PodMode.PER_USER_PER_CHAT,
    )
    pm = PodManager(cfg)
    mock_pod = MagicMock()
    mock_pod.metadata.labels = {"user-id-hash": "abc123", "chat-id-hash": "def456"}
    mock_pod.metadata.annotations = {"chat-slug": "chat-x"}
    mock_pod.metadata.name = "terminal-abc123-def456"
    mock_pod.metadata.creation_timestamp = datetime.utcnow()
    mock_pod.status.phase = "Running"
    mock_pod.status.pod_ip = "10.0.0.1"
    mock_k8s_client.list_terminal_pods.return_value = MagicMock(items=[mock_pod])

    await pm._reconcile_existing_pods()

    t = pm._pods["abc123-def456"]
    assert t.is_chat_pod
    assert t.pvc_name == "pvc-abc123"  # per-user, NOT pvc-abc123-def456
