"""Tests for storage management."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from terminal_proxy.config import Settings, StorageMode
from terminal_proxy.storage import StorageManager


@pytest.fixture
def settings():
    return Settings(
        namespace="test-ns",
        storage_mode=StorageMode.PER_USER,
        storage_per_user_size="5Gi",
        storage_shared_size="100Gi",
        storage_class_name="standard",
    )


@pytest.fixture
def storage_manager(settings):
    return StorageManager(settings)


@pytest.fixture
def mock_k8s_client():
    with patch("terminal_proxy.storage.k8s_client") as mock:
        yield mock


def test_create_user_pvc_already_exists(storage_manager, mock_k8s_client):
    mock_k8s_client.get_pvc.return_value = MagicMock()

    result = storage_manager.create_user_pvc("pvc-test", "user123")

    assert result is True
    mock_k8s_client.get_pvc.assert_called_once_with("pvc-test")
    mock_k8s_client.create_pvc.assert_not_called()
    mock_k8s_client.annotate_pvc.assert_called_once()


def test_create_user_pvc_new(storage_manager, mock_k8s_client):
    mock_k8s_client.get_pvc.return_value = None

    result = storage_manager.create_user_pvc("pvc-test", "user123")

    assert result is True
    mock_k8s_client.create_pvc.assert_called_once()
    manifest = mock_k8s_client.create_pvc.call_args[0][0]
    assert manifest["metadata"]["labels"]["type"] == "user"
    assert "terminal-proxy/last-active" in manifest["metadata"]["annotations"]


def test_create_user_pvc_wrong_mode(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.SHARED

    result = storage_manager.create_user_pvc("pvc-test", "user123")

    assert result is False
    mock_k8s_client.create_pvc.assert_not_called()


def test_delete_user_pvc(storage_manager, mock_k8s_client):
    storage_manager.delete_user_pvc("pvc-test")

    mock_k8s_client.delete_pvc.assert_called_once_with("pvc-test")


def test_delete_user_pvc_wrong_mode(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.SHARED

    storage_manager.delete_user_pvc("pvc-test")

    mock_k8s_client.delete_pvc.assert_not_called()


def test_ensure_shared_pvc_per_user_mode(storage_manager, mock_k8s_client):
    result = storage_manager.ensure_shared_pvc()

    assert result is None
    mock_k8s_client.get_pvc.assert_not_called()


def test_ensure_shared_pvc_already_exists(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.SHARED
    mock_k8s_client.get_pvc.return_value = MagicMock()

    result = storage_manager.ensure_shared_pvc()

    assert result == "terminal-shared-storage"
    mock_k8s_client.create_pvc.assert_not_called()


def test_ensure_shared_pvc_new(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.SHARED
    mock_k8s_client.get_pvc.return_value = None

    result = storage_manager.ensure_shared_pvc()

    assert result == "terminal-shared-storage"
    mock_k8s_client.create_pvc.assert_called_once()


def test_get_shared_pvc_node_wrong_mode(storage_manager, mock_k8s_client):
    result = storage_manager.get_shared_pvc_node()

    assert result is None
    mock_k8s_client.get_shared_pvc_node.assert_not_called()


def test_get_shared_pvc_node_cached(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.SHARED_RWO
    storage_manager._shared_pvc_node = "node-1"

    result = storage_manager.get_shared_pvc_node()

    assert result == "node-1"
    mock_k8s_client.get_shared_pvc_node.assert_not_called()


def test_get_shared_pvc_node_fetches(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.SHARED_RWO
    mock_k8s_client.get_shared_pvc_node.return_value = "node-2"

    result = storage_manager.get_shared_pvc_node()

    assert result == "node-2"
    mock_k8s_client.get_shared_pvc_node.assert_called_once()


def test_create_user_pvc_none_mode(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.NONE

    result = storage_manager.create_user_pvc("pvc-test", "user123")

    assert result is False
    mock_k8s_client.create_pvc.assert_not_called()


def test_delete_user_pvc_none_mode(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.NONE

    storage_manager.delete_user_pvc("pvc-test")

    mock_k8s_client.delete_pvc.assert_not_called()


def test_ensure_shared_pvc_none_mode(storage_manager, mock_k8s_client):
    storage_manager.cfg.storage_mode = StorageMode.NONE

    result = storage_manager.ensure_shared_pvc()

    assert result is None
    mock_k8s_client.get_pvc.assert_not_called()


def test_touch_pvc(storage_manager, mock_k8s_client):
    storage_manager.touch_pvc("pvc-test")

    mock_k8s_client.annotate_pvc.assert_called_once()
    call_args = mock_k8s_client.annotate_pvc.call_args[0]
    assert call_args[0] == "pvc-test"
    assert "terminal-proxy/last-active" in call_args[1]


def test_cleanup_expired_pvcs_skips_when_retain_false(settings, mock_k8s_client):
    settings.storage_retain_pvc = False
    sm = StorageManager(settings)
    sm.cleanup_expired_pvcs()
    mock_k8s_client.list_user_pvcs.assert_not_called()


def test_cleanup_expired_pvcs_skips_when_ttl_zero(settings, mock_k8s_client):
    settings.storage_retain_pvc = True
    settings.storage_pvc_retention_ttl_seconds = 0
    sm = StorageManager(settings)
    sm.cleanup_expired_pvcs()
    mock_k8s_client.list_user_pvcs.assert_not_called()


def test_cleanup_expired_pvcs_deletes_expired(settings, mock_k8s_client):
    settings.storage_retain_pvc = True
    settings.storage_pvc_retention_ttl_seconds = 3600
    sm = StorageManager(settings)

    old_ts = (datetime.utcnow() - timedelta(seconds=7200)).isoformat()
    pvc = MagicMock()
    pvc.metadata.name = "pvc-old"
    pvc.metadata.labels = {"user-id-hash": "abc123"}
    pvc.metadata.annotations = {"terminal-proxy/last-active": old_ts}
    mock_k8s_client.list_user_pvcs.return_value = MagicMock(items=[pvc])

    sm.cleanup_expired_pvcs()

    mock_k8s_client.delete_pvc.assert_called_once_with("pvc-old")


def test_cleanup_expired_pvcs_keeps_fresh(settings, mock_k8s_client):
    settings.storage_retain_pvc = True
    settings.storage_pvc_retention_ttl_seconds = 3600
    sm = StorageManager(settings)

    recent_ts = (datetime.utcnow() - timedelta(seconds=100)).isoformat()
    pvc = MagicMock()
    pvc.metadata.name = "pvc-fresh"
    pvc.metadata.labels = {"user-id-hash": "def456"}
    pvc.metadata.annotations = {"terminal-proxy/last-active": recent_ts}
    mock_k8s_client.list_user_pvcs.return_value = MagicMock(items=[pvc])

    sm.cleanup_expired_pvcs()

    mock_k8s_client.delete_pvc.assert_not_called()


def test_cleanup_expired_pvcs_skips_active_pod(settings, mock_k8s_client):
    settings.storage_retain_pvc = True
    settings.storage_pvc_retention_ttl_seconds = 3600
    sm = StorageManager(settings)

    old_ts = (datetime.utcnow() - timedelta(seconds=7200)).isoformat()
    pvc = MagicMock()
    pvc.metadata.name = "pvc-active"
    pvc.metadata.labels = {"user-id-hash": "abc123"}
    pvc.metadata.annotations = {"terminal-proxy/last-active": old_ts}
    mock_k8s_client.list_user_pvcs.return_value = MagicMock(items=[pvc])

    sm.cleanup_expired_pvcs(active_user_hashes={"abc123"})

    mock_k8s_client.delete_pvc.assert_not_called()


def test_cleanup_expired_pvcs_uses_creation_ts_fallback(settings, mock_k8s_client):
    settings.storage_retain_pvc = True
    settings.storage_pvc_retention_ttl_seconds = 3600
    sm = StorageManager(settings)

    pvc = MagicMock()
    pvc.metadata.name = "pvc-no-annotation"
    pvc.metadata.labels = {"user-id-hash": "xyz789"}
    pvc.metadata.annotations = {}
    pvc.metadata.creation_timestamp = datetime.utcnow() - timedelta(seconds=7200)
    mock_k8s_client.list_user_pvcs.return_value = MagicMock(items=[pvc])

    sm.cleanup_expired_pvcs()

    mock_k8s_client.delete_pvc.assert_called_once_with("pvc-no-annotation")
