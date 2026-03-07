"""Tests for storage management."""

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


def test_create_user_pvc_new(storage_manager, mock_k8s_client):
    mock_k8s_client.get_pvc.return_value = None
    
    result = storage_manager.create_user_pvc("pvc-test", "user123")
    
    assert result is True
    mock_k8s_client.create_pvc.assert_called_once()


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
