"""Tests for Kubernetes client wrapper."""

from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from terminal_proxy.k8s.client import K8sClient, is_retryable_exception


@pytest.fixture
def k8s_client():
    client = K8sClient(namespace="test-ns")
    with patch.object(client, "init"):
        client._initialized = True
        client._core_v1 = MagicMock()
        yield client


def test_is_retryable_exception_for_5xx():
    api_exception = ApiException(status=503)
    assert is_retryable_exception(api_exception) is True


def test_is_retryable_exception_for_404():
    api_exception = ApiException(status=404)
    assert is_retryable_exception(api_exception) is False


def test_is_retryable_exception_for_connection_error():
    assert is_retryable_exception(ConnectionError()) is True


def test_is_retryable_exception_for_timeout():
    assert is_retryable_exception(TimeoutError()) is True


def test_get_pod_success(k8s_client):
    mock_pod = MagicMock()
    mock_pod.metadata.name = "test-pod"
    k8s_client._core_v1.read_namespaced_pod.return_value = mock_pod

    result = k8s_client.get_pod("test-pod")

    assert result == mock_pod
    k8s_client._core_v1.read_namespaced_pod.assert_called_once_with("test-pod", "test-ns")


def test_get_pod_not_found(k8s_client):
    k8s_client._core_v1.read_namespaced_pod.side_effect = ApiException(status=404)

    result = k8s_client.get_pod("nonexistent")

    assert result is None


def test_list_terminal_pods(k8s_client):
    mock_pods = MagicMock()
    mock_pods.items = [MagicMock(metadata=MagicMock(name="pod1"))]
    k8s_client._core_v1.list_namespaced_pod.return_value = mock_pods

    result = k8s_client.list_terminal_pods()

    assert result == mock_pods
    k8s_client._core_v1.list_namespaced_pod.assert_called_once()


def test_create_pod(k8s_client):
    mock_pod = MagicMock()
    k8s_client._core_v1.create_namespaced_pod.return_value = mock_pod

    manifest = {"metadata": {"name": "test-pod"}}
    result = k8s_client.create_pod(manifest)

    assert result == mock_pod
    k8s_client._core_v1.create_namespaced_pod.assert_called_once_with("test-ns", manifest)


def test_delete_pod_success(k8s_client):
    k8s_client.delete_pod("test-pod")

    k8s_client._core_v1.delete_namespaced_pod.assert_called_once()


def test_delete_pod_not_found(k8s_client):
    k8s_client._core_v1.delete_namespaced_pod.side_effect = ApiException(status=404)

    k8s_client.delete_pod("nonexistent")

    k8s_client._core_v1.delete_namespaced_pod.assert_called_once()


def test_create_pvc(k8s_client):
    mock_pvc = MagicMock()
    k8s_client._core_v1.create_namespaced_persistent_volume_claim.return_value = mock_pvc

    manifest = {"metadata": {"name": "test-pvc"}}
    result = k8s_client.create_pvc(manifest)

    assert result == mock_pvc


@pytest.mark.asyncio
async def test_wait_for_pod_ready_success(k8s_client):
    mock_pod = MagicMock()
    mock_pod.status.phase = "Running"
    mock_pod.status.pod_ip = "10.0.0.1"

    with patch.object(k8s_client, "get_pod", return_value=mock_pod):
        ready, pod_ip = await k8s_client.wait_for_pod_ready("test-pod", timeout_seconds=5)

    assert ready is True
    assert pod_ip == "10.0.0.1"


@pytest.mark.asyncio
async def test_wait_for_pod_ready_timeout(k8s_client):
    mock_pod = MagicMock()
    mock_pod.status.phase = "Pending"
    mock_pod.status.pod_ip = None

    with patch.object(k8s_client, "get_pod", return_value=mock_pod):
        ready, pod_ip = await k8s_client.wait_for_pod_ready("test-pod", timeout_seconds=1)

    assert ready is False
    assert pod_ip is None


@pytest.mark.asyncio
async def test_wait_for_pod_ready_failed(k8s_client):
    mock_pod = MagicMock()
    mock_pod.status.phase = "Failed"

    with patch.object(k8s_client, "get_pod", return_value=mock_pod):
        ready, pod_ip = await k8s_client.wait_for_pod_ready("test-pod", timeout_seconds=5)

    assert ready is False
    assert pod_ip is None
