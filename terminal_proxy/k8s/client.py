"""Kubernetes API client wrapper."""

from __future__ import annotations

import logging
from typing import Any, cast

from kubernetes import client, config  # type: ignore
from kubernetes.client import V1PersistentVolumeClaim, V1Pod, V1PodList  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from terminal_proxy.config import settings

logger = logging.getLogger(__name__)


RETRYABLE_EXCEPTIONS = (ApiException, ConnectionError, TimeoutError)


def is_retryable_exception(exception: Exception) -> bool:
    """Check if an exception is retryable based on type and status code."""
    if isinstance(exception, ApiException):
        return exception.status in (429, 500, 502, 503, 504)
    return isinstance(exception, RETRYABLE_EXCEPTIONS)


class K8sClient:
    """Kubernetes API client wrapper with retry logic."""

    def __init__(self, namespace: str | None = None):
        """Initialize the K8s client with optional namespace override."""
        self.namespace = namespace or settings.namespace
        self._core_v1: client.CoreV1Api | None = None
        self._initialized = False

    def init(self) -> None:
        """Initialize the Kubernetes client configuration."""
        if self._initialized:
            return

        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except config.ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded kubeconfig from environment")
            except config.ConfigException as e:
                raise RuntimeError("Could not load Kubernetes configuration") from e

        self._core_v1 = client.CoreV1Api()
        self._initialized = True

    @property
    def core_v1(self) -> client.CoreV1Api:
        """Get the CoreV1 API client, initializing if necessary."""
        if not self._initialized:
            self.init()
        assert self._core_v1 is not None
        return self._core_v1

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    def get_pod(self, pod_name: str) -> V1Pod | None:
        """Get a pod by name, returning None if not found."""
        try:
            return self.core_v1.read_namespaced_pod(pod_name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    def list_terminal_pods(self) -> V1PodList:
        """List all terminal pods managed by this proxy."""
        return self.core_v1.list_namespaced_pod(
            self.namespace,
            label_selector=f"app={settings.labels_app},managed-by={settings.labels_managed_by}",
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    def create_pod(self, pod_manifest: dict[str, Any]) -> V1Pod:
        """Create a pod from the given manifest."""
        return self.core_v1.create_namespaced_pod(self.namespace, pod_manifest)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    def delete_pod(self, pod_name: str, grace_period_seconds: int = 30) -> None:
        """Delete a pod by name, ignoring 404 errors."""
        try:
            self.core_v1.delete_namespaced_pod(
                pod_name,
                self.namespace,
                grace_period_seconds=grace_period_seconds,
            )
        except ApiException as e:
            if e.status != 404:
                raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    def get_pvc(self, pvc_name: str) -> V1PersistentVolumeClaim | None:
        """Get a PVC by name, returning None if not found."""
        try:
            return self.core_v1.read_namespaced_persistent_volume_claim(pvc_name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    def create_pvc(self, pvc_manifest: dict[str, Any]) -> V1PersistentVolumeClaim:
        """Create a PVC from the given manifest."""
        return self.core_v1.create_namespaced_persistent_volume_claim(self.namespace, pvc_manifest)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True,
    )
    def delete_pvc(self, pvc_name: str) -> None:
        """Delete a PVC by name, ignoring 404 errors."""
        try:
            self.core_v1.delete_namespaced_persistent_volume_claim(pvc_name, self.namespace)
        except ApiException as e:
            if e.status != 404:
                raise

    async def wait_for_pod_ready(self, pod_name: str, timeout_seconds: int = 60) -> tuple[bool, str | None]:
        """Wait for a pod to become ready, returning (success, pod_ip)."""
        import asyncio

        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < timeout_seconds:
            pod = self.get_pod(pod_name)
            if pod is None:
                return False, None

            phase = pod.status.phase
            if phase == "Running":
                if pod.status.pod_ip:
                    return True, pod.status.pod_ip
            elif phase in ("Failed", "Unknown"):
                return False, None

            await asyncio.sleep(0.5)

        return False, None

    def get_shared_pvc_node(self, pvc_name: str) -> str | None:
        """Get the node name where pods using the shared PVC are running."""
        pods = self.list_terminal_pods()
        for pod in pods.items:
            for volume in pod.spec.volumes or []:
                if volume.persistent_volume_claim and volume.persistent_volume_claim.claim_name == pvc_name:
                    return cast(str | None, pod.spec.node_name)
        return None


k8s_client = K8sClient()
