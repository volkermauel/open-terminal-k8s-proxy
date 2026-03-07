"""Kubernetes API client wrapper."""

from __future__ import annotations

import logging
from typing import Optional

from kubernetes import client, config
from kubernetes.client import V1Pod, V1PersistentVolumeClaim, V1PodList
from kubernetes.client.rest import ApiException
from kubernetes.watch import Watch

from terminal_proxy.config import settings

logger = logging.getLogger(__name__)


class K8sClient:
    def __init__(self, namespace: Optional[str] = None):
        self.namespace = namespace or settings.namespace
        self._core_v1: Optional[client.CoreV1Api] = None
        self._initialized = False

    def init(self) -> None:
        if self._initialized:
            return

        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except config.ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded kubeconfig from environment")
            except config.ConfigException:
                raise RuntimeError("Could not load Kubernetes configuration")

        self._core_v1 = client.CoreV1Api()
        self._initialized = True

    @property
    def core_v1(self) -> client.CoreV1Api:
        if not self._initialized:
            self.init()
        assert self._core_v1 is not None
        return self._core_v1

    def get_pod(self, pod_name: str) -> Optional[V1Pod]:
        try:
            return self.core_v1.read_namespaced_pod(pod_name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def list_terminal_pods(self) -> V1PodList:
        return self.core_v1.list_namespaced_pod(
            self.namespace,
            label_selector=f"app={settings.labels_app},managed-by={settings.labels_managed_by}",
        )

    def create_pod(self, pod_manifest: dict) -> V1Pod:
        return self.core_v1.create_namespaced_pod(self.namespace, pod_manifest)

    def delete_pod(self, pod_name: str, grace_period_seconds: int = 30) -> None:
        try:
            self.core_v1.delete_namespaced_pod(
                pod_name,
                self.namespace,
                grace_period_seconds=grace_period_seconds,
            )
        except ApiException as e:
            if e.status != 404:
                raise

    def get_pvc(self, pvc_name: str) -> Optional[V1PersistentVolumeClaim]:
        try:
            return self.core_v1.read_namespaced_persistent_volume_claim(pvc_name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def create_pvc(self, pvc_manifest: dict) -> V1PersistentVolumeClaim:
        return self.core_v1.create_namespaced_persistent_volume_claim(self.namespace, pvc_manifest)

    def delete_pvc(self, pvc_name: str) -> None:
        try:
            self.core_v1.delete_namespaced_persistent_volume_claim(pvc_name, self.namespace)
        except ApiException as e:
            if e.status != 404:
                raise

    def wait_for_pod_ready(self, pod_name: str, timeout_seconds: int = 60) -> tuple[bool, Optional[str]]:
        import time

        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout_seconds:
            pod = self.get_pod(pod_name)
            if pod is None:
                return False, None

            phase = pod.status.phase
            if phase == "Running":
                if pod.status.pod_ip:
                    return True, pod.status.pod_ip
            elif phase in ("Failed", "Unknown"):
                return False, None

            time.sleep(0.5)

        return False, None

    def get_shared_pvc_node(self, pvc_name: str) -> Optional[str]:
        pods = self.list_terminal_pods()
        for pod in pods.items:
            for volume in pod.spec.volumes or []:
                if volume.persistent_volume_claim and volume.persistent_volume_claim.claim_name == pvc_name:
                    return pod.spec.node_name
        return None


k8s_client = K8sClient()
