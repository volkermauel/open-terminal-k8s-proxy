"""Build Kubernetes Pod and PVC manifests."""

from __future__ import annotations

import base64
import logging
from typing import Any, cast

from terminal_proxy.config import Settings, StorageMode
from terminal_proxy.models import TerminalPod

logger = logging.getLogger(__name__)

SHARED_PVC_NAME = "terminal-shared-storage"
API_KEY_SECRET_KEY = "api-key"


def build_pvc_manifest(
    pvc_name: str,
    size: str,
    storage_class_name: str,
    access_mode: str = "ReadWriteOnce",
    labels: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a Kubernetes PersistentVolumeClaim manifest."""
    spec: dict[str, Any] = {
        "accessModes": [access_mode],
        "resources": {
            "requests": {
                "storage": size,
            },
        },
    }

    if storage_class_name:
        spec["storageClassName"] = storage_class_name

    manifest: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name,
            "labels": labels or {},
        },
        "spec": spec,
    }

    return manifest


def build_secret_manifest(
    secret_name: str,
    api_key: str,
    labels: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a Kubernetes Secret manifest for storing the API key."""
    encoded_key = base64.b64encode(api_key.encode()).decode()
    manifest: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": secret_name,
            "labels": labels or {},
        },
        "type": "Opaque",
        "data": {
            API_KEY_SECRET_KEY: encoded_key,
        },
    }
    return manifest


def build_pod_manifest(
    terminal_pod: TerminalPod,
    cfg: Settings,
    pvc_name: str | None = None,
    shared_pvc_name: str | None = None,
    shared_sub_path: str | None = None,
    node_name: str | None = None,
    node_selector: dict[str, Any] | None = None,
    secret_name: str | None = None,
) -> dict[str, Any]:
    """Build a Kubernetes Pod manifest for a terminal instance."""
    labels = {
        "app": cfg.labels_app,
        "managed-by": cfg.labels_managed_by,
        "user-id-hash": terminal_pod.user_hash,
    }

    volumes = []
    volume_mounts = []

    if pvc_name:
        volumes.append(
            {
                "name": "user-data",
                "persistentVolumeClaim": {"claimName": pvc_name},
            }
        )
        volume_mounts.append(
            {
                "name": "user-data",
                "mountPath": "/data",
            }
        )
    elif shared_pvc_name:
        volumes.append(
            {
                "name": "shared-data",
                "persistentVolumeClaim": {"claimName": shared_pvc_name},
            }
        )
        mount = {
            "name": "shared-data",
            "mountPath": "/data",
        }
        if shared_sub_path:
            mount["subPath"] = shared_sub_path
        volume_mounts.append(mount)

    env_var: dict[str, Any]
    if secret_name:
        env_var = {
            "name": "OPEN_TERMINAL_API_KEY",
            "valueFrom": {
                "secretKeyRef": {
                    "name": secret_name,
                    "key": API_KEY_SECRET_KEY,
                }
            },
        }
    else:
        env_var = {"name": "OPEN_TERMINAL_API_KEY", "value": terminal_pod.api_key}

    container = {
        "name": "terminal",
        "image": cfg.terminal_image,
        "imagePullPolicy": cfg.terminal_image_pull_policy,
        "ports": [{"containerPort": cfg.terminal_service_port}],
        "env": [env_var],
        "resources": {
            "requests": {
                "cpu": cfg.terminal_cpu_request,
                "memory": cfg.terminal_memory_request,
            },
            "limits": {
                "cpu": cfg.terminal_cpu_limit,
                "memory": cfg.terminal_memory_limit,
            },
        },
        "volumeMounts": volume_mounts,
    }

    if cfg.terminal_ephemeral_storage_request:
        resources = cast(dict[str, Any], container["resources"])
        resources["requests"]["ephemeral-storage"] = (
            cfg.terminal_ephemeral_storage_request
        )
    if cfg.terminal_ephemeral_storage_limit:
        resources = cast(dict[str, Any], container["resources"])
        resources["limits"]["ephemeral-storage"] = (
            cfg.terminal_ephemeral_storage_limit
        )

    spec: dict[str, Any] = {
        "containers": [container],
        "volumes": volumes,
        "restartPolicy": "Never",
    }

    if node_name:
        spec["nodeName"] = node_name
    if node_selector:
        spec["nodeSelector"] = node_selector

    tolerations = cfg.terminal_tolerations
    if tolerations:
        spec["tolerations"] = tolerations

    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": terminal_pod.pod_name,
            "labels": labels,
        },
        "spec": spec,
    }

    return manifest


def build_service_manifest(
    terminal_pod: TerminalPod,
    cfg: Settings,
) -> dict[str, Any]:
    """Build a Kubernetes Service manifest for a terminal pod."""
    labels = {
        "app": cfg.labels_app,
        "managed-by": cfg.labels_managed_by,
        "user-id-hash": terminal_pod.user_hash,
    }

    manifest = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": terminal_pod.service_name,
            "labels": labels,
        },
        "spec": {
            "type": "ClusterIP",
            "selector": {
                "app": cfg.labels_app,
                "user-id-hash": terminal_pod.user_hash,
            },
            "ports": [
                {
                    "name": "http",
                    "port": 8000,
                    "targetPort": cfg.terminal_service_port,
                    "protocol": "TCP",
                }
            ],
        },
    }

    return manifest


def build_pod_for_user(
    terminal_pod: TerminalPod,
    cfg: Settings,
    shared_pvc_node: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any], dict[str, Any]]:
    """Build pod, optional PVC, secret, and service manifests based on storage mode."""
    pvc_manifest = None
    pvc_name = None
    shared_pvc_name = None
    shared_sub_path = None
    node_name = None

    labels = {
        "app": cfg.labels_app,
        "managed-by": cfg.labels_managed_by,
        "user-id-hash": terminal_pod.user_hash,
    }

    if cfg.storage_mode == StorageMode.PER_USER:
        if terminal_pod.pvc_name is None:
            raise ValueError("pvc_name is required for PER_USER storage mode")
        pvc_manifest = build_pvc_manifest(
            pvc_name=terminal_pod.pvc_name,
            size=cfg.storage_per_user_size,
            storage_class_name=cfg.storage_class_name,
            access_mode="ReadWriteOnce",
            labels=labels,
        )
        pvc_name = terminal_pod.pvc_name

    elif cfg.storage_mode == StorageMode.SHARED:
        shared_pvc_name = SHARED_PVC_NAME
        shared_sub_path = terminal_pod.user_hash

    elif cfg.storage_mode == StorageMode.SHARED_RWO:
        shared_pvc_name = SHARED_PVC_NAME
        shared_sub_path = terminal_pod.user_hash
        if shared_pvc_node:
            node_name = shared_pvc_node
    elif cfg.storage_mode == StorageMode.NONE:
        pass  # no PVC, no volume — ephemeral-storage limits protect the node

    secret_manifest = build_secret_manifest(
        secret_name=terminal_pod.secret_name,
        api_key=terminal_pod.api_key,
        labels=labels,
    )

    pod_manifest = build_pod_manifest(
        terminal_pod=terminal_pod,
        cfg=cfg,
        pvc_name=pvc_name,
        shared_pvc_name=shared_pvc_name,
        shared_sub_path=shared_sub_path,
        node_name=node_name,
        node_selector=cfg.terminal_node_selector or None,
        secret_name=terminal_pod.secret_name,
    )

    service_manifest = build_service_manifest(terminal_pod, cfg)

    return pod_manifest, pvc_manifest, secret_manifest, service_manifest
