"""Build Kubernetes Pod and PVC manifests."""

from __future__ import annotations

import logging

from terminal_proxy.config import Settings, StorageMode
from terminal_proxy.models import TerminalPod

logger = logging.getLogger(__name__)

SHARED_PVC_NAME = "terminal-shared-storage"


def build_pvc_manifest(
    pvc_name: str,
    size: str,
    storage_class_name: str,
    access_mode: str = "ReadWriteOnce",
    labels: dict | None = None,
) -> dict:
    manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name,
            "labels": labels or {},
        },
        "spec": {
            "accessModes": [access_mode],
            "resources": {
                "requests": {
                    "storage": size,
                },
            },
        },
    }

    if storage_class_name:
        manifest["spec"]["storageClassName"] = storage_class_name

    return manifest


def build_pod_manifest(
    terminal_pod: TerminalPod,
    cfg: Settings,
    pvc_name: str | None = None,
    shared_pvc_name: str | None = None,
    shared_sub_path: str | None = None,
    node_name: str | None = None,
    node_selector: dict | None = None,
) -> dict:
    labels = {
        "app": cfg.labels_app,
        "managed-by": cfg.labels_managed_by,
        "user-id-hash": terminal_pod.user_hash,
    }

    volumes = []
    volume_mounts = []

    if pvc_name:
        volumes.append({
            "name": "user-data",
            "persistentVolumeClaim": {"claimName": pvc_name},
        })
        volume_mounts.append({
            "name": "user-data",
            "mountPath": "/data",
        })
    elif shared_pvc_name:
        volumes.append({
            "name": "shared-data",
            "persistentVolumeClaim": {"claimName": shared_pvc_name},
        })
        mount = {
            "name": "shared-data",
            "mountPath": "/data",
        }
        if shared_sub_path:
            mount["subPath"] = shared_sub_path
        volume_mounts.append(mount)

    container = {
        "name": "terminal",
        "image": cfg.terminal_image,
        "imagePullPolicy": cfg.terminal_image_pull_policy,
        "ports": [{"containerPort": cfg.terminal_service_port}],
        "env": [
            {"name": "OPEN_TERMINAL_API_KEY", "value": terminal_pod.api_key},
        ],
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

    spec: dict = {
        "containers": [container],
        "volumes": volumes,
        "restartPolicy": "Never",
    }

    if node_name:
        spec["nodeName"] = node_name
    if node_selector:
        spec["nodeSelector"] = node_selector

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


def build_pod_for_user(
    terminal_pod: TerminalPod,
    cfg: Settings,
    shared_pvc_node: str | None = None,
) -> tuple[dict, dict | None]:
    pvc_manifest = None
    pvc_name = None
    shared_pvc_name = None
    shared_sub_path = None
    node_name = None

    if cfg.storage_mode == StorageMode.PER_USER:
        pvc_manifest = build_pvc_manifest(
            pvc_name=terminal_pod.pvc_name,
            size=cfg.storage_per_user_size,
            storage_class_name=cfg.storage_class_name,
            access_mode="ReadWriteOnce",
            labels={
                "app": cfg.labels_app,
                "managed-by": cfg.labels_managed_by,
                "user-id-hash": terminal_pod.user_hash,
            },
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

    pod_manifest = build_pod_manifest(
        terminal_pod=terminal_pod,
        cfg=cfg,
        pvc_name=pvc_name,
        shared_pvc_name=shared_pvc_name,
        shared_sub_path=shared_sub_path,
        node_name=node_name,
    )

    return pod_manifest, pvc_manifest
