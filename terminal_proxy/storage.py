"""Storage management for terminal pods."""

from __future__ import annotations

import logging

from terminal_proxy.config import Settings, StorageMode, settings
from terminal_proxy.k8s.client import k8s_client
from terminal_proxy.k8s.pod_builder import SHARED_PVC_NAME, build_pvc_manifest

logger = logging.getLogger(__name__)


class StorageManager:
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self._shared_pvc_node: str | None = None

    def ensure_shared_pvc(self) -> str | None:
        if self.cfg.storage_mode == StorageMode.PER_USER:
            return None

        existing = k8s_client.get_pvc(SHARED_PVC_NAME)
        if existing:
            logger.info(f"Shared PVC {SHARED_PVC_NAME} already exists")
            return SHARED_PVC_NAME

        access_mode = "ReadWriteMany" if self.cfg.storage_mode == StorageMode.SHARED else "ReadWriteOnce"

        manifest = build_pvc_manifest(
            pvc_name=SHARED_PVC_NAME,
            size=self.cfg.storage_shared_size,
            storage_class_name=self.cfg.storage_class_name,
            access_mode=access_mode,
            labels={
                "app": self.cfg.labels_app,
                "managed-by": self.cfg.labels_managed_by,
                "type": "shared",
            },
        )

        try:
            k8s_client.create_pvc(manifest)
            logger.info(f"Created shared PVC {SHARED_PVC_NAME} with {access_mode}")
            return SHARED_PVC_NAME
        except Exception as e:
            logger.error(f"Failed to create shared PVC: {e}")
            raise

    def get_shared_pvc_node(self) -> str | None:
        if self.cfg.storage_mode != StorageMode.SHARED_RWO:
            return None

        if self._shared_pvc_node:
            return self._shared_pvc_node

        self._shared_pvc_node = k8s_client.get_shared_pvc_node(SHARED_PVC_NAME)
        return self._shared_pvc_node

    def create_user_pvc(self, pvc_name: str, user_hash: str) -> bool:
        if self.cfg.storage_mode != StorageMode.PER_USER:
            return False

        existing = k8s_client.get_pvc(pvc_name)
        if existing:
            logger.debug(f"User PVC {pvc_name} already exists")
            return True

        manifest = build_pvc_manifest(
            pvc_name=pvc_name,
            size=self.cfg.storage_per_user_size,
            storage_class_name=self.cfg.storage_class_name,
            access_mode="ReadWriteOnce",
            labels={
                "app": self.cfg.labels_app,
                "managed-by": self.cfg.labels_managed_by,
                "user-id-hash": user_hash,
            },
        )

        try:
            k8s_client.create_pvc(manifest)
            logger.info(f"Created user PVC {pvc_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create user PVC {pvc_name}: {e}")
            raise

    def delete_user_pvc(self, pvc_name: str) -> None:
        if self.cfg.storage_mode != StorageMode.PER_USER:
            return

        try:
            k8s_client.delete_pvc(pvc_name)
            logger.info(f"Deleted user PVC {pvc_name}")
        except Exception as e:
            logger.warning(f"Failed to delete user PVC {pvc_name}: {e}")


storage_manager = StorageManager(settings)
