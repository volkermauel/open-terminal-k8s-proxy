"""Storage management for terminal pods."""

from __future__ import annotations

import logging
from datetime import datetime

from terminal_proxy.config import Settings, StorageMode, settings
from terminal_proxy.k8s.client import k8s_client
from terminal_proxy.k8s.pod_builder import (
    LAST_ACTIVE_ANNOTATION,
    SHARED_PVC_NAME,
    build_pvc_manifest,
)

logger = logging.getLogger(__name__)


class StorageManager:
    """Manages persistent storage for terminal pods."""

    def __init__(self, cfg: Settings):
        """Initialize the storage manager with configuration."""
        self.cfg = cfg
        self._shared_pvc_node: str | None = None

    def ensure_shared_pvc(self) -> str | None:
        """Ensure shared PVC exists for shared storage modes."""
        if self.cfg.storage_mode not in (StorageMode.SHARED, StorageMode.SHARED_RWO):
            return None

        existing = k8s_client.get_pvc(SHARED_PVC_NAME)
        if existing:
            logger.info(f"Shared PVC {SHARED_PVC_NAME} already exists")
            return SHARED_PVC_NAME

        access_mode = (
            "ReadWriteMany" if self.cfg.storage_mode == StorageMode.SHARED else "ReadWriteOnce"
        )

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
        """Get the node where shared RWO PVC is mounted."""
        if self.cfg.storage_mode != StorageMode.SHARED_RWO:
            return None

        if self._shared_pvc_node:
            return self._shared_pvc_node

        self._shared_pvc_node = k8s_client.get_shared_pvc_node(SHARED_PVC_NAME)
        return self._shared_pvc_node

    def _user_pvc_labels(self, user_hash: str) -> dict[str, str]:
        return {
            "app": self.cfg.labels_app,
            "managed-by": self.cfg.labels_managed_by,
            "user-id-hash": user_hash,
            "type": "user",
        }

    def create_user_pvc(self, pvc_name: str, user_hash: str) -> bool:
        """Create a user-specific PVC for PER_USER storage mode."""
        if self.cfg.storage_mode != StorageMode.PER_USER:
            return False

        existing = k8s_client.get_pvc(pvc_name)
        if existing:
            logger.debug(f"User PVC {pvc_name} already exists")
            self.touch_pvc(pvc_name)
            return True

        manifest = build_pvc_manifest(
            pvc_name=pvc_name,
            size=self.cfg.storage_per_user_size,
            storage_class_name=self.cfg.storage_class_name,
            access_mode="ReadWriteOnce",
            labels=self._user_pvc_labels(user_hash),
            annotations={LAST_ACTIVE_ANNOTATION: datetime.utcnow().isoformat()},
        )

        try:
            k8s_client.create_pvc(manifest)
            logger.info(f"Created user PVC {pvc_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create user PVC {pvc_name}: {e}")
            raise

    def touch_pvc(self, pvc_name: str) -> None:
        """Update the last-active annotation on a PVC to now."""
        try:
            k8s_client.annotate_pvc(
                pvc_name, {LAST_ACTIVE_ANNOTATION: datetime.utcnow().isoformat()}
            )
        except Exception as e:
            logger.warning(f"Failed to annotate PVC {pvc_name}: {e}")

    def delete_user_pvc(self, pvc_name: str) -> None:
        """Delete a user-specific PVC."""
        if self.cfg.storage_mode != StorageMode.PER_USER:
            return

        try:
            k8s_client.delete_pvc(pvc_name)
            logger.info(f"Deleted user PVC {pvc_name}")
        except Exception as e:
            logger.warning(f"Failed to delete user PVC {pvc_name}: {e}")

    def cleanup_expired_pvcs(self, active_user_hashes: set[str] | None = None) -> None:
        """Delete per-user PVCs that have exceeded their retention TTL."""
        if not self.cfg.storage_retain_pvc or self.cfg.storage_pvc_retention_ttl_seconds <= 0:
            return

        if self.cfg.storage_mode != StorageMode.PER_USER:
            return

        now = datetime.utcnow()
        ttl = self.cfg.storage_pvc_retention_ttl_seconds
        active = active_user_hashes or set()

        try:
            pvcs = k8s_client.list_user_pvcs()
        except Exception as e:
            logger.error(f"Failed to list user PVCs for TTL cleanup: {e}")
            return

        for pvc in pvcs.items:
            pvc_name = pvc.metadata.name
            user_hash = (pvc.metadata.labels or {}).get("user-id-hash", "")

            if user_hash in active:
                continue

            annotations = pvc.metadata.annotations or {}
            last_active_str = annotations.get(LAST_ACTIVE_ANNOTATION)
            if not last_active_str:
                last_active = pvc.metadata.creation_timestamp
                if last_active:
                    if last_active.tzinfo is not None:
                        last_active = last_active.replace(tzinfo=None)
                else:
                    continue
            else:
                try:
                    last_active = datetime.fromisoformat(last_active_str)
                except (ValueError, TypeError):
                    continue

            age_seconds = (now - last_active).total_seconds()
            if age_seconds > ttl:
                logger.info(
                    f"Deleting expired PVC {pvc_name} (age {age_seconds:.0f}s > TTL {ttl}s)"
                )
                self.delete_user_pvc(pvc_name)


storage_manager = StorageManager(settings)
