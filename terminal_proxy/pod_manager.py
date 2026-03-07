"""Pod lifecycle management."""

from __future__ import annotations

import asyncio
import logging
import secrets
from datetime import datetime
from typing import Optional

from terminal_proxy.config import Settings, StorageMode, settings
from terminal_proxy.k8s.client import k8s_client
from terminal_proxy.k8s.pod_builder import build_pod_for_user
from terminal_proxy.models import PodState, TerminalPod
from terminal_proxy.storage import storage_manager

logger = logging.getLogger(__name__)


class PodManager:
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self._pods: dict[str, TerminalPod] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await self._reconcile_existing_pods()
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Pod manager started")

    async def stop(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("Pod manager stopped")

    async def _reconcile_existing_pods(self) -> None:
        try:
            pods = k8s_client.list_terminal_pods()
            for pod in pods.items:
                user_hash = pod.metadata.labels.get("user-id-hash")
                if not user_hash:
                    continue

                if pod.status.phase == "Running":
                    terminal = TerminalPod(
                        user_id=user_hash,
                        user_hash=user_hash,
                        pod_name=pod.metadata.name,
                        pvc_name=f"pvc-{user_hash}" if self.cfg.storage_mode == StorageMode.PER_USER else None,
                        api_key=self._generate_api_key(),
                        state=PodState.RUNNING,
                        created_at=pod.metadata.creation_timestamp or datetime.utcnow(),
                        last_active_at=datetime.utcnow(),
                        pod_ip=pod.status.pod_ip,
                    )
                    self._pods[user_hash] = terminal
                    logger.info(f"Reconciled existing pod {pod.metadata.name} for user {user_hash}")
                else:
                    k8s_client.delete_pod(pod.metadata.name)
                    logger.info(f"Deleted non-running pod {pod.metadata.name}")
        except Exception as e:
            logger.error(f"Failed to reconcile existing pods: {e}")

    def _generate_api_key(self) -> str:
        return secrets.token_urlsafe(32)

    async def get_or_create(self, user_id: str) -> TerminalPod:
        user_hash = TerminalPod.create(user_id, "").user_hash

        async with self._lock:
            terminal = self._pods.get(user_hash)

            if terminal and terminal.state == PodState.RUNNING:
                terminal.last_active_at = datetime.utcnow()
                return terminal

            if terminal:
                if terminal.pvc_name:
                    storage_manager.delete_user_pvc(terminal.pvc_name)
                k8s_client.delete_pod(terminal.pod_name)
                del self._pods[user_hash]

            if len(self._pods) >= self.cfg.max_concurrent_pods:
                await self._evict_oldest()

            terminal = TerminalPod.create(user_id, self._generate_api_key())

            await self._create_pod_resources(terminal)

            self._pods[user_hash] = terminal
            return terminal

    async def _create_pod_resources(self, terminal: TerminalPod) -> None:
        try:
            if self.cfg.storage_mode in (StorageMode.SHARED, StorageMode.SHARED_RWO):
                storage_manager.ensure_shared_pvc()

            shared_pvc_node = storage_manager.get_shared_pvc_node()

            if self.cfg.storage_mode == StorageMode.PER_USER and terminal.pvc_name:
                storage_manager.create_user_pvc(terminal.pvc_name, terminal.user_hash)

            pod_manifest, pvc_manifest = build_pod_for_user(
                terminal_pod=terminal,
                cfg=self.cfg,
                shared_pvc_node=shared_pvc_node,
            )

            k8s_client.create_pod(pod_manifest)
            logger.info(f"Created pod {terminal.pod_name} for user {terminal.user_hash}")

            ready, pod_ip = k8s_client.wait_for_pod_ready(
                terminal.pod_name,
                timeout_seconds=self.cfg.pod_startup_timeout_seconds,
            )

            if ready and pod_ip:
                terminal.state = PodState.RUNNING
                terminal.pod_ip = pod_ip
                logger.info(f"Pod {terminal.pod_name} is ready at {pod_ip}")
            else:
                terminal.state = PodState.FAILED
                logger.error(f"Pod {terminal.pod_name} failed to start")
                k8s_client.delete_pod(terminal.pod_name)
                raise RuntimeError(f"Pod {terminal.pod_name} failed to become ready")

        except Exception as e:
            terminal.state = PodState.FAILED
            logger.error(f"Failed to create pod resources: {e}")
            raise

    async def _evict_oldest(self) -> None:
        if not self._pods:
            return

        oldest_hash = min(self._pods.keys(), key=lambda h: self._pods[h].last_active_at)
        oldest = self._pods[oldest_hash]

        logger.info(f"Evicting oldest pod {oldest.pod_name} (user {oldest.user_hash})")
        await self._delete_pod(oldest_hash)

    async def _delete_pod(self, user_hash: str) -> None:
        terminal = self._pods.pop(user_hash, None)
        if not terminal:
            return

        try:
            k8s_client.delete_pod(terminal.pod_name)
            logger.info(f"Deleted pod {terminal.pod_name}")
        except Exception as e:
            logger.warning(f"Failed to delete pod {terminal.pod_name}: {e}")

        if terminal.pvc_name and self.cfg.storage_mode == StorageMode.PER_USER:
            try:
                storage_manager.delete_user_pvc(terminal.pvc_name)
            except Exception as e:
                logger.warning(f"Failed to delete PVC {terminal.pvc_name}: {e}")

    async def _cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.cfg.pod_cleanup_interval_seconds)
                await self._cleanup_idle_pods()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")

    async def _cleanup_idle_pods(self) -> None:
        now = datetime.utcnow()
        to_evict = []

        for user_hash, terminal in self._pods.items():
            idle_seconds = (now - terminal.last_active_at).total_seconds()
            if idle_seconds > self.cfg.pod_idle_timeout_seconds:
                to_evict.append(user_hash)

        for user_hash in to_evict:
            logger.info(f"Cleaning up idle pod for user {user_hash}")
            await self._delete_pod(user_hash)

    def get_stats(self) -> dict:
        return {
            "active_pods": len(self._pods),
            "max_pods": self.cfg.max_concurrent_pods,
            "pods": [
                {
                    "user_hash": t.user_hash,
                    "pod_name": t.pod_name,
                    "state": t.state.value,
                    "last_active": t.last_active_at.isoformat(),
                }
                for t in self._pods.values()
            ],
        }


pod_manager = PodManager(settings)
