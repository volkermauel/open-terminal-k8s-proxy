"""Pod lifecycle management."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
from datetime import datetime
from typing import Any

from terminal_proxy.config import PodMode, Settings, StorageMode, settings
from terminal_proxy.k8s.client import k8s_client
from terminal_proxy.k8s.pod_builder import build_pod_for_user
from terminal_proxy.metrics import record_pod_startup
from terminal_proxy.models import PodState, TerminalPod, chat_id_to_hash
from terminal_proxy.storage import storage_manager

logger = logging.getLogger(__name__)


class PodManager:
    """Manages terminal pod lifecycle and tracking."""

    def __init__(self, cfg: Settings):
        """Initialize the pod manager with configuration."""
        self.cfg = cfg
        self._pods: dict[str, TerminalPod] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._health_check_task: asyncio.Task[None] | None = None
        self._pvc_cleanup_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the pod manager and cleanup tasks."""
        await self._reconcile_existing_pods()
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._pvc_cleanup_task = asyncio.create_task(self._pvc_cleanup_loop())
        logger.info("Pod manager started")

    async def stop(self) -> None:
        """Stop the pod manager and cleanup tasks."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
        if self._health_check_task:
            self._health_check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_check_task
        if self._pvc_cleanup_task:
            self._pvc_cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._pvc_cleanup_task
        logger.info("Pod manager stopped")

    async def _reconcile_existing_pods(self) -> None:
        try:
            pods = k8s_client.list_terminal_pods()
            for pod in pods.items:
                user_hash = pod.metadata.labels.get("user-id-hash")
                if not user_hash:
                    continue

                chat_hash = pod.metadata.labels.get("chat-id-hash")
                pod_key = f"{user_hash}-{chat_hash}" if chat_hash else user_hash

                if pod.status.phase == "Running":
                    secret_name = f"terminal-secret-{pod_key}"
                    api_key = self._get_api_key_from_secret(secret_name)

                    terminal = TerminalPod(
                        user_id=user_hash,
                        user_hash=user_hash,
                        chat_id=(pod.metadata.annotations or {}).get("chat-slug"),
                        chat_hash=chat_hash,
                        pod_name=pod.metadata.name,
                        service_name=f"terminal-{pod_key}",
                        secret_name=secret_name,
                        pvc_name=f"pvc-{user_hash}"
                        if self.cfg.storage_mode == StorageMode.PER_USER
                        else None,
                        api_key=api_key,
                        state=PodState.RUNNING,
                        created_at=pod.metadata.creation_timestamp or datetime.utcnow(),
                        last_active_at=datetime.utcnow(),
                        pod_ip=pod.status.pod_ip,
                    )
                    self._pods[pod_key] = terminal
                    logger.info(f"Reconciled existing pod {pod.metadata.name} for {pod_key}")
                else:
                    k8s_client.delete_service(f"terminal-{pod_key}")
                    k8s_client.delete_pod(pod.metadata.name)
                    k8s_client.delete_secret(f"terminal-secret-{pod_key}")
                    logger.info(f"Deleted non-running pod {pod.metadata.name}")
        except Exception as e:
            logger.error(f"Failed to reconcile existing pods: {e}")

    def _get_api_key_from_secret(self, secret_name: str) -> str:
        """Retrieve API key from a Kubernetes secret."""
        import base64

        secret = k8s_client.get_secret(secret_name)
        if secret and secret.data and "api-key" in secret.data:
            return base64.b64decode(secret.data["api-key"]).decode()
        new_key = self._generate_api_key()
        logger.warning(f"Secret {secret_name} not found or invalid, generated new key")
        return new_key

    def _generate_api_key(self) -> str:
        return secrets.token_urlsafe(32)

    def _handle_pvc_on_remove(self, terminal: TerminalPod) -> None:
        """Touch (retain) or delete the per-user PVC when a pod is removed.

        In perUserPerChat mode the PVC is shared across the user's chat pods, so it
        is only deleted once the user's last chat pod goes away.
        """
        if not terminal.pvc_name or self.cfg.storage_mode != StorageMode.PER_USER:
            return
        sibling_exists = any(
            t is not terminal and t.user_hash == terminal.user_hash
            for t in self._pods.values()
        )
        keep = self.cfg.storage_retain_pvc or (
            self.cfg.pod_mode == PodMode.PER_USER_PER_CHAT and sibling_exists
        )
        try:
            if keep:
                storage_manager.touch_pvc(terminal.pvc_name)
            else:
                storage_manager.delete_user_pvc(terminal.pvc_name)
        except Exception as e:
            logger.warning(f"Failed to update PVC {terminal.pvc_name}: {e}")

    async def get_or_create(self, user_id: str, chat_id: str | None = None) -> TerminalPod:
        """Get or create a terminal pod for the given user (and chat, in perChat mode)."""
        user_hash = TerminalPod.create(user_id, "").user_hash
        per_chat = self.cfg.pod_mode in (PodMode.PER_CHAT, PodMode.PER_USER_PER_CHAT) and bool(chat_id)
        pod_key = f"{user_hash}-{chat_id_to_hash(chat_id)}" if per_chat else user_hash  # type: ignore[arg-type]

        async with self._lock:
            terminal = self._pods.get(pod_key)

            if terminal and terminal.state == PodState.RUNNING:
                terminal.last_active_at = datetime.utcnow()
                if terminal.pvc_name:
                    storage_manager.touch_pvc(terminal.pvc_name)
                return terminal

            if terminal:
                # Reuse the leftover secret/service via the idempotent create path below;
                # only the stale pod is removed. PVC is refcounted for perUserPerChat.
                self._handle_pvc_on_remove(terminal)
                k8s_client.delete_pod(terminal.pod_name)
                del self._pods[pod_key]

            # Per-user cap (perChat mode): bound how many concurrent pods a single
            # user can hold by evicting that user's oldest idle pod.
            if per_chat and self.cfg.max_pods_per_user > 0:
                user_keys = [k for k, t in self._pods.items() if t.user_hash == user_hash]
                if len(user_keys) >= self.cfg.max_pods_per_user:
                    oldest = min(user_keys, key=lambda k: self._pods[k].last_active_at)
                    logger.info(
                        "Per-user pod cap (%d) reached for %s; evicting oldest chat pod %s",
                        self.cfg.max_pods_per_user,
                        user_hash,
                        oldest,
                    )
                    await self._delete_pod(oldest)
            if len(self._pods) >= self.cfg.max_concurrent_pods:
                await self._evict_oldest()

            terminal = TerminalPod.create(
                user_id, self._generate_api_key(), chat_id if per_chat else None
            )
            # perUserPerChat: per-chat pod backed by a dedicated per-user RWX PVC.
            if self.cfg.pod_mode == PodMode.PER_USER_PER_CHAT:
                terminal.pvc_name = f"pvc-{user_hash}"
            # Skip PVC creation
            if self.cfg.storage_mode == StorageMode.NONE:
                terminal.pvc_name = None

            await self._create_pod_resources(terminal)

            self._pods[pod_key] = terminal
            return terminal

    async def _create_pod_resources(self, terminal: TerminalPod) -> None:
        startup_start = datetime.utcnow()
        try:
            if self.cfg.storage_mode in (StorageMode.SHARED, StorageMode.SHARED_RWO):
                storage_manager.ensure_shared_pvc()

            shared_pvc_node = storage_manager.get_shared_pvc_node()

            if self.cfg.storage_mode == StorageMode.PER_USER and terminal.pvc_name:
                storage_manager.create_user_pvc(terminal.pvc_name, terminal.user_hash)

            pod_manifest, pvc_manifest, secret_manifest, service_manifest = build_pod_for_user(
                terminal_pod=terminal,
                cfg=self.cfg,
                shared_pvc_node=shared_pvc_node,
            )

            # Secret (idempotent): reuse a leftover from a removed pod, adopting its key.
            _, secret_created = k8s_client.create_or_get_secret(secret_manifest)
            if secret_created:
                logger.info(f"Created secret {terminal.secret_name} for user {terminal.user_hash}")
            else:
                terminal.api_key = self._get_api_key_from_secret(terminal.secret_name)
                logger.info(f"Reusing existing secret {terminal.secret_name}")

            # Pod (self-healing): adopt a live leftover, else remove a dead one and (re)create.
            existing_pod = k8s_client.get_pod(terminal.pod_name)
            live_phase = (
                existing_pod.status.phase
                if existing_pod is not None and existing_pod.status is not None
                else None
            )
            if live_phase in ("Pending", "Running"):
                logger.info(f"Adopting live pod {terminal.pod_name} (phase={live_phase})")
            else:
                if existing_pod is not None:
                    logger.info(f"Removing dead leftover pod {terminal.pod_name}")
                    k8s_client.delete_pod(terminal.pod_name)
                k8s_client.create_pod(pod_manifest)
                logger.info(f"Created pod {terminal.pod_name} for user {terminal.user_hash}")

            # Service (idempotent).
            k8s_client.create_or_get_service(service_manifest)
            logger.info(f"Ensured service {terminal.service_name} for user {terminal.user_hash}")

            ready, pod_ip = await k8s_client.wait_for_pod_ready(
                terminal.pod_name,
                terminal.service_name,
                timeout_seconds=self.cfg.pod_startup_timeout_seconds,
            )

            startup_duration = (datetime.utcnow() - startup_start).total_seconds()
            record_pod_startup(terminal.user_hash, startup_duration)

            if ready and pod_ip:
                terminal.state = PodState.RUNNING
                terminal.pod_ip = pod_ip
                logger.info(
                    f"Pod {terminal.pod_name} is ready at {pod_ip} via service {terminal.service_name} (startup: {startup_duration:.2f}s)"
                )
            else:
                terminal.state = PodState.FAILED
                logger.error(f"Pod {terminal.pod_name} failed to start")
                k8s_client.delete_service(terminal.service_name)
                k8s_client.delete_pod(terminal.pod_name)
                k8s_client.delete_secret(terminal.secret_name)
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
            k8s_client.delete_service(terminal.service_name)
            logger.info(f"Deleted service {terminal.service_name}")
        except Exception as e:
            logger.warning(f"Failed to delete service {terminal.service_name}: {e}")

        try:
            k8s_client.delete_pod(terminal.pod_name)
            logger.info(f"Deleted pod {terminal.pod_name}")
        except Exception as e:
            logger.warning(f"Failed to delete pod {terminal.pod_name}: {e}")

        try:
            k8s_client.delete_secret(terminal.secret_name)
            logger.info(f"Deleted secret {terminal.secret_name}")
        except Exception as e:
            logger.warning(f"Failed to delete secret {terminal.secret_name}: {e}")

        self._handle_pvc_on_remove(terminal)

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

        for pod_key, terminal in self._pods.items():
            idle_seconds = (now - terminal.last_active_at).total_seconds()
            timeout = (
                self.cfg.chat_pod_idle_timeout_seconds
                if terminal.is_chat_pod
                else self.cfg.pod_idle_timeout_seconds
            )
            if idle_seconds > timeout:
                to_evict.append(pod_key)

        for pod_key in to_evict:
            logger.info(f"Cleaning up idle pod {pod_key}")
            await self._delete_pod(pod_key)

    async def _health_check_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(30)
                await self._check_pod_health()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Health check loop error: {e}")

    async def _check_pod_health(self) -> None:
        to_remove = []

        for user_hash, terminal in self._pods.items():
            if terminal.state != PodState.RUNNING:
                continue

            try:
                pod = k8s_client.get_pod(terminal.pod_name)
                if pod is None or pod.status.phase in ("Failed", "Unknown"):
                    logger.warning(f"Pod {terminal.pod_name} is unhealthy, marking for removal")
                    to_remove.append(user_hash)
                elif pod.status.phase == "Running" and pod.status.pod_ip != terminal.pod_ip:
                    terminal.pod_ip = pod.status.pod_ip
                    logger.info(f"Updated pod {terminal.pod_name} IP to {terminal.pod_ip}")
            except Exception as e:
                logger.warning(f"Failed to check health of pod {terminal.pod_name}: {e}")

        for user_hash in to_remove:
            terminal_to_fail = self._pods.get(user_hash)
            if terminal_to_fail:
                terminal_to_fail.state = PodState.FAILED
            await self._delete_pod(user_hash)

    async def _pvc_cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.cfg.pod_cleanup_interval_seconds)
                storage_manager.cleanup_expired_pvcs(set(self._pods.keys()))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"PVC cleanup loop error: {e}")

    def get_stats(self) -> dict[str, Any]:
        """Get statistics about active pods."""
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
