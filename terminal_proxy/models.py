"""Pydantic models for the terminal proxy."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


def user_id_to_hash(user_id: str) -> str:
    """Convert user_id to a K8s-safe hash."""
    return hashlib.sha256(user_id.encode()).hexdigest()[:12]


def chat_id_to_hash(chat_id: str) -> str:
    """Convert chat_id (X-Session-Id) to a short K8s-safe hash."""
    return hashlib.sha256(chat_id.encode()).hexdigest()[:12]


def sanitize_chat_id(raw: str) -> str:
    """Sanitize a client-supplied chat id into a safe single path component.

    Keeps [A-Za-z0-9._-], collapses everything else to '-', strips leading/trailing
    '-' and '.', caps length, and falls back to a hash for empty/all-dot results.
    The output never contains a path separator and is never '.' or '..', so it is
    safe to append to a base directory (no path traversal).
    """
    if not raw:
        return hashlib.sha256(b"empty").hexdigest()[:16]
    slug = re.sub(r"[^A-Za-z0-9._-]", "-", raw)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-.")
    slug = slug[:64].rstrip("-.")
    if not slug:
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
    return slug


def sanitize_k8s_name(name: str) -> str:
    """Sanitize a name to be K8s-compatible (DNS label)."""
    name = name.lower()
    name = re.sub(r"[^a-z0-9-]", "-", name)
    name = re.sub(r"-+", "-", name)
    name = name.strip("-")
    return name[:63].rstrip("-") or "unknown"


class PodState(str, Enum):
    """Terminal pod lifecycle states."""

    CREATING = "creating"
    RUNNING = "running"
    FAILED = "failed"
    TERMINATED = "terminated"


@dataclass
class TerminalPod:
    """Represents a terminal pod and its metadata."""

    user_id: str
    user_hash: str
    pod_name: str
    service_name: str
    secret_name: str
    pvc_name: str | None
    api_key: str
    state: PodState
    created_at: datetime
    last_active_at: datetime
    pod_ip: str | None = None
    chat_id: str | None = None
    chat_hash: str | None = None
    active_connections: int = 0
    # Per-chat session-cwd bootstrap cache. Upstream open-terminal stores the
    # session cwd in an in-memory dict keyed by X-Session-Id, which is wiped on
    # container restart, so this cache is cleared by PodManager when the pod is
    # (re)created or its container restart_count increases.
    bootstrapped_chats: set[str] = field(default_factory=set)
    container_restart_count: int = 0

    @property
    def endpoint(self) -> str:
        """Get the HTTP endpoint for the terminal pod via service."""
        return f"http://{self.service_name}:8000"

    @property
    def is_chat_pod(self) -> bool:
        """True if this pod is scoped to a single chat (perChat mode)."""
        return self.chat_hash is not None

    @classmethod
    def create(cls, user_id: str, api_key: str, chat_id: str | None = None) -> TerminalPod:
        """Create a new TerminalPod instance with generated names and timestamps."""
        user_hash = user_id_to_hash(user_id)
        chat_hash = chat_id_to_hash(chat_id) if chat_id else None
        now = datetime.utcnow()
        if chat_hash:
            # perChat: one pod per (user, chat); no per-pod PVC (shared RWX volume).
            key = f"{user_hash}-{chat_hash}"
            return cls(
                user_id=user_id,
                user_hash=user_hash,
                chat_id=chat_id,
                chat_hash=chat_hash,
                pod_name=f"terminal-{key}",
                service_name=f"terminal-{key}",
                secret_name=f"terminal-secret-{key}",
                pvc_name=None,
                api_key=api_key,
                state=PodState.CREATING,
                created_at=now,
                last_active_at=now,
            )
        return cls(
            user_id=user_id,
            user_hash=user_hash,
            pod_name=f"terminal-{user_hash}",
            service_name=f"terminal-{user_hash}",
            secret_name=f"terminal-secret-{user_hash}",
            pvc_name=f"pvc-{user_hash}",  # used only in PER_USER mode; inert otherwise
            api_key=api_key,
            state=PodState.CREATING,
            created_at=now,
            last_active_at=now,
        )


@dataclass
class StorageInfo:
    """Information about persistent storage configuration."""

    pvc_name: str
    storage_class: str
    size: str
    access_mode: str
    sub_path: str | None = None


class HealthStatus(BaseModel):
    """Health check response model."""

    status: str = "ok"
    active_pods: int = 0
    max_pods: int = 0
    storage_mode: str = ""


class TerminalListResponse(BaseModel):
    """Response model for listing terminals."""

    terminals: list[dict[str, Any]]


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str
    detail: str | None = None


class K8sUnavailableError(Exception):
    """Raised when Kubernetes API is unavailable."""

    pass
