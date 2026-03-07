"""Pydantic models for the terminal proxy."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel


def user_id_to_hash(user_id: str) -> str:
    """Convert user_id to a K8s-safe hash."""
    return hashlib.sha256(user_id.encode()).hexdigest()[:12]


def sanitize_k8s_name(name: str) -> str:
    """Sanitize a name to be K8s-compatible (DNS label)."""
    name = name.lower()
    name = re.sub(r"[^a-z0-9-]", "-", name)
    name = re.sub(r"-+", "-", name)
    name = name.strip("-")
    return name[:63].rstrip("-") or "unknown"


class PodState(StrEnum):
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
    pvc_name: str | None
    api_key: str
    state: PodState
    created_at: datetime
    last_active_at: datetime
    pod_ip: str | None = None

    @property
    def endpoint(self) -> str:
        """Get the HTTP endpoint for the terminal pod."""
        if self.pod_ip:
            return f"http://{self.pod_ip}:8000"
        return f"http://{self.pod_name}.{self.pod_name}:8000"

    @classmethod
    def create(cls, user_id: str, api_key: str) -> TerminalPod:
        """Create a new TerminalPod instance with generated names and timestamps."""
        user_hash = user_id_to_hash(user_id)
        now = datetime.utcnow()
        return cls(
            user_id=user_id,
            user_hash=user_hash,
            pod_name=f"terminal-{user_hash}",
            pvc_name=f"pvc-{user_hash}",
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
