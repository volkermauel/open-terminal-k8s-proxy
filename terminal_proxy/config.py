"""Configuration via environment variables."""

from __future__ import annotations

from enum import Enum

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageMode(str, Enum):
    """Storage mode for persistent volumes."""

    NONE = "none"
    PER_USER = "perUser"
    SHARED = "shared"
    SHARED_RWO = "sharedRWO"


class PodMode(str, Enum):
    """Terminal pod provisioning mode."""

    PER_USER = "perUser"
    PER_CHAT = "perChat"
    PER_USER_PER_CHAT = "perUserPerChat"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    proxy_api_key: str = Field(
        default="",
        description="API key for authenticating requests to this proxy. Auto-generated if empty.",
    )
    proxy_host: str = Field(default="0.0.0.0", description="Host to bind the proxy server.")
    proxy_port: int = Field(default=8000, description="Port to bind the proxy server.")

    namespace: str = Field(
        default="default",
        description="Kubernetes namespace for terminal pods and PVCs.",
    )

    terminal_image: str = Field(
        default="ghcr.io/open-webui/open-terminal:latest",
        description="Container image for terminal pods.",
    )
    terminal_image_pull_policy: str = Field(
        default="IfNotPresent",
        description="Image pull policy for terminal pods.",
    )

    terminal_cpu_request: str = Field(default="500m", description="CPU request for terminal pods.")
    terminal_cpu_limit: str = Field(default="1000m", description="CPU limit for terminal pods.")
    terminal_memory_request: str = Field(
        default="512Mi", description="Memory request for terminal pods."
    )
    terminal_memory_limit: str = Field(default="4Gi", description="Memory limit for terminal pods.")
    terminal_ephemeral_storage_request: str = Field(
        default="5Gi",
        description="Ephemeral storage request for terminal pods. "
        "Controls scheduling. Set to empty string to disable.",
    )
    terminal_ephemeral_storage_limit: str = Field(
        default="5Gi",
        description="Ephemeral storage limit for terminal pods. "
        "Kubelet evicts the pod if total writable usage exceeds this. "
        "Set to empty string to disable.",
    )

    terminal_service_port: int = Field(
        default=8000,
        description="Port that terminal pods listen on.",
    )

    terminal_node_selector: dict[str, str] = Field(
        default_factory=dict, description="nodeSelector for terminal pods."
    )
    terminal_tolerations: list[dict[str, str]] = Field(
        default_factory=list, description="Tolerations for terminal pods."
    )

    storage_mode: StorageMode = Field(
        default=StorageMode.NONE,
        description="Storage mode: none (no PVC), perUser, shared (RWX), or sharedRWO (RWO with node affinity).",
    )
    storage_class_name: str = Field(
        default="",
        description="StorageClass for PVCs. Empty uses cluster default.",
    )
    storage_per_user_size: str = Field(
        default="5Gi", description="PVC size per user (perUser mode)."
    )
    storage_shared_size: str = Field(
        default="100Gi", description="Shared PVC size (shared/sharedRWO mode)."
    )
    storage_retain_pvc: bool = Field(
        default=False,
        description="Retain per-user PVCs after pod deletion. "
        "When true, PVCs outlive their pods and are reused on reconnection.",
    )
    storage_pvc_retention_ttl_seconds: int = Field(
        default=0,
        description="Max seconds to retain an unused per-user PVC after its pod is deleted. "
        "0 means retain forever. Only effective when storage_retain_pvc is true.",
    )

    max_concurrent_pods: int = Field(
        default=100,
        description="Maximum concurrent terminal pods. Evicts longest-idle when reached.",
    )
    max_pods_per_user: int = Field(
        default=5,
        description="Max concurrent terminal pods per user (enforced in perChat/perUserPerChat "
        "mode; the user's oldest pod is evicted). 0 disables the per-user cap.",
    )
    pod_idle_timeout_seconds: int = Field(
        default=3600,
        description="Seconds of inactivity before terminating a terminal pod.",
    )
    pod_startup_timeout_seconds: int = Field(
        default=60,
        description="Seconds to wait for a terminal pod to become ready.",
    )
    pod_cleanup_interval_seconds: int = Field(
        default=60,
        description="Interval between idle pod cleanup scans.",
    )
    data_mount_path: str = Field(
        default="/data",
        description="Mount path (and HOME / --cwd target) for terminal pod data volumes.",
    )
    pod_mode: PodMode = Field(
        default=PodMode.PER_USER,
        description="Pod provisioning mode: perUser (one pod per user), perChat (one pod per "
        "chat; requires shared storage), or perUserPerChat (one pod per chat with a dedicated "
        "per-user RWX PVC; requires perUser storage).",
    )
    chat_pod_idle_timeout_seconds: int = Field(
        default=300,
        description="Seconds of inactivity before terminating a per-chat terminal pod.",
    )
    per_chat_dirs_enabled: bool = Field(
        default=True,
        description="Create a per-chat working directory under the data mount for each X-Session-Id.",
    )
    mount_failure_recycle_threshold: int = Field(
        default=10,
        description="Consecutive data-mount/file-API failures tolerated before the terminal "
        "pod is deleted and recreated (self-healing for a pod that is Running but "
        "cannot access its /data volume, e.g. stale mount permissions). 0 disables.",
    )

    labels_app: str = Field(
        default="open-terminal-user", description="App label for terminal pods."
    )
    labels_managed_by: str = Field(
        default="terminal-proxy", description="Managed-by label for terminal pods."
    )

    cors_allowed_origins: str = Field(
        default="*", description="Comma-separated CORS allowed origins."
    )

    log_level: str = Field(default="INFO", description="Logging level.")

    @property
    def cors_origins(self) -> list[str]:
        """Parse CORS allowed origins into a list."""
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]

    @model_validator(mode="after")
    def _validate_pod_mode_storage(self) -> Settings:
        """Validate pod_mode <-> storage_mode compatibility."""
        if self.pod_mode == PodMode.PER_CHAT and self.storage_mode != StorageMode.SHARED:
            raise ValueError(
                "podMode 'perChat' requires storage.mode 'shared' (ReadWriteMany). "
                "Set storage.mode=shared before enabling podMode=perChat."
            )
        if self.pod_mode == PodMode.PER_USER_PER_CHAT and self.storage_mode != StorageMode.PER_USER:
            raise ValueError(
                "podMode 'perUserPerChat' requires storage.mode 'perUser' "
                "(a dedicated ReadWriteMany PVC per user, shared by that user's chat pods). "
                "Set storage.mode=perUser before enabling podMode=perUserPerChat."
            )
        return self


settings = Settings()
