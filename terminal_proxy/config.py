"""Configuration via environment variables."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageMode(StrEnum):
    """Storage mode for persistent volumes."""

    PER_USER = "perUser"
    SHARED = "shared"
    SHARED_RWO = "sharedRWO"


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
    terminal_memory_request: str = Field(default="512Mi", description="Memory request for terminal pods.")
    terminal_memory_limit: str = Field(default="4Gi", description="Memory limit for terminal pods.")

    terminal_service_port: int = Field(
        default=8000,
        description="Port that terminal pods listen on.",
    )

    storage_mode: StorageMode = Field(
        default=StorageMode.PER_USER,
        description="Storage mode: perUser, shared (RWX), or sharedRWO (RWO with node affinity).",
    )
    storage_class_name: str = Field(
        default="",
        description="StorageClass for PVCs. Empty uses cluster default.",
    )
    storage_per_user_size: str = Field(default="5Gi", description="PVC size per user (perUser mode).")
    storage_shared_size: str = Field(default="100Gi", description="Shared PVC size (shared/sharedRWO mode).")

    max_concurrent_pods: int = Field(
        default=100,
        description="Maximum concurrent terminal pods. Evicts longest-idle when reached.",
    )
    pod_idle_timeout_seconds: int = Field(
        default=300,
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

    labels_app: str = Field(default="open-terminal-user", description="App label for terminal pods.")
    labels_managed_by: str = Field(default="terminal-proxy", description="Managed-by label for terminal pods.")

    cors_allowed_origins: str = Field(default="*", description="Comma-separated CORS allowed origins.")

    log_level: str = Field(default="INFO", description="Logging level.")

    @property
    def cors_origins(self) -> list[str]:
        """Parse CORS allowed origins into a list."""
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]


settings = Settings()
