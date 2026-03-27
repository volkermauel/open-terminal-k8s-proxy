"""Structured logging configuration."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

from terminal_proxy.config import settings


def is_debug_enabled() -> bool:
    """Check if debug mode is enabled via DEBUG environment variable."""
    return os.environ.get("DEBUG", "").lower() in ("true", "1", "yes")


class StructuredFormatter(logging.Formatter):
    """JSON-structured log formatter."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON-structured string."""
        log_data: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "user_id"):
            log_data["user_id"] = record.user_id

        if hasattr(record, "pod_name"):
            log_data["pod_name"] = record.pod_name

        if hasattr(record, "duration_ms"):
            log_data["duration_ms"] = record.duration_ms

        return str(log_data)


def setup_logging() -> None:
    """Configure structured logging for the application."""
    debug_mode = is_debug_enabled()
    if debug_mode:
        log_level = logging.DEBUG
    else:
        log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredFormatter())

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(handler)

    if debug_mode:
        logging.getLogger("terminal_proxy").setLevel(logging.DEBUG)
        for logger_name in ["uvicorn", "uvicorn.access", "uvicorn.error"]:
            logging.getLogger(logger_name).setLevel(logging.DEBUG)
        root_logger.debug("Debug logging enabled via DEBUG=true")
    else:
        for logger_name in ["uvicorn", "uvicorn.access", "uvicorn.error"]:
            logging.getLogger(logger_name).setLevel(log_level)


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger instance."""
    return logging.getLogger(name)
