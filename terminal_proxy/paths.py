"""Helpers for browser capability URLs (/browser/view/{sid}/{token}/...)."""

from __future__ import annotations

import re

# Capability URLs embed a per-session token; keep it out of logs and metrics.
_TOKEN_IN_PATH_RE = re.compile(r"(browser/view/[^/]+)/[^/]+(/?)")


def mask_capability_token(path: str) -> str:
    """Replace the capability token segment with ***, keeping sid and the rest."""
    return _TOKEN_IN_PATH_RE.sub(r"\1/***/", path)


def is_capability_path(path: str) -> bool:
    """True for /browser/view/{sid}/{token}/... paths (token-authenticated)."""
    return _TOKEN_IN_PATH_RE.search(path) is not None
