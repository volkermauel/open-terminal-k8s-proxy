"""Per-chat working-directory bootstrap (perUser pod mode).

When a terminal session carries an ``X-Session-Id`` (chat id) we give it a private
working directory ``<data_mount_path>/<sanitized-chatid>`` on the terminal pod's data
volume, created on first use and seeded as that session's cwd. This runs entirely
proxy-side against the terminal pod's own ``/files`` API, so no upstream
``open-terminal`` change is required. It is best-effort: failures are logged but
never break terminal creation.
"""

from __future__ import annotations

import logging

import httpx

from terminal_proxy.config import Settings
from terminal_proxy.models import TerminalPod, sanitize_chat_id

logger = logging.getLogger(__name__)


async def ensure_chat_dir(terminal: TerminalPod, chat_id: str, cfg: Settings) -> None:
    """Ensure ``<data_mount_path>/<sanitized-chatid>`` exists and seed it as the session cwd.

    The terminal pod's home is ``cfg.data_mount_path`` (it is launched with
    ``--cwd <data_mount_path>``), so the chat directory is derived directly -- no
    round-trip to discover it. Calls the terminal pod's ``POST /files/mkdir``
    (idempotent), then ``POST /files/cwd`` keyed by the ``X-Session-Id`` header.
    Best-effort: failures are logged but never break terminal creation.
    """
    if not cfg.per_chat_dirs_enabled or not chat_id:
        return

    slug = sanitize_chat_id(chat_id)
    chat_dir = f"{cfg.data_mount_path.rstrip('/')}/{slug}"
    base = terminal.endpoint
    auth = {"Authorization": f"Bearer {terminal.api_key}"}

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as client:
        # mkdir must precede set_cwd: open-terminal rejects set_cwd on a missing dir.
        try:
            resp = await client.post(f"{base}/files/mkdir", headers=auth, json={"path": chat_dir})
            # 400 may indicate the directory already exists -- that is fine.
            if resp.status_code >= 400 and resp.status_code != 400:
                logger.warning(
                    "ensure_chat_dir: mkdir %s -> %s %s", chat_dir, resp.status_code, resp.text
                )
        except Exception as e:  # noqa: BLE001
            logger.warning("ensure_chat_dir: mkdir %s failed: %s", chat_dir, e)

        try:
            headers = {**auth, "X-Session-Id": chat_id}
            resp = await client.post(f"{base}/files/cwd", headers=headers, json={"path": chat_dir})
            if resp.status_code >= 400:
                logger.warning(
                    "ensure_chat_dir: set cwd %s -> %s %s", chat_dir, resp.status_code, resp.text
                )
        except Exception as e:  # noqa: BLE001
            logger.warning("ensure_chat_dir: set cwd %s failed: %s", chat_dir, e)
