"""Per-chat working-directory bootstrap (perUser pod mode).

When a terminal session carries an ``X-Session-Id`` (chat id) we give it a private
working directory ``<home>/<sanitized-chatid>`` on the terminal pod's data volume,
created on first use and seeded as that session's cwd. This runs entirely
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
    """Ensure ``<home>/<sanitized-chatid>`` exists and seed it as the session cwd.

    Calls the terminal pod's ``GET /files/cwd`` (to learn the effective home),
    ``POST /files/mkdir`` (idempotent), then ``POST /files/cwd`` keyed by the
    ``X-Session-Id`` header. Idempotent and best-effort.
    """
    if not cfg.per_chat_dirs_enabled or not chat_id:
        return

    slug = sanitize_chat_id(chat_id)
    base = terminal.endpoint
    auth = {"Authorization": f"Bearer {terminal.api_key}"}

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as client:
        home = cfg.data_mount_path
        try:
            resp = await client.get(f"{base}/files/cwd", headers=auth)
            if resp.status_code < 400:
                home = resp.json().get("home") or cfg.data_mount_path
            else:
                logger.warning(
                    "ensure_chat_dir: GET /files/cwd -> %s; using %s",
                    resp.status_code,
                    cfg.data_mount_path,
                )
        except Exception as e:  # noqa: BLE001
            logger.warning("ensure_chat_dir: GET /files/cwd failed: %s", e)

        chat_dir = f"{home.rstrip('/')}/{slug}"

        # mkdir must precede set_cwd: open-terminal 404s set_cwd on a missing dir.
        try:
            resp = await client.post(f"{base}/files/mkdir", headers=auth, json={"path": chat_dir})
            # 400 may indicate the directory already exists — that is fine.
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
