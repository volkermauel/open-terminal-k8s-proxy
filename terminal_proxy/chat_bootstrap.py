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

from terminal_proxy.config import PodMode, Settings
from terminal_proxy.models import TerminalPod, sanitize_chat_id

logger = logging.getLogger(__name__)


async def ensure_chat_dir(terminal: TerminalPod, chat_id: str | None, cfg: Settings) -> None:
    """Ensure ``<data_mount_path>/<sanitized-chatid>`` exists and seed it as the session cwd.

    Cached per (terminal pod, chat_id): open-terminal keeps the session cwd in an
    in-memory dict keyed by the ``X-Session-Id`` header, so once we have mkdir'd the
    chat dir and bound it to the session we need not repeat the two calls on every
    request. The cache lives on the ``TerminalPod`` and is cleared when the pod is
    (re)created or its container restart_count increases (see
    ``PodManager._check_pod_health``), matching the lifetime of the upstream store.

    perChat pods are launched directly in their chat dir (``--cwd``), so they are
    skipped. Best-effort: a failed ``set_cwd`` is not cached so the next request
    retries; failures never break terminal creation.
    """
    if cfg.pod_mode == PodMode.PER_CHAT or not cfg.per_chat_dirs_enabled or not chat_id:
        return
    if chat_id in terminal.bootstrapped_chats:
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
            else:
                # Only cache on a successful set_cwd so a transient failure retries.
                terminal.bootstrapped_chats.add(chat_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("ensure_chat_dir: set cwd %s failed: %s", chat_dir, e)
