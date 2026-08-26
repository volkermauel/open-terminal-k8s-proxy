# Tasks: Per-chat isolation

## 1. Config & model foundations

- [x] 1.1 Add settings to `terminal_proxy/config.py`: `data_mount_path: str = "/data"`,
  `per_chat_dirs: PerChatDirsConfig` (`enabled: bool = True`), `pod_mode: PodMode`
  enum (`perUser` default | `perChat`), `chat_pod_idle_timeout_seconds: int = 300`.
- [x] 1.2 Add startup validation: if `pod_mode == perChat` and `storage_mode != shared`
  ⇒ raise a clear configuration error (refuse to start). Cover with a config test.
- [x] 1.3 Extend `TerminalPod` (`terminal_proxy/models.py`) with optional
  `chat_id: str | None` and `chat_hash: str | None`; derive `pod_name`,
  `service_name`, `secret_name` from the composite key when `chat_hash` is set
  (`terminal-<userhash>-<chathash>` / `terminal-secret-...`), staying under 63 chars.
- [x] 1.4 Add a `chat_hash` helper (hash of `chat_id`, truncated) next to the existing
  `user_hash` derivation; add `sanitize_chat_id(raw) -> str` (allowlist
  `[A-Za-z0-9._-]`, collapse others to `-`, length cap, hash fallback for empty).

## 2. Pod builder: `--cwd`, HOME, initContainer, chat labels

- [x] 2.1 In `terminal_proxy/k8s/pod_builder.py`, build container `args` containing
  `["--cwd", <cwd_target>]` where `<cwd_target>` = `data_mount_path` (volume modes)
  or `/home/user` (`none` mode). Forward through `build_pod_manifest`/`build_pod_for_user`.
- [x] 2.2 Always set `HOME` env to the same `cwd_target` (today only set to `/data`
  when a volume exists) so `none` mode also gets `HOME=/home/user`.
- [x] 2.3 Accept an optional per-pod chat target: in `perChat` mode set
  `<cwd_target> = <data_mount_path>/<sanitized-chatid>` and add an `initContainer`
  that mounts the shared RWX volume and runs `mkdir -p <target>` (+ make it writable
  for the pod user via the existing `fsGroup`). No initContainer in `perUser` mode.
- [x] 2.4 Add `chat-id-hash` pod label and `chat-id` annotation (alongside existing
  `user-id-hash`/`user-id`) when `chat_hash` is set; keep existing labels otherwise.

## 3. Folder-per-chat runtime bootstrap (`perUser` mode)

- [x] 3.1 Add a small client helper (e.g. `terminal_proxy/chat_bootstrap.py` or a
  method on `HttpProxy`) to call a terminal pod's `GET /files/cwd`, `POST /files/mkdir`,
  and `POST /files/cwd` using the pod `endpoint` + `api_key` and forwarding
  `X-Session-Id`.
- [x] 3.2 Implement `ensure_chat_dir(terminal, chat_id)` → derive home via
  `GET /files/cwd` (fall back to `data_mount_path`), sanitize chat id, `mkdir`
  `<home>/<sanitized>`, then `set_cwd` to it (with `X-Session-Id`). Guard with
  `per_chat_dirs.enabled` and the presence of `chat_id`; make idempotent.
- [x] 3.3 Hook into `POST /api/terminals` handler in `terminal_proxy/main.py`: when
  `perChatDirs.enabled` and an `X-Session-Id` is present and pod mode is `perUser`,
  run `ensure_chat_dir` **before** forwarding the creation; then forward the
  original request. In `perChat` mode skip the bootstrap (pod already scoped).

## 4. Pod-per-chat provisioning & routing

- [x] 4.1 Extend `PodManager.get_or_create(user_id, chat_id=None)`: in `perUser`
  keep the `user_hash` key; in `perChat` key `_pods` by
  `f"{user_hash}-{chat_hash}"` and build a `TerminalPod` carrying `chat_id`/`chat_hash`.
- [x] 4.2 Update `_create_pod_resources`/`_delete_pod` to create/delete the chat
  pod's secret+pod+service (PVC is the shared RWX one, already managed by
  `StorageManager.ensure_shared_pvc`).
- [x] 4.3 Update `_reconcile_existing_pods` to rebuild chat pods from the
  `chat-id-hash` label + `chat-id` annotation (composite key), mirroring the user-pod
  reconcile path.
- [x] 4.4 Add `extract_chat_id(request)` in `terminal_proxy/main.py` and route all
  terminal-related endpoints through `get_terminal_for_user(user_id, chat_id)`; in
  `perChat` mode route by *(user, chat)*, falling back to a stable "default" chat
  pod per user (logged warning) when `X-Session-Id` is absent.

## 5. Idle-timeout eviction per pod kind

- [x] 5.1 In the pod cleanup scan, apply `chat_pod_idle_timeout_seconds` to pods
  whose `TerminalPod.chat_hash` is set and `pod_idle_timeout_seconds` to user pods.
- [x] 5.2 Confirm `_evict_oldest` (triggered at `max_concurrent_pods`) considers all
  pods regardless of kind (oldest idle first) so frequent chat switching is bounded.

## 6. Helm chart

- [x] 6.1 `values.yaml`: add `terminal.dataMountPath` (default `/data`), `perChatDirs`
  (`enabled: true`), `podMode` (`perUser`), `chatPodIdleTimeoutSeconds` (`300`).
- [x] 6.2 `templates/deployment.yaml`: surface the new settings as env vars
  (`DATA_MOUNT_PATH`, `PER_CHAT_DIRS_ENABLED`, `POD_MODE`, `CHAT_POD_IDLE_TIMEOUT_SECONDS`).
- [x] 6.3 `Chart.yaml`: bump version (minor).
- [x] 6.4 (Optional) add a values-test or NOTES.txt warning that `podMode: perChat`
  requires `storage.mode: shared`.

## 7. Tests & lint

- [x] 7.1 `pod_builder` tests: `--cwd` args present and correct in all storage modes;
  `HOME` always set; `perChat` adds initContainer + chat label/annotation; names ≤ 63 chars.
- [x] 7.2 `sanitize_chat_id` tests: strips `/`, `..`, query chars; collapses runs;
  length cap; empty → hash fallback; no path-separator leakage.
- [x] 7.3 Bootstrap tests: `ensure_chat_dir` calls `mkdir` then `set_cwd` with the
  right path + header; no-op when disabled or `chat_id` absent; idempotent on repeat.
- [x] 7.4 `PodManager` tests: `perChat` keys by composite and reconciles chat pods
  from labels; `perUser` unchanged; eviction uses correct timeout per kind; missing
  `X-Session-Id` in `perChat` falls back to default chat pod.
- [x] 7.5 Run `ruff check .`, `mypy terminal_proxy`, `pytest tests -v --tb=short`.

## 8. Docs

- [x] 8.1 README: new section — working-directory/persistence fix (`--cwd` on the
  mount), folder-per-chat (on by default, `X-Session-Id`, sanitization), and the
  optional `podMode: perChat` (requires `storage.mode: shared`, short idle timeout,
  resource trade-off).
- [x] 8.2 Add a `values.yaml` example block for enabling pod-per-chat with RWX.
