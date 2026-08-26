## Why

New terminal sessions open in `/` (or the image's `WORKDIR`) instead of the
user's home, and work done in the terminal is **not persisted** even when a PVC
is configured. Root cause: the proxy never pins the terminal pod's working
directory — the PVC is mounted at `/data` and `HOME=/data` is set, but the
process still opens in the image `WORKDIR` (`/home/user`), so commands write the
ephemeral layer while the PVC sits unused. On top of that, every chat in a user's
pod shares one working directory, so concurrent chats clobber each other's files.

## What Changes

- **Deterministic working directory on the persistent mount.** The proxy launches
  `open-terminal` with `--cwd <dataMountPath>` (and always sets `HOME`) pointing at
  the data mount (`/data` when a volume is configured, `/home/user` in `none`
  mode). This fixes both the `/` bug and the persistence gap — the terminal now
  opens *inside* the PVC, so files survive pod restarts. Mount path is
  configurable (`terminal.dataMountPath`, default `/data`).
- **Folder-per-chat (always on).** Each chat — identified by the `X-Session-Id`
  header — gets its own directory `<dataMountPath>/<chatid>` that is
  **auto-created** on first use and seeded as that session's working directory.
  Concurrent chats are isolated; the directory persists on the PVC. In `perUser`
  mode this is done proxy-side via the pod's existing `/files/mkdir` + `/files/cwd`
  endpoints at terminal creation; in `perChat` mode each pod is launched directly
  with `--cwd <dataMountPath>/<chatid>` (the dir ensured by an initContainer on
  the shared RWX volume). No upstream `open-terminal` change required.
- **Configurable pod-per-chat mode (opt-in, requires RWX).** A new `podMode`
  setting (`perUser`, default | `perChat`) provisions one terminal pod per
  *(user, chat)* instead of per user. Because every chat pod mounts the **same
  shared RWX volume**, pod-per-chat is only valid with `storage.mode: shared`
  (ReadWriteMany); the chart validates this. Folder-per-chat still runs (each
  chat pod opens in its own `<dataMountPath>/<chatid>` on the shared volume).
- **Short-lived chat pods.** In `perChat` mode, chat pods use a separate, shorter
  idle timeout (`chatPodIdleTimeoutSeconds`, default `300`) so users who switch
  chats frequently don't accumulate idle pods. User pods keep the existing idle
  timeout. Pod-cap/eviction rules apply to chat pods as today.

No **BREAKING** changes: default `podMode=perUser` preserves current behaviour;
folder-per-chat is additive (chats that send no `X-Session-Id` keep the legacy
single shared directory). `none` storage remains ephemeral by design.

## Capabilities

### New Capabilities

- `per-chat-workdirs`: Deterministic terminal working directory pinned to the
  persistent data mount, plus per-chat subdirectories (`<mount>/<chatid>`)
  auto-created and used as each chat session's working directory.
- `pod-per-chat-mode`: Optional one-terminal-pod-per-chat provisioning, gated on
  shared RWX storage, with a short per-chat-pod idle timeout.

### Modified Capabilities

*None yet* — there are no existing `openspec/specs/`.

## Impact

- **Code**
  - `terminal_proxy/config.py`: new settings — `data_mount_path`,
    `per_chat_dirs` (enabled/base path), `pod_mode`, `chat_pod_idle_timeout`;
    validation that `perChat` requires `storage.mode == shared`.
  - `terminal_proxy/k8s/pod_builder.py`: pass `open-terminal --cwd <dataMountPath>`
    via container `args` + always set `HOME`; in `perChat` mode use
    `--cwd <mount>/<chatid>` and add an `initContainer` (mkdir on the RWX volume);
    add `chat-id-hash` pod label + `chat-id` annotation when `perChat`.
  - `terminal_proxy/pod_manager.py`: pod lookup keyed by *(user, chat)* in
    `perChat` mode; per-chat idle-timeout eviction; create/delete chat pods.
  - `terminal_proxy/main.py`: extract `X-Session-Id`; route to chat pod when
    `perChat`; bootstrap folder-per-chat (`mkdir` + `cwd`) in `POST /api/terminals`
    for `perUser` mode.
  - `terminal_proxy/models.py`: add `chat_id`/`chat_hash` to `TerminalPod`.
- **Helm**
  - `values.yaml`: `terminal.dataMountPath`, `perChatDirs`, `podMode`,
    `chatPodIdleTimeoutSeconds`.
  - `templates/deployment.yaml`: surface new settings as env vars.
  - `Chart.yaml`: bump version.
- **Dependencies**: none new (reuses `httpx` for internal pod calls and the
  existing `kubernetes` client).
- **Constraints to document**: `perChat` requires `storage.mode: shared` (RWX);
  folder-per-chat requires the client to send `X-Session-Id` (falls back to one
  shared dir otherwise); in `none` storage mode nothing persists by design;
  `chatid` is sanitized to a safe directory name (no path traversal).
