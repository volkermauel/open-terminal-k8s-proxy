# Design: Per-chat isolation

## Context

`open-terminal-k8s-proxy` spawns one terminal pod per user (`terminal-<hash>`),
keyed in `PodManager._pods` by `user_hash`. `pod_builder.py` mounts an optional
PVC at `/data` and sets `HOME=/data` **only when a volume exists**, but it never
sets the container's `workingDir`. Upstream `open-terminal` (single-user mode,
the default) computes the terminal's home as `os.getcwd()` (`fs.py:35`,
`main.py:1571`), so the terminal opens in the image's `WORKDIR` (`/home/user` on
the official image, `/` or `/app` on variants/older tags) — **outside** the PVC.
Result: the file browser is rooted at `HOME=/data` (the PVC) but commands run in
`/home/user` (ephemeral), so terminal work is silently lost on pod restart. This
is both the reported "initial directory is `/`" bug and the persistence gap.

`open-terminal` already tracks a per-session working directory keyed by the
`X-Session-Id` header (`main.py:320, 332-339, 1559`) and exposes
`GET/POST /files/cwd` and `POST /files/mkdir` (body `{"path": ...}`). A brand-new
session with no tracked cwd falls back to `fs.home`. The proxy is a transparent
reverse proxy, so `X-Session-Id` already flows through.

## Goals / Non-Goals

**Goals**

- Make the terminal open **inside the persistent data mount**, deterministically,
  so terminal work persists when a PVC is configured.
- Give each chat its own working directory `<mount>/<chatid>`, auto-created, with
  no upstream `open-terminal` change (proxy-side only).
- Offer an **optional** one-pod-per-chat model for stronger isolation, gated on
  shared RWX storage, with short-lived chat pods.
- Preserve current behaviour by default (`podMode=perUser`); no breaking change.

**Non-Goals**

- Changing upstream `open-terminal` (all logic is proxy-side).
- Multi-user (`OPEN_TERMINAL_MULTI_USER`) mode support — out of scope; the proxy
  runs single-user pods today.
- Per-chat PVCs (one PVC per chat). Rejected in favour of one RWX volume + per-chat
  folders (see D4).
- Migrating historical/already-running pods' working directories (new pods only).

## Decisions

### D1: Launch `open-terminal` with `--cwd` pinned to the data mount (root-cause fix)

Pass `open-terminal --cwd <dataMountPath>` via the container `args` (`/data` when a
volume is configured, `/home/user` in `none` mode) and **always** set `HOME` to
match (today `HOME` is only set to `/data` when a volume exists). The image
entrypoint forwards `"$@"` to `open-terminal`, whose CLI applies `os.chdir(cwd)`
at startup (`cli.py:74`), so the process — and thus `os.getcwd()` — lands on the
PVC mount. The terminal, the file browser, and the PVC now agree. Mount path is
configurable (`terminal.dataMountPath`, default `/data`).

Alternative considered: set the container `workingDir`. Rejected — `--cwd` is the
`open-terminal`-native knob and is applied *after* the image entrypoint's setup
(e.g. the `/home/user` chown), making the working directory explicit in the launch
command and robust to future entrypoint changes.

### D2: Folder-per-chat via proxy bootstrap (no upstream change)

On `POST /api/terminals`, when an `X-Session-Id` (chat id) is present, the proxy
ensures `<home>/<chatid>` exists and seeds it as that session's cwd **before**
forwarding the creation:

1. `GET /files/cwd` → learn the pod's effective `home` (adapts to `/data` vs
   `/home/user` automatically); fall back to `terminal.dataMountPath`.
2. `POST /files/mkdir` `{"path": "<home>/<sanitized-chatid>"}` (idempotent).
3. `POST /files/cwd` `{"path": "<home>/<sanitized-chatid>"}` with the
   `X-Session-Id` header (open-terminal stores it in its session-cwd map).
4. Forward the original `POST /api/terminals`; open-terminal creates the terminal
   with cwd == the seeded session cwd.

Order matters: `set_cwd` 404s in single-user mode if the dir does not exist
(`main.py:485`), so `mkdir` must precede `set_cwd`. This runtime bootstrap runs in
`perUser` mode, where one pod hosts many chats; in `perChat` mode each pod is
launched directly in its chat dir (D3), so no bootstrap is needed. It is
idempotent under repeated calls and guarded by `perChatDirs.enabled` (default
`true`).

**`chatid` sanitization (security).** The header is client-controlled, so it must
not reach the filesystem verbatim. Sanitize to a safe slug: keep
`[A-Za-z0-9._-]`, replace everything else with `-`, collapse runs, enforce a
length cap (e.g. 64). If the sanitized result is empty, fall back to a hash of
the raw id. This prevents path traversal and weird directory names.

Alternative considered: implement auto-subdir in upstream `open-terminal`. Rejected
per the chosen proxy-only scope.

### D3: `perChat` pods launch directly in their chat dir; `perUser` uses runtime bootstrap

In `perChat` mode each chat pod is launched with
`--cwd <mount>/<sanitized-chatid>`, so `open-terminal` starts already scoped to
that chat's directory — `fs.home` *is* the chat dir (isolating the file browser
too) — and no runtime mkdir/cwd bootstrap is needed. Because every chat pod mounts
the **same** shared RWX volume, the per-chat folders live side-by-side on it.

`os.chdir` fails if the target does not exist, so a per-chat pod includes an
**initContainer** that mounts the RWX volume and runs
`mkdir -p <mount>/<sanitized-chatid>` (writable for the pod user via the existing
`fsGroup`) before the main container starts. In `perUser` mode the pod launches
with `--cwd <mount>` (the PVC mount point, always present), so no initContainer is
needed; per-chat scoping there is handled by the D2 runtime bootstrap.

### D4: pod-per-chat gated on RWX (`shared`) storage; fail-fast

`perChat` provisions many concurrent pods per user; with `perUser` (RWO) storage
each pod would need its own PVC pinned to one node — explosive and slow. With a
single shared **ReadWriteMany** volume, all chat pods mount it and isolate via
per-chat dirs (D3). Therefore `podMode: perChat` **requires** `storage.mode: shared`;
the chart/config validates this at startup and refuses to start otherwise. (The
existing `shared` mode already creates one RWX PVC `terminal-shared-storage` and
mounts it on every pod.)

Alternative considered: one PVC per chat. Rejected — PVC count and provisioning
latency scale with chats; RWX + folders is far cheaper and simpler.

### D5: Routing in `perChat` via `X-Session-Id` on every request

Today every proxied route resolves the pod via `get_terminal_for_user(user_id)`.
In `perChat` mode the pod is selected by *(user, chat)*, so routes need the chat
id. Introduce `extract_chat_id(request)` (reads `X-Session-Id`) and route through
`PodManager.get_or_create(user_id, chat_id)`, which keys by `user_hash` in
`perUser` mode and by `f"{user_hash}-{chat_hash}"` in `perChat` mode. If
`X-Session-Id` is absent in `perChat` mode, fall back to a single stable
"default" chat key for that user (logged warning) so clients that don't send the
header still work; `perChatDirs` itself degrades to one shared folder (today's
behaviour) when the header is missing.

### D6: Short idle timeout for chat pods

Add `chatPodIdleTimeoutSeconds` (default `300`). The cleanup scan applies it to
pods that carry a `chat-id-hash` label and the existing `pod_idle_timeout_seconds`
to user pods. `max_concurrent_pods` and oldest-eviction apply to **all** pods, so
a user flipping chats rapidly cannot exceed the global cap — the oldest idle chat
pod is evicted first.

### D7: Naming, labels, annotations for chat pods

- Pod/Service name: `terminal-<userhash>-<chathash>` (hashes truncated to stay
  well under the 63-char K8s limit; collision-free per user+chat).
- Labels: existing `app`, `managed-by`, `user-id-hash` **plus** `chat-id-hash`.
- Annotations: existing `user-id` **plus** `chat-id` (so reconcile can rebuild
  the composite key from a running pod, mirroring the network-policy pattern).
- `Secret` name: `terminal-secret-<userhash>-<chathash>`.
- Reconcile (`_reconcile_existing_pods`) rebuilds chat pods from the
  `chat-id-hash` label + `chat-id` annotation.

### D8: `TerminalPod` model changes

Add optional `chat_id: str | None` and `chat_hash: str | None` (and derive
`secret_name`/`service_name`/`pod_name` from the composite key when set). A
`TerminalPod` with `chat_hash` is a chat pod (selects the short idle timeout).

## Risks / Trade-offs

- **[perChat resource explosion]** → short idle timeout + global pod cap +
  oldest-eviction bound it; documented as a resource/isolation trade-off.
- **[Client omits `X-Session-Id`]** → folder-per-chat degrades to one shared dir;
  `perChat` degrades to one "default" chat pod per user (D5). Both are safe
  fallbacks, not errors.
- **[Data-mount write permissions]** → the PVC relies on `fsGroup` to be writable
  by the image's `user` (existing behaviour for `HOME=/data`); unchanged here.
  Document that operators must keep `fsGroup` correct.
- **[Path traversal / weird chat ids]** → sanitization (D2); spec enforces a safe
  slug.
- **[Race on first touch of a chat]** → `mkdir` is idempotent and `set_cwd` is
  last-writer-wins; both are safe under concurrent first requests.
- **[`none` storage is still ephemeral]** → by design; persistence requires
  `storage.mode` `perUser`/`shared`. The working-dir fix only guarantees the
  terminal *uses* the volume when one exists.

## Migration Plan

1. Roll out the new image + chart with defaults (`podMode: perUser`,
   `perChatDirs.enabled: true`). Net effect for existing deployments: terminals
   now open in `/data` (fixing the `/`+persistence bug) and each chat gets its own
   folder under the mount. No PVC change required.
2. (Optional) To enable pod-per-chat: set `storage.mode: shared` (RWX) +
   `podMode: perChat` (+ tune `chatPodIdleTimeoutSeconds`).
3. **Rollback**: `podMode: perUser` and/or `perChatDirs.enabled: false`. Existing
   pods/ PVCs are unaffected; chat folders already created remain on the volume.

## Open Questions

- Default `chatPodIdleTimeoutSeconds` — `300` (5 min) proposed; tune from
  observed chat-switch cadence.
- Missing `X-Session-Id` in `perChat` mode — default to one shared "default" chat
  pod per user (chosen) vs. reject with `400`. Propose the graceful fallback.
