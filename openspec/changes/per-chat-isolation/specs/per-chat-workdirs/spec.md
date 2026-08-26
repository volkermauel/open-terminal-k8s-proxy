# Spec Delta: per-chat-workdirs

## ADDED Requirements

### Requirement: Terminal working directory pinned to the persistent data mount

The system SHALL launch each terminal pod's `open-terminal` process with
`--cwd <dataMountPath>` (and set `HOME` to the same value), so the process working
directory equals the persistent data mount. When a volume is configured the mount
path SHALL be `<dataMountPath>` (default `/data`); in `none` storage mode it SHALL
be `/home/user`. This makes the terminal open inside the PVC so terminal-authored
files persist across pod restarts, and removes dependence on the container image's
`WORKDIR`.

#### Scenario: Terminal opens inside the PVC when a volume is configured

- **WHEN** a terminal pod is created with `storage.mode` `perUser`, `shared`, or
  `sharedRWO`
- **THEN** the container `args` SHALL include `--cwd /data` (or the configured
  `dataMountPath`) and `HOME` SHALL be set to that same path, so the terminal
  process working directory is the PVC mount.

#### Scenario: Terminal opens in /home/user when no volume is configured

- **WHEN** a terminal pod is created with `storage.mode` `none`
- **THEN** the container `args` SHALL include `--cwd /home/user` and `HOME` SHALL
  be `/home/user`.

### Requirement: Per-chat working directory created and used per session

The system SHALL give each terminal session carrying an `X-Session-Id` header a
dedicated directory `<dataMountPath>/<sanitized-chatid>`, ensure it exists, and
use it as that session's working directory, so concurrent chats are isolated. In
`perUser` pod mode this SHALL be done at terminal creation via the terminal pod's
`/files/mkdir` and `/files/cwd` endpoints (`mkdir` before `set_cwd`) before
forwarding the creation request; in `perChat` pod mode the directory is
established by launching the pod with `--cwd <dataMountPath>/<sanitized-chatid>`
(see `pod-per-chat-mode`).

#### Scenario: A new chat gets its own directory on first use

- **WHEN** a client creates a terminal session sending `X-Session-Id: <chatid>`
  and `perChatDirs.enabled` is `true` (default)
- **THEN** the system SHALL create `<dataMountPath>/<sanitized-chatid>` if it does
  not exist and SHALL seed it as that session's working directory, so the new
  terminal opens there.

#### Scenario: Concurrent chats are isolated

- **WHEN** two terminal sessions are created with different `X-Session-Id` values
  for the same user in `perUser` mode
- **THEN** each session SHALL resolve relative paths against its own
  `<dataMountPath>/<sanitized-chatid>` directory and SHALL NOT share a working
  directory with the other.

#### Scenario: Missing X-Session-Id falls back to the mount

- **WHEN** a terminal session is created with no `X-Session-Id` header
- **THEN** the system SHALL NOT create a per-chat directory and the session SHALL
  open in `<dataMountPath>` (the pod working directory), preserving legacy
  single-directory behaviour.

### Requirement: Chat id sanitized to a safe directory name

Because `X-Session-Id` is client-controlled, the system SHALL sanitize it before
using it as a path component: only `[A-Za-z0-9._-]` characters SHALL be retained
(runs of other characters collapsed to `-`), the result SHALL be length-capped,
and an empty sanitized result SHALL fall back to a hash of the raw id. The
sanitized value SHALL never contain path separators or allow traversal outside
`<dataMountPath>`.

#### Scenario: Unsafe characters are neutralized

- **WHEN** a client sends `X-Session-Id: a/../b?c=1`
- **THEN** the resulting directory name SHALL contain no `/`, `..`, or other path
  component, and the directory created SHALL be a single child of
  `<dataMountPath>`.

### Requirement: Per-chat working directory feature is configurable

The system SHALL provide a `perChatDirs.enabled` setting (default `true`) that
turns per-chat working directories on or off. When disabled, all sessions SHALL
open in `<dataMountPath>` regardless of `X-Session-Id`.

#### Scenario: Feature disabled by operator

- **WHEN** `perChatDirs.enabled` is `false`
- **THEN** no per-chat directories SHALL be created and every session SHALL use
  `<dataMountPath>` as its working directory.
