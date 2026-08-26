# Spec Delta: pod-per-chat-mode

## ADDED Requirements

### Requirement: Configurable pod provisioning mode

The system SHALL provide a `podMode` setting with values `perUser` (default) and
`perChat`. In `perUser` mode the system SHALL provision one terminal pod per user
(existing behaviour). In `perChat` mode the system SHALL provision one terminal
pod per *(user, chat)* pair, selected from the `X-User-Id` and `X-Session-Id`
headers.

#### Scenario: Default mode preserves existing behaviour

- **WHEN** `podMode` is unset or `perUser`
- **THEN** the system SHALL provision at most one terminal pod per `X-User-Id` and
  SHALL route all of that user's requests to it.

#### Scenario: perChat provisions a pod per chat

- **WHEN** `podMode` is `perChat` and a user creates terminals for two distinct
  `X-Session-Id` values
- **THEN** the system SHALL provision two distinct terminal pods, one per
  *(user, chat)*, and SHALL route each chat's requests to its own pod.

### Requirement: perChat mode requires shared RWX storage

Every chat pod SHALL mount the same volume concurrently, so `podMode: perChat`
MUST require `storage.mode: shared` (ReadWriteMany). The system SHALL validate
this at startup and SHALL refuse to start if `perChat` is configured with any
other storage mode.

#### Scenario: perChat rejected without RWX

- **WHEN** `podMode` is `perChat` and `storage.mode` is not `shared`
- **THEN** the proxy SHALL fail to start with a configuration error identifying
  the conflicting settings.

#### Scenario: perChat accepted with RWX

- **WHEN** `podMode` is `perChat` and `storage.mode` is `shared`
- **THEN** all chat pods SHALL mount the shared RWX volume at `dataMountPath` and
  SHALL isolate per chat via `<dataMountPath>/<sanitized-chatid>` directories.

### Requirement: perChat pod launches in its chat directory

In `perChat` mode each chat pod SHALL be launched with
`--cwd <dataMountPath>/<sanitized-chatid>` so `open-terminal` starts scoped to that
chat's directory. Because `os.chdir` fails on a missing directory, each chat pod
SHALL include an `initContainer` that mounts the shared RWX volume and creates the
chat directory (writable by the pod user) before the main container starts.

#### Scenario: Chat directory ensured before the terminal starts

- **WHEN** a new chat pod is provisioned for a chat id whose directory does not yet
  exist on the shared volume
- **THEN** an `initContainer` SHALL create
  `<dataMountPath>/<sanitized-chatid>` before the `open-terminal` main container
  starts, and the main container SHALL launch with `--cwd` pointing at it.

### Requirement: Short idle timeout for chat pods

The system SHALL apply a separate idle timeout `chatPodIdleTimeoutSeconds`
(default `300`) to chat pods (pods carrying a `chat-id-hash` label), evicting them
when idle longer than that threshold. User pods (`perUser` mode) SHALL continue to
use the existing `pod_idle_timeout_seconds`. The global `max_concurrent_pods` cap
and oldest-idle eviction SHALL apply to all pods regardless of mode.

#### Scenario: Idle chat pod is evicted quickly

- **WHEN** a chat pod has been idle longer than `chatPodIdleTimeoutSeconds`
- **THEN** the system SHALL evict (delete) that chat pod and its owned resources.

#### Scenario: User pods keep the longer timeout

- **WHEN** a user pod (`perUser` mode) has been idle longer than
  `chatPodIdleTimeoutSeconds` but less than `pod_idle_timeout_seconds`
- **THEN** the system SHALL NOT evict it.

### Requirement: Chat pods are labelled and reconcilable

Chat pods SHALL carry the existing `app`, `managed-by`, and `user-id-hash` labels
plus a `chat-id-hash` label, and a `chat-id` annotation (alongside `user-id`).
Pod, Service, and Secret names SHALL be derived from the composite
*(user-hash, chat-hash)* key and stay within the Kubernetes 63-character name
limit. On proxy restart the system SHALL reconcile existing chat pods from these
labels and annotations.

#### Scenario: Chat pod resources are named and labelled consistently

- **WHEN** a chat pod is created
- **THEN** its Pod, Service, and Secret SHALL be named from the user and chat
  hashes, its Pod SHALL carry a `chat-id-hash` label and `chat-id` annotation, and
  the names SHALL not exceed 63 characters.

#### Scenario: Chat pods reconciled on restart

- **WHEN** the proxy restarts while chat pods are running
- **THEN** the system SHALL rebuild its in-memory chat-pod index from each pod's
  `chat-id-hash` label and `chat-id` annotation.

### Requirement: perChat routing via X-Session-Id

In `perChat` mode all terminal-related requests SHALL be routed to the chat pod
selected by *(X-User-Id, X-Session-Id)*. When `X-Session-Id` is absent in
`perChat` mode, the system SHALL fall back to a single stable "default" chat pod
for that user (logged warning) so clients that omit the header continue to
function.

#### Scenario: Requests routed to the correct chat pod

- **WHEN** `podMode` is `perChat` and a request carries `X-User-Id` and
  `X-Session-Id`
- **THEN** the system SHALL route the request to the terminal pod keyed by that
  *(user, chat)* pair.

#### Scenario: Missing chat id falls back to a default chat pod

- **WHEN** `podMode` is `perChat` and a request carries `X-User-Id` but no
  `X-Session-Id`
- **THEN** the system SHALL route the request to a single per-user "default" chat
  pod (stable across requests) and SHALL log a warning.
