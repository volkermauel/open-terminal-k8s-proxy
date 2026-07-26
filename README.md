# Open Terminal K8s Proxy

A Kubernetes orchestrator that dynamically provisions per-user `open-terminal` instances.

## Overview

Open Terminal K8s Proxy acts as a reverse proxy and orchestrator that:

1. Accepts requests from Open WebUI with a `X-User-Id` header
2. Creates a dedicated terminal pod for each user (on demand)
3. Proxies all requests to the user's pod
4. Manages pod lifecycle (idle timeout, cleanup, eviction)

## Architecture

```
Open WebUI → K8s Proxy → User's Terminal Pod
                              ↓
                    Creates/manages pods via K8s API
```

Each user gets:

- A dedicated terminal pod (`terminal-{hash}`)
- Random API key for pod-to-proxy communication
- Isolated filesystem
- Optional persistent storage via PVC (`pvc-{hash}`)

## Installation

### Using Helm

```bash
helm install open-terminal-k8s-proxy ./open-terminal-k8s-proxy \
  --namespace terminals \
  --create-namespace \
  --set secret.data.PROXY_API_KEY=your-secret-key
```

### Configuration

| Parameter                                          | Default                            | Description                                                     |
|----------------------------------------------------|------------------------------------|---------------------------------------------------------------- |
| `secret.create`                                    | `true`                             | Create Secret with `secret.data` values                         |
| `secret.existingSecret`                            | `""`                               | Reference to external Secret (disables `create`)                |
| `secret.data.PROXY_API_KEY`                        | `""`                               | API key for Open WebUI → Proxy                                  |
| `terminalImage.repository`                         | `ghcr.io/open-webui/open-terminal` | Terminal container image                                        |
| `terminalImage.tag`                                | `latest`                           | Terminal image tag                                              |
| `storage.mode`                                     | `none`                             | Storage mode: `none`, `perUser`, `shared`, or `sharedRWO`       |
| `storage.perUser.size`                             | `5Gi`                              | PVC size per user (perUser mode)                                |
| `storage.shared.size`                              | `100Gi`                            | Shared PVC size (shared modes)                                  |
| `terminalResources.requests.ephemeral-storage`     | `5Gi`                              | Ephemeral storage request (scheduling)                          |
| `terminalResources.limits.ephemeral-storage`       | `5Gi`                              | Ephemeral storage limit (kubelet evicts pod if exceeded)        |
| `maxConcurrentPods`                                | `100`                              | Maximum concurrent terminal pods                                |
| `podIdleTimeoutSeconds`                            | `3600`                             | Idle timeout before pod termination                                    |
| `terminalResources.requests.cpu`                   | `500m`                             | CPU request per terminal pod                                    |
| `terminalResources.limits.cpu`                     | `1000m`                            | CPU limit per terminal pod                                      |
| `terminalResources.requests.memory`                | `512Mi`                            | Memory request per terminal pod                                 |
| `terminalResources.limits.memory`                  | `4Gi`                              | Memory limit per terminal pod                                   |
| `terminalNodeSelector`                             | `{}`                               | nodeSelector for terminal pods                                  |
| `terminalTolerations`                              | `[]`                               | Tolerations for terminal pods                                   |
| `terminalNetworkPolicy.enabled`                    | `false`                            | Create Kubernetes NetworkPolicy resources for terminal pods     |
| `terminalNetworkPolicy.mode`                       | `denyAll`                          | Terminal pod egress mode: `denyAll` or `allowNetworks`          |
| `terminalNetworkPolicy.allowedCIDRs`               | RFC1918, CGNAT, link-local         | CIDRs allowed when `mode` is `allowNetworks`                    |
| `terminalNetworkPolicy.dns.enabled`                | `false`                            | Also allow terminal pods to reach the configured DNS pods       |

### Terminal Network Policies

When `terminalNetworkPolicy.enabled` is true, the chart creates a Kubernetes
`NetworkPolicy` selecting dynamically created terminal pods with:

```yaml
app: open-terminal-user
managed-by: terminal-proxy
```

The selector labels are configurable via `terminalNetworkPolicy.podLabels` and
**must match** the proxy's `LABELS_APP` and `LABELS_MANAGED_BY` environment variables
(the defaults above match the proxy defaults). If you customize those labels on the
proxy, update `podLabels` accordingly — otherwise the policy selects no pods and
provides no isolation.

The policy always isolates ingress for terminal pods. By default, only the proxy
pod is allowed to connect to terminal pods on port `8000`, so Open WebUI traffic
can still flow through the proxy.

**No network at all** for terminal pod egress:

```yaml
terminalNetworkPolicy:
  enabled: true
  mode: denyAll
```

**Specific or configurable networks only**:

```yaml
terminalNetworkPolicy:
  enabled: true
  mode: allowNetworks
  allowedCIDRs:
    - 10.20.0.0/16
    - 192.168.50.0/24
```

**No internet but private/internal networks allowed** uses the same
`allowNetworks` mode with the default non-internet CIDRs:

```yaml
terminalNetworkPolicy:
  enabled: true
  mode: allowNetworks
  allowedCIDRs:
    - 10.0.0.0/8
    - 172.16.0.0/12
    - 192.168.0.0/16
    - 100.64.0.0/10
    - 169.254.0.0/16
```

Kubernetes `NetworkPolicy` is allow-list based. If your cluster pod CIDR,
service CIDR, VPC CIDR, or on-prem ranges are outside the defaults, add them to
`allowedCIDRs`. DNS egress is off by default; enable
`terminalNetworkPolicy.dns.enabled` only when terminals need name resolution and
adjust the selectors if your DNS pods are not labeled as `k8s-app: kube-dns` in
the `kube-system` namespace.

**Caveats:**

- **`denyAll` with DNS:** when `mode: denyAll` but `dns.enabled: true`, DNS egress
  (UDP/TCP 53 to the configured DNS pods) remains allowed — i.e. "deny all except DNS".
  Leave DNS disabled for a fully closed egress.
- **IPv4 only by default:** the default `allowedCIDRs` are IPv4 ranges. IPv6-only or
  dual-stack clusters must add their own IPv6 CIDRs.
- **Policies are additive:** Kubernetes `NetworkPolicy` is additive across policies in
  the same namespace. If other policies also select these terminal pods, they can widen
  the allowed traffic; this policy only fully denies egress when it is the sole egress
  policy selecting the pods.

### Understanding Storage

Terminal pods have two independent storage controls:

**1. PVC** (`storage.mode`) — optional persistent volume mounted at `/data`:

- `none` (default): no PVC, no mounted volume. Users write to the container filesystem.
- `perUser`: dedicated PVC per user, survives pod restarts
- `shared` / `sharedRWO`: shared PVC across users

**2. Ephemeral storage limits** (`terminalResources.*.ephemeral-storage`) — limits **total writable space** on the container:

- Container writable layer (`/tmp`, `/home`, `/var`, etc.)
- Container logs
- Enforced by kubelet — pod is evicted if the limit is exceeded

These are orthogonal. When `storage.mode: none`, ephemeral-storage limits are the **only** protection against a runaway `pip install` filling the node disk. When using a PVC, ephemeral-storage limits still protect writes *outside* the PVC mount.

### Storage Modes

1. **none** (default): No PVC. Container filesystem only.
   - All writes go to the container's writable layer
   - Protected by `ephemeral-storage` limits (kubelet-enforced)
   - Data destroyed when pod terminates
2. **perUser**: Each user gets their own PVC
    - Best isolation
    - Works with any StorageClass
    - Volume ownership set to gid 1000 via `fsGroup`
3. **shared**: Single PVC with ReadWriteMany access
    - Requires RWX-capable storage (NFS, CephFS)
    - Single volume for all users
    - Volume ownership set to gid 1000 via `fsGroup`
4. **sharedRWO**: Single PVC with ReadWriteOnce + node affinity
    - Works with standard RWO storage
    - All terminal pods scheduled to same node
    - Volume ownership set to gid 1000 via `fsGroup`

When a PVC is mounted, the pod `securityContext` is set with `fsGroup: 1000` and `fsGroupChangePolicy: "Always"`, ensuring the mounted volume contents are owned by gid 1000 on every pod start.

### Per-chat isolation & working directory

Every terminal pod with a mounted volume launches `open-terminal` with
`--cwd <mount>` (the PVC mount path, also set as `HOME`), so terminals open **inside**
the persistent data volume (`/data`) and writes reach the PVC. (In `none` mode no `--cwd`
is passed and `open-terminal` uses its image default working directory.) This fixes a bug
where terminals opened in the container root and writes never reached the PVC.

On top of that, each chat — identified by the `X-Session-Id` header Open WebUI sends —
gets its own working directory `<mount>/<chatid>`, created automatically on first use, so
concurrent chats don't clobber each other's files. Enable/disable with:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `terminal.dataMountPath` | `/data` | Path the PVC is mounted at (and used as `--cwd` / `HOME`) |
| `perChatDirs.enabled` | `true` | Create a per-chat working directory per `X-Session-Id` |

By default the proxy runs **one pod per user** (`podMode: perUser`). For stronger
isolation you can run **one pod per chat**. Two per-chat strategies are available:

- `perChat` — one shared RWX PVC for the whole cluster; each user gets a `subPath`.
- `perUserPerChat` — a **dedicated RWX PVC per user**, shared by that user's chat pods.

In both per-chat modes each chat (identified by `X-Session-Id`) gets its own pod and its
own working directory `<mount>/<chatid>` on the volume.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `podMode` | `perUser` | `perUser` (one pod per user), `perChat` (one pod per chat; requires `storage.mode: shared`), or `perUserPerChat` (one pod per chat with a dedicated per-user RWX PVC; requires `storage.mode: perUser`) |
| `maxPodsPerUser` | `5` | Max concurrent terminal pods per user in per-chat modes (the user's oldest pod is evicted; `0` = unlimited) |
| `chatPodIdleTimeoutSeconds` | `300` | Idle timeout for per-chat pods (users switching chats frequently won't accumulate pods) |

```yaml
# Option A: pod-per-chat on a single shared RWX volume
storage:
  mode: shared
podMode: perChat
chatPodIdleTimeoutSeconds: 300

# Option B: pod-per-chat with a dedicated RWX PVC per user
storage:
  mode: perUser
podMode: perUserPerChat
```

> **Note:** `perChat` requires `storage.mode: shared`; `perUserPerChat` requires
> `storage.mode: perUser` (per-user PVCs, created ReadWriteMany so the user's chat pods
> can mount concurrently). The proxy refuses to start otherwise. Per-chat isolation
> also requires the client to send `X-Session-Id`; without it, chats fall back to a
> single shared directory (and a single "default" pod per user). Per-chat modes can
> spawn many pods per user; `maxPodsPerUser` bounds that (evicting the user's oldest
> pod), and the global `maxConcurrentPods` still applies across all users.
>
> Terminal pods are launched as `open-terminal run --cwd <mount>` (the `run` subcommand
> is kept because container `args` replace the image `CMD`, not its `ENTRYPOINT`).

## Integration with Open WebUI

Add this proxy as an "Open Terminal" integration in Open WebUI admin settings:

- Name: `K8s Terminal Proxy`
- URL: `http://open-terminal-k8s-proxy.terminals.svc.cluster.local:8000`
- API Key: (the value you set in `secret.data.PROXY_API_KEY`)

## Using External Secrets

Instead of managing secrets in `values.yaml`, you can reference an existing Kubernetes Secret by setting `secret.existingSecret`. When using an external Secret, it must contain a `PROXY_API_KEY` key with the API key value.

## API Endpoints

The proxy implements the same API as open-terminal:

- `GET /files/list` - List files
- `GET/POST /files/read` - Read file content
- `POST /files/write` - Write file content
- `POST /files/replace` - Replace content in file
- `GET /files/grep` - Search file contents
- `GET /files/glob` - Search files by pattern
- `GET /info` - Operator-provided environment info
- `POST /execute` - Run command
- `GET /execute/{id}/status` - Get command status
- `POST /execute/{id}/input` - Send input to command
- `DELETE /execute/{id}` - Kill command
- WebSocket: `/api/terminals/{session_id}` - Interactive terminal session
- All other open-terminal endpoints (`/files/mkdir`, `/files/move`, `/files/delete`, `/files/upload`, `/files/archive`, `/notebooks/*`, `/system`) are forwarded transparently but hidden from the OpenAPI schema.

## Resource Requirements

Proxy:

- CPU: 100m request / 500m limit
- Memory: 128Mi request / 512Mi limit

Per terminal pod:

- CPU: 500m request / 1000m limit
- Memory: 512Mi request / 4Gi limit
- Ephemeral storage: 5Gi request / 5Gi limit (kubelet-enforced)

## Attributions

| PR | Title | Author | Date |
|----|-------|--------|------|
| #1 | Add support for emptyDir storage mode | [@ymarcus93](https://github.com/ymarcus93) | 2026-03-28 |
| #2 | Add terminal pod scheduling (nodeSelector+tolerations) | [@ymarcus93](https://github.com/ymarcus93) | 2026-03-28 |

## License

MIT
