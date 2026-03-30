# Open Terminal K8s Proxy

A Kubernetes orchestrator that dynamically provisions per-user `open-terminal` instances,## Overview

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
  --set proxyApiKey=your-secret-key
```

### Configuration

| Parameter                                          | Default                            | Description                                                     |
|----------------------------------------------------|------------------------------------|---------------------------------------------------------------- |
| `proxyApiKey`                                      | (auto-generated)                   | API key for Open WebUI → Proxy                                  |
| `terminalImage.repository`                         | `ghcr.io/open-webui/open-terminal` | Terminal container image                                        |
| `terminalImage.tag`                                | `latest`                           | Terminal image tag                                              |
| `storage.mode`                                     | `none`                             | Storage mode: `none`, `perUser`, `shared`, or `sharedRWO`       |
| `storage.perUser.size`                             | `5Gi`                              | PVC size per user (perUser mode)                                |
| `storage.shared.size`                              | `100Gi`                            | Shared PVC size (shared modes)                                  |
| `terminalResources.requests.ephemeral-storage`     | `5Gi`                              | Ephemeral storage request (scheduling)                          |
| `terminalResources.limits.ephemeral-storage`       | `5Gi`                              | Ephemeral storage limit (kubelet evicts pod if exceeded)        |
| `maxConcurrentPods`                                | `100`                              | Maximum concurrent terminal pods                                |
| `podIdleTimeoutSeconds`                            | `300`                              | Idle timeout before pod termination                             |
| `terminalResources.requests.cpu`                   | `500m`                             | CPU request per terminal pod                                    |
| `terminalResources.limits.cpu`                     | `1000m`                            | CPU limit per terminal pod                                      |
| `terminalResources.requests.memory`                | `512Mi`                            | Memory request per terminal pod                                 |
| `terminalResources.limits.memory`                  | `4Gi`                              | Memory limit per terminal pod                                   |
| `terminalNodeSelector`                             | `{}`                               | nodeSelector for terminal pods                                  |
| `terminalTolerations`                              | `[]`                               | Tolerations for terminal pods                                   |

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
3. **shared**: Single PVC with ReadWriteMany access
   - Requires RWX-capable storage (NFS, CephFS)
   - Single volume for all users
4. **sharedRWO**: Single PVC with ReadWriteOnce + node affinity
   - Works with standard RWO storage
   - All terminal pods scheduled to same node

## Integration with Open WebUI

Add this proxy as an "Open Terminal" integration in Open WebUI admin settings:

- Name: `K8s Terminal Proxy`
- URL: `http://open-terminal-k8s-proxy.terminals.svc.cluster.local:8000`
- API Key: (the value you set in `proxyApiKey`)

## API Endpoints

The proxy implements the same API as open-terminal:

- `GET /files/list` - List files
- `GET/POST /files/read` - Read file content
- `POST /files/write` - Write file content
- `POST /files/replace` - Replace content in file
- `GET /files/grep` - Search file contents
- `GET /files/glob` - Search files by pattern
- `POST /execute` - Run command
- `GET /execute/{id}/status` - Get command status
- `POST /execute/{id}/input` - Send input to command
- `DELETE /execute/{id}` - Kill command
- WebSocket: `/api/terminals/{session_id}` - Interactive terminal session

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
