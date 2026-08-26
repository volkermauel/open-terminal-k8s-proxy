## Why

The existing `terminalNetworkPolicy` feature applies a single egress policy to
**all** terminal pods uniformly (either `denyAll` or one shared `allowNetworks`
CIDR list). Operators cannot grant a *specific user* broader reach (e.g. to a
curated set of internal ranges plus a few external destinations) while keeping
everyone else locked down. This makes least-privilege per-user access — the
common "trusted user X may reach internal + named external systems, everyone
else gets no egress" pattern — impossible to express today.

## What Changes

- **Proxy-driven per-user `NetworkPolicy`.** When the proxy creates a terminal
  pod for a user, it also creates a namespace-scoped `NetworkPolicy` whose
  `podSelector` matches that user's `user-id-hash` pod label, granting the
  egress defined for that user. The policy is deleted when the pod is torn down.
- **Per-user allow + deny lists.** Each user defines `allowedCIDRs` and optional
  `deniedCIDRs`, plus optional DNS egress. Since Kubernetes `NetworkPolicy` has
  no first-class deny, denied CIDRs are realized as `ipBlock.except` carve-outs
  on the allow ranges; anything not allowed is already denied by the
  default-deny base policy.
- **User→egress map sourced from a ConfigMap.** A new setting
  `NETWORK_POLICY_CONFIG_PATH` points at a JSON file (mounted from a
  `ConfigMap`) mapping `user_id` → `{ allowedCIDRs, deniedCIDRs, dns }`, with an
  optional `default` entry. Operators edit one source of truth and never touch
  per-pod manifests.
- **Additive model on top of `denyAll`.** The Helm base policy is expected to
  run as `mode: denyAll`; per-user policies then *widen* egress for named users
  only. A user with no rule gets no per-user policy and stays fully restricted.
- **New `user-id` pod annotation** so the proxy can recover the `user_id` from a
  reconciled pod (only `user-id-hash` is a label today) and rebuild its policy
  on restart if needed.
- **RBAC grant** for `networkpolicies` (networking.k8s.io).
- **Helm `ConfigMap` + deployment wiring** to mount the rules file and set the
  env var.

No **BREAKING** changes; per-user behavior is opt-in (disabled unless the
config path is set and the base policy is enabled).

## Capabilities

### New Capabilities

- `per-user-egress`: Proxy creates/owns a namespace-scoped Kubernetes
  `NetworkPolicy` per terminal pod, keyed by `user_id`, granting that user a
  curated set of egress CIDRs with explicit denied-CIDR carve-outs
  (`ipBlock.except`) and optional DNS. Rules are sourced from a
  ConfigMap-mounted JSON map with a `default` fallback.

### Modified Capabilities

*None yet* — there are no existing `openspec/specs/`. (The global
`terminalNetworkPolicy` Helm capability currently has no spec; if/when one is
written, this change relates to it but does not alter its requirements.)

## Impact

- **Code**
  - `terminal_proxy/config.py`: new `network_policy_config_path` setting + rules
    model + JSON loader (with CIDR validation and deny-contradiction warnings).
  - `terminal_proxy/k8s/client.py`: `NetworkingV1Api`; `create/get/delete
    /network_policy` methods.
  - `terminal_proxy/k8s/network_policy_builder.py` (new): build the per-user
    manifest (selects `user-id-hash`; `ipBlock` allow rules with `except`
    carve-outs; optional DNS rule).
  - `terminal_proxy/k8s/pod_builder.py`: add `user-id` annotation alongside the
    existing labels.
  - `terminal_proxy/pod_manager.py`: create policy in `_create_pod_resources`
    when enabled + user has rules; delete in `_delete_pod`; reconcile rebuilds
    missing policy from the `user-id` annotation.
- **Helm**
  - `templates/role.yaml`: add `networkpolicies` (get/list/create/delete/patch),
    apiGroup `networking.k8s.io`.
  - `templates/configmap-networkpolicy.yaml` (new): render the rules JSON.
  - `templates/deployment.yaml`: env var + optional volume mount.
  - `values.yaml`: new `terminalNetworkPolicy.userRules` section + mount flag.
- **Docs**: README section explaining the additive model, the JSON schema, the
  CIDR-only limitation (K8s `NetworkPolicy` cannot match DNS hostnames), and that
  `deniedCIDRs` are `ipBlock.except` carve-outs (K8s has no native deny).
- **Dependencies**: none new (uses the existing `kubernetes` client).
- **Constraints to document**: `NetworkPolicy` `ipBlock` is CIDR-only —
  "external systems" must be expressed as IP ranges; a user with no rule stays
  at the base `denyAll`; `deniedCIDRs` are realized only as `ipBlock.except`
  carve-outs (K8s `NetworkPolicy` has no first-class deny, so a deny outside any
  allow is a no-op and a deny that contains an allow is a logged contradiction);
  additive policies mean other namespace policies can widen traffic.
