# Design: Per-user egress NetworkPolicy

## Context

`open-terminal-k8s-proxy` spawns one terminal pod per user (`terminal-<hash>`)
and already creates/owns the per-user `Secret`, `Service`, and (optionally)
`PVC`. Each pod is labeled `app`, `managed-by`, and `user-id-hash`. The
`terminalNetworkPolicy` Helm feature (added in v0.3.0) renders a single
namespace-scoped `NetworkPolicy` that selects **all** terminal pods uniformly —
either `denyAll` or one shared `allowNetworks` CIDR list.

We need least-privilege egress **per user**: a curated user may reach specific
internal ranges plus a few named-external CIDRs, while everyone else stays
locked down.

Kubernetes `NetworkPolicy` has no notion of "user" — it selects pods by label.
So per-user rules must be realized as **per-user policies** that match each
pod's `user-id-hash`.

## Goals / Non-Goals

**Goals**

- Grant/restrict egress per `user_id`, dynamically, at pod creation.
- Single source of truth operators can edit without redeploying code.
- Additive on top of the existing `denyAll` base; opt-in, no breaking change.
- Reuse the established per-user resource pattern (create-on-spawn,
  delete-on-teardown, reconcile-on-restart).

**Non-Goals**

- In-cluster hostname/DNS-name egress matching (`NetworkPolicy` `ipBlock` is
  CIDR-only; operators resolve hostnames to ranges).
- Mutating/`patch`-ingress policies (Ingress stays governed by the base Helm
  policy).
- Drift correction for rules changed *after* a policy exists (rebuild-on-restart
  only handles missing policies; live re-apply of edited rules is a follow-up).
- NetworkPolicy logging / flow visibility.

## Decisions

### D1: Proxy creates per-user policies (not static Helm, not tiers)

The proxy already owns per-user `Secret`/`Service`/`PVC`; adding per-user
`NetworkPolicy` to the same lifecycle is the natural extension and gives
automatic provisioning for any user without redeploy. Static Helm per-user
policies require recomputing the hash and redeploying for every change; tier/
label grouping is coarser and needs a tier map anyway. **Chosen: proxy-driven.**

### D2: Rules from a ConfigMap-mounted JSON file, not env vars

A nested `user_id → { allowedCIDRs, deniedCIDRs, dns }` map is awkward to express as flat
pydantic env settings. A JSON file mounted from a `ConfigMap` is the idiomatic
K8s pattern, lets operators update rules via `kubectl apply`/Helm upgrade
without code changes, and is easy to validate. New setting
`NETWORK_POLICY_CONFIG_PATH` points at it; empty/unset ⇒ feature off.

File shape:

```json
{
  "default": { "allowedCIDRs": ["10.0.0.0/8"], "dns": true },
  "users": {
    "alice@example.com": { "allowedCIDRs": ["0.0.0.0/0"], "deniedCIDRs": ["10.0.0.0/8", "172.16.0.0/12"], "dns": true }
  }
}
```

### D2.1: Denied CIDRs realized as `ipBlock.except` (K8s has no native deny)

Kubernetes `NetworkPolicy` is allow-list only — there is no deny rule. So a
rule's optional `deniedCIDRs` are realized as `ipBlock.except` entries attached
to the allow CIDR(s) that are supernets of them. The builder attaches each
`deniedCIDR` (via `ipaddress` subnet checks) as an `except` on every allow CIDR
that contains it. Two edge cases, documented in the spec:

- A `deniedCIDR` not contained by any allow ⇒ no-op (already denied by the
  default-deny base); dropped silently.
- A `deniedCIDR` that is a *supernet* of an allow ⇒ contradiction `except`
  can't express; the loader logs a warning and the narrower allow stands.

Alternative considered: a separate policy/CRD with deny semantics (e.g.
Calico/Tigera `GlobalNetworkPolicy` deny rules). Rejected — would require a
specific CNI and breaks portability; `except` is standard K8s and sufficient for
carve-outs.

### D3: Policy selects `user-id-hash`; reconcile uses a new `user-id` annotation

`user-id-hash` (a `sha256` truncation) is already a pod label and is the right
selector. But `user_id → hash` is one-way, and reconcile (on proxy restart)
only has the pod — it cannot recover `user_id` to re-resolve rules. Today the
pod has only the hash label. **Add a `user-id` annotation** (annotation, not
label — avoids label-length/PII concerns) so reconcile can recover `user_id`
and rebuild a *missing* policy. Existing policies survive restart unchanged
(they are cluster state), so reconcile only rebuilds absent ones.

Alternative considered: key the rules map by `user_hash` instead. Rejected —
operators reason in `user_id`s, not hashes; keeping the operator-facing key as
`user_id` is clearer and audit-friendly.

### D4: Additive model; base policy must be `denyAll`

Per-user policies only *widen* egress for named users. For unlisted users to
stay restricted, the base Helm policy must default to `denyAll`. Per-user
policies set `policyTypes: [Egress]` only (Ingress stays with the base policy,
which already allows only the proxy on :8000).

### D5: DNS per user mirrors the Helm DNS config

Each per-user DNS rule reuses the same DNS namespace/pod selectors and ports as
`terminalNetworkPolicy.dns`, surfaced into the proxy so the proxy can emit an
identical rule. Avoids a second source of DNS truth.

### D6: Naming and 63-char limit

Policy name: `terminal-netpol-<12-char-hash>` ⇒ well under the 63-char K8s name
limit and collision-free per user.

## Risks / Trade-offs

- **[Additive policies can be widened by others]** → Document that any other
  namespace `NetworkPolicy` selecting the same pods adds allowed traffic; this
  feature only guarantees restriction when it (and the base) are the sole
  egress policies.
- **[CIDR-only, no hostnames]** → Spec rejects non-CIDR entries; README
  documents that operators must resolve external hostnames to ranges.
- **[Rules file edits don't live-apply to existing policies]** → Rebuild only
  happens for missing policies on restart. Mitigation: document a pod bounce
  (or future drift-correction task) to pick up rule changes for live pods.
- **[New RBAC surface]** → Scope `networkpolicies` to the proxy `Role`
  (namespace-scoped), verbs limited to what's needed.
- **[Malformed rules file]** → Treat as feature-disabled + log; never crash pod
  creation.

## Migration Plan

1. Roll out the new image + Helm chart (feature off by default — no behavior
   change).
2. Operators enable base `terminalNetworkPolicy` (`mode: denyAll`) and add a
   rules `ConfigMap`.
3. Set `NETWORK_POLICY_CONFIG_PATH` / Helm `userRules` to mount it.
4. New/restarted terminal pods for listed users get curated egress; all others
   stay `denyAll`.

- **Rollback**: unset the config path / disable the mount; delete any stray
  `terminal-netpol-*` policies (`kubectl delete netpol -l managed-by=...`).

## Open Questions

- Should edited rules auto-apply to already-running pods (drift correction), or
  is restart/reconcile sufficient for v1? (Current plan: restart/reconcile
  only.)
- Do we want an opt-in "full internet" (`0.0.0.0/0`) shorthand for power users,
  or require explicit CIDRs always? (Current plan: explicit CIDRs only.)
