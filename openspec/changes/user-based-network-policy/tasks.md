# Tasks: Per-user egress NetworkPolicy

## 1. Config & rules model

- [ ] 1.1 Add `network_policy_config_path: str` setting to `terminal_proxy/config.py` (default empty ⇒ disabled).
- [ ] 1.2 Add a rules data model (pydantic) for the JSON file: `NetworkPolicyRules { default: UserEgressRule | None, users: dict[str, UserEgressRule] }`, `UserEgressRule { allowedCIDRs: list[str], deniedCIDRs: list[str] = [], dns: bool = False }`.
- [ ] 1.3 Implement a loader `load_network_policy_rules(path) -> NetworkPolicyRules | None` that validates JSON, validates every CIDR in `allowedCIDRs`/`deniedCIDRs` with `ipaddress`, and returns `None` (logging an error) on any failure. Log a warning for any `deniedCIDR` that is a supernet of an allowed CIDR (contradiction `except` cannot express).
- [ ] 1.4 Add DNS-config settings the proxy needs to emit DNS rules (ports + selectors) mirroring `terminalNetworkPolicy.dns` defaults.

## 2. Kubernetes client

- [ ] 2.1 Add lazy `NetworkingV1Api` to `terminal_proxy/k8s/client.py`.
- [ ] 2.2 Add `create_network_policy(manifest)`, `get_network_policy(name)`, and `delete_network_policy(name)` (ignore 404), with the same retry decorator as other methods.

## 3. Network policy builder

- [ ] 3.1 Create `terminal_proxy/k8s/network_policy_builder.py` with `build_network_policy_for_user(terminal_pod, rule, dns_cfg) -> dict`.
- [ ] 3.2 Manifest: name `terminal-netpol-<user_hash>`, `podSelector` matchLabels `user-id-hash: <hash>`, `policyTypes: [Egress]`, one egress `ipBlock` rule per allowed CIDR with `deniedCIDRs` attached as `except` on each allow CIDR that is their supernet, optional DNS rule (UDP+TCP on configured ports to configured selectors).

## 4. Pod builder annotation

- [ ] 4.1 In `terminal_proxy/k8s/pod_builder.py`, add a `user-id` annotation (value = `terminal_pod.user_id`) to all pod manifests that currently set labels, alongside the existing `user-id-hash` label.

## 5. PodManager lifecycle hooks

- [ ] 5.1 In `_create_pod_resources`, after pod creation: if rules loaded and user has a rule (explicit or `default`), build + create the per-user `NetworkPolicy`; log it.
- [ ] 5.2 In `_delete_pod`, delete the per-user `NetworkPolicy` (ignore 404), grouped with the existing service/pod/secret cleanup.
- [ ] 5.3 In `_reconcile_existing_pods`, for each running pod whose `user-id` annotation maps to a current rule but whose `terminal-netpol-<hash>` policy is missing, recreate it.

## 6. Helm chart

- [ ] 6.1 `templates/role.yaml`: add `networkpolicies` (`networking.k8s.io`) with `get, list, watch, create, delete` (gate on `terminalNetworkPolicy.userRules.enabled`).
- [ ] 6.2 New `templates/configmap-networkpolicy.yaml`: render the rules JSON from `terminalNetworkPolicy.userRules.data` when enabled.
- [ ] 6.3 `templates/deployment.yaml`: mount the ConfigMap and set `NETWORK_POLICY_CONFIG_PATH` + DNS settings when enabled.
- [ ] 6.4 `values.yaml`: add `terminalNetworkPolicy.userRules` section (`enabled`, `data` with default/users shape).

## 7. Tests

- [ ] 7.1 Builder test: correct manifest for a user with allowed CIDRs + DNS; selector matches `user-id-hash`; no DNS rule when `dns=false`; `deniedCIDR` that is a subnet of an allow appears as `ipBlock.except`; `deniedCIDR` outside any allow is omitted.
- [ ] 7.2 Config/loader tests: valid file parses; malformed JSON ⇒ None; invalid CIDR (in allow or deny) ⇒ None; feature off when path empty; warn on deny-that-contains-allow.
- [ ] 7.3 PodManager tests: creates policy when enabled+rule present; skips when disabled/unlisted; deletes on teardown; rebuilds missing on reconcile.
- [ ] 7.4 Run `ruff check .`, `mypy terminal_proxy`, `pytest tests -v --tb=short`.

## 8. Docs

- [ ] 8.1 README: new section — additive model, JSON schema, CIDR-only limitation, RBAC note, example `values.yaml`.
- [ ] 8.2 Update the existing Terminal Network Policies section to recommend `mode: denyAll` as the base when per-user rules are used.
