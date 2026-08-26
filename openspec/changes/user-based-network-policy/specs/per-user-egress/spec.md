# Spec Delta: per-user-egress

## ADDED Requirements

### Requirement: Per-user egress policy provisioning

The system SHALL provision one namespace-scoped Kubernetes `NetworkPolicy` per
terminal pod, owned by the proxy, whose `podSelector` matches that pod's
`user-id-hash` label, so that egress rules can be granted to individual users
independently of other users.

#### Scenario: User with a rule gets a per-user policy on pod creation

- **WHEN** the proxy creates a terminal pod for `user_id` whose `user_id`
  appears in the configured user→egress map
- **THEN** the proxy SHALL create a `NetworkPolicy` named
  `terminal-netpol-<user_id_hash>` that (a) selects the pod via
  `user-id-hash: <user_id_hash>`, (b) sets `policyTypes: [Egress]` (Ingress is
  governed by the base Helm policy), and (c) contains one egress `ipBlock` rule
  per allowed CIDR, with each denied CIDR applied as an `except` entry on every
  allow CIDR that is its supernet.

#### Scenario: User with no rule gets no per-user policy

- **WHEN** the proxy creates a terminal pod for a `user_id` that is absent from
  the user→egress map and no `default` entry applies
- **THEN** the proxy SHALL NOT create a per-user `NetworkPolicy`, leaving the
  pod subject only to the base Helm policy (expected `denyAll`).

#### Scenario: Default entry applies when user is not explicitly listed

- **WHEN** the configured map contains a `default` entry and the proxy creates a
  pod for a `user_id` not explicitly listed
- **THEN** the per-user policy SHALL be built from the `default` entry's
  `allowedCIDRs`, `deniedCIDRs`, and `dns` values.

### Requirement: User→egress rules sourced from a ConfigMap-mounted JSON map

The system SHALL read per-user egress rules from a JSON file whose path is given
by the `NETWORK_POLICY_CONFIG_PATH` setting (empty/unset ⇒ feature disabled).
The file format SHALL be an object with an optional `default` key and a `users`
object mapping `user_id` → `{ "allowedCIDRs": [<CIDR>, ...], "deniedCIDRs":
[<CIDR>, ...], "dns": <bool> }`, where `deniedCIDRs` is optional and `dns`
defaults to `false`.

#### Scenario: Feature disabled by default

- **WHEN** `NETWORK_POLICY_CONFIG_PATH` is empty or unset
- **THEN** the proxy SHALL create no per-user `NetworkPolicy` objects, even if a
  pod is created, regardless of the rules file.

#### Scenario: Malformed rules file disables the feature and logs an error

- **WHEN** the configured file exists but is not valid JSON or violates the
  schema
- **THEN** the proxy SHALL treat the feature as disabled (create no per-user
  policies), log an error, and continue serving pods.

#### Scenario: CIDR-only destinations

- **WHEN** any entry in a user's `allowedCIDRs` or `deniedCIDRs` is not a valid
  IPv4/IPv6 CIDR
- **THEN** the proxy SHALL reject the rules file as invalid (treated as
  malformed per the scenario above), because Kubernetes `NetworkPolicy`
  `ipBlock` (and its `except` list) accept CIDR ranges only and cannot match
  DNS hostnames.

### Requirement: Denied CIDR carve-outs via ipBlock.except

The system SHALL realize each `deniedCIDR` listed in a user rule as an `except`
entry on every allowed CIDR that is its supernet, because Kubernetes
`NetworkPolicy` has no first-class deny rule and `ipBlock.except` is the only
deny mechanism. A `deniedCIDR` not contained within any allowed CIDR SHALL be
treated as a no-op (the destination is already denied by the default-deny base
policy). The loader SHALL log a warning for any `deniedCIDR` that is a supernet
of an allowed CIDR — a contradiction that `except` cannot express — while still
emitting the narrower allow.

#### Scenario: Denied CIDR carves a hole in a broader allow

- **WHEN** a user's rule lists `"allowedCIDRs": ["0.0.0.0/0"]` and
  `"deniedCIDRs": ["10.0.0.0/8"]`
- **THEN** the per-user policy SHALL emit an egress rule
  `ipBlock: { cidr: 0.0.0.0/0, except: [10.0.0.0/8] }`, denying the internal
  range while allowing everything else.

#### Scenario: Denied CIDR outside any allow is a no-op

- **WHEN** a user's rule lists `"allowedCIDRs": ["203.0.113.0/24"]` and
  `"deniedCIDRs": ["10.0.0.0/8"]`
- **THEN** the per-user policy SHALL emit only the allow for `203.0.113.0/24`
  with no `except` entry, and the system SHALL NOT raise an error
  (`10.0.0.0/8` is already denied by default).

#### Scenario: Contradictory deny-that-contains-an-allow logs a warning

- **WHEN** a user's rule lists an allowed CIDR that is a subnet of a denied CIDR
- **THEN** the loader SHALL log a warning naming both CIDRs and continue, and
  the narrower allowed CIDR SHALL remain allowed (since `except` cannot deny a
  supernet of an allow).

### Requirement: Optional DNS egress per user

For a user whose rule sets `"dns": true`, the per-user `NetworkPolicy` SHALL
additionally allow egress to the cluster's DNS pods on the configured DNS ports
(UDP and TCP), using the same DNS selectors/ports as the base Helm
`terminalNetworkPolicy.dns` configuration. When `"dns"` is omitted it SHALL
default to `false`.

#### Scenario: DNS enabled for a user

- **WHEN** a user's rule has `"dns": true`
- **THEN** the per-user policy SHALL include a DNS egress rule targeting the
  configured DNS namespace/pod selectors on the configured ports for both UDP
  and TCP.

#### Scenario: DNS omitted defaults to off

- **WHEN** a user's rule omits the `dns` key
- **THEN** the per-user policy SHALL contain no DNS egress rule.

### Requirement: Policy lifecycle tied to pod lifecycle

The system SHALL delete a user's per-user `NetworkPolicy` when its terminal pod
is deleted, and SHALL rebuild a missing policy on reconciliation using the
pod's `user-id` annotation when the rules map still contains that user.

#### Scenario: Policy deleted with pod

- **WHEN** a terminal pod is torn down (idle eviction, health failure, or max-pod
  eviction)
- **THEN** the proxy SHALL delete that user's `terminal-netpol-<user_id_hash>`
  `NetworkPolicy`, ignoring not-found errors.

#### Scenario: Policy rebuilt on reconcile when missing

- **WHEN** the proxy restarts and reconciles an existing running terminal pod
  whose `user-id` annotation maps to a current rule, but no
  `terminal-netpol-<user_id_hash>` `NetworkPolicy` exists
- **THEN** the proxy SHALL recreate the per-user policy.

### Requirement: Additive model on top of a denyAll base

Per-user policies SHALL be additive and scoped to a single user. The base Helm
`terminalNetworkPolicy` is expected to run as `mode: denyAll` so that only users
with explicit per-user rules gain egress. The proxy SHALL document that other
namespace-scoped `NetworkPolicy` objects selecting the same pods can widen
traffic, since Kubernetes `NetworkPolicy` is additive.

#### Scenario: Unlisted user remains fully restricted

- **WHEN** the base policy is `denyAll` and a pod is created for a user with no
  rule and no applicable `default`
- **THEN** that pod SHALL have no per-user egress policy and therefore no
  outbound egress (modulo any unrelated namespace policies).

### Requirement: RBAC for NetworkPolicy resources

The proxy's Kubernetes `Role` SHALL grant `get`, `list`, `watch`, `create`, and
`delete` on `networkpolicies` in the `networking.k8s.io` API group when the
feature is enabled, so the proxy can manage per-user policies.

#### Scenario: Role grants networkpolicy verbs

- **WHEN** the chart is rendered with the feature enabled
- **THEN** the proxy `Role` SHALL include a rule for `networkpolicies`
  (`networking.k8s.io`) with at least `get`, `list`, `create`, and `delete`.
