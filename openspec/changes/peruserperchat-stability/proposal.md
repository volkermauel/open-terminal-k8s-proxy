# Proposal: perUserPerChat stability

## Problem

Deployed `perUserPerChat` mode thrashes: a terminal pod is evicted and recreated
every ~5 s, websockets die (open-terminal returns `4004 Session not found`), and
all proxied calls intermittently return `401 Invalid API key`.

Root cause (verified against deployed logs + upstream `open-webui/open-terminal`
source):

1. **Reads create pods.** `get_terminal_for_user()` → `pod_manager.get_or_create()`
   (which *creates* pods) is called by **every** endpoint, including open-webui's
   5 s `GET /ports` poll. Polling an *evicted* chat's terminal instantly recreates
   its pod.
2. **Cap churn.** With active chat terminals > `MAX_PODS_PER_USER` (default 5),
   each recreation triggers per-user cap eviction of the oldest pod, which its own
   next poll recreates → unbounded churn. Churn evicts **in-use** pods (WS 4004).
3. **Key desync.** During the evict→recreate race the running pod keeps its
   startup key while a recreated secret carries a new key, so the proxy's
   `terminal.api_key` ≠ the pod's `OPEN_TERMINAL_API_KEY` → `401` (and WS `4001`).

The `$HOME`-in-prompt observation is **not** a bug: cwd isolation works
(`/data/<chatid>`); it is only the prompt label.

## Approach

1. **Reads must not create pods (mode-aware).** Add `PodManager.lookup()` (returns
   the running pod or `None`, no creation). Add `get_terminal_for_read()`: in
   `perChat`/`perUserPerChat` → lookup-only (503 if absent); `perUser` unchanged
   (create). Only `POST /api/terminals` creates pods.
2. **Never evict a connected pod.** Track active WS connections per pod
   (`TerminalPod.active_connections`); per-user cap eviction, global eviction and
   idle cleanup all skip pods with `active_connections > 0`.
3. **Key resilience.** Reconcile reads the api-key from the pod's *actual*
   referenced secret (container env `secretKeyRef`), not by name assumption, so
   restart/eviction races cannot desync keys.

## Out of scope

- Chat-deletion cleanup (open-webui `chat.deleted` webhook) — separate change.
- open-webui recreating evicted terminals on its own (client behavior).
