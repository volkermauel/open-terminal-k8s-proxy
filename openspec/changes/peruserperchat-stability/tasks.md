# Tasks: perUserPerChat stability

## 1. Read endpoints stop creating pods

- [ ] 1.1 `models.py`: add `active_connections: int = 0` to `TerminalPod`.
- [ ] 1.2 `pod_manager.py`: add `async def lookup(user_id, chat_id) -> TerminalPod | None` — returns the running pod for the (user, chat) key from `_pods` without creating/touching k8s; does **not** bump `last_active_at`.
- [ ] 1.3 `main.py`: add `get_terminal_for_read(user_id, chat_id)` — `perChat`/`perUserPerChat` → `pod_manager.lookup` (503 if None); `perUser` → existing `get_terminal_for_user`.
- [ ] 1.4 `main.py`: switch all read/poll endpoints (`/ports`, `/proxy/{port}/{path}`, `/files/*`, `/execute*`, `/system`, `/info`, `GET /api/terminals`, `GET|DELETE /api/terminals/{id}`) to `get_terminal_for_read`.
- [ ] 1.5 `main.py`: `POST /api/terminals` stays on `get_terminal_for_user` (create); split the GET/POST handler so GET uses read-path.

## 2. Never evict a connected pod

- [ ] 2.1 `pod_manager.py`: `acquire(terminal)` / `release(terminal)` — inc/dec `active_connections`, touch `last_active_at` on acquire.
- [ ] 2.2 `main.py` websocket handler: `acquire` before `proxy_websocket`, `release` in `finally`.
- [ ] 2.3 `pod_manager.py`: per-user cap eviction, `_evict_oldest`, and `_cleanup_idle_pods` skip pods with `active_connections > 0`; if all candidates connected, cap path returns 503 instead of evicting.

## 3. Key resilience

- [ ] 3.1 `pod_manager.py`: `_api_key_from_pod(pod)` — read `OPEN_TERMINAL_API_KEY` `secretKeyRef` from the live pod spec, then the secret; fall back to name-based lookup.
- [ ] 3.2 `_reconcile_existing_pods` uses `_api_key_from_pod` so adopted pods authenticate with their real key.

## 4. Tests

- [ ] 4.1 `lookup` returns existing pod / None when absent (no k8s create calls).
- [ ] 4.2 `get_terminal_for_read` mode-aware (perChat 503 vs perUser create).
- [ ] 4.3 eviction + idle cleanup skip `active_connections > 0`.
- [ ] 4.4 reconcile reads key from pod's referenced secret.
- [ ] 4.5 websocket acquire/release brackets the proxy.

## 5. Verify & ship

- [ ] 5.1 `ruff check .`, `mypy terminal_proxy`, `pytest tests -v`, `helm lint` green.
- [ ] 5.2 Redeploy `perUserPerChat`; confirm no pod churn, WS connects, `/ports` 200.
