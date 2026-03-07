"""Prometheus metrics collection."""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

_request_counts: dict[str, list[float]] = defaultdict(list)
_request_latencies: dict[str, list[float]] = defaultdict(list)
_error_counts: dict[str, int] = defaultdict(int)
_pod_startup_durations: dict[str, float] = {}
_active_pods_by_state: dict[str, int] = defaultdict(int)


def record_request(method: str, path: str, latency_seconds: float, status_code: int) -> None:
    key = f"{method}:{path}"
    _request_latencies[key].append(latency_seconds)
    if len(_request_latencies[key]) > 1000:
        _request_latencies[key] = _request_latencies[key][-500:]
    if status_code >= 400:
        error_key = f"{key}:{status_code}"
        _error_counts[error_key] += 1


def record_pod_startup(user_hash: str, duration_seconds: float) -> None:
    _pod_startup_durations[user_hash] = duration_seconds
    if len(_pod_startup_durations) > 100:
        oldest = list(_pod_startup_durations.keys())[:50]
        for key in oldest:
            del _pod_startup_durations[key]


def update_pod_states(pods: dict[str, tuple[object, object]]) -> None:
    _active_pods_by_state.clear()
    for _, state in pods.values():
        state_str = getattr(state, "value", str(state))
        _active_pods_by_state[state_str] += 1


@contextmanager
def track_request_latency(method: str, path: str) -> Iterator[None]:
    start = time.time()
    yield
    latency = time.time() - start
    record_request(method, path, latency, 200)


def _calculate_percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * percentile / 100)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


def format_prometheus_metrics(
    active_pods: int,
    max_pods: int,
    storage_mode: str,
) -> str:
    lines = []

    lines.append("# HELP terminal_proxy_active_pods Number of active terminal pods")
    lines.append("# TYPE terminal_proxy_active_pods gauge")
    lines.append(f"terminal_proxy_active_pods {active_pods}")

    lines.append("# HELP terminal_proxy_max_pods Maximum allowed terminal pods")
    lines.append("# TYPE terminal_proxy_max_pods gauge")
    lines.append(f"terminal_proxy_max_pods {max_pods}")

    lines.append("# HELP terminal_proxy_storage_mode Current storage mode (1=active)")
    lines.append("# TYPE terminal_proxy_storage_mode gauge")
    lines.append(f'terminal_proxy_storage_mode{{mode="{storage_mode}"}} 1')

    lines.append("# HELP terminal_proxy_request_latency_seconds Request latency in seconds")
    lines.append("# TYPE terminal_proxy_request_latency_seconds summary")

    all_latencies: list[float] = []
    for _, latencies in _request_latencies.items():
        all_latencies.extend(latencies)

    if all_latencies:
        for p in [50, 90, 95, 99]:
            val = _calculate_percentile(all_latencies, p)
            lines.append(f'terminal_proxy_request_latency_seconds{{quantile="{p/100}"}} {val:.6f}')
        lines.append(f"terminal_proxy_request_latency_seconds_sum {sum(all_latencies):.6f}")
        lines.append(f"terminal_proxy_request_latency_seconds_count {len(all_latencies)}")

    lines.append("# HELP terminal_proxy_errors_total Total number of errors by endpoint and status")
    lines.append("# TYPE terminal_proxy_errors_total counter")
    for key, count in sorted(_error_counts.items()):
        parts = key.split(":", 2)
        if len(parts) == 3:
            method, path, status = parts
            lines.append(f'terminal_proxy_errors_total{{method="{method}",path="{path}",status="{status}"}} {count}')

    lines.append("# HELP terminal_proxy_pod_startup_duration_seconds Pod startup duration in seconds")
    lines.append("# TYPE terminal_proxy_pod_startup_duration_seconds summary")

    startup_durations = list(_pod_startup_durations.values())
    if startup_durations:
        for p in [50, 90, 95, 99]:
            val = _calculate_percentile(startup_durations, p)
            lines.append(f'terminal_proxy_pod_startup_duration_seconds{{quantile="{p/100}"}} {val:.6f}')
        lines.append(f"terminal_proxy_pod_startup_duration_seconds_sum {sum(startup_durations):.6f}")
        lines.append(f"terminal_proxy_pod_startup_duration_seconds_count {len(startup_durations)}")

    lines.append("# HELP terminal_proxy_pods_by_state Number of pods by state")
    lines.append("# TYPE terminal_proxy_pods_by_state gauge")
    for state, count in sorted(_active_pods_by_state.items()):
        lines.append(f'terminal_proxy_pods_by_state{{state="{state}"}} {count}')

    return "\n".join(lines) + "\n"
