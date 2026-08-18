"""Dependency-free TCHP metrics suitable for health/diagnostic endpoints."""

from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any


_COUNTER_METRICS = (
    "orchestration_context_reads_total",
    "orchestration_context_bytes_read_total",
    "orchestration_wake_coalesced_total",
    "orchestration_wake_failures_total",
    "orchestration_work_products_total",
    "orchestration_legacy_delegation_total",
    "orchestration_transcript_cross_role_reads_total",
)

_OBSERVATION_METRICS = (
    "orchestration_handoff_initial_prompt_bytes",
    "orchestration_handoff_context_refs",
    "orchestration_handoff_context_tokens_estimated",
    "orchestration_wakes_pending",
    "orchestration_wake_delivery_latency_seconds",
    "orchestration_task_blocked_duration_seconds",
)


class HandoffMetrics:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters: dict[str, int] = defaultdict(int)
        self._last: dict[str, int | float] = {}
        # Expose the complete metric contract even before the first event. This
        # makes a fresh process distinguishable from an exporter that forgot a
        # metric entirely.
        for name in _COUNTER_METRICS:
            self._counters[name] = 0
        for name in _OBSERVATION_METRICS:
            self._last[name] = 0

    def increment(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[str(name)] += int(value)

    def set_counter(self, name: str, value: int) -> None:
        """Synchronize a cumulative counter derived from durable state."""

        with self._lock:
            self._counters[str(name)] = max(0, int(value))

    def observe(self, name: str, value: int | float) -> None:
        with self._lock:
            self._last[str(name)] = value

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "last": dict(self._last),
            }
