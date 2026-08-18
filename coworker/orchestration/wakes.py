"""Durable wake queue facade used by scheduler, recovery and diagnostics."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from .handoff_models import WakeReason, WakeRequestRecord, WakeStatus
from .store import OrchestrationStore


class WakeService:
    def __init__(
        self,
        store: OrchestrationStore,
        *,
        max_attempts: int = 5,
        backoff_seconds: int = 1,
    ) -> None:
        self.store = store
        self.max_attempts = max(1, int(max_attempts))
        self.backoff_seconds = max(1, int(backoff_seconds))

    def enqueue_wake(self, target_task_id: str, reason: WakeReason | str, **kwargs):
        return self.store.enqueue_wake(target_task_id, reason, **kwargs)

    def claim_ready_wake(
        self, owner: str, *, claim_seconds: int = 60
    ) -> Optional[WakeRequestRecord]:
        return self.store.claim_ready_wake(owner, claim_seconds=claim_seconds)

    def defer_wake(self, wake_id: str, *, not_before: datetime) -> WakeRequestRecord:
        return self.store.defer_wake(wake_id, not_before=not_before)

    def mark_delivered(self, wake_id: str) -> WakeRequestRecord:
        return self.store.mark_wake_delivered(wake_id)

    def mark_completed(self, wake_id: str) -> WakeRequestRecord:
        return self.store.mark_wake_completed(wake_id)

    def mark_failed(self, wake_id: str, error: str) -> WakeRequestRecord:
        return self.store.mark_wake_failed(
            wake_id,
            error,
            max_attempts=self.max_attempts,
            backoff_seconds=self.backoff_seconds,
        )

    def recover_expired_claims(self) -> int:
        return self.store.recover_expired_wake_claims()

    def activate_due(self) -> int:
        return self.store.activate_due_wakes()

    def bind_to_run(
        self, wake_id: str, run_id: str, *, owner: str
    ) -> WakeRequestRecord:
        return self.store.bind_wake_to_run(wake_id, run_id, owner=owner)

    def list_wakes(
        self,
        *,
        task_id: Optional[str] = None,
        statuses: Optional[Sequence[WakeStatus]] = None,
        limit: int = 1_000,
        offset: int = 0,
    ) -> tuple[WakeRequestRecord, ...]:
        return self.store.list_wakes(
            task_id=task_id, statuses=statuses, limit=limit, offset=offset
        )
