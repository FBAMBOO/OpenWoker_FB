"""Task relation graph rules and event-driven parent/blocker resolution."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Optional, Sequence

from .handoff_models import TaskRelationRecord, TaskRelationType
from .store import OrchestrationStore


def _command(scope: str, value: Mapping[str, Any]) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return f"relation:{scope}:{hashlib.sha256(payload.encode()).hexdigest()[:24]}"


class TaskRelationService:
    def __init__(self, store: OrchestrationStore) -> None:
        self.store = store

    def add_relation(
        self,
        from_task_id: str,
        to_task_id: str,
        relation_type: TaskRelationType | str,
        **kwargs: Any,
    ) -> TaskRelationRecord:
        return self.store.add_relation(
            from_task_id, to_task_id, relation_type, **kwargs
        )

    def remove_relation(
        self,
        relation_id: str,
        *,
        actor: str,
        command_id: Optional[str] = None,
    ) -> TaskRelationRecord:
        return self.store.remove_relation(
            relation_id,
            actor=actor,
            command_id=(
                command_id
                or _command("remove", {"relation_id": relation_id, "actor": actor})
            ),
        )

    def list_relations(self, task_id: str) -> tuple[TaskRelationRecord, ...]:
        return self.store.list_relations(task_id)

    def replace_blockers(
        self,
        task_id: str,
        blocker_ids: Sequence[str],
        *,
        reason: str,
        owner: str,
        required_action: str,
        **kwargs: Any,
    ) -> tuple[TaskRelationRecord, ...]:
        return self.store.replace_blockers(
            task_id,
            blocker_ids,
            reason=reason,
            owner=owner,
            required_action=required_action,
            **kwargs,
        )

    def assert_no_cycle(
        self,
        from_task_id: str,
        to_task_id: str,
        relation_type: TaskRelationType | str,
    ) -> None:
        # A temporary add/remove would make the check racy. The store repeats this
        # exact graph check under BEGIN IMMEDIATE when the relation is persisted.
        relation_type = TaskRelationType(relation_type)
        with self.store._read() as connection:
            cycle = self.store._relation_cycle_path(
                connection, from_task_id, to_task_id, relation_type
            )
        if cycle:
            raise ValueError("relation would create a cycle: " + " -> ".join(cycle))

    def resolve_terminal(self, task_id: str) -> dict[str, Any]:
        return self.store.resolve_terminal_relations(task_id)
