"""Canonical Task Quality V2 workflow event state machine.

The ordered transition table in this module is the single executable source
for runtime validation, OpenAPI schema projection and generated GUI types.
Callers that already own a SQLite transaction use
``transition_workflow_in_transaction`` so their domain mutation and workflow
event commit atomically.  The public ``apply_workflow_event`` entrypoint also
records rejected attempts in a separate committed audit transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, TYPE_CHECKING

from ..errors import ConflictError, NotFoundError
from .models import (
    Archetype,
    BudgetMode,
    WorkflowStatus,
    canonical_status_values,
)

if TYPE_CHECKING:
    import sqlite3

    from ..store import OrchestrationStore


class _StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class WorkflowEvent(_StrEnum):
    ANALYSIS_REQUESTED = "analysis_requested"
    CANCEL_REQUESTED = "cancel_requested"
    ANALYSIS_READY = "analysis_ready"
    TARGET_AMBIGUOUS = "target_ambiguous"
    ANALYSIS_FAILED = "analysis_failed"
    TARGET_SELECTED = "target_selected"
    START_REQUESTED = "start_requested"
    CANDIDATE_CREATED = "candidate_created"
    RUNTIME_FAILED = "runtime_failed"
    CRASH_DETECTED = "crash_detected"
    VALIDATION_REQUIRES_REVIEW = "validation_requires_review"
    REPAIRABLE_FAILURE = "repairable_failure"
    ATTENTION_REQUIRED = "attention_required"
    FATAL_FAILURE = "fatal_failure"
    QUALITY_PUBLISHABLE = "quality_publishable"
    REPAIRED_CANDIDATE_CREATED = "repaired_candidate_created"
    REPAIR_EXHAUSTED = "repair_exhausted"
    REPAIR_FAILED = "repair_failed"
    RECOVERY_SUCCEEDED = "recovery_succeeded"
    RECOVERY_UNCERTAIN = "recovery_uncertain"
    RECONCILED_RESUME = "reconciled_resume"
    RECONCILED_FAIL = "reconciled_fail"
    RESUME_REQUESTED = "resume_requested"
    REPAIR_REQUESTED = "repair_requested"
    ARCHIVE_REQUESTED = "archive_requested"
    RETRY_REQUESTED = "retry_requested"


@dataclass(frozen=True)
class WorkflowTransitionRule:
    source: WorkflowStatus
    event: WorkflowEvent
    targets: tuple[WorkflowStatus, ...]
    uses_resume_status: bool = False
    server_selects_target: bool = False


@dataclass(frozen=True)
class WorkflowTransition:
    task_id: str
    event: WorkflowEvent
    from_status: WorkflowStatus
    to_status: WorkflowStatus
    resume_status: WorkflowStatus | None


def _rule(
    source: WorkflowStatus,
    event: WorkflowEvent,
    *targets: WorkflowStatus,
    uses_resume_status: bool = False,
    server_selects_target: bool = False,
) -> WorkflowTransitionRule:
    return WorkflowTransitionRule(
        source=source,
        event=event,
        targets=tuple(targets),
        uses_resume_status=uses_resume_status,
        server_selects_target=server_selects_target,
    )


# Keep this in specification order.  Do not introduce a transition elsewhere;
# consumers and generators derive their snapshots directly from this tuple.
WORKFLOW_TRANSITIONS: tuple[WorkflowTransitionRule, ...] = (
    _rule(WorkflowStatus.DRAFT, WorkflowEvent.ANALYSIS_REQUESTED, WorkflowStatus.ANALYZING),
    _rule(WorkflowStatus.DRAFT, WorkflowEvent.CANCEL_REQUESTED, WorkflowStatus.CANCELED),
    _rule(WorkflowStatus.ANALYZING, WorkflowEvent.ANALYSIS_READY, WorkflowStatus.READY),
    _rule(
        WorkflowStatus.ANALYZING,
        WorkflowEvent.TARGET_AMBIGUOUS,
        WorkflowStatus.NEEDS_TARGET_SELECTION,
    ),
    _rule(WorkflowStatus.ANALYZING, WorkflowEvent.ANALYSIS_FAILED, WorkflowStatus.DRAFT),
    _rule(WorkflowStatus.ANALYZING, WorkflowEvent.CANCEL_REQUESTED, WorkflowStatus.CANCELED),
    _rule(
        WorkflowStatus.NEEDS_TARGET_SELECTION,
        WorkflowEvent.TARGET_SELECTED,
        WorkflowStatus.ANALYZING,
    ),
    _rule(
        WorkflowStatus.NEEDS_TARGET_SELECTION,
        WorkflowEvent.CANCEL_REQUESTED,
        WorkflowStatus.CANCELED,
    ),
    _rule(WorkflowStatus.READY, WorkflowEvent.START_REQUESTED, WorkflowStatus.RUNNING),
    _rule(WorkflowStatus.READY, WorkflowEvent.CANCEL_REQUESTED, WorkflowStatus.CANCELED),
    _rule(WorkflowStatus.RUNNING, WorkflowEvent.CANDIDATE_CREATED, WorkflowStatus.VALIDATING),
    # The budget section and AC-B-003 require a live provider turn that reaches
    # its hard rail to pause for operator attention.  It uses the same bounded
    # attention event and remembers ``running`` for a server-authorized resume.
    _rule(WorkflowStatus.RUNNING, WorkflowEvent.ATTENTION_REQUIRED, WorkflowStatus.NEEDS_ATTENTION),
    _rule(WorkflowStatus.RUNNING, WorkflowEvent.RUNTIME_FAILED, WorkflowStatus.FAILED),
    _rule(WorkflowStatus.RUNNING, WorkflowEvent.CRASH_DETECTED, WorkflowStatus.RECOVERING),
    _rule(WorkflowStatus.RUNNING, WorkflowEvent.CANCEL_REQUESTED, WorkflowStatus.CANCELED),
    _rule(
        WorkflowStatus.VALIDATING,
        WorkflowEvent.VALIDATION_REQUIRES_REVIEW,
        WorkflowStatus.REVIEWING,
    ),
    _rule(WorkflowStatus.VALIDATING, WorkflowEvent.REPAIRABLE_FAILURE, WorkflowStatus.REPAIRING),
    _rule(WorkflowStatus.VALIDATING, WorkflowEvent.ATTENTION_REQUIRED, WorkflowStatus.NEEDS_ATTENTION),
    _rule(WorkflowStatus.VALIDATING, WorkflowEvent.FATAL_FAILURE, WorkflowStatus.FAILED),
    _rule(WorkflowStatus.VALIDATING, WorkflowEvent.CRASH_DETECTED, WorkflowStatus.RECOVERING),
    _rule(WorkflowStatus.REVIEWING, WorkflowEvent.QUALITY_PUBLISHABLE, WorkflowStatus.COMPLETED),
    _rule(WorkflowStatus.REVIEWING, WorkflowEvent.REPAIRABLE_FAILURE, WorkflowStatus.REPAIRING),
    _rule(WorkflowStatus.REVIEWING, WorkflowEvent.ATTENTION_REQUIRED, WorkflowStatus.NEEDS_ATTENTION),
    _rule(WorkflowStatus.REVIEWING, WorkflowEvent.FATAL_FAILURE, WorkflowStatus.FAILED),
    _rule(WorkflowStatus.REVIEWING, WorkflowEvent.CRASH_DETECTED, WorkflowStatus.RECOVERING),
    _rule(
        WorkflowStatus.REPAIRING,
        WorkflowEvent.REPAIRED_CANDIDATE_CREATED,
        WorkflowStatus.VALIDATING,
    ),
    _rule(WorkflowStatus.REPAIRING, WorkflowEvent.REPAIR_EXHAUSTED, WorkflowStatus.NEEDS_ATTENTION),
    _rule(WorkflowStatus.REPAIRING, WorkflowEvent.REPAIR_FAILED, WorkflowStatus.FAILED),
    _rule(WorkflowStatus.REPAIRING, WorkflowEvent.CRASH_DETECTED, WorkflowStatus.RECOVERING),
    _rule(
        WorkflowStatus.RECOVERING,
        WorkflowEvent.RECOVERY_SUCCEEDED,
        WorkflowStatus.RUNNING,
        WorkflowStatus.VALIDATING,
        WorkflowStatus.REVIEWING,
        WorkflowStatus.REPAIRING,
        uses_resume_status=True,
    ),
    _rule(
        WorkflowStatus.RECOVERING,
        WorkflowEvent.RECOVERY_UNCERTAIN,
        WorkflowStatus.NEEDS_RECONCILIATION,
    ),
    _rule(
        WorkflowStatus.NEEDS_RECONCILIATION,
        WorkflowEvent.RECONCILED_RESUME,
        WorkflowStatus.RUNNING,
        WorkflowStatus.VALIDATING,
        WorkflowStatus.REVIEWING,
        WorkflowStatus.REPAIRING,
        uses_resume_status=True,
    ),
    _rule(
        WorkflowStatus.NEEDS_RECONCILIATION,
        WorkflowEvent.RECONCILED_FAIL,
        WorkflowStatus.FAILED,
    ),
    _rule(
        WorkflowStatus.NEEDS_RECONCILIATION,
        WorkflowEvent.CANCEL_REQUESTED,
        WorkflowStatus.CANCELED,
    ),
    _rule(
        WorkflowStatus.NEEDS_ATTENTION,
        WorkflowEvent.RESUME_REQUESTED,
        WorkflowStatus.READY,
        WorkflowStatus.RUNNING,
        WorkflowStatus.VALIDATING,
        WorkflowStatus.REPAIRING,
        server_selects_target=True,
    ),
    _rule(
        WorkflowStatus.NEEDS_ATTENTION,
        WorkflowEvent.CANCEL_REQUESTED,
        WorkflowStatus.CANCELED,
    ),
    _rule(WorkflowStatus.COMPLETED, WorkflowEvent.REPAIR_REQUESTED, WorkflowStatus.REPAIRING),
    _rule(WorkflowStatus.COMPLETED, WorkflowEvent.ARCHIVE_REQUESTED, WorkflowStatus.ARCHIVED),
    _rule(WorkflowStatus.FAILED, WorkflowEvent.RETRY_REQUESTED, WorkflowStatus.READY),
    _rule(WorkflowStatus.FAILED, WorkflowEvent.ARCHIVE_REQUESTED, WorkflowStatus.ARCHIVED),
    _rule(WorkflowStatus.CANCELED, WorkflowEvent.ARCHIVE_REQUESTED, WorkflowStatus.ARCHIVED),
)

_TRANSITION_INDEX = {(item.source, item.event): item for item in WORKFLOW_TRANSITIONS}
_ACTIVE_RESUME_TARGETS = frozenset(
    {
        WorkflowStatus.RUNNING,
        WorkflowStatus.VALIDATING,
        WorkflowStatus.REVIEWING,
        WorkflowStatus.REPAIRING,
    }
)
_ATTENTION_RESUME_TARGETS = frozenset(
    {
        WorkflowStatus.READY,
        WorkflowStatus.RUNNING,
        WorkflowStatus.VALIDATING,
        WorkflowStatus.REPAIRING,
    }
)


class InvalidWorkflowTransition(ConflictError):
    """A workflow event is not legal for the persisted source state."""

    def __init__(
        self,
        *,
        task_id: str,
        source: str,
        event: str,
        requested_target: str | None = None,
        reason: str = "event is not allowed from the persisted workflow state",
    ) -> None:
        self.task_id = task_id
        self.source = source
        self.event = event
        self.requested_target = requested_target
        self.reason = reason
        super().__init__(
            f"invalid Task Quality workflow transition: {source} --{event}--> "
            f"{requested_target or '?'} ({reason})"
        )

    def audit_payload(self) -> dict[str, str | None]:
        return {
            "from_status": self.source,
            "event": self.event,
            "requested_target": self.requested_target,
            "reason": self.reason,
        }


def _coerce_event(value: WorkflowEvent | str) -> WorkflowEvent:
    try:
        return value if isinstance(value, WorkflowEvent) else WorkflowEvent(str(value))
    except ValueError as exc:
        raise InvalidWorkflowTransition(
            task_id="unknown",
            source="unknown",
            event=str(value),
            reason="unknown workflow event",
        ) from exc


def _remembered_attention_target(source: WorkflowStatus) -> WorkflowStatus | None:
    if source in _ATTENTION_RESUME_TARGETS:
        return source
    if source is WorkflowStatus.REVIEWING:
        # A review-time policy/waiver change must re-enter deterministic validation
        # before independent review is trusted again.
        return WorkflowStatus.VALIDATING
    return None


def transition_workflow_in_transaction(
    store: "OrchestrationStore",
    connection: "sqlite3.Connection",
    *,
    task_id: str,
    event: WorkflowEvent | str,
    server_target: WorkflowStatus | str | None = None,
    reason_code: str | None = None,
    clear_reason: bool = False,
    command_id: str | None = None,
) -> WorkflowTransition:
    """Validate and persist one canonical workflow event in ``connection``."""

    row = connection.execute(
        "SELECT workflow_status, workflow_resume_status FROM orch_tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"task {task_id} not found")
    source = WorkflowStatus(str(row["workflow_status"]))
    try:
        chosen_event = event if isinstance(event, WorkflowEvent) else WorkflowEvent(str(event))
    except ValueError as exc:
        raise InvalidWorkflowTransition(
            task_id=task_id,
            source=source.value,
            event=str(event),
            requested_target=str(server_target) if server_target is not None else None,
            reason="unknown workflow event",
        ) from exc
    rule = _TRANSITION_INDEX.get((source, chosen_event))
    if rule is None:
        raise InvalidWorkflowTransition(
            task_id=task_id,
            source=source.value,
            event=chosen_event.value,
            requested_target=str(server_target) if server_target is not None else None,
        )

    persisted_resume: WorkflowStatus | None = None
    if row["workflow_resume_status"]:
        try:
            persisted_resume = WorkflowStatus(str(row["workflow_resume_status"]))
        except ValueError as exc:
            raise InvalidWorkflowTransition(
                task_id=task_id,
                source=source.value,
                event=chosen_event.value,
                reason="persisted resume_status is not canonical",
            ) from exc

    if rule.uses_resume_status:
        if server_target is not None:
            raise InvalidWorkflowTransition(
                task_id=task_id,
                source=source.value,
                event=chosen_event.value,
                requested_target=str(server_target),
                reason="dynamic recovery target must come from persisted resume_status",
            )
        if persisted_resume not in _ACTIVE_RESUME_TARGETS:
            raise InvalidWorkflowTransition(
                task_id=task_id,
                source=source.value,
                event=chosen_event.value,
                reason="persisted resume_status is missing or outside the active allowlist",
            )
        target = persisted_resume
    elif rule.server_selects_target:
        try:
            target = (
                server_target
                if isinstance(server_target, WorkflowStatus)
                else WorkflowStatus(str(server_target))
            )
        except ValueError as exc:
            raise InvalidWorkflowTransition(
                task_id=task_id,
                source=source.value,
                event=chosen_event.value,
                requested_target=str(server_target),
                reason="server-selected resume target is not canonical",
            ) from exc
        if target not in _ATTENTION_RESUME_TARGETS or target not in rule.targets:
            raise InvalidWorkflowTransition(
                task_id=task_id,
                source=source.value,
                event=chosen_event.value,
                requested_target=target.value,
                reason="server-selected resume target is outside the reason-specific allowlist",
            )
    else:
        target = rule.targets[0]
        if server_target is not None and WorkflowStatus(str(server_target)) is not target:
            raise InvalidWorkflowTransition(
                task_id=task_id,
                source=source.value,
                event=chosen_event.value,
                requested_target=str(server_target),
                reason="fixed transition target cannot be overridden",
            )

    next_resume = persisted_resume
    if chosen_event is WorkflowEvent.CRASH_DETECTED:
        next_resume = source
    elif target is WorkflowStatus.NEEDS_ATTENTION:
        next_resume = _remembered_attention_target(source)
    elif chosen_event in {
        WorkflowEvent.RECOVERY_SUCCEEDED,
        WorkflowEvent.RECONCILED_RESUME,
        WorkflowEvent.RESUME_REQUESTED,
    } or target in {
        WorkflowStatus.COMPLETED,
        WorkflowStatus.FAILED,
        WorkflowStatus.CANCELED,
        WorkflowStatus.ARCHIVED,
    }:
        next_resume = None

    assignments = ["workflow_status=?", "workflow_resume_status=?"]
    parameters: list[Any] = [target.value, next_resume.value if next_resume else None]
    if reason_code is not None or clear_reason:
        assignments.append("quality_reason_code=?")
        parameters.append(str(reason_code)[:255] if reason_code is not None else None)
    parameters.append(task_id)
    connection.execute(
        f"UPDATE orch_tasks SET {', '.join(assignments)} WHERE id=?",
        tuple(parameters),
    )
    store._append_event(
        connection,
        task_id=task_id,
        aggregate_type="task_quality_workflow",
        aggregate_id=task_id,
        event_type="quality_workflow_transition",
        payload={
            "event": chosen_event.value,
            "from_status": source.value,
            "to_status": target.value,
            "resume_status": next_resume.value if next_resume else None,
            "reason_code": str(reason_code)[:255] if reason_code is not None else None,
        },
        command_id=command_id,
    )
    return WorkflowTransition(
        task_id=task_id,
        event=chosen_event,
        from_status=source,
        to_status=target,
        resume_status=next_resume,
    )


def apply_workflow_event(
    store: "OrchestrationStore",
    *,
    task_id: str,
    event: WorkflowEvent | str,
    server_target: WorkflowStatus | str | None = None,
    reason_code: str | None = None,
    clear_reason: bool = False,
    command_id: str | None = None,
) -> WorkflowTransition:
    """Apply an event and durably audit invalid attempts before re-raising."""

    try:
        with store._write() as connection:
            return transition_workflow_in_transaction(
                store,
                connection,
                task_id=task_id,
                event=event,
                server_target=server_target,
                reason_code=reason_code,
                clear_reason=clear_reason,
                command_id=command_id,
            )
    except InvalidWorkflowTransition as exc:
        with store._write() as connection:
            exists = connection.execute(
                "SELECT 1 FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
            if exists is not None:
                store._append_event(
                    connection,
                    task_id=task_id,
                    aggregate_type="task_quality_workflow",
                    aggregate_id=task_id,
                    event_type="invalid_transition",
                    payload=exc.audit_payload(),
                    command_id=command_id,
                )
        raise


def workflow_transition_snapshot() -> list[dict[str, Any]]:
    """Return the stable schema/generator projection of the canonical table."""

    return [
        {
            "from_status": item.source.value,
            "event": item.event.value,
            "to_statuses": [target.value for target in item.targets],
            "uses_resume_status": item.uses_resume_status,
            "server_selects_target": item.server_selects_target,
        }
        for item in WORKFLOW_TRANSITIONS
    ]


def task_quality_schema_snapshot() -> dict[str, Any]:
    """Canonical API/generator payload, including all four orthogonal axes."""

    statuses = canonical_status_values()
    return {
        "schema_version": 2,
        "workflow_statuses": statuses["workflow_status"],
        "quality_statuses": statuses["quality_status"],
        "artifact_statuses": statuses["artifact_status"],
        "budget_statuses": statuses["budget_status"],
        "budget_modes": [item.value for item in BudgetMode],
        "archetypes": [item.value for item in Archetype],
        "workflow_events": [item.value for item in WorkflowEvent],
        "workflow_transitions": workflow_transition_snapshot(),
    }


__all__ = [
    "InvalidWorkflowTransition",
    "WORKFLOW_TRANSITIONS",
    "WorkflowEvent",
    "WorkflowTransition",
    "WorkflowTransitionRule",
    "apply_workflow_event",
    "task_quality_schema_snapshot",
    "transition_workflow_in_transaction",
    "workflow_transition_snapshot",
]
