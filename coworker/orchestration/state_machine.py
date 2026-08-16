"""Pure transition rules for task status and the fixed orchestration stages."""

from __future__ import annotations

from .errors import InvalidTransition
from .models import OrchestrationStage, StageDisposition, TaskStatus


STAGE_ORDER: tuple[OrchestrationStage, ...] = (
    OrchestrationStage.INTAKE,
    OrchestrationStage.COMPLEXITY_ASSESSMENT,
    OrchestrationStage.CLARIFICATION,
    OrchestrationStage.PLANNING,
    OrchestrationStage.EXECUTION_REVIEW_TEST,
    OrchestrationStage.INTER_STEP_EVALUATION,
    OrchestrationStage.FINAL_ACCEPTANCE,
    OrchestrationStage.ARCHIVE,
)

if tuple(OrchestrationStage) != STAGE_ORDER:  # fail fast if the enum drifts
    raise RuntimeError("OrchestrationStage must contain exactly the eight canonical stages")


TERMINAL_TASK_STATUSES = frozenset(
    {
        TaskStatus.FAILED,
        TaskStatus.CANCELED,
        TaskStatus.COMPLETED,
        TaskStatus.ARCHIVED,
    }
)


_TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.DRAFT: frozenset({TaskStatus.QUEUED, TaskStatus.CANCELED}),
    TaskStatus.QUEUED: frozenset(
        {
            TaskStatus.RUNNING,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.CANCELING,
            TaskStatus.FAILED,
        }
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.WAITING_HUMAN,
            TaskStatus.WAITING_CHILD,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.NEEDS_RECONCILIATION,
            TaskStatus.CANCELING,
            TaskStatus.FAILED,
            TaskStatus.COMPLETED,
        }
    ),
    TaskStatus.WAITING_HUMAN: frozenset(
        {
            TaskStatus.RUNNING,
            TaskStatus.WAITING_CHILD,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.CANCELING,
            TaskStatus.FAILED,
        }
    ),
    TaskStatus.WAITING_CHILD: frozenset(
        {
            TaskStatus.RUNNING,
            TaskStatus.WAITING_HUMAN,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.CANCELING,
            TaskStatus.FAILED,
        }
    ),
    TaskStatus.PAUSED: frozenset(
        {TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.CANCELING}
    ),
    TaskStatus.BLOCKED: frozenset(
        {
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
            TaskStatus.NEEDS_RECONCILIATION,
            TaskStatus.CANCELING,
            TaskStatus.FAILED,
        }
    ),
    TaskStatus.NEEDS_RECONCILIATION: frozenset(
        {
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
            TaskStatus.BLOCKED,
            TaskStatus.CANCELING,
            TaskStatus.FAILED,
        }
    ),
    TaskStatus.CANCELING: frozenset({TaskStatus.CANCELED, TaskStatus.FAILED}),
    TaskStatus.FAILED: frozenset({TaskStatus.ARCHIVED}),
    TaskStatus.CANCELED: frozenset({TaskStatus.ARCHIVED}),
    TaskStatus.COMPLETED: frozenset({TaskStatus.ARCHIVED}),
    TaskStatus.ARCHIVED: frozenset(),
}


_STAGE_TRANSITIONS: dict[OrchestrationStage, frozenset[OrchestrationStage]] = {
    OrchestrationStage.INTAKE: frozenset(
        {OrchestrationStage.COMPLEXITY_ASSESSMENT}
    ),
    OrchestrationStage.COMPLEXITY_ASSESSMENT: frozenset(
        {OrchestrationStage.CLARIFICATION, OrchestrationStage.PLANNING}
    ),
    OrchestrationStage.CLARIFICATION: frozenset({OrchestrationStage.PLANNING}),
    OrchestrationStage.PLANNING: frozenset(
        {OrchestrationStage.EXECUTION_REVIEW_TEST}
    ),
    OrchestrationStage.EXECUTION_REVIEW_TEST: frozenset(
        {OrchestrationStage.INTER_STEP_EVALUATION}
    ),
    OrchestrationStage.INTER_STEP_EVALUATION: frozenset(
        {
            OrchestrationStage.EXECUTION_REVIEW_TEST,
            OrchestrationStage.PLANNING,
            OrchestrationStage.FINAL_ACCEPTANCE,
        }
    ),
    OrchestrationStage.FINAL_ACCEPTANCE: frozenset(
        {OrchestrationStage.PLANNING, OrchestrationStage.ARCHIVE}
    ),
    OrchestrationStage.ARCHIVE: frozenset(),
}


def allowed_task_transitions(status: TaskStatus) -> frozenset[TaskStatus]:
    return _TASK_TRANSITIONS[TaskStatus(status)]


def validate_task_transition(current: TaskStatus, target: TaskStatus) -> None:
    current, target = TaskStatus(current), TaskStatus(target)
    if target not in _TASK_TRANSITIONS[current]:
        raise InvalidTransition(f"task status cannot transition {current} -> {target}")


def is_terminal_task_status(status: TaskStatus) -> bool:
    return TaskStatus(status) in TERMINAL_TASK_STATUSES


def allowed_stage_transitions(
    stage: OrchestrationStage,
) -> frozenset[OrchestrationStage]:
    return _STAGE_TRANSITIONS[OrchestrationStage(stage)]


def validate_stage_transition(
    current: OrchestrationStage,
    target: OrchestrationStage,
    disposition: StageDisposition = StageDisposition.COMPLETED,
) -> None:
    """Validate stage movement, including skip and request-changes semantics.

    COMPLEXITY_ASSESSMENT -> PLANNING is the one shortcut; it records a skipped
    CLARIFICATION visit.  Evaluation and final acceptance may return to PLANNING
    only with REQUEST_CHANGES.  Evaluation may loop back to execution normally.
    """

    current = OrchestrationStage(current)
    target = OrchestrationStage(target)
    disposition = StageDisposition(disposition)
    if target not in _STAGE_TRANSITIONS[current]:
        raise InvalidTransition(f"stage cannot transition {current} -> {target}")

    shortcut = (
        current is OrchestrationStage.COMPLEXITY_ASSESSMENT
        and target is OrchestrationStage.PLANNING
    )
    planning_return = (
        current
        in {
            OrchestrationStage.INTER_STEP_EVALUATION,
            OrchestrationStage.FINAL_ACCEPTANCE,
        }
        and target is OrchestrationStage.PLANNING
    )
    clarification_skip = (
        current is OrchestrationStage.CLARIFICATION
        and target is OrchestrationStage.PLANNING
        and disposition is StageDisposition.SKIPPED
    )

    if shortcut and disposition is not StageDisposition.SKIPPED:
        raise InvalidTransition(
            "bypassing clarification must use the skipped disposition"
        )
    if planning_return and disposition is not StageDisposition.REQUEST_CHANGES:
        raise InvalidTransition(
            "evaluation/acceptance may return to planning only via request_changes"
        )
    if disposition is StageDisposition.REQUEST_CHANGES and not planning_return:
        raise InvalidTransition("request_changes is valid only when returning to planning")
    if disposition is StageDisposition.SKIPPED and not (shortcut or clarification_skip):
        raise InvalidTransition("only clarification may be skipped")


def next_linear_stage(stage: OrchestrationStage) -> OrchestrationStage | None:
    stage = OrchestrationStage(stage)
    index = STAGE_ORDER.index(stage)
    return STAGE_ORDER[index + 1] if index + 1 < len(STAGE_ORDER) else None
