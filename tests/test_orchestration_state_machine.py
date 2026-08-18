from __future__ import annotations

import pytest

from coworker.orchestration import (
    STAGE_ORDER,
    InvalidTransition,
    OrchestrationStage,
    StageDisposition,
    TaskStatus,
    is_terminal_task_status,
    validate_stage_transition,
    validate_task_transition,
)


def test_stage_enum_is_exact_locked_eight_stage_process():
    assert STAGE_ORDER == (
        OrchestrationStage.INTAKE,
        OrchestrationStage.COMPLEXITY_ASSESSMENT,
        OrchestrationStage.CLARIFICATION,
        OrchestrationStage.PLANNING,
        OrchestrationStage.EXECUTION_REVIEW_TEST,
        OrchestrationStage.INTER_STEP_EVALUATION,
        OrchestrationStage.FINAL_ACCEPTANCE,
        OrchestrationStage.ARCHIVE,
    )
    assert len(tuple(OrchestrationStage)) == 8


def test_task_status_is_independent_and_rejects_invalid_transitions():
    validate_task_transition(TaskStatus.DRAFT, TaskStatus.QUEUED)
    validate_task_transition(TaskStatus.RUNNING, TaskStatus.WAITING_HUMAN)
    validate_task_transition(TaskStatus.WAITING_HUMAN, TaskStatus.RUNNING)
    validate_task_transition(TaskStatus.WAITING_HUMAN, TaskStatus.WAITING_CHILD)
    validate_task_transition(TaskStatus.WAITING_CHILD, TaskStatus.WAITING_HUMAN)
    validate_task_transition(TaskStatus.COMPLETED, TaskStatus.ARCHIVED)
    validate_task_transition(TaskStatus.ARCHIVED, TaskStatus.COMPLETED)
    assert is_terminal_task_status(TaskStatus.COMPLETED)
    assert not is_terminal_task_status(TaskStatus.RUNNING)

    with pytest.raises(InvalidTransition):
        validate_task_transition(TaskStatus.DRAFT, TaskStatus.COMPLETED)
    with pytest.raises(InvalidTransition):
        validate_task_transition(TaskStatus.ARCHIVED, TaskStatus.RUNNING)


def test_stage_loops_skip_and_request_changes_rules():
    validate_stage_transition(
        OrchestrationStage.COMPLEXITY_ASSESSMENT,
        OrchestrationStage.PLANNING,
        StageDisposition.SKIPPED,
    )
    validate_stage_transition(
        OrchestrationStage.EXECUTION_REVIEW_TEST,
        OrchestrationStage.INTER_STEP_EVALUATION,
    )
    validate_stage_transition(
        OrchestrationStage.INTER_STEP_EVALUATION,
        OrchestrationStage.EXECUTION_REVIEW_TEST,
    )
    validate_stage_transition(
        OrchestrationStage.INTER_STEP_EVALUATION,
        OrchestrationStage.PLANNING,
        StageDisposition.REQUEST_CHANGES,
    )
    validate_stage_transition(
        OrchestrationStage.FINAL_ACCEPTANCE,
        OrchestrationStage.PLANNING,
        StageDisposition.REQUEST_CHANGES,
    )

    with pytest.raises(InvalidTransition):
        validate_stage_transition(
            OrchestrationStage.COMPLEXITY_ASSESSMENT,
            OrchestrationStage.PLANNING,
            StageDisposition.COMPLETED,
        )
    with pytest.raises(InvalidTransition):
        validate_stage_transition(
            OrchestrationStage.FINAL_ACCEPTANCE,
            OrchestrationStage.PLANNING,
            StageDisposition.COMPLETED,
        )
    with pytest.raises(InvalidTransition):
        validate_stage_transition(
            OrchestrationStage.PLANNING,
            OrchestrationStage.ARCHIVE,
        )
