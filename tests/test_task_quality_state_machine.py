from __future__ import annotations

import json

import pytest

from coworker.orchestration.models import TaskSpec
from coworker.orchestration.quality.contract_compiler import ContractCompiler
from coworker.orchestration.quality.contracts import ContractRepository
from coworker.orchestration.quality.models import WorkflowStatus
from coworker.orchestration.quality.state_machine import (
    InvalidWorkflowTransition,
    WorkflowEvent,
    apply_workflow_event,
    task_quality_schema_snapshot,
)
from coworker.orchestration.store import OrchestrationStore
from coworker.orchestration.service import OrchestrationService


@pytest.fixture
def workflow_task(tmp_path):
    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    task = store.create_task(
        TaskSpec(idempotency_key="quality-state", objective="Analyze the repository")
    )
    try:
        yield store, task.id
    finally:
        store.close()


def _state(store: OrchestrationStore, task_id: str) -> tuple[str, str | None]:
    with store._read() as connection:
        row = connection.execute(
            """
            SELECT workflow_status, workflow_resume_status
            FROM orch_tasks WHERE id=?
            """,
            (task_id,),
        ).fetchone()
    return row["workflow_status"], row["workflow_resume_status"]


def test_crash_resume_checkpoint_is_persisted_and_server_owned(workflow_task) -> None:
    store, task_id = workflow_task
    apply_workflow_event(store, task_id=task_id, event=WorkflowEvent.ANALYSIS_REQUESTED)
    apply_workflow_event(store, task_id=task_id, event=WorkflowEvent.ANALYSIS_READY)
    apply_workflow_event(store, task_id=task_id, event=WorkflowEvent.START_REQUESTED)
    apply_workflow_event(store, task_id=task_id, event=WorkflowEvent.CRASH_DETECTED)
    assert _state(store, task_id) == ("recovering", "running")

    with pytest.raises(InvalidWorkflowTransition, match="persisted resume_status"):
        apply_workflow_event(
            store,
            task_id=task_id,
            event=WorkflowEvent.RECOVERY_SUCCEEDED,
            server_target=WorkflowStatus.VALIDATING,
        )
    assert _state(store, task_id) == ("recovering", "running")

    applied = apply_workflow_event(
        store, task_id=task_id, event=WorkflowEvent.RECOVERY_SUCCEEDED
    )
    assert applied.to_status is WorkflowStatus.RUNNING
    assert _state(store, task_id) == ("running", None)


def test_reconciliation_uses_exact_persisted_checkpoint(workflow_task) -> None:
    store, task_id = workflow_task
    for event in (
        WorkflowEvent.ANALYSIS_REQUESTED,
        WorkflowEvent.ANALYSIS_READY,
        WorkflowEvent.START_REQUESTED,
        WorkflowEvent.CRASH_DETECTED,
        WorkflowEvent.RECOVERY_UNCERTAIN,
    ):
        apply_workflow_event(store, task_id=task_id, event=event)
    assert _state(store, task_id) == ("needs_reconciliation", "running")
    apply_workflow_event(
        store, task_id=task_id, event=WorkflowEvent.RECONCILED_RESUME
    )
    assert _state(store, task_id) == ("running", None)


def test_service_startup_hooks_recover_only_the_persisted_v2_checkpoint(
    workflow_task,
) -> None:
    store, task_id = workflow_task
    compiled = ContractCompiler().compile(
        task_id=task_id,
        objective="Analyze the repository and produce an evidence-backed report.",
    ).contract
    contracts = ContractRepository(store)
    contracts.save_draft(compiled)
    contracts.publish(compiled.id, if_match=compiled.content_hash)
    apply_workflow_event(store, task_id=task_id, event=WorkflowEvent.ANALYSIS_READY)
    apply_workflow_event(store, task_id=task_id, event=WorkflowEvent.START_REQUESTED)

    service = object.__new__(OrchestrationService)
    service.store = store
    service._begin_task_quality_recovery()
    assert _state(store, task_id) == ("recovering", "running")
    service._finish_task_quality_recovery()
    assert _state(store, task_id) == ("running", None)

    service._begin_task_quality_recovery()
    with store._write() as connection:
        connection.execute(
            "UPDATE orch_tasks SET status='needs_reconciliation' WHERE id=?",
            (task_id,),
        )
    service._finish_task_quality_recovery()
    assert _state(store, task_id) == ("needs_reconciliation", "running")
    apply_workflow_event(
        store,
        task_id=task_id,
        event=WorkflowEvent.RECONCILED_RESUME,
    )
    assert _state(store, task_id) == ("running", None)


def test_unlisted_transition_is_rejected_and_committed_to_audit(workflow_task) -> None:
    store, task_id = workflow_task
    with pytest.raises(InvalidWorkflowTransition):
        apply_workflow_event(
            store, task_id=task_id, event=WorkflowEvent.QUALITY_PUBLISHABLE
        )
    assert _state(store, task_id) == ("draft", None)
    with store._read() as connection:
        row = connection.execute(
            """
            SELECT event_type, payload_json FROM orch_events
            WHERE task_id=? ORDER BY sequence_no DESC LIMIT 1
            """,
            (task_id,),
        ).fetchone()
    assert row["event_type"] == "invalid_transition"
    assert json.loads(row["payload_json"]) == {
        "event": "quality_publishable",
        "from_status": "draft",
        "reason": "event is not allowed from the persisted workflow state",
        "requested_target": None,
    }


def test_schema_snapshot_includes_all_four_axes_and_event_source() -> None:
    snapshot = task_quality_schema_snapshot()
    assert snapshot["workflow_statuses"] == [item.value for item in WorkflowStatus]
    assert snapshot["workflow_events"] == [item.value for item in WorkflowEvent]
    assert {item["event"] for item in snapshot["workflow_transitions"]} == {
        item.value for item in WorkflowEvent
    }
    assert set(snapshot) >= {
        "workflow_statuses",
        "quality_statuses",
        "artifact_statuses",
        "budget_statuses",
    }
