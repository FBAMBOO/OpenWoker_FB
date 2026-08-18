from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

import coworker.orchestration.store as store_module

from coworker.orchestration import (
    ConflictError,
    EdgeSpec,
    EvidenceKind,
    GateConflict,
    GateKind,
    GateStatus,
    IdempotencyConflict,
    LeaseConflict,
    NodeKind,
    NodeSpec,
    OrchestrationStage,
    OrchestrationStore,
    PlanSpec,
    RetryPolicy,
    RunStatus,
    StageDisposition,
    TaskSpec,
    TaskStatus,
    VersionConflict,
)
from coworker.orchestration.migrations import load_migrations


def _create_task(store: OrchestrationStore, key: str = "task-key"):
    return store.create_task(
        TaskSpec(
            idempotency_key=key,
            objective="Implement and verify the orchestration core",
            workspace="/repo",
            constraints=("Do not publish",),
            acceptance_criteria=("Tests pass",),
            policy={"network": "deny"},
        ),
        command_id=f"create-{key}",
    )


def _create_plan(store: OrchestrationStore, task, *, attempts: int = 2):
    graph = store.create_plan_revision(
        task.id,
        PlanSpec(
            nodes=(
                NodeSpec(
                    "implement",
                    kind=NodeKind.EXECUTE,
                    retry_policy=RetryPolicy(max_attempts=attempts),
                    concurrency_key="workspace:/repo",
                ),
                NodeSpec("review", kind=NodeKind.REVIEW),
            ),
            edges=(EdgeSpec("implement", "review"),),
            metadata={"source": "test"},
        ),
        expected_task_version=task.version,
        created_by="planner",
        command_id=f"plan-{task.id}",
    )
    return graph, store.get_task(task.id)


def _queue_task(store: OrchestrationStore, task):
    return store.transition_task_status(
        task.id,
        TaskStatus.QUEUED,
        expected_version=task.version,
        command_id=f"queue-{task.id}",
    )


def _start_task(store: OrchestrationStore, task):
    return store.transition_task_status(
        task.id,
        TaskStatus.RUNNING,
        expected_version=task.version,
        command_id=f"start-{task.id}",
    )


def _checkpoint(run, claim, gate, call_id: str = "pending-call"):
    return {
        "schema_version": 1,
        "run_id": run.id,
        "attempt": run.attempt,
        "fencing_token": claim.lease.fencing_token,
        "session_id": run.session_id,
        "gate_id": gate.id,
        "blob_uri": "sha256:" + "a" * 64,
        "blob_sha256": "a" * 64,
        "pending_tool_call_ids": [call_id],
        "recovery_disposition": "pending_tools",
    }


def test_independent_wal_schema_and_checksummed_migration_ledger(tmp_path):
    db = tmp_path / "orchestration.db"
    store = OrchestrationStore(db)
    connection = store.connect()
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert {
            "orch_tasks",
            "orch_stage_history",
            "orch_plans",
            "orch_nodes",
            "orch_edges",
            "orch_runs",
            "orch_gates",
            "orch_evidence",
            "orch_events",
            "orch_outbox",
            "orch_leases",
            "orch_commands",
            "orch_run_activity",
            "orch_schema_migrations",
        } <= tables
        ledger = connection.execute(
            "SELECT version, name, checksum FROM orch_schema_migrations"
        ).fetchone()
        assert ledger[0] == 1 and ledger[1] == "initial" and len(ledger[2]) == 64
    finally:
        connection.close()
        store.close()

    # Reopening is an idempotent migration pass over the same independent DB.
    reopened = OrchestrationStore(db)
    assert reopened.connect().execute(
        "SELECT COUNT(*) FROM orch_schema_migrations"
    ).fetchone()[0] == len(load_migrations())
    reopened.close()


def test_task_idempotency_optimistic_version_and_stage_history(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    task = _create_task(store)
    replay = _create_task(store)
    assert replay.id == task.id and task.max_parallel_runs == 8

    with pytest.raises(IdempotencyConflict):
        store.create_task(
            TaskSpec(idempotency_key="task-key", objective="different"),
            command_id="different-command",
        )
    with pytest.raises(IdempotencyConflict):
        store.create_task(
            TaskSpec(idempotency_key="another-key", objective="different"),
            command_id="create-task-key",
        )
    with pytest.raises(VersionConflict):
        store.transition_task_status(
            task.id,
            TaskStatus.QUEUED,
            expected_version=99,
            command_id="bad-version",
        )

    task = store.transition_stage(
        task.id,
        OrchestrationStage.COMPLEXITY_ASSESSMENT,
        expected_version=task.version,
        command_id="stage-complexity",
    )
    task = store.transition_stage(
        task.id,
        OrchestrationStage.PLANNING,
        expected_version=task.version,
        disposition=StageDisposition.SKIPPED,
        command_id="skip-clarification",
    )
    task = store.transition_stage(
        task.id,
        OrchestrationStage.EXECUTION_REVIEW_TEST,
        expected_version=task.version,
        command_id="stage-execute",
    )
    task = store.transition_stage(
        task.id,
        OrchestrationStage.INTER_STEP_EVALUATION,
        expected_version=task.version,
        command_id="stage-evaluate",
    )
    task = store.transition_stage(
        task.id,
        OrchestrationStage.PLANNING,
        expected_version=task.version,
        disposition=StageDisposition.REQUEST_CHANGES,
        command_id="evaluation-changes",
    )

    history = store.stage_history(task.id)
    assert [entry.stage for entry in history] == [
        OrchestrationStage.INTAKE,
        OrchestrationStage.COMPLEXITY_ASSESSMENT,
        OrchestrationStage.CLARIFICATION,
        OrchestrationStage.PLANNING,
        OrchestrationStage.EXECUTION_REVIEW_TEST,
        OrchestrationStage.INTER_STEP_EVALUATION,
        OrchestrationStage.PLANNING,
    ]
    assert history[2].disposition is StageDisposition.SKIPPED
    assert history[-2].disposition is StageDisposition.REQUEST_CHANGES
    assert history[-1].disposition is StageDisposition.ACTIVE
    assert store.get_task(task.id).status is TaskStatus.DRAFT


def test_task_tree_applies_deterministic_depth_and_row_bounds(tmp_path):
    store = OrchestrationStore(tmp_path / "bounded-tree.db")
    try:
        root = _create_task(store, "tree-root")

        def child(parent_id: str, key: str):
            return store.create_task(
                TaskSpec(
                    idempotency_key=key,
                    objective=key,
                    parent_task_id=parent_id,
                ),
                command_id=f"create-{key}",
            )

        children = [child(root.id, f"tree-child-{index}") for index in range(4)]
        grandchildren = [
            child(item.id, f"tree-grandchild-{index}")
            for index, item in enumerate(children)
        ]
        child(grandchildren[0].id, "tree-beyond-detail-depth")

        depth_one = store.list_task_tree(root.id, max_depth=1, max_rows=100)
        assert depth_one[0].id == root.id
        assert {item.id for item in depth_one[1:]} == {
            item.id for item in children
        }
        assert not {item.id for item in grandchildren}.intersection(
            item.id for item in depth_one
        )

        first_page = store.list_task_tree(root.id, max_depth=3, max_rows=5)
        repeated = store.list_task_tree(root.id, max_depth=3, max_rows=5)
        assert len(first_page) == 5
        assert [item.id for item in repeated] == [item.id for item in first_page]
        assert first_page[0].id == root.id
        counts = store.count_task_children(
            (root.id, children[0].id, "missing-task")
        )
        assert counts == {
            root.id: 4,
            children[0].id: 1,
            "missing-task": 0,
        }

        assert store.list_task_tree(
            root.id, include_root=False, max_depth=0, max_rows=5
        ) == ()
        with pytest.raises(ValueError, match="max_depth"):
            store.list_task_tree(root.id, max_depth=-1)
        with pytest.raises(ValueError, match="max_rows"):
            store.list_task_tree(root.id, max_rows=0)
    finally:
        store.close()


def test_task_listing_supports_safe_pages_and_untruncated_internal_snapshot(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    created = [
        store.create_task(TaskSpec(f"task-{index}", f"Task {index}"))
        for index in range(105)
    ]

    all_tasks = store.list_all_tasks(page_size=7)
    assert len(all_tasks) == 105
    assert {task.id for task in all_tasks} == {task.id for task in created}
    assert store.list_tasks() == all_tasks[:100]
    assert store.list_tasks(limit=10, offset=100) == all_tasks[100:]
    assert store.list_tasks(limit=2, offset=-50) == all_tasks[:2]


def test_approved_clarification_updates_contract_idempotently_and_is_audited(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    task = _create_task(store)
    task = store.transition_stage(
        task.id,
        OrchestrationStage.COMPLEXITY_ASSESSMENT,
        expected_version=task.version,
    )
    task = store.transition_stage(
        task.id,
        OrchestrationStage.CLARIFICATION,
        expected_version=task.version,
    )
    task = _queue_task(store, task)
    task = store.transition_task_status(
        task.id,
        TaskStatus.RUNNING,
        expected_version=task.version,
    )
    gate = store.open_task_gate(
        task.id,
        kind=GateKind.CLARIFICATION,
        source_key=f"{task.id}:clarification",
        prompt={"question": "What is accepted?"},
    )
    store.resolve_gate(
        gate.id,
        GateStatus.APPROVED,
        {"decision": "submit", "response": "The report cites source A"},
        resolved_by="owner",
        expected_version=gate.version,
    )
    before = store.get_task(task.id)

    applied = store.apply_clarification(
        task.id,
        "  The report cites source A  ",
        expected_version=before.version,
        resolved_by="owner",
        gate_id=gate.id,
        command_id="apply-clarification",
    )
    assert applied.acceptance_criteria == ("Tests pass", "The report cites source A")
    assert applied.input["clarifications"] == [
        {
            "response": "The report cites source A",
            "resolved_by": "owner",
            "gate_id": gate.id,
            "applied_at": applied.updated_at.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
        }
    ]

    replay = store.apply_clarification(
        task.id,
        "The report cites source A",
        expected_version=before.version,
        resolved_by="owner",
        gate_id=gate.id,
        command_id="apply-clarification",
    )
    assert replay.version == applied.version
    assert len(replay.input["clarifications"]) == 1
    with pytest.raises(IdempotencyConflict):
        store.apply_clarification(
            task.id,
            "A different criterion",
            expected_version=before.version,
            resolved_by="owner",
            gate_id=gate.id,
            command_id="apply-clarification",
        )
    with pytest.raises(VersionConflict):
        store.apply_clarification(
            task.id,
            "Another criterion",
            expected_version=before.version,
            resolved_by="owner",
            gate_id=gate.id,
            command_id="stale-clarification",
        )
    event = store.list_events(task_id=task.id)[-1]
    assert event.event_type == "task.clarification_applied"
    assert event.payload == {
        "criterion": "The report cites source A",
        "resolved_by": "owner",
        "gate_id": gate.id,
    }


def test_open_task_gate_prompt_can_be_amended_with_audited_cas(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    task = _start_task(store, _queue_task(store, _create_task(store)))
    gate = store.open_task_gate(
        task.id,
        kind=GateKind.RECONCILIATION,
        source_key=f"{task.id}:legacy-reconciliation",
        prompt={"actions": ["request_changes", "cancel"]},
    )

    amended = store.amend_task_gate_prompt(
        gate.id,
        {"actions": ["retry", "request_changes", "cancel"]},
        expected_version=gate.version,
        command_id="amend-legacy-gate",
    )
    replay = store.amend_task_gate_prompt(
        gate.id,
        {"actions": ["retry", "request_changes", "cancel"]},
        expected_version=gate.version,
        command_id="amend-legacy-gate",
    )

    assert amended.version == gate.version + 1
    assert replay == amended
    assert amended.prompt["actions"][0] == "retry"
    assert any(
        event.event_type == "gate.prompt_amended"
        and event.aggregate_id == gate.id
        for event in store.list_events(task_id=task.id)
    )
    with pytest.raises(GateConflict):
        store.amend_task_gate_prompt(
            gate.id,
            {"actions": ["cancel"]},
            expected_version=gate.version,
            command_id="stale-amend-legacy-gate",
        )


def test_plan_revisions_and_evidence_are_database_immutable(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    task = _create_task(store)
    first, task = _create_plan(store, task)
    second = store.create_plan_revision(
        task.id,
        PlanSpec(nodes=(NodeSpec("replacement", kind=NodeKind.EVALUATE),)),
        expected_task_version=task.version,
        created_by="planner",
        command_id="plan-2",
    )
    assert first.plan.revision == 1
    assert second.plan.revision == 2
    assert second.plan.parent_plan_id == first.plan.id
    assert store.get_plan(first.plan.id).plan.content_hash == first.plan.content_hash

    evidence = store.add_evidence(
        task.id,
        plan_id=first.plan.id,
        kind=EvidenceKind.TEST_RESULT,
        payload={"passed": 12, "failed": 0},
        created_by="tester",
        command_id="evidence-1",
    )
    replay = store.add_evidence(
        task.id,
        plan_id=first.plan.id,
        kind=EvidenceKind.TEST_RESULT,
        payload={"passed": 12, "failed": 0},
        created_by="tester",
        command_id="evidence-1",
    )
    assert replay.id == evidence.id

    connection = store.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="plans are immutable"):
            connection.execute(
                "UPDATE orch_plans SET created_by = 'tamper' WHERE id = ?",
                (first.plan.id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="evidence is immutable"):
            connection.execute(
                "DELETE FROM orch_evidence WHERE id = ?", (evidence.id,)
            )
    finally:
        connection.close()


def test_atomic_run_claim_fencing_and_completion(tmp_path):
    db = tmp_path / "orch.db"
    setup = OrchestrationStore(db)
    task = _create_task(setup)
    graph, task = _create_plan(setup, task)
    task = _queue_task(setup, task)
    task = _start_task(setup, task)
    run = setup.enqueue_run(
        task.id, "implement", command_id="enqueue-implement"
    )

    stores = (OrchestrationStore(db), OrchestrationStore(db))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda item: item[1].claim_next_run(
                    item[0], command_id=f"claim-{item[0]}"
                ),
                (("worker-a", stores[0]), ("worker-b", stores[1])),
            )
        )
    claims = [claim for claim in results if claim is not None]
    assert len(claims) == 1
    claim = claims[0]
    assert claim.run.id == run.id
    assert claim.run.status is RunStatus.CLAIMED
    assert claim.lease.fencing_token == 1

    setup.start_run(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        command_id="start-run",
    )
    with pytest.raises(LeaseConflict):
        setup.complete_run(
            run.id,
            "stale-token",
            claim.lease.fencing_token,
            command_id="stale-complete",
        )
    completed = setup.complete_run(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        output={"result": "ok"},
        command_id="complete-run",
    )
    assert completed.status is RunStatus.SUCCEEDED
    assert completed.output == {"result": "ok"}
    assert setup.complete_run(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        output={"result": "ok"},
        command_id="complete-run",
    ).status is RunStatus.SUCCEEDED


def test_run_activity_is_fenced_idempotent_redacted_and_immutable(
    tmp_path, monkeypatch
):
    store = OrchestrationStore(tmp_path / "activity.db")
    task = _create_task(store, "activity-task")
    _, task = _create_plan(store, task)
    task = _queue_task(store, task)
    task = _start_task(store, task)
    run = store.enqueue_run(task.id, "implement", command_id="enqueue-activity")
    claim = store.claim_next_run("activity-worker", command_id="claim-activity")
    assert claim is not None
    store.start_run(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        command_id="start-activity",
    )

    first = store.append_run_activity(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        event_key="tool-1:started",
        source_id="tool-1",
        kind="tool",
        status="running",
        title="Command",
        summary="curl -H 'Authorization: Bearer super-secret-token' /health",
        detail={
            "command": "deploy --api-key=private-value",
            "access_token": "must-never-persist",
            "input_tokens": 42,
        },
    )
    replay = store.append_run_activity(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        event_key="tool-1:started",
        source_id="tool-1",
        kind="tool",
        status="running",
        title="Changed title is ignored by idempotency",
    )
    second = store.append_run_activity(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        event_key="tool-1:completed",
        source_id="tool-1",
        kind="tool",
        status="completed",
        title="Command",
        detail={"exit_code": 0},
    )

    assert replay == first
    assert second.sequence > first.sequence
    page = store.list_run_activity(task.id, run.id, limit=10)
    assert [item.event_key for item in page] == ["tool-1:started", "tool-1:completed"]
    assert "super-secret-token" not in page[0].summary
    assert "private-value" not in page[0].detail["command"]
    assert page[0].detail["access_token"] == "[REDACTED]"
    assert page[0].detail["input_tokens"] == 42
    assert store.list_run_activity(
        task.id, run.id, after_sequence=first.sequence, limit=10
    ) == (second,)

    with pytest.raises(LeaseConflict):
        store.append_run_activity(
            run.id,
            "stale-token",
            claim.lease.fencing_token,
            event_key="stale",
            source_id="stale",
            kind="lifecycle",
            status="running",
            title="Stale worker",
        )

    monkeypatch.setattr(store_module, "MAX_RUN_ACTIVITY_ROWS", 2)
    marker = store.append_run_activity(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        event_key="too-many-events",
        source_id="noisy-provider",
        kind="lifecycle",
        status="running",
        title="This row is replaced by the cap marker",
    )
    assert marker.event_key == "openworker:activity_truncated"
    assert marker.detail == {"retained_limit": 2}

    connection = store.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="run activity is immutable"):
            connection.execute(
                "UPDATE orch_run_activity SET title = 'tampered' WHERE id = ?",
                (first.id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="run activity is immutable"):
            connection.execute("DELETE FROM orch_run_activity WHERE id = ?", (first.id,))
    finally:
        connection.close()


def test_gate_compare_and_swap_requeues_same_attempt(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    task = _create_task(store)
    _, task = _create_plan(store, task)
    task = _queue_task(store, task)
    task = _start_task(store, task)
    run = store.enqueue_run(task.id, "implement", command_id="enqueue")
    claim = store.claim_next_run("worker", command_id="claim")
    assert claim is not None
    store.start_run(run.id, claim.lease.token, 1, command_id="start")
    gate = store.prepare_run_gate(
        run.id,
        claim.lease.token,
        1,
        kind=GateKind.PERMISSION,
        source_key=f"{run.id}:tool-1:permission",
        prompt={"tool": "write_file"},
        command_id="prepare-gate",
    )
    gate = store.commit_prepared_gate(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        gate_id=gate.id,
        checkpoint=_checkpoint(store.get_run(run.id), claim, gate),
        command_id="commit-gate",
    )
    assert gate.status is GateStatus.OPEN
    assert store.get_run(run.id).status is RunStatus.WAITING_GATE
    assert store.get_task(task.id).status is TaskStatus.WAITING_HUMAN

    resolved = store.resolve_gate(
        gate.id,
        GateStatus.APPROVED,
        {"allowed": True},
        resolved_by="owner",
        expected_version=gate.version,
        command_id="resolve-gate",
    )
    assert resolved.status is GateStatus.APPROVED and resolved.version == gate.version + 1
    with pytest.raises(GateConflict):
        store.resolve_gate(
            gate.id,
            GateStatus.REJECTED,
            {"allowed": False},
            resolved_by="other",
            expected_version=1,
            command_id="resolve-again",
        )
    resumed = store.claim_next_run("worker-2", command_id="claim-resumed")
    assert resumed is not None
    assert resumed.run.id == run.id and resumed.run.attempt == 1
    assert resumed.lease.fencing_token == 2


def test_cancel_pending_runs_closes_run_gate_in_one_idempotent_command(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    task = _create_task(store)
    _, task = _create_plan(store, task)
    task = _queue_task(store, task)
    task = _start_task(store, task)
    run = store.enqueue_run(task.id, "implement", command_id="enqueue")
    claim = store.claim_next_run("worker", command_id="claim")
    assert claim is not None
    store.start_run(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        command_id="start",
    )
    run_gate = store.prepare_run_gate(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        kind=GateKind.PERMISSION,
        source_key=f"{run.id}:permission",
        prompt={"tool": "write_file"},
        command_id="prepare-run-gate",
    )
    run_gate = store.commit_prepared_gate(
        run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        gate_id=run_gate.id,
        checkpoint=_checkpoint(store.get_run(run.id), claim, run_gate),
        command_id="commit-run-gate",
    )
    assert store.cancel_task_runs(task.id, command_id="cancel-pending") == 1
    assert store.get_run(run.id).status is RunStatus.CANCELED
    assert store.get_gate(run_gate.id).status is GateStatus.CANCELED
    assert [
        event.aggregate_id
        for event in store.list_events(task_id=task.id)
        if event.event_type == "gate.canceled"
    ] == [run_gate.id]

    assert store.cancel_task_runs(task.id, command_id="cancel-pending") == 1
    assert len(
        [
            event
            for event in store.list_events(task_id=task.id)
            if event.event_type == "gate.canceled"
        ]
    ) == 1


def test_prepared_gate_is_unresolvable_until_checkpoint_and_lease_commit(tmp_path):
    store = OrchestrationStore(tmp_path / "prepared-gate.db")
    try:
        task = _create_task(store, "prepared-gate")
        _, task = _create_plan(store, task)
        task = _queue_task(store, task)
        task = _start_task(store, task)
        run = store.enqueue_run(task.id, "implement")
        claim = store.claim_next_run("preparing-worker")
        assert claim is not None and claim.run.id == run.id
        store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)

        gate = store.prepare_run_gate(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=f"{run.id}:question:prepared",
            prompt={"title": "Wait", "actions": ["submit"]},
        )
        assert gate.status is GateStatus.PREPARING
        assert store.get_run(run.id).status is RunStatus.RUNNING
        store.assert_run_lease(
            run.id, claim.lease.token, claim.lease.fencing_token
        )
        assert store.get_task(task.id).status is TaskStatus.RUNNING
        with pytest.raises(GateConflict, match="already preparing"):
            store.resolve_gate(
                gate.id,
                GateStatus.APPROVED,
                {"decision": "submit", "response": "too early"},
                resolved_by="racing-user",
                expected_version=gate.version,
            )

        checkpoint = _checkpoint(
            store.get_run(run.id), claim, gate, "question-call"
        )
        committed = store.commit_prepared_gate(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            gate_id=gate.id,
            checkpoint=checkpoint,
            command_id="commit-prepared-gate",
        )
        assert committed.status is GateStatus.OPEN
        assert store.get_run(run.id).status is RunStatus.WAITING_GATE
        assert store.get_task(task.id).status is TaskStatus.WAITING_HUMAN
        with pytest.raises(LeaseConflict):
            store.assert_run_lease(
                run.id, claim.lease.token, claim.lease.fencing_token
            )
        replayed = store.commit_prepared_gate(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            gate_id=gate.id,
            checkpoint=checkpoint,
            command_id="commit-prepared-gate",
        )
        assert replayed == committed
    finally:
        store.close()


def test_failed_run_atomically_cancels_its_unpublished_prepared_gate(tmp_path):
    store = OrchestrationStore(tmp_path / "prepared-gate-failure.db")
    try:
        task = _create_task(store, "prepared-gate-failure")
        _, task = _create_plan(store, task)
        task = _start_task(store, _queue_task(store, task))
        run = store.enqueue_run(task.id, "implement")
        claim = store.claim_next_run("cleanup-worker")
        assert claim is not None
        store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
        gate = store.prepare_run_gate(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=f"{run.id}:question:cleanup",
            prompt={"title": "Wait"},
        )

        failed = store.fail_run(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            error_kind="process_tree_cleanup_failed",
            error_message="descendant survived cleanup",
        )
        assert failed.error_kind == "process_tree_cleanup_failed"
        canceled = store.get_gate(gate.id)
        assert canceled.status is GateStatus.CANCELED
        assert canceled.resolution["reason"] == "process_tree_cleanup_failed"
        with pytest.raises(GateConflict, match="already canceled"):
            store.resolve_gate(
                gate.id,
                GateStatus.APPROVED,
                {"decision": "submit"},
                resolved_by="late-user",
                expected_version=canceled.version,
            )
    finally:
        store.close()


def _start_two_sibling_runs(store: OrchestrationStore, key: str):
    task = _create_task(store, key)
    graph = store.create_plan_revision(
        task.id,
        PlanSpec(nodes=(NodeSpec("left"), NodeSpec("right"))),
        expected_task_version=task.version,
        created_by="test",
    )
    task = _start_task(store, _queue_task(store, store.get_task(task.id)))
    left = store.enqueue_run(task.id, "left")
    right = store.enqueue_run(task.id, "right")
    left_claim = store.claim_next_run("left-worker")
    right_claim = store.claim_next_run("right-worker")
    assert left_claim is not None and left_claim.run.id == left.id
    assert right_claim is not None and right_claim.run.id == right.id
    for claim in (left_claim, right_claim):
        store.start_run(
            claim.run.id, claim.lease.token, claim.lease.fencing_token
        )
    return task, graph, left_claim, right_claim


def test_claim_next_run_preserves_fifo_when_enqueue_timestamps_tie(
    monkeypatch, tmp_path
):
    fixed = datetime(2026, 8, 4, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(store_module, "_now", lambda: fixed)
    store = OrchestrationStore(tmp_path / "fifo-tie.db")
    try:
        _task, _graph, left, right = _start_two_sibling_runs(store, "fifo-tie")
        assert left.run.node_key == "left"
        assert right.run.node_key == "right"
        assert left.run.created_at == right.run.created_at
    finally:
        store.close()


def test_sibling_can_commit_success_while_another_run_waits_at_gate(tmp_path):
    store = OrchestrationStore(tmp_path / "sibling-success.db")
    try:
        task, _graph, left, right = _start_two_sibling_runs(
            store, "sibling-success"
        )
        gate = store.prepare_run_gate(
            left.run.id,
            left.lease.token,
            left.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=f"{left.run.id}:question:sibling",
            prompt={"title": "Wait", "actions": ["submit"]},
        )
        store.commit_prepared_gate(
            left.run.id,
            left.lease.token,
            left.lease.fencing_token,
            gate_id=gate.id,
            checkpoint=_checkpoint(
                store.get_run(left.run.id), left, gate, "left-call"
            ),
        )
        assert store.get_task(task.id).status is TaskStatus.WAITING_HUMAN

        completed = store.complete_run(
            right.run.id,
            right.lease.token,
            right.lease.fencing_token,
            output={"result": "right sibling settled exactly once"},
        )
        assert completed.status is RunStatus.SUCCEEDED
        assert completed.output["result"] == "right sibling settled exactly once"
        assert store.get_task(task.id).status is TaskStatus.WAITING_HUMAN
    finally:
        store.close()


def test_multiple_sibling_gates_keep_aggregate_wait_until_all_are_resolved(tmp_path):
    store = OrchestrationStore(tmp_path / "sibling-gates.db")
    try:
        task, _graph, left, right = _start_two_sibling_runs(
            store, "sibling-gates"
        )
        left_gate = store.prepare_run_gate(
            left.run.id,
            left.lease.token,
            left.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=f"{left.run.id}:question:left",
            prompt={"title": "Left", "actions": ["submit"]},
        )
        left_gate = store.commit_prepared_gate(
            left.run.id,
            left.lease.token,
            left.lease.fencing_token,
            gate_id=left_gate.id,
            checkpoint=_checkpoint(
                store.get_run(left.run.id), left, left_gate, "left-call"
            ),
        )

        # The second active sibling retains its own lease/fence even though the
        # aggregate task now advertises human attention.
        right_gate = store.prepare_run_gate(
            right.run.id,
            right.lease.token,
            right.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=f"{right.run.id}:question:right",
            prompt={"title": "Right", "actions": ["submit"]},
        )
        right_gate = store.commit_prepared_gate(
            right.run.id,
            right.lease.token,
            right.lease.fencing_token,
            gate_id=right_gate.id,
            checkpoint=_checkpoint(
                store.get_run(right.run.id), right, right_gate, "right-call"
            ),
        )
        store.resolve_gate(
            left_gate.id,
            GateStatus.APPROVED,
            {"decision": "submit", "response": "left"},
            resolved_by="owner",
            expected_version=left_gate.version,
        )
        assert store.get_task(task.id).status is TaskStatus.WAITING_HUMAN

        store.resolve_gate(
            right_gate.id,
            GateStatus.APPROVED,
            {"decision": "submit", "response": "right"},
            resolved_by="owner",
            expected_version=right_gate.version,
        )
        assert store.get_task(task.id).status is TaskStatus.RUNNING
        assert {
            store.get_run(left.run.id).status,
            store.get_run(right.run.id).status,
        } == {RunStatus.QUEUED}
    finally:
        store.close()


def test_shutdown_release_reprepares_unpublished_gate_without_fake_answer(tmp_path):
    store = OrchestrationStore(tmp_path / "reprepare-gate.db")
    try:
        task = _create_task(store, "reprepare-gate")
        _, task = _create_plan(store, task)
        task = _start_task(store, _queue_task(store, task))
        run = store.enqueue_run(task.id, "implement")
        first = store.claim_next_run("first-worker")
        assert first is not None
        store.start_run(run.id, first.lease.token, first.lease.fencing_token)
        source = f"{run.id}:question:restart"
        gate = store.prepare_run_gate(
            run.id,
            first.lease.token,
            first.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=source,
            prompt={"title": "Retry me", "actions": ["submit"]},
        )
        assert store.list_gates(task.id) == ()
        assert store.count_gates(task.id) == 0
        assert store.count_gates(task.id, include_internal=True) == 1
        assert store.list_gates(task.id, include_internal=True) == (gate,)
        assert not any(
            event.event_type == "gate.prepared"
            for event in store.list_events(task_id=task.id)
        )

        store.release_run(
            run.id,
            first.lease.token,
            first.lease.fencing_token,
            reason="service_shutdown",
        )
        aborted = store.get_gate(gate.id)
        assert aborted.status is GateStatus.CANCELED
        assert aborted.published_at is None
        assert aborted.resolution["publication_state"] == "unpublished"
        assert store.list_gates(task.id) == ()

        # A separately published gate must sort before this old preparation once
        # the latter is retried and only then crosses its publication point.
        middle = store.open_task_gate(
            task.id,
            kind=GateKind.PLAN_APPROVAL,
            source_key=f"{task.id}:middle-published-gate",
            prompt={"title": "Published between attempts", "actions": ["approve"]},
        )
        store.resolve_gate(
            middle.id,
            GateStatus.APPROVED,
            {"decision": "approve"},
            resolved_by="owner",
            expected_version=middle.version,
        )
        connection = store.connect()
        try:
            connection.execute(
                "UPDATE orch_gates SET opened_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00.000000Z", gate.id),
            )
            connection.execute(
                "UPDATE orch_gates SET published_at = ? WHERE id = ?",
                ("2020-01-01T00:00:00.000000Z", middle.id),
            )
            connection.commit()
        finally:
            connection.close()

        second = store.claim_next_run("replacement-worker")
        assert second is not None and second.run.id == run.id
        store.start_run(run.id, second.lease.token, second.lease.fencing_token)
        reprepared = store.prepare_run_gate(
            run.id,
            second.lease.token,
            second.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=source,
            prompt={"title": "Retry me", "actions": ["submit"]},
        )
        assert reprepared.id == gate.id
        assert reprepared.status is GateStatus.PREPARING
        assert reprepared.resolution is None
        assert reprepared.resolved_by is None
        assert reprepared.version > aborted.version

        committed = store.commit_prepared_gate(
            run.id,
            second.lease.token,
            second.lease.fencing_token,
            gate_id=reprepared.id,
            checkpoint=_checkpoint(
                store.get_run(run.id), second, reprepared, "restart-call"
            ),
        )
        assert committed.status is GateStatus.OPEN
        assert committed.published_at is not None
        assert store.list_gates(task.id)[-1].id == committed.id
        event_types = [
            event.event_type for event in store.list_events(task_id=task.id)
        ]
        assert event_types.count("gate.preparation_aborted") == 1
        assert event_types.count("gate.prepared") == 1
        assert [
            event.event_type
            for event in store.list_events(task_id=task.id)
            if event.aggregate_id == committed.id
        ][-2:] == ["gate.prepared", "gate.opened"]
    finally:
        store.close()


def test_checkpoint_identity_is_rejected_before_gate_publication(tmp_path):
    store = OrchestrationStore(tmp_path / "checkpoint-identity.db")
    try:
        task = _create_task(store, "checkpoint-identity")
        _, task = _create_plan(store, task)
        task = _start_task(store, _queue_task(store, task))
        run = store.enqueue_run(task.id, "implement")
        claim = store.claim_next_run("checkpoint-worker")
        assert claim is not None
        store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
        gate = store.prepare_run_gate(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=f"{run.id}:question:identity",
            prompt={"title": "Identity"},
        )
        invalid = {
            **_checkpoint(store.get_run(run.id), claim, gate),
            "blob_uri": "sha256:" + "b" * 64,
        }
        with pytest.raises(ValueError, match="blob identity"):
            store.commit_prepared_gate(
                run.id,
                claim.lease.token,
                claim.lease.fencing_token,
                gate_id=gate.id,
                checkpoint=invalid,
            )
        assert store.get_gate(gate.id).status is GateStatus.PREPARING
        assert store.get_gate(gate.id).published_at is None
        store.assert_run_lease(
            run.id, claim.lease.token, claim.lease.fencing_token
        )
    finally:
        store.close()


def test_expired_preparation_becomes_lost_and_never_published(tmp_path):
    store = OrchestrationStore(tmp_path / "expired-preparation.db")
    try:
        task = _create_task(store, "expired-preparation")
        _, task = _create_plan(store, task)
        task = _start_task(store, _queue_task(store, task))
        run = store.enqueue_run(task.id, "implement")
        now = datetime.now(timezone.utc)
        claim = store.claim_next_run(
            "vanished-worker", lease_seconds=1, now=now
        )
        assert claim is not None
        store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
        gate = store.prepare_run_gate(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            kind=GateKind.QUESTION,
            source_key=f"{run.id}:question:crash",
            prompt={"title": "Never opened"},
        )

        assert store.reap_expired_leases(
            now=now + timedelta(seconds=2)
        ) == 1
        assert store.get_run(run.id).status is RunStatus.LOST
        aborted = store.get_gate(gate.id)
        assert aborted.status is GateStatus.CANCELED
        assert aborted.published_at is None
        assert store.list_gates(task.id) == ()
        assert [
            event.event_type
            for event in store.list_events(task_id=task.id)
            if event.aggregate_id in {run.id, gate.id}
        ][-2:] == ["gate.preparation_aborted", "run.lost"]
    finally:
        store.close()


def test_single_phase_run_gate_entrypoints_fail_closed(tmp_path):
    store = OrchestrationStore(tmp_path / "single-phase-disabled.db")
    try:
        with pytest.raises(ConflictError, match="single-phase run gates are disabled"):
            store.open_gate(
                "run",
                "lease",
                1,
                kind=GateKind.QUESTION,
                source_key="forbidden",
                prompt={},
            )
        with pytest.raises(ConflictError, match="single-phase child gates are disabled"):
            store.open_child_wait(
                "run",
                "lease",
                1,
                child_task_id="child",
                source_key="forbidden-child",
            )
    finally:
        store.close()


def test_expired_lease_event_chain_and_outbox(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    task = _create_task(store)
    _, task = _create_plan(store, task)
    task = _queue_task(store, task)
    task = _start_task(store, task)
    run = store.enqueue_run(task.id, "implement", command_id="enqueue")
    claimed_at = datetime.now(timezone.utc)
    claim = store.claim_next_run(
        "worker",
        lease_seconds=10,
        now=claimed_at,
        command_id="claim",
    )
    assert claim is not None
    assert store.reap_expired_leases(
        now=claimed_at + timedelta(seconds=11), command_id="reap"
    ) == 1
    assert store.get_run(run.id).status is RunStatus.LOST
    with pytest.raises(LeaseConflict):
        store.complete_run(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            command_id="late-complete",
        )

    assert store.verify_event_chain()
    events = store.list_events(task_id=task.id)
    assert events and events[-1].event_type == "run.lost"
    outbox = store.claim_outbox("publisher", limit=100)
    assert len(outbox) == len(store.list_events())
    assert outbox[0].payload["sequence"] >= 1
    assert outbox[0].payload["event_hash"]
    assert "sequence_pending" not in outbox[0].payload
    published = store.mark_outbox_published(outbox[0].id, "publisher")
    assert published.published_at is not None

    connection = store.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE orch_events SET event_type = 'tampered' WHERE id = ?",
                (events[0].id,),
            )
    finally:
        connection.close()


def test_outbox_dead_letter_is_durable_and_not_reclaimed(tmp_path):
    store = OrchestrationStore(tmp_path / "outbox-dead-letter.db")
    task = _create_task(store)
    claimed = store.claim_outbox("publisher", limit=1)
    assert claimed

    dead = store.mark_outbox_dead_lettered(
        claimed[0].id, "publisher", "permanent delivery failure"
    )

    assert dead.dead_lettered_at is not None
    assert dead.last_error == "permanent delivery failure"
    assert all(item.id != dead.id for item in store.claim_outbox("other", limit=100))
    health = store.outbox_health()
    assert health["dead_letters"] == 1
    assert health["pending"] >= 0
    assert [item.id for item in store.list_outbox_dead_letters()] == [dead.id]
    requeued, audit, replayed = store.requeue_outbox(
        dead.id,
        idempotency_key="requeue-permanent-failure-1",
        actor="on-call@example.com",
        reason="The downstream subscriber has recovered.",
    )
    assert replayed is False
    assert requeued.dead_lettered_at is None
    assert requeued.attempts == 0
    assert audit.snapshot_attempts == dead.attempts
    assert audit.snapshot_last_error == "permanent delivery failure"
    assert audit.snapshot_dead_lettered_at == dead.dead_lettered_at
    assert audit.actor == "on-call@example.com"
    assert audit.reason == "The downstream subscriber has recovered."
    assert store.outbox_health()["dead_letters"] == 0

    same_item, same_audit, replayed = store.requeue_outbox(
        dead.id,
        idempotency_key="requeue-permanent-failure-1",
        actor="on-call@example.com",
        reason="The downstream subscriber has recovered.",
    )
    assert replayed is True
    assert same_item.id == dead.id
    assert same_audit.id == audit.id
    assert len(store.list_outbox_requeue_history(dead.id)) == 1
    with pytest.raises(IdempotencyConflict, match="different input"):
        store.requeue_outbox(
            dead.id,
            idempotency_key="requeue-permanent-failure-1",
            actor="another-operator@example.com",
            reason="A different request must conflict.",
        )

    connection = store.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE orch_outbox_requeue_history SET reason = 'tampered' WHERE id = ?",
                (audit.id,),
            )
    finally:
        connection.close()
    events = [
        event
        for event in store.list_events()
        if event.event_type == "outbox.requeued"
    ]
    assert len(events) == 1
    assert events[0].payload["actor"] == "on-call@example.com"
    assert events[0].payload["history_id"] == audit.id
    assert task.id


def test_outbox_requeue_history_keeps_each_dead_letter_snapshot(tmp_path):
    store = OrchestrationStore(tmp_path / "outbox-requeue-history.db")
    _create_task(store)
    original = store.claim_outbox("publisher-one", limit=1)[0]
    first_dead = store.mark_outbox_dead_lettered(
        original.id, "publisher-one", "first terminal failure"
    )
    store.requeue_outbox(
        original.id,
        idempotency_key="requeue-cycle-1",
        actor="operator-one",
        reason="First repair was deployed.",
    )

    claimed = store.claim_outbox("publisher-two", limit=100)
    original_again = next(item for item in claimed if item.id == original.id)
    second_dead = store.mark_outbox_dead_lettered(
        original.id, "publisher-two", "second terminal failure"
    )
    store.requeue_outbox(
        original.id,
        idempotency_key="requeue-cycle-2",
        actor="operator-two",
        reason="Second repair was deployed.",
    )

    history = store.list_outbox_requeue_history(original.id)
    assert [item.command_id for item in history] == [
        "requeue-cycle-2",
        "requeue-cycle-1",
    ]
    assert history[0].snapshot_attempts == second_dead.attempts
    assert history[0].snapshot_last_error == "second terminal failure"
    assert history[1].snapshot_attempts == first_dead.attempts
    assert history[1].snapshot_last_error == "first terminal failure"


def test_scheduler_leader_lease_prevents_two_active_services(tmp_path):
    database = tmp_path / "scheduler-leader.db"
    first = OrchestrationStore(database)
    second = OrchestrationStore(database)
    now = datetime.now(timezone.utc)

    first_token, first_epoch = first.acquire_scheduler_leader(
        "first", lease_seconds=10, now=now
    )
    with pytest.raises(LeaseConflict, match="already owned"):
        second.acquire_scheduler_leader("second", lease_seconds=10, now=now)

    second_token, second_epoch = second.acquire_scheduler_leader(
        "second", lease_seconds=10, now=now + timedelta(seconds=11)
    )
    assert second_epoch == first_epoch + 1
    with pytest.raises(LeaseConflict, match="was lost"):
        first.heartbeat_scheduler_leader(
            "first",
            first_token,
            first_epoch,
            now=now + timedelta(seconds=12),
        )
    second.heartbeat_scheduler_leader(
        "second",
        second_token,
        second_epoch,
        now=now + timedelta(seconds=12),
    )
    assert second.release_scheduler_leader("second", second_token, second_epoch)

    third_token, third_epoch = first.acquire_scheduler_leader(
        "third", lease_seconds=10, now=now + timedelta(seconds=13)
    )
    assert third_epoch == second_epoch + 1
    assert first.release_scheduler_leader("third", third_token, third_epoch)


def test_scheduler_epoch_fences_every_bound_domain_write(tmp_path):
    database = tmp_path / "scheduler-write-fence.db"
    old = OrchestrationStore(database)
    replacement = OrchestrationStore(database)
    now = datetime.now(timezone.utc)

    old_token, old_epoch = old.acquire_scheduler_leader(
        "old", lease_seconds=10, now=now
    )
    old.bind_scheduler_fence("old", old_token, old_epoch)
    first_task = _create_task(old, key="old-leader-task")
    assert first_task.id

    new_token, new_epoch = replacement.acquire_scheduler_leader(
        "replacement", lease_seconds=10, now=now + timedelta(seconds=11)
    )
    replacement.bind_scheduler_fence("replacement", new_token, new_epoch)
    with pytest.raises(LeaseConflict, match="fencing rejected stale leader"):
        _create_task(old, key="stale-leader-task")

    current_task = _create_task(replacement, key="replacement-task")
    assert current_task.id
    assert replacement.release_scheduler_leader(
        "replacement", new_token, new_epoch
    )


def test_expired_scheduler_cannot_resurrect_its_own_epoch(tmp_path):
    store = OrchestrationStore(tmp_path / "expired-scheduler.db")
    now = datetime.now(timezone.utc)
    token, epoch = store.acquire_scheduler_leader(
        "paused-process", lease_seconds=10, now=now
    )

    with pytest.raises(LeaseConflict, match="was lost"):
        store.heartbeat_scheduler_leader(
            "paused-process",
            token,
            epoch,
            lease_seconds=10,
            now=now + timedelta(seconds=11),
        )

    replacement_token, replacement_epoch = store.acquire_scheduler_leader(
        "replacement", lease_seconds=10, now=now + timedelta(seconds=11)
    )
    assert replacement_epoch == epoch + 1
    assert store.release_scheduler_leader(
        "replacement", replacement_token, replacement_epoch
    )
