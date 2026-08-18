from __future__ import annotations

import json
from pathlib import Path

from coworker.orchestration.models import (
    GateKind,
    GateStatus,
    OrchestrationStage,
    RunStatus,
    TaskStatus,
)
from coworker.orchestration.service import OrchestrationService


class FakeManager:
    default_workspace = None
    model = "gpt-5.6-sol"

    def _provider_configured(self, _provider: str) -> bool:
        return True

    def get_settings(self) -> dict:
        return {
            "models": [self.model],
            "model_labels": {self.model: "Test model"},
            "model_context_windows": {self.model: 400_000},
        }

    async def broadcast_event(self, _event) -> None:
        return None


def _low_complexity() -> dict[str, int]:
    return {
        "scope": 1,
        "uncertainty": 1,
        "dependencies": 0,
        "side_effects": 0,
        "parallelism": 0,
        "verification": 1,
    }


def _create_running_plan(
    service: OrchestrationService,
    plan: dict,
    *,
    key: str,
) -> str:
    task_id = service.create_task(
        {
            "idempotency_key": key,
            "objective": "Prove the configured failure-policy semantics",
            "domain": "knowledge",
            "acceptance_criteria": ["every terminal result is audited"],
            "complexity_factors": _low_complexity(),
            "plan": plan,
            "auto_start": False,
        }
    )["id"]
    service.submit_task(task_id)
    service._advance_task(task_id)
    return task_id


def _fail_next(service: OrchestrationService, expected_key: str) -> None:
    claim = service.store.claim_next_run(f"worker-{expected_key}")
    assert claim is not None
    assert claim.run.node_key == expected_key
    service.store.start_run(
        claim.run.id,
        claim.lease.token,
        claim.lease.fencing_token,
    )
    service.store.fail_run(
        claim.run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        error_kind="test_failure",
        error_message=f"{expected_key} failed by design",
    )


def _complete_next(service: OrchestrationService, expected_key: str) -> None:
    claim = service.store.claim_next_run(f"worker-{expected_key}-complete")
    assert claim is not None
    assert claim.run.node_key == expected_key
    service.store.start_run(
        claim.run.id,
        claim.lease.token,
        claim.lease.fencing_token,
    )
    service.store.complete_run(
        claim.run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        output={"summary": f"{expected_key} completed"},
    )


def _latest(service: OrchestrationService, task_id: str):
    return service._latest_runs(service.store.list_runs(task_id))


def test_continue_preserves_edge_conditions_and_unrelated_work(tmp_path: Path) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = _create_running_plan(
            service,
            {
                "nodes": [
                    {
                        "key": "fails",
                        "failure_policy": "continue",
                        "priority": 100,
                    },
                    {"key": "independent", "priority": 10},
                    {"key": "failure_handler"},
                    {"key": "success_only"},
                ],
                "edges": [
                    {
                        "from": "fails",
                        "to": "failure_handler",
                        "condition": "failure",
                    },
                    {"from": "fails", "to": "success_only", "condition": "success"},
                ],
            },
            key="continue-policy",
        )
        _fail_next(service, "fails")

        service._advance_task(task_id)

        latest = _latest(service, task_id)
        assert latest["fails"].status is RunStatus.FAILED
        assert latest["failure_handler"].status is RunStatus.QUEUED
        assert latest["success_only"].status is RunStatus.SKIPPED
        assert latest["independent"].status is RunStatus.QUEUED
    finally:
        service.store.close()


def test_skip_dependents_suppresses_only_transitive_descendants(tmp_path: Path) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = _create_running_plan(
            service,
            {
                "nodes": [
                    {
                        "key": "fails",
                        "failure_policy": "skip_dependents",
                        "priority": 100,
                    },
                    {"key": "independent", "priority": 10},
                    {"key": "failure_handler"},
                    {"key": "grandchild"},
                ],
                "edges": [
                    {
                        "from": "fails",
                        "to": "failure_handler",
                        "condition": "failure",
                    },
                    {
                        "from": "failure_handler",
                        "to": "grandchild",
                        "condition": "always",
                    },
                ],
            },
            key="skip-dependents-policy",
        )
        _fail_next(service, "fails")

        service._advance_task(task_id)

        latest = _latest(service, task_id)
        assert latest["failure_handler"].status is RunStatus.SKIPPED
        assert latest["grandchild"].status is RunStatus.SKIPPED
        assert latest["independent"].status is RunStatus.QUEUED
        assert latest["failure_handler"].error_kind == "failure_policy"
        assert "skip_dependents from failed node fails" in (
            latest["failure_handler"].error_message or ""
        )
    finally:
        service.store.close()


def test_fail_fast_retries_then_suppresses_all_not_started_work(tmp_path: Path) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = _create_running_plan(
            service,
            {
                "nodes": [
                    {
                        "key": "fails",
                        "failure_policy": "fail_fast",
                        "priority": 100,
                        "retry_policy": {
                            "max_attempts": 2,
                            "initial_delay_seconds": 0,
                            "jitter": 0,
                        },
                    },
                    {"key": "in_flight", "priority": 20},
                    {"key": "queued_branch", "priority": 10},
                    {"key": "failure_handler"},
                ],
                "edges": [
                    {
                        "from": "fails",
                        "to": "failure_handler",
                        "condition": "failure",
                    }
                ],
            },
            key="fail-fast-policy",
        )
        _fail_next(service, "fails")

        in_flight = service.store.claim_next_run("worker-in-flight")
        assert in_flight is not None and in_flight.run.node_key == "in_flight"
        service.store.start_run(
            in_flight.run.id,
            in_flight.lease.token,
            in_flight.lease.fencing_token,
        )

        service._advance_task(task_id)
        latest = _latest(service, task_id)
        assert latest["fails"].attempt == 2
        assert latest["fails"].status is RunStatus.QUEUED
        assert latest["in_flight"].status is RunStatus.RUNNING
        assert latest["queued_branch"].status is RunStatus.QUEUED

        _fail_next(service, "fails")
        service._advance_task(task_id)

        latest = _latest(service, task_id)
        assert latest["fails"].attempt == 2
        assert latest["fails"].status is RunStatus.FAILED
        assert latest["in_flight"].status is RunStatus.RUNNING
        assert latest["queued_branch"].status is RunStatus.SKIPPED
        assert latest["failure_handler"].status is RunStatus.SKIPPED
        policy_events = [
            event
            for event in service.store.list_events(task_id=task_id)
            if event.event_type == "run.skipped"
            and event.payload.get("policy_controlled") is True
        ]
        assert {event.payload["node_key"] for event in policy_events} == {
            "queued_branch",
            "failure_handler",
        }
        assert service.store.verify_event_chain() is True
    finally:
        service.store.close()


def test_explicit_retry_reopens_unstarted_fail_fast_descendants(tmp_path: Path) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = _create_running_plan(
            service,
            {
                "nodes": [
                    {
                        "key": "fails",
                        "failure_policy": "fail_fast",
                        "priority": 100,
                        "retry_policy": {
                            "max_attempts": 2,
                            "initial_delay_seconds": 0,
                            "jitter": 0,
                        },
                    },
                    {"key": "downstream", "retry_policy": {"max_attempts": 1}},
                    {"key": "final", "retry_policy": {"max_attempts": 1}},
                ],
                "edges": [
                    {"from": "fails", "to": "downstream", "condition": "success"},
                    {"from": "downstream", "to": "final", "condition": "success"},
                ],
            },
            key="explicit-retry-reopens-policy-skips",
        )
        claim = service.store.claim_next_run("worker-cleanup-unknown")
        assert claim is not None and claim.run.node_key == "fails"
        service.store.start_run(
            claim.run.id, claim.lease.token, claim.lease.fencing_token
        )
        service.store.fail_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            error_kind="process_tree_cleanup_failed",
            error_message="cleanup state is unknown",
        )

        service._advance_task(task_id)
        skipped = _latest(service, task_id)
        downstream_id = skipped["downstream"].id
        final_id = skipped["final"].id
        assert skipped["downstream"].status is RunStatus.SKIPPED
        assert skipped["final"].status is RunStatus.SKIPPED
        service._advance_task(task_id)
        gate = next(
            item
            for item in service.store.list_gates(task_id, statuses=(GateStatus.OPEN,))
            if item.kind is GateKind.RECONCILIATION
        )
        service.resolve_gate(
            task_id,
            gate.id,
            decision="retry",
            expected_version=gate.version,
            idempotency_key="retry-cleanup-unknown-source",
        )
        service._advance_task(task_id)
        _complete_next(service, "fails")

        service._advance_task(task_id)
        after_source = _latest(service, task_id)
        assert after_source["downstream"].id == downstream_id
        assert after_source["downstream"].attempt == 1
        assert after_source["downstream"].status is RunStatus.QUEUED
        assert after_source["final"].id == final_id
        assert after_source["final"].status is RunStatus.SKIPPED

        _complete_next(service, "downstream")
        service._advance_task(task_id)
        after_downstream = _latest(service, task_id)
        assert after_downstream["final"].id == final_id
        assert after_downstream["final"].attempt == 1
        assert after_downstream["final"].status is RunStatus.QUEUED
        reopened = [
            event
            for event in service.store.list_events(task_id=task_id)
            if event.event_type == "run.reopened"
        ]
        assert [event.payload["node_key"] for event in reopened] == [
            "downstream",
            "final",
        ]
        assert service.store.verify_event_chain() is True
    finally:
        service.store.close()


def test_startup_repairs_stale_reconciliation_gate_after_successful_retry(
    tmp_path: Path,
) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = _create_running_plan(
            service,
            {
                "nodes": [
                    {
                        "key": "fails",
                        "failure_policy": "fail_fast",
                        "priority": 100,
                        "retry_policy": {
                            "max_attempts": 2,
                            "initial_delay_seconds": 0,
                            "jitter": 0,
                        },
                    },
                    {"key": "downstream", "retry_policy": {"max_attempts": 1}},
                ],
                "edges": [
                    {"from": "fails", "to": "downstream", "condition": "success"}
                ],
            },
            key="repair-stale-policy-skip-gate",
        )
        claim = service.store.claim_next_run("worker-stale-gate-source")
        assert claim is not None and claim.run.node_key == "fails"
        service.store.start_run(
            claim.run.id, claim.lease.token, claim.lease.fencing_token
        )
        service.store.fail_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            error_kind="process_tree_cleanup_failed",
            error_message="cleanup state is unknown",
        )
        service._advance_task(task_id)
        service._advance_task(task_id)
        retry_gate = next(
            item
            for item in service.store.list_gates(task_id, statuses=(GateStatus.OPEN,))
            if item.kind is GateKind.RECONCILIATION
        )
        service.resolve_gate(
            task_id,
            retry_gate.id,
            decision="retry",
            expected_version=retry_gate.version,
            idempotency_key="retry-before-stale-gate-repair",
        )
        service._advance_task(task_id)
        _complete_next(service, "fails")

        task = service.store.get_task(task_id)
        service._transition_stage(
            task,
            OrchestrationStage.INTER_STEP_EVALUATION,
            "simulate-legacy-premature-evaluation",
        )
        task = service.store.get_task(task_id)
        skipped = _latest(service, task_id)["downstream"]
        stale_gate = service._open_lifecycle_gate(
            task,
            GateKind.RECONCILIATION,
            f"{task.id}:reconciliation:{task.active_plan_id}:legacy-stale-skip",
            {
                "title": "Execution needs reconciliation",
                "failed_runs": [],
                "workspace_commit_failures": [],
                "failed_children": [],
                "verification": [
                    {
                        "node_key": "downstream",
                        "run_id": skipped.id,
                        "status": "unknown",
                    }
                ],
                "actions": ["request_changes", "cancel"],
            },
        )
        assert service.store.get_task(task_id).status is TaskStatus.WAITING_HUMAN

        assert service._repair_superseded_policy_skip_gates() == 1
        repaired_task = service.store.get_task(task_id)
        repaired_gate = service.store.get_gate(stale_gate.id)
        assert repaired_task.status is TaskStatus.RUNNING
        assert repaired_task.current_stage.value == "execution_review_test"
        assert repaired_gate.status is GateStatus.APPROVED
        assert repaired_gate.resolution["decision"] == "resume_policy_skips"

        service._advance_task(task_id)
        assert _latest(service, task_id)["downstream"].status is RunStatus.QUEUED
        assert service.store.verify_event_chain() is True
    finally:
        service.store.close()


def test_exhausted_reconciliation_omits_retry_and_legacy_retry_replans(
    tmp_path: Path,
) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = _create_running_plan(
            service,
            {
                "nodes": [
                    {
                        "key": "fails",
                        "failure_policy": "fail_fast",
                        "retry_policy": {
                            "max_attempts": 1,
                            "initial_delay_seconds": 0,
                            "jitter": 0,
                        },
                    }
                ],
                "edges": [],
            },
            key="exhausted-reconciliation-replan",
        )
        first_graph = service.store.get_plan(
            service.store.get_task(task_id).active_plan_id or ""
        )
        _fail_next(service, "fails")

        service._advance_task(task_id)

        gate = service.store.list_gates(
            task_id, statuses=(GateStatus.OPEN,)
        )[0]
        assert gate.kind is GateKind.RECONCILIATION
        assert gate.prompt["actions"] == ["request_changes", "cancel"]
        failed = service.store.list_runs(task_id)[0]
        assert failed.attempt == 1
        assert failed.node_id == first_graph.nodes[0].id

        # Simulate a reconciliation prompt persisted by an older release, which
        # advertised Retry even after the immutable attempt budget was exhausted.
        legacy_prompt = {**dict(gate.prompt), "actions": ["retry", "request_changes", "cancel"]}
        with service.store.connect() as connection:
            connection.execute(
                "UPDATE orch_gates SET prompt_json = ? WHERE id = ?",
                (
                    json.dumps(
                        legacy_prompt,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                    gate.id,
                ),
            )
        gate = service.store.get_gate(gate.id)
        service.resolve_gate(
            task_id,
            gate.id,
            decision="retry",
            expected_version=gate.version,
            idempotency_key="resolve-legacy-exhausted-retry",
        )

        service._advance_task(task_id)

        replanned_task = service.store.get_task(task_id)
        assert replanned_task.active_plan_id != first_graph.plan.id
        second_graph = service.store.get_plan(replanned_task.active_plan_id or "")
        assert second_graph.plan.revision == 2
        assert second_graph.plan.parent_plan_id == first_graph.plan.id
        assert second_graph.plan.metadata["revision_reason"] == "retry_exhausted_replan"
        assert second_graph.nodes[0].key == first_graph.nodes[0].key == "fails"
        assert second_graph.nodes[0].id != first_graph.nodes[0].id

        second_runs = [
            run
            for run in service.store.list_runs(task_id)
            if run.plan_id == second_graph.plan.id
        ]
        assert len(second_runs) == 1
        assert second_runs[0].attempt == 1
        assert second_runs[0].node_id == second_graph.nodes[0].id
        assert second_runs[0].session_id == f"__orch__{second_graph.nodes[0].id}_1"
        assert second_runs[0].session_id != failed.session_id

        markers = [
            item
            for item in service.store.list_evidence(task_id)
            if item.payload.get("retry_exhausted_replan") is True
        ]
        assert len(markers) == 1
        assert markers[0].payload["gate_id"] == gate.id
        assert markers[0].payload["plan_id"] == first_graph.plan.id
        assert markers[0].payload["failed_runs"] == [
            {
                "run_id": failed.id,
                "node_id": failed.node_id,
                "node_key": "fails",
                "attempt": 1,
                "max_attempts": 1,
            }
        ]

        # Re-entering the coordinator must reuse the active revision and marker.
        service._advance_task(task_id)
        assert len(service.store.list_plans(task_id)) == 2
        assert len(
            [
                item
                for item in service.store.list_evidence(task_id)
                if item.payload.get("retry_exhausted_replan") is True
            ]
        ) == 1
        assert service.store.verify_event_chain() is True
    finally:
        service.store.close()


def test_reconciliation_request_changes_keeps_feedback_revision_semantics(
    tmp_path: Path,
) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = _create_running_plan(
            service,
            {
                "nodes": [
                    {
                        "key": "fails",
                        "instructions": "Perform the original bounded work.",
                        "retry_policy": {"max_attempts": 1},
                    }
                ],
                "edges": [],
            },
            key="request-changes-revision-semantics",
        )
        first_graph = service.store.get_plan(
            service.store.get_task(task_id).active_plan_id or ""
        )
        _fail_next(service, "fails")
        service._advance_task(task_id)
        gate = service.store.list_gates(
            task_id, statuses=(GateStatus.OPEN,)
        )[0]
        feedback = "Re-run the same bounded node after applying the repaired runtime contract."

        service.resolve_gate(
            task_id,
            gate.id,
            decision="request_changes",
            response=feedback,
            expected_version=gate.version,
            idempotency_key="resolve-request-changes-revision",
        )
        service._advance_task(task_id)

        task = service.store.get_task(task_id)
        second_graph = service.store.get_plan(task.active_plan_id or "")
        assert second_graph.plan.revision == 2
        assert second_graph.plan.parent_plan_id == first_graph.plan.id
        assert second_graph.plan.metadata["revision_reason"] == "request_changes"
        assert second_graph.plan.metadata["feedback"] == feedback
        assert second_graph.nodes[0].input["revision_feedback"] == feedback
        assert feedback in second_graph.nodes[0].instructions
        assert not any(
            item.payload.get("retry_exhausted_replan") is True
            for item in service.store.list_evidence(task_id)
        )
        second_run = next(
            run
            for run in service.store.list_runs(task_id)
            if run.plan_id == second_graph.plan.id
        )
        assert second_run.attempt == 1
        assert second_run.node_id != first_graph.nodes[0].id
        assert service.store.verify_event_chain() is True
    finally:
        service.store.close()


def test_manual_failure_gate_survives_restart_and_controls_descendants(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    service = OrchestrationService(FakeManager(), data_dir, executor=object())
    task_id = _create_running_plan(
        service,
        {
            "nodes": [
                {
                    "key": "fails",
                    "failure_policy": "manual",
                    "priority": 100,
                    "retry_policy": {
                        "max_attempts": 2,
                        "initial_delay_seconds": 0,
                        "jitter": 0,
                    },
                },
                {"key": "independent", "priority": 10},
                {"key": "failure_handler"},
            ],
            "edges": [
                {
                    "from": "fails",
                    "to": "failure_handler",
                    "condition": "failure",
                }
            ],
        },
        key="manual-policy",
    )
    _fail_next(service, "fails")
    service._advance_task(task_id)
    task = service.store.get_task(task_id)
    gates = service.store.list_gates(task_id, statuses=(GateStatus.OPEN,))
    assert task.status is TaskStatus.WAITING_HUMAN
    assert len(gates) == 1 and gates[0].kind is GateKind.RECONCILIATION
    assert gates[0].prompt["actions"] == [
        "retry",
        "continue",
        "skip_dependents",
        "cancel",
    ]
    first_gate_id = gates[0].id
    service.store.close()

    recovered = OrchestrationService(FakeManager(), data_dir, executor=object())
    try:
        assert recovered.store.verify_event_chain() is True
        gate = recovered.store.get_gate(first_gate_id)
        assert gate.status is GateStatus.OPEN
        recovered.resolve_gate(task_id, gate.id, decision="retry")
        recovered._advance_task(task_id)
        latest = _latest(recovered, task_id)
        assert latest["fails"].attempt == 2
        assert latest["fails"].status is RunStatus.QUEUED
        assert latest["independent"].status is RunStatus.QUEUED

        _fail_next(recovered, "fails")
        recovered._advance_task(task_id)
        second_gate = recovered.store.list_gates(
            task_id, statuses=(GateStatus.OPEN,)
        )[0]
        assert second_gate.id != first_gate_id
        assert second_gate.prompt["actions"] == [
            "continue",
            "skip_dependents",
            "cancel",
        ]

        recovered.resolve_gate(task_id, second_gate.id, decision="skip_dependents")
        recovered._advance_task(task_id)
        latest = _latest(recovered, task_id)
        assert latest["failure_handler"].status is RunStatus.SKIPPED
        assert latest["independent"].status is RunStatus.QUEUED
        assert recovered.store.verify_event_chain() is True
    finally:
        recovered.store.close()


def test_manual_continue_releases_normal_failure_edge_scheduling(
    tmp_path: Path,
) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = _create_running_plan(
            service,
            {
                "nodes": [
                    {
                        "key": "fails",
                        "failure_policy": "manual",
                        "priority": 100,
                    },
                    {"key": "independent", "priority": 10},
                    {"key": "failure_handler"},
                ],
                "edges": [
                    {
                        "from": "fails",
                        "to": "failure_handler",
                        "condition": "failure",
                    }
                ],
            },
            key="manual-continue-policy",
        )
        _fail_next(service, "fails")
        service._advance_task(task_id)
        gate = service.store.list_gates(
            task_id, statuses=(GateStatus.OPEN,)
        )[0]

        service.resolve_gate(task_id, gate.id, decision="continue")
        service._advance_task(task_id)

        latest = _latest(service, task_id)
        assert latest["failure_handler"].status is RunStatus.QUEUED
        assert latest["independent"].status is RunStatus.QUEUED
        assert service.store.get_task(task_id).status is TaskStatus.RUNNING
    finally:
        service.store.close()


def test_integrator_must_precede_independent_verification_and_final_evaluation(
    tmp_path: Path,
) -> None:
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task = service.create_task(
            {
                "objective": "Implement, verify, integrate, and evaluate a candidate",
                "domain": "knowledge",
                "acceptance_criteria": ["the integrated result passes verification"],
                "complexity_factors": _low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "implement", "kind": "execute", "agent": "worker"},
                        {"key": "review", "kind": "review", "agent": "reviewer"},
                        {"key": "test", "kind": "test", "agent": "tester"},
                        {
                            "key": "integrate",
                            "kind": "integrate",
                            "agent": "integrator",
                        },
                        {
                            "key": "evaluate",
                            "kind": "evaluate",
                            "agent": "evaluator",
                        },
                    ],
                    "edges": [
                        {"from": "implement", "to": "integrate"},
                        {"from": "integrate", "to": "review"},
                        {"from": "integrate", "to": "test"},
                        {"from": "review", "to": "evaluate"},
                        {"from": "test", "to": "evaluate"},
                    ],
                },
                "auto_start": False,
            }
        )
        graph = service._ensure_plan(service.store.get_task(task["id"]))
        assert [node.key for node in graph.nodes] == [
            "implement",
            "review",
            "test",
            "integrate",
            "evaluate",
        ]
    finally:
        service.store.close()
