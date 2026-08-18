from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from coworker.orchestration.errors import LeaseConflict
from coworker.orchestration.executor import ExecutionOutcome
from coworker.orchestration.models import (
    EffectSafety,
    EvidenceKind,
    GateKind,
    GateStatus,
    NodeSpec,
    PlanSpec,
    RunStatus,
    TaskSpec,
    TaskStatus,
)
from coworker.orchestration.runtime import (
    PermissionSet,
    RuntimeBudget,
    RuntimeKind,
    RuntimeManager,
    RuntimeSpec,
    RuntimeStatus,
)
from coworker.orchestration.service import OrchestrationService
from coworker.orchestration.store import OrchestrationStore


class FakeManager:
    def __init__(self, workspace: Path | None = None):
        self.default_workspace = str(workspace) if workspace else None
        self.model = "gpt-5.6-sol"

    def _provider_configured(self, _provider: str) -> bool:
        return True

    def get_settings(self):
        return {
            "models": [self.model],
            "model_labels": {},
            "model_context_windows": {self.model: 400_000},
        }

    async def broadcast_event(self, _event):
        return None


async def wait_until(predicate, timeout: float = 10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(0.03)
    raise AssertionError("condition not reached")


class StructuredOutcomeExecutor:
    def __init__(self, output: dict):
        self.output = output

    async def execute(self, context):
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or "structured-test-session",
            output=self.output,
            usage={"model_calls": 1, "tokens": 100},
        )


class WorkspaceCapturingExecutor(StructuredOutcomeExecutor):
    def __init__(self, output: dict):
        super().__init__(output)
        self.workspaces: list[Path | None] = []

    async def execute(self, context):
        self.workspaces.append(context.workspace)
        return await super().execute(context)


class WorkProductHandoffExecutor:
    def __init__(self) -> None:
        self.reviewer_products: list[dict] = []

    async def execute(self, context):
        if context.node.key == "execute":
            completion = {
                "summary": "The candidate report is complete.",
                "criterion_results": {"criterion-1": "pass"},
                "work_products": [
                    {
                        "deliverable_id": "deliverable-1",
                        "kind": "artifact",
                        "title": "Candidate report",
                        "summary": "Immutable report body for isolated review.",
                    }
                ],
                "remaining_risks": [],
            }
            return ExecutionOutcome(
                status="succeeded",
                session_id=context.claim.run.session_id or "execute-session",
                output={"summary": completion["summary"], "handoff_result": completion},
            )
        assert context.execution_envelope is not None
        self.reviewer_products = list(
            context.execution_envelope.context_manifest.get("work_products") or ()
        )
        criteria = {
            criterion: "pass" for criterion in context.task.acceptance_criteria
        }
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or "review-session",
            output={
                "summary": "The published Work Product was reviewed.",
                "verdict": {
                    "status": "pass",
                    "summary": "The published Work Product was reviewed.",
                    "criteria": criteria,
                },
            },
        )


def _structured_result_task_request(*, idempotency_key: str) -> dict:
    return {
        "idempotency_key": idempotency_key,
        "title": "Read-only structured result",
        "objective": "Produce a read-only result",
        "domain": "knowledge",
        "read_only": True,
        "acceptance_criteria": ["read only"],
        "complexity_factors": {
            "scope": 0,
            "uncertainty": 0,
            "dependencies": 0,
            "side_effects": 0,
            "parallelism": 0,
            "verification": 0,
        },
        "plan": {
            "nodes": [
                {
                    "key": "understand",
                    "kind": "agent",
                    "agent": "orchestrator",
                }
            ],
            "edges": [],
        },
        "brief": {
            "title": "Read-only structured result",
            "objective": "Produce a read-only result",
            "scope": {
                "whole_task": True,
                "reason": "The objective is the complete bounded scope.",
            },
            "instructions": ["Return a concise result without changing state."],
            "constraints": ["Do not modify files."],
            "acceptance_criteria": [
                {
                    "id": "criterion-1",
                    "text": "read only",
                    "required": True,
                }
            ],
            "deliverables": [
                {
                    "id": "deliverable-1",
                    "kind": "artifact",
                    "title": "Read-only analysis report",
                    "required": True,
                }
            ],
            "result_contract": {"schema_id": "analysis_result_v1"},
        },
    }


@pytest.mark.asyncio
async def test_read_only_workspace_skips_snapshot_and_reports_pre_agent_progress(
    tmp_path,
    monkeypatch,
):
    workspace = tmp_path / "large-read-only-workspace"
    workspace.mkdir()
    (workspace / "dbt_project.yml").write_text("name: example\n", encoding="utf-8")
    provider_result = {
        "summary": "The workspace was inspected without mutation.",
        "status": "pass",
        "criteria": {"read only": "pass"},
        "files_touched": [],
        "checks": ["Read-only workspace mounted."],
        "remaining_risks": [],
    }
    executor = WorkspaceCapturingExecutor(
        {"summary": provider_result["summary"], "structured_result": provider_result}
    )
    service = OrchestrationService(
        FakeManager(workspace),
        tmp_path / "read-only-direct-workspace",
        executor=executor,
        poll_seconds=0.03,
    )
    await service.start()
    try:
        def unexpected_snapshot(*_args, **_kwargs):
            raise AssertionError("read-only execution must not copy the workspace")

        monkeypatch.setattr(service.workspaces, "prepare", unexpected_snapshot)
        request = _structured_result_task_request(
            idempotency_key="read-only-direct-workspace"
        )
        request.update({"domain": "code", "workspace": str(workspace)})
        task_id = service.create_task(request)["id"]

        run = await wait_until(
            lambda: next(
                (
                    item
                    for item in service.store.list_runs(task_id)
                    if item.status in {RunStatus.SUCCEEDED, RunStatus.FAILED}
                ),
                None,
            )
        )

        assert run.status is RunStatus.SUCCEEDED
        assert executor.workspaces == [workspace.resolve()]
        activity = service.store.list_run_activity(task_id, run.id)
        assert [item.title for item in activity[:3]] == [
            "Read-only workspace ready",
            "Preparing execution subject",
            "Execution subject ready",
        ]
        assert activity[0].detail == {"isolation": "read_only_source"}
        assert not (
            service.workspaces.base_dir
            / "snapshots"
            / service._task_snapshot_id(task_id)
        ).exists()
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_subscription_structured_result_is_not_mistaken_for_complete_task(
    tmp_path,
):
    provider_result = {
        "summary": "The read-only boundary is understood.",
        "status": "pass",
        "criteria": {"read only": "pass"},
        "files_touched": [],
        "checks": ["No mutation tools were used."],
        "remaining_risks": [],
    }
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "subscription-structured",
        executor=StructuredOutcomeExecutor(
            {"summary": provider_result["summary"], "structured_result": provider_result}
        ),
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            _structured_result_task_request(
                idempotency_key="subscription-structured-result"
            )
        )["id"]

        run = await wait_until(
            lambda: next(
                (
                    item
                    for item in service.store.list_runs(task_id)
                    if item.status in {RunStatus.SUCCEEDED, RunStatus.FAILED}
                ),
                None,
            )
        )

        assert run.status is RunStatus.SUCCEEDED
        assert run.output["structured_result"] == provider_result
        assert "result" not in run.output
        assert service.store.list_work_products(task_id) == ()
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_explicit_handoff_result_still_uses_atomic_structured_settlement(
    tmp_path,
):
    completion = {
        "summary": "The explicit handoff is complete.",
        "criterion_results": {"criterion-1": "pass"},
        "work_products": [
            {
                "deliverable_id": "deliverable-1",
                "kind": "artifact",
                "title": "Read-only analysis report",
                "summary": "A bounded immutable result.",
            }
        ],
        "remaining_risks": [],
    }
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "explicit-handoff",
        executor=StructuredOutcomeExecutor(
            {
                "summary": completion["summary"],
                "structured_result": completion,
                "handoff_result": completion,
            }
        ),
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            _structured_result_task_request(idempotency_key="explicit-handoff-result")
        )["id"]

        run = await wait_until(
            lambda: next(
                (
                    item
                    for item in service.store.list_runs(task_id)
                    if item.status in {RunStatus.SUCCEEDED, RunStatus.FAILED}
                ),
                None,
            )
        )

        assert run.status is RunStatus.SUCCEEDED
        assert run.output["result"]["criterion_results"] == {
            "criterion-1": "pass"
        }
        products = service.store.list_work_products(task_id)
        assert len(products) == 1
        assert products[0].metadata["deliverable_id"] == "deliverable-1"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_downstream_verifier_envelope_contains_upstream_work_product(tmp_path):
    executor = WorkProductHandoffExecutor()
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "downstream-work-product",
        executor=executor,
        poll_seconds=0.03,
    )
    await service.start()
    try:
        request = _structured_result_task_request(
            idempotency_key="downstream-work-product"
        )
        request["plan"] = {
            "nodes": [
                {"key": "execute", "kind": "execute", "agent": "worker"},
                {"key": "review", "kind": "review", "agent": "reviewer"},
            ],
            "edges": [{"from": "execute", "to": "review"}],
        }
        task_id = service.create_task(request)["id"]

        await wait_until(
            lambda: next(
                (
                    item
                    for item in service.store.list_runs(task_id)
                    if item.node_key == "review" and item.status is RunStatus.SUCCEEDED
                ),
                None,
            )
        )

        assert len(executor.reviewer_products) == 1
        assert executor.reviewer_products[0]["title"] == "Candidate report"
        assert (
            executor.reviewer_products[0]["summary"]
            == "Immutable report body for isolated review."
        )
    finally:
        await service.stop()


def _start_claimed_parent(
    service: OrchestrationService,
    *,
    owner: str,
    lease_seconds: int = 60,
    start_run: bool = True,
):
    task_id = service.create_task(
        {
            "idempotency_key": f"claimed-parent:{owner}",
            "objective": f"delegate child work for {owner}",
            "domain": "knowledge",
            "acceptance_criteria": ["delegation remains fenced"],
            "complexity_factors": {
                "scope": 1,
                "uncertainty": 1,
                "dependencies": 0,
                "side_effects": 0,
                "parallelism": 0,
                "verification": 1,
            },
            "plan": {
                "nodes": [{"key": "execute", "agent": "worker"}],
                "edges": [],
            },
            "auto_start": False,
        }
    )["id"]
    service.submit_task(task_id)
    service._advance_task(task_id)
    now = datetime.now(timezone.utc)
    claim = service.store.claim_next_run(
        owner,
        lease_seconds=lease_seconds,
        now=now,
        command_id=f"claim-{owner}",
    )
    assert claim is not None and claim.run.task_id == task_id
    if start_run:
        service.store.start_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            command_id=f"start-{owner}",
        )
    return task_id, claim


def test_task_containers_do_not_consume_agent_depth_or_concurrency():
    manager = RuntimeManager()
    root = manager.add_root(
        RuntimeSpec(
            "task:root",
            "orchestrator",
            "root task",
            budget=RuntimeBudget(20, 20, 20_000, 2_000),
            permissions=PermissionSet(can_delegate=True),
            kind=RuntimeKind.TASK,
        )
    )
    manager.start(root.runtime_id)
    run = manager.spawn_child(
        root.runtime_id,
        RuntimeSpec(
            "run:root",
            "worker",
            "root agent",
            budget=RuntimeBudget(10, 10, 10_000, 1_000),
            permissions=PermissionSet(can_delegate=True),
        ),
    )
    manager.start(run.runtime_id)
    child_task = manager.spawn_child(
        run.runtime_id,
        RuntimeSpec(
            "task:child",
            "worker",
            "child task",
            budget=RuntimeBudget(5, 5, 5_000, 500),
            permissions=PermissionSet(can_delegate=True),
            kind=RuntimeKind.TASK,
        ),
    )
    manager.start(child_task.runtime_id)
    child_run = manager.spawn_child(
        child_task.runtime_id,
        RuntimeSpec(
            "run:child",
            "tester",
            "child agent",
            budget=RuntimeBudget(2, 2, 2_000, 200),
        ),
    )
    manager.start(child_run.runtime_id)

    assert (run.depth, child_task.depth, child_run.depth) == (0, 0, 1)
    assert manager.active_count == 2
    assert manager.work_unit_count == 2
    manager.suspend(run.runtime_id)
    assert manager.active_count == 1
    manager.resume(run.runtime_id)
    assert manager.get(run.runtime_id).status is RuntimeStatus.RUNNING


def test_child_delegation_is_stable_across_lost_attempts_and_keeps_logical_ownership(
    tmp_path,
):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = service.create_task(
            {
                "objective": "delegate idempotent child work",
                "domain": "knowledge",
                "acceptance_criteria": ["the child is created once"],
                "complexity_factors": {
                    "scope": 1,
                    "uncertainty": 1,
                    "risk": 1,
                    "dependencies": 0,
                    "verification": 0,
                },
                "plan": {
                    "nodes": [
                        {
                            "key": "execute",
                            "agent": "worker",
                            "priority": 10,
                            "retry_policy": {
                                "max_attempts": 2,
                                "initial_delay_seconds": 0,
                            },
                        }
                    ],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)

        now = datetime.now(timezone.utc)
        claim = service.store.claim_next_run(
            "attempt-one", lease_seconds=1, now=now, command_id="claim-attempt-one"
        )
        assert claim is not None and claim.run.task_id == task_id
        service.store.start_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            command_id="start-attempt-one",
        )
        base = {
            "task_id": task_id,
            "run_id": claim.run.id,
            "node_id": claim.run.node_id,
            "lease_token": claim.lease.token,
            "fencing_token": claim.lease.fencing_token,
            "role": "worker",
            "objective": "stable delegated work",
        }
        first = service._spawn_child(base)
        assert first["ok"] is True
        child_id = str(first["task_id"])

        assert service.store.reap_expired_leases(
            now=now + timedelta(seconds=2), command_id="lose-attempt-one"
        ) == 1
        assert service.store.get_run(claim.run.id).status is RunStatus.LOST
        parent = service.store.get_task(task_id)
        graph = service.store.get_plan(claim.run.plan_id)
        # A vanished worker cannot attest that its process tree was reaped. The
        # logical child identity remains reusable, but only after the normal
        # reconciliation path records an explicit human retry decision.
        assert service._retry_failed(parent, graph, explicit=False) is False
        assert service._retry_failed(parent, graph, explicit=True) is True
        retry = max(
            service.store.list_runs(task_id), key=lambda run: (run.attempt, run.created_at)
        )
        assert retry.attempt == 2 and retry.id != claim.run.id
        retry_claim = service.store.claim_next_run(
            "attempt-two",
            now=now + timedelta(seconds=3),
            command_id="claim-attempt-two",
        )
        assert retry_claim is not None and retry_claim.run.id == retry.id
        service.store.start_run(
            retry.id,
            retry_claim.lease.token,
            retry_claim.lease.fencing_token,
            command_id="start-attempt-two",
        )

        # The stable logical key survives an attempt change, while authority is
        # always supplied by the currently leased attempt.
        retry_base = {
            **base,
            "run_id": retry.id,
            "lease_token": retry_claim.lease.token,
            "fencing_token": retry_claim.lease.fencing_token,
        }
        replay = service._spawn_child(retry_base)
        assert replay["ok"] is True and replay["task_id"] == child_id
        assert replay["replayed"] is True
        assert replay["parent_run_id"] == claim.run.id
        child = service.store.get_task(child_id)
        legacy_brief = service.store.get_active_brief(child_id)
        assert legacy_brief.status.value == "published"
        assert legacy_brief.objective == "stable delegated work"
        assert any(
            event.event_type == "legacy_delegation_used"
            for event in service.store.list_events(task_id=child_id)
        )
        assert "upstream_context" not in child.input
        runtime_meta = dict(child.input["_runtime"])
        assert runtime_meta["parent_run_id"] == claim.run.id
        assert runtime_meta["parent_plan_id"] == retry.plan_id

        # A retry may operate the child only because it owns the same plan node;
        # ownership is not relaxed to every run under the parent task.
        lookup = service._lookup_child(
            {
                "task_id": child_id,
                "parent_task_id": task_id,
                "parent_run_id": retry.id,
                "lease_token": retry_claim.lease.token,
                "fencing_token": retry_claim.lease.fencing_token,
            }
        )
        assert lookup["ok"] is True and lookup["parent_run_id"] == claim.run.id
        delegations = [
            evidence
            for evidence in service.store.list_evidence(task_id)
            if evidence.payload.get("action") == "child_delegation_replayed"
        ]
        assert len(delegations) == 1
        assert delegations[0].run_id == retry.id
        assert delegations[0].payload["origin_parent_run_id"] == claim.run.id
        assert len(
            [
                task
                for task in service.store.list_all_tasks()
                if task.parent_task_id == task_id
            ]
        ) == 1

        # Explicit operation ids are aliases and reject semantic key reuse.
        explicit = service._spawn_child(
            {
                **retry_base,
                "objective": "second logical child",
                "operation_id": "child-slot-two",
            }
        )
        assert explicit["ok"] is True
        alias_replay = service._spawn_child(
            {
                **retry_base,
                "objective": "second logical child",
                "child_key": "child-slot-two",
            }
        )
        assert alias_replay["task_id"] == explicit["task_id"]
        conflict = service._spawn_child(
            {
                **retry_base,
                "objective": "different work under the same operation",
                "operation_id": "child-slot-two",
            }
        )
        assert conflict == {
            "ok": False,
            "error": "child operation key was reused with different input",
        }
        gate = service.store.prepare_child_wait(
            retry.id,
            retry_claim.lease.token,
            retry_claim.lease.fencing_token,
            child_task_id=child_id,
            source_key=f"{retry.id}:child_wait:{child_id}",
        )
        assert gate.run_id == retry.id and gate.prompt["child_task_id"] == child_id
    finally:
        service.store.close()


def test_child_controls_reject_missing_lease_capability(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id, claim = _start_claimed_parent(service, owner="capability-parent")
        spawn_request = {
            "task_id": task_id,
            "run_id": claim.run.id,
            "node_id": claim.run.node_id,
            "lease_token": claim.lease.token,
            "fencing_token": claim.lease.fencing_token,
            "role": "worker",
            "objective": "capability-protected child",
            "operation_id": "capability-protected-child",
        }

        for missing in ("lease_token", "fencing_token"):
            unauthorized_spawn = dict(spawn_request)
            unauthorized_spawn.pop(missing)
            rejected = service._spawn_child(unauthorized_spawn)
            assert rejected["ok"] is False
            assert "lease/fence" in str(rejected["error"])

        spawned = service._spawn_child(spawn_request)
        assert spawned["ok"] is True
        child_id = str(spawned["task_id"])
        child_before = service.store.get_task(child_id)
        control_request = {
            "task_id": child_id,
            "parent_task_id": task_id,
            "parent_run_id": claim.run.id,
            "lease_token": claim.lease.token,
            "fencing_token": claim.lease.fencing_token,
        }
        for operation in (service._lookup_child, service._cancel_child):
            for missing in ("lease_token", "fencing_token"):
                unauthorized_control = dict(control_request)
                unauthorized_control.pop(missing)
                rejected = operation(unauthorized_control)
                assert rejected["ok"] is False
                assert "lease/fence" in str(rejected["error"])

        child_after = service.store.get_task(child_id)
        assert (child_after.status, child_after.version) == (
            child_before.status,
            child_before.version,
        )
        assert [
            task.id
            for task in service.store.list_all_tasks()
            if task.parent_task_id == task_id
        ] == [child_id]
    finally:
        service.store.close()


def test_lost_parent_attempt_cannot_lookup_or_cancel_its_child(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id, claim = _start_claimed_parent(
            service,
            owner="lost-parent",
            lease_seconds=1,
        )
        spawned = service._spawn_child(
            {
                "task_id": task_id,
                "run_id": claim.run.id,
                "node_id": claim.run.node_id,
                "lease_token": claim.lease.token,
                "fencing_token": claim.lease.fencing_token,
                "role": "worker",
                "objective": "child must outlive stale authority",
                "operation_id": "lost-parent-child",
            }
        )
        assert spawned["ok"] is True
        child_id = str(spawned["task_id"])
        child_before = service.store.get_task(child_id)

        assert service.store.reap_expired_leases(
            now=claim.lease.expires_at + timedelta(seconds=1),
            command_id="reap-lost-parent-capability",
        ) == 1
        assert service.store.get_run(claim.run.id).status is RunStatus.LOST
        stale_control = {
            "task_id": child_id,
            "parent_task_id": task_id,
            "parent_run_id": claim.run.id,
            "lease_token": claim.lease.token,
            "fencing_token": claim.lease.fencing_token,
        }

        assert service._lookup_child(stale_control)["ok"] is False
        assert service._cancel_child(stale_control)["ok"] is False
        child_after = service.store.get_task(child_id)
        assert (child_after.status, child_after.version) == (
            child_before.status,
            child_before.version,
        )
    finally:
        service.store.close()


def test_single_child_profile_preserves_parent_settlement_headroom(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        draft = service.catalog.clone_profile(
            "worker",
            "single-child-worker",
            overrides={"display_name": "Single child worker", "max_children": 1},
        )
        service.catalog.publish_profile(
            "single-child-worker", expected_etag=draft["draft"]["etag"]
        )
        task_id = service.create_task(
            {
                "objective": "delegate once and settle parent usage",
                "domain": "knowledge",
                "acceptance_criteria": ["parent usage remains chargeable"],
                "complexity_factors": {
                    "scope": 1,
                    "uncertainty": 1,
                    "risk": 1,
                    "dependencies": 0,
                    "verification": 0,
                },
                "budget": {
                    "model_calls": 20,
                    "tool_calls": 20,
                    "tokens": 2_000,
                    "wall_seconds": 200,
                },
                "plan": {
                    "nodes": [{"key": "execute", "agent": "single-child-worker"}],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)
        claim = service.store.claim_next_run("single-child-parent")
        assert claim is not None and claim.run.task_id == task_id
        service.store.start_run(
            claim.run.id, claim.lease.token, claim.lease.fencing_token
        )
        spawned = service._spawn_child(
            {
                "task_id": task_id,
                "run_id": claim.run.id,
                "node_id": claim.run.node_id,
                "lease_token": claim.lease.token,
                "fencing_token": claim.lease.fencing_token,
                "role": "worker",
                "objective": "bounded child work",
            }
        )
        assert spawned["ok"] is True

        # Persisting the parent outcome charges usage during runtime recovery while
        # its child is still live. The child reservation must leave room for it.
        service.store.complete_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            output={
                "usage": {
                    "model_calls": 1,
                    "tool_calls": 1,
                    "tokens": 1,
                    "wall_seconds": 1,
                }
            },
        )
        runtime = service._runtime_for_task(task_id, rebuild=True)
        parent_runtime = runtime.get(service._run_runtime_id(claim.run.id))
        assert parent_runtime.direct_usage == RuntimeBudget(1, 1, 1, 1)
        assert parent_runtime.remaining_budget.model_calls > 0
        assert parent_runtime.remaining_budget.tool_calls > 0
        assert parent_runtime.remaining_budget.tokens > 0
        assert parent_runtime.remaining_budget.wall_seconds > 0
    finally:
        service.store.close()


def test_cancel_task_runs_closes_queued_and_child_waiting_attempts(tmp_path):
    store = OrchestrationStore(tmp_path / "orch.db")
    try:
        parent = store.create_task(TaskSpec("parent", "parent"))
        parent = store.transition_task_status(
            parent.id, TaskStatus.QUEUED, expected_version=parent.version
        )
        parent = store.transition_task_status(
            parent.id, TaskStatus.RUNNING, expected_version=parent.version
        )
        graph = store.create_plan_revision(
            parent.id,
            PlanSpec(nodes=(NodeSpec("wait", priority=1), NodeSpec("queued"))),
            expected_task_version=parent.version,
            created_by="test",
        )
        waiting = store.enqueue_run(parent.id, "wait")
        queued = store.enqueue_run(parent.id, "queued")
        claim = store.claim_next_run("worker")
        assert claim and claim.run.id == waiting.id
        store.start_run(waiting.id, claim.lease.token, claim.lease.fencing_token)
        child = store.create_task(
            TaskSpec(
                "child",
                "child",
                parent_task_id=parent.id,
                input={"_runtime": {"parent_run_id": waiting.id}},
            )
        )
        gate = store.prepare_child_wait(
            waiting.id,
            claim.lease.token,
            claim.lease.fencing_token,
            child_task_id=child.id,
            source_key="wait-child",
        )
        active = store.get_run(waiting.id)
        gate = store.commit_prepared_gate(
            waiting.id,
            claim.lease.token,
            claim.lease.fencing_token,
            gate_id=gate.id,
            checkpoint={
                "schema_version": 1,
                "run_id": active.id,
                "attempt": active.attempt,
                "fencing_token": claim.lease.fencing_token,
                "session_id": active.session_id,
                "gate_id": gate.id,
                "blob_uri": "sha256:" + "a" * 64,
                "blob_sha256": "a" * 64,
                "pending_tool_call_ids": ["wait-child-call"],
                "recovery_disposition": "pending_tools",
            },
        )

        assert store.cancel_task_runs(parent.id) == 2
        assert store.get_run(waiting.id).status is RunStatus.CANCELED
        assert store.get_run(queued.id).status is RunStatus.CANCELED
        assert store.get_gate(gate.id).status is GateStatus.CANCELED
    finally:
        store.close()


class LeaseLossExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.canceled = asyncio.Event()
        self.interrupted: list[str] = []

    async def execute(self, _context):
        self.started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.canceled.set()
            raise

    def interrupt(self, run_id: str) -> None:
        self.interrupted.append(run_id)


class CountingExecutor:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, _context):
        self.calls += 1
        raise AssertionError("required-context preflight must precede executor dispatch")


@pytest.mark.asyncio
async def test_required_context_preflight_reconciles_without_executor_call(tmp_path):
    executor = CountingExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "required-context-data", executor=executor
    )
    try:
        missing = "sha256:" + "f" * 64
        task_id = service.create_task(
            {
                "idempotency_key": "required-context-preflight",
                "objective": "Use required external evidence",
                "domain": "knowledge",
                "brief": {
                    "title": "Required context preflight",
                    "objective": "Use required external evidence",
                    "scope": {"whole_task": True, "reason": "bounded test"},
                    "instructions": ["Verify required context before execution"],
                    "acceptance_criteria": [
                        {
                            "id": "AC-01",
                            "text": "Missing evidence prevents dispatch",
                            "required": True,
                        }
                    ],
                    "deliverables": [
                        {
                            "id": "DEL-01",
                            "kind": "test_result",
                            "title": "Preflight result",
                            "required": True,
                        }
                    ],
                    "result_contract": {"schema_id": "preflight_v1"},
                },
                "context_refs": [
                    {
                        "requirement": "required",
                        "ref_type": "artifact",
                        "display_name": "Required missing artifact",
                        "selection_reason": "Execution depends on this evidence",
                        "locator": {"blob_uri": missing},
                        "content_hash": missing,
                        "delivery_mode": "on_demand",
                    }
                ],
                "plan": {
                    "nodes": [{"key": "execute", "agent": "worker"}],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)
        claim = service.store.claim_next_run(
            "required-context-worker", command_id="claim-required-context"
        )
        assert claim is not None and claim.run.task_id == task_id

        await service._execute_claim(claim)

        assert executor.calls == 0
        assert service.store.get_run(claim.run.id).status is RunStatus.FAILED
        assert (
            service.store.get_run(claim.run.id).error_kind
            == "required_context_unavailable"
        )
        assert (
            service.store.get_task(task_id).status
            is TaskStatus.NEEDS_RECONCILIATION
        )
        verification_events = [
            event
            for event in service.store.list_events(task_id=task_id)
            if event.event_type == "context_ref_verified"
        ]
        assert len(verification_events) == 1
        assert verification_events[0].payload["available"] is False
    finally:
        service.store.close()


@pytest.mark.asyncio
async def test_heartbeat_lease_conflict_interrupts_executor_and_quarantines_attempt(
    tmp_path,
    monkeypatch,
):
    executor = LeaseLossExecutor()
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=executor)
    try:
        _task_id, claim = _start_claimed_parent(
            service,
            owner="heartbeat-loss",
            lease_seconds=1,
            start_run=False,
        )

        async def lose_lease(active_claim):
            await executor.started.wait()
            assert service.store.reap_expired_leases(
                now=active_claim.lease.expires_at + timedelta(seconds=1),
                command_id="reap-during-heartbeat",
            ) == 1
            with pytest.raises(LeaseConflict) as lost:
                service.store.heartbeat(
                    active_claim.run.id,
                    active_claim.lease.token,
                    active_claim.lease.fencing_token,
                    command_id="heartbeat-after-reap",
                )
            raise lost.value

        monkeypatch.setattr(service, "_heartbeat", lose_lease)
        await asyncio.wait_for(service._execute_claim(claim), timeout=3)

        assert executor.canceled.is_set()
        assert claim.run.id in executor.interrupted
        assert service.store.get_run(claim.run.id).status is RunStatus.LOST
    finally:
        service.store.close()


class ThreadAwareBlockingExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.canceled = asyncio.Event()
        self.run_id: str | None = None
        self.cancellation_thread: int | None = None
        self.interrupted: list[str] = []

    async def execute(self, context):
        self.run_id = context.claim.run.id
        self.started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancellation_thread = threading.get_ident()
            self.canceled.set()
            raise

    def interrupt(self, run_id: str) -> None:
        self.interrupted.append(run_id)


class PauseAwareBlockingExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.run_id: str | None = None
        self.interrupted: list[str] = []

    async def execute(self, context):
        self.loop = asyncio.get_running_loop()
        self.run_id = context.claim.run.id
        self.started.set()
        await self.release.wait()
        return ExecutionOutcome(
            status="failed",
            session_id=context.claim.run.session_id or "pause-test",
            error_kind="agent_interrupted",
            error_message="paused by operator",
            usage={"model_calls": 1, "wall_seconds": 1},
        )

    def interrupt(self, run_id: str) -> None:
        self.interrupted.append(run_id)
        assert self.loop is not None
        self.loop.call_soon_threadsafe(self.release.set)


@pytest.mark.asyncio
async def test_pause_interrupts_inflight_agent_without_canceling_task(tmp_path):
    executor = PauseAwareBlockingExecutor()
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "pause-data",
        executor=executor,
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "pause a running attempt and stop its process",
                "domain": "knowledge",
                "acceptance_criteria": ["the Agent stops while the task stays resumable"],
                "complexity_factors": {
                    "scope": 1,
                    "uncertainty": 1,
                    "dependencies": 0,
                    "side_effects": 0,
                    "parallelism": 0,
                    "verification": 1,
                },
                "plan": {
                    "nodes": [{"key": "execute", "agent": "worker"}],
                    "edges": [],
                },
            }
        )["id"]
        await asyncio.wait_for(executor.started.wait(), timeout=5)

        paused = await asyncio.to_thread(service.pause_task, task_id)

        assert paused.status is TaskStatus.PAUSED
        await wait_until(
            lambda: service.store.get_run(executor.run_id or "").status
            is RunStatus.FAILED,
            timeout=3,
        )
        assert executor.interrupted == [executor.run_id]
        assert service.store.get_task(task_id).status is TaskStatus.PAUSED
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_cancel_task_from_worker_thread_cancels_job_on_event_loop(tmp_path):
    executor = ThreadAwareBlockingExecutor()
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "data",
        executor=executor,
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "cancel a running attempt from an API worker thread",
                "domain": "knowledge",
                "acceptance_criteria": ["the attempt is canceled safely"],
                "complexity_factors": {
                    "scope": 1,
                    "uncertainty": 1,
                    "dependencies": 0,
                    "side_effects": 0,
                    "parallelism": 0,
                    "verification": 1,
                },
                "plan": {
                    "nodes": [{"key": "execute", "agent": "worker"}],
                    "edges": [],
                },
            }
        )["id"]
        await asyncio.wait_for(executor.started.wait(), timeout=5)
        event_loop_thread = threading.get_ident()

        def cancel_from_worker_thread():
            return threading.get_ident(), service.cancel_task(task_id)

        caller_thread, requested = await asyncio.to_thread(cancel_from_worker_thread)
        assert caller_thread != event_loop_thread
        assert requested.status in {TaskStatus.CANCELING, TaskStatus.CANCELED}
        await asyncio.wait_for(executor.canceled.wait(), timeout=3)
        await wait_until(
            lambda: service.store.get_task(task_id).status is TaskStatus.CANCELED,
            timeout=5,
        )

        assert executor.cancellation_thread == event_loop_thread
        assert executor.run_id in executor.interrupted
    finally:
        await service.stop()


def test_usage_segments_default_missing_counters_to_zero_and_wide_graph_rebuilds(
    tmp_path,
):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task = service.store.create_task(TaskSpec("usage", "usage"))
        task = service.store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = service.store.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        graph = service.store.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec("one"),)),
            expected_task_version=task.version,
            created_by="test",
        )
        run = service.store.enqueue_run(task.id, "one")
        service.store.add_evidence(
            task.id,
            kind="metric",
            payload={
                "runtime_usage_segment": True,
                "usage": {"model_calls": 1},
            },
            created_by="test",
            plan_id=graph.plan.id,
            node_id=graph.nodes[0].id,
            run_id=run.id,
        )
        assert service._usage_for_run(run) == RuntimeBudget(model_calls=1)

        plan = {
            "nodes": [
                {"key": f"root-{index}", "agent": "worker"}
                for index in range(10)
            ],
            "edges": [],
        }
        wide_id = service.create_task(
            {
                "objective": "wide graph",
                "domain": "knowledge",
                "acceptance_criteria": ["done"],
                "budget": {"model_calls": 20},
                "complexity_factors": {
                    "scope": 1,
                    "uncertainty": 1,
                    "dependencies": 0,
                    "side_effects": 0,
                    "parallelism": 0,
                    "verification": 1,
                },
                "plan": plan,
                "auto_start": False,
            }
        )["id"]
        service.submit_task(wide_id)
        service._advance_task(wide_id)
        runtime = service._runtime_for_task(wide_id)
        assert runtime.work_unit_count == 10
        assert len(service.store.list_runs(wide_id)) == 10
    finally:
        service.store.close()


class DelegatingExecutor:
    def __init__(self):
        self.service: OrchestrationService | None = None
        self.release_child = asyncio.Event()
        self.spawned: dict | None = None

    async def execute(self, context):
        assert self.service is not None
        if context.task.parent_task_id and context.node.key == "execute":
            await self.release_child.wait()
        elif not context.task.parent_task_id and context.node.key == "execute":
            self.spawned = dict(
                self.service._spawn_child(
                    {
                        "task_id": context.task.id,
                        "run_id": context.claim.run.id,
                        "node_id": context.node.id,
                        "lease_token": context.claim.lease.token,
                        "fencing_token": context.claim.lease.fencing_token,
                        "role": "worker",
                        "objective": "bounded child work",
                    }
                )
            )
            assert self.spawned["ok"] is True
        output = {"summary": "ok", "usage": {"model_calls": 1}}
        if context.profile.role.value in {"reviewer", "tester", "evaluator", "scorer"}:
            output["verdict"] = {
                "status": "pass",
                "criteria": {
                    criterion: "pass" for criterion in context.task.acceptance_criteria
                },
            }
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or "hidden",
            output=output,
            usage={"model_calls": 1},
        )


@pytest.mark.asyncio
async def test_parent_waits_for_unjoined_child_and_parent_run_is_auditable(tmp_path):
    executor = DelegatingExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "data", executor=executor, poll_seconds=0.03
    )
    executor.service = service
    await service.start()
    try:
        parent_id = service.create_task(
            {
                "objective": "delegate once",
                "domain": "knowledge",
                "acceptance_criteria": ["child is complete"],
            }
        )["id"]
        await wait_until(
            lambda: service.store.get_task(parent_id).status
            is TaskStatus.WAITING_CHILD
        )
        child = next(
            task
            for task in service.store.list_tasks()
            if task.parent_task_id == parent_id
        )
        parent_run_id = str((child.input.get("_runtime") or {})["parent_run_id"])
        assert service.task_detail(parent_id)["children"][0]["parent_run_id"] == parent_run_id
        assert service.task_detail(child.id)["parent_run_id"] == parent_run_id

        executor.release_child.set()
        await wait_until(
            lambda: service.store.get_task(parent_id).status is TaskStatus.COMPLETED,
            timeout=15,
        )
    finally:
        await service.stop()


def test_runtime_rebuild_uses_one_sqlite_snapshot_during_child_settlement(
    tmp_path, monkeypatch
):
    """A projection must never combine an old child row with a newer DAG run.

    The conversion hook pauses immediately after the task-tree SELECT. A second
    store then completes the child and starts the dependent run. Separate read
    connections would observe that impossible combination and reject the durable
    dependent run; the explicit snapshot must see either side of the change.
    """

    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    writer = OrchestrationStore(service.store.path)
    projection_thread: threading.Thread | None = None
    release_snapshot = threading.Event()
    try:
        parent_id = service.create_task(
            {
                "objective": "project a parent and delegated child consistently",
                "domain": "knowledge",
                "acceptance_criteria": ["the child settles before dependent work"],
                "complexity_factors": {
                    "scope": 1,
                    "uncertainty": 1,
                    "dependencies": 1,
                    "side_effects": 0,
                    "parallelism": 0,
                    "verification": 1,
                },
                "plan": {
                    "nodes": [
                        {"key": "execute", "agent": "worker"},
                        {"key": "after", "agent": "worker"},
                    ],
                    "edges": [
                        {
                            "from": "execute",
                            "to": "after",
                            "condition": "success",
                            "required": True,
                        }
                    ],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(parent_id)
        service._advance_task(parent_id)
        first = service.store.claim_next_run("snapshot-parent")
        assert first is not None and first.run.node_key == "execute"
        service.store.start_run(
            first.run.id, first.lease.token, first.lease.fencing_token
        )
        service.store.complete_run(
            first.run.id,
            first.lease.token,
            first.lease.fencing_token,
            output={"summary": "parent returned after delegation"},
        )
        child = service.store.create_task(
            TaskSpec(
                "snapshot-child",
                "finish delegated work",
                budget={
                    "model_calls": 1,
                    "tool_calls": 1,
                    "tokens": 10,
                    "wall_seconds": 10,
                },
                input={"_runtime": {"parent_run_id": first.run.id}},
                parent_task_id=parent_id,
                parent_node_id=first.run.node_id,
            )
        )
        child = service.store.transition_task_status(
            child.id, TaskStatus.QUEUED, expected_version=child.version
        )
        child = service.store.transition_task_status(
            child.id, TaskStatus.RUNNING, expected_version=child.version
        )
        parent = service.store.get_task(parent_id)
        assert parent.active_plan_id is not None

        snapshot_started = threading.Event()
        projection_ident: list[int] = []
        original_task_from_row = service.store._task_from_row

        def pause_after_old_tree_read(row):
            record = original_task_from_row(row)
            if (
                projection_ident
                and threading.get_ident() == projection_ident[0]
                and record.id == child.id
                and not snapshot_started.is_set()
            ):
                snapshot_started.set()
                if not release_snapshot.wait(5):
                    raise AssertionError("writer did not release runtime snapshot")
            return record

        monkeypatch.setattr(service.store, "_task_from_row", pause_after_old_tree_read)
        projection: dict[str, RuntimeManager] = {}
        projection_error: list[BaseException] = []

        def rebuild() -> None:
            projection_ident.append(threading.get_ident())
            try:
                projection["before"] = service._rebuild_runtime_tree(parent_id)
            except BaseException as exc:  # surface thread failures in the test thread
                projection_error.append(exc)

        projection_thread = threading.Thread(target=rebuild, daemon=True)
        projection_thread.start()
        assert snapshot_started.wait(5)

        child = writer.get_task(child.id)
        writer.transition_task_status(
            child.id,
            TaskStatus.COMPLETED,
            expected_version=child.version,
            command_id="snapshot-child-completed",
        )
        dependent = writer.enqueue_run(
            parent_id,
            "after",
            plan_id=parent.active_plan_id,
            command_id="snapshot-dependent-enqueued",
        )
        second = writer.claim_next_run(
            "snapshot-dependent-worker", command_id="snapshot-dependent-claimed"
        )
        assert second is not None and second.run.id == dependent.id
        writer.start_run(
            second.run.id,
            second.lease.token,
            second.lease.fencing_token,
            command_id="snapshot-dependent-started",
        )
        release_snapshot.set()
        projection_thread.join(5)
        assert not projection_thread.is_alive()
        assert projection_error == []

        # The first pinned snapshot predates the dependent run. A fresh snapshot
        # sees both the terminal child and the running dependent, so its enforced
        # predecessor is correctly reconstructed as succeeded.
        with pytest.raises(KeyError):
            projection["before"].get(service._run_runtime_id(second.run.id))
        after = service._rebuild_runtime_tree(parent_id)
        assert (
            after.get(service._run_runtime_id(first.run.id)).status
            is RuntimeStatus.SUCCEEDED
        )
        assert (
            after.get(service._run_runtime_id(second.run.id)).status
            is RuntimeStatus.RUNNING
        )
        assert service.store.get_task(parent_id).status is TaskStatus.RUNNING
    finally:
        release_snapshot.set()
        if projection_thread is not None:
            projection_thread.join(5)
        writer.close()
        service.store.close()


def test_completed_child_wait_does_not_resume_before_checkpoint_is_durable(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        parent_id, claim = _start_claimed_parent(service, owner="checkpoint-race")
        child = service.store.create_task(
            TaskSpec(
                "checkpoint-race-child",
                "already completed child",
                parent_task_id=parent_id,
                parent_node_id=claim.run.node_id,
                input={"_runtime": {"parent_run_id": claim.run.id}},
            )
        )
        child = service.store.transition_task_status(
            child.id, TaskStatus.CANCELED, expected_version=child.version
        )
        gate = service.store.prepare_child_wait(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            child_task_id=child.id,
            source_key="checkpoint-race-wait",
        )

        # Child completion cannot observe or resolve an unpublished preparation.
        service._resume_completed_child_waits()
        assert service.store.get_gate(gate.id).status is GateStatus.PREPARING
        assert service.store.get_run(claim.run.id).status is RunStatus.RUNNING

        checkpoint_payload = {
            "schema_version": 1,
            "run_id": claim.run.id,
            "attempt": claim.run.attempt,
            "fencing_token": claim.lease.fencing_token,
            "session_id": claim.run.session_id or f"__orch__{claim.run.id}",
            "gate_id": gate.id,
            "recovery_disposition": "pending_tools",
            "pending_tool_call_ids": ["join-child-1"],
            "messages": [{"role": "assistant", "content": "waiting"}],
        }
        ref = service.blobs.put_json(checkpoint_payload)
        gate = service.store.commit_prepared_gate(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            gate_id=gate.id,
            checkpoint={
                "schema_version": 1,
                "run_id": claim.run.id,
                "attempt": claim.run.attempt,
                "fencing_token": claim.lease.fencing_token,
                "session_id": claim.run.session_id,
                "gate_id": gate.id,
                "blob_uri": ref.uri,
                "blob_sha256": ref.sha256,
                "pending_tool_call_ids": ["join-child-1"],
                "recovery_disposition": "pending_tools",
            },
        )
        assert gate.status is GateStatus.OPEN
        assert service.store.get_run(claim.run.id).status is RunStatus.WAITING_GATE
        service._resume_completed_child_waits()
        assert service.store.get_gate(gate.id).status is GateStatus.APPROVED
        assert service.store.get_run(claim.run.id).status is RunStatus.QUEUED
        assert service.store.get_task(parent_id).status is TaskStatus.RUNNING
    finally:
        service.store.close()


def test_child_wait_is_reprepared_after_shutdown_release(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "child-reprepare", executor=object())
    try:
        parent_id, first = _start_claimed_parent(
            service, owner="child-reprepare-first"
        )
        child = service.store.create_task(
            TaskSpec(
                "child-reprepare-child",
                "survive parent worker restart",
                parent_task_id=parent_id,
                parent_node_id=first.run.node_id,
                input={"_runtime": {"parent_run_id": first.run.id}},
            )
        )
        source = f"{first.run.id}:child_wait:{child.id}"
        gate = service.store.prepare_child_wait(
            first.run.id,
            first.lease.token,
            first.lease.fencing_token,
            child_task_id=child.id,
            source_key=source,
        )
        service.store.release_run(
            first.run.id,
            first.lease.token,
            first.lease.fencing_token,
            reason="service_shutdown",
        )
        aborted = service.store.get_gate(gate.id)
        assert aborted.status is GateStatus.CANCELED
        assert aborted.published_at is None

        second = service.store.claim_next_run("child-reprepare-second")
        assert second is not None and second.run.id == first.run.id
        service.store.start_run(
            second.run.id, second.lease.token, second.lease.fencing_token
        )
        reprepared = service.store.prepare_child_wait(
            second.run.id,
            second.lease.token,
            second.lease.fencing_token,
            child_task_id=child.id,
            source_key=source,
        )
        assert reprepared.id == gate.id
        assert reprepared.status is GateStatus.PREPARING
        assert reprepared.resolution is None
        assert reprepared.version > aborted.version
    finally:
        service.store.close()


def test_child_result_envelope_is_hash_stable_and_available_after_restart(tmp_path):
    data_dir = tmp_path / "data"
    first = OrchestrationService(FakeManager(), data_dir, executor=object())
    parent_id, claim = _start_claimed_parent(first, owner="result-restart")
    spawned = first._spawn_child(
        {
            "task_id": parent_id,
            "run_id": claim.run.id,
            "node_id": claim.run.node_id,
            "lease_token": claim.lease.token,
            "fencing_token": claim.lease.fencing_token,
            "role": "worker",
            "objective": "produce a durable child result",
            "operation_id": "durable-result",
        }
    )
    assert spawned["ok"] is True
    child = first.store.get_task(str(spawned["task_id"]))
    child = first.store.transition_task_status(
        child.id, TaskStatus.RUNNING, expected_version=child.version
    )
    first.store.add_evidence(
        child.id,
        kind=EvidenceKind.DECISION,
        payload={
            "title": "Final acceptance",
            "accepted": True,
            "subject": {"kind": "knowledge", "sha256": "a" * 64},
            "publication": {"status": "internal"},
        },
        created_by="test-acceptor",
    )
    child = first.store.transition_task_status(
        child.id, TaskStatus.COMPLETED, expected_version=child.version
    )
    result = first._task_result_envelope(child)
    child = first.store.transition_task_status(
        child.id,
        TaskStatus.ARCHIVED,
        expected_version=child.version,
        output={"archived_from": "completed", "result": result, "result_hash": result["result_hash"]},
    )
    first.store.close()

    second = OrchestrationService(FakeManager(), data_dir, executor=object())
    try:
        lookup = second._lookup_child(
            {
                "task_id": child.id,
                "parent_task_id": parent_id,
                "parent_run_id": claim.run.id,
                "lease_token": claim.lease.token,
                "fencing_token": claim.lease.fencing_token,
            }
        )
        assert lookup["ok"] is True
        assert lookup["result"] == result
        assert lookup["result"]["result_hash"] == result["result_hash"]
        consumed = [
            item
            for item in second.store.list_evidence(parent_id)
            if item.payload.get("action") == "child_result_consumed"
        ]
        assert len(consumed) == 1
        assert consumed[0].payload["result_hash"] == result["result_hash"]

        # The envelope hash is an enforced consumption boundary, not decorative
        # metadata. Simulate out-of-band database corruption after restart.
        tampered_output = dict(second.store.get_task(child.id).output or {})
        tampered_result = dict(tampered_output["result"])
        tampered_result["status"] = "failed"
        tampered_output["result"] = tampered_result
        connection = second.store.connect()
        try:
            connection.execute(
                "UPDATE orch_tasks SET output_json = ? WHERE id = ?",
                (json.dumps(tampered_output, sort_keys=True), child.id),
            )
            connection.commit()
        finally:
            connection.close()
        rejected = second._lookup_child(
            {
                "task_id": child.id,
                "parent_task_id": parent_id,
                "parent_run_id": claim.run.id,
                "lease_token": claim.lease.token,
                "fencing_token": claim.lease.fencing_token,
            }
        )
        assert rejected == {
            "ok": False,
            "error": "child result integrity verification failed",
        }
    finally:
        second.store.close()


@pytest.mark.asyncio
async def test_cancel_cascade_recovers_after_failure_following_root_intent(tmp_path, monkeypatch):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())

    def running_task(key: str, *, parent_id: str | None = None):
        task = service.store.create_task(
            TaskSpec(key, key, parent_task_id=parent_id)
        )
        task = service.store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = service.store.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        service.store.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec("execute", agent="worker"),)),
            expected_task_version=task.version,
            created_by="test",
        )
        service.store.enqueue_run(task.id, "execute")
        return service.store.get_task(task.id)

    try:
        root = running_task("cancel-root")
        child = running_task("cancel-child", parent_id=root.id)
        grandchild = running_task("cancel-grandchild", parent_id=child.id)
        original = service.store.cancel_task_runs

        def fail_after_root_intent(*_args, **_kwargs):
            raise RuntimeError("simulated process loss")

        monkeypatch.setattr(service.store, "cancel_task_runs", fail_after_root_intent)
        with pytest.raises(RuntimeError, match="simulated process loss"):
            service.cancel_task(root.id)
        assert service.store.get_task(root.id).status is TaskStatus.CANCELING
        assert service.store.get_task(child.id).status is TaskStatus.RUNNING

        monkeypatch.setattr(service.store, "cancel_task_runs", original)
        service.cancel_task(root.id)
        for _ in range(4):
            await service._coordinate_tasks()
        assert service.store.get_task(root.id).status is TaskStatus.CANCELED
        assert service.store.get_task(child.id).status is TaskStatus.CANCELED
        assert service.store.get_task(grandchild.id).status is TaskStatus.CANCELED
        assert all(
            run.status is RunStatus.CANCELED
            for task in (root, child, grandchild)
            for run in service.store.list_runs(task.id)
        )
    finally:
        service.store.close()


class ShutdownBlockingExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.canceled = asyncio.Event()

    async def execute(self, _context):
        self.started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.canceled.set()
            raise


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("effect_safety", "expected_run", "expected_task"),
    [
        (EffectSafety.IDEMPOTENT, RunStatus.QUEUED, TaskStatus.RUNNING),
        (
            EffectSafety.NON_IDEMPOTENT,
            RunStatus.FAILED,
            TaskStatus.NEEDS_RECONCILIATION,
        ),
    ],
)
async def test_graceful_shutdown_requeues_only_safe_work(
    tmp_path, effect_safety, expected_run, expected_task
):
    executor = ShutdownBlockingExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / effect_safety.value, executor=executor
    )
    try:
        task_id = service.create_task(
            {
                "idempotency_key": f"shutdown-{effect_safety.value}",
                "objective": "work",
                "domain": "knowledge",
                "acceptance_criteria": ["work settles safely"],
                "complexity_factors": {
                    "scope": 1,
                    "uncertainty": 1,
                    "dependencies": 0,
                    "side_effects": 0,
                    "parallelism": 0,
                    "verification": 1,
                },
                "plan": {
                    "nodes": [
                        {
                            "key": "execute",
                            "agent": "worker",
                            "effect_safety": effect_safety.value,
                        }
                    ],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)
        task = service.store.get_task(task_id)
        run = service.store.list_runs(task_id)[0]
        claim = service.store.claim_next_run("shutdown-worker")
        assert claim is not None
        job = asyncio.create_task(service._execute_claim(claim))
        await asyncio.wait_for(executor.started.wait(), timeout=3)
        service._closing = True
        job.cancel()
        with pytest.raises(asyncio.CancelledError):
            await job

        recovered = service.store.get_run(run.id)
        assert recovered.status is expected_run
        assert service.store.get_task(task.id).status is expected_task
        assert executor.canceled.is_set()
        if effect_safety is EffectSafety.NON_IDEMPOTENT:
            assert recovered.error_kind == "shutdown_interrupted_non_idempotent"
    finally:
        service.store.close()


def test_explicit_low_cannot_downgrade_destructive_risk_and_read_only_is_runtime_policy(
    tmp_path,
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    service = OrchestrationService(FakeManager(workspace), tmp_path / "data", executor=object())
    try:
        detail = service.create_task(
            {
                "objective": "inspect only",
                "domain": "code",
                "workspace": str(workspace),
                "acceptance_criteria": ["inspection complete"],
                "risk_tier": "low",
                "destructive_or_irreversible": True,
                "read_only": True,
                "auto_start": False,
            }
        )
        task = service.store.get_task(detail["id"])
        assert task.risk_tier.value == "critical"
        assert task.policy["read_only"] is True
        permissions = service._profile_permissions(
            task, service.catalog.resolve_profile("worker")
        )
        assert "write_file" not in permissions.tools
        assert "apply_patch" not in permissions.tools
        assert permissions.mode == "plan"
        assert permissions.roots and permissions.roots[0].writable is False
    finally:
        service.store.close()
