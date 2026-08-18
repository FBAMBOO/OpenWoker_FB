from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from coworker.orchestration import service as orchestration_service_module
from coworker.orchestration.executor import ExecutionOutcome
from coworker.orchestration.models import GateKind, GateStatus, TaskSpec, TaskStatus
from coworker.orchestration.service import OrchestrationService


class FakeManager:
    def __init__(self, workspace: Path | None = None):
        self.default_workspace = str(workspace) if workspace else None
        self.model = "gpt-5.6-sol"
        self.events = []

    def _provider_configured(self, _provider: str) -> bool:
        return True

    def get_settings(self):
        return {
            "models": [self.model],
            "model_labels": {self.model: "Test model"},
            "model_context_windows": {self.model: 400_000},
        }

    async def broadcast_event(self, event):
        self.events.append(event)


class SuccessfulExecutor:
    def __init__(self):
        self.calls = []

    async def execute(self, context):
        self.calls.append((context.claim.run.id, context.profile.role.value, context.workspace))
        if context.profile.role.value in {"worker", "integrator"} and context.workspace:
            (context.workspace / "orchestrated.txt").write_text("delivered", encoding="utf-8")
        route = context.routing.audit_record()
        output = {
            "summary": f"{context.profile.role.value} complete",
            "profile": {"profile_id": context.profile.profile_id, "version": context.profile.version},
            "routing": route,
        }
        if context.profile.role.value in {"reviewer", "tester", "evaluator", "scorer"}:
            output["verdict"] = {
                "status": "pass",
                "summary": "all checks passed",
                "criteria": {
                    criterion: "pass" for criterion in context.task.acceptance_criteria
                },
            }
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or "hidden",
            summary=f"{context.profile.role.value} complete",
            output=output,
            evidence=({"kind": "note", "title": "Run result", "summary": "ok"},),
        )


async def wait_until(predicate, *, timeout=8.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(0.05)
    raise AssertionError("condition was not reached before timeout")


def test_task_detail_bounds_large_child_tree_and_reports_truncation(
    tmp_path, monkeypatch
):
    service = OrchestrationService(
        FakeManager(), tmp_path / "data", executor=object()
    )
    try:
        root_id = service.create_task(
            {
                "idempotency_key": "bounded-detail-root",
                "objective": "Render a bounded task hierarchy",
                "domain": "knowledge",
                "acceptance_criteria": ["detail is bounded"],
                "auto_start": False,
            }
        )["id"]
        children = []
        for index in range(8):
            children.append(
                service.store.create_task(
                    TaskSpec(
                        idempotency_key=f"bounded-detail-child-{index}",
                        objective=f"Child {index}",
                        parent_task_id=root_id,
                    ),
                    command_id=f"bounded-detail-child-{index}",
                )
            )
        for index, child in enumerate(children):
            service.store.create_task(
                TaskSpec(
                    idempotency_key=f"bounded-detail-grandchild-{index}",
                    objective=f"Grandchild {index}",
                    parent_task_id=child.id,
                ),
                command_id=f"bounded-detail-grandchild-{index}",
            )

        monkeypatch.setattr(
            orchestration_service_module, "_DETAIL_CHILD_DEPTH", 1
        )
        monkeypatch.setattr(
            orchestration_service_module, "_DETAIL_TREE_ROW_LIMIT", 5
        )
        original_tree = service.store.list_task_tree
        tree_calls = []

        def bounded_tree(*args, **kwargs):
            tree_calls.append(dict(kwargs))
            return original_tree(*args, **kwargs)

        monkeypatch.setattr(service.store, "list_task_tree", bounded_tree)
        original_gates = service.store.list_gates

        def bounded_gates(*args, **kwargs):
            assert kwargs.get("limit") is not None
            return original_gates(*args, **kwargs)

        monkeypatch.setattr(service.store, "list_gates", bounded_gates)

        detail = service.task_detail(root_id)

        assert tree_calls == [{"max_depth": 1, "max_rows": 6}]
        assert len(detail["children"]) == 4
        assert len(detail["children_details"]) == 4
        assert detail["children_page"] == {
            "truncated": True,
            "tree_truncated": True,
            "depth_limit_reached": False,
            "returned": 4,
            "total": 8,
            "tree_row_limit": 5,
        }
        assert all(
            child["children_page"]["depth_limit_reached"]
            for child in detail["children_details"]
        )
        assert len(detail["runtime"]) <= detail["runtime_page"]["limit"]
        assert detail["runtime_page"]["returned"] == len(detail["runtime"])
        assert detail["runtime_page"]["truncated"] is False
    finally:
        service.store.close()


def test_failed_run_detail_gate_and_activity_expose_bounded_error_message(
    tmp_path, monkeypatch
):
    service = OrchestrationService(
        FakeManager(), tmp_path / "data", executor=object()
    )
    try:
        task_id = service.create_task(
            {
                "idempotency_key": "failed-run-read-model-diagnostic",
                "objective": "Expose a failed run diagnostic through the read model",
                "domain": "knowledge",
                "acceptance_criteria": ["The failure remains auditable"],
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
                            "key": "diagnose",
                            "retry_policy": {"max_attempts": 1},
                        }
                    ],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)
        claim = service.store.claim_next_run("diagnostic-worker")
        assert claim is not None
        service.store.start_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
        )
        diagnostic = "HTTP 400 invalid_json_schema: " + ("schema-detail-" * 20)
        service.store.fail_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            error_kind="codex_turn_failed",
            error_message=diagnostic,
        )
        monkeypatch.setattr(
            orchestration_service_module,
            "_RUN_ERROR_MESSAGE_LIMIT",
            64,
        )

        service._advance_task(task_id)
        detail = service.task_detail(task_id)

        expected = diagnostic[:64]
        run = next(item for item in detail["runs"] if item["id"] == claim.run.id)
        assert run["error_kind"] == "codex_turn_failed"
        assert run["error_message"] == expected
        assert run["summary"] == expected
        assert len(run["error_message"]) == 64

        reconciliation = next(
            item
            for item in detail["attention"]
            if item["kind"] == GateKind.RECONCILIATION.value
        )
        failed_run = reconciliation["prompt"]["failed_runs"][0]
        assert failed_run["id"] == claim.run.id
        assert failed_run["error_kind"] == "codex_turn_failed"
        assert failed_run["error_message"] == expected
        assert failed_run["summary"] == expected

        activity = next(
            item
            for item in detail["activity"]
            if item["type"] == "run.failed" and item["id"]
        )
        assert activity["error_kind"] == "codex_turn_failed"
        assert activity["error_message"] == expected

        runs_page = service.task_runs_page(task_id)
        paged_run = next(
            item for item in runs_page["runs"] if item["id"] == claim.run.id
        )
        assert paged_run["error_message"] == expected
        assert service.store.verify_event_chain() is True
    finally:
        service.store.close()


@pytest.mark.asyncio
async def test_low_risk_task_runs_all_eight_stages_and_remains_completed(tmp_path):
    executor = SuccessfulExecutor()
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=executor)
    await service.start()
    try:
        created = service.create_task(
            {
                "objective": "Summarize the supplied facts",
                "domain": "knowledge",
                "acceptance_criteria": ["A concise summary exists"],
            }
        )
        task_id = created["id"]
        await wait_until(lambda: service.store.get_task(task_id).status is TaskStatus.COMPLETED)
        detail = service.task_detail(task_id)
        assert detail["stage"] == "archive"
        assert [item["stage"] for item in detail["stages"]] == [
            "intake",
            "complexity_assessment",
            "clarification",
            "planning",
            "execution_review_test",
            "inter_step_evaluation",
            "final_acceptance",
            "archive",
        ]
        assert detail["stages"][2]["status"] == "skipped"
        assert detail["stages"][-1]["status"] == "completed"
        assert detail["progress"] == 100
        assert {run["node_key"] for run in detail["runs"]} == {"execute", "evaluate"}
        assert service.store.verify_event_chain() is True
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_code_task_requires_plan_and_final_gates_with_isolated_roles(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    (workspace / "before.txt").write_text("before", encoding="utf-8")
    executor = SuccessfulExecutor()
    service = OrchestrationService(
        FakeManager(workspace), tmp_path / "data", executor=executor, poll_seconds=0.05
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Create the requested local result",
                "domain": "code",
                "acceptance_criteria": ["orchestrated.txt contains delivered"],
            }
        )["id"]

        plan_gate = await wait_until(
            lambda: next(
                (
                    gate
                    for gate in service.store.list_gates(task_id, statuses=(GateStatus.OPEN,))
                    if gate.kind is GateKind.PLAN_APPROVAL
                ),
                None,
            )
        )
        service.resolve_gate(task_id, plan_gate.id, decision="approve")

        final_gate = await wait_until(
            lambda: next(
                (
                    gate
                    for gate in service.store.list_gates(task_id, statuses=(GateStatus.OPEN,))
                    if gate.kind is GateKind.FINAL_ACCEPTANCE
                ),
                None,
            ),
            timeout=15,
        )
        roles = {role for _, role, _ in executor.calls}
        assert {"worker", "reviewer", "tester", "evaluator"} <= roles
        workspace_ids = {str(path) for _, _, path in executor.calls if path}
        assert len(workspace_ids) == 4
        # Every run works in orchestration-owned snapshots. The formal workspace is
        # intentionally unchanged until the final acceptance decision is durable.
        assert not (workspace / "orchestrated.txt").exists()

        service.resolve_gate(task_id, final_gate.id, decision="accept")
        await wait_until(lambda: service.store.get_task(task_id).status is TaskStatus.COMPLETED)
        assert (workspace / "orchestrated.txt").read_text(encoding="utf-8") == "delivered"
        detail = service.task_detail(task_id)
        assert detail["attention_count"] == 0
        assert any(item["kind"] == "artifact" for item in detail["evidence"])
    finally:
        await service.stop()
