from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import coworker.orchestration.executor as executor_module
from coworker.events import Event, EventType
from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.errors import GateConflict, LeaseConflict
from coworker.orchestration.executor import OpenWorkerExecutor, RunExecutionContext
from coworker.orchestration.models import (
    GateStatus,
    NodeSpec,
    PlanSpec,
    RunStatus,
    TaskSpec,
    TaskStatus,
)
from coworker.orchestration.profiles import builtin_profile
from coworker.orchestration.routing import ModelCandidate, ModelRouter, RoutingRequest
from coworker.orchestration.store import OrchestrationStore
from coworker.providers import AssistantTurn, ModelCapabilities, ProviderClient, ToolCall
from coworker.server.manager import SessionManager


class ScriptedProvider(ProviderClient):
    def __init__(self, turns):
        self.turns = list(turns)

    def complete(self, **_kwargs):
        return self.turns.pop(0)

    def capabilities(self, _model):
        return ModelCapabilities()


def test_tool_guard_requires_current_scheduler_epoch_as_well_as_run_lease(tmp_path):
    database = tmp_path / "tool-scheduler-fence.db"
    old = OrchestrationStore(database)
    replacement = OrchestrationStore(database)
    now = datetime.now(timezone.utc)
    old_token, old_epoch = old.acquire_scheduler_leader(
        "old-scheduler", lease_seconds=15, now=now
    )
    old.bind_scheduler_fence("old-scheduler", old_token, old_epoch)
    try:
        task = old.create_task(
            TaskSpec(
                idempotency_key="tool-fence-task",
                objective="Reject side effects from a replaced scheduler",
            )
        )
        task = old.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = old.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        graph = old.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec(key="write", agent="worker"),)),
            expected_task_version=task.version,
            created_by="test",
        )
        run = old.enqueue_run(task.id, "write")
        claim = old.claim_next_run("old-run-worker", lease_seconds=60)
        assert claim is not None and claim.run.id == run.id
        old.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
        route = ModelRouter(
            (ModelCandidate("gpt-5.6-sol", quality=100, context_window=400_000),)
        ).select(RoutingRequest(purpose="tool-fence-test"))
        context = RunExecutionContext(
            task=old.get_task(task.id),
            graph=graph,
            node=graph.nodes[0],
            claim=claim,
            profile=builtin_profile("worker"),
            routing=route,
            workspace=None,
        )

        replacement_token, replacement_epoch = replacement.acquire_scheduler_leader(
            "replacement-scheduler",
            lease_seconds=15,
            now=now + timedelta(seconds=16),
        )
        # The run claim itself remains live, reproducing the split-brain window.
        assert replacement.assert_run_lease(
            run.id, claim.lease.token, claim.lease.fencing_token
        )
        with pytest.raises(LeaseConflict, match="leader lease was lost"):
            OpenWorkerExecutor(object(), old)._guard_tool(
                context,
                ToolCall(
                    id="late-write",
                    name="write_file",
                    arguments={"path": "late.txt", "content": "must not run"},
                ),
            )
        assert replacement.release_scheduler_leader(
            "replacement-scheduler", replacement_token, replacement_epoch
        )
    finally:
        old.close()
        replacement.close()


@pytest.mark.asyncio
async def test_completed_turn_fails_when_shell_containment_was_not_reaped(
    tmp_path, monkeypatch
):
    store = OrchestrationStore(tmp_path / "containment-outcome.db")
    try:
        task = store.create_task(
            TaskSpec(
                idempotency_key="containment-outcome",
                objective="Do not accept output from an escaped process tree",
            )
        )
        task = store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = store.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        graph = store.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec(key="execute", agent="worker"),)),
            expected_task_version=task.version,
            created_by="test",
        )
        run = store.enqueue_run(task.id, "execute", session_id="__orch__contained")
        claim = store.claim_next_run("contained-worker")
        assert claim is not None and claim.run.id == run.id
        store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
        route = ModelRouter(
            (ModelCandidate("gpt-5.6-sol", quality=100, context_window=400_000),)
        ).select(RoutingRequest(purpose="containment-outcome"))
        context = RunExecutionContext(
            task=store.get_task(task.id),
            graph=graph,
            node=graph.nodes[0],
            claim=claim,
            profile=builtin_profile("worker"),
            routing=route,
            workspace=None,
        )

        class CompletedEngine:
            def __init__(self) -> None:
                self.messages = [{"role": "assistant", "content": "looks complete"}]
                self.executor = SimpleNamespace(
                    containment_failed=True, close=lambda: None
                )
                self.compaction_settings = None

            @staticmethod
            def recovery_state():
                return SimpleNamespace(disposition="clean", pending_tools=[])

            async def run(self, _prompt):
                yield Event(EventType.TURN_END, {"status": "completed"})

        engine = CompletedEngine()
        monkeypatch.setattr(
            executor_module, "build_engine", lambda **_kwargs: engine
        )
        manager = SimpleNamespace(
            session_store=SimpleNamespace(load=lambda _session_id: None),
            model="gpt-5.6-sol",
            provider=object(),
            secrets=object(),
            audit_store=SimpleNamespace(append=lambda _record: None),
            compaction_settings=None,
            save=lambda _session_id, _engine: None,
        )

        outcome = await OpenWorkerExecutor(manager, store).execute(context)

        assert outcome.status == "failed"
        assert outcome.error_kind == "process_tree_cleanup_failed"
        assert "requires reconciliation" in (outcome.error_message or "")
    finally:
        store.close()


@pytest.mark.asyncio
async def test_suspended_gate_stays_unpublished_when_close_discovers_cleanup_breach(
    tmp_path, monkeypatch
):
    store = OrchestrationStore(tmp_path / "suspended-cleanup.db")
    try:
        task = store.create_task(
            TaskSpec(
                idempotency_key="suspended-cleanup",
                objective="Never publish a gate before shell cleanup settles",
            )
        )
        task = store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = store.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        graph = store.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec(key="execute", agent="worker"),)),
            expected_task_version=task.version,
            created_by="test",
        )
        run = store.enqueue_run(task.id, "execute", session_id="__orch__suspend")
        claim = store.claim_next_run("suspending-worker")
        assert claim is not None
        store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
        route = ModelRouter(
            (ModelCandidate("gpt-5.6-sol", quality=100, context_window=400_000),)
        ).select(RoutingRequest(purpose="suspended-cleanup"))
        context = RunExecutionContext(
            task=store.get_task(task.id),
            graph=graph,
            node=graph.nodes[0],
            claim=claim,
            profile=builtin_profile("worker"),
            routing=route,
            workspace=None,
        )

        class ClosingExecutor:
            containment_failed = False

            def close(self) -> None:
                self.containment_failed = True

        local_executor = ClosingExecutor()

        class SuspendedEngine:
            def __init__(self) -> None:
                self.messages = [{"role": "assistant", "content": "waiting"}]
                self.executor = local_executor
                self.compaction_settings = None
                self.gate_id = None

            def recovery_state(self):
                return SimpleNamespace(
                    disposition="pending_tools" if self.gate_id else "clean",
                    pending_tool_calls=(
                        [SimpleNamespace(id="question-call")] if self.gate_id else []
                    ),
                )

            async def run(self, _prompt):
                gate = store.prepare_run_gate(
                    run.id,
                    claim.lease.token,
                    claim.lease.fencing_token,
                    kind=executor_module.GateKind.QUESTION,
                    source_key=f"{run.id}:question:late-cleanup",
                    prompt={"title": "Wait", "actions": ["submit"]},
                )
                self.gate_id = gate.id
                yield Event(
                    EventType.TURN_SUSPENDED,
                    {"interaction_id": gate.id, "interaction_kind": "question"},
                )

        engine = SuspendedEngine()
        monkeypatch.setattr(executor_module, "build_engine", lambda **_kwargs: engine)
        manager = SimpleNamespace(
            session_store=SimpleNamespace(load=lambda _session_id: None),
            model="gpt-5.6-sol",
            provider=object(),
            secrets=object(),
            audit_store=SimpleNamespace(append=lambda _record: None),
            compaction_settings=None,
            save=lambda _session_id, _engine: None,
        )

        outcome = await OpenWorkerExecutor(
            manager,
            store,
            blob_store=ContentAddressedBlobStore(tmp_path / "suspended-blobs"),
        ).execute(context)

        assert outcome.status == "failed"
        assert outcome.error_kind == "process_tree_cleanup_failed"
        prepared = store.get_gate(str(engine.gate_id))
        assert prepared.status is GateStatus.PREPARING
        assert store.get_run(run.id).status is RunStatus.RUNNING
        with pytest.raises(GateConflict, match="already preparing"):
            store.resolve_gate(
                prepared.id,
                GateStatus.APPROVED,
                {"decision": "submit", "response": "too early"},
                resolved_by="racing-user",
                expected_version=prepared.version,
            )

        store.fail_run(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            error_kind=outcome.error_kind,
            error_message=outcome.error_message or "cleanup failed",
        )
        assert store.get_gate(prepared.id).status is GateStatus.CANCELED
        assert store.get_run(run.id).error_kind == "process_tree_cleanup_failed"
    finally:
        store.close()


@pytest.mark.asyncio
async def test_cancellation_yields_failed_outcome_when_close_discovers_containment_breach(
    tmp_path, monkeypatch
):
    store = OrchestrationStore(tmp_path / "containment-cancel.db")
    try:
        task = store.create_task(
            TaskSpec(
                idempotency_key="containment-cancel",
                objective="Preserve a cleanup breach discovered during cancellation",
            )
        )
        task = store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = store.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        graph = store.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec(key="execute", agent="worker"),)),
            expected_task_version=task.version,
            created_by="test",
        )
        run = store.enqueue_run(task.id, "execute", session_id="__orch__cancelled")
        claim = store.claim_next_run("cancelled-worker")
        assert claim is not None and claim.run.id == run.id
        store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
        route = ModelRouter(
            (ModelCandidate("gpt-5.6-sol", quality=100, context_window=400_000),)
        ).select(RoutingRequest(purpose="containment-cancel"))
        context = RunExecutionContext(
            task=store.get_task(task.id),
            graph=graph,
            node=graph.nodes[0],
            claim=claim,
            profile=builtin_profile("worker"),
            routing=route,
            workspace=None,
        )
        started = asyncio.Event()

        class ClosingExecutor:
            containment_failed = False

            def close(self) -> None:
                self.containment_failed = True

        local_executor = ClosingExecutor()

        class BlockingEngine:
            def __init__(self) -> None:
                self.messages = []
                self.executor = local_executor
                self.compaction_settings = None
                self.interrupted = False

            @staticmethod
            def recovery_state():
                return SimpleNamespace(disposition="clean", pending_tools=[])

            async def run(self, _prompt):
                started.set()
                await asyncio.Event().wait()
                if False:
                    yield Event(EventType.TURN_END, {"status": "completed"})

            def mark_interrupted(self) -> None:
                self.interrupted = True

        engine = BlockingEngine()
        monkeypatch.setattr(executor_module, "build_engine", lambda **_kwargs: engine)
        manager = SimpleNamespace(
            session_store=SimpleNamespace(load=lambda _session_id: None),
            model="gpt-5.6-sol",
            provider=object(),
            secrets=object(),
            audit_store=SimpleNamespace(append=lambda _record: None),
            compaction_settings=None,
            save=lambda _session_id, _engine: None,
        )

        execution = asyncio.create_task(OpenWorkerExecutor(manager, store).execute(context))
        await started.wait()
        execution.cancel()
        outcome = await execution

        assert engine.interrupted is True
        assert local_executor.containment_failed is True
        assert outcome.status == "failed"
        assert outcome.error_kind == "process_tree_cleanup_failed"
    finally:
        store.close()


@pytest.mark.asyncio
async def test_permission_gate_releases_and_resumes_the_same_hidden_session(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    provider = ScriptedProvider(
        [
            AssistantTurn(
                tool_calls=[
                    ToolCall(
                        id="write-1",
                        name="write_file",
                        arguments={"path": "result.txt", "content": "durable"},
                    )
                ]
            ),
            AssistantTurn(text="Work completed and verified."),
        ]
    )
    manager = SessionManager(
        workspace=workspace, data_dir=tmp_path / "manager", provider=provider
    )
    store = OrchestrationStore(tmp_path / "orchestration.db")
    executor = OpenWorkerExecutor(
        manager, store, blob_store=ContentAddressedBlobStore(tmp_path / "blobs")
    )
    try:
        task = store.create_task(TaskSpec(idempotency_key="resume", objective="Write the result", workspace=str(workspace)))
        task = store.transition_task_status(task.id, TaskStatus.QUEUED, expected_version=task.version)
        task = store.transition_task_status(task.id, TaskStatus.RUNNING, expected_version=task.version)
        graph = store.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec(key="write", title="Write", instructions="Write result.txt", agent="worker"),)),
            expected_task_version=task.version,
            created_by="test",
        )
        run = store.enqueue_run(
            task.id,
            "write",
            session_id=f"__orch__{task.id}_write_1",
        )
        claim = store.claim_next_run("worker-one")
        assert claim and claim.run.id == run.id
        store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
        route = ModelRouter(
            (ModelCandidate("gpt-5.6-sol", quality=100, context_window=400_000),)
        ).select(
            RoutingRequest(purpose="test")
        )
        context = RunExecutionContext(
            task=store.get_task(task.id),
            graph=graph,
            node=graph.nodes[0],
            claim=claim,
            profile=builtin_profile("worker"),
            routing=route,
            workspace=workspace,
        )

        suspended = await executor.execute(context)
        assert suspended.status == "suspended"
        assert suspended.gate_id
        gate = store.get_gate(suspended.gate_id)
        assert store.get_run(run.id).status is RunStatus.RUNNING
        assert gate.status is GateStatus.PREPARING
        gate = store.commit_prepared_gate(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            gate_id=gate.id,
            checkpoint=suspended.output["engine_checkpoint"],
        )
        waiting = store.get_run(run.id)
        assert waiting.status is RunStatus.WAITING_GATE
        assert gate.status is GateStatus.OPEN
        assert manager.session_store.load(run.session_id).messages[-1]["role"] == "assistant"

        store.resolve_gate(
            gate.id,
            GateStatus.APPROVED,
            {"decision": "approve"},
            resolved_by="tester",
            expected_version=gate.version,
        )
        resumed_claim = store.claim_next_run("worker-two")
        assert resumed_claim and resumed_claim.run.id == run.id
        store.start_run(
            run.id, resumed_claim.lease.token, resumed_claim.lease.fencing_token
        )
        resumed = await executor.execute(
            RunExecutionContext(
                task=store.get_task(task.id),
                graph=graph,
                node=graph.nodes[0],
                claim=resumed_claim,
                profile=builtin_profile("worker"),
                routing=route,
                workspace=workspace,
            )
        )
        assert resumed.status == "succeeded"
        store.complete_run(
            run.id,
            resumed_claim.lease.token,
            resumed_claim.lease.fencing_token,
            output=resumed.output,
        )
        assert (workspace / "result.txt").read_text(encoding="utf-8") == "durable"
        assert manager.session_store.load(run.session_id).messages[-1]["content"] == "Work completed and verified."
    finally:
        store.close()
        await manager.aclose()


@pytest.mark.asyncio
async def test_permission_gate_and_hidden_session_resume_after_process_restart(tmp_path):
    """A durable gate must not rely on the original manager/engine object.

    This simulates a hard process boundary by closing every first-generation object,
    reopening both stores, and supplying a fresh provider and executor.  The unanswered
    tool call must resume in the same attempt/session without asking the model to repeat
    the turn.
    """

    workspace = tmp_path / "project"
    workspace.mkdir()
    manager_dir = tmp_path / "manager"
    database = tmp_path / "orchestration.db"
    session_id = "__orch__restart_gate"

    first_manager = SessionManager(
        workspace=workspace,
        data_dir=manager_dir,
        provider=ScriptedProvider(
            [
                AssistantTurn(
                    tool_calls=[
                        ToolCall(
                            id="restart-write-1",
                            name="write_file",
                            arguments={
                                "path": "after-restart.txt",
                                "content": "resumed",
                            },
                        )
                    ]
                )
            ]
        ),
    )
    first_store = OrchestrationStore(database)
    blobs = ContentAddressedBlobStore(tmp_path / "blobs")
    first_executor = OpenWorkerExecutor(
        first_manager, first_store, blob_store=blobs
    )
    route = ModelRouter(
        (ModelCandidate("gpt-5.6-sol", quality=100, context_window=400_000),)
    ).select(RoutingRequest(purpose="restart-test"))
    try:
        task = first_store.create_task(
            TaskSpec(
                idempotency_key="restart-resume",
                objective="Write after restart",
                workspace=str(workspace),
            )
        )
        task = first_store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = first_store.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        graph = first_store.create_plan_revision(
            task.id,
            PlanSpec(
                nodes=(
                    NodeSpec(
                        key="write",
                        title="Write",
                        instructions="Write after-restart.txt",
                        agent="worker",
                    ),
                )
            ),
            expected_task_version=task.version,
            created_by="test",
        )
        run = first_store.enqueue_run(task.id, "write", session_id=session_id)
        claim = first_store.claim_next_run("before-restart")
        assert claim is not None
        first_store.start_run(
            run.id, claim.lease.token, claim.lease.fencing_token
        )
        suspended = await first_executor.execute(
            RunExecutionContext(
                task=first_store.get_task(task.id),
                graph=graph,
                node=graph.nodes[0],
                claim=claim,
                profile=builtin_profile("worker"),
                routing=route,
                workspace=workspace,
            )
        )
        assert suspended.status == "suspended" and suspended.gate_id
        prepared = first_store.get_gate(suspended.gate_id)
        assert prepared.status is GateStatus.PREPARING
        first_store.commit_prepared_gate(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            gate_id=prepared.id,
            checkpoint=suspended.output["engine_checkpoint"],
        )
        assert first_store.get_run(run.id).status is RunStatus.WAITING_GATE
        assert first_manager.session_store.load(session_id) is not None
    finally:
        first_store.close()
        await first_manager.aclose()

    second_manager = SessionManager(
        workspace=workspace,
        data_dir=manager_dir,
        provider=ScriptedProvider([AssistantTurn(text="Restarted run completed.")]),
    )
    second_store = OrchestrationStore(database)
    second_executor = OpenWorkerExecutor(
        second_manager, second_store, blob_store=blobs
    )
    try:
        recovered_run = second_store.get_run(run.id)
        recovered_gate = second_store.get_gate(str(suspended.gate_id))
        assert recovered_run.status is RunStatus.WAITING_GATE
        assert recovered_gate.status is GateStatus.OPEN
        second_store.resolve_gate(
            recovered_gate.id,
            GateStatus.APPROVED,
            {"decision": "approve"},
            resolved_by="restart-test",
            expected_version=recovered_gate.version,
            command_id="resolve-after-process-restart",
        )
        resumed_claim = second_store.claim_next_run("after-restart")
        assert resumed_claim is not None and resumed_claim.run.id == run.id
        second_store.start_run(
            run.id,
            resumed_claim.lease.token,
            resumed_claim.lease.fencing_token,
        )
        recovered_graph = second_store.get_plan(graph.plan.id)
        resumed = await second_executor.execute(
            RunExecutionContext(
                task=second_store.get_task(task.id),
                graph=recovered_graph,
                node=recovered_graph.nodes[0],
                claim=resumed_claim,
                profile=builtin_profile("worker"),
                routing=route,
                workspace=workspace,
            )
        )
        assert resumed.status == "succeeded"
        second_store.complete_run(
            run.id,
            resumed_claim.lease.token,
            resumed_claim.lease.fencing_token,
            output=resumed.output,
        )
        assert (workspace / "after-restart.txt").read_text(encoding="utf-8") == "resumed"
        saved = second_manager.session_store.load(session_id)
        assert saved is not None
        assert saved.messages[-1]["content"] == "Restarted run completed."
        assert second_store.verify_event_chain() is True
    finally:
        second_store.close()
        await second_manager.aclose()
