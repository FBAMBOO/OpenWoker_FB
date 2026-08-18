from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from coworker.orchestration.errors import ConflictError, GateConflict, LeaseConflict
from coworker.orchestration.executor import ExecutionOutcome
from coworker.orchestration.models import (
    EvidenceKind,
    GateKind,
    GateStatus,
    NodeSpec,
    OrchestrationStage,
    PlanSpec,
    RetryPolicy,
    RunStatus,
    StageDisposition,
    TaskSpec,
    TaskStatus,
)
from coworker.orchestration.runtime import RuntimeStatus
from coworker.orchestration.service import OrchestrationService


class FakeManager:
    def __init__(self, workspace: Path | None = None) -> None:
        self.default_workspace = str(workspace) if workspace else None
        self.model = "gpt-5.6-sol"

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


async def wait_until(predicate, *, timeout: float = 10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(0.03)
    raise AssertionError("condition was not reached before timeout")


def low_complexity() -> dict[str, int]:
    return {
        "scope": 1,
        "uncertainty": 1,
        "dependencies": 0,
        "side_effects": 0,
        "parallelism": 0,
        "verification": 1,
    }


def _publishable_code_task(service: OrchestrationService, workspace: Path):
    task_id = service.create_task(
        {
            "objective": "Publish a sealed workspace candidate",
            "domain": "code",
            "workspace": str(workspace),
            "acceptance_criteria": ["artifact.txt contains accepted"],
            "plan": {
                "nodes": [
                    {"key": "execute", "agent": "worker"},
                    {"key": "review", "kind": "review", "agent": "reviewer"},
                    {"key": "test", "kind": "test", "agent": "tester"},
                    {"key": "evaluate", "kind": "evaluate", "agent": "evaluator"},
                ],
                "edges": [
                    {"from": "execute", "to": "review"},
                    {"from": "execute", "to": "test"},
                    {"from": "review", "to": "evaluate"},
                    {"from": "test", "to": "evaluate"},
                ],
            },
            "auto_start": False,
        }
    )["id"]
    service.submit_task(task_id)
    service._advance_task(task_id)
    gate = next(
        item
        for item in service.store.list_gates(task_id, statuses=(GateStatus.OPEN,))
        if item.kind is GateKind.PLAN_APPROVAL
    )
    service.resolve_gate(task_id, gate.id, decision="approve")
    task = service.store.get_task(task_id)
    return task, service.store.get_plan(task.active_plan_id or "")


def test_run_candidate_tampering_after_completion_fails_closed(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    (workspace / "artifact.txt").write_text("baseline", encoding="utf-8")
    service = OrchestrationService(
        FakeManager(workspace), tmp_path / "data", executor=object()
    )
    try:
        task, _graph = _publishable_code_task(service, workspace)
        run = service.store.enqueue_run(task.id, "execute")
        claim = service.store.claim_next_run("sealed-run")
        assert claim is not None and claim.run.id == run.id
        service.store.start_run(
            run.id, claim.lease.token, claim.lease.fencing_token
        )
        snapshot = service._ensure_run_snapshot(task, run)
        assert snapshot is not None
        (snapshot.candidate / "artifact.txt").write_text(
            "accepted-A", encoding="utf-8"
        )
        sealed = service.workspaces.collect_candidate(snapshot)
        service.store.complete_run(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            output={
                "workspace_commit": {
                    "status": "pending",
                    "snapshot_id": snapshot.snapshot_id,
                    "candidate_manifest_sha256": sealed.candidate_manifest.digest,
                    "patch_sha256": sealed.patch_sha256,
                    "fencing_token": claim.lease.fencing_token,
                }
            },
        )
        (snapshot.candidate / "artifact.txt").write_text(
            "tampered-B", encoding="utf-8"
        )

        asyncio.run(service._finalize_succeeded_run(run.id))

        settled = service.store.get_run(run.id)
        assert settled.output["workspace_commit"]["status"] == "failed"
        task_snapshot = service._ensure_task_snapshot(task)
        assert task_snapshot is not None
        assert (task_snapshot.candidate / "artifact.txt").read_text(
            encoding="utf-8"
        ) == "baseline"
    finally:
        service.store.close()


def test_cancel_and_formal_publication_have_one_linearized_order(tmp_path, monkeypatch):
    workspace = tmp_path / "project"
    workspace.mkdir()
    (workspace / "artifact.txt").write_text("baseline", encoding="utf-8")
    service = OrchestrationService(
        FakeManager(workspace), tmp_path / "data", executor=object()
    )
    try:
        task, graph = _publishable_code_task(service, workspace)
        snapshot = service._ensure_task_snapshot(task)
        assert snapshot is not None
        (snapshot.candidate / "artifact.txt").write_text(
            "accepted", encoding="utf-8"
        )
        subject = service._candidate_subject(task, graph)
        entered = threading.Event()
        release = threading.Event()
        original = service._deliver_with_commit_lock

        def blocked_delivery(active_snapshot, **kwargs):
            entered.set()
            assert release.wait(5)
            return original(active_snapshot, **kwargs)

        monkeypatch.setattr(service, "_deliver_with_commit_lock", blocked_delivery)
        publication: dict[str, object] = {}

        def publish() -> None:
            try:
                publication["payload"] = service._publish_task_candidate(
                    task, graph, subject, actor="acceptor"
                )
            except BaseException as exc:
                publication["error"] = exc

        publish_thread = threading.Thread(target=publish)
        publish_thread.start()
        assert entered.wait(5)
        cancel_thread = threading.Thread(target=lambda: service.cancel_task(task.id))
        cancel_thread.start()
        time.sleep(0.1)

        # Publication owns the shared source fence, so cancellation has not yet
        # acquired its durable linearization point.
        assert cancel_thread.is_alive()
        assert service.store.get_task(task.id).status is TaskStatus.RUNNING
        release.set()
        publish_thread.join(10)
        cancel_thread.join(10)

        assert publication.get("error") is None
        assert not publish_thread.is_alive() and not cancel_thread.is_alive()
        assert (workspace / "artifact.txt").read_text(encoding="utf-8") == "accepted"
        assert service.store.get_task(task.id).status is TaskStatus.CANCELING
        events = service.store.list_events(task_id=task.id)
        published_sequence = next(
            item.sequence
            for item in events
            if item.event_type == "evidence.added"
            and item.aggregate_id
            == next(
                evidence.id
                for evidence in service.store.list_evidence(task.id)
                if evidence.payload.get("action") == "workspace_published"
            )
        )
        canceled_sequence = next(
            item.sequence
            for item in events
            if item.event_type == "task.status_changed"
            and item.payload.get("to") == "canceling"
        )
        assert published_sequence < canceled_sequence
    finally:
        service.store.close()


def test_cancel_winning_publication_fence_prevents_formal_delivery(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    (workspace / "artifact.txt").write_text("baseline", encoding="utf-8")
    service = OrchestrationService(
        FakeManager(workspace), tmp_path / "data", executor=object()
    )
    try:
        task, graph = _publishable_code_task(service, workspace)
        snapshot = service._ensure_task_snapshot(task)
        assert snapshot is not None
        (snapshot.candidate / "artifact.txt").write_text(
            "accepted", encoding="utf-8"
        )
        subject = service._candidate_subject(task, graph)

        service.cancel_task(task.id)
        with pytest.raises(ConflictError, match="task changed before"):
            service._publish_task_candidate(task, graph, subject, actor="acceptor")
        assert (workspace / "artifact.txt").read_text(encoding="utf-8") == "baseline"
        assert not any(
            item.payload.get("action") == "workspace_published"
            for item in service.store.list_evidence(task.id)
        )
    finally:
        service.store.close()


def test_publication_recovery_rejects_retained_candidate_that_breaks_seal(
    tmp_path, monkeypatch
):
    workspace = tmp_path / "project"
    workspace.mkdir()
    (workspace / "artifact.txt").write_text("baseline", encoding="utf-8")
    data_dir = tmp_path / "data"
    service = OrchestrationService(
        FakeManager(workspace), data_dir, executor=object()
    )
    task, graph = _publishable_code_task(service, workspace)
    snapshot = service._ensure_task_snapshot(task)
    assert snapshot is not None
    (snapshot.candidate / "artifact.txt").write_text("accepted", encoding="utf-8")
    subject = service._candidate_subject(task, graph)
    original_add = service.store.add_evidence

    def crash_after_delivery(*args, **kwargs):
        if dict(kwargs.get("payload") or {}).get("action") == "workspace_published":
            raise RuntimeError("crash after durable delivery journal")
        return original_add(*args, **kwargs)

    monkeypatch.setattr(service.store, "add_evidence", crash_after_delivery)
    with pytest.raises(RuntimeError, match="crash after durable"):
        service._publish_task_candidate(task, graph, subject, actor="acceptor")
    assert (workspace / "artifact.txt").read_text(encoding="utf-8") == "accepted"
    assert not any(
        item.payload.get("action") == "workspace_published"
        for item in service.store.list_evidence(task.id)
    )
    service.store.close()

    # An orphaned writer changes retained staging after the formal delivery. Recovery
    # must not manufacture a publication receipt for this different subject.
    (snapshot.candidate / "artifact.txt").write_text("tampered", encoding="utf-8")
    recovered = OrchestrationService(
        FakeManager(workspace), data_dir, executor=object()
    )
    try:
        recovered._recover_delivered_publications()
        assert recovered.store.get_task(task.id).status is TaskStatus.NEEDS_RECONCILIATION
        assert not any(
            item.payload.get("action") == "workspace_published"
            for item in recovered.store.list_evidence(task.id)
        )
        assert any(
            item.payload.get("action") == "workspace_publication_recovery_failed"
            for item in recovered.store.list_evidence(task.id)
        )
    finally:
        recovered.store.close()


def test_health_fails_closed_when_a_scheduler_tick_is_stale(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())

    class AliveTask:
        @staticmethod
        def done() -> bool:
            return False

    try:
        now = datetime.now(timezone.utc)
        service._loop_task = AliveTask()  # type: ignore[assignment]
        service._scheduler_started_at = now - timedelta(minutes=2)
        service._last_scheduler_success = now - timedelta(minutes=1)
        service._last_scheduler_tick_started = now - timedelta(seconds=31)

        health = service.health_snapshot()

        assert health["ready"] is False
        assert health["state"] == "unhealthy"
        assert health["stale"] is True
        assert health["stale_after_seconds"] == 30.0
    finally:
        service._loop_task = None
        service.store.close()


@pytest.mark.asyncio
async def test_only_one_scheduler_can_lead_a_shared_database(tmp_path):
    data_dir = tmp_path / "shared-data"
    first = OrchestrationService(
        FakeManager(), data_dir, executor=object(), poll_seconds=0.03
    )
    second = OrchestrationService(
        FakeManager(), data_dir, executor=object(), poll_seconds=0.03
    )
    await first.start()
    try:
        with pytest.raises(LeaseConflict, match="already owned"):
            await second.start()
        assert first.health_snapshot()["leader"]["held"] is True
    finally:
        await first.stop()

    replacement = OrchestrationService(
        FakeManager(), data_dir, executor=object(), poll_seconds=0.03
    )
    try:
        await replacement.start()
        assert replacement.health_snapshot()["leader"]["held"] is True
        with pytest.raises(LeaseConflict, match="store is closed"):
            first.store.create_task(
                TaskSpec(
                    idempotency_key="late-write-after-stop",
                    objective="A stopped leader must remain fail-closed",
                )
            )
    finally:
        await replacement.stop()
        second.store.close()


@pytest.mark.asyncio
async def test_stop_drains_leader_owned_threads_before_releasing_epoch(tmp_path):
    data_dir = tmp_path / "drained-leader"
    entered = threading.Event()
    release = threading.Event()
    first = OrchestrationService(
        FakeManager(), data_dir, executor=object(), poll_seconds=0.01
    )
    replacement = OrchestrationService(
        FakeManager(), data_dir, executor=object(), poll_seconds=0.01
    )
    await first.start()

    def blocked_tick() -> None:
        entered.set()
        if not release.wait(5):
            raise AssertionError("test did not release blocked scheduler operation")
        first.store.assert_scheduler_fence()

    async def coordinate_once() -> None:
        await first._durable_to_thread(blocked_tick)

    first._coordinate_tasks = coordinate_once  # type: ignore[method-assign]
    first.wake()
    await wait_until(entered.is_set)
    stop_task = asyncio.create_task(first.stop())
    await asyncio.sleep(0.05)
    assert not stop_task.done()
    with pytest.raises(LeaseConflict, match="already owned"):
        await replacement.start()

    release.set()
    await asyncio.wait_for(stop_task, timeout=5)
    try:
        await replacement.start()
        assert replacement.health_snapshot()["leader"]["held"] is True
    finally:
        await replacement.stop()


@pytest.mark.asyncio
async def test_canceled_startup_drains_recovery_before_releasing_epoch(tmp_path):
    data_dir = tmp_path / "drained-startup"
    entered = threading.Event()
    release = threading.Event()
    first = OrchestrationService(FakeManager(), data_dir, executor=object())
    replacement = OrchestrationService(FakeManager(), data_dir, executor=object())

    def blocked_verification() -> bool:
        entered.set()
        if not release.wait(5):
            raise AssertionError("test did not release startup verification")
        return True

    first.store.verify_event_chain = blocked_verification  # type: ignore[method-assign]
    start_task = asyncio.create_task(first.start())
    await wait_until(entered.is_set)
    start_task.cancel()
    await asyncio.sleep(0.05)
    assert not start_task.done()
    with pytest.raises(LeaseConflict, match="already owned"):
        await replacement.start()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(start_task, timeout=5)
    try:
        await replacement.start()
    finally:
        await replacement.stop()


def test_process_tree_cleanup_failure_cannot_retry_without_human_decision(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "cleanup-retry", executor=object())
    try:
        task = service.store.create_task(
            TaskSpec(
                idempotency_key="cleanup-retry",
                objective="Require reconciliation after containment failure",
            )
        )
        task = service.store.transition_task_status(
            task.id, TaskStatus.QUEUED, expected_version=task.version
        )
        task = service.store.transition_task_status(
            task.id, TaskStatus.RUNNING, expected_version=task.version
        )
        graph = service.store.create_plan_revision(
            task.id,
            PlanSpec(
                nodes=(
                    NodeSpec(
                        key="execute",
                        agent="worker",
                        retry_policy=RetryPolicy(max_attempts=3),
                    ),
                )
            ),
            expected_task_version=task.version,
            created_by="test",
        )
        run = service.store.enqueue_run(task.id, "execute")
        claim = service.store.claim_next_run("cleanup-worker")
        assert claim is not None and claim.run.id == run.id
        service.store.start_run(
            run.id, claim.lease.token, claim.lease.fencing_token
        )
        failed = service.store.fail_run(
            run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            error_kind="process_tree_cleanup_failed",
            error_message="escaped descendant may still be alive",
        )

        assert service._can_retry(graph.nodes[0], failed, explicit=False) is False
        assert service._can_retry(graph.nodes[0], failed, explicit=True) is True
    finally:
        service.store.close()


@pytest.mark.asyncio
async def test_service_atomically_commits_checkpoint_gate_and_lease_release(tmp_path):
    class PreparedSuspensionExecutor:
        def __init__(self) -> None:
            self.service = None
            self.gate_id = None
            self.claim = None

        async def execute(self, context):
            assert self.service is not None
            self.claim = context.claim
            gate = self.service.store.prepare_run_gate(
                context.claim.run.id,
                context.claim.lease.token,
                context.claim.lease.fencing_token,
                kind=GateKind.QUESTION,
                source_key=f"{context.claim.run.id}:question:two-phase",
                prompt={
                    "title": "Choose a format",
                    "question": "Which output format should be used?",
                    "actions": ["submit", "cancel"],
                },
            )
            self.gate_id = gate.id
            checkpoint_payload = {
                "schema_version": 1,
                "run_id": context.claim.run.id,
                "attempt": context.claim.run.attempt,
                "fencing_token": context.claim.lease.fencing_token,
                "session_id": context.claim.run.session_id,
                "gate_id": gate.id,
                "recovery_disposition": "pending_tools",
                "pending_tool_call_ids": ["question-call"],
                "messages": [{"role": "assistant", "content": "waiting"}],
            }
            ref = self.service.blobs.put_json(checkpoint_payload)
            return ExecutionOutcome(
                status="suspended",
                session_id=context.claim.run.session_id or "hidden",
                gate_id=gate.id,
                output={
                    "engine_checkpoint": {
                        "schema_version": 1,
                        "run_id": context.claim.run.id,
                        "attempt": context.claim.run.attempt,
                        "fencing_token": context.claim.lease.fencing_token,
                        "session_id": context.claim.run.session_id,
                        "gate_id": gate.id,
                        "blob_uri": ref.uri,
                        "blob_sha256": ref.sha256,
                        "pending_tool_call_ids": ["question-call"],
                        "recovery_disposition": "pending_tools",
                    }
                },
            )

    executor = PreparedSuspensionExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "two-phase-gate", executor=executor,
        poll_seconds=0.05
    )
    executor.service = service
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Suspend only after a recoverable checkpoint is durable",
                "domain": "knowledge",
                "acceptance_criteria": ["the selected format is used"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [{"key": "execute", "agent": "worker"}],
                    "edges": [],
                },
            }
        )["id"]
        task = await wait_until(
            lambda: (
                current
                if (current := service.store.get_task(task_id)).status
                is TaskStatus.WAITING_HUMAN
                else None
            )
        )
        assert executor.gate_id is not None and executor.claim is not None
        gate = service.store.get_gate(executor.gate_id)
        run = service.store.get_run(executor.claim.run.id)

        assert task.status is TaskStatus.WAITING_HUMAN
        assert gate.status is GateStatus.OPEN
        assert run.status is RunStatus.WAITING_GATE
        assert service._valid_run_checkpoint(gate) is True
        with pytest.raises(LeaseConflict, match="no longer active|does not hold"):
            service.store.assert_run_lease(
                run.id,
                executor.claim.lease.token,
                executor.claim.lease.fencing_token,
            )
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_cancels_prepared_gate_when_cleanup_fails_before_publication(
    tmp_path,
):
    class PreparedCleanupBreachExecutor:
        def __init__(self) -> None:
            self.service = None
            self.calls = 0
            self.gate_id = None

        async def execute(self, context):
            assert self.service is not None
            self.calls += 1
            gate = self.service.store.prepare_run_gate(
                context.claim.run.id,
                context.claim.lease.token,
                context.claim.lease.fencing_token,
                kind=GateKind.QUESTION,
                source_key=f"{context.claim.run.id}:question:cleanup-breach",
                prompt={"title": "Unsafe to expose", "actions": ["submit"]},
            )
            self.gate_id = gate.id
            return ExecutionOutcome(
                status="failed",
                session_id=context.claim.run.session_id or "hidden",
                gate_id=gate.id,
                error_kind="process_tree_cleanup_failed",
                error_message="late cleanup discovered an escaped descendant",
            )

    executor = PreparedCleanupBreachExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "prepared-cleanup-breach", executor=executor,
        poll_seconds=0.05
    )
    executor.service = service
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Never publish an interaction before containment settles",
                "domain": "knowledge",
                "acceptance_criteria": ["cleanup state is formally reconciled"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {
                            "key": "execute",
                            "agent": "worker",
                            "retry_policy": {
                                "max_attempts": 2,
                                "initial_delay_seconds": 0,
                            },
                        }
                    ],
                    "edges": [],
                },
            }
        )["id"]
        reconciliation = await wait_until(
            lambda: next(
                (
                    gate
                    for gate in service.store.list_gates(
                        task_id, statuses=(GateStatus.OPEN,)
                    )
                    if gate.kind is GateKind.RECONCILIATION
                ),
                None,
            )
        )
        assert executor.gate_id is not None
        unpublished = service.store.get_gate(executor.gate_id)
        runs = service.store.list_runs(task_id)

        assert unpublished.status is GateStatus.CANCELED
        assert unpublished.id != reconciliation.id
        assert executor.calls == 1
        assert len(runs) == 1
        assert runs[0].status is RunStatus.FAILED
        assert runs[0].error_kind == "process_tree_cleanup_failed"
        assert reconciliation.prompt["failed_runs"][0]["error_kind"] == (
            "process_tree_cleanup_failed"
        )
        with pytest.raises(GateConflict, match="already canceled"):
            service.store.resolve_gate(
                unpublished.id,
                GateStatus.APPROVED,
                {"decision": "submit", "response": "too late"},
                resolved_by="racing-user",
                expected_version=unpublished.version,
            )
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_corrupt_checkpoint_never_publishes_prepared_gate_or_prompt(tmp_path):
    class CorruptCheckpointExecutor:
        def __init__(self) -> None:
            self.service = None
            self.gate_id = None

        async def execute(self, context):
            assert self.service is not None
            gate = self.service.store.prepare_run_gate(
                context.claim.run.id,
                context.claim.lease.token,
                context.claim.lease.fencing_token,
                kind=GateKind.QUESTION,
                source_key=f"{context.claim.run.id}:question:corrupt-checkpoint",
                prompt={
                    "title": "Must remain private",
                    "question": "This prompt has no trustworthy checkpoint",
                    "actions": ["submit"],
                },
            )
            self.gate_id = gate.id
            ref = self.service.blobs.put_json(
                {
                    "schema_version": 1,
                    "run_id": "different-run",
                    "attempt": context.claim.run.attempt,
                    "fencing_token": context.claim.lease.fencing_token,
                    "session_id": context.claim.run.session_id,
                    "gate_id": gate.id,
                    "recovery_disposition": "pending_tools",
                    "pending_tool_call_ids": ["corrupt-call"],
                    "messages": [],
                }
            )
            return ExecutionOutcome(
                status="suspended",
                session_id=context.claim.run.session_id or "hidden",
                gate_id=gate.id,
                output={
                    "engine_checkpoint": {
                        "schema_version": 1,
                        "run_id": context.claim.run.id,
                        "attempt": context.claim.run.attempt,
                        "fencing_token": context.claim.lease.fencing_token,
                        "session_id": context.claim.run.session_id,
                        "gate_id": gate.id,
                        "blob_uri": ref.uri,
                        "blob_sha256": ref.sha256,
                        "pending_tool_call_ids": ["corrupt-call"],
                        "recovery_disposition": "pending_tools",
                    }
                },
            )

    executor = CorruptCheckpointExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "corrupt-checkpoint", executor=executor,
        poll_seconds=0.05
    )
    executor.service = service
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Reject a checkpoint bound to another run",
                "domain": "knowledge",
                "acceptance_criteria": ["invalid interactions remain unpublished"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {
                            "key": "execute",
                            "agent": "worker",
                            "retry_policy": {"max_attempts": 1},
                        }
                    ],
                    "edges": [],
                },
            }
        )["id"]
        await wait_until(
            lambda: any(
                run.status is RunStatus.FAILED
                for run in service.store.list_runs(task_id)
            )
        )
        assert executor.gate_id is not None
        internal = service.store.get_gate(executor.gate_id)
        detail = service.task_detail(task_id)

        assert internal.status is GateStatus.CANCELED
        assert internal.published_at is None
        assert service.store.list_gates(
            task_id, include_internal=True
        )[0].id == internal.id
        assert all(item.id != internal.id for item in service.store.list_gates(task_id))
        assert all(item["id"] != internal.id for item in detail["attention"])
        assert all(item["type"] != "gate.prepared" for item in detail["activity"])
        assert any(
            item["type"] == "gate.preparation_aborted"
            for item in detail["activity"]
        )
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_persists_inflight_sibling_after_other_agent_opens_gate(tmp_path):
    class SiblingGateExecutor:
        def __init__(self) -> None:
            self.service = None
            self.right_started = asyncio.Event()
            self.gate_id = None
            self.calls: list[str] = []

        async def execute(self, context):
            assert self.service is not None
            self.calls.append(context.node.key)
            if context.node.key == "right":
                self.right_started.set()
                await wait_until(
                    lambda: self.service.store.get_task(context.task.id).status
                    is TaskStatus.WAITING_HUMAN
                )
                return ExecutionOutcome(
                    status="succeeded",
                    session_id=context.claim.run.session_id or "right",
                    output={"summary": "right sibling completed"},
                )

            await self.right_started.wait()
            gate = self.service.store.prepare_run_gate(
                context.claim.run.id,
                context.claim.lease.token,
                context.claim.lease.fencing_token,
                kind=GateKind.QUESTION,
                source_key=f"{context.claim.run.id}:question:sibling-e2e",
                prompt={"title": "Left waits", "actions": ["submit"]},
            )
            self.gate_id = gate.id
            payload = {
                "schema_version": 1,
                "run_id": context.claim.run.id,
                "attempt": context.claim.run.attempt,
                "fencing_token": context.claim.lease.fencing_token,
                "session_id": context.claim.run.session_id,
                "gate_id": gate.id,
                "recovery_disposition": "pending_tools",
                "pending_tool_call_ids": ["left-call"],
                "messages": [{"role": "assistant", "content": "waiting"}],
            }
            ref = self.service.blobs.put_json(payload)
            return ExecutionOutcome(
                status="suspended",
                session_id=context.claim.run.session_id or "left",
                gate_id=gate.id,
                output={
                    "engine_checkpoint": {
                        "schema_version": 1,
                        "run_id": context.claim.run.id,
                        "attempt": context.claim.run.attempt,
                        "fencing_token": context.claim.lease.fencing_token,
                        "session_id": context.claim.run.session_id,
                        "gate_id": gate.id,
                        "blob_uri": ref.uri,
                        "blob_sha256": ref.sha256,
                        "pending_tool_call_ids": ["left-call"],
                        "recovery_disposition": "pending_tools",
                    }
                },
            )

    executor = SiblingGateExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "sibling-gate-e2e", executor=executor,
        poll_seconds=0.05
    )
    executor.service = service
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Settle concurrent branches without duplicate work",
                "domain": "knowledge",
                "acceptance_criteria": ["both branches remain authoritative"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "left", "agent": "worker", "priority": 20},
                        {"key": "right", "agent": "worker", "priority": 10},
                    ],
                    "edges": [],
                },
            }
        )["id"]
        await wait_until(
            lambda: any(
                run.node_key == "right" and run.status is RunStatus.SUCCEEDED
                for run in service.store.list_runs(task_id)
            )
        )
        runs = service.store.list_runs(task_id)
        assert sorted(executor.calls) == ["left", "right"]
        assert len(runs) == 2
        assert next(run for run in runs if run.node_key == "left").status is (
            RunStatus.WAITING_GATE
        )
        assert next(run for run in runs if run.node_key == "right").status is (
            RunStatus.SUCCEEDED
        )
        assert service.store.get_task(task_id).status is TaskStatus.WAITING_HUMAN
        assert executor.gate_id is not None
        assert service.store.get_gate(executor.gate_id).status is GateStatus.OPEN
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_shutdown_persists_cleanup_breach_and_restart_opens_reconciliation(
    tmp_path,
):
    started = asyncio.Event()

    class CleanupBreachExecutor:
        async def execute(self, context):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return ExecutionOutcome(
                    status="failed",
                    session_id=context.claim.run.session_id or "hidden",
                    error_kind="process_tree_cleanup_failed",
                    error_message="process tree could not be reaped during shutdown",
                )

        @staticmethod
        def interrupt(_run_id):
            return None

    data_dir = tmp_path / "shutdown-cleanup"
    first = OrchestrationService(
        FakeManager(), data_dir, executor=CleanupBreachExecutor(), poll_seconds=0.05
    )
    await first.start()
    task_id = first.create_task(
        {
            "objective": "Do not retry after shutdown loses process containment",
            "domain": "knowledge",
            "acceptance_criteria": ["cleanup is reconciled"],
            "complexity_factors": low_complexity(),
        }
    )["id"]
    await wait_until(started.is_set)
    await first.stop()

    class MustNotRetryExecutor:
        calls = 0

        async def execute(self, _context):
            self.calls += 1
            raise AssertionError("containment failure was automatically retried")

    replacement_executor = MustNotRetryExecutor()
    replacement = OrchestrationService(
        FakeManager(), data_dir, executor=replacement_executor, poll_seconds=0.05
    )
    await replacement.start()
    try:
        gate = await wait_until(
            lambda: next(
                (
                    item
                    for item in replacement.store.list_gates(
                        task_id, statuses=(GateStatus.OPEN,)
                    )
                    if item.kind is GateKind.RECONCILIATION
                ),
                None,
            )
        )
        runs = replacement.store.list_runs(task_id)
        assert gate.prompt["failed_runs"][0]["error_kind"] == (
            "process_tree_cleanup_failed"
        )
        assert len(runs) == 2
        cleanup_run = next(
            run for run in runs if run.error_kind == "process_tree_cleanup_failed"
        )
        assert cleanup_run.status is RunStatus.FAILED
        assert replacement_executor.calls == 0
        assert replacement.store.get_task(task_id).status is TaskStatus.WAITING_HUMAN
    finally:
        await replacement.stop()


@pytest.mark.asyncio
async def test_cleanup_breach_remains_authoritative_when_usage_exceeds_budget(tmp_path):
    class OverBudgetCleanupExecutor:
        calls = 0

        async def execute(self, context):
            self.calls += 1
            return ExecutionOutcome(
                status="failed",
                session_id=context.claim.run.session_id or "hidden",
                error_kind="process_tree_cleanup_failed",
                error_message="cleanup crossed its deadline and left a descendant",
                usage={
                    "model_calls": 10_000,
                    "tool_calls": 0,
                    "tokens": 0,
                    "wall_seconds": 10_000,
                },
            )

    executor = OverBudgetCleanupExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "cleanup-over-budget", executor=executor,
        poll_seconds=0.05
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Keep containment failure authoritative over accounting",
                "domain": "knowledge",
                "acceptance_criteria": ["the cleanup breach is reconciled"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {
                            "key": "execute",
                            "agent": "worker",
                            "retry_policy": {
                                "max_attempts": 2,
                                "initial_delay_seconds": 0,
                            },
                        }
                    ],
                    "edges": [],
                },
            }
        )["id"]
        gate = await wait_until(
            lambda: next(
                (
                    item
                    for item in service.store.list_gates(
                        task_id, statuses=(GateStatus.OPEN,)
                    )
                    if item.kind is GateKind.RECONCILIATION
                ),
                None,
            )
        )
        runs = service.store.list_runs(task_id)
        assert len(runs) == 1
        assert runs[0].status is RunStatus.FAILED
        assert runs[0].error_kind == "process_tree_cleanup_failed"
        assert executor.calls == 1
        assert gate.prompt["failed_runs"][0]["error_kind"] == (
            "process_tree_cleanup_failed"
        )
        usage = next(
            item.payload
            for item in service.store.list_evidence(task_id)
            if item.payload.get("runtime_usage_segment")
        )
        assert usage["usage"]["model_calls"] == 10_000
        assert usage["accounted_usage"]["model_calls"] < 10_000
        assert usage["budget_exceeded"] is True
        assert usage["accounting_error"]["kind"] == "BudgetExceededError"
        # Recovery must consume the bounded ledger value while retaining the full
        # observed usage above, so projection remains available for the operator.
        service._runtime_for_task(task_id, rebuild=True)
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_standard_usage_breach_is_bounded_and_not_automatically_retried(
    tmp_path,
):
    class OverBudgetExecutor:
        calls = 0

        async def execute(self, context):
            self.calls += 1
            return ExecutionOutcome(
                status="succeeded",
                session_id=context.claim.run.session_id or "hidden",
                summary="The provider returned after crossing its allocation.",
                usage={
                    "model_calls": 10_000,
                    "tool_calls": 10_000,
                    "tokens": 10_000_000,
                    "wall_seconds": 10_000,
                },
            )

    executor = OverBudgetExecutor()
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "standard-over-budget",
        executor=executor,
        poll_seconds=0.05,
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Stop a normal run at its durable budget ceiling",
                "domain": "knowledge",
                "acceptance_criteria": ["the budget breach is reconciled"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {
                            "key": "execute",
                            "agent": "worker",
                            "retry_policy": {
                                "max_attempts": 2,
                                "initial_delay_seconds": 0,
                            },
                        }
                    ],
                    "edges": [],
                },
            }
        )["id"]
        gate = await wait_until(
            lambda: next(
                (
                    item
                    for item in service.store.list_gates(
                        task_id, statuses=(GateStatus.OPEN,)
                    )
                    if item.kind is GateKind.RECONCILIATION
                ),
                None,
            )
        )
        runs = service.store.list_runs(task_id)
        assert len(runs) == 1
        assert runs[0].status is RunStatus.FAILED
        assert runs[0].error_kind == "runtime_limit"
        assert executor.calls == 1
        assert gate.prompt["actions"] == ["request_changes", "cancel"]
        usage = next(
            item.payload
            for item in service.store.list_evidence(task_id)
            if item.payload.get("runtime_usage_segment")
        )
        assert usage["usage"]["tokens"] == 10_000_000
        assert usage["accounted_usage"]["tokens"] < 10_000_000
        assert usage["budget_exceeded"] is True
        service._runtime_for_task(task_id, rebuild=True)
    finally:
        await service.stop()


class _TerminationCleanupExecutor:
    def __init__(self, *, hold_cleanup: bool = False) -> None:
        self.started = asyncio.Event()
        self.cleanup_started = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        if not hold_cleanup:
            self.cleanup_release.set()

    async def execute(self, context):
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cleanup_started.set()
            await self.cleanup_release.wait()
            return ExecutionOutcome(
                status="failed",
                session_id=context.claim.run.session_id or "hidden",
                error_kind="process_tree_cleanup_failed",
                error_message="termination discovered an unreaped process tree",
            )

    @staticmethod
    def interrupt(_run_id):
        return None


def _lease_guard_context(run_id: str = "run-termination"):
    return SimpleNamespace(
        claim=SimpleNamespace(
            run=SimpleNamespace(id=run_id, session_id=f"__orch__{run_id}")
        )
    )


@pytest.mark.asyncio
async def test_timeout_settlement_preserves_process_cleanup_breach(tmp_path):
    executor = _TerminationCleanupExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "timeout-settlement", executor=executor
    )
    heartbeat = asyncio.create_task(asyncio.Event().wait())
    try:
        outcome = await service._execute_with_lease_guard(
            _lease_guard_context("run-timeout"), heartbeat, timeout_seconds=0.05
        )
        assert executor.started.is_set()
        assert executor.cleanup_started.is_set()
        assert outcome.error_kind == "process_tree_cleanup_failed"
    finally:
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        service.store.close()
        service.catalog.close()


@pytest.mark.asyncio
async def test_heartbeat_failure_settlement_preserves_process_cleanup_breach(tmp_path):
    executor = _TerminationCleanupExecutor()
    service = OrchestrationService(
        FakeManager(), tmp_path / "heartbeat-settlement", executor=executor
    )

    async def lose_lease():
        await executor.started.wait()
        raise LeaseConflict("run heartbeat lost its lease")

    heartbeat = asyncio.create_task(lose_lease())
    try:
        outcome = await service._execute_with_lease_guard(
            _lease_guard_context("run-heartbeat"), heartbeat, timeout_seconds=5
        )
        assert executor.cleanup_started.is_set()
        assert outcome.error_kind == "process_tree_cleanup_failed"
    finally:
        await asyncio.gather(heartbeat, return_exceptions=True)
        service.store.close()
        service.catalog.close()


@pytest.mark.asyncio
async def test_second_cancel_cannot_abandon_process_cleanup_settlement(tmp_path):
    executor = _TerminationCleanupExecutor(hold_cleanup=True)
    service = OrchestrationService(
        FakeManager(), tmp_path / "double-cancel-settlement", executor=executor
    )
    heartbeat = asyncio.create_task(asyncio.Event().wait())
    guarded = asyncio.create_task(
        service._execute_with_lease_guard(
            _lease_guard_context("run-double-cancel"), heartbeat, timeout_seconds=5
        )
    )
    try:
        await executor.started.wait()
        guarded.cancel()
        await executor.cleanup_started.wait()
        guarded.cancel()
        await asyncio.sleep(0)
        assert not guarded.done()
        executor.cleanup_release.set()
        outcome = await guarded
        assert outcome.error_kind == "process_tree_cleanup_failed"
    finally:
        executor.cleanup_release.set()
        heartbeat.cancel()
        await asyncio.gather(heartbeat, guarded, return_exceptions=True)
        service.store.close()
        service.catalog.close()


def test_lost_run_requires_formal_reconciliation_before_retry(tmp_path):
    service = OrchestrationService(
        FakeManager(), tmp_path / "lost-cleanup-unknown", executor=object()
    )
    try:
        task_id = service.create_task(
            {
                "objective": "Do not retry work whose cleanup status is unknown",
                "domain": "knowledge",
                "acceptance_criteria": ["lost work is reconciled"],
                "complexity_factors": low_complexity(),
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)
        queued = next(
            run
            for run in service.store.list_runs(task_id)
            if run.status is RunStatus.QUEUED
        )
        claim = service.store.claim_next_run("worker-that-disappears", lease_seconds=1)
        assert claim is not None and claim.run.id == queued.id
        service.store.start_run(
            queued.id, claim.lease.token, claim.lease.fencing_token
        )
        assert service.store.reap_expired_leases(
            now=datetime.now(timezone.utc) + timedelta(seconds=2)
        ) == 1
        lost = service.store.get_run(queued.id)
        graph = service.store.get_plan(lost.plan_id)
        node = next(item for item in graph.nodes if item.id == lost.node_id)

        assert lost.status is RunStatus.LOST
        assert service._can_retry(node, lost, explicit=False) is False
        assert service._can_retry(node, lost, explicit=True) is True

        gate = None
        for _ in range(4):
            service._advance_task(task_id)
            gate = next(
                (
                    item
                    for item in service.store.list_gates(
                        task_id, statuses=(GateStatus.OPEN,)
                    )
                    if item.kind is GateKind.RECONCILIATION
                ),
                None,
            )
            if gate is not None:
                break
        assert gate is not None
        lost_payload = next(
            item for item in gate.prompt["failed_runs"] if item["id"] == lost.id
        )
        assert lost_payload["status"] == "failed"
        assert lost_payload["error_kind"] == "lease_expired"
        assert gate.prompt["actions"] == ["retry", "request_changes", "cancel"]
        assert service.store.get_task(task_id).status is TaskStatus.WAITING_HUMAN
        assert len(
            [run for run in service.store.list_runs(task_id) if run.node_id == lost.node_id]
        ) == 1
    finally:
        service.store.close()
        service.catalog.close()


def test_any_join_queues_and_runtime_starts_while_other_branch_is_running(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = service.create_task(
            {
                "objective": "Run either successful branch before joining",
                "domain": "knowledge",
                "acceptance_criteria": ["the join runs"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "fast", "agent": "worker", "priority": 20},
                        {"key": "slow", "agent": "worker", "priority": 10},
                        {
                            "key": "join",
                            "agent": "worker",
                            "join_policy": "any",
                        },
                    ],
                    "edges": [
                        {"from": "fast", "to": "join", "condition": "success"},
                        {"from": "slow", "to": "join", "condition": "success"},
                    ],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)

        first = service.store.claim_next_run("worker-fast")
        assert first is not None and first.run.node_key == "fast"
        service.store.start_run(
            first.run.id,
            first.lease.token,
            first.lease.fencing_token,
        )
        service.store.complete_run(
            first.run.id,
            first.lease.token,
            first.lease.fencing_token,
            output={"summary": "fast branch passed"},
        )

        second = service.store.claim_next_run("worker-slow")
        assert second is not None and second.run.node_key == "slow"
        service.store.start_run(
            second.run.id,
            second.lease.token,
            second.lease.fencing_token,
        )

        service._advance_task(task_id)
        runs = {run.node_key: run for run in service.store.list_runs(task_id)}
        assert runs["slow"].status is RunStatus.RUNNING
        assert runs["join"].status is RunStatus.QUEUED

        runtime = service._runtime_for_task(task_id, rebuild=True)
        join_runtime_id = service._run_runtime_id(runs["join"].id)
        join_runtime = runtime.get(join_runtime_id)
        assert len(join_runtime.spec.dependencies) == 1
        runtime.start(join_runtime_id)
        assert runtime.get(join_runtime_id).status is RuntimeStatus.RUNNING
    finally:
        service.store.close()


class RejectingVerificationExecutor:
    async def execute(self, context):
        output = {"summary": f"{context.profile.role.value} completed"}
        evidence: tuple[dict, ...] = ()
        criteria = {
            criterion: "fail" for criterion in context.task.acceptance_criteria
        }
        if context.profile.role.value == "reviewer":
            output["verdict"] = {
                "status": "fail",
                "summary": "review rejected the result",
                "criteria": criteria,
                "findings": ["acceptance criterion is not met"],
            }
        elif context.profile.role.value == "evaluator":
            evidence = (
                {
                    "kind": "review",
                    "verdict": "fail",
                    "summary": "evidence rejected the result",
                    "criteria": criteria,
                    "findings": ["review failure remains unresolved"],
                },
            )
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or "hidden",
            output=output,
            evidence=evidence,
        )


class AdjudicatingVerificationExecutor:
    async def execute(self, context):
        role = context.profile.role.value
        criteria = {
            criterion: "pass" for criterion in context.task.acceptance_criteria
        }
        output = {"summary": f"{role} completed"}
        if role in {"reviewer", "tester", "evaluator"}:
            status = "fail" if role == "tester" else "pass"
            output["verdict"] = {
                "status": status,
                "summary": (
                    "tester raised a non-criterion report objection"
                    if role == "tester"
                    else "the current candidate satisfies the acceptance contract"
                ),
                "criteria": criteria,
                "findings": (
                    ["report scope wording should be clarified"]
                    if role == "tester"
                    else []
                ),
                "subject": dict(context.subject),
            }
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or "adjudication",
            output=output,
        )


class PassingCompatibilityReplayExecutor:
    async def execute(self, context):
        role = context.profile.role.value
        status = (
            "pass"
            if role not in {"reviewer", "tester", "evaluator"}
            or context.claim.run.attempt > 1
            else "unknown"
        )
        summary = f"{role} attempt {context.claim.run.attempt}: {status}"
        criteria = {
            criterion: status for criterion in context.task.acceptance_criteria
        }
        output = {
            "summary": summary,
            "structured_result": {
                "status": status,
                "summary": summary,
                "criteria": criteria,
            },
            "subscription_runtime": {"runtime_id": "legacy-subscription"},
        }
        if role in {"reviewer", "tester", "evaluator"}:
            output["verdict"] = {
                "status": status,
                "summary": summary,
                "criteria": criteria,
                "subject": dict(context.subject),
            }
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or "compatibility-replay",
            output=output,
        )


class PassingEnvelopeContinuationExecutor(PassingCompatibilityReplayExecutor):
    async def execute(self, context):
        outcome = await super().execute(context)
        if (
            context.profile.role.value == "evaluator"
            and context.claim.run.attempt == 2
        ):
            criteria = {
                criterion: "unknown"
                for criterion in context.task.acceptance_criteria
            }
            summary = "evaluator attempt 2: upstream verdict summaries were omitted"
            output = dict(outcome.output)
            output["summary"] = summary
            output["structured_result"] = {
                "status": "unknown",
                "summary": summary,
                "criteria": criteria,
            }
            output["verdict"] = {
                "status": "unknown",
                "summary": summary,
                "criteria": criteria,
                "subject": dict(context.subject),
            }
            return ExecutionOutcome(
                status="succeeded",
                session_id=outcome.session_id,
                output=output,
            )
        return outcome


def test_legacy_subscription_result_is_backfilled_as_handoff_product(tmp_path):
    service = OrchestrationService(
        FakeManager(), tmp_path / "legacy-product", executor=object()
    )
    try:
        task_id = service.create_task(
            {
                "objective": "Produce a durable analysis",
                "domain": "knowledge",
                "acceptance_criteria": ["the analysis is available"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "execute", "kind": "execute", "agent": "worker"}
                    ],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)
        claim = service.store.claim_next_run("legacy-subscription-worker")
        assert claim is not None
        service.store.start_run(
            claim.run.id, claim.lease.token, claim.lease.fencing_token
        )
        service.store.complete_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            output={
                "summary": "The durable analysis is complete.",
                "structured_result": {
                    "status": "pass",
                    "summary": "The durable analysis is complete.",
                    "criteria": {"the analysis is available": "pass"},
                },
                "subscription_runtime": {"runtime_id": "claude-code-subscription"},
            },
        )

        assert service.store.list_work_products(task_id) == ()
        assert service._repair_legacy_subscription_work_products() == 1
        assert service._repair_legacy_subscription_work_products() == 0
        product = service.store.list_work_products(task_id)[0]
        assert product.run_id is None
        assert product.metadata["source_run_id"] == claim.run.id
        assert product.summary == "The durable analysis is complete."
        assert product.artifact_id and product.artifact_id.startswith("sha256:")
    finally:
        service.store.close()
        service.catalog.close()


@pytest.mark.asyncio
async def test_legacy_handoff_reverification_replays_in_dependency_waves(tmp_path):
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "legacy-reverification",
        executor=PassingCompatibilityReplayExecutor(),
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Produce and verify a recovered subscription result",
                "domain": "knowledge",
                "acceptance_criteria": ["the recovered result is verified"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "execute", "kind": "execute", "agent": "worker"},
                        {
                            "key": "review",
                            "kind": "review",
                            "agent": "reviewer",
                            "retry_policy": {"max_attempts": 1},
                        },
                        {
                            "key": "test",
                            "kind": "test",
                            "agent": "tester",
                            "retry_policy": {"max_attempts": 1},
                        },
                        {
                            "key": "evaluate",
                            "kind": "evaluate",
                            "agent": "evaluator",
                            "retry_policy": {"max_attempts": 1},
                        },
                    ],
                    "edges": [
                        {"from": "execute", "to": "review"},
                        {"from": "execute", "to": "test"},
                        {"from": "review", "to": "evaluate"},
                        {"from": "test", "to": "evaluate"},
                    ],
                },
            }
        )["id"]
        gates = await wait_until(
            lambda: service.store.list_gates(
                task_id, statuses=(GateStatus.OPEN,)
            ),
            timeout=15,
        )
        gate = gates[0]
        assert [
            item.get("id") if isinstance(item, dict) else item
            for item in gate.prompt["actions"]
        ] == ["accept_current", "request_changes", "cancel"]
        assert service._repair_legacy_subscription_work_products() == 4
        assert service._repair_legacy_verification_reconciliation_gates() == 1
        gate = service.store.get_gate(gate.id)
        assert [
            item.get("id") if isinstance(item, dict) else item
            for item in gate.prompt["actions"]
        ] == ["accept_current", "retry", "request_changes", "cancel"]
        assert set(
            gate.prompt["compatibility_retry"]["base_attempts"]
        ) == {"review", "test", "evaluate"}

        service.resolve_gate(
            task_id,
            gate.id,
            decision="retry",
            expected_version=gate.version,
            idempotency_key="retry-legacy-verification-chain",
        )
        runs = await wait_until(
            lambda: (
                rows
                if len(rows := service.store.list_runs(task_id)) == 7
                and all(row.status is RunStatus.SUCCEEDED for row in rows)
                else None
            ),
            timeout=20,
        )
        attempts: dict[str, list] = {}
        for run in runs:
            attempts.setdefault(run.node_key, []).append(run)
        assert [run.attempt for run in attempts["execute"]] == [1]
        assert sorted(run.attempt for run in attempts["review"]) == [1, 2]
        assert sorted(run.attempt for run in attempts["test"]) == [1, 2]
        assert sorted(run.attempt for run in attempts["evaluate"]) == [1, 2]
        evaluator_retry = next(
            run for run in attempts["evaluate"] if run.attempt == 2
        )
        assert evaluator_retry.created_at >= max(
            run.finished_at
            for key in ("review", "test")
            for run in attempts[key]
            if run.attempt == 2 and run.finished_at is not None
        )
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_legacy_reverification_gets_one_bounded_envelope_continuation(tmp_path):
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "legacy-envelope-continuation",
        executor=PassingEnvelopeContinuationExecutor(),
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Produce and verify a recovered subscription result",
                "domain": "knowledge",
                "acceptance_criteria": ["the recovered result is verified"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "execute", "kind": "execute", "agent": "worker"},
                        {
                            "key": "review",
                            "kind": "review",
                            "agent": "reviewer",
                            "retry_policy": {"max_attempts": 1},
                        },
                        {
                            "key": "test",
                            "kind": "test",
                            "agent": "tester",
                            "retry_policy": {"max_attempts": 1},
                        },
                        {
                            "key": "evaluate",
                            "kind": "evaluate",
                            "agent": "evaluator",
                            "retry_policy": {"max_attempts": 1},
                        },
                    ],
                    "edges": [
                        {"from": "execute", "to": "review"},
                        {"from": "execute", "to": "test"},
                        {"from": "review", "to": "evaluate"},
                        {"from": "test", "to": "evaluate"},
                    ],
                },
            }
        )["id"]
        first_gate = (
            await wait_until(
                lambda: service.store.list_gates(
                    task_id, statuses=(GateStatus.OPEN,)
                ),
                timeout=15,
            )
        )[0]
        assert service._repair_legacy_subscription_work_products() == 4
        assert service._repair_legacy_verification_reconciliation_gates() == 1
        first_gate = service.store.get_gate(first_gate.id)
        service.resolve_gate(
            task_id,
            first_gate.id,
            decision="retry",
            expected_version=first_gate.version,
            idempotency_key="retry-before-bounded-envelope-continuation",
        )

        second_gate = (
            await wait_until(
                lambda: next(
                    (
                        gate
                        for gate in service.store.list_gates(
                            task_id, statuses=(GateStatus.OPEN,)
                        )
                        if gate.id != first_gate.id
                    ),
                    None,
                ),
                timeout=20,
            )
        )
        evaluator_retry = max(
            (
                run
                for run in service.store.list_runs(task_id)
                if run.node_key == "evaluate"
            ),
            key=lambda run: run.attempt,
        )
        assert evaluator_retry.attempt == 2
        service.create_operator_work_product(
            task_id,
            {
                "kind": "evaluation",
                "title": "Evaluator attempt 2 result",
                "summary": str((evaluator_retry.output or {}).get("summary") or ""),
                "metadata": {
                    "source": "subscription_structured_result_test",
                    "source_run_id": evaluator_retry.id,
                },
            },
        )

        assert service._repair_legacy_verification_reconciliation_gates() == 1
        second_gate = service.store.get_gate(second_gate.id)
        assert [
            item.get("id") if isinstance(item, dict) else item
            for item in second_gate.prompt["actions"]
        ] == ["accept_current", "retry", "request_changes", "cancel"]
        assert second_gate.prompt["compatibility_retry"] == {
            "reason": "bounded_work_product_envelope_handoff",
            "base_attempts": {
                "evaluate": {
                    "run_id": evaluator_retry.id,
                    "attempt": 2,
                }
            },
        }
        service.resolve_gate(
            task_id,
            second_gate.id,
            decision="retry",
            expected_version=second_gate.version,
            idempotency_key="retry-bounded-envelope-continuation",
        )
        runs = await wait_until(
            lambda: (
                rows
                if len(rows := service.store.list_runs(task_id)) == 8
                and all(row.status is RunStatus.SUCCEEDED for row in rows)
                else None
            ),
            timeout=20,
        )
        assert sorted(
            run.attempt for run in runs if run.node_key == "evaluate"
        ) == [1, 2, 3]
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_evaluator_adjudicates_independent_verifier_dissent(tmp_path):
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "adjudicated-verification",
        executor=AdjudicatingVerificationExecutor(),
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Produce and adjudicate an independently checked result",
                "domain": "knowledge",
                "acceptance_criteria": ["the result is correct"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "execute", "kind": "execute", "agent": "worker"},
                        {"key": "review", "kind": "review", "agent": "reviewer"},
                        {"key": "test", "kind": "test", "agent": "tester"},
                        {
                            "key": "evaluate",
                            "kind": "evaluate",
                            "agent": "evaluator",
                        },
                    ],
                    "edges": [
                        {"from": "execute", "to": "review"},
                        {"from": "execute", "to": "test"},
                        {"from": "review", "to": "evaluate"},
                        {"from": "test", "to": "evaluate"},
                    ],
                },
            }
        )["id"]

        task = await wait_until(
            lambda: (
                current
                if (current := service.store.get_task(task_id)).status
                is TaskStatus.COMPLETED
                else None
            ),
            timeout=15,
        )
        assert task.status is TaskStatus.COMPLETED
        assert service.store.list_gates(
            task_id, statuses=(GateStatus.OPEN,)
        ) == ()
        evaluation = next(
            item
            for item in reversed(service.store.list_evidence(task_id))
            if item.payload.get("title") == "Inter-step evaluation"
        )
        assert evaluation.payload["verdict"] == "proceed"
        assert evaluation.payload["adjudication"]["authority"] == "evaluator"
        assert len(evaluation.payload["adjudication"]["dissenting_run_ids"]) == 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_human_can_accept_current_verification_without_restarting_agents(tmp_path):
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "accepted-current-verification",
        executor=RejectingVerificationExecutor(),
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Produce a result whose current evidence can be accepted",
                "domain": "knowledge",
                "acceptance_criteria": ["the result is correct"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "execute", "kind": "execute", "agent": "worker"},
                        {
                            "key": "review",
                            "kind": "review",
                            "agent": "reviewer",
                            "retry_policy": {"max_attempts": 2},
                        },
                        {
                            "key": "evaluate",
                            "kind": "evaluate",
                            "agent": "evaluator",
                            "retry_policy": {"max_attempts": 2},
                        },
                    ],
                    "edges": [
                        {"from": "execute", "to": "review"},
                        {"from": "review", "to": "evaluate"},
                    ],
                },
            }
        )["id"]
        gate = (
            await wait_until(
                lambda: service.store.list_gates(
                    task_id, statuses=(GateStatus.OPEN,)
                ),
                timeout=15,
            )
        )[0]
        action_ids = [
            item.get("id") if isinstance(item, dict) else item
            for item in gate.prompt["actions"]
        ]
        assert action_ids == [
            "accept_current",
            "retry",
            "request_changes",
            "cancel",
        ]
        original_run_ids = {
            run.id for run in service.store.list_runs(task_id)
        }

        service.resolve_gate(
            task_id,
            gate.id,
            decision="accept_current",
            response="The completed evidence is sufficient for this task.",
            expected_version=gate.version,
            idempotency_key="accept-current-verification",
        )
        await wait_until(
            lambda: (
                current
                if (current := service.store.get_task(task_id)).status
                is TaskStatus.COMPLETED
                else None
            ),
            timeout=15,
        )
        assert {
            run.id for run in service.store.list_runs(task_id)
        } == original_run_ids
        accepted = next(
            item
            for item in reversed(service.store.list_evidence(task_id))
            if item.payload.get("title") == "Final acceptance"
        )
        assert accepted.payload["override"] is True
        assert accepted.payload["verification_override_gate_id"] == gate.id
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_successful_verification_runs_with_fail_verdict_open_reconciliation(tmp_path):
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "data",
        executor=RejectingVerificationExecutor(),
        poll_seconds=0.03,
    )
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Produce and independently verify a result",
                "domain": "knowledge",
                "acceptance_criteria": ["the result is correct"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [
                        {"key": "execute", "kind": "execute", "agent": "worker"},
                        {
                            "key": "review",
                            "kind": "review",
                            "agent": "reviewer",
                            "retry_policy": {"max_attempts": 2},
                        },
                        {
                            "key": "evaluate",
                            "kind": "evaluate",
                            "agent": "evaluator",
                            "retry_policy": {"max_attempts": 2},
                        },
                    ],
                    "edges": [
                        {"from": "execute", "to": "review"},
                        {"from": "review", "to": "evaluate"},
                    ],
                },
            }
        )["id"]

        gates = await wait_until(
            lambda: service.store.list_gates(
                task_id, statuses=(GateStatus.OPEN,)
            ),
            timeout=15,
        )
        runs = service.store.list_runs(task_id)
        assert len(runs) == 3
        assert all(run.status is RunStatus.SUCCEEDED for run in runs)
        assert service.store.get_task(task_id).status is not TaskStatus.ARCHIVED
        assert {gate.kind for gate in gates} == {GateKind.RECONCILIATION}
        gate = gates[0]
        assert [
            item.get("id") if isinstance(item, dict) else item
            for item in gate.prompt["actions"]
        ] == ["accept_current", "retry", "request_changes", "cancel"]

        legacy_prompt = dict(gate.prompt)
        legacy_prompt.update(
            {
                "title": "Execution needs reconciliation",
                "description": "A verifier could not retrieve the candidate.",
                "actions": ["request_changes", "cancel"],
            }
        )
        gate = service.store.amend_task_gate_prompt(
            gate.id,
            legacy_prompt,
            expected_version=gate.version,
            command_id="simulate-legacy-verdict-gate",
        )
        assert service._repair_legacy_verification_reconciliation_gates() == 1
        gate = service.store.get_gate(gate.id)
        assert gate.prompt["title"] == "Verification needs reconciliation"
        assert [
            item.get("id") if isinstance(item, dict) else item
            for item in gate.prompt["actions"]
        ] == ["accept_current", "retry", "request_changes", "cancel"]

        service.resolve_gate(
            task_id,
            gate.id,
            decision="retry",
            expected_version=gate.version,
            idempotency_key="retry-adverse-verification",
        )
        retried = await wait_until(
            lambda: (
                rows
                if len(
                    rows := service.store.list_runs(task_id)
                ) >= 5
                and all(
                    item.status in {RunStatus.SUCCEEDED, RunStatus.FAILED}
                    for item in rows
                )
                else None
            ),
            timeout=15,
        )
        attempts = {}
        for item in retried:
            attempts.setdefault(item.node_key, []).append(item.attempt)
        assert sorted(attempts["execute"]) == [1]
        assert sorted(attempts["review"]) == [1, 2]
        assert sorted(attempts["evaluate"]) == [1, 2]
    finally:
        await service.stop()


def test_plan_budget_is_rejected_before_write_and_explicit_budget_supports_64_nodes(
    tmp_path,
):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        too_wide = {
            "nodes": [{"key": f"root-{index}", "agent": "worker"} for index in range(21)],
            "edges": [],
        }
        with pytest.raises(ValueError, match="model_calls=20.*21 DAG nodes"):
            service.create_task(
                {
                    "idempotency_key": "insufficient-wide-budget",
                    "objective": "A wide task without enough model calls",
                    "domain": "knowledge",
                    "acceptance_criteria": ["all roots finish"],
                    "plan": too_wide,
                    "auto_start": False,
                }
            )
        assert service.store.list_all_tasks() == ()

        maximum_plan = {
            "nodes": [{"key": f"root-{index}", "agent": "worker"} for index in range(64)],
            "edges": [],
        }
        task_id = service.create_task(
            {
                "idempotency_key": "sufficient-maximum-budget",
                "objective": "A maximum-width bounded task",
                "domain": "knowledge",
                "acceptance_criteria": ["all roots finish"],
                "complexity_factors": low_complexity(),
                "budget": {
                    "model_calls": 64,
                    "tool_calls": 64,
                    "tokens": 64_000,
                    "wall_seconds": 6_400,
                },
                "plan": maximum_plan,
                "auto_start": False,
            }
        )["id"]
        graph = service._ensure_plan(service.store.get_task(task_id))
        assert len(graph.nodes) == 64
        assert service.store.get_task(task_id).active_plan_id == graph.plan.id
    finally:
        service.store.close()


def test_replan_same_node_key_uses_a_new_isolated_session(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = service.create_task(
            {
                "objective": "Replan one logical step without sharing role context",
                "domain": "knowledge",
                "acceptance_criteria": ["the result is independently reviewed"],
                "complexity_factors": low_complexity(),
                # Keep the session-identity regression independent from aggregate
                # reservation exhaustion while two plan revisions coexist.
                "budget": {
                    "model_calls": 20,
                    "tool_calls": 20,
                    "tokens": 20_000,
                    "wall_seconds": 2_000,
                    "run_budget": {
                        "model_calls": 5,
                        "tool_calls": 5,
                        "tokens": 5_000,
                        "wall_seconds": 500,
                    },
                },
                "plan": {
                    "nodes": [
                        {"key": "same-step", "kind": "execute", "agent": "worker"}
                    ],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        first_graph = service._ensure_plan(service.store.get_task(task_id))
        first = service._enqueue(
            service.store.get_task(task_id), first_graph, first_graph.nodes[0]
        )

        second_spec = service._plan_from_payload(
            {
                "nodes": [
                    {"key": "same-step", "kind": "review", "agent": "reviewer"}
                ],
                "edges": [],
            }
        )
        fresh = service.store.get_task(task_id)
        second_graph = service.store.create_plan_revision(
            task_id,
            second_spec,
            expected_task_version=fresh.version,
            created_by="test-replanner",
        )
        second = service._enqueue(
            service.store.get_task(task_id), second_graph, second_graph.nodes[0]
        )

        assert first.node_key == second.node_key == "same-step"
        assert first.attempt == second.attempt == 1
        assert first.node_id != second.node_id
        assert first.session_id == f"__orch__{first.node_id}_1"
        assert second.session_id == f"__orch__{second.node_id}_1"
        assert first.session_id != second.session_id
    finally:
        service.store.close()


def test_single_node_retry_receives_remaining_logical_budget(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = service.create_task(
            {
                "objective": "Retry one bounded work unit",
                "domain": "knowledge",
                "acceptance_criteria": ["retry succeeds"],
                "complexity_factors": low_complexity(),
                "budget": {
                    "model_calls": 5,
                    "tool_calls": 10,
                    "tokens": 1_000,
                    "wall_seconds": 100,
                },
                "plan": {
                    "nodes": [
                        {
                            "key": "only",
                            "agent": "worker",
                            "retry_policy": {"max_attempts": 2},
                        }
                    ],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)
        first = service.store.claim_next_run("first-attempt")
        assert first is not None
        service.store.start_run(
            first.run.id, first.lease.token, first.lease.fencing_token
        )
        service.store.add_evidence(
            task_id,
            kind=EvidenceKind.METRIC,
            payload={
                "runtime_usage_segment": True,
                "usage": {
                    "model_calls": 1,
                    "tool_calls": 1,
                    "tokens": 100,
                    "wall_seconds": 2,
                },
            },
            created_by="test",
            run_id=first.run.id,
            plan_id=first.run.plan_id,
            node_id=first.run.node_id,
        )
        service.store.fail_run(
            first.run.id,
            first.lease.token,
            first.lease.fencing_token,
            error_kind="transient",
            error_message="retry me",
        )

        service._advance_task(task_id)
        runs = sorted(service.store.list_runs(task_id), key=lambda run: run.attempt)
        assert [run.attempt for run in runs] == [1, 2]
        retry_runtime = service._runtime_for_task(task_id, rebuild=True).get(
            service._run_runtime_id(runs[1].id)
        )
        assert retry_runtime.spec.budget.model_calls == 4
        assert retry_runtime.spec.budget.tokens == 900
        assert service.store.get_task(task_id).status is TaskStatus.RUNNING
    finally:
        service.store.close()


def test_legacy_over_budget_run_rebuilds_and_cannot_enqueue_zero_budget_retry(
    tmp_path,
):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = service.create_task(
            {
                "objective": "Recover a historical over-budget terminal run",
                "domain": "knowledge",
                "acceptance_criteria": ["recovery remains operable"],
                "complexity_factors": low_complexity(),
                "budget": {
                    "model_calls": 2,
                    "tool_calls": 4,
                    "tokens": 100,
                    "wall_seconds": 10,
                },
                "plan": {
                    "nodes": [
                        {
                            "key": "only",
                            "agent": "worker",
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
        claim = service.store.claim_next_run("legacy-over-budget")
        assert claim is not None
        service.store.start_run(
            claim.run.id, claim.lease.token, claim.lease.fencing_token
        )
        service.store.add_evidence(
            task_id,
            kind=EvidenceKind.METRIC,
            payload={
                "runtime_usage_segment": True,
                # Older releases persisted only the measured value.
                "usage": {
                    "model_calls": 20,
                    "tool_calls": 40,
                    "tokens": 10_000,
                    "wall_seconds": 1_000,
                },
            },
            created_by="legacy-runtime",
            run_id=claim.run.id,
            plan_id=claim.run.plan_id,
            node_id=claim.run.node_id,
        )
        failed = service.store.fail_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            error_kind="runtime_limit",
            error_message="runtime budget exceeded",
        )

        runtime = service._runtime_for_task(task_id, rebuild=True)
        runtime_node = runtime.get(service._run_runtime_id(failed.id))
        assert runtime_node.direct_usage == runtime_node.spec.budget
        graph = service.store.get_plan(failed.plan_id)
        task = service.store.get_task(task_id)
        assert service._can_retry(graph.nodes[0], failed, explicit=True) is False
        assert service._retry_run(task, graph, graph.nodes[0], failed) is False
        assert len(service.store.list_runs(task_id)) == 1
        historical_retry = service.store.enqueue_run(
            task_id,
            graph.nodes[0].key,
            plan_id=graph.plan.id,
            attempt=2,
        )
        assert service._run_payload(historical_retry)["budget"] == {
            "model_calls": 0,
            "tool_calls": 0,
            "tokens": 0,
            "wall_seconds": 0,
        }
    finally:
        service.store.close()


def test_unlimited_mode_reopens_historical_budget_failure_without_old_cap(tmp_path):
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "data",
        executor=object(),
        enforce_runtime_budgets=False,
    )
    try:
        task_id = service.create_task(
            {
                "objective": "Continue after removing runtime budget ceilings",
                "domain": "knowledge",
                "acceptance_criteria": ["the run can continue"],
                "complexity_factors": low_complexity(),
                "budget": {
                    "model_calls": 1,
                    "tool_calls": 1,
                    "tokens": 1,
                    "wall_seconds": 1,
                },
                "plan": {
                    "nodes": [
                        {
                            "key": "only",
                            "agent": "worker",
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
        claim = service.store.claim_next_run("unlimited-budget-recovery")
        assert claim is not None
        service.store.start_run(
            claim.run.id, claim.lease.token, claim.lease.fencing_token
        )
        failed = service.store.fail_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            error_kind="runtime_limit",
            error_message="historical runtime budget exceeded",
        )

        graph = service.store.get_plan(failed.plan_id)
        task = service.store.get_task(task_id)
        runtime = service._runtime_for_task(task_id, rebuild=True)
        runtime_node = runtime.get(service._run_runtime_id(failed.id))

        assert runtime_node.spec.budget.is_unlimited is True
        assert service._can_retry(graph.nodes[0], failed, explicit=True) is True
        assert service._run_payload(failed)["budget"] is None
        assert service._retry_run(task, graph, graph.nodes[0], failed) is True
        assert [run.attempt for run in service.store.list_runs(task_id)] == [1, 2]
        assert service.task_detail(task_id)["runtime_budget_mode"] == "unlimited"
    finally:
        service.store.close()


def test_clarification_accepts_only_whitelisted_nonempty_submission(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        task_id = service.create_task(
            {
                "objective": "Clarify the expected report",
                "domain": "knowledge",
                "acceptance_criteria": [],
                "auto_start": False,
            }
        )["id"]
        service.submit_task(task_id)
        service._advance_task(task_id)
        gate = next(
            gate
            for gate in service.store.list_gates(
                task_id, statuses=(GateStatus.OPEN,)
            )
            if gate.kind is GateKind.CLARIFICATION
        )

        with pytest.raises(ValueError, match="is not allowed"):
            service.resolve_gate(task_id, gate.id, decision="approve")
        with pytest.raises(ValueError, match="requires a non-empty response"):
            service.resolve_gate(task_id, gate.id, decision="submit", response="   ")
        assert service.store.get_gate(gate.id).status is GateStatus.OPEN

        response = "The report must cite the supplied source and include a conclusion"
        service.resolve_gate(
            task_id,
            gate.id,
            decision="submit",
            response=response,
            resolved_by="task-owner",
        )
        service._advance_task(task_id)
        task = service.store.get_task(task_id)
        assert response in task.acceptance_criteria
        clarification = task.input["clarifications"][-1]
        assert {
            key: clarification[key]
            for key in ("response", "resolved_by", "gate_id")
        } == {
            "response": response,
            "resolved_by": "task-owner",
            "gate_id": gate.id,
        }
        assert clarification["applied_at"]
    finally:
        service.store.close()


def test_final_reject_is_atomic_retriable_and_writes_one_decision_evidence(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    fault_connection = None
    try:
        task = service.store.create_task(
            TaskSpec(
                idempotency_key="atomic-final-reject",
                objective="Reject a candidate that does not meet acceptance",
                acceptance_criteria=("the final decision is durable",),
            ),
            command_id="create-atomic-final-reject",
        )
        task = service.store.transition_stage(
            task.id,
            OrchestrationStage.COMPLEXITY_ASSESSMENT,
            expected_version=task.version,
            command_id="atomic-reject-complexity",
        )
        task = service.store.transition_stage(
            task.id,
            OrchestrationStage.PLANNING,
            expected_version=task.version,
            disposition=StageDisposition.SKIPPED,
            command_id="atomic-reject-planning",
        )
        task = service.store.transition_stage(
            task.id,
            OrchestrationStage.EXECUTION_REVIEW_TEST,
            expected_version=task.version,
            command_id="atomic-reject-execution",
        )
        task = service.store.transition_stage(
            task.id,
            OrchestrationStage.INTER_STEP_EVALUATION,
            expected_version=task.version,
            command_id="atomic-reject-evaluation",
        )
        task = service.store.transition_stage(
            task.id,
            OrchestrationStage.FINAL_ACCEPTANCE,
            expected_version=task.version,
            command_id="atomic-reject-final-stage",
        )
        task = service.store.transition_task_status(
            task.id,
            TaskStatus.QUEUED,
            expected_version=task.version,
            command_id="queue-atomic-final-reject",
        )
        task = service.store.transition_task_status(
            task.id,
            TaskStatus.RUNNING,
            expected_version=task.version,
            command_id="run-atomic-final-reject",
        )
        gate = service.store.open_task_gate(
            task.id,
            kind=GateKind.FINAL_ACCEPTANCE,
            source_key=f"{task.id}:atomic-final-acceptance",
            prompt={
                "question": "Accept this candidate?",
                "actions": ["accept", "reject"],
            },
            command_id="open-atomic-final-reject",
        )
        waiting_task = service.store.get_task(task.id)
        assert waiting_task.status is TaskStatus.WAITING_HUMAN

        # Abort precisely when the durable decision evidence is inserted. If gate,
        # task, and evidence are not one SQLite transaction, this leaves a partial
        # rejection that cannot be safely retried with the same command key.
        escaped_task_id = task.id.replace("'", "''")
        fault_connection = service.store.connect()
        fault_connection.execute(
            f"""
            CREATE TRIGGER reject_final_decision_fault
            BEFORE INSERT ON orch_evidence
            WHEN NEW.task_id = '{escaped_task_id}'
            BEGIN
                SELECT RAISE(ABORT, 'injected final decision evidence failure');
            END
            """
        )
        fault_connection.commit()
        command_key = "owner-reject-final-candidate"
        response = "The candidate omits the required durable-decision proof"
        with pytest.raises(
            sqlite3.IntegrityError,
            match="injected final decision evidence failure",
        ):
            service.resolve_gate(
                task.id,
                gate.id,
                decision="reject",
                response=response,
                resolved_by="acceptance-owner",
                idempotency_key=command_key,
            )

        rolled_back_gate = service.store.get_gate(gate.id)
        rolled_back_task = service.store.get_task(task.id)
        assert (rolled_back_gate.status, rolled_back_gate.version) == (
            GateStatus.OPEN,
            gate.version,
        )
        assert (rolled_back_task.status, rolled_back_task.version) == (
            TaskStatus.WAITING_HUMAN,
            waiting_task.version,
        )
        assert not [
            evidence
            for evidence in service.store.list_evidence(task.id)
            if evidence.payload.get("gate_id") == gate.id
        ]

        fault_connection.execute("DROP TRIGGER reject_final_decision_fault")
        fault_connection.commit()
        resolved = service.resolve_gate(
            task.id,
            gate.id,
            decision="reject",
            response=response,
            resolved_by="acceptance-owner",
            idempotency_key=command_key,
        )
        assert resolved.status is GateStatus.REJECTED
        rejected_task = service.store.get_task(task.id)
        assert rejected_task.status is TaskStatus.FAILED
        assert rejected_task.output == {
            "accepted": False,
            "rejected_by": "acceptance-owner",
            "reason": response,
            "gate_id": gate.id,
        }
        assert service.store.stage_history(task.id)[-1].disposition is StageDisposition.FAILED

        # Response-loss replay is a read of the original durable command, not a
        # second decision/evidence append.
        replay = service.resolve_gate(
            task.id,
            gate.id,
            decision="reject",
            response=response,
            resolved_by="acceptance-owner",
            idempotency_key=command_key,
        )
        assert replay.id == gate.id and replay.version == resolved.version
        decisions = [
            evidence
            for evidence in service.store.list_evidence(task.id)
            if evidence.kind is EvidenceKind.DECISION
            and evidence.payload.get("gate_id") == gate.id
        ]
        assert len(decisions) == 1
        assert decisions[0].payload["decision"] == "reject"
        assert decisions[0].payload["response"] == response
        assert decisions[0].payload["resolved_by"] == "acceptance-owner"
        assert service.store.verify_event_chain() is True
    finally:
        if fault_connection is not None:
            fault_connection.close()
        service.store.close()


def test_archived_failed_child_prevents_parent_auto_acceptance(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        parent_id = service.create_task(
            {
                "objective": "Complete parent work only if the child succeeds",
                "domain": "knowledge",
                "acceptance_criteria": ["parent and child both succeed"],
                "complexity_factors": low_complexity(),
                "plan": {
                    "nodes": [{"key": "execute", "agent": "worker"}],
                    "edges": [],
                },
                "auto_start": False,
            }
        )["id"]
        service.submit_task(parent_id)
        service._advance_task(parent_id)
        claim = service.store.claim_next_run("parent-worker")
        assert claim is not None and claim.run.task_id == parent_id
        service.store.start_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
        )
        service.store.complete_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            output={"summary": "parent execution succeeded"},
        )

        # Persist the child under the Agent attempt that delegated it. This mirrors
        # production hierarchy reconstruction and keeps the child's reservation inside
        # the parent run budget rather than beside that run on the task container.
        child = service.store.create_task(
            TaskSpec(
                idempotency_key="failed-child",
                objective="Child work that fails",
                parent_task_id=parent_id,
                parent_node_id=claim.run.node_id,
                input={"_runtime": {"parent_run_id": claim.run.id}},
            )
        )
        child = service.store.transition_task_status(
            child.id, TaskStatus.QUEUED, expected_version=child.version
        )
        child = service.store.transition_task_status(
            child.id, TaskStatus.RUNNING, expected_version=child.version
        )
        child = service.store.transition_task_status(
            child.id, TaskStatus.FAILED, expected_version=child.version
        )
        child = service.archive_task(child.id)
        assert child.status is TaskStatus.ARCHIVED
        assert child.output["archived_from"] == "failed"

        service._advance_task(parent_id)

        parent = service.store.get_task(parent_id)
        gates = service.store.list_gates(parent_id, statuses=(GateStatus.OPEN,))
        assert parent.status is not TaskStatus.ARCHIVED
        assert gates
        assert gates[-1].kind in {
            GateKind.RECONCILIATION,
            GateKind.FINAL_ACCEPTANCE,
        }
        assert not any(
            evidence.payload.get("accepted") is True
            for evidence in service.store.list_evidence(parent_id)
        )
    finally:
        service.store.close()


def test_internal_descendant_and_runtime_scans_are_not_capped_at_100_tasks(tmp_path):
    service = OrchestrationService(FakeManager(), tmp_path / "data", executor=object())
    try:
        root = service.store.create_task(
            TaskSpec(idempotency_key="scan-root", objective="Scan root")
        )
        for index in range(104):
            service.store.create_task(
                TaskSpec(
                    idempotency_key=f"scan-filler-{index}",
                    objective=f"Filler task {index}",
                )
            )
        child = service.store.create_task(
            TaskSpec(
                idempotency_key="scan-child-after-page-one",
                objective="Child after the first control-plane page",
                parent_task_id=root.id,
            )
        )

        first_page_ids = {task.id for task in service.store.list_tasks()}
        assert len(first_page_ids) == 100
        assert child.id not in first_page_ids
        assert len(service._all_tasks()) == 106
        assert [task.id for task in service._descendants(root.id)] == [child.id]

        runtime = service._rebuild_runtime_tree(root.id)
        assert runtime.get(service._task_runtime_id(child.id)).spec.metadata["task_id"] == child.id
    finally:
        service.store.close()
