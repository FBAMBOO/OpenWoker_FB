from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from coworker.orchestration.executor import ExecutionOutcome
from coworker.orchestration.models import GateKind, GateStatus, TaskStatus
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


async def wait_until(predicate, *, timeout: float = 15.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(0.03)
    raise AssertionError("condition was not reached before timeout")


def open_gate(service: OrchestrationService, task_id: str, kind: GateKind):
    return next(
        (
            gate
            for gate in service.store.list_gates(
                task_id, statuses=(GateStatus.OPEN,)
            )
            if gate.kind is kind
        ),
        None,
    )


def publications(service: OrchestrationService, task_id: str):
    return [
        item
        for item in service.store.list_evidence(task_id)
        if item.payload.get("action") == "workspace_published"
    ]


class AcceptanceExecutor:
    """Deterministic executor for candidate/publication contract tests."""

    def __init__(
        self,
        *,
        reviewer_status: str = "pass",
        reviewer_criterion_status: str | None = None,
        mutate_candidate_during_review: bool = False,
    ) -> None:
        self.reviewer_status = reviewer_status
        self.reviewer_criterion_status = reviewer_criterion_status
        self.mutate_candidate_during_review = mutate_candidate_during_review
        self.service: OrchestrationService | None = None
        self._candidate_mutated = False

    async def execute(self, context):
        role = context.profile.role.value
        if role == "worker" and context.workspace is not None:
            (context.workspace / "orchestrated.txt").write_text(
                "candidate-result", encoding="utf-8"
            )

        if (
            role == "reviewer"
            and self.mutate_candidate_during_review
            and not self._candidate_mutated
        ):
            assert self.service is not None
            snapshot = self.service._ensure_task_snapshot(context.task)
            assert snapshot is not None
            (snapshot.candidate / "out-of-band-change.txt").write_text(
                "changed after the reviewer captured its subject", encoding="utf-8"
            )
            self._candidate_mutated = True

        output = {"summary": f"{role} completed"}
        if role in {"reviewer", "tester", "evaluator", "scorer"}:
            status = self.reviewer_status if role == "reviewer" else "pass"
            criterion_status = (
                self.reviewer_criterion_status
                if role == "reviewer" and self.reviewer_criterion_status is not None
                else status
            )
            output["verdict"] = {
                "status": status,
                "summary": f"{role} verdict is {status}",
                "criteria": {
                    criterion: criterion_status
                    for criterion in context.task.acceptance_criteria
                },
                # The service is authoritative for this value and overwrites it with
                # the subject captured immediately before this run starts.
                "subject": dict(context.subject),
            }
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or "test-session",
            output=output,
        )


async def start_code_task(
    service: OrchestrationService,
    *,
    objective: str = "Produce an independently verified workspace change",
) -> str:
    task_id = service.create_task(
        {
            "objective": objective,
            "domain": "code",
            "acceptance_criteria": ["orchestrated.txt contains candidate-result"],
        }
    )["id"]
    gate = await wait_until(
        lambda: open_gate(service, task_id, GateKind.PLAN_APPROVAL)
    )
    service.resolve_gate(task_id, gate.id, decision="approve")
    return task_id


@pytest.mark.asyncio
async def test_review_failure_never_mutates_the_formal_workspace(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    (workspace / "baseline.txt").write_text("original", encoding="utf-8")
    executor = AcceptanceExecutor(
        reviewer_status="fail", reviewer_criterion_status="fail"
    )
    service = OrchestrationService(
        FakeManager(workspace),
        tmp_path / "data",
        executor=executor,
        poll_seconds=0.03,
    )
    executor.service = service
    await service.start()
    try:
        task_id = await start_code_task(service)
        reconciliation = await wait_until(
            lambda: open_gate(service, task_id, GateKind.RECONCILIATION)
        )

        task = service.store.get_task(task_id)
        candidate = service._ensure_task_snapshot(task)
        assert candidate is not None
        assert (candidate.candidate / "orchestrated.txt").read_text(
            encoding="utf-8"
        ) == "candidate-result"
        assert (workspace / "baseline.txt").read_text(encoding="utf-8") == "original"
        assert not (workspace / "orchestrated.txt").exists()
        assert publications(service, task_id) == []
        review = next(
            report
            for report in reconciliation.prompt["verification"]
            if report["role"] == "reviewer"
        )
        assert review["status"] == "fail"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_candidate_is_published_once_and_only_after_final_acceptance(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    executor = AcceptanceExecutor()
    service = OrchestrationService(
        FakeManager(workspace),
        tmp_path / "data",
        executor=executor,
        poll_seconds=0.03,
    )
    executor.service = service
    await service.start()
    try:
        task_id = await start_code_task(service)
        gate = await wait_until(
            lambda: open_gate(service, task_id, GateKind.FINAL_ACCEPTANCE)
        )

        assert not (workspace / "orchestrated.txt").exists()
        assert publications(service, task_id) == []

        service.resolve_gate(task_id, gate.id, decision="accept")
        await wait_until(
            lambda: service.store.get_task(task_id).status is TaskStatus.ARCHIVED
        )

        assert (workspace / "orchestrated.txt").read_text(
            encoding="utf-8"
        ) == "candidate-result"
        assert len(publications(service, task_id)) == 1

        # Re-driving a terminal task must not publish the accepted revision twice.
        service._advance_task(task_id)
        assert len(publications(service, task_id)) == 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_restart_reconciles_publish_completed_before_audit_append(
    tmp_path, monkeypatch
):
    workspace = tmp_path / "project"
    workspace.mkdir()
    data_dir = tmp_path / "data"
    executor = AcceptanceExecutor()
    service = OrchestrationService(
        FakeManager(workspace),
        data_dir,
        executor=executor,
        poll_seconds=0.03,
    )
    executor.service = service
    await service.start()
    recovered: OrchestrationService | None = None
    try:
        task_id = await start_code_task(service)
        gate = await wait_until(
            lambda: open_gate(service, task_id, GateKind.FINAL_ACCEPTANCE)
        )
        # Stop automatic coordination so the fault is placed exactly between the
        # filesystem journal commit and the SQLite evidence append. Keep the leader
        # heartbeat/store alive for the manual crash injection: a fully stopped
        # service is deliberately tombstoned and must never accept late writes.
        service._closing = True
        control_tasks = [
            item for item in (service._loop_task, service._outbox_task) if item
        ]
        for item in control_tasks:
            item.cancel()
        await asyncio.gather(*control_tasks, return_exceptions=True)
        service._loop_task = None
        service._outbox_task = None
        service.resolve_gate(task_id, gate.id, decision="accept")
        real_add_evidence = service.store.add_evidence

        def fail_publication_evidence(*args, **kwargs):
            if dict(kwargs.get("payload") or {}).get("action") == "workspace_published":
                raise RuntimeError("injected crash after workspace publication")
            return real_add_evidence(*args, **kwargs)

        monkeypatch.setattr(service.store, "add_evidence", fail_publication_evidence)
        with pytest.raises(RuntimeError, match="injected crash"):
            service._advance_task(task_id)

        assert (workspace / "orchestrated.txt").read_text(
            encoding="utf-8"
        ) == "candidate-result"
        assert publications(service, task_id) == []
        delivered_before = [
            item
            for item in service.workspaces.journal()
            if item.status == "delivered"
            and item.snapshot_id == service._task_snapshot_id(task_id)
        ]
        assert len(delivered_before) == 1
        sealed = next(
            item
            for item in service.store.list_evidence(task_id)
            if item.payload.get("action") == "workspace_publication_sealed"
        )
        assert delivered_before[0].candidate_manifest_sha256 == str(
            sealed.payload["subject"]["manifest_sha256"]
        )
        assert delivered_before[0].patch_sha256 == str(
            sealed.payload["subject"]["patch_sha256"]
        )
        await service.stop()

        restarted_executor = AcceptanceExecutor()
        recovered = OrchestrationService(
            FakeManager(workspace),
            data_dir,
            executor=restarted_executor,
            poll_seconds=0.03,
        )
        restarted_executor.service = recovered
        await recovered.start()
        await wait_until(
            lambda: recovered.store.get_task(task_id).status is TaskStatus.ARCHIVED
        )

        recovered_publications = publications(recovered, task_id)
        assert len(recovered_publications) == 1
        assert recovered_publications[0].payload["recovered"] is True
        assert recovered_publications[0].payload["subject"] == sealed.payload["subject"]
        assert (
            recovered_publications[0].payload["receipt"]["transaction_id"]
            == delivered_before[0].transaction_id
        )
        delivered_after = [
            item
            for item in recovered.workspaces.journal()
            if item.status == "delivered"
            and item.snapshot_id == recovered._task_snapshot_id(task_id)
        ]
        assert len(delivered_after) == 1
    finally:
        if recovered is not None:
            await recovered.stop()
        # ``stop`` is idempotent enough for a service whose loop is already closed;
        # this also covers setup failures before the deliberate stop above.
        await service.stop()


@pytest.mark.asyncio
async def test_final_rejection_is_terminal_and_never_publishes_candidate(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    executor = AcceptanceExecutor()
    service = OrchestrationService(
        FakeManager(workspace),
        tmp_path / "data",
        executor=executor,
        poll_seconds=0.03,
    )
    executor.service = service
    await service.start()
    try:
        task_id = await start_code_task(service)
        gate = await wait_until(
            lambda: open_gate(service, task_id, GateKind.FINAL_ACCEPTANCE)
        )
        assert not (workspace / "orchestrated.txt").exists()

        service.resolve_gate(
            task_id,
            gate.id,
            decision="reject",
            response="The candidate is not approved for publication",
            resolved_by="acceptance-owner",
        )

        task = service.store.get_task(task_id)
        assert task.status is TaskStatus.FAILED
        assert task.output["accepted"] is False
        assert task.output["rejected_by"] == "acceptance-owner"
        assert not (workspace / "orchestrated.txt").exists()
        assert publications(service, task_id) == []
    finally:
        await service.stop()


@pytest.mark.parametrize(
    ("late_kind", "late_agent"),
    [
        pytest.param("execute", "worker", id="late-worker"),
        pytest.param("integrate", "integrator", id="late-integrator"),
        pytest.param("noop", "worker", id="worker-disguised-as-noop"),
    ],
)
def test_plan_rejects_candidate_producer_after_formal_verification(
    tmp_path, late_kind, late_agent
):
    workspace = tmp_path / "project"
    workspace.mkdir()
    service = OrchestrationService(
        FakeManager(workspace), tmp_path / "data", executor=object()
    )
    try:
        with pytest.raises(ValueError, match="verification node .* candidate-producing"):
            service.create_task(
                {
                    "objective": "Reject a post-verification mutation bypass",
                    "domain": "code",
                    "acceptance_criteria": ["all mutations were independently verified"],
                    "plan": {
                        "nodes": [
                            {
                                "key": "execute",
                                "kind": "execute",
                                "agent": "worker",
                            },
                            {
                                "key": "review",
                                "kind": "review",
                                "agent": "reviewer",
                            },
                            {
                                "key": "test",
                                "kind": "test",
                                "agent": "tester",
                            },
                            {
                                "key": "late",
                                "kind": late_kind,
                                "agent": late_agent,
                            },
                            {
                                "key": "evaluate",
                                "kind": "evaluate",
                                "agent": "evaluator",
                            },
                        ],
                        "edges": [
                            {"from": "execute", "to": "review"},
                            {"from": "review", "to": "test"},
                            {"from": "test", "to": "late"},
                            {"from": "late", "to": "evaluate"},
                        ],
                    },
                    "auto_start": False,
                }
            )
        assert service.store.list_all_tasks() == ()
    finally:
        service.store.close()


@pytest.mark.asyncio
async def test_empty_verification_set_cannot_auto_pass_acceptance(tmp_path):
    executor = AcceptanceExecutor()
    service = OrchestrationService(
        FakeManager(),
        tmp_path / "data",
        executor=executor,
        poll_seconds=0.03,
    )
    executor.service = service
    await service.start()
    try:
        task_id = service.create_task(
            {
                "objective": "Do not treat an empty verification set as passing",
                "domain": "knowledge",
                "acceptance_criteria": ["the result was independently verified"],
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
                            "kind": "execute",
                            "agent": "worker",
                        }
                    ],
                    "edges": [],
                },
            }
        )["id"]
        gate = await wait_until(
            lambda: open_gate(service, task_id, GateKind.FINAL_ACCEPTANCE)
        )

        assert gate.prompt["verification"] == []
        assert gate.prompt["criteria"] == {
            "the result was independently verified": "unknown"
        }
        action_ids = {
            action if isinstance(action, str) else action["id"]
            for action in gate.prompt["actions"]
        }
        assert "accept" not in action_ids
        assert "override_accept" in action_ids
        detail_gate = next(
            item
            for item in service.task_detail(task_id)["attention"]
            if item["id"] == gate.id
        )
        override = next(
            item for item in detail_gate["actions"] if item["id"] == "override_accept"
        )
        assert override["requires_response"] is True
        assert service.store.get_task(task_id).status is TaskStatus.WAITING_HUMAN
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_pass_verdict_with_failed_criterion_is_normalized_to_failure(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    executor = AcceptanceExecutor(
        reviewer_status="pass", reviewer_criterion_status="fail"
    )
    service = OrchestrationService(
        FakeManager(workspace),
        tmp_path / "data",
        executor=executor,
        poll_seconds=0.03,
    )
    executor.service = service
    await service.start()
    try:
        task_id = await start_code_task(service)
        gate = await wait_until(
            lambda: open_gate(service, task_id, GateKind.RECONCILIATION)
        )

        reviewer_run = next(
            run
            for run in service.store.list_runs(task_id)
            if run.node_key == "review"
        )
        assert reviewer_run.output["verdict"]["status"] == "fail"
        reviewer_report = next(
            report
            for report in gate.prompt["verification"]
            if report["role"] == "reviewer"
        )
        assert reviewer_report["status"] == "fail"
        assert not (workspace / "orchestrated.txt").exists()
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_verdict_for_an_old_candidate_hash_opens_reconciliation(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    executor = AcceptanceExecutor(mutate_candidate_during_review=True)
    service = OrchestrationService(
        FakeManager(workspace),
        tmp_path / "data",
        executor=executor,
        poll_seconds=0.03,
    )
    executor.service = service
    await service.start()
    try:
        task_id = await start_code_task(service)
        gate = await wait_until(
            lambda: open_gate(service, task_id, GateKind.RECONCILIATION)
        )

        reviewer_report = next(
            report
            for report in gate.prompt["verification"]
            if report["role"] == "reviewer"
        )
        assert reviewer_report["subject_matches"] is False
        assert reviewer_report["status"] == "unknown"
        assert "current candidate revision" in reviewer_report["findings"][-1]
        assert reviewer_report["subject"]["manifest_sha256"] != next(
            report["subject"]["manifest_sha256"]
            for report in gate.prompt["verification"]
            if report["role"] == "tester"
        )
        assert not (workspace / "orchestrated.txt").exists()
        assert publications(service, task_id) == []
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_accepting_a_gate_for_a_stale_candidate_does_not_publish(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    executor = AcceptanceExecutor()
    service = OrchestrationService(
        FakeManager(workspace),
        tmp_path / "data",
        executor=executor,
        poll_seconds=0.03,
    )
    executor.service = service
    await service.start()
    try:
        task_id = await start_code_task(service)
        old_gate = await wait_until(
            lambda: open_gate(service, task_id, GateKind.FINAL_ACCEPTANCE)
        )
        old_subject = dict(old_gate.prompt["subject"])

        task = service.store.get_task(task_id)
        snapshot = service._ensure_task_snapshot(task)
        assert snapshot is not None
        (snapshot.candidate / "changed-after-gate.txt").write_text(
            "this revision was never accepted", encoding="utf-8"
        )

        service.resolve_gate(task_id, old_gate.id, decision="accept")
        new_gate = await wait_until(
            lambda: next(
                (
                    gate
                    for gate in service.store.list_gates(
                        task_id, statuses=(GateStatus.OPEN,)
                    )
                    if gate.kind is GateKind.FINAL_ACCEPTANCE
                    and gate.id != old_gate.id
                ),
                None,
            )
        )

        assert (
            new_gate.prompt["subject"]["manifest_sha256"]
            != old_subject["manifest_sha256"]
        )
        action_ids = {
            action if isinstance(action, str) else action["id"]
            for action in new_gate.prompt["actions"]
        }
        assert "accept" not in action_ids
        assert "override_accept" in action_ids
        assert not (workspace / "orchestrated.txt").exists()
        assert publications(service, task_id) == []
    finally:
        await service.stop()


def test_knowledge_task_does_not_implicitly_inherit_manager_workspace(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    (workspace / "baseline.txt").write_text("unchanged", encoding="utf-8")
    service = OrchestrationService(
        FakeManager(workspace), tmp_path / "data", executor=object()
    )
    try:
        task_id = service.create_task(
            {
                "objective": "Answer a question without entering the code workspace",
                "domain": "knowledge",
                "acceptance_criteria": ["the answer is independently evaluated"],
                "auto_start": False,
            }
        )["id"]
        task = service.store.get_task(task_id)

        assert task.workspace is None
        assert service._ensure_task_snapshot(task) is None
        assert (workspace / "baseline.txt").read_text(encoding="utf-8") == "unchanged"
        assert list(workspace.iterdir()) == [workspace / "baseline.txt"]
    finally:
        service.store.close()


def test_explicit_writable_workspace_is_never_low_risk_for_knowledge_domain(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    service = OrchestrationService(
        FakeManager(), tmp_path / "data", executor=object()
    )
    try:
        task_id = service.create_task(
            {
                "objective": "Write a researched decision into the supplied project",
                "domain": "knowledge",
                "workspace": str(workspace),
                "acceptance_criteria": ["the decision is independently verified"],
                "complexity_factors": {
                    "scope": 1,
                    "uncertainty": 1,
                    "dependencies": 0,
                    "side_effects": 0,
                    "parallelism": 0,
                    "verification": 1,
                },
                "auto_start": False,
            }
        )["id"]
        task = service.store.get_task(task_id)

        assert task.workspace == str(workspace.resolve())
        assert task.risk_tier.value == "medium"
        assert task.policy["plan_approval_required"] is True
        assert task.policy["final_acceptance_required"] is True
        assert task.policy["require_review"] is True
        assert task.policy["require_tests"] is True
    finally:
        service.store.close()
