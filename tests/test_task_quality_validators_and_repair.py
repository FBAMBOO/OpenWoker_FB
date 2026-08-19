from __future__ import annotations

import hashlib
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.errors import ConflictError
from coworker.orchestration.models import NodeSpec, OrchestrationStage, PlanSpec, TaskSpec
from coworker.orchestration.quality.artifacts import ArtifactService
from coworker.orchestration.quality.contract_compiler import ContractCompiler
from coworker.orchestration.quality.contracts import ContractRepository
from coworker.orchestration.quality.evidence import EvidenceLedger
from coworker.orchestration.quality.models import (
    ArtifactVersionStatus,
    CoverageResult,
    EvidenceSupport,
    FindingInput,
    Severity,
    TriState,
)
from coworker.orchestration.quality.findings import materialize_finding
from coworker.orchestration.quality.repair import RepairCoordinator, RepairExhausted
from coworker.orchestration.quality.facade import TaskQualityFacade
from coworker.orchestration.quality.plan_compiler import compile_strategy_plan
from coworker.orchestration.quality.repo_inventory import RepositoryInventoryService
from coworker.orchestration.quality.repository_resolver import RepositoryResolver
from coworker.orchestration.quality.repository_snapshot import RepositorySnapshotService
from coworker.orchestration.quality.strategy_selector import StrategySelector
from coworker.orchestration.quality.state_machine import WorkflowEvent, apply_workflow_event
from coworker.orchestration.quality.validators import DeterministicValidatorEngine, ValidationInputs
from coworker.orchestration.quality.validators.engine import FOCUSED_QUESTION_GATE_IDS
from coworker.orchestration.store import OrchestrationStore
from coworker.orchestration.service import OrchestrationService


PROMPT = (
    "Read-only analyze the current Fabric/dbt project architecture. Identify entry, models, "
    "macros, tests, seeds, snapshots and deployment relationships. Produce a Markdown report "
    "with file evidence and do not modify source files."
)
AREAS = ("entry", "models", "macros", "tests", "seeds", "snapshots", "deployment")


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True,
        encoding="utf-8",
    ).stdout.strip()


def _repo(root: Path) -> None:
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.email", "quality@example.test")
    _git(root, "config", "user.name", "Quality Test")
    files = {
        "dbt_project.yml": "name: fixture\nprofile: fixture\n",
        "models/a.sql": "select 1 as id\n",
        "models/b.sql": "select * from {{ ref('a') }}\n",
        "macros/m.sql": "{% macro m() %}1{% endmacro %}\n",
        "tests/t.sql": "select * from {{ ref('b') }} where id < 0\n",
        "seeds/s.csv": "id\n1\n",
        "snapshots/s.sql": "{% snapshot s %}select * from {{ ref('b') }}{% endsnapshot %}\n",
        ".github/workflows/deploy.yml": "name: deploy\nrun-name: dbt run\n",
    }
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    _git(root, "update-ref", "refs/remotes/origin/main", "HEAD")


def _report(*, include_macros: bool = True) -> bytes:
    sections = [
        "Baseline and Method", "Architecture Overview", "Entry", "Models",
        *( ["Macros"] if include_macros else [] ),
        "Tests", "Seeds", "Snapshots", "Deployment", "Relationships", "Risks", "Limitations",
    ]
    return ("\n\n".join(f"# {section}\nEvidence-backed {section}." for section in sections) + "\n").encode()


def _upload(service, task_id, run_id, contract, content, *, parent=None):
    deliverable = next(item for item in contract.deliverables if item.primary)
    created = service.create(
        task_id,
        logical_deliverable_id=deliverable.id,
        filename=deliverable.filename,
        mime_type=deliverable.mime_type,
        run_id=run_id,
        parent_artifact_id=parent,
    )
    service.append(created["upload_id"], sequence=0, content=content, chunk_hash=_digest(content))
    artifact = service.complete(created["upload_id"], expected_sha256=_digest(content))
    with service.store._read() as connection:
        workflow = connection.execute(
            "SELECT workflow_status FROM orch_tasks WHERE id=?", (task_id,)
        ).fetchone()["workflow_status"]
    if workflow == "running":
        apply_workflow_event(
            service.store,
            task_id=task_id,
            event=WorkflowEvent.CANDIDATE_CREATED,
        )
    return artifact


@pytest.fixture
def validation_context(tmp_path):
    repo = tmp_path / "repo"
    _repo(repo)
    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(store, ContentAddressedBlobStore(tmp_path / "state" / "blobs"))
    snapshots = RepositorySnapshotService(store, artifacts)
    task = store.create_task(TaskSpec(idempotency_key="validators", objective=PROMPT))
    plan = store.create_plan_revision(
        task.id,
        PlanSpec(nodes=(NodeSpec(key="producer", agent="worker"), NodeSpec(key="reviewer", agent="reviewer", kind="review")), edges=()),
        expected_task_version=task.version,
        created_by="test",
    )
    with store._write() as connection:
        connection.execute("UPDATE orch_tasks SET status='queued' WHERE id=?", (task.id,))
    producer = store.enqueue_run(task.id, "producer", plan_id=plan.plan.id)
    reviewer = store.enqueue_run(task.id, "reviewer", plan_id=plan.plan.id)
    contracts = ContractRepository(store)
    draft = ContractCompiler().compile(task_id=task.id, objective=PROMPT).contract
    contracts.save_draft(draft)
    contract = contracts.publish(draft.id, if_match=draft.content_hash)
    snapshot = snapshots.freeze(
        task_id=task.id,
        resolution=RepositoryResolver().resolve(repo, objective=PROMPT),
    )
    inventories = RepositoryInventoryService(store, artifacts, snapshots)
    inventory = inventories.build(snapshot.id)
    selector = StrategySelector(store)
    selector.publish(
        selector.select(contract=contract, snapshot=snapshot, inventory=inventory)
    )
    apply_workflow_event(
        store, task_id=task.id, event=WorkflowEvent.START_REQUESTED
    )
    ledger = EvidenceLedger(store, snapshots)
    try:
        yield store, artifacts, snapshots, inventories, ledger, task, producer, reviewer, contract, snapshot, inventory
    finally:
        store.close()


def _complete_inputs(context, *, include_macros=True, partial_review=False):
    store, artifacts, snapshots, _, ledger, task, producer, reviewer, contract, snapshot, inventory = context
    artifact = _upload(artifacts, task.id, producer.id, contract, _report(include_macros=include_macros))
    evidence_ids = []
    coverage = []
    area_paths = {
        "entry": "dbt_project.yml", "models": "models/b.sql", "macros": "macros/m.sql",
        "tests": "tests/t.sql", "seeds": "seeds/s.csv", "snapshots": "snapshots/s.sql",
        "deployment": ".github/workflows/deploy.yml",
    }
    for area, path in area_paths.items():
        claim = ledger.create_claim(
            task_id=task.id, artifact_id=artifact.id, section_id=area,
            text=f"{area} is present in the frozen repository", claim_type="fact",
            severity=Severity.HIGH if area in {"models", "deployment"} else Severity.INFO,
            requirement_ids=("req-required-domains",),
        )
        evidence = ledger.create_file_evidence(
            claim_id=claim.id, snapshot_id=snapshot.id, path=path, line_start=1, line_end=1,
            support=EvidenceSupport.SUPPORTS, created_by_run_id=producer.id,
        )
        evidence_ids.append(evidence.id)
        coverage.append(CoverageResult(
            requirement_id="req-required-domains", area=area, status=TriState.PASS,
            claim_ids=(claim.id,), evidence_count=1, notes="covered", validator_id="fixture",
        ))
    limitation = ledger.create_claim(
        task_id=task.id, artifact_id=artifact.id, section_id="limitations",
        text="Static analysis cannot prove runtime state", claim_type="limitation",
    )
    ledger.create_file_evidence(
        claim_id=limitation.id, snapshot_id=snapshot.id, path="dbt_project.yml",
        line_start=1, line_end=1, support="context", created_by_run_id=producer.id,
    )
    absence = ledger.create_claim(
        task_id=task.id, artifact_id=artifact.id, section_id="limitations",
        text="No second dbt project marker was observed", claim_type="absence",
    )
    ledger.create_negative_evidence(
        claim_id=absence.id, query="dbt_project.yml", tool_version="fixture@1",
        scope_paths=("dbt_project.yml",), excluded_paths=(), result_count=1,
        query_result_hash=_digest(b"dbt_project.yml"), limitations=("Frozen manifest scope only",),
    )
    ledger.record_inventory_metric(
        inventory_id=inventory.id, name="resource_files", value=7, unit="files",
        query_key="fixture-query", subtotals={area: 1 for area in AREAS}, reconciles_to=7,
    )
    receipt = artifacts.bind_candidate(
        run_id=reviewer.id, artifact_id=artifact.id, expected_sha256=artifact.sha256,
        verifier_profile_id="reviewer", caller_task_id=task.id,
    )
    end = artifact.byte_size // 2 if partial_review else artifact.byte_size
    artifacts.read(
        artifact.id, expected_sha256=artifact.sha256, start_byte=0, end_byte=end,
        caller_task_id=task.id, caller_run_id=reviewer.id, receipt_id=receipt.id,
    )
    receipt = artifacts.fresh_complete_receipt(
        run_id=reviewer.id, artifact_id=artifact.id, expected_sha256=artifact.sha256,
    ) or artifacts.get_receipt(receipt.id)
    return ValidationInputs(
        contract=contract, snapshot=snapshot, artifact=artifact, reviewer_run_id=reviewer.id,
        read_receipt=receipt, result_schema_valid=True,
        result_schema_id="analysis_report_result_v2", coverage_results=tuple(coverage),
        lineage_layers=3, lineage_evidence_ids=tuple(evidence_ids[:3]),
        execution_control_evidence_ids=(evidence_ids[-1],), budget_status="within_budget",
        budget_integrity=True,
    )


def test_complete_validator_set_passes_and_verifies_artifact(validation_context) -> None:
    store, artifacts, snapshots, *_ = validation_context
    inputs = _complete_inputs(validation_context)
    report = DeterministicValidatorEngine(store, artifacts, snapshots).run(inputs)
    assert report.passed is True, {
        "failures": [
            (item.subject_id, item.status.value, item.reason_code)
            for item in report.gate_results
            if item.status is not TriState.PASS
        ],
        "receipt": inputs.read_receipt.model_dump(mode="json"),
        "artifact": inputs.artifact.model_dump(mode="json"),
        "reviewer_run_id": inputs.reviewer_run_id,
    }
    assert {item.subject_id for item in report.gate_results} == {f"QG-{index:03d}" for index in range(1, 17)}
    assert report.findings == ()
    assert artifacts.get(inputs.artifact.id).status is ArtifactVersionStatus.VERIFIED


def test_selected_focused_gates_ignore_repository_only_coverage(validation_context) -> None:
    store, artifacts, snapshots, *_ = validation_context
    inputs = replace(
        _complete_inputs(validation_context),
        coverage_results=(),
        lineage_layers=0,
        lineage_evidence_ids=(),
        execution_control_evidence_ids=(),
    )
    report = DeterministicValidatorEngine(store, artifacts, snapshots).run_selected(
        inputs,
        gate_ids=FOCUSED_QUESTION_GATE_IDS,
    )
    assert report.passed is True
    assert {item.subject_id for item in report.gate_results} == set(
        FOCUSED_QUESTION_GATE_IDS
    )
    assert not {"QG-003", "QG-004", "QG-005", "QG-006"}.intersection(
        item.subject_id for item in report.gate_results
    )


def test_missing_section_and_partial_review_create_blocking_findings(validation_context) -> None:
    store, artifacts, snapshots, *_ = validation_context
    inputs = _complete_inputs(validation_context, include_macros=False, partial_review=True)
    coverage = tuple(
        item.model_copy(update={"status": TriState.FAIL, "evidence_count": 0, "claim_ids": ()})
        if item.area == "macros" else item
        for item in inputs.coverage_results
    )
    inputs = replace(inputs, coverage_results=coverage)
    report = DeterministicValidatorEngine(store, artifacts, snapshots).run(inputs)
    failed = {item.subject_id for item in report.gate_results if item.status is not TriState.PASS}
    assert {"QG-003", "QG-004", "QG-012", "QG-013", "QG-014"}.issubset(failed)
    assert all(item.blocking for item in report.findings)


def test_repair_is_bounded_section_scoped_and_requires_child_version(validation_context) -> None:
    store, artifacts, _, _, _, task, producer, _, contract, _, _ = validation_context
    source = _upload(artifacts, task.id, producer.id, contract, _report(include_macros=False))
    reviewer = validation_context[7]
    source_receipt = artifacts.bind_candidate(
        run_id=reviewer.id,
        artifact_id=source.id,
        expected_sha256=source.sha256,
        verifier_profile_id="reviewer",
        caller_task_id=task.id,
    )
    artifacts.read(
        source.id,
        expected_sha256=source.sha256,
        start_byte=0,
        end_byte=source.byte_size,
        caller_task_id=task.id,
        caller_run_id=reviewer.id,
        receipt_id=source_receipt.id,
    )
    source_receipt = artifacts.fresh_complete_receipt(
        run_id=reviewer.id,
        artifact_id=source.id,
        expected_sha256=source.sha256,
    )
    assert source_receipt is not None
    finding = materialize_finding(
        FindingInput(
            category="coverage", severity="high", blocking=True, repairable=True,
            requirement_id="req-required-domains", section_id="macros",
            message="Macros section is missing", suggested_fix="Add the evidence-backed section",
        ),
        task_id=task.id, artifact_id=source.id, artifact_hash=source.sha256,
    )
    coordinator = RepairCoordinator(store, artifacts)
    request = coordinator.request(
        task_id=task.id, source_artifact_id=source.id, findings=(finding,),
        budget_allocation={"reported_tokens": 50_000}, budget_available=True,
    )
    assert request.allowed_sections == ("macros",)
    with pytest.raises(ConflictError, match="child version"):
        coordinator.complete(request.id, result_artifact_id=source.id)
    child = _upload(
        artifacts, task.id, producer.id, contract, _report(), parent=source.id,
    )
    with store._write() as connection:
        connection.execute(
            """
            CREATE TRIGGER inject_repair_publish_crash
            BEFORE UPDATE OF primary_artifact_id ON orch_tasks
            BEGIN SELECT RAISE(ABORT, 'injected repair publish crash'); END
            """
        )
    with pytest.raises(Exception, match="injected repair publish crash"):
        coordinator.complete(request.id, result_artifact_id=child.id)
    with store._read() as connection:
        repair_row = connection.execute(
            "SELECT status, result_artifact_id FROM orch_repair_requests WHERE id=?",
            (request.id,),
        ).fetchone()
        assert dict(repair_row) == {"status": "pending", "result_artifact_id": None}
    with store._write() as connection:
        connection.execute("DROP TRIGGER inject_repair_publish_crash")
    completed = coordinator.complete(request.id, result_artifact_id=child.id)
    assert completed.result_artifact_id == child.id
    assert child.version == source.version + 1
    assert artifacts.get(source.id).status is ArtifactVersionStatus.SUPERSEDED
    assert artifacts.get(source.id).sha256 == source.sha256

    # A complete receipt is subject-bound. Repair v2 must be rebound and read
    # from byte zero; the old version's receipt cannot satisfy the child hash.
    assert artifacts.fresh_complete_receipt(
        run_id=reviewer.id,
        artifact_id=child.id,
        expected_sha256=child.sha256,
    ) is None
    child_receipt = artifacts.bind_candidate(
        run_id=reviewer.id,
        artifact_id=child.id,
        expected_sha256=child.sha256,
        verifier_profile_id="reviewer",
        caller_task_id=task.id,
    )
    assert child_receipt.id != source_receipt.id
    artifacts.read(
        child.id,
        expected_sha256=child.sha256,
        start_byte=0,
        end_byte=child.byte_size,
        caller_task_id=task.id,
        caller_run_id=reviewer.id,
        receipt_id=child_receipt.id,
    )
    fresh_child_receipt = artifacts.fresh_complete_receipt(
        run_id=reviewer.id,
        artifact_id=child.id,
        expected_sha256=child.sha256,
    )
    assert fresh_child_receipt is not None
    assert fresh_child_receipt.artifact_id == child.id
    assert fresh_child_receipt.artifact_hash == child.sha256

    second_finding = finding.model_copy(update={
        "id": "finding_second", "artifact_id": child.id, "artifact_hash": child.sha256,
    })
    second = coordinator.request(
        task_id=task.id, source_artifact_id=child.id, findings=(second_finding,),
        budget_allocation={"reported_tokens": 50_000}, budget_available=True,
    )
    assert second.attempt == 2
    with pytest.raises(RepairExhausted):
        coordinator.request(
            task_id=task.id, source_artifact_id=child.id, findings=(second_finding,),
            budget_allocation={"reported_tokens": 50_000}, budget_available=True,
        )


def test_repair_without_budget_stops_for_attention(validation_context) -> None:
    store, artifacts, _, _, _, task, producer, _, contract, _, _ = validation_context
    source = _upload(artifacts, task.id, producer.id, contract, _report(include_macros=False))
    finding = materialize_finding(
        FindingInput(
            category="coverage", severity="high", blocking=True, repairable=True,
            section_id="macros", message="Missing macros section",
        ), task_id=task.id, artifact_id=source.id, artifact_hash=source.sha256,
    )
    with pytest.raises(ConflictError, match="budget"):
        RepairCoordinator(store, artifacts).request(
            task_id=task.id, source_artifact_id=source.id, findings=(finding,),
            budget_allocation={}, budget_available=False,
        )
    with store._read() as connection:
        status = connection.execute(
            "SELECT workflow_status FROM orch_tasks WHERE id=?", (task.id,)
        ).fetchone()["workflow_status"]
    assert status == "needs_attention"


def test_repair_rejects_changes_outside_authorized_markdown_section(
    validation_context,
) -> None:
    store, artifacts, _, _, _, task, producer, _, contract, _, _ = validation_context
    source = _upload(
        artifacts,
        task.id,
        producer.id,
        contract,
        _report(include_macros=False),
    )
    finding = materialize_finding(
        FindingInput(
            category="coverage",
            severity="high",
            blocking=True,
            repairable=True,
            section_id="macros",
            message="Macros section is missing",
        ),
        task_id=task.id,
        artifact_id=source.id,
        artifact_hash=source.sha256,
    )
    request = RepairCoordinator(store, artifacts).request(
        task_id=task.id,
        source_artifact_id=source.id,
        findings=(finding,),
        budget_allocation={"reported_tokens": 10_000},
        budget_available=True,
    )
    tampered = _report().replace(
        b"Evidence-backed Models.",
        b"Unauthorized Models rewrite.",
    )
    child = _upload(
        artifacts,
        task.id,
        producer.id,
        contract,
        tampered,
        parent=source.id,
    )
    with pytest.raises(ConflictError, match="outside its authorization"):
        RepairCoordinator(store, artifacts).complete(
            request.id,
            result_artifact_id=child.id,
        )


def test_pending_repair_compiles_fresh_acyclic_quality_plan(validation_context) -> None:
    (
        store,
        artifacts,
        snapshots,
        _,
        _,
        task,
        producer,
        _,
        contract,
        snapshot,
        inventory,
    ) = validation_context
    selector = StrategySelector(store)
    with store._read() as connection:
        active_strategy_id = connection.execute(
            "SELECT active_strategy_id FROM orch_tasks WHERE id=?", (task.id,)
        ).fetchone()["active_strategy_id"]
    strategy = selector.get(active_strategy_id)
    store.create_plan_revision(
        task.id,
        compile_strategy_plan(strategy),
        expected_task_version=store.get_task(task.id).version,
        created_by="test",
    )
    source = _upload(
        artifacts,
        task.id,
        producer.id,
        contract,
        _report(include_macros=False),
    )
    finding = materialize_finding(
        FindingInput(
            category="coverage",
            severity="high",
            blocking=True,
            repairable=True,
            section_id="macros",
            message="Macros section is missing",
        ),
        task_id=task.id,
        artifact_id=source.id,
        artifact_hash=source.sha256,
    )
    coordinator = RepairCoordinator(store, artifacts)
    request = coordinator.request(
        task_id=task.id,
        source_artifact_id=source.id,
        findings=(finding,),
        budget_allocation={
            "reported_tokens": 150_000,
            "model_calls": 8,
            "tool_calls": 20,
            "active_seconds": 300,
            "tool_payload_bytes": 16 * 1024 * 1024,
        },
        budget_available=True,
    )
    with store._write() as connection:
        connection.execute(
            """
            UPDATE orch_tasks SET status='running',
                current_stage='inter_step_evaluation', primary_artifact_id=?
            WHERE id=?
            """,
            (source.id, task.id),
        )
        connection.execute(
            """
            UPDATE orch_stage_history SET stage='inter_step_evaluation'
            WHERE task_id=? AND disposition='active'
            """,
            (task.id,),
        )
    shell = object.__new__(OrchestrationService)
    shell.store = store
    shell.quality_artifacts = artifacts
    shell.quality_contracts = ContractRepository(store)
    shell.quality_repository_resolver = RepositoryResolver()
    shell.quality_snapshots = snapshots
    shell.quality_strategies = selector
    shell.quality_budgets = SimpleNamespace()
    shell.quality_workflow = SimpleNamespace(repairs=coordinator)
    shell.quality = TaskQualityFacade(shell)
    assert shell._prepare_quality_v2_repair(store.get_task(task.id)) == "started"
    refreshed = store.get_task(task.id)
    graph = store.get_plan(refreshed.active_plan_id)
    assert refreshed.current_stage is OrchestrationStage.EXECUTION_REVIEW_TEST
    assert [item.key for item in graph.nodes] == [
        "synthesize",
        "validate",
        "review",
        "adjudicate",
        "publish",
    ]
    assert graph.plan.metadata["repair_request_id"] == request.id
    assert graph.plan.metadata["repair_node_allocations"]["validate"]["model_calls"] == 0
    assert sum(
        item["reserved_reported_tokens"]
        for item in graph.plan.metadata["repair_node_allocations"].values()
    ) == 150_000
