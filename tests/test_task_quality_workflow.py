from __future__ import annotations

import hashlib
import json
import subprocess
from types import SimpleNamespace

import pytest
from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.models import TaskSpec
from coworker.orchestration.quality.artifacts import ArtifactService
from coworker.orchestration.quality.budgets import BudgetService
from coworker.orchestration.quality.contract_compiler import ContractCompiler
from coworker.orchestration.quality.contracts import ContractRepository
from coworker.orchestration.quality.evidence import EvidenceLedger
from coworker.orchestration.quality.facade import TaskQualityFacade
from coworker.orchestration.quality.models import (
    BudgetProfile,
    RubricDimensionScore,
    Severity,
)
from coworker.orchestration.quality.plan_compiler import compile_strategy_plan
from coworker.orchestration.quality.repo_inventory import RepositoryInventoryService
from coworker.orchestration.quality.repository_resolver import RepositoryResolver
from coworker.orchestration.quality.repository_snapshot import RepositorySnapshotService
from coworker.orchestration.quality.rubrics import (
    create_rubric_score,
    repository_analysis_rubric,
)
from coworker.orchestration.quality.strategy_selector import StrategySelector
from coworker.orchestration.quality.validators import DeterministicValidatorEngine
from coworker.orchestration.quality.workflow import (
    QualityWorkflowDependencies,
    QualityWorkflowEngine,
)
from coworker.orchestration.store import OrchestrationStore
from coworker.orchestration.service import OrchestrationService


PROMPT = (
    "Read-only analyze the current dbt project architecture. Identify the project entry, "
    "models, macros, tests, seeds, snapshots, deployment controls and relationships. "
    "Produce a Markdown report with file evidence, risks and limitations."
)


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _git(root, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def _repository(root) -> None:
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
        "snapshots/s.sql": (
            "{% snapshot s %}select * from {{ ref('b') }}{% endsnapshot %}\n"
        ),
        ".github/workflows/deploy.yml": "name: deploy\nrun-name: dbt run\n",
    }
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    _git(root, "update-ref", "refs/remotes/origin/main", "HEAD")


def _report() -> bytes:
    sections = (
        "Baseline and Method",
        "Architecture Overview",
        "Entry",
        "Models",
        "Macros",
        "Tests",
        "Seeds",
        "Snapshots",
        "Deployment",
        "Relationships",
        "Risks",
        "Limitations",
    )
    return (
        "\n\n".join(
            f"# {section}\nEvidence-backed {section}." for section in sections
        )
        + "\n"
    ).encode("utf-8")


def _context(task, graph, key: str, run):
    node = next(item for item in graph.nodes if item.key == key)
    return SimpleNamespace(
        task=task,
        graph=graph,
        node=node,
        claim=SimpleNamespace(run=run),
    )


def test_deterministic_workflow_adjudicates_and_publishes_exact_primary(tmp_path) -> None:
    repo = tmp_path / "repo"
    _repository(repo)
    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(
        store, ContentAddressedBlobStore(tmp_path / "state" / "blobs")
    )
    snapshots = RepositorySnapshotService(store, artifacts)
    inventories = RepositoryInventoryService(store, artifacts, snapshots)
    contracts = ContractRepository(store)
    strategies = StrategySelector(store)
    budgets = BudgetService(store)
    validators = DeterministicValidatorEngine(store, artifacts, snapshots)
    workflow = QualityWorkflowEngine(
        QualityWorkflowDependencies(
            store=store,
            contracts=contracts,
            snapshots=snapshots,
            strategies=strategies,
            artifacts=artifacts,
            inventories=inventories,
            validators=validators,
            budgets=budgets,
        )
    )
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="quality-workflow", objective=PROMPT)
        )
        draft = ContractCompiler().compile(task_id=task.id, objective=PROMPT).contract
        contracts.save_draft(draft)
        contract = contracts.publish(draft.id, if_match=draft.content_hash)
        snapshot = snapshots.freeze(
            task_id=task.id,
            resolution=RepositoryResolver().resolve(repo, objective=PROMPT),
        )
        inventory = inventories.build(snapshot.id)
        strategy = strategies.publish(
            strategies.select(
                contract=contract, snapshot=snapshot, inventory=inventory
            )
        )
        profile_value = dict(strategy.budget_profile)
        profile_value.pop("source", None)
        budgets.create(
            task_id=task.id,
            strategy_id=strategy.id,
            profile=BudgetProfile.model_validate(profile_value),
            provider_usage_semantics={"reported_tokens": "provider_total"},
        )
        graph = store.create_plan_revision(
            task.id,
            compile_strategy_plan(strategy),
            expected_task_version=store.get_task(task.id).version,
            created_by="test",
        )
        with store._write() as connection:
            connection.execute(
                "UPDATE orch_tasks SET status='queued' WHERE id=?", (task.id,)
            )
        runs = {
            key: store.enqueue_run(task.id, key, plan_id=graph.plan.id)
            for key in ("synthesize", "validate", "review", "adjudicate", "publish")
        }
        content = _report()
        deliverable = next(item for item in contract.deliverables if item.primary)
        upload = artifacts.create(
            task.id,
            logical_deliverable_id=deliverable.id,
            filename=deliverable.filename,
            mime_type=deliverable.mime_type,
            run_id=runs["synthesize"].id,
            producer_profile_id="worker",
        )
        artifacts.append(
            upload["upload_id"],
            sequence=0,
            content=content,
            chunk_hash=_digest(content),
        )
        artifact = artifacts.complete(
            upload["upload_id"], expected_sha256=_digest(content)
        )
        with store._write() as connection:
            connection.execute(
                """
                UPDATE orch_runs SET status='succeeded', output_json=?
                WHERE id=?
                """,
                (
                    json.dumps(
                        {
                            "structured_result": {
                                "schema_id": "analysis_report_result_v2",
                                "schema_version": 2,
                            }
                        }
                    ),
                    runs["synthesize"].id,
                ),
            )
            connection.execute(
                """
                UPDATE orch_tasks SET primary_artifact_id=?, artifact_status='draft',
                workflow_status='validating', quality_status='checking',
                budget_status='within_budget' WHERE id=?
                """,
                (artifact.id, task.id),
            )

        ledger = EvidenceLedger(store, snapshots)
        area_paths = {
            "entry": "dbt_project.yml",
            "models": "models/b.sql",
            "macros": "macros/m.sql",
            "tests": "tests/t.sql",
            "seeds": "seeds/s.csv",
            "snapshots": "snapshots/s.sql",
            "deployment": ".github/workflows/deploy.yml",
        }
        area_evidence: dict[str, str] = {}
        for area, path in area_paths.items():
            claim = ledger.create_claim(
                task_id=task.id,
                artifact_id=artifact.id,
                section_id=area,
                text=f"{area} is present in the frozen repository",
                claim_type="fact",
                severity=(
                    Severity.HIGH if area in {"models", "deployment"} else Severity.INFO
                ),
                requirement_ids=("req-required-domains",),
                source_key=f"fixture:{area}",
            )
            evidence = ledger.create_file_evidence(
                claim_id=claim.id,
                snapshot_id=snapshot.id,
                path=path,
                line_start=1,
                line_end=1,
                support="supports",
                created_by_run_id=runs["synthesize"].id,
            )
            area_evidence[area] = evidence.id
            ledger.record_coverage(
                task_id=task.id,
                artifact_id=artifact.id,
                requirement_id="req-required-domains",
                area=area,
                status="pass",
                claim_ids=(claim.id,),
                evidence_count=1,
                notes="fixture coverage",
                validator_id="fixture@1",
            )
        relationship_requirement = next(
            item for item in contract.requirements if item.category.value == "relationship"
        )
        relationship = ledger.create_claim(
            task_id=task.id,
            artifact_id=artifact.id,
            section_id="relationships",
            text="The entry, model graph, and deployment control form a lineage chain.",
            claim_type="fact",
            severity="high",
            requirement_ids=(relationship_requirement.id,),
            source_key="fixture:relationship",
        )
        for path in (
            "dbt_project.yml",
            "models/b.sql",
            ".github/workflows/deploy.yml",
        ):
            ledger.create_file_evidence(
                claim_id=relationship.id,
                snapshot_id=snapshot.id,
                path=path,
                line_start=1,
                line_end=1,
                support="supports",
                created_by_run_id=runs["synthesize"].id,
            )
        limitation = ledger.create_claim(
            task_id=task.id,
            artifact_id=artifact.id,
            section_id="limitations",
            text="Static inspection cannot prove runtime state.",
            claim_type="limitation",
            source_key="fixture:limitation",
        )
        ledger.create_file_evidence(
            claim_id=limitation.id,
            snapshot_id=snapshot.id,
            path="dbt_project.yml",
            line_start=1,
            line_end=1,
            support="context",
            created_by_run_id=runs["synthesize"].id,
        )
        absence = ledger.create_claim(
            task_id=task.id,
            artifact_id=artifact.id,
            section_id="limitations",
            text="No second project entry was observed.",
            claim_type="absence",
            source_key="fixture:absence",
        )
        ledger.create_negative_evidence(
            claim_id=absence.id,
            query="dbt_project.yml",
            tool_version="fixture@1",
            scope_paths=("dbt_project.yml",),
            excluded_paths=(),
            result_count=1,
            query_result_hash=_digest(b"dbt_project.yml"),
            limitations=("Frozen manifest scope only",),
        )
        ledger.record_inventory_metric(
            inventory_id=inventory.id,
            name="resource_files",
            value=7,
            unit="files",
            query_key="fixture-query",
            subtotals={area: 1 for area in area_paths},
            reconciles_to=7,
        )
        receipt = artifacts.bind_candidate(
            run_id=runs["review"].id,
            artifact_id=artifact.id,
            expected_sha256=artifact.sha256,
            verifier_profile_id="reviewer",
            caller_task_id=task.id,
        )
        artifacts.read(
            artifact.id,
            expected_sha256=artifact.sha256,
            start_byte=0,
            end_byte=artifact.byte_size,
            caller_task_id=task.id,
            caller_run_id=runs["review"].id,
            receipt_id=receipt.id,
        )
        receipt = artifacts.fresh_complete_receipt(
            run_id=runs["review"].id,
            artifact_id=artifact.id,
            expected_sha256=artifact.sha256,
        )
        assert receipt is not None
        rubric = repository_analysis_rubric()
        score = create_rubric_score(
            rubric=rubric,
            artifact_id=artifact.id,
            artifact_hash=artifact.sha256,
            scorer_run_id=runs["review"].id,
            authorized_scorer_run_id=runs["review"].id,
            dimension_scores=(
                RubricDimensionScore(
                    dimension_id=dimension.id,
                    points=dimension.max_points,
                    rationale="Evidence-backed fixture score.",
                )
                for dimension in rubric.dimensions
            ),
            read_receipt=receipt,
        )
        with store._write() as connection:
            connection.execute(
                "UPDATE orch_runs SET status='succeeded', output_json='{}' WHERE id=?",
                (runs["review"].id,),
            )
            connection.execute(
                """
                INSERT INTO orch_rubric_scores(
                    id, rubric_id, rubric_version, artifact_id, artifact_hash,
                    scorer_run_id, dimension_scores_json, total, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    score.id,
                    score.rubric_id,
                    score.rubric_version,
                    score.artifact_id,
                    score.artifact_hash,
                    score.scorer_run_id,
                    json.dumps(
                        [item.model_dump(mode="json") for item in score.dimension_scores]
                    ),
                    score.total,
                    score.created_at.isoformat().replace("+00:00", "Z"),
                ),
            )

        prevalidation = workflow.execute(
            _context(task, graph, "validate", runs["validate"])
        )
        assert prevalidation.status == "succeeded", prevalidation.error_message
        with store._write() as connection:
            connection.execute(
                """
                CREATE TRIGGER inject_evaluation_commit_crash
                AFTER INSERT ON orch_quality_evaluations
                WHEN NEW.evaluation_type='final'
                BEGIN SELECT RAISE(ABORT, 'injected evaluation commit crash'); END
                """
            )
        interrupted = workflow.execute(
            _context(task, graph, "adjudicate", runs["adjudicate"])
        )
        assert interrupted.status == "failed"
        assert "injected evaluation commit crash" in interrupted.error_message
        with store._read() as connection:
            assert connection.execute(
                "SELECT COUNT(*) FROM orch_quality_evaluations WHERE task_id=?",
                (task.id,),
            ).fetchone()[0] == 0
            assert connection.execute(
                "SELECT quality_status FROM orch_tasks WHERE id=?", (task.id,)
            ).fetchone()[0] == "checking"
        with store._write() as connection:
            connection.execute("DROP TRIGGER inject_evaluation_commit_crash")
        decision = workflow.execute(
            _context(task, graph, "adjudicate", runs["adjudicate"])
        )
        assert decision.status == "succeeded", decision.error_message
        assert decision.output == {
            "decision": "publish",
            "quality_status": "pass",
            "waiver_ids": [],
        }
        publication = workflow.execute(
            _context(task, graph, "publish", runs["publish"])
        )
        assert publication.status == "succeeded", publication.error_message
        with store._read() as connection:
            final = connection.execute(
                """
                SELECT artifact_id, artifact_hash, verdict, decision
                FROM orch_quality_evaluations
                WHERE task_id=? AND evaluation_type='final'
                """,
                (task.id,),
            ).fetchone()
            task_row = connection.execute(
                """
                SELECT workflow_status, quality_status, artifact_status,
                       primary_artifact_id, budget_status
                FROM orch_tasks WHERE id=?
                """,
                (task.id,),
            ).fetchone()
        assert dict(final) == {
            "artifact_id": artifact.id,
            "artifact_hash": artifact.sha256,
            "verdict": "pass",
            "decision": "publish",
        }
        assert dict(task_row) == {
            "workflow_status": "completed",
            "quality_status": "pass",
            "artifact_status": "verified",
            "primary_artifact_id": artifact.id,
            "budget_status": "within_budget",
        }
        with store._write() as connection, pytest.raises(Exception, match="immutable"):
            connection.execute(
                """
                UPDATE orch_quality_evaluations SET verdict='fail'
                WHERE task_id=? AND evaluation_type='final'
                """,
                (task.id,),
            )

        # A later hard-budget stop must fail the final lifecycle guard. V2 never
        # falls through to the legacy accept-current / override-accept gates.
        with store._write() as connection:
            connection.execute(
                """
                UPDATE orch_tasks SET status='running',
                    current_stage='inter_step_evaluation',
                    workflow_status='completed', budget_status='exhausted'
                WHERE id=?
                """,
                (task.id,),
            )
        service_shell = object.__new__(OrchestrationService)
        service_shell.store = store
        service_shell.quality_artifacts = artifacts
        service_shell.quality_contracts = contracts
        service_shell.quality_repository_resolver = RepositoryResolver()
        service_shell.quality_snapshots = snapshots
        service_shell.quality_strategies = strategies
        service_shell.quality_budgets = budgets
        service_shell.quality = TaskQualityFacade(service_shell)
        blocked = service_shell._advance_task(task.id)
        assert blocked.status.value == "needs_reconciliation"
        projection = service_shell.quality.task_projection(task.id)
        # Corrupting orthogonal facts after publication must not invent the
        # unlisted completed -> needs_attention transition.  The scheduler is
        # held for reconciliation while the inconsistent V2 facts remain
        # visible to observability.
        assert projection["workflow_status"] == "completed"
        assert projection["budget_status"] == "exhausted"
        assert all(gate.kind.value != "final_acceptance" for gate in store.list_gates(task.id))
    finally:
        store.close()
