from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.dag import validate_plan
from coworker.orchestration.errors import DAGValidationError
from coworker.orchestration.models import TaskSpec
from coworker.orchestration.quality.artifacts import ArtifactService
from coworker.orchestration.quality.contract_compiler import ContractCompiler
from coworker.orchestration.quality.contracts import ContractRepository
from coworker.orchestration.quality.models import Archetype
from coworker.orchestration.quality.plan_compiler import PlanProposalV2, compile_strategy_plan
from coworker.orchestration.quality.repo_inventory import RepositoryInventoryService
from coworker.orchestration.quality.repository_resolver import RepositoryResolver
from coworker.orchestration.quality.repository_snapshot import RepositorySnapshotService
from coworker.orchestration.quality.strategy_selector import REPO_AREAS, StrategySelector
from coworker.orchestration.store import OrchestrationStore


PROMPT = (
    "Read-only analyze the current Fabric/dbt project architecture. Identify the dbt project "
    "entry, models, macros, tests, seeds, snapshots, deployment configuration and relationships. "
    "Produce a Markdown architecture report with file evidence and do not modify any source file."
)


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
        "dbt_project.yml": "name: fixture\n",
        "models/a.sql": "select 1\n",
        "models/b.sql": "select * from {{ ref('a') }}\n",
        "macros/m.sql": "{% macro m() %}1{% endmacro %}\n",
        "tests/t.sql": "select 1 where false\n",
        "seeds/s.csv": "id\n1\n",
        "snapshots/s.sql": "{% snapshot s %}select 1{% endsnapshot %}\n",
        ".github/workflows/deploy.yml": "name: deploy\n",
    }
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    _git(root, "update-ref", "refs/remotes/origin/main", "HEAD")


@pytest.fixture
def strategy_context(tmp_path):
    repo = tmp_path / "repo"
    _repo(repo)
    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(store, ContentAddressedBlobStore(tmp_path / "state" / "blobs"))
    snapshots = RepositorySnapshotService(store, artifacts)
    contracts = ContractRepository(store)
    task = store.create_task(TaskSpec(idempotency_key="strategy", objective=PROMPT))
    compiled = ContractCompiler().compile(task_id=task.id, objective=PROMPT).contract
    contracts.save_draft(compiled)
    contract = contracts.publish(compiled.id, if_match=compiled.content_hash)
    snapshot = snapshots.freeze(
        task_id=task.id,
        resolution=RepositoryResolver().resolve(repo, objective=PROMPT),
    )
    inventories = RepositoryInventoryService(store, artifacts, snapshots)
    inventory = inventories.build(snapshot.id)
    try:
        yield store, contracts, task, contract, snapshot, inventory
    finally:
        store.close()


def test_repo_analysis_strategy_has_parallel_exclusive_collectors_without_fake_planner(
    strategy_context,
) -> None:
    _, _, _, contract, snapshot, inventory = strategy_context
    strategy = StrategySelector().select(
        contract=contract, snapshot=snapshot, inventory=inventory
    )
    assert strategy.template_id == "repo-analysis-v2"
    assert strategy.assessment.cognitive_complexity > strategy.assessment.operational_risk
    keys = {item.key for item in strategy.nodes}
    assert {"understand", "plan"}.isdisjoint(keys)
    collectors = [item for item in strategy.nodes if item.kind == "evidence_collector"]
    assert 1 <= len(collectors) <= 4
    flattened = [str(area) for item in collectors for area in item.config["areas"]]
    assert set(flattened) == set(REPO_AREAS)
    assert len(flattened) == len(set(flattened))
    assert strategy.semantic_scorer_node_key == "review"
    assert sum(item.key == strategy.semantic_scorer_node_key for item in strategy.nodes) == 1
    assert strategy.effective_policy["collector_count"]["source"] == "evidence_assessment"
    assert strategy.budget_profile["mode"] == "hard"


def test_direct_bindings_do_not_implicitly_forward_ancestor_summaries(strategy_context) -> None:
    _, _, _, contract, snapshot, inventory = strategy_context
    strategy = StrategySelector().select(contract=contract, snapshot=snapshot, inventory=inventory)
    reviewer = [item for item in strategy.input_bindings if item.consumer_node_key == "review"]
    assert {item.source_type.value for item in reviewer} == {
        "artifact", "contract", "snapshot", "evidence_bundle"
    }
    assert all("summary" not in item.source_selector for item in reviewer)
    plan = compile_strategy_plan(strategy)
    assert validate_plan(plan)[0] == "resolve_inventory"
    assert plan.metadata["semantic_scorer_node_key"] == "review"
    reviewer_node = next(item for item in plan.nodes if item.key == "review")
    assert len(reviewer_node.input["direct_bindings"]) == len(reviewer)


def test_strategy_request_cannot_override_integrity_policy_or_flags(
    strategy_context,
) -> None:
    _, _, _, contract, snapshot, inventory = strategy_context
    selector = StrategySelector()
    with pytest.raises(PermissionError, match="security/profile precedence"):
        selector.select(
            contract=contract,
            snapshot=snapshot,
            inventory=inventory,
            explicit_policy={
                "source_workspace_write": {
                    "value": True,
                    "source": "request_body",
                }
            },
        )
    for override in (
        {"artifact_v2_enabled": False},
        {"repository_snapshot_required": False},
        {"semantic_quality_gate_enabled": False},
        {"runtime_budget_enforcement_mode": "observe"},
    ):
        with pytest.raises(PermissionError, match="cannot be overridden"):
            selector.select(
                contract=contract,
                snapshot=snapshot,
                inventory=inventory,
                feature_flags=override,
            )
    with pytest.raises(ValueError, match="unsupported Task Quality feature flags"):
        selector.select(
            contract=contract,
            snapshot=snapshot,
            inventory=inventory,
            feature_flags={"invented_bypass": True},
        )
    enabled = selector.select(
        contract=contract,
        snapshot=snapshot,
        inventory=inventory,
        feature_flags={"auto_repair": True},
    )
    assert enabled.feature_flags["auto_repair"] is True
    assert enabled.feature_flags["auto_repair_enabled"] is True


def test_focused_question_uses_one_producer_and_one_independent_scorer(tmp_path) -> None:
    repo = tmp_path / "repo"
    _repo(repo)
    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(
        store,
        ContentAddressedBlobStore(tmp_path / "state" / "blobs"),
    )
    snapshots = RepositorySnapshotService(store, artifacts)
    contracts = ContractRepository(store)
    objective = "What value is returned by the selected fixture?"
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="focused-strategy", objective=objective)
        )
        draft = ContractCompiler().compile(
            task_id=task.id,
            objective=objective,
        ).contract
        assert draft.archetype is Archetype.FOCUSED_QUESTION
        assert draft.deliverables[0].result_schema_id == "analysis_report_result_v2"
        contracts.save_draft(draft)
        contract = contracts.publish(draft.id, if_match=draft.content_hash)
        snapshot = snapshots.freeze(
            task_id=task.id,
            resolution=RepositoryResolver().resolve(repo, objective=objective),
        )
        selector = StrategySelector(store)
        strategy = selector.select(contract=contract, snapshot=snapshot)
        model_nodes = tuple(item for item in strategy.nodes if not item.deterministic)
        assert [(item.role, item.kind) for item in model_nodes] == [
            ("worker", "focused_answer"),
            ("reviewer", "independent_review"),
        ]
        assert len(strategy.nodes) == 5
        assert strategy.semantic_scorer_node_key == "review"
        assert strategy.rubric_id == "focused-question-quality"
        assert strategy.effective_policy["independent_review"]["value"] is True
        assert validate_plan(compile_strategy_plan(strategy))[0] == "answer"
        selector.publish(strategy)
        with store._read() as connection:
            rubric = connection.execute(
                "SELECT applicable_archetypes_json FROM orch_quality_rubrics WHERE id=?",
                (strategy.rubric_id,),
            ).fetchone()
        assert "focused_question" in rubric["applicable_archetypes_json"]
    finally:
        store.close()


def test_strategy_publish_is_immutable_and_binds_active_task(strategy_context) -> None:
    store, _, task, contract, snapshot, inventory = strategy_context
    selector = StrategySelector(store)
    strategy = selector.select(contract=contract, snapshot=snapshot, inventory=inventory)
    selector.publish(strategy)
    replay = selector.get(strategy.id)
    assert replay.content_hash == strategy.content_hash
    assert replay.input_bindings == strategy.input_bindings
    with store._read() as connection:
        task_row = connection.execute(
            "SELECT active_strategy_id, workflow_status FROM orch_tasks WHERE id=?", (task.id,)
        ).fetchone()
    assert task_row["active_strategy_id"] == strategy.id
    assert task_row["workflow_status"] == "ready"
    with store._write() as connection, pytest.raises(Exception, match="immutable"):
        connection.execute(
            "UPDATE orch_execution_strategies SET template_id='tampered' WHERE id=?",
            (strategy.id,),
        )


def test_plan_proposal_cycle_and_authority_escalation_are_rejected(strategy_context) -> None:
    _, _, _, contract, snapshot, inventory = strategy_context
    strategy = StrategySelector().select(contract=contract, snapshot=snapshot, inventory=inventory)
    cycle = PlanProposalV2(
        nodes=strategy.nodes,
        edges=(*strategy.edges, {"source": "publish", "target": "resolve_inventory"}),
        input_bindings=strategy.input_bindings,
        rationale="Keep the strategy but inject a cycle for validation testing.",
    )
    with pytest.raises(DAGValidationError, match="cycle"):
        compile_strategy_plan(strategy, proposal=cycle)
    changed = tuple(
        item.model_copy(update={"role": "worker"}) if item.key == "review" else item
        for item in strategy.nodes
    )
    escalation = PlanProposalV2(
        nodes=changed,
        edges=strategy.edges,
        input_bindings=strategy.input_bindings,
        rationale="Attempt to turn the independent reviewer into a producer.",
    )
    with pytest.raises(PermissionError, match="authority"):
        compile_strategy_plan(strategy, proposal=escalation)


def test_contract_requirement_ids_are_scoped_per_contract(tmp_path) -> None:
    store = OrchestrationStore(tmp_path / "state.db")
    contracts = ContractRepository(store)
    try:
        for index in range(2):
            task = store.create_task(TaskSpec(idempotency_key=f"contract-{index}", objective=PROMPT))
            contract = ContractCompiler().compile(task_id=task.id, objective=PROMPT).contract
            contracts.save_draft(contract)
            contracts.publish(contract.id, if_match=contract.content_hash).verify_content_hash()
        with store._read() as connection:
            count = connection.execute(
                "SELECT COUNT(*) AS count FROM orch_contract_requirements WHERE id='req-required-domains'"
            ).fetchone()["count"]
        assert count == 2
    finally:
        store.close()
