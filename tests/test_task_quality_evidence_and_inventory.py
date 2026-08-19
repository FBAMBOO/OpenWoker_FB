from __future__ import annotations

import concurrent.futures
import subprocess
import threading
import time
from pathlib import Path

import pytest

from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.errors import ConflictError
from coworker.orchestration.models import NodeSpec, PlanSpec, TaskSpec
from coworker.orchestration.quality.artifacts import ArtifactService
from coworker.orchestration.quality.evidence import EvidenceLedger
from coworker.orchestration.quality.models import ClaimType, EvidenceSupport
from coworker.orchestration.quality.query_cache import RepositoryQueryCache
from coworker.orchestration.quality.repo_inventory import RepositoryInventoryService
from coworker.orchestration.quality.repo_tools import SnapshotRepoTools
from coworker.orchestration.quality.repository_resolver import RepositoryResolver
from coworker.orchestration.quality.repository_snapshot import RepositorySnapshotService
from coworker.orchestration.store import OrchestrationStore


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def _fixture_repo(root: Path) -> None:
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.email", "quality@example.test")
    _git(root, "config", "user.name", "Quality Test")
    files = {
        "dbt_project.yml": "name: fixture\n",
        "models/orders.sql": "select * from {{ ref('stg_orders') }}\n",
        "models/stg_orders.sql": "select * from {{ source('raw', 'orders') }}\n",
        "macros/audit.sql": "{% macro audit() %}select 1{% endmacro %}\n",
        "tests/orders_positive.sql": "select * from {{ ref('orders') }} where id < 0\n",
        "seeds/countries.csv": "code,name\nCN,China\n",
        "snapshots/orders.sql": "{% snapshot orders %}select 1{% endsnapshot %}\n",
        ".github/workflows/deploy.yml": "name: deploy\n",
    }
    for relative, text in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    _git(root, "update-ref", "refs/remotes/origin/main", "HEAD")


@pytest.fixture
def quality_context(tmp_path):
    repo = tmp_path / "repo"
    _fixture_repo(repo)
    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(store, ContentAddressedBlobStore(tmp_path / "state" / "blobs"))
    snapshots = RepositorySnapshotService(store, artifacts)
    task = store.create_task(
        TaskSpec(idempotency_key="evidence-inventory", objective="Read-only dbt analysis")
    )
    plan = store.create_plan_revision(
        task.id,
        PlanSpec(nodes=(NodeSpec(key="collector", agent="worker"),), edges=()),
        expected_task_version=task.version,
        created_by="test",
    )
    with store._write() as connection:
        connection.execute("UPDATE orch_tasks SET status='queued' WHERE id=?", (task.id,))
    run = store.enqueue_run(task.id, "collector", plan_id=plan.plan.id)
    snapshot = snapshots.freeze(
        task_id=task.id,
        resolution=RepositoryResolver().resolve(repo, objective="Analyze the default dbt project"),
    )
    inventories = RepositoryInventoryService(store, artifacts, snapshots)
    cache = RepositoryQueryCache(store, artifacts)
    tools = SnapshotRepoTools(snapshots, inventories, cache)
    ledger = EvidenceLedger(store, snapshots)
    try:
        yield store, artifacts, snapshots, inventories, tools, ledger, task, run, snapshot
    finally:
        store.close()


def test_inventory_is_shared_immutable_and_dbt_counts_are_reconcilable(quality_context) -> None:
    _, artifacts, _, inventories, tools, _, _, _, snapshot = quality_context
    first = inventories.build(snapshot.id)
    second = inventories.build(snapshot.id)
    assert first.id == second.id
    record, value = inventories.get(first.id)
    assert record.content_hash == artifacts.get(record.artifact_id).sha256
    assert value["dbt_static"]["counts"] == {
        "models": 2,
        "macro_sql": 1,
        "sql_tests": 1,
        "seeds": 1,
        "snapshots": 1,
        "pipeline_yaml": 1,
    }
    assert {edge["kind"] for edge in value["dbt_static"]["edges"]} == {"ref", "source"}
    assert tools.get_inventory(snapshot.id)["metadata"]["id"] == first.id


def test_normalized_query_cache_reuses_scan_and_requires_bypass_reason(quality_context) -> None:
    store, _, _, _, tools, _, _, _, snapshot = quality_context
    first = tools.search_snapshot(snapshot.id, "ref(", paths=("models", "tests"))
    second = tools.search_snapshot(snapshot.id, "ref(", paths=("tests", "models", "models"))
    assert first["query_key"] == second["query_key"]
    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert tools.metrics.cache_misses == 1
    assert tools.metrics.cache_hits == 1
    assert tools.metrics.duplicate_non_cached_ratio == 0
    with pytest.raises(ValueError, match="auditable reason"):
        tools.search_snapshot(snapshot.id, "ref(", bypass_cache=True)
    tools.search_snapshot(
        snapshot.id,
        "ref(",
        paths=("models", "tests"),
        bypass_cache=True,
        bypass_reason="validate cache",
    )
    assert tools.metrics.bypasses == 1
    assert tools.metrics.duplicate_non_cached_ratio == pytest.approx(1 / 3)
    with store._read() as connection:
        row = connection.execute(
            "SELECT hit_count FROM orch_repo_query_cache WHERE query_key=?", (first["query_key"],)
        ).fetchone()
    assert row["hit_count"] == 1


def test_four_collectors_singleflight_one_identical_underlying_scan(
    quality_context,
) -> None:
    _, _, _, _, tools, _, task, _, snapshot = quality_context
    barrier = threading.Barrier(4)
    counter_lock = threading.Lock()
    scan_count = 0

    def scan():
        nonlocal scan_count
        with counter_lock:
            scan_count += 1
        time.sleep(0.05)
        return {"results": [{"path": "models/orders.sql"}], "complete": True}

    def collect(_index: int):
        barrier.wait(timeout=5)
        return tools.cache.execute(
            task_id=task.id,
            snapshot_id=snapshot.id,
            tool_name="singleflight-scan",
            tool_version="1",
            args={"paths": ["models"]},
            operation=scan,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = tuple(pool.map(collect, range(4)))

    assert scan_count == 1
    assert len({item.query_key for item in results}) == 1
    assert len({item.artifact_id for item in results}) == 1
    assert sum(not item.cache_hit for item in results) == 1


def test_typed_claim_evidence_negative_search_and_metric_ledger(quality_context) -> None:
    _, artifacts, snapshots, inventories, tools, ledger, task, run, snapshot = quality_context
    report = artifacts.store_internal_json(
        task_id=task.id,
        logical_deliverable_id="report-ledger-test",
        filename="report_ledger.json",
        value={"result": "draft"},
    )
    claim = ledger.create_claim(
        task_id=task.id,
        artifact_id=report.id,
        section_id="architecture",
        text="orders depends on stg_orders",
        claim_type=ClaimType.FACT,
        requirement_ids=("req-relationship",),
    )
    evidence = ledger.create_file_evidence(
        claim_id=claim.id,
        snapshot_id=snapshot.id,
        path="models/orders.sql",
        line_start=1,
        line_end=1,
        support=EvidenceSupport.SUPPORTS,
        created_by_run_id=run.id,
    )
    expected = snapshots.read_file_lines(snapshot.id, "models/orders.sql", start_line=1, end_line=1)
    assert evidence.blob_hash == expected["blob_hash"]
    assert evidence.excerpt_hash == expected["excerpt_hash"]

    absence = ledger.create_claim(
        task_id=task.id,
        artifact_id=report.id,
        section_id="limitations",
        text="No Python models were observed in the frozen scope",
        claim_type=ClaimType.ABSENCE,
    )
    search = tools.search_snapshot(snapshot.id, ".py")
    negative = ledger.create_negative_evidence(
        claim_id=absence.id,
        query=".py",
        tool_version="snapshot-repo-tools@1",
        scope_paths=("models",),
        excluded_paths=(),
        result_count=len(search["results"]),
        query_result_hash=search["result_hash"],
        limitations=("Filename/content literal search only",),
    )
    assert negative.result_count == 0
    with pytest.raises(ConflictError, match="absence claim"):
        ledger.create_negative_evidence(
            claim_id=claim.id,
            query="missing",
            tool_version="v1",
            scope_paths=("models",),
            excluded_paths=(),
            result_count=0,
            query_result_hash=search["result_hash"],
            limitations=("bounded",),
        )

    inventory = inventories.build(snapshot.id)
    metric = ledger.record_inventory_metric(
        inventory_id=inventory.id,
        name="dbt_resources",
        value=6,
        unit="files",
        query_key=search["query_key"],
        subtotals={"models": 2, "macros": 1, "tests": 1, "seeds": 1, "snapshots": 1},
        reconciles_to=6,
    )
    assert ledger.metric_reconciles(metric) is True
    assert ledger.metric_reconciles(metric.model_copy(update={"value": 7})) is False


def test_evidence_is_snapshot_and_task_scoped(quality_context) -> None:
    store, artifacts, _, _, _, ledger, task, run, snapshot = quality_context
    report = artifacts.store_internal_json(
        task_id=task.id,
        logical_deliverable_id="scope-test",
        filename="scope_test.json",
        value={"scope": "task"},
    )
    claim = ledger.create_claim(
        task_id=task.id,
        artifact_id=report.id,
        section_id="scope",
        text="scoped",
        claim_type="fact",
    )
    other = store.create_task(TaskSpec(idempotency_key="other-evidence", objective="other"))
    with pytest.raises(PermissionError):
        ledger.create_claim(
            task_id=other.id,
            artifact_id=report.id,
            section_id="leak",
            text="cross task",
            claim_type="fact",
        )
    with pytest.raises(ValueError):
        ledger.create_file_evidence(
            claim_id=claim.id,
            snapshot_id=snapshot.id,
            path="../outside",
            line_start=1,
            line_end=1,
            support="supports",
            created_by_run_id=run.id,
        )
