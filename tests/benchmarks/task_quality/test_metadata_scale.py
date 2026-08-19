from __future__ import annotations

import json
import time
from types import SimpleNamespace

from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.models import NodeSpec, PlanSpec, TaskSpec
from coworker.orchestration.quality.artifacts import ArtifactService
from coworker.orchestration.quality.evidence import EvidenceLedger
from coworker.orchestration.quality.findings import (
    deduplicate_findings,
    materialize_finding,
)
from coworker.orchestration.quality.facade import (
    TaskQualityFacade,
    _cursor_token,
    _hash,
)
from coworker.orchestration.quality.models import ClaimType, FindingInput, Severity
from coworker.orchestration.quality.repository_resolver import RepositoryResolver
from coworker.orchestration.quality.repository_snapshot import RepositorySnapshotService
from coworker.orchestration.store import OrchestrationStore


def test_one_million_evidence_metadata_uses_bounded_keyset_pages(tmp_path) -> None:
    """PERF-Q-02: exercise the real ledger schema at the locked 1M scale."""

    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(
        store, ContentAddressedBlobStore(tmp_path / "state" / "blobs")
    )
    snapshots = RepositorySnapshotService(store, artifacts)
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="perf-evidence-1m", objective="Scale fixture")
        )
        plan = store.create_plan_revision(
            task.id,
            PlanSpec(nodes=(NodeSpec(key="collector", agent="worker"),), edges=()),
            expected_task_version=task.version,
            created_by="performance-test",
        )
        with store._write() as connection:
            connection.execute(
                "UPDATE orch_tasks SET status='queued' WHERE id=?", (task.id,)
            )
        run = store.enqueue_run(task.id, "collector", plan_id=plan.plan.id)
        workspace = tmp_path / "fixture"
        workspace.mkdir()
        (workspace / "marker.txt").write_text("frozen\n", encoding="utf-8")
        snapshot = snapshots.freeze(
            task_id=task.id,
            resolution=RepositoryResolver().resolve(
                workspace, objective="Analyze this directory"
            ),
        )
        report = artifacts.store_internal_json(
            task_id=task.id,
            logical_deliverable_id="perf-report",
            filename="perf_report.json",
            value={"fixture": "metadata-only"},
        )
        claim = EvidenceLedger(store, snapshots).create_claim(
            task_id=task.id,
            artifact_id=report.id,
            section_id="scale",
            text="Synthetic metadata scale claim",
            claim_type=ClaimType.FACT,
        )

        inserted_at = time.perf_counter()
        with store._write() as connection:
            connection.execute(
                """
                WITH RECURSIVE sequence(value) AS (
                    VALUES(1)
                    UNION ALL
                    SELECT value + 1 FROM sequence WHERE value < 1000000
                )
                INSERT INTO orch_evidence_refs(
                    id, claim_id, snapshot_id, path, line_start, line_end,
                    blob_hash, git_blob_oid, excerpt_hash, evidence_type,
                    support, content_withheld, created_by_run_id, created_at
                )
                SELECT printf('scale-evidence-%07d', value), ?, ?, 'x', value, value,
                       'h', NULL, 'h', 'file', 'supports', 1, ?, 'scale'
                FROM sequence
                """,
                (claim.id, snapshot.id, run.id),
            )
        insert_seconds = time.perf_counter() - inserted_at

        service = SimpleNamespace(
            store=store,
            quality_artifacts=artifacts,
            quality_contracts=None,
            quality_repository_resolver=None,
            quality_snapshots=snapshots,
            quality_strategies=None,
            quality_budgets=None,
        )
        facade = TaskQualityFacade(service)
        first_started = time.perf_counter()
        first = facade.evidence(task.id, offset=0, limit=200)
        first_seconds = time.perf_counter() - first_started
        assert len(first["evidence"]["items"]) == 200
        assert first["evidence"]["has_more"] is True
        assert first["evidence"]["next_cursor"]

        scope_hash = _hash({"claim_id": None, "path": None})
        tail_cursor = _cursor_token(
            stream="evidence",
            task_id=task.id,
            rowid=999_800,
            scope_hash=scope_hash,
        )
        tail_started = time.perf_counter()
        tail = facade.evidence(
            task.id, offset=0, limit=200, cursor=tail_cursor
        )
        tail_seconds = time.perf_counter() - tail_started
        assert len(tail["evidence"]["items"]) == 200
        assert tail["evidence"]["has_more"] is False
        assert tail["evidence"]["items"][0]["line_start"] == 999_801

        # Broad portability rails. Release evidence records actual measurements
        # and compares them with the approved CI p95 baseline.
        assert insert_seconds < 60
        assert first_seconds < 2
        assert tail_seconds < 2
    finally:
        store.close()


def test_one_hundred_findings_are_deduped_and_returned_in_bounded_pages(
    tmp_path,
) -> None:
    """PERF-Q-05: exercise typed fingerprints and the real quality read model."""

    store = OrchestrationStore(tmp_path / "state" / "orchestration.db")
    artifacts = ArtifactService(
        store, ContentAddressedBlobStore(tmp_path / "state" / "blobs")
    )
    try:
        task = store.create_task(
            TaskSpec(idempotency_key="perf-findings-100", objective="Bound findings")
        )
        artifact = artifacts.store_internal_json(
            task_id=task.id,
            logical_deliverable_id="perf-findings",
            filename="perf_findings.json",
            value={"fixture": "findings"},
        )
        started = time.perf_counter()
        candidates = []
        for index in range(100):
            draft = FindingInput(
                category="coverage",
                severity=Severity.HIGH,
                blocking=True,
                repairable=True,
                requirement_id=f"REQ-PERF-{index:03d}",
                message=f"Required area {index:03d} is missing",
            )
            candidates.extend(
                materialize_finding(
                    draft,
                    task_id=task.id,
                    artifact_id=artifact.id,
                    artifact_hash=str(artifact.sha256),
                )
                for _duplicate in range(2)
            )
        findings = deduplicate_findings(candidates)
        fingerprint_seconds = time.perf_counter() - started
        assert len(findings) == 100
        assert len({item.fingerprint for item in findings}) == 100

        with store._write() as connection:
            connection.executemany(
                """
                INSERT INTO orch_quality_findings(
                    id, fingerprint, task_id, artifact_id, artifact_hash,
                    category, severity, blocking, repairable, requirement_id,
                    claim_id, section_id, message, evidence_refs_json,
                    suggested_fix, status, supersedes_finding_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id,
                        item.fingerprint,
                        item.task_id,
                        item.artifact_id,
                        item.artifact_hash,
                        item.category.value,
                        item.severity.value,
                        int(item.blocking),
                        int(item.repairable),
                        item.requirement_id,
                        item.claim_id,
                        item.section_id,
                        item.message,
                        json.dumps(list(item.evidence_refs)),
                        item.suggested_fix,
                        item.status.value,
                        item.supersedes_finding_id,
                        item.created_at.isoformat().replace("+00:00", "Z"),
                    )
                    for item in findings
                ],
            )

        facade = TaskQualityFacade(
            SimpleNamespace(
                store=store,
                quality_artifacts=artifacts,
                quality_contracts=None,
                quality_repository_resolver=None,
                quality_snapshots=None,
                quality_strategies=None,
                quality_budgets=None,
            )
        )
        page_started = time.perf_counter()
        first = facade.quality(task.id, offset=0, limit=25)
        page_seconds = time.perf_counter() - page_started
        first_page = first["findings"]
        assert len(first_page["items"]) == 25
        assert first_page["has_more"] is True
        assert first_page["next_cursor"]
        second = facade.quality(
            task.id,
            offset=0,
            limit=25,
            finding_cursor=first_page["next_cursor"],
        )["findings"]
        assert len(second["items"]) == 25
        assert not {
            item["id"] for item in first_page["items"]
        }.intersection(item["id"] for item in second["items"])
        assert len(json.dumps(first_page, sort_keys=True)) < 128_000
        assert fingerprint_seconds < 1
        assert page_seconds < 2
    finally:
        store.close()
