"""Typed claim, evidence, coverage and inventory-metric ledger."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from ..errors import ConflictError, NotFoundError
from ..store import OrchestrationStore
from .models import (
    Claim,
    ClaimStatus,
    ClaimType,
    CoverageResult,
    EvidenceRef,
    EvidenceSupport,
    InventoryMetric,
    NegativeEvidence,
    Severity,
    TriState,
)
from .repository_snapshot import RepositorySnapshotService


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class EvidenceLedger:
    def __init__(
        self, store: OrchestrationStore, snapshots: RepositorySnapshotService
    ) -> None:
        self.store = store
        self.snapshots = snapshots

    def create_claim(
        self,
        *,
        task_id: str,
        artifact_id: str,
        section_id: str,
        text: str,
        claim_type: ClaimType | str,
        severity: Severity | str = Severity.INFO,
        confidence: float = 1.0,
        requirement_ids: Iterable[str] = (),
        source_key: str | None = None,
    ) -> Claim:
        claim_id = f"claim_{uuid.uuid4().hex}"
        normalized_source_key = str(source_key).strip() if source_key else None
        chosen_requirement_ids = tuple(str(item) for item in requirement_ids)
        with self.store._write() as connection:
            artifact = connection.execute(
                """
                SELECT task_id, version, status FROM orch_artifact_versions WHERE id = ?
                """,
                (artifact_id,),
            ).fetchone()
            if artifact is None:
                raise NotFoundError(f"artifact {artifact_id} not found")
            if artifact["task_id"] != task_id:
                raise PermissionError("claim artifact is outside the task namespace")
            if artifact["status"] == "uploading":
                raise ConflictError("claims cannot bind an unfinished artifact")
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_claims(
                    id, task_id, artifact_id, artifact_version, section_id, text,
                    claim_type, severity, confidence, requirement_ids_json, source_key,
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?)
                """,
                (
                    claim_id,
                    task_id,
                    artifact_id,
                    artifact["version"],
                    section_id,
                    text,
                    ClaimType(claim_type).value,
                    Severity(severity).value,
                    float(confidence),
                    _json(list(chosen_requirement_ids)),
                    normalized_source_key,
                    _now(),
                ),
            )
            if normalized_source_key:
                winner = connection.execute(
                    """
                    SELECT * FROM orch_claims
                    WHERE artifact_id=? AND source_key=?
                    """,
                    (artifact_id, normalized_source_key),
                ).fetchone()
                if winner is None:
                    raise ConflictError("idempotent claim write disappeared")
                expected = (
                    task_id,
                    artifact_id,
                    section_id,
                    text,
                    ClaimType(claim_type).value,
                    Severity(severity).value,
                    float(confidence),
                    _json(list(chosen_requirement_ids)),
                )
                observed = (
                    winner["task_id"],
                    winner["artifact_id"],
                    winner["section_id"],
                    winner["text"],
                    winner["claim_type"],
                    winner["severity"],
                    float(winner["confidence"]),
                    winner["requirement_ids_json"],
                )
                if observed != expected:
                    raise ConflictError(
                        "claim source_key was replayed with different content"
                    )
                claim_id = str(winner["id"])
        return self.get_claim(claim_id)

    def get_claim(self, claim_id: str) -> Claim:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_claims WHERE id = ?", (claim_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"claim {claim_id} not found")
        return Claim(
            id=row["id"],
            task_id=row["task_id"],
            artifact_id=row["artifact_id"],
            artifact_version=row["artifact_version"],
            section_id=row["section_id"],
            text=row["text"],
            claim_type=row["claim_type"],
            severity=row["severity"],
            confidence=row["confidence"],
            requirement_ids=tuple(json.loads(row["requirement_ids_json"])),
            status=row["status"],
        )

    def create_file_evidence(
        self,
        *,
        claim_id: str,
        snapshot_id: str,
        path: str,
        line_start: int,
        line_end: int,
        support: EvidenceSupport | str,
        created_by_run_id: str,
    ) -> EvidenceRef:
        claim = self.get_claim(claim_id)
        snapshot = self.snapshots.get(snapshot_id)
        if snapshot.task_id != claim.task_id:
            raise PermissionError("evidence snapshot is outside the claim task")
        excerpt = self.snapshots.read_file_lines(
            snapshot_id, path, start_line=line_start, end_line=line_end
        )
        evidence_id = f"evidence_{uuid.uuid4().hex}"
        with self.store._write() as connection:
            run = connection.execute(
                "SELECT task_id FROM orch_runs WHERE id = ?", (created_by_run_id,)
            ).fetchone()
            if run is None or run["task_id"] != claim.task_id:
                raise PermissionError("evidence producer run is outside the claim task")
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_evidence_refs(
                    id, claim_id, snapshot_id, path, line_start, line_end,
                    blob_hash, excerpt_hash, evidence_type, support,
                    content_withheld, created_by_run_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'file_range', ?, 0, ?, ?)
                """,
                (
                    evidence_id,
                    claim_id,
                    snapshot_id,
                    excerpt["path"],
                    excerpt["line_start"],
                    excerpt["line_end"],
                    excerpt["blob_hash"],
                    excerpt["excerpt_hash"],
                    EvidenceSupport(support).value,
                    created_by_run_id,
                    _now(),
                ),
            )
            winner = connection.execute(
                """
                SELECT * FROM orch_evidence_refs
                WHERE claim_id=? AND snapshot_id=? AND path=? AND line_start=?
                  AND line_end=? AND support=?
                """,
                (
                    claim_id,
                    snapshot_id,
                    excerpt["path"],
                    excerpt["line_start"],
                    excerpt["line_end"],
                    EvidenceSupport(support).value,
                ),
            ).fetchone()
            if winner is None:
                raise ConflictError("idempotent evidence write disappeared")
            evidence_id = str(winner["id"])
        return EvidenceRef(
            id=evidence_id,
            claim_id=claim_id,
            snapshot_id=snapshot_id,
            path=excerpt["path"],
            line_start=excerpt["line_start"],
            line_end=excerpt["line_end"],
            blob_hash=excerpt["blob_hash"],
            excerpt_hash=excerpt["excerpt_hash"],
            support=support,
            created_by_run_id=created_by_run_id,
        )

    def create_negative_evidence(
        self,
        *,
        claim_id: str,
        query: str,
        tool_version: str,
        scope_paths: Iterable[str],
        excluded_paths: Iterable[str],
        result_count: int,
        query_result_hash: str,
        limitations: Iterable[str],
    ) -> NegativeEvidence:
        claim = self.get_claim(claim_id)
        if claim.claim_type is not ClaimType.ABSENCE:
            raise ConflictError("negative evidence may bind only an absence claim")
        record = NegativeEvidence(
            id=f"negative_{uuid.uuid4().hex}",
            claim_id=claim_id,
            query=query,
            tool_version=tool_version,
            scope_paths=tuple(scope_paths),
            excluded_paths=tuple(excluded_paths),
            result_count=result_count,
            query_result_hash=query_result_hash,
            limitations=tuple(limitations),
        )
        with self.store._write() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_negative_evidence(
                    id, claim_id, query, tool_version, scope_paths_json,
                    excluded_paths_json, result_count, query_result_hash,
                    limitations_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.claim_id,
                    record.query,
                    record.tool_version,
                    _json(list(record.scope_paths)),
                    _json(list(record.excluded_paths)),
                    record.result_count,
                    record.query_result_hash,
                    _json(list(record.limitations)),
                    _now(),
                ),
            )
            winner = connection.execute(
                """
                SELECT id FROM orch_negative_evidence
                WHERE claim_id=? AND query=? AND query_result_hash=?
                """,
                (record.claim_id, record.query, record.query_result_hash),
            ).fetchone()
        if winner is None:
            raise ConflictError("idempotent negative-evidence write disappeared")
        return record.model_copy(update={"id": str(winner["id"])})

    def record_coverage(
        self,
        *,
        task_id: str,
        artifact_id: str,
        requirement_id: str,
        area: str,
        status: TriState | str,
        claim_ids: Iterable[str],
        evidence_count: int,
        notes: str,
        validator_id: str,
    ) -> CoverageResult:
        record = CoverageResult(
            requirement_id=requirement_id,
            area=area,
            status=status,
            claim_ids=tuple(claim_ids),
            evidence_count=evidence_count,
            notes=notes,
            validator_id=validator_id,
        )
        with self.store._write() as connection:
            artifact = connection.execute(
                "SELECT task_id FROM orch_artifact_versions WHERE id = ?", (artifact_id,)
            ).fetchone()
            if artifact is None or artifact["task_id"] != task_id:
                raise PermissionError("coverage artifact is outside the task namespace")
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_coverage_results(
                    id, task_id, artifact_id, requirement_id, area, status,
                    claim_ids_json, evidence_count, notes, validator_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"coverage_{uuid.uuid4().hex}",
                    task_id,
                    artifact_id,
                    requirement_id,
                    area,
                    record.status.value,
                    _json(list(record.claim_ids)),
                    record.evidence_count,
                    record.notes,
                    record.validator_id,
                    _now(),
                ),
            )
        return record

    def record_inventory_metric(
        self,
        *,
        inventory_id: str,
        name: str,
        value: int | float,
        unit: str,
        query_key: str,
        subtotals: Mapping[str, int | float],
        reconciles_to: int | float | None,
        tolerance: float = 0,
    ) -> InventoryMetric:
        record = InventoryMetric(
            id=f"metric_{uuid.uuid4().hex}",
            name=name,
            value=value,
            unit=unit,
            query_key=query_key,
            subtotals=dict(subtotals),
            reconciles_to=reconciles_to,
            tolerance=tolerance,
        )
        with self.store._write() as connection:
            if connection.execute(
                "SELECT 1 FROM orch_repository_inventories WHERE id = ?", (inventory_id,)
            ).fetchone() is None:
                raise NotFoundError(f"inventory {inventory_id} not found")
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_inventory_metrics(
                    id, inventory_id, name, value, unit, query_key, subtotals_json,
                    reconciles_to, tolerance, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    inventory_id,
                    record.name,
                    record.value,
                    record.unit,
                    record.query_key,
                    _json(dict(record.subtotals)),
                    record.reconciles_to,
                    record.tolerance,
                    _now(),
                ),
            )
            winner = connection.execute(
                """
                SELECT id FROM orch_inventory_metrics
                WHERE inventory_id=? AND name=? AND query_key=?
                """,
                (inventory_id, record.name, record.query_key),
            ).fetchone()
        if winner is None:
            raise ConflictError("idempotent inventory-metric write disappeared")
        return record.model_copy(update={"id": str(winner["id"])})

    @staticmethod
    def metric_reconciles(metric: InventoryMetric) -> bool:
        if metric.reconciles_to is None:
            return False
        subtotal = sum(float(value) for value in metric.subtotals.values())
        return abs(subtotal - float(metric.reconciles_to)) <= metric.tolerance and abs(
            float(metric.value) - float(metric.reconciles_to)
        ) <= metric.tolerance
