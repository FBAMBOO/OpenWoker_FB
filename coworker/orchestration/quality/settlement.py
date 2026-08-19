"""Server-authoritative settlement for all Task Quality V2 role results."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from ..store import OrchestrationStore
from .evidence import EvidenceLedger
from .findings import lint_unstructured_finding, materialize_finding
from .gates import create_gate_result
from .models import (
    FindingInput,
    GateSubjectType,
    RubricDimensionScore,
    TriState,
)
from .rubrics import create_rubric_score, rubric_for_archetype
from .schemas import (
    SchemaRegistryError,
    bind_result_context,
    validate_model_result,
)


def _now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class QualitySettlementDependencies:
    store: OrchestrationStore
    contracts: Any
    strategies: Any
    artifacts: Any
    snapshots: Any


class QualityResultSettlementService:
    """Validate exact schemas, bind identity, and persist role-authorized facts."""

    def __init__(self, dependencies: QualitySettlementDependencies) -> None:
        self.dependencies = dependencies

    def settle(
        self,
        context: Any,
        raw: Mapping[str, Any],
        *,
        expected_schema_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        deps = self.dependencies
        store = deps.store
        validated = validate_model_result(
            raw,
            expected_schema_id=expected_schema_id,
            expected_schema_version=2,
        )
        structured = validated.model_dump(mode="json")
        with store._read() as connection:
            task = connection.execute(
                """
                SELECT active_contract_id, active_snapshot_id, active_strategy_id
                FROM orch_tasks WHERE id=?
                """,
                (context.task.id,),
            ).fetchone()
        if (
            task is None
            or not task["active_contract_id"]
            or not task["active_snapshot_id"]
            or not task["active_strategy_id"]
        ):
            raise SchemaRegistryError(
                "quality result cannot bind without frozen contract, snapshot and strategy"
            )
        contract = deps.contracts.get(task["active_contract_id"])
        strategy = deps.strategies.get(task["active_strategy_id"])
        if contract.task_id != context.task.id or strategy.task_id != context.task.id:
            raise SchemaRegistryError("quality settlement identity is cross-task")
        execution_status = str(structured.get("execution_status") or "")
        if execution_status == "completed":
            if expected_schema_id == "analysis_report_result_v2":
                self._settle_analysis(
                    context,
                    structured,
                    contract,
                    snapshot_id=str(task["active_snapshot_id"]),
                )
            elif expected_schema_id == "review_result_v2":
                self._settle_review(context, structured, contract, strategy)
            elif expected_schema_id == "evidence_bundle_result_v2":
                self._settle_evidence(context, structured)
            elif expected_schema_id == "final_quality_decision_v2":
                self._settle_final_decision(context, structured)
        elif execution_status in {"partial", "failed"}:
            self._validate_noncompleted_artifacts(context, structured)
        else:
            raise SchemaRegistryError("quality result has no recognized execution status")

        bound = bind_result_context(
            validated,
            task_id=context.task.id,
            run_id=context.claim.run.id,
            contract_id=str(task["active_contract_id"]),
            snapshot_id=str(task["active_snapshot_id"]),
        )
        return structured, bound.model_dump(mode="json")

    def _settle_analysis(
        self,
        context: Any,
        structured: Mapping[str, Any],
        contract: Any,
        *,
        snapshot_id: str,
    ) -> None:
        primary = dict(structured.get("primary_artifact") or {})
        artifact = self.dependencies.artifacts.get(str(primary.get("artifact_id") or ""))
        deliverable = next(
            (item for item in contract.deliverables if item.primary),
            None,
        )
        if (
            deliverable is None
            or artifact.task_id != context.task.id
            or artifact.logical_deliverable_id != deliverable.id
            or artifact.status.value in {"uploading", "rejected"}
            or artifact.sha256 != primary.get("sha256")
            or artifact.filename != primary.get("filename")
            or artifact.mime_type != primary.get("mime_type")
            or artifact.byte_size != primary.get("byte_size")
            or artifact.run_id != context.claim.run.id
        ):
            raise SchemaRegistryError(
                "analysis primary_artifact does not match this run's immutable task artifact"
            )
        requirement_ids = {item.id for item in contract.requirements}
        for claim in structured.get("requirement_claims") or ():
            requirement_id = str(dict(claim).get("requirement_id") or "")
            if requirement_id not in requirement_ids:
                raise SchemaRegistryError(
                    "analysis result references an undeclared contract requirement"
                )
            self._assert_ids(
                table="orch_evidence_refs e JOIN orch_claims c ON c.id=e.claim_id",
                ids=dict(claim).get("evidence_ids") or (),
                task_id=context.task.id,
                task_column="c.task_id",
                id_column="e.id",
                label="evidence reference",
                )
        self._materialize_analysis_claims(
            context,
            artifact=artifact,
            contract=contract,
            snapshot_id=snapshot_id,
            coverage_claims=structured.get("coverage_claims") or (),
        )
        with self.dependencies.store._write() as connection:
            connection.execute(
                """
                UPDATE orch_tasks
                SET primary_artifact_id=?, artifact_status=?, quality_status='checking'
                WHERE id=?
                """,
                (artifact.id, artifact.status.value, context.task.id),
            )

    def _materialize_analysis_claims(
        self,
        context: Any,
        *,
        artifact: Any,
        contract: Any,
        snapshot_id: str,
        coverage_claims: Iterable[Any],
    ) -> None:
        """Turn producer claim-ledger rows into server-validated canonical records."""

        ledger = EvidenceLedger(self.dependencies.store, self.dependencies.snapshots)
        requirement_ids = {item.id for item in contract.requirements}
        seen_keys: set[str] = set()
        for index, raw in enumerate(coverage_claims):
            if not isinstance(raw, Mapping):
                raise SchemaRegistryError("analysis coverage_claims entries must be objects")
            item = dict(raw)
            key = str(item.get("claim_key") or f"claim-{index + 1}").strip()
            if not key or key in seen_keys:
                raise SchemaRegistryError("analysis claim keys must be non-empty and unique")
            seen_keys.add(key)
            requirement_id = str(item.get("requirement_id") or "").strip()
            chosen_requirements = tuple(
                dict.fromkeys(
                    (
                        *(str(value) for value in item.get("requirement_ids") or ()),
                        requirement_id,
                    )
                )
            )
            chosen_requirements = tuple(value for value in chosen_requirements if value)
            if (
                not requirement_id
                or requirement_id not in requirement_ids
                or not set(chosen_requirements).issubset(requirement_ids)
            ):
                raise SchemaRegistryError(
                    "analysis claim references an undeclared contract requirement"
                )
            area = str(item.get("area") or "").strip()
            section_id = str(item.get("section_id") or area).strip()
            text = str(item.get("text") or "").strip()
            if not area or not section_id or not text:
                raise SchemaRegistryError(
                    "analysis claim requires area, section_id and non-empty text"
                )
            source_evidence_ids = tuple(
                dict.fromkeys(
                    str(value)
                    for value in (
                        item.get("source_evidence_ids")
                        or item.get("evidence_ids")
                        or ()
                    )
                    if str(value)
                )
            )
            source_negative_ids = tuple(
                dict.fromkeys(
                    str(value)
                    for value in item.get("source_negative_search_ids") or ()
                    if str(value)
                )
            )
            with self.dependencies.store._read() as connection:
                source_evidence = (
                    connection.execute(
                        """
                        SELECT e.* FROM orch_evidence_refs e
                        JOIN orch_claims c ON c.id=e.claim_id
                        WHERE c.task_id=? AND e.snapshot_id=? AND e.id IN (
                        """
                        + ",".join("?" for _ in source_evidence_ids)
                        + ")",
                        (context.task.id, snapshot_id, *source_evidence_ids),
                    ).fetchall()
                    if source_evidence_ids
                    else ()
                )
                source_negative = (
                    connection.execute(
                        """
                        SELECT n.* FROM orch_negative_evidence n
                        JOIN orch_claims c ON c.id=n.claim_id
                        WHERE c.task_id=? AND n.id IN (
                        """
                        + ",".join("?" for _ in source_negative_ids)
                        + ")",
                        (context.task.id, *source_negative_ids),
                    ).fetchall()
                    if source_negative_ids
                    else ()
                )
            if {str(row["id"]) for row in source_evidence} != set(
                source_evidence_ids
            ) or {str(row["id"]) for row in source_negative} != set(
                source_negative_ids
            ):
                raise SchemaRegistryError(
                    "analysis claim references missing, stale-snapshot, or cross-task evidence"
                )
            claim = ledger.create_claim(
                task_id=context.task.id,
                artifact_id=artifact.id,
                section_id=section_id,
                text=text,
                claim_type=str(item.get("claim_type") or "fact"),
                severity=str(item.get("severity") or "info"),
                confidence=float(item.get("confidence", 1.0)),
                requirement_ids=chosen_requirements,
                source_key=f"{context.claim.run.id}:{key}",
            )
            copied_evidence_ids: list[str] = []
            for source in source_evidence:
                copied = ledger.create_file_evidence(
                    claim_id=claim.id,
                    snapshot_id=snapshot_id,
                    path=str(source["path"]),
                    line_start=int(source["line_start"]),
                    line_end=int(source["line_end"]),
                    support=str(source["support"]),
                    created_by_run_id=context.claim.run.id,
                )
                copied_evidence_ids.append(copied.id)
            copied_negative_ids: list[str] = []
            for source in source_negative:
                copied = ledger.create_negative_evidence(
                    claim_id=claim.id,
                    query=str(source["query"]),
                    tool_version=str(source["tool_version"]),
                    scope_paths=tuple(json.loads(source["scope_paths_json"])),
                    excluded_paths=tuple(json.loads(source["excluded_paths_json"])),
                    result_count=int(source["result_count"]),
                    query_result_hash=str(source["query_result_hash"]),
                    limitations=tuple(json.loads(source["limitations_json"])),
                )
                copied_negative_ids.append(copied.id)
            evidence_count = len(copied_evidence_ids) + len(copied_negative_ids)
            ledger.record_coverage(
                task_id=context.task.id,
                artifact_id=artifact.id,
                requirement_id=requirement_id,
                area=area,
                status="pass" if evidence_count else "unknown",
                claim_ids=(claim.id,),
                evidence_count=evidence_count,
                notes=str(item.get("notes") or "server-derived from copied evidence"),
                validator_id=f"analysis-ledger:{context.claim.run.id}",
            )

    def _settle_evidence(
        self,
        context: Any,
        structured: Mapping[str, Any],
    ) -> None:
        self._assert_ids(
            table="orch_claims",
            ids=structured.get("claim_ids") or (),
            task_id=context.task.id,
            task_column="task_id",
            label="claim",
        )
        self._assert_ids(
            table="orch_evidence_refs e JOIN orch_claims c ON c.id=e.claim_id",
            ids=structured.get("evidence_ref_ids") or (),
            task_id=context.task.id,
            task_column="c.task_id",
            id_column="e.id",
            label="evidence reference",
        )
        self._assert_ids(
            table=(
                "orch_inventory_metrics m "
                "JOIN orch_repository_inventories i ON i.id=m.inventory_id "
                "JOIN orch_repository_snapshots s ON s.id=i.snapshot_id"
            ),
            ids=structured.get("inventory_metric_ids") or (),
            task_id=context.task.id,
            task_column="s.task_id",
            id_column="m.id",
            label="inventory metric",
        )
        self._assert_ids(
            table="orch_negative_evidence n JOIN orch_claims c ON c.id=n.claim_id",
            ids=structured.get("negative_search_ids") or (),
            task_id=context.task.id,
            task_column="c.task_id",
            id_column="n.id",
            label="negative evidence",
        )
        claim_ids = tuple(str(item) for item in structured.get("claim_ids") or ())
        if claim_ids:
            with self.dependencies.store._write() as connection:
                connection.execute(
                    "UPDATE orch_claims SET status='verified' WHERE id IN ("
                    + ",".join("?" for _ in claim_ids)
                    + ") AND status='draft'",
                    claim_ids,
                )

    def _settle_review(
        self,
        context: Any,
        structured: Mapping[str, Any],
        contract: Any,
        strategy: Any,
    ) -> None:
        artifact = self.dependencies.artifacts.get(
            str(structured.get("subject_artifact_id") or "")
        )
        if (
            artifact.task_id != context.task.id
            or artifact.sha256 != structured.get("subject_artifact_hash")
            or artifact.status.value in {"uploading", "rejected"}
        ):
            raise SchemaRegistryError(
                "review subject does not match an immutable task artifact"
            )
        receipt = self.dependencies.artifacts.fresh_complete_receipt(
            run_id=context.claim.run.id,
            artifact_id=artifact.id,
            expected_sha256=str(artifact.sha256),
        )
        if structured.get("verdict") == "pass" and receipt is None:
            raise SchemaRegistryError(
                "a passing review requires a fresh server-derived 100% read receipt"
            )
        requirement_by_id = {item.id: item for item in contract.requirements}
        criterion_inputs = [dict(item) for item in structured.get("criterion_results") or ()]
        submitted_ids = {str(item.get("requirement_id") or "") for item in criterion_inputs}
        required_ids = {item.id for item in contract.requirements if item.required}
        if not required_ids.issubset(submitted_ids) or not submitted_ids.issubset(
            requirement_by_id
        ):
            raise SchemaRegistryError(
                "review criterion_results do not cover exactly declared requirements"
            )
        for item in criterion_inputs:
            self._assert_ids(
                table="orch_evidence_refs e JOIN orch_claims c ON c.id=e.claim_id",
                ids=item.get("evidence_ids") or (),
                task_id=context.task.id,
                task_column="c.task_id",
                id_column="e.id",
                label="criterion evidence",
            )
        finding_inputs = [FindingInput.model_validate(item) for item in structured.get("findings") or ()]
        linted = lint_unstructured_finding(
            summary=str(structured.get("summary") or ""),
            findings=finding_inputs,
            required_v2=True,
        )
        if linted is not None:
            finding_inputs.append(linted)
        findings = [
            materialize_finding(
                item,
                task_id=context.task.id,
                artifact_id=artifact.id,
                artifact_hash=str(artifact.sha256),
                required_v2=True,
            )
            for item in finding_inputs
        ]
        for finding in findings:
            self._assert_ids(
                table="orch_evidence_refs e JOIN orch_claims c ON c.id=e.claim_id",
                ids=finding.evidence_refs,
                task_id=context.task.id,
                task_column="c.task_id",
                id_column="e.id",
                label="finding evidence",
            )

        scores = structured.get("rubric_dimension_scores")
        rubric_score = None
        rubric = rubric_for_archetype(strategy.archetype)
        if scores is not None:
            if strategy.semantic_scorer_node_key != context.node.key:
                raise SchemaRegistryError(
                    "only the frozen semantic scorer node may submit dimension scores"
                )
            rubric_score = create_rubric_score(
                rubric=rubric,
                artifact_id=artifact.id,
                artifact_hash=str(artifact.sha256),
                scorer_run_id=context.claim.run.id,
                authorized_scorer_run_id=context.claim.run.id,
                dimension_scores=(
                    RubricDimensionScore.model_validate(item) for item in scores
                ),
                read_receipt=receipt,
            )

        now = _now()
        finding_ids: list[str] = []
        gate_ids: list[str] = []
        with self.dependencies.store._write() as connection:
            for finding in findings:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO orch_quality_findings(
                        id, fingerprint, task_id, artifact_id, artifact_hash,
                        category, severity, blocking, repairable, requirement_id,
                        claim_id, section_id, message, evidence_refs_json,
                        suggested_fix, status, supersedes_finding_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        finding.id,
                        finding.fingerprint,
                        finding.task_id,
                        finding.artifact_id,
                        finding.artifact_hash,
                        finding.category.value,
                        finding.severity.value,
                        int(finding.blocking),
                        int(finding.repairable),
                        finding.requirement_id,
                        finding.claim_id,
                        finding.section_id,
                        finding.message,
                        _json(list(finding.evidence_refs)),
                        finding.suggested_fix,
                        finding.status.value,
                        finding.supersedes_finding_id,
                        now,
                    ),
                )
                existing = connection.execute(
                    """
                    SELECT id FROM orch_quality_findings
                    WHERE task_id=? AND artifact_id=? AND fingerprint=?
                    """,
                    (context.task.id, artifact.id, finding.fingerprint),
                ).fetchone()
                if existing is not None:
                    finding_ids.append(str(existing["id"]))
            for item in criterion_inputs:
                requirement = requirement_by_id[str(item["requirement_id"])]
                gate = create_gate_result(
                    gate_id="QG-003",
                    task_id=context.task.id,
                    artifact_id=artifact.id,
                    artifact_hash=str(artifact.sha256),
                    status=TriState(str(item["status"])),
                    validator_id=f"review:{context.claim.run.id}",
                    evidence_ids=item.get("evidence_ids") or (),
                    waivable_override=bool(requirement.waivable),
                ).model_copy(
                    update={
                        "subject_type": GateSubjectType.CRITERION,
                        "subject_id": requirement.id,
                        "reason_code": "required_criterion_not_passed",
                    }
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO orch_gate_results(
                        id, task_id, artifact_id, artifact_hash, subject_type,
                        subject_id, subject_version, status, waivable, reason_code,
                        evidence_ids_json, validator_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        gate.id,
                        gate.task_id,
                        gate.artifact_id,
                        gate.artifact_hash,
                        gate.subject_type.value,
                        gate.subject_id,
                        gate.subject_version,
                        gate.status.value,
                        int(gate.waivable),
                        gate.reason_code,
                        _json(list(gate.evidence_ids)),
                        gate.validator_id,
                        now,
                    ),
                )
                persisted = connection.execute(
                    """
                    SELECT id FROM orch_gate_results
                    WHERE task_id=? AND artifact_id=? AND subject_type='criterion'
                      AND subject_id=? AND subject_version=1 AND validator_id=?
                    """,
                    (
                        context.task.id,
                        artifact.id,
                        requirement.id,
                        f"review:{context.claim.run.id}",
                    ),
                ).fetchone()
                if persisted is not None:
                    gate_ids.append(str(persisted["id"]))
            rubric_score_id = None
            total_score = None
            if rubric_score is not None:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO orch_rubric_scores(
                        id, rubric_id, rubric_version, artifact_id, artifact_hash,
                        scorer_run_id, dimension_scores_json, total, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rubric_score.id,
                        rubric_score.rubric_id,
                        rubric_score.rubric_version,
                        rubric_score.artifact_id,
                        rubric_score.artifact_hash,
                        rubric_score.scorer_run_id,
                        _json(
                            [item.model_dump(mode="json") for item in rubric_score.dimension_scores]
                        ),
                        rubric_score.total,
                        now,
                    ),
                )
                persisted_score = connection.execute(
                    """
                    SELECT id, total FROM orch_rubric_scores
                    WHERE artifact_id=? AND rubric_id=? AND rubric_version=?
                      AND scorer_run_id=?
                    """,
                    (
                        artifact.id,
                        rubric_score.rubric_id,
                        rubric_score.rubric_version,
                        context.claim.run.id,
                    ),
                ).fetchone()
                rubric_score_id = str(persisted_score["id"])
                total_score = int(persisted_score["total"])
            evaluation_payload = {
                "artifact_id": artifact.id,
                "artifact_hash": artifact.sha256,
                "criterion_results": criterion_inputs,
                "finding_ids": sorted(finding_ids),
                "rubric_score_id": rubric_score_id,
                "verdict": str(structured.get("verdict")),
                "read_receipt_id": receipt.id if receipt else None,
            }
            content_hash = _hash(evaluation_payload)
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_quality_evaluations(
                    id, task_id, artifact_id, artifact_hash, evaluation_type,
                    validator_id, validator_version, rubric_id, rubric_version,
                    criterion_results_json, coverage_results_json, rubric_score_id,
                    total_score, verdict, read_receipt_id, finding_ids_json,
                    created_by_run_id, content_hash, created_at
                ) VALUES (?, ?, ?, ?, 'review', ?, '2', ?, ?, ?, '[]', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"evaluation_{uuid.uuid4().hex}",
                    context.task.id,
                    artifact.id,
                    artifact.sha256,
                    f"review:{context.node.key}",
                    rubric.id if rubric_score else None,
                    rubric.version if rubric_score else None,
                    _json(criterion_inputs),
                    rubric_score_id,
                    total_score,
                    str(structured.get("verdict")),
                    receipt.id if receipt else None,
                    _json(sorted(finding_ids)),
                    context.claim.run.id,
                    content_hash,
                    now,
                ),
            )

    def _settle_final_decision(
        self,
        context: Any,
        structured: Mapping[str, Any],
    ) -> None:
        artifact = self.dependencies.artifacts.get(
            str(structured.get("subject_artifact_id") or "")
        )
        if (
            artifact.task_id != context.task.id
            or artifact.sha256 != structured.get("subject_artifact_hash")
        ):
            raise SchemaRegistryError("final decision subject artifact is invalid")
        self._assert_ids(
            table="orch_gate_results",
            ids=structured.get("hard_gate_results") or (),
            task_id=context.task.id,
            task_column="task_id",
            label="hard-gate result",
        )
        self._assert_ids(
            table="orch_quality_findings",
            ids=structured.get("open_blocking_finding_ids") or (),
            task_id=context.task.id,
            task_column="task_id",
            label="quality finding",
        )
        self._assert_ids(
            table="orch_gate_results",
            ids=structured.get("criterion_results") or (),
            task_id=context.task.id,
            task_column="task_id",
            label="criterion result",
        )
        self._assert_ids(
            table="orch_rubric_scores r JOIN orch_artifact_versions a ON a.id=r.artifact_id",
            ids=(structured.get("rubric_score_id"),),
            task_id=context.task.id,
            task_column="a.task_id",
            id_column="r.id",
            label="rubric score",
        )

    def _validate_noncompleted_artifacts(
        self,
        context: Any,
        structured: Mapping[str, Any],
    ) -> None:
        if structured.get("execution_status") == "partial":
            checkpoint = dict(structured.get("checkpoint") or {})
            artifact = self.dependencies.artifacts.get(
                str(checkpoint.get("artifact_id") or "")
            )
            if (
                artifact.task_id != context.task.id
                or artifact.sha256 != checkpoint.get("content_hash")
                or artifact.status.value == "uploading"
            ):
                raise SchemaRegistryError(
                    "partial result checkpoint is not an immutable task artifact"
                )
            ids = structured.get("provisional_artifact_ids") or ()
        else:
            ids = structured.get("diagnostic_artifact_ids") or ()
        self._assert_ids(
            table="orch_artifact_versions",
            ids=ids,
            task_id=context.task.id,
            task_column="task_id",
            label="diagnostic artifact",
        )

    def _assert_ids(
        self,
        *,
        table: str,
        ids: Iterable[Any],
        task_id: str,
        task_column: str,
        label: str,
        id_column: str = "id",
    ) -> None:
        chosen = tuple(dict.fromkeys(str(item) for item in ids if item is not None and str(item)))
        if not chosen:
            return
        with self.dependencies.store._read() as connection:
            rows = connection.execute(
                f"SELECT {id_column} AS id FROM {table} "
                f"WHERE {task_column}=? AND {id_column} IN ("
                + ",".join("?" for _ in chosen)
                + ")",
                (task_id, *chosen),
            ).fetchall()
        observed = {str(row["id"]) for row in rows}
        if observed != set(chosen):
            raise SchemaRegistryError(
                f"quality result references missing or cross-task {label} ids"
            )
