"""Deterministic Task Quality V2 service-node execution and adjudication."""

from __future__ import annotations

import json
import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from ..errors import ConflictError
from ..executor import ExecutionOutcome
from ..store import OrchestrationStore
from .adjudicator import adjudicate
from .models import (
    Archetype,
    ArtifactVersionStatus,
    BudgetStatus,
    CoverageResult,
    Finding,
    GateResult,
    QualityWaiver,
    RubricDimensionScore,
    RubricScore,
)
from .repair import RepairCoordinator, RepairExhausted
from .rubrics import rubric_for_archetype
from .state_machine import WorkflowEvent, transition_workflow_in_transaction
from .validators.base import ValidationInputs
from .validators.engine import FOCUSED_QUESTION_GATE_IDS


@dataclass(frozen=True)
class QualityWorkflowDependencies:
    store: OrchestrationStore
    contracts: Any
    snapshots: Any
    strategies: Any
    artifacts: Any
    inventories: Any
    validators: Any
    budgets: Any


class QualityWorkflowEngine:
    """Execute frozen ``role=service`` nodes without spending model budget."""

    def __init__(self, dependencies: QualityWorkflowDependencies) -> None:
        self.dependencies = dependencies
        self.repairs = RepairCoordinator(dependencies.store, dependencies.artifacts)

    def execute(self, context: Any) -> ExecutionOutcome:
        kind = str(context.node.metadata.get("strategy_kind") or "")
        try:
            if kind == "resolve_inventory":
                return self._resolve_inventory(context)
            if kind == "deterministic_validation":
                return self._prevalidate(context)
            if kind == "server_adjudication":
                return self._adjudicate(context)
            if kind == "publish_verified_artifact":
                return self._publish(context)
            return self._failed(
                context,
                "quality_service_node_unknown",
                f"unsupported deterministic quality node kind: {kind or 'missing'}",
            )
        except Exception as exc:
            return self._failed(
                context,
                "quality_service_node_failed",
                f"{type(exc).__name__}: {exc}",
            )

    def _resolve_inventory(self, context: Any) -> ExecutionOutcome:
        _contract, snapshot, _strategy = self._active(context.task.id)
        inventory = self.dependencies.inventories.build(snapshot.id)
        return self._succeeded(
            context,
            "Shared repository inventory is ready.",
            {"repository_inventory": inventory.model_dump(mode="json")},
        )

    def _prevalidate(self, context: Any) -> ExecutionOutcome:
        inputs = self._validation_inputs(context)
        report = self._run_validators(
            inputs,
            persist_excluding=frozenset({"QG-013", "QG-014"}),
            verify_artifact=False,
        )
        adverse = [
            item
            for item in report.gate_results
            if item.subject_id not in {"QG-013", "QG-014"}
            and item.status.value != "pass"
        ]
        if adverse:
            self._mark_blocked(
                context.task.id,
                workflow="repairing",
                reason="quality_prevalidation_failed",
            )
            repairable = tuple(
                finding
                for finding in report.findings
                if finding.requirement_id not in {"QG-013", "QG-014"}
                and finding.blocking
                and finding.repairable
            )
            self._request_repair_if_possible(
                context.task.id,
                inputs.artifact.id,
                repairable,
            )
            return self._failed(
                context,
                "quality_prevalidation_failed",
                "deterministic hard gates failed before independent review",
                output={
                    "failed_gate_ids": [item.subject_id for item in adverse],
                    "finding_ids": [item.id for item in repairable],
                },
            )
        return self._succeeded(
            context,
            "Pre-review deterministic quality gates passed.",
            {
                "gate_results": [
                    item.model_dump(mode="json")
                    for item in report.gate_results
                    if item.subject_id not in {"QG-013", "QG-014"}
                ]
            },
        )

    def _adjudicate(self, context: Any) -> ExecutionOutcome:
        with self.dependencies.store._write() as connection:
            row = connection.execute(
                "SELECT workflow_status FROM orch_tasks WHERE id=?",
                (context.task.id,),
            ).fetchone()
            if row is not None and row["workflow_status"] == "validating":
                transition_workflow_in_transaction(
                    self.dependencies.store,
                    connection,
                    task_id=context.task.id,
                    event=WorkflowEvent.VALIDATION_REQUIRES_REVIEW,
                    command_id=f"quality-review:{context.claim.run.id}",
                )
        inputs = self._validation_inputs(context)
        report = self._run_validators(inputs)
        # Validator publication may have advanced the artifact status; bind the exact
        # refreshed immutable record before final adjudication.
        artifact = self.dependencies.artifacts.get(inputs.artifact.id)
        contract, _snapshot, strategy = self._active(context.task.id)
        rubric = rubric_for_archetype(contract.archetype)
        with self.dependencies.store._read() as connection:
            gate_rows = connection.execute(
                """
                SELECT * FROM orch_gate_results
                WHERE task_id=? AND artifact_id=?
                ORDER BY created_at, id
                """,
                (context.task.id, artifact.id),
            ).fetchall()
            finding_rows = connection.execute(
                """
                SELECT * FROM orch_quality_findings
                WHERE task_id=? AND artifact_id=? ORDER BY created_at, id
                """,
                (context.task.id, artifact.id),
            ).fetchall()
            score_row = connection.execute(
                """
                SELECT * FROM orch_rubric_scores
                WHERE artifact_id=? AND rubric_id=? AND rubric_version=?
                ORDER BY created_at DESC, id DESC LIMIT 1
                """,
                (artifact.id, rubric.id, rubric.version),
            ).fetchone()
            waiver_rows = connection.execute(
                """
                SELECT * FROM orch_quality_waivers
                WHERE task_id=? AND artifact_id=? ORDER BY created_at, id
                """,
                (context.task.id, artifact.id),
            ).fetchall()
            task_row = connection.execute(
                """
                SELECT budget_status FROM orch_tasks WHERE id=?
                """,
                (context.task.id,),
            ).fetchone()
            repair_attempts = int(
                connection.execute(
                    "SELECT COUNT(*) AS value FROM orch_repair_requests WHERE task_id=?",
                    (context.task.id,),
                ).fetchone()["value"]
            )
        gates = tuple(self._gate(row) for row in gate_rows)
        findings = tuple(self._finding(row) for row in finding_rows)
        score = self._score(score_row) if score_row is not None else None
        waivers = tuple(self._waiver(row) for row in waiver_rows)
        selected_gate_ids = (
            FOCUSED_QUESTION_GATE_IDS
            if contract.archetype is Archetype.FOCUSED_QUESTION
            else None
        )
        hard_gates = tuple(
            item
            for item in gates
            if item.subject_type.value == "hard_gate"
            and (selected_gate_ids is None or item.subject_id in selected_gate_ids)
        )
        criteria = tuple(item for item in gates if item.subject_type.value == "criterion")
        reviewer_run_id = inputs.reviewer_run_id
        outcome = adjudicate(
            task_id=context.task.id,
            contract_id=contract.id,
            contract_version=contract.version,
            artifact=artifact,
            candidate_hash=str(artifact.sha256),
            result_schema_valid=inputs.result_schema_valid,
            reviewer_run_id=reviewer_run_id,
            read_receipt=inputs.read_receipt,
            hard_gate_results=hard_gates,
            required_criterion_results=criteria,
            findings=findings,
            rubric=rubric,
            rubric_score=score,
            budget_status=BudgetStatus(task_row["budget_status"]),
            waivers=waivers,
            repair_attempts=repair_attempts,
            max_repair_attempts=strategy.max_repair_attempts,
        )
        if outcome.publishable:
            self.repairs.mark_validated(result_artifact_id=artifact.id)
        self._persist_final(
            context,
            artifact=artifact,
            outcome=outcome,
            gates=hard_gates,
            findings=findings,
            score=score,
            receipt=inputs.read_receipt,
            rubric=rubric,
        )
        if not outcome.publishable:
            repairable = tuple(
                item
                for item in findings
                if item.status.value == "open" and item.blocking and item.repairable
            )
            if outcome.decision.value == "repair":
                self._request_repair_if_possible(
                    context.task.id,
                    artifact.id,
                    repairable,
                )
            return self._failed(
                context,
                f"quality_{outcome.decision.value}",
                outcome.reason_code,
                output={
                    "decision": outcome.decision.value,
                    "quality_status": outcome.quality_status.value,
                    "reason_code": outcome.reason_code,
                    "failed_subjects": [
                        {
                            "subject_type": item.subject_type.value,
                            "subject_id": item.subject_id,
                            "reason_code": item.reason_code,
                        }
                        for item in outcome.uncovered_subjects
                    ],
                },
            )
        return self._succeeded(
            context,
            "Server-authoritative quality adjudication passed.",
            {
                "decision": outcome.decision.value,
                "quality_status": outcome.quality_status.value,
                "waiver_ids": list(outcome.waiver_ids),
            },
        )

    def _publish(self, context: Any) -> ExecutionOutcome:
        with self.dependencies.store._read() as connection:
            task = connection.execute(
                """
                SELECT quality_status, primary_artifact_id
                FROM orch_tasks WHERE id=?
                """,
                (context.task.id,),
            ).fetchone()
        if (
            task is None
            or task["quality_status"] not in {"pass", "waived"}
            or not task["primary_artifact_id"]
        ):
            return self._failed(
                context,
                "quality_not_publishable",
                "primary artifact cannot publish before authoritative quality pass/waiver",
            )
        with self.dependencies.store._write() as connection:
            artifact = self.dependencies.artifacts.publish_primary(
                str(task["primary_artifact_id"]),
                _connection=connection,
            )
            transition_workflow_in_transaction(
                self.dependencies.store,
                connection,
                task_id=context.task.id,
                event=WorkflowEvent.QUALITY_PUBLISHABLE,
                clear_reason=True,
                command_id=f"quality-publish:{artifact.id}",
            )
        return self._succeeded(
            context,
            "Verified primary deliverable published.",
            {"primary_artifact": artifact.model_dump(mode="json")},
        )

    def _validation_inputs(self, context: Any) -> ValidationInputs:
        contract, snapshot, strategy = self._active(context.task.id)
        with self.dependencies.store._read() as connection:
            task = connection.execute(
                """
                SELECT primary_artifact_id, budget_status, active_budget_ledger_id
                FROM orch_tasks WHERE id=?
                """,
                (context.task.id,),
            ).fetchone()
            if task is None or not task["primary_artifact_id"]:
                raise ValueError("quality validation requires a primary artifact candidate")
            artifact = self.dependencies.artifacts.get(task["primary_artifact_id"])
            coverage_rows = connection.execute(
                """
                SELECT * FROM orch_coverage_results WHERE task_id=? AND artifact_id=?
                ORDER BY created_at, id
                """,
                (context.task.id, artifact.id),
            ).fetchall()
            finding_rows = connection.execute(
                """
                SELECT * FROM orch_quality_findings WHERE task_id=? AND artifact_id=?
                ORDER BY created_at, id
                """,
                (context.task.id, artifact.id),
            ).fetchall()
            claim_rows = connection.execute(
                """
                SELECT * FROM orch_claims WHERE task_id=? AND artifact_id=?
                ORDER BY created_at, id
                """,
                (context.task.id, artifact.id),
            ).fetchall()
            evidence_rows = connection.execute(
                """
                SELECT e.* FROM orch_evidence_refs e
                JOIN orch_claims c ON c.id=e.claim_id
                WHERE c.task_id=? AND c.artifact_id=? ORDER BY e.created_at, e.id
                """,
                (context.task.id, artifact.id),
            ).fetchall()
        runs = [
            item
            for item in self.dependencies.store.list_runs(context.task.id)
            if item.plan_id == context.graph.plan.id
        ]
        by_node: dict[str, Any] = {}
        node_key_by_id = {item.id: item.key for item in context.graph.nodes}
        for run in runs:
            if run.status.value != "succeeded":
                continue
            key = node_key_by_id.get(run.node_id, "")
            previous = by_node.get(key)
            if previous is None or (run.attempt, run.created_at, run.id) > (
                previous.attempt,
                previous.created_at,
                previous.id,
            ):
                by_node[key] = run
        primary_deliverable = next(
            (item for item in contract.deliverables if item.primary),
            None,
        )
        expected_result_schema_id = (
            primary_deliverable.result_schema_id if primary_deliverable else ""
        )
        synth_node = next(
            (
                item
                for item in strategy.nodes
                if item.config.get("result_schema_id") == expected_result_schema_id
            ),
            None,
        )
        synth_run = by_node.get(synth_node.key if synth_node else "synthesize")
        synth_result = (
            dict((synth_run.output or {}).get("structured_result") or {})
            if synth_run is not None
            else {}
        )
        reviewer_run = by_node.get(strategy.semantic_scorer_node_key)
        receipt = None
        if reviewer_run is not None and artifact.sha256:
            receipt = self.dependencies.artifacts.fresh_complete_receipt(
                run_id=reviewer_run.id,
                artifact_id=artifact.id,
                expected_sha256=artifact.sha256,
            )
        coverage = tuple(
            CoverageResult(
                requirement_id=row["requirement_id"],
                area=row["area"],
                status=row["status"],
                claim_ids=tuple(json.loads(row["claim_ids_json"])),
                evidence_count=row["evidence_count"],
                notes=row["notes"],
                validator_id=row["validator_id"],
            )
            for row in coverage_rows
        )
        findings = tuple(self._finding(row) for row in finding_rows)
        evidence_by_claim: dict[str, list[Any]] = {}
        for row in evidence_rows:
            evidence_by_claim.setdefault(str(row["claim_id"]), []).append(row)
        relationship_ids = {
            item.id for item in contract.requirements if item.category.value == "relationship"
        }
        lineage_evidence: list[str] = []
        lineage_paths: set[str] = set()
        for row in claim_rows:
            requirement_ids = set(json.loads(row["requirement_ids_json"]))
            if not relationship_ids.intersection(requirement_ids):
                continue
            for evidence in evidence_by_claim.get(str(row["id"]), ()):
                lineage_evidence.append(str(evidence["id"]))
                lineage_paths.add(str(evidence["path"]))
        control_claim_ids = {
            claim_id
            for item in coverage
            if item.area in {"deployment", "profiles", "pipelines", "notebooks"}
            for claim_id in item.claim_ids
        }
        control_evidence = tuple(
            str(row["id"])
            for row in evidence_rows
            if str(row["claim_id"]) in control_claim_ids
        )
        budget_integrity = True
        if task["active_budget_ledger_id"]:
            ledger = self.dependencies.budgets.get(task["active_budget_ledger_id"])
            limits = ledger.effective_limits.model_dump(mode="json")
            if ledger.mode.value != "unlimited":
                budget_integrity = all(
                    int(ledger.consumed.get(name, 0))
                    + int(ledger.reserved.get(name, 0))
                    <= int(limit)
                    for name, limit in limits.items()
                    if limit is not None
                )
        return ValidationInputs(
            contract=contract,
            snapshot=snapshot,
            artifact=artifact,
            reviewer_run_id=reviewer_run.id if reviewer_run is not None else "",
            read_receipt=receipt,
            result_schema_valid=bool(
                synth_result.get("schema_id") == expected_result_schema_id
                and synth_result.get("schema_version") == 2
            ),
            result_schema_id=str(synth_result.get("schema_id") or ""),
            source_workspace_changes=tuple(
                dict(item) for item in synth_result.get("source_workspace_changes") or ()
            ),
            coverage_results=coverage,
            lineage_layers=min(3, len(lineage_paths)),
            lineage_evidence_ids=tuple(dict.fromkeys(lineage_evidence)),
            execution_control_evidence_ids=tuple(dict.fromkeys(control_evidence)),
            existing_findings=findings,
            budget_status=BudgetStatus(task["budget_status"]),
            budget_integrity=budget_integrity,
        )

    def _run_validators(
        self,
        inputs: ValidationInputs,
        *,
        persist_excluding: frozenset[str] = frozenset(),
        verify_artifact: bool = True,
    ) -> Any:
        if inputs.contract.archetype is Archetype.FOCUSED_QUESTION:
            return self.dependencies.validators.run_selected(
                inputs,
                gate_ids=FOCUSED_QUESTION_GATE_IDS,
                persist=True,
                persist_excluding=persist_excluding,
                verify_artifact=verify_artifact,
            )
        if inputs.contract.archetype is Archetype.REPO_ANALYSIS:
            return self.dependencies.validators.run(
                inputs,
                persist=True,
                persist_excluding=persist_excluding,
                verify_artifact=verify_artifact,
            )
        raise ValueError(
            f"deterministic quality workflow does not support {inputs.contract.archetype.value}"
        )

    def _active(self, task_id: str) -> tuple[Any, Any, Any]:
        with self.dependencies.store._read() as connection:
            row = connection.execute(
                """
                SELECT active_contract_id, active_snapshot_id, active_strategy_id
                FROM orch_tasks WHERE id=?
                """,
                (task_id,),
            ).fetchone()
        if (
            row is None
            or not row["active_contract_id"]
            or not row["active_snapshot_id"]
            or not row["active_strategy_id"]
        ):
            raise ValueError("quality workflow identity is incomplete")
        return (
            self.dependencies.contracts.get(row["active_contract_id"]),
            self.dependencies.snapshots.get(row["active_snapshot_id"]),
            self.dependencies.strategies.get(row["active_strategy_id"]),
        )

    def _request_repair_if_possible(
        self,
        task_id: str,
        artifact_id: str,
        findings: tuple[Finding, ...],
    ) -> None:
        if not findings:
            return
        _contract, _snapshot, strategy = self._active(task_id)
        flags = dict(strategy.feature_flags)
        if not bool(flags.get("auto_repair") or flags.get("auto_repair_enabled")):
            self._mark_blocked(
                task_id,
                workflow="needs_attention",
                reason="repair_requires_operator_request",
            )
            return
        with self.dependencies.store._read() as connection:
            active = connection.execute(
                """
                SELECT 1 FROM orch_repair_requests
                WHERE task_id=? AND status IN ('pending','running') LIMIT 1
                """,
                (task_id,),
            ).fetchone()
            task = connection.execute(
                "SELECT budget_status FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
        if active is not None:
            return
        try:
            self.repairs.request(
                task_id=task_id,
                source_artifact_id=artifact_id,
                findings=findings,
                budget_allocation={
                    "reported_tokens": 150_000,
                    "model_calls": 8,
                    "tool_calls": 20,
                    "active_seconds": 300,
                    "tool_payload_bytes": 16 * 1024 * 1024,
                },
                budget_available=bool(
                    task is not None and task["budget_status"] != "exhausted"
                ),
            )
        except (ConflictError, RepairExhausted, ValueError):
            self._mark_blocked(
                task_id,
                workflow="needs_attention",
                reason="repair_exhausted",
            )

    def _persist_final(
        self,
        context: Any,
        *,
        artifact: Any,
        outcome: Any,
        gates: tuple[GateResult, ...],
        findings: tuple[Finding, ...],
        score: RubricScore | None,
        receipt: Any,
        rubric: Any,
    ) -> None:
        payload = {
            "decision": outcome.decision.value,
            "quality_status": outcome.quality_status.value,
            "reason_code": outcome.reason_code,
            "gate_ids": [item.id for item in gates],
            "finding_ids": [item.id for item in findings],
            "rubric_score_id": score.id if score else None,
        }
        content_hash = "sha256:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        )
        with self.dependencies.store._write() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_quality_evaluations(
                    id, task_id, artifact_id, artifact_hash, evaluation_type,
                    validator_id, validator_version, rubric_id, rubric_version,
                    criterion_results_json, coverage_results_json, rubric_score_id,
                    total_score, verdict, decision, read_receipt_id, finding_ids_json,
                    created_by_run_id, content_hash, created_at
                ) VALUES (?, ?, ?, ?, 'final', 'server-adjudicator', '2', ?, ?,
                          ?, '[]', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"evaluation_{uuid.uuid4().hex}",
                    context.task.id,
                    artifact.id,
                    artifact.sha256,
                    score.rubric_id if score else rubric.id,
                    score.rubric_version if score else rubric.version,
                    json.dumps([item.id for item in gates]),
                    score.id if score else None,
                    score.total if score else None,
                    "pass" if outcome.publishable else "fail",
                    outcome.decision.value,
                    receipt.id if receipt else None,
                    json.dumps([item.id for item in findings]),
                    context.claim.run.id,
                    content_hash,
                    now,
                ),
            )
            if not outcome.publishable:
                transition_workflow_in_transaction(
                    self.dependencies.store,
                    connection,
                    task_id=context.task.id,
                    event=(
                        WorkflowEvent.REPAIRABLE_FAILURE
                        if outcome.decision.value == "repair"
                        else WorkflowEvent.ATTENTION_REQUIRED
                    ),
                    reason_code=outcome.reason_code,
                    command_id=f"quality-adjudication:{content_hash}",
                )
            connection.execute(
                """
                UPDATE orch_tasks
                SET quality_status=?, quality_reason_code=?
                WHERE id=?
                """,
                (
                    outcome.quality_status.value,
                    outcome.reason_code,
                    context.task.id,
                ),
            )

    def _mark_blocked(self, task_id: str, *, workflow: str, reason: str) -> None:
        with self.dependencies.store._write() as connection:
            row = connection.execute(
                "SELECT workflow_status FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
            current = str(row["workflow_status"] if row is not None else "")
            event: WorkflowEvent | None = None
            if workflow == "repairing" and current in {"validating", "reviewing"}:
                event = WorkflowEvent.REPAIRABLE_FAILURE
            elif workflow == "needs_attention":
                if current in {"validating", "reviewing"}:
                    event = WorkflowEvent.ATTENTION_REQUIRED
                elif current == "repairing":
                    event = WorkflowEvent.REPAIR_EXHAUSTED
            if event is not None:
                transition_workflow_in_transaction(
                    self.dependencies.store,
                    connection,
                    task_id=task_id,
                    event=event,
                    reason_code=reason,
                    command_id=f"quality-blocked:{task_id}:{reason}",
                )
            connection.execute(
                """
                UPDATE orch_tasks SET quality_status='fail', quality_reason_code=?
                WHERE id=?
                """,
                (reason, task_id),
            )

    @staticmethod
    def _gate(row: Any) -> GateResult:
        return GateResult(
            id=row["id"],
            task_id=row["task_id"],
            artifact_id=row["artifact_id"],
            artifact_hash=row["artifact_hash"],
            subject_type=row["subject_type"],
            subject_id=row["subject_id"],
            subject_version=row["subject_version"],
            status=row["status"],
            waivable=bool(row["waivable"]),
            reason_code=row["reason_code"],
            evidence_ids=tuple(json.loads(row["evidence_ids_json"])),
            validator_id=row["validator_id"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _finding(row: Any) -> Finding:
        return Finding(
            id=row["id"],
            fingerprint=row["fingerprint"],
            task_id=row["task_id"],
            artifact_id=row["artifact_id"],
            artifact_hash=row["artifact_hash"],
            category=row["category"],
            severity=row["severity"],
            blocking=bool(row["blocking"]),
            repairable=bool(row["repairable"]),
            requirement_id=row["requirement_id"],
            claim_id=row["claim_id"],
            section_id=row["section_id"],
            message=row["message"],
            evidence_refs=tuple(json.loads(row["evidence_refs_json"])),
            suggested_fix=row["suggested_fix"],
            status=row["status"],
            supersedes_finding_id=row["supersedes_finding_id"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _score(row: Any) -> RubricScore:
        return RubricScore(
            id=row["id"],
            rubric_id=row["rubric_id"],
            rubric_version=row["rubric_version"],
            artifact_id=row["artifact_id"],
            artifact_hash=row["artifact_hash"],
            scorer_run_id=row["scorer_run_id"],
            dimension_scores=tuple(
                RubricDimensionScore.model_validate(item)
                for item in json.loads(row["dimension_scores_json"])
            ),
            total=row["total"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _waiver(row: Any) -> QualityWaiver:
        return QualityWaiver(
            id=row["id"],
            task_id=row["task_id"],
            artifact_id=row["artifact_id"],
            artifact_hash=row["artifact_hash"],
            contract_id=row["contract_id"],
            contract_version=row["contract_version"],
            subject_type=row["subject_type"],
            subject_id=row["subject_id"],
            subject_version=row["subject_version"],
            rubric_id=row["rubric_id"],
            rubric_version=row["rubric_version"],
            actor_id=row["actor_id"],
            reason=row["reason"],
            reference=row["reference"],
            expires_at=row["expires_at"],
            revoked_at=row["revoked_at"],
            created_at=row["created_at"],
            signature_hash=row["signature_hash"],
        )

    @staticmethod
    def _succeeded(
        context: Any,
        summary: str,
        output: Mapping[str, Any],
    ) -> ExecutionOutcome:
        return ExecutionOutcome(
            status="succeeded",
            session_id=context.claim.run.session_id or f"__orch__{context.claim.run.id}",
            summary=summary,
            output=dict(output),
            evidence=({"kind": "note", "title": summary},),
            usage={"model_calls": 0, "tool_calls": 0, "tokens": 0, "wall_seconds": 0},
        )

    @staticmethod
    def _failed(
        context: Any,
        error_kind: str,
        message: str,
        *,
        output: Mapping[str, Any] | None = None,
    ) -> ExecutionOutcome:
        return ExecutionOutcome(
            status="failed",
            session_id=context.claim.run.session_id or f"__orch__{context.claim.run.id}",
            summary=message,
            output=dict(output or {}),
            evidence=({"kind": "note", "title": "Quality service node failed"},),
            usage={"model_calls": 0, "tool_calls": 0, "tokens": 0, "wall_seconds": 0},
            error_kind=error_kind,
            error_message=message,
        )
