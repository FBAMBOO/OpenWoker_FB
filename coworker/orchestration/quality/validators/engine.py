"""Run and persist the complete QG-001..QG-016 deterministic gate set."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any

from ...store import OrchestrationStore
from ..artifacts import ArtifactService
from ..findings import deduplicate_findings, materialize_finding
from ..gates import assert_complete_gate_set, create_gate_result
from ..models import (
    ArtifactVersionStatus,
    Finding,
    FindingCategory,
    FindingInput,
    Severity,
    TriState,
)
from ..repository_snapshot import RepositorySnapshotService
from ..state_machine import WorkflowEvent, transition_workflow_in_transaction
from . import (
    artifact_contract,
    baseline,
    budget,
    citation,
    claim_support,
    coverage,
    findings_gate,
    inventory,
    relationships,
    review,
    schema_integrity,
    workspace_integrity,
)
from .base import Check, ValidationInputs


@dataclass(frozen=True, slots=True)
class ValidationReport:
    gate_results: tuple
    findings: tuple[Finding, ...]

    @property
    def passed(self) -> bool:
        return all(item.status is TriState.PASS for item in self.gate_results)


_FINDING_POLICY: dict[str, tuple[FindingCategory, Severity, bool]] = {
    "QG-001": (FindingCategory.SECURITY, Severity.CRITICAL, False),
    "QG-002": (FindingCategory.BASELINE, Severity.CRITICAL, False),
    "QG-003": (FindingCategory.COVERAGE, Severity.HIGH, True),
    "QG-004": (FindingCategory.COVERAGE, Severity.HIGH, True),
    "QG-005": (FindingCategory.COVERAGE, Severity.HIGH, True),
    "QG-006": (FindingCategory.COVERAGE, Severity.HIGH, True),
    "QG-007": (FindingCategory.SUPPORT, Severity.HIGH, True),
    "QG-008": (FindingCategory.SUPPORT, Severity.HIGH, True),
    "QG-009": (FindingCategory.LIMITATION, Severity.HIGH, True),
    "QG-010": (FindingCategory.CITATION, Severity.HIGH, True),
    "QG-011": (FindingCategory.CONSISTENCY, Severity.HIGH, True),
    "QG-012": (FindingCategory.SCHEMA, Severity.HIGH, True),
    "QG-013": (FindingCategory.SCHEMA, Severity.HIGH, True),
    "QG-014": (FindingCategory.SCHEMA, Severity.HIGH, True),
    "QG-015": (FindingCategory.SCHEMA, Severity.HIGH, True),
    "QG-016": (FindingCategory.BUDGET, Severity.CRITICAL, False),
}

FOCUSED_QUESTION_GATE_IDS = frozenset(
    {"QG-001", "QG-002", "QG-012", "QG-013", "QG-014", "QG-015", "QG-016"}
)


class DeterministicValidatorEngine:
    VERSION = "deterministic-validator-suite@1"

    def __init__(
        self,
        store: OrchestrationStore,
        artifacts: ArtifactService,
        snapshots: RepositorySnapshotService,
    ) -> None:
        self.store = store
        self.artifacts = artifacts
        self.snapshots = snapshots

    def run(
        self,
        inputs: ValidationInputs,
        *,
        persist: bool = True,
        persist_excluding: frozenset[str] = frozenset(),
        verify_artifact: bool = True,
    ) -> ValidationReport:
        return self._run(
            inputs,
            gate_ids=frozenset(_FINDING_POLICY),
            require_complete_repository_set=True,
            persist=persist,
            persist_excluding=persist_excluding,
            verify_artifact=verify_artifact,
        )

    def run_selected(
        self,
        inputs: ValidationInputs,
        *,
        gate_ids: frozenset[str],
        persist: bool = True,
        persist_excluding: frozenset[str] = frozenset(),
        verify_artifact: bool = True,
    ) -> ValidationReport:
        """Run the exact deterministic gate subset frozen for a non-repository contract."""

        selected = frozenset(str(item) for item in gate_ids)
        unknown = selected.difference(_FINDING_POLICY)
        if not selected or unknown:
            raise ValueError(
                f"selected hard gate set must be non-empty and registered (unknown={sorted(unknown)})"
            )
        return self._run(
            inputs,
            gate_ids=selected,
            require_complete_repository_set=False,
            persist=persist,
            persist_excluding=persist_excluding,
            verify_artifact=verify_artifact,
        )

    def _run(
        self,
        inputs: ValidationInputs,
        *,
        gate_ids: frozenset[str],
        require_complete_repository_set: bool,
        persist: bool,
        persist_excluding: frozenset[str],
        verify_artifact: bool,
    ) -> ValidationReport:
        if not inputs.artifact.sha256:
            raise ValueError("deterministic validation requires a finalized artifact hash")
        checks = self._selected_checks(inputs, gate_ids)
        findings = [self._finding(check, inputs) for check in checks if check.status is not TriState.PASS]
        findings = list(deduplicate_findings(item for item in findings if item is not None))
        if "QG-014" in gate_ids:
            qg14_inputs = replace(
                inputs,
                existing_findings=tuple((*inputs.existing_findings, *findings)),
            )
            authority = findings_gate.validate(qg14_inputs)
            checks.append(authority)
            if authority.status is not TriState.PASS:
                authority_finding = self._finding(authority, inputs)
                if authority_finding is not None:
                    findings = list(deduplicate_findings((*findings, authority_finding)))
        gates = tuple(
            create_gate_result(
                gate_id=check.gate_id,
                task_id=inputs.contract.task_id,
                artifact_id=inputs.artifact.id,
                artifact_hash=inputs.artifact.sha256,
                status=check.status,
                validator_id=check.validator_id,
                evidence_ids=check.evidence_ids,
            )
            for check in checks
        )
        if require_complete_repository_set:
            gates = assert_complete_gate_set(gates)
        else:
            observed = [item.subject_id for item in gates]
            duplicates = sorted({item for item in observed if observed.count(item) > 1})
            missing = sorted(gate_ids.difference(observed))
            extra = sorted(set(observed).difference(gate_ids))
            if duplicates or missing or extra:
                raise ValueError(
                    "selected hard gate set mismatch "
                    f"(missing={missing}, extra={extra}, duplicates={duplicates})"
                )
        report = ValidationReport(gate_results=gates, findings=tuple(findings))
        if persist:
            self._persist(
                report,
                inputs,
                excluding=persist_excluding,
                verify_artifact=verify_artifact,
            )
        return report

    def _selected_checks(
        self,
        inputs: ValidationInputs,
        gate_ids: frozenset[str],
    ) -> list[Check]:
        """Evaluate only selected validators so irrelevant checks cannot create findings."""

        checks: list[Check] = []
        if "QG-001" in gate_ids:
            checks.append(workspace_integrity.validate(inputs))
        if "QG-002" in gate_ids:
            checks.append(baseline.validate(inputs))
        if gate_ids.intersection({"QG-003", "QG-004"}):
            checks.extend(
                item for item in coverage.validate(inputs) if item.gate_id in gate_ids
            )
        if gate_ids.intersection({"QG-005", "QG-006"}):
            checks.extend(
                item for item in relationships.validate(inputs) if item.gate_id in gate_ids
            )
        if gate_ids.intersection({"QG-007", "QG-008", "QG-009"}):
            checks.extend(
                item
                for item in claim_support.validate(self.store, inputs)
                if item.gate_id in gate_ids
            )
        if "QG-010" in gate_ids:
            checks.append(citation.validate(self.store, self.snapshots, inputs))
        if "QG-011" in gate_ids:
            checks.append(inventory.validate(self.store, inputs))
        if "QG-012" in gate_ids:
            checks.append(artifact_contract.validate(self.artifacts, inputs))
        if "QG-013" in gate_ids:
            checks.append(review.validate(inputs))
        if "QG-015" in gate_ids:
            checks.append(schema_integrity.validate(inputs))
        if "QG-016" in gate_ids:
            checks.append(budget.validate(inputs))
        return checks

    @staticmethod
    def _finding(check: Check, inputs: ValidationInputs) -> Finding | None:
        category, severity, repairable = _FINDING_POLICY[check.gate_id]
        return materialize_finding(
            FindingInput(
                category=category,
                severity=severity,
                blocking=True,
                repairable=repairable,
                requirement_id=check.gate_id,
                section_id=check.gate_id,
                message=(
                    f"{check.gate_id} failed in {check.validator_id}"
                    + (f": {check.detail}" if check.detail else "")
                ),
                evidence_refs=check.evidence_ids,
                suggested_fix=(
                    "Repair the located report section and rerun the required validators."
                    if repairable
                    else "Human attention is required; this failure cannot be auto-repaired."
                ),
            ),
            task_id=inputs.contract.task_id,
            artifact_id=inputs.artifact.id,
            artifact_hash=str(inputs.artifact.sha256),
        )

    def _persist(
        self,
        report: ValidationReport,
        inputs: ValidationInputs,
        *,
        excluding: frozenset[str] = frozenset(),
        verify_artifact: bool = True,
    ) -> None:
        with self.store._write() as connection:
            task = connection.execute(
                "SELECT workflow_status FROM orch_tasks WHERE id=?",
                (inputs.contract.task_id,),
            ).fetchone()
            workflow = str(task["workflow_status"] if task is not None else "")
            if workflow == "running":
                transition_workflow_in_transaction(
                    self.store,
                    connection,
                    task_id=inputs.contract.task_id,
                    event=WorkflowEvent.CANDIDATE_CREATED,
                    command_id=f"quality-validate:{inputs.artifact.id}",
                )
            elif workflow == "repairing":
                transition_workflow_in_transaction(
                    self.store,
                    connection,
                    task_id=inputs.contract.task_id,
                    event=WorkflowEvent.REPAIRED_CANDIDATE_CREATED,
                    command_id=f"quality-revalidate:{inputs.artifact.id}",
                )
            connection.execute(
                "UPDATE orch_tasks SET quality_status='checking' WHERE id=?",
                (inputs.contract.task_id,),
            )
            for gate in report.gate_results:
                if gate.subject_id in excluding:
                    continue
                connection.execute(
                    """
                    INSERT OR IGNORE INTO orch_gate_results(
                        id, task_id, artifact_id, artifact_hash, subject_type,
                        subject_id, subject_version, status, waivable, reason_code,
                        evidence_ids_json, validator_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        gate.id, gate.task_id, gate.artifact_id, gate.artifact_hash,
                        gate.subject_type.value, gate.subject_id, gate.subject_version,
                        gate.status.value, int(gate.waivable), gate.reason_code,
                        json.dumps(list(gate.evidence_ids)), gate.validator_id,
                        gate.created_at.isoformat().replace("+00:00", "Z"),
                    ),
                )
            for finding in report.findings:
                if finding.requirement_id in excluding or finding.section_id in excluding:
                    continue
                connection.execute(
                    """
                    INSERT OR IGNORE INTO orch_quality_findings(
                        id, fingerprint, task_id, artifact_id, artifact_hash, category,
                        severity, blocking, repairable, requirement_id, claim_id,
                        section_id, message, evidence_refs_json, suggested_fix, status,
                        supersedes_finding_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        finding.id, finding.fingerprint, finding.task_id, finding.artifact_id,
                        finding.artifact_hash, finding.category.value, finding.severity.value,
                        int(finding.blocking), int(finding.repairable), finding.requirement_id,
                        finding.claim_id, finding.section_id, finding.message,
                        json.dumps(list(finding.evidence_refs)), finding.suggested_fix,
                        finding.status.value, finding.supersedes_finding_id,
                        finding.created_at.isoformat().replace("+00:00", "Z") if finding.created_at else "",
                    ),
                )
        effective_passed = all(
            item.status is TriState.PASS
            for item in report.gate_results
            if item.subject_id not in excluding
        )
        if verify_artifact and effective_passed and inputs.artifact.status in {
            ArtifactVersionStatus.DRAFT,
            ArtifactVersionStatus.VALIDATING,
        }:
            if inputs.artifact.status is ArtifactVersionStatus.DRAFT:
                self.artifacts.set_status(inputs.artifact.id, ArtifactVersionStatus.VALIDATING)
            self.artifacts.set_status(inputs.artifact.id, ArtifactVersionStatus.VERIFIED)
