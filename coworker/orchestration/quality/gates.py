"""Canonical repository-analysis hard gates and immutable result helpers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping

from .models import GateResult, GateSubjectType, TriState


@dataclass(frozen=True, slots=True)
class HardGateDefinition:
    id: str
    title: str
    reason_code: str
    waivable: bool


REPOSITORY_ANALYSIS_HARD_GATES: tuple[HardGateDefinition, ...] = (
    HardGateDefinition("QG-001", "Source workspace unchanged", "workspace_changed", False),
    HardGateDefinition("QG-002", "Baseline frozen", "baseline_not_frozen", False),
    HardGateDefinition("QG-003", "Required domains", "required_domain_missing", False),
    HardGateDefinition("QG-004", "Evidence per domain", "domain_evidence_missing", False),
    HardGateDefinition("QG-005", "Relationship coverage", "relationship_missing", False),
    HardGateDefinition("QG-006", "Execution control plane", "control_plane_missing", False),
    HardGateDefinition("QG-007", "Claim support", "claim_support_missing", False),
    HardGateDefinition("QG-008", "Negative evidence discipline", "negative_evidence_invalid", False),
    HardGateDefinition("QG-009", "Limitations", "limitations_missing", False),
    HardGateDefinition("QG-010", "Citation resolution", "citation_unresolved", False),
    HardGateDefinition("QG-011", "Inventory reconciliation", "inventory_mismatch", False),
    HardGateDefinition("QG-012", "Artifact contract", "artifact_contract_invalid", False),
    HardGateDefinition("QG-013", "Complete independent review", "review_incomplete", False),
    HardGateDefinition("QG-014", "Findings authoritative", "blocking_finding_open", False),
    HardGateDefinition("QG-015", "Schema integrity", "schema_integrity_failed", False),
    HardGateDefinition("QG-016", "Budget integrity", "budget_integrity_failed", False),
)

_GATE_BY_ID: Mapping[str, HardGateDefinition] = {
    definition.id: definition for definition in REPOSITORY_ANALYSIS_HARD_GATES
}

if len(_GATE_BY_ID) != len(REPOSITORY_ANALYSIS_HARD_GATES):
    raise RuntimeError("repository-analysis hard gate ids must be unique")


NON_WAIVABLE_GATE_IDS = frozenset(
    {"QG-001", "QG-002", "QG-010", "QG-012", "QG-013", "QG-015", "QG-016"}
)


def gate_definition(gate_id: str) -> HardGateDefinition:
    try:
        return _GATE_BY_ID[str(gate_id)]
    except KeyError as exc:
        raise ValueError(f"unknown repository-analysis hard gate {gate_id!r}") from exc


def create_gate_result(
    *,
    gate_id: str,
    task_id: str,
    artifact_id: str,
    artifact_hash: str,
    status: TriState | str,
    validator_id: str,
    evidence_ids: Iterable[str] = (),
    subject_version: int = 1,
    waivable_override: bool | None = None,
) -> GateResult:
    definition = gate_definition(gate_id)
    waivable = definition.waivable if waivable_override is None else bool(waivable_override)
    if gate_id in NON_WAIVABLE_GATE_IDS and waivable:
        raise ValueError(f"{gate_id} is an invariant non-waivable hard gate")
    return GateResult(
        id=f"gate_result_{uuid.uuid4().hex}",
        task_id=task_id,
        artifact_id=artifact_id,
        artifact_hash=artifact_hash,
        subject_type=GateSubjectType.HARD_GATE,
        subject_id=gate_id,
        subject_version=subject_version,
        status=TriState(status),
        waivable=waivable,
        reason_code=definition.reason_code,
        evidence_ids=tuple(evidence_ids),
        validator_id=validator_id,
        created_at=datetime.now(timezone.utc),
    )


def assert_complete_gate_set(results: Iterable[GateResult]) -> tuple[GateResult, ...]:
    chosen = tuple(results)
    ids = [result.subject_id for result in chosen]
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    if duplicates:
        raise ValueError("duplicate hard gate results: " + ", ".join(duplicates))
    missing = sorted(set(_GATE_BY_ID).difference(ids))
    unknown = sorted(set(ids).difference(_GATE_BY_ID))
    if missing or unknown:
        raise ValueError(
            f"hard gate set is incomplete (missing={missing}, unknown={unknown})"
        )
    return chosen
