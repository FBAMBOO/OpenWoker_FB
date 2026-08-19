from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..models import (
    ArtifactReadReceipt,
    ArtifactVersion,
    BudgetStatus,
    CoverageResult,
    Finding,
    GateResult,
    RepositorySnapshot,
    TaskContractV2,
    TriState,
)


@dataclass(frozen=True, slots=True)
class ValidationInputs:
    contract: TaskContractV2
    snapshot: RepositorySnapshot
    artifact: ArtifactVersion
    reviewer_run_id: str
    read_receipt: ArtifactReadReceipt | None
    result_schema_valid: bool
    result_schema_id: str
    source_workspace_changes: tuple[dict, ...] = ()
    coverage_results: tuple[CoverageResult, ...] = ()
    lineage_layers: int = 0
    lineage_evidence_ids: tuple[str, ...] = ()
    execution_control_evidence_ids: tuple[str, ...] = ()
    existing_findings: tuple[Finding, ...] = ()
    budget_status: BudgetStatus = BudgetStatus.WITHIN_BUDGET
    budget_integrity: bool = True


@dataclass(frozen=True, slots=True)
class Check:
    gate_id: str
    status: TriState
    validator_id: str
    evidence_ids: tuple[str, ...] = ()
    detail: str = ""


def state(value: bool, *, unknown: bool = False) -> TriState:
    if unknown:
        return TriState.UNKNOWN
    return TriState.PASS if value else TriState.FAIL


def evidence_ids(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(item) for item in values if str(item)))
