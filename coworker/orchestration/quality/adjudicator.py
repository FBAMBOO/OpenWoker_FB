"""Server-authoritative acceptance adjudication; evaluator prose has no authority."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from .models import (
    ArtifactReadReceipt,
    ArtifactVersion,
    ArtifactVersionStatus,
    BudgetStatus,
    FinalDecision,
    Finding,
    FindingStatus,
    GateResult,
    QualityRubric,
    QualityStatus,
    QualityWaiver,
    RubricScore,
    TriState,
    WaiverSubjectType,
)


@dataclass(frozen=True, slots=True)
class FailedSubject:
    subject_type: WaiverSubjectType
    subject_id: str
    subject_version: int
    waivable: bool
    repairable: bool
    reason_code: str


@dataclass(frozen=True, slots=True)
class AdjudicationOutcome:
    decision: FinalDecision
    quality_status: QualityStatus
    reason_code: str
    failed_subjects: tuple[FailedSubject, ...]
    uncovered_subjects: tuple[FailedSubject, ...]
    waiver_ids: tuple[str, ...]
    ignored_evaluator_recommendation: str | None = None

    @property
    def publishable(self) -> bool:
        return self.decision is FinalDecision.PUBLISH


def _waiver_for(
    subject: FailedSubject,
    *,
    waivers: Iterable[QualityWaiver],
    task_id: str,
    artifact_id: str,
    artifact_hash: str,
    contract_id: str,
    contract_version: int,
    rubric: QualityRubric,
    now: datetime,
) -> QualityWaiver | None:
    if not subject.waivable:
        return None
    for waiver in waivers:
        if (
            waiver.task_id == task_id
            and waiver.artifact_id == artifact_id
            and waiver.artifact_hash == artifact_hash
            and waiver.contract_id == contract_id
            and waiver.contract_version == contract_version
            and waiver.subject_type is subject.subject_type
            and waiver.subject_id == subject.subject_id
            and waiver.subject_version == subject.subject_version
            and waiver.revoked_at is None
            and (waiver.expires_at is None or waiver.expires_at > now)
            and (
                subject.subject_type is not WaiverSubjectType.SEMANTIC_SCORE
                or (
                    waiver.rubric_id == rubric.id
                    and waiver.rubric_version == rubric.version
                )
            )
        ):
            return waiver
    return None


def adjudicate(
    *,
    task_id: str,
    contract_id: str,
    contract_version: int,
    artifact: ArtifactVersion | None,
    candidate_hash: str,
    result_schema_valid: bool,
    reviewer_run_id: str,
    read_receipt: ArtifactReadReceipt | None,
    hard_gate_results: Iterable[GateResult],
    required_criterion_results: Iterable[GateResult],
    findings: Iterable[Finding],
    rubric: QualityRubric,
    rubric_score: RubricScore | None,
    budget_status: BudgetStatus | str,
    waivers: Iterable[QualityWaiver] = (),
    repair_attempts: int = 0,
    max_repair_attempts: int = 2,
    evaluator_recommendation: str | None = None,
    now: datetime | None = None,
) -> AdjudicationOutcome:
    """Apply all hard facts before considering repair/waiver publication."""

    when = now or datetime.now(timezone.utc)
    budget = BudgetStatus(budget_status)
    failed: list[FailedSubject] = []

    if artifact is None or artifact.sha256 != candidate_hash:
        failed.append(
            FailedSubject(
                WaiverSubjectType.GATE_RESULT,
                "QG-012",
                1,
                False,
                False,
                "candidate_artifact_invalid",
            )
        )
        artifact_id = artifact.id if artifact is not None else "missing"
    else:
        artifact_id = artifact.id
        if artifact.status is not ArtifactVersionStatus.VERIFIED:
            failed.append(
                FailedSubject(
                    WaiverSubjectType.GATE_RESULT,
                    "QG-012",
                    1,
                    False,
                    True,
                    "candidate_artifact_not_verified",
                )
            )
    if not result_schema_valid:
        failed.append(
            FailedSubject(
                WaiverSubjectType.GATE_RESULT,
                "QG-015",
                1,
                False,
                True,
                "result_contract_invalid",
            )
        )
    receipt_complete = bool(
        artifact is not None
        and read_receipt is not None
        and read_receipt.run_id == reviewer_run_id
        and read_receipt.artifact_id == artifact.id
        and read_receipt.artifact_hash == candidate_hash
        and read_receipt.coverage_ratio == 1.0
        and read_receipt.covered_bytes == artifact.byte_size
        and read_receipt.completed_at is not None
        and read_receipt.completed_at > read_receipt.candidate_bound_at
    )
    if not receipt_complete:
        failed.append(
            FailedSubject(
                WaiverSubjectType.GATE_RESULT,
                "QG-013",
                1,
                False,
                True,
                "candidate_not_fully_reviewed",
            )
        )
    if budget is BudgetStatus.EXHAUSTED:
        failed.append(
            FailedSubject(
                WaiverSubjectType.GATE_RESULT,
                "QG-016",
                1,
                False,
                False,
                "budget_exhausted",
            )
        )
    elif budget is BudgetStatus.OVER_BUDGET:
        failed.append(
            FailedSubject(
                WaiverSubjectType.SOFT_BUDGET,
                "soft_budget",
                1,
                True,
                False,
                "soft_budget_overrun",
            )
        )
    elif budget in {BudgetStatus.UNCONFIGURED}:
        failed.append(
            FailedSubject(
                WaiverSubjectType.GATE_RESULT,
                "QG-016",
                1,
                False,
                True,
                "budget_unconfigured",
            )
        )

    for result in (*tuple(hard_gate_results), *tuple(required_criterion_results)):
        if artifact is not None and (
            result.task_id != task_id
            or result.artifact_id != artifact.id
            or result.artifact_hash != candidate_hash
        ):
            failed.append(
                FailedSubject(
                    WaiverSubjectType.GATE_RESULT,
                    result.subject_id,
                    result.subject_version,
                    False,
                    True,
                    "gate_subject_mismatch",
                )
            )
        elif result.status is not TriState.PASS:
            subject_type = (
                WaiverSubjectType.CRITERION
                if result.subject_type.value == "criterion"
                else WaiverSubjectType.GATE_RESULT
            )
            failed.append(
                FailedSubject(
                    subject_type,
                    result.subject_id,
                    result.subject_version,
                    result.waivable,
                    True,
                    result.reason_code,
                )
            )

    for finding in findings:
        if finding.status is FindingStatus.OPEN and finding.blocking:
            failed.append(
                FailedSubject(
                    WaiverSubjectType.FINDING,
                    finding.id,
                    1,
                    finding.category.value not in {"security", "schema"},
                    finding.repairable,
                    f"blocking_finding:{finding.category.value}",
                )
            )

    valid_score = bool(
        artifact is not None
        and rubric_score is not None
        and rubric_score.rubric_id == rubric.id
        and rubric_score.rubric_version == rubric.version
        and rubric_score.artifact_id == artifact.id
        and rubric_score.artifact_hash == candidate_hash
    )
    if not valid_score or rubric_score is None or rubric_score.total < rubric.pass_threshold:
        failed.append(
            FailedSubject(
                WaiverSubjectType.SEMANTIC_SCORE,
                rubric.id,
                rubric.version,
                True,
                True,
                "semantic_score_unknown" if not valid_score else "semantic_score_below_threshold",
            )
        )

    # Deduplicate without allowing a later, weaker record to downgrade authority.
    unique: dict[tuple[WaiverSubjectType, str, int], FailedSubject] = {}
    for subject in failed:
        key = (subject.subject_type, subject.subject_id, subject.subject_version)
        previous = unique.get(key)
        if previous is None or (previous.waivable and not subject.waivable):
            unique[key] = subject
    failed_subjects = tuple(unique.values())
    used: list[str] = []
    uncovered: list[FailedSubject] = []
    if artifact is None:
        uncovered = list(failed_subjects)
    else:
        for subject in failed_subjects:
            waiver = _waiver_for(
                subject,
                waivers=waivers,
                task_id=task_id,
                artifact_id=artifact.id,
                artifact_hash=candidate_hash,
                contract_id=contract_id,
                contract_version=contract_version,
                rubric=rubric,
                now=when,
            )
            if waiver is None:
                uncovered.append(subject)
            else:
                used.append(waiver.id)

    ignored = evaluator_recommendation if evaluator_recommendation is not None else None
    if not uncovered:
        return AdjudicationOutcome(
            decision=FinalDecision.PUBLISH,
            quality_status=QualityStatus.WAIVED if used else QualityStatus.PASS,
            reason_code="quality_waived" if used else "quality_passed",
            failed_subjects=failed_subjects,
            uncovered_subjects=(),
            waiver_ids=tuple(used),
            ignored_evaluator_recommendation=ignored,
        )

    reason_codes = {subject.reason_code for subject in uncovered}
    if "budget_exhausted" in reason_codes:
        decision = FinalDecision.NEEDS_ATTENTION
        reason = "budget_exhausted"
    elif "candidate_artifact_invalid" in reason_codes:
        decision = FinalDecision.REJECT
        reason = "candidate_artifact_invalid"
    elif all(subject.repairable for subject in uncovered) and repair_attempts < max_repair_attempts:
        decision = FinalDecision.REPAIR
        reason = "quality_repair_required"
    else:
        decision = FinalDecision.NEEDS_ATTENTION
        reason = "repair_exhausted" if repair_attempts >= max_repair_attempts else "quality_blocked"
    return AdjudicationOutcome(
        decision=decision,
        quality_status=QualityStatus.FAIL,
        reason_code=reason,
        failed_subjects=failed_subjects,
        uncovered_subjects=tuple(uncovered),
        waiver_ids=tuple(used),
        ignored_evaluator_recommendation=ignored,
    )
