from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from coworker.orchestration.quality.adjudicator import adjudicate
from coworker.orchestration.quality.findings import (
    finding_delta,
    lint_unstructured_finding,
    materialize_finding,
)
from coworker.orchestration.quality.gates import (
    REPOSITORY_ANALYSIS_HARD_GATES,
    assert_complete_gate_set,
    create_gate_result,
)
from coworker.orchestration.quality.models import (
    ArtifactReadReceipt,
    ArtifactVersion,
    ArtifactVersionStatus,
    BudgetStatus,
    ByteRange,
    FindingCategory,
    FindingInput,
    FinalDecision,
    QualityStatus,
    QualityWaiver,
    RubricDimensionScore,
    Severity,
    WaiverSubjectType,
)
from coworker.orchestration.quality.rubrics import (
    create_rubric_score,
    repository_analysis_rubric,
)


NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)
HASH = "sha256:" + "a" * 64


def _artifact(status: str = "verified") -> ArtifactVersion:
    return ArtifactVersion(
        id="artifact_1",
        logical_deliverable_id="report",
        task_id="task_1",
        run_id="run_producer",
        attempt=1,
        version=1,
        filename="report.md",
        mime_type="text/markdown",
        blob_uri=HASH,
        sha256=HASH,
        byte_size=100,
        status=status,
        producer_profile_id="producer",
        created_at=NOW,
        finalized_at=NOW,
    )


def _receipt(*, coverage: float = 1.0) -> ArtifactReadReceipt:
    covered = 100 if coverage == 1.0 else int(100 * coverage)
    return ArtifactReadReceipt(
        id="receipt_1",
        verifier_profile_id="reviewer",
        run_id="run_reviewer",
        artifact_id="artifact_1",
        artifact_hash=HASH,
        ranges=(ByteRange(start_byte=0, end_byte=covered),),
        covered_bytes=covered,
        coverage_ratio=coverage,
        candidate_bound_at=NOW,
        completed_at=NOW + timedelta(seconds=1) if coverage == 1.0 else None,
    )


def _gates(*, failed: str | None = None):
    return tuple(
        create_gate_result(
            gate_id=definition.id,
            task_id="task_1",
            artifact_id="artifact_1",
            artifact_hash=HASH,
            status="fail" if definition.id == failed else "pass",
            validator_id="validator@1",
        )
        for definition in REPOSITORY_ANALYSIS_HARD_GATES
    )


def _score(receipt: ArtifactReadReceipt | None = None, *, subtract: int = 0):
    rubric = repository_analysis_rubric()
    remaining = subtract
    scores = []
    for dimension in rubric.dimensions:
        reduction = min(dimension.max_points, remaining)
        remaining -= reduction
        scores.append(
            RubricDimensionScore(
                dimension_id=dimension.id,
                points=dimension.max_points - reduction,
                rationale="Evidence-backed score.",
            )
        )
    return create_rubric_score(
        rubric=rubric,
        artifact_id="artifact_1",
        artifact_hash=HASH,
        scorer_run_id="run_reviewer",
        authorized_scorer_run_id="run_reviewer",
        dimension_scores=scores,
        read_receipt=receipt or _receipt(),
    )


def _adjudicate(**overrides):
    rubric = repository_analysis_rubric()
    receipt = overrides.pop("read_receipt", _receipt())
    supplied_score = overrides.pop("rubric_score", ...)
    score = (
        _score(receipt)
        if supplied_score is ... and receipt.coverage_ratio == 1.0
        else None
        if supplied_score is ...
        else supplied_score
    )
    values = {
        "task_id": "task_1",
        "contract_id": "contract_1",
        "contract_version": 1,
        "artifact": _artifact(),
        "candidate_hash": HASH,
        "result_schema_valid": True,
        "reviewer_run_id": "run_reviewer",
        "read_receipt": receipt,
        "hard_gate_results": _gates(),
        "required_criterion_results": (),
        "findings": (),
        "rubric": rubric,
        "rubric_score": score,
        "budget_status": BudgetStatus.WITHIN_BUDGET,
        "now": NOW + timedelta(seconds=2),
    }
    values.update(overrides)
    return adjudicate(**values)


def test_all_authoritative_inputs_pass_and_evaluator_does_not_replace_them() -> None:
    outcome = _adjudicate(evaluator_recommendation="REJECT")
    assert outcome.decision is FinalDecision.PUBLISH
    assert outcome.quality_status is QualityStatus.PASS
    assert outcome.ignored_evaluator_recommendation == "REJECT"


def test_only_the_strategy_scorer_can_submit_dimensions_and_total_is_server_sum() -> None:
    rubric = repository_analysis_rubric()
    dimensions = tuple(
        RubricDimensionScore(
            dimension_id=item.id,
            points=item.max_points - (1 if index == 0 else 0),
            rationale="Server-authorized scoring fixture.",
        )
        for index, item in enumerate(rubric.dimensions)
    )
    with pytest.raises(PermissionError, match="strategy-designated"):
        create_rubric_score(
            rubric=rubric,
            artifact_id="artifact_1",
            artifact_hash=HASH,
            scorer_run_id="run_evaluator",
            authorized_scorer_run_id="run_reviewer",
            dimension_scores=dimensions,
            read_receipt=_receipt(),
        )
    score = create_rubric_score(
        rubric=rubric,
        artifact_id="artifact_1",
        artifact_hash=HASH,
        scorer_run_id="run_reviewer",
        authorized_scorer_run_id="run_reviewer",
        dimension_scores=dimensions,
        read_receipt=_receipt(),
    )
    assert score.total == sum(item.points for item in dimensions) == 99


def test_read_only_criterion_cannot_hide_missing_architecture_coverage() -> None:
    outcome = _adjudicate(
        hard_gate_results=_gates(failed="QG-003"),
        evaluator_recommendation="ACCEPT",
    )
    assert outcome.decision is FinalDecision.REPAIR
    assert outcome.quality_status is QualityStatus.FAIL
    assert any(item.subject_id == "QG-003" for item in outcome.uncovered_subjects)


def test_evaluator_accept_cannot_override_open_blocking_finding() -> None:
    finding = materialize_finding(
        FindingInput(
            category=FindingCategory.CITATION,
            severity=Severity.HIGH,
            blocking=False,  # policy escalates this hard category
            repairable=True,
            section_id="models",
            message="Citation line range does not resolve on the frozen snapshot.",
        ),
        task_id="task_1",
        artifact_id="artifact_1",
        artifact_hash=HASH,
    )
    assert finding.blocking is True
    outcome = _adjudicate(findings=(finding,), evaluator_recommendation="ACCEPT")
    assert outcome.decision is FinalDecision.REPAIR
    assert any(item.subject_id == finding.id for item in outcome.uncovered_subjects)


def test_incomplete_review_and_unknown_schema_fail_closed() -> None:
    receipt = _receipt(coverage=0.4)
    outcome = _adjudicate(
        read_receipt=receipt,
        rubric_score=None,
        result_schema_valid=False,
        evaluator_recommendation="ACCEPT",
    )
    assert outcome.decision is FinalDecision.REPAIR
    assert {item.subject_id for item in outcome.uncovered_subjects}.issuperset(
        {"QG-013", "QG-015"}
    )


def test_hard_budget_exhaustion_is_needs_attention_not_completed() -> None:
    outcome = _adjudicate(budget_status=BudgetStatus.EXHAUSTED)
    assert outcome.decision is FinalDecision.NEEDS_ATTENTION
    assert outcome.reason_code == "budget_exhausted"
    assert outcome.publishable is False


def test_exact_active_score_waiver_yields_waived_without_mutating_failure() -> None:
    rubric = repository_analysis_rubric()
    low_score = _score(subtract=20)
    waiver = QualityWaiver(
        id="waiver_1",
        task_id="task_1",
        artifact_id="artifact_1",
        artifact_hash=HASH,
        contract_id="contract_1",
        contract_version=1,
        subject_type=WaiverSubjectType.SEMANTIC_SCORE,
        subject_id=rubric.id,
        subject_version=rubric.version,
        rubric_id=rubric.id,
        rubric_version=rubric.version,
        actor_id="quality-admin",
        reason="Accepted benchmark variance",
        created_at=NOW,
        signature_hash="sha256:" + "b" * 64,
    )
    outcome = _adjudicate(rubric_score=low_score, waivers=(waiver,))
    assert outcome.decision is FinalDecision.PUBLISH
    assert outcome.quality_status is QualityStatus.WAIVED
    assert outcome.waiver_ids == ("waiver_1",)
    assert outcome.failed_subjects[0].reason_code == "semantic_score_below_threshold"


def test_waiver_never_covers_nonwaivable_artifact_or_receipt_integrity() -> None:
    waiver = QualityWaiver(
        id="invalid_scope_waiver",
        task_id="task_1",
        artifact_id="artifact_1",
        artifact_hash=HASH,
        contract_id="contract_1",
        contract_version=1,
        subject_type=WaiverSubjectType.GATE_RESULT,
        subject_id="QG-013",
        subject_version=1,
        actor_id="quality-admin",
        reason="Cannot waive exact read completeness",
        created_at=NOW,
        signature_hash="sha256:" + "c" * 64,
    )
    outcome = _adjudicate(
        read_receipt=_receipt(coverage=0.4), rubric_score=None, waivers=(waiver,)
    )
    assert outcome.publishable is False
    assert "QG-013" in {item.subject_id for item in outcome.uncovered_subjects}


def test_repair_is_bounded_and_same_finding_delta_is_visible() -> None:
    finding = materialize_finding(
        FindingInput(
            category=FindingCategory.COVERAGE,
            severity=Severity.HIGH,
            blocking=True,
            repairable=True,
            requirement_id="req-macros",
            message="Macros section is missing.",
        ),
        task_id="task_1",
        artifact_id="artifact_1",
        artifact_hash=HASH,
    )
    outcome = _adjudicate(findings=(finding,), repair_attempts=2)
    assert outcome.decision is FinalDecision.NEEDS_ATTENTION
    assert outcome.reason_code == "repair_exhausted"
    assert finding_delta((finding,), (finding,))["unchanged"] == (finding.fingerprint,)


def test_summary_problem_with_empty_findings_becomes_typed_finding() -> None:
    draft = lint_unstructured_finding(
        summary="The report is incomplete and a required citation is missing.",
        findings=(),
        required_v2=True,
    )
    assert draft is not None
    assert draft.category is FindingCategory.SCHEMA
    assert draft.blocking is True


def test_gate_set_requires_all_sixteen_once() -> None:
    assert len(assert_complete_gate_set(_gates())) == 16
    with pytest.raises(ValueError, match="incomplete"):
        assert_complete_gate_set(_gates()[:-1])
