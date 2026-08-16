import pytest

from coworker.orchestration.policy import (
    ComplexityFactors,
    ComplexityLevel,
    RiskTier,
    assess_complexity,
    classify_risk,
    complexity_score,
    evaluate_acceptance,
)


def test_weighted_complexity_boundaries_and_validation():
    assert complexity_score(ComplexityFactors()) == 0
    assert complexity_score(ComplexityFactors(5, 5, 5, 5, 5, 5)) == 100
    with pytest.raises(ValueError):
        ComplexityFactors(scope=6)


def test_code_write_gets_standard_floor_and_human_gates():
    result = assess_complexity(
        ComplexityFactors(scope=1),
        risk=classify_risk(workspace_writes=True),
        domain="code",
        acceptance_criteria_present=True,
    )
    assert result.score == 25
    assert result.level is ComplexityLevel.STANDARD
    assert result.risk is RiskTier.MEDIUM
    assert result.plan_approval_required
    assert result.final_acceptance_required
    assert result.review_required and result.tests_required


def test_uncertain_or_criteria_free_task_requires_clarification():
    result = assess_complexity(
        ComplexityFactors(uncertainty=3),
        risk=RiskTier.LOW,
        domain="knowledge",
        acceptance_criteria_present=False,
    )
    assert result.clarification_required


def test_evidence_acceptance_is_independent_from_human_approval_requirement():
    accepted = evaluate_acceptance(
        risk=RiskTier.LOW,
        criteria={"answer sourced": "pass", "format valid": "pass"},
    )
    assert accepted.accepted and not accepted.requires_human

    human = evaluate_acceptance(
        risk=RiskTier.MEDIUM,
        criteria={"tests": "pass"},
    )
    assert human.accepted and human.requires_human

    incomplete = evaluate_acceptance(
        risk=RiskTier.LOW,
        criteria={"tests": "unknown"},
        unresolved_gates=1,
    )
    assert not incomplete.accepted
    assert len(incomplete.reasons) == 2
