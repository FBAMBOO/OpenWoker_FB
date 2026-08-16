"""Deterministic complexity, risk, gate, and acceptance policy.

Models may propose factor values and rationale, but this module owns the score,
hard risk floors, and whether a human gate is mandatory.  Keeping that decision out
of prompts makes orchestration replayable and prevents an agent from self-approving.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


class ComplexityLevel(str, Enum):
    TRIVIAL = "trivial"
    STANDARD = "standard"
    COMPLEX = "complex"
    CRITICAL = "critical"


class RiskTier(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


_WEIGHTS = {
    "scope": 20,
    "uncertainty": 20,
    "dependencies": 15,
    "side_effects": 20,
    "parallelism": 10,
    "verification": 15,
}


@dataclass(frozen=True, slots=True)
class ComplexityFactors:
    scope: int = 0
    uncertainty: int = 0
    dependencies: int = 0
    side_effects: int = 0
    parallelism: int = 0
    verification: int = 0

    def __post_init__(self) -> None:
        for name in _WEIGHTS:
            value = int(getattr(self, name))
            if not 0 <= value <= 5:
                raise ValueError(f"{name} must be between 0 and 5")
            object.__setattr__(self, name, value)

    def as_dict(self) -> dict[str, int]:
        return {name: getattr(self, name) for name in _WEIGHTS}


@dataclass(frozen=True, slots=True)
class ComplexityAssessment:
    score: int
    level: ComplexityLevel
    factors: ComplexityFactors
    risk: RiskTier
    rationale: tuple[str, ...] = ()
    clarification_required: bool = False
    plan_approval_required: bool = False
    final_acceptance_required: bool = False
    review_required: bool = False
    tests_required: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "level": self.level.value,
            "factors": self.factors.as_dict(),
            "risk": self.risk.value,
            "rationale": list(self.rationale),
            "policy": {
                "clarification_required": self.clarification_required,
                "plan_approval_required": self.plan_approval_required,
                "final_acceptance_required": self.final_acceptance_required,
                "review_required": self.review_required,
                "tests_required": self.tests_required,
            },
        }


def complexity_score(factors: ComplexityFactors) -> int:
    return round(
        sum(getattr(factors, name) * weight / 5 for name, weight in _WEIGHTS.items())
    )


def complexity_level(score: int) -> ComplexityLevel:
    if not 0 <= score <= 100:
        raise ValueError("complexity score must be between 0 and 100")
    if score < 25:
        return ComplexityLevel.TRIVIAL
    if score < 50:
        return ComplexityLevel.STANDARD
    if score < 75:
        return ComplexityLevel.COMPLEX
    return ComplexityLevel.CRITICAL


def classify_risk(
    *,
    workspace_writes: bool = False,
    external_writes: bool = False,
    privileged_or_secret: bool = False,
    destructive_or_irreversible: bool = False,
) -> RiskTier:
    if destructive_or_irreversible:
        return RiskTier.CRITICAL
    if external_writes or privileged_or_secret:
        return RiskTier.HIGH
    if workspace_writes:
        return RiskTier.MEDIUM
    return RiskTier.LOW


def assess_complexity(
    factors: ComplexityFactors,
    *,
    risk: RiskTier,
    domain: str,
    acceptance_criteria_present: bool,
    rationale: tuple[str, ...] | list[str] = (),
) -> ComplexityAssessment:
    """Apply deterministic floors to a scorer's proposed factor values."""
    raw = complexity_score(factors)
    floor = 0
    reasons = [str(item) for item in rationale if str(item).strip()]
    if domain == "code" and risk is not RiskTier.LOW:
        floor = max(floor, 25)
        reasons.append("code changes are at least standard complexity")
    if risk is RiskTier.HIGH:
        floor = max(floor, 50)
        reasons.append("high-risk side effects require complex handling")
    if risk is RiskTier.CRITICAL:
        floor = 75
        reasons.append("critical risk requires critical handling")
    score = max(raw, floor)
    human = risk is not RiskTier.LOW
    return ComplexityAssessment(
        score=score,
        level=complexity_level(score),
        factors=factors,
        risk=risk,
        rationale=tuple(dict.fromkeys(reasons)),
        clarification_required=(
            not acceptance_criteria_present or factors.uncertainty >= 3
        ),
        plan_approval_required=human,
        final_acceptance_required=human,
        review_required=(domain == "code" and risk is not RiskTier.LOW)
        or score >= 25,
        tests_required=(domain == "code" and risk is not RiskTier.LOW),
    )


@dataclass(frozen=True, slots=True)
class AcceptanceVerdict:
    accepted: bool
    requires_human: bool
    reasons: tuple[str, ...] = field(default_factory=tuple)


def evaluate_acceptance(
    *,
    risk: RiskTier,
    criteria: Mapping[str, str],
    unresolved_gates: int = 0,
    ambiguous_effects: int = 0,
    required_nodes_complete: bool = True,
) -> AcceptanceVerdict:
    """Return whether policy may accept; it never treats unknown evidence as passing."""
    reasons: list[str] = []
    if not criteria:
        reasons.append("no acceptance criteria")
    for criterion, verdict in criteria.items():
        if str(verdict).lower() != "pass":
            reasons.append(f"criterion {criterion!r} is {verdict!r}, not pass")
    if unresolved_gates:
        reasons.append(f"{unresolved_gates} unresolved gate(s)")
    if ambiguous_effects:
        reasons.append(f"{ambiguous_effects} ambiguous side effect(s)")
    if not required_nodes_complete:
        reasons.append("required nodes are incomplete")
    requires_human = risk is not RiskTier.LOW
    return AcceptanceVerdict(
        # Evidence validity and approval authority are deliberately independent.
        # A medium/high-risk result may satisfy every acceptance criterion while
        # still requiring a human signature.  Callers use ``requires_human`` to
        # open that gate; ``accepted`` determines whether the ordinary ``accept``
        # action is safe or an explicitly justified override is required.
        accepted=not reasons,
        requires_human=requires_human,
        reasons=tuple(reasons),
    )
