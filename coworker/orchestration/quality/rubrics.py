"""Versioned semantic rubric and server-authoritative score construction."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Iterable

from .models import (
    Archetype,
    ArtifactReadReceipt,
    QualityRubric,
    RubricDimension,
    RubricDimensionScore,
    RubricScore,
    model_content_sha256,
)


def repository_analysis_rubric() -> QualityRubric:
    dimensions = (
        RubricDimension(
            id="instruction_scope",
            title="Instruction adherence and scope",
            max_points=10,
            instructions="Honor read-only, target scope and requested deliverable format.",
        ),
        RubricDimension(
            id="baseline_method",
            title="Baseline and method discipline",
            max_points=10,
            instructions="Use the frozen snapshot and disclose method, assumptions and freshness.",
        ),
        RubricDimension(
            id="required_domains",
            title="Required architecture domains",
            max_points=15,
            instructions="Cover every contract-required domain with sufficient depth.",
        ),
        RubricDimension(
            id="relationships_synthesis",
            title="Component relationships and synthesis",
            max_points=15,
            instructions="Explain DAG, cross-layer lineage, boundaries and deployment relationships.",
        ),
        RubricDimension(
            id="evidence_traceability",
            title="File evidence and traceability",
            max_points=20,
            instructions="Citations resolve on the frozen snapshot and directly support claims.",
            required_evidence_types=("file_range",),
        ),
        RubricDimension(
            id="quantitative_reproducibility",
            title="Quantitative reproducibility",
            max_points=10,
            instructions="Inventories reconcile and can be reproduced from recorded queries.",
        ),
        RubricDimension(
            id="risk_actionability",
            title="Risk and actionability",
            max_points=10,
            instructions="Prioritize evidence-backed risks and concrete next actions.",
        ),
        RubricDimension(
            id="limitations",
            title="Limitations and epistemic boundaries",
            max_points=5,
            instructions="Separate static inference, runtime facts and unverified negatives.",
        ),
        RubricDimension(
            id="information_structure",
            title="Expression and information structure",
            max_points=5,
            instructions="Make the deliverable readable, auditable and directly usable.",
        ),
    )
    draft = QualityRubric(
        id="repo-analysis-quality-first",
        version=1,
        name="Repository analysis quality-first",
        applicable_archetypes=(Archetype.REPO_ANALYSIS,),
        dimensions=dimensions,
        pass_threshold=85,
        content_hash="sha256:" + "0" * 64,
    )
    return draft.model_copy(update={"content_hash": model_content_sha256(draft)})


def focused_question_rubric() -> QualityRubric:
    """Return the lightweight, independently scored focused-answer rubric."""

    dimensions = (
        RubricDimension(
            id="instruction_scope",
            title="Instruction adherence and scope",
            max_points=20,
            instructions="Answer the frozen objective directly and honor its constraints.",
        ),
        RubricDimension(
            id="answer_correctness",
            title="Answer correctness",
            max_points=35,
            instructions="Reach conclusions that are accurate and supported by the available inputs.",
        ),
        RubricDimension(
            id="answer_completeness",
            title="Answer completeness",
            max_points=20,
            instructions="Address every required criterion without unrelated expansion.",
        ),
        RubricDimension(
            id="reasoning_support",
            title="Reasoning and support",
            max_points=15,
            instructions="Make the reasoning auditable and distinguish evidence from inference.",
        ),
        RubricDimension(
            id="clarity_limitations",
            title="Clarity and limitations",
            max_points=10,
            instructions="Communicate clearly and disclose material uncertainty or limitations.",
        ),
    )
    draft = QualityRubric(
        id="focused-question-quality",
        version=1,
        name="Focused question quality",
        applicable_archetypes=(Archetype.FOCUSED_QUESTION,),
        dimensions=dimensions,
        pass_threshold=85,
        content_hash="sha256:" + "0" * 64,
    )
    return draft.model_copy(update={"content_hash": model_content_sha256(draft)})


def rubric_for_archetype(archetype: Archetype | str) -> QualityRubric:
    """Resolve the immutable rubric frozen by a supported V2 strategy archetype."""

    selected = Archetype(archetype)
    if selected is Archetype.REPO_ANALYSIS:
        return repository_analysis_rubric()
    if selected is Archetype.FOCUSED_QUESTION:
        return focused_question_rubric()
    raise ValueError(f"no Task Quality V2 rubric is registered for {selected.value}")


def create_rubric_score(
    *,
    rubric: QualityRubric,
    artifact_id: str,
    artifact_hash: str,
    scorer_run_id: str,
    authorized_scorer_run_id: str,
    dimension_scores: Iterable[RubricDimensionScore],
    read_receipt: ArtifactReadReceipt | None,
) -> RubricScore:
    if scorer_run_id != authorized_scorer_run_id:
        raise PermissionError("only the strategy-designated semantic scorer may score")
    if (
        read_receipt is None
        or read_receipt.run_id != scorer_run_id
        or read_receipt.artifact_id != artifact_id
        or read_receipt.artifact_hash != artifact_hash
        or read_receipt.completed_at is None
        or read_receipt.coverage_ratio != 1.0
        or read_receipt.completed_at <= read_receipt.candidate_bound_at
    ):
        raise ValueError("semantic scoring requires a fresh complete exact-artifact receipt")
    submitted = tuple(dimension_scores)
    by_id = {item.dimension_id: item for item in submitted}
    if len(by_id) != len(submitted):
        raise ValueError("rubric dimension scores must be unique")
    expected = {item.id: item for item in rubric.dimensions}
    if set(by_id) != set(expected):
        raise ValueError(
            f"rubric dimension set mismatch: expected={sorted(expected)}, observed={sorted(by_id)}"
        )
    for dimension_id, score in by_id.items():
        if score.points > expected[dimension_id].max_points:
            raise ValueError(
                f"dimension {dimension_id} exceeds max {expected[dimension_id].max_points}"
            )
    ordered = tuple(by_id[dimension.id] for dimension in rubric.dimensions)
    return RubricScore(
        id=f"rubric_score_{uuid.uuid4().hex}",
        rubric_id=rubric.id,
        rubric_version=rubric.version,
        artifact_id=artifact_id,
        artifact_hash=artifact_hash,
        scorer_run_id=scorer_run_id,
        dimension_scores=ordered,
        total=sum(item.points for item in ordered),
        created_at=datetime.now(timezone.utc),
    )
