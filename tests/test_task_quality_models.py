from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from coworker.orchestration.quality.compat import (
    LegacyResultAdapter,
    LegacyResultAdapterError,
)
from coworker.orchestration.quality.models import (
    Archetype,
    CompilerMetadata,
    Constraint,
    ConstraintEnforcement,
    DeliverableSpec,
    Requirement,
    RequirementCategory,
    RequirementSource,
    TaskContractV2,
    VerificationMethod,
    canonical_status_values,
)
from coworker.orchestration.quality.schemas import (
    SchemaRegistryError,
    bind_result_context,
    json_schema,
    validate_model_result,
)


def _contract(*, duplicate_text: bool = False) -> TaskContractV2:
    requirements = [
        Requirement(
            id="req-read-only",
            category=RequirementCategory.SAFETY,
            text="Do not modify the source workspace.",
            source=RequirementSource.EXPLICIT_PROMPT,
            required=True,
            hard_gate=True,
            verification_method=VerificationMethod.WORKSPACE_UNCHANGED,
            waivable=False,
        ),
        Requirement(
            id="req-coverage",
            category=RequirementCategory.COVERAGE,
            text="Cover all required architecture domains.",
            source=RequirementSource.ARCHETYPE,
            required=True,
            hard_gate=True,
            verification_method=VerificationMethod.COVERAGE,
            verification_spec={"areas": ["entry", "models"]},
            waivable=False,
        ),
    ]
    if duplicate_text:
        requirements.append(
            requirements[-1].model_copy(update={"id": "req-coverage-duplicate"})
        )
    return TaskContractV2(
        id="contract_1",
        task_id="task_1",
        version=1,
        status="draft",
        title="Repository analysis",
        objective="Read-only repository analysis",
        original_prompt_hash="sha256:" + "1" * 64,
        archetype=Archetype.REPO_ANALYSIS,
        requirements=tuple(requirements),
        constraints=(
            Constraint(
                id="constraint-source-write",
                type="source_workspace_write",
                text="Source workspace writes are forbidden.",
                enforcement=ConstraintEnforcement.SANDBOX,
                source=RequirementSource.EXPLICIT_PROMPT,
                hard=True,
                verification_method=VerificationMethod.WORKSPACE_UNCHANGED,
                value=False,
            ),
        ),
        deliverables=(
            DeliverableSpec(
                id="report",
                kind="analysis_report",
                filename="report.md",
                mime_type="text/markdown",
                primary=True,
                required_sections=("baseline_and_method", "limitations"),
                result_schema_id="analysis_report_result_v2",
            ),
        ),
        quality_profile_id="repo-analysis-quality-first@1",
        compiler=CompilerMetadata(ruleset_version="1", confidence=1.0),
        content_hash="sha256:" + "0" * 64,
    )


def _analysis_completed() -> dict:
    return {
        "schema_id": "analysis_report_result_v2",
        "schema_version": 2,
        "execution_status": "completed",
        "summary": "Report created.",
        "primary_artifact": {
            "artifact_id": "artifact_1",
            "sha256": "sha256:" + "a" * 64,
            "filename": "report.md",
            "mime_type": "text/markdown",
            "byte_size": 42,
        },
        "requirement_claims": [
            {
                "requirement_id": "req-coverage",
                "claimed_status": "addressed",
                "evidence_ids": [],
            }
        ],
        "coverage_claims": [],
        "claim_ledger_id": "claims_1",
        "risks": [],
        "limitations": ["Static analysis only."],
        "source_workspace_changes": [],
    }


def test_status_enums_are_one_canonical_four_axis_source() -> None:
    assert canonical_status_values() == {
        "workflow_status": [
            "draft",
            "analyzing",
            "needs_target_selection",
            "ready",
            "running",
            "validating",
            "reviewing",
            "repairing",
            "recovering",
            "needs_reconciliation",
            "needs_attention",
            "completed",
            "failed",
            "canceled",
            "archived",
        ],
        "quality_status": ["pending", "checking", "pass", "fail", "unknown", "waived"],
        "artifact_status": [
            "none",
            "uploading",
            "draft",
            "validating",
            "verified",
            "rejected",
            "superseded",
        ],
        "budget_status": [
            "unconfigured",
            "within_budget",
            "warning",
            "exhausted",
            "over_budget",
            "unlimited",
        ],
    }


def test_role_result_schema_has_discriminated_mutually_exclusive_shapes() -> None:
    schema = json_schema("analysis_report_result_v2")
    assert len(schema["oneOf"]) == 3
    assert schema["discriminator"]["propertyName"] == "execution_status"
    completed = validate_model_result(
        _analysis_completed(), expected_schema_id="analysis_report_result_v2"
    )
    bound = bind_result_context(
        completed,
        task_id="task_1",
        run_id="run_1",
        contract_id="contract_1",
        snapshot_id="snapshot_1",
    )
    assert bound.task_id == "task_1"
    assert bound.payload["primary_artifact"]["artifact_id"] == "artifact_1"

    partial = validate_model_result(
        {
            "schema_id": "analysis_report_result_v2",
            "schema_version": 2,
            "execution_status": "partial",
            "summary": "Checkpointed before completion.",
            "checkpoint": {
                "artifact_id": "artifact_checkpoint",
                "content_hash": "sha256:" + "b" * 64,
                "resume_cursor": "section:relationships",
            },
            "incomplete_reasons": ["Budget checkpoint"],
            "provisional_artifact_ids": ["artifact_checkpoint"],
        },
        expected_schema_id="analysis_report_result_v2",
    )
    failed = validate_model_result(
        {
            "schema_id": "analysis_report_result_v2",
            "schema_version": 2,
            "execution_status": "failed",
            "summary": "The producer failed closed.",
            "error": {
                "code": "PRODUCER_FAILED",
                "message": "No primary result was published.",
                "retryable": False,
            },
            "diagnostic_artifact_ids": [],
        },
        expected_schema_id="analysis_report_result_v2",
    )
    assert partial.root.execution_status == "partial"
    assert failed.root.execution_status == "failed"

    mixed = {
        **_analysis_completed(),
        "error": {"code": "FAILED", "message": "bad", "retryable": False},
    }
    with pytest.raises(SchemaRegistryError):
        validate_model_result(mixed, expected_schema_id="analysis_report_result_v2")
    with pytest.raises(SchemaRegistryError):
        validate_model_result(
            {
                "schema_id": "analysis_report_result_v2",
                "schema_version": 2,
                "execution_status": "partial",
                "summary": "Invalid mixed partial.",
                "checkpoint": {
                    "artifact_id": "artifact_checkpoint",
                    "content_hash": "sha256:" + "b" * 64,
                    "resume_cursor": "section:relationships",
                },
                "incomplete_reasons": ["Budget checkpoint"],
                "primary_artifact": _analysis_completed()["primary_artifact"],
            },
            expected_schema_id="analysis_report_result_v2",
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("task_id", "forged_task"),
        ("run_id", "forged_run"),
        ("read_complete", True),
        ("total_score", 100),
    ],
)
def test_model_cannot_submit_server_authority_fields(field: str, value: object) -> None:
    with pytest.raises(SchemaRegistryError, match="server-authoritative"):
        validate_model_result(
            {**_analysis_completed(), field: value},
            expected_schema_id="analysis_report_result_v2",
        )


def test_unknown_or_missing_result_schema_fails_closed() -> None:
    with pytest.raises(SchemaRegistryError) as unknown:
        validate_model_result(
            {**_analysis_completed(), "schema_version": 99},
            expected_schema_id="analysis_report_result_v2",
        )
    assert unknown.value.expected == "analysis_report_result_v2@2"
    assert unknown.value.observed == "analysis_report_result_v2@99"

    missing = _analysis_completed()
    del missing["primary_artifact"]
    with pytest.raises(SchemaRegistryError):
        validate_model_result(missing, expected_schema_id="analysis_report_result_v2")


def test_contract_requires_exactly_one_primary_deliverable() -> None:
    with pytest.raises(ValidationError, match="exactly one primary"):
        TaskContractV2.model_validate(
            {
                **_contract().model_dump(mode="json"),
                "deliverables": [
                    {
                        **_contract().deliverables[0].model_dump(mode="json"),
                        "primary": False,
                    }
                ],
            }
        )


def test_legacy_adapter_maps_exact_fields_and_never_silently_drops() -> None:
    adapter = LegacyResultAdapter(_contract())
    legacy = {
        "summary": "Legacy report summary",
        "status": "pass",
        "criteria": [
            {
                "criterion": "Cover all required architecture domains.",
                "status": "pass",
            }
        ],
        "files_touched": [],
        "checks": ["read-only"],
        "remaining_risks": ["Runtime facts not inspected"],
    }
    adapted = adapter.adapt(legacy, role="producer")
    assert adapted.payload["requirement_claims"] == [
        {
            "requirement_id": "req-coverage",
            "claimed_status": "addressed",
            "evidence_ids": [],
        }
    ]
    assert adapted.payload["risks"] == ["Runtime facts not inspected"]
    assert adapted.payload["primary_artifact_status"] == "unknown"
    assert adapted.compatibility_warnings

    with pytest.raises(LegacyResultAdapterError, match="unmapped fields"):
        adapter.adapt({**legacy, "discard_me": "not silently"}, role="producer")


def test_legacy_adapter_rejects_ambiguous_criterion_text() -> None:
    adapter = LegacyResultAdapter(_contract(duplicate_text=True))
    legacy = {
        "summary": "x",
        "status": "unknown",
        "criteria": [
            {
                "criterion": "Cover all required architecture domains.",
                "status": "unknown",
            }
        ],
        "files_touched": [],
        "checks": [],
        "remaining_risks": [],
    }
    with pytest.raises(LegacyResultAdapterError, match="ambiguous"):
        adapter.adapt(legacy, role="reviewer")
