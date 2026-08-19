"""Strict JSON Schema registry and result-version negotiation for TQE V2."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from pydantic import BaseModel, ValidationError

from .models import (
    AnalysisReportResult,
    BoundResultEnvelope,
    EvidenceBundleResult,
    FinalQualityDecisionResult,
    ReviewResult,
    TaskContractV2,
)


class SchemaRegistryError(ValueError):
    """Raised when a schema is unknown or a payload fails closed validation."""

    def __init__(
        self,
        message: str,
        *,
        expected: str | None = None,
        observed: str | None = None,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(message)
        self.expected = expected
        self.observed = observed
        self.errors = errors or []

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": "RESULT_SCHEMA_INVALID",
            "message": str(self),
            "retryable": False,
            "details": {
                "expected": self.expected,
                "observed": self.observed,
                "errors": self.errors,
            },
        }


@dataclass(frozen=True, slots=True)
class SchemaRegistration:
    schema_id: str
    schema_version: int
    model: type[BaseModel]
    model_authored: bool


_REGISTRATIONS = (
    SchemaRegistration("task_contract_v2", 2, TaskContractV2, False),
    SchemaRegistration("evidence_bundle_result_v2", 2, EvidenceBundleResult, True),
    SchemaRegistration("analysis_report_result_v2", 2, AnalysisReportResult, True),
    SchemaRegistration("review_result_v2", 2, ReviewResult, True),
    SchemaRegistration(
        "final_quality_decision_v2", 2, FinalQualityDecisionResult, True
    ),
)

SCHEMA_REGISTRY: dict[tuple[str, int], SchemaRegistration] = {
    (item.schema_id, item.schema_version): item for item in _REGISTRATIONS
}

if len(SCHEMA_REGISTRY) != len(_REGISTRATIONS):
    raise RuntimeError("quality schema registrations must be unique")


# These fields are injected or derived by settlement.  A model returning one is
# not merely redundant: accepting it would create an identity/authority spoofing
# ambiguity, so validation rejects the payload before Pydantic sees it.
SERVER_AUTHORITY_FIELDS = frozenset(
    {
        "task_id",
        "run_id",
        "contract_id",
        "snapshot_id",
        "read_receipt_id",
        "read_complete",
        "read_ranges",
        "covered_bytes",
        "total_score",
        "scorer_run_id",
    }
)


def registration(schema_id: str, schema_version: int) -> SchemaRegistration:
    key = (str(schema_id), int(schema_version))
    selected = SCHEMA_REGISTRY.get(key)
    if selected is not None:
        return selected
    known_versions = sorted(
        version for candidate, version in SCHEMA_REGISTRY if candidate == key[0]
    )
    observed = f"{key[0]}@{key[1]}"
    expected = (
        ",".join(f"{key[0]}@{version}" for version in known_versions)
        if known_versions
        else "registered schema_id"
    )
    raise SchemaRegistryError(
        f"unknown schema or schema version: {observed}",
        expected=expected,
        observed=observed,
    )


def json_schema(schema_id: str, schema_version: int = 2) -> dict[str, Any]:
    return registration(schema_id, schema_version).model.model_json_schema(
        mode="validation"
    )


def schema_registry_snapshot() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "schemas": [
            {
                "schema_id": item.schema_id,
                "schema_version": item.schema_version,
                "model_authored": item.model_authored,
                "json_schema": item.model.model_json_schema(mode="validation"),
            }
            for item in _REGISTRATIONS
        ],
    }


def validate_model_result(
    payload: Mapping[str, Any],
    *,
    expected_schema_id: str,
    expected_schema_version: int = 2,
) -> BaseModel:
    """Validate a raw model payload without guessing or dropping fields."""

    if not isinstance(payload, Mapping):
        raise SchemaRegistryError(
            "model result must be a JSON object",
            expected=f"{expected_schema_id}@{expected_schema_version}",
            observed=type(payload).__name__,
        )
    supplied = dict(payload)
    forbidden = sorted(SERVER_AUTHORITY_FIELDS.intersection(supplied))
    if forbidden:
        raise SchemaRegistryError(
            "model result contains server-authoritative fields",
            expected="model-authored payload without identity/receipt/total fields",
            observed=", ".join(forbidden),
        )
    observed_id = supplied.get("schema_id")
    observed_version = supplied.get("schema_version")
    expected = f"{expected_schema_id}@{expected_schema_version}"
    observed = f"{observed_id}@{observed_version}"
    if observed_id != expected_schema_id or observed_version != expected_schema_version:
        raise SchemaRegistryError(
            "result schema id/version does not match the frozen strategy contract",
            expected=expected,
            observed=observed,
        )
    selected = registration(expected_schema_id, expected_schema_version)
    if not selected.model_authored:
        raise SchemaRegistryError(
            f"schema {expected} is not a model-authored result schema",
            expected="model-authored result schema",
            observed=expected,
        )
    try:
        return selected.model.model_validate(supplied)
    except ValidationError as exc:
        raise SchemaRegistryError(
            f"payload does not conform to {expected}",
            expected=expected,
            observed=observed,
            errors=exc.errors(include_url=False),
        ) from exc


def bind_result_context(
    validated: BaseModel,
    *,
    task_id: str,
    run_id: str,
    contract_id: str,
    snapshot_id: str,
) -> BoundResultEnvelope:
    """Create the persisted identity envelope from trusted run-bound values."""

    raw = validated.model_dump(mode="json")
    if set(SERVER_AUTHORITY_FIELDS).intersection(raw):
        raise SchemaRegistryError(
            "validated result unexpectedly contains server-authoritative fields"
        )
    return BoundResultEnvelope(
        schema_id=str(raw.pop("schema_id")),
        schema_version=int(raw.pop("schema_version")),
        task_id=str(task_id),
        run_id=str(run_id),
        contract_id=str(contract_id),
        snapshot_id=str(snapshot_id),
        execution_status=raw.pop("execution_status"),
        summary=str(raw.pop("summary")),
        payload=raw,
    )


def negotiate_exact_version(schema_id: str, offered_versions: list[int]) -> int:
    """Choose only a registered exact version; no downgrade guessing is allowed."""

    registered = sorted(
        version for candidate, version in SCHEMA_REGISTRY if candidate == schema_id
    )
    common = sorted(set(registered).intersection(int(v) for v in offered_versions))
    if not common:
        raise SchemaRegistryError(
            f"no mutually supported version for {schema_id}",
            expected=str(registered),
            observed=str(sorted(set(offered_versions))),
        )
    return common[-1]
