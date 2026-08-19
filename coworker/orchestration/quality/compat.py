"""Explicit, loss-audited adapters for legacy V1 role results."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .models import Requirement, TaskContractV2


class LegacyResultAdapterError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CompatibilityWarning:
    code: str
    field: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "field": self.field, "message": self.message}


@dataclass(frozen=True, slots=True)
class AdaptedLegacyResult:
    adapter_version: str
    schema_id: str
    payload: Mapping[str, Any]
    compatibility_warnings: tuple[CompatibilityWarning, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "adapter_version": self.adapter_version,
            "schema_id": self.schema_id,
            "payload": dict(self.payload),
            "compatibility_warnings": [
                warning.as_dict() for warning in self.compatibility_warnings
            ],
        }


class LegacyResultAdapter:
    """Map registered V1 shapes while making every loss/ambiguity visible."""

    version = "legacy-result-adapter@1"
    _COMMON_FIELDS = frozenset(
        {"summary", "status", "criteria", "files_touched", "checks", "remaining_risks"}
    )

    def __init__(self, contract: TaskContractV2) -> None:
        self.contract = contract
        self._by_id = {requirement.id: requirement for requirement in contract.requirements}
        self._by_text: dict[str, list[Requirement]] = {}
        for requirement in contract.requirements:
            key = self._normalize(requirement.text)
            self._by_text.setdefault(key, []).append(requirement)

    @staticmethod
    def _normalize(value: str) -> str:
        return " ".join(str(value).strip().casefold().split())

    def _requirement_id(self, raw: Any) -> str:
        if isinstance(raw, Mapping):
            candidate_id = str(raw.get("id") or raw.get("requirement_id") or "").strip()
            candidate_text = str(raw.get("criterion") or raw.get("text") or "").strip()
        else:
            candidate_id = ""
            candidate_text = str(raw).strip()
        if candidate_id:
            requirement = self._by_id.get(candidate_id)
            if requirement is None:
                raise LegacyResultAdapterError(
                    f"legacy criterion references unknown requirement id {candidate_id!r}"
                )
            if candidate_text and self._normalize(candidate_text) != self._normalize(
                requirement.text
            ):
                raise LegacyResultAdapterError(
                    f"legacy criterion id/text disagree for {candidate_id!r}"
                )
            return requirement.id
        matches = self._by_text.get(self._normalize(candidate_text), [])
        if len(matches) != 1:
            reason = "missing" if not matches else "ambiguous"
            raise LegacyResultAdapterError(
                f"legacy criterion mapping is {reason}: {candidate_text!r}"
            )
        return matches[0].id

    def _criteria(self, raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
            raise LegacyResultAdapterError("legacy criteria must be an array")
        output: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, Mapping):
                raise LegacyResultAdapterError("each legacy criterion must be an object")
            unknown = sorted(set(item).difference({"id", "requirement_id", "criterion", "text", "status"}))
            if unknown:
                raise LegacyResultAdapterError(
                    "legacy criterion contains unmapped fields: " + ", ".join(unknown)
                )
            status = str(item.get("status") or "").strip()
            if status not in {"pass", "fail", "unknown"}:
                raise LegacyResultAdapterError(
                    f"legacy criterion has invalid status {status!r}"
                )
            output.append(
                {
                    "requirement_id": self._requirement_id(item),
                    "status": status,
                    "rationale": "Adapted from legacy criterion result.",
                    "evidence_ids": [],
                }
            )
        return output

    @staticmethod
    def _string_array(raw: Any, field: str) -> list[str]:
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
            raise LegacyResultAdapterError(f"legacy {field} must be an array")
        return [str(item) for item in raw]

    def adapt(self, result: Mapping[str, Any], *, role: str) -> AdaptedLegacyResult:
        if not isinstance(result, Mapping):
            raise LegacyResultAdapterError("legacy result must be an object")
        unknown = sorted(set(result).difference(self._COMMON_FIELDS))
        if unknown:
            # The adapter has no authority to silently discard fields.  A future
            # registered adapter version may add a deliberate mapping.
            raise LegacyResultAdapterError(
                "legacy result contains unmapped fields: " + ", ".join(unknown)
            )
        missing = sorted(self._COMMON_FIELDS.difference(result))
        if missing:
            raise LegacyResultAdapterError(
                "legacy result is missing required fields: " + ", ".join(missing)
            )
        normalized_role = str(role).strip().casefold()
        criteria = self._criteria(result["criteria"])
        risks = self._string_array(result["remaining_risks"], "remaining_risks")
        files = self._string_array(result["files_touched"], "files_touched")
        checks = self._string_array(result["checks"], "checks")
        summary = str(result["summary"])
        warnings: list[CompatibilityWarning] = [
            CompatibilityWarning(
                "LEGACY_RESULT_ADAPTED",
                "schema_id",
                f"Result was converted by {self.version}; V1 data remains non-authoritative.",
            )
        ]

        if normalized_role in {"reviewer", "tester", "evaluator", "review", "test", "evaluate"}:
            payload: dict[str, Any] = {
                "summary": summary,
                "criterion_results": criteria,
                "risks": risks,
                "checks": checks,
                "legacy_files_touched": files,
                "verdict": str(result["status"]),
            }
            schema_id = "legacy_review_projection_v2"
        else:
            requirement_claims = [
                {
                    "requirement_id": item["requirement_id"],
                    "claimed_status": (
                        "addressed"
                        if item["status"] == "pass"
                        else "not_addressed"
                        if item["status"] == "fail"
                        else "unknown"
                    ),
                    "evidence_ids": [],
                }
                for item in criteria
            ]
            payload = {
                "summary": summary,
                "requirement_claims": requirement_claims,
                "risks": risks,
                "legacy_files_touched": files,
                "checks": checks,
                "primary_artifact": None,
                "primary_artifact_status": "unknown",
            }
            schema_id = "legacy_analysis_projection_v2"
            warnings.append(
                CompatibilityWarning(
                    "PRIMARY_ARTIFACT_UNKNOWN",
                    "primary_artifact",
                    "V1 result did not uniquely identify a finalized primary artifact.",
                )
            )
        return AdaptedLegacyResult(
            adapter_version=self.version,
            schema_id=schema_id,
            payload=payload,
            compatibility_warnings=tuple(warnings),
        )
