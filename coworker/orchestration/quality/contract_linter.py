"""Semantic completeness and conflict linting for TaskContractV2."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .archetypes.repo_analysis import REPOSITORY_ANALYSIS_AREAS
from .models import (
    Archetype,
    RequirementCategory,
    TaskContractV2,
    VerificationMethod,
)


@dataclass(frozen=True, slots=True)
class ContractLintIssue:
    code: str
    path: str
    message: str
    blocking: bool = True

    def as_dict(self) -> dict[str, str | bool]:
        return {
            "code": self.code,
            "path": self.path,
            "message": self.message,
            "blocking": self.blocking,
        }


def lint_contract(contract: TaskContractV2) -> tuple[ContractLintIssue, ...]:
    issues: list[ContractLintIssue] = []
    required = [item for item in contract.requirements if item.required]
    if not required:
        issues.append(
            ContractLintIssue(
                "NO_REQUIRED_REQUIREMENTS",
                "requirements",
                "At least one independently verifiable required requirement is needed.",
            )
        )
    for index, requirement in enumerate(required):
        if requirement.verification_method is VerificationMethod.MANUAL and requirement.hard_gate:
            issues.append(
                ContractLintIssue(
                    "HARD_GATE_MANUAL_ONLY",
                    f"requirements[{index}].verification_method",
                    "A hard gate cannot rely only on an unscoped manual assertion.",
                )
            )
    if required and all(item.category is RequirementCategory.SAFETY for item in required):
        issues.append(
            ContractLintIssue(
                "PERMISSION_ONLY_CONTRACT",
                "requirements",
                "Permissions such as read-only do not define result quality.",
            )
        )

    primary = [item for item in contract.deliverables if item.primary]
    if len(primary) != 1:
        issues.append(
            ContractLintIssue(
                "PRIMARY_DELIVERABLE_COUNT",
                "deliverables",
                "Exactly one primary deliverable is required.",
            )
        )
    if contract.archetype is Archetype.REPO_ANALYSIS:
        categories = {
            item.category for item in required if item.hard_gate
        }
        for category in (
            RequirementCategory.CURRENTNESS,
            RequirementCategory.EVIDENCE,
            RequirementCategory.COVERAGE,
            RequirementCategory.RELATIONSHIP,
            RequirementCategory.LIMITATION,
            RequirementCategory.SAFETY,
        ):
            if category not in categories:
                issues.append(
                    ContractLintIssue(
                        f"MISSING_{category.value.upper()}_GATE",
                        "requirements",
                        f"repo_analysis requires a {category.value} hard gate.",
                    )
                )
        coverage = set()
        for item in required:
            if item.category is RequirementCategory.COVERAGE:
                raw = item.verification_spec.get("areas", ())
                if isinstance(raw, Iterable) and not isinstance(raw, (str, bytes)):
                    coverage.update(str(value) for value in raw)
        missing = sorted(set(REPOSITORY_ANALYSIS_AREAS).difference(coverage))
        if missing:
            issues.append(
                ContractLintIssue(
                    "REPOSITORY_AREAS_INCOMPLETE",
                    "requirements",
                    "Required repository-analysis areas are missing: " + ", ".join(missing),
                )
            )
        if primary:
            deliverable = primary[0]
            if deliverable.mime_type != "text/markdown" or not deliverable.filename.casefold().endswith(".md"):
                issues.append(
                    ContractLintIssue(
                        "REPO_ANALYSIS_DELIVERABLE_FORMAT",
                        "deliverables",
                        "repo_analysis primary deliverable must be a named Markdown artifact.",
                    )
                )
            missing_sections = sorted(
                {"baseline_and_method", "risks", "limitations"}.difference(
                    deliverable.required_sections
                )
            )
            if missing_sections:
                issues.append(
                    ContractLintIssue(
                        "REQUIRED_SECTIONS_INCOMPLETE",
                        "deliverables[primary].required_sections",
                        "Primary report sections are missing: " + ", ".join(missing_sections),
                    )
                )
        constraints = {item.type: item.value for item in contract.constraints}
        expected_constraints = {
            "source_workspace_write": False,
            "task_artifact_write": True,
            "external_write": False,
            "network_access": False,
        }
        for name, expected in expected_constraints.items():
            if constraints.get(name) is not expected:
                issues.append(
                    ContractLintIssue(
                        "PERMISSION_BOUNDARY_INCOMPLETE",
                        "constraints",
                        f"repo_analysis requires {name}={str(expected).lower()}.",
                    )
                )
    return tuple(issues)


def assert_contract_publishable(contract: TaskContractV2) -> None:
    issues = tuple(item for item in lint_contract(contract) if item.blocking)
    if issues:
        detail = "; ".join(f"{item.code}: {item.message}" for item in issues)
        raise ValueError(f"contract is not semantically publishable: {detail}")
    contract.verify_content_hash()
