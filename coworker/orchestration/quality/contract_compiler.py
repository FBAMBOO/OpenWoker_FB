"""Goal-only deterministic TaskContractV2 compiler with traceable source spans."""

from __future__ import annotations

import hashlib
import re
import unicodedata
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .archetypes.repo_analysis import (
    REPOSITORY_ANALYSIS_AREAS,
    REPOSITORY_ANALYSIS_REQUIRED_SECTIONS,
)
from .contract_linter import ContractLintIssue, lint_contract
from .contract_rules import (
    CONTRACT_RULESET_VERSION,
    CURRENT_CHECKOUT_RULE,
    EVIDENCE_RULE,
    EXPLICIT_REF_RULE,
    MARKDOWN_RULE,
    READ_ONLY_RULE,
    RELATIONSHIP_RULE,
    classify_archetype,
)
from .models import (
    Archetype,
    CompilerMetadata,
    Constraint,
    ConstraintEnforcement,
    ContractScope,
    DeliverableSpec,
    Requirement,
    RequirementCategory,
    RequirementSource,
    SourceSpan,
    TaskContractV2,
    VerificationMethod,
    model_content_sha256,
)


@dataclass(frozen=True, slots=True)
class ContractConflict:
    code: str
    higher_source: str
    lower_source: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "higher_source": self.higher_source,
            "lower_source": self.lower_source,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class CompilationResult:
    contract: TaskContractV2
    issues: tuple[ContractLintIssue, ...]
    conflicts: tuple[ContractConflict, ...]
    cache_hit: bool

    @property
    def start_allowed(self) -> bool:
        return not self.conflicts and not any(item.blocking for item in self.issues)


def normalize_prompt(prompt: str) -> str:
    normalized = unicodedata.normalize("NFC", str(prompt)).strip()
    if not normalized:
        raise ValueError("objective is required")
    if len(normalized.encode("utf-8")) > 131_072:
        raise ValueError("objective exceeds the contract compiler input limit")
    return normalized


def _prompt_hash(prompt: str) -> str:
    return "sha256:" + hashlib.sha256(prompt.encode("utf-8")).hexdigest()


def _source_span(prompt: str, match: re.Match[str] | None) -> SourceSpan | None:
    if match is None:
        return None
    before = prompt[: match.start()].encode("utf-8")
    selected = prompt[match.start() : match.end()].encode("utf-8")
    return SourceSpan(
        start_byte=len(before),
        end_byte=len(before) + len(selected),
        text_hash="sha256:" + hashlib.sha256(selected).hexdigest(),
    )


def _requirement(
    identifier: str,
    category: RequirementCategory,
    text: str,
    method: VerificationMethod,
    *,
    source: RequirementSource = RequirementSource.ARCHETYPE,
    span: SourceSpan | None = None,
    verification_spec: Mapping[str, Any] | None = None,
    waivable: bool = False,
) -> Requirement:
    return Requirement(
        id=identifier,
        category=category,
        text=text,
        required=True,
        hard_gate=True,
        source=source,
        source_span=span,
        confidence=1.0 if source is RequirementSource.INFERRED else None,
        verification_method=method,
        verification_spec=dict(verification_spec or {}),
        waivable=waivable,
    )


class ContractCompiler:
    """Rules-first compiler; identical normalized inputs are evaluated once."""

    def __init__(self) -> None:
        self._cache: dict[str, CompilationResult] = {}

    def compile(
        self,
        *,
        task_id: str,
        objective: str,
        title: str | None = None,
        language: str = "zh-CN",
        version: int = 1,
        explicit_permissions: Mapping[str, bool] | None = None,
        user_criteria: Iterable[str] = (),
        quality_profile_id: str | None = None,
    ) -> CompilationResult:
        prompt = normalize_prompt(objective)
        permissions = dict(explicit_permissions or {})
        archetype = classify_archetype(prompt)
        quality_profile_id = quality_profile_id or (
            "repo-analysis-quality-first@1"
            if archetype is Archetype.REPO_ANALYSIS
            else "focused-question-quality@1"
            if archetype is Archetype.FOCUSED_QUESTION
            else "task-quality-v2-default@1"
        )
        key = _prompt_hash(
            "\n".join(
                [
                    task_id,
                    str(version),
                    prompt,
                    repr(sorted(permissions.items())),
                    repr(tuple(user_criteria)),
                    quality_profile_id,
                ]
            )
        )
        cached = self._cache.get(key)
        if cached is not None:
            return CompilationResult(
                cached.contract, cached.issues, cached.conflicts, cache_hit=True
            )
        requirements: list[Requirement] = []
        constraints: list[Constraint] = []
        conflicts: list[ContractConflict] = []
        read_only_match = READ_ONLY_RULE.search(prompt)

        if archetype is Archetype.REPO_ANALYSIS:
            requirements.extend(
                (
                    _requirement(
                        "req-baseline-frozen",
                        RequirementCategory.CURRENTNESS,
                        "Resolve and freeze the authoritative repository/project target before model work.",
                        VerificationMethod.CITATION,
                        verification_spec={
                            "include": [
                                "repo_root",
                                "project_root",
                                "snapshot_kind",
                                "selected_ref",
                                "commit_or_content_hash",
                                "method",
                            ],
                            "target_preference": (
                                "current_checkout"
                                if CURRENT_CHECKOUT_RULE.search(prompt)
                                else "explicit_ref"
                                if EXPLICIT_REF_RULE.search(prompt)
                                else "quality_first_current_project"
                            ),
                        },
                    ),
                    _requirement(
                        "req-required-domains",
                        RequirementCategory.COVERAGE,
                        "Cover entry, models, macros, tests, seeds, snapshots and deployment.",
                        VerificationMethod.COVERAGE,
                        verification_spec={"areas": list(REPOSITORY_ANALYSIS_AREAS)},
                    ),
                    _requirement(
                        "req-component-relationships",
                        RequirementCategory.RELATIONSHIP,
                        "Explain component relationships, at least one three-layer lineage, and the execution control chain.",
                        VerificationMethod.CLAIM_SUPPORT,
                        source=(
                            RequirementSource.EXPLICIT_PROMPT
                            if RELATIONSHIP_RULE.search(prompt)
                            else RequirementSource.ARCHETYPE
                        ),
                        span=_source_span(prompt, RELATIONSHIP_RULE.search(prompt)),
                        verification_spec={
                            "minimum_lineage_layers": 3,
                            "require_execution_control_plane": True,
                        },
                    ),
                    _requirement(
                        "req-file-evidence",
                        RequirementCategory.EVIDENCE,
                        "Resolve every citation on the frozen snapshot and directly support P0/P1 claims.",
                        VerificationMethod.CITATION,
                        source=(
                            RequirementSource.EXPLICIT_PROMPT
                            if EVIDENCE_RULE.search(prompt)
                            else RequirementSource.ARCHETYPE
                        ),
                        span=_source_span(prompt, EVIDENCE_RULE.search(prompt)),
                        verification_spec={
                            "citation_resolution_ratio": 1.0,
                            "high_priority_claim_direct_evidence": 1.0,
                        },
                    ),
                    _requirement(
                        "req-inventory-reconciliation",
                        RequirementCategory.EVIDENCE,
                        "Resource totals and subtotals must reconcile to reproducible inventory queries.",
                        VerificationMethod.INVENTORY_RECONCILE,
                    ),
                    _requirement(
                        "req-limitations",
                        RequirementCategory.LIMITATION,
                        "State static-analysis limits, assumptions, negative-search scope, and unverified runtime facts.",
                        VerificationMethod.CLAIM_SUPPORT,
                    ),
                    _requirement(
                        "req-source-unchanged",
                        RequirementCategory.SAFETY,
                        "Do not modify the source workspace; task-owned report artifacts are allowed.",
                        VerificationMethod.WORKSPACE_UNCHANGED,
                        source=(
                            RequirementSource.EXPLICIT_PROMPT
                            if read_only_match
                            else RequirementSource.ARCHETYPE
                        ),
                        span=_source_span(prompt, read_only_match),
                    ),
                    _requirement(
                        "req-markdown-report",
                        RequirementCategory.FORMAT,
                        "Produce one named Markdown architecture report as the primary task artifact.",
                        VerificationMethod.ARTIFACT_EXISTS,
                        source=(
                            RequirementSource.EXPLICIT_PROMPT
                            if MARKDOWN_RULE.search(prompt)
                            else RequirementSource.ARCHETYPE
                        ),
                        span=_source_span(prompt, MARKDOWN_RULE.search(prompt)),
                    ),
                )
            )
        else:
            requirements.append(
                _requirement(
                    "req-objective",
                    RequirementCategory.SCOPE,
                    prompt,
                    VerificationMethod.SEMANTIC_RUBRIC,
                    source=RequirementSource.EXPLICIT_PROMPT,
                    span=SourceSpan(
                        start_byte=0,
                        end_byte=len(prompt.encode("utf-8")),
                        text_hash=_prompt_hash(prompt),
                    ),
                )
            )

        for index, criterion in enumerate(user_criteria):
            text = str(criterion).strip()
            if text:
                requirements.append(
                    _requirement(
                        f"req-user-{index + 1}",
                        RequirementCategory.SCOPE,
                        text,
                        VerificationMethod.SEMANTIC_RUBRIC,
                        source=RequirementSource.USER_CUSTOM,
                        waivable=True,
                    )
                )

        permission_defaults = {
            "source_workspace_write": False,
            "task_artifact_write": True,
            "external_write": False,
            "network_access": False,
        }
        for name, safe_default in permission_defaults.items():
            explicit = permissions.get(name)
            chosen = safe_default if explicit is None else bool(explicit)
            if archetype is Archetype.REPO_ANALYSIS and name in {
                "source_workspace_write",
                "external_write",
                "network_access",
            } and chosen:
                conflicts.append(
                    ContractConflict(
                        "PERMISSION_CEILING_CONFLICT",
                        "security_policy",
                        "explicit_ui",
                        f"repo_analysis security policy does not permit {name}=true",
                    )
                )
                chosen = False
            if (
                name == "source_workspace_write"
                and read_only_match
                and explicit is not None
                and bool(explicit)
            ):
                conflicts.append(
                    ContractConflict(
                        "READ_ONLY_CONFLICT",
                        "explicit_prompt",
                        "explicit_ui",
                        "Prompt requires read-only source access but UI requested writes.",
                    )
                )
                chosen = False
            constraints.append(
                Constraint(
                    id=f"constraint-{name.replace('_', '-')}",
                    type=name,
                    text=f"{name}={str(chosen).lower()}",
                    enforcement=(
                        ConstraintEnforcement.SANDBOX
                        if name == "source_workspace_write"
                        else ConstraintEnforcement.PERMISSION
                    ),
                    source=(
                        RequirementSource.EXPLICIT_UI
                        if explicit is not None
                        else RequirementSource.POLICY
                    ),
                    hard=True,
                    verification_method=(
                        VerificationMethod.WORKSPACE_UNCHANGED
                        if name == "source_workspace_write"
                        else VerificationMethod.MANUAL
                    ),
                    value=chosen,
                )
            )

        fabric_dbt = bool(re.search(r"(?i)fabric\s*/?\s*dbt|fabric.*dbt|dbt.*fabric", prompt))
        deliverable = DeliverableSpec(
            id="deliverable-architecture-report" if archetype is Archetype.REPO_ANALYSIS else "deliverable-primary",
            kind="analysis_report" if archetype is Archetype.REPO_ANALYSIS else "task_result",
            filename=(
                "fabric_dbt_architecture_report.md"
                if fabric_dbt
                else "repository_architecture_report.md"
                if archetype is Archetype.REPO_ANALYSIS
                else "task_result.md"
            ),
            mime_type="text/markdown",
            primary=True,
            required=True,
            required_sections=(
                REPOSITORY_ANALYSIS_REQUIRED_SECTIONS
                if archetype is Archetype.REPO_ANALYSIS
                else ("result", "limitations")
            ),
            # V2 currently has one canonical producer-result envelope.  The
            # deliverable kind/sections still specialize the artifact itself.
            result_schema_id="analysis_report_result_v2",
        )
        draft = TaskContractV2(
            id=f"contract_{uuid.uuid4().hex}",
            task_id=task_id,
            version=version,
            status="draft",
            title=(title or ("Repository architecture analysis" if archetype is Archetype.REPO_ANALYSIS else prompt[:200])),
            objective=prompt,
            background="",
            scope=ContractScope(include=(), exclude=(".git object bodies",), whole_task=True),
            instructions=("Analyze only the frozen repository snapshot.",),
            original_prompt_hash=_prompt_hash(prompt),
            archetype=archetype,
            language=language,
            requirements=tuple(requirements),
            constraints=tuple(constraints),
            non_goals=("Do not treat static inference as verified runtime behavior.",),
            deliverables=(deliverable,),
            quality_profile_id=quality_profile_id,
            compiler=CompilerMetadata(
                ruleset_version=CONTRACT_RULESET_VERSION,
                model_runtime_id=None,
                confidence=0.96 if archetype is Archetype.REPO_ANALYSIS else 0.85,
            ),
            content_hash="sha256:" + "0" * 64,
        )
        contract = draft.model_copy(update={"content_hash": draft.computed_content_hash()})
        result = CompilationResult(
            contract=contract,
            issues=lint_contract(contract),
            conflicts=tuple(conflicts),
            cache_hit=False,
        )
        self._cache[key] = result
        return result


def compile_contract(**kwargs: Any) -> TaskContractV2:
    result = ContractCompiler().compile(**kwargs)
    if not result.start_allowed:
        details = [item.message for item in result.issues if item.blocking]
        details.extend(item.message for item in result.conflicts)
        raise ValueError("contract compilation blocked: " + "; ".join(details))
    return result.contract
