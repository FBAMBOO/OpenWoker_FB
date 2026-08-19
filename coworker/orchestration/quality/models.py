"""Canonical immutable domain models for Task Quality Engine V2.

These models are the source of truth for JSON Schema, OpenAPI projections and
generated GUI enums.  Model-authored result payloads intentionally omit
task/run identity and read-receipt fields; the service injects those values from
the run-bound context after strict validation.
"""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from datetime import datetime
from enum import Enum
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Mapping, Sequence, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_validator,
    model_validator,
)


class _StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class QualityModel(BaseModel):
    """Strict, immutable base used by every persisted V2 value object."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )


def canonical_json(value: Any) -> bytes:
    """Serialize a model/value deterministically for content addressing."""

    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude_none=True)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def content_sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value)).hexdigest()


def model_content_sha256(model: BaseModel) -> str:
    return content_sha256(
        model.model_dump(mode="json", exclude_none=True, exclude={"content_hash"})
    )


class WorkflowStatus(_StrEnum):
    DRAFT = "draft"
    ANALYZING = "analyzing"
    NEEDS_TARGET_SELECTION = "needs_target_selection"
    READY = "ready"
    RUNNING = "running"
    VALIDATING = "validating"
    REVIEWING = "reviewing"
    REPAIRING = "repairing"
    RECOVERING = "recovering"
    NEEDS_RECONCILIATION = "needs_reconciliation"
    NEEDS_ATTENTION = "needs_attention"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    ARCHIVED = "archived"


class QualityStatus(_StrEnum):
    PENDING = "pending"
    CHECKING = "checking"
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"
    WAIVED = "waived"


class ArtifactStatus(_StrEnum):
    NONE = "none"
    UPLOADING = "uploading"
    DRAFT = "draft"
    VALIDATING = "validating"
    VERIFIED = "verified"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


class BudgetStatus(_StrEnum):
    UNCONFIGURED = "unconfigured"
    WITHIN_BUDGET = "within_budget"
    WARNING = "warning"
    EXHAUSTED = "exhausted"
    OVER_BUDGET = "over_budget"
    UNLIMITED = "unlimited"


class ContractStatus(_StrEnum):
    DRAFT = "draft"
    PUBLISHED = "published"
    SUPERSEDED = "superseded"


class Archetype(_StrEnum):
    REPO_ANALYSIS = "repo_analysis"
    CODE_CHANGE = "code_change"
    FOCUSED_QUESTION = "focused_question"
    DOCUMENT_GENERATION = "document_generation"
    INCIDENT_TRIAGE = "incident_triage"
    CUSTOM = "custom"


class RequirementCategory(_StrEnum):
    SCOPE = "scope"
    COVERAGE = "coverage"
    RELATIONSHIP = "relationship"
    EVIDENCE = "evidence"
    CURRENTNESS = "currentness"
    FORMAT = "format"
    SAFETY = "safety"
    LIMITATION = "limitation"
    PERFORMANCE = "performance"


class RequirementSource(_StrEnum):
    EXPLICIT_UI = "explicit_ui"
    EXPLICIT_PROMPT = "explicit_prompt"
    USER_CUSTOM = "user_custom"
    ARCHETYPE = "archetype"
    POLICY = "policy"
    INFERRED = "inferred"


class VerificationMethod(_StrEnum):
    ARTIFACT_EXISTS = "artifact_exists"
    COVERAGE = "coverage"
    CITATION = "citation"
    CLAIM_SUPPORT = "claim_support"
    INVENTORY_RECONCILE = "inventory_reconcile"
    WORKSPACE_UNCHANGED = "workspace_unchanged"
    SEMANTIC_RUBRIC = "semantic_rubric"
    MANUAL = "manual"


class ConstraintEnforcement(_StrEnum):
    PERMISSION = "permission"
    SANDBOX = "sandbox"
    VALIDATOR = "validator"
    INSTRUCTION = "instruction"


class SourceSpan(QualityModel):
    """Zero-based, half-open byte range in NFC-normalized UTF-8 prompt bytes."""

    start_byte: int = Field(ge=0)
    end_byte: int = Field(gt=0)
    text_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _ordered(self) -> "SourceSpan":
        if self.end_byte <= self.start_byte:
            raise ValueError("source span end_byte must be greater than start_byte")
        return self


class ContractScope(QualityModel):
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    whole_task: bool = True


class CompilerMetadata(QualityModel):
    ruleset_version: str
    model_runtime_id: str | None = None
    confidence: float = Field(ge=0, le=1)


class Requirement(QualityModel):
    id: str = Field(min_length=1, max_length=160)
    category: RequirementCategory
    text: str = Field(min_length=1, max_length=8_192)
    required: bool = True
    hard_gate: bool = False
    source: RequirementSource
    source_span: SourceSpan | None = None
    confidence: float | None = Field(default=None, ge=0, le=1)
    verification_method: VerificationMethod
    verification_spec: Mapping[str, Any] = Field(default_factory=dict)
    waivable: bool = False

    @model_validator(mode="after")
    def _inference_has_confidence(self) -> "Requirement":
        if self.source is RequirementSource.INFERRED and self.confidence is None:
            raise ValueError("inferred requirements require confidence")
        return self


class Constraint(QualityModel):
    id: str = Field(min_length=1, max_length=160)
    type: str = Field(min_length=1, max_length=160)
    text: str = Field(min_length=1, max_length=8_192)
    enforcement: ConstraintEnforcement
    source: RequirementSource
    hard: bool = True
    verification_method: VerificationMethod
    value: bool | str | int | float | None = None


class DeliverableSpec(QualityModel):
    id: str = Field(min_length=1, max_length=160)
    kind: str = Field(min_length=1, max_length=160)
    filename: str = Field(min_length=1, max_length=255)
    mime_type: str = Field(min_length=1, max_length=255)
    channel: Literal["task_artifact_store"] = "task_artifact_store"
    required: bool = True
    primary: bool = False
    required_sections: tuple[str, ...] = ()
    result_schema_id: str = Field(min_length=1, max_length=160)

    @field_validator("filename")
    @classmethod
    def _safe_filename(cls, value: str) -> str:
        if value in {".", ".."} or "/" in value or "\\" in value or "\x00" in value:
            raise ValueError("deliverable filename must be a safe basename")
        return value


class TaskContractV2(QualityModel):
    schema_id: Literal["task_contract_v2"] = "task_contract_v2"
    schema_version: Literal[2] = 2
    id: str = Field(min_length=1, max_length=160)
    task_id: str = Field(min_length=1, max_length=160)
    version: int = Field(ge=1)
    status: ContractStatus = ContractStatus.DRAFT
    title: str = Field(min_length=1, max_length=512)
    objective: str = Field(min_length=1, max_length=131_072)
    background: str = Field(default="", max_length=131_072)
    scope: ContractScope = Field(default_factory=ContractScope)
    instructions: tuple[str, ...] = ()
    original_prompt_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    archetype: Archetype
    language: str = Field(default="en", min_length=2, max_length=35)
    requirements: tuple[Requirement, ...]
    constraints: tuple[Constraint, ...]
    non_goals: tuple[str, ...] = ()
    deliverables: tuple[DeliverableSpec, ...]
    quality_profile_id: str = Field(min_length=1, max_length=255)
    compiler: CompilerMetadata
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _unique_and_publishable_shape(self) -> "TaskContractV2":
        requirement_ids = [item.id for item in self.requirements]
        if len(requirement_ids) != len(set(requirement_ids)):
            raise ValueError("requirement ids must be unique within a contract")
        constraint_ids = [item.id for item in self.constraints]
        if len(constraint_ids) != len(set(constraint_ids)):
            raise ValueError("constraint ids must be unique within a contract")
        deliverable_ids = [item.id for item in self.deliverables]
        if len(deliverable_ids) != len(set(deliverable_ids)):
            raise ValueError("deliverable ids must be unique within a contract")
        primary = [item for item in self.deliverables if item.primary]
        if len(primary) != 1:
            raise ValueError("a contract must declare exactly one primary deliverable")
        return self

    def computed_content_hash(self) -> str:
        # Lifecycle status is projection metadata, not part of the immutable
        # semantic contract body. Publishing must not invalidate its ETag/hash.
        return content_sha256(
            self.model_dump(
                mode="json",
                exclude_none=True,
                exclude={"content_hash", "status"},
            )
        )

    def verify_content_hash(self) -> None:
        observed = self.computed_content_hash()
        if self.content_hash != observed:
            raise ValueError(
                f"contract content hash mismatch: expected {self.content_hash}, observed {observed}"
            )


class SnapshotKind(_StrEnum):
    COMMIT = "commit"
    WORKING_TREE = "working_tree"
    DIRECTORY = "directory"


class VcsType(_StrEnum):
    GIT = "git"
    NONE = "none"


class VcsObjectFormat(_StrEnum):
    SHA1 = "sha1"
    SHA256 = "sha256"


class RepositorySnapshot(QualityModel):
    schema_id: Literal["repository_snapshot_v2"] = "repository_snapshot_v2"
    schema_version: Literal[2] = 2
    id: str
    task_id: str
    version: int = Field(default=1, ge=1)
    workspace_root: str
    repo_root: str
    project_root: str = "."
    vcs_type: VcsType
    snapshot_kind: SnapshotKind
    selected_ref: str | None = None
    vcs_object_format: VcsObjectFormat | None = None
    commit_oid: str | None = None
    base_tree_oid: str | None = None
    head_oid: str | None = None
    current_branch: str | None = None
    default_ref: str | None = None
    upstream_ref: str | None = None
    ahead: int | None = Field(default=None, ge=0)
    behind: int | None = Field(default=None, ge=0)
    dirty: bool = False
    worktree_count: int = Field(default=1, ge=0)
    duplicate_roots: tuple[str, ...] = ()
    ignore_rules_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    manifest_artifact_id: str
    manifest_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    overlay_artifact_id: str | None = None
    overlay_hash: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    directory_pack_artifact_id: str | None = None
    directory_pack_hash: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    resolution_confidence: float = Field(ge=0, le=1)
    resolution_reason: str = Field(min_length=1, max_length=8_192)
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    created_at: datetime

    @model_validator(mode="after")
    def _snapshot_shape(self) -> "RepositorySnapshot":
        if self.vcs_type is VcsType.GIT:
            if self.vcs_object_format is None or not self.commit_oid or not self.base_tree_oid:
                raise ValueError("git snapshots require object format, commit_oid and base_tree_oid")
            expected = 40 if self.vcs_object_format is VcsObjectFormat.SHA1 else 64
            for name, value in (
                ("commit_oid", self.commit_oid),
                ("base_tree_oid", self.base_tree_oid),
                ("head_oid", self.head_oid),
            ):
                if value is not None and (
                    len(value) != expected or any(ch not in "0123456789abcdef" for ch in value)
                ):
                    raise ValueError(f"{name} does not match {self.vcs_object_format.value}")
        if self.snapshot_kind is SnapshotKind.WORKING_TREE and (
            not self.overlay_artifact_id or not self.overlay_hash
        ):
            raise ValueError("working-tree snapshots require immutable overlay data")
        if self.snapshot_kind is SnapshotKind.DIRECTORY and (
            not self.directory_pack_artifact_id or not self.directory_pack_hash
        ):
            raise ValueError("directory snapshots require an immutable directory pack")
        return self


class Assessment(QualityModel):
    cognitive_complexity: int = Field(ge=0, le=100)
    operational_risk: int = Field(ge=0, le=100)
    evidence_workload: int = Field(ge=0, le=100)
    rationale: tuple[str, ...] = ()


class BindingSourceType(_StrEnum):
    CONTRACT = "contract"
    SNAPSHOT = "snapshot"
    INVENTORY = "inventory"
    EVIDENCE_BUNDLE = "evidence_bundle"
    ARTIFACT = "artifact"
    FINDING_SET = "finding_set"


class BindingRequirement(_StrEnum):
    REQUIRED = "required"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"


class BindingDeliveryMode(_StrEnum):
    INLINE_METADATA = "inline_metadata"
    ON_DEMAND = "on_demand"
    MOUNTED_READONLY = "mounted_readonly"


class NodeInputBinding(QualityModel):
    consumer_node_key: str
    source_type: BindingSourceType
    source_selector: Mapping[str, Any]
    requirement: BindingRequirement = BindingRequirement.REQUIRED
    delivery_mode: BindingDeliveryMode = BindingDeliveryMode.ON_DEMAND
    max_bytes: int = Field(default=262_144, ge=1)
    must_verify_hash: bool = True


class StrategyNode(QualityModel):
    key: str
    role: str
    kind: str
    coverage_group: str | None = None
    deterministic: bool = False
    config: Mapping[str, Any] = Field(default_factory=dict)


class StrategyEdge(QualityModel):
    source: str
    target: str
    condition: str = "success"


class ExecutionStrategy(QualityModel):
    schema_id: Literal["execution_strategy_v2"] = "execution_strategy_v2"
    schema_version: Literal[2] = 2
    id: str
    task_id: str
    version: int = Field(ge=1)
    archetype: Archetype
    template_id: str
    contract_id: str
    snapshot_id: str
    rubric_id: str
    assessment: Assessment
    effective_policy: Mapping[str, Any]
    feature_flags: Mapping[str, str | bool]
    nodes: tuple[StrategyNode, ...]
    edges: tuple[StrategyEdge, ...]
    input_bindings: tuple[NodeInputBinding, ...]
    semantic_scorer_node_key: str
    budget_profile: Mapping[str, Any]
    max_repair_attempts: int = Field(default=2, ge=0, le=2)
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    created_at: datetime | None = None

    @model_validator(mode="after")
    def _strategy_references(self) -> "ExecutionStrategy":
        keys = [node.key for node in self.nodes]
        if len(keys) != len(set(keys)):
            raise ValueError("strategy node keys must be unique")
        known = set(keys)
        if self.semantic_scorer_node_key not in known:
            raise ValueError("semantic scorer must reference exactly one strategy node")
        for edge in self.edges:
            if edge.source not in known or edge.target not in known or edge.source == edge.target:
                raise ValueError("strategy edge references an invalid node")
        for binding in self.input_bindings:
            if binding.consumer_node_key not in known:
                raise ValueError("input binding consumer is not a strategy node")
        return self


class ClaimType(_StrEnum):
    FACT = "fact"
    INFERENCE = "inference"
    ABSENCE = "absence"
    RISK = "risk"
    RECOMMENDATION = "recommendation"
    LIMITATION = "limitation"


class Severity(_StrEnum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ClaimStatus(_StrEnum):
    DRAFT = "draft"
    VERIFIED = "verified"
    DISPUTED = "disputed"
    SUPERSEDED = "superseded"


class Claim(QualityModel):
    id: str
    task_id: str
    artifact_id: str
    artifact_version: int = Field(ge=1)
    section_id: str
    text: str
    claim_type: ClaimType
    severity: Severity
    confidence: float = Field(ge=0, le=1)
    requirement_ids: tuple[str, ...] = ()
    status: ClaimStatus = ClaimStatus.DRAFT


class EvidenceSupport(_StrEnum):
    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"
    CONTEXT = "context"


def validate_repo_relative_path(value: str) -> str:
    value = unicodedata.normalize("NFC", value)
    if not value or "\x00" in value or "\\" in value:
        raise ValueError("path must be a normalized repository-relative POSIX path")
    if value.startswith("/") or value.startswith("//"):
        raise ValueError("absolute and UNC paths are forbidden")
    if len(value) >= 2 and value[1] == ":":
        raise ValueError("drive-qualified and drive-relative paths are forbidden")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("path traversal is forbidden")
    return path.as_posix()


class EvidenceRef(QualityModel):
    id: str
    claim_id: str
    snapshot_id: str
    path: str
    line_start: int = Field(ge=1)
    line_end: int = Field(ge=1)
    blob_hash: str
    excerpt_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    evidence_type: str = "file_range"
    support: EvidenceSupport
    created_by_run_id: str
    git_blob_oid: str | None = None
    content_withheld: bool = False

    @field_validator("path")
    @classmethod
    def _path_is_safe(cls, value: str) -> str:
        return validate_repo_relative_path(value)

    @model_validator(mode="after")
    def _line_range(self) -> "EvidenceRef":
        if self.line_end < self.line_start:
            raise ValueError("line_end cannot precede line_start")
        return self


class NegativeEvidence(QualityModel):
    id: str
    claim_id: str
    query: str
    tool_version: str
    scope_paths: tuple[str, ...]
    excluded_paths: tuple[str, ...] = ()
    result_count: int = Field(ge=0)
    query_result_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    limitations: tuple[str, ...]

    @field_validator("scope_paths", "excluded_paths")
    @classmethod
    def _safe_paths(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(validate_repo_relative_path(value) for value in values)


class TriState(_StrEnum):
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"


class CoverageResult(QualityModel):
    requirement_id: str
    area: str
    status: TriState
    claim_ids: tuple[str, ...] = ()
    evidence_count: int = Field(default=0, ge=0)
    notes: str = ""
    validator_id: str


class InventoryMetric(QualityModel):
    id: str
    name: str
    value: int | float
    unit: str
    query_key: str
    subtotals: Mapping[str, int | float] = Field(default_factory=dict)
    reconciles_to: int | float | None = None
    tolerance: float = Field(default=0, ge=0)


class RepositoryInventory(QualityModel):
    id: str
    snapshot_id: str
    tool_version: str
    artifact_id: str
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    file_count: int = Field(ge=0)
    total_bytes: int = Field(ge=0)
    project_markers: tuple[str, ...] = ()
    generated_at: datetime


class ArtifactVersionStatus(_StrEnum):
    UPLOADING = "uploading"
    DRAFT = "draft"
    VALIDATING = "validating"
    VERIFIED = "verified"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


class ArtifactVersion(QualityModel):
    id: str
    logical_deliverable_id: str
    task_id: str
    run_id: str | None = None
    attempt: int = Field(default=1, ge=1)
    version: int = Field(ge=1)
    filename: str
    mime_type: str
    blob_uri: str | None = None
    sha256: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    byte_size: int | None = Field(default=None, ge=0)
    section_index_artifact_id: str | None = None
    chunk_manifest_artifact_id: str | None = None
    status: ArtifactVersionStatus
    producer_profile_id: str | None = None
    parent_artifact_id: str | None = None
    created_at: datetime
    finalized_at: datetime | None = None


class ByteRange(QualityModel):
    start_byte: int = Field(ge=0)
    end_byte: int = Field(gt=0)

    @model_validator(mode="after")
    def _ordered(self) -> "ByteRange":
        if self.end_byte <= self.start_byte:
            raise ValueError("byte range must be non-empty")
        return self


class ArtifactReadReceipt(QualityModel):
    id: str
    verifier_profile_id: str
    run_id: str
    artifact_id: str
    artifact_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    ranges: tuple[ByteRange, ...]
    covered_bytes: int = Field(ge=0)
    coverage_ratio: float = Field(ge=0, le=1)
    candidate_bound_at: datetime
    completed_at: datetime | None = None


class GateSubjectType(_StrEnum):
    HARD_GATE = "hard_gate"
    CRITERION = "criterion"
    FINDING = "finding"
    SEMANTIC_SCORE = "semantic_score"
    SOFT_BUDGET = "soft_budget"


class GateResult(QualityModel):
    id: str
    task_id: str
    artifact_id: str
    artifact_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    subject_type: GateSubjectType
    subject_id: str
    subject_version: int = Field(ge=1)
    status: TriState
    waivable: bool
    reason_code: str
    evidence_ids: tuple[str, ...] = ()
    validator_id: str
    created_at: datetime


class FindingCategory(_StrEnum):
    BASELINE = "baseline"
    COVERAGE = "coverage"
    CITATION = "citation"
    SUPPORT = "support"
    CONSISTENCY = "consistency"
    SCHEMA = "schema"
    SECURITY = "security"
    BUDGET = "budget"
    STYLE = "style"
    LIMITATION = "limitation"


class FindingStatus(_StrEnum):
    OPEN = "open"
    REPAIRING = "repairing"
    RESOLVED = "resolved"
    DISMISSED = "dismissed"


class Finding(QualityModel):
    id: str
    fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    task_id: str
    artifact_id: str
    artifact_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    category: FindingCategory
    severity: Severity
    blocking: bool
    repairable: bool
    requirement_id: str | None = None
    claim_id: str | None = None
    section_id: str | None = None
    message: str = Field(min_length=1, max_length=16_384)
    evidence_refs: tuple[str, ...] = ()
    suggested_fix: str | None = None
    status: FindingStatus = FindingStatus.OPEN
    supersedes_finding_id: str | None = None
    created_at: datetime | None = None

    @model_validator(mode="after")
    def _has_locator(self) -> "Finding":
        if not (self.requirement_id or self.claim_id or self.section_id):
            raise ValueError("a finding requires a requirement, claim, or section locator")
        return self


def finding_fingerprint(
    category: FindingCategory | str,
    subject: str,
    message: str,
) -> str:
    normalized = " ".join(unicodedata.normalize("NFC", message).casefold().split())
    return content_sha256(
        {"category": str(category), "subject": subject, "message": normalized}
    )


class RepairRequestStatus(_StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    EXHAUSTED = "exhausted"


class RepairRequest(QualityModel):
    id: str
    task_id: str
    source_artifact_id: str
    target_version: int = Field(ge=2)
    finding_ids: tuple[str, ...] = Field(min_length=1)
    allowed_sections: tuple[str, ...] = ()
    required_validators: tuple[str, ...] = ()
    budget_allocation: Mapping[str, int] = Field(default_factory=dict)
    attempt: int = Field(ge=1, le=2)
    status: RepairRequestStatus = RepairRequestStatus.PENDING
    result_artifact_id: str | None = None


class WaiverSubjectType(_StrEnum):
    GATE_RESULT = "gate_result"
    CRITERION = "criterion"
    FINDING = "finding"
    SEMANTIC_SCORE = "semantic_score"
    SOFT_BUDGET = "soft_budget"


class QualityWaiver(QualityModel):
    id: str
    task_id: str
    artifact_id: str
    artifact_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    contract_id: str
    contract_version: int = Field(ge=1)
    subject_type: WaiverSubjectType
    subject_id: str
    subject_version: int = Field(ge=1)
    rubric_id: str | None = None
    rubric_version: int | None = Field(default=None, ge=1)
    actor_id: str
    reason: str
    reference: str | None = None
    expires_at: datetime | None = None
    revoked_at: datetime | None = None
    created_at: datetime
    signature_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class RubricDimension(QualityModel):
    id: str
    title: str
    max_points: int = Field(ge=1, le=100)
    instructions: str
    anchors: Mapping[str, str] = Field(default_factory=dict)
    required_evidence_types: tuple[str, ...] = ()


class QualityRubric(QualityModel):
    id: str
    version: int = Field(ge=1)
    name: str
    applicable_archetypes: tuple[Archetype, ...]
    dimensions: tuple[RubricDimension, ...]
    pass_threshold: int = Field(ge=0, le=100)
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _totals_one_hundred(self) -> "QualityRubric":
        ids = [item.id for item in self.dimensions]
        if len(ids) != len(set(ids)):
            raise ValueError("rubric dimension ids must be unique")
        if sum(item.max_points for item in self.dimensions) != 100:
            raise ValueError("rubric max_points must total exactly 100")
        return self


class RubricDimensionScore(QualityModel):
    dimension_id: str
    points: int = Field(ge=0, le=100)
    rationale: str
    evidence_ids: tuple[str, ...] = ()
    finding_ids: tuple[str, ...] = ()


class RubricScore(QualityModel):
    id: str
    rubric_id: str
    rubric_version: int = Field(ge=1)
    artifact_id: str
    artifact_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    scorer_run_id: str
    dimension_scores: tuple[RubricDimensionScore, ...]
    total: int = Field(ge=0, le=100)
    created_at: datetime


class EvaluationType(_StrEnum):
    DETERMINISTIC = "deterministic"
    SEMANTIC = "semantic"
    REVIEW = "review"
    FINAL = "final"


class QualityEvaluation(QualityModel):
    id: str
    task_id: str
    artifact_id: str
    artifact_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    evaluation_type: EvaluationType
    validator_id: str
    validator_version: str
    rubric_id: str | None = None
    rubric_version: int | None = Field(default=None, ge=1)
    criterion_results: tuple[Mapping[str, Any], ...] = ()
    coverage_results: tuple[CoverageResult, ...] = ()
    rubric_score_id: str | None = None
    total_score: int | None = Field(default=None, ge=0, le=100)
    verdict: TriState
    read_receipt_id: str | None = None
    finding_ids: tuple[str, ...] = ()
    created_by_run_id: str | None = None
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    created_at: datetime


class BudgetMode(_StrEnum):
    HARD = "hard"
    SOFT = "soft"
    UNLIMITED = "unlimited"


BUDGET_DIMENSIONS = (
    "model_calls",
    "tool_calls",
    "reported_tokens",
    "active_seconds",
    "tool_payload_bytes",
)


class BudgetLimits(QualityModel):
    model_calls: int | None = Field(default=None, ge=0)
    tool_calls: int | None = Field(default=None, ge=0)
    reported_tokens: int | None = Field(default=None, ge=0)
    active_seconds: int | None = Field(default=None, ge=0)
    tool_payload_bytes: int | None = Field(default=None, ge=0)


class BudgetProfile(QualityModel):
    id: str
    mode: BudgetMode
    limits: BudgetLimits = Field(default_factory=BudgetLimits)
    warning_thresholds: tuple[float, ...] = (0.8, 0.95)
    node_allocations: Mapping[str, Mapping[str, int]] = Field(default_factory=dict)

    @field_validator("warning_thresholds")
    @classmethod
    def _thresholds(cls, values: tuple[float, ...]) -> tuple[float, ...]:
        if any(not math.isfinite(value) or value <= 0 or value >= 1 for value in values):
            raise ValueError("warning thresholds must be finite values between 0 and 1")
        if tuple(sorted(set(values))) != values:
            raise ValueError("warning thresholds must be unique and increasing")
        return values

    @model_validator(mode="after")
    def _unlimited_has_no_business_limits(self) -> "BudgetProfile":
        if self.mode is BudgetMode.UNLIMITED and any(
            getattr(self.limits, dimension) is not None for dimension in BUDGET_DIMENSIONS
        ):
            raise ValueError("unlimited profiles must not expose misleading business limits")
        if self.mode is not BudgetMode.UNLIMITED and any(
            getattr(self.limits, dimension) is None for dimension in BUDGET_DIMENSIONS
        ):
            raise ValueError("finite budget profiles require every canonical limit")
        return self


class BudgetLedger(QualityModel):
    id: str
    task_id: str
    strategy_id: str
    mode: BudgetMode
    source_profile_id: str
    effective_limits: BudgetLimits
    reserved: Mapping[str, int]
    consumed: Mapping[str, int]
    remaining: Mapping[str, int | None]
    provider_usage_semantics: Mapping[str, Any]
    over_budget: bool
    version: int = Field(ge=1)
    fencing_token: int = Field(ge=0)


class PrimaryArtifactRef(QualityModel):
    artifact_id: str
    sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    filename: str
    mime_type: str
    byte_size: int = Field(ge=0)


class RequirementClaimStatus(_StrEnum):
    ADDRESSED = "addressed"
    NOT_ADDRESSED = "not_addressed"
    UNKNOWN = "unknown"


class RequirementClaim(QualityModel):
    requirement_id: str
    claimed_status: RequirementClaimStatus
    evidence_ids: tuple[str, ...] = ()


class CriterionResultInput(QualityModel):
    requirement_id: str
    status: TriState
    rationale: str
    evidence_ids: tuple[str, ...] = ()


class ResultCheckpoint(QualityModel):
    artifact_id: str
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    resume_cursor: str


class ResultError(QualityModel):
    code: str
    message: str
    retryable: bool


class FindingInput(QualityModel):
    category: FindingCategory
    severity: Severity
    blocking: bool
    repairable: bool
    requirement_id: str | None = None
    claim_id: str | None = None
    section_id: str | None = None
    message: str
    evidence_refs: tuple[str, ...] = ()
    suggested_fix: str | None = None

    @model_validator(mode="after")
    def _locator(self) -> "FindingInput":
        if not (self.requirement_id or self.claim_id or self.section_id):
            raise ValueError("finding input requires a subject locator")
        return self


class ModelResultBase(QualityModel):
    schema_id: str
    schema_version: Literal[2] = 2
    summary: str = Field(max_length=2_048)


class EvidenceBundleCompleted(ModelResultBase):
    schema_id: Literal["evidence_bundle_result_v2"] = "evidence_bundle_result_v2"
    execution_status: Literal["completed"]
    coverage_group: str
    claim_ids: tuple[str, ...]
    evidence_ref_ids: tuple[str, ...]
    inventory_metric_ids: tuple[str, ...]
    negative_search_ids: tuple[str, ...]
    open_questions: tuple[str, ...]
    limitations: tuple[str, ...]


class EvidenceBundlePartial(ModelResultBase):
    schema_id: Literal["evidence_bundle_result_v2"] = "evidence_bundle_result_v2"
    execution_status: Literal["partial"]
    checkpoint: ResultCheckpoint
    incomplete_reasons: tuple[str, ...] = Field(min_length=1)
    provisional_artifact_ids: tuple[str, ...] = ()


class EvidenceBundleFailed(ModelResultBase):
    schema_id: Literal["evidence_bundle_result_v2"] = "evidence_bundle_result_v2"
    execution_status: Literal["failed"]
    error: ResultError
    diagnostic_artifact_ids: tuple[str, ...] = ()


class EvidenceBundleResult(RootModel[
    Annotated[
        EvidenceBundleCompleted | EvidenceBundlePartial | EvidenceBundleFailed,
        Field(discriminator="execution_status"),
    ]
]):
    pass


class AnalysisReportCompleted(ModelResultBase):
    schema_id: Literal["analysis_report_result_v2"] = "analysis_report_result_v2"
    execution_status: Literal["completed"]
    primary_artifact: PrimaryArtifactRef
    requirement_claims: tuple[RequirementClaim, ...]
    coverage_claims: tuple[Mapping[str, Any], ...]
    claim_ledger_id: str
    risks: tuple[Mapping[str, Any], ...]
    limitations: tuple[str, ...]
    source_workspace_changes: tuple[Mapping[str, Any], ...]


class AnalysisReportPartial(ModelResultBase):
    schema_id: Literal["analysis_report_result_v2"] = "analysis_report_result_v2"
    execution_status: Literal["partial"]
    checkpoint: ResultCheckpoint
    incomplete_reasons: tuple[str, ...] = Field(min_length=1)
    provisional_artifact_ids: tuple[str, ...] = ()


class AnalysisReportFailed(ModelResultBase):
    schema_id: Literal["analysis_report_result_v2"] = "analysis_report_result_v2"
    execution_status: Literal["failed"]
    error: ResultError
    diagnostic_artifact_ids: tuple[str, ...] = ()


class AnalysisReportResult(RootModel[
    Annotated[
        AnalysisReportCompleted | AnalysisReportPartial | AnalysisReportFailed,
        Field(discriminator="execution_status"),
    ]
]):
    pass


class ReviewCompleted(ModelResultBase):
    schema_id: Literal["review_result_v2"] = "review_result_v2"
    execution_status: Literal["completed"]
    subject_artifact_id: str
    subject_artifact_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    criterion_results: tuple[CriterionResultInput, ...]
    findings: tuple[FindingInput, ...]
    verdict: TriState
    rubric_dimension_scores: tuple[RubricDimensionScore, ...] | None = None


class ReviewPartial(ModelResultBase):
    schema_id: Literal["review_result_v2"] = "review_result_v2"
    execution_status: Literal["partial"]
    checkpoint: ResultCheckpoint
    incomplete_reasons: tuple[str, ...] = Field(min_length=1)
    provisional_artifact_ids: tuple[str, ...] = ()


class ReviewFailed(ModelResultBase):
    schema_id: Literal["review_result_v2"] = "review_result_v2"
    execution_status: Literal["failed"]
    error: ResultError
    diagnostic_artifact_ids: tuple[str, ...] = ()


class ReviewResult(RootModel[
    Annotated[
        ReviewCompleted | ReviewPartial | ReviewFailed,
        Field(discriminator="execution_status"),
    ]
]):
    pass


class FinalDecision(_StrEnum):
    PUBLISH = "publish"
    REPAIR = "repair"
    NEEDS_ATTENTION = "needs_attention"
    REJECT = "reject"


class FinalQualityDecisionCompleted(ModelResultBase):
    schema_id: Literal["final_quality_decision_v2"] = "final_quality_decision_v2"
    execution_status: Literal["completed"]
    subject_artifact_id: str
    subject_artifact_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    hard_gate_results: tuple[str, ...]
    open_blocking_finding_ids: tuple[str, ...]
    criterion_results: tuple[str, ...]
    rubric_score_id: str
    decision: FinalDecision


class FinalQualityDecisionPartial(ModelResultBase):
    schema_id: Literal["final_quality_decision_v2"] = "final_quality_decision_v2"
    execution_status: Literal["partial"]
    checkpoint: ResultCheckpoint
    incomplete_reasons: tuple[str, ...] = Field(min_length=1)
    provisional_artifact_ids: tuple[str, ...] = ()


class FinalQualityDecisionFailed(ModelResultBase):
    schema_id: Literal["final_quality_decision_v2"] = "final_quality_decision_v2"
    execution_status: Literal["failed"]
    error: ResultError
    diagnostic_artifact_ids: tuple[str, ...] = ()


class FinalQualityDecisionResult(RootModel[
    Annotated[
        FinalQualityDecisionCompleted
        | FinalQualityDecisionPartial
        | FinalQualityDecisionFailed,
        Field(discriminator="execution_status"),
    ]
]):
    pass


RoleResult: TypeAlias = (
    EvidenceBundleResult | AnalysisReportResult | ReviewResult | FinalQualityDecisionResult
)


class BoundResultEnvelope(QualityModel):
    """Persisted identity envelope created only by settlement code."""

    schema_id: str
    schema_version: Literal[2] = 2
    task_id: str
    run_id: str
    contract_id: str
    snapshot_id: str
    execution_status: Literal["completed", "partial", "failed"]
    summary: str = Field(max_length=2_048)
    payload: Mapping[str, Any]


CANONICAL_STATUS_ENUMS: Mapping[str, type[_StrEnum]] = {
    "workflow_status": WorkflowStatus,
    "quality_status": QualityStatus,
    "artifact_status": ArtifactStatus,
    "budget_status": BudgetStatus,
}


def canonical_status_values() -> dict[str, list[str]]:
    return {
        name: [member.value for member in enum]
        for name, enum in CANONICAL_STATUS_ENUMS.items()
    }
