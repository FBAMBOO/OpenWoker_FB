"""Domain types for the Task-Centric Handoff Protocol (TCHP).

The records in this module are deliberately detached from the mutable scheduler
aggregates.  Published briefs, context references, comments, relations and work
products are durable communication facts; runs and wakes merely consume them.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional, Sequence


_SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(
        r"(?i)\b(?:api[_-]?key|access[_-]?token|client[_-]?secret|password)"
        r"\s*[\"']?\s*[:=]\s*[\"']?[A-Za-z0-9_./+\-=]{12,}"
    ),
    re.compile(r"\bghp_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
)


class _StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class BriefStatus(_StrEnum):
    DRAFT = "draft"
    PUBLISHED = "published"
    SUPERSEDED = "superseded"


class ContextRequirement(_StrEnum):
    REQUIRED = "required"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"


class ContextRefType(_StrEnum):
    FILE = "file"
    FILE_RANGE = "file_range"
    ARTIFACT = "artifact"
    TASK_OUTPUT = "task_output"
    WORK_PRODUCT = "work_product"
    TASK_COMMENT = "task_comment"
    EVENT_RANGE = "event_range"
    URL = "url"
    WORKSPACE_QUERY = "workspace_query"
    GIT_DIFF = "git_diff"


class ContextDeliveryMode(_StrEnum):
    METADATA_ONLY = "metadata_only"
    EXCERPT = "excerpt"
    ON_DEMAND = "on_demand"


class TaskRelationType(_StrEnum):
    PARENT = "parent"
    BLOCKS = "blocks"
    REVIEWS = "reviews"
    RELATED = "related"
    SUPERSEDES = "supersedes"


class WakeReason(_StrEnum):
    ASSIGNMENT = "assignment"
    TASK_ASSIGNED = "task_assigned"
    TASK_COMMENTED = "task_commented"
    TASK_COMMENT_MENTIONED = "task_comment_mentioned"
    TASK_CHILDREN_COMPLETED = "task_children_completed"
    TASK_BLOCKERS_RESOLVED = "task_blockers_resolved"
    GATE_RESOLVED = "gate_resolved"
    RETRY_REQUESTED = "retry_requested"
    REPLAN_REQUESTED = "replan_requested"
    MANUAL_RESUME = "manual_resume"
    LEASE_RECOVERED = "lease_recovered"
    BRIEF_REVISION_AVAILABLE = "brief_revision_available"
    REVIEW_ASSIGNED = "review_assigned"


class WakeStatus(_StrEnum):
    PENDING = "pending"
    DEFERRED = "deferred"
    CLAIMED = "claimed"
    DELIVERED = "delivered"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class WorkProductKind(_StrEnum):
    PLAN = "plan"
    PROGRESS_REPORT = "progress_report"
    IMPLEMENTATION_PATCH = "implementation_patch"
    PULL_REQUEST = "pull_request"
    COMMIT = "commit"
    BRANCH = "branch"
    WORKSPACE_FILE = "workspace_file"
    ARTIFACT = "artifact"
    TEST_RESULT = "test_result"
    REVIEW_REPORT = "review_report"
    EVALUATION = "evaluation"
    PREVIEW_URL = "preview_url"
    RUNTIME_SERVICE = "runtime_service"
    OTHER = "other"


class HandoffValidationError(ValueError):
    """A handoff contract is invalid, with stable field-addressable details."""

    def __init__(self, issues: Sequence[Mapping[str, Any]]) -> None:
        self.issues = tuple(dict(issue) for issue in issues)
        super().__init__("; ".join(str(issue.get("message") or "invalid handoff") for issue in self.issues))


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return tuple(sorted((_freeze(item) for item in value), key=repr))
    if isinstance(value, float) and not math.isfinite(value):
        raise HandoffValidationError(({"path": "$", "code": "non_finite", "message": "handoff data cannot contain NaN or infinity"},))
    if value is None or isinstance(value, (str, int, float, bool, datetime, Enum)):
        return value
    if is_dataclass(value):
        return _freeze({item.name: getattr(value, item.name) for item in fields(value)})
    raise HandoffValidationError(({"path": "$", "code": "invalid_type", "message": f"handoff data must be JSON-compatible, got {type(value).__name__}"},))


def jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if is_dataclass(value):
        return {
            item.name: jsonable(getattr(value, item.name)) for item in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [jsonable(item) for item in value]
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def contains_secret_like(value: Any) -> bool:
    """Conservatively detect credential material in handoff-visible metadata."""

    text = value if isinstance(value, str) else canonical_json(value)
    return any(pattern.search(text) for pattern in _SECRET_PATTERNS)


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _strings(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(item for item in (str(value).strip() for value in values) if item)


def _maps(values: Iterable[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    return tuple(_freeze(dict(value)) for value in values)


@dataclass(frozen=True, slots=True)
class TaskBriefDraft:
    title: str
    objective: str
    background: str = ""
    scope: Mapping[str, Any] = field(default_factory=dict, hash=False)
    instructions: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    non_goals: tuple[str, ...] = ()
    acceptance_criteria: tuple[Mapping[str, Any], ...] = field(default_factory=tuple, hash=False)
    deliverables: tuple[Mapping[str, Any], ...] = field(default_factory=tuple, hash=False)
    result_contract: Mapping[str, Any] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        title = str(self.title).strip()
        objective = str(self.objective).strip()
        if len(title) > 200:
            raise HandoffValidationError(({"path": "title", "code": "too_long", "message": "title must be at most 200 characters"},))
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "objective", objective)
        object.__setattr__(self, "background", str(self.background).strip())
        object.__setattr__(self, "scope", _freeze(dict(self.scope)))
        object.__setattr__(self, "instructions", _strings(self.instructions))
        object.__setattr__(self, "constraints", _strings(self.constraints))
        object.__setattr__(self, "non_goals", _strings(self.non_goals))
        object.__setattr__(self, "acceptance_criteria", _maps(self.acceptance_criteria))
        object.__setattr__(self, "deliverables", _maps(self.deliverables))
        object.__setattr__(self, "result_contract", _freeze(dict(self.result_contract)))
        if contains_secret_like(
            {
                "title": self.title,
                "objective": self.objective,
                "background": self.background,
                "scope": self.scope,
                "instructions": self.instructions,
                "constraints": self.constraints,
                "non_goals": self.non_goals,
                "acceptance_criteria": self.acceptance_criteria,
                "deliverables": self.deliverables,
                "result_contract": self.result_contract,
            }
        ):
            raise HandoffValidationError(
                (
                    {
                        "path": "$",
                        "code": "secret_detected",
                        "message": "task Brief cannot contain secret-like values",
                    },
                )
            )
        if len(canonical_json(self.to_dict()).encode("utf-8")) > 262_144:
            raise HandoffValidationError(
                (
                    {
                        "path": "$",
                        "code": "too_large",
                        "message": "task Brief must be at most 256 KiB",
                    },
                )
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TaskBriefDraft":
        raw_criteria = value.get("acceptance_criteria") or ()
        criteria = tuple(
            dict(item) if isinstance(item, Mapping) else {"id": f"AC-{index:02d}", "text": str(item), "required": True}
            for index, item in enumerate(raw_criteria, 1)
        )
        raw_deliverables = value.get("deliverables") or ()
        deliverables = tuple(
            dict(item) if isinstance(item, Mapping) else {"id": f"DEL-{index:02d}", "kind": str(item), "title": str(item), "required": True}
            for index, item in enumerate(raw_deliverables, 1)
        )
        return cls(
            title=str(value.get("title") or value.get("objective") or ""),
            objective=str(value.get("objective") or ""),
            background=str(value.get("background") or ""),
            scope=dict(value.get("scope") or {}),
            instructions=tuple(value.get("instructions") or ()),
            constraints=tuple(value.get("constraints") or ()),
            non_goals=tuple(value.get("non_goals") or ()),
            acceptance_criteria=criteria,
            deliverables=deliverables,
            result_contract=dict(value.get("result_contract") or {}),
        )

    def validation_issues(
        self,
        *,
        required_fields: Sequence[str] = ("objective", "scope", "acceptance_criteria", "deliverables"),
        informational: bool = False,
    ) -> tuple[dict[str, Any], ...]:
        issues: list[dict[str, Any]] = []

        def required(path: str, message: str) -> None:
            issues.append({"path": path, "code": "required", "message": message})

        wanted = set(required_fields)
        if "objective" in wanted and not self.objective:
            required("objective", "objective is required")
        if not self.title:
            required("title", "title is required")
        scope_values = self.scope.get("include") or self.scope.get("included_paths") or self.scope.get("components")
        whole_task = bool(self.scope.get("whole_task")) and bool(str(self.scope.get("reason") or "").strip())
        if "scope" in wanted and not scope_values and not whole_task:
            required("scope", "scope must include bounded targets or a justified whole_task declaration")
        if "acceptance_criteria" in wanted and not informational and not self.acceptance_criteria:
            required("acceptance_criteria", "at least one acceptance criterion is required")
        if "deliverables" in wanted and not self.deliverables:
            required("deliverables", "at least one deliverable is required")
        if not self.instructions:
            required("instructions", "at least one concrete instruction is required")
        elif len(self.instructions) == 1 and self.instructions[0].strip().lower() in {
            "handle this task", "process this task", "do the task", "处理这个任务"
        }:
            issues.append({"path": "instructions", "code": "too_vague", "message": "instructions must describe concrete work"})
        schema_id = str(self.result_contract.get("schema_id") or "").strip()
        if not schema_id:
            required("result_contract.schema_id", "a parseable result contract schema_id is required")
        criterion_ids: set[str] = set()
        for index, criterion in enumerate(self.acceptance_criteria):
            criterion_id = str(criterion.get("id") or "").strip()
            if not criterion_id or not str(criterion.get("text") or "").strip():
                issues.append({"path": f"acceptance_criteria[{index}]", "code": "invalid", "message": "criterion id and text are required"})
            elif criterion_id in criterion_ids:
                issues.append({"path": f"acceptance_criteria[{index}].id", "code": "duplicate", "message": f"duplicate criterion id: {criterion_id}"})
            criterion_ids.add(criterion_id)
        deliverable_ids: set[str] = set()
        for index, deliverable in enumerate(self.deliverables):
            deliverable_id = str(deliverable.get("id") or "").strip()
            if not deliverable_id or not str(deliverable.get("kind") or "").strip():
                issues.append({"path": f"deliverables[{index}]", "code": "invalid", "message": "deliverable id and kind are required"})
            elif deliverable_id in deliverable_ids:
                issues.append({"path": f"deliverables[{index}].id", "code": "duplicate", "message": f"duplicate deliverable id: {deliverable_id}"})
            deliverable_ids.add(deliverable_id)
        return tuple(issues)

    def validate(self, **kwargs: Any) -> "TaskBriefDraft":
        issues = self.validation_issues(**kwargs)
        if issues:
            raise HandoffValidationError(issues)
        return self

    def to_dict(self) -> dict[str, Any]:
        return jsonable(self)

    @property
    def content_hash(self) -> str:
        return canonical_hash(self.to_dict())


@dataclass(frozen=True, slots=True)
class TaskBriefRecord:
    id: str
    task_id: str
    revision: int
    status: BriefStatus
    title: str
    objective: str
    background: str
    scope: Mapping[str, Any]
    instructions: tuple[str, ...]
    constraints: tuple[str, ...]
    non_goals: tuple[str, ...]
    acceptance_criteria: tuple[Mapping[str, Any], ...]
    deliverables: tuple[Mapping[str, Any], ...]
    result_contract: Mapping[str, Any]
    created_by_task_id: Optional[str]
    created_by_run_id: Optional[str]
    content_hash: str
    created_at: datetime
    published_at: Optional[datetime]

    def to_dict(self) -> dict[str, Any]:
        return jsonable(self)


@dataclass(frozen=True, slots=True)
class ContextRefDraft:
    requirement: ContextRequirement | str
    ref_type: ContextRefType | str
    display_name: str
    selection_reason: str
    locator: Mapping[str, Any]
    delivery_mode: ContextDeliveryMode | str = ContextDeliveryMode.ON_DEMAND
    summary: str = ""
    mime_type: Optional[str] = None
    content_hash: Optional[str] = None
    byte_size: Optional[int] = None
    token_estimate: Optional[int] = None
    provenance: Mapping[str, Any] = field(default_factory=dict, hash=False)
    trust_level: str = "untrusted"

    def __post_init__(self) -> None:
        requirement = ContextRequirement(str(self.requirement))
        ref_type = ContextRefType(str(self.ref_type))
        delivery = ContextDeliveryMode(str(self.delivery_mode))
        display_name = str(self.display_name).strip()
        reason = str(self.selection_reason).strip()
        if not display_name or not reason:
            raise HandoffValidationError(({"path": "context_ref", "code": "required", "message": "display_name and selection_reason are required"},))
        if self.byte_size is not None and int(self.byte_size) < 0:
            raise HandoffValidationError(({"path": "byte_size", "code": "invalid", "message": "byte_size cannot be negative"},))
        if self.token_estimate is not None and int(self.token_estimate) < 0:
            raise HandoffValidationError(({"path": "token_estimate", "code": "invalid", "message": "token_estimate cannot be negative"},))
        locator = dict(self.locator)
        issues: list[dict[str, Any]] = []
        if ref_type in {ContextRefType.FILE, ContextRefType.FILE_RANGE, ContextRefType.GIT_DIFF}:
            if not str(locator.get("relative_path") or "").strip():
                issues.append({"path": "locator.relative_path", "code": "required", "message": "a workspace-relative path is required"})
        if ref_type is ContextRefType.FILE_RANGE:
            start = locator.get("start_line")
            end = locator.get("end_line")
            if start is None or end is None:
                issues.append({"path": "locator", "code": "required", "message": "file ranges require start_line and end_line"})
            else:
                try:
                    first, last = int(start), int(end)
                    if first < 1 or last < first:
                        raise ValueError
                except (TypeError, ValueError):
                    issues.append({"path": "locator", "code": "invalid_range", "message": "line range must satisfy 1 <= start_line <= end_line"})
        if ref_type is ContextRefType.ARTIFACT and not any(
            locator.get(key) for key in ("artifact_id", "blob_uri")
        ):
            issues.append({"path": "locator", "code": "required", "message": "artifact_id or blob_uri is required"})
        if ref_type is ContextRefType.WORK_PRODUCT and not locator.get("work_product_id"):
            issues.append({"path": "locator.work_product_id", "code": "required", "message": "work_product_id is required"})
        if ref_type is ContextRefType.URL:
            from urllib.parse import urlparse

            parsed = urlparse(str(locator.get("url") or ""))
            if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
                issues.append({"path": "locator.url", "code": "invalid", "message": "URL must be an http(s) URL without embedded credentials"})
        if contains_secret_like(
            {
                "display_name": display_name,
                "selection_reason": reason,
                "summary": self.summary,
                "locator": locator,
                "provenance": self.provenance,
            }
        ):
            issues.append(
                {
                    "path": "$",
                    "code": "secret_detected",
                    "message": "context reference metadata cannot contain secret-like values",
                }
            )
        if issues:
            raise HandoffValidationError(issues)
        object.__setattr__(self, "requirement", requirement)
        object.__setattr__(self, "ref_type", ref_type)
        object.__setattr__(self, "delivery_mode", delivery)
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "selection_reason", reason)
        object.__setattr__(self, "summary", str(self.summary).strip())
        object.__setattr__(self, "locator", _freeze(locator))
        object.__setattr__(self, "provenance", _freeze(dict(self.provenance)))
        object.__setattr__(self, "byte_size", None if self.byte_size is None else int(self.byte_size))
        object.__setattr__(self, "token_estimate", None if self.token_estimate is None else int(self.token_estimate))
        trust_level = str(self.trust_level or "untrusted").strip()
        if trust_level not in {
            "untrusted",
            "system_generated",
            "operator_provided",
            "operator_verified",
            "agent_generated",
            "external_untrusted",
        }:
            raise HandoffValidationError(({"path": "trust_level", "code": "invalid", "message": "unsupported context trust level"},))
        object.__setattr__(self, "trust_level", trust_level)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ContextRefDraft":
        return cls(
            requirement=str(value.get("requirement") or "optional"),
            ref_type=str(value.get("ref_type") or "artifact"),
            display_name=str(value.get("display_name") or ""),
            selection_reason=str(value.get("selection_reason") or ""),
            locator=dict(value.get("locator") or {}),
            delivery_mode=str(value.get("delivery_mode") or "on_demand"),
            summary=str(value.get("summary") or ""),
            mime_type=str(value["mime_type"]) if value.get("mime_type") else None,
            content_hash=str(value["content_hash"]) if value.get("content_hash") else None,
            byte_size=value.get("byte_size"),
            token_estimate=value.get("token_estimate"),
            provenance=dict(value.get("provenance") or {}),
            trust_level=str(value.get("trust_level") or "untrusted"),
        )

    def to_dict(self) -> dict[str, Any]:
        return jsonable(self)


@dataclass(frozen=True, slots=True)
class ContextRefRecord:
    id: str
    task_id: str
    brief_id: str
    requirement: ContextRequirement
    ref_type: ContextRefType
    display_name: str
    summary: str
    selection_reason: str
    locator: Mapping[str, Any]
    delivery_mode: ContextDeliveryMode
    mime_type: Optional[str]
    content_hash: Optional[str]
    byte_size: Optional[int]
    token_estimate: Optional[int]
    provenance: Mapping[str, Any]
    trust_level: str
    created_by_task_id: Optional[str]
    created_by_run_id: Optional[str]
    created_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return jsonable(self)


@dataclass(frozen=True, slots=True)
class TaskRelationRecord:
    id: str
    from_task_id: str
    to_task_id: str
    relation_type: TaskRelationType
    metadata: Mapping[str, Any]
    created_by_task_id: Optional[str]
    created_by_run_id: Optional[str]
    created_at: datetime
    removed_at: Optional[datetime] = None


@dataclass(frozen=True, slots=True)
class WakeRequestRecord:
    id: str
    target_task_id: str
    target_run_id: Optional[str]
    reason: WakeReason
    source_task_id: Optional[str]
    source_run_id: Optional[str]
    source_event_id: Optional[str]
    payload: Mapping[str, Any]
    dedupe_key: str
    status: WakeStatus
    coalesced_count: int
    attempts: int
    not_before: datetime
    claimed_by: Optional[str]
    claimed_until: Optional[datetime]
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime
    delivered_at: Optional[datetime]
    completed_at: Optional[datetime]


@dataclass(frozen=True, slots=True)
class TaskCommentRecord:
    id: str
    task_id: str
    sequence: int
    author_type: str
    author_id: str
    created_by_run_id: Optional[str]
    body_markdown: str
    metadata: Mapping[str, Any]
    reply_to_comment_id: Optional[str]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WorkProductRecord:
    id: str
    task_id: str
    run_id: Optional[str]
    kind: WorkProductKind
    title: str
    summary: str
    evidence_id: Optional[str]
    artifact_id: Optional[str]
    uri: Optional[str]
    content_hash: Optional[str]
    metadata: Mapping[str, Any]
    verification_status: str
    created_by: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ExecutionEnvelope:
    schema_version: int
    dispatch_id: Optional[str]
    wake: Mapping[str, Any]
    task: Mapping[str, Any]
    brief: Mapping[str, Any]
    assignment: Mapping[str, Any]
    context_manifest: Mapping[str, Any]
    capability_contract: Mapping[str, Any]
    result_contract: Mapping[str, Any]
    trace: Mapping[str, Any]

    def __post_init__(self) -> None:
        for name in ("wake", "task", "brief", "assignment", "context_manifest", "capability_contract", "result_contract", "trace"):
            object.__setattr__(self, name, _freeze(dict(getattr(self, name))))

    def to_dict(self) -> dict[str, Any]:
        return jsonable(self)


@dataclass(frozen=True, slots=True)
class DelegationRequest:
    operation_id: str
    role: str
    brief: TaskBriefDraft
    context_refs: tuple[ContextRefDraft, ...] = ()
    blocked_by_task_ids: tuple[str, ...] = ()
    priority: int = 0
    runtime_preset_id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class DelegationResult:
    child_task_id: str
    brief_id: str
    brief_revision: int
    status: str
    wake_id: Optional[str]
    replayed: bool = False
