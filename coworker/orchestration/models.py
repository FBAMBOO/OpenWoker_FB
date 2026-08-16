"""Typed domain objects for durable multi-agent orchestration.

Task status and workflow stage are intentionally separate.  A task can wait, pause,
or require reconciliation without losing where it is in the fixed eight-stage process.
Plans and evidence are immutable records; runs, gates, tasks, and leases are mutable
aggregates protected by optimistic versions or fencing tokens.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Optional, Sequence


class _StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class TaskStatus(_StrEnum):
    DRAFT = "draft"
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_HUMAN = "waiting_human"
    WAITING_CHILD = "waiting_child"
    PAUSED = "paused"
    BLOCKED = "blocked"
    NEEDS_RECONCILIATION = "needs_reconciliation"
    CANCELING = "canceling"
    FAILED = "failed"
    CANCELED = "canceled"
    COMPLETED = "completed"
    ARCHIVED = "archived"


class OrchestrationStage(_StrEnum):
    INTAKE = "intake"
    COMPLEXITY_ASSESSMENT = "complexity_assessment"
    CLARIFICATION = "clarification"
    PLANNING = "planning"
    EXECUTION_REVIEW_TEST = "execution_review_test"
    INTER_STEP_EVALUATION = "inter_step_evaluation"
    FINAL_ACCEPTANCE = "final_acceptance"
    ARCHIVE = "archive"


class StageDisposition(_StrEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    REQUEST_CHANGES = "request_changes"
    CANCELED = "canceled"
    FAILED = "failed"


class TaskDomain(_StrEnum):
    CODE = "code"
    KNOWLEDGE = "knowledge"


class ComplexityLevel(_StrEnum):
    TRIVIAL = "trivial"
    STANDARD = "standard"
    COMPLEX = "complex"
    CRITICAL = "critical"


class RiskTier(_StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class NodeKind(_StrEnum):
    EXECUTE = "execute"
    REVIEW = "review"
    TEST = "test"
    INTEGRATE = "integrate"
    EVALUATE = "evaluate"
    AGENT = "agent"
    HUMAN_GATE = "human_gate"
    CHILD_TASK = "child_task"
    NOOP = "noop"


class JoinPolicy(_StrEnum):
    ALL = "all"
    ANY = "any"


class FailurePolicy(_StrEnum):
    FAIL_FAST = "fail_fast"
    CONTINUE = "continue"
    SKIP_DEPENDENTS = "skip_dependents"
    MANUAL = "manual"


class EffectSafety(_StrEnum):
    READ_ONLY = "read_only"
    IDEMPOTENT = "idempotent"
    NON_IDEMPOTENT = "non_idempotent"


class EdgeCondition(_StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    TERMINAL = "terminal"
    ALWAYS = "always"


class RunStatus(_StrEnum):
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    WAITING_GATE = "waiting_gate"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"
    LOST = "lost"
    SKIPPED = "skipped"


class GateKind(_StrEnum):
    CLARIFICATION = "clarification"
    PLAN_APPROVAL = "plan_approval"
    BUDGET = "budget"
    WORKSPACE_CONFLICT = "workspace_conflict"
    RECONCILIATION = "reconciliation"
    APPROVAL = "approval"
    QUESTION = "question"
    PERMISSION = "permission"
    PLAN = "plan"
    REVIEW = "review"
    RECOVERY = "recovery"
    CHILD_WAIT = "child_wait"
    FINAL_ACCEPTANCE = "final_acceptance"


class GateStatus(_StrEnum):
    PREPARING = "preparing"
    OPEN = "open"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELED = "canceled"


class EvidenceKind(_StrEnum):
    AUDIT_EVENT = "audit_event"
    METRIC = "metric"
    EXTERNAL_LINK = "external_link"
    NOTE = "note"
    ARTIFACT = "artifact"
    TEST_RESULT = "test_result"
    REVIEW = "review"
    DECISION = "decision"
    LOG = "log"
    CHECKPOINT = "checkpoint"
    OTHER = "other"


class CommandStatus(_StrEnum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


JsonMap = Mapping[str, Any]


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    initial_delay_seconds: float = 1.0
    multiplier: float = 2.0
    max_delay_seconds: float = 300.0
    jitter: float = 0.2


@dataclass(frozen=True)
class TaskSpec:
    idempotency_key: str
    objective: str
    domain: TaskDomain = TaskDomain.CODE
    workspace: Optional[str] = None
    title: Optional[str] = None
    constraints: Sequence[str] = field(default_factory=tuple)
    acceptance_criteria: Sequence[str] = field(default_factory=tuple)
    complexity_score: Optional[float] = None
    complexity_level: Optional[ComplexityLevel] = None
    risk_tier: RiskTier = RiskTier.LOW
    budget: JsonMap = field(default_factory=dict)
    policy: JsonMap = field(default_factory=dict)
    input: JsonMap = field(default_factory=dict)
    priority: int = 0
    max_parallel_runs: int = 8
    parent_task_id: Optional[str] = None
    parent_node_id: Optional[str] = None


@dataclass(frozen=True)
class TaskRecord:
    id: str
    idempotency_key: str
    title: str
    objective: str
    domain: TaskDomain
    workspace: Optional[str]
    constraints: tuple[str, ...]
    acceptance_criteria: tuple[str, ...]
    complexity_score: Optional[float]
    complexity_level: Optional[ComplexityLevel]
    risk_tier: RiskTier
    budget: JsonMap
    policy: JsonMap
    input: JsonMap
    output: Optional[JsonMap]
    status: TaskStatus
    current_stage: OrchestrationStage
    active_plan_id: Optional[str]
    parent_task_id: Optional[str]
    parent_node_id: Optional[str]
    priority: int
    max_parallel_runs: int
    version: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class StageHistoryRecord:
    id: str
    task_id: str
    sequence: int
    stage: OrchestrationStage
    disposition: StageDisposition
    entered_at: datetime
    exited_at: Optional[datetime]
    detail: JsonMap
    command_id: Optional[str]


@dataclass(frozen=True)
class NodeSpec:
    key: str
    title: str = ""
    instructions: str = ""
    kind: NodeKind = NodeKind.EXECUTE
    agent: str = "code"
    model: Optional[str] = None
    input: JsonMap = field(default_factory=dict)
    join_policy: JoinPolicy = JoinPolicy.ALL
    failure_policy: FailurePolicy = FailurePolicy.FAIL_FAST
    effect_safety: EffectSafety = EffectSafety.READ_ONLY
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    timeout_seconds: int = 900
    priority: int = 0
    concurrency_key: Optional[str] = None
    metadata: JsonMap = field(default_factory=dict)


@dataclass(frozen=True)
class EdgeSpec:
    from_node: str
    to_node: str
    condition: EdgeCondition = EdgeCondition.SUCCESS
    required: bool = True
    metadata: JsonMap = field(default_factory=dict)


@dataclass(frozen=True)
class PlanSpec:
    nodes: Sequence[NodeSpec]
    edges: Sequence[EdgeSpec] = field(default_factory=tuple)
    metadata: JsonMap = field(default_factory=dict)


@dataclass(frozen=True)
class PlanRecord:
    id: str
    task_id: str
    revision: int
    parent_plan_id: Optional[str]
    content_hash: str
    metadata: JsonMap
    created_by: str
    created_at: datetime


@dataclass(frozen=True)
class NodeRecord:
    id: str
    plan_id: str
    key: str
    title: str
    instructions: str
    kind: NodeKind
    agent: str
    model: Optional[str]
    input: JsonMap
    join_policy: JoinPolicy
    failure_policy: FailurePolicy
    effect_safety: EffectSafety
    retry_policy: RetryPolicy
    timeout_seconds: int
    priority: int
    concurrency_key: Optional[str]
    metadata: JsonMap


@dataclass(frozen=True)
class EdgeRecord:
    id: str
    plan_id: str
    from_node_id: str
    to_node_id: str
    from_node: str
    to_node: str
    condition: EdgeCondition
    required: bool
    metadata: JsonMap


@dataclass(frozen=True)
class PlanGraph:
    plan: PlanRecord
    nodes: tuple[NodeRecord, ...]
    edges: tuple[EdgeRecord, ...]


@dataclass(frozen=True)
class RunRecord:
    id: str
    task_id: str
    plan_id: str
    node_id: str
    node_key: str
    attempt: int
    status: RunStatus
    session_id: Optional[str]
    priority: int
    ready_at: datetime
    fencing_token: int
    output: Optional[JsonMap]
    error_kind: Optional[str]
    error_message: Optional[str]
    version: int
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]


@dataclass(frozen=True)
class LeaseRecord:
    id: str
    run_id: str
    owner: str
    token: str
    fencing_token: int
    expires_at: datetime
    heartbeat_at: datetime
    created_at: datetime


@dataclass(frozen=True)
class RunClaim:
    run: RunRecord
    lease: LeaseRecord


@dataclass(frozen=True)
class GateRecord:
    id: str
    task_id: str
    # Lifecycle gates (clarification, plan approval, final acceptance) belong to
    # the task itself; tool/permission gates additionally point at a concrete run.
    run_id: Optional[str]
    node_id: Optional[str]
    kind: GateKind
    status: GateStatus
    source_key: str
    prompt: JsonMap
    resolution: Optional[JsonMap]
    resolved_by: Optional[str]
    version: int
    opened_at: datetime
    # Null means the run-owned gate is still an internal preparation that has
    # never crossed the atomic checkpoint/cleanup publication point.
    published_at: Optional[datetime]
    resolved_at: Optional[datetime]
    expires_at: Optional[datetime]


@dataclass(frozen=True)
class EvidenceRecord:
    id: str
    task_id: str
    plan_id: Optional[str]
    node_id: Optional[str]
    run_id: Optional[str]
    kind: EvidenceKind
    mime_type: str
    content_hash: str
    payload: JsonMap
    blob_uri: Optional[str]
    created_by: str
    created_at: datetime


@dataclass(frozen=True)
class EventRecord:
    sequence: int
    id: str
    task_id: Optional[str]
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: JsonMap
    previous_hash: str
    event_hash: str
    command_id: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class OutboxRecord:
    id: str
    event_id: str
    topic: str
    payload: JsonMap
    available_at: datetime
    attempts: int
    locked_by: Optional[str]
    locked_until: Optional[datetime]
    published_at: Optional[datetime]
    dead_lettered_at: Optional[datetime]
    last_error: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class OutboxRequeueRecord:
    id: str
    outbox_id: str
    command_id: str
    actor: str
    reason: str
    snapshot_attempts: int
    snapshot_last_error: Optional[str]
    snapshot_dead_lettered_at: datetime
    requeued_at: datetime


@dataclass(frozen=True)
class CommandRecord:
    id: str
    name: str
    scope: str
    request_hash: str
    status: CommandStatus
    result: Optional[JsonMap]
    error: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]
