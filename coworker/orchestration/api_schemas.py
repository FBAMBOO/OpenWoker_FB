"""Typed HTTP payloads for Task-Centric Handoff endpoints."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from .quality.models import (
    Archetype,
    ArtifactStatus,
    BudgetMode,
    BudgetStatus,
    QualityStatus,
    WorkflowStatus,
)
from .quality.state_machine import WorkflowEvent


class HandoffModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TaskQualityWorkflowTransitionPayload(HandoffModel):
    from_status: WorkflowStatus
    event: WorkflowEvent
    to_statuses: list[WorkflowStatus]
    uses_resume_status: bool
    server_selects_target: bool


class TaskQualitySchemaSnapshot(HandoffModel):
    """Typed OpenAPI projection generated from the Python canonical source."""

    schema_version: int = 2
    workflow_statuses: list[WorkflowStatus]
    quality_statuses: list[QualityStatus]
    artifact_statuses: list[ArtifactStatus]
    budget_statuses: list[BudgetStatus]
    budget_modes: list[BudgetMode]
    archetypes: list[Archetype]
    workflow_events: list[WorkflowEvent]
    workflow_transitions: list[TaskQualityWorkflowTransitionPayload]


class TaskBriefPayload(HandoffModel):
    operation_id: Optional[str] = None
    title: str = ""
    objective: str = ""
    background: str = ""
    scope: dict[str, Any] = Field(default_factory=dict)
    instructions: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    non_goals: list[str] = Field(default_factory=list)
    acceptance_criteria: list[dict[str, Any]] = Field(default_factory=list)
    deliverables: list[dict[str, Any]] = Field(default_factory=list)
    result_contract: dict[str, Any] = Field(default_factory=dict)


class ContextRefPayload(HandoffModel):
    requirement: str = "optional"
    ref_type: str
    display_name: str
    selection_reason: str
    locator: dict[str, Any]
    delivery_mode: str = "on_demand"
    summary: str = ""
    mime_type: Optional[str] = None
    content_hash: Optional[str] = None
    byte_size: Optional[int] = None
    token_estimate: Optional[int] = None
    provenance: dict[str, Any] = Field(default_factory=dict)
    trust_level: str = "untrusted"


class AddContextRefRequest(HandoffModel):
    operation_id: Optional[str] = None
    brief_id: Optional[str] = None
    context_ref: ContextRefPayload


class DelegateTaskRequest(HandoffModel):
    operation_id: str
    role: str
    brief: TaskBriefPayload
    context_refs: list[ContextRefPayload] = Field(default_factory=list)
    blocked_by_task_ids: list[str] = Field(default_factory=list)
    priority: int = 0
    runtime_preset_id: Optional[str] = None
    run_id: str
    lease_token: str
    fencing_token: int


class TaskRelationRequest(HandoffModel):
    operation_id: Optional[str] = None
    from_task_id: Optional[str] = None
    to_task_id: Optional[str] = None
    relation_type: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class BlockerSetRequest(HandoffModel):
    operation_id: Optional[str] = None
    task_ids: list[str] = Field(default_factory=list)
    reason: str = ""
    owner: str = "local-user"
    required_action: str = "Complete all blocker tasks"


class TaskCommentRequest(HandoffModel):
    body_markdown: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    reply_to_comment_id: Optional[str] = None
    command_id: Optional[str] = None
    operation_id: Optional[str] = None


class WorkProductRequest(HandoffModel):
    kind: str
    title: str
    summary: str = ""
    evidence_id: Optional[str] = None
    artifact_id: Optional[str] = None
    uri: Optional[str] = None
    content_hash: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    verification_status: str = "unverified"
    command_id: Optional[str] = None
    operation_id: Optional[str] = None


class ResultQuestionRequest(HandoffModel):
    question: str = Field(min_length=1, max_length=4_000)
    operation_id: Optional[str] = None


class HandoffSettingsPayload(HandoffModel):
    structured_handoff_enabled: bool = True
    structured_handoff_required_for_new_tasks: bool = False
    legacy_spawn_agent_enabled: bool = True
    default_context_token_budget: int = Field(8_000, ge=0, le=1_000_000)
    max_context_refs: int = Field(50, ge=0, le=1_000)
    max_inline_bytes_per_ref: int = Field(8_192, ge=0, le=65_536)
    max_inline_bytes_total: int = Field(32_768, ge=0, le=65_536)
    max_comment_batch: int = Field(100, ge=1, le=1_000)
    wake_coalesce_window_ms: int = Field(1_000, ge=0, le=60_000)
    wake_max_attempts: int = Field(5, ge=1, le=100)
    wake_backoff_seconds: int = Field(1, ge=1, le=3_600)
    context_read_audit_enabled: bool = True
    transcript_sharing_default: bool = False
