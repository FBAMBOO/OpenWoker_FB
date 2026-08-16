"""Adapter from durable orchestration runs to OpenWorker's existing TurnEngine.

Every run gets a hidden, append-only OpenWorker session.  Reviewer and tester runs are
fresh sessions with role-specific tool ceilings; they never inherit worker conversation
history.  A permission/question suspends the engine, persists the unanswered tool call,
opens a durable gate, and releases the worker lease.  Resolving the gate requeues the
same attempt, which rebuilds the session and calls ``TurnEngine.resume``.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional

from aisuite.agents import ToolMetadata

from ..agent import build_engine
from ..agents import Agent, code_agent
from ..engine import (
    ApprovalOutcome,
    DeferredInteraction,
    PermissionRequest,
)
from ..events import EventType
from ..permissions import Mode
from .blobs import ContentAddressedBlobStore
from .models import (
    GateKind,
    GateStatus,
    NodeKind,
    NodeRecord,
    PlanGraph,
    RunClaim,
    TaskRecord,
)
from .profiles import AgentProfile, AgentRole
from .routing import RoutingDecision
from .runtime import DEFAULT_RUN_BUDGET, PermissionSet, RuntimeBudget
from .store import OrchestrationStore


ChildSpawner = Callable[[dict[str, Any]], Mapping[str, Any]]
ChildLookup = Callable[[Mapping[str, Any]], Mapping[str, Any]]
ChildCancel = Callable[[Mapping[str, Any]], Mapping[str, Any]]
GateNotifier = Callable[[str], Awaitable[None] | None]


_READ_ONLY_TOOLS = frozenset(
    {
        "list_files",
        "read_file",
        "read_file_lines",
        "grep",
        "git_status",
        "git_diff",
        "git_log",
        "web_search",
        "web_fetch",
        "ask_user",
        "spawn_agent",
        "wait_agent",
        "cancel_agent",
        "todo_write",
        "submit_verdict",
    }
)
_TEST_TOOLS = _READ_ONLY_TOOLS | frozenset(
    {"run_shell", "shell_task_output", "shell_task_kill"}
)
_ROLE_CEILINGS: dict[AgentRole, Optional[frozenset[str]]] = {
    AgentRole.ORCHESTRATOR: frozenset(
        {"ask_user", "spawn_agent", "wait_agent", "cancel_agent", "todo_write"}
    ),
    AgentRole.PLANNER: _READ_ONLY_TOOLS,
    AgentRole.REVIEWER: _READ_ONLY_TOOLS,
    AgentRole.EVALUATOR: _READ_ONLY_TOOLS,
    AgentRole.SCORER: _READ_ONLY_TOOLS,
    AgentRole.EXPLORER: _READ_ONLY_TOOLS,
    AgentRole.TESTER: _TEST_TOOLS,
    AgentRole.WORKER: None,
    AgentRole.INTEGRATOR: None,
}


@dataclass(frozen=True)
class RunExecutionContext:
    task: TaskRecord
    graph: PlanGraph
    node: NodeRecord
    claim: RunClaim
    profile: AgentProfile
    routing: RoutingDecision
    workspace: Optional[Path]
    parent_runtime_id: Optional[str] = None
    runtime_id: Optional[str] = None
    runtime_budget: RuntimeBudget = DEFAULT_RUN_BUDGET
    effective_permissions: Optional[PermissionSet] = None
    subject: Mapping[str, Any] = field(default_factory=dict)
    upstream_context: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class ExecutionOutcome:
    status: str
    session_id: str
    summary: str = ""
    output: Mapping[str, Any] = field(default_factory=dict)
    evidence: tuple[Mapping[str, Any], ...] = ()
    usage: Mapping[str, int] = field(default_factory=dict)
    gate_id: Optional[str] = None
    error_kind: Optional[str] = None
    error_message: Optional[str] = None


class OpenWorkerExecutor:
    """Execute one claimed orchestration run using the native OpenWorker engine."""

    def __init__(
        self,
        manager: Any,
        store: OrchestrationStore,
        *,
        spawn_child: Optional[ChildSpawner] = None,
        lookup_child: Optional[ChildLookup] = None,
        cancel_child: Optional[ChildCancel] = None,
        on_gate: Optional[GateNotifier] = None,
        blob_store: Optional[ContentAddressedBlobStore] = None,
    ) -> None:
        self.manager = manager
        self.store = store
        self.spawn_child = spawn_child
        self.lookup_child = lookup_child
        self.cancel_child = cancel_child
        self.on_gate = on_gate
        self.blob_store = blob_store
        self._active_lock = threading.RLock()
        self._active_engines: dict[str, Any] = {}

    def interrupt(self, run_id: str) -> None:
        with self._active_lock:
            engine = self._active_engines.get(run_id)
        if engine is not None:
            engine.request_interrupt()

    async def execute(self, context: RunExecutionContext) -> ExecutionOutcome:
        run = context.claim.run
        session_id = run.session_id or f"__orch__{run.id}"
        record = self.manager.session_store.load(session_id)
        checkpoint_messages: Optional[list[dict[str, Any]]] = None
        checkpoint = dict((run.output or {}).get("engine_checkpoint") or {})
        if checkpoint:
            try:
                if self.blob_store is None:
                    raise ValueError("checkpoint blob store is unavailable")
                payload = json.loads(
                    self.blob_store.get(str(checkpoint.get("blob_uri") or "")).decode(
                        "utf-8"
                    )
                )
                if (
                    str(payload.get("run_id") or "") != run.id
                    or str(payload.get("session_id") or "") != session_id
                    or str(payload.get("gate_id") or "")
                    != str(checkpoint.get("gate_id") or "")
                    or payload.get("recovery_disposition") != "pending_tools"
                ):
                    raise ValueError("checkpoint identity does not match the claimed run")
                checkpoint_messages = [dict(item) for item in payload.get("messages") or ()]
                if not checkpoint_messages:
                    raise ValueError("checkpoint contains no conversation messages")
            except Exception as exc:
                return ExecutionOutcome(
                    status="failed",
                    session_id=session_id,
                    error_kind="recovery_checkpoint_invalid",
                    error_message=str(exc),
                )
        verdict_report: dict[str, Any] = {}
        extra_tools = self._runtime_tools(context, verdict_report)
        mode = self._mode_for(context.profile, context.effective_permissions)
        allowed_tools = self._effective_tools(
            context.profile,
            context.effective_permissions,
            read_only=bool(context.task.policy.get("read_only", False)),
        )
        callbacks = self._gate_callbacks(context)
        base = code_agent()
        agent = Agent(
            # Persist as the built-in Code surface so an operator can open the hidden
            # run transcript after completion. The frozen orchestration profile remains
            # in run/evidence records and supplies this engine's actual instructions.
            name="code",
            title=context.profile.display_name,
            system_prompt=self._system_prompt(context),
            needs_workspace=context.workspace is not None,
            tool_factory=base.tool_factory,
            family="code",
        )

        if (
            context.runtime_budget.model_calls <= 0
            or context.runtime_budget.wall_seconds <= 0
        ):
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                error_kind="budget_exceeded",
                error_message="run has no model-call or wall-clock budget remaining",
            )

        try:
            command_ceiling = (
                sorted(context.effective_permissions.commands)
                if context.effective_permissions
                and context.effective_permissions.commands is not None
                else [
                    str(item)
                    for item in context.profile.metadata.get("allowed_commands", ())
                ]
            )
            engine = build_engine(
                agent=agent,
                workspace=context.workspace,
                model=context.routing.selected_model or self.manager.model,
                mode=mode,
                provider=self.manager.provider,
                memory_store=None,  # role isolation: no cross-session memory injection
                messages=(
                    checkpoint_messages
                    if checkpoint_messages is not None
                    else record.messages
                    if record
                    else None
                ),
                extra_tools=extra_tools,
                secrets=self.manager.secrets,
                session_id=session_id,
                audit_sink=self.manager.audit_store.append,
                approver=callbacks["approver"],
                question_asker=callbacks["question"],
                plan_approver=callbacks["plan"],
                directory_requester=callbacks["directory"],
                max_iterations=max(
                    1,
                    min(
                        context.profile.max_iterations,
                        context.runtime_budget.model_calls,
                    ),
                ),
                allowed_commands=command_ceiling,
                # Never expose an unbounded shell to a durable Agent. Missing
                # profile/parent command authority becomes an empty hard ceiling.
                command_ceiling=command_ceiling,
                # Commands are disposable process trees: timeout, cancellation, normal
                # completion, and lease-boundary close all reap descendants.
                contain_shell_process_tree=True,
                roots=(
                    [
                        {
                            "path": context.workspace,
                            "writable": any(
                                root.writable
                                for root in (context.effective_permissions.roots or ())
                            ),
                        }
                    ]
                    if context.workspace is not None and context.effective_permissions
                    else None
                ),
                skill_filter=set(),
                connector_filter=set(),
                tool_filter=allowed_tools,
                tool_guard=lambda tool_call: self._guard_tool(context, tool_call),
                # A Worker may create AGENTS.md inside the candidate.  It is an
                # auditable artifact, never authority over Reviewer/Tester/Evaluator
                # system prompts (or over another frozen orchestration role).
                load_workspace_instructions=False,
            )
        except Exception as exc:
            return ExecutionOutcome(
                status="failed",
                session_id=session_id,
                error_kind="engine_build_failed",
                error_message=str(exc),
            )

        engine.compaction_settings = self.manager.compaction_settings
        with self._active_lock:
            self._active_engines[run.id] = engine
        model_calls = tool_calls = tokens = 0
        suspended_gate: Optional[str] = None
        terminal_status = "failed"
        error_kind: Optional[str] = None
        error_message: Optional[str] = None
        cancelled_error: Optional[asyncio.CancelledError] = None
        engine_checkpoint: Optional[dict[str, Any]] = None
        started = time.monotonic()

        recovery = engine.recovery_state()
        persisted = checkpoint_messages is not None or record is not None
        iterator = (
            engine.resume()
            if persisted and recovery.disposition == "pending_tools"
            else engine.retry()
            if persisted and recovery.disposition == "retriable_error"
            else engine.continue_interrupted()
            if persisted and recovery.disposition == "interrupted"
            else engine.run(self._user_prompt(context))
        )
        try:
            async for event in iterator:
                data = dict(event.data or {})
                if event.type is EventType.ASSISTANT_MESSAGE:
                    model_calls += 1
                    usage = data.get("usage") or {}
                    tokens += sum(
                        int(usage.get(name, 0) or 0)
                        for name in ("input", "output", "cache_read", "cache_write")
                    )
                elif event.type is EventType.TOOL_FINISHED:
                    tool_calls += 1
                elif event.type is EventType.TURN_SUSPENDED:
                    suspended_gate = str(data.get("interaction_id") or "") or None
                    terminal_status = "suspended"
                elif event.type is EventType.ERROR:
                    error_kind = str(data.get("error_type") or "engine_error")
                    error_message = str(data.get("error") or "agent execution failed")
                    terminal_status = "failed"
                elif event.type is EventType.TURN_END:
                    terminal_status = (
                        "succeeded" if data.get("status") == "completed" else "failed"
                    )
                    if terminal_status == "failed":
                        error_kind = "turn_incomplete"
                        error_message = str(data.get("status") or "turn did not complete")
                current_usage = RuntimeBudget(
                    model_calls=model_calls,
                    tool_calls=tool_calls,
                    tokens=tokens,
                    wall_seconds=max(0, int(time.monotonic() - started)),
                )
                if not current_usage.fits_within(context.runtime_budget):
                    engine.request_interrupt()
                    terminal_status = "failed"
                    error_kind = "budget_exceeded"
                    error_message = "run exceeded its effective runtime budget"
                    break
        except asyncio.CancelledError as exc:
            engine.mark_interrupted()
            # Defer propagation until the owned shell has been closed.  Cleanup can
            # discover a descendant that escaped the bounded process tree only at
            # this lease-boundary close.  That containment breach is authoritative
            # and must become a durable failed outcome instead of being hidden by
            # the original cancellation.
            cancelled_error = exc
        except Exception as exc:
            terminal_status = "failed"
            error_kind = type(exc).__name__
            error_message = str(exc)
        finally:
            with self._active_lock:
                self._active_engines.pop(run.id, None)
            local_executor = getattr(engine, "executor", None)
            if bool(getattr(local_executor, "containment_failed", False)):
                terminal_status = "failed"
                suspended_gate = None
                error_kind = "process_tree_cleanup_failed"
                error_message = (
                    "a shell descendant escaped or outlived bounded process-tree cleanup; "
                    "the run requires reconciliation"
                )
            # A LocalExecutor owns a persistent shell whose cwd would otherwise pin an
            # isolated workspace on Windows. Conversation state is durable; shell state
            # is intentionally disposable at every orchestration lease boundary. Close
            # it before creating a resumable checkpoint: a prepared gate must never be
            # published if lease-boundary cleanup discovers a containment breach.
            if local_executor is not None:
                try:
                    local_executor.close()
                except Exception as exc:
                    terminal_status = "failed"
                    suspended_gate = None
                    error_kind = "process_tree_cleanup_failed"
                    error_message = (
                        "process-tree cleanup raised an exception and requires "
                        f"reconciliation: {exc}"
                    )
                if bool(getattr(local_executor, "containment_failed", False)):
                    terminal_status = "failed"
                    suspended_gate = None
                    error_kind = "process_tree_cleanup_failed"
                    error_message = (
                        "a shell descendant escaped or outlived bounded process-tree "
                        "cleanup; the run requires reconciliation"
                    )
            if terminal_status == "suspended":
                if not suspended_gate:
                    terminal_status = "failed"
                    error_kind = "suspension_protocol_invalid"
                    error_message = "suspended turn did not identify its prepared gate"
                elif self.blob_store is None:
                    terminal_status = "failed"
                    error_kind = "checkpoint_unavailable"
                    error_message = "suspended turn has no durable checkpoint store"
                else:
                    recovery = engine.recovery_state()
                    pending_calls = getattr(recovery, "pending_tool_calls", None)
                    if pending_calls is None:
                        pending_calls = getattr(recovery, "pending_tools", ())
                    pending_ids = [item.id for item in pending_calls]
                    if recovery.disposition != "pending_tools" or not pending_ids:
                        terminal_status = "failed"
                        error_kind = "checkpoint_invalid"
                        error_message = (
                            "suspended turn has no resumable pending tool checkpoint"
                        )
                    else:
                        checkpoint_payload = {
                            "schema_version": 1,
                            "run_id": run.id,
                            "attempt": run.attempt,
                            "fencing_token": context.claim.lease.fencing_token,
                            "session_id": session_id,
                            "gate_id": suspended_gate,
                            "recovery_disposition": recovery.disposition,
                            "pending_tool_call_ids": pending_ids,
                            "messages": engine.messages,
                        }
                        ref = self.blob_store.put_json(checkpoint_payload)
                        engine_checkpoint = {
                            "schema_version": 1,
                            "run_id": run.id,
                            "attempt": run.attempt,
                            "fencing_token": context.claim.lease.fencing_token,
                            "session_id": session_id,
                            "gate_id": suspended_gate,
                            "blob_uri": ref.uri,
                            "blob_sha256": ref.sha256,
                            "pending_tool_call_ids": pending_ids,
                            "recovery_disposition": recovery.disposition,
                        }
            self.manager.save(session_id, engine)

        if cancelled_error is not None and error_kind != "process_tree_cleanup_failed":
            raise cancelled_error

        summary = self._last_assistant_text(engine.messages)
        elapsed = max(0, int(time.monotonic() - started))
        usage = {
            "model_calls": model_calls,
            "tool_calls": tool_calls,
            "tokens": tokens,
            "wall_seconds": elapsed,
        }
        output = {
            "summary": summary,
            "profile": {
                "profile_id": context.profile.profile_id,
                "version": context.profile.version,
                "content_hash": context.profile.content_hash,
            },
            "routing": context.routing.audit_record(),
            "usage": usage,
            "session_id": session_id,
        }
        if engine_checkpoint is not None:
            output["engine_checkpoint"] = engine_checkpoint
        if verdict_report:
            verdict_report.setdefault("schema_version", 1)
            verdict_report.setdefault("task_id", context.task.id)
            verdict_report.setdefault("plan_id", context.graph.plan.id)
            verdict_report.setdefault("run_id", run.id)
            verdict_report.setdefault("role", context.profile.role.value)
            verdict_report.setdefault("subject", dict(context.subject))
            output["verdict"] = dict(verdict_report)
        elif context.node.kind in {
            NodeKind.REVIEW,
            NodeKind.TEST,
            NodeKind.EVALUATE,
        } or context.profile.role in {
            AgentRole.REVIEWER,
            AgentRole.TESTER,
            AgentRole.EVALUATOR,
            AgentRole.SCORER,
        }:
            # A successful model turn is not itself a verification verdict. Missing
            # structured evidence remains unknown and therefore cannot auto-accept.
            output["verdict"] = {
                "status": "unknown",
                "summary": "verification Agent did not submit a structured verdict",
                "criteria": {},
                "findings": [],
                "schema_version": 1,
                "task_id": context.task.id,
                "plan_id": context.graph.plan.id,
                "run_id": run.id,
                "role": context.profile.role.value,
                "subject": dict(context.subject),
            }
        evidence = (
            {
                "kind": "log",
                "title": f"{context.profile.display_name} result",
                "summary": summary,
                "session_id": session_id,
            },
            {"kind": "metric", "title": "Run usage", "usage": usage},
        )
        return ExecutionOutcome(
            status=terminal_status,
            session_id=session_id,
            summary=summary,
            output=output,
            evidence=evidence,
            usage=usage,
            gate_id=suspended_gate,
            error_kind=error_kind,
            error_message=error_message,
        )

    def _guard_tool(self, context: RunExecutionContext, tool_call: Any) -> None:
        """Fence each side effect and reject process lifetimes beyond the run lease."""

        # A run lease alone is insufficient during scheduler failover: the old
        # process may still own a 60-second run claim after its 15-second scheduler
        # epoch was replaced. Renewing the bound scheduler identity first rejects
        # that stale process before the registry crosses an external-effect boundary.
        # Standalone executor tests intentionally use an unbound store, where this is
        # a no-op and the run fence remains authoritative.
        self.store.renew_scheduler_fence()
        self.store.assert_run_lease(
            context.claim.run.id,
            context.claim.lease.token,
            context.claim.lease.fencing_token,
        )
        if (
            str(getattr(tool_call, "name", "")) == "run_shell"
            and bool(dict(getattr(tool_call, "arguments", {}) or {}).get("run_in_background"))
        ):
            raise PermissionError(
                "background shell processes are disabled in orchestration runtimes"
            )

    def _runtime_tools(
        self, context: RunExecutionContext, verdict_report: dict[str, Any]
    ) -> list[Callable[..., Any]]:
        allowed_roles = {role.value for role in context.profile.allowed_child_roles}
        parent = {
            "task_id": context.task.id,
            "run_id": context.claim.run.id,
            "node_id": context.node.id,
            "parent_runtime_id": context.parent_runtime_id,
            "allowed_roles": sorted(allowed_roles),
            "workspace": str(context.workspace) if context.workspace else None,
            "lease_token": context.claim.lease.token,
            "fencing_token": context.claim.lease.fencing_token,
        }

        def spawn_agent(
            role: str,
            task: str,
            operation_id: Optional[str] = None,
            child_key: Optional[str] = None,
        ) -> dict[str, Any]:
            """Spawn a bounded child Agent task.

            ``operation_id`` (or its compatibility alias ``child_key``) identifies
            one logical delegation across a lost/retried parent Agent attempt.
            Reusing it with different work is rejected.
            """

            chosen = str(role).strip().lower()
            if not self.spawn_child:
                return {"ok": False, "error": "child delegation is unavailable"}
            if chosen not in allowed_roles:
                return {"ok": False, "error": f"child role is not allowed: {chosen}"}
            if not str(operation_id or child_key or "").strip():
                return {
                    "ok": False,
                    "error": (
                        "operation_id is required so a crash/replay cannot duplicate or "
                        "collapse child delegations"
                    ),
                }
            request = {**parent, "role": chosen, "objective": task}
            if operation_id is not None:
                request["operation_id"] = operation_id
            if child_key is not None:
                request["child_key"] = child_key
            return dict(self.spawn_child(request))

        def wait_agent(task_id: str) -> dict[str, Any] | DeferredInteraction:
            """Return the durable current status and result of a child task."""

            if not self.lookup_child:
                return {"ok": False, "error": "child lookup is unavailable"}
            lookup = {
                "task_id": task_id,
                "parent_task_id": context.task.id,
                "parent_run_id": context.claim.run.id,
                "lease_token": context.claim.lease.token,
                "fencing_token": context.claim.lease.fencing_token,
            }
            state = dict(self.lookup_child(lookup))
            if not state.get("ok"):
                return state
            if str(state.get("status")) in {
                "failed",
                "canceled",
                "completed",
                "archived",
            }:
                return state
            gate = self.store.prepare_child_wait(
                context.claim.run.id,
                context.claim.lease.token,
                context.claim.lease.fencing_token,
                child_task_id=task_id,
                source_key=f"{context.claim.run.id}:child_wait:{task_id}",
            )
            return DeferredInteraction(
                gate.id,
                "child_wait",
                {"child_task_id": task_id},
            )

        def cancel_agent(task_id: str) -> dict[str, Any]:
            """Request cancellation of a child task owned by this orchestration tree."""

            if not self.cancel_child:
                return {"ok": False, "error": "child cancellation is unavailable"}
            return dict(
                self.cancel_child(
                    {
                        "task_id": task_id,
                        "parent_task_id": context.task.id,
                        "parent_run_id": context.claim.run.id,
                        "lease_token": context.claim.lease.token,
                        "fencing_token": context.claim.lease.fencing_token,
                    }
                )
            )

        def submit_verdict(
            verdict: str,
            summary: str,
            criteria: Optional[dict[str, str]] = None,
            findings: Optional[list[str]] = None,
        ) -> dict[str, Any]:
            """Submit the role's structured verification verdict before finishing."""

            aliases = {
                "accept": "pass",
                "accepted": "pass",
                "approve": "pass",
                "approved": "pass",
                "reject": "fail",
                "rejected": "fail",
                "retry": "fail",
                "replan": "fail",
                "escalate": "unknown",
            }
            status = aliases.get(str(verdict).strip().lower(), str(verdict).strip().lower())
            if status not in {"pass", "fail", "unknown"}:
                return {
                    "ok": False,
                    "error": "verdict must be pass, fail, or unknown",
                }
            normalized_criteria: dict[str, str] = {}
            for criterion, value in dict(criteria or {}).items():
                normalized = aliases.get(str(value).strip().lower(), str(value).strip().lower())
                if normalized not in {"pass", "fail", "unknown"}:
                    return {
                        "ok": False,
                        "error": f"criterion verdict must be pass, fail, or unknown: {criterion}",
                    }
                normalized_criteria[str(criterion)] = normalized
            declared = set(context.task.acceptance_criteria)
            unknown_criteria = set(normalized_criteria) - declared
            if unknown_criteria:
                return {
                    "ok": False,
                    "error": "verdict contains undeclared criteria: "
                    + ", ".join(sorted(unknown_criteria)),
                }
            missing = declared - set(normalized_criteria)
            if any(value == "fail" for value in normalized_criteria.values()):
                status = "fail"
            elif status == "pass" and (
                missing
                or any(value == "unknown" for value in normalized_criteria.values())
            ):
                status = "unknown"
            normalized_summary = str(summary).strip()
            if not normalized_summary:
                return {"ok": False, "error": "verdict summary must not be empty"}
            verdict_report.clear()
            verdict_report.update(
                {
                    "status": status,
                    "summary": normalized_summary,
                    "criteria": normalized_criteria,
                    "findings": [str(item) for item in (findings or ()) if str(item).strip()],
                    "missing_criteria": sorted(missing),
                    "subject": dict(context.subject),
                }
            )
            return {"ok": True, **verdict_report}

        # A child wait must remain serial even when a provider emits multiple calls.
        # The engine recognizes DeferredInteraction from ordinary tools and leaves this
        # call unanswered for durable resume.
        wait_agent.__aisuite_tool_metadata__ = ToolMetadata(
            name="wait_agent",
            category="orchestration",
            risk_level="high",
            capabilities=["wait_child"],
            requires_approval=False,
        )

        return [spawn_agent, wait_agent, cancel_agent, submit_verdict]

    def _gate_callbacks(self, context: RunExecutionContext) -> dict[str, Any]:
        run = context.claim.run
        lease = context.claim.lease

        def existing(source_key: str):
            return next(
                (
                    gate
                    for gate in self.store.list_gates(
                        context.task.id, include_internal=True
                    )
                    if gate.source_key == source_key
                ),
                None,
            )

        async def open_or_resume(
            *, kind: GateKind, source_key: str, prompt: Mapping[str, Any]
        ) -> tuple[str, Optional[Mapping[str, Any]]]:
            gate = existing(source_key)
            if gate is None or (
                gate.status is GateStatus.CANCELED
                and gate.published_at is None
            ):
                gate = self.store.prepare_run_gate(
                    run.id,
                    lease.token,
                    lease.fencing_token,
                    kind=kind,
                    source_key=source_key,
                    prompt=prompt,
                )
            if gate.status in {GateStatus.PREPARING, GateStatus.OPEN}:
                return gate.id, None
            return gate.id, gate.resolution or {"decision": gate.status.value}

        async def approver(request: PermissionRequest):
            key = f"{run.id}:permission:{request.tool_call_id or request.tool_name}"
            gate_id, resolution = await open_or_resume(
                kind=GateKind.PERMISSION,
                source_key=key,
                prompt={
                    "title": f"Allow {request.tool_name}?",
                    "description": request.reason,
                    "tool": request.tool_name,
                    "arguments": request.arguments,
                    "actions": ["approve", "reject"],
                },
            )
            if resolution is None:
                return DeferredInteraction(gate_id, "permission", {"tool": request.tool_name})
            return (
                ApprovalOutcome.ONCE
                if str(resolution.get("decision", "approve")) in {"approve", "approved", "once"}
                else ApprovalOutcome.DENY
            )

        async def question(data: dict[str, Any], tool_call_id: Optional[str] = None):
            call_id = str(tool_call_id or data.get("tool_call_id") or _stable_prompt_id(data))
            gate_id, resolution = await open_or_resume(
                kind=GateKind.QUESTION,
                source_key=f"{run.id}:question:{call_id}",
                prompt={"title": data.get("header") or "Agent question", **data},
            )
            if resolution is None:
                return DeferredInteraction(gate_id, "question", {})
            return {
                "answer": resolution.get("response")
                or resolution.get("answer")
                or resolution.get("decision", "")
            }

        async def plan(data: dict[str, Any], tool_call_id: Optional[str] = None):
            call_id = str(tool_call_id or data.get("tool_call_id") or _stable_prompt_id(data))
            gate_id, resolution = await open_or_resume(
                kind=GateKind.PLAN,
                source_key=f"{run.id}:plan:{call_id}",
                prompt={"title": "Agent plan", **data},
            )
            if resolution is None:
                return DeferredInteraction(gate_id, "plan", {})
            return {
                "approved": str(resolution.get("decision", "")) in {"approve", "approved"},
                "feedback": resolution.get("response", ""),
            }

        async def directory(data: dict[str, Any], tool_call_id: Optional[str] = None):
            call_id = str(tool_call_id or data.get("tool_call_id") or _stable_prompt_id(data))
            gate_id, resolution = await open_or_resume(
                kind=GateKind.PERMISSION,
                source_key=f"{run.id}:directory:{call_id}",
                prompt={"title": "Directory access", **data},
            )
            if resolution is None:
                return DeferredInteraction(gate_id, "directory", {})
            return dict(resolution)

        return {
            "approver": approver,
            "question": question,
            "plan": plan,
            "directory": directory,
        }

    @staticmethod
    def _mode_for(
        profile: AgentProfile, permissions: Optional[PermissionSet] = None
    ) -> Mode:
        if profile.role in {
            AgentRole.PLANNER,
            AgentRole.REVIEWER,
            AgentRole.EVALUATOR,
            AgentRole.SCORER,
            AgentRole.EXPLORER,
        }:
            requested = Mode.DISCUSS
        else:
            try:
                requested = Mode(profile.permission_mode)
            except ValueError:
                requested = Mode.INTERACTIVE
        if permissions is None:
            return requested
        try:
            return Mode(permissions.mode)
        except ValueError:
            return requested

    @staticmethod
    def _effective_tools(
        profile: AgentProfile,
        permissions: Optional[PermissionSet] = None,
        *,
        read_only: bool = False,
    ) -> set[str]:
        requested = set(profile.allowed_tools)
        if profile.role in {
            AgentRole.REVIEWER,
            AgentRole.TESTER,
            AgentRole.EVALUATOR,
            AgentRole.SCORER,
        }:
            requested.add("submit_verdict")
        if profile.allowed_child_roles and "spawn_agent" in requested:
            requested.update({"wait_agent", "cancel_agent"})
        ceiling = _ROLE_CEILINGS[profile.role]
        effective = requested if ceiling is None else requested & set(ceiling)
        if permissions is not None and permissions.tools is not None:
            effective &= set(permissions.tools)
        if permissions is not None and not permissions.network:
            effective -= {"web_search", "web_fetch"}
        if read_only:
            effective &= set(_READ_ONLY_TOOLS)
        return effective

    @staticmethod
    def _last_assistant_text(messages: list[dict[str, Any]]) -> str:
        for message in reversed(messages):
            if message.get("role") == "assistant" and message.get("content"):
                content = message["content"]
                return content if isinstance(content, str) else str(content)
        return ""

    @staticmethod
    def _system_prompt(context: RunExecutionContext) -> str:
        criteria = "\n".join(f"- {item}" for item in context.task.acceptance_criteria) or "- Complete the scoped node correctly."
        return (
            f"{context.profile.instructions}\n\n"
            "You are one isolated role in a durable multi-agent orchestration. "
            "Do not assume another role's private conversation. Inspect the workspace and "
            "the supplied evidence yourself. Never commit or push. End with a concise, "
            "evidence-backed result including files touched, checks run, and remaining risks. "
            "Reviewer, Tester, Evaluator, and Scorer roles must call submit_verdict with "
            "pass, fail, or unknown before finishing.\n\n"
            f"Acceptance criteria:\n{criteria}"
        )

    @staticmethod
    def _user_prompt(context: RunExecutionContext) -> str:
        configured_upstream = context.node.input.get("upstream", {})
        return (
            f"Task: {context.task.objective}\n\n"
            f"Current DAG node: {context.node.title or context.node.key} "
            f"({context.node.kind.value})\n"
            f"Assignment: {context.node.instructions or context.task.objective}\n\n"
            f"Constraints: {list(context.task.constraints)}\n"
            f"Candidate subject: {dict(context.subject)}\n"
            f"Durable upstream run evidence: {list(context.upstream_context)}\n"
            f"Configured upstream input: {configured_upstream}"
        )


def _stable_prompt_id(data: Mapping[str, Any]) -> str:
    import hashlib
    import json

    payload = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
