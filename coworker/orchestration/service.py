"""Recoverable eight-stage orchestration service.

This module is the coordination boundary: REST handlers submit commands, the durable
store owns truth, and a restart-safe scheduler derives the next legal action from that
truth.  No workflow progress depends on an in-memory coroutine surviving a restart.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import uuid
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from ..providers.matrix import MATRIX
from .activity import bounded_activity_text
from .blobs import BlobIntegrityError, ContentAddressedBlobStore
from .communications import TaskCommunicationService
from .context import (
    ContextBudgetCalculator,
    ContextManifestBuilder,
    ContextPolicy,
    ContextRefResolver,
    LegacyUpstreamExternalizer,
)
from .catalogs import ConfigurationCatalog
from .dag import validate_plan
from .errors import (
    ConflictError,
    IdempotencyConflict,
    LeaseConflict,
    NotFoundError,
    VersionConflict,
)
from .executor import ExecutionOutcome, OpenWorkerExecutor, RunExecutionContext
from .envelope import build_execution_envelope, render_initial_user_prompt
from .handoff_models import (
    BriefStatus,
    ContextRefDraft,
    ContextRefType,
    ContextRequirement,
    HandoffValidationError,
    TaskBriefDraft,
    TaskRelationType,
    WakeReason,
    WakeStatus,
    WorkProductKind,
    contains_secret_like,
    jsonable as handoff_jsonable,
)
from .handoff_settings import HandoffRuntimeSettings
from .models import (
    ComplexityLevel,
    EdgeCondition,
    EdgeSpec,
    EffectSafety,
    EvidenceKind,
    FailurePolicy,
    GateKind,
    GateRecord,
    GateStatus,
    JoinPolicy,
    NodeKind,
    NodeRecord,
    NodeSpec,
    OrchestrationStage,
    PlanGraph,
    PlanSpec,
    RetryPolicy,
    RiskTier,
    RunClaim,
    RunRecord,
    RunStatus,
    StageDisposition,
    TaskDomain,
    TaskRecord,
    TaskSpec,
    TaskStatus,
)
from .policy import (
    ComplexityFactors,
    RiskTier as PolicyRiskTier,
    assess_complexity,
    classify_risk,
    evaluate_acceptance,
)
from .presets import RuntimePreset, runtime_preset, runtime_presets
from .profiles import AgentProfile, AgentRole
from .quality.artifacts import ArtifactService as QualityArtifactService
from .quality.budgets import (
    BudgetExceeded as QualityBudgetExceeded,
    BudgetService as QualityBudgetService,
    ProviderUsage as QualityProviderUsage,
)
from .quality.benchmark import TaskQualityBenchmarkService
from .quality.contract_compiler import ContractCompiler
from .quality.contract_rules import READ_ONLY_RULE
from .quality.contracts import ContractRepository
from .quality.evidence import EvidenceLedger
from .quality.facade import TaskQualityFacade
from .quality.query_cache import RepositoryQueryCache
from .quality.observability import TaskQualityObservability
from .quality.repo_inventory import RepositoryInventoryService
from .quality.repo_tools import SnapshotRepoTools
from .quality.runtime_tools import (
    QUALITY_TOOL_NAMES,
    QualityRuntimeDependencies,
    TaskQualityRunToolFactory,
    quality_tool_names_for_role,
)
from .quality.settlement import (
    QualityResultSettlementService,
    QualitySettlementDependencies,
)
from .quality.repository_resolver import RepositoryResolver
from .quality.repository_snapshot import RepositorySnapshotService
from .quality.strategy_selector import StrategySelector
from .quality.state_machine import (
    WorkflowEvent,
    apply_workflow_event,
    transition_workflow_in_transaction,
)
from .quality.validators import DeterministicValidatorEngine
from .quality.workflow import QualityWorkflowDependencies, QualityWorkflowEngine
from .observability import HandoffMetrics
from .relations import TaskRelationService
from .routing import (
    ModelCandidate,
    ModelPolicy,
    ModelRouter,
    NoEligibleModelError,
    RoutingRequest,
    provider_for,
)
from .runtime import (
    DEFAULT_TASK_BUDGET,
    UNLIMITED_RUNTIME_BUDGET,
    BudgetExceededError,
    PermissionSet,
    RootPermission,
    RuntimeBudget,
    RuntimeKind,
    RuntimeLimitError,
    RuntimeLimits,
    RuntimeManager,
    RuntimeSpec,
    RuntimeStateError,
    RuntimeStatus,
)
from .store import OrchestrationStore
from .wakes import WakeService
from .work_products import WorkProductService
from .subscription_runtime import (
    SubscriptionDispatchExecutor,
    SubscriptionRuntimeRegistry,
)
from .workspace import (
    WorkspaceConflictError,
    WorkspaceError,
    WorkspaceManager,
    WorkspaceSnapshot,
)


logger = logging.getLogger("coworker.orchestration")

_ACTIVE_TASKS = frozenset(
    {
        TaskStatus.QUEUED,
        TaskStatus.RUNNING,
        TaskStatus.WAITING_HUMAN,
        TaskStatus.WAITING_CHILD,
        TaskStatus.CANCELING,
    }
)
_ACTIVE_RUNS = frozenset(
    {RunStatus.QUEUED, RunStatus.CLAIMED, RunStatus.RUNNING, RunStatus.WAITING_GATE}
)
_TERMINAL_RUNS = frozenset(
    {
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.TIMED_OUT,
        RunStatus.CANCELED,
        RunStatus.LOST,
        RunStatus.SKIPPED,
    }
)
_FAILED_RUNS = frozenset(
    {RunStatus.FAILED, RunStatus.TIMED_OUT, RunStatus.CANCELED, RunStatus.LOST}
)
_READ_ONLY_RUNTIME_TOOLS = frozenset(
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
        "get_task_context",
        "list_context_refs",
        "read_context_ref",
        "delegate_task",
        "post_task_comment",
        "list_task_comments",
        "add_task_blockers",
        "remove_task_blocker",
        "create_work_product",
        "complete_task",
        "fail_task",
    }
) | QUALITY_TOOL_NAMES
_MUTATING_DELIVERABLE_KINDS = frozenset(
    {
        WorkProductKind.IMPLEMENTATION_PATCH.value,
        WorkProductKind.PULL_REQUEST.value,
        WorkProductKind.COMMIT.value,
        WorkProductKind.BRANCH.value,
    }
)
_TERMINAL_TASKS = frozenset(
    {
        TaskStatus.FAILED,
        TaskStatus.CANCELED,
        TaskStatus.COMPLETED,
        TaskStatus.ARCHIVED,
    }
)
_DETAIL_RUN_LIMIT = 500
_DETAIL_EVIDENCE_LIMIT = 500
_DETAIL_GATE_LIMIT = 200
_DETAIL_ACTIVITY_LIMIT = 500
_DETAIL_CHILD_DEPTH = 3
_DETAIL_TREE_ROW_LIMIT = 256
_DETAIL_RUNTIME_LIMIT = 256
_RUN_ERROR_MESSAGE_LIMIT = 8_000
_LEGACY_HANDOFF_RETRY_REASON = "legacy_subscription_result_handoff"
_BOUNDED_ENVELOPE_RETRY_REASON = "bounded_work_product_envelope_handoff"
_COMPATIBILITY_RETRY_REASONS = frozenset(
    {_LEGACY_HANDOFF_RETRY_REASON, _BOUNDED_ENVELOPE_RETRY_REASON}
)
_UNSET = object()


def _iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _command(prefix: str, *parts: Any) -> str:
    raw = ":".join(str(part) for part in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"orch-{prefix}-{digest}"


def _jsonable(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    return value


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            _jsonable(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


class OrchestrationService:
    """Lifecycle coordinator, worker pool, recovery loop, and read model."""

    def __init__(
        self,
        manager: Any,
        data_dir: str | Path,
        *,
        max_concurrency: int = 8,
        poll_seconds: float = 0.25,
        executor: Optional[Any] = None,
        enforce_runtime_budgets: bool = True,
    ) -> None:
        self.manager = manager
        self.base = Path(data_dir).expanduser().resolve()
        self.base.mkdir(parents=True, exist_ok=True)
        self.store = OrchestrationStore(self.base / "orchestration.db")
        self.catalog = ConfigurationCatalog(self.base / "catalog.json")
        self.blobs = ContentAddressedBlobStore(self.base / "blobs")
        self.quality_artifacts = QualityArtifactService(self.store, self.blobs)
        self.quality_contract_compiler = ContractCompiler()
        self.quality_contracts = ContractRepository(self.store)
        self.quality_repository_resolver = RepositoryResolver()
        self.quality_snapshots = RepositorySnapshotService(
            self.store, self.quality_artifacts
        )
        self.quality_inventories = RepositoryInventoryService(
            self.store, self.quality_artifacts, self.quality_snapshots
        )
        self.quality_query_cache = RepositoryQueryCache(
            self.store, self.quality_artifacts
        )
        self.quality_repo_tools = SnapshotRepoTools(
            self.quality_snapshots,
            self.quality_inventories,
            self.quality_query_cache,
        )
        self.quality_evidence = EvidenceLedger(self.store, self.quality_snapshots)
        self.quality_strategies = StrategySelector(self.store)
        self.quality_budgets = QualityBudgetService(self.store)
        self.quality_benchmarks = TaskQualityBenchmarkService(
            Path(__file__).resolve().parent / "quality" / "benchmark_suites",
            self.base / "task-quality-benchmarks.json",
        )
        self.quality_observability = TaskQualityObservability(
            self.store,
            repo_tools=self.quality_repo_tools,
            benchmarks=self.quality_benchmarks,
        )
        self.quality_validators = DeterministicValidatorEngine(
            self.store, self.quality_artifacts, self.quality_snapshots
        )
        self.quality_workflow = QualityWorkflowEngine(
            QualityWorkflowDependencies(
                store=self.store,
                contracts=self.quality_contracts,
                snapshots=self.quality_snapshots,
                strategies=self.quality_strategies,
                artifacts=self.quality_artifacts,
                inventories=self.quality_inventories,
                validators=self.quality_validators,
                budgets=self.quality_budgets,
            )
        )
        self.quality_runtime_tools = TaskQualityRunToolFactory(
            QualityRuntimeDependencies(
                store=self.store,
                contracts=self.quality_contracts,
                snapshots=self.quality_snapshots,
                strategies=self.quality_strategies,
                inventories=self.quality_inventories,
                repo_tools=self.quality_repo_tools,
                artifacts=self.quality_artifacts,
            )
        )
        self.quality_settlement = QualityResultSettlementService(
            QualitySettlementDependencies(
                store=self.store,
                contracts=self.quality_contracts,
                strategies=self.quality_strategies,
                artifacts=self.quality_artifacts,
                snapshots=self.quality_snapshots,
            )
        )
        self.quality = TaskQualityFacade(self)
        self.legacy_upstream_externalizer = LegacyUpstreamExternalizer(self.blobs)
        self._backfill_legacy_upstream_context()
        self.workspaces = WorkspaceManager(self.base / "workspaces")
        try:
            if hasattr(manager, "orchestration_handoff_settings"):
                application_settings = {
                    "orchestration_handoff": dict(
                        manager.orchestration_handoff_settings() or {}
                    )
                }
            else:
                application_settings = dict(manager.get_settings() or {})
        except Exception:
            application_settings = {}
        raw_handoff_settings = application_settings.get("orchestration_handoff")
        if not isinstance(raw_handoff_settings, Mapping):
            orchestration_settings = application_settings.get("orchestration")
            raw_handoff_settings = (
                dict(orchestration_settings).get("communication")
                if isinstance(orchestration_settings, Mapping)
                else {}
            )
        self.handoff_settings = HandoffRuntimeSettings.from_mapping(
            raw_handoff_settings if isinstance(raw_handoff_settings, Mapping) else {}
        )
        self.context_resolver = ContextRefResolver(
            self.store,
            blob_store=self.blobs,
            policy=ContextPolicy(
                max_initial_context_tokens=self.handoff_settings.default_context_token_budget,
                max_context_refs=self.handoff_settings.max_context_refs,
                max_inline_bytes_per_ref=self.handoff_settings.max_inline_bytes_per_ref,
                max_inline_bytes_total=self.handoff_settings.max_inline_bytes_total,
                context_read_audit_enabled=self.handoff_settings.context_read_audit_enabled,
            ),
        )
        self.relations = TaskRelationService(self.store)
        self.wakes = WakeService(
            self.store,
            max_attempts=self.handoff_settings.wake_max_attempts,
            backoff_seconds=self.handoff_settings.wake_backoff_seconds,
        )
        self.communications = TaskCommunicationService(
            self.store,
            blob_store=self.blobs,
            max_batch=self.handoff_settings.max_comment_batch,
            wake_coalesce_window_ms=self.handoff_settings.wake_coalesce_window_ms,
        )
        self.work_products = WorkProductService(self.store)
        self.handoff_metrics = HandoffMetrics()
        self.max_concurrency = max(1, min(int(max_concurrency), 8))
        self.enforce_runtime_budgets = bool(enforce_runtime_budgets)
        self.runtime_limits = RuntimeLimits(max_concurrency=self.max_concurrency)
        self.poll_seconds = max(0.05, float(poll_seconds))
        self.worker_id = f"local-{uuid.uuid4().hex[:12]}"
        self.subscription_runtimes = SubscriptionRuntimeRegistry(
            manager,
            self.store,
            self.blobs,
            self.base / "subscription-runtimes",
            local_owner_eligible=bool(
                getattr(manager, "subscription_local_owner_eligible", True)
            ),
            quality_settlement=self.quality_settlement,
        )
        if executor is not None:
            # Tests and embedders may inject a complete executor boundary. Do not
            # silently wrap or replace it; the subscription catalog remains readable.
            self.executor = executor
        else:
            native_executor = OpenWorkerExecutor(
                manager,
                self.store,
                spawn_child=self._spawn_child,
                lookup_child=self._lookup_child,
                cancel_child=self._cancel_child,
                on_gate=self._gate_opened,
                blob_store=self.blobs,
                context_resolver=self.context_resolver,
                handoff_metrics=self.handoff_metrics,
                profile_resolver=self.catalog.resolve_profile,
                quality_tool_factory=self.quality_runtime_tools,
                quality_settlement=self.quality_settlement,
                wake_coalesce_window_ms=self.handoff_settings.wake_coalesce_window_ms,
            )
            self.executor = SubscriptionDispatchExecutor(
                native_executor, self.subscription_runtimes
            )
        self._loop_task: Optional[asyncio.Task[None]] = None
        self._outbox_task: Optional[asyncio.Task[None]] = None
        self._leader_heartbeat_task: Optional[asyncio.Task[None]] = None
        self._jobs: dict[str, asyncio.Task[None]] = {}
        self._closing = False
        self._wake = asyncio.Event()
        self._outbox_wake = asyncio.Event()
        self._thread_operations: set[asyncio.Task[Any]] = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._runtime_lock = threading.RLock()
        self._workspace_commit_lock = threading.RLock()
        self._finalize_lock = asyncio.Lock()
        self._runtime_trees: dict[str, RuntimeManager] = {}
        self._runtime_task_roots: dict[str, str] = {}
        self._last_lease_reap = datetime.min.replace(tzinfo=timezone.utc)
        self._last_handoff_metrics_refresh = datetime.min.replace(
            tzinfo=timezone.utc
        )
        self._scheduler_started_at: Optional[datetime] = None
        self._last_scheduler_tick_started: Optional[datetime] = None
        self._last_scheduler_success: Optional[datetime] = None
        self._last_scheduler_error_at: Optional[datetime] = None
        self._last_scheduler_error: Optional[str] = None
        self._consecutive_scheduler_failures = 0
        self._leader_token: Optional[str] = None
        self._leader_epoch: Optional[int] = None
        self._leader_lost = False
        # Keep failover bounded while tolerating transient Windows filesystem and
        # antivirus stalls. Workspace preparation runs off-loop, but a saturated
        # local disk can still delay the heartbeat long enough to fence a healthy
        # scheduler when the lease is only a few seconds long.
        self._leader_lease_seconds = 60
        self._last_leader_heartbeat: Optional[datetime] = None
        self._last_outbox_success: Optional[datetime] = None
        self._last_outbox_error: Optional[str] = None
        self._outbox_pending = 0
        self._outbox_dead_letters = 0
        self._oldest_outbox_pending_at: Optional[str] = None

    def _backfill_legacy_upstream_context(self) -> int:
        """Externalize historical upstream input without changing compatibility columns."""

        created = 0
        for task in self.store.list_all_tasks():
            if "upstream" in task.input:
                raw = task.input.get("upstream")
            elif "upstream_context" in task.input:
                raw = task.input.get("upstream_context")
            else:
                continue
            if raw in (None, "", (), [], {}):
                continue
            refs = self.store.list_context_refs(
                task.id, brief_id=task.active_brief_id
            )
            if any(
                str(item.provenance.get("source") or "") == "legacy_upstream"
                for item in refs
            ):
                continue
            payload = dict(raw) if isinstance(raw, Mapping) else {"value": raw}
            draft = self.legacy_upstream_externalizer.externalize(
                payload,
                display_name=f"Legacy upstream input for {task.title}",
            )
            self.store.backfill_legacy_upstream_ref(task.id, draft)
            created += 1
        return created

    def update_handoff_settings(self, value: Mapping[str, Any]) -> dict[str, Any]:
        """Apply a validated runtime communication policy without a service restart."""

        settings = HandoffRuntimeSettings.from_mapping(value)
        self.handoff_settings = settings
        self.context_resolver.policy = ContextPolicy(
            max_initial_context_tokens=settings.default_context_token_budget,
            max_context_refs=settings.max_context_refs,
            max_inline_bytes_per_ref=settings.max_inline_bytes_per_ref,
            max_inline_bytes_total=settings.max_inline_bytes_total,
            context_read_audit_enabled=settings.context_read_audit_enabled,
        )
        self.wakes.max_attempts = settings.wake_max_attempts
        self.wakes.backoff_seconds = settings.wake_backoff_seconds
        self.communications.max_batch = max(
            1, min(settings.max_comment_batch, 1_000)
        )
        self.communications.wake_coalesce_window_ms = (
            settings.wake_coalesce_window_ms
        )
        native_executor = getattr(self.executor, "native", self.executor)
        handoff_tools = getattr(native_executor, "handoff_tools", None)
        if handoff_tools is not None:
            handoff_tools.wake_coalesce_window_ms = (
                settings.wake_coalesce_window_ms
            )
        return settings.to_dict()

    # -- process lifecycle ------------------------------------------------
    async def start(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            return
        # Acquire the startup/leader fence before any cancellation-resistant
        # recovery read.  Otherwise a canceled verifier and a replacement process
        # can recover the same database concurrently.  Event-chain verification is
        # still the first recovery action and no scheduler work begins before it.
        token, epoch = self.store.acquire_scheduler_leader(
            self.worker_id, lease_seconds=self._leader_lease_seconds
        )
        self.store.bind_scheduler_fence(self.worker_id, token, epoch)
        self._leader_token = token
        self._leader_epoch = epoch
        self._leader_lost = False
        self._closing = False
        self._leader_heartbeat_task = asyncio.create_task(
            self._leader_heartbeat_loop(),
            name="openworker-orchestration-leader-heartbeat",
        )
        try:
            await self._durable_to_thread(self.store.verify_event_chain)
            await self._durable_to_thread(
                self.store.reap_expired_leases,
                command_id=f"startup-reap-{uuid.uuid4().hex}",
            )
            await self._durable_to_thread(self._begin_task_quality_recovery)
            await self._durable_to_thread(self.wakes.recover_expired_claims)
            await self._durable_to_thread(self.wakes.activate_due)
            await self._durable_to_thread(self._repair_legacy_final_rejections)
            await self._durable_to_thread(self._reconcile_orphaned_gate_checkpoints)
            await self._durable_to_thread(
                self._repair_superseded_policy_skip_gates
            )
            await self._durable_to_thread(
                self._repair_legacy_subscription_work_products
            )
            await self._durable_to_thread(
                self._repair_evaluator_adjudicated_gates
            )
            await self._durable_to_thread(
                self._repair_legacy_verification_reconciliation_gates
            )
            await self._durable_to_thread(self.store.verify_relation_consistency)
            await self._recover_incomplete_deliveries()
            await self._durable_to_thread(self._recover_delivered_publications)
            await self._recover_pending_run_commits()
            await self._durable_to_thread(self._cleanup_archived_workspaces)
            await self._durable_to_thread(self._rebuild_all_runtimes)
            await self._durable_to_thread(self._finish_task_quality_recovery)
            outbox = await self._durable_to_thread(self.store.outbox_health)
            self._outbox_pending = int(outbox["pending"])
            self._outbox_dead_letters = int(outbox["dead_letters"])
            self._oldest_outbox_pending_at = outbox["oldest_pending_at"]
            if self._leader_lost:
                raise LeaseConflict("scheduler leader lease was lost during recovery")
        except BaseException:
            if self._leader_heartbeat_task is not None:
                self._leader_heartbeat_task.cancel()
                await asyncio.gather(
                    self._leader_heartbeat_task, return_exceptions=True
                )
                self._leader_heartbeat_task = None
            await self._drain_thread_operations()
            try:
                self.store.release_scheduler_leader(self.worker_id, token, epoch)
            except Exception:
                logger.exception(
                    "could not release scheduler leader after failed startup"
                )
            self._leader_token = None
            self._leader_epoch = None
            raise
        self._scheduler_started_at = datetime.now(timezone.utc)
        self._last_scheduler_tick_started = None
        self._last_scheduler_success = None
        self._last_scheduler_error_at = None
        self._last_scheduler_error = None
        self._consecutive_scheduler_failures = 0
        self._loop = asyncio.get_running_loop()
        self._loop_task = asyncio.create_task(
            self._scheduler_loop(), name="openworker-orchestration"
        )
        self._outbox_task = asyncio.create_task(
            self._outbox_loop(), name="openworker-orchestration-outbox"
        )
        self._wake.set()
        self._outbox_wake.set()

    async def stop(self) -> None:
        self._closing = True
        self._wake.set()
        self._outbox_wake.set()
        control_tasks = [
            task
            for task in (
                self._loop_task,
                self._outbox_task,
            )
            if task is not None
        ]
        for task in control_tasks:
            task.cancel()
        if control_tasks:
            await asyncio.gather(*control_tasks, return_exceptions=True)
        self._loop_task = None
        self._outbox_task = None
        jobs = list(self._jobs.values())
        for job in jobs:
            job.cancel()
        if jobs:
            await asyncio.gather(*jobs, return_exceptions=True)
        self._jobs.clear()
        # A cancelled task waiting on asyncio.to_thread does not cancel its OS
        # thread.  Never relinquish the epoch until every leader-owned operation
        # has actually returned.
        await self._drain_thread_operations()
        # Keep renewing leadership while scheduler/jobs drain.  Cancel the
        # heartbeat only after no old thread can still cross a commit boundary.
        if self._leader_heartbeat_task is not None:
            self._leader_heartbeat_task.cancel()
            await asyncio.gather(
                self._leader_heartbeat_task, return_exceptions=True
            )
            self._leader_heartbeat_task = None
        if self._leader_token is not None and self._leader_epoch is not None:
            try:
                self.store.release_scheduler_leader(
                    self.worker_id, self._leader_token, self._leader_epoch
                )
            except Exception:
                logger.exception("could not release orchestration scheduler leader lease")
        self._leader_token = None
        self._leader_epoch = None
        self.store.close()
        try:
            self.catalog.close()
        except Exception:
            logger.exception("could not close orchestration configuration catalog")

    def wake(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(self._wake.set)
            loop.call_soon_threadsafe(self._outbox_wake.set)

    async def _durable_to_thread(self, function: Any, /, *args: Any, **kwargs: Any):
        """Run a leader-owned blocking operation without abandoning it on cancel.

        Cancelling ``asyncio.to_thread`` only cancels its asyncio waiter; the OS thread
        keeps running. Releasing the scheduler leader at that point would allow a new
        process to overlap the old filesystem/database commit. This wrapper delays
        cancellation propagation until the thread has actually settled, so ``stop`` and
        failed startup can safely release leadership afterwards.
        """

        operation = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
        self._thread_operations.add(operation)
        cancelled = False
        try:
            while not operation.done():
                try:
                    await asyncio.shield(operation)
                except asyncio.CancelledError:
                    cancelled = True
            if cancelled:
                try:
                    operation.result()
                except Exception:
                    logger.exception(
                        "leader-owned blocking operation failed while cancellation drained"
                    )
                raise asyncio.CancelledError
            return operation.result()
        finally:
            self._thread_operations.discard(operation)

    async def _drain_thread_operations(self) -> None:
        """Wait for all cancellation-resistant leader operations to settle."""

        while self._thread_operations:
            pending = tuple(self._thread_operations)
            await asyncio.gather(*pending, return_exceptions=True)

    async def _scheduler_loop(self) -> None:
        while not self._closing:
            try:
                now = datetime.now(timezone.utc)
                self._last_scheduler_tick_started = now
                if (now - self._last_lease_reap).total_seconds() >= 5:
                    reaped = self.store.reap_expired_leases(
                        now=now,
                        command_id=f"periodic-reap-{uuid.uuid4().hex}",
                    )
                    self._last_lease_reap = now
                    if reaped:
                        self._rebuild_all_runtimes()
                self._resume_completed_child_waits()
                await self._recover_pending_run_commits()
                await self._dispatch_ready_wakes()
                await self._coordinate_tasks()
                await self._claim_work()
                if (
                    now - self._last_handoff_metrics_refresh
                ).total_seconds() >= 5:
                    await self._durable_to_thread(
                        self._refresh_handoff_metrics, now=now
                    )
                    self._last_handoff_metrics_refresh = now
                self._record_scheduler_success()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._record_scheduler_failure(exc)
                logger.exception("orchestration scheduler tick failed")
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_seconds)
            except asyncio.TimeoutError:
                pass

    async def _leader_heartbeat_loop(self) -> None:
        interval = max(1.0, self._leader_lease_seconds / 3)
        # Shutdown deliberately keeps this loop alive until all leader-owned
        # scheduler/job threads are drained; `_closing` stops new work but must
        # not let the lease expire underneath an in-flight commit.
        while self._leader_token is not None and self._leader_epoch is not None:
            try:
                await asyncio.sleep(interval)
                if self._leader_token is None or self._leader_epoch is None:
                    raise LeaseConflict("scheduler leader identity is missing")
                await self._durable_to_thread(
                    self.store.heartbeat_scheduler_leader,
                    self.worker_id,
                    self._leader_token,
                    self._leader_epoch,
                    lease_seconds=self._leader_lease_seconds,
                )
                self._last_leader_heartbeat = datetime.now(timezone.utc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._leader_lost = True
                self._record_scheduler_failure(exc)
                logger.exception("orchestration scheduler leader lease was lost")
                if self._loop_task is not None:
                    self._loop_task.cancel()
                if self._outbox_task is not None:
                    self._outbox_task.cancel()
                for job in tuple(self._jobs.values()):
                    job.cancel()
                return

    async def _outbox_loop(self) -> None:
        while not self._closing:
            self._outbox_wake.clear()
            try:
                await self._publish_outbox()
                metrics = await self._durable_to_thread(self.store.outbox_health)
                self._outbox_pending = int(metrics["pending"])
                self._outbox_dead_letters = int(metrics["dead_letters"])
                self._oldest_outbox_pending_at = metrics["oldest_pending_at"]
                self._last_outbox_success = datetime.now(timezone.utc)
                self._last_outbox_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_outbox_error = f"{type(exc).__name__}: {exc}"[:2_000]
                logger.exception("orchestration outbox tick failed")
            try:
                await asyncio.wait_for(
                    self._outbox_wake.wait(), timeout=self.poll_seconds
                )
            except asyncio.TimeoutError:
                pass

    def _record_scheduler_success(self) -> None:
        self._last_scheduler_success = datetime.now(timezone.utc)
        self._last_scheduler_error = None
        self._consecutive_scheduler_failures = 0

    def _record_scheduler_failure(self, error: BaseException) -> None:
        self._last_scheduler_error_at = datetime.now(timezone.utc)
        self._last_scheduler_error = f"{type(error).__name__}: {error}"[:2_000]
        self._consecutive_scheduler_failures += 1

    def health_snapshot(self) -> dict[str, Any]:
        """Return an operational readiness view without touching durable state."""

        loop_alive = self._loop_task is not None and not self._loop_task.done()
        outbox_alive = self._outbox_task is not None and not self._outbox_task.done()
        leader_heartbeat_alive = (
            self._leader_heartbeat_task is not None
            and not self._leader_heartbeat_task.done()
        )
        failure_limit = 3
        stale_after_seconds = max(30.0, self.poll_seconds * 20.0)
        now = datetime.now(timezone.utc)
        tick_in_progress = bool(
            self._last_scheduler_tick_started
            and (
                self._last_scheduler_success is None
                or self._last_scheduler_success < self._last_scheduler_tick_started
            )
        )
        scheduler_stale = bool(
            tick_in_progress
            and self._last_scheduler_tick_started
            and (now - self._last_scheduler_tick_started).total_seconds()
            > stale_after_seconds
        )
        outbox_stale_after_seconds = max(60.0, self.poll_seconds * 40.0)
        outbox_reference = self._last_outbox_success or self._scheduler_started_at
        outbox_stale = bool(
            outbox_alive
            and self._outbox_pending > 0
            and outbox_reference
            and (now - outbox_reference).total_seconds() > outbox_stale_after_seconds
        )
        ready = (
            loop_alive
            and outbox_alive
            and leader_heartbeat_alive
            and not self._leader_lost
            and not self._closing
            and not scheduler_stale
            and not outbox_stale
            and self._outbox_dead_letters == 0
            and self._consecutive_scheduler_failures < failure_limit
        )
        state = (
            "stopping"
            if self._closing
            else "stopped"
            if not loop_alive
            else "unhealthy"
            if scheduler_stale
            or outbox_stale
            or not outbox_alive
            or not leader_heartbeat_alive
            or self._leader_lost
            or self._outbox_dead_letters > 0
            or self._consecutive_scheduler_failures >= failure_limit
            else "degraded"
            if self._consecutive_scheduler_failures
            else "healthy"
        )
        return {
            "ready": ready,
            "state": state,
            "loop_alive": loop_alive,
            "leader": {
                "held": self._leader_token is not None and not self._leader_lost,
                "epoch": self._leader_epoch,
                "heartbeat_alive": leader_heartbeat_alive,
                "last_heartbeat_at": _iso(self._last_leader_heartbeat),
            },
            "outbox": {
                "loop_alive": outbox_alive,
                "last_success_at": _iso(self._last_outbox_success),
                "last_error": self._last_outbox_error,
                "pending": self._outbox_pending,
                "dead_letters": self._outbox_dead_letters,
                "oldest_pending_at": self._oldest_outbox_pending_at,
                "stale": outbox_stale,
                "stale_after_seconds": outbox_stale_after_seconds,
            },
            "closing": self._closing,
            "started_at": _iso(self._scheduler_started_at),
            "last_tick_started_at": _iso(self._last_scheduler_tick_started),
            "last_success_at": _iso(self._last_scheduler_success),
            "last_error_at": _iso(self._last_scheduler_error_at),
            "last_error": self._last_scheduler_error,
            "consecutive_failures": self._consecutive_scheduler_failures,
            "failure_limit": failure_limit,
            "stale": scheduler_stale,
            "stale_after_seconds": stale_after_seconds,
            "active_jobs": sum(1 for job in self._jobs.values() if not job.done()),
            "handoff": {
                "settings": self.handoff_settings.to_dict(),
                "metrics": self.handoff_metrics.snapshot(),
            },
            "task_quality": self.quality_observability.snapshot(),
        }

    async def _coordinate_tasks(self) -> None:
        for task in self._all_tasks(statuses=tuple(_ACTIVE_TASKS)):
            if task.status is TaskStatus.CANCELING:
                # Cancellation is a durable intent. Re-drive the whole subtree on
                # every tick so a crash halfway through the cascade converges after
                # restart instead of orphaning a live descendant.
                await self._durable_to_thread(self.cancel_task, task.id)
                fresh = self.store.get_task(task.id)
                subtree = [fresh, *self._descendants(fresh.id)]
                runs = [
                    run
                    for current in subtree
                    for run in self.store.list_runs(current.id)
                ]
                if (
                    not any(run.status in _ACTIVE_RUNS for run in runs)
                    and all(
                        child.status in _TERMINAL_TASKS
                        for child in subtree[1:]
                    )
                ):
                    self._transition_status(
                        fresh, TaskStatus.CANCELED, "cancel-finished"
                    )
                continue
            if task.status in {TaskStatus.WAITING_HUMAN, TaskStatus.WAITING_CHILD}:
                continue
            try:
                # Candidate hashing, merge and atomic publication can perform
                # substantial filesystem I/O. Keep it off the scheduler event loop
                # so lease heartbeats and cancellation remain responsive.
                await self._durable_to_thread(self._advance_task, task.id)
            except (ConflictError, VersionConflict):
                continue  # a gate/API command won the optimistic race; next tick re-derives
            except Exception as exc:
                logger.exception("could not advance orchestration task %s", task.id)
                self._block_task(task.id, "coordinator_error", str(exc))

    async def _dispatch_ready_wakes(self) -> None:
        """Turn durable wake intent into at-most-one run delivery per claim."""

        await self._durable_to_thread(self.wakes.activate_due)
        await self._durable_to_thread(self.wakes.recover_expired_claims)
        for _ in range(64):
            wake = await self._durable_to_thread(
                self.wakes.claim_ready_wake,
                self.worker_id,
                claim_seconds=60,
            )
            if wake is None:
                return
            try:
                task = self.store.get_task(wake.target_task_id)
                if task.status in _TERMINAL_TASKS or task.status is TaskStatus.CANCELING:
                    self.store.cancel_claimed_wake(
                        wake.id,
                        reason=f"target task is {task.status.value}",
                        command_id=_command("wake-cancel-terminal", wake.id),
                    )
                    continue
                if bool(wake.payload.get("notice_only")):
                    self.wakes.mark_delivered(wake.id)
                    self.wakes.mark_completed(wake.id)
                    self._observe_wake_delivery(wake)
                    continue
                if task.status in {TaskStatus.QUEUED, TaskStatus.RUNNING}:
                    try:
                        await self._durable_to_thread(self._advance_task, task.id)
                    except (ConflictError, VersionConflict):
                        pass
                candidates = self.store.list_runs(
                    task.id,
                    statuses=(
                        RunStatus.QUEUED,
                        RunStatus.CLAIMED,
                        RunStatus.RUNNING,
                        RunStatus.WAITING_GATE,
                    ),
                    limit=100,
                    newest=True,
                )
                selected = next(
                    (
                        item
                        for item in reversed(candidates)
                        if wake.target_run_id == item.id
                    ),
                    None,
                )
                if selected is None:
                    selected = next(
                        (
                            item
                            for item in candidates
                            if item.status is RunStatus.QUEUED
                        ),
                        None,
                    )
                if selected is None:
                    # A task-level assignment can race the worker claim: by the
                    # time the wake dispatcher observes it, the only eligible run
                    # may already be claimed or running. All are valid live binding
                    # targets in the durable store.
                    selected = next(
                        (
                            item
                            for item in candidates
                            if item.status
                            in {
                                RunStatus.CLAIMED,
                                RunStatus.RUNNING,
                                RunStatus.WAITING_GATE,
                            }
                        ),
                        None,
                    )
                if selected is not None:
                    self.wakes.bind_to_run(
                        wake.id, selected.id, owner=self.worker_id
                    )
                    self._observe_wake_delivery(wake)
                    continue
                # Refresh after _advance_task: a completed DAG may have moved from
                # execution to evaluation during this dispatch attempt. Assignment
                # intent is then already satisfied even though no live run remains.
                task = self.store.get_task(task.id)
                if (
                    task.status in _TERMINAL_TASKS
                    or task.status is TaskStatus.CANCELING
                ):
                    self.store.cancel_claimed_wake(
                        wake.id,
                        reason=f"target task is {task.status.value}",
                        command_id=_command("wake-cancel-terminal", wake.id),
                    )
                    continue
                if (
                    wake.reason in {WakeReason.ASSIGNMENT, WakeReason.TASK_ASSIGNED}
                    and task.current_stage
                    in {
                        OrchestrationStage.INTER_STEP_EVALUATION,
                        OrchestrationStage.FINAL_ACCEPTANCE,
                        OrchestrationStage.ARCHIVE,
                    }
                ):
                    self.wakes.mark_delivered(wake.id)
                    self.wakes.mark_completed(wake.id)
                    self._observe_wake_delivery(wake)
                    continue
                self.wakes.defer_wake(
                    wake.id,
                    not_before=datetime.now(timezone.utc)
                    + timedelta(seconds=self._wake_deferral_seconds(wake.attempts)),
                )
            except Exception as exc:
                logger.exception("could not deliver orchestration wake %s", wake.id)
                failed = self.wakes.mark_failed(wake.id, str(exc))
                self.handoff_metrics.increment("orchestration_wake_failures_total")
                # Retain the preview-era spelling for diagnostic consumers.
                self.handoff_metrics.increment("orchestration_wake_failed_total")
                if failed.status is WakeStatus.FAILED:
                    self.handoff_metrics.increment(
                        "orchestration_wake_dead_letter_total"
                    )

    def _wake_deferral_seconds(self, attempts: int) -> float:
        """Back off routine no-run races without dead-lettering valid intent."""

        base = max(
            1.0,
            float(self.wakes.backoff_seconds),
            float(self.poll_seconds) * 4,
        )
        exponent = min(6, max(0, int(attempts) - 1))
        return min(60.0, base * (2**exponent))

    def _observe_wake_delivery(self, wake: Any) -> None:
        self.handoff_metrics.increment("orchestration_wake_delivered_total")
        self.handoff_metrics.observe(
            "orchestration_wake_delivery_latency_seconds",
            max(0.0, (datetime.now(timezone.utc) - wake.created_at).total_seconds()),
        )

    def _refresh_handoff_metrics(
        self, *, now: Optional[datetime] = None
    ) -> None:
        snapshot = self.store.handoff_observability_snapshot(now=now)
        self.handoff_metrics.observe(
            "orchestration_wakes_pending", snapshot["wakes_pending"]
        )
        self.handoff_metrics.set_counter(
            "orchestration_wake_coalesced_total",
            int(snapshot["wake_coalesced_total"]),
        )
        self.handoff_metrics.observe(
            "orchestration_task_blocked_duration_seconds",
            snapshot["task_blocked_duration_seconds"],
        )

    async def _claim_work(self) -> None:
        capacity = self.max_concurrency - len(self._jobs)
        if capacity <= 0 or not self._has_queued_runs():
            return
        for _ in range(capacity):
            claim = self.store.claim_next_run(
                self.worker_id,
                lease_seconds=60,
                command_id=f"claim-{uuid.uuid4().hex}",
            )
            if claim is None:
                break
            job = asyncio.create_task(
                self._execute_claim(claim), name=f"orchestration-run-{claim.run.id}"
            )
            self._jobs[claim.run.id] = job
            job.add_done_callback(
                lambda _done, run_id=claim.run.id: self._job_finished(run_id)
            )

    def _job_finished(self, run_id: str) -> None:
        self._jobs.pop(run_id, None)
        self.wake()

    def _has_queued_runs(self) -> bool:
        for task in self._all_tasks(statuses=(TaskStatus.QUEUED, TaskStatus.RUNNING)):
            if any(run.status is RunStatus.QUEUED for run in self.store.list_runs(task.id)):
                return True
        return False

    async def _publish_outbox(self) -> None:
        items = await self._durable_to_thread(
            self.store.claim_outbox, self.worker_id, limit=20
        )
        for item in items:
            try:
                # The broker cannot enforce our SQLite fencing token, so renew and
                # validate immediately before the bounded external effect.  event_id
                # remains the consumer deduplication key for crash-boundary replay.
                await self._durable_to_thread(self.store.renew_scheduler_fence)
                await asyncio.wait_for(
                    self.manager.broadcast_event(
                        {"type": "orchestration_event", "data": dict(item.payload)}
                    ),
                    timeout=2.0,
                )
                await self._durable_to_thread(
                    self.store.mark_outbox_published, item.id, self.worker_id
                )
            except Exception as exc:
                if item.attempts >= 10:
                    await self._durable_to_thread(
                        self.store.mark_outbox_dead_lettered,
                        item.id,
                        self.worker_id,
                        str(exc),
                    )
                    self._outbox_dead_letters += 1
                    continue
                delay = min(300.0, float(2 ** min(item.attempts, 8)))
                jitter = (
                    int(hashlib.sha256(item.id.encode("utf-8")).hexdigest()[:4], 16)
                    / 0xFFFF
                    * min(30.0, delay * 0.2)
                )
                await self._durable_to_thread(
                    self.store.mark_outbox_failed,
                    item.id,
                    self.worker_id,
                    str(exc),
                    retry_at=datetime.now(timezone.utc)
                    + timedelta(seconds=delay + jitter),
                )

    # -- runtime projection and recovery --------------------------------
    def _begin_task_quality_recovery(self) -> None:
        """Persist exact active V2 checkpoints before rebuilding runtime state."""

        with self.store._read() as connection:
            rows = connection.execute(
                """
                SELECT id FROM orch_tasks
                WHERE active_contract_id IS NOT NULL
                  AND workflow_status IN ('running','validating','reviewing','repairing')
                ORDER BY created_at, id
                """
            ).fetchall()
        for row in rows:
            apply_workflow_event(
                self.store,
                task_id=str(row["id"]),
                event=WorkflowEvent.CRASH_DETECTED,
                reason_code="process_restart_recovery",
                command_id=f"quality-recovery-begin:{row['id']}",
            )

    def _finish_task_quality_recovery(self) -> None:
        """Resume only the server-persisted checkpoint after runtime rebuild."""

        with self.store._read() as connection:
            rows = connection.execute(
                """
                SELECT id, status FROM orch_tasks
                WHERE active_contract_id IS NOT NULL AND workflow_status='recovering'
                ORDER BY created_at, id
                """
            ).fetchall()
        for row in rows:
            event = (
                WorkflowEvent.RECOVERY_UNCERTAIN
                if row["status"] == TaskStatus.NEEDS_RECONCILIATION.value
                else WorkflowEvent.RECOVERY_SUCCEEDED
            )
            apply_workflow_event(
                self.store,
                task_id=str(row["id"]),
                event=event,
                reason_code=(
                    "recovery_uncertain"
                    if event is WorkflowEvent.RECOVERY_UNCERTAIN
                    else None
                ),
                clear_reason=event is WorkflowEvent.RECOVERY_SUCCEEDED,
                command_id=f"quality-recovery-finish:{row['id']}",
            )

    @staticmethod
    def _task_runtime_id(task_id: str) -> str:
        return f"task:{task_id}"

    @staticmethod
    def _run_runtime_id(run_id: str) -> str:
        return f"run:{run_id}"

    @staticmethod
    def _budget(value: Mapping[str, Any] | None) -> RuntimeBudget:
        raw = dict(value or {})
        return RuntimeBudget(
            model_calls=int(raw.get("model_calls", DEFAULT_TASK_BUDGET.model_calls)),
            tool_calls=int(raw.get("tool_calls", DEFAULT_TASK_BUDGET.tool_calls)),
            tokens=int(raw.get("tokens", raw.get("reported_tokens", DEFAULT_TASK_BUDGET.tokens))),
            wall_seconds=int(
                raw.get("wall_seconds", raw.get("active_seconds", DEFAULT_TASK_BUDGET.wall_seconds))
            ),
        )

    @staticmethod
    def _bounded_budget(
        usage: RuntimeBudget, ceiling: RuntimeBudget
    ) -> RuntimeBudget:
        """Clamp observed usage for ledger recovery without changing audit truth."""

        return RuntimeBudget(
            model_calls=min(usage.model_calls, ceiling.model_calls),
            tool_calls=min(usage.tool_calls, ceiling.tool_calls),
            tokens=min(usage.tokens, ceiling.tokens),
            wall_seconds=min(usage.wall_seconds, ceiling.wall_seconds),
        )

    @staticmethod
    def _permission_from_dict(value: Mapping[str, Any]) -> PermissionSet:
        return PermissionSet(
            tools=(
                frozenset(str(item) for item in value.get("tools", ()))
                if value.get("tools") is not None
                else None
            ),
            commands=(
                frozenset(str(item) for item in value.get("commands", ()))
                if value.get("commands") is not None
                else None
            ),
            roots=tuple(value.get("roots") or ()) if value.get("roots") is not None else None,
            mode=str(value.get("mode") or "interactive"),
            network=bool(value.get("network", False)),
            external_writes=bool(value.get("external_writes", False)),
            can_delegate=bool(value.get("can_delegate", False)),
        )

    def _task_permissions(self, task: TaskRecord) -> PermissionSet:
        read_only = bool(task.policy.get("read_only", False))
        runtime_meta = dict(task.input.get("_runtime") or {})
        persisted = runtime_meta.get("effective_permissions")
        if isinstance(persisted, Mapping):
            permissions = self._permission_from_dict(persisted)
            if not read_only:
                return permissions
            read_only_roots = (
                (RootPermission(task.workspace, writable=False),)
                if task.workspace
                else ()
            )
            # Persisted delegated grants can come from an older service version.
            # Re-intersect them with the current task's immutable read-only ceiling
            # so recovery can never restore write authority.
            return permissions.intersect(
                PermissionSet(
                    tools=_READ_ONLY_RUNTIME_TOOLS,
                    commands=frozenset(),
                    roots=read_only_roots,
                    mode="plan",
                    network=permissions.network,
                    external_writes=False,
                    can_delegate=permissions.can_delegate,
                )
            )
        roots = None
        if task.workspace:
            roots = (
                RootPermission(
                    task.workspace,
                    writable=not read_only,
                ),
            )
        elif read_only:
            roots = ()
        return PermissionSet(
            tools=_READ_ONLY_RUNTIME_TOOLS if read_only else None,
            commands=frozenset() if read_only else None,
            roots=roots,
            mode="plan" if read_only else "interactive",
            network=bool(task.policy.get("network", False)),
            external_writes=(
                bool(task.policy.get("external_writes", False))
                and not read_only
            ),
            can_delegate=True,
        )

    def _profile_permissions(self, task: TaskRecord, profile: Any) -> PermissionSet:
        task_read_only = bool(task.policy.get("read_only", False))
        read_only_role = profile.role in {
            AgentRole.PLANNER,
            AgentRole.REVIEWER,
            AgentRole.EVALUATOR,
            AgentRole.SCORER,
            AgentRole.EXPLORER,
        }
        roots = None
        if task.workspace:
            roots = (
                RootPermission(
                    task.workspace,
                    writable=(
                        not read_only_role
                        and not task_read_only
                    ),
                ),
            )
        elif task_read_only:
            roots = ()
        tools = set(profile.allowed_tools)
        if bool(task.input.get("task_quality_v2")):
            tools.update(quality_tool_names_for_role(profile.role))
        if profile.role in {
            AgentRole.REVIEWER,
            AgentRole.TESTER,
            AgentRole.EVALUATOR,
            AgentRole.SCORER,
        }:
            tools.add("submit_verdict")
        if profile.allowed_child_roles and "spawn_agent" in tools:
            tools.update({"wait_agent", "cancel_agent"})
        if task_read_only:
            tools &= set(_READ_ONLY_RUNTIME_TOOLS)
        if not bool(task.policy.get("network", False)):
            tools -= {"web_search", "web_fetch"}
        configured_commands = profile.metadata.get("allowed_commands")
        return PermissionSet(
            tools=frozenset(tools),
            commands=(
                frozenset()
                if task_read_only
                else frozenset(str(item) for item in configured_commands)
                if configured_commands is not None
                else None
            ),
            roots=roots,
            mode=(
                "plan"
                if task_read_only
                else profile.permission_mode
            ),
            network=(
                bool(task.policy.get("network", False))
                and bool(profile.metadata.get("network", False))
            ),
            external_writes=(
                not task_read_only
                and bool(task.policy.get("external_writes", False))
                and bool(profile.metadata.get("external_writes", False))
            ),
            can_delegate=bool(profile.allowed_child_roles and profile.max_children),
        )

    def _profile_for_node(self, node: NodeRecord | NodeSpec) -> AgentProfile:
        """Resolve the executable profile frozen into an immutable plan revision.

        New plans carry the complete profile spec.  The reference-only branch is kept
        solely for plans written by the first orchestration preview; its recorded hash
        is still verified so an upgrade cannot silently change a resumed run.
        """

        snapshot = dict(node.metadata.get("profile_snapshot") or {})
        raw_spec = snapshot.get("spec")
        if isinstance(raw_spec, Mapping):
            profile = AgentProfile.from_dict(raw_spec)
        else:
            profile = self.catalog.resolve_profile(
                str(snapshot.get("profile_id") or node.agent),
                int(snapshot["version"]) if snapshot.get("version") else None,
            )
        expected = str(snapshot.get("content_hash") or "")
        if expected and profile.content_hash != expected:
            raise ConflictError(
                f"frozen profile hash mismatch for {profile.profile_id}@{profile.version}"
            )
        return profile

    @staticmethod
    def _policy_for_node(node: NodeRecord | NodeSpec) -> ModelPolicy:
        snapshot = dict(node.metadata.get("model_policy_snapshot") or {})
        raw_spec = snapshot.get("spec")
        if not isinstance(raw_spec, Mapping):
            raise ConflictError("plan does not contain a replayable model-policy snapshot")
        policy = ModelPolicy(**dict(raw_spec))
        expected = str(snapshot.get("content_hash") or "")
        if expected and _canonical_hash(policy.audit_dict()) != expected:
            raise ConflictError(
                f"frozen model-policy hash mismatch for {policy.policy_id}@{policy.version}"
            )
        return policy

    @staticmethod
    def _profile_mutates_candidate(profile: AgentProfile) -> bool:
        return profile.role in {AgentRole.WORKER, AgentRole.INTEGRATOR}

    def _plan_has_worker_producer(self, spec: PlanSpec) -> bool:
        """Return whether a validated plan owns writable execution itself.

        ``profile_id`` drives the generated node only for the legacy plan shape.  A
        runtime preset or an explicit plan instead names every executable profile,
        so a non-Worker task container is safe only when the graph contains a real
        Execute node backed by a server-resolved Worker profile.  Requiring both the
        formal node kind and role keeps this exception fail-closed.
        """

        return any(
            node.kind is NodeKind.EXECUTE
            and self.catalog.resolve_profile(node.agent).role is AgentRole.WORKER
            for node in spec.nodes
        )

    def _run_budget(
        self, task: TaskRecord, graph: PlanGraph, node: NodeRecord
    ) -> RuntimeBudget:
        if not self.enforce_runtime_budgets:
            return UNLIMITED_RUNTIME_BUDGET
        quality = self._quality_budget_binding(task.id, node.key)
        if quality is not None:
            mode, _ledger_id, allocation = quality
            if mode == "unlimited":
                return UNLIMITED_RUNTIME_BUDGET
            return RuntimeBudget(
                model_calls=int(allocation.get("model_calls", 0) or 0),
                tool_calls=int(allocation.get("tool_calls", 0) or 0),
                tokens=int(
                    allocation.get(
                        "max_reported_tokens",
                        allocation.get("reserved_reported_tokens", 0),
                    )
                    or 0
                ),
                wall_seconds=int(allocation.get("active_seconds", 0) or 0),
            )
        explicit = task.budget.get("run_budget") if isinstance(task.budget, Mapping) else None
        if isinstance(explicit, Mapping):
            return self._budget(explicit)
        total = self._budget(task.budget)
        # Every logical node may be queued in the recovered projection. Divide by
        # total graph work units, not just concurrently claimed slots, so reservations
        # can never exceed the task ceiling merely because the graph is wide.
        slots = max(1, len(graph.nodes))

        def share(value: int) -> int:
            return value // slots

        return RuntimeBudget(
            share(total.model_calls),
            share(total.tool_calls),
            share(total.tokens),
            share(total.wall_seconds),
        )

    def _quality_budget_binding(
        self, task_id: str, node_key: str
    ) -> tuple[str, str | None, dict[str, int]] | None:
        """Resolve one frozen strategy allocation; never manufacture equal shares."""

        with self.store._read() as connection:
            row = connection.execute(
                """
                SELECT t.active_budget_ledger_id, s.budget_profile_json,
                       p.metadata_json AS active_plan_metadata_json
                FROM orch_tasks t
                LEFT JOIN orch_execution_strategies s
                  ON s.id=t.active_strategy_id
                LEFT JOIN orch_plans p ON p.id=t.active_plan_id
                WHERE t.id=? AND t.active_strategy_id IS NOT NULL
                """,
                (task_id,),
            ).fetchone()
            if row is None or row["budget_profile_json"] is None:
                return None
            ledger = (
                connection.execute(
                    "SELECT mode FROM orch_budget_ledgers WHERE id=?",
                    (row["active_budget_ledger_id"],),
                ).fetchone()
                if row["active_budget_ledger_id"]
                else None
            )
        profile = json.loads(row["budget_profile_json"])
        allocations = dict(profile.get("node_allocations") or {})
        plan_metadata = json.loads(row["active_plan_metadata_json"] or "{}")
        repair_allocations = plan_metadata.get("repair_node_allocations")
        if isinstance(repair_allocations, Mapping):
            allocations = dict(repair_allocations)
        raw = allocations.get(node_key)
        if not isinstance(raw, Mapping):
            raise ConflictError(
                f"frozen quality strategy has no budget allocation for node {node_key}"
            )
        mode = str(ledger["mode"] if ledger is not None else profile.get("mode") or "hard")
        return (
            mode,
            str(row["active_budget_ledger_id"])
            if row["active_budget_ledger_id"]
            else None,
            {str(key): int(value) for key, value in raw.items()},
        )

    @staticmethod
    def _quality_reservation_amounts(allocation: Mapping[str, int]) -> dict[str, int]:
        return {
            "model_calls": int(allocation.get("model_calls", 0) or 0),
            "tool_calls": int(allocation.get("tool_calls", 0) or 0),
            "reported_tokens": int(
                allocation.get("reserved_reported_tokens", 0) or 0
            ),
            "active_seconds": int(allocation.get("active_seconds", 0) or 0),
            "tool_payload_bytes": int(
                allocation.get("tool_payload_bytes", 0) or 0
            ),
        }

    def _attempt_budget(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        node: NodeRecord,
        spent: RuntimeBudget,
    ) -> RuntimeBudget:
        allocation = self._run_budget(task, graph, node)
        if not spent.fits_within(allocation):
            raise BudgetExceededError(
                f"work unit {node.key} already exceeded its logical allocation"
            )
        return allocation - spent

    def _validate_plan_budget(self, budget: Mapping[str, Any], spec: PlanSpec) -> None:
        if not self.enforce_runtime_budgets:
            return
        total = self._budget(budget)
        node_count = len(spec.nodes)
        if not node_count:
            return
        explicit = budget.get("run_budget")
        if isinstance(explicit, Mapping):
            per_run = self._budget(explicit)
            if not per_run.fits_within(total):
                raise ValueError("run_budget cannot exceed the task budget")
            roots = {
                node.key for node in spec.nodes
            } - {edge.to_node for edge in spec.edges}
            for name in ("model_calls", "tool_calls", "tokens", "wall_seconds"):
                if getattr(per_run, name) * len(roots) > getattr(total, name):
                    raise ValueError(
                        f"run_budget.{name} cannot reserve all {len(roots)} root nodes"
                    )
            return
        for name in ("model_calls", "tokens", "wall_seconds"):
            if getattr(total, name) < node_count:
                raise ValueError(
                    f"task budget {name}={getattr(total, name)} cannot execute "
                    f"{node_count} DAG nodes; increase the budget"
                )

    def _usage_for_run(
        self, run: RunRecord, evidence: Optional[Sequence[Any]] = None
    ) -> RuntimeBudget:
        segments = [
            item
            for item in (
                evidence
                if evidence is not None
                else self.store.list_runtime_usage_evidence(
                    run.task_id, (run.id,)
                )
            )
            if item.run_id == run.id and item.payload.get("runtime_usage_segment")
        ]
        if segments:
            total = RuntimeBudget()
            for item in segments:
                # ``usage`` is the immutable observation. A terminal containment
                # cleanup may exceed its budget, in which case ``accounted_usage``
                # is the bounded amount applied to the runtime ledger. Keeping both
                # avoids either falsifying audit evidence or making recovery unable
                # to reconstruct a valid budget tree.
                usage = dict(
                    item.payload.get("accounted_usage")
                    or item.payload.get("usage")
                    or {}
                )
                total += RuntimeBudget(
                    model_calls=int(usage.get("model_calls", 0) or 0),
                    tool_calls=int(usage.get("tool_calls", 0) or 0),
                    tokens=int(usage.get("tokens", 0) or 0),
                    wall_seconds=int(usage.get("wall_seconds", 0) or 0),
                )
            return total
        output = dict(run.output or {})
        usage = output.get("usage")
        if not isinstance(usage, Mapping):
            return RuntimeBudget()
        return RuntimeBudget(
            model_calls=int(usage.get("model_calls", 0) or 0),
            tool_calls=int(usage.get("tool_calls", 0) or 0),
            tokens=int(usage.get("tokens", 0) or 0),
            wall_seconds=int(usage.get("wall_seconds", 0) or 0),
        )

    def _observed_usage_for_run(
        self, run: RunRecord, evidence: Optional[Sequence[Any]] = None
    ) -> RuntimeBudget:
        """Return actual provider usage, independent of any finite ledger cap."""

        segments = [
            item
            for item in (
                evidence
                if evidence is not None
                else self.store.list_runtime_usage_evidence(
                    run.task_id, (run.id,)
                )
            )
            if item.run_id == run.id and item.payload.get("runtime_usage_segment")
        ]
        if segments:
            total = RuntimeBudget()
            for item in segments:
                usage = dict(item.payload.get("usage") or {})
                total += RuntimeBudget(
                    model_calls=int(usage.get("model_calls", 0) or 0),
                    tool_calls=int(usage.get("tool_calls", 0) or 0),
                    tokens=int(usage.get("tokens", 0) or 0),
                    wall_seconds=int(usage.get("wall_seconds", 0) or 0),
                )
            return total
        output = dict(run.output or {})
        usage = output.get("usage")
        if not isinstance(usage, Mapping):
            return RuntimeBudget()
        return RuntimeBudget(
            model_calls=int(usage.get("model_calls", 0) or 0),
            tool_calls=int(usage.get("tool_calls", 0) or 0),
            tokens=int(usage.get("tokens", 0) or 0),
            wall_seconds=int(usage.get("wall_seconds", 0) or 0),
        )

    def _record_usage_segment(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        node: NodeRecord,
        run: RunRecord,
        profile: AgentProfile,
        usage: Mapping[str, Any],
        *,
        segment_key: str,
        accounted_usage: Optional[RuntimeBudget] = None,
        accounting_error: Optional[BaseException] = None,
    ) -> RuntimeBudget:
        measured = RuntimeBudget(
            model_calls=int(usage.get("model_calls", 0) or 0),
            tool_calls=int(usage.get("tool_calls", 0) or 0),
            tokens=int(usage.get("tokens", 0) or 0),
            wall_seconds=int(usage.get("wall_seconds", 0) or 0),
        )
        payload: dict[str, Any] = {
            "title": "Runtime usage segment",
            "runtime_usage_segment": True,
            "segment_key": segment_key,
            "node_key": node.key,
            "profile_id": profile.profile_id,
            "profile_version": profile.version,
            "role": profile.role.value,
            # Preserve actual observed usage even when cleanup itself crossed a
            # limit. Runtime reconstruction uses the separately bounded accounted
            # value so audit truth cannot make the in-memory budget ledger invalid.
            "usage": measured.as_dict(),
        }
        if accounted_usage is not None:
            payload["accounted_usage"] = accounted_usage.as_dict()
            payload["budget_exceeded"] = accounted_usage != measured
        if accounting_error is not None:
            payload["accounting_error"] = {
                "kind": type(accounting_error).__name__,
                "message": str(accounting_error),
            }
        self.store.add_evidence(
            task.id,
            kind=EvidenceKind.METRIC,
            payload=payload,
            created_by="orchestration-runtime",
            plan_id=graph.plan.id,
            node_id=node.id,
            run_id=run.id,
            command_id=_command("runtime-usage", run.id, segment_key),
        )
        return measured

    def _root_task_id(self, task_id: str) -> str:
        seen: set[str] = set()
        task = self.store.get_task(task_id)
        while task.parent_task_id:
            if task.id in seen:
                raise RuntimeStateError("durable task parent cycle detected")
            seen.add(task.id)
            task = self.store.get_task(task.parent_task_id)
        return task.id

    def _all_tasks(
        self, *, statuses: Optional[Sequence[TaskStatus]] = None
    ) -> tuple[TaskRecord, ...]:
        """Use the store's unbounded paged scan for coordinator-owned operations."""

        return self.store.list_all_tasks(statuses=statuses)

    def _descendants(self, task_id: str) -> list[TaskRecord]:
        return list(self.store.list_task_tree(task_id, include_root=False))

    def _plan_descendants(self, task_id: str, plan_id: str) -> list[TaskRecord]:
        """Return only child branches delegated by this immutable plan revision."""

        descendants = self._descendants(task_id)
        by_parent: dict[str, list[TaskRecord]] = {}
        for child in descendants:
            if child.parent_task_id:
                by_parent.setdefault(child.parent_task_id, []).append(child)
        selected: list[TaskRecord] = []

        def collect(parent_id: str) -> None:
            for child in by_parent.get(parent_id, ()):
                selected.append(child)
                collect(child.id)

        for child in by_parent.get(task_id, ()):
            metadata = dict(child.input.get("_runtime") or {})
            if str(metadata.get("parent_plan_id") or "") != plan_id:
                continue
            selected.append(child)
            collect(child.id)
        return selected

    @staticmethod
    def _terminal_outcome(task: TaskRecord) -> TaskStatus:
        if task.status is not TaskStatus.ARCHIVED:
            return task.status
        archived_from = str((task.output or {}).get("archived_from") or "completed")
        try:
            return TaskStatus(archived_from)
        except ValueError:
            return TaskStatus.COMPLETED

    @classmethod
    def _child_succeeded(cls, task: TaskRecord) -> bool:
        return cls._terminal_outcome(task) is TaskStatus.COMPLETED

    def _task_result_envelope(self, task: TaskRecord) -> dict[str, Any]:
        """Build the bounded, hash-addressed result consumed by a parent Agent."""

        if self._is_task_quality_v2(task.id):
            projection = self.quality.task_projection(task.id)
            primary = projection.get("primary_deliverable")
            envelope = {
                "schema_version": 2,
                "child_task_id": task.id,
                "task_id": task.id,
                "plan_id": task.active_plan_id,
                "status": (
                    TaskStatus.COMPLETED.value
                    if task.status is TaskStatus.RUNNING
                    and task.current_stage is OrchestrationStage.ARCHIVE
                    else task.status.value
                ),
                "summary": (
                    f"Published primary deliverable {primary['filename']}"
                    if isinstance(primary, Mapping)
                    else ""
                ),
                "primary_deliverable": dict(primary)
                if isinstance(primary, Mapping)
                else None,
                "quality_status": projection["quality_status"],
                "artifact_status": projection["artifact_status"],
                "budget_status": projection["budget_status"],
                "quality_verdict": projection.get("quality_verdict"),
                "quality_refs": projection.get("quality_refs"),
                "completed_at": _iso(task.updated_at),
            }
            envelope["result_hash"] = _canonical_hash(envelope)
            return envelope

        graph = self.store.get_plan(task.active_plan_id) if task.active_plan_id else None
        runs = self.store.list_runs(task.id)
        latest = self._latest_runs(runs)
        run_results: list[dict[str, Any]] = []
        if graph is not None:
            for node in graph.nodes:
                run = latest.get(node.key)
                if run is None:
                    continue
                output = dict(run.output or {})
                verdict = output.get("verdict")
                run_results.append(
                    {
                        "node_id": node.id,
                        "node_key": node.key,
                        "kind": node.kind.value,
                        "role": self._profile_for_node(node).role.value,
                        "run_id": run.id,
                        "status": run.status.value,
                        "summary": str(output.get("summary") or run.error_message or "")[
                            :8_000
                        ],
                        "verdict": dict(verdict) if isinstance(verdict, Mapping) else None,
                        "candidate_artifact": (
                            dict(output["candidate_artifact"])
                            if isinstance(output.get("candidate_artifact"), Mapping)
                            else None
                        ),
                    }
                )
        all_evidence = self.store.list_evidence(task.id)
        accepted = next(
            (
                dict(item.payload)
                for item in reversed(all_evidence)
                if bool(item.payload.get("accepted"))
            ),
            None,
        )
        evidence_refs = [
            {
                "id": item.id,
                "kind": item.kind.value,
                "content_hash": item.content_hash,
                "blob_uri": item.blob_uri,
                "mime_type": item.mime_type,
                "run_id": item.run_id,
            }
            for item in all_evidence[-128:]
        ]
        structured_results = [
            dict(item)
            for run in runs
            for item in ((run.output or {}).get("result"),)
            if isinstance(item, Mapping)
            and int(item.get("schema_version") or 0) >= 2
        ]
        latest_structured = structured_results[-1] if structured_results else {}
        products = self.store.list_work_products(task.id, limit=1_000)
        active_brief = self.store.get_active_brief(task.id)
        summary = str(
            latest_structured.get("summary")
            or next(
                (
                    item["summary"]
                    for item in reversed(run_results)
                    if str(item.get("summary") or "").strip()
                ),
                "",
            )
        )[:8_000]
        envelope: dict[str, Any] = {
            "schema_version": 2,
            "child_task_id": task.id,
            "task_id": task.id,
            "brief_revision": active_brief.revision,
            "plan_id": task.active_plan_id,
            "status": (
                TaskStatus.COMPLETED.value
                if task.status is TaskStatus.RUNNING
                and task.current_stage is OrchestrationStage.ARCHIVE
                else task.status.value
            ),
            "summary": summary,
            "criterion_results": dict(
                latest_structured.get("criterion_results") or {}
            ),
            "work_product_refs": [item.id for item in products],
            "artifact_refs": [
                str(item.artifact_id or item.uri)
                for item in products
                if item.artifact_id or item.uri
            ],
            "remaining_risks": list(
                latest_structured.get("remaining_risks") or ()
            )[:100],
            "completed_at": (
                latest_structured.get("completed_at")
                or _iso(task.updated_at)
            ),
            "accepted_subject": dict((accepted or {}).get("subject") or {}),
            "publication": dict((accepted or {}).get("publication") or {}),
            "runs": run_results,
            "evidence": evidence_refs,
            "evidence_total": len(all_evidence),
            "evidence_truncated": len(all_evidence) > len(evidence_refs),
        }
        envelope["result_hash"] = _canonical_hash(envelope)
        return envelope

    def _is_task_quality_v2(self, task_id: str) -> bool:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT active_contract_id FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
        return bool(row is not None and row["active_contract_id"])

    def _quality_v2_completion_eligibility(
        self, task_id: str
    ) -> tuple[bool, dict[str, Any], tuple[str, ...]]:
        """Recheck every publish invariant at the lifecycle completion boundary."""

        projection = self.quality.task_projection(task_id)
        reasons: list[str] = []
        primary = projection.get("primary_deliverable")
        if projection.get("workflow_status") != "completed":
            reasons.append("workflow_not_completed")
        if projection.get("quality_status") not in {"pass", "waived"}:
            reasons.append("quality_not_publishable")
        if (
            not isinstance(primary, Mapping)
            or primary.get("status") != "verified"
            or not primary.get("sha256")
        ):
            reasons.append("primary_artifact_not_verified")
        budget_status = str(projection.get("budget_status") or "")
        budget_allowed = budget_status in {"within_budget", "warning", "unlimited"}
        if budget_status == "over_budget":
            strategy_ref = str(
                (projection.get("quality_refs") or {}).get("strategy_id") or ""
            )
            strategy = (
                self.quality_strategies.get(strategy_ref) if strategy_ref else None
            )
            soft_policy = (
                dict(strategy.effective_policy).get("soft_budget_publish", {})
                if strategy is not None
                else {}
            )
            budget_allowed = bool(
                strategy is not None
                and str(strategy.budget_profile.get("mode") or "") == "soft"
                and isinstance(soft_policy, Mapping)
                and soft_policy.get("value") is True
            )
        if not budget_allowed:
            reasons.append("budget_not_publishable")
        with self.store._read() as connection:
            evaluation = connection.execute(
                """
                SELECT artifact_id, artifact_hash, decision, verdict
                FROM orch_quality_evaluations
                WHERE task_id=? AND evaluation_type='final'
                ORDER BY created_at DESC, id DESC LIMIT 1
                """,
                (task_id,),
            ).fetchone()
        if (
            evaluation is None
            or not isinstance(primary, Mapping)
            or evaluation["artifact_id"] != primary.get("artifact_id")
            or evaluation["artifact_hash"] != primary.get("sha256")
            or evaluation["decision"] != "publish"
            or evaluation["verdict"] != "pass"
        ):
            reasons.append("authoritative_publish_decision_missing")
        return not reasons, projection, tuple(reasons)

    def _hold_quality_v2_completion(
        self, task: TaskRecord, reasons: Sequence[str]
    ) -> TaskRecord:
        reason = str(reasons[0] if reasons else "quality_attention_required")
        with self.store._write() as connection:
            row = connection.execute(
                "SELECT workflow_status FROM orch_tasks WHERE id=?", (task.id,)
            ).fetchone()
            current = str(row["workflow_status"] if row is not None else "")
            event = None
            if current in {"running", "validating", "reviewing"}:
                event = WorkflowEvent.ATTENTION_REQUIRED
            elif current == "repairing":
                event = WorkflowEvent.REPAIR_EXHAUSTED
            if event is not None:
                transition_workflow_in_transaction(
                    self.store,
                    connection,
                    task_id=task.id,
                    event=event,
                    reason_code=reason,
                    command_id=f"quality-completion-hold:{task.id}:{reason}",
                )
            connection.execute(
                """
                UPDATE orch_tasks SET quality_reason_code=? WHERE id=?
                """,
                (reason, task.id),
            )
        fresh = self.store.get_task(task.id)
        if fresh.status is TaskStatus.RUNNING:
            return self._transition_status(
                fresh,
                TaskStatus.NEEDS_RECONCILIATION,
                reason,
                output={
                    **dict(fresh.output or {}),
                    "task_quality_v2": True,
                    "quality_completion_blockers": list(reasons),
                },
            )
        return fresh

    def _prepare_quality_v2_repair(self, task: TaskRecord) -> str:
        """Compile one active RepairRequest into a fresh acyclic plan revision."""

        with self.store._read() as connection:
            row = connection.execute(
                """
                SELECT * FROM orch_repair_requests
                WHERE task_id=? AND status IN ('pending','running')
                ORDER BY attempt DESC, created_at DESC LIMIT 1
                """,
                (task.id,),
            ).fetchone()
        if row is None:
            return "none"
        graph = self.store.get_plan(task.active_plan_id or "")
        if str(graph.plan.metadata.get("repair_request_id") or "") == row["id"]:
            self.quality_workflow.repairs.fail_active(
                str(row["id"]),
                reason="repair_plan_failed",
            )
            return "failed"
        strategy = self.quality_strategies.get(
            str(
                self.quality.task_projection(task.id)["quality_refs"]["strategy_id"]
            )
        )
        producer = next(
            (
                item
                for item in strategy.nodes
                if item.config.get("result_schema_id") == "analysis_report_result_v2"
            ),
            None,
        )
        if producer is None:
            raise ConflictError("repair strategy has no canonical artifact producer")
        keys = (producer.key, "validate", "review", "adjudicate", "publish")
        source_nodes = {item.key: item for item in graph.nodes}
        missing = tuple(key for key in keys if key not in source_nodes)
        if missing:
            raise ConflictError(
                "repair plan cannot preserve missing strategy nodes: "
                + ", ".join(missing)
            )
        allocations = self._repair_node_allocations(
            dict(json.loads(row["budget_allocation_json"])),
            producer_key=producer.key,
        )
        specs: list[NodeSpec] = []
        for key in keys:
            source = source_nodes[key]
            node_input = dict(source.input)
            config = dict(node_input.get("quality_node_config") or {})
            config.update(
                {
                    "repair_mode": True,
                    "repair_request_id": str(row["id"]),
                    "repair_attempt": int(row["attempt"]),
                }
            )
            node_input["quality_node_config"] = config
            instructions = source.instructions
            if key == producer.key:
                instructions = (
                    "Execute only the active bounded RepairRequest. Call "
                    "get_repair_request(), read the exact immutable parent artifact, "
                    "change only allowed_sections, create the child with "
                    "create_repaired_artifact(), finalize it, and submit the canonical "
                    "analysis_report_result_v2. Do not modify the source workspace."
                )
            specs.append(
                NodeSpec(
                    key=source.key,
                    title=(
                        f"Repair artifact v{int(row['target_version'])}"
                        if key == producer.key
                        else source.title
                    ),
                    instructions=instructions,
                    kind=source.kind,
                    agent=source.agent,
                    model=source.model,
                    input=node_input,
                    join_policy=source.join_policy,
                    failure_policy=FailurePolicy.FAIL_FAST,
                    effect_safety=source.effect_safety,
                    retry_policy=RetryPolicy(max_attempts=1),
                    timeout_seconds=source.timeout_seconds,
                    priority=source.priority,
                    concurrency_key=source.concurrency_key,
                    metadata={
                        **dict(source.metadata),
                        "repair_request_id": str(row["id"]),
                        "repair_attempt": int(row["attempt"]),
                    },
                )
            )
        repair_spec = PlanSpec(
            nodes=tuple(specs),
            edges=tuple(
                EdgeSpec(
                    from_node=source,
                    to_node=target,
                    condition=EdgeCondition.SUCCESS,
                    required=True,
                    metadata={"quality_condition": "publish" if target == "publish" else "success"},
                )
                for source, target in zip(keys, keys[1:])
            ),
            metadata={
                "generated": "task-quality-v2-repair",
                "strategy_id": strategy.id,
                "strategy_hash": strategy.content_hash,
                "repair_request_id": str(row["id"]),
                "repair_attempt": int(row["attempt"]),
                "repair_source_artifact_id": str(row["source_artifact_id"]),
                "repair_node_allocations": allocations,
            },
        )
        self.store.create_plan_revision(
            task.id,
            repair_spec,
            expected_task_version=task.version,
            created_by="task-quality-v2-repair-coordinator",
            command_id=_command("quality-repair-plan", str(row["id"])),
        )
        fresh = self.store.get_task(task.id)
        self._transition_stage(
            fresh,
            OrchestrationStage.EXECUTION_REVIEW_TEST,
            "task-quality-v2-repair-started",
        )
        return "started"

    @staticmethod
    def _repair_node_allocations(
        total: Mapping[str, int],
        *,
        producer_key: str,
    ) -> dict[str, dict[str, int]]:
        """Split one repair envelope across producer/scorer plus zero-model services."""

        tokens = max(0, int(total.get("reported_tokens", 0) or 0))
        model_calls = max(0, int(total.get("model_calls", 0) or 0))
        tool_calls = max(0, int(total.get("tool_calls", 0) or 0))
        active_seconds = max(3, int(total.get("active_seconds", 0) or 0))
        payload_bytes = max(0, int(total.get("tool_payload_bytes", 0) or 0))

        def split(value: int) -> tuple[int, int]:
            producer_value = (value * 2 + 2) // 3
            return producer_value, value - producer_value

        producer_tokens, review_tokens = split(tokens)
        producer_models, review_models = split(model_calls)
        producer_tools, review_tools = split(tool_calls)
        producer_seconds, review_seconds = split(active_seconds - 3)
        producer_payload, review_payload = split(payload_bytes)

        def allocation(
            reserved_tokens: int,
            calls: int,
            tools: int,
            seconds: int,
            payload: int,
        ) -> dict[str, int]:
            return {
                "min_reported_tokens": 0 if reserved_tokens == 0 else max(1, reserved_tokens // 4),
                "reserved_reported_tokens": reserved_tokens,
                "max_reported_tokens": max(reserved_tokens, int(reserved_tokens * 1.35)),
                "model_calls": calls,
                "tool_calls": tools,
                "active_seconds": seconds,
                "tool_payload_bytes": payload,
            }

        zero = allocation(0, 0, 0, 1, 0)
        return {
            producer_key: allocation(
                producer_tokens,
                producer_models,
                producer_tools,
                producer_seconds,
                producer_payload,
            ),
            "validate": dict(zero),
            "review": allocation(
                review_tokens,
                review_models,
                review_tools,
                review_seconds,
                review_payload,
            ),
            "adjudicate": dict(zero),
            "publish": dict(zero),
        }

    @staticmethod
    def _normalize_verdict(value: Any) -> str:
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
        text = str(value or "").strip().lower()
        normalized = aliases.get(text, text)
        return normalized if normalized in {"pass", "fail", "unknown"} else "unknown"

    @staticmethod
    def _explicit_handoff_result(
        output: Mapping[str, Any],
    ) -> Optional[dict[str, Any]]:
        """Return only a complete_task result, never a vendor structured result.

        Subscription runtimes have long exposed ``structured_result`` with the
        provider-neutral ``status/criteria/files_touched/checks`` schema.  Durable
        handoff settlement instead requires ``criterion_results/work_products``.
        Treating the former as the latter made a successful subscription turn fail
        after completion because criterion text was mistaken for a missing Brief ID.
        """

        explicit = output.get("handoff_result")
        if isinstance(explicit, Mapping):
            return dict(explicit)
        legacy = output.get("structured_result")
        if isinstance(legacy, Mapping) and {
            "criterion_results",
            "work_products",
        }.issubset(legacy):
            return dict(legacy)
        return None

    def _verification_reports(
        self,
        task_id: str,
        graph: PlanGraph,
        latest: Mapping[str, RunRecord],
        *,
        expected_subject: Optional[Mapping[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        task = self.store.get_task(task_id)
        evidence_by_run: dict[str, list[Any]] = {}
        for item in self.store.list_evidence(task_id):
            if item.run_id:
                evidence_by_run.setdefault(item.run_id, []).append(item)
        reports: list[dict[str, Any]] = []
        verification_kinds = {NodeKind.REVIEW, NodeKind.TEST, NodeKind.EVALUATE}
        for node in graph.nodes:
            try:
                role = self._profile_for_node(node).role
            except Exception:
                role = None
            expected_roles = {
                NodeKind.REVIEW: {AgentRole.REVIEWER},
                NodeKind.TEST: {AgentRole.TESTER},
                NodeKind.EVALUATE: {AgentRole.EVALUATOR, AgentRole.SCORER},
            }.get(node.kind)
            # Only a semantically validated kind/role pair is formal evidence.
            # A verifier profile attached to an AGENT/EXECUTE node must never
            # self-certify that same node's work, including for preview-era plans.
            if node.kind not in verification_kinds or role not in (expected_roles or set()):
                continue
            run = latest.get(node.key)
            raw: Any = None
            source = "missing"
            if run is not None:
                output = dict(run.output or {})
                if output.get("verdict") is not None:
                    raw = output["verdict"]
                    source = "run_output"
                else:
                    for item in reversed(evidence_by_run.get(run.id, ())):
                        if "verdict" in item.payload:
                            raw = item.payload
                            source = f"evidence:{item.id}"
                            break
            if isinstance(raw, Mapping):
                status = self._normalize_verdict(raw.get("status", raw.get("verdict")))
                criteria = {
                    str(key): self._normalize_verdict(value)
                    for key, value in dict(raw.get("criteria") or {}).items()
                }
                summary = str(raw.get("summary") or "")
                findings = [str(item) for item in (raw.get("findings") or ())]
                subject = dict(raw.get("subject") or {})
            else:
                status = self._normalize_verdict(raw)
                criteria = {}
                summary = ""
                findings = []
                subject = {}
            missing_criteria = set(task.acceptance_criteria) - set(criteria)
            if any(value == "fail" for value in criteria.values()):
                status = "fail"
            elif status == "pass" and (
                missing_criteria
                or any(value == "unknown" for value in criteria.values())
            ):
                status = "unknown"
            subject_matches = True
            if expected_subject is not None:
                subject_matches = (
                    str(subject.get("manifest_sha256") or "")
                    == str(expected_subject.get("manifest_sha256") or "")
                    and str(subject.get("acceptance_contract_hash") or "")
                    == str(expected_subject.get("acceptance_contract_hash") or "")
                )
                if not subject_matches:
                    status = "unknown"
                    findings.append("verdict is not bound to the current candidate revision")
            reports.append(
                {
                    "node_id": node.id,
                    "node_key": node.key,
                    "run_id": run.id if run else None,
                    "role": role.value if role else node.agent,
                    "status": status,
                    "criteria": criteria,
                    "summary": summary,
                    "findings": findings,
                    "source": source,
                    "subject": subject,
                    "subject_matches": subject_matches,
                    "missing_criteria": sorted(missing_criteria),
                }
            )
        return reports

    @staticmethod
    def _verification_adjudication(
        reports: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Choose the role that is responsible for settling verifier dissent.

        Evaluator/scorer nodes are graph-validated to run downstream of the
        independent reviewer/tester nodes.  A complete passing verdict from every
        configured adjudicator therefore settles an earlier dissent without
        discarding it.  If an adjudicator is absent, incomplete, or adverse, all
        verifier reports remain authoritative and the disagreement stays open.
        """

        adjudicator_roles = {AgentRole.EVALUATOR.value, AgentRole.SCORER.value}
        adjudicators = [
            dict(report)
            for report in reports
            if str(report.get("role") or "").lower() in adjudicator_roles
        ]
        adjudicated = bool(adjudicators) and all(
            str(report.get("status") or "unknown") == "pass"
            for report in adjudicators
        )
        dissent = [
            dict(report)
            for report in reports
            if str(report.get("role") or "").lower() not in adjudicator_roles
            and str(report.get("status") or "unknown") != "pass"
        ]
        authoritative = adjudicators if adjudicated else [dict(report) for report in reports]
        return {
            "adjudicated": adjudicated,
            "authority": "evaluator" if adjudicated else "verification_consensus",
            "authoritative": authoritative,
            "adjudicators": adjudicators,
            "dissent": dissent,
        }

    @staticmethod
    def _verification_signature(
        reports: Sequence[Mapping[str, Any]],
    ) -> str:
        material = sorted(
            (
                str(report.get("node_id") or report.get("node_key") or ""),
                str(report.get("run_id") or ""),
                str(report.get("role") or ""),
                str(report.get("status") or "unknown"),
            )
            for report in reports
        )
        return hashlib.sha256(
            json.dumps(material, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:16]

    def _accepted_current_verification_gate(
        self,
        task_id: str,
        graph: PlanGraph,
        reports: Sequence[Mapping[str, Any]],
    ) -> Optional[GateRecord]:
        """Return a human override only when it names the current immutable runs."""

        signature = self._verification_signature(reports)
        expected_runs = sorted(
            (
                str(report.get("node_id") or report.get("node_key") or ""),
                str(report.get("run_id") or ""),
            )
            for report in reports
        )
        prefix = f"{task_id}:reconciliation:{graph.plan.id}:"
        for gate in reversed(self.store.list_gates(task_id)):
            resolution = dict(gate.resolution or {})
            if (
                gate.kind is not GateKind.RECONCILIATION
                or gate.status is not GateStatus.APPROVED
                or gate.run_id is not None
                or not gate.source_key.startswith(prefix)
                or str(resolution.get("decision") or "") != "accept_current"
            ):
                continue
            prompt = dict(gate.prompt)
            if any(
                prompt.get(key)
                for key in (
                    "failed_runs",
                    "workspace_commit_failures",
                    "failed_children",
                )
            ):
                continue
            prompt_signature = str(prompt.get("verification_signature") or "")
            if prompt_signature:
                if prompt_signature == signature:
                    return gate
                continue
            prompt_runs = sorted(
                (
                    str(report.get("node_id") or report.get("node_key") or ""),
                    str(report.get("run_id") or ""),
                )
                for report in (prompt.get("verification") or ())
                if isinstance(report, Mapping)
            )
            if prompt_runs == expected_runs:
                return gate
        return None

    def _rebuild_all_runtimes(self) -> None:
        roots = [task.id for task in self.store.list_root_tasks()]
        with self._runtime_lock:
            self._runtime_trees.clear()
            self._runtime_task_roots.clear()
        for root_id in roots:
            try:
                self._rebuild_runtime_tree(root_id)
            except Exception as exc:
                logger.exception("could not rebuild runtime tree %s", root_id)
                self._block_task(root_id, "runtime_recovery_failed", str(exc))

    def _rebuild_runtime_tree(self, root_task_id: str) -> RuntimeManager:
        (
            task_records,
            runs_by_task,
            snapshot_graphs,
            usage_evidence_by_run,
        ) = self.store.runtime_tree_snapshot(root_task_id)
        tasks = {task.id: task for task in task_records}
        children: dict[str, list[TaskRecord]] = {task_id: [] for task_id in tasks}
        by_parent_run: dict[str, list[TaskRecord]] = {}
        for task in tasks.values():
            if not task.parent_task_id:
                continue
            meta = dict(task.input.get("_runtime") or {})
            parent_run = str(meta.get("parent_run_id") or "")
            if parent_run:
                effective_parent_run = parent_run
                # Keep the immutable origin run in task metadata, but project a
                # stable logical delegation beneath the newest attempt of the same
                # plan node. This releases a LOST attempt's reservation and lets its
                # retry wait on/reuse the already-running child.
                if meta.get("spawn_key") and meta.get("parent_plan_id"):
                    candidates = [
                        run
                        for run in runs_by_task.get(task.parent_task_id, ())
                        if run.plan_id == str(meta["parent_plan_id"])
                        and run.node_id == task.parent_node_id
                    ]
                    if candidates:
                        effective_parent_run = max(
                            candidates,
                            key=lambda run: (run.attempt, run.created_at, run.id),
                        ).id
                by_parent_run.setdefault(effective_parent_run, []).append(task)
            else:
                children.setdefault(task.parent_task_id, []).append(task)
        manager = RuntimeManager(limits=self.runtime_limits)
        registered_tasks: set[str] = set()

        def add_task(task: TaskRecord, parent_runtime_id: Optional[str]) -> None:
            if task.id in registered_tasks:
                raise RuntimeStateError(f"duplicate durable task in runtime tree: {task.id}")
            registered_tasks.add(task.id)
            task_runtime_id = self._task_runtime_id(task.id)
            spec = RuntimeSpec(
                runtime_id=task_runtime_id,
                profile_id=str(task.policy.get("profile_id") or "orchestrator"),
                task=task.objective,
                budget=(
                    self._budget(task.budget)
                    if self.enforce_runtime_budgets
                    else UNLIMITED_RUNTIME_BUDGET
                ),
                permissions=self._task_permissions(task),
                parent_id=parent_runtime_id,
                metadata={"task_id": task.id},
                kind=RuntimeKind.TASK,
            )
            if parent_runtime_id is None:
                manager.add_root(spec)
            else:
                manager.spawn_child(parent_runtime_id, spec)
            if task.status is not TaskStatus.DRAFT:
                manager.start(task_runtime_id)

            runs = sorted(
                runs_by_task.get(task.id, ()),
                key=lambda item: (item.created_at, item.attempt, item.id),
            )
            graphs: dict[str, PlanGraph] = {}
            spent_by_node: dict[str, RuntimeBudget] = {}
            for run in runs:
                graph = graphs.get(run.plan_id)
                if graph is None:
                    try:
                        graph = snapshot_graphs[run.plan_id]
                    except KeyError as exc:
                        raise RuntimeStateError(
                            f"durable run {run.id} references missing plan {run.plan_id}"
                        ) from exc
                    graphs[run.plan_id] = graph
                node = next(item for item in graph.nodes if item.id == run.node_id)
                profile = self._profile_for_node(node)
                runtime_id = self._run_runtime_id(run.id)
                incoming = [edge for edge in graph.edges if edge.to_node == node.key]
                durable_dependencies: list[str] = []
                enforced_dependencies: list[str] = []
                predecessors: list[tuple[Any, RunRecord, str]] = []
                for edge in incoming:
                    candidates = [
                        candidate
                        for candidate in runs
                        if candidate.plan_id == run.plan_id
                        and candidate.node_key == edge.from_node
                        and candidate.created_at <= run.created_at
                    ]
                    if not candidates:
                        continue
                    predecessor = max(
                        candidates, key=lambda item: (item.attempt, item.created_at, item.id)
                    )
                    predecessor_id = self._run_runtime_id(predecessor.id)
                    durable_dependencies.append(predecessor_id)
                    predecessors.append((edge, predecessor, predecessor_id))
                # A durable SKIPPED result is the scheduler's terminal dependency
                # decision.  Requiring its deliberately-unsatisfied predecessor to
                # succeed would make runtime reconstruction fail after a restart.
                if run.status is RunStatus.SKIPPED:
                    enforced_dependencies = []
                elif node.join_policy is JoinPolicy.ALL:
                    enforced_dependencies.extend(
                        predecessor_id
                        for edge, _predecessor, predecessor_id in predecessors
                        if edge.required and edge.condition is EdgeCondition.SUCCESS
                    )
                else:
                    selected = next(
                        (
                            (edge, predecessor, predecessor_id)
                            for edge, predecessor, predecessor_id in predecessors
                            if predecessor.status in _TERMINAL_RUNS
                            and self._edge_matches(edge.condition, predecessor.status)
                        ),
                        None,
                    )
                    if selected is not None and selected[1].status is RunStatus.SUCCEEDED:
                        enforced_dependencies.append(selected[2])
                spent = spent_by_node.get(node.id, RuntimeBudget())
                runtime_node = manager.spawn_child(
                    task_runtime_id,
                    RuntimeSpec(
                        runtime_id=runtime_id,
                        profile_id=profile.profile_id,
                        profile_version=profile.version,
                        profile_content_hash=profile.content_hash,
                        task=node.instructions or task.objective,
                        budget=self._attempt_budget(task, graph, node, spent),
                        permissions=self._profile_permissions(task, profile),
                        parent_id=task_runtime_id,
                        dependencies=tuple(enforced_dependencies),
                        attempt=run.attempt,
                        work_unit_id=f"work:{node.id}",
                        metadata={
                            "task_id": task.id,
                            "run_id": run.id,
                            "node_id": node.id,
                            "durable_dependencies": durable_dependencies,
                        },
                        kind=RuntimeKind.AGENT,
                    ),
                )
                if run.status is not RunStatus.QUEUED:
                    manager.start(runtime_id)
                for child in sorted(
                    by_parent_run.get(run.id, ()), key=lambda item: (item.created_at, item.id)
                ):
                    add_task(child, runtime_id)
                if run.status is not RunStatus.QUEUED:
                    usage = self._usage_for_run(
                        run, usage_evidence_by_run.get(run.id, ())
                    )
                    # Historical versions could persist the measured usage before
                    # noticing that it crossed this attempt's allocation. Preserve
                    # that immutable evidence, but rebuild the ledger with only the
                    # amount the attempt could have consumed. Recovery must never
                    # block an operator merely because the terminal run was over
                    # budget.
                    accounted = self._bounded_budget(
                        usage, runtime_node.remaining_budget
                    )
                    if accounted != RuntimeBudget():
                        manager.charge(runtime_id, accounted)
                    spent_by_node[node.id] = spent + accounted
                if run.status is RunStatus.WAITING_GATE:
                    manager.suspend(runtime_id)
                elif run.status in _TERMINAL_RUNS:
                    target = (
                        RuntimeStatus.SUCCEEDED
                        if run.status in {RunStatus.SUCCEEDED, RunStatus.SKIPPED}
                        else RuntimeStatus.CANCELED
                        if run.status is RunStatus.CANCELED
                        else RuntimeStatus.FAILED
                    )
                    try:
                        manager.finish(runtime_id, target)
                    except RuntimeStateError:
                        # A durable parent run may have returned without joining a live
                        # child. Keep the derived runtime suspended until that child ends.
                        manager.suspend(runtime_id)
                runtime_node.effective_permissions.assert_within(
                    manager.get(task_runtime_id).effective_permissions
                )

            for child in sorted(
                children.get(task.id, ()), key=lambda item: (item.created_at, item.id)
            ):
                add_task(child, task_runtime_id)

            task_node = manager.get(task_runtime_id)
            if task.status in _TERMINAL_TASKS:
                target = (
                    RuntimeStatus.SUCCEEDED
                    if task.status in {TaskStatus.COMPLETED, TaskStatus.ARCHIVED}
                    else RuntimeStatus.CANCELED
                    if task.status is TaskStatus.CANCELED
                    else RuntimeStatus.FAILED
                )
                try:
                    manager.finish(task_runtime_id, target)
                except RuntimeStateError:
                    if task_node.status is RuntimeStatus.RUNNING:
                        manager.suspend(task_runtime_id)
            elif task.status in {
                TaskStatus.WAITING_HUMAN,
                TaskStatus.WAITING_CHILD,
                TaskStatus.PAUSED,
                TaskStatus.BLOCKED,
                TaskStatus.NEEDS_RECONCILIATION,
            } and task_node.status is RuntimeStatus.RUNNING:
                manager.suspend(task_runtime_id)

        add_task(tasks[root_task_id], None)
        with self._runtime_lock:
            self._runtime_trees[root_task_id] = manager
            for task_id in registered_tasks:
                self._runtime_task_roots[task_id] = root_task_id
        return manager

    def _runtime_for_task(self, task_id: str, *, rebuild: bool = False) -> RuntimeManager:
        root_id = self._root_task_id(task_id)
        with self._runtime_lock:
            manager = self._runtime_trees.get(root_id)
        if manager is None or rebuild:
            manager = self._rebuild_runtime_tree(root_id)
        return manager

    async def _recover_incomplete_deliveries(self) -> None:
        for entry in self.workspaces.incomplete_deliveries():
            run: Optional[RunRecord] = None
            task: Optional[TaskRecord] = None
            if entry.snapshot_id.startswith("run-"):
                try:
                    run = self.store.get_run(entry.snapshot_id.removeprefix("run-"))
                    task = self.store.get_task(run.task_id)
                except NotFoundError:
                    logger.error(
                        "incomplete delivery %s has no associated durable run",
                        entry.transaction_id,
                    )
            elif entry.snapshot_id.startswith("task-"):
                try:
                    task = self.store.get_task(
                        entry.snapshot_id.removeprefix("task-")
                    )
                except NotFoundError:
                    logger.error(
                        "incomplete delivery %s has no associated durable task",
                        entry.transaction_id,
                    )
            try:
                recovered = await self._durable_to_thread(
                    self.workspaces.recover,
                    entry.transaction_id,
                    fence_check=self.store.renew_scheduler_fence,
                )
                logger.warning("recovered interrupted workspace delivery %s", entry.transaction_id)
                if task is not None:
                    self.store.add_evidence(
                        task.id,
                        kind=EvidenceKind.CHECKPOINT,
                        payload={
                            "title": (
                                "Interrupted workspace publication rolled back"
                                if run is None
                                else "Interrupted workspace delivery recovered"
                            ),
                            **recovered.to_dict(),
                        },
                        created_by="workspace-recovery",
                        run_id=run.id if run is not None else None,
                        node_id=run.node_id if run is not None else None,
                        plan_id=(
                            run.plan_id if run is not None else task.active_plan_id
                        ),
                        command_id=_command("workspace-recovered", entry.transaction_id),
                    )
            except Exception as exc:
                logger.exception("workspace recovery failed for %s", entry.transaction_id)
                if task is not None:
                    self.store.add_evidence(
                        task.id,
                        kind=EvidenceKind.CHECKPOINT,
                        payload={
                            "title": "Workspace recovery failed",
                            "transaction_id": entry.transaction_id,
                            "error": str(exc),
                        },
                        created_by="workspace-recovery",
                        run_id=run.id if run is not None else None,
                        node_id=run.node_id if run is not None else None,
                        plan_id=(
                            run.plan_id if run is not None else task.active_plan_id
                        ),
                        command_id=_command("workspace-recovery-failed", entry.transaction_id),
                    )
                    fresh = self.store.get_task(task.id)
                    if run is None and fresh.status is TaskStatus.RUNNING:
                        self._transition_status(
                            fresh,
                            TaskStatus.NEEDS_RECONCILIATION,
                            "workspace-publication-rollback-failed",
                            output={
                                **dict(fresh.output or {}),
                                "error_kind": "workspace_recovery_failed",
                                "error": str(exc),
                                "transaction_id": entry.transaction_id,
                            },
                        )
                    else:
                        self._block_task(
                            task.id, "workspace_recovery_failed", str(exc)
                        )

    def _recover_delivered_publications(self) -> None:
        """Bridge a delivered workspace journal entry back into durable task evidence.

        Filesystem publication and the SQLite audit append cannot share one atomic
        transaction. A crash after the journal's durable ``delivered`` record but
        before ``workspace_published`` evidence would otherwise make restart deliver
        the same accepted candidate again. Task snapshots have a stable ``task-`` id,
        so we can verify their retained candidate/patch and idempotently reconstruct
        the missing checkpoint before the scheduler resumes.
        """

        for entry in self.workspaces.journal():
            if entry.status != "delivered" or not entry.snapshot_id.startswith("task-"):
                continue
            task_id = entry.snapshot_id.removeprefix("task-")
            try:
                task = self.store.get_task(task_id)
            except NotFoundError:
                continue
            if not task.active_plan_id:
                continue
            graph = self.store.get_plan(task.active_plan_id)

            def publication_recorded() -> bool:
                return any(
                    item.payload.get("action") == "workspace_published"
                    and str(
                        (item.payload.get("receipt") or {}).get("transaction_id")
                        or ""
                    )
                    == entry.transaction_id
                    for item in self.store.list_evidence(task.id)
                )

            if publication_recorded():
                continue
            try:
                snapshot = self.workspaces.load(entry.snapshot_id)
                if (
                    not task.workspace
                    or Path(entry.source_root).resolve() != Path(task.workspace).resolve()
                    or snapshot.source_root.resolve() != Path(task.workspace).resolve()
                ):
                    raise WorkspaceError(
                        "delivered task journal does not match the task workspace"
                    )
                candidate = self.workspaces.collect_candidate(snapshot)
                if (
                    not entry.candidate_manifest_sha256
                    or candidate.candidate_manifest.digest
                    != entry.candidate_manifest_sha256
                    or candidate.patch_sha256 != entry.patch_sha256
                ):
                    raise WorkspaceError(
                        "retained candidate does not match the delivered journal seal"
                    )
                sealed = next(
                    (
                        item
                        for item in reversed(self.store.list_evidence(task.id))
                        if item.plan_id == graph.plan.id
                        and item.payload.get("action")
                        == "workspace_publication_sealed"
                        and str(
                            (item.payload.get("subject") or {}).get(
                                "manifest_sha256"
                            )
                            or ""
                        )
                        == entry.candidate_manifest_sha256
                        and str(
                            (item.payload.get("subject") or {}).get("patch_sha256")
                            or ""
                        )
                        == entry.patch_sha256
                    ),
                    None,
                )
                if sealed is None:
                    raise WorkspaceError(
                        "delivered publication has no matching durable sealed subject"
                    )
                subject = dict(sealed.payload.get("subject") or {})
                payload = {
                    "title": "Accepted workspace publication recovered",
                    "action": "workspace_published",
                    "subject": subject,
                    "receipt": entry.to_dict(),
                    "recovered": True,
                }
                self.store.add_evidence(
                    task.id,
                    kind=EvidenceKind.CHECKPOINT,
                    payload=payload,
                    created_by="workspace-recovery",
                    plan_id=graph.plan.id,
                    command_id=_command(
                        "workspace-published-recovered", entry.transaction_id
                    ),
                )
            except Exception as exc:
                # Another local service may have won the idempotent evidence append
                # and archived/cleaned the snapshot after our initial read.
                if publication_recorded():
                    continue
                logger.exception(
                    "could not reconcile delivered task publication %s",
                    entry.transaction_id,
                )
                self.store.add_evidence(
                    task.id,
                    kind=EvidenceKind.CHECKPOINT,
                    payload={
                        "title": "Delivered workspace publication needs reconciliation",
                        "action": "workspace_publication_recovery_failed",
                        "receipt": entry.to_dict(),
                        "error": str(exc),
                    },
                    created_by="workspace-recovery",
                    plan_id=graph.plan.id,
                    command_id=_command(
                        "workspace-publication-recovery-failed",
                        entry.transaction_id,
                    ),
                )
                fresh = self.store.get_task(task.id)
                if fresh.status is TaskStatus.RUNNING:
                    self._transition_status(
                        fresh,
                        TaskStatus.NEEDS_RECONCILIATION,
                        "workspace-publication-recovery-failed",
                        output={
                            **dict(fresh.output or {}),
                            "error_kind": "workspace_publication_recovery_failed",
                            "error": str(exc),
                            "transaction_id": entry.transaction_id,
                        },
                    )

    def _resume_completed_child_waits(self) -> None:
        for parent in self._all_tasks(
            statuses=(
                TaskStatus.RUNNING,
                TaskStatus.WAITING_HUMAN,
                TaskStatus.WAITING_CHILD,
                TaskStatus.PAUSED,
            )
        ):
            open_child_gates = [
                gate
                for gate in self.store.list_gates(parent.id, statuses=(GateStatus.OPEN,))
                if gate.kind is GateKind.CHILD_WAIT
            ]
            for gate in open_child_gates:
                child_id = str(gate.prompt.get("child_task_id") or "")
                try:
                    child = self.store.get_task(child_id)
                except NotFoundError:
                    continue
                if child.status not in _TERMINAL_TASKS:
                    continue
                # The two-phase gate commit already publishes the checkpoint and child
                # wait atomically. Keep validating the blob here so corruption or an
                # incomplete legacy record can never requeue an unanswered tool call.
                if not self._valid_run_checkpoint(gate):
                    continue
                self.store.resolve_gate(
                    gate.id,
                    GateStatus.APPROVED,
                    {"decision": "continue", "child_status": child.status.value},
                    resolved_by="orchestration-runtime",
                    expected_version=gate.version,
                    command_id=_command("child-resume", gate.id, child.version),
                )
                self._runtime_for_task(parent.id, rebuild=True)
            if open_child_gates:
                continue
            descendants = self._descendants(parent.id)
            if descendants and all(child.status in _TERMINAL_TASKS for child in descendants):
                fresh = self.store.get_task(parent.id)
                if fresh.status is TaskStatus.WAITING_CHILD:
                    self.store.transition_task_status(
                        fresh.id,
                        TaskStatus.RUNNING,
                        expected_version=fresh.version,
                        command_id=_command("descendants-settled", fresh.id, fresh.version),
                    )
                    self._runtime_for_task(fresh.id, rebuild=True)

    # -- commands ---------------------------------------------------------
    def create_task(
        self,
        request: Mapping[str, Any],
        *,
        _parent_context: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        raw_brief = request.get("brief")
        brief_draft = (
            TaskBriefDraft.from_mapping(raw_brief)
            if isinstance(raw_brief, Mapping)
            else None
        )
        objective = str(
            brief_draft.objective if brief_draft else request.get("objective") or ""
        ).strip()
        if not objective:
            raise ValueError("objective is required")
        domain = TaskDomain(str(request.get("domain") or "code"))
        read_only = bool(request.get("read_only", False))
        external_writes = bool(request.get("external_writes", False))
        if read_only and external_writes:
            raise ValueError(
                "read_only=true cannot be combined with external_writes=true; "
                "remove external_writes or create an explicitly writable task"
            )
        if read_only and brief_draft is not None:
            mutating_deliverables = sorted(
                {
                    str(item.get("kind") or "").strip()
                    for item in brief_draft.deliverables
                    if str(item.get("kind") or "").strip()
                    in _MUTATING_DELIVERABLE_KINDS
                }
            )
            if mutating_deliverables:
                raise ValueError(
                    "read-only tasks cannot require mutating deliverables: "
                    + ", ".join(mutating_deliverables)
                    + "; use artifact, review_report, test_result, plan, or other"
                )
        # Knowledge tasks must not accidentally inherit a writable project merely
        # because the desktop currently has one open.  Code tasks retain that useful
        # default; every explicit workspace is validated for either domain.
        supplied_workspace = request.get("workspace")
        workspace = (
            supplied_workspace
            if supplied_workspace not in {None, ""}
            else self.manager.default_workspace
            if domain is TaskDomain.CODE
            else None
        )
        if workspace:
            workspace_path = Path(str(workspace)).expanduser()
            if not workspace_path.is_dir():
                raise ValueError("workspace must be an existing directory")
            workspace = str(workspace_path.resolve())
        elif domain is TaskDomain.CODE:
            raise ValueError("a code orchestration requires an existing workspace")
        runtime_preset_id = str(request.get("runtime_preset_id") or "").strip()
        selected_preset = (
            runtime_preset(runtime_preset_id) if runtime_preset_id else None
        )
        if selected_preset and domain.value not in selected_preset.domains:
            raise ValueError(
                f"runtime preset {selected_preset.preset_id} does not support "
                f"domain {domain.value}"
            )
        if selected_preset and request.get("requested_model"):
            raise ValueError(
                "runtime_preset_id and requested_model are mutually exclusive; "
                "use per-node models for explicit custom-plan overrides"
            )
        criteria = (
            tuple(
                str(item.get("text") or "").strip()
                for item in brief_draft.acceptance_criteria
                if str(item.get("text") or "").strip()
            )
            if brief_draft
            else tuple(str(v).strip() for v in request.get("acceptance_criteria", ()) if str(v).strip())
        )
        constraints = (
            brief_draft.constraints
            if brief_draft
            else tuple(str(v).strip() for v in request.get("constraints", ()) if str(v).strip())
        )
        if not read_only and READ_ONLY_RULE.search(
            "\n".join((objective, *constraints))
        ):
            raise ValueError(
                "objective or constraints require read-only source access; "
                "set read_only=true or remove the conflicting read-only instruction"
            )
        profile_id = str(request.get("profile_id") or "worker")
        model_policy_id = str(request.get("model_policy_id") or "quality-first")
        primary_profile = self.catalog.resolve_profile(profile_id)
        communication_policy = primary_profile.communication_policy
        if brief_draft is not None and self.handoff_settings.structured_handoff_enabled:
            missing_outcome_tools = {
                "complete_task",
                "fail_task",
            } - set(primary_profile.allowed_tools)
            if missing_outcome_tools:
                raise ValueError(
                    "structured handoff profile is missing required outcome tools: "
                    + ", ".join(sorted(missing_outcome_tools))
                )
        context_policy = ContextPolicy(
            max_initial_context_tokens=min(
                self.handoff_settings.default_context_token_budget,
                communication_policy.max_initial_context_tokens,
            ),
            max_context_refs=min(
                self.handoff_settings.max_context_refs,
                communication_policy.max_context_refs,
            ),
            max_inline_bytes_per_ref=min(
                self.handoff_settings.max_inline_bytes_per_ref,
                communication_policy.max_inline_bytes_per_ref,
            ),
            max_inline_bytes_total=min(
                self.handoff_settings.max_inline_bytes_total,
                communication_policy.max_inline_bytes_total,
            ),
            allowed_context_ref_types=communication_policy.allowed_context_ref_types,
            allow_full_transcript_reference=communication_policy.allow_full_transcript_reference,
            network=bool(request.get("network", False)),
            context_read_audit_enabled=self.handoff_settings.context_read_audit_enabled,
        )
        raw_context_refs = request.get("context_refs") or ()
        prepared_refs: list[ContextRefDraft] = []
        for raw_ref in raw_context_refs:
            if not isinstance(raw_ref, Mapping):
                raise ValueError("each context reference must be an object")
            context_ref = ContextRefDraft.from_mapping(raw_ref)
            if context_ref.ref_type in {
                ContextRefType.FILE,
                ContextRefType.FILE_RANGE,
                ContextRefType.GIT_DIFF,
            }:
                if not workspace:
                    raise ValueError("file context references require a workspace")
                context_ref = self.context_resolver.prepare_file_ref(
                    str(workspace), context_ref
                )
            prepared_refs.append(context_ref)
        context_refs = ContextManifestBuilder(context_policy).normalize(prepared_refs)
        allowed_primary_roles = {
            AgentRole.WORKER,
            AgentRole.PLANNER,
            AgentRole.EXPLORER,
            AgentRole.ORCHESTRATOR,
        }
        if _parent_context is None and primary_profile.role not in allowed_primary_roles:
            raise ValueError(
                "primary profile must be worker, planner, explorer, or orchestrator; "
                f"{primary_profile.role.value} is an isolated lifecycle role"
            )
        model_policy = self.catalog.resolve_policy(model_policy_id)
        if (
            selected_preset
            and selected_preset.fallback_mode == "strict"
            and model_policy.fallback_for_explicit
        ):
            raise ValueError(
                f"runtime preset {selected_preset.preset_id} is strict and cannot use "
                f"model policy {model_policy.policy_id} with fallback_for_explicit=true"
            )
        supplied_budget = dict(request.get("budget") or {})
        normalized_budget: dict[str, Any] = self._budget(supplied_budget).as_dict()
        if isinstance(supplied_budget.get("run_budget"), Mapping):
            normalized_budget["run_budget"] = self._budget(
                supplied_budget["run_budget"]
            ).as_dict()
        assessment = self._assessment(
            request, domain, bool(criteria), workspace=str(workspace) if workspace else None
        )
        require_review = (
            bool(request.get("require_review", False))
            or assessment.review_required
            or bool(selected_preset and selected_preset.require_review)
        )
        require_tests = (
            bool(request.get("require_tests", False))
            or assessment.tests_required
            or bool(selected_preset and selected_preset.require_tests)
        )
        raw_plan = request.get("plan")
        parsed_plan: Optional[PlanSpec] = None
        if isinstance(raw_plan, Mapping) and raw_plan.get("nodes"):
            parsed_plan = self._plan_from_payload(raw_plan)
            if selected_preset:
                parsed_plan = self._apply_runtime_preset(parsed_plan, selected_preset)
            validate_plan(parsed_plan)
            self._validate_plan_budget(normalized_budget, parsed_plan)
            self._validate_plan_semantics(
                parsed_plan,
                require_review=require_review,
                require_tests=require_tests,
                read_only=read_only,
            )
        elif selected_preset:
            # Validate the generated preset graph before the task is persisted or
            # submitted. A task whose budget cannot reserve all preset work units is
            # a request error, not a recoverable scheduler failure.
            parsed_plan = self._preset_plan_spec(
                selected_preset,
                objective=objective,
                acceptance_criteria=criteria,
                workspace=str(workspace) if workspace else None,
                read_only=read_only,
            )
            self._validate_plan_budget(normalized_budget, parsed_plan)
            self._validate_plan_semantics(
                parsed_plan,
                require_review=require_review,
                require_tests=require_tests,
                read_only=read_only,
            )
        if (
            _parent_context is None
            and domain is TaskDomain.CODE
            and not read_only
            and primary_profile.role is not AgentRole.WORKER
            and (
                parsed_plan is None
                or not self._plan_has_worker_producer(parsed_plan)
            )
        ):
            raise ValueError(
                "a writable code task with primary profile role "
                f"{primary_profile.role.value!r} requires profile_id='worker' unless "
                "runtime_preset_id or a custom plan supplies a validated execute "
                "node with a Worker profile; alternatively set read_only=true"
            )
        preset_snapshot = selected_preset.to_dict() if selected_preset else None
        policy = {
            **assessment.as_dict()["policy"],
            "assessment": assessment.as_dict(),
            "profile_id": profile_id,
            "model_policy_id": model_policy_id,
            "require_review": require_review,
            "require_tests": require_tests,
            "read_only": read_only,
            "network": bool(request.get("network", False)),
            "external_writes": external_writes,
            "structured_handoff": bool(
                brief_draft is not None
                and self.handoff_settings.structured_handoff_enabled
            ),
            "runtime_preset_id": selected_preset.preset_id if selected_preset else None,
            "runtime_preset_version": selected_preset.version if selected_preset else None,
            "runtime_preset_hash": (
                preset_snapshot["content_hash"] if preset_snapshot else None
            ),
            "runtime_preset_snapshot": preset_snapshot,
        }
        input_data = dict(request.get("input") or {})
        if request.get("plan") is not None:
            input_data["plan"] = request["plan"]
        if request.get("requested_model"):
            input_data["requested_model"] = str(request["requested_model"])
        if selected_preset:
            input_data["runtime_preset_id"] = selected_preset.preset_id
        idempotency_key = str(request.get("idempotency_key") or f"api:{uuid.uuid4().hex}")
        if request.get("parent_task_id") and _parent_context is None:
            raise ValueError("child tasks must be created by an owned parent Agent runtime")
        if _parent_context is not None and (
            str(_parent_context.get("parent_task_id") or "")
            != str(request.get("parent_task_id") or "")
            or str(_parent_context.get("parent_run_id") or "")
            != str((input_data.get("_runtime") or {}).get("parent_run_id") or "")
        ):
            raise ValueError("child runtime parent context does not match durable metadata")
        task = self.store.create_task(
            TaskSpec(
                idempotency_key=idempotency_key,
                title=str(
                    brief_draft.title
                    if brief_draft
                    else request.get("title") or objective[:120]
                ),
                objective=objective,
                domain=domain,
                workspace=str(workspace) if workspace else None,
                constraints=constraints,
                acceptance_criteria=criteria,
                complexity_score=assessment.score,
                complexity_level=ComplexityLevel(assessment.level.value),
                risk_tier=RiskTier(assessment.risk.value),
                budget=normalized_budget,
                policy=policy,
                input=input_data,
                priority=int(request.get("priority", 0)),
                max_parallel_runs=max(1, min(int(request.get("max_parallel_runs", 8)), 8)),
                parent_task_id=request.get("parent_task_id"),
                parent_node_id=request.get("parent_node_id"),
            ),
            brief=brief_draft,
            context_refs=context_refs,
            publish_brief=bool(request.get("publish_brief", True)),
            command_id=str(request.get("command_id") or _command("create", idempotency_key)),
        )
        if brief_draft is not None and bool(request.get("publish_brief", True)):
            self.handoff_metrics.increment("orchestration_brief_published_total")
        if context_refs:
            self.handoff_metrics.increment(
                "orchestration_context_refs_created_total", len(context_refs)
            )
        self.store.add_evidence(
            task.id,
            kind=EvidenceKind.DECISION,
            payload={"title": "Complexity assessment", **assessment.as_dict()},
            created_by="orchestration-policy",
            command_id=_command("assessment", task.id),
        )
        if preset_snapshot:
            self.store.add_evidence(
                task.id,
                kind=EvidenceKind.DECISION,
                payload={
                    "title": "Runtime preset selected",
                    "preset": preset_snapshot,
                    "fallback_mode": selected_preset.fallback_mode,
                },
                created_by="orchestration-runtime-preset",
                command_id=_command(
                    "runtime-preset",
                    task.id,
                    selected_preset.preset_id,
                    selected_preset.version,
                ),
            )
        if (
            bool(request.get("auto_start", True))
            and bool(request.get("publish_brief", True))
            and task.status is TaskStatus.DRAFT
        ):
            task = self.submit_task(task.id)
        self._runtime_for_task(task.id, rebuild=True)
        self.wake()
        return self.task_detail(task.id)

    def submit_task(self, task_id: str) -> TaskRecord:
        task = self.store.get_task(task_id)
        if bool(task.policy.get("structured_handoff")):
            try:
                brief = self.store.get_active_brief(task.id)
            except NotFoundError as exc:
                raise ConflictError(
                    "publish a valid Task Brief before submitting this task"
                ) from exc
            if brief.status is not BriefStatus.PUBLISHED:
                raise ConflictError(
                    "publish a valid Task Brief before submitting this task"
                )
        if task.status is TaskStatus.DRAFT:
            task = self.store.transition_task_status(
                task.id,
                TaskStatus.QUEUED,
                expected_version=task.version,
                command_id=_command("submit", task.id, task.version),
            )
        elif task.status is not TaskStatus.QUEUED:
            raise ConflictError(f"cannot submit a task in {task.status.value} state")
        self.wake()
        return task

    def pause_task(self, task_id: str) -> TaskRecord:
        task = self.store.get_task(task_id)
        publication_fence = (
            self.workspaces.source_fence(task.workspace)
            if task.workspace
            else nullcontext()
        )
        # Pause and formal publication share the same linearization point. A pause
        # that wins the fence prevents the final status recheck from crossing into
        # the user's workspace; a publication that wins completes before PAUSED.
        with publication_fence:
            task = self.store.get_task(task_id)
            if task.status in {
                TaskStatus.RUNNING,
                TaskStatus.QUEUED,
                TaskStatus.WAITING_HUMAN,
                TaskStatus.WAITING_CHILD,
            }:
                task = self.store.transition_task_status(
                    task.id,
                    TaskStatus.PAUSED,
                    expected_version=task.version,
                    command_id=f"pause-{uuid.uuid4().hex}",
                )
            elif task.status is not TaskStatus.PAUSED:
                raise ConflictError(f"cannot pause a task in {task.status.value} state")
        # PAUSED is an execution stop, not only a scheduler flag. Interrupt every
        # currently owned process for this task without canceling its asyncio job:
        # the executor can reap its process tree, persist measured usage, and leave
        # a normal retry/reconciliation record for a later resume.
        interrupt = getattr(self.executor, "interrupt", None)
        if callable(interrupt):
            for run in self.store.list_runs(
                task.id,
                statuses=(RunStatus.CLAIMED, RunStatus.RUNNING),
                limit=1_000,
            ):
                interrupt(run.id)
        return task

    def resume_task(self, task_id: str) -> TaskRecord:
        task = self.store.get_task(task_id)
        if task.status in {TaskStatus.PAUSED, TaskStatus.BLOCKED, TaskStatus.NEEDS_RECONCILIATION}:
            target = TaskStatus.QUEUED if task.status is TaskStatus.PAUSED else TaskStatus.RUNNING
            task = self.store.transition_task_status(
                task.id,
                target,
                expected_version=task.version,
                command_id=f"resume-{uuid.uuid4().hex}",
            )
        elif task.status not in {TaskStatus.QUEUED, TaskStatus.RUNNING}:
            raise ConflictError(f"cannot resume a task in {task.status.value} state")
        self.wake()
        return task

    def cancel_task(self, task_id: str) -> TaskRecord:
        all_tasks = self.store.list_task_tree(task_id)
        descendants: list[TaskRecord] = []

        def collect(parent_id: str) -> None:
            for child in all_tasks:
                if child.parent_task_id == parent_id:
                    collect(child.id)
                    descendants.append(child)

        collect(task_id)
        task = self.store.get_task(task_id)
        # Persist the root cancellation intent before touching descendants.  If the
        # process stops halfway through the cascade, the coordinator can discover the
        # root in CANCELING and deterministically re-drive the whole subtree.
        for current in [task, *descendants]:
            fresh = self.store.get_task(current.id)
            publication_fence = (
                self.workspaces.source_fence(fresh.workspace)
                if fresh.workspace
                else nullcontext()
            )
            # This is the same cross-process source fence held by formal publication.
            # Whichever command acquires it first is the externally visible order:
            # cancellation first prevents delivery; publication first completes before
            # the durable CANCELING transition.
            with publication_fence:
                fresh = self.store.get_task(current.id)
                if fresh.status in _TERMINAL_TASKS:
                    continue
                if self._is_task_quality_v2(fresh.id):
                    with self.store._read() as connection:
                        quality_row = connection.execute(
                            "SELECT workflow_status FROM orch_tasks WHERE id=?",
                            (fresh.id,),
                        ).fetchone()
                    quality_status = str(
                        quality_row["workflow_status"] if quality_row is not None else ""
                    )
                    if quality_status not in {"canceled", "archived"}:
                        apply_workflow_event(
                            self.store,
                            task_id=fresh.id,
                            event=WorkflowEvent.CANCEL_REQUESTED,
                            reason_code="user_canceled",
                            command_id=f"quality-cancel:{fresh.id}",
                        )
                if fresh.status is TaskStatus.DRAFT:
                    canceled = self.store.transition_task_status(
                        fresh.id,
                        TaskStatus.CANCELED,
                        expected_version=fresh.version,
                        command_id=f"cancel-{uuid.uuid4().hex}",
                    )
                    self.relations.resolve_terminal(canceled.id)
                    continue
                if fresh.status is not TaskStatus.CANCELING:
                    fresh = self.store.transition_task_status(
                        fresh.id,
                        TaskStatus.CANCELING,
                        expected_version=fresh.version,
                        command_id=f"cancel-request-{uuid.uuid4().hex}",
                    )
                self.store.cancel_task_runs(
                    fresh.id,
                    command_id=_command("cancel-pending-runs", fresh.id, fresh.version),
                )
                for run in self.store.list_runs(fresh.id):
                    if (
                        run.status is RunStatus.SUCCEEDED
                        and self._workspace_commit_status(run) == "pending"
                    ):
                        self.store.merge_run_output(
                            run.id,
                            {
                                "workspace_commit": {
                                    **dict((run.output or {}).get("workspace_commit") or {}),
                                    "status": "aborted",
                                    "reason": "owning task was canceled before candidate commit",
                                }
                            },
                            allowed_statuses=(RunStatus.SUCCEEDED,),
                            command_id=_command("cancel-workspace-commit", run.id),
                        )
                    job = self._jobs.get(run.id)
                    if job is not None:
                        interrupt = getattr(self.executor, "interrupt", None)
                        if callable(interrupt):
                            interrupt(run.id)
                        loop = self._loop
                        if loop is not None and loop.is_running():
                            loop.call_soon_threadsafe(job.cancel)
                        else:
                            job.cancel()
        try:
            runtime = self._runtime_for_task(task.id, rebuild=True)
            node = runtime.get(self._task_runtime_id(task.id))
            if not node.status.terminal:
                runtime.cancel(node.runtime_id)
        except (RuntimeStateError, KeyError):
            pass
        task = self.store.get_task(task.id)
        self.wake()
        return task

    def archive_task(self, task_id: str) -> TaskRecord:
        task = self.store.get_task(task_id)
        if task.status in {TaskStatus.COMPLETED, TaskStatus.CANCELED, TaskStatus.FAILED}:
            if self._is_task_quality_v2(task.id):
                with self.store._read() as connection:
                    workflow = connection.execute(
                        "SELECT workflow_status FROM orch_tasks WHERE id=?",
                        (task.id,),
                    ).fetchone()["workflow_status"]
                if workflow != "archived":
                    apply_workflow_event(
                        self.store,
                        task_id=task.id,
                        event=WorkflowEvent.ARCHIVE_REQUESTED,
                        command_id=f"quality-archive:{task.id}",
                    )
            task = self.store.transition_task_status(
                task.id,
                TaskStatus.ARCHIVED,
                expected_version=task.version,
                output={**dict(task.output or {}), "archived_from": task.status.value},
                command_id=f"archive-{uuid.uuid4().hex}",
            )
            self.relations.resolve_terminal(task.id)
        elif task.status is not TaskStatus.ARCHIVED:
            raise ConflictError(f"cannot archive a task in {task.status.value} state")
        self._cleanup_task_workspaces(task.id)
        return task

    def restore_task(self, task_id: str) -> TaskRecord:
        task = self.store.get_task(task_id)
        if self._is_task_quality_v2(task.id):
            raise ConflictError(
                "archived Task Quality V2 tasks are immutable restore-view records"
            )
        if task.status is TaskStatus.ARCHIVED:
            archived_from = str(
                dict(task.output or {}).get("archived_from") or "completed"
            )
            if archived_from != TaskStatus.COMPLETED.value:
                raise ConflictError(
                    "only successfully completed tasks can be restored"
                )
            restored_output = dict(task.output or {})
            restored_output.pop("archived_from", None)
            task = self.store.transition_task_status(
                task.id,
                TaskStatus.COMPLETED,
                expected_version=task.version,
                output=restored_output,
                command_id=f"restore-{uuid.uuid4().hex}",
            )
        elif task.status is not TaskStatus.COMPLETED:
            raise ConflictError(f"cannot restore a task in {task.status.value} state")
        return task

    def _cleanup_task_workspaces(self, task_id: str) -> None:
        for run in self.store.list_runs(task_id):
            snapshot_id = f"run-{run.id}"[:95]
            try:
                snapshot = self.workspaces.load(snapshot_id)
            except KeyError:
                continue
            try:
                self.workspaces.cleanup(snapshot)
            except WorkspaceError:
                # An incomplete delivery must retain its backup. Startup recovery runs
                # before the archived-workspace sweep and will make a later retry safe.
                logger.exception("could not clean archived workspace %s", snapshot_id)
        task_snapshot_id = f"task-{task_id}"[:95]
        try:
            task_snapshot = self.workspaces.load(task_snapshot_id)
        except KeyError:
            return
        try:
            self.workspaces.cleanup(task_snapshot)
        except WorkspaceError:
            logger.exception("could not clean archived task workspace %s", task_snapshot_id)

    def _cleanup_archived_workspaces(self) -> None:
        for task in self._all_tasks(statuses=(TaskStatus.ARCHIVED,)):
            self._cleanup_task_workspaces(task.id)

    def _valid_run_checkpoint(self, gate: GateRecord) -> bool:
        if gate.run_id is None:
            return True
        try:
            run = self.store.get_run(gate.run_id)
            checkpoint = dict((run.output or {}).get("engine_checkpoint") or {})
            if (
                run.status is not RunStatus.WAITING_GATE
                or str(checkpoint.get("gate_id") or "") != gate.id
                or checkpoint.get("recovery_disposition") != "pending_tools"
                or not checkpoint.get("pending_tool_call_ids")
            ):
                return False
            self._verified_checkpoint_payload(run, gate.id, checkpoint)
            return True
        except Exception:
            return False

    def _verified_checkpoint_payload(
        self,
        run: RunRecord,
        gate_id: str,
        checkpoint: Mapping[str, Any],
        *,
        require_reference_identity: bool = False,
    ) -> dict[str, Any]:
        """Load and bind a checkpoint blob to one exact fenced run attempt."""

        digest = str(checkpoint.get("blob_sha256") or "")
        uri = str(checkpoint.get("blob_uri") or "")
        if uri != f"sha256:{digest}":
            raise ConflictError("checkpoint URI and digest do not match")
        try:
            payload = json.loads(self.blobs.get(uri).decode("utf-8"))
        except Exception as exc:
            raise ConflictError(f"checkpoint blob is missing or corrupt: {exc}") from exc
        if not isinstance(payload, Mapping):
            raise ConflictError("checkpoint blob must contain a JSON object")
        pending_ids = [str(value) for value in checkpoint.get("pending_tool_call_ids") or ()]
        expected_session = run.session_id or str(payload.get("session_id") or "")
        try:
            payload_attempt = int(payload.get("attempt", -1))
            payload_fence = int(payload.get("fencing_token", -1))
            reference_attempt = int(checkpoint.get("attempt", -1))
            reference_fence = int(checkpoint.get("fencing_token", -1))
        except (TypeError, ValueError) as exc:
            raise ConflictError("checkpoint attempt/fencing token is invalid") from exc
        if (
            payload.get("schema_version") != 1
            or str(payload.get("run_id") or "") != run.id
            or payload_attempt != run.attempt
            or payload_fence != run.fencing_token
            or str(payload.get("session_id") or "") != expected_session
            or str(payload.get("gate_id") or "") != gate_id
            or payload.get("recovery_disposition") != "pending_tools"
            or list(payload.get("pending_tool_call_ids") or ()) != pending_ids
            or not pending_ids
        ):
            raise ConflictError("checkpoint blob does not match the active run/gate")
        if require_reference_identity and (
            checkpoint.get("schema_version") != 1
            or str(checkpoint.get("run_id") or "") != run.id
            or reference_attempt != run.attempt
            or reference_fence != run.fencing_token
            or str(checkpoint.get("session_id") or "") != expected_session
        ):
            raise ConflictError("checkpoint reference does not match the active run")
        return dict(payload)

    def _reconcile_orphaned_gate_checkpoints(self) -> None:
        for task in self._all_tasks(
            statuses=(TaskStatus.WAITING_HUMAN, TaskStatus.WAITING_CHILD)
        ):
            for gate in self.store.list_gates(task.id, statuses=(GateStatus.OPEN,)):
                if gate.run_id is None or self._valid_run_checkpoint(gate):
                    continue
                self.store.replace_orphaned_run_gate(
                    gate.id,
                    reason=(
                        "The process stopped after opening the interaction gate but before "
                        "a complete, hash-verified Agent checkpoint became durable. Automatic "
                        "re-execution is disabled to avoid duplicate side effects."
                    ),
                    command_id=_command("orphaned-run-gate", gate.id),
                )

    def _repair_legacy_final_rejections(self) -> None:
        """Repair preview-era two-transaction final rejections on startup."""

        for task in self._all_tasks(statuses=(TaskStatus.BLOCKED,)):
            rejected = next(
                (
                    gate
                    for gate in reversed(self.store.list_gates(task.id))
                    if gate.kind is GateKind.FINAL_ACCEPTANCE
                    and gate.status is GateStatus.REJECTED
                ),
                None,
            )
            if rejected is None:
                continue
            fresh = self.store.get_task(task.id)
            failed = self.store.transition_task_status(
                fresh.id,
                TaskStatus.FAILED,
                expected_version=fresh.version,
                output={
                    **dict(fresh.output or {}),
                    "accepted": False,
                    "gate_id": rejected.id,
                    "reason": str(
                        (rejected.resolution or {}).get("response")
                        or "final acceptance rejected"
                    ),
                },
                command_id=_command("repair-final-rejection", rejected.id),
            )
            self.relations.resolve_terminal(failed.id)

    def _repair_superseded_policy_skip_gates(self) -> int:
        """Close reconciliation gates made stale by a successful explicit retry.

        Older coordinators evaluated policy-controlled skips as final even after
        their source node later succeeded.  A crash between the gate repair and
        stage transition is safe: the approved recovery decision is deterministic
        and the next startup completes the remaining transition.
        """

        repaired = 0
        for task in self._all_tasks(
            statuses=(TaskStatus.WAITING_HUMAN, TaskStatus.RUNNING)
        ):
            if (
                task.current_stage is not OrchestrationStage.INTER_STEP_EVALUATION
                or not task.active_plan_id
            ):
                continue
            graph = self.store.get_plan(task.active_plan_id)
            latest = self._latest_runs(
                [
                    run
                    for run in self.store.list_runs(task.id)
                    if run.plan_id == graph.plan.id
                ]
            )
            superseded = self._superseded_policy_skips(latest)
            if not superseded or any(
                run.status in _FAILED_RUNS for run in latest.values()
            ):
                continue
            superseded_run_ids = {
                skipped.id for skipped, _source in superseded.values()
            }
            gates = [
                gate
                for gate in self.store.list_gates(task.id)
                if gate.kind is GateKind.RECONCILIATION
                and gate.run_id is None
                and gate.source_key.startswith(
                    f"{task.id}:reconciliation:{graph.plan.id}:"
                )
                and (
                    gate.status is GateStatus.OPEN
                    or (
                        gate.status is GateStatus.APPROVED
                        and str((gate.resolution or {}).get("decision") or "")
                        == "resume_policy_skips"
                    )
                )
            ]
            if len(gates) != 1:
                continue
            gate = gates[0]
            if gate.status is GateStatus.OPEN:
                prompt = dict(gate.prompt)
                if any(
                    prompt.get(key)
                    for key in (
                        "failed_runs",
                        "workspace_commit_failures",
                        "failed_children",
                    )
                ):
                    continue
                adverse = [
                    report
                    for report in (prompt.get("verification") or ())
                    if isinstance(report, Mapping)
                    and str(report.get("status") or "") != "pass"
                ]
                if any(
                    str(report.get("run_id") or "") not in superseded_run_ids
                    for report in adverse
                ):
                    continue
                self.store.resolve_gate(
                    gate.id,
                    GateStatus.APPROVED,
                    {
                        "decision": "resume_policy_skips",
                        "response": (
                            "A successful source retry superseded unstarted "
                            "failure-policy skips."
                        ),
                    },
                    resolved_by="orchestration-recovery",
                    expected_version=gate.version,
                    command_id=_command("repair-policy-skip-gate", gate.id),
                )
            elif str((gate.resolution or {}).get("decision") or "") != (
                "resume_policy_skips"
            ):
                continue
            fresh = self.store.get_task(task.id)
            if (
                fresh.status is TaskStatus.RUNNING
                and fresh.current_stage is OrchestrationStage.INTER_STEP_EVALUATION
            ):
                self._transition_stage(
                    fresh,
                    OrchestrationStage.EXECUTION_REVIEW_TEST,
                    "repair-successful-retry-policy-skips",
                )
                repaired += 1
        return repaired

    def _repair_legacy_subscription_work_products(self) -> int:
        """Publish safe handoff records for pre-adapter subscription results.

        Older subscription runtimes stored their validated structured result only in
        ``orch_runs.output_json``. Isolated downstream roles correctly refuse to read
        another Agent's private transcript/output, so those otherwise successful runs
        appeared to have produced no candidate. Recovery creates an immutable,
        task-owned compatibility Work Product from that already-durable public result.
        New runs publish a run-owned product before settlement and never enter here.
        """

        repaired = 0
        kinds = {
            NodeKind.EXECUTE: WorkProductKind.ARTIFACT,
            NodeKind.INTEGRATE: WorkProductKind.ARTIFACT,
            NodeKind.REVIEW: WorkProductKind.REVIEW_REPORT,
            NodeKind.TEST: WorkProductKind.TEST_RESULT,
            NodeKind.EVALUATE: WorkProductKind.EVALUATION,
        }
        for task in self._all_tasks(
            statuses=(
                TaskStatus.RUNNING,
                TaskStatus.WAITING_HUMAN,
                TaskStatus.WAITING_CHILD,
                TaskStatus.PAUSED,
                TaskStatus.BLOCKED,
                TaskStatus.NEEDS_RECONCILIATION,
            )
        ):
            products = self.store.list_work_products(task.id, limit=1_000)
            represented_runs = {
                str(product.run_id or product.metadata.get("source_run_id") or "")
                for product in products
            }
            graphs: dict[str, PlanGraph] = {}
            for run in self.store.list_runs(task.id):
                output = dict(run.output or {})
                structured = output.get("structured_result")
                if (
                    run.status is not RunStatus.SUCCEEDED
                    or run.id in represented_runs
                    or not isinstance(structured, Mapping)
                    or not isinstance(output.get("subscription_runtime"), Mapping)
                ):
                    continue
                graph = graphs.get(run.plan_id)
                if graph is None:
                    graph = self.store.get_plan(run.plan_id)
                    graphs[run.plan_id] = graph
                node = next(
                    (item for item in graph.nodes if item.id == run.node_id), None
                )
                if node is None:
                    continue
                metadata: dict[str, Any] = {
                    "source": "legacy_subscription_structured_result",
                    "source_run_id": run.id,
                    "node_key": node.key,
                    "node_kind": node.kind.value,
                    "status": str(structured.get("status") or "unknown").lower(),
                }
                title = f"{node.title or node.key} result"
                if run.brief_id and node.kind is NodeKind.EXECUTE:
                    brief = self.store.get_brief_by_id(run.brief_id)
                    required = [
                        item
                        for item in brief.deliverables
                        if bool(item.get("required", True))
                    ]
                    if len(required) == 1:
                        deliverable = required[0]
                        metadata["deliverable_id"] = str(
                            deliverable.get("id") or ""
                        )
                        title = str(
                            deliverable.get("title")
                            or deliverable.get("kind")
                            or title
                        )
                safe_title = bounded_activity_text(title, 500)
                safe_summary = bounded_activity_text(
                    str(structured.get("summary") or ""), 16_000
                )
                if contains_secret_like(safe_title):
                    safe_title = f"Recovered {node.key} result"
                if contains_secret_like(safe_summary):
                    safe_summary = (
                        "[redacted legacy result summary; inspect the authorized "
                        "artifact through an audited context read]"
                    )
                try:
                    artifact = self.blobs.put_json(
                        {
                            "schema_version": 1,
                            "task_id": task.id,
                            "source_run_id": run.id,
                            "node_key": node.key,
                            "structured_result": dict(structured),
                        }
                    )
                    self.work_products.create(
                        task.id,
                        kind=kinds.get(node.kind, WorkProductKind.OTHER),
                        title=safe_title,
                        summary=safe_summary,
                        artifact_id=artifact.uri,
                        uri=artifact.uri,
                        content_hash=f"sha256:{artifact.sha256}",
                        metadata=metadata,
                        verification_status="unverified",
                        created_by="orchestration-recovery",
                        command_id=_command(
                            "repair-subscription-work-product",
                            run.id,
                            artifact.sha256,
                        ),
                    )
                except (OSError, PermissionError, TypeError, ValueError):
                    logger.warning(
                        "could not backfill subscription Work Product for run %s",
                        run.id,
                        exc_info=True,
                    )
                    continue
                represented_runs.add(run.id)
                repaired += 1
        return repaired

    def _repair_evaluator_adjudicated_gates(self) -> int:
        """Close legacy verifier-conflict gates an Evaluator already settled.

        Recovery is deliberately narrow: only the active plan's task-owned
        reconciliation gate, with no execution/workspace/child failure and the
        exact current verification runs, may be resolved automatically.
        """

        repaired = 0
        for task in self._all_tasks(statuses=(TaskStatus.WAITING_HUMAN,)):
            if (
                task.current_stage is not OrchestrationStage.INTER_STEP_EVALUATION
                or not task.active_plan_id
            ):
                continue
            graph = self.store.get_plan(task.active_plan_id)
            latest = self._latest_runs(
                run
                for run in self.store.list_runs(task.id)
                if run.plan_id == graph.plan.id
            )
            if any(
                run.status in _FAILED_RUNS
                or self._workspace_commit_status(run) == "failed"
                for run in latest.values()
            ) or any(
                self._terminal_outcome(child)
                in {TaskStatus.FAILED, TaskStatus.CANCELED}
                for child in self._plan_descendants(task.id, graph.plan.id)
            ):
                # An execution reconciliation gate cannot be auto-settled by a
                # verifier verdict. More importantly, its retained candidate may
                # be enormous; startup recovery must not hash it pointlessly.
                continue
            subject = self._candidate_subject(task, graph, latest)
            verification = self._verification_reports(
                task.id,
                graph,
                latest,
                expected_subject=subject,
            )
            adjudication = self._verification_adjudication(verification)
            if not adjudication["adjudicated"] or not adjudication["dissent"]:
                continue
            open_gates = [
                gate
                for gate in self.store.list_gates(
                    task.id, statuses=(GateStatus.OPEN,)
                )
                if gate.kind is GateKind.RECONCILIATION
                and gate.run_id is None
                and gate.source_key.startswith(
                    f"{task.id}:reconciliation:{graph.plan.id}:"
                )
            ]
            if len(open_gates) != 1:
                continue
            gate = open_gates[0]
            prompt = dict(gate.prompt)
            if any(
                prompt.get(key)
                for key in (
                    "failed_runs",
                    "workspace_commit_failures",
                    "failed_children",
                )
            ):
                continue
            expected_runs = sorted(
                (
                    str(report.get("node_id") or report.get("node_key") or ""),
                    str(report.get("run_id") or ""),
                )
                for report in verification
            )
            prompt_runs = sorted(
                (
                    str(report.get("node_id") or report.get("node_key") or ""),
                    str(report.get("run_id") or ""),
                )
                for report in (prompt.get("verification") or ())
                if isinstance(report, Mapping)
            )
            if prompt_runs != expected_runs:
                continue
            self.store.resolve_gate(
                gate.id,
                GateStatus.APPROVED,
                {
                    "decision": "evaluator_adjudicated",
                    "response": (
                        "The downstream Evaluator reviewed the independent "
                        "verdicts and issued a complete passing decision for the "
                        "current candidate. Earlier dissent remains in the audit."
                    ),
                },
                resolved_by="orchestration-evaluator",
                expected_version=gate.version,
                command_id=_command("repair-evaluator-adjudication", gate.id),
            )
            repaired += 1
        return repaired

    def _legacy_handoff_verification_candidates(
        self,
        task_id: str,
        graph: PlanGraph,
        latest: Mapping[str, RunRecord],
        reports: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Return verifier attempts affected by the pre-Work-Product handoff bug."""

        recovered_run_ids = {
            str(product.metadata.get("source_run_id") or "")
            for product in self.store.list_work_products(task_id, limit=1_000)
            if str(product.metadata.get("source") or "")
            == "legacy_subscription_structured_result"
        }
        if not recovered_run_ids:
            return {}
        nodes = {node.key: node for node in graph.nodes}
        candidates: dict[str, dict[str, Any]] = {}
        for report in reports:
            if str(report.get("status") or "unknown") == "pass":
                continue
            node_key = str(report.get("node_key") or "")
            node = nodes.get(node_key)
            run = latest.get(node_key)
            if (
                node is None
                or run is None
                or node.kind
                not in {NodeKind.REVIEW, NodeKind.TEST, NodeKind.EVALUATE}
                or run.status is not RunStatus.SUCCEEDED
                or run.id not in recovered_run_ids
                or not isinstance(
                    dict(run.output or {}).get("subscription_runtime"), Mapping
                )
            ):
                continue
            candidates[node_key] = {
                "run_id": run.id,
                "attempt": run.attempt,
            }
        return candidates

    def _legacy_handoff_retry_continuation_candidates(
        self,
        task_id: str,
        graph: PlanGraph,
        latest: Mapping[str, RunRecord],
        reports: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Allow one final verifier replay after the legacy repair exposed evidence.

        The first compatibility replay can itself have received a greedy bounded
        Work Product prompt produced by the old coordinator. Only runs exactly one
        attempt beyond an approved *legacy* repair qualify, so this cannot create an
        unbounded chain of extra attempts.
        """

        legacy_bases = [
            self._compatibility_retry_base_attempts(gate)
            for gate in self.store.list_gates(task_id)
            if gate.kind is GateKind.RECONCILIATION
            and gate.status is GateStatus.APPROVED
            and str((gate.resolution or {}).get("decision") or "") == "retry"
            and isinstance(gate.prompt.get("compatibility_retry"), Mapping)
            and str(
                (gate.prompt.get("compatibility_retry") or {}).get("reason") or ""
            )
            == _LEGACY_HANDOFF_RETRY_REASON
        ]
        legacy_bases = [item for item in legacy_bases if item]
        if not legacy_bases:
            return {}
        represented_runs = {
            str(product.run_id or product.metadata.get("source_run_id") or "")
            for product in self.store.list_work_products(task_id, limit=1_000)
        }
        nodes = {node.key: node for node in graph.nodes}
        candidates: dict[str, dict[str, Any]] = {}
        for report in reports:
            if str(report.get("status") or "unknown") == "pass":
                continue
            node_key = str(report.get("node_key") or "")
            node = nodes.get(node_key)
            run = latest.get(node_key)
            if (
                node is None
                or run is None
                or node.kind
                not in {NodeKind.REVIEW, NodeKind.TEST, NodeKind.EVALUATE}
                or run.status is not RunStatus.SUCCEEDED
                or run.id not in represented_runs
                or not isinstance(
                    dict(run.output or {}).get("subscription_runtime"), Mapping
                )
            ):
                continue
            if any(
                (base := attempts.get(node_key)) is not None
                and run.attempt == int(base["attempt"]) + 1
                for attempts in legacy_bases
            ):
                candidates[node_key] = {
                    "run_id": run.id,
                    "attempt": run.attempt,
                }
        return candidates

    def _repair_legacy_verification_reconciliation_gates(self) -> int:
        """Upgrade open verifier gates without discarding completed results."""

        repaired = 0
        for task in self._all_tasks(statuses=(TaskStatus.WAITING_HUMAN,)):
            if (
                task.current_stage is not OrchestrationStage.INTER_STEP_EVALUATION
                or not task.active_plan_id
            ):
                continue
            graph = self.store.get_plan(task.active_plan_id)
            latest = self._latest_runs(
                run
                for run in self.store.list_runs(task.id)
                if run.plan_id == graph.plan.id
            )
            prefix = f"{task.id}:reconciliation:{graph.plan.id}:"
            for gate in self.store.list_gates(
                task.id, statuses=(GateStatus.OPEN,)
            ):
                if (
                    gate.kind is not GateKind.RECONCILIATION
                    or gate.run_id is not None
                    or not gate.source_key.startswith(prefix)
                ):
                    continue
                prompt = dict(gate.prompt)
                action_ids = {
                    str(
                        item.get("id") or item.get("action") or ""
                        if isinstance(item, Mapping)
                        else item
                    )
                    for item in prompt.get("actions") or ()
                }
                if any(
                    prompt.get(key)
                    for key in (
                        "failed_runs",
                        "workspace_commit_failures",
                        "failed_children",
                    )
                ):
                    continue
                adverse = [
                    report
                    for report in (prompt.get("verification") or ())
                    if isinstance(report, Mapping)
                    and str(report.get("status") or "unknown") != "pass"
                ]
                existing_compatibility = isinstance(
                    prompt.get("compatibility_retry"), Mapping
                )
                compatibility = (
                    {}
                    if existing_compatibility
                    else self._legacy_handoff_verification_candidates(
                        task.id, graph, latest, adverse
                    )
                )
                compatibility_reason = _LEGACY_HANDOFF_RETRY_REASON
                if not compatibility and not existing_compatibility:
                    compatibility = (
                        self._legacy_handoff_retry_continuation_candidates(
                            task.id, graph, latest, adverse
                        )
                    )
                    compatibility_reason = _BOUNDED_ENVELOPE_RETRY_REASON
                retryable = self._retryable_adverse_verification_runs(
                    graph, latest, adverse
                )
                can_retry = bool(
                    existing_compatibility or compatibility or retryable
                )
                if "accept_current" in action_ids and (
                    ("retry" in action_ids) == can_retry
                ):
                    continue
                actions: list[Any] = [
                    {
                        "id": "accept_current",
                        "label": "Accept current results",
                        "tone": "primary",
                        "requires_response": True,
                    }
                ]
                if can_retry:
                    actions.append(
                        {
                            "id": "retry",
                            "label": "Re-run disputed checks",
                            "tone": "neutral",
                        }
                    )
                actions.extend(
                    [
                        {
                            "id": "request_changes",
                            "label": "Revise deliverable",
                            "tone": "neutral",
                            "requires_response": True,
                        },
                        "cancel",
                    ]
                )
                prompt.update(
                    {
                        "title": "Verification needs reconciliation",
                        "description": (
                            "Execution and its completed work products are preserved, "
                            "but the verification roles did not reach a decisive "
                            "result. Accept the current evidence, re-run only the "
                            "disputed checks, or request a deliverable revision."
                        ),
                        "verification_signature": (
                            self._verification_signature(
                                [
                                    report
                                    for report in prompt.get("verification") or ()
                                    if isinstance(report, Mapping)
                                ]
                            )
                        ),
                        "completed_work_products": len(
                            self.store.list_work_products(
                                task.id, limit=10_000
                            )
                        ),
                        "actions": actions,
                    }
                )
                if compatibility:
                    prompt["compatibility_retry"] = {
                        "reason": compatibility_reason,
                        "base_attempts": compatibility,
                    }
                self.store.amend_task_gate_prompt(
                    gate.id,
                    prompt,
                    expected_version=gate.version,
                    command_id=_command(
                        "repair-verdict-decision-gate", gate.id, "v2"
                    ),
                )
                repaired += 1
        return repaired

    @staticmethod
    def _task_snapshot_id(task_id: str) -> str:
        return f"task-{task_id}"[:95]

    def _ensure_task_snapshot(self, task: TaskRecord) -> Optional[WorkspaceSnapshot]:
        """Return the durable task candidate whose source is the formal workspace.

        Run snapshots are children of this candidate.  Consequently ordinary run
        delivery only mutates orchestration-owned storage; the user's workspace is
        touched exactly once, after final acceptance.

        Read-only tasks are the deliberate exception: their runtime sandbox already
        prevents writes, so copying a large workspace twice adds no isolation.  They
        read the formal source directly and publish a result set rather than a patch.
        """

        if not task.workspace or bool(task.policy.get("read_only", False)):
            return None
        snapshot_id = self._task_snapshot_id(task.id)
        with self._workspace_commit_lock:
            try:
                snapshot = self.workspaces.load(snapshot_id)
            except KeyError:
                snapshot = self.workspaces.prepare(task.workspace, snapshot_id=snapshot_id)
            if Path(snapshot.source_root).resolve() != Path(task.workspace).resolve():
                raise WorkspaceError(
                    f"task candidate {snapshot_id} belongs to another source workspace"
                )
            return snapshot

    def _candidate_subject(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        latest: Optional[Mapping[str, RunRecord]] = None,
    ) -> dict[str, Any]:
        contract_hash = _canonical_hash(
            {
                "task_id": task.id,
                "plan_id": graph.plan.id,
                "criteria": list(task.acceptance_criteria),
            }
        )
        task_snapshot = self._ensure_task_snapshot(task)
        if task_snapshot is not None:
            candidate = self.workspaces.collect_candidate(task_snapshot)
            return {
                "kind": "workspace_manifest",
                "revision_id": f"{graph.plan.id}:{candidate.candidate_manifest.digest}",
                "manifest_sha256": candidate.candidate_manifest.digest,
                "baseline_manifest_sha256": task_snapshot.baseline_manifest.digest,
                "patch_sha256": candidate.patch_sha256,
                "acceptance_contract_hash": contract_hash,
            }

        chosen = latest or self._latest_runs(
            run for run in self.store.list_runs(task.id) if run.plan_id == graph.plan.id
        )
        producer_outputs: list[dict[str, Any]] = []
        for node in graph.nodes:
            try:
                profile = self._profile_for_node(node)
            except Exception:
                continue
            if not self._profile_mutates_candidate(profile):
                continue
            run = chosen.get(node.key)
            if run is not None and run.status is RunStatus.SUCCEEDED:
                producer_outputs.append(
                    {
                        "node_id": node.id,
                        "run_id": run.id,
                        "output_hash": _canonical_hash(dict(run.output or {})),
                    }
                )
        manifest_hash = _canonical_hash(producer_outputs)
        return {
            "kind": "result_set",
            "revision_id": f"{graph.plan.id}:{manifest_hash}",
            "manifest_sha256": manifest_hash,
            "baseline_manifest_sha256": None,
            "patch_sha256": None,
            "acceptance_contract_hash": contract_hash,
        }

    def _quality_assignment_context(
        self,
        task: TaskRecord,
        node: NodeRecord,
        remaining_budget: RuntimeBudget,
    ) -> dict[str, Any]:
        """Build the bounded authoritative V2 assignment block for one turn."""

        with self.store._read() as connection:
            row = connection.execute(
                """
                SELECT active_contract_id, active_snapshot_id, active_strategy_id
                FROM orch_tasks WHERE id=?
                """,
                (task.id,),
            ).fetchone()
            repair_row = connection.execute(
                """
                SELECT * FROM orch_repair_requests
                WHERE task_id=? AND status IN ('pending','running')
                ORDER BY attempt DESC, created_at DESC LIMIT 1
                """,
                (task.id,),
            ).fetchone()
        if (
            row is None
            or not row["active_contract_id"]
            or not row["active_snapshot_id"]
            or not row["active_strategy_id"]
        ):
            raise ConflictError(
                "Task Quality V2 execution requires frozen contract, snapshot and strategy"
            )
        contract = self.quality_contracts.get(row["active_contract_id"])
        snapshot = self.quality_snapshots.get(row["active_snapshot_id"])
        strategy = self.quality_strategies.get(row["active_strategy_id"])
        contract_value = contract.model_dump(mode="json", exclude_none=True)
        encoded = json.dumps(
            contract_value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        omitted_fields: list[str] = []
        if len(encoded) > 20 * 1024:
            # Required and hard requirements, constraints and deliverables are never
            # omitted. Optional soft requirements remain available through
            # get_task_contract() and are named explicitly here.
            requirements = list(contract_value.get("requirements") or ())
            retained = [
                item
                for item in requirements
                if bool(item.get("required")) or bool(item.get("hard_gate"))
            ]
            omitted = [
                str(item.get("id") or "")
                for item in requirements
                if item not in retained
            ]
            if omitted:
                contract_value["requirements"] = retained
                omitted_fields.append(
                    "contract.requirements.optional:" + ",".join(omitted)
                )
        quality_ledger = None
        binding = self._quality_budget_binding(task.id, node.key)
        if binding is not None and binding[1] is not None:
            quality_ledger = self.quality_budgets.get(binding[1]).model_dump(
                mode="json"
            )
        repair_context = None
        if (
            repair_row is not None
            and str(node.metadata.get("repair_request_id") or "")
            == str(repair_row["id"])
        ):
            repair_context = {
                "id": str(repair_row["id"]),
                "source_artifact_id": str(repair_row["source_artifact_id"]),
                "target_version": int(repair_row["target_version"]),
                "finding_ids": json.loads(repair_row["finding_ids_json"]),
                "allowed_sections": json.loads(repair_row["allowed_sections_json"]),
                "required_validators": json.loads(
                    repair_row["required_validators_json"]
                ),
                "attempt": int(repair_row["attempt"]),
                "status": str(repair_row["status"]),
            }
        return {
            "contract": contract_value,
            "snapshot": snapshot.model_dump(
                mode="json",
                exclude={"workspace_root", "repo_root"},
                exclude_none=True,
            ),
            "strategy": {
                "id": strategy.id,
                "version": strategy.version,
                "content_hash": strategy.content_hash,
                "template_id": strategy.template_id,
                "semantic_scorer_node_key": strategy.semantic_scorer_node_key,
                "effective_policy": dict(strategy.effective_policy),
                "feature_flags": dict(strategy.feature_flags),
            },
            "direct_input_bindings": list(
                node.input.get("direct_bindings") or ()
            ),
            "run_budget_remaining": {
                "model_calls": remaining_budget.model_calls,
                "tool_calls": remaining_budget.tool_calls,
                "reported_tokens": remaining_budget.tokens,
                "active_seconds": remaining_budget.wall_seconds,
            },
            "quality_budget_ledger": quality_ledger,
            "repair_request": repair_context,
            "omitted_fields": omitted_fields,
            "on_demand_tools": [
                "get_task_contract",
                "get_repository_snapshot",
                "get_execution_strategy",
            ],
        }

    def _ensure_run_snapshot(
        self, task: TaskRecord, run: RunRecord
    ) -> Optional[WorkspaceSnapshot]:
        task_snapshot = self._ensure_task_snapshot(task)
        if task_snapshot is None:
            return None
        snapshot_id = f"run-{run.id}"[:95]
        with self._workspace_commit_lock:
            try:
                snapshot = self.workspaces.load(snapshot_id)
            except KeyError:
                snapshot = self.workspaces.prepare(
                    task_snapshot.candidate, snapshot_id=snapshot_id
                )
            if snapshot.source_root.resolve() != task_snapshot.candidate.resolve():
                raise WorkspaceError(
                    f"run snapshot {snapshot_id} is not based on task candidate"
                )
            return snapshot

    @staticmethod
    def _workspace_commit_status(run: RunRecord) -> Optional[str]:
        commit = dict((run.output or {}).get("workspace_commit") or {})
        return str(commit.get("status") or "") or None

    def _upstream_context(
        self, task: TaskRecord, graph: PlanGraph, node: NodeRecord
    ) -> tuple[Mapping[str, Any], ...]:
        parents: dict[str, set[str]] = {item.key: set() for item in graph.nodes}
        for edge in graph.edges:
            parents[edge.to_node].add(edge.from_node)
        distances: dict[str, int] = {}
        pending = [(key, 1) for key in parents[node.key]]
        while pending:
            key, distance = pending.pop()
            previous = distances.get(key)
            if previous is not None and previous <= distance:
                continue
            distances[key] = distance
            pending.extend((parent, distance + 1) for parent in parents[key])
        latest = self._latest_runs(
            run for run in self.store.list_runs(task.id) if run.plan_id == graph.plan.id
        )
        result: list[Mapping[str, Any]] = []
        graph_order = {item.key: index for index, item in enumerate(graph.nodes)}
        upstream_nodes = sorted(
            (item for item in graph.nodes if item.key in distances),
            key=lambda item: (distances[item.key], graph_order[item.key]),
        )
        for upstream in upstream_nodes:
            run = latest.get(upstream.key)
            if run is None:
                continue
            result.append(
                {
                    "node_id": upstream.id,
                    "node_key": upstream.key,
                    "kind": upstream.kind.value,
                    "run_id": run.id,
                    "distance": distances[upstream.key],
                    "status": run.status.value,
                    "output": dict(run.output or {}),
                }
            )
        return tuple(result)

    def _published_subject(
        self, task_id: str, plan_id: str, manifest_sha256: str
    ) -> Optional[Mapping[str, Any]]:
        for item in reversed(self.store.list_evidence(task_id)):
            payload = dict(item.payload)
            if (
                payload.get("action") == "workspace_published"
                and item.plan_id == plan_id
                and str((payload.get("subject") or {}).get("manifest_sha256") or "")
                == manifest_sha256
            ):
                return payload
        return None

    def _publish_task_candidate(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        subject: Mapping[str, Any],
        *,
        actor: str,
    ) -> Optional[Mapping[str, Any]]:
        snapshot = self._ensure_task_snapshot(task)
        if snapshot is None:
            return {"action": "result_accepted", "subject": dict(subject)}
        manifest_sha256 = str(subject.get("manifest_sha256") or "")
        patch_sha256 = str(subject.get("patch_sha256") or "")
        if not manifest_sha256 or not patch_sha256:
            raise WorkspaceError("accepted workspace subject is not fully sealed")
        with self.workspaces.delivery_fence(snapshot):
            fresh = self.store.get_task(task.id)
            if (
                fresh.status is not TaskStatus.RUNNING
                or fresh.active_plan_id != graph.plan.id
                or fresh.version != task.version
            ):
                raise ConflictError(
                    "task changed before formal workspace publication"
                )
            # Re-check after acquiring the same source fence used by cancellation.
            # This removes both the multi-process duplicate-delivery race and the
            # status-check/delivery TOCTOU window.
            existing = self._published_subject(
                task.id, graph.plan.id, manifest_sha256
            )
            if existing is not None:
                return existing
            current = self._candidate_subject(fresh, graph)
            if (
                current.get("manifest_sha256") != manifest_sha256
                or current.get("patch_sha256") != patch_sha256
                or current.get("acceptance_contract_hash")
                != subject.get("acceptance_contract_hash")
            ):
                raise WorkspaceError("accepted candidate changed before publication")
            sealed_payload = {
                "title": "Workspace publication sealed",
                "action": "workspace_publication_sealed",
                "subject": dict(subject),
                "task_version": fresh.version,
                "plan_id": graph.plan.id,
            }
            self.store.add_evidence(
                task.id,
                kind=EvidenceKind.CHECKPOINT,
                payload=sealed_payload,
                created_by=actor,
                plan_id=graph.plan.id,
                command_id=_command(
                    "workspace-publication-sealed",
                    task.id,
                    graph.plan.id,
                    manifest_sha256,
                ),
            )
            receipt = self._deliver_with_commit_lock(
                snapshot,
                expected_candidate_manifest_sha256=manifest_sha256,
                expected_patch_sha256=patch_sha256,
                fence_held=True,
            )
            payload = {
                "title": "Accepted workspace publication",
                "action": "workspace_published",
                "subject": dict(subject),
                "receipt": receipt.to_dict(),
            }
            self.store.add_evidence(
                task.id,
                kind=EvidenceKind.CHECKPOINT,
                payload=payload,
                created_by=actor,
                plan_id=graph.plan.id,
                command_id=_command(
                    "workspace-published", task.id, graph.plan.id, manifest_sha256
                ),
            )
            return payload

    def resolve_gate(
        self,
        task_id: str,
        gate_id: str,
        *,
        decision: str,
        response: str = "",
        resolved_by: str = "local-user",
        expected_version: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> GateRecord:
        gate = self.store.get_gate(gate_id)
        if gate.task_id != task_id:
            raise NotFoundError(f"gate {gate_id} does not belong to task {task_id}")
        action = str(decision).strip().lower()
        action_specs = gate.prompt.get("actions")
        if action_specs is None:
            action_specs = {
                GateKind.PERMISSION: ("approve", "once", "reject", "cancel"),
                GateKind.QUESTION: ("answer", "submit", "cancel"),
                GateKind.PLAN: ("approve", "request_changes", "reject", "cancel"),
            }.get(gate.kind, ("approve", "reject", "cancel"))
        allowed: dict[str, bool] = {}
        for item in action_specs:
            if isinstance(item, Mapping):
                name = str(item.get("id") or item.get("action") or "").strip().lower()
                requires_response = bool(item.get("requires_response", False))
            else:
                name = str(item).strip().lower()
                requires_response = name in {
                    "accept_current",
                    "answer",
                    "submit",
                    "request_changes",
                }
            if name:
                allowed[name] = requires_response
        # A formal rejection is an auditable acceptance decision, not a generic
        # dismiss button. Enforce its rationale even for gates created by older
        # versions whose action list represented ``reject`` as a bare string.
        if gate.kind is GateKind.FINAL_ACCEPTANCE and "reject" in allowed:
            allowed["reject"] = True
        if action not in allowed:
            raise ValueError(
                f"decision {action!r} is not allowed for {gate.kind.value}; "
                f"expected one of: {', '.join(sorted(allowed))}"
            )
        if allowed[action] and not str(response).strip():
            raise ValueError(f"decision {action!r} requires a non-empty response")
        approving = action in {
            "approve",
            "approved",
            "accept",
            "answer",
            "submit",
            "retry",
            "request_changes",
            "continue",
            "skip_dependents",
            "once",
            "override_accept",
            "accept_current",
        }
        status = GateStatus.APPROVED if approving else (
            GateStatus.CANCELED if action == "cancel" else GateStatus.REJECTED
        )
        if gate.run_id is not None and approving and not self._valid_run_checkpoint(gate):
            raise ConflictError(
                "the suspended Agent checkpoint is not durable or no longer matches this gate"
            )
        supplied_keys = [
            str(value).strip()
            for value in (idempotency_key, command_id)
            if value is not None
        ]
        if any(not value for value in supplied_keys):
            raise ValueError("idempotency_key/command_id cannot be empty")
        if len(set(supplied_keys)) > 1:
            raise ValueError("idempotency_key and command_id must match when both are supplied")
        client_key = supplied_keys[0] if supplied_keys else None
        resolve_command_id = (
            # Client command ids are global within gate resolution. Reusing one
            # against another gate changes the durable scope and is a conflict.
            _command("gate-resolve-client", client_key)
            if client_key is not None
            else f"gate-resolve-{uuid.uuid4().hex}"
        )
        resolved = self.store.resolve_gate(
            gate.id,
            status,
            {"decision": action, "response": response},
            resolved_by=resolved_by,
            # Keep None as part of the durable command request. The store resolves
            # it inside the first CAS transaction, after checking for a replay.
            expected_version=expected_version,
            command_id=resolve_command_id,
        )
        if gate.kind is GateKind.FINAL_ACCEPTANCE and action == "reject":
            self._runtime_for_task(task_id, rebuild=True)
        self.wake()
        return resolved

    # -- deterministic lifecycle ----------------------------------------
    def _advance_task(self, task_id: str) -> TaskRecord:
        for _ in range(16):
            task = self.store.get_task(task_id)
            if task.status is TaskStatus.QUEUED:
                task = self._transition_status(task, TaskStatus.RUNNING, "dispatch")
                continue
            if task.status is not TaskStatus.RUNNING:
                return task
            stage = task.current_stage
            if stage is OrchestrationStage.INTAKE:
                self._transition_stage(task, OrchestrationStage.COMPLEXITY_ASSESSMENT, "intake-complete")
                continue
            if stage is OrchestrationStage.COMPLEXITY_ASSESSMENT:
                if bool(task.policy.get("clarification_required")):
                    self._transition_stage(task, OrchestrationStage.CLARIFICATION, "clarification-required")
                else:
                    self._transition_stage(
                        task,
                        OrchestrationStage.PLANNING,
                        "clarification-skipped",
                        disposition=StageDisposition.SKIPPED,
                    )
                continue
            if stage is OrchestrationStage.CLARIFICATION:
                gate = self._gate(task, GateKind.CLARIFICATION, f"{task.id}:clarification")
                if gate is None:
                    self._open_lifecycle_gate(
                        task,
                        GateKind.CLARIFICATION,
                        f"{task.id}:clarification",
                        {
                            "title": "Clarify acceptance criteria",
                            "description": "The task is ambiguous or has no explicit acceptance criteria.",
                            "question": "What outcome should be treated as accepted?",
                            "actions": ["submit", "cancel"],
                        },
                    )
                    return self.store.get_task(task.id)
                if gate.status is GateStatus.APPROVED:
                    response = str((gate.resolution or {}).get("response") or "").strip()
                    if not response:
                        raise ConflictError("approved clarification gate has no response")
                    applied = any(
                        str(item.get("gate_id") or "") == gate.id
                        for item in (task.input.get("clarifications") or ())
                        if isinstance(item, Mapping)
                    )
                    if not applied:
                        task = self.store.apply_clarification(
                            task.id,
                            response,
                            expected_version=task.version,
                            resolved_by=str(gate.resolved_by or "local-user"),
                            gate_id=gate.id,
                            command_id=_command("apply-clarification", gate.id, gate.version),
                        )
                    self._transition_stage(task, OrchestrationStage.PLANNING, "clarification-resolved")
                    continue
                return task
            if stage is OrchestrationStage.PLANNING:
                graph = self._ensure_plan(task)
                task = self.store.get_task(task.id)
                if bool(task.policy.get("plan_approval_required")):
                    source = f"{task.id}:plan:{graph.plan.id}"
                    gate = self._gate(task, GateKind.PLAN_APPROVAL, source)
                    if gate is None:
                        self._open_lifecycle_gate(
                            task,
                            GateKind.PLAN_APPROVAL,
                            source,
                            {
                                "title": f"Approve plan revision {graph.plan.revision}",
                                "description": "Review the immutable DAG before any execution begins.",
                                "plan": self._graph_payload(graph),
                                "actions": ["approve", "request_changes", "cancel"],
                            },
                        )
                        return self.store.get_task(task.id)
                    if gate.status is not GateStatus.APPROVED:
                        return task
                    if str((gate.resolution or {}).get("decision")) == "request_changes":
                        self._revise_plan(task, graph, gate)
                        continue
                self._transition_stage(task, OrchestrationStage.EXECUTION_REVIEW_TEST, "plan-approved")
                continue
            if stage is OrchestrationStage.EXECUTION_REVIEW_TEST:
                state = self._schedule_graph(task)
                if state == "complete":
                    self._transition_stage(task, OrchestrationStage.INTER_STEP_EVALUATION, "dag-complete")
                    continue
                if state == "replan":
                    self._transition_stage(
                        task,
                        OrchestrationStage.PLANNING,
                        "recovery-requested-replan",
                        disposition=StageDisposition.REQUEST_CHANGES,
                    )
                    continue
                return self.store.get_task(task.id)
            if stage is OrchestrationStage.INTER_STEP_EVALUATION:
                if self._is_task_quality_v2(task.id):
                    repair_state = self._prepare_quality_v2_repair(task)
                    if repair_state == "started":
                        continue
                    eligible, _projection, reasons = (
                        self._quality_v2_completion_eligibility(task.id)
                    )
                    if not eligible:
                        if repair_state == "failed":
                            reasons = ("repair_plan_failed", *reasons)
                        return self._hold_quality_v2_completion(task, reasons)
                    self._transition_stage(
                        task,
                        OrchestrationStage.FINAL_ACCEPTANCE,
                        "task-quality-v2-publish-authorized",
                    )
                    continue
                graph = self.store.get_plan(task.active_plan_id or "")
                runs = [
                    run
                    for run in self.store.list_runs(task.id)
                    if run.plan_id == graph.plan.id
                ]
                latest_runs = tuple(self._latest_runs(runs).values())
                latest_by_key = self._latest_runs(runs)
                failed = [run for run in latest_runs if run.status in _FAILED_RUNS]
                workspace_failures = [
                    run
                    for run in latest_runs
                    if self._workspace_commit_status(run) == "failed"
                ]
                descendants = self._plan_descendants(task.id, graph.plan.id)
                failed_children = [
                    child
                    for child in descendants
                    if self._terminal_outcome(child)
                    in {TaskStatus.FAILED, TaskStatus.CANCELED}
                ]
                execution_failed = bool(
                    failed or workspace_failures or failed_children
                )
                if execution_failed:
                    # Execution-level failure is already sufficient to require a
                    # reconciliation gate. Avoid traversing or hashing a potentially
                    # enormous candidate snapshot that cannot change that decision.
                    verification = []
                else:
                    subject = self._candidate_subject(task, graph, latest_by_key)
                    verification = self._verification_reports(
                        task.id,
                        graph,
                        latest_by_key,
                        expected_subject=subject,
                    )
                adjudication = self._verification_adjudication(verification)
                authoritative_verification = adjudication["authoritative"]
                adverse_verdicts = [
                    report
                    for report in authoritative_verification
                    if report["status"] != "pass"
                ]
                superseded_policy_skips = self._superseded_policy_skips(latest_by_key)
                superseded_run_ids = {
                    skipped.id for skipped, _source in superseded_policy_skips.values()
                }
                if (
                    superseded_policy_skips
                    and not failed
                    and not workspace_failures
                    and not failed_children
                    and all(
                        str(report.get("run_id") or "") in superseded_run_ids
                        for report in adverse_verdicts
                    )
                ):
                    # A successful explicit retry invalidates downstream fail-fast /
                    # skip-dependents markers.  Loop back before evaluation opens a
                    # misleading human gate; the scheduler will reopen only the
                    # unstarted attempts whose dependency conditions are now ready.
                    self._transition_stage(
                        task,
                        OrchestrationStage.EXECUTION_REVIEW_TEST,
                        "successful-retry-reopens-policy-skips",
                    )
                    continue
                compatibility_retry = None
                if (
                    not failed
                    and not workspace_failures
                    and not failed_children
                    and adverse_verdicts
                ):
                    compatibility_retry = (
                        self._continue_compatibility_verification_retry(
                            task,
                            graph,
                            latest_by_key,
                            verification,
                        )
                    )
                    if compatibility_retry is True:
                        self._transition_stage(
                            task,
                            OrchestrationStage.EXECUTION_REVIEW_TEST,
                            "compatibility-verification-retry",
                        )
                        continue
                retryable_failed = self._retryable_failed_runs(graph, failed)
                retryable_verification = self._retryable_adverse_verification_runs(
                    graph,
                    latest_by_key,
                    adverse_verdicts,
                    excluded_run_ids=frozenset(run.id for run in failed),
                )
                self.store.add_evidence(
                    task.id,
                    kind=EvidenceKind.REVIEW,
                    payload={
                        "title": "Inter-step evaluation",
                        "plan_id": graph.plan.id,
                        "failed_runs": [run.id for run in failed],
                        "failed_children": [child.id for child in failed_children],
                        "verification": verification,
                        "adjudication": {
                            "authority": adjudication["authority"],
                            "adjudicated": adjudication["adjudicated"],
                            "adjudicator_run_ids": [
                                report.get("run_id")
                                for report in adjudication["adjudicators"]
                            ],
                            "dissenting_run_ids": [
                                report.get("run_id")
                                for report in adjudication["dissent"]
                            ],
                        },
                        "verdict": (
                            "reconcile"
                            if failed
                            or workspace_failures
                            or failed_children
                            or adverse_verdicts
                            else "proceed"
                        ),
                    },
                    created_by="orchestration-evaluator",
                    plan_id=graph.plan.id,
                    command_id=_command(
                        "evaluation-adjudication-v2", graph.plan.id, len(runs)
                    ),
                )
                if failed or workspace_failures or failed_children or adverse_verdicts:
                    failure_signature = hashlib.sha256(
                        json.dumps(
                            {
                                "runs": sorted(
                                    (run.id, run.attempt, run.status.value)
                                    for run in failed
                                ),
                                "workspace_commits": sorted(
                                    (run.id, self._workspace_commit_status(run))
                                    for run in workspace_failures
                                ),
                                "children": sorted(
                                    (child.id, child.status.value)
                                    for child in failed_children
                                ),
                                "verification": sorted(
                                    (report["node_id"], report["run_id"], report["status"])
                                    for report in adverse_verdicts
                                ),
                            },
                            separators=(",", ":"),
                        ).encode("utf-8")
                    ).hexdigest()[:16]
                    source = (
                        f"{task.id}:reconciliation:{graph.plan.id}:{failure_signature}"
                    )
                    gate = self._gate(task, GateKind.RECONCILIATION, source)
                    if gate is None:
                        self._open_lifecycle_gate(
                            task,
                            GateKind.RECONCILIATION,
                            source,
                            {
                                "title": (
                                    "Execution needs reconciliation"
                                    if failed or workspace_failures or failed_children
                                    else "Verification needs reconciliation"
                                ),
                                "description": (
                                    "One or more required executions did not succeed."
                                    if failed or workspace_failures or failed_children
                                    else (
                                        "Execution and its completed work products are "
                                        "preserved, but the verification roles did not "
                                        "reach a decisive result. Accept the current "
                                        "evidence, re-run only the disputed checks, or "
                                        "request a deliverable revision."
                                    )
                                ),
                                "failed_runs": [self._run_payload(run) for run in failed],
                                "workspace_commit_failures": [
                                    self._run_payload(run) for run in workspace_failures
                                ],
                                "failed_children": [
                                    self._task_summary(child) for child in failed_children
                                ],
                                "verification": verification,
                                "verification_signature": (
                                    self._verification_signature(verification)
                                ),
                                "completed_work_products": len(
                                    self.store.list_work_products(
                                        task.id, limit=10_000
                                    )
                                ),
                                "actions": (
                                    [
                                        {
                                            "id": "accept_current",
                                            "label": "Accept current results",
                                            "tone": "primary",
                                            "requires_response": True,
                                        },
                                        {
                                            "id": "retry",
                                            "label": "Re-run disputed checks",
                                            "tone": "neutral",
                                        },
                                        {
                                            "id": "request_changes",
                                            "label": "Revise deliverable",
                                            "tone": "neutral",
                                            "requires_response": True,
                                        },
                                        "cancel",
                                    ]
                                    if not (
                                        failed
                                        or workspace_failures
                                        or failed_children
                                    )
                                    and (
                                        retryable_failed or retryable_verification
                                    )
                                    else [
                                        {
                                            "id": "accept_current",
                                            "label": "Accept current results",
                                            "tone": "primary",
                                            "requires_response": True,
                                        },
                                        {
                                            "id": "request_changes",
                                            "label": "Revise deliverable",
                                            "tone": "neutral",
                                            "requires_response": True,
                                        },
                                        "cancel",
                                    ]
                                    if not (
                                        failed
                                        or workspace_failures
                                        or failed_children
                                    )
                                    else (
                                        ["retry", "request_changes", "cancel"]
                                        if (
                                            retryable_failed
                                            or retryable_verification
                                        )
                                        and not workspace_failures
                                        else ["request_changes", "cancel"]
                                    )
                                ),
                            },
                        )
                        return self.store.get_task(task.id)
                    if gate.status is not GateStatus.APPROVED:
                        return task
                    decision = str((gate.resolution or {}).get("decision"))
                    if decision == "accept_current":
                        self._transition_stage(
                            task,
                            OrchestrationStage.FINAL_ACCEPTANCE,
                            "current-verification-accepted",
                        )
                    elif decision == "retry":
                        scheduled = self._retry_failed(task, graph, explicit=True)
                        nodes_by_id = {node.id: node for node in graph.nodes}
                        compatibility_gate = bool(
                            gate.prompt.get("compatibility_retry")
                        )
                        for adverse_run in (
                            () if compatibility_gate else retryable_verification
                        ):
                            adverse_node = nodes_by_id.get(adverse_run.node_id)
                            if adverse_node is not None:
                                scheduled = (
                                    self._retry_run(
                                        task, graph, adverse_node, adverse_run
                                    )
                                    or scheduled
                                )
                        if scheduled:
                            self._transition_stage(
                                task,
                                OrchestrationStage.EXECUTION_REVIEW_TEST,
                                "retry-approved",
                            )
                        else:
                            self._record_retry_exhausted_replan(
                                task,
                                graph,
                                gate,
                                tuple(failed)
                                + tuple(retryable_verification)
                                + tuple(
                                    latest_by_key[key]
                                    for key in self._compatibility_retry_base_attempts(
                                        gate
                                    )
                                    if key in latest_by_key
                                ),
                            )
                            self._transition_stage(
                                task,
                                OrchestrationStage.PLANNING,
                                "retry-exhausted-replan-required",
                                disposition=StageDisposition.REQUEST_CHANGES,
                            )
                    else:
                        self._transition_stage(
                            task,
                            OrchestrationStage.PLANNING,
                            "replan-requested",
                            disposition=StageDisposition.REQUEST_CHANGES,
                        )
                    continue
                self._transition_stage(task, OrchestrationStage.FINAL_ACCEPTANCE, "evaluation-passed")
                continue
            if stage is OrchestrationStage.FINAL_ACCEPTANCE:
                if self._is_task_quality_v2(task.id):
                    eligible, _projection, reasons = (
                        self._quality_v2_completion_eligibility(task.id)
                    )
                    if not eligible:
                        return self._hold_quality_v2_completion(task, reasons)
                    self._transition_stage(
                        task, OrchestrationStage.ARCHIVE, "task-quality-v2-published"
                    )
                    continue
                graph = self.store.get_plan(task.active_plan_id or "")
                history_runs = [
                    run
                    for run in self.store.list_runs(task.id)
                    if run.plan_id == graph.plan.id
                ]
                latest_by_key = self._latest_runs(history_runs)
                runs = list(latest_by_key.values())
                descendants = self._plan_descendants(task.id, graph.plan.id)
                subject = self._candidate_subject(task, graph, latest_by_key)
                verification = self._verification_reports(
                    task.id,
                    graph,
                    latest_by_key,
                    expected_subject=subject,
                )
                adjudication = self._verification_adjudication(verification)
                authoritative_verification = adjudication["authoritative"]
                accepted_current_gate = self._accepted_current_verification_gate(
                    task.id, graph, verification
                )
                verification_passed = bool(authoritative_verification) and all(
                    report["status"] == "pass"
                    for report in authoritative_verification
                )
                verification_passed = bool(
                    verification_passed or accepted_current_gate is not None
                )
                execution_passed = (
                    len(runs) == len(graph.nodes)
                    and all(
                        run.status in {RunStatus.SUCCEEDED, RunStatus.SKIPPED}
                        for run in runs
                    )
                    and all(self._child_succeeded(child) for child in descendants)
                    and verification_passed
                )
                criteria: dict[str, str] = {}
                for criterion in task.acceptance_criteria:
                    statuses = [
                        report["criteria"].get(criterion, "unknown")
                        for report in authoritative_verification
                    ]
                    criteria[criterion] = (
                        "fail"
                        if "fail" in statuses
                        else "pass"
                        if statuses and all(status == "pass" for status in statuses)
                        else "unknown"
                    )
                verdict = evaluate_acceptance(
                    risk=PolicyRiskTier(task.risk_tier.value),
                    criteria=criteria,
                    unresolved_gates=len(self.store.list_gates(task.id, statuses=(GateStatus.OPEN,))),
                    required_nodes_complete=(
                        execution_passed
                    ),
                )
                requires_gate = (
                    verdict.requires_human or not verdict.accepted
                ) and accepted_current_gate is None
                accepting_actor = (
                    accepted_current_gate.resolved_by
                    if accepted_current_gate is not None
                    else "orchestration-policy"
                ) or "local-user"
                if requires_gate:
                    source = (
                        f"{task.id}:final:{graph.plan.id}:"
                        f"{subject['manifest_sha256']}:"
                        f"{subject['acceptance_contract_hash']}"
                    )
                    gate = self._gate(task, GateKind.FINAL_ACCEPTANCE, source)
                    if gate is None:
                        actions: list[Any]
                        reject_action = {
                            "id": "reject",
                            "tone": "danger",
                            "requires_response": True,
                        }
                        if verdict.accepted:
                            actions = ["accept", "request_changes", reject_action]
                        else:
                            actions = [
                                "request_changes",
                                {
                                    "id": "override_accept",
                                    "requires_response": True,
                                },
                                reject_action,
                            ]
                        self._open_lifecycle_gate(
                            task,
                            GateKind.FINAL_ACCEPTANCE,
                            source,
                            {
                                "title": "Final acceptance",
                                "description": "Formally accept the evidence-backed result or request another revision.",
                                "criteria": criteria,
                                "verification": verification,
                                "subject": subject,
                                "policy_reasons": list(verdict.reasons),
                                "actions": actions,
                            },
                        )
                        return self.store.get_task(task.id)
                    if gate.status is not GateStatus.APPROVED:
                        return task
                    if str((gate.resolution or {}).get("decision")) == "request_changes":
                        self._transition_stage(
                            task,
                            OrchestrationStage.PLANNING,
                            "acceptance-changes-requested",
                            disposition=StageDisposition.REQUEST_CHANGES,
                        )
                        continue
                    accepting_actor = gate.resolved_by or "local-user"
                try:
                    publication = self._publish_task_candidate(
                        task,
                        graph,
                        subject,
                        actor=accepting_actor,
                    )
                except WorkspaceConflictError as exc:
                    preflight = exc.preflight
                    conflict_source = (
                        f"{task.id}:publish-conflict:{graph.plan.id}:"
                        f"{subject['manifest_sha256']}:{preflight.source_manifest_digest}"
                    )
                    conflict_gate = self._gate(
                        task, GateKind.WORKSPACE_CONFLICT, conflict_source
                    )
                    if conflict_gate is None:
                        self._open_lifecycle_gate(
                            task,
                            GateKind.WORKSPACE_CONFLICT,
                            conflict_source,
                            {
                                "title": "Accepted candidate conflicts with current workspace",
                                "description": (
                                    "The formal workspace changed after the candidate was "
                                    "sealed. Replan/rebase before publication."
                                ),
                                "subject": subject,
                                "preflight": preflight.to_dict(),
                                "actions": ["request_changes", "cancel"],
                            },
                        )
                        return self.store.get_task(task.id)
                    if conflict_gate.status is not GateStatus.APPROVED:
                        return task
                    self._transition_stage(
                        task,
                        OrchestrationStage.PLANNING,
                        "publication-conflict-replan",
                        disposition=StageDisposition.REQUEST_CHANGES,
                    )
                    continue
                self.store.add_evidence(
                    task.id,
                    kind=EvidenceKind.DECISION,
                    payload={
                        "title": "Final acceptance",
                        "accepted": True,
                        "criteria": criteria,
                        "verification": verification,
                        "adjudication": {
                            "authority": adjudication["authority"],
                            "adjudicated": adjudication["adjudicated"],
                            "adjudicator_run_ids": [
                                report.get("run_id")
                                for report in adjudication["adjudicators"]
                            ],
                            "dissenting_run_ids": [
                                report.get("run_id")
                                for report in adjudication["dissent"]
                            ],
                        },
                        "subject": subject,
                        "publication": publication,
                        "override": (
                            accepted_current_gate is not None
                            or (
                                not verdict.accepted
                                and requires_gate
                                and str((gate.resolution or {}).get("decision"))
                                == "override_accept"
                            )
                        ),
                        "verification_override_gate_id": (
                            accepted_current_gate.id
                            if accepted_current_gate is not None
                            else None
                        ),
                    },
                    created_by=accepting_actor,
                    plan_id=graph.plan.id,
                    command_id=_command("accepted", graph.plan.id),
                )
                self._transition_stage(task, OrchestrationStage.ARCHIVE, "accepted")
                continue
            if stage is OrchestrationStage.ARCHIVE:
                if self._is_task_quality_v2(task.id):
                    eligible, _projection, reasons = (
                        self._quality_v2_completion_eligibility(task.id)
                    )
                    if not eligible:
                        return self._hold_quality_v2_completion(task, reasons)
                if task.status is TaskStatus.RUNNING:
                    result = self._task_result_envelope(task)
                    task = self._transition_status(
                        task,
                        TaskStatus.COMPLETED,
                        "lifecycle-complete",
                        output={
                            "plan_id": task.active_plan_id,
                            "evidence_count": len(self.store.list_evidence(task.id)),
                            "result": result,
                            "result_hash": result["result_hash"],
                        },
                    )
                # Completion and archival are intentionally distinct. The result
                # remains visible and interactive in COMPLETED until the operator
                # explicitly files it with archive_task().
                self._cleanup_task_workspaces(task.id)
                return task
        raise RuntimeError(f"task {task_id} exceeded coordinator transition rail")

    def _assessment(
        self,
        request: Mapping[str, Any],
        domain: TaskDomain,
        criteria: bool,
        *,
        workspace: Optional[str] = None,
    ):
        supplied = dict(request.get("complexity_factors") or {})
        objective = str(request.get("objective") or "")
        plan = request.get("plan") or {}
        nodes = plan.get("nodes", ()) if isinstance(plan, Mapping) else ()
        defaults = {
            "scope": min(5, max(1, len(objective) // 300 + len(request.get("acceptance_criteria", ())) // 3)),
            "uncertainty": 1 if criteria else 4,
            "dependencies": min(5, len(request.get("dependencies", ())) + (1 if request.get("constraints") else 0)),
            # Risk follows effective capability, not the caller's semantic domain label.
            "side_effects": 2 if workspace and not request.get("read_only") else 0,
            "parallelism": min(5, max(0, len(nodes) - 1)),
            "verification": (
                3
                if domain is TaskDomain.CODE
                or bool(workspace and not request.get("read_only"))
                else 1
            ),
        }
        factors = ComplexityFactors(**{name: int(supplied.get(name, value)) for name, value in defaults.items()})
        inferred_risk = classify_risk(
            workspace_writes=bool(workspace) and not bool(request.get("read_only")),
            external_writes=bool(request.get("external_writes")),
            privileged_or_secret=bool(request.get("privileged_or_secret")),
            destructive_or_irreversible=bool(request.get("destructive_or_irreversible")),
        )
        explicit_risk = request.get("risk_tier")
        risk = inferred_risk
        if explicit_risk:
            explicit = PolicyRiskTier(str(explicit_risk))
            rank = {
                PolicyRiskTier.LOW: 0,
                PolicyRiskTier.MEDIUM: 1,
                PolicyRiskTier.HIGH: 2,
                PolicyRiskTier.CRITICAL: 3,
            }
            risk = max((inferred_risk, explicit), key=rank.__getitem__)
        effective_domain = (
            TaskDomain.CODE.value
            if workspace and not request.get("read_only")
            else domain.value
        )
        return assess_complexity(
            factors,
            risk=risk,
            domain=effective_domain,
            acceptance_criteria_present=criteria,
        )

    def _transition_status(
        self,
        task: TaskRecord,
        target: TaskStatus,
        reason: str,
        *,
        output: Optional[Mapping[str, Any]] = None,
    ) -> TaskRecord:
        changed = self.store.transition_task_status(
            task.id,
            target,
            expected_version=task.version,
            output=output,
            command_id=_command("status", task.id, task.version, target.value, reason),
        )
        if target in _TERMINAL_TASKS:
            try:
                self.relations.resolve_terminal(changed.id)
            except Exception:
                # The terminal status is already durable. Recovery and the next
                # scheduler pass may safely repeat relation projection by version.
                logger.exception(
                    "could not project terminal relations for task %s", changed.id
                )
        return changed

    def _transition_stage(
        self,
        task: TaskRecord,
        target: OrchestrationStage,
        reason: str,
        *,
        disposition: StageDisposition = StageDisposition.COMPLETED,
    ) -> TaskRecord:
        return self.store.transition_stage(
            task.id,
            target,
            expected_version=task.version,
            disposition=disposition,
            detail={"reason": reason},
            command_id=_command("stage", task.id, task.version, target.value, reason),
        )

    def _open_lifecycle_gate(
        self, task: TaskRecord, kind: GateKind, source_key: str, prompt: Mapping[str, Any]
    ) -> GateRecord:
        return self.store.open_task_gate(
            task.id,
            kind=kind,
            source_key=source_key,
            prompt=prompt,
            command_id=_command("task-gate", source_key),
        )

    def _gate(self, task: TaskRecord, kind: GateKind, source_key: str) -> Optional[GateRecord]:
        return next(
            (
                gate
                for gate in self.store.list_gates(task.id)
                if gate.kind is kind and gate.source_key == source_key
            ),
            None,
        )

    # -- plans and DAG scheduling ----------------------------------------
    def _ensure_plan(self, task: TaskRecord) -> PlanGraph:
        if task.active_plan_id:
            graph = self.store.get_plan(task.active_plan_id)
            change_gate = self._latest_change_gate(task)
            if change_gate and change_gate.resolved_at and graph.plan.created_at <= change_gate.resolved_at:
                return self._revise_plan(task, graph, change_gate)
            return graph
        spec = self._plan_spec(task)
        return self.store.create_plan_revision(
            task.id,
            spec,
            expected_task_version=task.version,
            created_by="orchestration-planner",
            command_id=_command("plan", task.id, 1),
        )

    def _revise_plan(self, task: TaskRecord, graph: PlanGraph, gate: GateRecord) -> PlanGraph:
        fresh = self.store.get_task(task.id)
        if fresh.active_plan_id != graph.plan.id:
            return self.store.get_plan(fresh.active_plan_id or "")
        feedback = str((gate.resolution or {}).get("response") or "").strip()
        retry_exhausted = gate.id in self._retry_exhausted_replan_gate_ids(
            fresh.id,
            plan_id=graph.plan.id,
        )
        feedback_block = (
            f"\n\nRevision feedback (gate {gate.id}):\n{feedback}" if feedback else ""
        )
        spec = PlanSpec(
            nodes=tuple(
                NodeSpec(
                    key=node.key,
                    title=node.title,
                    instructions=node.instructions + feedback_block,
                    kind=node.kind,
                    agent=node.agent,
                    model=node.model,
                    input={**dict(node.input), "revision_feedback": feedback},
                    join_policy=node.join_policy,
                    failure_policy=node.failure_policy,
                    effect_safety=node.effect_safety,
                    retry_policy=node.retry_policy,
                    timeout_seconds=node.timeout_seconds,
                    priority=node.priority,
                    concurrency_key=node.concurrency_key,
                    metadata=node.metadata,
                )
                for node in graph.nodes
            ),
            edges=tuple(
                EdgeSpec(
                    from_node=edge.from_node,
                    to_node=edge.to_node,
                    condition=edge.condition,
                    required=edge.required,
                    metadata=edge.metadata,
                )
                for edge in graph.edges
            ),
            metadata={
                **dict(graph.plan.metadata),
                "revision_requested_by_gate": gate.id,
                "revision_reason": (
                    "retry_exhausted_replan" if retry_exhausted else "request_changes"
                ),
                "feedback": feedback,
            },
        )
        return self.store.create_plan_revision(
            fresh.id,
            spec,
            expected_task_version=fresh.version,
            created_by="orchestration-planner",
            command_id=_command("revise", graph.plan.id, gate.id),
        )

    def _latest_change_gate(self, task: TaskRecord) -> Optional[GateRecord]:
        exhausted_retry_gates = self._retry_exhausted_replan_gate_ids(task.id)
        gates = [
            gate
            for gate in self.store.list_gates(task.id)
            if gate.status is GateStatus.APPROVED
            and (
                str((gate.resolution or {}).get("decision")) == "request_changes"
                or gate.id in exhausted_retry_gates
            )
        ]
        return (
            max(
                gates,
                key=lambda gate: (
                    gate.resolved_at or gate.opened_at,
                    gate.opened_at,
                    gate.id,
                ),
            )
            if gates
            else None
        )

    def _record_retry_exhausted_replan(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        gate: GateRecord,
        failed: Sequence[RunRecord],
    ) -> None:
        """Persist the operator-approved fallback before crossing into planning.

        The evidence is the restart boundary: if the process stops after the stage
        transition but before the new plan is created, ``_ensure_plan`` finds this
        marker and deterministically creates the same immutable revision.  Older
        gates may still advertise ``retry`` even though their attempts are already
        exhausted, so the decision must never fall back to the unchanged plan.
        """

        nodes = {node.id: node for node in graph.nodes}
        failed_runs = []
        for run in sorted(failed, key=lambda item: (item.node_key, item.attempt, item.id)):
            node = nodes.get(run.node_id)
            failed_runs.append(
                {
                    "run_id": run.id,
                    "node_id": run.node_id,
                    "node_key": run.node_key,
                    "attempt": run.attempt,
                    "max_attempts": (
                        node.retry_policy.max_attempts if node is not None else None
                    ),
                }
            )
        self.store.add_evidence(
            task.id,
            kind=EvidenceKind.DECISION,
            payload={
                "title": "Retry attempts exhausted; plan revision required",
                "retry_exhausted_replan": True,
                "decision": "retry",
                "disposition": "request_changes",
                "reason": (
                    "No failed run remains eligible for another attempt in the "
                    "current immutable plan."
                ),
                "gate_id": gate.id,
                "plan_id": graph.plan.id,
                "failed_runs": failed_runs,
            },
            created_by=gate.resolved_by or "orchestration-evaluator",
            plan_id=graph.plan.id,
            command_id=_command(
                "retry-exhausted-replan",
                task.id,
                graph.plan.id,
                gate.id,
            ),
        )

    def _retry_exhausted_replan_gate_ids(
        self,
        task_id: str,
        *,
        plan_id: Optional[str] = None,
    ) -> frozenset[str]:
        return frozenset(
            str(item.payload.get("gate_id"))
            for item in self.store.list_evidence(task_id)
            if item.payload.get("retry_exhausted_replan") is True
            and item.payload.get("gate_id")
            and (
                plan_id is None
                or str(item.payload.get("plan_id") or "") == str(plan_id)
            )
        )

    def _plan_spec(self, task: TaskRecord) -> PlanSpec:
        selected_preset = self._runtime_preset_for_task(task)
        raw = task.input.get("plan")
        if isinstance(raw, Mapping) and raw.get("nodes"):
            spec = self._plan_from_payload(raw)
            if selected_preset:
                spec = self._apply_runtime_preset(spec, selected_preset)
            self._validate_plan_semantics(
                spec,
                require_review=bool(task.policy.get("require_review")),
                require_tests=bool(task.policy.get("require_tests")),
                read_only=bool(task.policy.get("read_only", False)),
            )
            return self._freeze_plan_profiles(task, spec)

        if selected_preset:
            generated = self._preset_plan_spec(
                selected_preset,
                objective=task.objective,
                acceptance_criteria=task.acceptance_criteria,
                workspace=task.workspace,
                read_only=bool(task.policy.get("read_only", False)),
            )
            self._validate_plan_budget(task.budget, generated)
            self._validate_plan_semantics(
                generated,
                require_review=bool(task.policy.get("require_review")),
                require_tests=bool(task.policy.get("require_tests")),
                read_only=bool(task.policy.get("read_only", False)),
            )
            return self._freeze_plan_profiles(task, generated)

        first_profile = str(task.policy.get("profile_id") or "worker")
        first_profile_spec = self.catalog.resolve_profile(first_profile)
        primary_kind = {
            AgentRole.WORKER: NodeKind.EXECUTE,
            AgentRole.REVIEWER: NodeKind.REVIEW,
            AgentRole.TESTER: NodeKind.TEST,
            AgentRole.INTEGRATOR: NodeKind.INTEGRATE,
            AgentRole.EVALUATOR: NodeKind.EVALUATE,
            AgentRole.SCORER: NodeKind.EVALUATE,
        }.get(first_profile_spec.role, NodeKind.AGENT)
        nodes: list[NodeSpec] = [
            NodeSpec(
                key="execute",
                title="Execute scoped work",
                instructions=task.objective,
                kind=primary_kind,
                agent=first_profile,
                effect_safety=EffectSafety.IDEMPOTENT,
                retry_policy=RetryPolicy(max_attempts=3),
                concurrency_key=f"workspace:{task.workspace}" if task.workspace else None,
            )
        ]
        edges: list[EdgeSpec] = []
        prior = "execute"
        if task.policy.get("require_review"):
            nodes.append(
                NodeSpec(
                    key="review",
                    title="Independent review",
                    instructions="Review the delivered change against the criteria and report all findings.",
                    kind=NodeKind.REVIEW,
                    agent="reviewer",
                    effect_safety=EffectSafety.READ_ONLY,
                )
            )
            edges.append(EdgeSpec(prior, "review"))
            prior = "review"
        if task.policy.get("require_tests"):
            nodes.append(
                NodeSpec(
                    key="test",
                    title="Independent verification",
                    instructions="Run the narrowest relevant test suite and report reproducible results.",
                    kind=NodeKind.TEST,
                    agent="tester",
                    effect_safety=EffectSafety.READ_ONLY,
                    retry_policy=RetryPolicy(max_attempts=2),
                )
            )
            edges.append(EdgeSpec(prior, "test"))
            prior = "test"
        nodes.append(
            NodeSpec(
                key="evaluate",
                title="Evaluate evidence",
                instructions="Evaluate upstream execution, review, and test evidence against every criterion.",
                kind=NodeKind.EVALUATE,
                agent="evaluator",
                effect_safety=EffectSafety.READ_ONLY,
            )
        )
        edges.append(EdgeSpec(prior, "evaluate"))
        generated = PlanSpec(
            nodes=tuple(nodes),
            edges=tuple(edges),
            metadata={"generated": "deterministic-v1"},
        )
        self._validate_plan_semantics(
            generated,
            require_review=bool(task.policy.get("require_review")),
            require_tests=bool(task.policy.get("require_tests")),
            read_only=bool(task.policy.get("read_only", False)),
        )
        return self._freeze_plan_profiles(task, generated)

    def _runtime_preset_for_task(self, task: TaskRecord) -> Optional[RuntimePreset]:
        preset_id = str(task.policy.get("runtime_preset_id") or "").strip()
        if not preset_id:
            return None
        raw_snapshot = task.policy.get("runtime_preset_snapshot")
        if isinstance(raw_snapshot, Mapping):
            preset = RuntimePreset.from_dict(raw_snapshot)
            expected_hash = str(
                task.policy.get("runtime_preset_hash")
                or raw_snapshot.get("content_hash")
                or ""
            )
            actual_hash = preset.to_dict()["content_hash"]
            if expected_hash and actual_hash != expected_hash:
                raise ConflictError(
                    f"frozen runtime preset hash mismatch for {preset_id}"
                )
        else:
            # Compatibility for an early task record that stored only the identity.
            preset = runtime_preset(preset_id)
        if preset.preset_id != preset_id:
            raise ConflictError("frozen runtime preset identity mismatch")
        expected_version = task.policy.get("runtime_preset_version")
        if expected_version is not None and preset.version != int(expected_version):
            raise ConflictError("frozen runtime preset version mismatch")
        if task.domain.value not in preset.domains:
            raise ConflictError(
                f"runtime preset {preset.preset_id} does not support "
                f"domain {task.domain.value}"
            )
        return preset

    def _apply_runtime_preset(
        self, spec: PlanSpec, preset: RuntimePreset
    ) -> PlanSpec:
        nodes: list[NodeSpec] = []
        for node in spec.nodes:
            role = self.catalog.resolve_profile(node.agent).role
            model = node.model or preset.model_for(role)
            nodes.append(
                NodeSpec(
                    key=node.key,
                    title=node.title,
                    instructions=node.instructions,
                    kind=node.kind,
                    agent=node.agent,
                    model=model,
                    input=node.input,
                    join_policy=node.join_policy,
                    failure_policy=node.failure_policy,
                    effect_safety=node.effect_safety,
                    retry_policy=node.retry_policy,
                    timeout_seconds=node.timeout_seconds,
                    priority=node.priority,
                    concurrency_key=node.concurrency_key,
                    metadata={
                        **dict(node.metadata),
                        "runtime_preset_binding": {
                            "preset_id": preset.preset_id,
                            "version": preset.version,
                            "role": role.value,
                            "model": model,
                            "source": "explicit_node" if node.model else "preset_role",
                            "fallback_mode": preset.fallback_mode,
                        },
                    },
                )
            )
        return PlanSpec(
            nodes=tuple(nodes),
            edges=spec.edges,
            metadata={
                **dict(spec.metadata),
                "runtime_preset": preset.to_dict(),
            },
        )

    def _preset_plan_spec(
        self,
        preset: RuntimePreset,
        *,
        objective: str,
        acceptance_criteria: Sequence[str],
        workspace: Optional[str],
        read_only: bool = False,
    ) -> PlanSpec:
        if preset.plan_template != "codex-led-code-v1":
            raise ConflictError(
                f"unsupported runtime preset plan template: {preset.plan_template}"
            )
        criteria = "\n".join(f"- {item}" for item in acceptance_criteria)
        criteria_block = criteria or "- No explicit criterion supplied; report this as a risk."
        nodes = (
            NodeSpec(
                key="understand",
                title="Understand objective and constraints",
                instructions=(
                    "Normalize the objective, constraints, approval boundaries, and "
                    "acceptance criteria into a concise, evidence-ready execution brief. "
                    "Identify ambiguity but do not modify the workspace.\n\n"
                    f"Objective:\n{objective}\n\nAcceptance criteria:\n{criteria_block}"
                ),
                kind=NodeKind.AGENT,
                agent=AgentRole.ORCHESTRATOR.value,
                effect_safety=EffectSafety.READ_ONLY,
                retry_policy=RetryPolicy(max_attempts=2),
            ),
            NodeSpec(
                key="explore",
                title="Explore repository evidence",
                instructions=(
                    "Inspect the repository read-only. Locate the relevant architecture, "
                    "dependencies, tests, conventions, and risk surfaces. Return concrete "
                    "file evidence and unresolved questions; do not propose unscoped work."
                ),
                kind=NodeKind.AGENT,
                agent=AgentRole.EXPLORER.value,
                effect_safety=EffectSafety.READ_ONLY,
                retry_policy=RetryPolicy(max_attempts=2),
            ),
            NodeSpec(
                key="plan",
                title=(
                    "Prepare evidence-synthesis delivery plan"
                    if read_only
                    else "Prepare dependency-aware implementation handoff"
                ),
                instructions=(
                    (
                        "Using only the durable objective and upstream evidence, produce "
                        "a bounded plan for the requested read-only deliverable: evidence "
                        "gaps, source dependencies, deliverable structure, validation "
                        "checks, stop conditions, and criterion-to-evidence mapping. Do "
                        "not modify files, request write access, or silently broaden scope."
                    )
                    if read_only
                    else (
                        "Using only the durable objective and upstream evidence, produce a "
                        "bounded implementation handoff: ordered changes, dependencies, "
                        "verification commands, stop conditions, and criterion-to-evidence "
                        "mapping. Do not modify files or silently broaden scope."
                    )
                ),
                kind=NodeKind.AGENT,
                agent=AgentRole.PLANNER.value,
                effect_safety=EffectSafety.READ_ONLY,
                retry_policy=RetryPolicy(max_attempts=2),
            ),
            NodeSpec(
                key="execute",
                title=(
                    "Produce evidence-backed read-only deliverable"
                    if read_only
                    else "Implement approved scoped work"
                ),
                instructions=(
                    (
                        "Produce the objective's requested evidence-backed deliverable "
                        "using the durable understanding, repository evidence, and delivery "
                        "plan. Inspect sources only: do not modify files or external systems. "
                        "Include concrete file/source evidence, criterion coverage, material "
                        "limitations, and unresolved questions; explicitly state that no "
                        "files were changed."
                    )
                    if read_only
                    else (
                        "Implement the objective in the isolated candidate workspace using "
                        "the durable understanding, repository evidence, and planning "
                        "handoff. Preserve existing behavior outside scope and report "
                        "changed files and verification evidence."
                    )
                ),
                kind=NodeKind.EXECUTE,
                agent=AgentRole.WORKER.value,
                effect_safety=(
                    EffectSafety.READ_ONLY
                    if read_only
                    else EffectSafety.IDEMPOTENT
                ),
                retry_policy=RetryPolicy(max_attempts=3),
                concurrency_key=(
                    f"workspace:{workspace}"
                    if workspace and not read_only
                    else None
                ),
            ),
            NodeSpec(
                key="review",
                title=(
                    "Independent review of final read-only deliverable"
                    if read_only
                    else "Independent cross-provider review"
                ),
                instructions=(
                    (
                        "Review the final read-only deliverable with fresh context. Challenge "
                        "its reasoning, source support, scope coverage, and treatment of "
                        "uncertainty; report every blocking finding against the acceptance "
                        "criteria with concrete file/source evidence."
                    )
                    if read_only
                    else (
                        "Review the final candidate read-only with fresh context. Challenge "
                        "the Codex understanding, plan, and implementation; report every "
                        "blocking finding against the acceptance criteria with file evidence."
                    )
                ),
                kind=NodeKind.REVIEW,
                agent=AgentRole.REVIEWER.value,
                effect_safety=EffectSafety.READ_ONLY,
            ),
            NodeSpec(
                key="test",
                title=(
                    "Independent read-only evidence verification"
                    if read_only
                    else "Independent verification and adversarial testing"
                ),
                instructions=(
                    (
                        "Verify cited files and sources, criterion coverage, traceability, "
                        "internal consistency, and unsupported claims using read-only "
                        "inspection tools. Report evidence gaps without modifying files or "
                        "depending on shell commands that create caches or other outputs."
                    )
                    if read_only
                    else (
                        "Select and run the narrowest sufficient deterministic checks in the "
                        "isolated test snapshot. Include regression, edge-case, and "
                        "data-contract checks where relevant; distinguish code failures from "
                        "environment gaps."
                    )
                ),
                kind=NodeKind.TEST,
                agent=AgentRole.TESTER.value,
                effect_safety=EffectSafety.READ_ONLY,
                retry_policy=RetryPolicy(max_attempts=2),
            ),
            NodeSpec(
                key="evaluate",
                title="Evaluate all independent evidence",
                instructions=(
                    "Wait for both Reviewer and Tester. Evaluate the final work and every "
                    "independent verdict against each acceptance criterion. Never infer "
                    "approval from missing, malformed, or unknown evidence."
                ),
                kind=NodeKind.EVALUATE,
                agent=AgentRole.EVALUATOR.value,
                effect_safety=EffectSafety.READ_ONLY,
            ),
        )
        edges = (
            EdgeSpec("understand", "explore"),
            EdgeSpec("explore", "plan"),
            EdgeSpec("plan", "execute"),
            EdgeSpec("execute", "review"),
            EdgeSpec("execute", "test"),
            EdgeSpec("review", "evaluate"),
            EdgeSpec("test", "evaluate"),
        )
        return self._apply_runtime_preset(
            PlanSpec(
                nodes=nodes,
                edges=edges,
                metadata={
                    "generated": "runtime-preset-v1",
                    "plan_template": preset.plan_template,
                    "control_plane": (
                        "complexity, acceptance, and archive remain deterministic"
                    ),
                },
            ),
            preset,
        )

    def _validate_plan_semantics(
        self,
        spec: PlanSpec,
        *,
        require_review: bool = False,
        require_tests: bool = False,
        read_only: bool = False,
    ) -> None:
        profiles: dict[str, AgentProfile] = {}
        roles: dict[str, AgentRole] = {}
        for node in spec.nodes:
            profile = self.catalog.resolve_profile(node.agent)
            profiles[node.key] = profile
            role = profile.role
            roles[node.key] = role
            required_roles = {
                NodeKind.EXECUTE: {AgentRole.WORKER},
                NodeKind.REVIEW: {AgentRole.REVIEWER},
                NodeKind.TEST: {AgentRole.TESTER},
                NodeKind.INTEGRATE: {AgentRole.INTEGRATOR},
                NodeKind.EVALUATE: {AgentRole.EVALUATOR, AgentRole.SCORER},
            }.get(node.kind)
            if required_roles and role not in required_roles:
                expected = ", ".join(sorted(item.value for item in required_roles))
                raise ValueError(
                    f"node {node.key} kind {node.kind.value} requires profile role {expected}"
                )
            formal_kind_for_role = {
                AgentRole.REVIEWER: {NodeKind.REVIEW},
                AgentRole.TESTER: {NodeKind.TEST},
                AgentRole.INTEGRATOR: {NodeKind.INTEGRATE},
                AgentRole.EVALUATOR: {NodeKind.EVALUATE},
                AgentRole.SCORER: {NodeKind.EVALUATE},
            }.get(role)
            if formal_kind_for_role and node.kind not in formal_kind_for_role:
                expected = ", ".join(
                    sorted(item.value for item in formal_kind_for_role)
                )
                raise ValueError(
                    f"profile role {role.value} must use node kind {expected}, "
                    f"not {node.kind.value}"
                )
        if require_review and not any(
            node.kind is NodeKind.REVIEW and roles[node.key] is AgentRole.REVIEWER
            for node in spec.nodes
        ):
            raise ValueError("risk policy requires an isolated reviewer node")
        if require_tests and not any(
            node.kind is NodeKind.TEST and roles[node.key] is AgentRole.TESTER
            for node in spec.nodes
        ):
            raise ValueError("risk policy requires an isolated tester node")

        formal_verifiers = {
            node.key
            for node in spec.nodes
            if node.kind in {NodeKind.REVIEW, NodeKind.TEST, NodeKind.EVALUATE}
        }
        # A verifier-less custom graph may still be useful for manual workflows and
        # low-level scheduling tests, but it can never auto-accept: final acceptance
        # leaves every criterion unknown and requires an explicit, reasoned override.

        safe_parents: dict[str, set[str]] = {node.key: set() for node in spec.nodes}
        for edge in spec.edges:
            if edge.required and edge.condition is EdgeCondition.SUCCESS:
                safe_parents[edge.to_node].add(edge.from_node)

        def ancestors(key: str) -> set[str]:
            result: set[str] = set()
            pending = list(safe_parents[key])
            while pending:
                parent = pending.pop()
                if parent in result:
                    continue
                result.add(parent)
                pending.extend(safe_parents[parent])
            return result

        # Derive mutation capability from the frozen server-side profile, never from
        # caller-controlled node kind/effect_safety.  Every formal verifier must run
        # after the final mutation; therefore an Integrator that changes the candidate
        # necessarily precedes Review/Test and cannot smuggle in a post-review patch.
        producers = {
            node.key
            for node in spec.nodes
            if not read_only and self._profile_mutates_candidate(profiles[node.key])
        }
        verification = {
            node.key
            for node in spec.nodes
            if node.kind in {NodeKind.REVIEW, NodeKind.TEST}
        }
        for node in spec.nodes:
            if node.key in formal_verifiers:
                missing = producers - ancestors(node.key)
                if missing:
                    raise ValueError(
                        f"verification node {node.key} must be success-downstream of "
                        f"all candidate-producing work nodes: {', '.join(sorted(missing))}"
                    )
            if node.kind is NodeKind.EVALUATE:
                missing = (producers | verification) - ancestors(node.key)
                if missing:
                    raise ValueError(
                        f"evaluation node {node.key} must be success-downstream of "
                        f"all producer and verification nodes: "
                        f"{', '.join(sorted(missing))}"
                    )
            if node.kind in {NodeKind.REVIEW, NodeKind.TEST, NodeKind.EVALUATE}:
                if node.join_policy is not JoinPolicy.ALL:
                    raise ValueError(
                        f"formal verification node {node.key} must use join_policy=all"
                    )

    def _freeze_plan_profiles(self, task: TaskRecord, spec: PlanSpec) -> PlanSpec:
        policy_id = str(task.policy.get("model_policy_id") or "quality-first")
        model_policy = self.catalog.resolve_policy(policy_id)
        preset = self._runtime_preset_for_task(task)
        if (
            preset
            and preset.fallback_mode == "strict"
            and model_policy.fallback_for_explicit
        ):
            raise ConflictError(
                f"runtime preset {preset.preset_id} cannot freeze model policy "
                f"{model_policy.policy_id}@{model_policy.version} with "
                "fallback_for_explicit=true"
            )
        policy_spec = model_policy.audit_dict()
        policy_hash = _canonical_hash(policy_spec)
        frozen: list[NodeSpec] = []
        for node in spec.nodes:
            profile = self.catalog.resolve_profile(node.agent)
            frozen.append(
                NodeSpec(
                    key=node.key,
                    title=node.title,
                    instructions=node.instructions,
                    kind=node.kind,
                    agent=node.agent,
                    model=node.model,
                    input=node.input,
                    join_policy=node.join_policy,
                    failure_policy=node.failure_policy,
                    effect_safety=node.effect_safety,
                    retry_policy=node.retry_policy,
                    timeout_seconds=node.timeout_seconds,
                    priority=node.priority,
                    concurrency_key=(
                        (node.concurrency_key or f"candidate:{task.id}")
                        if task.workspace
                        and self._profile_mutates_candidate(profile)
                        and not bool(task.policy.get("read_only", False))
                        else node.concurrency_key
                    ),
                    metadata={
                        **dict(node.metadata),
                        "profile_snapshot": {
                            "profile_id": profile.profile_id,
                            "version": profile.version,
                            "content_hash": profile.content_hash,
                            "spec": profile.to_dict(),
                        },
                        "model_policy_snapshot": {
                            "policy_id": model_policy.policy_id,
                            "version": model_policy.version,
                            "content_hash": policy_hash,
                            "spec": policy_spec,
                        },
                    },
                )
            )
        return PlanSpec(nodes=tuple(frozen), edges=spec.edges, metadata=spec.metadata)

    @staticmethod
    def _plan_from_payload(raw: Mapping[str, Any]) -> PlanSpec:
        nodes = []
        for item in raw.get("nodes", ()):
            retry = item.get("retry_policy") or {}
            nodes.append(
                NodeSpec(
                    key=str(item["key"]),
                    title=str(item.get("title", "")),
                    instructions=str(item.get("instructions", "")),
                    kind=NodeKind(str(item.get("kind", "execute"))),
                    agent=str(item.get("agent", "worker")),
                    model=item.get("model"),
                    input=dict(item.get("input") or {}),
                    join_policy=JoinPolicy(str(item.get("join_policy", "all"))),
                    failure_policy=FailurePolicy(str(item.get("failure_policy", "fail_fast"))),
                    effect_safety=EffectSafety(str(item.get("effect_safety", "read_only"))),
                    retry_policy=RetryPolicy(**retry),
                    timeout_seconds=int(item.get("timeout_seconds", 900)),
                    priority=int(item.get("priority", 0)),
                    concurrency_key=item.get("concurrency_key"),
                    metadata=dict(item.get("metadata") or {}),
                )
            )
        edges = tuple(
            EdgeSpec(
                from_node=str(item.get("from_node") or item.get("from")),
                to_node=str(item.get("to_node") or item.get("to")),
                condition=EdgeCondition(str(item.get("condition", "success"))),
                required=bool(item.get("required", True)),
                metadata=dict(item.get("metadata") or {}),
            )
            for item in raw.get("edges", ())
        )
        return PlanSpec(nodes=tuple(nodes), edges=edges, metadata=dict(raw.get("metadata") or {}))

    def _schedule_graph(self, task: TaskRecord) -> str:
        graph = self.store.get_plan(task.active_plan_id or "")
        descendants = self._plan_descendants(task.id, graph.plan.id)
        if any(child.status not in _TERMINAL_TASKS for child in descendants):
            fresh = self.store.get_task(task.id)
            if fresh.status is TaskStatus.RUNNING:
                self._transition_status(fresh, TaskStatus.WAITING_CHILD, "descendants-active")
            return "active"
        runs = [run for run in self.store.list_runs(task.id) if run.plan_id == graph.plan.id]
        recorded_latest = self._latest_runs(runs)
        superseded_policy_skips = self._superseded_policy_skips(recorded_latest)
        # A failure-policy skip represents work that never started.  Once the
        # failure which caused it has a newer successful attempt, remove the skip
        # from the scheduler's effective view so dependency readiness can be
        # derived again.  The immutable skip event remains in history; the same
        # unstarted attempt is reopened only when its dependencies are ready.
        latest = {
            key: run
            for key, run in recorded_latest.items()
            if key not in superseded_policy_skips
        }

        def recovery_gate_plan_id(gate: GateRecord) -> str:
            frozen = str(gate.prompt.get("plan_id") or "")
            if frozen:
                return frozen
            run_id = str(gate.prompt.get("run_id") or "")
            if not run_id:
                return ""
            try:
                return self.store.get_run(run_id).plan_id
            except NotFoundError:
                return ""

        if any(
            gate.kind is GateKind.RECOVERY
            and gate.status is GateStatus.APPROVED
            and str((gate.resolution or {}).get("decision")) == "request_changes"
            and recovery_gate_plan_id(gate) == graph.plan.id
            for gate in self.store.list_gates(task.id)
        ):
            return "replan"
        if any(self._workspace_commit_status(run) == "pending" for run in latest.values()):
            return "active"
        failed_commits = [
            run for run in latest.values() if self._workspace_commit_status(run) == "failed"
        ]
        if failed_commits:
            changed = False
            source_run = failed_commits[0]
            for node in graph.nodes:
                current = latest.get(node.key)
                if current is not None and current.status is not RunStatus.QUEUED:
                    continue
                self.store.skip_pending_node(
                    task.id,
                    node.key,
                    plan_id=graph.plan.id,
                    reason=f"task candidate commit failed after run {source_run.id}",
                    command_id=_command(
                        "candidate-commit-skip", graph.plan.id, source_run.id, node.key
                    ),
                )
                changed = True
            if changed:
                return "active"
            refreshed = self._latest_runs(
                run
                for run in self.store.list_runs(task.id)
                if run.plan_id == graph.plan.id
            )
            if any(run.status in _ACTIVE_RUNS for run in refreshed.values()):
                return "active"
            return "complete"

        # MANUAL is a scheduling barrier, not merely "no automatic retry".  Its
        # durable decision is handled before any other failure can enqueue work.
        manual_wait, manual_skips = self._manual_failure_control(
            task, graph, latest
        )
        if manual_wait:
            return "active"

        # A policy only takes terminal control after its own automatic retry is
        # unavailable or exhausted.  FAIL_FAST suppresses every not-yet-started
        # node; SKIP_DEPENDENTS suppresses only the transitive descendant cone.
        policy_skips = dict(manual_skips)
        for node in graph.nodes:
            run = latest.get(node.key)
            if run is None or run.status not in _FAILED_RUNS:
                continue
            if self._can_retry(node, run, explicit=False):
                continue
            if node.failure_policy is FailurePolicy.FAIL_FAST:
                for target in graph.nodes:
                    policy_skips.setdefault(
                        target.key,
                        (
                            node.key,
                            FailurePolicy.FAIL_FAST,
                            run.id,
                        ),
                    )
            elif node.failure_policy is FailurePolicy.SKIP_DEPENDENTS:
                for target_key in self._graph_descendants(graph, node.key):
                    policy_skips.setdefault(
                        target_key,
                        (
                            node.key,
                            FailurePolicy.SKIP_DEPENDENTS,
                            run.id,
                        ),
                    )

        if self._retry_failed(
            task,
            graph,
            explicit=False,
            excluded_keys=frozenset(policy_skips),
        ):
            return "active"

        policy_changed = False
        for node in graph.nodes:
            source = policy_skips.get(node.key)
            if source is None:
                continue
            current = latest.get(node.key)
            if current is not None and current.status is not RunStatus.QUEUED:
                continue
            if current is None and node.key in superseded_policy_skips:
                # Another still-active failure policy suppresses this node.  Its
                # earlier unstarted skip remains a sufficient terminal marker;
                # do not reopen it merely to skip it again.
                latest[node.key] = superseded_policy_skips[node.key][0]
                continue
            source_key, policy, source_run_id = source
            self.store.skip_pending_node(
                task.id,
                node.key,
                plan_id=graph.plan.id,
                reason=(
                    f"{policy.value} from failed node {source_key}; "
                    "attempt had not started"
                ),
                command_id=_command(
                    "failure-policy-skip",
                    graph.plan.id,
                    source_run_id,
                    policy.value,
                    node.key,
                ),
            )
            policy_changed = True
        if policy_changed:
            self._runtime_for_task(task.id, rebuild=True)
            return "active"

        incoming: dict[str, list[Any]] = {node.key: [] for node in graph.nodes}
        for edge in graph.edges:
            incoming[edge.to_node].append(edge)
        changed = False
        for node in graph.nodes:
            if node.key in latest:
                continue
            edges = incoming[node.key]
            if not edges:
                stale = superseded_policy_skips.get(node.key)
                if stale is None:
                    latest[node.key] = self._enqueue(task, graph, node)
                else:
                    skipped, recovered_source = stale
                    latest[node.key] = self.store.reopen_policy_skipped_run(
                        skipped.id,
                        reason=(
                            "failure-policy source recovered in successful run "
                            f"{recovered_source.id}"
                        ),
                        session_id=f"__orch__{node.id}_{skipped.attempt}",
                        command_id=_command(
                            "reopen-policy-skip",
                            graph.plan.id,
                            skipped.id,
                            recovered_source.id,
                        ),
                    )
                changed = True
                continue
            source_runs = [latest.get(edge.from_node) for edge in edges]
            if node.join_policy is JoinPolicy.ALL:
                if any(
                    run is None or run.status not in _TERMINAL_RUNS
                    for run in source_runs
                ):
                    continue
                matches = [
                    self._edge_matches(edge.condition, run.status)
                    for edge, run in zip(edges, source_runs)
                    if run
                ]
                required = [
                    match for match, edge in zip(matches, edges) if edge.required
                ]
                ready = all(required if required else matches)
            else:
                applicable = [
                    (edge, run)
                    for edge, run in zip(edges, source_runs)
                    if edge.required
                ] or list(zip(edges, source_runs))
                ready = any(
                    run is not None
                    and run.status in _TERMINAL_RUNS
                    and self._edge_matches(edge.condition, run.status)
                    for edge, run in applicable
                )
                if not ready and any(
                    run is None or run.status not in _TERMINAL_RUNS
                    for _, run in applicable
                ):
                    continue
            if ready:
                stale = superseded_policy_skips.get(node.key)
                if stale is None:
                    latest[node.key] = self._enqueue(task, graph, node)
                else:
                    skipped, recovered_source = stale
                    latest[node.key] = self.store.reopen_policy_skipped_run(
                        skipped.id,
                        reason=(
                            "failure-policy source recovered in successful run "
                            f"{recovered_source.id}"
                        ),
                        session_id=f"__orch__{node.id}_{skipped.attempt}",
                        command_id=_command(
                            "reopen-policy-skip",
                            graph.plan.id,
                            skipped.id,
                            recovered_source.id,
                        ),
                    )
                changed = True
            else:
                stale = superseded_policy_skips.get(node.key)
                if stale is not None:
                    # The recovered source no longer satisfies this node's edge
                    # condition (for example a failure-only handler).  Its prior
                    # unstarted skip remains the correct terminal disposition.
                    latest[node.key] = stale[0]
                    continue
                else:
                    latest[node.key] = self.store.skip_node(
                        task.id,
                        node.key,
                        plan_id=graph.plan.id,
                        reason="required dependency condition was not satisfied",
                        command_id=_command("skip", graph.plan.id, node.key),
                    )
                    changed = True
        if changed:
            self._runtime_for_task(task.id, rebuild=True)
            return "active"
        latest = self._latest_runs(
            [run for run in self.store.list_runs(task.id) if run.plan_id == graph.plan.id]
        )
        return "complete" if len(latest) == len(graph.nodes) and all(run.status in _TERMINAL_RUNS for run in latest.values()) else "active"

    def _can_retry(
        self,
        node: NodeRecord,
        run: RunRecord,
        *,
        explicit: bool,
    ) -> bool:
        if run.attempt >= node.retry_policy.max_attempts:
            return False
        if self.enforce_runtime_budgets and run.error_kind in {
            "budget_exceeded",
            "runtime_budget_exceeded",
            "runtime_limit",
        }:
            # The logical work unit has exhausted at least one hard budget
            # dimension. Re-running the same plan node cannot restore that budget;
            # reconciliation must create a revised plan or cancel the task.
            return False
        if explicit:
            return True
        if (
            run.status is RunStatus.LOST
            or run.error_kind in {"lease_expired", "process_tree_cleanup_failed"}
        ):
            # A descendant may still exist outside the owned process group/job. A
            # worker that lost its lease is no longer authorized to record whether
            # cleanup succeeded, so LOST is cleanup-unknown by definition. Never
            # start another automatic attempt until a human reconciles external
            # state through the normal inter-step gate.
            return False
        return (
            node.failure_policy is not FailurePolicy.MANUAL
            and node.effect_safety is not EffectSafety.NON_IDEMPOTENT
        )

    @staticmethod
    def _graph_descendants(graph: PlanGraph, start: str) -> frozenset[str]:
        outgoing: dict[str, list[str]] = {node.key: [] for node in graph.nodes}
        for edge in graph.edges:
            outgoing[edge.from_node].append(edge.to_node)
        found: set[str] = set()
        pending = list(outgoing[start])
        while pending:
            key = pending.pop()
            if key in found:
                continue
            found.add(key)
            pending.extend(outgoing[key])
        return frozenset(found)

    @staticmethod
    def _graph_ancestors(graph: PlanGraph, start: str) -> frozenset[str]:
        incoming: dict[str, list[str]] = {node.key: [] for node in graph.nodes}
        for edge in graph.edges:
            incoming[edge.to_node].append(edge.from_node)
        found: set[str] = set()
        pending = list(incoming[start])
        while pending:
            key = pending.pop()
            if key in found:
                continue
            found.add(key)
            pending.extend(incoming[key])
        return frozenset(found)

    def _manual_failure_control(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        latest: Mapping[str, RunRecord],
    ) -> tuple[bool, dict[str, tuple[str, FailurePolicy, str]]]:
        """Resolve MANUAL failures into retry/continue/skip/cancel decisions.

        Returns ``(waiting_or_retried, skip_targets)``.  Gate source keys include
        the immutable plan and failed attempt, so process restart reuses exactly
        the same decision instead of opening a duplicate or replaying an action.
        """

        resolved: list[tuple[NodeRecord, RunRecord, str]] = []
        for node in graph.nodes:
            run = latest.get(node.key)
            if (
                run is None
                or run.status not in _FAILED_RUNS
                or node.failure_policy is not FailurePolicy.MANUAL
            ):
                continue
            source = (
                f"{task.id}:manual-failure:{graph.plan.id}:"
                f"{node.id}:{run.id}"
            )
            gate = self._gate(task, GateKind.RECONCILIATION, source)
            if gate is None:
                actions = []
                if self._can_retry(node, run, explicit=True):
                    actions.append("retry")
                actions.extend(("continue", "skip_dependents", "cancel"))
                self._open_lifecycle_gate(
                    task,
                    GateKind.RECONCILIATION,
                    source,
                    {
                        "title": f"Manual failure decision: {node.title or node.key}",
                        "description": (
                            "This node's failure policy requires an explicit "
                            "disposition before DAG scheduling can continue."
                        ),
                        "plan_id": graph.plan.id,
                        "node_id": node.id,
                        "node_key": node.key,
                        "run_id": run.id,
                        "attempt": run.attempt,
                        "run_status": run.status.value,
                        "error_kind": run.error_kind,
                        "error_message": run.error_message,
                        "actions": actions,
                    },
                )
                return True, {}
            if gate.status is GateStatus.OPEN:
                return True, {}
            if gate.status is not GateStatus.APPROVED:
                # Rejected/canceled gates move the task out of RUNNING.  Returning
                # a barrier here is defensive if this method observes a stale task.
                return True, {}
            decision = str((gate.resolution or {}).get("decision") or "")
            if decision not in {"retry", "continue", "skip_dependents"}:
                raise ConflictError(
                    f"manual failure gate {gate.id} has invalid decision {decision!r}"
                )
            resolved.append((node, run, decision))

        retried = False
        skip_targets: dict[str, tuple[str, FailurePolicy, str]] = {}
        for node, run, decision in resolved:
            if decision == "retry":
                if not self._retry_run(task, graph, node, run):
                    raise RuntimeLimitError(
                        f"manual retry for {node.key} exceeds its attempt limit"
                    )
                retried = True
            elif decision == "skip_dependents":
                for target_key in self._graph_descendants(graph, node.key):
                    skip_targets.setdefault(
                        target_key,
                        (node.key, FailurePolicy.MANUAL, run.id),
                    )
        return retried, skip_targets

    def _retryable_failed_runs(
        self,
        graph: PlanGraph,
        failed: Sequence[RunRecord],
    ) -> tuple[RunRecord, ...]:
        nodes = {node.id: node for node in graph.nodes}
        return tuple(
            run
            for run in failed
            if (node := nodes.get(run.node_id)) is not None
            and self._can_retry(node, run, explicit=True)
        )

    @staticmethod
    def _compatibility_retry_base_attempts(
        gate: GateRecord,
    ) -> dict[str, dict[str, Any]]:
        raw = gate.prompt.get("compatibility_retry")
        if (
            not isinstance(raw, Mapping)
            or str(raw.get("reason") or "") not in _COMPATIBILITY_RETRY_REASONS
        ):
            return {}
        attempts = raw.get("base_attempts")
        if not isinstance(attempts, Mapping):
            return {}
        parsed: dict[str, dict[str, Any]] = {}
        for key, value in attempts.items():
            if not isinstance(value, Mapping):
                continue
            run_id = str(value.get("run_id") or "")
            try:
                attempt = int(value.get("attempt") or 0)
            except (TypeError, ValueError):
                continue
            if run_id and attempt > 0:
                parsed[str(key)] = {"run_id": run_id, "attempt": attempt}
        return parsed

    def _continue_compatibility_verification_retry(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        latest: Mapping[str, RunRecord],
        verification_reports: Sequence[Mapping[str, Any]],
    ) -> Optional[bool]:
        """Continue an approved one-shot repair in dependency-safe waves.

        ``True`` means an active/new replay requires returning to execution,
        ``False`` means the repair exists but cannot safely advance, and ``None``
        means no compatibility replay applies to the current evaluation.
        """

        prefix = f"{task.id}:reconciliation:{graph.plan.id}:"
        selected_gate = next(
            (
                gate
                for gate in reversed(self.store.list_gates(task.id))
                if gate.kind is GateKind.RECONCILIATION
                and gate.run_id is None
                and gate.source_key.startswith(prefix)
                and gate.status is GateStatus.APPROVED
                and str((gate.resolution or {}).get("decision") or "") == "retry"
                and self._compatibility_retry_base_attempts(gate)
            ),
            None,
        )
        if selected_gate is None:
            return None
        base = self._compatibility_retry_base_attempts(selected_gate)
        reports = {
            str(report.get("node_key") or ""): report
            for report in verification_reports
        }
        selected_keys = set(base)
        if any(
            (run := latest.get(key)) is not None
            and run.attempt > int(value["attempt"])
            and run.status in _ACTIVE_RUNS
            for key, value in base.items()
        ):
            return True

        candidates = {
            key
            for key in selected_keys
            if key in reports
            and str(reports[key].get("status") or "unknown") != "pass"
            and (run := latest.get(key)) is not None
            and run.id == str(base[key]["run_id"])
            and run.attempt == int(base[key]["attempt"])
            and run.status is RunStatus.SUCCEEDED
        }
        if not candidates:
            return None

        ancestors = {
            key: self._graph_ancestors(graph, key) & selected_keys
            for key in candidates
        }
        ready: list[str] = []
        for key in sorted(candidates):
            if ancestors[key] & candidates:
                continue
            prerequisites_passed = True
            for ancestor in ancestors[key]:
                prior = latest.get(ancestor)
                prior_report = reports.get(ancestor)
                if (
                    prior is None
                    or prior.attempt <= int(base[ancestor]["attempt"])
                    or prior.status is not RunStatus.SUCCEEDED
                    or prior_report is None
                    or str(prior_report.get("status") or "unknown") != "pass"
                ):
                    prerequisites_passed = False
                    break
            if prerequisites_passed:
                ready.append(key)

        nodes = {node.key: node for node in graph.nodes}
        scheduled = False
        for key in ready:
            node = nodes.get(key)
            run = latest.get(key)
            if node is not None and run is not None:
                scheduled = (
                    self._retry_run(
                        task,
                        graph,
                        node,
                        run,
                        compatibility_gate=selected_gate,
                    )
                    or scheduled
                )
        return scheduled

    def _retryable_adverse_verification_runs(
        self,
        graph: PlanGraph,
        latest: Mapping[str, RunRecord],
        reports: Sequence[Mapping[str, Any]],
        *,
        excluded_run_ids: frozenset[str] = frozenset(),
    ) -> tuple[RunRecord, ...]:
        """Return successful verifier calls whose formal verdict needs another turn.

        A provider process can finish successfully while its verdict is ``unknown`` or
        ``fail``.  Treating only failed process statuses as retryable trapped these
        tasks behind a Human Gate with no Retry action, even after the missing evidence
        became available.
        """

        nodes = {node.key: node for node in graph.nodes}
        selected: list[RunRecord] = []
        seen: set[str] = set()
        for report in reports:
            if str(report.get("status") or "unknown") == "pass":
                continue
            node_key = str(report.get("node_key") or "")
            run = latest.get(node_key)
            node = nodes.get(node_key)
            if (
                run is None
                or node is None
                or run.id in excluded_run_ids
                or run.id in seen
                or not self._can_retry(node, run, explicit=True)
            ):
                continue
            seen.add(run.id)
            selected.append(run)
        return tuple(selected)

    def _retry_failed(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        *,
        explicit: bool,
        excluded_keys: frozenset[str] = frozenset(),
    ) -> bool:
        latest = self._latest_runs(
            [run for run in self.store.list_runs(task.id) if run.plan_id == graph.plan.id]
        )
        scheduled = False
        for node in graph.nodes:
            if node.key in excluded_keys:
                continue
            run = latest.get(node.key)
            if run is None:
                continue
            if run.status not in _FAILED_RUNS:
                continue
            if self._can_retry(node, run, explicit=explicit):
                scheduled = self._retry_run(task, graph, node, run) or scheduled
        return scheduled

    def _retry_run(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        node: NodeRecord,
        run: RunRecord,
        *,
        compatibility_gate: Optional[GateRecord] = None,
    ) -> bool:
        if (
            run.attempt >= node.retry_policy.max_attempts
            and compatibility_gate is None
        ):
            return False
        allocation = self._run_budget(task, graph, node)
        spent = RuntimeBudget()
        for prior in sorted(
            (
                item
                for item in self.store.list_runs(task.id)
                if item.plan_id == graph.plan.id
                and item.node_id == node.id
                and item.status is not RunStatus.QUEUED
            ),
            key=lambda item: (item.attempt, item.created_at, item.id),
        ):
            available = allocation - spent
            accounted = self._bounded_budget(
                self._usage_for_run(prior), available
            )
            spent += accounted
        remaining = allocation - spent
        if (
            remaining.model_calls < 1
            or remaining.tokens < 1
            or remaining.wall_seconds < 1
        ):
            return False
        next_attempt = run.attempt + 1
        delay = min(
            node.retry_policy.max_delay_seconds,
            node.retry_policy.initial_delay_seconds
            * (node.retry_policy.multiplier ** max(0, next_attempt - 2)),
        )
        if node.retry_policy.jitter:
            digest = hashlib.sha256(
                f"{run.id}:{node.id}:{next_attempt}".encode("utf-8")
            ).digest()
            unit = int.from_bytes(digest[:8], "big") / (2**64 - 1)
            delay *= 1 + node.retry_policy.jitter * (2 * unit - 1)
        base_time = run.finished_at or run.created_at
        ready_at = base_time + timedelta(seconds=max(0.0, delay))
        self._enqueue(
            task,
            graph,
            node,
            attempt=next_attempt,
            ready_at=ready_at,
            recovery_gate_id=(
                compatibility_gate.id if compatibility_gate is not None else None
            ),
        )
        return True

    def _enqueue(
        self,
        task: TaskRecord,
        graph: PlanGraph,
        node: NodeRecord,
        *,
        attempt: Optional[int] = None,
        ready_at: Optional[datetime] = None,
        recovery_gate_id: Optional[str] = None,
    ) -> RunRecord:
        chosen_attempt = attempt or 1
        if chosen_attempt > self.runtime_limits.max_attempts_per_node:
            raise RuntimeLimitError(
                f"attempt {chosen_attempt} exceeds runtime attempt limit"
            )
        run = self.store.enqueue_run(
            task.id,
            node.key,
            plan_id=graph.plan.id,
            attempt=chosen_attempt,
            ready_at=ready_at,
            # A node key and attempt are unique only inside one plan revision.  Bind
            # the transcript to the immutable node identity so replanning can never
            # make a Reviewer/Tester inherit a prior Worker's private conversation.
            session_id=f"__orch__{node.id}_{chosen_attempt}",
            recovery_gate_id=recovery_gate_id,
            command_id=_command("enqueue", graph.plan.id, node.key, chosen_attempt),
        )
        self._runtime_for_task(task.id, rebuild=True)
        return run

    @staticmethod
    def _latest_runs(runs: Sequence[RunRecord]) -> dict[str, RunRecord]:
        latest: dict[str, RunRecord] = {}
        for run in runs:
            if run.node_key not in latest or run.attempt > latest[run.node_key].attempt:
                latest[run.node_key] = run
        return latest

    @staticmethod
    def _superseded_policy_skips(
        latest: Mapping[str, RunRecord],
    ) -> dict[str, tuple[RunRecord, RunRecord]]:
        """Return unstarted policy skips invalidated by a successful source retry."""

        superseded: dict[str, tuple[RunRecord, RunRecord]] = {}
        marker = " from failed node "
        policy_prefixes = tuple(f"{policy.value}{marker}" for policy in FailurePolicy)
        for node_key, run in latest.items():
            message = str(run.error_message or "")
            if (
                run.status is not RunStatus.SKIPPED
                or run.error_kind != "failure_policy"
                or run.started_at is not None
                or not message.startswith(policy_prefixes)
            ):
                continue
            source_key = message.split(marker, 1)[1].split(";", 1)[0].strip()
            source = latest.get(source_key)
            if (
                source is not None
                and source.status is RunStatus.SUCCEEDED
                and source.created_at > run.created_at
            ):
                superseded[node_key] = (run, source)
        return superseded

    @staticmethod
    def _edge_matches(condition: EdgeCondition, status: RunStatus) -> bool:
        if condition is EdgeCondition.ALWAYS or condition is EdgeCondition.TERMINAL:
            return status in _TERMINAL_RUNS
        if condition is EdgeCondition.SUCCESS:
            return status is RunStatus.SUCCEEDED
        return status in _FAILED_RUNS

    async def _finalize_succeeded_run(self, run_id: str) -> RunRecord:
        async with self._finalize_lock:
            return await self._finalize_succeeded_run_unlocked(run_id)

    async def _finalize_succeeded_run_unlocked(self, run_id: str) -> RunRecord:
        """Apply the durable post-success hand-off into the task candidate.

        ``complete_run`` is the fencing commit point.  Only after that succeeds may
        this method mutate the orchestration-owned task candidate.  A pending marker
        blocks the node's concurrency key and DAG scheduling, so a crash simply causes
        startup to replay this idempotent hand-off before any dependent run starts.
        """

        run = self.store.get_run(run_id)
        if run.status is not RunStatus.SUCCEEDED:
            return run
        task = self.store.get_task(run.task_id)
        graph = self.store.get_plan(run.plan_id)
        node = next(item for item in graph.nodes if item.id == run.node_id)
        profile = self._profile_for_node(node)
        output = dict(run.output or {})

        artifact = output.get("candidate_artifact")
        if isinstance(artifact, Mapping):
            blob = dict(artifact.get("blob") or {})
            self.store.add_evidence(
                task.id,
                kind=EvidenceKind.ARTIFACT,
                payload={"title": "Candidate patch", **dict(artifact)},
                created_by=profile.profile_id,
                blob_uri=str(blob.get("uri") or "") or None,
                content_hash=str(blob.get("sha256") or "") or None,
                plan_id=graph.plan.id,
                node_id=node.id,
                run_id=run.id,
                mime_type=str(blob.get("mime_type") or "text/x-diff"),
                command_id=_command("patch-evidence", run.id),
            )

        commit = dict(output.get("workspace_commit") or {})
        if commit.get("status") == "pending":
            snapshot_id = str(commit.get("snapshot_id") or "")
            try:
                snapshot = self.workspaces.load(snapshot_id)
                receipt = await self._durable_to_thread(
                    self._deliver_pending_run_candidate,
                    snapshot,
                    task.id,
                    run.id,
                    commit,
                )
            except (WorkspaceError, KeyError) as exc:
                failed = {**commit, "status": "failed", "error": str(exc)}
                run = self.store.merge_run_output(
                    run.id,
                    {"workspace_commit": failed},
                    allowed_statuses=(RunStatus.SUCCEEDED,),
                    command_id=_command(
                        "candidate-commit-failed", run.id, _canonical_hash(failed)
                    ),
                )
                return run
            applied = {
                **commit,
                "status": "applied",
                "receipt": receipt.to_dict(),
            }
            try:
                run = self.store.merge_run_output(
                    run.id,
                    {"workspace_commit": applied},
                    allowed_statuses=(RunStatus.SUCCEEDED,),
                    command_id=_command("candidate-commit-applied", run.id),
                )
            except IdempotencyConflict:
                run = self.store.get_run(run.id)
                if self._workspace_commit_status(run) != "applied":
                    raise
            self.store.add_evidence(
                task.id,
                kind=EvidenceKind.CHECKPOINT,
                payload={
                    "title": "Task candidate commit",
                    "scope": "orchestration_staging",
                    **receipt.to_dict(),
                },
                created_by="workspace-manager",
                plan_id=graph.plan.id,
                node_id=node.id,
                run_id=run.id,
                command_id=_command("candidate-commit-evidence", run.id),
            )

        for index, evidence in enumerate(output.get("evidence_records") or ()):
            if not isinstance(evidence, Mapping):
                continue
            kind_raw = str(evidence.get("kind") or "note")
            kind = (
                EvidenceKind(kind_raw)
                if kind_raw in {item.value for item in EvidenceKind}
                else EvidenceKind.NOTE
            )
            blob_info = (
                dict(evidence.get("blob") or {})
                if isinstance(evidence.get("blob"), Mapping)
                else {}
            )
            self.store.add_evidence(
                task.id,
                kind=kind,
                payload=dict(evidence),
                created_by=profile.profile_id,
                blob_uri=str(
                    evidence.get("uri")
                    or blob_info.get("uri")
                    or ""
                )
                or None,
                content_hash=str(
                    evidence.get("sha256")
                    or blob_info.get("sha256")
                    or ""
                )
                or None,
                mime_type=str(
                    evidence.get("mime_type")
                    or blob_info.get("mime_type")
                    or "application/json"
                ),
                plan_id=graph.plan.id,
                node_id=node.id,
                run_id=run.id,
                command_id=_command("run-evidence", run.id, index),
            )
        return self.store.get_run(run.id)

    def _deliver_with_commit_lock(
        self,
        snapshot: WorkspaceSnapshot,
        *,
        expected_candidate_manifest_sha256: Optional[str] = None,
        expected_patch_sha256: Optional[str] = None,
        fence_held: bool = False,
    ):
        with self._workspace_commit_lock:
            return self.workspaces.deliver(
                snapshot,
                expected_candidate_manifest_sha256=(
                    expected_candidate_manifest_sha256
                ),
                expected_patch_sha256=expected_patch_sha256,
                fence_check=self.store.renew_scheduler_fence,
                _fence_held=fence_held,
            )

    def _deliver_pending_run_candidate(
        self,
        snapshot: WorkspaceSnapshot,
        task_id: str,
        run_id: str,
        sealed_commit: Mapping[str, Any],
    ):
        """Validate a completed run's sealed bytes under the delivery fence."""

        expected_manifest = str(
            sealed_commit.get("candidate_manifest_sha256") or ""
        )
        expected_patch = str(sealed_commit.get("patch_sha256") or "")
        if not expected_manifest or not expected_patch:
            raise WorkspaceError("pending run candidate is missing sealed digests")
        with self.workspaces.delivery_fence(snapshot):
            task = self.store.get_task(task_id)
            run = self.store.get_run(run_id)
            current_commit = dict((run.output or {}).get("workspace_commit") or {})
            if task.status not in {
                TaskStatus.RUNNING,
                TaskStatus.WAITING_HUMAN,
                TaskStatus.WAITING_CHILD,
                TaskStatus.PAUSED,
            }:
                raise WorkspaceError(
                    f"run candidate cannot commit while task is {task.status.value}"
                )
            if (
                run.status is not RunStatus.SUCCEEDED
                or current_commit.get("status") != "pending"
                or str(current_commit.get("candidate_manifest_sha256") or "")
                != expected_manifest
                or str(current_commit.get("patch_sha256") or "")
                != expected_patch
            ):
                raise WorkspaceError("pending run candidate seal is no longer current")
            return self._deliver_with_commit_lock(
                snapshot,
                expected_candidate_manifest_sha256=expected_manifest,
                expected_patch_sha256=expected_patch,
                fence_held=True,
            )

    async def _recover_pending_run_commits(self) -> None:
        for run in self.store.list_pending_workspace_commits():
            await self._finalize_succeeded_run(run.id)

    async def _execute_with_lease_guard(
        self,
        context: RunExecutionContext,
        heartbeat: asyncio.Task[None],
        timeout_seconds: Optional[int],
    ) -> ExecutionOutcome:
        execution = asyncio.create_task(
            self.executor.execute(context),
            name=f"orchestration-agent-{context.claim.run.id}",
        )
        primary_error: Optional[BaseException] = None
        try:
            done, _pending = await asyncio.wait(
                {execution, heartbeat},
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if execution in done:
                return await execution
            if heartbeat in done:
                error = heartbeat.exception()
                if error is not None:
                    raise error
                raise LeaseConflict(
                    f"heartbeat stopped for active run {context.claim.run.id}"
                )
            raise asyncio.TimeoutError
        except asyncio.CancelledError as exc:
            primary_error = exc
        except Exception as exc:
            primary_error = exc

        # Every non-success termination source (timeout, heartbeat/lease failure,
        # shutdown, or user cancellation) has one cancellation-resistant settlement
        # path. A second Task.cancel() must not let the owner abandon a shell cleanup
        # that is still deciding whether process containment failed.
        interrupt = getattr(self.executor, "interrupt", None)
        if callable(interrupt):
            interrupt(context.claim.run.id)
        if not execution.done():
            execution.cancel()
        canceled_while_draining = False
        while not execution.done():
            try:
                await asyncio.shield(execution)
            except asyncio.CancelledError:
                # If the nested task is done, this is its own cancellation. If it
                # remains live, another caller cancellation interrupted our shield;
                # remember it and continue draining without forwarding it inward.
                if not execution.done():
                    canceled_while_draining = True
            except Exception:
                break
        try:
            result: Any = execution.result()
        except BaseException as exc:  # includes the nested task's CancelledError
            result = exc
        if (
            isinstance(result, ExecutionOutcome)
            and result.status == "failed"
            and result.error_kind == "process_tree_cleanup_failed"
        ):
            return result
        if canceled_while_draining and not isinstance(
            primary_error, asyncio.CancelledError
        ):
            raise asyncio.CancelledError
        if primary_error is not None:
            raise primary_error
        if isinstance(result, BaseException):
            raise result
        if isinstance(result, ExecutionOutcome):
            return result
        raise RuntimeError("orchestration executor returned an invalid outcome")

    # -- run execution ---------------------------------------------------
    def _required_context_preflight(
        self,
        task: TaskRecord,
        run: RunRecord,
        refs: Sequence[Any],
    ) -> Optional[tuple[Any, dict[str, Any]]]:
        """Verify required refs before any Agent/provider turn is dispatched."""

        for ref in refs:
            if ref.requirement is not ContextRequirement.REQUIRED:
                continue
            try:
                result = self.context_resolver.verify(
                    ref, workspace=task.workspace
                )
            except Exception as exc:
                result = {
                    "available": False,
                    "stale": False,
                    "reason": f"{type(exc).__name__}: {exc}"[:2_000],
                }
            self.store.record_context_ref_verification(
                ref.id,
                run_id=run.id,
                result=result,
                command_id=_command(
                    "context-preflight", run.id, ref.id, _canonical_hash(result)
                ),
            )
            if not bool(result.get("available")) or bool(result.get("stale")):
                return ref, result
        return None

    def _record_run_activity(
        self,
        claim: RunClaim,
        *,
        event_key: str,
        source_id: str,
        kind: str,
        status: str,
        title: str,
        summary: str = "",
        detail: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Expose coordinator work that happens before an Agent runtime starts."""

        try:
            self.store.append_run_activity(
                claim.run.id,
                claim.lease.token,
                claim.lease.fencing_token,
                event_key=event_key,
                source_id=source_id,
                kind=kind,
                status=status,
                title=title,
                summary=summary,
                detail=detail,
            )
        except Exception:
            logger.warning(
                "could not append coordinator activity for run %s",
                claim.run.id,
                exc_info=True,
            )

    async def _execute_claim(self, claim: RunClaim) -> None:
        run = claim.run
        heartbeat: Optional[asyncio.Task[None]] = None
        snapshot = None
        node: Optional[NodeRecord] = None
        quality_reservation_id: Optional[str] = None
        quality_reservation_token: Optional[int] = None
        quality_reservation_settled = False
        quality_budget_exhausted = False
        runtime_id = self._run_runtime_id(run.id)
        wake_was_delivered = False
        try:
            self.store.start_run(
                run.id,
                claim.lease.token,
                claim.lease.fencing_token,
                command_id=_command("run-start", run.id, claim.lease.fencing_token),
            )
            heartbeat = asyncio.create_task(self._heartbeat(claim))
            task = self.store.get_task(run.task_id)
            runtime = self._runtime_for_task(task.id, rebuild=True)
            runtime_node = runtime.get(runtime_id)
            if runtime_node.status is RuntimeStatus.SUSPENDED:
                runtime.resume(runtime_id)
            elif runtime_node.status is RuntimeStatus.PENDING:
                runtime.start(runtime_id)
            remaining_budget = runtime_node.remaining_budget
            graph = self.store.get_plan(run.plan_id)
            node = next(item for item in graph.nodes if item.id == run.node_id)
            quality_binding = self._quality_budget_binding(task.id, node.key)
            if quality_binding is not None:
                _mode, quality_ledger_id, quality_allocation = quality_binding
                if quality_ledger_id is None:
                    raise ConflictError(
                        "quality strategy execution requires an active budget ledger"
                    )
                (
                    quality_reservation_id,
                    quality_reservation_token,
                ) = self.quality_budgets.reserve(
                    quality_ledger_id,
                    amounts=self._quality_reservation_amounts(quality_allocation),
                    purpose=f"strategy-node:{node.key}:attempt:{run.attempt}",
                    run_id=run.id,
                    reservation_id=f"quality-reservation:{run.id}",
                )
            profile = self._profile_for_node(node)
            model_policy = self._policy_for_node(node)
            preset = self._runtime_preset_for_task(task)
            if (
                preset
                and preset.fallback_mode == "strict"
                and model_policy.fallback_for_explicit
            ):
                raise ConflictError(
                    f"frozen model policy for strict runtime preset "
                    f"{preset.preset_id} permits explicit-model fallback"
                )
            router = ModelRouter(self.model_candidates(), policy=model_policy)
            runtime_checkpoint = dict(
                (run.output or {}).get("subscription_runtime_checkpoint") or {}
            )
            # Once a durable attempt is bound to a vendor session, its runtime/model
            # is part of the recovery identity. A transient health change must never
            # make the same attempt resume through another provider.
            checkpoint_model = str(runtime_checkpoint.get("runtime_id") or "")
            routing = router.select(
                RoutingRequest(
                    purpose=f"{node.kind.value}:{profile.role.value}",
                    required_capabilities=frozenset({"tools"}) if profile.allowed_tools else frozenset(),
                    requested_model=(
                        checkpoint_model
                        or node.model
                        or task.input.get("requested_model")
                    ),
                    correlation={"task_id": task.id, "run_id": run.id, "node_id": node.id},
                )
            )
            self.store.add_evidence(
                task.id,
                kind=EvidenceKind.DECISION,
                payload={"title": "Model routing", **routing.audit_record()},
                created_by="model-router",
                plan_id=graph.plan.id,
                node_id=node.id,
                run_id=run.id,
                command_id=_command("route", run.id, routing.decision_id),
            )

            workspace: Optional[Path] = None
            if task.workspace:
                workspace_source = (
                    f"coordinator:attempt-{run.attempt}:workspace"
                )
                if bool(task.policy.get("read_only", False)):
                    workspace = Path(task.workspace).expanduser().resolve()
                    if not workspace.is_dir():
                        raise WorkspaceError(
                            f"read-only workspace is not a directory: {workspace}"
                        )
                    self._record_run_activity(
                        claim,
                        event_key=f"{workspace_source}:ready",
                        source_id=workspace_source,
                        kind="lifecycle",
                        status="completed",
                        title="Read-only workspace ready",
                        summary=(
                            "The source workspace is mounted through the runtime's "
                            "read-only sandbox; no snapshot copy is required."
                        ),
                        detail={"isolation": "read_only_source"},
                    )
                else:
                    self._record_run_activity(
                        claim,
                        event_key=f"{workspace_source}:started",
                        source_id=workspace_source,
                        kind="lifecycle",
                        status="running",
                        title="Preparing isolated workspace",
                        summary="Creating the Agent's durable workspace snapshot.",
                        detail={"isolation": "writable_snapshot"},
                    )
                    try:
                        snapshot = await self._durable_to_thread(
                            self._ensure_run_snapshot, task, run
                        )
                    except Exception as exc:
                        self._record_run_activity(
                            claim,
                            event_key=f"{workspace_source}:failed",
                            source_id=workspace_source,
                            kind="error",
                            status="failed",
                            title="Workspace preparation failed",
                            summary=str(exc),
                        )
                        raise
                    workspace = snapshot.candidate if snapshot is not None else None
                    self._record_run_activity(
                        claim,
                        event_key=f"{workspace_source}:completed",
                        source_id=workspace_source,
                        kind="lifecycle",
                        status="completed",
                        title="Isolated workspace ready",
                        summary="The durable workspace snapshot is ready for the Agent.",
                        detail={"isolation": "writable_snapshot"},
                    )
            subject_source = f"coordinator:attempt-{run.attempt}:subject"
            self._record_run_activity(
                claim,
                event_key=f"{subject_source}:started",
                source_id=subject_source,
                kind="lifecycle",
                status="running",
                title="Preparing execution subject",
                summary="Binding this run to the current task revision.",
            )
            try:
                subject = await self._durable_to_thread(
                    self._candidate_subject, task, graph
                )
                if bool(node.metadata.get("task_quality_v2")):
                    subject = {
                        **subject,
                        "task_quality_v2": await self._durable_to_thread(
                            self._quality_assignment_context,
                            task,
                            node,
                            remaining_budget,
                        ),
                    }
            except Exception as exc:
                self._record_run_activity(
                    claim,
                    event_key=f"{subject_source}:failed",
                    source_id=subject_source,
                    kind="error",
                    status="failed",
                    title="Execution subject preparation failed",
                    summary=str(exc),
                )
                raise
            self._record_run_activity(
                claim,
                event_key=f"{subject_source}:completed",
                source_id=subject_source,
                kind="lifecycle",
                status="completed",
                title="Execution subject ready",
                summary="The Agent can now start against the bound task revision.",
                detail={"subject_kind": subject.get("kind")},
            )
            # Each run is pinned to the Brief revision captured at enqueue time.
            # Raw upstream outputs stay outside the initial prompt and are exposed
            # only through explicit, auditable ContextRefs.
            brief = (
                self.store.get_brief_by_id(run.brief_id)
                if run.brief_id
                else self.store.get_active_brief(task.id)
            )
            context_refs = self.store.list_context_refs(
                task.id, brief_id=brief.id
            )
            delivered_wakes = self.store.list_wakes(
                task_id=task.id,
                statuses=(WakeStatus.CLAIMED, WakeStatus.DELIVERED),
                limit=100,
            )
            delivery_wake = next(
                (
                    item
                    for item in reversed(delivered_wakes)
                    if item.target_run_id in {None, run.id}
                ),
                None,
            )
            wake_was_delivered = delivery_wake is not None
            context_issue = self._required_context_preflight(
                task, run, context_refs
            )
            if context_issue is not None:
                failed_ref, verification = context_issue
                error_kind = (
                    "required_context_stale"
                    if bool(verification.get("stale"))
                    else "required_context_unavailable"
                )
                message = (
                    f"required ContextRef {failed_ref.id} failed dispatch preflight: "
                    f"{verification.get('reason') or error_kind}"
                )
                self.store.fail_run(
                    run.id,
                    claim.lease.token,
                    claim.lease.fencing_token,
                    error_kind=error_kind,
                    error_message=message,
                    output={
                        "context_ref_id": failed_ref.id,
                        "verification": verification,
                    },
                    command_id=_command(
                        "run-context-preflight-failed",
                        run.id,
                        claim.lease.fencing_token,
                        failed_ref.id,
                    ),
                )
                try:
                    runtime.finish(runtime_id, RuntimeStatus.FAILED)
                except RuntimeStateError:
                    pass
                fresh = self.store.get_task(task.id)
                if fresh.status in {
                    TaskStatus.QUEUED,
                    TaskStatus.RUNNING,
                    TaskStatus.WAITING_HUMAN,
                    TaskStatus.WAITING_CHILD,
                    TaskStatus.PAUSED,
                    TaskStatus.BLOCKED,
                }:
                    self._transition_status(
                        fresh,
                        TaskStatus.NEEDS_RECONCILIATION,
                        error_kind,
                        output={
                            **dict(fresh.output or {}),
                            "error_kind": error_kind,
                            "error": message,
                            "context_ref_id": failed_ref.id,
                        },
                    )
                self._runtime_for_task(task.id, rebuild=True)
                return
            effective_tools = set(profile.allowed_tools)
            if bool(node.metadata.get("task_quality_v2")):
                effective_tools.update(quality_tool_names_for_role(profile.role))
            if runtime_node.effective_permissions.tools is not None:
                effective_tools &= set(runtime_node.effective_permissions.tools)
            execution_envelope = None
            if bool(task.policy.get("structured_handoff")):
                # A Work Product is the durable, explicit cross-role result channel.
                # It is safe to expose task-local immutable summaries to downstream
                # nodes; private transcripts and arbitrary upstream run outputs remain
                # excluded.  Products created by a just-finished dependency are
                # committed before the scheduler can dispatch this run.
                upstream_context = self._upstream_context(task, graph, node)
                upstream_run_order = {
                    str(item.get("run_id") or ""): index
                    for index, item in enumerate(upstream_context)
                    if str(item.get("run_id") or "")
                }
                upstream_run_ids = set(upstream_run_order)
                selected_products = [
                    product
                    for product in self.store.list_work_products(task.id, limit=100)
                    if (
                        product.run_id in upstream_run_ids
                        or (
                            product.run_id is None
                            and (
                                not str(
                                    product.metadata.get("source_run_id") or ""
                                )
                                or str(product.metadata.get("source_run_id") or "")
                                in upstream_run_ids
                            )
                        )
                    )
                ]

                def product_rank(product: Any) -> tuple[int, datetime, str]:
                    source_run_id = str(
                        product.run_id
                        or product.metadata.get("source_run_id")
                        or ""
                    )
                    # Explicit task-owned/operator products have no source run and
                    # remain first-class candidate evidence.
                    rank = (
                        -1
                        if not source_run_id
                        else upstream_run_order.get(
                            source_run_id, len(upstream_run_order)
                        )
                    )
                    return rank, product.created_at, product.id

                handoff_products = tuple(sorted(selected_products, key=product_rank))
                execution_envelope = build_execution_envelope(
                    task=task,
                    brief=brief,
                    claim=claim,
                    node=node,
                    profile=profile,
                    routing=routing,
                    context_refs=context_refs,
                    work_products=handoff_products,
                    wake=delivery_wake,
                    workspace_id=(
                        snapshot.snapshot_id if snapshot is not None else None
                    ),
                    effective_tools=sorted(effective_tools),
                )
                prompt_bytes = len(
                    render_initial_user_prompt(execution_envelope).encode("utf-8")
                )
                self.handoff_metrics.observe(
                    "orchestration_handoff_initial_prompt_bytes", prompt_bytes
                )
                self.handoff_metrics.observe(
                    "orchestration_handoff_context_refs",
                    int(execution_envelope.context_manifest.get("ref_count") or 0),
                )
                self.handoff_metrics.observe(
                    "orchestration_handoff_context_tokens_estimated",
                    int(
                        execution_envelope.context_manifest.get("estimated_tokens")
                        or 0
                    ),
                )
                self.handoff_metrics.observe(
                    "orchestration_envelope_bytes",
                    len(
                        json.dumps(
                            execution_envelope.to_dict(),
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    ),
                )
            context = RunExecutionContext(
                task=task,
                graph=graph,
                node=node,
                claim=claim,
                profile=profile,
                routing=routing,
                workspace=workspace,
                parent_runtime_id=runtime_id,
                runtime_id=runtime_id,
                runtime_budget=remaining_budget,
                effective_permissions=runtime_node.effective_permissions,
                subject=subject,
                upstream_context=(),
                brief=brief,
                execution_envelope=execution_envelope,
            )
            timeout_seconds = (
                min(node.timeout_seconds, remaining_budget.wall_seconds)
                if self.enforce_runtime_budgets
                else None
            )
            if timeout_seconds is not None and timeout_seconds <= 0:
                raise BudgetExceededError("run has no wall-clock budget remaining")
            if node.kind is NodeKind.NOOP and bool(
                node.metadata.get("task_quality_v2")
            ):
                outcome = await self._durable_to_thread(
                    self.quality_workflow.execute, context
                )
            else:
                outcome = await self._execute_with_lease_guard(
                    context, heartbeat, timeout_seconds
                )
            if (
                quality_reservation_id is not None
                and quality_reservation_token is not None
            ):
                raw_provider_total = outcome.usage.get("provider_reported_tokens")
                if raw_provider_total is None:
                    raw_provider_total = outcome.usage.get("tokens", 0)
                ledger = self.quality_budgets.consume(
                    quality_reservation_id,
                    usage=QualityProviderUsage(
                        model_calls=int(outcome.usage.get("model_calls", 0) or 0),
                        tool_calls=int(outcome.usage.get("tool_calls", 0) or 0),
                        input_tokens=max(
                            int(outcome.usage.get("input_tokens", 0) or 0),
                            int(
                                outcome.usage.get("cached_input_tokens", 0) or 0
                            ),
                        ),
                        cached_input_tokens=int(
                            outcome.usage.get("cached_input_tokens", 0) or 0
                        ),
                        output_tokens=int(
                            outcome.usage.get("output_tokens", 0) or 0
                        ),
                        reasoning_tokens=int(
                            outcome.usage.get("reasoning_tokens", 0) or 0
                        ),
                        provider_reported_tokens=int(raw_provider_total or 0),
                        provider_reported_includes_cached=(
                            bool(outcome.usage["provider_reported_includes_cached"])
                            if outcome.usage.get(
                                "provider_reported_includes_cached"
                            )
                            is not None
                            else None
                        ),
                        active_seconds=int(
                            outcome.usage.get("wall_seconds", 0) or 0
                        ),
                        tool_payload_bytes=int(
                            outcome.usage.get("tool_payload_bytes", 0) or 0
                        ),
                    ),
                    fencing_token=quality_reservation_token,
                    final=True,
                )
                quality_reservation_settled = True
                with self.store._read() as connection:
                    budget_row = connection.execute(
                        "SELECT budget_status FROM orch_tasks WHERE id=?", (task.id,)
                    ).fetchone()
                quality_budget_exhausted = bool(
                    budget_row is not None
                    and budget_row["budget_status"] == "exhausted"
                )
            segment_key = str(
                outcome.gate_id
                or f"fence-{claim.lease.fencing_token}-{outcome.status}"
            )
            if outcome.error_kind == "process_tree_cleanup_failed":
                # Containment state is the authoritative terminal result. Persist it
                # under the live run fence before usage accounting: cleanup time can
                # itself exceed the remaining budget, but that secondary fact must
                # never replace the breach with a retryable runtime_limit failure.
                self.store.fail_run(
                    run.id,
                    claim.lease.token,
                    claim.lease.fencing_token,
                    error_kind="process_tree_cleanup_failed",
                    error_message=(
                        outcome.error_message
                        or "process-tree cleanup failed; reconciliation is required"
                    ),
                    command_id=_command(
                        "run-fail", run.id, claim.lease.fencing_token
                    ),
                )
                measured = RuntimeBudget(
                    model_calls=int(outcome.usage.get("model_calls", 0) or 0),
                    tool_calls=int(outcome.usage.get("tool_calls", 0) or 0),
                    tokens=int(outcome.usage.get("tokens", 0) or 0),
                    wall_seconds=int(outcome.usage.get("wall_seconds", 0) or 0),
                )
                accounted = measured
                accounting_error: Optional[BaseException] = None
                try:
                    runtime.charge(runtime_id, measured)
                except (BudgetExceededError, RuntimeLimitError, RuntimeStateError) as exc:
                    accounting_error = exc
                    try:
                        available = runtime.get(runtime_id).remaining_budget
                        accounted = RuntimeBudget(
                            model_calls=min(measured.model_calls, available.model_calls),
                            tool_calls=min(measured.tool_calls, available.tool_calls),
                            tokens=min(measured.tokens, available.tokens),
                            wall_seconds=min(
                                measured.wall_seconds, available.wall_seconds
                            ),
                        )
                        if accounted != RuntimeBudget():
                            runtime.charge(runtime_id, accounted)
                    except (BudgetExceededError, RuntimeLimitError, RuntimeStateError) as cap_exc:
                        accounted = RuntimeBudget()
                        accounting_error = RuntimeStateError(
                            f"{exc}; bounded accounting also failed: {cap_exc}"
                        )
                try:
                    self._record_usage_segment(
                        task,
                        graph,
                        node,
                        run,
                        profile,
                        outcome.usage,
                        segment_key=segment_key,
                        accounted_usage=accounted,
                        accounting_error=accounting_error,
                    )
                except Exception:
                    logger.exception(
                        "could not append cleanup-breach usage evidence for run %s",
                        run.id,
                    )
                try:
                    runtime.finish(runtime_id, RuntimeStatus.FAILED)
                except RuntimeStateError:
                    logger.exception(
                        "could not finish cleanup-breach runtime %s", runtime_id
                    )
                try:
                    self._runtime_for_task(task.id, rebuild=True)
                except Exception:
                    logger.exception(
                        "could not rebuild cleanup-breach runtime projection for %s",
                        run.id,
                    )
                return
            measured = RuntimeBudget(
                model_calls=int(outcome.usage.get("model_calls", 0) or 0),
                tool_calls=int(outcome.usage.get("tool_calls", 0) or 0),
                tokens=int(outcome.usage.get("tokens", 0) or 0),
                wall_seconds=int(outcome.usage.get("wall_seconds", 0) or 0),
            )
            try:
                runtime.charge(runtime_id, measured)
            except (BudgetExceededError, RuntimeLimitError, RuntimeStateError) as exc:
                available = runtime.get(runtime_id).remaining_budget
                accounted = self._bounded_budget(measured, available)
                accounting_error: BaseException = exc
                try:
                    if accounted != RuntimeBudget():
                        runtime.charge(runtime_id, accounted)
                except (BudgetExceededError, RuntimeLimitError, RuntimeStateError) as cap_exc:
                    accounted = RuntimeBudget()
                    accounting_error = RuntimeStateError(
                        f"{exc}; bounded accounting also failed: {cap_exc}"
                    )
                self._record_usage_segment(
                    task,
                    graph,
                    node,
                    run,
                    profile,
                    outcome.usage,
                    segment_key=segment_key,
                    accounted_usage=accounted,
                    accounting_error=accounting_error,
                )
                raise exc
            self._record_usage_segment(
                task,
                graph,
                node,
                run,
                profile,
                outcome.usage,
                segment_key=segment_key,
            )
            if quality_budget_exhausted:
                self.store.fail_run(
                    run.id,
                    claim.lease.token,
                    claim.lease.fencing_token,
                    error_kind="budget_exceeded",
                    error_message=(
                        "provider usage exhausted the frozen Task Quality V2 root budget"
                    ),
                    output={
                        **dict(outcome.output),
                        "checkpoint_preserved": True,
                        "budget_ledger_id": ledger.id,
                    },
                    command_id=_command(
                        "quality-budget-exhausted",
                        run.id,
                        claim.lease.fencing_token,
                    ),
                )
                runtime.finish(runtime_id, RuntimeStatus.FAILED)
                self._runtime_for_task(task.id, rebuild=True)
                return
            if outcome.status == "suspended":
                checkpoint = outcome.output.get("engine_checkpoint")
                if not outcome.gate_id or not isinstance(checkpoint, Mapping):
                    raise ConflictError(
                        "suspended Agent did not return a prepared gate checkpoint"
                    )
                active_run = self.store.get_run(run.id)
                self._verified_checkpoint_payload(
                    active_run,
                    outcome.gate_id,
                    checkpoint,
                    require_reference_identity=True,
                )
                committed_gate = self.store.commit_prepared_gate(
                    run.id,
                    claim.lease.token,
                    claim.lease.fencing_token,
                    gate_id=outcome.gate_id,
                    checkpoint=dict(checkpoint),
                    command_id=_command(
                        "gate-commit",
                        run.id,
                        claim.lease.fencing_token,
                        outcome.gate_id,
                    ),
                )
                runtime.suspend(runtime_id)
                self._runtime_for_task(task.id, rebuild=True)
                await self._gate_opened(committed_gate.id)
                return  # checkpoint + open gate + lease release committed atomically
            if outcome.status != "succeeded":
                self.store.fail_run(
                    run.id,
                    claim.lease.token,
                    claim.lease.fencing_token,
                    error_kind=outcome.error_kind or "agent_failed",
                    error_message=outcome.error_message or "agent run did not complete",
                    output=outcome.output,
                    command_id=_command("run-fail", run.id, claim.lease.fencing_token),
                )
                runtime.finish(runtime_id, RuntimeStatus.FAILED)
                self._runtime_for_task(task.id, rebuild=True)
                return

            output = dict(outcome.output)
            raw_verdict = output.get("verdict")
            if isinstance(raw_verdict, Mapping):
                verdict = dict(raw_verdict)
                criteria = {
                    str(key): self._normalize_verdict(value)
                    for key, value in dict(verdict.get("criteria") or {}).items()
                }
                status = self._normalize_verdict(
                    verdict.get("status", verdict.get("verdict"))
                )
                missing = set(task.acceptance_criteria) - set(criteria)
                if any(value == "fail" for value in criteria.values()):
                    status = "fail"
                elif status == "pass" and (
                    missing or any(value == "unknown" for value in criteria.values())
                ):
                    status = "unknown"
                output["verdict"] = {
                    **verdict,
                    "schema_version": 1,
                    "task_id": task.id,
                    "plan_id": graph.plan.id,
                    "run_id": run.id,
                    "role": profile.role.value,
                    "status": status,
                    "criteria": criteria,
                    "missing_criteria": sorted(missing),
                    "subject": dict(subject),
                }
            if snapshot is not None:
                candidate = await self._durable_to_thread(
                    self.workspaces.collect_candidate, snapshot
                )
                patch_ref = self.blobs.put(candidate.patch.encode("utf-8"), mime_type="text/x-diff")
                output["candidate"] = candidate.to_dict(include_patch=False)
                output["candidate_artifact"] = {
                    **candidate.to_dict(include_patch=False),
                    "blob": patch_ref.as_dict(),
                }
                if (
                    self._profile_mutates_candidate(profile)
                    and not bool(task.policy.get("read_only", False))
                ):
                    output["workspace_commit"] = {
                        "status": "pending",
                        "snapshot_id": snapshot.snapshot_id,
                        "source_scope": "task_candidate",
                        "candidate_manifest_sha256": candidate.candidate_manifest.digest,
                        "patch_sha256": candidate.patch_sha256,
                        "fencing_token": claim.lease.fencing_token,
                    }
            output["evidence_records"] = [dict(item) for item in outcome.evidence]
            handoff_result = self._explicit_handoff_result(output)
            if handoff_result is not None:
                self.store.complete_run_structured(
                    run.id,
                    claim.lease.token,
                    claim.lease.fencing_token,
                    output=output,
                    result=handoff_result,
                    created_by=profile.profile_id,
                    command_id=_command(
                        "run-complete-structured",
                        run.id,
                        claim.lease.fencing_token,
                    ),
                )
            else:
                self.store.complete_run(
                    run.id,
                    claim.lease.token,
                    claim.lease.fencing_token,
                    output=output,
                    command_id=_command(
                        "run-complete", run.id, claim.lease.fencing_token
                    ),
                )
            await self._finalize_succeeded_run(run.id)
            try:
                runtime.finish(runtime_id, RuntimeStatus.SUCCEEDED)
            except RuntimeStateError:
                runtime.suspend(runtime_id)
            self._runtime_for_task(task.id, rebuild=True)
        except asyncio.TimeoutError:
            self._safe_fail(claim, "timeout", "agent run exceeded its timeout", RunStatus.TIMED_OUT)
        except asyncio.CancelledError:
            if self._closing:
                if node is not None and node.effect_safety is EffectSafety.NON_IDEMPOTENT:
                    self._safe_fail(
                        claim,
                        "shutdown_interrupted_non_idempotent",
                        "service stopped during non-idempotent work; manual reconciliation is required",
                    )
                    try:
                        interrupted_task = self.store.get_task(run.task_id)
                        if interrupted_task.status is TaskStatus.RUNNING:
                            self._transition_status(
                                interrupted_task,
                                TaskStatus.NEEDS_RECONCILIATION,
                                "shutdown-interrupted-non-idempotent",
                            )
                    except (ConflictError, VersionConflict):
                        pass
                else:
                    try:
                        self.store.release_run(
                            run.id,
                            claim.lease.token,
                            claim.lease.fencing_token,
                            reason="service_shutdown",
                            command_id=_command(
                                "shutdown-release", run.id, claim.lease.fencing_token
                            ),
                        )
                    except (LeaseConflict, ConflictError):
                        pass
            else:
                self._safe_fail(
                    claim,
                    "canceled",
                    "orchestration user canceled the run",
                    RunStatus.CANCELED,
                )
            raise
        except LeaseConflict:
            # The authoritative attempt is already gone.  Its transcript remains
            # quarantined audit data, but it must not submit any further state.
            interrupt = getattr(self.executor, "interrupt", None)
            if callable(interrupt):
                interrupt(run.id)
        except WorkspaceConflictError as exc:
            self._safe_fail(claim, "workspace_conflict", str(exc))
        except NoEligibleModelError as exc:
            self._safe_fail(claim, "no_eligible_model", str(exc))
        except QualityBudgetExceeded as exc:
            self._safe_fail(claim, "budget_exceeded", str(exc))
        except (BudgetExceededError, RuntimeLimitError, RuntimeStateError) as exc:
            self._safe_fail(claim, "runtime_limit", str(exc))
        except Exception as exc:
            logger.exception("orchestration run %s failed", run.id)
            self._safe_fail(claim, type(exc).__name__, str(exc))
        finally:
            if (
                quality_reservation_id is not None
                and quality_reservation_token is not None
                and not quality_reservation_settled
            ):
                try:
                    self.quality_budgets.release(
                        quality_reservation_id,
                        fencing_token=quality_reservation_token,
                    )
                except (ConflictError, NotFoundError):
                    logger.warning(
                        "could not release quality budget reservation for run %s",
                        run.id,
                        exc_info=True,
                    )
            if wake_was_delivered:
                try:
                    self._complete_delivered_run_wakes(run.task_id, run.id)
                except Exception:
                    logger.exception(
                        "could not settle delivered wakes for run %s", run.id
                    )
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            try:
                self._runtime_for_task(run.task_id, rebuild=True)
            except Exception:
                logger.exception("could not refresh runtime projection for %s", run.id)

    def _complete_delivered_run_wakes(self, task_id: str, run_id: str) -> None:
        for wake in self.store.list_wakes(
            task_id=task_id, statuses=(WakeStatus.DELIVERED,), limit=1_000
        ):
            if wake.target_run_id == run_id:
                self.wakes.mark_completed(wake.id)
                self.handoff_metrics.increment(
                    "orchestration_wake_completed_total"
                )

    async def _heartbeat(self, claim: RunClaim) -> None:
        while True:
            await asyncio.sleep(20)
            self.store.heartbeat(
                claim.run.id,
                claim.lease.token,
                claim.lease.fencing_token,
                lease_seconds=60,
                command_id=f"heartbeat-{uuid.uuid4().hex}",
            )

    def _safe_fail(
        self,
        claim: RunClaim,
        kind: str,
        message: str,
        status: RunStatus = RunStatus.FAILED,
    ) -> None:
        try:
            self.store.fail_run(
                claim.run.id,
                claim.lease.token,
                claim.lease.fencing_token,
                error_kind=kind,
                error_message=message,
                status=status,
                command_id=f"safe-fail-{uuid.uuid4().hex}",
            )
        except (LeaseConflict, ConflictError):
            pass

    # -- hierarchy -------------------------------------------------------
    def _spawn_child(self, payload: dict[str, Any]) -> Mapping[str, Any]:
        # The in-process lock avoids spending/reserving the same logical child
        # budget twice. The durable task idempotency key remains the cross-process
        # authority if multiple service instances race.
        with self._runtime_lock:
            return self._spawn_child_locked(payload)

    def _spawn_child_locked(self, payload: dict[str, Any]) -> Mapping[str, Any]:
        parent = self.store.get_task(str(payload["task_id"]))
        parent_run_id = str(payload.get("run_id") or "")
        try:
            parent_run = self.store.get_run(parent_run_id)
        except NotFoundError:
            return {"ok": False, "error": "parent run not found"}
        if parent_run.task_id != parent.id:
            return {"ok": False, "error": "parent run does not own this task"}
        if parent_run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
            return {"ok": False, "error": "parent run is not active"}
        try:
            self.store.assert_run_lease(
                parent_run.id,
                str(payload.get("lease_token") or ""),
                int(payload.get("fencing_token", -1)),
            )
        except (LeaseConflict, TypeError, ValueError):
            return {"ok": False, "error": "parent run lease/fence is no longer current"}
        requested_node_id = str(payload.get("node_id") or parent_run.node_id)
        if requested_node_id != parent_run.node_id:
            return {"ok": False, "error": "parent run does not own this plan node"}
        role = str(payload["role"]).strip().lower()
        raw_brief = payload.get("brief")
        structured = isinstance(raw_brief, Mapping)
        if (
            not structured
            and self.handoff_settings.structured_handoff_required_for_new_tasks
        ):
            return {
                "ok": False,
                "error": "structured handoff is required; use delegate_task with a complete Brief",
            }
        if not structured and not self.handoff_settings.legacy_spawn_agent_enabled:
            return {
                "ok": False,
                "error": "legacy spawn_agent is disabled; use delegate_task with a Brief",
            }
        brief_draft = (
            TaskBriefDraft.from_mapping(raw_brief)
            if structured
            else None
        )
        objective = str(
            brief_draft.objective
            if brief_draft is not None
            else payload.get("objective") or ""
        ).strip()
        if not role or not objective:
            return {"ok": False, "error": "child role and objective are required"}
        supplied_keys = [
            str(value).strip()
            for value in (payload.get("operation_id"), payload.get("child_key"))
            if value is not None
        ]
        if any(not value for value in supplied_keys):
            return {"ok": False, "error": "operation_id/child_key cannot be empty"}
        if len(set(supplied_keys)) > 1:
            return {
                "ok": False,
                "error": "operation_id and child_key must match when both are supplied",
            }
        operation_id = supplied_keys[0] if supplied_keys else None
        try:
            graph = self.store.get_plan(parent_run.plan_id)
            parent_node = next(item for item in graph.nodes if item.id == parent_run.node_id)
            parent_profile = self._profile_for_node(parent_node)
            allowed = {
                item.value for item in parent_profile.allowed_child_roles
            } & {
                item.value
                for item in parent_profile.communication_policy.allowed_child_roles
            }
            if not parent_profile.communication_policy.can_delegate:
                allowed = set()
            if role not in allowed:
                return {"ok": False, "error": f"child role is not allowed: {role}"}

            owner = {
                "parent_task_id": parent.id,
                "parent_plan_id": parent_run.plan_id,
                "parent_node_id": parent_run.node_id,
            }
            # An explicit operation id names the logical side effect. Without one,
            # preserve backwards compatibility with spawn_agent(role, task) while
            # deriving identity only from stable plan data -- never a run attempt id.
            logical_operation = (
                {"operation_id": operation_id}
                if operation_id is not None
                else {"role": role, "objective": objective}
            )
            identity_json = json.dumps(
                {**owner, **logical_operation},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            key = "child:" + hashlib.sha256(identity_json.encode("utf-8")).hexdigest()
            request_json = json.dumps(
                {
                    **owner,
                    "operation_id": operation_id,
                    "role": role,
                    "objective": objective,
                    "brief": brief_draft.to_dict() if brief_draft else None,
                    "context_refs": list(payload.get("context_refs") or ()),
                    "blocked_by_task_ids": list(
                        payload.get("blocked_by_task_ids") or ()
                    ),
                    "priority": int(payload.get("priority", 0)),
                    "runtime_preset_id": payload.get("runtime_preset_id"),
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            request_hash = hashlib.sha256(request_json.encode("utf-8")).hexdigest()
            existing = next(
                (
                    task
                    for task in self._all_tasks()
                    if task.parent_task_id == parent.id
                    and str((task.input.get("_runtime") or {}).get("spawn_key")) == key
                ),
                None,
            )
            if existing is not None:
                return self._replay_child_delegation(
                    parent, parent_run, existing, key, request_hash
                )
            owned = [
                task
                for task in self._all_tasks()
                if task.parent_task_id == parent.id
                and task.parent_node_id == parent_run.node_id
                and (
                    str((task.input.get("_runtime") or {}).get("parent_plan_id"))
                    == parent_run.plan_id
                    # Compatibility for children created before logical ownership
                    # metadata existed: retain their original run-scoped limit.
                    or str((task.input.get("_runtime") or {}).get("parent_run_id"))
                    == parent_run.id
                )
            ]
            if len(owned) >= parent_profile.max_children:
                return {
                    "ok": False,
                    "error": f"profile child limit reached ({parent_profile.max_children})",
                }
            runtime = self._runtime_for_task(parent.id, rebuild=True)
            parent_runtime_id = self._run_runtime_id(parent_run.id)
            runtime_parent = runtime.get(parent_runtime_id)
            remaining = runtime_parent.remaining_budget
            slots = max(1, parent_profile.max_children - len(owned))
            minimum_model_calls = 4 if parent.domain is TaskDomain.CODE else 2

            def settlement_headroom(value: int) -> int:
                # Runtime usage is durably charged after the Agent returns, while
                # spawn_agent reserves a child immediately. Keep a positive quarter
                # for the in-flight parent so max_children=1 cannot reserve 100% and
                # make its own model/tool/token/time accounting unrecoverable.
                return 0 if value <= 0 else max(1, value // 4)

            def allocate(value: int, minimum: int = 1) -> int:
                available = max(0, value - settlement_headroom(value))
                if available == 0:
                    return 0
                return min(available, max(minimum, available // slots))

            if self.enforce_runtime_budgets:
                child_budget = RuntimeBudget(
                    allocate(remaining.model_calls, minimum_model_calls),
                    allocate(remaining.tool_calls),
                    allocate(remaining.tokens),
                    allocate(remaining.wall_seconds),
                )
                if child_budget.model_calls < minimum_model_calls:
                    raise BudgetExceededError(
                        "parent has insufficient model-call budget for a child lifecycle "
                        "after reserving settlement headroom"
                    )
                if child_budget.wall_seconds == 0:
                    raise BudgetExceededError("parent has no executable budget for a child")
            else:
                child_budget = UNLIMITED_RUNTIME_BUDGET
            child_profile = self.catalog.resolve_profile(role)
            requested_permissions = self._profile_permissions(parent, child_profile)
            preflight = runtime.spawn_child(
                parent_runtime_id,
                RuntimeSpec(
                    runtime_id=f"preflight:{key[-32:]}",
                    profile_id=child_profile.profile_id,
                    profile_version=child_profile.version,
                    profile_content_hash=child_profile.content_hash,
                    task=objective,
                    budget=child_budget,
                    permissions=requested_permissions,
                    parent_id=parent_runtime_id,
                    metadata={"preflight": True},
                    kind=RuntimeKind.TASK,
                ),
            )
            runtime_meta = {
                "spawn_key": key,
                "spawn_request_hash": request_hash,
                "operation_id": operation_id,
                "parent_run_id": parent_run.id,
                "parent_plan_id": parent_run.plan_id,
                "parent_node_id": parent_run.node_id,
                "parent_runtime_id": parent_runtime_id,
                "effective_permissions": preflight.effective_permissions.as_dict(),
                "requested_permissions": requested_permissions.as_dict(),
                "denied_escalations": list(preflight.denied_escalations),
            }
            # Writable delegated tasks compose isolated staging layers. A read-only
            # tree can safely share the parent's formal source because every child
            # inherits the same read-only policy and runtime sandbox.
            parent_read_only = bool(parent.policy.get("read_only", False))
            parent_candidate = self._ensure_task_snapshot(parent)
            child_workspace = (
                str(parent.workspace)
                if parent_read_only and parent.workspace
                else str(parent_candidate.candidate)
                if parent_candidate is not None
                else None
            )
            runtime_meta["workspace_scope"] = (
                "parent_read_only_source"
                if parent_read_only and child_workspace
                else "parent_task_candidate"
                if child_workspace
                else "none"
            )
            if parent_candidate is not None:
                runtime_meta["parent_workspace_snapshot_id"] = (
                    parent_candidate.snapshot_id
                )
            if brief_draft is None:
                legacy_criteria = tuple(
                    {
                        "id": f"AC-{index:02d}",
                        "text": item,
                        "verification": "legacy",
                        "required": True,
                    }
                    for index, item in enumerate(parent.acceptance_criteria, 1)
                ) or (
                    {
                        "id": "AC-LEGACY-01",
                        "text": "Complete the bounded child assignment",
                        "verification": "parent_review",
                        "required": True,
                    },
                )
                brief_draft = TaskBriefDraft(
                    title=f"{role.title()} child: {objective[:80]}",
                    objective=objective,
                    background="Created through the legacy spawn_agent compatibility wrapper.",
                    scope={
                        "whole_task": True,
                        "reason": "Legacy spawn_agent supplied only a bounded task string.",
                    },
                    instructions=(objective,),
                    constraints=parent.constraints,
                    acceptance_criteria=legacy_criteria,
                    deliverables=(
                        {
                            "id": "DEL-LEGACY-RESULT",
                            "kind": "other",
                            "title": "Structured legacy child result",
                            "required": True,
                        },
                    ),
                    result_contract={"schema_id": "legacy_result_v1"},
                )
            policy = child_profile.communication_policy
            prepared_refs: list[ContextRefDraft] = []
            for raw_ref in payload.get("context_refs") or ():
                if not isinstance(raw_ref, Mapping):
                    raise ValueError("each context reference must be an object")
                context_ref = ContextRefDraft.from_mapping(raw_ref)
                if context_ref.ref_type in {
                    ContextRefType.FILE,
                    ContextRefType.FILE_RANGE,
                    ContextRefType.GIT_DIFF,
                }:
                    if not child_workspace:
                        raise ValueError("file context references require a child workspace")
                    context_ref = self.context_resolver.prepare_file_ref(
                        child_workspace, context_ref
                    )
                prepared_refs.append(context_ref)
            allowed_context_types = tuple(
                item
                for item in policy.allowed_context_ref_types
                if item in parent_profile.communication_policy.allowed_context_ref_types
            )
            context_policy = ContextPolicy(
                max_initial_context_tokens=min(
                    policy.max_initial_context_tokens,
                    parent_profile.communication_policy.max_initial_context_tokens,
                    self.handoff_settings.default_context_token_budget,
                ),
                max_context_refs=min(
                    policy.max_context_refs,
                    parent_profile.communication_policy.max_context_refs,
                    self.handoff_settings.max_context_refs,
                ),
                max_inline_bytes_per_ref=min(
                    policy.max_inline_bytes_per_ref,
                    parent_profile.communication_policy.max_inline_bytes_per_ref,
                    self.handoff_settings.max_inline_bytes_per_ref,
                ),
                max_inline_bytes_total=min(
                    policy.max_inline_bytes_total,
                    parent_profile.communication_policy.max_inline_bytes_total,
                    self.handoff_settings.max_inline_bytes_total,
                ),
                allowed_context_ref_types=allowed_context_types,
                allow_full_transcript_reference=(
                    policy.allow_full_transcript_reference
                    and parent_profile.communication_policy.allow_full_transcript_reference
                ),
                network=bool(parent.policy.get("network", False)),
                context_read_audit_enabled=self.handoff_settings.context_read_audit_enabled,
            )
            context_refs = ContextManifestBuilder(context_policy).normalize(
                prepared_refs
            )
            blocked_by = tuple(
                dict.fromkeys(
                    str(item).strip()
                    for item in payload.get("blocked_by_task_ids") or ()
                    if str(item).strip()
                )
            )
            root_task_id = self._root_task_id(parent.id)
            tree_ids = {item.id for item in self.store.list_task_tree(root_task_id)}
            if any(item not in tree_ids for item in blocked_by):
                raise ValueError("blockers must belong to the same orchestration tree")
            brief_draft.validate(
                required_fields=policy.required_brief_fields,
                informational=False,
            )
            child_request = {
                "objective": objective,
                "domain": parent.domain.value,
                "workspace": child_workspace,
                "acceptance_criteria": [
                    str(item.get("text") or "")
                    for item in brief_draft.acceptance_criteria
                ],
                "constraints": list(brief_draft.constraints),
                "profile_id": role,
                "read_only": bool(parent.policy.get("read_only", False)),
            }
            assessment = self._assessment(
                child_request,
                parent.domain,
                bool(brief_draft.acceptance_criteria),
                workspace=child_workspace,
            )
            requested_preset_id = str(
                payload.get("runtime_preset_id") or ""
            ).strip()
            child_preset = (
                runtime_preset(requested_preset_id)
                if requested_preset_id
                else None
            )
            if child_preset and parent.domain.value not in child_preset.domains:
                raise ValueError(
                    f"runtime preset {child_preset.preset_id} does not support "
                    f"domain {parent.domain.value}"
                )
            child_preset_snapshot = (
                child_preset.to_dict() if child_preset is not None else None
            )
            child_policy = {
                **assessment.as_dict()["policy"],
                "assessment": assessment.as_dict(),
                "profile_id": role,
                "model_policy_id": child_profile.model_policy,
                "require_review": False,
                "require_tests": False,
                "read_only": bool(parent.policy.get("read_only", False)),
                "network": bool(parent.policy.get("network", False)),
                "external_writes": bool(parent.policy.get("external_writes", False)),
                "structured_handoff": bool(
                    self.handoff_settings.structured_handoff_enabled
                ),
                "legacy_delegation": not structured,
                "runtime_preset_id": (
                    child_preset.preset_id if child_preset else None
                ),
                "runtime_preset_version": (
                    child_preset.version if child_preset else None
                ),
                "runtime_preset_hash": (
                    child_preset_snapshot["content_hash"]
                    if child_preset_snapshot
                    else None
                ),
                "runtime_preset_snapshot": child_preset_snapshot,
            }
            result = self.store.create_delegated_task(
                TaskSpec(
                    idempotency_key=key,
                    title=brief_draft.title,
                    objective=brief_draft.objective,
                    domain=parent.domain,
                    workspace=child_workspace,
                    constraints=brief_draft.constraints,
                    acceptance_criteria=tuple(
                        str(item.get("text") or "")
                        for item in brief_draft.acceptance_criteria
                    ),
                    complexity_score=assessment.score,
                    complexity_level=ComplexityLevel(assessment.level.value),
                    risk_tier=RiskTier(assessment.risk.value),
                    budget=child_budget.as_dict(),
                    policy=child_policy,
                    input={
                        "_runtime": runtime_meta,
                        **(
                            {"runtime_preset_id": child_preset.preset_id}
                            if child_preset
                            else {}
                        ),
                    },
                    priority=int(payload.get("priority", 0)),
                    max_parallel_runs=parent.max_parallel_runs,
                    parent_task_id=parent.id,
                    parent_node_id=parent_run.node_id,
                ),
                parent_run_id=parent_run.id,
                lease_token=str(payload.get("lease_token") or ""),
                fencing_token=int(payload.get("fencing_token", -1)),
                brief=brief_draft,
                context_refs=context_refs,
                blocked_by_task_ids=blocked_by,
                command_id=f"delegate:{key}",
            )
            child = result["task"]
            detail = self._task_summary(child)
            if not structured:
                self.handoff_metrics.increment(
                    "orchestration_legacy_delegation_total"
                )
            else:
                self.handoff_metrics.increment(
                    "orchestration_delegations_total"
                )
            self._record_child_delegation(
                parent,
                parent_run,
                child,
                key,
                origin_parent_run_id=parent_run.id,
                replayed=False,
            )
        except IdempotencyConflict:
            # Another service may have won the same stable child key after our
            # read. Only accept its result after verifying the complete request.
            self._runtime_for_task(parent.id, rebuild=True)
            try:
                existing = self.store.get_task_by_idempotency_key(key)
            except NotFoundError:
                return {"ok": False, "error": "child delegation conflicted"}
            return self._replay_child_delegation(
                parent, parent_run, existing, key, request_hash
            )
        except (BudgetExceededError, RuntimeLimitError, RuntimeStateError, ValueError) as exc:
            self._runtime_for_task(parent.id, rebuild=True)
            return {"ok": False, "error": str(exc)}
        return {
            "ok": True,
            "task_id": detail["id"],
            "status": detail["status"],
            "brief_id": child.active_brief_id,
            "brief_revision": 1,
            "links": {
                "task": f"/v1/orchestration/tasks/{child.id}",
                "brief": f"/v1/orchestration/tasks/{child.id}/briefs/1",
            },
        }

    def _replay_child_delegation(
        self,
        parent: TaskRecord,
        caller_run: RunRecord,
        child: TaskRecord,
        spawn_key: str,
        request_hash: str,
    ) -> Mapping[str, Any]:
        meta = dict(child.input.get("_runtime") or {})
        if (
            child.parent_task_id != parent.id
            or child.parent_node_id != caller_run.node_id
            or str(meta.get("parent_plan_id") or "") != caller_run.plan_id
            or str(meta.get("spawn_key") or "") != spawn_key
        ):
            return {"ok": False, "error": "child operation is owned by another Agent"}
        if str(meta.get("spawn_request_hash") or "") != request_hash:
            return {
                "ok": False,
                "error": "child operation key was reused with different input",
            }
        # Child creation spans the durable DRAFT insert, assessment evidence, and
        # submit transition. A process may stop between those transactions; replaying
        # the stable delegation key must finish that protocol rather than leave a
        # DRAFT child that the parent waits on forever.
        assessment = dict(child.policy.get("assessment") or {})
        if assessment:
            self.store.add_evidence(
                child.id,
                kind=EvidenceKind.DECISION,
                payload={"title": "Complexity assessment", **assessment},
                created_by="orchestration-policy",
                command_id=_command("assessment", child.id),
            )
        if child.status is TaskStatus.DRAFT:
            child = self.submit_task(child.id)
        origin_run_id = str(meta.get("parent_run_id") or "")
        self._record_child_delegation(
            parent,
            caller_run,
            child,
            spawn_key,
            origin_parent_run_id=origin_run_id,
            replayed=True,
        )
        return {
            "ok": True,
            "task_id": child.id,
            "status": child.status.value,
            "replayed": True,
            "parent_run_id": origin_run_id,
            "delegated_by_run_id": caller_run.id,
        }

    def _record_child_delegation(
        self,
        parent: TaskRecord,
        caller_run: RunRecord,
        child: TaskRecord,
        spawn_key: str,
        *,
        origin_parent_run_id: str,
        replayed: bool,
    ) -> None:
        self.store.add_evidence(
            parent.id,
            kind=EvidenceKind.DECISION,
            payload={
                "title": "Child delegation replayed" if replayed else "Child delegated",
                "action": "child_delegation_replayed" if replayed else "child_delegated",
                "child_task_id": child.id,
                "spawn_key": spawn_key,
                "parent_plan_id": caller_run.plan_id,
                "parent_node_id": caller_run.node_id,
                "origin_parent_run_id": origin_parent_run_id,
                "caller_parent_run_id": caller_run.id,
            },
            created_by="orchestrator",
            plan_id=caller_run.plan_id,
            node_id=caller_run.node_id,
            run_id=caller_run.id,
            command_id=_command(
                "child-delegation",
                child.id,
                caller_run.id,
                "replay" if replayed else "create",
            ),
        )

    def _lookup_child(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        task_id = str(payload.get("task_id") or "")
        try:
            task = self.store.get_task(task_id)
        except NotFoundError:
            return {"ok": False, "error": "child task not found"}
        meta = dict(task.input.get("_runtime") or {})
        caller_run_id = str(payload.get("parent_run_id") or "")
        try:
            caller_run = self.store.get_run(caller_run_id)
        except NotFoundError:
            return {"ok": False, "error": "parent run not found"}
        if caller_run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
            return {"ok": False, "error": "parent run is not active"}
        try:
            self.store.assert_run_lease(
                caller_run.id,
                str(payload.get("lease_token") or ""),
                int(payload.get("fencing_token", -1)),
            )
        except (LeaseConflict, TypeError, ValueError):
            return {"ok": False, "error": "parent run lease/fence is no longer current"}
        parent_task_id = str(payload.get("parent_task_id") or "")
        exact_attempt = str(meta.get("parent_run_id") or "") == caller_run.id
        logical_owner = (
            bool(meta.get("spawn_key"))
            and str(meta.get("parent_plan_id") or "") == caller_run.plan_id
            and task.parent_node_id == caller_run.node_id
        )
        if (
            task.parent_task_id != parent_task_id
            or caller_run.task_id != parent_task_id
            or not (exact_attempt or logical_owner)
        ):
            return {"ok": False, "error": "child task is not owned by this parent run"}
        result = (
            dict((task.output or {}).get("result") or {})
            or self._task_result_envelope(task)
            if task.status in _TERMINAL_TASKS
            else None
        )
        if result:
            supplied_hash = str(result.get("result_hash") or "")
            hashed_payload = dict(result)
            hashed_payload.pop("result_hash", None)
            persisted_hash = str((task.output or {}).get("result_hash") or supplied_hash)
            if (
                not supplied_hash
                or _canonical_hash(hashed_payload) != supplied_hash
                or persisted_hash != supplied_hash
            ):
                return {
                    "ok": False,
                    "error": "child result integrity verification failed",
                }
        if result and result.get("result_hash"):
            self.store.add_evidence(
                caller_run.task_id,
                kind=EvidenceKind.CHECKPOINT,
                payload={
                    "title": "Child result consumed",
                    "action": "child_result_consumed",
                    "child_task_id": task.id,
                    "result_hash": result["result_hash"],
                },
                created_by="orchestration-runtime",
                plan_id=caller_run.plan_id,
                node_id=caller_run.node_id,
                run_id=caller_run.id,
                command_id=_command(
                    "child-result-consumed",
                    caller_run.id,
                    task.id,
                    result["result_hash"],
                ),
            )
        return {
            "ok": True,
            "task_id": task.id,
            "parent_task_id": task.parent_task_id,
            "status": task.status.value,
            "stage": task.current_stage.value,
            "output": dict(task.output or {}),
            "result": result,
            "parent_run_id": meta.get("parent_run_id"),
        }

    def _cancel_child(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        owned = self._lookup_child(payload)
        if not owned.get("ok"):
            return owned
        task_id = str(payload.get("task_id") or "")
        try:
            task = self.cancel_task(task_id)
        except NotFoundError:
            return {"ok": False, "error": "child task not found"}
        return {"ok": True, "task_id": task.id, "status": task.status.value}

    async def _gate_opened(self, _gate_id: str) -> None:
        self.wake()

    # -- model catalog ---------------------------------------------------
    def model_candidates(self) -> tuple[ModelCandidate, ...]:
        settings = self.manager.get_settings()
        selectable = set(settings.get("models") or ())
        labels = settings.get("model_labels") or {}
        contexts = settings.get("model_context_windows") or {}
        result: list[ModelCandidate] = []
        all_ids = list(dict.fromkeys([*MATRIX, *selectable]))
        for index, model_id in enumerate(all_ids):
            entry = MATRIX.get(model_id)
            provider = provider_for(model_id)
            configured = bool(self.manager._provider_configured(provider))
            try:
                caps = entry.caps if entry else self.manager.provider.capabilities(model_id)
                capability_names = frozenset(
                    name
                    for name in ("tools", "vision", "pdf", "parallel_tool_calls", "streaming")
                    if bool(getattr(caps, name, False))
                )
            except Exception:
                capability_names = frozenset()
            quality = max(50, 100 - index // 3) if entry else 50
            result.append(
                ModelCandidate(
                    model_id=model_id,
                    quality=quality,
                    capabilities=capability_names,
                    provider=provider,
                    context_window=(entry.context_window if entry else contexts.get(model_id)),
                    latency_rank=index,
                    configured=configured,
                    available=configured,
                    verified=entry is not None,
                    catalog_revision="openworker-matrix",
                )
            )
        existing = {item.model_id for item in result}
        result.extend(
            item
            for item in self.subscription_runtimes.model_candidates()
            if item.model_id not in existing
        )
        return tuple(result)

    def model_catalog(self) -> list[dict[str, Any]]:
        labels = self.manager.get_settings().get("model_labels") or {}
        selectable = set(self.manager.get_settings().get("models") or ())
        runtime_specs = {
            item.runtime_id: item for item in self.subscription_runtimes.specs
        }
        runtime_health = {
            item.runtime_id: self.subscription_runtimes.health(item.runtime_id)
            for item in self.subscription_runtimes.specs
        }
        return [
            {
                "id": item.model_id,
                "label": labels.get(
                    item.model_id,
                    runtime_specs[item.model_id].display_name
                    if item.model_id in runtime_specs
                    else item.model_id,
                ),
                "provider": item.provider,
                "source": (
                    "subscription-runtime"
                    if item.model_id in runtime_specs
                    else "curated"
                    if item.model_id in MATRIX
                    else "custom"
                ),
                "quality": item.quality,
                "configured": item.configured,
                "in_composer_picker": item.model_id in selectable,
                "availability": (
                    "configured"
                    if item.available
                    else "blocked_by_policy"
                    if item.model_id in runtime_health
                    and runtime_health[item.model_id].authenticated
                    and not runtime_health[item.model_id].policy_eligible
                    else "unavailable"
                    if item.configured
                    else "unconfigured"
                ),
                "availability_reason": (
                    runtime_health[item.model_id].reason
                    if item.model_id in runtime_health
                    else ""
                ),
                "verified": item.verified,
                "capabilities": sorted(item.capabilities),
                "context_window": item.context_window,
                "input_microusd_per_million": item.input_microusd_per_million,
                "output_microusd_per_million": item.output_microusd_per_million,
                "latency_rank": item.latency_rank,
                "catalog_revision": item.catalog_revision,
                "runtime": (
                    {
                        "protocol": runtime_specs[item.model_id].protocol,
                        "model": runtime_specs[item.model_id].cli_model,
                        "reasoning_effort": runtime_specs[
                            item.model_id
                        ].reasoning_effort,
                        "local_owner_only": runtime_specs[
                            item.model_id
                        ].local_owner_only,
                        "interactive_only": runtime_specs[
                            item.model_id
                        ].interactive_only,
                    }
                    if item.model_id in runtime_specs
                    else None
                ),
            }
            for item in self.model_candidates()
        ]

    def subscription_runtime_catalog(
        self, *, refresh: bool = False
    ) -> list[dict[str, Any]]:
        """Return sanitized local CLI/auth/capability health without model calls."""

        return self.subscription_runtimes.health_snapshot(refresh=refresh)

    def interactive_subscription_runtime_catalog(
        self, *, refresh: bool = False
    ) -> list[dict[str, Any]]:
        """Return sanitized subscription choices for foreground Agent sessions."""

        return self.subscription_runtimes.interactive_catalog(refresh=refresh)

    def runtime_preset_catalog(
        self, *, refresh: bool = False
    ) -> list[dict[str, Any]]:
        """Return built-in role bindings plus sanitized runtime readiness.

        Discovery never invokes a model. Runtime health is the same cached local CLI
        probe exposed by ``subscription_runtime_catalog``; only ``refresh=true`` asks
        the registry to repeat those probes.
        """

        health_by_runtime = {
            str(item.get("runtime_id") or ""): dict(item.get("health") or {})
            for item in self.subscription_runtimes.health_snapshot(refresh=refresh)
        }
        result: list[dict[str, Any]] = []
        for preset in runtime_presets():
            snapshot = preset.to_dict()
            template_roles = {
                AgentRole.ORCHESTRATOR,
                AgentRole.EXPLORER,
                AgentRole.PLANNER,
                AgentRole.WORKER,
                AgentRole.REVIEWER,
                AgentRole.TESTER,
                AgentRole.EVALUATOR,
            }

            def access_for(role: AgentRole) -> str:
                if role is AgentRole.WORKER:
                    return "candidate_workspace"
                if role is AgentRole.INTEGRATOR:
                    return "integration_workspace"
                if role is AgentRole.TESTER:
                    return "disposable_test_snapshot"
                return "read_only_snapshot"

            roles = [
                {
                    "role": role.value,
                    "runtime_id": model,
                    "access": access_for(role),
                    "fresh_session": True,
                    "required": role in template_roles,
                }
                for role, model in sorted(
                    preset.role_models.items(), key=lambda item: item[0].value
                )
            ]
            required_runtime_ids = sorted(
                {
                    model
                    for role, model in preset.role_models.items()
                    if role in template_roles
                }
            )
            unavailable_runtime_ids: list[str] = []
            reasons: list[str] = []
            for runtime_id in required_runtime_ids:
                health = health_by_runtime.get(runtime_id)
                if health and bool(health.get("available")) and bool(
                    health.get("policy_eligible")
                ):
                    continue
                unavailable_runtime_ids.append(runtime_id)
                reason = str((health or {}).get("reason") or "runtime is not registered")
                reasons.append(f"{runtime_id}: {reason}")
            available = not unavailable_runtime_ids
            result.append(
                {
                    **snapshot,
                    "id": preset.preset_id,
                    "name": preset.display_name,
                    "builtin": True,
                    "is_default": bool(preset.default_for_domains),
                    "roles": roles,
                    "required_runtime_ids": required_runtime_ids,
                    "available": available,
                    "availability": "available" if available else "unavailable",
                    "unavailable_runtime_ids": unavailable_runtime_ids,
                    "availability_reason": "; ".join(reasons),
                }
            )
        return result

    def simulate_routing(self, policy_spec: Mapping[str, Any], request: Mapping[str, Any]):
        from .catalogs import _policy_from_spec

        policy = _policy_from_spec(policy_spec, version=1)
        facts = RoutingRequest(
            purpose=str(request.get("purpose") or "simulation"),
            required_capabilities=frozenset(request.get("required_capabilities", ())),
            input_tokens=int(request.get("input_tokens", 0)),
            reserved_output_tokens=int(request.get("reserved_output_tokens", 4096)),
            minimum_context=int(request.get("minimum_context", 0)),
            max_cost_microusd=request.get("max_cost_microusd"),
            requested_model=request.get("requested_model"),
            preferred_models=tuple(request.get("preferred_models", ())),
            allowed_providers=tuple(request.get("allowed_providers", ())),
            excluded_models=tuple(request.get("excluded_models", ())),
            correlation=dict(request.get("correlation") or {}),
        )
        return ModelRouter(self.model_candidates(), policy=policy).route(facts).audit_record()

    # -- structured handoff control plane -------------------------------
    def validate_task_brief(
        self, task_id: str, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        profile = self.catalog.resolve_profile(
            str(task.policy.get("profile_id") or "worker")
        )
        draft = TaskBriefDraft.from_mapping(value)
        issues = draft.validation_issues(
            required_fields=profile.communication_policy.required_brief_fields
        )
        return {
            "valid": not issues,
            "errors": list(issues),
            "content_hash": draft.content_hash,
        }

    def list_task_briefs(self, task_id: str) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.store.list_briefs(task_id)]

    def get_task_brief(self, task_id: str, revision: int) -> dict[str, Any]:
        return self.store.get_brief(task_id, revision).to_dict()

    def create_task_brief_draft(
        self,
        task_id: str,
        value: Mapping[str, Any],
        *,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        if task.status in _TERMINAL_TASKS or task.status is TaskStatus.CANCELING:
            raise ConflictError("a terminal task cannot receive a new Brief revision")
        try:
            current = self.store.get_active_brief(task_id)
        except NotFoundError:
            current = None
        record = self.store.create_brief_draft(
            task_id,
            TaskBriefDraft.from_mapping(value),
            copy_context_from_brief_id=(
                str(
                    value.get("copy_context_from_brief_id")
                    or (current.id if current is not None else "")
                )
                or None
            ),
            command_id=(
                command_id
                or _command("brief-create", task_id, _canonical_hash(dict(value)))
            ),
        )
        return record.to_dict()

    def update_task_brief_draft(
        self,
        task_id: str,
        revision: int,
        value: Mapping[str, Any],
        *,
        expected_hash: str,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        normalized_hash = str(expected_hash).strip().strip('"')
        record = self.store.update_brief_draft(
            task_id,
            revision,
            TaskBriefDraft.from_mapping(value),
            expected_hash=normalized_hash,
            command_id=(
                command_id
                or _command("brief-update", task_id, revision, normalized_hash)
            ),
        )
        return record.to_dict()

    def publish_task_brief(
        self,
        task_id: str,
        revision: int,
        *,
        expected_hash: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        draft = self.store.get_brief(task_id, revision)
        if expected_hash is not None and draft.content_hash != str(expected_hash):
            raise ConflictError("task brief draft changed since it was loaded")
        profile = self.catalog.resolve_profile(
            str(task.policy.get("profile_id") or "worker")
        )
        TaskBriefDraft(
            title=draft.title,
            objective=draft.objective,
            background=draft.background,
            scope=draft.scope,
            instructions=draft.instructions,
            constraints=draft.constraints,
            non_goals=draft.non_goals,
            acceptance_criteria=draft.acceptance_criteria,
            deliverables=draft.deliverables,
            result_contract=draft.result_contract,
        ).validate(
            required_fields=profile.communication_policy.required_brief_fields
        )
        record = self.store.publish_brief(
            task_id,
            revision,
            command_id=(
                command_id
                or _command("brief-publish", task_id, revision, draft.content_hash)
            ),
        )
        self.handoff_metrics.increment("orchestration_brief_published_total")
        fresh = self.store.get_task(task_id)
        if fresh.status in {
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
            TaskStatus.WAITING_HUMAN,
            TaskStatus.WAITING_CHILD,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
        }:
            self.wakes.enqueue_wake(
                task_id,
                WakeReason.BRIEF_REVISION_AVAILABLE,
                payload={
                    "brief_id": record.id,
                    "brief_revision": record.revision,
                    "content_hash": record.content_hash,
                },
                dedupe_key=f"{task_id}:brief_revision:{record.id}",
            )
            self.wake()
        return record.to_dict()

    def list_task_context_refs(
        self,
        task_id: str,
        *,
        brief_id: Optional[str] = None,
        requirement: Optional[str] = None,
        ref_type: Optional[str] = None,
        limit: int = 1_000,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        rows = self.store.list_context_refs(
            task_id,
            brief_id=brief_id,
            requirement=(ContextRequirement(requirement) if requirement else None),
            ref_type=(ContextRefType(ref_type) if ref_type else None),
            limit=limit,
            offset=offset,
        )
        read_events: dict[str, list[Any]] = {}
        for event in self.store.list_events(task_id=task_id, limit=10_000):
            if event.event_type == "context_ref_read":
                read_events.setdefault(event.aggregate_id, []).append(event)
        return [
            {
                **item.to_dict(),
                "read_count": len(read_events.get(item.id, ())),
                "last_read_at": (
                    _iso(read_events[item.id][-1].created_at)
                    if read_events.get(item.id)
                    else None
                ),
                "last_read_by_run_id": (
                    read_events[item.id][-1].payload.get("run_id")
                    if read_events.get(item.id)
                    else None
                ),
            }
            for item in rows
        ]

    def add_task_context_ref(
        self,
        task_id: str,
        value: Mapping[str, Any],
        *,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        brief_id = str(value.get("brief_id") or "")
        if not brief_id:
            drafts = [
                item
                for item in self.store.list_briefs(task_id)
                if item.status is BriefStatus.DRAFT
            ]
            if not drafts:
                raise ConflictError("a draft Brief is required before adding context")
            brief_id = drafts[-1].id
        draft = ContextRefDraft.from_mapping(value.get("context_ref") or value)
        profile = self.catalog.resolve_profile(
            str(task.policy.get("profile_id") or "worker")
        )
        if draft.ref_type not in profile.communication_policy.allowed_context_ref_types:
            raise PermissionError(
                f"context ref type is not permitted: {draft.ref_type.value}"
            )
        if draft.ref_type in {
            ContextRefType.FILE,
            ContextRefType.FILE_RANGE,
            ContextRefType.GIT_DIFF,
        }:
            if not task.workspace:
                raise ValueError("file context references require a workspace")
            draft = self.context_resolver.prepare_file_ref(task.workspace, draft)
        existing = self.store.list_context_refs(task_id, brief_id=brief_id)
        if len(existing) >= min(
            self.handoff_settings.max_context_refs,
            profile.communication_policy.max_context_refs,
        ):
            raise ValueError("context reference limit reached")
        record = self.store.add_context_ref(
            task_id,
            brief_id,
            draft,
            command_id=(
                command_id
                or _command("context-add", task_id, brief_id, draft.to_dict())
            ),
        )
        return record.to_dict()

    def get_context_ref_metadata(self, ref_id: str) -> dict[str, Any]:
        return self.store.get_context_ref(ref_id).to_dict()

    def read_context_ref_content(
        self,
        ref_id: str,
        *,
        start_line: Optional[int] = None,
        end_line: Optional[int] = None,
    ) -> dict[str, Any]:
        ref = self.store.get_context_ref(ref_id)
        task = self.store.get_task(ref.task_id)
        result = self.context_resolver.read(
            ref.id,
            task_id=task.id,
            run_id=None,
            workspace=task.workspace,
            start_line=start_line,
            end_line=end_line,
        )
        self.handoff_metrics.increment("orchestration_context_reads_total")
        self.handoff_metrics.increment(
            "orchestration_context_bytes_read_total",
            int(result.get("byte_size") or 0),
        )
        return result

    def verify_context_ref(
        self, ref_id: str, *, command_id: Optional[str] = None
    ) -> dict[str, Any]:
        ref = self.store.get_context_ref(ref_id)
        task = self.store.get_task(ref.task_id)
        result = self.context_resolver.verify(ref, workspace=task.workspace)
        self.store.record_context_ref_verification(
            ref.id,
            run_id=None,
            result=result,
            command_id=(
                command_id
                or _command("context-verify", ref.id, _canonical_hash(result))
            ),
        )
        return {
            "id": ref.id,
            "task_id": task.id,
            **result,
        }

    def delegate_task(
        self, parent_task_id: str, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        run_id = str(value.get("run_id") or "")
        run = self.store.get_run(run_id)
        if run.task_id != parent_task_id:
            raise PermissionError("delegation run does not own the parent task")
        result = self._spawn_child(
            {
                **dict(value),
                "task_id": parent_task_id,
                "node_id": run.node_id,
            }
        )
        if not bool(result.get("ok")):
            message = str(result.get("error") or "delegation failed")
            if "not allowed" in message or "cannot delegate" in message:
                raise PermissionError(message)
            raise ConflictError(message)
        self.wake()
        return dict(result)

    def task_relations(self, task_id: str) -> list[dict[str, Any]]:
        return [
            handoff_jsonable(item) for item in self.relations.list_relations(task_id)
        ]

    def add_task_relation(
        self,
        task_id: str,
        value: Mapping[str, Any],
        *,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        relation_type = TaskRelationType(str(value.get("relation_type") or "related"))
        from_task_id = str(value.get("from_task_id") or task_id)
        to_task_id = str(value.get("to_task_id") or task_id)
        if task_id not in {from_task_id, to_task_id}:
            raise PermissionError(
                "a task-scoped relation endpoint must include that task"
            )
        record = self.relations.add_relation(
            from_task_id,
            to_task_id,
            relation_type,
            metadata=dict(value.get("metadata") or {}),
            created_by_task_id=None,
            created_by_run_id=None,
            command_id=command_id,
        )
        return handoff_jsonable(record)

    def remove_task_relation(
        self,
        task_id: str,
        relation_id: str,
        *,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        relation = next(
            (
                item
                for item in self.store.list_relations(task_id)
                if item.id == relation_id
            ),
            None,
        )
        if relation is None:
            raise NotFoundError(f"relation not found: {relation_id}")
        return handoff_jsonable(
            self.relations.remove_relation(
                relation_id, actor="local-user", command_id=command_id
            )
        )

    def replace_task_blockers(
        self,
        task_id: str,
        value: Mapping[str, Any],
        *,
        command_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        blocker_ids = tuple(
            dict.fromkeys(
                str(item).strip()
                for item in value.get("task_ids") or ()
                if str(item).strip()
            )
        )
        records = self.relations.replace_blockers(
            task_id,
            blocker_ids,
            reason=str(value.get("reason") or "Operator updated blockers"),
            owner=str(value.get("owner") or "local-user"),
            required_action=str(
                value.get("required_action") or "Complete all blocker tasks"
            ),
            command_id=command_id,
        )
        self.wake()
        return [handoff_jsonable(item) for item in records]

    def task_comments(
        self, task_id: str, *, after_sequence: int = 0
    ) -> dict[str, Any]:
        delta = self.communications.delta(
            task_id, after_sequence=after_sequence
        )
        return {
            **delta,
            "comments": [handoff_jsonable(item) for item in delta["comments"]],
        }

    def task_comment(self, task_id: str, comment_id: str) -> dict[str, Any]:
        comment = self.store.get_task_comment(comment_id)
        if comment.task_id != task_id:
            raise NotFoundError(f"comment not found: {comment_id}")
        return handoff_jsonable(comment)

    def post_operator_comment(
        self, task_id: str, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        body = str(value.get("body_markdown") or value.get("body") or "")
        metadata = dict(value.get("metadata") or {})
        metadata["mentions"] = list(
            dict.fromkeys(
                str(item).strip()
                for item in metadata.get("mentions", ())
                if str(item).strip()
            )
        )
        for target in metadata["mentions"]:
            if target.startswith("task:"):
                continue
            target_profile = self.catalog.resolve_profile(target)
            if not target_profile.communication_policy.can_mention_receive:
                raise PermissionError(
                    f"profile cannot receive mentions: {target_profile.profile_id}"
                )
        comment = self.communications.post_comment(
            task_id,
            body,
            author_type="operator",
            author_id="local-user",
            metadata=metadata,
            reply_to_comment_id=(
                str(value["reply_to_comment_id"])
                if value.get("reply_to_comment_id")
                else None
            ),
            wake_owner=True,
            command_id=str(
                value.get("command_id")
                or _command("comment", task_id, _canonical_hash(dict(value)))
            ),
        )
        self.handoff_metrics.increment("orchestration_comments_total")
        self.wake()
        return handoff_jsonable(comment)

    def task_work_products(self, task_id: str) -> list[dict[str, Any]]:
        products = self.work_products.list(task_id)
        latest_verification: dict[str, Mapping[str, Any]] = {}
        for event in self.store.list_events(task_id=task_id, limit=10_000):
            if event.event_type == "work_product_verified":
                latest_verification[event.aggregate_id] = event.payload
        return [
            {
                **handoff_jsonable(item),
                **(
                    {
                        "verification_status": str(
                            latest_verification[item.id].get(
                                "verification_status", item.verification_status
                            )
                        ),
                        "verification": dict(latest_verification[item.id]),
                    }
                    if item.id in latest_verification
                    else {}
                ),
            }
            for item in products
        ]

    def _result_question_payload(
        self, source_task_id: str, question_task: TaskRecord
    ) -> dict[str, Any]:
        metadata = dict(question_task.input.get("result_follow_up") or {})
        products = self.store.list_work_products(question_task.id, limit=1_000)
        answer_product = next(
            (
                item
                for item in reversed(products)
                if str(item.metadata.get("deliverable_id") or "") == "answer-1"
            ),
            None,
        )
        if answer_product is None:
            answer_product = next(
                (
                    item
                    for item in reversed(products)
                    if item.kind
                    not in {
                        WorkProductKind.PLAN,
                        WorkProductKind.REVIEW_REPORT,
                        WorkProductKind.TEST_RESULT,
                        WorkProductKind.EVALUATION,
                    }
                ),
                None,
            )
        stored_result = dict(question_task.output or {}).get("result")
        fallback_summary = (
            str(dict(stored_result).get("summary") or "")
            if isinstance(stored_result, Mapping)
            else ""
        )
        return {
            "id": question_task.id,
            "task_id": question_task.id,
            "source_task_id": source_task_id,
            "question": str(metadata.get("question") or question_task.objective),
            "status": question_task.status.value,
            "terminal_outcome": self._terminal_outcome(question_task).value,
            "stage": question_task.current_stage.value,
            "progress": self._task_summary(question_task)["progress"],
            "answer": (
                str(answer_product.summary)
                if answer_product is not None
                else fallback_summary or None
            ),
            "answer_work_product_id": (
                answer_product.id if answer_product is not None else None
            ),
            "answer_artifact_id": (
                answer_product.artifact_id if answer_product is not None else None
            ),
            "source_work_product_ids": list(
                metadata.get("source_work_product_ids") or ()
            ),
            "created_at": _iso(question_task.created_at),
            "updated_at": _iso(question_task.updated_at),
        }

    def result_questions(self, task_id: str) -> list[dict[str, Any]]:
        self.store.get_task(task_id)
        question_tasks: list[TaskRecord] = []
        seen: set[str] = set()
        for relation in self.store.list_relations(task_id):
            if (
                relation.from_task_id != task_id
                or relation.relation_type is not TaskRelationType.RELATED
                or str(relation.metadata.get("kind") or "") != "result_question"
                or relation.to_task_id in seen
            ):
                continue
            seen.add(relation.to_task_id)
            question_tasks.append(self.store.get_task(relation.to_task_id))
        question_tasks.sort(key=lambda item: (item.created_at, item.id), reverse=True)
        return [
            self._result_question_payload(task_id, item) for item in question_tasks
        ]

    def ask_result_question(
        self,
        task_id: str,
        question: str,
        *,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        source = self.store.get_task(task_id)
        if self._terminal_outcome(source) is not TaskStatus.COMPLETED:
            raise ConflictError(
                "result questions are available only after successful completion"
            )
        normalized_question = str(question).strip()
        if not normalized_question:
            raise ValueError("question is required")
        if len(normalized_question) > 4_000:
            raise ValueError("question must be at most 4000 characters")

        products = list(self.store.list_work_products(source.id, limit=1_000))
        if not products:
            raise ConflictError("the completed task has no result work products")
        try:
            brief = self.store.get_active_brief(source.id)
            deliverables = tuple(brief.deliverables)
        except NotFoundError:
            deliverables = ()
        declared_ids = {
            str(item.get("id") or "") for item in deliverables if item.get("id")
        }
        selected = [
            item
            for item in products
            if str(item.metadata.get("deliverable_id") or "") in declared_ids
        ]
        if not selected:
            selected = [
                item
                for item in products
                if str(item.metadata.get("node_kind") or "") == NodeKind.EXECUTE.value
                and item.kind
                not in {
                    WorkProductKind.PLAN,
                    WorkProductKind.REVIEW_REPORT,
                    WorkProductKind.TEST_RESULT,
                    WorkProductKind.EVALUATION,
                }
            ]
        if not selected:
            selected = [
                item
                for item in products
                if item.kind
                not in {
                    WorkProductKind.PLAN,
                    WorkProductKind.REVIEW_REPORT,
                    WorkProductKind.TEST_RESULT,
                    WorkProductKind.EVALUATION,
                }
            ]
        if not selected:
            selected = [products[-1]]

        max_refs = max(1, min(self.handoff_settings.max_context_refs, 20))
        selected = selected[-max_refs:]
        context_refs: list[dict[str, Any]] = []
        for product in selected:
            artifact_id = str(product.artifact_id or product.uri or "")
            content_hash = str(product.content_hash or "")
            mime_type = "application/json"
            if not artifact_id.startswith("sha256:"):
                artifact = self.blobs.put_json(
                    {
                        "schema_version": 1,
                        "source_task_id": source.id,
                        "work_product": handoff_jsonable(product),
                    }
                )
                artifact_id = artifact.uri
                content_hash = f"sha256:{artifact.sha256}"
                mime_type = artifact.mime_type
            context_refs.append(
                {
                    "requirement": "required",
                    "ref_type": ContextRefType.ARTIFACT.value,
                    "display_name": product.title,
                    "selection_reason": (
                        "Declared final result used to answer the operator's "
                        "follow-up question"
                    ),
                    "locator": {"artifact_id": artifact_id},
                    "delivery_mode": "on_demand",
                    "summary": f"Result work product {product.id}",
                    "mime_type": mime_type,
                    "content_hash": content_hash or artifact_id,
                    "provenance": {
                        "source_task_id": source.id,
                        "source_work_product_id": product.id,
                    },
                    "trust_level": "agent_generated",
                }
            )

        operation = str(command_id or uuid.uuid4().hex)
        idempotency_digest = hashlib.sha256(operation.encode("utf-8")).hexdigest()
        title_question = " ".join(normalized_question.split())
        follow_up = self.create_task(
            {
                "idempotency_key": (
                    f"result-question:{source.id}:{idempotency_digest}"
                ),
                "title": f"Follow-up: {title_question[:150]}",
                "objective": normalized_question,
                "domain": TaskDomain.KNOWLEDGE.value,
                "read_only": True,
                "profile_id": "worker",
                "model_policy_id": str(
                    source.policy.get("model_policy_id") or "quality-first"
                ),
                "acceptance_criteria": [
                    "The answer directly addresses the operator's question.",
                    "Every factual claim is grounded in the supplied result artifact.",
                ],
                "require_review": False,
                "require_tests": False,
                "complexity_factors": {
                    "scope": 0,
                    "uncertainty": 0,
                    "dependencies": 0,
                    "side_effects": 0,
                    "parallelism": 0,
                    "verification": 0,
                },
                "brief": {
                    "title": f"Result follow-up for {source.title}"[:200],
                    "objective": normalized_question,
                    "background": (
                        f"Answer a question about completed task {source.id}. "
                        "The source task remains immutable and is not restarted."
                    ),
                    "scope": {
                        "components": ["declared result artifacts"],
                        "source_task_id": source.id,
                    },
                    "instructions": [
                        "Read the required result artifact context before answering.",
                        "Answer only from the supplied result; clearly label any inference or missing information.",
                        "Preserve and cite file paths, line references, evidence identifiers, or artifact identifiers when relevant.",
                        "Return a concise standalone answer to the operator's question.",
                    ],
                    "constraints": [
                        "Read-only follow-up: do not modify the source task or workspace.",
                        "Do not claim evidence that is absent from the supplied result.",
                    ],
                    "non_goals": [
                        "Do not re-run or replace the completed source task."
                    ],
                    "acceptance_criteria": [
                        {
                            "id": "AC-01",
                            "text": "The answer directly addresses the question.",
                            "required": True,
                        },
                        {
                            "id": "AC-02",
                            "text": "Claims are traceable to the supplied result evidence.",
                            "required": True,
                        },
                    ],
                    "deliverables": [
                        {
                            "id": "answer-1",
                            "kind": WorkProductKind.ARTIFACT.value,
                            "title": "Answer",
                            "required": True,
                        }
                    ],
                    "result_contract": {
                        "schema_id": "openworker.result_question_answer.v1"
                    },
                },
                "context_refs": context_refs,
                "input": {
                    "result_follow_up": {
                        "source_task_id": source.id,
                        "question": normalized_question,
                        "source_work_product_ids": [item.id for item in selected],
                    }
                },
                "publish_brief": True,
                "auto_start": True,
                "command_id": _command(
                    "result-question-create", source.id, operation
                ),
            }
        )
        follow_up_id = str(follow_up["id"])
        self.relations.add_relation(
            source.id,
            follow_up_id,
            TaskRelationType.RELATED,
            metadata={
                "kind": "result_question",
                "question": normalized_question,
            },
            created_by_task_id=None,
            created_by_run_id=None,
            command_id=_command(
                "result-question-relation", source.id, follow_up_id
            ),
        )
        return self._result_question_payload(
            source.id, self.store.get_task(follow_up_id)
        )

    def create_operator_work_product(
        self, task_id: str, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        product = self.work_products.create(
            task_id,
            kind=str(value.get("kind") or "other"),
            title=str(value.get("title") or ""),
            summary=str(value.get("summary") or ""),
            workspace=task.workspace,
            uri=(str(value["uri"]) if value.get("uri") else None),
            evidence_id=(
                str(value["evidence_id"]) if value.get("evidence_id") else None
            ),
            artifact_id=(
                str(value["artifact_id"]) if value.get("artifact_id") else None
            ),
            content_hash=(
                str(value["content_hash"]) if value.get("content_hash") else None
            ),
            metadata=dict(value.get("metadata") or {}),
            verification_status=str(
                value.get("verification_status") or "unverified"
            ),
            created_by="local-user",
            command_id=str(
                value.get("command_id")
                or _command("work-product", task_id, _canonical_hash(dict(value)))
            ),
        )
        self.handoff_metrics.increment("orchestration_work_products_total")
        return handoff_jsonable(product)

    def get_work_product(self, product_id: str) -> dict[str, Any]:
        product = self.store.get_work_product(product_id)
        return next(
            item
            for item in self.task_work_products(product.task_id)
            if item["id"] == product_id
        )

    def verify_work_product(
        self, product_id: str, *, command_id: Optional[str] = None
    ) -> dict[str, Any]:
        product = self.store.get_work_product(product_id)
        task = self.store.get_task(product.task_id)
        available = True
        actual_hash: Optional[str] = None
        try:
            if product.uri and product.uri.startswith("workspace:"):
                if not task.workspace:
                    raise FileNotFoundError("task has no workspace")
                relative = product.uri.removeprefix("workspace:").lstrip("/")
                path = ContextRefResolver.canonical_workspace_path(
                    task.workspace, relative
                )
                actual_hash = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
            elif product.artifact_id:
                data = self.blobs.get(product.artifact_id)
                actual_hash = "sha256:" + hashlib.sha256(data).hexdigest()
            elif product.uri and product.uri.startswith("sha256:"):
                data = self.blobs.get(product.uri)
                actual_hash = "sha256:" + hashlib.sha256(data).hexdigest()
            else:
                raise ConflictError(
                    "work product has no locally verifiable workspace or artifact content"
                )
        except (OSError, ValueError, BlobIntegrityError):
            available = False
            actual_hash = None
        return self.work_products.verify(
            product_id,
            available=available,
            actual_hash=actual_hash,
            actor="local-user",
            command_id=command_id,
        )

    def task_wakes(self, task_id: str) -> list[dict[str, Any]]:
        self.store.get_task(task_id)
        return [
            handoff_jsonable(item)
            for item in self.wakes.list_wakes(task_id=task_id, limit=1_000)
        ]

    def list_wakes(
        self, *, status: Optional[str] = None, limit: int = 1_000, offset: int = 0
    ) -> list[dict[str, Any]]:
        statuses = (WakeStatus(status),) if status else None
        return [
            handoff_jsonable(item)
            for item in self.wakes.list_wakes(
                statuses=statuses, limit=limit, offset=offset
            )
        ]

    def retry_wake(
        self, wake_id: str, *, command_id: Optional[str] = None
    ) -> dict[str, Any]:
        record = self.store.retry_wake(wake_id, command_id=command_id)
        self.wake()
        return handoff_jsonable(record)

    def cancel_wake(
        self, wake_id: str, *, command_id: Optional[str] = None
    ) -> dict[str, Any]:
        return handoff_jsonable(
            self.store.cancel_wake(wake_id, command_id=command_id)
        )

    def heartbeat_context(
        self,
        task_id: str,
        *,
        after_sequence: int = 0,
        run_id: Optional[str] = None,
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        try:
            brief = self.store.get_active_brief(task_id)
        except NotFoundError:
            revisions = self.store.list_briefs(task_id)
            if not revisions:
                raise
            # Draft tasks still need an inspectable heartbeat envelope even
            # though no execution is permitted until a Brief is published.
            brief = revisions[-1]
        refs = self.store.list_context_refs(task_id, brief_id=brief.id)
        relations = self.store.list_relations(task_id)
        comments = self.communications.delta(
            task_id, after_sequence=after_sequence
        )
        ancestors: list[dict[str, Any]] = []
        cursor = task
        for _ in range(16):
            if not cursor.parent_task_id:
                break
            cursor = self.store.get_task(cursor.parent_task_id)
            ancestors.append(
                {
                    "task_id": cursor.id,
                    "title": cursor.title,
                    "objective_summary": cursor.objective[:500],
                }
            )
        groups: dict[str, list[dict[str, Any]]] = {
            "children": [],
            "blocked_by": [],
            "blocks": [],
            "reviews": [],
            "related": [],
        }
        for relation in relations:
            payload = handoff_jsonable(relation)
            if relation.relation_type is TaskRelationType.PARENT:
                key = "children" if relation.from_task_id == task_id else "parent"
            elif relation.relation_type is TaskRelationType.BLOCKS:
                key = "blocks" if relation.from_task_id == task_id else "blocked_by"
            elif relation.relation_type is TaskRelationType.REVIEWS:
                key = "reviews"
            else:
                key = "related"
            groups.setdefault(key, []).append(payload)
        wakes = self.store.list_wakes(task_id=task_id, limit=100)
        selected_wake = next(
            (
                item
                for item in reversed(wakes)
                if run_id is None or item.target_run_id in {None, run_id}
            ),
            None,
        )
        products = self.store.list_work_products(task_id, limit=100)
        children = [
            item for item in self.store.list_task_tree(task_id, max_depth=1, max_rows=101)
            if item.parent_task_id == task_id
        ]
        child_results = [
            {
                "task_id": child.id,
                "status": child.status.value,
                "summary": str(
                    dict(child.output or {}).get("result", {}).get("summary")
                    if isinstance(dict(child.output or {}).get("result"), Mapping)
                    else dict(child.output or {}).get("summary") or ""
                )[:2_000],
                "work_product_refs": [
                    item.id for item in self.store.list_work_products(child.id, limit=100)
                ],
            }
            for child in children
            if child.status in _TERMINAL_TASKS
        ]
        manifest = ContextBudgetCalculator.manifest(refs)
        return {
            "schema_version": 1,
            "task": {
                "id": task.id,
                "title": task.title,
                "status": task.status.value,
                "stage": task.current_stage.value,
                "parent_task_id": task.parent_task_id,
            },
            "brief": brief.to_dict(),
            "ancestors": ancestors,
            "relations": groups,
            "context_manifest": {
                "count": manifest["ref_count"],
                "required": manifest["required_count"],
                "estimated_tokens": manifest["estimated_tokens"],
            },
            "comments": {
                "latest_sequence": comments["latest_sequence"],
                "after_sequence": comments["after_sequence"],
                "new_count": comments["new_count"],
                "inline_batch": [
                    handoff_jsonable(item) for item in comments["comments"]
                ],
                "fallback_fetch_needed": comments["fallback_fetch_needed"],
            },
            "child_results": child_results,
            "work_products": [handoff_jsonable(item) for item in products],
            "wake": handoff_jsonable(selected_wake) if selected_wake else None,
        }

    # -- read model ------------------------------------------------------
    def list_tasks(
        self,
        *,
        statuses: Optional[Sequence[TaskStatus]] = None,
        workflow_statuses: Optional[Sequence[str]] = None,
        quality_statuses: Optional[Sequence[str]] = None,
        artifact_statuses: Optional[Sequence[str]] = None,
        budget_statuses: Optional[Sequence[str]] = None,
        archetypes: Optional[Sequence[str]] = None,
        repo: Optional[str] = None,
        snapshot_ref: Optional[str] = None,
        budget_modes: Optional[Sequence[str]] = None,
        has_waiver: Optional[bool] = None,
        repair_count: Optional[int] = None,
        created_by: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        filters = {
            "workflow_status": {str(item) for item in (workflow_statuses or ())},
            "quality_status": {str(item) for item in (quality_statuses or ())},
            "artifact_status": {str(item) for item in (artifact_statuses or ())},
            "budget_status": {str(item) for item in (budget_statuses or ())},
            "archetype": {str(item) for item in (archetypes or ())},
            "budget_mode": {str(item) for item in (budget_modes or ())},
        }
        repo_filter = str(repo or "").strip().casefold()
        ref_filter = str(snapshot_ref or "").strip()
        creator_filter = str(created_by or "").strip().casefold()
        values: list[dict[str, Any]] = []
        for task in self.store.list_all_tasks(statuses=statuses):
            summary = self._task_summary(task)
            quality = self.quality.task_list_projection(task.id)
            value = {**summary, **quality}
            if any(
                allowed and str(value.get(axis) or "") not in allowed
                for axis, allowed in filters.items()
                if axis != "budget_mode"
            ):
                continue
            if filters["budget_mode"] and str(
                (value.get("effective_budget") or {}).get("mode") or ""
            ) not in filters["budget_mode"]:
                continue
            target = value.get("target") or {}
            if repo_filter and repo_filter not in str(
                target.get("repo_root") or target.get("repo") or ""
            ).casefold():
                continue
            if ref_filter and str(target.get("snapshot_ref") or "") != ref_filter:
                continue
            if has_waiver is not None and bool(value.get("has_waiver")) is not has_waiver:
                continue
            if repair_count is not None and int(
                (value.get("run_summary") or {}).get("repairs") or 0
            ) != int(repair_count):
                continue
            if creator_filter and str(value.get("created_by") or "").casefold() != creator_filter:
                continue
            value["attention_reason"] = value.get("quality_reason_code")
            values.append(value)
        start = max(0, int(offset))
        return values[start : start + max(1, min(int(limit), 10_000))]

    def get_blob(self, digest: str) -> tuple[bytes, str]:
        normalized = str(digest).strip().lower().removeprefix("sha256:")
        match = self.store.find_evidence_blob(normalized)
        product = (
            None
            if match is not None
            else self.store.find_work_product_artifact(normalized)
        )
        if match is None and product is None:
            raise NotFoundError(f"evidence blob not found: {normalized}")
        try:
            return (
                self.blobs.get(normalized),
                match.mime_type if match is not None else "application/octet-stream",
            )
        except FileNotFoundError as exc:
            raise NotFoundError(f"evidence blob file is missing: {normalized}") from exc

    def task_detail(self, task_id: str) -> dict[str, Any]:
        tree_page = self.store.list_task_tree(
            task_id,
            max_depth=_DETAIL_CHILD_DEPTH,
            max_rows=_DETAIL_TREE_ROW_LIMIT + 1,
        )
        tree_truncated = len(tree_page) > _DETAIL_TREE_ROW_LIMIT
        tree_records = tree_page[:_DETAIL_TREE_ROW_LIMIT]
        child_counts = self.store.count_task_children(
            tuple(item.id for item in tree_records)
        )
        detail = self._task_detail(
            task_id,
            all_tasks=tree_records,
            child_counts=child_counts,
            include_runtime=True,
            child_depth=_DETAIL_CHILD_DEPTH,
            tree_truncated=tree_truncated,
            tree_row_limit=_DETAIL_TREE_ROW_LIMIT,
        )
        # The detail header and dashboard intentionally share the same bounded V2
        # projection so the four status axes, frozen target, score, gates and
        # budget never disagree when an operator opens a row.
        detail.update(self.quality.task_list_projection(task_id))
        return detail

    def _task_detail(
        self,
        task_id: str,
        *,
        all_tasks: Sequence[TaskRecord],
        child_counts: Mapping[str, int],
        include_runtime: bool,
        child_depth: int,
        tree_truncated: bool,
        tree_row_limit: int,
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        summary = self._task_summary(task)
        history = self.store.stage_projection(task.id)
        row_scale = 1 if include_runtime else 2
        gate_limit = max(1, _DETAIL_GATE_LIMIT // row_scale)
        run_limit = max(1, _DETAIL_RUN_LIMIT // row_scale)
        evidence_limit = max(1, _DETAIL_EVIDENCE_LIMIT // row_scale)
        event_page_size = max(1, _DETAIL_ACTIVITY_LIMIT // row_scale)
        gate_page = self.store.list_gates(
            task.id,
            statuses=tuple(
                status for status in GateStatus if status is not GateStatus.PREPARING
            ),
            limit=gate_limit + 1,
            newest=True,
        )
        run_page = self.store.list_runs(
            task.id, limit=run_limit + 1, newest=True
        )
        evidence_page = self.store.list_evidence(
            task.id, limit=evidence_limit + 1, newest=True
        )
        gates = gate_page[-gate_limit:]
        runs = run_page[-run_limit:]
        evidence = evidence_page[-evidence_limit:]
        event_page = self.store.list_events(
            task_id=task.id,
            newest=True,
            limit=event_page_size + 1,
        )
        events = event_page[-event_page_size:]
        has_older_events = len(event_page) > event_page_size
        graph = self.store.get_plan(task.active_plan_id) if task.active_plan_id else None
        latest = self._latest_runs(runs)
        parent_run_id = (task.input.get("_runtime") or {}).get("parent_run_id")
        runtime_truncated = False
        runtime_snapshot: list[dict[str, Any]] = []
        if include_runtime:
            # A read endpoint must not rebuild the complete durable hierarchy merely
            # to render detail. The scheduler owns runtime reconstruction; detail only
            # projects an already-loaded runtime and caps the response independently.
            with self._runtime_lock:
                runtime_root = self._runtime_task_roots.get(task.id)
                runtime = (
                    self._runtime_trees.get(runtime_root)
                    if runtime_root is not None
                    else None
                )
            if runtime is not None:
                loaded_runtime = runtime.snapshot()
                runtime_truncated = len(loaded_runtime) > _DETAIL_RUNTIME_LIMIT
                runtime_snapshot = list(loaded_runtime[:_DETAIL_RUNTIME_LIMIT])
                if not self.enforce_runtime_budgets:
                    runtime_snapshot = [
                        {**item, "budget": None, "budget_mode": "unlimited"}
                        for item in runtime_snapshot
                    ]
        stage_items = []
        for stage in OrchestrationStage:
            visits = [item for item in history if item.stage is stage]
            active = next((item for item in reversed(visits) if item.disposition is StageDisposition.ACTIVE), None)
            finished = next((item for item in reversed(visits) if item.exited_at), None)
            status = (
                "running"
                if active
                else "completed"
                if finished and finished.disposition is StageDisposition.COMPLETED
                else "skipped"
                if finished and finished.disposition is StageDisposition.SKIPPED
                else "failed"
                if finished and finished.disposition is StageDisposition.FAILED
                else "canceled"
                if finished and finished.disposition is StageDisposition.CANCELED
                else "needs_reconciliation"
                if finished and finished.disposition is StageDisposition.REQUEST_CHANGES
                else "pending"
            )
            stage_items.append(
                {
                    "stage": stage.value,
                    "status": status,
                    "started_at": _iso((active or finished).entered_at) if (active or finished) else None,
                    "completed_at": _iso(finished.exited_at) if finished else None,
                }
            )
        direct_children = [
            item for item in all_tasks if item.parent_task_id == task.id
        ]
        total_children = int(child_counts.get(task.id, 0))
        children_omitted = total_children > len(direct_children)
        depth_limit_reached = child_depth <= 0 and children_omitted
        usage_by_run: dict[str, list[Any]] = {}
        for item in self.store.list_runtime_usage_evidence(
            task.id, tuple(run.id for run in runs)
        ):
            if item.run_id is not None:
                usage_by_run.setdefault(item.run_id, []).append(item)
        run_payloads = [
            self._run_payload(
                run,
                parent_run_id=parent_run_id,
                usage_evidence=usage_by_run.get(run.id, ()),
            )
            for run in runs
        ]
        run_payloads_by_id = {item["id"]: item for item in run_payloads}
        try:
            active_brief = self.store.get_active_brief(task.id)
        except NotFoundError:
            revisions = self.store.list_briefs(task.id)
            if not revisions:
                raise
            active_brief = revisions[-1]
        if include_runtime:
            handoff_refs = self.store.list_context_refs(
                task.id, brief_id=active_brief.id, limit=1_000
            )
            handoff_relations = self.store.list_relations(task.id)
            handoff_comments = self.store.list_task_comments(
                task.id, after_sequence=0, limit=1_000
            )
            handoff_products = self.store.list_work_products(task.id, limit=1_000)
            handoff_wakes = self.store.list_wakes(task_id=task.id, limit=1_000)
        else:
            handoff_refs = ()
            handoff_relations = ()
            handoff_comments = ()
            handoff_products = ()
            handoff_wakes = ()
        stored_result = dict(task.output or {}).get("result")
        return {
            **summary,
            "result": (
                dict(stored_result)
                if isinstance(stored_result, Mapping)
                else None
            ),
            "brief": active_brief.to_dict(),
            "handoff_summary": {
                "protocol": (
                    "structured"
                    if bool(task.policy.get("structured_handoff"))
                    else "legacy"
                ),
                "loaded": include_runtime,
                "context": ContextBudgetCalculator.manifest(handoff_refs),
                "relations": {
                    item.value: sum(
                        relation.relation_type is item
                        for relation in handoff_relations
                    )
                    for item in TaskRelationType
                },
                "comments": {
                    "count": len(handoff_comments),
                    "latest_sequence": (
                        handoff_comments[-1].sequence if handoff_comments else 0
                    ),
                    "content_included": False,
                },
                "work_products": {"count": len(handoff_products)},
                "wakes": {
                    "count": len(handoff_wakes),
                    "pending": sum(
                        item.status in {WakeStatus.PENDING, WakeStatus.DEFERRED}
                        for item in handoff_wakes
                    ),
                    "failed": sum(
                        item.status is WakeStatus.FAILED for item in handoff_wakes
                    ),
                },
            },
            "stages": stage_items,
            "attention": [self._gate_payload(gate) for gate in gates],
            "attention_page": {
                "has_more": len(gate_page) > gate_limit,
                "page_size": gate_limit,
            },
            "nodes": [
                {
                    "id": node.id,
                    "key": node.key,
                    "title": node.title or node.key,
                    "description": node.instructions,
                    "kind": node.kind.value,
                    "status": self._work_status(latest.get(node.key).status if latest.get(node.key) else None),
                    "depends_on": [
                        next(item.id for item in graph.nodes if item.key == edge.from_node)
                        for edge in graph.edges
                        if edge.to_node == node.key
                    ],
                    "profile_name": node.agent,
                    "profile_version": (node.metadata.get("profile_snapshot") or {}).get("version"),
                    "model": node.model,
                    "runtime_preset_binding": dict(
                        node.metadata.get("runtime_preset_binding") or {}
                    ),
                    "run_ids": [run.id for run in runs if run.node_id == node.id],
                }
                for node in (graph.nodes if graph else ())
            ],
            "edges": [
                {
                    "from": next(item.id for item in graph.nodes if item.key == edge.from_node),
                    "to": next(item.id for item in graph.nodes if item.key == edge.to_node),
                }
                for edge in (graph.edges if graph else ())
            ],
            "runs": run_payloads,
            "runs_page": {
                "has_more": len(run_page) > run_limit,
                "page_size": run_limit,
            },
            "evidence": [self._evidence_payload(item) for item in evidence],
            "evidence_page": {
                "has_more": len(evidence_page) > evidence_limit,
                "page_size": evidence_limit,
            },
            "activity": [
                self._activity_payload(
                    item,
                    history,
                    task.current_stage,
                    run_payloads_by_id,
                )
                for item in events
            ],
            "activity_page": {
                "has_more": has_older_events,
                "next_sequence": events[0].sequence if has_older_events and events else None,
                "next_parameter": "before_sequence",
                "page_size": event_page_size,
            },
            "profile_snapshot": self._primary_snapshot(graph, "profile_snapshot"),
            "model_policy_snapshot": self._primary_snapshot(graph, "model_policy_snapshot"),
            "parent_task_id": task.parent_task_id,
            "children": [
                {
                    **self._task_summary(item),
                    "parent_run_id": (item.input.get("_runtime") or {}).get(
                        "parent_run_id"
                    ),
                }
                for item in direct_children
            ],
            "children_page": {
                "truncated": tree_truncated or children_omitted,
                "tree_truncated": tree_truncated,
                "depth_limit_reached": depth_limit_reached,
                "returned": len(direct_children),
                "total": total_children,
                "tree_row_limit": tree_row_limit,
            },
            "children_details": [
                self._task_detail(
                    item.id,
                    all_tasks=all_tasks,
                    child_counts=child_counts,
                    include_runtime=False,
                    child_depth=child_depth - 1,
                    tree_truncated=tree_truncated,
                    tree_row_limit=tree_row_limit,
                )
                for item in (direct_children if child_depth > 0 else ())
            ],
            "detail_limits": {
                "child_depth": child_depth,
                "attention": gate_limit,
                "runs": run_limit,
                "evidence": evidence_limit,
                "activity": event_page_size,
            },
            "runtime": runtime_snapshot,
            "runtime_budget_mode": (
                "enforced" if self.enforce_runtime_budgets else "unlimited"
            ),
            "runtime_page": {
                "truncated": runtime_truncated,
                "returned": len(runtime_snapshot),
                "limit": _DETAIL_RUNTIME_LIMIT,
            },
        }

    def run_transcript(
        self,
        task_id: str,
        run_id: str,
        *,
        offset: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        """Return a read-only, task-scoped transcript for an orchestration run.

        Missing/retired sessions are represented explicitly instead of making a UI
        guess that the run id is itself a session id.
        """

        self.store.get_task(task_id)
        run = self.store.get_run(run_id)
        if run.task_id != task_id:
            raise NotFoundError(f"run {run_id} does not belong to task {task_id}")
        session_id = run.session_id
        record = self.manager.session_store.load(session_id) if session_id else None
        offset = max(0, int(offset))
        limit = max(1, min(int(limit), 2_000))
        all_messages = record.messages if record is not None else []
        messages = all_messages[offset : offset + limit]
        return {
            "task_id": task_id,
            "run_id": run.id,
            "session_id": session_id,
            "available": record is not None,
            "title": record.title if record is not None else run.node_key,
            "messages": _jsonable(messages),
            "message_count": len(all_messages),
            "offset": offset,
            "limit": limit,
            "has_more": offset + len(messages) < len(all_messages),
            "next_offset": (
                offset + len(messages)
                if offset + len(messages) < len(all_messages)
                else None
            ),
            "updated_at": record.updated_at if record is not None else None,
        }

    def task_runs_page(
        self, task_id: str, *, offset: int = 0, limit: int = 200
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        offset = max(0, int(offset))
        limit = max(1, min(int(limit), 500))
        page = self.store.list_runs(
            task_id, limit=limit + 1, newest=True, offset=offset
        )
        runs = page[-limit:]
        parent_run_id = (task.input.get("_runtime") or {}).get("parent_run_id")
        usage_by_run: dict[str, list[Any]] = {}
        for item in self.store.list_runtime_usage_evidence(
            task.id, tuple(run.id for run in runs)
        ):
            if item.run_id is not None:
                usage_by_run.setdefault(item.run_id, []).append(item)
        return {
            "task_id": task_id,
            "runs": [
                self._run_payload(
                    run,
                    parent_run_id=parent_run_id,
                    usage_evidence=usage_by_run.get(run.id, ()),
                )
                for run in runs
            ],
            "offset": offset,
            "limit": limit,
            "has_more": len(page) > limit,
            "next_offset": offset + limit if len(page) > limit else None,
            "order": "oldest_to_newest",
        }

    def task_gates_page(
        self, task_id: str, *, offset: int = 0, limit: int = 200
    ) -> dict[str, Any]:
        """Return a bounded, newest-first window of published gate history."""

        self.store.get_task(task_id)
        offset = max(0, int(offset))
        limit = max(1, min(int(limit), 500))
        page = self.store.list_gates(
            task_id, limit=limit + 1, newest=True, offset=offset
        )
        gates = page[-limit:]
        has_more = len(page) > limit
        return {
            "task_id": task_id,
            "gates": [self._gate_payload(gate) for gate in gates],
            "offset": offset,
            "limit": limit,
            "has_more": has_more,
            "next_offset": offset + limit if has_more else None,
            "order": "oldest_to_newest",
        }

    def task_evidence_page(
        self, task_id: str, *, offset: int = 0, limit: int = 200
    ) -> dict[str, Any]:
        self.store.get_task(task_id)
        offset = max(0, int(offset))
        limit = max(1, min(int(limit), 500))
        page = self.store.list_evidence(
            task_id, limit=limit + 1, newest=True, offset=offset
        )
        evidence = page[-limit:]
        return {
            "task_id": task_id,
            "evidence": [self._evidence_payload(item) for item in evidence],
            "offset": offset,
            "limit": limit,
            "has_more": len(page) > limit,
            "next_offset": offset + limit if len(page) > limit else None,
            "order": "oldest_to_newest",
        }

    def outbox_dead_letters(
        self, *, offset: int = 0, limit: int = 100
    ) -> dict[str, Any]:
        offset = max(0, int(offset))
        limit = max(1, min(int(limit), 500))
        page = self.store.list_outbox_dead_letters(
            limit=limit + 1, offset=offset
        )
        items = page[:limit]
        histories, history_totals = self.store.list_outbox_requeue_histories(
            [item.id for item in items], per_item_limit=20
        )
        return {
            "items": [
                {
                    "id": item.id,
                    "event_id": item.event_id,
                    "topic": item.topic,
                    "attempts": item.attempts,
                    "last_error": item.last_error,
                    "dead_lettered_at": _iso(item.dead_lettered_at),
                    "payload": dict(item.payload),
                    "requeue_history": [
                        self._outbox_requeue_payload(entry)
                        for entry in histories.get(item.id, ())
                    ],
                    "requeue_history_total": history_totals.get(item.id, 0),
                    "requeue_history_truncated": history_totals.get(item.id, 0)
                    > len(histories.get(item.id, ())),
                }
                for item in items
            ],
            "offset": offset,
            "limit": limit,
            "has_more": len(page) > limit,
            "next_offset": offset + limit if len(page) > limit else None,
        }

    def outbox_dead_letter_detail(
        self,
        outbox_id: str,
        *,
        history_offset: int = 0,
        history_limit: int = 100,
    ) -> dict[str, Any]:
        item = self.store.get_outbox(outbox_id)
        history_offset = max(0, int(history_offset))
        history_limit = max(1, min(int(history_limit), 1_000))
        history = self.store.list_outbox_requeue_history(
            outbox_id, limit=history_limit, offset=history_offset
        )
        total = self.store.count_outbox_requeue_history(outbox_id)
        return {
            **self._outbox_payload(item),
            "requeue_history": [
                self._outbox_requeue_payload(entry) for entry in history
            ],
            "requeue_history_offset": history_offset,
            "requeue_history_limit": history_limit,
            "requeue_history_total": total,
            "requeue_history_has_more": history_offset + len(history) < total,
            "requeue_history_next_offset": (
                history_offset + len(history)
                if history_offset + len(history) < total
                else None
            ),
        }

    def requeue_outbox(
        self,
        outbox_id: str,
        *,
        idempotency_key: str,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        item, audit, replayed = self.store.requeue_outbox(
            outbox_id,
            idempotency_key=idempotency_key,
            actor=actor,
            reason=reason,
        )
        metrics = self.store.outbox_health()
        self._outbox_pending = int(metrics["pending"])
        self._outbox_dead_letters = int(metrics["dead_letters"])
        self._oldest_outbox_pending_at = metrics["oldest_pending_at"]
        self.wake()
        return {
            "id": item.id,
            "event_id": item.event_id,
            "status": "queued",
            "attempts": 0,
            "replayed": replayed,
            "audit": self._outbox_requeue_payload(audit),
            "current": self._outbox_payload(item),
            "requeue_history": [
                self._outbox_requeue_payload(entry)
                for entry in self.store.list_outbox_requeue_history(
                    outbox_id, limit=100
                )
            ],
            "requeue_history_total": self.store.count_outbox_requeue_history(
                outbox_id
            ),
        }

    @staticmethod
    def _outbox_payload(item: Any) -> dict[str, Any]:
        status = (
            "published"
            if item.published_at is not None
            else "dead_lettered"
            if item.dead_lettered_at is not None
            else "leased"
            if item.locked_until is not None
            else "queued"
        )
        return {
            "id": item.id,
            "event_id": item.event_id,
            "topic": item.topic,
            "status": status,
            "attempts": item.attempts,
            "last_error": item.last_error,
            "available_at": _iso(item.available_at),
            "locked_by": item.locked_by,
            "locked_until": _iso(item.locked_until),
            "published_at": _iso(item.published_at),
            "dead_lettered_at": _iso(item.dead_lettered_at),
            "created_at": _iso(item.created_at),
            "payload": dict(item.payload),
        }

    @staticmethod
    def _outbox_requeue_payload(item: Any) -> dict[str, Any]:
        return {
            "id": item.id,
            "outbox_id": item.outbox_id,
            "idempotency_key": item.command_id,
            "actor": item.actor,
            "reason": item.reason,
            "snapshot": {
                "attempts": item.snapshot_attempts,
                "last_error": item.snapshot_last_error,
                "dead_lettered_at": _iso(item.snapshot_dead_lettered_at),
            },
            "requeued_at": _iso(item.requeued_at),
        }

    @staticmethod
    def _event_actor(event: Any) -> str:
        payload = event.payload
        for key in (
            "resolved_by",
            "created_by",
            "actor",
            "accepted_by",
            "rejected_by",
            "requested_by",
            "worker_id",
        ):
            value = str(payload.get(key) or "").strip()
            if value:
                return value
        return "orchestration-system"

    @classmethod
    def _event_observability_fields(cls, event: Any) -> dict[str, Any]:
        """Derive a content-free trace projection for API/log consumers."""

        payload = dict(event.payload)

        def first(*keys: str) -> Optional[str]:
            for key in keys:
                value = str(payload.get(key) or "").strip()
                if value:
                    return value
            return None

        aggregate_type = str(event.aggregate_type)
        aggregate_id = str(event.aggregate_id)
        run_id = first(
            "run_id", "source_run_id", "parent_run_id", "created_by_run_id"
        )
        if run_id is None and aggregate_type == "run":
            run_id = aggregate_id
        return {
            "actor": cls._event_actor(event),
            "task_id": event.task_id,
            "run_id": run_id,
            "brief_id": (
                first("brief_id")
                or (aggregate_id if aggregate_type == "task_brief" else None)
            ),
            "brief_revision": payload.get("brief_revision"),
            "wake_id": (
                aggregate_id if aggregate_type == "wake_request" else first("wake_id")
            ),
            "wake_reason": first("reason", "wake_reason"),
            "context_ref_id": (
                aggregate_id
                if aggregate_type == "context_ref"
                else first("context_ref_id", "ref_id")
            ),
            "relation_id": (
                aggregate_id if aggregate_type == "task_relation" else first("relation_id")
            ),
            "work_product_id": (
                aggregate_id if aggregate_type == "work_product" else first("work_product_id")
            ),
            "correlation_id": first("correlation_id") or event.task_id,
            "causation_id": (
                first("causation_id", "source_event_id") or event.command_id
            ),
        }

    def _activity_payload(
        self,
        event: Any,
        history: Sequence[Any],
        current_stage: OrchestrationStage,
        runs_by_id: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        payload = {
            "id": event.id,
            "type": event.event_type,
            "summary": event.event_type.replace(".", " "),
            "detail": json.dumps(event.payload, ensure_ascii=False),
            "actor": self._event_actor(event),
            "created_at": _iso(event.created_at),
            "stage": self._event_stage(event, history, current_stage),
            "sequence": event.sequence,
            "event_hash": event.event_hash,
            **self._event_observability_fields(event),
        }
        if event.aggregate_type == "run" and event.event_type in {
            "run.failed",
            "run.timed_out",
            "run.canceled",
            "run.lost",
            "run.skipped",
        }:
            run = runs_by_id.get(str(event.aggregate_id))
            if run is not None:
                payload["error_kind"] = run.get("error_kind")
                payload["error_message"] = run.get("error_message")
        return payload

    def _task_summary(self, task: TaskRecord) -> dict[str, Any]:
        history = self.store.stage_projection(task.id)
        complete = {item.stage for item in history if item.exited_at and item.disposition in {StageDisposition.COMPLETED, StageDisposition.SKIPPED}}
        attention_count = self.store.count_gates(
            task.id, statuses=(GateStatus.OPEN,)
        )
        return {
            "id": task.id,
            "title": task.title,
            "objective": task.objective,
            "status": task.status.value,
            "stage": task.current_stage.value,
            "progress": round(100 * len(complete) / 8),
            "attention_count": attention_count,
            "updated_at": _iso(task.updated_at),
            "created_at": _iso(task.created_at),
            "profile_name": str(task.policy.get("profile_id") or "worker"),
            "runtime_preset_id": task.policy.get("runtime_preset_id"),
            "runtime_preset_version": task.policy.get("runtime_preset_version"),
            "complexity": task.complexity_level.value if task.complexity_level else None,
            "complexity_score": task.complexity_score,
            "risk": task.risk_tier.value,
            "parent_task_id": task.parent_task_id,
            "parent_run_id": (task.input.get("_runtime") or {}).get("parent_run_id"),
            "terminal_outcome": self._terminal_outcome(task).value,
        }

    @staticmethod
    def _event_stage(event: Any, history: Sequence[Any], current: OrchestrationStage) -> str:
        explicit = event.payload.get("stage")
        if not explicit and event.event_type == "task.stage_changed":
            explicit = event.payload.get("to")
        if explicit:
            return str(explicit)
        for visit in reversed(history):
            if event.created_at < visit.entered_at:
                continue
            if visit.exited_at is None or event.created_at <= visit.exited_at:
                return visit.stage.value
        return current.value

    @staticmethod
    def _work_status(status: Optional[RunStatus]) -> str:
        if status is None:
            return "pending"
        return {
            RunStatus.QUEUED: "ready",
            RunStatus.CLAIMED: "running",
            RunStatus.RUNNING: "running",
            RunStatus.WAITING_GATE: "waiting",
            RunStatus.SUCCEEDED: "completed",
            RunStatus.FAILED: "failed",
            RunStatus.TIMED_OUT: "failed",
            RunStatus.CANCELED: "cancelled",
            RunStatus.LOST: "failed",
            RunStatus.SKIPPED: "skipped",
        }[status]

    def _run_payload(
        self,
        run: RunRecord,
        *,
        parent_run_id: Any = _UNSET,
        usage_evidence: Optional[Sequence[Any]] = None,
    ) -> dict[str, Any]:
        output = dict(run.output or {})
        output_profile = (
            dict(output.get("profile") or {})
            if isinstance(output.get("profile"), Mapping)
            else {}
        )
        route = dict(output.get("routing") or {})
        error_message = str(run.error_message or "")[:_RUN_ERROR_MESSAGE_LIMIT]
        task = self.store.get_task(run.task_id)
        if parent_run_id is _UNSET:
            parent_run_id = (task.input.get("_runtime") or {}).get("parent_run_id")
        attempt_budget: Optional[RuntimeBudget] = None
        profile_id = str(output_profile.get("profile_id") or "")
        profile_version = output_profile.get("version")
        role = str(output_profile.get("role") or "")
        try:
            graph = self.store.get_plan(run.plan_id)
            node = next(item for item in graph.nodes if item.id == run.node_id)
            profile_snapshot = dict(node.metadata.get("profile_snapshot") or {})
            profile_spec = (
                dict(profile_snapshot.get("spec") or {})
                if isinstance(profile_snapshot.get("spec"), Mapping)
                else {}
            )
            profile_id = profile_id or str(
                profile_snapshot.get("profile_id") or node.agent
            )
            profile_version = profile_version or profile_snapshot.get("version")
            role = role or str(profile_spec.get("role") or "")
            if self.enforce_runtime_budgets:
                allocation = self._run_budget(task, graph, node)
                spent = RuntimeBudget()
                prior_runs = sorted(
                    (
                        item
                        for item in self.store.list_runs(task.id)
                        if item.plan_id == run.plan_id
                        and item.node_id == run.node_id
                        and (item.created_at, item.attempt, item.id)
                        < (run.created_at, run.attempt, run.id)
                        and item.status is not RunStatus.QUEUED
                    ),
                    key=lambda item: (item.created_at, item.attempt, item.id),
                )
                for prior in prior_runs:
                    available = allocation - spent
                    spent += self._bounded_budget(
                        self._usage_for_run(prior), available
                    )
                attempt_budget = allocation - spent
        except (BudgetExceededError, NotFoundError, StopIteration):
            # Legacy/corrupt rows remain inspectable even when their original
            # attempt allocation cannot be reconstructed.
            attempt_budget = None
        observed_usage = self._observed_usage_for_run(run, usage_evidence)
        return {
            "id": run.id,
            "node_id": run.node_id,
            "node_key": run.node_key,
            "title": run.node_key,
            "agent_name": profile_id or None,
            "profile_version": profile_version,
            "role": role or None,
            "status": self._work_status(run.status),
            "model_id": route.get("selected_model"),
            "routing_reason": route.get("reason"),
            "attempt": run.attempt,
            "started_at": _iso(run.started_at),
            "completed_at": _iso(run.finished_at),
            "summary": output.get("summary") or error_message,
            "session_id": run.session_id,
            "error_kind": run.error_kind,
            "error_message": error_message,
            "parent_run_id": parent_run_id,
            "usage": observed_usage.as_dict(),
            "budget": (
                attempt_budget.as_dict()
                if self.enforce_runtime_budgets and attempt_budget is not None
                else None
            ),
        }

    @staticmethod
    def _evidence_payload(item: Any) -> dict[str, Any]:
        return {
            "id": item.id,
            "title": str(item.payload.get("title") or item.kind.value),
            "kind": item.kind.value,
            "summary": str(
                item.payload.get("summary") or item.payload.get("verdict") or ""
            ),
            "uri": item.blob_uri or item.payload.get("uri"),
            "run_id": item.run_id,
            "created_at": _iso(item.created_at),
            "content_hash": item.content_hash,
            "payload": dict(item.payload),
            "subject": (
                dict(item.payload.get("subject") or {})
                if isinstance(item.payload.get("subject"), Mapping)
                else {}
            ),
            "subject_matches": item.payload.get("subject_matches"),
            "missing_criteria": (
                [str(value) for value in item.payload.get("missing_criteria") or ()]
                if isinstance(item.payload.get("missing_criteria"), (list, tuple))
                else []
            ),
            "actor": item.created_by,
        }

    @staticmethod
    def _gate_payload(gate: GateRecord) -> dict[str, Any]:
        prompt = dict(gate.prompt)
        actions = prompt.get("actions") or ["approve", "reject"]
        normalized_actions: list[dict[str, Any]] = []
        for raw in actions:
            if isinstance(raw, Mapping):
                action_id = str(raw.get("id") or raw.get("action") or "").strip()
                if not action_id:
                    continue
                label = str(
                    raw.get("label")
                    or action_id.replace("_", " ").title()
                )
                tone = str(raw.get("tone") or "").strip().lower()
                requires_response = bool(raw.get("requires_response", False))
            else:
                action_id = str(raw).strip()
                if not action_id:
                    continue
                label = action_id.replace("_", " ").title()
                tone = ""
                requires_response = action_id in {
                    "accept_current",
                    "submit",
                    "request_changes",
                }
            if tone not in {"primary", "neutral", "danger"}:
                tone = (
                    "primary"
                    if action_id
                    in {
                        "accept_current",
                        "approve",
                        "accept",
                        "submit",
                        "retry",
                    }
                    else "danger"
                    if action_id in {"reject", "cancel"}
                    else "neutral"
                )
            normalized_actions.append(
                {
                    "id": action_id,
                    "label": label,
                    "tone": tone,
                    "requires_response": requires_response,
                }
            )
        return {
            "id": gate.id,
            "kind": gate.kind.value,
            "title": prompt.get("title") or gate.kind.value.replace("_", " ").title(),
            "description": prompt.get("description") or prompt.get("question") or "",
            "status": "pending" if gate.status is GateStatus.OPEN else "resolved",
            "actions": normalized_actions,
            "response_placeholder": "Add context or requested changes",
            "created_at": _iso(gate.published_at or gate.opened_at),
            "resolved_at": _iso(gate.resolved_at),
            "resolution": (gate.resolution or {}).get("response") or (gate.resolution or {}).get("decision"),
            "version": gate.version,
            "prompt": prompt,
        }

    @staticmethod
    def _primary_snapshot(graph: Optional[PlanGraph], key: str) -> Optional[dict[str, Any]]:
        if not graph or not graph.nodes:
            return None
        value = dict(graph.nodes[0].metadata.get(key) or {})
        if not value:
            return None
        return {
            "id": value.get("profile_id") or value.get("policy_id"),
            "name": value.get("profile_id") or value.get("policy_id"),
            "version": value.get("version"),
            "content_hash": value.get("content_hash"),
        }

    @staticmethod
    def _graph_payload(graph: PlanGraph) -> dict[str, Any]:
        return {
            "id": graph.plan.id,
            "revision": graph.plan.revision,
            "content_hash": graph.plan.content_hash,
            "nodes": [
                {"key": node.key, "title": node.title, "kind": node.kind.value, "agent": node.agent}
                for node in graph.nodes
            ],
            "edges": [
                {"from": edge.from_node, "to": edge.to_node, "condition": edge.condition.value}
                for edge in graph.edges
            ],
        }

    def _block_task(self, task_id: str, kind: str, message: str) -> None:
        try:
            task = self.store.get_task(task_id)
            if task.status in {TaskStatus.RUNNING, TaskStatus.QUEUED}:
                self.store.transition_task_status(
                    task.id,
                    TaskStatus.BLOCKED,
                    expected_version=task.version,
                    output={"error_kind": kind, "error": message},
                    command_id=f"block-{uuid.uuid4().hex}",
                )
        except Exception:
            logger.exception("could not block failed orchestration task %s", task_id)
