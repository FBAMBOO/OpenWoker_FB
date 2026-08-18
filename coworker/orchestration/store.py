"""Transactional SQLite persistence for the orchestration domain.

The store owns an independent WAL database.  All aggregate mutations use
``BEGIN IMMEDIATE`` so command deduplication, optimistic updates, events, and outbox
records commit together.  Runs additionally require a lease token and monotonically
increasing fencing token, preventing a stale worker from committing after reassignment.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence
from urllib.parse import urlparse

from .dag import validate_plan
from .activity import (
    MAX_RUN_ACTIVITY_ROWS,
    RUN_ACTIVITY_KINDS,
    RUN_ACTIVITY_STATUSES,
    bounded_activity_text,
    sanitize_activity_detail,
)
from .errors import (
    ConflictError,
    GateConflict,
    IdempotencyConflict,
    IntegrityError,
    LeaseConflict,
    NotFoundError,
    VersionConflict,
)
from .migrations import apply_migrations
from .handoff_models import (
    BriefStatus,
    ContextDeliveryMode,
    ContextRefDraft,
    ContextRefRecord,
    ContextRefType,
    ContextRequirement,
    HandoffValidationError,
    TaskBriefDraft,
    TaskBriefRecord,
    TaskCommentRecord,
    TaskRelationRecord,
    TaskRelationType,
    WakeReason,
    WakeRequestRecord,
    WakeStatus,
    WorkProductKind,
    WorkProductRecord,
    contains_secret_like,
)
from .models import (
    CommandRecord,
    CommandStatus,
    ComplexityLevel,
    EdgeCondition,
    EdgeRecord,
    EdgeSpec,
    EffectSafety,
    EvidenceKind,
    EvidenceRecord,
    FailurePolicy,
    GateKind,
    GateRecord,
    GateStatus,
    JoinPolicy,
    LeaseRecord,
    NodeKind,
    NodeRecord,
    NodeSpec,
    OrchestrationStage,
    OutboxRecord,
    OutboxRequeueRecord,
    PlanGraph,
    PlanRecord,
    PlanSpec,
    RetryPolicy,
    RiskTier,
    RunClaim,
    RunActivityRecord,
    RunRecord,
    RunStatus,
    StageDisposition,
    StageHistoryRecord,
    TaskDomain,
    TaskRecord,
    TaskSpec,
    TaskStatus,
    EventRecord,
)
from .state_machine import validate_stage_transition, validate_task_transition


_ZERO_HASH = "0" * 64
_ACTIVE_RUN_STATUSES = (RunStatus.CLAIMED.value, RunStatus.RUNNING.value)
_RUN_SETTLEMENT_TASK_STATUSES = frozenset(
    {
        TaskStatus.RUNNING,
        TaskStatus.WAITING_HUMAN,
        TaskStatus.WAITING_CHILD,
        TaskStatus.PAUSED,
    }
)
_TERMINAL_RUN_STATUSES = frozenset(
    {
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.TIMED_OUT,
        RunStatus.CANCELED,
        RunStatus.LOST,
        RunStatus.SKIPPED,
    }
)
_FAILED_RUN_STATUSES = frozenset(
    {
        RunStatus.FAILED,
        RunStatus.TIMED_OUT,
        RunStatus.CANCELED,
        RunStatus.LOST,
    }
)
_MENTION_PROFILE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_MAX_MENTIONS_PER_COMMENT = 10
_MAX_LIVE_MENTION_WAKES_PER_TASK = 100
_COMPATIBILITY_RETRY_REASONS = frozenset(
    {
        "legacy_subscription_result_handoff",
        "bounded_work_product_envelope_handoff",
    }
)


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _time(value: str | None) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return _stamp(value)
    return value


def _json(value: Any) -> str:
    return json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def _load(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _event_hash(previous_hash: str, envelope: Mapping[str, Any]) -> str:
    body = previous_hash.encode("ascii") + b"\n" + _json(envelope).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


class OrchestrationStore:
    """Durable domain store.  Separate instances may safely share one DB file."""

    def __init__(
        self, path: str | Path, *, busy_timeout_ms: int = 5_000
    ) -> None:
        self.path = str(path)
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        self._uri = False
        self._anchor: Optional[sqlite3.Connection] = None
        self._closed = False
        # Services bind the scheduler identity after acquiring leadership.  Once
        # bound, every ordinary write transaction verifies that identity and its
        # unexpired epoch under the same BEGIN IMMEDIATE lock as the mutation.
        # Standalone stores remain unfenced for migrations, diagnostics and unit
        # tests that intentionally exercise the persistence layer directly.
        self._scheduler_fence: Optional[tuple[str, str, int]] = None
        if self.path == ":memory:":
            self.path = f"file:orchestration_{uuid.uuid4().hex}?mode=memory&cache=shared"
            self._uri = True
            self._anchor = self._new_connection()
            apply_migrations(self._anchor)
            self._backfill_legacy_briefs_connection(self._anchor)
            self._backfill_parent_relations_connection(self._anchor)
        else:
            target = Path(self.path).expanduser()
            target.parent.mkdir(parents=True, exist_ok=True)
            self.path = str(target)
            connection = self._new_connection()
            try:
                apply_migrations(connection)
                self._backfill_legacy_briefs_connection(connection)
                self._backfill_parent_relations_connection(connection)
            finally:
                connection.close()

    def _new_connection(self) -> sqlite3.Connection:
        if self._closed:
            raise LeaseConflict("orchestration store is closed")
        connection = sqlite3.connect(
            self.path,
            uri=self._uri,
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA journal_mode = WAL")
        # Audit/events and command results are control-plane truth.  FULL keeps WAL
        # commits durable across an OS/power failure instead of accepting SQLite's
        # NORMAL-mode window where the newest committed transaction may be lost.
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def connect(self) -> sqlite3.Connection:
        """Return a configured caller-owned connection, primarily for diagnostics."""

        return self._new_connection()

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        connection = self._new_connection()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _write(
        self, *, enforce_scheduler_fence: bool = True
    ) -> Iterator[sqlite3.Connection]:
        connection = self._new_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if self._closed:
                raise LeaseConflict("orchestration store is closed")
            if enforce_scheduler_fence:
                self._assert_scheduler_fence(connection)
            yield connection
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def bind_scheduler_fence(self, owner: str, token: str, epoch: int) -> None:
        """Fence all subsequent domain writes to one scheduler lease epoch."""

        identity = (str(owner).strip(), str(token).strip(), int(epoch))
        if not identity[0] or not identity[1] or identity[2] < 1:
            raise ValueError("a complete scheduler fence identity is required")
        self._scheduler_fence = identity

    def _assert_scheduler_fence(self, connection: sqlite3.Connection) -> None:
        identity = self._scheduler_fence
        if identity is None:
            return
        owner, token, epoch = identity
        row = connection.execute(
            "SELECT owner, token, epoch, expires_at "
            "FROM orch_scheduler_leader WHERE singleton = 1"
        ).fetchone()
        now = _now()
        if (
            row is None
            or row["owner"] != owner
            or row["token"] != token
            or int(row["epoch"]) != epoch
            or (_time(row["expires_at"]) or datetime.min.replace(tzinfo=timezone.utc))
            <= now
        ):
            raise LeaseConflict(
                f"scheduler fencing rejected stale leader {owner} at epoch {epoch}"
            )

    def assert_scheduler_fence(self) -> None:
        """Fail closed when the bound scheduler no longer owns a live lease."""

        with self._write():
            pass

    def renew_scheduler_fence(self, *, lease_seconds: int = 15) -> None:
        """Renew the currently bound lease immediately before an external effect."""

        identity = self._scheduler_fence
        if identity is None:
            return
        self.heartbeat_scheduler_leader(
            identity[0], identity[1], identity[2], lease_seconds=lease_seconds
        )

    def close(self) -> None:
        # A store object is intentionally one-shot. Retain any bound stale epoch
        # and tombstone the instance so delayed API workers/callbacks cannot turn a
        # formerly leader-controlled store back into an unfenced standalone writer.
        self._closed = True
        if self._anchor is not None:
            self._anchor.close()
            self._anchor = None

    # -- command ledger -----------------------------------------------------
    def _start_command(
        self,
        connection: sqlite3.Connection,
        command_id: str,
        name: str,
        scope: str,
        request: Any,
    ) -> Optional[dict[str, Any]]:
        request_hash = _digest(request)
        row = connection.execute(
            "SELECT * FROM orch_commands WHERE id = ?", (command_id,)
        ).fetchone()
        if row is not None:
            if (
                row["name"] != name
                or row["scope"] != scope
                or row["request_hash"] != request_hash
            ):
                raise IdempotencyConflict(
                    f"command {command_id} was reused with different input"
                )
            if row["status"] == CommandStatus.COMPLETED.value:
                return _load(row["result_json"], {})
            raise ConflictError(f"command {command_id} is already in progress")
        connection.execute(
            """
            INSERT INTO orch_commands(
                id, name, scope, request_hash, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                command_id,
                name,
                scope,
                request_hash,
                CommandStatus.IN_PROGRESS.value,
                _stamp(_now()),
            ),
        )
        return None

    def _finish_command(
        self,
        connection: sqlite3.Connection,
        command_id: str,
        result: Mapping[str, Any],
    ) -> None:
        changed = connection.execute(
            """
            UPDATE orch_commands
            SET status = ?, result_json = ?, completed_at = ?
            WHERE id = ? AND status = ?
            """,
            (
                CommandStatus.COMPLETED.value,
                _json(result),
                _stamp(_now()),
                command_id,
                CommandStatus.IN_PROGRESS.value,
            ),
        ).rowcount
        if changed != 1:
            raise IntegrityError(f"could not complete command {command_id}")

    @staticmethod
    def _command_id(command_id: Optional[str]) -> str:
        return command_id or _id("cmd")

    def get_command(self, command_id: str) -> CommandRecord:
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_commands WHERE id = ?", (command_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"command not found: {command_id}")
        return CommandRecord(
            id=row["id"],
            name=row["name"],
            scope=row["scope"],
            request_hash=row["request_hash"],
            status=CommandStatus(row["status"]),
            result=_load(row["result_json"]),
            error=row["error"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
            completed_at=_time(row["completed_at"]),
        )

    # -- structured handoff bootstrap ------------------------------------
    @staticmethod
    def _legacy_brief_for_spec(spec: TaskSpec) -> TaskBriefDraft:
        objective = (
            "[redacted legacy objective: use the runtime secret mechanism]"
            if contains_secret_like(spec.objective)
            else spec.objective
        )
        safe_constraints = tuple(
            "[redacted legacy constraint]" if contains_secret_like(item) else str(item)
            for item in spec.constraints
        )
        criteria = tuple(
            {
                "id": f"AC-{index:02d}",
                "text": (
                    "[redacted legacy acceptance criterion]"
                    if contains_secret_like(item)
                    else str(item)
                ),
                "verification": "legacy",
                "required": True,
            }
            for index, item in enumerate(spec.acceptance_criteria, 1)
            if str(item).strip()
        )
        return TaskBriefDraft(
            title=(
                "Redacted legacy task"
                if contains_secret_like(spec.title or spec.objective)
                else str(spec.title or objective)[:200]
            ),
            objective=objective,
            background="Synthetic compatibility contract for a legacy task.",
            scope={
                "whole_task": True,
                "reason": "Legacy TaskSpec did not carry an explicit bounded scope.",
            },
            instructions=(objective,),
            constraints=safe_constraints,
            acceptance_criteria=criteria,
            deliverables=(
                {
                    "id": "DEL-LEGACY-RESULT",
                    "kind": "other",
                    "title": "Legacy task result",
                    "required": False,
                },
            ),
            result_contract={
                "schema_id": "legacy_result_v1",
                "required_fields": ["summary"],
                "allow_freeform_summary": True,
            },
        )

    @classmethod
    def _legacy_brief_for_row(cls, row: sqlite3.Row) -> TaskBriefDraft:
        return cls._legacy_brief_for_spec(
            TaskSpec(
                idempotency_key=row["idempotency_key"],
                title=row["title"],
                objective=row["objective"],
                domain=TaskDomain(row["domain"]),
                workspace=row["workspace"],
                constraints=tuple(_load(row["constraints_json"], [])),
                acceptance_criteria=tuple(
                    _load(row["acceptance_criteria_json"], [])
                ),
                complexity_score=row["complexity_score"],
                complexity_level=(
                    ComplexityLevel(row["complexity_level"])
                    if row["complexity_level"] is not None
                    else None
                ),
                risk_tier=RiskTier(row["risk_tier"]),
                budget=_load(row["budget_json"], {}),
                policy=_load(row["policy_json"], {}),
                input=_load(row["input_json"], {}),
                priority=int(row["priority"]),
                max_parallel_runs=int(row["max_parallel_runs"]),
                parent_task_id=row["parent_task_id"],
                parent_node_id=row["parent_node_id"],
            )
        )

    def _backfill_legacy_briefs_connection(
        self, connection: sqlite3.Connection
    ) -> int:
        """Create one canonical synthetic published brief for every legacy task.

        The hook is intentionally idempotent and runs immediately after migrations,
        before the store becomes visible to a scheduler or API worker.
        """

        connection.execute("BEGIN IMMEDIATE")
        try:
            rows = connection.execute(
                """
                SELECT task.* FROM orch_tasks task
                LEFT JOIN orch_task_briefs brief ON brief.task_id = task.id
                WHERE brief.id IS NULL OR task.active_brief_id IS NULL
                ORDER BY task.created_at, task.id
                """
            ).fetchall()
            count = 0
            for row in rows:
                existing = connection.execute(
                    """
                    SELECT * FROM orch_task_briefs
                    WHERE task_id = ? AND status IN ('published', 'superseded')
                    ORDER BY revision DESC LIMIT 1
                    """,
                    (row["id"],),
                ).fetchone()
                if existing is None:
                    brief_id = self._insert_brief_record(
                        connection,
                        task_id=row["id"],
                        draft=self._legacy_brief_for_row(row),
                        status=BriefStatus.PUBLISHED,
                        created_by_task_id=row["parent_task_id"],
                        created_by_run_id=None,
                        context_refs=(),
                        command_id=f"legacy-brief-backfill:{row['id']}",
                    )
                    count += 1
                else:
                    brief_id = existing["id"]
                connection.execute(
                    "UPDATE orch_tasks SET active_brief_id = ? WHERE id = ?",
                    (brief_id, row["id"]),
                )
            connection.commit()
            return count
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise

    def backfill_legacy_briefs(self) -> int:
        with self._write() as connection:
            rows = connection.execute(
                """
                SELECT task.* FROM orch_tasks task
                LEFT JOIN orch_task_briefs brief ON brief.task_id = task.id
                WHERE brief.id IS NULL OR task.active_brief_id IS NULL
                ORDER BY task.created_at, task.id
                """
            ).fetchall()
            count = 0
            for row in rows:
                existing = connection.execute(
                    """
                    SELECT * FROM orch_task_briefs
                    WHERE task_id = ? AND status IN ('published', 'superseded')
                    ORDER BY revision DESC LIMIT 1
                    """,
                    (row["id"],),
                ).fetchone()
                if existing is None:
                    brief_id = self._insert_brief_record(
                        connection,
                        task_id=row["id"],
                        draft=self._legacy_brief_for_row(row),
                        status=BriefStatus.PUBLISHED,
                        created_by_task_id=row["parent_task_id"],
                        created_by_run_id=None,
                        context_refs=(),
                        command_id=f"legacy-brief-backfill:{row['id']}",
                    )
                    count += 1
                else:
                    brief_id = existing["id"]
                connection.execute(
                    "UPDATE orch_tasks SET active_brief_id = ? WHERE id = ?",
                    (brief_id, row["id"]),
                )
            return count

    def _backfill_parent_relations_connection(
        self, connection: sqlite3.Connection
    ) -> int:
        connection.execute("BEGIN IMMEDIATE")
        try:
            rows = connection.execute(
                """
                SELECT child.id AS child_id, child.parent_task_id AS parent_id,
                       child.parent_node_id AS parent_node_id
                FROM orch_tasks child
                LEFT JOIN orch_task_relations relation
                  ON relation.from_task_id = child.parent_task_id
                 AND relation.to_task_id = child.id
                 AND relation.relation_type = 'parent'
                 AND relation.removed_at IS NULL
                WHERE child.parent_task_id IS NOT NULL AND relation.id IS NULL
                ORDER BY child.created_at, child.id
                """
            ).fetchall()
            for row in rows:
                relation_id = _id("relation")
                connection.execute(
                    """
                    INSERT INTO orch_task_relations(
                        id, from_task_id, to_task_id, relation_type, metadata_json,
                        created_by_task_id, created_at
                    ) VALUES (?, ?, ?, 'parent', ?, ?, ?)
                    """,
                    (
                        relation_id,
                        row["parent_id"],
                        row["child_id"],
                        _json({"legacy_backfill": True, "parent_node_id": row["parent_node_id"]}),
                        row["parent_id"],
                        _stamp(_now()),
                    ),
                )
                self._append_event(
                    connection,
                    task_id=row["child_id"],
                    aggregate_type="task_relation",
                    aggregate_id=relation_id,
                    event_type="relation_added",
                    payload={
                        "from_task_id": row["parent_id"],
                        "to_task_id": row["child_id"],
                        "relation_type": "parent",
                        "legacy_backfill": True,
                    },
                    command_id=f"legacy-parent-relation:{row['child_id']}",
                )
            connection.commit()
            return len(rows)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise

    def _insert_brief_record(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        draft: TaskBriefDraft,
        status: BriefStatus,
        created_by_task_id: Optional[str],
        created_by_run_id: Optional[str],
        context_refs: Sequence[ContextRefDraft],
        command_id: Optional[str],
    ) -> str:
        revision = int(
            connection.execute(
                "SELECT COALESCE(MAX(revision), 0) + 1 FROM orch_task_briefs WHERE task_id = ?",
                (task_id,),
            ).fetchone()[0]
        )
        brief_id = _id("brief")
        created_at = _stamp(_now())
        published_at = created_at if status is BriefStatus.PUBLISHED else None
        connection.execute(
            """
            INSERT INTO orch_task_briefs(
                id, task_id, revision, status, title, objective, background,
                scope_json, instructions_json, constraints_json, non_goals_json,
                acceptance_criteria_json, deliverables_json, result_contract_json,
                created_by_task_id, created_by_run_id, content_hash, created_at,
                published_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                brief_id,
                task_id,
                revision,
                status.value,
                draft.title,
                draft.objective,
                draft.background,
                _json(draft.scope),
                _json(draft.instructions),
                _json(draft.constraints),
                _json(draft.non_goals),
                _json(draft.acceptance_criteria),
                _json(draft.deliverables),
                _json(draft.result_contract),
                created_by_task_id,
                created_by_run_id,
                draft.content_hash,
                created_at,
                published_at,
            ),
        )
        self._append_event(
            connection,
            task_id=task_id,
            aggregate_type="task_brief",
            aggregate_id=brief_id,
            event_type="brief_draft_created",
            payload={"revision": revision, "content_hash": draft.content_hash},
            command_id=command_id,
        )
        for context_ref in context_refs:
            self._insert_context_ref(
                connection,
                task_id=task_id,
                brief_id=brief_id,
                draft=context_ref,
                created_by_task_id=created_by_task_id,
                created_by_run_id=created_by_run_id,
                command_id=command_id,
            )
        if status is BriefStatus.PUBLISHED:
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task_brief",
                aggregate_id=brief_id,
                event_type="brief_published",
                payload={"revision": revision, "content_hash": draft.content_hash},
                command_id=command_id,
            )
        return brief_id

    def _insert_context_ref(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        brief_id: str,
        draft: ContextRefDraft,
        created_by_task_id: Optional[str],
        created_by_run_id: Optional[str],
        command_id: Optional[str],
    ) -> str:
        ref_id = _id("ctx")
        connection.execute(
            """
            INSERT INTO orch_context_refs(
                id, task_id, brief_id, requirement, ref_type, display_name,
                summary, selection_reason, locator_json, delivery_mode, mime_type,
                content_hash, byte_size, token_estimate, provenance_json,
                trust_level, created_by_task_id, created_by_run_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ref_id,
                task_id,
                brief_id,
                ContextRequirement(draft.requirement).value,
                ContextRefType(draft.ref_type).value,
                draft.display_name,
                draft.summary,
                draft.selection_reason,
                _json(draft.locator),
                ContextDeliveryMode(draft.delivery_mode).value,
                draft.mime_type,
                draft.content_hash,
                draft.byte_size,
                draft.token_estimate,
                _json(draft.provenance),
                draft.trust_level,
                created_by_task_id,
                created_by_run_id,
                _stamp(_now()),
            ),
        )
        self._append_event(
            connection,
            task_id=task_id,
            aggregate_type="context_ref",
            aggregate_id=ref_id,
            event_type="context_ref_added",
            payload={
                "brief_id": brief_id,
                "ref_type": ContextRefType(draft.ref_type).value,
                "delivery_mode": ContextDeliveryMode(draft.delivery_mode).value,
            },
            command_id=command_id,
        )
        return ref_id

    # -- task aggregate -----------------------------------------------------
    def create_task(
        self,
        spec: TaskSpec,
        *,
        brief: Optional[TaskBriefDraft | Mapping[str, Any]] = None,
        context_refs: Sequence[ContextRefDraft | Mapping[str, Any]] = (),
        publish_brief: bool = True,
        command_id: Optional[str] = None,
    ) -> TaskRecord:
        if not spec.idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        if not spec.objective.strip():
            raise ValueError("objective is required")
        if spec.max_parallel_runs < 1:
            raise ValueError("max_parallel_runs must be >= 1")
        command_id = self._command_id(command_id)
        chosen_brief = (
            brief
            if isinstance(brief, TaskBriefDraft)
            else TaskBriefDraft.from_mapping(brief)
            if isinstance(brief, Mapping)
            else self._legacy_brief_for_spec(spec)
        )
        chosen_refs = tuple(
            item
            if isinstance(item, ContextRefDraft)
            else ContextRefDraft.from_mapping(item)
            for item in context_refs
        )
        if publish_brief:
            chosen_brief.validate(informational=brief is None and not spec.acceptance_criteria)
        legacy_creation_hash = _digest(_jsonable(spec))
        creation = {
            "task": _jsonable(spec),
            "brief": chosen_brief.to_dict(),
            "context_refs": [item.to_dict() for item in chosen_refs],
            "publish_brief": bool(publish_brief),
        }
        legacy_request = brief is None and not chosen_refs and publish_brief
        creation_hash = legacy_creation_hash if legacy_request else _digest(creation)
        command_request: Any = _jsonable(spec) if legacy_request else creation
        with self._write() as connection:
            replay = self._start_command(
                connection,
                command_id,
                "task.create",
                spec.idempotency_key,
                command_request,
            )
            if replay is not None:
                return self._require_task(connection, replay["task_id"])

            existing = connection.execute(
                "SELECT * FROM orch_tasks WHERE idempotency_key = ?",
                (spec.idempotency_key,),
            ).fetchone()
            if existing is not None:
                compatible_hashes = {creation_hash}
                if brief is None and not chosen_refs and publish_brief:
                    compatible_hashes.add(legacy_creation_hash)
                if existing["creation_hash"] not in compatible_hashes:
                    raise IdempotencyConflict(
                        f"task key {spec.idempotency_key} was reused with different input"
                    )
                self._finish_command(connection, command_id, {"task_id": existing["id"]})
                return self._task_from_row(existing)

            task_id = _id("task")
            stage_id = _id("stage")
            now = _stamp(_now())
            title = (spec.title or spec.objective).strip()[:200]
            connection.execute(
                """
                INSERT INTO orch_tasks(
                    id, idempotency_key, creation_hash, title, objective, domain,
                    workspace, constraints_json, acceptance_criteria_json,
                    complexity_score, complexity_level, risk_tier, budget_json,
                    policy_json, input_json, status, current_stage, parent_task_id,
                    parent_node_id, priority, max_parallel_runs, version, created_at,
                    updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?
                )
                """,
                (
                    task_id,
                    spec.idempotency_key,
                    creation_hash,
                    title,
                    spec.objective,
                    TaskDomain(spec.domain).value,
                    spec.workspace,
                    _json(tuple(spec.constraints)),
                    _json(tuple(spec.acceptance_criteria)),
                    spec.complexity_score,
                    spec.complexity_level.value if spec.complexity_level else None,
                    RiskTier(spec.risk_tier).value,
                    _json(spec.budget),
                    _json(spec.policy),
                    _json(spec.input),
                    TaskStatus.DRAFT.value,
                    OrchestrationStage.INTAKE.value,
                    spec.parent_task_id,
                    spec.parent_node_id,
                    spec.priority,
                    spec.max_parallel_runs,
                    now,
                    now,
                ),
            )
            brief_id = self._insert_brief_record(
                connection,
                task_id=task_id,
                draft=chosen_brief,
                status=(BriefStatus.PUBLISHED if publish_brief else BriefStatus.DRAFT),
                created_by_task_id=spec.parent_task_id,
                created_by_run_id=None,
                context_refs=chosen_refs,
                command_id=command_id,
            )
            if publish_brief:
                connection.execute(
                    "UPDATE orch_tasks SET active_brief_id = ? WHERE id = ?",
                    (brief_id, task_id),
                )
            connection.execute(
                """
                INSERT INTO orch_stage_history(
                    id, task_id, sequence_no, stage, disposition, entered_at,
                    detail_json, command_id
                ) VALUES (?, ?, 1, ?, ?, ?, '{}', ?)
                """,
                (
                    stage_id,
                    task_id,
                    OrchestrationStage.INTAKE.value,
                    StageDisposition.ACTIVE.value,
                    now,
                    command_id,
                ),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task",
                aggregate_id=task_id,
                event_type="task.created",
                payload={
                    "status": TaskStatus.DRAFT.value,
                    "stage": OrchestrationStage.INTAKE.value,
                    "objective": spec.objective,
                },
                command_id=command_id,
            )
            if spec.parent_task_id:
                # Keep the legacy projection and the first-class graph edge in the
                # same transaction.  Older callers may still populate only
                # ``parent_task_id``; readers must never observe a task whose graph
                # says something different.
                self._require_task(connection, spec.parent_task_id)
                relation_id = _id("relation")
                connection.execute(
                    """
                    INSERT INTO orch_task_relations(
                        id, from_task_id, to_task_id, relation_type, metadata_json,
                        created_by_task_id, created_at
                    ) VALUES (?, ?, ?, 'parent', ?, ?, ?)
                    """,
                    (
                        relation_id,
                        spec.parent_task_id,
                        task_id,
                        _json({"parent_node_id": spec.parent_node_id}),
                        spec.parent_task_id,
                        now,
                    ),
                )
                self._append_event(
                    connection,
                    task_id=task_id,
                    aggregate_type="task_relation",
                    aggregate_id=relation_id,
                    event_type="relation_added",
                    payload={
                        "from_task_id": spec.parent_task_id,
                        "to_task_id": task_id,
                        "relation_type": "parent",
                    },
                    command_id=command_id,
                )
            self._finish_command(connection, command_id, {"task_id": task_id})
            return self._require_task(connection, task_id)

    def create_delegated_task(
        self,
        spec: TaskSpec,
        *,
        parent_run_id: str,
        lease_token: str,
        fencing_token: int,
        brief: TaskBriefDraft | Mapping[str, Any],
        context_refs: Sequence[ContextRefDraft | Mapping[str, Any]] = (),
        blocked_by_task_ids: Sequence[str] = (),
        command_id: str,
    ) -> dict[str, Any]:
        """Atomically persist a complete child handoff and scheduling intent."""

        if not spec.parent_task_id or not spec.parent_node_id:
            raise ValueError("delegated tasks require parent task and node ids")
        if not spec.idempotency_key.strip():
            raise ValueError("delegated task idempotency key is required")
        chosen_brief = brief if isinstance(brief, TaskBriefDraft) else TaskBriefDraft.from_mapping(brief)
        chosen_brief.validate()
        chosen_refs = tuple(
            item if isinstance(item, ContextRefDraft) else ContextRefDraft.from_mapping(item)
            for item in context_refs
        )
        blockers = tuple(
            dict.fromkeys(str(item).strip() for item in blocked_by_task_ids if str(item).strip())
        )
        creation = {
            "task": _jsonable(spec),
            "parent_run_id": parent_run_id,
            "brief": chosen_brief.to_dict(),
            "context_refs": [item.to_dict() for item in chosen_refs],
            "blocked_by_task_ids": list(blockers),
        }
        creation_hash = _digest(creation)
        with self._write() as connection:
            replay = self._start_command(
                connection,
                str(command_id),
                "task.delegate",
                f"{spec.parent_task_id}:{spec.idempotency_key}",
                creation,
            )
            if replay is not None:
                task = self._require_task(connection, replay["task_id"])
                brief_record = self._require_brief(connection, replay["brief_id"])
                return {
                    "task": task,
                    "brief": brief_record,
                    "wake": (
                        self._require_wake(connection, replay["wake_id"])
                        if replay.get("wake_id")
                        else None
                    ),
                    "replayed": True,
                }
            parent = self._require_task(connection, spec.parent_task_id)
            parent_run = self._require_run(connection, parent_run_id)
            if parent_run.task_id != parent.id or parent_run.node_id != spec.parent_node_id:
                raise LeaseConflict("delegation run does not own the parent task/node")
            if parent_run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise LeaseConflict("delegation run is no longer active")
            self._require_lease(
                connection, parent_run.id, lease_token, int(fencing_token)
            )
            existing = connection.execute(
                "SELECT * FROM orch_tasks WHERE idempotency_key = ?",
                (spec.idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["creation_hash"] != creation_hash:
                    raise IdempotencyConflict(
                        f"task key {spec.idempotency_key} was reused with different input"
                    )
                brief_row = connection.execute(
                    "SELECT * FROM orch_task_briefs WHERE id = ?",
                    (existing["active_brief_id"],),
                ).fetchone()
                if brief_row is None:
                    raise IntegrityError("delegated task exists without a published brief")
                wake_row = connection.execute(
                    """
                    SELECT * FROM orch_wake_requests
                    WHERE target_task_id = ? AND reason = 'task_assigned'
                    ORDER BY created_at LIMIT 1
                    """,
                    (existing["id"],),
                ).fetchone()
                result = {
                    "task_id": existing["id"],
                    "brief_id": brief_row["id"],
                    "wake_id": wake_row["id"] if wake_row else None,
                }
                self._finish_command(connection, str(command_id), result)
                return {
                    "task": self._task_from_row(existing),
                    "brief": self._brief_from_row(brief_row),
                    "wake": self._wake_from_row(wake_row) if wake_row else None,
                    "replayed": True,
                }
            for blocker_id in blockers:
                if blocker_id == spec.parent_task_id:
                    # Parent-as-blocker is legal only if an external event can make
                    # progress; in a strict parent/child tree it creates a wait cycle.
                    raise ConflictError("a parent task cannot block its own child delegation")
                self._require_task(connection, blocker_id)
            task_id = _id("task")
            stage_id = _id("stage")
            now = _stamp(_now())
            unresolved = []
            for blocker_id in blockers:
                blocker = self._require_task(connection, blocker_id)
                if blocker.status is not TaskStatus.COMPLETED:
                    archived_from = str((blocker.output or {}).get("archived_from") or "")
                    if blocker.status is not TaskStatus.ARCHIVED or archived_from != "completed":
                        unresolved.append(blocker_id)
            initial_status = TaskStatus.BLOCKED if unresolved else TaskStatus.QUEUED
            connection.execute(
                """
                INSERT INTO orch_tasks(
                    id, idempotency_key, creation_hash, title, objective, domain,
                    workspace, constraints_json, acceptance_criteria_json,
                    complexity_score, complexity_level, risk_tier, budget_json,
                    policy_json, input_json, status, current_stage, parent_task_id,
                    parent_node_id, priority, max_parallel_runs, version, created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    task_id,
                    spec.idempotency_key,
                    creation_hash,
                    chosen_brief.title,
                    chosen_brief.objective,
                    TaskDomain(spec.domain).value,
                    spec.workspace,
                    _json(chosen_brief.constraints),
                    _json(tuple(str(item.get("text") or "") for item in chosen_brief.acceptance_criteria)),
                    spec.complexity_score,
                    spec.complexity_level.value if spec.complexity_level else None,
                    RiskTier(spec.risk_tier).value,
                    _json(spec.budget),
                    _json(spec.policy),
                    _json(spec.input),
                    initial_status.value,
                    OrchestrationStage.INTAKE.value,
                    spec.parent_task_id,
                    spec.parent_node_id,
                    spec.priority,
                    spec.max_parallel_runs,
                    now,
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO orch_stage_history(
                    id, task_id, sequence_no, stage, disposition, entered_at,
                    detail_json, command_id
                ) VALUES (?, ?, 1, 'intake', 'active', ?, '{}', ?)
                """,
                (stage_id, task_id, now, str(command_id)),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task",
                aggregate_id=task_id,
                event_type="task.created",
                payload={"status": initial_status.value, "stage": "intake", "objective": chosen_brief.objective},
                command_id=str(command_id),
            )
            brief_id = self._insert_brief_record(
                connection,
                task_id=task_id,
                draft=chosen_brief,
                status=BriefStatus.PUBLISHED,
                created_by_task_id=parent.id,
                created_by_run_id=parent_run.id,
                context_refs=chosen_refs,
                command_id=str(command_id),
            )
            connection.execute(
                "UPDATE orch_tasks SET active_brief_id = ? WHERE id = ?",
                (brief_id, task_id),
            )
            parent_relation_id = _id("relation")
            connection.execute(
                """
                INSERT INTO orch_task_relations(
                    id, from_task_id, to_task_id, relation_type, metadata_json,
                    created_by_task_id, created_by_run_id, created_at
                ) VALUES (?, ?, ?, 'parent', ?, ?, ?, ?)
                """,
                (
                    parent_relation_id,
                    parent.id,
                    task_id,
                    _json({"parent_node_id": parent_run.node_id}),
                    parent.id,
                    parent_run.id,
                    now,
                ),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task_relation",
                aggregate_id=parent_relation_id,
                event_type="relation_added",
                payload={"from_task_id": parent.id, "to_task_id": task_id, "relation_type": "parent"},
                command_id=str(command_id),
            )
            for blocker_id in blockers:
                relation_id = _id("relation")
                connection.execute(
                    """
                    INSERT INTO orch_task_relations(
                        id, from_task_id, to_task_id, relation_type, metadata_json,
                        created_by_task_id, created_by_run_id, created_at
                    ) VALUES (?, ?, ?, 'blocks', '{}', ?, ?, ?)
                    """,
                    (relation_id, blocker_id, task_id, parent.id, parent_run.id, now),
                )
                self._append_event(
                    connection,
                    task_id=task_id,
                    aggregate_type="task_relation",
                    aggregate_id=relation_id,
                    event_type="relation_added",
                    payload={"from_task_id": blocker_id, "to_task_id": task_id, "relation_type": "blocks"},
                    command_id=str(command_id),
                )
            delegated_event = self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task",
                aggregate_id=task_id,
                event_type="task_delegated",
                payload={
                    "parent_task_id": parent.id,
                    "parent_run_id": parent_run.id,
                    "brief_id": brief_id,
                    "brief_revision": 1,
                    "context_ref_count": len(chosen_refs),
                    "blocked_by_task_ids": list(blockers),
                },
                command_id=str(command_id),
            )
            if bool(spec.policy.get("legacy_delegation")):
                self._append_event(
                    connection,
                    task_id=task_id,
                    aggregate_type="task",
                    aggregate_id=task_id,
                    event_type="legacy_delegation_used",
                    payload={
                        "parent_task_id": parent.id,
                        "parent_run_id": parent_run.id,
                    },
                    command_id=str(command_id),
                )
            wake = None
            if initial_status is TaskStatus.QUEUED:
                wake = self._enqueue_wake_connection(
                    connection,
                    target_task_id=task_id,
                    target_run_id=None,
                    reason=WakeReason.TASK_ASSIGNED,
                    source_task_id=parent.id,
                    source_run_id=parent_run.id,
                    source_event_id=delegated_event.id,
                    payload={"brief_id": brief_id, "brief_revision": 1, "parent_task_id": parent.id},
                    dedupe_key=f"{task_id}:current:task_assigned:{brief_id}",
                    not_before=None,
                    command_id=str(command_id),
                )
            result = {
                "task_id": task_id,
                "brief_id": brief_id,
                "wake_id": wake.id if wake else None,
            }
            self._finish_command(connection, str(command_id), result)
            return {
                "task": self._require_task(connection, task_id),
                "brief": self._require_brief(connection, brief_id),
                "wake": wake,
                "replayed": False,
            }

    # -- versioned task briefs and context manifests ---------------------
    def create_brief_draft(
        self,
        task_id: str,
        draft: TaskBriefDraft | Mapping[str, Any],
        *,
        created_by_task_id: Optional[str] = None,
        created_by_run_id: Optional[str] = None,
        copy_context_from_brief_id: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> TaskBriefRecord:
        command_id = self._command_id(command_id)
        chosen = draft if isinstance(draft, TaskBriefDraft) else TaskBriefDraft.from_mapping(draft)
        request = {
            "task_id": task_id,
            "brief": chosen.to_dict(),
            "created_by_task_id": created_by_task_id,
            "created_by_run_id": created_by_run_id,
            "copy_context_from_brief_id": copy_context_from_brief_id,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "brief.create_draft", task_id, request
            )
            if replay is not None:
                return self._require_brief(connection, replay["brief_id"])
            self._require_task(connection, task_id)
            refs: tuple[ContextRefDraft, ...] = ()
            if copy_context_from_brief_id:
                source = self._require_brief(connection, copy_context_from_brief_id)
                if source.task_id != task_id:
                    raise ConflictError("context may only be copied within one task")
                rows = connection.execute(
                    "SELECT * FROM orch_context_refs WHERE brief_id = ? ORDER BY created_at, id",
                    (source.id,),
                ).fetchall()
                refs = tuple(
                    ContextRefDraft(
                        requirement=row["requirement"],
                        ref_type=row["ref_type"],
                        display_name=row["display_name"],
                        summary=row["summary"],
                        selection_reason=row["selection_reason"],
                        locator=_load(row["locator_json"], {}),
                        delivery_mode=row["delivery_mode"],
                        mime_type=row["mime_type"],
                        content_hash=row["content_hash"],
                        byte_size=row["byte_size"],
                        token_estimate=row["token_estimate"],
                        provenance=_load(row["provenance_json"], {}),
                        trust_level=row["trust_level"],
                    )
                    for row in rows
                )
            brief_id = self._insert_brief_record(
                connection,
                task_id=task_id,
                draft=chosen,
                status=BriefStatus.DRAFT,
                created_by_task_id=created_by_task_id,
                created_by_run_id=created_by_run_id,
                context_refs=refs,
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"brief_id": brief_id})
            return self._require_brief(connection, brief_id)

    def update_brief_draft(
        self,
        task_id: str,
        revision: int,
        draft: TaskBriefDraft | Mapping[str, Any],
        *,
        expected_hash: str,
        command_id: Optional[str] = None,
    ) -> TaskBriefRecord:
        chosen = draft if isinstance(draft, TaskBriefDraft) else TaskBriefDraft.from_mapping(draft)
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "revision": int(revision),
            "brief": chosen.to_dict(),
            "expected_hash": expected_hash,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "brief.update_draft", task_id, request
            )
            if replay is not None:
                return self._require_brief(connection, replay["brief_id"])
            row = connection.execute(
                "SELECT * FROM orch_task_briefs WHERE task_id = ? AND revision = ?",
                (task_id, int(revision)),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"brief revision not found: {task_id}@{revision}")
            if BriefStatus(row["status"]) is not BriefStatus.DRAFT:
                raise ConflictError("published task briefs are immutable; create a new revision")
            if str(row["content_hash"]) != str(expected_hash):
                raise VersionConflict("task brief draft changed since it was loaded")
            connection.execute(
                """
                UPDATE orch_task_briefs SET
                    title = ?, objective = ?, background = ?, scope_json = ?,
                    instructions_json = ?, constraints_json = ?, non_goals_json = ?,
                    acceptance_criteria_json = ?, deliverables_json = ?,
                    result_contract_json = ?, content_hash = ?
                WHERE id = ? AND status = 'draft' AND content_hash = ?
                """,
                (
                    chosen.title,
                    chosen.objective,
                    chosen.background,
                    _json(chosen.scope),
                    _json(chosen.instructions),
                    _json(chosen.constraints),
                    _json(chosen.non_goals),
                    _json(chosen.acceptance_criteria),
                    _json(chosen.deliverables),
                    _json(chosen.result_contract),
                    chosen.content_hash,
                    row["id"],
                    expected_hash,
                ),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task_brief",
                aggregate_id=row["id"],
                event_type="brief_draft_updated",
                payload={"revision": int(revision), "content_hash": chosen.content_hash},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"brief_id": row["id"]})
            return self._require_brief(connection, row["id"])

    def publish_brief(
        self,
        task_id: str,
        revision: int,
        *,
        expected_previous_revision: Optional[int] = None,
        required_fields: Sequence[str] = (
            "objective",
            "scope",
            "acceptance_criteria",
            "deliverables",
        ),
        informational: bool = False,
        command_id: Optional[str] = None,
    ) -> TaskBriefRecord:
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "revision": int(revision),
            "expected_previous_revision": expected_previous_revision,
            "required_fields": list(required_fields),
            "informational": bool(informational),
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "brief.publish", task_id, request
            )
            if replay is not None:
                return self._require_brief(connection, replay["brief_id"])
            task = self._require_task(connection, task_id)
            row = connection.execute(
                "SELECT * FROM orch_task_briefs WHERE task_id = ? AND revision = ?",
                (task_id, int(revision)),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"brief revision not found: {task_id}@{revision}")
            if BriefStatus(row["status"]) is not BriefStatus.DRAFT:
                raise ConflictError("only a draft task brief can be published")
            current = (
                self._require_brief(connection, task.active_brief_id)
                if task.active_brief_id
                else None
            )
            actual_previous = current.revision if current else 0
            if (
                expected_previous_revision is not None
                and int(expected_previous_revision) != actual_previous
            ):
                raise VersionConflict(
                    f"expected active brief revision {expected_previous_revision}, found {actual_previous}"
                )
            draft = self._brief_draft_from_row(row)
            draft.validate(
                required_fields=required_fields,
                informational=informational,
            )
            now = _stamp(_now())
            if current and current.status is BriefStatus.PUBLISHED:
                connection.execute(
                    "UPDATE orch_task_briefs SET status = 'superseded' WHERE id = ?",
                    (current.id,),
                )
            connection.execute(
                "UPDATE orch_task_briefs SET status = 'published', published_at = ? WHERE id = ?",
                (now, row["id"]),
            )
            connection.execute(
                "UPDATE orch_tasks SET active_brief_id = ?, version = version + 1, updated_at = ? WHERE id = ?",
                (row["id"], now, task_id),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task_brief",
                aggregate_id=row["id"],
                event_type="brief_published",
                payload={"revision": int(revision), "content_hash": row["content_hash"]},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"brief_id": row["id"]})
            return self._require_brief(connection, row["id"])

    def get_active_brief(self, task_id: str) -> TaskBriefRecord:
        with self._read() as connection:
            task = self._require_task(connection, task_id)
            if not task.active_brief_id:
                raise NotFoundError(f"task {task_id} has no published brief")
            return self._require_brief(connection, task.active_brief_id)

    def get_brief_by_id(self, brief_id: str) -> TaskBriefRecord:
        """Return an exact immutable revision by its durable identity."""

        with self._read() as connection:
            return self._require_brief(connection, brief_id)

    def get_brief(self, task_id: str, revision: int) -> TaskBriefRecord:
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_task_briefs WHERE task_id = ? AND revision = ?",
                (task_id, int(revision)),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"brief revision not found: {task_id}@{revision}")
        return self._brief_from_row(row)

    def list_briefs(self, task_id: str) -> tuple[TaskBriefRecord, ...]:
        with self._read() as connection:
            self._require_task(connection, task_id)
            rows = connection.execute(
                "SELECT * FROM orch_task_briefs WHERE task_id = ? ORDER BY revision",
                (task_id,),
            ).fetchall()
        return tuple(self._brief_from_row(row) for row in rows)

    def add_context_ref(
        self,
        task_id: str,
        brief_id: str,
        draft: ContextRefDraft | Mapping[str, Any],
        *,
        created_by_task_id: Optional[str] = None,
        created_by_run_id: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> ContextRefRecord:
        chosen = draft if isinstance(draft, ContextRefDraft) else ContextRefDraft.from_mapping(draft)
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "brief_id": brief_id,
            "context_ref": chosen.to_dict(),
            "created_by_task_id": created_by_task_id,
            "created_by_run_id": created_by_run_id,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "context_ref.add", task_id, request
            )
            if replay is not None:
                return self._require_context_ref(connection, replay["ref_id"])
            brief = self._require_brief(connection, brief_id)
            if brief.task_id != task_id:
                raise ConflictError("context reference brief does not belong to task")
            if brief.status is not BriefStatus.DRAFT:
                raise ConflictError("context references can only be added to a draft brief")
            ref_id = self._insert_context_ref(
                connection,
                task_id=task_id,
                brief_id=brief_id,
                draft=chosen,
                created_by_task_id=created_by_task_id,
                created_by_run_id=created_by_run_id,
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"ref_id": ref_id})
            return self._require_context_ref(connection, ref_id)

    def backfill_legacy_upstream_ref(
        self,
        task_id: str,
        draft: ContextRefDraft,
    ) -> ContextRefRecord:
        """Attach one idempotent compatibility ref to a published synthetic Brief."""

        command_id = f"legacy-upstream-context:{task_id}"
        request = {"task_id": task_id, "context_ref": draft.to_dict()}
        with self._write() as connection:
            replay = self._start_command(
                connection,
                command_id,
                "context_ref.backfill_legacy_upstream",
                task_id,
                request,
            )
            if replay is not None:
                return self._require_context_ref(connection, replay["ref_id"])
            task = self._require_task(connection, task_id)
            if not task.active_brief_id:
                raise ConflictError("legacy upstream backfill requires an active Brief")
            rows = connection.execute(
                """
                SELECT * FROM orch_context_refs
                WHERE task_id = ? AND brief_id = ?
                ORDER BY created_at, id
                """,
                (task_id, task.active_brief_id),
            ).fetchall()
            existing = next(
                (
                    row
                    for row in rows
                    if str((_load(row["provenance_json"], {}) or {}).get("source"))
                    == "legacy_upstream"
                ),
                None,
            )
            if existing is not None:
                ref_id = str(existing["id"])
            else:
                ref_id = self._insert_context_ref(
                    connection,
                    task_id=task_id,
                    brief_id=task.active_brief_id,
                    draft=draft,
                    created_by_task_id=task.parent_task_id,
                    created_by_run_id=None,
                    command_id=command_id,
                )
            self._finish_command(connection, command_id, {"ref_id": ref_id})
            return self._require_context_ref(connection, ref_id)

    def remove_context_ref(
        self,
        task_id: str,
        ref_id: str,
        *,
        command_id: Optional[str] = None,
    ) -> None:
        command_id = self._command_id(command_id)
        request = {"task_id": task_id, "ref_id": ref_id}
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "context_ref.remove", task_id, request
            )
            if replay is not None:
                return
            ref = self._require_context_ref(connection, ref_id)
            if ref.task_id != task_id:
                raise NotFoundError(f"context reference not found: {ref_id}")
            brief = self._require_brief(connection, ref.brief_id)
            if brief.status is not BriefStatus.DRAFT:
                raise ConflictError("published context references are immutable")
            connection.execute("DELETE FROM orch_context_refs WHERE id = ?", (ref_id,))
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="context_ref",
                aggregate_id=ref_id,
                event_type="context_ref_removed",
                payload={"brief_id": ref.brief_id},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"removed": True})

    def get_context_ref(self, ref_id: str) -> ContextRefRecord:
        with self._read() as connection:
            return self._require_context_ref(connection, ref_id)

    def list_context_refs(
        self,
        task_id: str,
        *,
        brief_id: Optional[str] = None,
        requirement: Optional[ContextRequirement] = None,
        ref_type: Optional[ContextRefType] = None,
        limit: int = 1_000,
        offset: int = 0,
    ) -> tuple[ContextRefRecord, ...]:
        limit = max(1, min(int(limit), 10_000))
        offset = max(0, int(offset))
        where = ["task_id = ?"]
        params: list[Any] = [task_id]
        if brief_id:
            where.append("brief_id = ?")
            params.append(brief_id)
        if requirement is not None:
            where.append("requirement = ?")
            params.append(ContextRequirement(requirement).value)
        if ref_type is not None:
            where.append("ref_type = ?")
            params.append(ContextRefType(ref_type).value)
        params.extend((limit, offset))
        with self._read() as connection:
            self._require_task(connection, task_id)
            rows = connection.execute(
                "SELECT * FROM orch_context_refs WHERE "
                + " AND ".join(where)
                + " ORDER BY created_at, id LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        return tuple(self._context_ref_from_row(row) for row in rows)

    def record_context_ref_read(
        self,
        ref_id: str,
        *,
        run_id: Optional[str],
        bytes_read: int,
        content_hash: Optional[str],
        stale: bool,
        command_id: Optional[str] = None,
    ) -> None:
        command_id = self._command_id(command_id)
        request = {
            "ref_id": ref_id,
            "run_id": run_id,
            "bytes_read": max(0, int(bytes_read)),
            "content_hash": content_hash,
            "stale": bool(stale),
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "context_ref.read", ref_id, request
            )
            if replay is not None:
                return
            ref = self._require_context_ref(connection, ref_id)
            self._append_event(
                connection,
                task_id=ref.task_id,
                aggregate_type="context_ref",
                aggregate_id=ref.id,
                event_type="context_ref_read",
                payload=request,
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"recorded": True})

    def record_context_ref_verification(
        self,
        ref_id: str,
        *,
        run_id: Optional[str],
        result: Mapping[str, Any],
        command_id: Optional[str] = None,
    ) -> None:
        """Append a content-free verification/staleness fact to the audit chain."""

        command_id = self._command_id(command_id)
        safe_result = {
            key: value
            for key, value in dict(result).items()
            if key
            in {
                "available",
                "content_hash",
                "expected_hash",
                "stale",
                "byte_size",
                "reason",
            }
        }
        request = {"ref_id": ref_id, "run_id": run_id, **safe_result}
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "context_ref.verify", ref_id, request
            )
            if replay is not None:
                return
            ref = self._require_context_ref(connection, ref_id)
            self._append_event(
                connection,
                task_id=ref.task_id,
                aggregate_type="context_ref",
                aggregate_id=ref.id,
                event_type=(
                    "context_ref_stale"
                    if bool(safe_result.get("stale"))
                    else "context_ref_verified"
                ),
                payload=request,
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"recorded": True})

    # -- task relations ---------------------------------------------------
    @staticmethod
    def _relation_cycle_path(
        connection: sqlite3.Connection,
        from_task_id: str,
        to_task_id: str,
        relation_type: TaskRelationType,
    ) -> Optional[list[str]]:
        row = connection.execute(
            """
            WITH RECURSIVE walk(task_id, path) AS (
                SELECT ?, ?
                UNION ALL
                SELECT relation.to_task_id, walk.path || '>' || relation.to_task_id
                FROM orch_task_relations relation
                JOIN walk ON relation.from_task_id = walk.task_id
                WHERE relation.relation_type = ? AND relation.removed_at IS NULL
                  AND instr('>' || walk.path || '>', '>' || relation.to_task_id || '>') = 0
            )
            SELECT path FROM walk WHERE task_id = ? LIMIT 1
            """,
            (to_task_id, to_task_id, relation_type.value, from_task_id),
        ).fetchone()
        if row is None:
            return None
        return [from_task_id, *str(row["path"]).split(">")]

    def add_relation(
        self,
        from_task_id: str,
        to_task_id: str,
        relation_type: TaskRelationType | str,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
        created_by_task_id: Optional[str] = None,
        created_by_run_id: Optional[str] = None,
        lease_token: Optional[str] = None,
        fencing_token: Optional[int] = None,
        command_id: Optional[str] = None,
    ) -> TaskRelationRecord:
        relation_type = TaskRelationType(relation_type)
        if from_task_id == to_task_id:
            raise ConflictError("a task cannot relate to itself")
        if contains_secret_like(dict(metadata or {})):
            raise ValueError("relation metadata cannot contain secret-like values")
        command_id = self._command_id(command_id)
        request = {
            "from_task_id": from_task_id,
            "to_task_id": to_task_id,
            "relation_type": relation_type.value,
            "metadata": dict(metadata or {}),
            "created_by_task_id": created_by_task_id,
            "created_by_run_id": created_by_run_id,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection,
                command_id,
                "relation.add",
                f"{from_task_id}:{to_task_id}",
                request,
            )
            if replay is not None:
                return self._require_relation(connection, replay["relation_id"])
            self._require_task(connection, from_task_id)
            target_task = self._require_task(connection, to_task_id)
            if created_by_run_id:
                if lease_token is None or fencing_token is None:
                    raise LeaseConflict("run-bound relation writes require lease and fencing tokens")
                run = self._require_run(connection, created_by_run_id)
                self._require_lease(connection, run.id, lease_token, fencing_token)
                if created_by_task_id and run.task_id != created_by_task_id:
                    raise LeaseConflict("relation actor task does not own the verified run")
            existing = connection.execute(
                """
                SELECT * FROM orch_task_relations
                WHERE from_task_id = ? AND to_task_id = ? AND relation_type = ?
                  AND removed_at IS NULL
                """,
                (from_task_id, to_task_id, relation_type.value),
            ).fetchone()
            if existing is not None:
                self._finish_command(
                    connection, command_id, {"relation_id": existing["id"]}
                )
                return self._relation_from_row(existing)
            if relation_type is TaskRelationType.PARENT:
                other_parent = connection.execute(
                    """
                    SELECT id, from_task_id FROM orch_task_relations
                    WHERE to_task_id = ? AND relation_type = 'parent'
                      AND removed_at IS NULL
                    LIMIT 1
                    """,
                    (to_task_id,),
                ).fetchone()
                if other_parent is not None:
                    raise ConflictError(
                        f"task {to_task_id} already has parent relation "
                        f"{other_parent['id']} from {other_parent['from_task_id']}"
                    )
                if (
                    target_task.parent_task_id is not None
                    and target_task.parent_task_id != from_task_id
                ):
                    raise ConflictError(
                        f"task {to_task_id} already projects parent "
                        f"{target_task.parent_task_id}"
                    )
            if relation_type in {TaskRelationType.PARENT, TaskRelationType.BLOCKS}:
                cycle = self._relation_cycle_path(
                    connection, from_task_id, to_task_id, relation_type
                )
                if cycle:
                    raise ConflictError("relation would create a cycle: " + " -> ".join(cycle))
            relation_id = _id("relation")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_task_relations(
                    id, from_task_id, to_task_id, relation_type, metadata_json,
                    created_by_task_id, created_by_run_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    relation_id,
                    from_task_id,
                    to_task_id,
                    relation_type.value,
                    _json(metadata or {}),
                    created_by_task_id,
                    created_by_run_id,
                    now,
                ),
            )
            if relation_type is TaskRelationType.PARENT and target_task.parent_task_id is None:
                connection.execute(
                    """
                    UPDATE orch_tasks
                    SET parent_task_id = ?, parent_node_id = COALESCE(?, parent_node_id),
                        version = version + 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        from_task_id,
                        dict(metadata or {}).get("parent_node_id"),
                        now,
                        to_task_id,
                    ),
                )
            self._append_event(
                connection,
                task_id=to_task_id,
                aggregate_type="task_relation",
                aggregate_id=relation_id,
                event_type="relation_added",
                payload={
                    "from_task_id": from_task_id,
                    "to_task_id": to_task_id,
                    "relation_type": relation_type.value,
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"relation_id": relation_id})
            return self._require_relation(connection, relation_id)

    def remove_relation(
        self,
        relation_id: str,
        *,
        actor: str,
        command_id: Optional[str] = None,
    ) -> TaskRelationRecord:
        command_id = self._command_id(command_id)
        request = {"relation_id": relation_id, "actor": str(actor)}
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "relation.remove", relation_id, request
            )
            if replay is not None:
                return self._require_relation(
                    connection, replay["relation_id"], include_removed=True
                )
            relation = self._require_relation(connection, relation_id)
            now = _stamp(_now())
            if relation.relation_type is TaskRelationType.PARENT:
                child = self._require_task(connection, relation.to_task_id)
                if child.parent_task_id not in {None, relation.from_task_id}:
                    raise IntegrityError(
                        f"parent relation {relation.id} conflicts with task projection"
                    )
                if child.parent_task_id == relation.from_task_id:
                    connection.execute(
                        """
                        UPDATE orch_tasks
                        SET parent_task_id = NULL, parent_node_id = NULL,
                            version = version + 1, updated_at = ?
                        WHERE id = ?
                        """,
                        (now, child.id),
                    )
            connection.execute(
                "UPDATE orch_task_relations SET removed_at = ? WHERE id = ? AND removed_at IS NULL",
                (now, relation_id),
            )
            self._append_event(
                connection,
                task_id=relation.to_task_id,
                aggregate_type="task_relation",
                aggregate_id=relation.id,
                event_type="relation_removed",
                payload={
                    "from_task_id": relation.from_task_id,
                    "to_task_id": relation.to_task_id,
                    "relation_type": relation.relation_type.value,
                    "actor": str(actor),
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"relation_id": relation_id})
            return self._require_relation(connection, relation_id, include_removed=True)

    def list_relations(
        self,
        task_id: str,
        *,
        relation_type: Optional[TaskRelationType] = None,
        include_removed: bool = False,
    ) -> tuple[TaskRelationRecord, ...]:
        where = ["(from_task_id = ? OR to_task_id = ?)"]
        params: list[Any] = [task_id, task_id]
        if relation_type is not None:
            where.append("relation_type = ?")
            params.append(TaskRelationType(relation_type).value)
        if not include_removed:
            where.append("removed_at IS NULL")
        with self._read() as connection:
            self._require_task(connection, task_id)
            rows = connection.execute(
                "SELECT * FROM orch_task_relations WHERE "
                + " AND ".join(where)
                + " ORDER BY created_at, id",
                params,
            ).fetchall()
        return tuple(self._relation_from_row(row) for row in rows)

    def verify_relation_consistency(self) -> dict[str, Any]:
        """Fail closed if the durable task projection and live graph diverge.

        ``parent_task_id`` is retained for backwards-compatible reads while
        ``PARENT`` is the canonical first-class graph representation.  Both are
        deliberately checked at startup so historical/manual database damage
        cannot cause the relation resolver to wake the wrong task.
        """

        with self._read() as connection:
            task_rows = connection.execute(
                "SELECT id, parent_task_id FROM orch_tasks ORDER BY id"
            ).fetchall()
            relation_rows = connection.execute(
                """
                SELECT id, from_task_id, to_task_id, relation_type
                FROM orch_task_relations
                WHERE removed_at IS NULL
                  AND relation_type IN ('parent', 'blocks')
                ORDER BY relation_type, created_at, id
                """
            ).fetchall()

        projected_parent = {
            str(row["id"]): (
                str(row["parent_task_id"])
                if row["parent_task_id"] is not None
                else None
            )
            for row in task_rows
        }
        graph_parents: dict[str, list[tuple[str, str]]] = {}
        adjacency: dict[TaskRelationType, dict[str, list[str]]] = {
            TaskRelationType.PARENT: {},
            TaskRelationType.BLOCKS: {},
        }
        for row in relation_rows:
            relation_type = TaskRelationType(str(row["relation_type"]))
            source = str(row["from_task_id"])
            target = str(row["to_task_id"])
            adjacency[relation_type].setdefault(source, []).append(target)
            if relation_type is TaskRelationType.PARENT:
                graph_parents.setdefault(target, []).append(
                    (str(row["id"]), source)
                )

        for task_id, parent_id in projected_parent.items():
            parents = graph_parents.get(task_id, [])
            if len(parents) > 1:
                relation_ids = ", ".join(item[0] for item in parents[:3])
                raise IntegrityError(
                    f"task {task_id} has multiple live parent relations: {relation_ids}"
                )
            graph_parent = parents[0][1] if parents else None
            if graph_parent != parent_id:
                raise IntegrityError(
                    f"task {task_id} parent projection {parent_id!r} does not match "
                    f"live relation {graph_parent!r}"
                )

        def assert_acyclic(
            relation_type: TaskRelationType, graph: Mapping[str, Sequence[str]]
        ) -> None:
            # Iterative color DFS avoids Python's recursion ceiling on a large
            # historical blocker graph while retaining a useful bounded cycle path.
            state: dict[str, int] = {}
            all_nodes = sorted(
                set(graph)
                | set(projected_parent)
                | {child for children in graph.values() for child in children}
            )
            for root in all_nodes:
                if state.get(root, 0) == 2:
                    continue
                path: list[str] = []
                position: dict[str, int] = {}
                stack: list[tuple[str, int]] = [(root, 0)]
                while stack:
                    node, index = stack[-1]
                    if state.get(node, 0) == 0:
                        state[node] = 1
                        position[node] = len(path)
                        path.append(node)
                    children = graph.get(node, ())
                    if index < len(children):
                        child = children[index]
                        stack[-1] = (node, index + 1)
                        child_state = state.get(child, 0)
                        if child_state == 1:
                            start = position[child]
                            cycle = path[start:] + [child]
                            raise IntegrityError(
                                f"{relation_type.value} relation cycle: "
                                + " -> ".join(cycle[:65])
                            )
                        if child_state == 0:
                            stack.append((child, 0))
                        continue
                    stack.pop()
                    state[node] = 2
                    position.pop(node, None)
                    if path and path[-1] == node:
                        path.pop()

        for relation_type, graph in adjacency.items():
            assert_acyclic(relation_type, graph)
        return {
            "valid": True,
            "task_count": len(projected_parent),
            "parent_relation_count": sum(
                len(items) for items in graph_parents.values()
            ),
            "blocks_relation_count": sum(
                len(items)
                for items in adjacency[TaskRelationType.BLOCKS].values()
            ),
        }

    def replace_blockers(
        self,
        task_id: str,
        blocker_ids: Sequence[str],
        *,
        reason: str,
        owner: str,
        required_action: str,
        created_by_task_id: Optional[str] = None,
        created_by_run_id: Optional[str] = None,
        lease_token: Optional[str] = None,
        fencing_token: Optional[int] = None,
        command_id: Optional[str] = None,
    ) -> tuple[TaskRelationRecord, ...]:
        blockers = tuple(dict.fromkeys(str(item).strip() for item in blocker_ids if str(item).strip()))
        if task_id in blockers:
            raise ConflictError("a task cannot block itself")
        if not str(reason).strip() or not str(required_action).strip():
            raise ValueError("blocker reason and required_action are required")
        if contains_secret_like(
            {
                "reason": str(reason),
                "owner": str(owner),
                "required_action": str(required_action),
            }
        ):
            raise ValueError("blocker metadata cannot contain secret-like values")
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "blocker_ids": list(blockers),
            "reason": str(reason),
            "owner": str(owner),
            "required_action": str(required_action),
            "created_by_task_id": created_by_task_id,
            "created_by_run_id": created_by_run_id,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "blockers.replace", task_id, request
            )
            if replay is not None:
                return tuple(
                    self._require_relation(connection, item)
                    for item in replay.get("relation_ids", ())
                )
            task = self._require_task(connection, task_id)
            if created_by_run_id:
                if lease_token is None or fencing_token is None:
                    raise LeaseConflict("run-bound blocker writes require lease and fencing tokens")
                run = self._require_run(connection, created_by_run_id)
                self._require_lease(connection, run.id, lease_token, fencing_token)
                if run.task_id != task_id:
                    raise LeaseConflict("a run may only block its own task")
            for blocker_id in blockers:
                self._require_task(connection, blocker_id)
                cycle = self._relation_cycle_path(
                    connection, blocker_id, task_id, TaskRelationType.BLOCKS
                )
                if cycle:
                    raise ConflictError("blocker would create a cycle: " + " -> ".join(cycle))
            existing_rows = connection.execute(
                """
                SELECT * FROM orch_task_relations
                WHERE to_task_id = ? AND relation_type = 'blocks' AND removed_at IS NULL
                """,
                (task_id,),
            ).fetchall()
            by_source = {str(row["from_task_id"]): row for row in existing_rows}
            now = _stamp(_now())
            for source, row in by_source.items():
                if source not in blockers:
                    connection.execute(
                        "UPDATE orch_task_relations SET removed_at = ? WHERE id = ?",
                        (now, row["id"]),
                    )
                    self._append_event(
                        connection,
                        task_id=task_id,
                        aggregate_type="task_relation",
                        aggregate_id=row["id"],
                        event_type="relation_removed",
                        payload={"relation_type": "blocks", "from_task_id": source, "to_task_id": task_id},
                        command_id=command_id,
                    )
            relation_ids: list[str] = []
            for blocker_id in blockers:
                row = by_source.get(blocker_id)
                if row is not None:
                    relation_ids.append(row["id"])
                    continue
                relation_id = _id("relation")
                connection.execute(
                    """
                    INSERT INTO orch_task_relations(
                        id, from_task_id, to_task_id, relation_type, metadata_json,
                        created_by_task_id, created_by_run_id, created_at
                    ) VALUES (?, ?, ?, 'blocks', ?, ?, ?, ?)
                    """,
                    (
                        relation_id,
                        blocker_id,
                        task_id,
                        _json({"reason": reason, "owner": owner, "required_action": required_action}),
                        created_by_task_id,
                        created_by_run_id,
                        now,
                    ),
                )
                relation_ids.append(relation_id)
                self._append_event(
                    connection,
                    task_id=task_id,
                    aggregate_type="task_relation",
                    aggregate_id=relation_id,
                    event_type="relation_added",
                    payload={"relation_type": "blocks", "from_task_id": blocker_id, "to_task_id": task_id},
                    command_id=command_id,
                )
            if blockers and task.status in {TaskStatus.QUEUED, TaskStatus.RUNNING}:
                validate_task_transition(task.status, TaskStatus.BLOCKED)
                connection.execute(
                    "UPDATE orch_tasks SET status = 'blocked', version = version + 1, updated_at = ? WHERE id = ?",
                    (now, task_id),
                )
                self._append_event(
                    connection,
                    task_id=task_id,
                    aggregate_type="task",
                    aggregate_id=task_id,
                    event_type="task.blocked",
                    payload={"blocker_ids": list(blockers), "owner": owner, "required_action": required_action},
                    command_id=command_id,
                )
            elif (
                not blockers
                and not str(owner).strip()
                and task.status is TaskStatus.BLOCKED
            ):
                validate_task_transition(TaskStatus.BLOCKED, TaskStatus.QUEUED)
                connection.execute(
                    "UPDATE orch_tasks SET status = 'queued', version = version + 1, updated_at = ? WHERE id = ?",
                    (now, task_id),
                )
                self._append_event(
                    connection,
                    task_id=task_id,
                    aggregate_type="task",
                    aggregate_id=task_id,
                    event_type="blockers_resolved",
                    payload={"blocker_ids": [], "relation_set_version": _digest([])},
                    command_id=command_id,
                )
                self._enqueue_wake_connection(
                    connection,
                    target_task_id=task_id,
                    target_run_id=None,
                    reason=WakeReason.TASK_BLOCKERS_RESOLVED,
                    source_task_id=created_by_task_id,
                    source_run_id=created_by_run_id,
                    source_event_id=None,
                    payload={"blocker_ids": [], "relation_set_version": _digest([])},
                    dedupe_key=f"{task_id}:current:task_blockers_resolved:{_digest([])}",
                    not_before=None,
                    command_id=command_id,
                )
            self._finish_command(connection, command_id, {"relation_ids": relation_ids})
            return tuple(self._require_relation(connection, item) for item in relation_ids)

    def resolve_terminal_relations(
        self,
        task_id: str,
        *,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Atomically derive parent/dependent wakes from one terminal task."""

        task_snapshot = self.get_task(task_id)
        command_id = str(
            command_id or f"terminal-relations:{task_id}:{task_snapshot.version}"
        )
        request = {"task_id": task_id, "task_version": task_snapshot.version}
        terminal_values = {
            TaskStatus.FAILED,
            TaskStatus.CANCELED,
            TaskStatus.COMPLETED,
            TaskStatus.ARCHIVED,
        }

        def completed(row: sqlite3.Row) -> bool:
            status = TaskStatus(row["status"])
            if status is TaskStatus.COMPLETED:
                return True
            if status is TaskStatus.ARCHIVED:
                return str((_load(row["output_json"], {}) or {}).get("archived_from") or "completed") == "completed"
            return False

        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "relations.resolve_terminal", task_id, request
            )
            if replay is not None:
                return dict(replay)
            terminal = self._require_task(connection, task_id)
            if terminal.status not in terminal_values:
                raise ConflictError("relation resolution requires a terminal source task")
            parent_wakes: list[str] = []
            blocker_wakes: list[str] = []
            attention_tasks: list[str] = []
            parent_rows = connection.execute(
                """
                SELECT DISTINCT relation.from_task_id AS parent_id
                FROM orch_task_relations relation
                WHERE relation.to_task_id = ? AND relation.relation_type = 'parent'
                  AND relation.removed_at IS NULL
                """,
                (task_id,),
            ).fetchall()
            for parent_row in parent_rows:
                parent = self._require_task(connection, parent_row["parent_id"])
                if parent.status in terminal_values:
                    continue
                child_rows = connection.execute(
                    """
                    SELECT child.* FROM orch_task_relations relation
                    JOIN orch_tasks child ON child.id = relation.to_task_id
                    WHERE relation.from_task_id = ? AND relation.relation_type = 'parent'
                      AND relation.removed_at IS NULL
                    ORDER BY child.created_at, child.id
                    """,
                    (parent.id,),
                ).fetchall()
                if not child_rows or any(TaskStatus(row["status"]) not in terminal_values for row in child_rows):
                    continue
                waiting_gate = connection.execute(
                    """
                    SELECT 1 FROM orch_gates
                    WHERE task_id = ? AND kind = 'child_wait' AND status = 'open'
                    LIMIT 1
                    """,
                    (parent.id,),
                ).fetchone()
                if parent.status is not TaskStatus.WAITING_CHILD and waiting_gate is None:
                    continue
                children: list[dict[str, Any]] = []
                for child in child_rows:
                    products = connection.execute(
                        "SELECT id FROM orch_work_products WHERE task_id = ? ORDER BY created_at, id",
                        (child["id"],),
                    ).fetchall()
                    output = _load(child["output_json"], {}) or {}
                    result = dict(output.get("result") or {})
                    children.append(
                        {
                            "task_id": child["id"],
                            "status": child["status"],
                            "summary": str(result.get("summary") or output.get("summary") or "")[:8_000],
                            "work_product_refs": [row["id"] for row in products],
                        }
                    )
                relation_version = _digest(
                    [(item["task_id"], item["status"], item["work_product_refs"]) for item in children]
                )
                wake = self._enqueue_wake_connection(
                    connection,
                    target_task_id=parent.id,
                    target_run_id=None,
                    reason=WakeReason.TASK_CHILDREN_COMPLETED,
                    source_task_id=task_id,
                    source_run_id=None,
                    source_event_id=None,
                    payload={"children": children, "relation_set_version": relation_version},
                    dedupe_key=f"{parent.id}:current:task_children_completed:{relation_version}",
                    not_before=None,
                    command_id=command_id,
                )
                parent_wakes.append(wake.id)
            dependent_rows = connection.execute(
                """
                SELECT DISTINCT relation.to_task_id AS dependent_id
                FROM orch_task_relations relation
                WHERE relation.from_task_id = ? AND relation.relation_type = 'blocks'
                  AND relation.removed_at IS NULL
                """,
                (task_id,),
            ).fetchall()
            for dependent_row in dependent_rows:
                dependent = self._require_task(connection, dependent_row["dependent_id"])
                blocker_rows = connection.execute(
                    """
                    SELECT blocker.* FROM orch_task_relations relation
                    JOIN orch_tasks blocker ON blocker.id = relation.from_task_id
                    WHERE relation.to_task_id = ? AND relation.relation_type = 'blocks'
                      AND relation.removed_at IS NULL
                    ORDER BY blocker.created_at, blocker.id
                    """,
                    (dependent.id,),
                ).fetchall()
                canceled = [
                    row["id"]
                    for row in blocker_rows
                    if TaskStatus(row["status"]) is TaskStatus.CANCELED
                    or (
                        TaskStatus(row["status"]) is TaskStatus.ARCHIVED
                        and str((_load(row["output_json"], {}) or {}).get("archived_from") or "") == "canceled"
                    )
                ]
                if canceled:
                    self._append_event(
                        connection,
                        task_id=dependent.id,
                        aggregate_type="task",
                        aggregate_id=dependent.id,
                        event_type="blocker_canceled_attention",
                        payload={"canceled_blocker_ids": canceled, "required_action": "remove_or_replace_blocker"},
                        command_id=command_id,
                    )
                    attention_tasks.append(dependent.id)
                    continue
                if blocker_rows and all(completed(row) for row in blocker_rows):
                    blocker_ids = [row["id"] for row in blocker_rows]
                    relation_version = _digest([(row["id"], row["version"]) for row in blocker_rows])
                    if dependent.status is TaskStatus.BLOCKED:
                        validate_task_transition(TaskStatus.BLOCKED, TaskStatus.QUEUED)
                        now = _stamp(_now())
                        connection.execute(
                            """
                            UPDATE orch_tasks SET status = 'queued', version = version + 1,
                                updated_at = ? WHERE id = ? AND status = 'blocked'
                            """,
                            (now, dependent.id),
                        )
                        self._append_event(
                            connection,
                            task_id=dependent.id,
                            aggregate_type="task",
                            aggregate_id=dependent.id,
                            event_type="blockers_resolved",
                            payload={"blocker_ids": blocker_ids, "relation_set_version": relation_version},
                            command_id=command_id,
                        )
                    wake = self._enqueue_wake_connection(
                        connection,
                        target_task_id=dependent.id,
                        target_run_id=None,
                        reason=WakeReason.TASK_BLOCKERS_RESOLVED,
                        source_task_id=task_id,
                        source_run_id=None,
                        source_event_id=None,
                        payload={"blocker_ids": blocker_ids, "relation_set_version": relation_version},
                        dedupe_key=f"{dependent.id}:current:task_blockers_resolved:{relation_version}",
                        not_before=None,
                        command_id=command_id,
                    )
                    blocker_wakes.append(wake.id)
            result = {
                "parent_wake_ids": parent_wakes,
                "blocker_wake_ids": blocker_wakes,
                "attention_task_ids": attention_tasks,
            }
            self._finish_command(connection, command_id, result)
            return result

    # -- durable wake queue -----------------------------------------------
    def _enqueue_wake_connection(
        self,
        connection: sqlite3.Connection,
        *,
        target_task_id: str,
        target_run_id: Optional[str],
        reason: WakeReason,
        source_task_id: Optional[str],
        source_run_id: Optional[str],
        source_event_id: Optional[str],
        payload: Mapping[str, Any],
        dedupe_key: str,
        not_before: Optional[datetime],
        command_id: Optional[str],
    ) -> WakeRequestRecord:
        self._require_task(connection, target_task_id)
        existing = connection.execute(
            """
            SELECT * FROM orch_wake_requests
            WHERE dedupe_key = ? AND status IN ('pending', 'deferred', 'claimed', 'delivered')
            """,
            (dedupe_key,),
        ).fetchone()
        now = _now()
        if existing is not None:
            merged = dict(_load(existing["payload_json"], {}))
            incoming = dict(payload)
            if reason in {WakeReason.TASK_COMMENTED, WakeReason.TASK_COMMENT_MENTIONED}:
                merged_ids = list(
                    dict.fromkeys(
                        [str(item) for item in merged.get("comment_ids", ())]
                        + [str(item) for item in incoming.get("comment_ids", ())]
                    )
                )
                merged.update(incoming)
                if len(merged_ids) > 100:
                    merged["comment_ids"] = merged_ids[-100:]
                    merged["fallback_fetch_needed"] = True
                    merged["after_sequence"] = max(
                        0, int(merged.get("latest_sequence") or 0) - 100
                    )
                else:
                    merged["comment_ids"] = merged_ids
            else:
                merged.update(incoming)
            connection.execute(
                """
                UPDATE orch_wake_requests
                SET payload_json = ?, coalesced_count = coalesced_count + 1,
                    updated_at = ?, not_before = MIN(not_before, ?)
                WHERE id = ?
                """,
                (
                    _json(merged),
                    _stamp(now),
                    _stamp(not_before or now),
                    existing["id"],
                ),
            )
            self._append_event(
                connection,
                task_id=target_task_id,
                aggregate_type="wake_request",
                aggregate_id=existing["id"],
                event_type="wake_coalesced",
                payload={"reason": reason.value, "dedupe_key": dedupe_key},
                command_id=command_id,
            )
            return self._require_wake(connection, existing["id"])
        wake_id = _id("wake")
        stamp = _stamp(now)
        connection.execute(
            """
            INSERT INTO orch_wake_requests(
                id, target_task_id, target_run_id, reason, source_task_id,
                source_run_id, source_event_id, payload_json, dedupe_key, status,
                coalesced_count, attempts, not_before, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, ?, ?, ?)
            """,
            (
                wake_id,
                target_task_id,
                target_run_id,
                reason.value,
                source_task_id,
                source_run_id,
                source_event_id,
                _json(payload),
                dedupe_key,
                _stamp(not_before or now),
                stamp,
                stamp,
            ),
        )
        self._append_event(
            connection,
            task_id=target_task_id,
            aggregate_type="wake_request",
            aggregate_id=wake_id,
            event_type="wake_enqueued",
            payload={"reason": reason.value, "dedupe_key": dedupe_key},
            command_id=command_id,
        )
        return self._require_wake(connection, wake_id)

    def enqueue_wake(
        self,
        target_task_id: str,
        reason: WakeReason | str,
        *,
        target_run_id: Optional[str] = None,
        source_task_id: Optional[str] = None,
        source_run_id: Optional[str] = None,
        source_event_id: Optional[str] = None,
        payload: Optional[Mapping[str, Any]] = None,
        dedupe_key: Optional[str] = None,
        not_before: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> WakeRequestRecord:
        reason = WakeReason(reason)
        chosen_key = str(
            dedupe_key
            or f"{target_task_id}:{target_run_id or 'current'}:{reason.value}:{source_event_id or source_task_id or 'system'}"
        )
        command_id = self._command_id(command_id)
        request = {
            "target_task_id": target_task_id,
            "target_run_id": target_run_id,
            "reason": reason.value,
            "source_task_id": source_task_id,
            "source_run_id": source_run_id,
            "source_event_id": source_event_id,
            "payload": dict(payload or {}),
            "dedupe_key": chosen_key,
            "not_before": not_before,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "wake.enqueue", target_task_id, request
            )
            if replay is not None:
                return self._require_wake(connection, replay["wake_id"])
            wake = self._enqueue_wake_connection(
                connection,
                target_task_id=target_task_id,
                target_run_id=target_run_id,
                reason=reason,
                source_task_id=source_task_id,
                source_run_id=source_run_id,
                source_event_id=source_event_id,
                payload=payload or {},
                dedupe_key=chosen_key,
                not_before=not_before,
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"wake_id": wake.id})
            return wake

    def claim_ready_wake(
        self,
        owner: str,
        *,
        claim_seconds: int = 60,
        now: Optional[datetime] = None,
    ) -> Optional[WakeRequestRecord]:
        chosen_now = now or _now()
        with self._write() as connection:
            row = connection.execute(
                """
                SELECT * FROM orch_wake_requests
                WHERE status = 'pending' AND not_before <= ?
                ORDER BY not_before, created_at, id LIMIT 1
                """,
                (_stamp(chosen_now),),
            ).fetchone()
            if row is None:
                return None
            until = chosen_now + timedelta(seconds=max(1, int(claim_seconds)))
            changed = connection.execute(
                """
                UPDATE orch_wake_requests
                SET status = 'claimed', claimed_by = ?, claimed_until = ?,
                    attempts = attempts + 1, updated_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (owner, _stamp(until), _stamp(chosen_now), row["id"]),
            ).rowcount
            if changed != 1:
                return None
            self._append_event(
                connection,
                task_id=row["target_task_id"],
                aggregate_type="wake_request",
                aggregate_id=row["id"],
                event_type="wake_claimed",
                payload={"claimed_by": owner, "claimed_until": _stamp(until)},
                command_id=None,
            )
            return self._require_wake(connection, row["id"])

    def activate_due_wakes(self, *, now: Optional[datetime] = None) -> int:
        """Move due deferred deliveries back to the claimable queue."""

        chosen_now = now or _now()
        with self._write() as connection:
            rows = connection.execute(
                """
                SELECT * FROM orch_wake_requests
                WHERE status = 'deferred' AND not_before <= ?
                ORDER BY not_before, created_at, id
                """,
                (_stamp(chosen_now),),
            ).fetchall()
            for row in rows:
                connection.execute(
                    """
                    UPDATE orch_wake_requests
                    SET status = 'pending', claimed_by = NULL, claimed_until = NULL,
                        updated_at = ? WHERE id = ? AND status = 'deferred'
                    """,
                    (_stamp(chosen_now), row["id"]),
                )
                self._append_event(
                    connection,
                    task_id=row["target_task_id"],
                    aggregate_type="wake_request",
                    aggregate_id=row["id"],
                    event_type="wake_reactivated",
                    payload={},
                    command_id=None,
                )
            return len(rows)

    def bind_wake_to_run(
        self,
        wake_id: str,
        run_id: str,
        *,
        owner: str,
        command_id: Optional[str] = None,
    ) -> WakeRequestRecord:
        """Bind a claimed wake to one queued attempt and mark its delta delivered."""

        command_id = self._command_id(command_id)
        request = {"wake_id": wake_id, "run_id": run_id, "owner": str(owner)}
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "wake.bind_run", wake_id, request
            )
            if replay is not None:
                return self._require_wake(connection, wake_id)
            wake = self._require_wake(connection, wake_id)
            if wake.status is not WakeStatus.CLAIMED or wake.claimed_by != str(owner):
                raise LeaseConflict("wake claim is no longer owned by this scheduler")
            run = self._require_run(connection, run_id)
            if run.task_id != wake.target_task_id:
                raise ConflictError("wake and run belong to different tasks")
            if run.status not in {
                RunStatus.QUEUED,
                RunStatus.CLAIMED,
                RunStatus.RUNNING,
                RunStatus.WAITING_GATE,
            }:
                raise ConflictError("wake can only bind to a live run")
            now = _stamp(_now())
            connection.execute(
                """
                UPDATE orch_wake_requests
                SET target_run_id = ?, status = 'delivered', delivered_at = ?,
                    updated_at = ?, claimed_by = NULL, claimed_until = NULL
                WHERE id = ? AND status = 'claimed' AND claimed_by = ?
                """,
                (run.id, now, now, wake.id, str(owner)),
            )
            self._append_event(
                connection,
                task_id=wake.target_task_id,
                aggregate_type="wake_request",
                aggregate_id=wake.id,
                event_type="wake_delivered",
                payload={"run_id": run.id, "reason": wake.reason.value},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"wake_id": wake.id})
            return self._require_wake(connection, wake.id)

    def cancel_claimed_wake(
        self,
        wake_id: str,
        *,
        reason: str,
        command_id: Optional[str] = None,
    ) -> WakeRequestRecord:
        return self._transition_wake(
            wake_id,
            expected=(WakeStatus.CLAIMED,),
            target=WakeStatus.CANCELED,
            error=str(reason)[:8_000],
            command_id=command_id,
        )

    def _transition_wake(
        self,
        wake_id: str,
        *,
        expected: Sequence[WakeStatus],
        target: WakeStatus,
        error: Optional[str] = None,
        not_before: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> WakeRequestRecord:
        command_id = self._command_id(command_id)
        request = {
            "wake_id": wake_id,
            "expected": [WakeStatus(item).value for item in expected],
            "target": target.value,
            "error": error,
            "not_before": not_before,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, f"wake.{target.value}", wake_id, request
            )
            if replay is not None:
                return self._require_wake(connection, wake_id)
            wake = self._require_wake(connection, wake_id)
            allowed = {WakeStatus(item) for item in expected}
            if wake.status not in allowed:
                raise ConflictError(
                    f"wake {wake_id} is {wake.status.value}, expected "
                    + ", ".join(sorted(item.value for item in allowed))
                )
            now = _now()
            assignments = [
                "status = ?",
                "updated_at = ?",
                "last_error = ?",
            ]
            values: list[Any] = [target.value, _stamp(now), error]
            if target is not WakeStatus.CLAIMED:
                assignments.extend(["claimed_by = NULL", "claimed_until = NULL"])
            if not_before is not None:
                assignments.append("not_before = ?")
                values.append(_stamp(not_before))
            if target is WakeStatus.DELIVERED:
                assignments.append("delivered_at = ?")
                values.append(_stamp(now))
            if target is WakeStatus.COMPLETED:
                assignments.append("completed_at = ?")
                values.append(_stamp(now))
            values.append(wake_id)
            connection.execute(
                "UPDATE orch_wake_requests SET " + ", ".join(assignments) + " WHERE id = ?",
                values,
            )
            self._append_event(
                connection,
                task_id=wake.target_task_id,
                aggregate_type="wake_request",
                aggregate_id=wake.id,
                event_type=f"wake_{target.value}",
                payload={"reason": wake.reason.value, "error": error},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"wake_id": wake_id})
            return self._require_wake(connection, wake_id)

    def defer_wake(self, wake_id: str, *, not_before: datetime, command_id: Optional[str] = None) -> WakeRequestRecord:
        return self._transition_wake(wake_id, expected=(WakeStatus.CLAIMED, WakeStatus.PENDING), target=WakeStatus.DEFERRED, not_before=not_before, command_id=command_id)

    def mark_wake_delivered(self, wake_id: str, *, command_id: Optional[str] = None) -> WakeRequestRecord:
        return self._transition_wake(wake_id, expected=(WakeStatus.CLAIMED,), target=WakeStatus.DELIVERED, command_id=command_id)

    def mark_wake_completed(self, wake_id: str, *, command_id: Optional[str] = None) -> WakeRequestRecord:
        return self._transition_wake(wake_id, expected=(WakeStatus.DELIVERED,), target=WakeStatus.COMPLETED, command_id=command_id)

    def mark_wake_failed(
        self,
        wake_id: str,
        error: str,
        *,
        max_attempts: int = 5,
        backoff_seconds: int = 1,
        command_id: Optional[str] = None,
    ) -> WakeRequestRecord:
        wake = self.get_wake(wake_id)
        if wake.attempts < max(1, int(max_attempts)):
            delay = min(
                300,
                max(1, int(backoff_seconds))
                * (2 ** max(0, wake.attempts - 1)),
            )
            return self._transition_wake(
                wake_id,
                expected=(WakeStatus.CLAIMED, WakeStatus.DELIVERED),
                target=WakeStatus.PENDING,
                error=str(error)[:8_000],
                not_before=_now() + timedelta(seconds=delay),
                command_id=command_id,
            )
        failed = self._transition_wake(
            wake_id,
            expected=(WakeStatus.CLAIMED, WakeStatus.DELIVERED),
            target=WakeStatus.FAILED,
            error=str(error)[:8_000],
            command_id=command_id,
        )
        task = self.get_task(failed.target_task_id)
        if task.status in {
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
            TaskStatus.WAITING_HUMAN,
            TaskStatus.WAITING_CHILD,
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
        }:
            self.transition_task_status(
                task.id,
                TaskStatus.NEEDS_RECONCILIATION,
                expected_version=task.version,
                command_id=f"wake-dead-letter:{wake_id}",
            )
        return self.get_wake(wake_id)

    def recover_expired_wake_claims(
        self, *, now: Optional[datetime] = None
    ) -> int:
        chosen_now = now or _now()
        with self._write() as connection:
            rows = connection.execute(
                """
                SELECT * FROM orch_wake_requests
                WHERE status = 'claimed' AND claimed_until IS NOT NULL AND claimed_until <= ?
                """,
                (_stamp(chosen_now),),
            ).fetchall()
            for row in rows:
                connection.execute(
                    """
                    UPDATE orch_wake_requests SET status = 'pending', claimed_by = NULL,
                        claimed_until = NULL, updated_at = ? WHERE id = ?
                    """,
                    (_stamp(chosen_now), row["id"]),
                )
                self._append_event(
                    connection,
                    task_id=row["target_task_id"],
                    aggregate_type="wake_request",
                    aggregate_id=row["id"],
                    event_type="wake_claim_recovered",
                    payload={"previous_claimed_by": row["claimed_by"]},
                    command_id=None,
                )
            return len(rows)

    def retry_wake(self, wake_id: str, *, command_id: Optional[str] = None) -> WakeRequestRecord:
        return self._transition_wake(
            wake_id,
            expected=(WakeStatus.FAILED,),
            target=WakeStatus.PENDING,
            not_before=_now(),
            command_id=command_id,
        )

    def cancel_wake(self, wake_id: str, *, command_id: Optional[str] = None) -> WakeRequestRecord:
        return self._transition_wake(
            wake_id,
            expected=(WakeStatus.PENDING, WakeStatus.DEFERRED),
            target=WakeStatus.CANCELED,
            command_id=command_id,
        )

    def get_wake(self, wake_id: str) -> WakeRequestRecord:
        with self._read() as connection:
            return self._require_wake(connection, wake_id)

    def list_wakes(
        self,
        *,
        task_id: Optional[str] = None,
        statuses: Optional[Sequence[WakeStatus]] = None,
        limit: int = 1_000,
        offset: int = 0,
    ) -> tuple[WakeRequestRecord, ...]:
        where: list[str] = []
        params: list[Any] = []
        if task_id:
            where.append("target_task_id = ?")
            params.append(task_id)
        if statuses:
            values = [WakeStatus(item).value for item in statuses]
            where.append("status IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
        params.extend((max(1, min(int(limit), 10_000)), max(0, int(offset))))
        clause = " WHERE " + " AND ".join(where) if where else ""
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM orch_wake_requests"
                + clause
                + " ORDER BY created_at, id LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        return tuple(self._wake_from_row(row) for row in rows)

    def handoff_observability_snapshot(
        self, *, now: Optional[datetime] = None
    ) -> dict[str, int | float]:
        """Return bounded queue/blocking aggregates for the metrics cache.

        The queries use status-leading indexes and aggregate in SQLite, so metric
        refresh cost does not grow with the payload size of wakes or tasks.
        """

        chosen_now = now or _now()
        with self._read() as connection:
            pending = int(
                connection.execute(
                    "SELECT COUNT(*) FROM orch_wake_requests WHERE status = 'pending'"
                ).fetchone()[0]
            )
            coalesced_total = int(
                connection.execute(
                """
                    SELECT COALESCE(SUM(coalesced_count), 0)
                    FROM orch_wake_requests
                """
                ).fetchone()[0]
            )
            oldest_blocked = connection.execute(
                """
                SELECT updated_at FROM orch_tasks
                WHERE status = 'blocked'
                ORDER BY updated_at ASC LIMIT 1
                """
            ).fetchone()
        blocked_seconds = 0.0
        if oldest_blocked is not None:
            blocked_at = _time(oldest_blocked["updated_at"])
            if blocked_at is not None:
                blocked_seconds = max(
                    0.0, (chosen_now - blocked_at).total_seconds()
                )
        return {
            "wakes_pending": pending,
            "wake_coalesced_total": coalesced_total,
            "task_blocked_duration_seconds": blocked_seconds,
        }

    # -- comments and structured mentions --------------------------------
    @staticmethod
    def _sanitize_comment_markdown(body: str) -> str:
        value = str(body).replace("\x00", "")
        value = re.sub(
            r"(?is)<\s*(script|iframe|object|embed|style)\b[^>]*>.*?<\s*/\s*\1\s*>",
            "",
            value,
        )
        value = re.sub(r"(?is)\son[a-z]+\s*=\s*(['\"]).*?\1", "", value)
        value = re.sub(r"(?is)javascript\s*:", "", value)
        return value.strip()

    def post_task_comment(
        self,
        task_id: str,
        body_markdown: str,
        *,
        author_type: str,
        author_id: str,
        metadata: Optional[Mapping[str, Any]] = None,
        reply_to_comment_id: Optional[str] = None,
        created_by_run_id: Optional[str] = None,
        lease_token: Optional[str] = None,
        fencing_token: Optional[int] = None,
        wake_owner: bool = True,
        wake_coalesce_window_ms: int = 1_000,
        command_id: Optional[str] = None,
    ) -> TaskCommentRecord:
        author_type = str(author_type).strip().lower()
        if author_type not in {"operator", "agent", "system"}:
            raise ValueError("comment author_type must be operator, agent, or system")
        author_id = str(author_id).strip()
        if not author_id:
            raise ValueError("comment author_id is required")
        body = self._sanitize_comment_markdown(body_markdown)
        if not body:
            raise ValueError("comment body is required")
        if contains_secret_like({"body": body, "metadata": dict(metadata or {})}):
            raise ValueError(
                "comment cannot contain secret-like values; use the runtime secret mechanism"
            )
        if len(body.encode("utf-8")) > 65_536:
            raise ValueError("comment exceeds 65536 bytes; store large content as an artifact")
        chosen_metadata = dict(metadata or {})
        mentions = tuple(
            dict.fromkeys(
                str(item).strip()
                for item in chosen_metadata.get("mentions", ())
                if str(item).strip()
            )
        )
        if len(mentions) > _MAX_MENTIONS_PER_COMMENT:
            raise ValueError(
                f"a comment may mention at most {_MAX_MENTIONS_PER_COMMENT} targets"
            )
        for target in mentions:
            profile_target = target.removeprefix("task:") if target.startswith("task:") else target
            if not _MENTION_PROFILE_ID.fullmatch(profile_target):
                raise ValueError(f"invalid canonical mention target: {target!r}")
        chosen_metadata["mentions"] = list(mentions)
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "body_markdown": body,
            "author_type": author_type,
            "author_id": author_id,
            "metadata": chosen_metadata,
            "reply_to_comment_id": reply_to_comment_id,
            "created_by_run_id": created_by_run_id,
            "wake_owner": bool(wake_owner),
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "comment.post", task_id, request
            )
            if replay is not None:
                return self._require_comment(connection, replay["comment_id"])
            task = self._require_task(connection, task_id)
            if created_by_run_id:
                if lease_token is None or fencing_token is None:
                    raise LeaseConflict("Agent comments require lease and fencing tokens")
                run = self._require_run(connection, created_by_run_id)
                self._require_lease(connection, run.id, lease_token, fencing_token)
                if run.task_id != task_id:
                    raise LeaseConflict("an Agent may only comment its current task")
            if reply_to_comment_id:
                reply = self._require_comment(connection, reply_to_comment_id)
                if reply.task_id != task_id:
                    raise ConflictError("reply target belongs to another task")
            sequence = int(
                connection.execute(
                    "SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM orch_task_comments WHERE task_id = ?",
                    (task_id,),
                ).fetchone()[0]
            )
            comment_id = _id("comment")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_task_comments(
                    id, task_id, sequence_no, author_type, author_id,
                    created_by_run_id, body_markdown, metadata_json,
                    reply_to_comment_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    comment_id,
                    task_id,
                    sequence,
                    author_type,
                    author_id,
                    created_by_run_id,
                    body,
                    _json(chosen_metadata),
                    reply_to_comment_id,
                    now,
                ),
            )
            event = self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task_comment",
                aggregate_id=comment_id,
                event_type="comment_added",
                payload={
                    "sequence": sequence,
                    "author_type": author_type,
                    "author_id": author_id,
                    "mentions": list(mentions),
                    "request_response": bool(chosen_metadata.get("request_response")),
                },
                command_id=command_id,
            )
            should_wake_owner = bool(wake_owner) and (
                created_by_run_id is None
                or bool(chosen_metadata.get("request_response"))
                or task.status
                in {
                    TaskStatus.WAITING_HUMAN,
                    TaskStatus.PAUSED,
                    TaskStatus.BLOCKED,
                    TaskStatus.COMPLETED,
                }
            )
            if should_wake_owner:
                owner_dedupe_key = (
                    f"{task_id}:current:task_commented:{comment_id}"
                )
                coalesce_window = max(0, int(wake_coalesce_window_ms))
                if coalesce_window:
                    candidate = connection.execute(
                        """
                        SELECT dedupe_key, updated_at
                        FROM orch_wake_requests
                        WHERE target_task_id = ? AND reason = 'task_commented'
                          AND status IN ('pending', 'deferred', 'claimed', 'delivered')
                        ORDER BY updated_at DESC, id DESC LIMIT 1
                        """,
                        (task_id,),
                    ).fetchone()
                    if candidate is not None:
                        updated_at = _time(candidate["updated_at"])
                        if updated_at is not None and (
                            _now() - updated_at
                        ).total_seconds() * 1_000 <= coalesce_window:
                            owner_dedupe_key = str(candidate["dedupe_key"])
                self._enqueue_wake_connection(
                    connection,
                    target_task_id=task_id,
                    target_run_id=None,
                    reason=WakeReason.TASK_COMMENTED,
                    source_task_id=task_id,
                    source_run_id=created_by_run_id,
                    source_event_id=event.id,
                    payload={
                        "comment_ids": [comment_id],
                        "latest_sequence": sequence,
                        "fallback_fetch_needed": False,
                    },
                    dedupe_key=owner_dedupe_key,
                    not_before=None,
                    command_id=command_id,
                )
            for target in mentions:
                target_task_id = task_id
                target_profile_id = target
                if target.startswith("task:"):
                    target_task_id = target.removeprefix("task:").strip()
                    self._require_task(connection, target_task_id)
                    if self._root_task_id_connection(
                        connection, target_task_id
                    ) != self._root_task_id_connection(connection, task_id):
                        raise PermissionError(
                            "task mention target must be in the same orchestration tree"
                        )
                    target_profile_id = "task-owner"
                live_mentions = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM orch_wake_requests
                        WHERE source_task_id = ? AND reason = 'task_comment_mentioned'
                          AND status IN ('pending', 'deferred', 'claimed', 'delivered')
                        """,
                        (task_id,),
                    ).fetchone()[0]
                )
                if live_mentions >= _MAX_LIVE_MENTION_WAKES_PER_TASK:
                    raise ConflictError("task mention wake budget is exhausted")
                self._append_event(
                    connection,
                    task_id=task_id,
                    aggregate_type="task_comment",
                    aggregate_id=comment_id,
                    event_type="mention_detected",
                    payload={
                        "target_profile_id": target_profile_id,
                        "target_task_id": target_task_id,
                        "sequence": sequence,
                    },
                    command_id=command_id,
                )
                self._enqueue_wake_connection(
                    connection,
                    target_task_id=target_task_id,
                    target_run_id=None,
                    reason=WakeReason.TASK_COMMENT_MENTIONED,
                    source_task_id=task_id,
                    source_run_id=created_by_run_id,
                    source_event_id=event.id,
                    payload={
                        "comment_ids": [comment_id],
                        "target_profile_id": target_profile_id,
                        "mentioned_from_task_id": task_id,
                        "notice_only": True,
                    },
                    dedupe_key=f"{target_task_id}:mention:{target_profile_id}:{comment_id}",
                    not_before=None,
                    command_id=command_id,
                )
            self._finish_command(connection, command_id, {"comment_id": comment_id})
            return self._require_comment(connection, comment_id)

    def get_task_comment(self, comment_id: str) -> TaskCommentRecord:
        with self._read() as connection:
            return self._require_comment(connection, comment_id)

    def list_task_comments(
        self,
        task_id: str,
        *,
        after_sequence: int = 0,
        limit: int = 100,
    ) -> tuple[TaskCommentRecord, ...]:
        limit = max(1, min(int(limit), 1_000))
        with self._read() as connection:
            self._require_task(connection, task_id)
            rows = connection.execute(
                """
                SELECT * FROM orch_task_comments
                WHERE task_id = ? AND sequence_no > ?
                ORDER BY sequence_no LIMIT ?
                """,
                (task_id, max(0, int(after_sequence)), limit),
            ).fetchall()
        return tuple(self._comment_from_row(row) for row in rows)

    # -- immutable work products -----------------------------------------
    @staticmethod
    def _validate_work_product_uri(
        uri: Optional[str],
        *,
        kind: WorkProductKind,
        workspace: Optional[str],
    ) -> None:
        if not uri:
            if kind is WorkProductKind.WORKSPACE_FILE:
                raise ValueError("workspace_file products require a workspace URI")
            return
        parsed = urlparse(str(uri))
        if not parsed.scheme:
            raise ValueError("work product URI must use an explicit allowed scheme")
        if parsed.scheme not in {
            "http",
            "https",
            "sha256",
            "workspace",
            "git",
        }:
            raise ValueError(f"work product URI scheme is not allowed: {parsed.scheme}")
        if parsed.username or parsed.password:
            raise ValueError("work product URI cannot contain embedded credentials")
        if kind is WorkProductKind.WORKSPACE_FILE and parsed.scheme != "workspace":
            raise ValueError("workspace_file products require a workspace URI")
        if parsed.scheme == "workspace":
            if workspace is None:
                raise ValueError("workspace URI requires a task workspace")
            raw = str(uri).removeprefix("workspace:")
            if (
                not raw
                or parsed.netloc
                or parsed.query
                or parsed.fragment
                or "%" in raw
                or raw.startswith(("/", "\\"))
            ):
                raise ValueError("workspace URI must contain one relative path")
            pure = Path(raw)
            if pure.is_absolute() or ".." in pure.parts:
                raise ValueError("workspace URI cannot escape the task workspace")
            root = Path(workspace).expanduser().resolve(strict=True)
            candidate = (root / pure).resolve(strict=True)
            try:
                candidate.relative_to(root)
            except ValueError as exc:
                raise PermissionError(
                    "workspace work product resolves outside the task workspace"
                ) from exc
            if not candidate.is_file():
                raise ValueError("workspace work product must reference a file")

    def _insert_work_product(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        run_id: Optional[str],
        kind: WorkProductKind,
        title: str,
        summary: str,
        evidence_id: Optional[str],
        artifact_id: Optional[str],
        uri: Optional[str],
        content_hash: Optional[str],
        metadata: Mapping[str, Any],
        verification_status: str,
        created_by: str,
        command_id: Optional[str],
    ) -> WorkProductRecord:
        task = self._require_task(connection, task_id)
        self._validate_work_product_uri(
            uri, kind=kind, workspace=task.workspace
        )
        if not str(title).strip():
            raise ValueError("work product title is required")
        if contains_secret_like(
            {
                "title": str(title),
                "summary": str(summary),
                "uri": uri,
                "metadata": dict(metadata),
            }
        ):
            raise ValueError(
                "work product metadata cannot contain secret-like values"
            )
        if verification_status not in {"unverified", "verified", "stale", "missing", "failed"}:
            raise ValueError("invalid work product verification status")
        if run_id:
            run = self._require_run(connection, run_id)
            if run.task_id != task_id:
                raise ConflictError("work product run belongs to another task")
        if evidence_id:
            evidence = self._require_evidence(connection, evidence_id)
            if evidence.task_id != task_id:
                raise ConflictError("work product evidence belongs to another task")
        product_id = _id("wp")
        connection.execute(
            """
            INSERT INTO orch_work_products(
                id, task_id, run_id, kind, title, summary, evidence_id,
                artifact_id, uri, content_hash, metadata_json,
                verification_status, created_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                product_id,
                task_id,
                run_id,
                kind.value,
                str(title).strip()[:500],
                str(summary).strip()[:16_000],
                evidence_id,
                artifact_id,
                uri,
                content_hash,
                _json(metadata),
                verification_status,
                str(created_by).strip(),
                _stamp(_now()),
            ),
        )
        self._append_event(
            connection,
            task_id=task_id,
            aggregate_type="work_product",
            aggregate_id=product_id,
            event_type="work_product_created",
            payload={
                "kind": kind.value,
                "title": str(title).strip()[:500],
                "run_id": run_id,
                "content_hash": content_hash,
            },
            command_id=command_id,
        )
        return self._require_work_product(connection, product_id)

    def create_work_product(
        self,
        task_id: str,
        *,
        kind: WorkProductKind | str,
        title: str,
        summary: str = "",
        run_id: Optional[str] = None,
        evidence_id: Optional[str] = None,
        artifact_id: Optional[str] = None,
        uri: Optional[str] = None,
        content_hash: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        verification_status: str = "unverified",
        created_by: str,
        lease_token: Optional[str] = None,
        fencing_token: Optional[int] = None,
        command_id: Optional[str] = None,
    ) -> WorkProductRecord:
        kind = WorkProductKind(kind)
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "run_id": run_id,
            "kind": kind.value,
            "title": str(title),
            "summary": str(summary),
            "evidence_id": evidence_id,
            "artifact_id": artifact_id,
            "uri": uri,
            "content_hash": content_hash,
            "metadata": dict(metadata or {}),
            "verification_status": verification_status,
            "created_by": str(created_by),
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "work_product.create", task_id, request
            )
            if replay is not None:
                return self._require_work_product(connection, replay["work_product_id"])
            if run_id:
                if lease_token is None or fencing_token is None:
                    raise LeaseConflict("Agent work products require lease and fencing tokens")
                run = self._require_run(connection, run_id)
                self._require_lease(connection, run.id, lease_token, fencing_token)
                if run.task_id != task_id:
                    raise LeaseConflict("work product run belongs to another task")
            product = self._insert_work_product(
                connection,
                task_id=task_id,
                run_id=run_id,
                kind=kind,
                title=title,
                summary=summary,
                evidence_id=evidence_id,
                artifact_id=artifact_id,
                uri=uri,
                content_hash=content_hash,
                metadata=metadata or {},
                verification_status=verification_status,
                created_by=created_by,
                command_id=command_id,
            )
            self._finish_command(
                connection, command_id, {"work_product_id": product.id}
            )
            return product

    def get_work_product(self, product_id: str) -> WorkProductRecord:
        with self._read() as connection:
            return self._require_work_product(connection, product_id)

    def list_work_products(
        self,
        task_id: str,
        *,
        kind: Optional[WorkProductKind] = None,
        limit: int = 1_000,
        offset: int = 0,
    ) -> tuple[WorkProductRecord, ...]:
        where = ["task_id = ?"]
        params: list[Any] = [task_id]
        if kind is not None:
            where.append("kind = ?")
            params.append(WorkProductKind(kind).value)
        params.extend((max(1, min(int(limit), 10_000)), max(0, int(offset))))
        with self._read() as connection:
            self._require_task(connection, task_id)
            rows = connection.execute(
                "SELECT * FROM orch_work_products WHERE "
                + " AND ".join(where)
                + " ORDER BY created_at, id LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        return tuple(self._work_product_from_row(row) for row in rows)

    def verify_work_product(
        self,
        product_id: str,
        *,
        available: bool,
        actual_hash: Optional[str],
        actor: str,
        command_id: Optional[str] = None,
    ) -> dict[str, Any]:
        command_id = self._command_id(command_id)
        request = {
            "product_id": product_id,
            "available": bool(available),
            "actual_hash": actual_hash,
            "actor": str(actor),
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "work_product.verify", product_id, request
            )
            if replay is not None:
                return dict(replay)
            product = self._require_work_product(connection, product_id)
            status = (
                "missing"
                if not available
                else "stale"
                if product.content_hash and product.content_hash != actual_hash
                else "verified"
            )
            result = {
                "work_product_id": product.id,
                "verification_status": status,
                "expected_hash": product.content_hash,
                "actual_hash": actual_hash,
                "available": bool(available),
            }
            self._append_event(
                connection,
                task_id=product.task_id,
                aggregate_type="work_product",
                aggregate_id=product.id,
                event_type="work_product_verified",
                payload={**result, "actor": str(actor)},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, result)
            return result

    def get_task(self, task_id: str) -> TaskRecord:
        with self._read() as connection:
            return self._require_task(connection, task_id)

    def get_task_by_idempotency_key(self, key: str) -> TaskRecord:
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_tasks WHERE idempotency_key = ?", (key,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"task not found for idempotency key: {key}")
        return self._task_from_row(row)

    def list_tasks(
        self,
        *,
        statuses: Optional[Sequence[TaskStatus]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[TaskRecord, ...]:
        limit = max(1, min(int(limit), 10_000))
        offset = max(0, int(offset))
        params: list[Any] = []
        where = ""
        if statuses:
            values = [TaskStatus(status).value for status in statuses]
            where = "WHERE status IN (" + ",".join("?" for _ in values) + ")"
            params.extend(values)
        params.extend((limit, offset))
        with self._read() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM orch_tasks {where}
                ORDER BY priority DESC, created_at, id LIMIT ? OFFSET ?
                """,
                params,
            ).fetchall()
        return tuple(self._task_from_row(row) for row in rows)

    def list_all_tasks(
        self,
        *,
        statuses: Optional[Sequence[TaskStatus]] = None,
        page_size: int = 1_000,
    ) -> tuple[TaskRecord, ...]:
        """Return one snapshot of every matching task without the control-plane cap.

        Runtime recovery, descendant traversal, and cancellation must never inherit the
        paginated UI default.  ``fetchmany`` bounds the SQLite/Python transfer batch while
        keeping one SELECT cursor open, so concurrent priority/status changes cannot cause
        offset pagination to duplicate or omit rows within this snapshot.
        """

        page_size = max(1, min(int(page_size), 10_000))
        params: list[Any] = []
        where = ""
        if statuses:
            values = [TaskStatus(status).value for status in statuses]
            where = "WHERE status IN (" + ",".join("?" for _ in values) + ")"
            params.extend(values)
        with self._read() as connection:
            cursor = connection.execute(
                f"""
                SELECT * FROM orch_tasks {where}
                ORDER BY priority DESC, created_at, id
                """,
                params,
            )
            records: list[TaskRecord] = []
            while True:
                rows = cursor.fetchmany(page_size)
                if not rows:
                    break
                records.extend(self._task_from_row(row) for row in rows)
        return tuple(records)

    def list_root_tasks(
        self, *, include_archived: bool = False
    ) -> tuple[TaskRecord, ...]:
        where = "parent_task_id IS NULL"
        params: tuple[Any, ...] = ()
        if not include_archived:
            where += " AND status <> ?"
            params = (TaskStatus.ARCHIVED.value,)
        with self._read() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM orch_tasks
                WHERE {where}
                ORDER BY priority DESC, created_at, id
                """,
                params,
            ).fetchall()
        return tuple(self._task_from_row(row) for row in rows)

    def list_task_tree(
        self,
        root_task_id: str,
        *,
        include_root: bool = True,
        max_depth: Optional[int] = None,
        max_rows: Optional[int] = None,
    ) -> tuple[TaskRecord, ...]:
        """Read a deterministic parent/child tree with optional hard bounds.

        Coordinator-owned callers intentionally omit the limits because they must
        observe a complete durable subtree.  Interactive read models supply both
        limits so a large delegated hierarchy cannot turn one detail request into
        an unbounded recursive query or response.
        """

        bounded_depth = None if max_depth is None else int(max_depth)
        bounded_rows = None if max_rows is None else int(max_rows)
        if bounded_depth is not None and bounded_depth < 0:
            raise ValueError("max_depth must be >= 0")
        if bounded_rows is not None and bounded_rows < 1:
            raise ValueError("max_rows must be >= 1")

        depth_clause = (
            "" if bounded_depth is None else "WHERE tree.depth < ?"
        )
        limit_clause = "" if bounded_rows is None else "LIMIT ?"
        params: list[Any] = [root_task_id]
        if bounded_depth is not None:
            params.append(bounded_depth)
        params.append(1 if include_root else 0)
        if bounded_rows is not None:
            params.append(bounded_rows)

        with self._read() as connection:
            self._require_task(connection, root_task_id)
            rows = connection.execute(
                f"""
                WITH RECURSIVE tree(id, depth) AS (
                    SELECT id, 0 FROM orch_tasks WHERE id = ?
                    UNION ALL
                    SELECT child.id, tree.depth + 1
                    FROM orch_tasks child
                    JOIN tree ON child.parent_task_id = tree.id
                    {depth_clause}
                )
                SELECT task.*
                FROM tree JOIN orch_tasks task ON task.id = tree.id
                WHERE ? OR tree.depth > 0
                ORDER BY tree.depth, task.created_at, task.id
                {limit_clause}
                """,
                params,
            ).fetchall()
        return tuple(self._task_from_row(row) for row in rows)

    def runtime_tree_snapshot(
        self, root_task_id: str
    ) -> tuple[
        tuple[TaskRecord, ...],
        Mapping[str, tuple[RunRecord, ...]],
        Mapping[str, PlanGraph],
        Mapping[str, tuple[EvidenceRecord, ...]],
    ]:
        """Read all durable inputs for one runtime projection at one DB snapshot.

        Runtime reconstruction combines task state, run state, and immutable plan
        topology.  Reading those ledgers through separate connections can observe
        different WAL commits: for example, an old live-child task row together with
        a newly claimed downstream run.  Such a combination never existed durably and
        must not be interpreted as a broken dependency.  An explicit read transaction
        pins all query groups to one SQLite snapshot while still allowing WAL
        writers to make progress.
        """

        with self._read() as connection:
            connection.execute("BEGIN")
            try:
                self._require_task(connection, root_task_id)
                task_rows = connection.execute(
                    """
                    WITH RECURSIVE tree(id, depth) AS (
                        SELECT id, 0 FROM orch_tasks WHERE id = ?
                        UNION ALL
                        SELECT child.id, tree.depth + 1
                        FROM orch_tasks child
                        JOIN tree ON child.parent_task_id = tree.id
                    )
                    SELECT task.*
                    FROM tree JOIN orch_tasks task ON task.id = tree.id
                    ORDER BY tree.depth, task.created_at, task.id
                    """,
                    (root_task_id,),
                ).fetchall()
                tasks = tuple(self._task_from_row(row) for row in task_rows)
                runs_by_task: dict[str, list[RunRecord]] = {
                    task.id: [] for task in tasks
                }
                run_rows = connection.execute(
                    """
                    WITH RECURSIVE tree(id) AS (
                        SELECT id FROM orch_tasks WHERE id = ?
                        UNION ALL
                        SELECT child.id
                        FROM orch_tasks child
                        JOIN tree ON child.parent_task_id = tree.id
                    )
                    SELECT r.*, n.node_key
                    FROM tree
                    JOIN orch_runs r ON r.task_id = tree.id
                    JOIN orch_nodes n ON n.id = r.node_id
                    ORDER BY r.task_id, r.created_at, r.attempt, r.id
                    """,
                    (root_task_id,),
                ).fetchall()
                for row in run_rows:
                    run = self._run_from_row(row)
                    runs_by_task[run.task_id].append(run)
                plans = {
                    plan_id: self._get_plan_graph(connection, plan_id)
                    for plan_id in sorted(
                        {run.plan_id for runs in runs_by_task.values() for run in runs}
                    )
                }
                usage_by_run: dict[str, list[EvidenceRecord]] = {}
                evidence_rows = connection.execute(
                    """
                    WITH RECURSIVE tree(id) AS (
                        SELECT id FROM orch_tasks WHERE id = ?
                        UNION ALL
                        SELECT child.id
                        FROM orch_tasks child
                        JOIN tree ON child.parent_task_id = tree.id
                    )
                    SELECT e.*
                    FROM tree
                    JOIN orch_evidence e ON e.task_id = tree.id
                    WHERE e.run_id IS NOT NULL
                      AND json_extract(
                          e.payload_json, '$.runtime_usage_segment'
                      ) = 1
                    ORDER BY e.run_id, e.created_at, e.id
                    """,
                    (root_task_id,),
                ).fetchall()
                for row in evidence_rows:
                    evidence = self._evidence_from_row(row)
                    if evidence.run_id is None:
                        raise IntegrityError(
                            f"runtime usage evidence {evidence.id} has no run"
                        )
                    usage_by_run.setdefault(evidence.run_id, []).append(evidence)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return (
            tasks,
            {task_id: tuple(runs) for task_id, runs in runs_by_task.items()},
            plans,
            {run_id: tuple(items) for run_id, items in usage_by_run.items()},
        )

    def count_task_children(self, task_ids: Sequence[str]) -> dict[str, int]:
        """Return direct-child counts for a bounded set of already-read tasks."""

        chosen = tuple(dict.fromkeys(str(task_id) for task_id in task_ids if task_id))
        counts = {task_id: 0 for task_id in chosen}
        if not chosen:
            return counts
        with self._read() as connection:
            for index in range(0, len(chosen), 400):
                chunk = chosen[index : index + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = connection.execute(
                    f"""
                    SELECT parent_task_id, COUNT(*) AS child_count
                    FROM orch_tasks
                    WHERE parent_task_id IN ({placeholders})
                    GROUP BY parent_task_id
                    """,
                    chunk,
                ).fetchall()
                for row in rows:
                    counts[str(row["parent_task_id"])] = int(row["child_count"])
        return counts

    def transition_task_status(
        self,
        task_id: str,
        target: TaskStatus,
        *,
        expected_version: int,
        output: Optional[Mapping[str, Any]] = None,
        command_id: Optional[str] = None,
    ) -> TaskRecord:
        target = TaskStatus(target)
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "target": target.value,
            "expected_version": expected_version,
            "output": output,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "task.transition_status", task_id, request
            )
            if replay is not None:
                return self._require_task(connection, replay["task_id"])
            task = self._require_task(connection, task_id)
            if task.version != expected_version:
                raise VersionConflict(
                    f"task {task_id} expected version {expected_version}, found {task.version}"
                )
            if target is TaskStatus.QUEUED and not task.active_brief_id:
                raise ConflictError("a task requires a published brief before it can be queued")
            validate_task_transition(task.status, target)
            now = _stamp(_now())
            changed = connection.execute(
                """
                UPDATE orch_tasks
                SET status = ?, output_json = COALESCE(?, output_json),
                    version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (
                    target.value,
                    _json(output) if output is not None else None,
                    now,
                    task_id,
                    expected_version,
                ),
            ).rowcount
            if changed != 1:
                raise VersionConflict(f"task {task_id} changed concurrently")
            # A terminal task has no next stage transition to close the active history
            # row. Seal it in the same transaction so an archived successful lifecycle
            # reports all eight stages as completed instead of leaving `archive` active.
            terminal_disposition = {
                TaskStatus.COMPLETED: StageDisposition.COMPLETED,
                TaskStatus.FAILED: StageDisposition.FAILED,
                TaskStatus.CANCELED: StageDisposition.CANCELED,
                # Compatibility for databases created before terminal-stage sealing:
                # archiving an already terminal task closes any legacy active row.
                TaskStatus.ARCHIVED: (
                    StageDisposition.COMPLETED
                    if task.status is TaskStatus.COMPLETED
                    else StageDisposition.CANCELED
                    if task.status is TaskStatus.CANCELED
                    else StageDisposition.FAILED
                ),
            }.get(target)
            if terminal_disposition is not None:
                connection.execute(
                    """
                    UPDATE orch_stage_history
                    SET disposition = ?, exited_at = ?, detail_json = ?
                    WHERE task_id = ? AND disposition = 'active'
                    """,
                    (
                        terminal_disposition.value,
                        now,
                        _json({"reason": f"task_{target.value}"}),
                        task_id,
                    ),
                )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task",
                aggregate_id=task_id,
                event_type="task.status_changed",
                payload={"from": task.status.value, "to": target.value},
                command_id=command_id,
            )
            if task.status is TaskStatus.DRAFT and target is TaskStatus.QUEUED:
                brief = self._require_brief(connection, task.active_brief_id)  # type: ignore[arg-type]
                reason = (
                    WakeReason.TASK_ASSIGNED
                    if task.parent_task_id
                    else WakeReason.ASSIGNMENT
                )
                self._enqueue_wake_connection(
                    connection,
                    target_task_id=task.id,
                    target_run_id=None,
                    reason=reason,
                    source_task_id=task.parent_task_id,
                    source_run_id=None,
                    source_event_id=None,
                    payload={
                        "brief_id": brief.id,
                        "brief_revision": brief.revision,
                        "profile_id": task.policy.get("profile_id"),
                    },
                    dedupe_key=f"{task.id}:current:{reason.value}:{brief.id}",
                    not_before=None,
                    command_id=command_id,
                )
            if target in {TaskStatus.CANCELING, TaskStatus.CANCELED}:
                wakes = connection.execute(
                    """
                    SELECT id FROM orch_wake_requests
                    WHERE target_task_id = ? AND status IN ('pending', 'deferred')
                    """,
                    (task.id,),
                ).fetchall()
                connection.execute(
                    """
                    UPDATE orch_wake_requests
                    SET status = 'canceled', updated_at = ?, claimed_by = NULL,
                        claimed_until = NULL
                    WHERE target_task_id = ? AND status IN ('pending', 'deferred')
                    """,
                    (now, task.id),
                )
                for wake_row in wakes:
                    self._append_event(
                        connection,
                        task_id=task.id,
                        aggregate_type="wake_request",
                        aggregate_id=wake_row["id"],
                        event_type="wake_canceled",
                        payload={"reason": "task_cancellation"},
                        command_id=command_id,
                    )
            self._finish_command(connection, command_id, {"task_id": task_id})
            return self._require_task(connection, task_id)

    def transition_stage(
        self,
        task_id: str,
        target: OrchestrationStage,
        *,
        expected_version: int,
        disposition: StageDisposition = StageDisposition.COMPLETED,
        detail: Optional[Mapping[str, Any]] = None,
        command_id: Optional[str] = None,
    ) -> TaskRecord:
        target = OrchestrationStage(target)
        disposition = StageDisposition(disposition)
        if disposition is StageDisposition.ACTIVE:
            raise ValueError("the exited stage disposition cannot remain active")
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "target": target.value,
            "expected_version": expected_version,
            "disposition": disposition.value,
            "detail": detail or {},
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "task.transition_stage", task_id, request
            )
            if replay is not None:
                return self._require_task(connection, replay["task_id"])
            task = self._require_task(connection, task_id)
            if task.version != expected_version:
                raise VersionConflict(
                    f"task {task_id} expected version {expected_version}, found {task.version}"
                )
            validate_stage_transition(task.current_stage, target, disposition)
            active = connection.execute(
                """
                SELECT * FROM orch_stage_history
                WHERE task_id = ? AND disposition = 'active'
                """,
                (task_id,),
            ).fetchone()
            if active is None or active["stage"] != task.current_stage.value:
                raise IntegrityError(f"task {task_id} has no matching active stage")
            now = _stamp(_now())
            shortcut = (
                task.current_stage is OrchestrationStage.COMPLEXITY_ASSESSMENT
                and target is OrchestrationStage.PLANNING
            )
            exited_disposition = (
                StageDisposition.COMPLETED if shortcut else disposition
            )
            connection.execute(
                """
                UPDATE orch_stage_history
                SET disposition = ?, exited_at = ?, detail_json = ?
                WHERE id = ? AND disposition = 'active'
                """,
                (
                    exited_disposition.value,
                    now,
                    _json(detail or {}),
                    active["id"],
                ),
            )
            sequence = int(active["sequence_no"])
            skipped: list[str] = []
            if shortcut:
                sequence += 1
                connection.execute(
                    """
                    INSERT INTO orch_stage_history(
                        id, task_id, sequence_no, stage, disposition, entered_at,
                        exited_at, detail_json, command_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _id("stage"),
                        task_id,
                        sequence,
                        OrchestrationStage.CLARIFICATION.value,
                        StageDisposition.SKIPPED.value,
                        now,
                        now,
                        _json(detail or {}),
                        command_id,
                    ),
                )
                skipped.append(OrchestrationStage.CLARIFICATION.value)
            sequence += 1
            connection.execute(
                """
                INSERT INTO orch_stage_history(
                    id, task_id, sequence_no, stage, disposition, entered_at,
                    detail_json, command_id
                ) VALUES (?, ?, ?, ?, 'active', ?, '{}', ?)
                """,
                (_id("stage"), task_id, sequence, target.value, now, command_id),
            )
            changed = connection.execute(
                """
                UPDATE orch_tasks
                SET current_stage = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (target.value, now, task_id, expected_version),
            ).rowcount
            if changed != 1:
                raise VersionConflict(f"task {task_id} changed concurrently")
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task",
                aggregate_id=task_id,
                event_type="task.stage_changed",
                payload={
                    "from": task.current_stage.value,
                    "to": target.value,
                    "disposition": disposition.value,
                    "skipped": skipped,
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"task_id": task_id})
            return self._require_task(connection, task_id)

    def apply_clarification(
        self,
        task_id: str,
        response: str,
        *,
        expected_version: int,
        resolved_by: str,
        gate_id: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> TaskRecord:
        """Atomically incorporate an approved clarification into the task contract."""

        criterion = str(response).strip()
        actor = str(resolved_by).strip()
        if not criterion:
            raise ValueError("clarification response is required")
        if not actor:
            raise ValueError("resolved_by is required")
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "response": criterion,
            "expected_version": expected_version,
            "resolved_by": actor,
            "gate_id": gate_id,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection,
                command_id,
                "task.apply_clarification",
                task_id,
                request,
            )
            if replay is not None:
                return self._require_task(connection, replay["task_id"])
            task = self._require_task(connection, task_id)
            if task.version != expected_version:
                raise VersionConflict(
                    f"task {task_id} expected version {expected_version}, found {task.version}"
                )
            if task.current_stage is not OrchestrationStage.CLARIFICATION:
                raise ConflictError(
                    f"task {task_id} is not in the clarification stage"
                )
            if gate_id is not None:
                gate = self._require_gate(connection, gate_id)
                if gate.task_id != task_id or gate.kind is not GateKind.CLARIFICATION:
                    raise ConflictError(
                        f"gate {gate_id} is not a clarification gate for task {task_id}"
                    )
                if gate.status is not GateStatus.APPROVED:
                    raise GateConflict(
                        f"clarification gate {gate_id} is {gate.status.value}, not approved"
                    )

            criteria = list(task.acceptance_criteria)
            if criterion not in criteria:
                criteria.append(criterion)
            input_data = dict(task.input)
            existing = input_data.get("clarifications", [])
            if not isinstance(existing, list):
                raise IntegrityError("task input.clarifications must be a list")
            now = _stamp(_now())
            input_data["clarifications"] = [
                *existing,
                {
                    "response": criterion,
                    "resolved_by": actor,
                    "gate_id": gate_id,
                    "applied_at": now,
                },
            ]
            changed = connection.execute(
                """
                UPDATE orch_tasks
                SET acceptance_criteria_json = ?, input_json = ?,
                    version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (
                    _json(criteria),
                    _json(input_data),
                    now,
                    task_id,
                    expected_version,
                ),
            ).rowcount
            if changed != 1:
                raise VersionConflict(f"task {task_id} changed concurrently")
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="task",
                aggregate_id=task_id,
                event_type="task.clarification_applied",
                payload={
                    "criterion": criterion,
                    "resolved_by": actor,
                    "gate_id": gate_id,
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"task_id": task_id})
            return self._require_task(connection, task_id)

    def stage_history(
        self,
        task_id: str,
        *,
        limit: Optional[int] = None,
        newest: bool = False,
    ) -> tuple[StageHistoryRecord, ...]:
        bounded = max(1, min(int(limit), 10_001)) if limit is not None else None
        with self._read() as connection:
            self._require_task(connection, task_id)
            if bounded is not None and newest:
                rows = connection.execute(
                    """
                    SELECT * FROM (
                        SELECT * FROM orch_stage_history
                        WHERE task_id = ? ORDER BY sequence_no DESC LIMIT ?
                    ) ORDER BY sequence_no
                    """,
                    (task_id, bounded),
                ).fetchall()
            else:
                suffix = " LIMIT ?" if bounded is not None else ""
                params: list[Any] = [task_id]
                if bounded is not None:
                    params.append(bounded)
                rows = connection.execute(
                    "SELECT * FROM orch_stage_history "
                    f"WHERE task_id = ? ORDER BY sequence_no{suffix}",
                    params,
                ).fetchall()
        return tuple(self._stage_from_row(row) for row in rows)

    def stage_projection(self, task_id: str) -> tuple[StageHistoryRecord, ...]:
        """Return only the two newest visits per lifecycle stage for a read model."""

        with self._read() as connection:
            self._require_task(connection, task_id)
            rows = connection.execute(
                """
                WITH ranked AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY stage ORDER BY sequence_no DESC
                    ) AS stage_rank
                    FROM orch_stage_history WHERE task_id = ?
                )
                SELECT * FROM ranked WHERE stage_rank <= 2 ORDER BY sequence_no
                """,
                (task_id,),
            ).fetchall()
        return tuple(self._stage_from_row(row) for row in rows)

    # -- immutable plan revisions ------------------------------------------
    def create_plan_revision(
        self,
        task_id: str,
        spec: PlanSpec,
        *,
        expected_task_version: int,
        created_by: str,
        command_id: Optional[str] = None,
    ) -> PlanGraph:
        order = validate_plan(spec)
        plan_payload = {
            "nodes": [_jsonable(node) for node in spec.nodes],
            "edges": [_jsonable(edge) for edge in spec.edges],
            "metadata": _jsonable(spec.metadata),
            "topological_order": order,
        }
        content_hash = _digest(plan_payload)
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "expected_task_version": expected_task_version,
            "created_by": created_by,
            "content_hash": content_hash,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "plan.create_revision", task_id, request
            )
            if replay is not None:
                return self._get_plan_graph(connection, replay["plan_id"])
            task = self._require_task(connection, task_id)
            if task.version != expected_task_version:
                raise VersionConflict(
                    f"task {task_id} expected version {expected_task_version}, found {task.version}"
                )
            latest = connection.execute(
                "SELECT id, revision FROM orch_plans WHERE task_id = ? ORDER BY revision DESC LIMIT 1",
                (task_id,),
            ).fetchone()
            revision = int(latest["revision"]) + 1 if latest else 1
            parent_plan_id = latest["id"] if latest else None
            plan_id = _id("plan")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_plans(
                    id, task_id, revision, parent_plan_id, content_hash,
                    metadata_json, created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan_id,
                    task_id,
                    revision,
                    parent_plan_id,
                    content_hash,
                    _json(spec.metadata),
                    created_by,
                    now,
                ),
            )
            node_ids: dict[str, str] = {}
            for node in spec.nodes:
                node_id = _id("node")
                node_ids[node.key] = node_id
                connection.execute(
                    """
                    INSERT INTO orch_nodes(
                        id, plan_id, node_key, title, instructions, kind, agent,
                        model, input_json, join_policy, failure_policy, effect_safety,
                        retry_policy_json, timeout_seconds, priority, concurrency_key,
                        metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        node_id,
                        plan_id,
                        node.key,
                        node.title,
                        node.instructions,
                        NodeKind(node.kind).value,
                        node.agent,
                        node.model,
                        _json(node.input),
                        JoinPolicy(node.join_policy).value,
                        FailurePolicy(node.failure_policy).value,
                        EffectSafety(node.effect_safety).value,
                        _json(node.retry_policy),
                        node.timeout_seconds,
                        node.priority,
                        node.concurrency_key,
                        _json(node.metadata),
                    ),
                )
            for edge in spec.edges:
                connection.execute(
                    """
                    INSERT INTO orch_edges(
                        id, plan_id, from_node_id, to_node_id, condition,
                        required, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _id("edge"),
                        plan_id,
                        node_ids[edge.from_node],
                        node_ids[edge.to_node],
                        EdgeCondition(edge.condition).value,
                        int(edge.required),
                        _json(edge.metadata),
                    ),
                )
            changed = connection.execute(
                """
                UPDATE orch_tasks
                SET active_plan_id = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                (plan_id, now, task_id, expected_task_version),
            ).rowcount
            if changed != 1:
                raise VersionConflict(f"task {task_id} changed concurrently")
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="plan",
                aggregate_id=plan_id,
                event_type="plan.revision_created",
                payload={
                    "revision": revision,
                    "content_hash": content_hash,
                    "topological_order": order,
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"plan_id": plan_id})
            return self._get_plan_graph(connection, plan_id)

    def get_plan(self, plan_id: str) -> PlanGraph:
        with self._read() as connection:
            return self._get_plan_graph(connection, plan_id)

    def list_plans(self, task_id: str) -> tuple[PlanRecord, ...]:
        with self._read() as connection:
            self._require_task(connection, task_id)
            rows = connection.execute(
                "SELECT * FROM orch_plans WHERE task_id = ? ORDER BY revision",
                (task_id,),
            ).fetchall()
        return tuple(self._plan_from_row(row) for row in rows)

    # -- runs and fenced leases --------------------------------------------
    def enqueue_run(
        self,
        task_id: str,
        node_key: str,
        *,
        plan_id: Optional[str] = None,
        attempt: Optional[int] = None,
        ready_at: Optional[datetime] = None,
        priority: Optional[int] = None,
        session_id: Optional[str] = None,
        recovery_gate_id: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> RunRecord:
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "plan_id": plan_id,
            "node_key": node_key,
            "attempt": attempt,
            "ready_at": ready_at,
            "priority": priority,
            "session_id": session_id,
            "recovery_gate_id": recovery_gate_id,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.enqueue", task_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            task = self._require_task(connection, task_id)
            if task.status not in {TaskStatus.QUEUED, TaskStatus.RUNNING}:
                raise ConflictError(
                    "runs may be enqueued only for a queued or running task, "
                    f"found {task.status.value}"
                )
            selected_plan = plan_id or task.active_plan_id
            if selected_plan != task.active_plan_id:
                raise ConflictError("runs may be enqueued only for the active plan revision")
            if not selected_plan:
                raise ConflictError(f"task {task_id} has no active plan")
            plan = connection.execute(
                "SELECT * FROM orch_plans WHERE id = ? AND task_id = ?",
                (selected_plan, task_id),
            ).fetchone()
            if plan is None:
                raise NotFoundError(f"plan {selected_plan} does not belong to task {task_id}")
            node = connection.execute(
                "SELECT * FROM orch_nodes WHERE plan_id = ? AND node_key = ?",
                (selected_plan, node_key),
            ).fetchone()
            if node is None:
                raise NotFoundError(f"node not found in plan: {node_key}")
            latest = connection.execute(
                "SELECT MAX(attempt) FROM orch_runs WHERE node_id = ?", (node["id"],)
            ).fetchone()[0]
            chosen_attempt = int(attempt) if attempt is not None else int(latest or 0) + 1
            retry = _load(node["retry_policy_json"], {})
            max_attempts = int(retry.get("max_attempts", 1))
            if chosen_attempt < 1:
                raise ConflictError(f"attempt {chosen_attempt} must be positive")
            compatibility_override = False
            if chosen_attempt > max_attempts and recovery_gate_id:
                gate = self._require_gate(connection, recovery_gate_id)
                compatibility = gate.prompt.get("compatibility_retry")
                base_attempts = (
                    compatibility.get("base_attempts")
                    if isinstance(compatibility, Mapping)
                    else None
                )
                base = (
                    base_attempts.get(node_key)
                    if isinstance(base_attempts, Mapping)
                    else None
                )
                source_run_id = (
                    str(base.get("run_id") or "")
                    if isinstance(base, Mapping)
                    else ""
                )
                try:
                    source_attempt = (
                        int(base.get("attempt") or 0)
                        if isinstance(base, Mapping)
                        else 0
                    )
                except (TypeError, ValueError):
                    source_attempt = 0
                source_run = (
                    self._require_run(connection, source_run_id)
                    if source_run_id
                    else None
                )
                compatibility_override = bool(
                    gate.task_id == task_id
                    and gate.run_id is None
                    and gate.kind is GateKind.RECONCILIATION
                    and gate.status is GateStatus.APPROVED
                    and str((gate.resolution or {}).get("decision") or "")
                    == "retry"
                    and isinstance(compatibility, Mapping)
                    and str(compatibility.get("reason") or "")
                    in _COMPATIBILITY_RETRY_REASONS
                    and source_run is not None
                    and source_run.task_id == task_id
                    and source_run.node_id == node["id"]
                    and source_run.status is RunStatus.SUCCEEDED
                    and source_run.attempt == source_attempt
                    and int(latest or 0) == source_attempt
                    and chosen_attempt == source_attempt + 1
                    and node["kind"]
                    in {
                        NodeKind.REVIEW.value,
                        NodeKind.TEST.value,
                        NodeKind.EVALUATE.value,
                    }
                )
            if chosen_attempt > max_attempts and not compatibility_override:
                raise ConflictError(
                    f"attempt {chosen_attempt} exceeds node max_attempts "
                    f"{max_attempts}"
                )
            run_id = _id("run")
            now = _now()
            connection.execute(
                """
                INSERT INTO orch_runs(
                    id, task_id, plan_id, node_id, attempt, status, session_id,
                    priority, ready_at, fencing_token, version, created_at, brief_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?)
                """,
                (
                    run_id,
                    task_id,
                    selected_plan,
                    node["id"],
                    chosen_attempt,
                    RunStatus.QUEUED.value,
                    session_id,
                    int(priority if priority is not None else node["priority"]),
                    _stamp(ready_at or now),
                    _stamp(now),
                    task.active_brief_id,
                ),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.queued",
                payload={
                    "node_key": node_key,
                    "attempt": chosen_attempt,
                    "recovery_gate_id": (
                        recovery_gate_id if compatibility_override else None
                    ),
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run_id})
            return self._require_run(connection, run_id)

    def skip_node(
        self,
        task_id: str,
        node_key: str,
        *,
        reason: str,
        plan_id: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> RunRecord:
        """Persist a terminal node result when its DAG edge condition cannot match."""

        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "plan_id": plan_id,
            "node_key": node_key,
            "reason": reason,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.skip", task_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            task = self._require_task(connection, task_id)
            selected_plan = plan_id or task.active_plan_id
            if not selected_plan:
                raise ConflictError(f"task {task_id} has no active plan")
            node = connection.execute(
                "SELECT * FROM orch_nodes WHERE plan_id = ? AND node_key = ?",
                (selected_plan, node_key),
            ).fetchone()
            if node is None:
                raise NotFoundError(f"node not found in plan: {node_key}")
            existing = connection.execute(
                """
                SELECT r.*, n.node_key FROM orch_runs r
                JOIN orch_nodes n ON n.id = r.node_id
                WHERE r.node_id = ? ORDER BY r.attempt DESC LIMIT 1
                """,
                (node["id"],),
            ).fetchone()
            if existing is not None:
                if RunStatus(existing["status"]) is RunStatus.SKIPPED:
                    self._finish_command(connection, command_id, {"run_id": existing["id"]})
                    return self._run_from_row(existing)
                raise ConflictError(f"node {node_key} already has an execution attempt")
            run_id = _id("run")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_runs(
                    id, task_id, plan_id, node_id, attempt, status, priority,
                    ready_at, fencing_token, error_kind, error_message, version,
                    created_at, finished_at, brief_id
                ) VALUES (?, ?, ?, ?, 1, 'skipped', ?, ?, 0,
                          'edge_condition', ?, 1, ?, ?, ?)
                """,
                (
                    run_id,
                    task_id,
                    selected_plan,
                    node["id"],
                    int(node["priority"]),
                    now,
                    reason[:2_000],
                    now,
                    now,
                    task.active_brief_id,
                ),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.skipped",
                payload={"node_key": node_key, "reason": reason},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run_id})
            return self._require_run(connection, run_id)

    def skip_pending_node(
        self,
        task_id: str,
        node_key: str,
        *,
        reason: str,
        plan_id: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> RunRecord:
        """Apply an audited failure-policy skip before an attempt starts.

        A failure policy is allowed to suppress a node that has not yet been
        materialized or whose latest attempt is still queued.  Claimed, running,
        gate-waiting, and already-finished attempts are never rewritten: their
        fenced execution result remains authoritative.
        """

        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "plan_id": plan_id,
            "node_key": node_key,
            "reason": reason,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.skip_pending", task_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            task = self._require_task(connection, task_id)
            selected_plan = plan_id or task.active_plan_id
            if not selected_plan:
                raise ConflictError(f"task {task_id} has no active plan")
            node = connection.execute(
                "SELECT * FROM orch_nodes WHERE plan_id = ? AND node_key = ?",
                (selected_plan, node_key),
            ).fetchone()
            if node is None:
                raise NotFoundError(f"node not found in plan: {node_key}")
            existing = connection.execute(
                """
                SELECT r.*, n.node_key FROM orch_runs r
                JOIN orch_nodes n ON n.id = r.node_id
                WHERE r.node_id = ? ORDER BY r.attempt DESC LIMIT 1
                """,
                (node["id"],),
            ).fetchone()
            now = _stamp(_now())
            if existing is None:
                run_id = _id("run")
                connection.execute(
                    """
                    INSERT INTO orch_runs(
                        id, task_id, plan_id, node_id, attempt, status, priority,
                        ready_at, fencing_token, error_kind, error_message, version,
                        created_at, finished_at, brief_id
                    ) VALUES (?, ?, ?, ?, 1, 'skipped', ?, ?, 0,
                              'failure_policy', ?, 1, ?, ?, ?)
                    """,
                    (
                        run_id,
                        task_id,
                        selected_plan,
                        node["id"],
                        int(node["priority"]),
                        now,
                        reason[:2_000],
                        now,
                        now,
                        task.active_brief_id,
                    ),
                )
                previous_status = None
            else:
                run_id = str(existing["id"])
                status = RunStatus(existing["status"])
                if status is RunStatus.SKIPPED:
                    self._finish_command(connection, command_id, {"run_id": run_id})
                    return self._require_run(connection, run_id)
                if status is not RunStatus.QUEUED:
                    raise ConflictError(
                        f"node {node_key} already has a {status.value} attempt"
                    )
                changed = connection.execute(
                    """
                    UPDATE orch_runs
                    SET status = 'skipped', error_kind = 'failure_policy',
                        error_message = ?, finished_at = ?, version = version + 1
                    WHERE id = ? AND status = 'queued' AND version = ?
                    """,
                    (reason[:2_000], now, run_id, existing["version"]),
                ).rowcount
                if changed != 1:
                    raise ConflictError(
                        f"node {node_key} was claimed while applying failure policy"
                    )
                previous_status = status.value
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.skipped",
                payload={
                    "node_key": node_key,
                    "reason": reason,
                    "from": previous_status,
                    "policy_controlled": True,
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run_id})
            return self._require_run(connection, run_id)

    def reopen_policy_skipped_run(
        self,
        run_id: str,
        *,
        reason: str,
        session_id: str,
        ready_at: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> RunRecord:
        """Requeue an unstarted attempt after its failure-policy cause recovered.

        A policy-controlled skip is not an Agent execution result: it has no lease,
        checkpoint, or started timestamp.  Reopening that same attempt preserves the
        node's real retry budget while retaining the original skip and reopen events.
        Fenced or otherwise terminal execution results remain immutable.
        """

        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "reason": reason,
            "session_id": session_id,
            "ready_at": ready_at,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.reopen_policy_skip", run_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            row = connection.execute(
                """
                SELECT r.*, n.node_key FROM orch_runs r
                JOIN orch_nodes n ON n.id = r.node_id
                WHERE r.id = ?
                """,
                (run_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"run not found: {run_id}")
            if (
                RunStatus(row["status"]) is not RunStatus.SKIPPED
                or str(row["error_kind"] or "") != "failure_policy"
                or row["started_at"] is not None
            ):
                raise ConflictError(
                    "only an unstarted failure-policy skip may be reopened"
                )
            if connection.execute(
                "SELECT 1 FROM orch_leases WHERE run_id = ?", (run_id,)
            ).fetchone() is not None:
                raise ConflictError("a leased run cannot be reopened")
            task = self._require_task(connection, str(row["task_id"]))
            if task.status not in {TaskStatus.QUEUED, TaskStatus.RUNNING}:
                raise ConflictError(
                    "policy-skipped runs may be reopened only for a queued or running task"
                )
            reopened_at = _stamp(_now())
            changed = connection.execute(
                """
                UPDATE orch_runs
                SET status = 'queued', session_id = ?, ready_at = ?,
                    error_kind = NULL, error_message = NULL, finished_at = NULL,
                    brief_id = ?, created_at = ?, version = version + 1
                WHERE id = ? AND status = 'skipped'
                  AND error_kind = 'failure_policy' AND started_at IS NULL
                  AND version = ?
                """,
                (
                    session_id,
                    _stamp(ready_at or _now()),
                    task.active_brief_id,
                    reopened_at,
                    run_id,
                    int(row["version"]),
                ),
            ).rowcount
            if changed != 1:
                raise ConflictError(f"run {run_id} changed while it was being reopened")
            self._append_event(
                connection,
                task_id=task.id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.reopened",
                payload={
                    "node_key": str(row["node_key"]),
                    "attempt": int(row["attempt"]),
                    "from": RunStatus.SKIPPED.value,
                    "to": RunStatus.QUEUED.value,
                    "reason": reason,
                    "policy_controlled": True,
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run_id})
            return self._require_run(connection, run_id)

    def get_run(self, run_id: str) -> RunRecord:
        with self._read() as connection:
            return self._require_run(connection, run_id)

    def append_run_activity(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        event_key: str,
        source_id: str,
        kind: str,
        status: str,
        title: str,
        summary: str = "",
        detail: Optional[Mapping[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> RunActivityRecord:
        """Append one safe live-activity row under the current run lease.

        ``event_key`` makes protocol retries idempotent.  The hard per-run row cap
        prevents a noisy vendor stream from growing orchestration.db without bound.
        """

        normalized_kind = str(kind).strip().lower()
        normalized_status = str(status).strip().lower()
        if normalized_kind not in RUN_ACTIVITY_KINDS:
            raise ValueError(f"unsupported run activity kind: {kind}")
        if normalized_status not in RUN_ACTIVITY_STATUSES:
            raise ValueError(f"unsupported run activity status: {status}")
        normalized_key = bounded_activity_text(event_key, 256).strip()
        normalized_source = bounded_activity_text(source_id, 256).strip()
        normalized_title = bounded_activity_text(title, 200).strip()
        if not normalized_key or not normalized_source or not normalized_title:
            raise ValueError("run activity requires event_key, source_id, and title")
        normalized_summary = bounded_activity_text(summary, 4_096)
        normalized_detail = sanitize_activity_detail(dict(detail or {}))
        created = _stamp(created_at or _now())
        activity_id = "activity_" + hashlib.sha256(
            f"{run_id}\n{normalized_key}".encode("utf-8")
        ).hexdigest()[:32]

        with self._write() as connection:
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            existing = connection.execute(
                "SELECT * FROM orch_run_activity WHERE run_id = ? AND event_key = ?",
                (run_id, normalized_key),
            ).fetchone()
            if existing is not None:
                return self._run_activity_from_row(existing)

            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM orch_run_activity WHERE run_id = ?", (run_id,)
                ).fetchone()[0]
            )
            if count >= MAX_RUN_ACTIVITY_ROWS:
                normalized_key = "openworker:activity_truncated"
                normalized_source = "openworker:activity_stream"
                normalized_kind = "lifecycle"
                normalized_status = "info"
                normalized_title = "Activity stream truncated"
                normalized_summary = (
                    f"Only the first {MAX_RUN_ACTIVITY_ROWS:,} activity records are retained."
                )
                normalized_detail = {"retained_limit": MAX_RUN_ACTIVITY_ROWS}
                activity_id = "activity_" + hashlib.sha256(
                    f"{run_id}\n{normalized_key}".encode("utf-8")
                ).hexdigest()[:32]
                existing = connection.execute(
                    "SELECT * FROM orch_run_activity WHERE run_id = ? AND event_key = ?",
                    (run_id, normalized_key),
                ).fetchone()
                if existing is not None:
                    return self._run_activity_from_row(existing)

            connection.execute(
                """
                INSERT INTO orch_run_activity(
                    id, task_id, run_id, event_key, source_id, kind, status,
                    title, summary, detail_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    activity_id,
                    run.task_id,
                    run_id,
                    normalized_key,
                    normalized_source,
                    normalized_kind,
                    normalized_status,
                    normalized_title,
                    normalized_summary,
                    _json(normalized_detail),
                    created,
                ),
            )
            row = connection.execute(
                "SELECT * FROM orch_run_activity WHERE id = ?", (activity_id,)
            ).fetchone()
            assert row is not None
            return self._run_activity_from_row(row)

    def list_run_activity(
        self,
        task_id: str,
        run_id: str,
        *,
        after_sequence: int = 0,
        before_sequence: Optional[int] = None,
        newest: bool = False,
        limit: int = 1_000,
    ) -> tuple[RunActivityRecord, ...]:
        """Return a bounded chronological page for one task-owned run."""

        bounded = max(1, min(int(limit), 2_001))
        params: list[Any] = [run_id, task_id, max(0, int(after_sequence))]
        where = "run_id = ? AND task_id = ? AND sequence_no > ?"
        if before_sequence is not None:
            where += " AND sequence_no < ?"
            params.append(max(1, int(before_sequence)))
        params.append(bounded)
        with self._read() as connection:
            self._require_task(connection, task_id)
            run = self._require_run(connection, run_id)
            if run.task_id != task_id:
                raise NotFoundError(f"run not found for task {task_id}: {run_id}")
            rows = connection.execute(
                f"""
                SELECT * FROM orch_run_activity WHERE {where}
                ORDER BY sequence_no {'DESC' if newest else 'ASC'} LIMIT ?
                """,
                params,
            ).fetchall()
        if newest:
            rows.reverse()
        return tuple(self._run_activity_from_row(row) for row in rows)

    def list_runs(
        self,
        task_id: str,
        *,
        statuses: Optional[Sequence[RunStatus]] = None,
        limit: Optional[int] = None,
        newest: bool = False,
        offset: int = 0,
    ) -> tuple[RunRecord, ...]:
        params: list[Any] = [task_id]
        where = "r.task_id = ?"
        if statuses:
            values = [RunStatus(status).value for status in statuses]
            where += " AND r.status IN (" + ",".join("?" for _ in values) + ")"
            params.extend(values)
        bounded = max(1, min(int(limit), 10_001)) if limit is not None else None
        skipped = max(0, int(offset))
        with self._read() as connection:
            self._require_task(connection, task_id)
            select = (
                "SELECT r.*, n.node_key FROM orch_runs r "
                "JOIN orch_nodes n ON n.id = r.node_id "
                f"WHERE {where}"
            )
            if bounded is not None and newest:
                rows = connection.execute(
                    "SELECT * FROM ("
                    + select
                    + " ORDER BY r.created_at DESC, r.id DESC LIMIT ? OFFSET ?"
                    + ") ORDER BY created_at, id",
                    [*params, bounded, skipped],
                ).fetchall()
            else:
                rows = connection.execute(
                    select
                    + " ORDER BY r.created_at, r.id"
                    + (" LIMIT ? OFFSET ?" if bounded is not None else ""),
                    [*params, bounded, skipped] if bounded is not None else params,
                ).fetchall()
        return tuple(self._run_from_row(row) for row in rows)

    def list_pending_workspace_commits(
        self, *, limit: int = 1_000
    ) -> tuple[RunRecord, ...]:
        """Return only succeeded runs whose candidate hand-off still needs recovery."""

        bounded = max(1, min(int(limit), 10_000))
        with self._read() as connection:
            rows = connection.execute(
                """
                SELECT r.*, n.node_key FROM orch_runs r
                JOIN orch_nodes n ON n.id = r.node_id
                WHERE r.status = 'succeeded'
                  AND json_extract(
                        r.output_json, '$.workspace_commit.status'
                      ) = 'pending'
                ORDER BY r.finished_at, r.id
                LIMIT ?
                """,
                (bounded,),
            ).fetchall()
        return tuple(self._run_from_row(row) for row in rows)

    @staticmethod
    def _queued_run_dependencies_ready(
        connection: sqlite3.Connection, row: sqlite3.Row
    ) -> bool:
        """Recheck the latest durable predecessors before granting a run lease.

        The coordinator normally enqueues a node only after its dependencies settle.
        A human-approved retry can enqueue several disputed verification attempts in
        one pass, though, so a downstream retry may coexist briefly with its upstream
        retry.  Claiming both in the same scheduler tick races the runtime projection
        and can start the downstream verifier against stale evidence.
        """

        edges = connection.execute(
            """
            SELECT condition, required, from_node_id
            FROM orch_edges
            WHERE plan_id = ? AND to_node_id = ?
            ORDER BY id
            """,
            (row["plan_id"], row["node_id"]),
        ).fetchall()
        if not edges:
            return True
        required = [edge for edge in edges if bool(edge["required"])]
        applicable = required or edges
        matches: list[bool] = []
        for edge in applicable:
            predecessor = connection.execute(
                """
                SELECT status
                FROM orch_runs
                WHERE plan_id = ? AND node_id = ?
                ORDER BY attempt DESC, created_at DESC, id DESC
                LIMIT 1
                """,
                (row["plan_id"], edge["from_node_id"]),
            ).fetchone()
            if predecessor is None:
                matches.append(False)
                continue
            status = RunStatus(predecessor["status"])
            condition = EdgeCondition(edge["condition"])
            if condition in {EdgeCondition.ALWAYS, EdgeCondition.TERMINAL}:
                matches.append(status in _TERMINAL_RUN_STATUSES)
            elif condition is EdgeCondition.SUCCESS:
                matches.append(status is RunStatus.SUCCEEDED)
            else:
                matches.append(status in _FAILED_RUN_STATUSES)
        if JoinPolicy(row["join_policy"]) is JoinPolicy.ALL:
            return all(matches)
        return any(matches)

    def claim_next_run(
        self,
        worker_id: str,
        *,
        lease_seconds: int = 60,
        now: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> Optional[RunClaim]:
        if not worker_id.strip():
            raise ValueError("worker_id is required")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        chosen_now = now or _now()
        command_id = self._command_id(command_id)
        request = {
            "worker_id": worker_id,
            "lease_seconds": lease_seconds,
            "now": now,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.claim", worker_id, request
            )
            if replay is not None:
                claim_payload = replay.get("claim")
                return self._claim_from_payload(claim_payload) if claim_payload else None
            candidates = connection.execute(
                """
                SELECT r.*, n.node_key, n.concurrency_key, t.priority AS task_priority,
                       t.max_parallel_runs, n.join_policy
                FROM orch_runs r
                JOIN orch_nodes n ON n.id = r.node_id
                JOIN orch_tasks t ON t.id = r.task_id
                WHERE r.status = 'queued'
                  AND r.ready_at <= ?
                  AND t.status IN ('queued', 'running')
                  AND (
                      SELECT COUNT(*) FROM orch_runs active
                      WHERE active.task_id = r.task_id
                        AND (
                            active.status IN ('claimed', 'running')
                            OR (
                                active.status = 'succeeded'
                                AND json_extract(
                                    active.output_json, '$.workspace_commit.status'
                                ) = 'pending'
                            )
                        )
                  ) < t.max_parallel_runs
                  AND (
                      n.concurrency_key IS NULL OR NOT EXISTS (
                          SELECT 1 FROM orch_runs occupied
                          JOIN orch_nodes occupied_node ON occupied_node.id = occupied.node_id
                          WHERE (
                                occupied.status IN ('claimed', 'running')
                                OR (
                                    occupied.status = 'succeeded'
                                    AND json_extract(
                                        occupied.output_json, '$.workspace_commit.status'
                                    ) = 'pending'
                                )
                            )
                            AND occupied_node.concurrency_key = n.concurrency_key
                      )
                  )
                -- Windows clocks can return the same microsecond for adjacent enqueue
                -- transactions.  ``id`` is random and therefore is not a FIFO tie-breaker;
                -- SQLite rowid preserves the committed insertion order for those ties.
                ORDER BY r.priority DESC, t.priority DESC, r.ready_at, r.created_at,
                         r.rowid, r.id
                """,
                (_stamp(chosen_now),),
            )
            row = next(
                (
                    candidate
                    for candidate in candidates
                    if self._queued_run_dependencies_ready(connection, candidate)
                ),
                None,
            )
            if row is None:
                self._finish_command(connection, command_id, {"claim": None})
                return None

            fencing_token = int(row["fencing_token"]) + 1
            token = uuid.uuid4().hex
            lease_id = _id("lease")
            expires = chosen_now + timedelta(seconds=lease_seconds)
            changed = connection.execute(
                """
                UPDATE orch_runs
                SET status = 'claimed', fencing_token = ?, version = version + 1
                WHERE id = ? AND status = 'queued' AND version = ?
                """,
                (fencing_token, row["id"], row["version"]),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"run {row['id']} was claimed concurrently")
            connection.execute(
                """
                INSERT INTO orch_leases(
                    id, run_id, owner, token, fencing_token, expires_at,
                    heartbeat_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease_id,
                    row["id"],
                    worker_id,
                    token,
                    fencing_token,
                    _stamp(expires),
                    _stamp(chosen_now),
                    _stamp(chosen_now),
                ),
            )
            task = self._require_task(connection, row["task_id"])
            if task.status is TaskStatus.QUEUED:
                connection.execute(
                    """
                    UPDATE orch_tasks SET status = 'running', version = version + 1,
                        updated_at = ? WHERE id = ? AND status = 'queued'
                    """,
                    (_stamp(chosen_now), task.id),
                )
                self._append_event(
                    connection,
                    task_id=task.id,
                    aggregate_type="task",
                    aggregate_id=task.id,
                    event_type="task.status_changed",
                    payload={"from": "queued", "to": "running"},
                    command_id=command_id,
                )
            self._append_event(
                connection,
                task_id=row["task_id"],
                aggregate_type="run",
                aggregate_id=row["id"],
                event_type="run.claimed",
                payload={
                    "worker_id": worker_id,
                    "fencing_token": fencing_token,
                    "expires_at": _stamp(expires),
                },
                command_id=command_id,
            )
            claim = self._get_claim(connection, row["id"])
            if claim is None:
                raise IntegrityError("claimed run has no lease")
            self._finish_command(
                connection, command_id, {"claim": _jsonable(claim)}
            )
            return claim

    def start_run(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        command_id: Optional[str] = None,
    ) -> RunRecord:
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.start", run_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status is not RunStatus.CLAIMED:
                raise ConflictError(f"run {run_id} is not claimed")
            task = self._require_task(connection, run.task_id)
            if task.status is not TaskStatus.RUNNING:
                raise ConflictError(
                    f"run cannot start while task is {task.status.value}"
                )
            now = _stamp(_now())
            connection.execute(
                """
                UPDATE orch_runs SET status = 'running', started_at = COALESCE(started_at, ?),
                    version = version + 1 WHERE id = ? AND status = 'claimed'
                """,
                (now, run_id),
            )
            self._append_event(
                connection,
                task_id=run.task_id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.started",
                payload={"fencing_token": fencing_token},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run_id})
            return self._require_run(connection, run_id)

    def release_run(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        reason: str = "worker_shutdown",
        command_id: Optional[str] = None,
    ) -> RunRecord:
        """Fenced, recoverable release of the same run attempt back to the queue."""

        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "reason": reason,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.release", run_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            task = self._require_task(connection, run.task_id)
            if task.status not in _RUN_SETTLEMENT_TASK_STATUSES:
                raise ConflictError(
                    f"run cannot be released while task is {task.status.value}"
                )
            now = _stamp(_now())
            changed = connection.execute(
                """
                UPDATE orch_runs
                SET status = 'queued', ready_at = ?, error_kind = NULL,
                    error_message = NULL, finished_at = NULL, version = version + 1
                WHERE id = ? AND fencing_token = ?
                  AND status IN ('claimed', 'running')
                """,
                (now, run_id, fencing_token),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"run {run_id} lost its fencing token")
            self._cancel_preparing_gates(
                connection,
                run_id=run.id,
                task_id=run.task_id,
                reason=reason,
                now=now,
                command_id=command_id,
            )
            connection.execute(
                "DELETE FROM orch_leases WHERE run_id = ? AND token = ?",
                (run_id, lease_token),
            )
            self._append_event(
                connection,
                task_id=run.task_id,
                aggregate_type="run",
                aggregate_id=run.id,
                event_type="run.requeued",
                payload={"reason": reason, "fencing_token": fencing_token},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run.id})
            return self._require_run(connection, run.id)

    def heartbeat(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        lease_seconds: int = 60,
        now: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> LeaseRecord:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        chosen_now = now or _now()
        # Heartbeats are naturally idempotent fenced maintenance writes. Recording
        # each one in the permanent user-command ledger would grow the database by
        # thousands of rows per active run per day without adding audit value.
        with self._write() as connection:
            self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            expires = chosen_now + timedelta(seconds=lease_seconds)
            changed = connection.execute(
                """
                UPDATE orch_leases SET heartbeat_at = ?, expires_at = ?
                WHERE run_id = ? AND token = ? AND fencing_token = ?
                """,
                (
                    _stamp(chosen_now),
                    _stamp(expires),
                    run_id,
                    lease_token,
                    fencing_token,
                ),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"lease for run {run_id} was lost")
            return self._require_lease(connection, run_id, lease_token, fencing_token)

    def assert_run_lease(
        self, run_id: str, lease_token: str, fencing_token: int
    ) -> LeaseRecord:
        """Fail closed unless this exact active attempt owns an unexpired lease."""

        with self._read() as connection:
            run = self._require_run(connection, run_id)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise LeaseConflict(f"run {run_id} is no longer active")
            return self._require_lease(
                connection, run_id, lease_token, fencing_token
            )

    def complete_run(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        output: Optional[Mapping[str, Any]] = None,
        command_id: Optional[str] = None,
    ) -> RunRecord:
        return self._finish_run(
            run_id,
            lease_token,
            fencing_token,
            status=RunStatus.SUCCEEDED,
            output=output,
            error_kind=None,
            error_message=None,
            command_id=command_id,
        )

    def complete_run_structured(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        output: Mapping[str, Any],
        result: Mapping[str, Any],
        created_by: str,
        command_id: Optional[str] = None,
    ) -> RunRecord:
        """Atomically validate and persist a run's TCHP result and work products."""

        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": int(fencing_token),
            "output": dict(output),
            "result": dict(result),
            "created_by": str(created_by),
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.complete_structured", run_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            task = self._require_task(connection, run.task_id)
            if task.status not in _RUN_SETTLEMENT_TASK_STATUSES:
                raise ConflictError(
                    f"run cannot succeed while task is {task.status.value}"
                )
            brief = self._require_brief(
                connection, run.brief_id or task.active_brief_id  # type: ignore[arg-type]
            )
            summary = str(result.get("summary") or "").strip()
            issues: list[dict[str, Any]] = []
            if not summary:
                issues.append(
                    {
                        "path": "summary",
                        "code": "required",
                        "message": "completion summary is required",
                    }
                )
            allowed_results = {"pass", "fail", "unknown", "not_applicable"}
            criterion_results = {
                str(key): str(value).strip().lower()
                for key, value in dict(result.get("criterion_results") or {}).items()
            }
            criteria = {
                str(item.get("id") or ""): item
                for item in brief.acceptance_criteria
                if str(item.get("id") or "")
            }
            for criterion_id in sorted(set(criterion_results) - set(criteria)):
                issues.append(
                    {
                        "path": f"criterion_results.{criterion_id}",
                        "code": "unknown",
                        "message": "criterion is not declared in the published Brief",
                    }
                )
            for criterion_id, criterion in criteria.items():
                value = criterion_results.get(criterion_id)
                if value is not None and value not in allowed_results:
                    issues.append(
                        {
                            "path": f"criterion_results.{criterion_id}",
                            "code": "invalid",
                            "message": "result must be pass, fail, unknown, or not_applicable",
                        }
                    )
                if bool(criterion.get("required", True)) and value != "pass":
                    issues.append(
                        {
                            "path": f"criterion_results.{criterion_id}",
                            "code": "required_pass",
                            "message": "required criterion must be reported as pass",
                        }
                    )
            if issues:
                raise HandoffValidationError(issues)

            product_ids: list[str] = []
            artifact_refs: list[str] = []
            satisfied_deliverables: set[str] = set()
            deliverables = {
                str(item.get("id") or ""): item
                for item in brief.deliverables
                if str(item.get("id") or "")
            }
            for index, raw_product in enumerate(result.get("work_products") or ()):
                if not isinstance(raw_product, Mapping):
                    raise HandoffValidationError(
                        (
                            {
                                "path": f"work_products[{index}]",
                                "code": "invalid",
                                "message": "work product must be an object",
                            },
                        )
                    )
                existing_id = str(
                    raw_product.get("id") or raw_product.get("work_product_id") or ""
                ).strip()
                if existing_id:
                    product = self._require_work_product(connection, existing_id)
                    if product.task_id != task.id:
                        raise ConflictError(
                            "completion work product belongs to another task"
                        )
                else:
                    metadata = dict(raw_product.get("metadata") or {})
                    deliverable_id = str(
                        raw_product.get("deliverable_id")
                        or metadata.get("deliverable_id")
                        or ""
                    ).strip()
                    if deliverable_id:
                        metadata["deliverable_id"] = deliverable_id
                    product = self._insert_work_product(
                        connection,
                        task_id=task.id,
                        run_id=run.id,
                        kind=WorkProductKind(str(raw_product.get("kind") or "other")),
                        title=str(raw_product.get("title") or ""),
                        summary=str(raw_product.get("summary") or ""),
                        evidence_id=(
                            str(raw_product["evidence_id"])
                            if raw_product.get("evidence_id")
                            else None
                        ),
                        artifact_id=(
                            str(raw_product["artifact_id"])
                            if raw_product.get("artifact_id")
                            else None
                        ),
                        uri=(str(raw_product["uri"]) if raw_product.get("uri") else None),
                        content_hash=(
                            str(raw_product["content_hash"])
                            if raw_product.get("content_hash")
                            else None
                        ),
                        metadata=metadata,
                        verification_status=str(
                            raw_product.get("verification_status") or "unverified"
                        ),
                        created_by=str(created_by),
                        command_id=command_id,
                    )
                product_ids.append(product.id)
                product_deliverable = str(
                    product.metadata.get("deliverable_id") or ""
                ).strip()
                if product_deliverable:
                    satisfied_deliverables.add(product_deliverable)
                else:
                    matching = [
                        identifier
                        for identifier, deliverable in deliverables.items()
                        if str(deliverable.get("kind") or "other")
                        == product.kind.value
                    ]
                    if len(matching) == 1:
                        satisfied_deliverables.add(matching[0])
                if product.artifact_id:
                    artifact_refs.append(product.artifact_id)
                elif product.uri:
                    artifact_refs.append(product.uri)
            missing_deliverables = [
                identifier
                for identifier, deliverable in deliverables.items()
                if bool(deliverable.get("required", True))
                and identifier not in satisfied_deliverables
            ]
            if missing_deliverables:
                raise HandoffValidationError(
                    tuple(
                        {
                            "path": f"deliverables.{identifier}",
                            "code": "missing_work_product",
                            "message": "required deliverable has no linked work product",
                        }
                        for identifier in missing_deliverables
                    )
                )

            now = _now()
            result_envelope = {
                "schema_version": 2,
                "child_task_id": task.id,
                "brief_revision": brief.revision,
                "status": "completed",
                "summary": summary[:16_000],
                "criterion_results": criterion_results,
                "work_product_refs": product_ids,
                "artifact_refs": artifact_refs,
                "remaining_risks": [
                    str(item)[:2_000]
                    for item in result.get("remaining_risks") or ()
                    if str(item).strip()
                ],
                "follow_up_task_ids": [
                    str(item)
                    for item in result.get("follow_up_task_ids") or ()
                    if str(item).strip()
                ],
                "completed_at": _stamp(now),
            }
            final_output = {
                **dict(output),
                "structured_result": dict(result),
                "result": result_envelope,
            }
            changed = connection.execute(
                """
                UPDATE orch_runs
                SET status = 'succeeded', output_json = ?, error_kind = NULL,
                    error_message = NULL, finished_at = ?, version = version + 1
                WHERE id = ? AND fencing_token = ? AND status IN ('claimed', 'running')
                """,
                (_json(final_output), _stamp(now), run_id, int(fencing_token)),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"run {run_id} lost its fencing token")
            self._cancel_preparing_gates(
                connection,
                run_id=run.id,
                task_id=run.task_id,
                reason="run_succeeded",
                now=_stamp(now),
                command_id=command_id,
            )
            connection.execute(
                "DELETE FROM orch_leases WHERE run_id = ? AND token = ?",
                (run_id, lease_token),
            )
            sequence = int(
                connection.execute(
                    "SELECT COALESCE(MAX(sequence_no), 0) + 1 FROM orch_task_comments WHERE task_id = ?",
                    (task.id,),
                ).fetchone()[0]
            )
            comment_id = _id("comment")
            connection.execute(
                """
                INSERT INTO orch_task_comments(
                    id, task_id, sequence_no, author_type, author_id,
                    created_by_run_id, body_markdown, metadata_json,
                    reply_to_comment_id, created_at
                ) VALUES (?, ?, ?, 'agent', ?, ?, ?, ?, NULL, ?)
                """,
                (
                    comment_id,
                    task.id,
                    sequence,
                    str(created_by),
                    run.id,
                    f"Completed: {summary}"[:16_000],
                    _json(
                        {
                            "kind": "completion",
                            "criterion_results": criterion_results,
                            "work_product_refs": product_ids,
                        }
                    ),
                    _stamp(now),
                ),
            )
            self._append_event(
                connection,
                task_id=task.id,
                aggregate_type="task_comment",
                aggregate_id=comment_id,
                event_type="comment_added",
                payload={
                    "sequence": sequence,
                    "author_type": "agent",
                    "author_id": str(created_by),
                    "completion": True,
                },
                command_id=command_id,
            )
            self._append_event(
                connection,
                task_id=task.id,
                aggregate_type="run",
                aggregate_id=run.id,
                event_type="structured_result_submitted",
                payload={
                    "brief_revision": brief.revision,
                    "work_product_refs": product_ids,
                    "criterion_results": criterion_results,
                },
                command_id=command_id,
            )
            self._append_event(
                connection,
                task_id=task.id,
                aggregate_type="run",
                aggregate_id=run.id,
                event_type="run.succeeded",
                payload={"fencing_token": int(fencing_token), "error_kind": None},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run.id})
            return self._require_run(connection, run.id)

    def fail_run(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        error_kind: str,
        error_message: str,
        status: RunStatus = RunStatus.FAILED,
        output: Optional[Mapping[str, Any]] = None,
        command_id: Optional[str] = None,
    ) -> RunRecord:
        status = RunStatus(status)
        if status not in {RunStatus.FAILED, RunStatus.TIMED_OUT, RunStatus.CANCELED}:
            raise ValueError("failure status must be failed, timed_out, or canceled")
        return self._finish_run(
            run_id,
            lease_token,
            fencing_token,
            status=status,
            output=output,
            error_kind=error_kind,
            error_message=error_message,
            command_id=command_id,
        )

    def merge_run_output(
        self,
        run_id: str,
        patch: Mapping[str, Any],
        *,
        allowed_statuses: Sequence[RunStatus] = (
            RunStatus.WAITING_GATE,
            RunStatus.SUCCEEDED,
        ),
        command_id: Optional[str] = None,
    ) -> RunRecord:
        """Durably merge post-run protocol metadata into a non-executing run.

        This is used for two recoverable hand-offs whose durable truth necessarily
        follows the fenced execution transaction: hidden-session checkpoints and
        candidate-workspace commit receipts.  It cannot mutate an actively executing
        attempt and every update is command-idempotent and evented.
        """

        statuses = tuple(RunStatus(item) for item in allowed_statuses)
        if not statuses or any(
            item in {RunStatus.CLAIMED, RunStatus.RUNNING, RunStatus.QUEUED}
            for item in statuses
        ):
            raise ValueError("run output merge is restricted to non-executing states")
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "patch": patch,
            "allowed_statuses": [item.value for item in statuses],
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.merge_output", run_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            run = self._require_run(connection, run_id)
            if run.status not in statuses:
                raise ConflictError(
                    f"run {run_id} is {run.status.value}; output merge is not allowed"
                )
            merged = {**dict(run.output or {}), **dict(patch)}
            changed = connection.execute(
                """
                UPDATE orch_runs SET output_json = ?, version = version + 1
                WHERE id = ? AND version = ? AND status = ?
                """,
                (_json(merged), run_id, run.version, run.status.value),
            ).rowcount
            if changed != 1:
                raise ConflictError(f"run {run_id} changed concurrently")
            self._append_event(
                connection,
                task_id=run.task_id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.output_merged",
                payload={"keys": sorted(str(key) for key in patch)},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run_id})
            return self._require_run(connection, run_id)

    def checkpoint_active_run(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        checkpoint: Mapping[str, Any],
        command_id: Optional[str] = None,
    ) -> RunRecord:
        """Persist resumable runtime state while an attempt still owns its lease.

        External agent runtimes learn their vendor session/thread identifier only
        after launch.  Saving it after the process exits would leave a shutdown
        window in which the same durable attempt could not be resumed.  This method
        closes that window without weakening fencing: only the exact live lease may
        update the checkpoint, and every distinct checkpoint is command-idempotent
        and hash-chained in the orchestration event log.

        ``checkpoint`` is protocol metadata, never credentials.  Callers are
        responsible for keeping tokens, environment variables, and prompts out of
        it.
        """

        if not checkpoint:
            raise ValueError("active run checkpoint must not be empty")
        prohibited_markers = (
            "access_token",
            "api_key",
            "auth_token",
            "authorization",
            "bearer",
            "cookie",
            "credential",
            "lease_token",
            "password",
            "secret",
        )
        unsafe_keys = [
            str(key)
            for key in checkpoint
            if any(
                marker in str(key).strip().lower().replace("-", "_")
                for marker in prohibited_markers
            )
        ]
        if unsafe_keys:
            raise ValueError(
                "active run checkpoint contains credential-like fields: "
                + ", ".join(sorted(unsafe_keys))
            )
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "checkpoint": checkpoint,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.checkpoint_active", run_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            merged = {
                **dict(run.output or {}),
                "subscription_runtime_checkpoint": dict(checkpoint),
            }
            changed = connection.execute(
                """
                UPDATE orch_runs SET output_json = ?, version = version + 1
                WHERE id = ? AND fencing_token = ?
                  AND status IN ('claimed', 'running')
                """,
                (_json(merged), run_id, fencing_token),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"run {run_id} lost its fencing token")
            # Deliberately record only a safe field inventory and stable runtime
            # identifiers.  A future adapter cannot accidentally leak a token into
            # the append-only audit chain by adding a checkpoint field.
            self._append_event(
                connection,
                task_id=run.task_id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.runtime_checkpointed",
                payload={
                    "fencing_token": fencing_token,
                    "runtime_id": str(checkpoint.get("runtime_id") or ""),
                    "external_session_id": str(
                        checkpoint.get("external_session_id") or ""
                    ),
                    "keys": sorted(str(key) for key in checkpoint),
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run_id})
            return self._require_run(connection, run_id)

    def cancel_task_runs(
        self,
        task_id: str,
        *,
        command_id: Optional[str] = None,
    ) -> int:
        """Cancel every non-executing run that cannot observe task cancellation.

        Claimed/running attempts retain their fenced lease and are interrupted by their
        owning worker.  Queued attempts and gate-waiting attempts have no executing
        coroutine, so leaving them active would keep a task in ``canceling`` forever.
        """

        command_id = self._command_id(command_id)
        request = {"task_id": task_id}
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.cancel_task_pending", task_id, request
            )
            if replay is not None:
                return int(replay["count"])
            self._require_task(connection, task_id)
            rows = connection.execute(
                """
                SELECT id, status FROM orch_runs
                WHERE task_id = ? AND status IN ('queued', 'waiting_gate')
                ORDER BY created_at, id
                """,
                (task_id,),
            ).fetchall()
            open_gates = connection.execute(
                """
                SELECT id, run_id, kind, source_key FROM orch_gates
                WHERE task_id = ? AND status = 'open'
                -- Windows clocks can give two sequential inserts the same timestamp.
                -- rowid preserves the durable insertion order for emitted audit events.
                ORDER BY rowid
                """,
                (task_id,),
            ).fetchall()
            now = _stamp(_now())
            run_ids = [str(row["id"]) for row in rows]
            if run_ids:
                placeholders = ",".join("?" for _ in run_ids)
                connection.execute(
                    f"""
                    UPDATE orch_runs
                    SET status = 'canceled', error_kind = 'task_canceled',
                        error_message = 'owning task was canceled', finished_at = ?,
                        version = version + 1
                    WHERE id IN ({placeholders})
                      AND status IN ('queued', 'waiting_gate')
                    """,
                    (now, *run_ids),
                )
                connection.execute(
                    f"DELETE FROM orch_leases WHERE run_id IN ({placeholders})",
                    run_ids,
                )
                for row in rows:
                    self._append_event(
                        connection,
                        task_id=task_id,
                        aggregate_type="run",
                        aggregate_id=str(row["id"]),
                        event_type="run.canceled",
                        payload={"reason": "task_canceled", "from": str(row["status"])},
                        command_id=command_id,
                    )
            if open_gates:
                connection.execute(
                    """
                    UPDATE orch_gates
                    SET status = 'canceled', resolution_json = ?,
                        resolved_by = 'orchestration-cancel', resolved_at = ?,
                        version = version + 1
                    WHERE task_id = ? AND status = 'open'
                    """,
                    (
                        _json({"decision": "cancel", "reason": "task_canceled"}),
                        now,
                        task_id,
                    ),
                )
                for gate in open_gates:
                    self._append_event(
                        connection,
                        task_id=task_id,
                        aggregate_type="gate",
                        aggregate_id=str(gate["id"]),
                        event_type="gate.canceled",
                        payload={
                            "reason": "task_canceled",
                            "run_id": gate["run_id"],
                            "kind": str(gate["kind"]),
                            "source_key": str(gate["source_key"]),
                        },
                        command_id=command_id,
                    )
            self._finish_command(connection, command_id, {"count": len(run_ids)})
            return len(run_ids)

    def _cancel_preparing_gates(
        self,
        connection: sqlite3.Connection,
        *,
        run_id: str,
        task_id: str,
        reason: str,
        now: str,
        command_id: Optional[str],
    ) -> int:
        """Cancel gates that never crossed the suspension commit point."""

        rows = connection.execute(
            """
            SELECT id, kind, source_key FROM orch_gates
            WHERE run_id = ? AND status = 'preparing'
            ORDER BY rowid
            """,
            (run_id,),
        ).fetchall()
        if not rows:
            return 0
        connection.execute(
            """
            UPDATE orch_gates
            SET status = 'canceled', resolution_json = ?,
                resolved_by = 'orchestration-runtime', resolved_at = ?,
                version = version + 1
            WHERE run_id = ? AND status = 'preparing'
            """,
            (
                _json(
                    {
                        "decision": "cancel",
                        "reason": reason,
                        "publication_state": "unpublished",
                    }
                ),
                now,
                run_id,
            ),
        )
        for row in rows:
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="gate",
                aggregate_id=str(row["id"]),
                event_type="gate.preparation_aborted",
                payload={
                    "reason": reason,
                    "publication_state": "unpublished",
                    "run_id": run_id,
                    "kind": str(row["kind"]),
                    "source_key": str(row["source_key"]),
                },
                command_id=command_id,
            )
        return len(rows)

    @staticmethod
    def _open_gate_wait_status(
        connection: sqlite3.Connection, task_id: str
    ) -> Optional[TaskStatus]:
        """Project all published open gates into one aggregate task wait state."""

        rows = connection.execute(
            """
            SELECT kind FROM orch_gates
            WHERE task_id = ? AND status = 'open' AND published_at IS NOT NULL
            """,
            (task_id,),
        ).fetchall()
        if not rows:
            return None
        if any(str(row["kind"]) != GateKind.CHILD_WAIT.value for row in rows):
            return TaskStatus.WAITING_HUMAN
        return TaskStatus.WAITING_CHILD

    def _finish_run(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        status: RunStatus,
        output: Optional[Mapping[str, Any]],
        error_kind: Optional[str],
        error_message: Optional[str],
        command_id: Optional[str],
    ) -> RunRecord:
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "status": status.value,
            "output": output,
            "error_kind": error_kind,
            "error_message": error_message,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "run.finish", run_id, request
            )
            if replay is not None:
                return self._require_run(connection, replay["run_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            task = self._require_task(connection, run.task_id)
            if (
                status is RunStatus.SUCCEEDED
                and task.status not in _RUN_SETTLEMENT_TASK_STATUSES
            ):
                raise ConflictError(
                    f"run cannot succeed while task is {task.status.value}"
                )
            now = _stamp(_now())
            changed = connection.execute(
                """
                UPDATE orch_runs
                SET status = ?, output_json = ?, error_kind = ?, error_message = ?,
                    finished_at = ?, version = version + 1
                WHERE id = ? AND fencing_token = ? AND status IN ('claimed', 'running')
                """,
                (
                    status.value,
                    _json(output) if output is not None else None,
                    error_kind,
                    error_message,
                    now,
                    run_id,
                    fencing_token,
                ),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"run {run_id} lost its fencing token")
            self._cancel_preparing_gates(
                connection,
                run_id=run.id,
                task_id=run.task_id,
                reason=error_kind or f"run_{status.value}",
                now=now,
                command_id=command_id,
            )
            connection.execute(
                "DELETE FROM orch_leases WHERE run_id = ? AND token = ?",
                (run_id, lease_token),
            )
            self._append_event(
                connection,
                task_id=run.task_id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type=f"run.{status.value}",
                payload={
                    "fencing_token": fencing_token,
                    "error_kind": error_kind,
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"run_id": run_id})
            return self._require_run(connection, run_id)

    def reap_expired_leases(
        self,
        *,
        now: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> int:
        chosen_now = now or _now()
        # Reaping is state-idempotent: once the fenced run leaves an active state,
        # another pass changes nothing. Keep only actual ``run.lost`` audit events;
        # empty polling ticks must not populate the permanent command ledger.
        with self._write() as connection:
            expired = connection.execute(
                """
                SELECT l.*, r.task_id, r.status AS run_status
                FROM orch_leases l JOIN orch_runs r ON r.id = l.run_id
                WHERE l.expires_at <= ? ORDER BY l.expires_at, l.id
                """,
                (_stamp(chosen_now),),
            ).fetchall()
            count = 0
            for lease in expired:
                changed = connection.execute(
                    """
                    UPDATE orch_runs
                    SET status = 'lost', error_kind = 'lease_expired',
                        error_message = 'worker lease expired', finished_at = ?,
                        version = version + 1
                    WHERE id = ? AND fencing_token = ?
                      AND status IN ('claimed', 'running')
                    """,
                    (_stamp(chosen_now), lease["run_id"], lease["fencing_token"]),
                ).rowcount
                connection.execute("DELETE FROM orch_leases WHERE id = ?", (lease["id"],))
                if changed:
                    count += 1
                    self._cancel_preparing_gates(
                        connection,
                        run_id=str(lease["run_id"]),
                        task_id=str(lease["task_id"]),
                        reason="lease_expired",
                        now=_stamp(chosen_now),
                        command_id=command_id,
                    )
                    self._append_event(
                        connection,
                        task_id=lease["task_id"],
                        aggregate_type="run",
                        aggregate_id=lease["run_id"],
                        event_type="run.lost",
                        payload={
                            "reason": "lease_expired",
                            "fencing_token": lease["fencing_token"],
                        },
                        command_id=command_id,
                    )
            return count

    # -- durable human gates ----------------------------------------------
    def open_task_gate(
        self,
        task_id: str,
        *,
        kind: GateKind,
        source_key: str,
        prompt: Mapping[str, Any],
        expires_at: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> GateRecord:
        """Open a durable lifecycle gate that is not owned by an Agent run."""

        if not source_key.strip():
            raise ValueError("source_key is required")
        kind = GateKind(kind)
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "kind": kind.value,
            "source_key": source_key,
            "prompt": prompt,
            "expires_at": expires_at,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "task_gate.open", task_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["gate_id"])
            task = self._require_task(connection, task_id)
            existing = connection.execute(
                "SELECT * FROM orch_gates WHERE source_key = ?", (source_key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["task_id"] != task_id
                    or existing["run_id"] is not None
                    or existing["kind"] != kind.value
                    or existing["prompt_json"] != _json(prompt)
                ):
                    raise IdempotencyConflict(
                        f"gate source key {source_key} was reused with different input"
                    )
                self._finish_command(connection, command_id, {"gate_id": existing["id"]})
                return self._gate_from_row(existing)
            if task.status is not TaskStatus.RUNNING:
                raise ConflictError(
                    f"lifecycle gates require a running task, found {task.status.value}"
                )
            gate_id = _id("gate")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_gates(
                    id, task_id, run_id, node_id, kind, status, source_key,
                    prompt_json, version, opened_at, published_at, expires_at
                ) VALUES (?, ?, NULL, NULL, ?, 'open', ?, ?, 1, ?, ?, ?)
                """,
                (
                    gate_id,
                    task_id,
                    kind.value,
                    source_key,
                    _json(prompt),
                    now,
                    now,
                    _stamp(expires_at) if expires_at else None,
                ),
            )
            changed = connection.execute(
                """
                UPDATE orch_tasks SET status = 'waiting_human', version = version + 1,
                    updated_at = ? WHERE id = ? AND version = ? AND status = 'running'
                """,
                (now, task_id, task.version),
            ).rowcount
            if changed != 1:
                raise VersionConflict(f"task {task_id} changed while opening a gate")
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="gate",
                aggregate_id=gate_id,
                event_type="gate.opened",
                payload={"kind": kind.value, "run_id": None, "source_key": source_key},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"gate_id": gate_id})
            return self._require_gate(connection, gate_id)

    def amend_task_gate_prompt(
        self,
        gate_id: str,
        prompt: Mapping[str, Any],
        *,
        expected_version: Optional[int] = None,
        command_id: Optional[str] = None,
    ) -> GateRecord:
        """Replace the prompt of an open lifecycle gate with an audited CAS write.

        This is intentionally limited to task-owned gates. Run-owned interaction
        prompts are part of a sealed Agent checkpoint and must never be rewritten.
        The operation supports deterministic compatibility repairs when a newer
        coordinator can safely offer an action that an older version omitted.
        """

        command_id = self._command_id(command_id)
        request = {
            "gate_id": gate_id,
            "prompt": dict(prompt),
            "expected_version": expected_version,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "task_gate.prompt_amend", gate_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["gate_id"])
            gate = self._require_gate(connection, gate_id)
            effective_version = (
                gate.version if expected_version is None else int(expected_version)
            )
            if gate.run_id is not None:
                raise ConflictError(
                    "run-owned gate prompts are sealed and cannot be amended"
                )
            if gate.status is not GateStatus.OPEN:
                raise GateConflict(f"gate {gate_id} is already {gate.status.value}")
            if gate.version != effective_version:
                raise GateConflict(
                    f"gate {gate_id} expected version {effective_version}, "
                    f"found {gate.version}"
                )
            encoded = _json(prompt)
            if encoded != _json(gate.prompt):
                changed = connection.execute(
                    """
                    UPDATE orch_gates
                    SET prompt_json = ?, version = version + 1
                    WHERE id = ? AND run_id IS NULL AND status = 'open'
                      AND version = ?
                    """,
                    (encoded, gate.id, effective_version),
                ).rowcount
                if changed != 1:
                    raise GateConflict(
                        f"gate {gate_id} changed while amending its prompt"
                    )
                self._append_event(
                    connection,
                    task_id=gate.task_id,
                    aggregate_type="gate",
                    aggregate_id=gate.id,
                    event_type="gate.prompt_amended",
                    payload={
                        "kind": gate.kind.value,
                        "source_key": gate.source_key,
                        "actions": list(prompt.get("actions") or ()),
                    },
                    command_id=command_id,
                )
            self._finish_command(connection, command_id, {"gate_id": gate.id})
            return self._require_gate(connection, gate.id)

    def prepare_run_gate(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        kind: GateKind,
        source_key: str,
        prompt: Mapping[str, Any],
        expires_at: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> GateRecord:
        """Create an unresolvable run gate without releasing execution authority.

        The executor must finish checkpointing and process-tree cleanup before
        ``commit_prepared_gate`` atomically exposes the gate and releases the run
        lease. This removes the approval-vs-cleanup race from the suspension path.
        """

        if not source_key.strip():
            raise ValueError("source_key is required")
        kind = GateKind(kind)
        if kind is GateKind.CHILD_WAIT:
            raise ValueError("use prepare_child_wait for child gates")
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "kind": kind.value,
            "source_key": source_key,
            "prompt": prompt,
            "expires_at": expires_at,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "gate.prepare", run_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["gate_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            task = self._require_task(connection, run.task_id)
            if task.status not in _RUN_SETTLEMENT_TASK_STATUSES:
                raise ConflictError(
                    f"run gate cannot prepare while task is {task.status.value}"
                )
            existing = connection.execute(
                "SELECT * FROM orch_gates WHERE source_key = ?", (source_key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["run_id"] != run_id
                    or existing["kind"] != kind.value
                    or existing["prompt_json"] != _json(prompt)
                ):
                    raise IdempotencyConflict(
                        f"gate source key {source_key} was reused with different input"
                    )
                if (
                    existing["status"] == GateStatus.CANCELED.value
                    and existing["published_at"] is None
                ):
                    # A shutdown/release may abort a preparation while retaining the
                    # same durable run attempt. Reuse its identity, but never replay
                    # the runtime-generated cancellation as a user's answer.
                    connection.execute(
                        """
                        UPDATE orch_gates
                        SET status = 'preparing', resolution_json = NULL,
                            resolved_by = NULL, resolved_at = NULL,
                            expires_at = ?, version = version + 1
                        WHERE id = ? AND status = 'canceled'
                          AND published_at IS NULL
                        """,
                        (
                            _stamp(expires_at) if expires_at else None,
                            existing["id"],
                        ),
                    )
                self._finish_command(
                    connection, command_id, {"gate_id": existing["id"]}
                )
                return self._require_gate(connection, str(existing["id"]))
            gate_id = _id("gate")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_gates(
                    id, task_id, run_id, node_id, kind, status, source_key,
                    prompt_json, version, opened_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, 'preparing', ?, ?, 1, ?, ?)
                """,
                (
                    gate_id,
                    run.task_id,
                    run.id,
                    run.node_id,
                    kind.value,
                    source_key,
                    _json(prompt),
                    now,
                    _stamp(expires_at) if expires_at else None,
                ),
            )
            self._finish_command(connection, command_id, {"gate_id": gate_id})
            return self._require_gate(connection, gate_id)

    def prepare_child_wait(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        child_task_id: str,
        source_key: str,
        command_id: Optional[str] = None,
    ) -> GateRecord:
        """Prepare a child-wait gate while retaining the parent run lease."""

        if not source_key.strip():
            raise ValueError("source_key is required")
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "child_task_id": child_task_id,
            "source_key": source_key,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "gate.prepare_child_wait", run_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["gate_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            task = self._require_task(connection, run.task_id)
            if task.status not in _RUN_SETTLEMENT_TASK_STATUSES:
                raise ConflictError(
                    f"child wait cannot prepare while task is {task.status.value}"
                )
            child = self._require_task(connection, child_task_id)
            runtime_meta = dict(child.input.get("_runtime") or {})
            exact_attempt = str(runtime_meta.get("parent_run_id") or "") == run.id
            logical_owner = (
                bool(runtime_meta.get("spawn_key"))
                and str(runtime_meta.get("parent_plan_id") or "") == run.plan_id
                and child.parent_node_id == run.node_id
            )
            if child.parent_task_id != run.task_id or not (exact_attempt or logical_owner):
                raise ConflictError(
                    f"task {child_task_id} is not owned by parent run {run_id}"
                )
            prompt = {
                "type": "child_wait",
                "title": f"Waiting for child task {child_task_id}",
                "description": (
                    "The parent Agent is durably suspended until the child is terminal."
                ),
                "child_task_id": child_task_id,
                "actions": [],
            }
            existing = connection.execute(
                "SELECT * FROM orch_gates WHERE source_key = ?", (source_key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["run_id"] != run.id
                    or existing["kind"] != GateKind.CHILD_WAIT.value
                    or existing["prompt_json"] != _json(prompt)
                ):
                    raise IdempotencyConflict(
                        f"gate source key {source_key} was reused with different input"
                    )
                if (
                    existing["status"] == GateStatus.CANCELED.value
                    and existing["published_at"] is None
                ):
                    connection.execute(
                        """
                        UPDATE orch_gates
                        SET status = 'preparing', resolution_json = NULL,
                            resolved_by = NULL, resolved_at = NULL,
                            version = version + 1
                        WHERE id = ? AND status = 'canceled'
                          AND published_at IS NULL
                        """,
                        (existing["id"],),
                    )
                self._finish_command(
                    connection, command_id, {"gate_id": existing["id"]}
                )
                return self._require_gate(connection, str(existing["id"]))
            gate_id = _id("gate")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_gates(
                    id, task_id, run_id, node_id, kind, status, source_key,
                    prompt_json, version, opened_at
                ) VALUES (?, ?, ?, ?, 'child_wait', 'preparing', ?, ?, 1, ?)
                """,
                (
                    gate_id,
                    run.task_id,
                    run.id,
                    run.node_id,
                    source_key,
                    _json(prompt),
                    now,
                ),
            )
            self._finish_command(connection, command_id, {"gate_id": gate_id})
            return self._require_gate(connection, gate_id)

    def commit_prepared_gate(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        gate_id: str,
        checkpoint: Mapping[str, Any],
        command_id: Optional[str] = None,
    ) -> GateRecord:
        """Atomically publish a prepared gate, checkpoint, and lease release."""

        if checkpoint.get("schema_version") != 1:
            raise ValueError("prepared gate checkpoint schema_version must be 1")
        if str(checkpoint.get("gate_id") or "") != gate_id:
            raise ValueError("checkpoint gate_id does not match the prepared gate")
        if checkpoint.get("recovery_disposition") != "pending_tools":
            raise ValueError("prepared gate checkpoint is not resumable")
        digest = str(checkpoint.get("blob_sha256") or "")
        blob_uri = str(checkpoint.get("blob_uri") or "")
        if (
            len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or blob_uri != f"sha256:{digest}"
        ):
            raise ValueError("prepared gate checkpoint blob identity is required")
        raw_pending_ids = checkpoint.get("pending_tool_call_ids")
        if (
            not isinstance(raw_pending_ids, (list, tuple))
            or not raw_pending_ids
            or any(not isinstance(item, str) or not item for item in raw_pending_ids)
            or len(set(raw_pending_ids)) != len(raw_pending_ids)
        ):
            raise ValueError("prepared gate checkpoint has no pending tool calls")
        try:
            checkpoint_attempt = int(checkpoint.get("attempt", -1))
            checkpoint_fence = int(checkpoint.get("fencing_token", -1))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "prepared gate checkpoint attempt/fencing token is invalid"
            ) from exc
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "gate_id": gate_id,
            "checkpoint": checkpoint,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "gate.commit_prepared", run_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["gate_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            if (
                str(checkpoint.get("run_id") or "") != run.id
                or checkpoint_attempt != run.attempt
                or checkpoint_fence != fencing_token
                or (
                    run.session_id is not None
                    and str(checkpoint.get("session_id") or "") != run.session_id
                )
            ):
                raise ValueError(
                    "prepared gate checkpoint does not match the active run attempt"
                )
            gate = self._require_gate(connection, gate_id)
            if (
                gate.run_id != run.id
                or gate.task_id != run.task_id
                or gate.status is not GateStatus.PREPARING
            ):
                raise ConflictError(
                    f"gate {gate_id} is not prepared for active run {run_id}"
                )
            task = self._require_task(connection, run.task_id)
            if task.status not in _RUN_SETTLEMENT_TASK_STATUSES:
                raise ConflictError(
                    f"prepared gate cannot commit while task is {task.status.value}"
                )
            now = _stamp(_now())
            merged_output = {
                **dict(run.output or {}),
                "engine_checkpoint": dict(checkpoint),
            }
            changed = connection.execute(
                """
                UPDATE orch_runs
                SET status = 'waiting_gate', output_json = ?, version = version + 1
                WHERE id = ? AND fencing_token = ?
                  AND status IN ('claimed', 'running')
                """,
                (_json(merged_output), run.id, fencing_token),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"run {run.id} lost its fencing token")
            opened = connection.execute(
                """
                UPDATE orch_gates
                SET status = 'open', published_at = ?, version = version + 1
                WHERE id = ? AND run_id = ? AND status = 'preparing'
                """,
                (now, gate.id, run.id),
            ).rowcount
            if opened != 1:
                raise ConflictError(f"prepared gate {gate.id} changed concurrently")
            connection.execute(
                "DELETE FROM orch_leases WHERE run_id = ? AND token = ?",
                (run.id, lease_token),
            )
            if task.status is not TaskStatus.PAUSED:
                target = self._open_gate_wait_status(connection, task.id)
                if target is None:
                    raise IntegrityError(
                        f"committed gate {gate.id} is missing from task wait projection"
                    )
                if task.status is not target:
                    validate_task_transition(task.status, target)
                    task_changed = connection.execute(
                        """
                        UPDATE orch_tasks
                        SET status = ?, version = version + 1, updated_at = ?
                        WHERE id = ? AND version = ? AND status = ?
                        """,
                        (
                            target.value,
                            now,
                            task.id,
                            task.version,
                            task.status.value,
                        ),
                    ).rowcount
                    if task_changed != 1:
                        raise VersionConflict(
                            f"task {task.id} changed while committing gate"
                        )
            self._append_event(
                connection,
                task_id=task.id,
                aggregate_type="gate",
                aggregate_id=gate.id,
                event_type="gate.prepared",
                payload={
                    "kind": gate.kind.value,
                    "run_id": run.id,
                    "source_key": gate.source_key,
                },
                command_id=command_id,
            )
            self._append_event(
                connection,
                task_id=task.id,
                aggregate_type="gate",
                aggregate_id=gate.id,
                event_type="gate.opened",
                payload={
                    "kind": gate.kind.value,
                    "run_id": run.id,
                    "source_key": gate.source_key,
                    "checkpoint_sha256": str(checkpoint["blob_sha256"]),
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"gate_id": gate.id})
            return self._require_gate(connection, gate.id)

    def open_gate(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        kind: GateKind,
        source_key: str,
        prompt: Mapping[str, Any],
        expires_at: Optional[datetime] = None,
        command_id: Optional[str] = None,
    ) -> GateRecord:
        raise ConflictError(
            "single-phase run gates are disabled; use prepare_run_gate and "
            "commit_prepared_gate"
        )
        if not source_key.strip():
            raise ValueError("source_key is required")
        kind = GateKind(kind)
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "kind": kind.value,
            "source_key": source_key,
            "prompt": prompt,
            "expires_at": expires_at,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "gate.open", run_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["gate_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            task = self._require_task(connection, run.task_id)
            if task.status is not TaskStatus.RUNNING:
                raise ConflictError(
                    f"run gate cannot open while task is {task.status.value}"
                )
            existing = connection.execute(
                "SELECT * FROM orch_gates WHERE source_key = ?", (source_key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["run_id"] != run_id
                    or existing["kind"] != kind.value
                    or existing["prompt_json"] != _json(prompt)
                ):
                    raise IdempotencyConflict(
                        f"gate source key {source_key} was reused with different input"
                    )
                self._finish_command(
                    connection, command_id, {"gate_id": existing["id"]}
                )
                return self._gate_from_row(existing)
            gate_id = _id("gate")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_gates(
                    id, task_id, run_id, node_id, kind, status, source_key,
                    prompt_json, version, opened_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, 'open', ?, ?, 1, ?, ?)
                """,
                (
                    gate_id,
                    run.task_id,
                    run_id,
                    run.node_id,
                    kind.value,
                    source_key,
                    _json(prompt),
                    now,
                    _stamp(expires_at) if expires_at else None,
                ),
            )
            changed = connection.execute(
                """
                UPDATE orch_runs SET status = 'waiting_gate', version = version + 1
                WHERE id = ? AND fencing_token = ? AND status IN ('claimed', 'running')
                """,
                (run_id, fencing_token),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"run {run_id} lost its fencing token")
            connection.execute("DELETE FROM orch_leases WHERE run_id = ?", (run_id,))
            if task.status is TaskStatus.RUNNING:
                connection.execute(
                    """
                    UPDATE orch_tasks SET status = 'waiting_human', version = version + 1,
                        updated_at = ? WHERE id = ? AND status = 'running'
                    """,
                    (now, task.id),
                )
            self._append_event(
                connection,
                task_id=run.task_id,
                aggregate_type="gate",
                aggregate_id=gate_id,
                event_type="gate.opened",
                payload={"kind": kind.value, "run_id": run_id, "source_key": source_key},
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"gate_id": gate_id})
            return self._require_gate(connection, gate_id)

    def open_child_wait(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        child_task_id: str,
        source_key: str,
        command_id: Optional[str] = None,
    ) -> GateRecord:
        """Atomically suspend a parent run until one of its owned children is terminal."""

        raise ConflictError(
            "single-phase child gates are disabled; use prepare_child_wait and "
            "commit_prepared_gate"
        )
        if not source_key.strip():
            raise ValueError("source_key is required")
        command_id = self._command_id(command_id)
        request = {
            "run_id": run_id,
            "lease_token": lease_token,
            "fencing_token": fencing_token,
            "child_task_id": child_task_id,
            "source_key": source_key,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "gate.open_child_wait", run_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["gate_id"])
            run = self._require_run(connection, run_id)
            self._require_lease(connection, run_id, lease_token, fencing_token)
            if run.status not in {RunStatus.CLAIMED, RunStatus.RUNNING}:
                raise ConflictError(f"run {run_id} is not active")
            task = self._require_task(connection, run.task_id)
            if task.status is not TaskStatus.RUNNING:
                raise ConflictError(
                    f"child wait cannot open while task is {task.status.value}"
                )
            child = self._require_task(connection, child_task_id)
            runtime_meta = dict(child.input.get("_runtime") or {})
            exact_attempt = str(runtime_meta.get("parent_run_id") or "") == run.id
            logical_owner = (
                bool(runtime_meta.get("spawn_key"))
                and str(runtime_meta.get("parent_plan_id") or "") == run.plan_id
                and child.parent_node_id == run.node_id
            )
            if (
                child.parent_task_id != run.task_id
                or not (exact_attempt or logical_owner)
            ):
                raise ConflictError(
                    f"task {child_task_id} is not owned by parent run {run_id}"
                )
            prompt = {
                "type": "child_wait",
                "title": f"Waiting for child task {child_task_id}",
                "description": "The parent Agent is durably suspended until the child is terminal.",
                "child_task_id": child_task_id,
                "actions": [],
            }
            existing = connection.execute(
                "SELECT * FROM orch_gates WHERE source_key = ?", (source_key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["run_id"] != run_id
                    or existing["kind"] != GateKind.CHILD_WAIT.value
                    or existing["prompt_json"] != _json(prompt)
                ):
                    raise IdempotencyConflict(
                        f"gate source key {source_key} was reused with different input"
                    )
                self._finish_command(connection, command_id, {"gate_id": existing["id"]})
                return self._gate_from_row(existing)
            gate_id = _id("gate")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_gates(
                    id, task_id, run_id, node_id, kind, status, source_key,
                    prompt_json, version, opened_at
                ) VALUES (?, ?, ?, ?, 'child_wait', 'open', ?, ?, 1, ?)
                """,
                (gate_id, run.task_id, run.id, run.node_id, source_key, _json(prompt), now),
            )
            changed = connection.execute(
                """
                UPDATE orch_runs SET status = 'waiting_gate', version = version + 1
                WHERE id = ? AND fencing_token = ? AND status IN ('claimed', 'running')
                """,
                (run.id, fencing_token),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"run {run.id} lost its fencing token")
            connection.execute("DELETE FROM orch_leases WHERE run_id = ?", (run.id,))
            if task.status is TaskStatus.RUNNING:
                validate_task_transition(task.status, TaskStatus.WAITING_CHILD)
                connection.execute(
                    """
                    UPDATE orch_tasks SET status = 'waiting_child', version = version + 1,
                        updated_at = ? WHERE id = ? AND status = 'running'
                    """,
                    (now, task.id),
                )
            self._append_event(
                connection,
                task_id=run.task_id,
                aggregate_type="gate",
                aggregate_id=gate_id,
                event_type="gate.opened",
                payload={
                    "kind": GateKind.CHILD_WAIT.value,
                    "run_id": run.id,
                    "child_task_id": child.id,
                    "source_key": source_key,
                },
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"gate_id": gate_id})
            return self._require_gate(connection, gate_id)

    def get_gate(self, gate_id: str) -> GateRecord:
        with self._read() as connection:
            return self._require_gate(connection, gate_id)

    def replace_orphaned_run_gate(
        self,
        gate_id: str,
        *,
        reason: str,
        command_id: Optional[str] = None,
    ) -> GateRecord:
        """Atomically quarantine a gate that has no trustworthy engine checkpoint."""

        command_id = self._command_id(command_id)
        request = {"gate_id": gate_id, "reason": reason}
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "gate.recover_orphan", gate_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["recovery_gate_id"])
            gate = self._require_gate(connection, gate_id)
            if gate.status is not GateStatus.OPEN or gate.run_id is None:
                raise ConflictError(f"gate {gate_id} is not an open run gate")
            run = self._require_run(connection, gate.run_id)
            if run.status is not RunStatus.WAITING_GATE:
                raise ConflictError(f"run {run.id} is not waiting on gate {gate_id}")
            task = self._require_task(connection, gate.task_id)
            now = _stamp(_now())
            connection.execute(
                """
                UPDATE orch_gates
                SET status = 'canceled', resolution_json = ?,
                    resolved_by = 'orchestration-recovery', resolved_at = ?,
                    version = version + 1
                WHERE id = ? AND status = 'open'
                """,
                (
                    _json({"decision": "quarantine", "reason": reason}),
                    now,
                    gate.id,
                ),
            )
            connection.execute(
                """
                UPDATE orch_runs
                SET status = 'failed', error_kind = 'recovery_checkpoint_missing',
                    error_message = ?, finished_at = ?, version = version + 1
                WHERE id = ? AND status = 'waiting_gate'
                """,
                (reason, now, run.id),
            )
            recovery_id = _id("gate")
            source_key = f"{gate.source_key}:recovery"
            prompt = {
                "title": "Suspended Agent requires recovery",
                "description": reason,
                "orphaned_gate_id": gate.id,
                "run_id": run.id,
                "plan_id": run.plan_id,
                "actions": ["request_changes", "cancel"],
            }
            connection.execute(
                """
                INSERT INTO orch_gates(
                    id, task_id, run_id, node_id, kind, status, source_key,
                    prompt_json, version, opened_at, published_at
                ) VALUES (?, ?, NULL, ?, 'recovery', 'open', ?, ?, 1, ?, ?)
                """,
                (
                    recovery_id,
                    task.id,
                    run.node_id,
                    source_key,
                    _json(prompt),
                    now,
                    now,
                ),
            )
            for aggregate_type, aggregate_id, event_type, payload in (
                (
                    "gate",
                    gate.id,
                    "gate.canceled",
                    {"reason": reason, "replacement_gate_id": recovery_id},
                ),
                (
                    "run",
                    run.id,
                    "run.failed",
                    {"error_kind": "recovery_checkpoint_missing", "gate_id": gate.id},
                ),
                (
                    "gate",
                    recovery_id,
                    "gate.opened",
                    {"kind": GateKind.RECOVERY.value, "source_key": source_key},
                ),
            ):
                self._append_event(
                    connection,
                    task_id=task.id,
                    aggregate_type=aggregate_type,
                    aggregate_id=aggregate_id,
                    event_type=event_type,
                    payload=payload,
                    command_id=command_id,
                )
            self._finish_command(
                connection,
                command_id,
                {"recovery_gate_id": recovery_id},
            )
            return self._require_gate(connection, recovery_id)

    def list_gates(
        self,
        task_id: str,
        *,
        statuses: Optional[Sequence[GateStatus]] = None,
        limit: Optional[int] = None,
        newest: bool = False,
        include_internal: bool = False,
        offset: int = 0,
    ) -> tuple[GateRecord, ...]:
        params: list[Any] = [task_id]
        where = "task_id = ?"
        if not include_internal:
            where += " AND published_at IS NOT NULL"
        if statuses:
            values = [GateStatus(status).value for status in statuses]
            where += " AND status IN (" + ",".join("?" for _ in values) + ")"
            params.extend(values)
        bounded = max(1, min(int(limit), 10_001)) if limit is not None else None
        skipped = max(0, int(offset))
        order_column = "opened_at" if include_internal else "published_at"
        with self._read() as connection:
            self._require_task(connection, task_id)
            base = f"SELECT * FROM orch_gates WHERE {where}"
            if bounded is not None and newest:
                rows = connection.execute(
                    "SELECT * FROM ("
                    + base
                    + f" ORDER BY {order_column} DESC, id DESC LIMIT ? OFFSET ?"
                    + f") ORDER BY {order_column}, id",
                    [*params, bounded, skipped],
                ).fetchall()
            else:
                rows = connection.execute(
                    base
                    + f" ORDER BY {order_column}, id"
                    + (" LIMIT ? OFFSET ?" if bounded is not None else ""),
                    (
                        [*params, bounded, skipped]
                        if bounded is not None
                        else params
                    ),
                ).fetchall()
        return tuple(self._gate_from_row(row) for row in rows)

    def count_gates(
        self,
        task_id: str,
        *,
        statuses: Optional[Sequence[GateStatus]] = None,
        include_internal: bool = False,
    ) -> int:
        """Count task gates without materializing an unbounded result set."""

        params: list[Any] = [task_id]
        where = "task_id = ?"
        if not include_internal:
            where += " AND published_at IS NOT NULL"
        if statuses:
            values = [GateStatus(status).value for status in statuses]
            where += " AND status IN (" + ",".join("?" for _ in values) + ")"
            params.extend(values)
        with self._read() as connection:
            self._require_task(connection, task_id)
            row = connection.execute(
                f"SELECT COUNT(*) AS gate_count FROM orch_gates WHERE {where}",
                params,
            ).fetchone()
        return int(row["gate_count"])

    def resolve_gate(
        self,
        gate_id: str,
        status: GateStatus,
        resolution: Mapping[str, Any],
        *,
        resolved_by: str,
        expected_version: Optional[int],
        command_id: Optional[str] = None,
    ) -> GateRecord:
        status = GateStatus(status)
        if status in {GateStatus.PREPARING, GateStatus.OPEN}:
            raise ValueError("a gate cannot be resolved to preparing/open")
        command_id = self._command_id(command_id)
        request = {
            "gate_id": gate_id,
            "status": status.value,
            "resolution": resolution,
            "resolved_by": resolved_by,
            "expected_version": expected_version,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "gate.resolve", gate_id, request
            )
            if replay is not None:
                return self._require_gate(connection, replay["gate_id"])
            gate = self._require_gate(connection, gate_id)
            # ``None`` means "compare with the version observed by this command".
            # It must be resolved only after the durable command replay check: an
            # HTTP response-loss retry sees the gate at version 2 but still replays
            # the original version-1 command instead of manufacturing a new request.
            effective_version = (
                gate.version if expected_version is None else int(expected_version)
            )
            if gate.status is not GateStatus.OPEN:
                raise GateConflict(f"gate {gate_id} is already {gate.status.value}")
            if gate.version != effective_version:
                raise GateConflict(
                    f"gate {gate_id} expected version {effective_version}, found {gate.version}"
                )
            now = _stamp(_now())
            changed = connection.execute(
                """
                UPDATE orch_gates
                SET status = ?, resolution_json = ?, resolved_by = ?, resolved_at = ?,
                    version = version + 1
                WHERE id = ? AND status = 'open' AND version = ?
                """,
                (
                    status.value,
                    _json(resolution),
                    resolved_by,
                    now,
                    gate_id,
                    effective_version,
                ),
            ).rowcount
            if changed != 1:
                raise GateConflict(f"gate {gate_id} was resolved concurrently")

            run_status = (
                RunStatus.QUEUED if status is GateStatus.APPROVED
                else RunStatus.CANCELED if status is GateStatus.CANCELED
                else RunStatus.FAILED
            )
            if gate.run_id is not None:
                connection.execute(
                    """
                    UPDATE orch_runs
                    SET status = ?, ready_at = ?,
                        error_kind = CASE WHEN ? = 'queued' THEN NULL ELSE 'gate_' || ? END,
                        error_message = CASE WHEN ? = 'queued' THEN NULL ELSE 'gate resolved ' || ? END,
                        finished_at = CASE WHEN ? = 'queued' THEN NULL ELSE ? END,
                        version = version + 1
                    WHERE id = ? AND status = 'waiting_gate'
                    """,
                    (
                        run_status.value,
                        now,
                        run_status.value,
                        status.value,
                        run_status.value,
                        status.value,
                        run_status.value,
                        now,
                        gate.run_id,
                    ),
                )
            task = self._require_task(connection, gate.task_id)
            target_task_status: Optional[TaskStatus] = None
            if (
                gate.run_id is not None
                and status is GateStatus.APPROVED
                and task.status
                in {
                    TaskStatus.RUNNING,
                    TaskStatus.WAITING_HUMAN,
                    TaskStatus.WAITING_CHILD,
                }
            ):
                # Multiple sibling Agents may wait at independent gates. Resolving
                # one must not resume the aggregate task while another published
                # interaction is still open.
                target_task_status = (
                    self._open_gate_wait_status(connection, task.id)
                    or TaskStatus.RUNNING
                )
                if target_task_status is task.status:
                    target_task_status = None
            elif task.status is TaskStatus.WAITING_HUMAN:
                if status is GateStatus.APPROVED:
                    target_task_status = TaskStatus.RUNNING
                elif status is GateStatus.CANCELED:
                    target_task_status = TaskStatus.CANCELING
                elif gate.kind is GateKind.FINAL_ACCEPTANCE:
                    # Final rejection is a terminal domain command, not a generic
                    # blocked gate.  Keeping it in this transaction removes the
                    # crash window where a restart could strand the task forever.
                    target_task_status = TaskStatus.FAILED
                else:
                    target_task_status = TaskStatus.BLOCKED
            elif (
                task.status is TaskStatus.WAITING_CHILD
                and gate.kind is GateKind.CHILD_WAIT
            ):
                target_task_status = (
                    TaskStatus.RUNNING
                    if status is GateStatus.APPROVED
                    else TaskStatus.CANCELING
                    if status is GateStatus.CANCELED
                    else TaskStatus.BLOCKED
                )
            if target_task_status is not None:
                validate_task_transition(task.status, target_task_status)
                task_output: Optional[Mapping[str, Any]] = None
                if (
                    gate.kind is GateKind.FINAL_ACCEPTANCE
                    and target_task_status is TaskStatus.FAILED
                ):
                    task_output = {
                        **dict(task.output or {}),
                        "accepted": False,
                        "rejected_by": resolved_by,
                        "reason": str(resolution.get("response") or "final acceptance rejected"),
                        "gate_id": gate.id,
                    }
                connection.execute(
                    """
                    UPDATE orch_tasks SET status = ?,
                        output_json = COALESCE(?, output_json),
                        version = version + 1, updated_at = ?
                    WHERE id = ? AND version = ?
                    """,
                    (
                        target_task_status.value,
                        _json(task_output) if task_output is not None else None,
                        now,
                        task.id,
                        task.version,
                    ),
                )
                if target_task_status is TaskStatus.FAILED:
                    connection.execute(
                        """
                        UPDATE orch_stage_history
                        SET disposition = 'failed', exited_at = ?, detail_json = ?
                        WHERE task_id = ? AND disposition = 'active'
                        """,
                        (
                            now,
                            _json({"reason": "final_acceptance_rejected", "gate_id": gate.id}),
                            task.id,
                        ),
                    )
                self._append_event(
                    connection,
                    task_id=task.id,
                    aggregate_type="task",
                    aggregate_id=task.id,
                    event_type="task.status_changed",
                    payload={
                        "from": task.status.value,
                        "to": target_task_status.value,
                        "gate_id": gate.id,
                    },
                    command_id=command_id,
                )

            decision_payload = {
                "title": f"{gate.kind.value} decision",
                "gate_id": gate.id,
                "decision": str(resolution.get("decision") or status.value),
                "response": str(resolution.get("response") or ""),
                "resolved_by": resolved_by,
            }
            evidence_id = _id("evidence")
            decision_hash = _digest(decision_payload)
            connection.execute(
                """
                INSERT INTO orch_evidence(
                    id, task_id, plan_id, node_id, run_id, kind, mime_type,
                    content_hash, payload_json, blob_uri, created_by, created_at
                ) VALUES (?, ?, NULL, ?, ?, ?, 'application/json', ?, ?, NULL, ?, ?)
                """,
                (
                    evidence_id,
                    gate.task_id,
                    gate.node_id,
                    gate.run_id,
                    EvidenceKind.DECISION.value,
                    decision_hash,
                    _json(decision_payload),
                    resolved_by,
                    now,
                ),
            )
            self._append_event(
                connection,
                task_id=gate.task_id,
                aggregate_type="evidence",
                aggregate_id=evidence_id,
                event_type="evidence.added",
                payload={
                    "kind": EvidenceKind.DECISION.value,
                    "content_hash": decision_hash,
                    "gate_id": gate.id,
                    "created_by": resolved_by,
                },
                command_id=command_id,
            )
            self._append_event(
                connection,
                task_id=gate.task_id,
                aggregate_type="gate",
                aggregate_id=gate_id,
                event_type=f"gate.{status.value}",
                payload={"run_id": gate.run_id, "resolved_by": resolved_by},
                command_id=command_id,
            )
            checkpoint = {}
            if gate.run_id:
                resumed_run = self._require_run(connection, gate.run_id)
                checkpoint = dict((resumed_run.output or {}).get("engine_checkpoint") or {})
            self._enqueue_wake_connection(
                connection,
                target_task_id=gate.task_id,
                target_run_id=gate.run_id,
                reason=WakeReason.GATE_RESOLVED,
                source_task_id=gate.task_id,
                source_run_id=gate.run_id,
                source_event_id=None,
                payload={
                    "gate_id": gate.id,
                    "status": status.value,
                    "response_delta": dict(resolution),
                    "checkpoint_ref": checkpoint.get("blob_uri"),
                },
                dedupe_key=f"{gate.task_id}:{gate.run_id or 'current'}:gate_resolved:{gate.id}",
                not_before=None,
                command_id=command_id,
            )
            self._finish_command(connection, command_id, {"gate_id": gate_id})
            return self._require_gate(connection, gate_id)

    # -- immutable evidence ------------------------------------------------
    def add_evidence(
        self,
        task_id: str,
        *,
        kind: EvidenceKind,
        payload: Mapping[str, Any],
        created_by: str,
        mime_type: str = "application/json",
        content_hash: Optional[str] = None,
        blob_uri: Optional[str] = None,
        plan_id: Optional[str] = None,
        node_id: Optional[str] = None,
        run_id: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> EvidenceRecord:
        kind = EvidenceKind(kind)
        chosen_hash = content_hash or _digest(payload)
        command_id = self._command_id(command_id)
        request = {
            "task_id": task_id,
            "kind": kind.value,
            "payload": payload,
            "created_by": created_by,
            "mime_type": mime_type,
            "content_hash": chosen_hash,
            "blob_uri": blob_uri,
            "plan_id": plan_id,
            "node_id": node_id,
            "run_id": run_id,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "evidence.add", task_id, request
            )
            if replay is not None:
                return self._require_evidence(connection, replay["evidence_id"])
            self._require_task(connection, task_id)
            if plan_id is not None:
                row = connection.execute(
                    "SELECT task_id FROM orch_plans WHERE id = ?", (plan_id,)
                ).fetchone()
                if row is None or row["task_id"] != task_id:
                    raise NotFoundError(f"plan {plan_id} does not belong to task {task_id}")
            if run_id is not None:
                run = self._require_run(connection, run_id)
                if run.task_id != task_id:
                    raise ConflictError(f"run {run_id} does not belong to task {task_id}")
            evidence_id = _id("evidence")
            now = _stamp(_now())
            connection.execute(
                """
                INSERT INTO orch_evidence(
                    id, task_id, plan_id, node_id, run_id, kind, mime_type,
                    content_hash, payload_json, blob_uri, created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence_id,
                    task_id,
                    plan_id,
                    node_id,
                    run_id,
                    kind.value,
                    mime_type,
                    chosen_hash,
                    _json(payload),
                    blob_uri,
                    created_by,
                    now,
                ),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="evidence",
                aggregate_id=evidence_id,
                event_type="evidence.added",
                payload={
                    "kind": kind.value,
                    "content_hash": chosen_hash,
                    "created_by": created_by,
                },
                command_id=command_id,
            )
            self._finish_command(
                connection, command_id, {"evidence_id": evidence_id}
            )
            return self._require_evidence(connection, evidence_id)

    def list_evidence(
        self,
        task_id: str,
        *,
        limit: Optional[int] = None,
        newest: bool = False,
        offset: int = 0,
    ) -> tuple[EvidenceRecord, ...]:
        bounded = max(1, min(int(limit), 10_001)) if limit is not None else None
        skipped = max(0, int(offset))
        with self._read() as connection:
            self._require_task(connection, task_id)
            if bounded is not None and newest:
                rows = connection.execute(
                    """
                    SELECT * FROM (
                        SELECT *, rowid AS _evidence_order
                        FROM orch_evidence WHERE task_id = ?
                        ORDER BY created_at DESC, rowid DESC LIMIT ? OFFSET ?
                    ) ORDER BY created_at, _evidence_order
                    """,
                    (task_id, bounded, skipped),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM orch_evidence WHERE task_id = ? "
                    "ORDER BY created_at, rowid"
                    + (" LIMIT ? OFFSET ?" if bounded is not None else ""),
                    (task_id, bounded, skipped)
                    if bounded is not None
                    else (task_id,),
                ).fetchall()
        return tuple(self._evidence_from_row(row) for row in rows)

    def find_evidence_blob(self, digest: str) -> Optional[EvidenceRecord]:
        """Resolve a content-addressed blob reference with one indexed query."""

        normalized = str(digest).strip().lower().removeprefix("sha256:")
        with self._read() as connection:
            row = connection.execute(
                """
                SELECT * FROM orch_evidence
                WHERE content_hash = ? AND blob_uri = ?
                ORDER BY created_at, id LIMIT 1
                """,
                (normalized, f"sha256:{normalized}"),
            ).fetchone()
        return self._evidence_from_row(row) if row is not None else None

    def find_work_product_artifact(
        self, digest: str
    ) -> Optional[WorkProductRecord]:
        """Resolve a blob referenced by an immutable Work Product."""

        normalized = str(digest).strip().lower().removeprefix("sha256:")
        uri = f"sha256:{normalized}"
        with self._read() as connection:
            row = connection.execute(
                """
                SELECT * FROM orch_work_products
                WHERE artifact_id = ? OR uri = ?
                   OR content_hash = ? OR content_hash = ?
                ORDER BY created_at, id LIMIT 1
                """,
                (uri, uri, normalized, uri),
            ).fetchone()
        return self._work_product_from_row(row) if row is not None else None

    # -- append-only events and transactional outbox -----------------------
    def list_events(
        self,
        *,
        task_id: Optional[str] = None,
        after_sequence: int = 0,
        before_sequence: Optional[int] = None,
        newest: bool = False,
        limit: int = 1_000,
    ) -> tuple[EventRecord, ...]:
        """List an audit page without changing the historical forward cursor API.

        ``newest=True`` returns the newest matching page in chronological order.  A
        caller can then pass its first sequence as ``before_sequence`` to walk
        backwards.  The default remains the original forward, ``after_sequence``
        contract used by outbox/audit consumers.
        """

        # API pages may request one look-ahead row to compute ``has_more`` for
        # the public 10,000-row maximum without issuing a second query.
        limit = max(1, min(int(limit), 10_001))
        params: list[Any] = [max(0, int(after_sequence))]
        where = "sequence_no > ?"
        if before_sequence is not None:
            before = max(1, int(before_sequence))
            where += " AND sequence_no < ?"
            params.append(before)
        if task_id is not None:
            where += " AND task_id = ?"
            params.append(task_id)
        params.append(limit)
        with self._read() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM orch_events WHERE {where}
                ORDER BY sequence_no {'DESC' if newest else 'ASC'} LIMIT ?
                """,
                params,
            ).fetchall()
        if newest:
            rows.reverse()
        return tuple(self._event_from_row(row) for row in rows)

    def verify_event_chain(self) -> bool:
        """Recompute the global event chain; raise if any row was corrupted."""

        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM orch_events ORDER BY sequence_no"
            ).fetchall()
        previous = _ZERO_HASH
        for row in rows:
            if row["previous_hash"] != previous:
                raise IntegrityError(
                    f"event {row['id']} previous hash does not match the chain"
                )
            envelope = self._event_envelope(
                event_id=row["id"],
                task_id=row["task_id"],
                aggregate_type=row["aggregate_type"],
                aggregate_id=row["aggregate_id"],
                event_type=row["event_type"],
                payload=_load(row["payload_json"], {}),
                command_id=row["command_id"],
                created_at=row["created_at"],
            )
            expected = _event_hash(previous, envelope)
            if row["event_hash"] != expected:
                raise IntegrityError(f"event {row['id']} hash is invalid")
            previous = expected
        return True

    def verify_event_page(
        self, events: Sequence[EventRecord]
    ) -> dict[str, Any]:
        """Verify a bounded audit page and each row's immediate global predecessor.

        This is intentionally not presented as a genesis-to-tip verification.  Startup
        performs that full fail-closed check once; interactive pagination must remain
        bounded as the append-only audit history grows.
        """

        page = tuple(events)
        if not page:
            return {
                "valid": True,
                "scope": "page_with_predecessors",
                "verified_events": 0,
                "through_sequence": None,
                "through_hash": None,
            }
        if len({event.sequence for event in page}) != len(page):
            raise IntegrityError("audit page contains duplicate sequence numbers")

        page_by_sequence = {event.sequence: event for event in page}
        needed = sorted(
            {
                event.sequence - 1
                for event in page
                if event.sequence > 1 and event.sequence - 1 not in page_by_sequence
            }
        )
        predecessors: dict[int, EventRecord] = {}
        with self._read() as connection:
            # Stay below conservative SQLite variable limits while keeping the amount
            # of work proportional to the requested page, never the global history.
            for index in range(0, len(needed), 400):
                chunk = needed[index : index + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = connection.execute(
                    f"SELECT * FROM orch_events WHERE sequence_no IN ({placeholders})",
                    chunk,
                ).fetchall()
                predecessors.update(
                    {
                        int(row["sequence_no"]): self._event_from_row(row)
                        for row in rows
                    }
                )
        missing = [sequence for sequence in needed if sequence not in predecessors]
        if missing:
            raise IntegrityError(
                f"audit page predecessor is missing at sequence {missing[0]}"
            )

        verified = {**predecessors, **page_by_sequence}
        for event in verified.values():
            envelope = self._event_envelope(
                event_id=event.id,
                task_id=event.task_id,
                aggregate_type=event.aggregate_type,
                aggregate_id=event.aggregate_id,
                event_type=event.event_type,
                payload=event.payload,
                command_id=event.command_id,
                created_at=_stamp(event.created_at),
            )
            expected = _event_hash(event.previous_hash, envelope)
            if event.event_hash != expected:
                raise IntegrityError(f"event {event.id} hash is invalid")

        for event in page:
            expected_previous = (
                _ZERO_HASH
                if event.sequence == 1
                else verified[event.sequence - 1].event_hash
            )
            if event.previous_hash != expected_previous:
                raise IntegrityError(
                    f"event {event.id} previous hash does not match sequence "
                    f"{event.sequence - 1}"
                )

        tip = max(page, key=lambda event: event.sequence)
        return {
            "valid": True,
            "scope": "page_with_predecessors",
            "verified_events": len(page),
            "through_sequence": tip.sequence,
            "through_hash": tip.event_hash,
        }

    def claim_outbox(
        self,
        worker_id: str,
        *,
        limit: int = 100,
        lease_seconds: int = 30,
        now: Optional[datetime] = None,
    ) -> tuple[OutboxRecord, ...]:
        if not worker_id.strip():
            raise ValueError("worker_id is required")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        chosen_now = now or _now()
        until = chosen_now + timedelta(seconds=lease_seconds)
        limit = max(1, min(int(limit), 1_000))
        with self._write() as connection:
            rows = connection.execute(
                """
                SELECT id FROM orch_outbox
                WHERE published_at IS NULL AND dead_lettered_at IS NULL
                  AND available_at <= ?
                  AND (locked_until IS NULL OR locked_until <= ?)
                ORDER BY available_at, created_at, id LIMIT ?
                """,
                (_stamp(chosen_now), _stamp(chosen_now), limit),
            ).fetchall()
            ids = [row["id"] for row in rows]
            for outbox_id in ids:
                connection.execute(
                    """
                    UPDATE orch_outbox
                    SET locked_by = ?, locked_until = ?, attempts = attempts + 1
                    WHERE id = ? AND published_at IS NULL
                      AND dead_lettered_at IS NULL
                      AND (locked_until IS NULL OR locked_until <= ?)
                    """,
                    (worker_id, _stamp(until), outbox_id, _stamp(chosen_now)),
                )
            if not ids:
                return tuple()
            placeholders = ",".join("?" for _ in ids)
            claimed = connection.execute(
                f"""
                SELECT * FROM orch_outbox
                WHERE id IN ({placeholders}) AND locked_by = ?
                ORDER BY available_at, created_at, id
                """,
                [*ids, worker_id],
            ).fetchall()
            return tuple(self._outbox_from_row(row) for row in claimed)

    def mark_outbox_published(
        self,
        outbox_id: str,
        worker_id: str,
        *,
        published_at: Optional[datetime] = None,
    ) -> OutboxRecord:
        with self._write() as connection:
            changed = connection.execute(
                """
                UPDATE orch_outbox
                SET published_at = ?, locked_by = NULL, locked_until = NULL,
                    last_error = NULL
                WHERE id = ? AND published_at IS NULL AND locked_by = ?
                """,
                (_stamp(published_at or _now()), outbox_id, worker_id),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"outbox item {outbox_id} is not leased by {worker_id}")
            row = connection.execute(
                "SELECT * FROM orch_outbox WHERE id = ?", (outbox_id,)
            ).fetchone()
            return self._outbox_from_row(row)

    def mark_outbox_failed(
        self,
        outbox_id: str,
        worker_id: str,
        error: str,
        *,
        retry_at: Optional[datetime] = None,
    ) -> OutboxRecord:
        with self._write() as connection:
            changed = connection.execute(
                """
                UPDATE orch_outbox
                SET available_at = ?, locked_by = NULL, locked_until = NULL,
                    last_error = ?
                WHERE id = ? AND published_at IS NULL AND locked_by = ?
                """,
                (_stamp(retry_at or _now()), error[:2_000], outbox_id, worker_id),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(f"outbox item {outbox_id} is not leased by {worker_id}")
            row = connection.execute(
                "SELECT * FROM orch_outbox WHERE id = ?", (outbox_id,)
            ).fetchone()
            return self._outbox_from_row(row)

    def mark_outbox_dead_lettered(
        self,
        outbox_id: str,
        worker_id: str,
        error: str,
        *,
        dead_lettered_at: Optional[datetime] = None,
    ) -> OutboxRecord:
        with self._write() as connection:
            changed = connection.execute(
                """
                UPDATE orch_outbox
                SET dead_lettered_at = ?, locked_by = NULL, locked_until = NULL,
                    last_error = ?
                WHERE id = ? AND published_at IS NULL
                  AND dead_lettered_at IS NULL AND locked_by = ?
                """,
                (
                    _stamp(dead_lettered_at or _now()),
                    error[:2_000],
                    outbox_id,
                    worker_id,
                ),
            ).rowcount
            if changed != 1:
                raise LeaseConflict(
                    f"outbox item {outbox_id} is not leased by {worker_id}"
                )
            row = connection.execute(
                "SELECT * FROM orch_outbox WHERE id = ?", (outbox_id,)
            ).fetchone()
            return self._outbox_from_row(row)

    def outbox_health(self) -> dict[str, Any]:
        with self._read() as connection:
            row = connection.execute(
                """
                SELECT
                    SUM(CASE WHEN published_at IS NULL
                                  AND dead_lettered_at IS NULL THEN 1 ELSE 0 END)
                        AS pending,
                    SUM(CASE WHEN dead_lettered_at IS NOT NULL THEN 1 ELSE 0 END)
                        AS dead_letters,
                    MIN(CASE WHEN published_at IS NULL
                                  AND dead_lettered_at IS NULL THEN created_at END)
                        AS oldest_pending_at
                FROM orch_outbox
                """
            ).fetchone()
        return {
            "pending": int(row["pending"] or 0),
            "dead_letters": int(row["dead_letters"] or 0),
            "oldest_pending_at": row["oldest_pending_at"],
        }

    def list_outbox_dead_letters(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[OutboxRecord, ...]:
        bounded = max(1, min(int(limit), 500))
        skipped = max(0, int(offset))
        with self._read() as connection:
            rows = connection.execute(
                """
                SELECT * FROM orch_outbox
                WHERE dead_lettered_at IS NOT NULL
                ORDER BY dead_lettered_at DESC, id DESC LIMIT ? OFFSET ?
                """,
                (bounded, skipped),
            ).fetchall()
        return tuple(self._outbox_from_row(row) for row in rows)

    def get_outbox(self, outbox_id: str) -> OutboxRecord:
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_outbox WHERE id = ?", (outbox_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"outbox item not found: {outbox_id}")
        return self._outbox_from_row(row)

    def list_outbox_requeue_history(
        self, outbox_id: str, *, limit: int = 100, offset: int = 0
    ) -> tuple[OutboxRequeueRecord, ...]:
        bounded = max(1, min(int(limit), 1_000))
        skipped = max(0, int(offset))
        with self._read() as connection:
            if connection.execute(
                "SELECT 1 FROM orch_outbox WHERE id = ?", (outbox_id,)
            ).fetchone() is None:
                raise NotFoundError(f"outbox item not found: {outbox_id}")
            rows = connection.execute(
                """
                SELECT * FROM orch_outbox_requeue_history
                WHERE outbox_id = ?
                ORDER BY requeued_at DESC, id DESC LIMIT ? OFFSET ?
                """,
                (outbox_id, bounded, skipped),
            ).fetchall()
        return tuple(self._outbox_requeue_from_row(row) for row in rows)

    def count_outbox_requeue_history(self, outbox_id: str) -> int:
        with self._read() as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM orch_outbox_requeue_history WHERE outbox_id = ?",
                (outbox_id,),
            ).fetchone()
        return int(row[0])

    def list_outbox_requeue_histories(
        self, outbox_ids: Sequence[str], *, per_item_limit: int = 20
    ) -> tuple[dict[str, tuple[OutboxRequeueRecord, ...]], dict[str, int]]:
        """Fetch bounded per-item history for a dead-letter page in one query."""

        ids = tuple(dict.fromkeys(str(item) for item in outbox_ids if str(item)))
        if not ids:
            return {}, {}
        limit = max(1, min(int(per_item_limit), 100))
        placeholders = ",".join("?" for _ in ids)
        with self._read() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM (
                    SELECT history.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY outbox_id
                               ORDER BY requeued_at DESC, id DESC
                           ) AS history_rank,
                           COUNT(*) OVER (PARTITION BY outbox_id) AS history_total
                    FROM orch_outbox_requeue_history AS history
                    WHERE outbox_id IN ({placeholders})
                ) WHERE history_rank <= ?
                ORDER BY outbox_id, requeued_at DESC, id DESC
                """,
                (*ids, limit),
            ).fetchall()
        grouped: dict[str, list[OutboxRequeueRecord]] = {item: [] for item in ids}
        totals: dict[str, int] = {item: 0 for item in ids}
        for row in rows:
            grouped[str(row["outbox_id"])].append(
                self._outbox_requeue_from_row(row)
            )
            totals[str(row["outbox_id"])] = int(row["history_total"])
        return {key: tuple(value) for key, value in grouped.items()}, totals

    def requeue_outbox(
        self,
        outbox_id: str,
        *,
        idempotency_key: str,
        actor: str,
        reason: str,
    ) -> tuple[OutboxRecord, OutboxRequeueRecord, bool]:
        command_id = str(idempotency_key).strip()
        chosen_actor = str(actor).strip()
        chosen_reason = str(reason).strip()
        if not command_id:
            raise ValueError("idempotency key is required")
        if len(command_id) > 256:
            raise ValueError("idempotency key is too long")
        if not chosen_actor or len(chosen_actor) > 200:
            raise ValueError("actor must contain between 1 and 200 characters")
        if not chosen_reason or len(chosen_reason) > 2_000:
            raise ValueError("reason must contain between 1 and 2000 characters")
        request = {
            "outbox_id": outbox_id,
            "actor": chosen_actor,
            "reason": chosen_reason,
        }
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "outbox.requeue", outbox_id, request
            )
            if replay is not None:
                row = connection.execute(
                    "SELECT * FROM orch_outbox WHERE id = ?", (outbox_id,)
                ).fetchone()
                history = connection.execute(
                    "SELECT * FROM orch_outbox_requeue_history WHERE id = ?",
                    (str(replay.get("history_id") or ""),),
                ).fetchone()
                if row is None or history is None:
                    raise IntegrityError(
                        f"completed requeue command {command_id} has missing result records"
                    )
                return (
                    self._outbox_from_row(row),
                    self._outbox_requeue_from_row(history),
                    True,
                )
            row = connection.execute(
                "SELECT * FROM orch_outbox WHERE id = ?", (outbox_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"outbox item not found: {outbox_id}")
            if row["published_at"] is not None:
                raise ConflictError(f"outbox item {outbox_id} is already published")
            if row["dead_lettered_at"] is None:
                raise ConflictError(f"outbox item {outbox_id} is not dead-lettered")
            requeued_at = _stamp(_now())
            history_id = _id("outbox_requeue")
            connection.execute(
                """
                INSERT INTO orch_outbox_requeue_history(
                    id, outbox_id, command_id, actor, reason, snapshot_attempts,
                    snapshot_last_error, snapshot_dead_lettered_at, requeued_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    history_id,
                    outbox_id,
                    command_id,
                    chosen_actor,
                    chosen_reason,
                    int(row["attempts"]),
                    row["last_error"],
                    row["dead_lettered_at"],
                    requeued_at,
                ),
            )
            changed = connection.execute(
                """
                UPDATE orch_outbox
                SET dead_lettered_at = NULL, attempts = 0, available_at = ?,
                    locked_by = NULL, locked_until = NULL, last_error = NULL
                WHERE id = ? AND dead_lettered_at = ? AND published_at IS NULL
                """,
                (requeued_at, outbox_id, row["dead_lettered_at"]),
            ).rowcount
            if changed != 1:
                raise ConflictError(f"outbox item {outbox_id} changed during requeue")
            event = connection.execute(
                "SELECT task_id FROM orch_events WHERE id = ?", (row["event_id"],)
            ).fetchone()
            self._append_event(
                connection,
                task_id=event["task_id"] if event is not None else None,
                aggregate_type="outbox",
                aggregate_id=outbox_id,
                event_type="outbox.requeued",
                payload={
                    "history_id": history_id,
                    "actor": chosen_actor,
                    "reason": chosen_reason,
                    "snapshot_attempts": int(row["attempts"]),
                    "snapshot_last_error": row["last_error"],
                    "snapshot_dead_lettered_at": row["dead_lettered_at"],
                },
                command_id=command_id,
            )
            self._finish_command(
                connection,
                command_id,
                {
                    "outbox_id": outbox_id,
                    "history_id": history_id,
                    "status": "queued",
                    "attempts": 0,
                    "requeued_at": requeued_at,
                },
            )
            refreshed = connection.execute(
                "SELECT * FROM orch_outbox WHERE id = ?", (outbox_id,)
            ).fetchone()
            history = connection.execute(
                "SELECT * FROM orch_outbox_requeue_history WHERE id = ?",
                (history_id,),
            ).fetchone()
            if refreshed is None or history is None:
                raise IntegrityError(f"requeue result for {outbox_id} was not persisted")
            return (
                self._outbox_from_row(refreshed),
                self._outbox_requeue_from_row(history),
                False,
            )

    # -- single-active scheduler lease -----------------------------------
    def acquire_scheduler_leader(
        self,
        owner: str,
        *,
        lease_seconds: int = 15,
        now: Optional[datetime] = None,
    ) -> tuple[str, int]:
        if not owner.strip():
            raise ValueError("scheduler owner is required")
        if lease_seconds < 3:
            raise ValueError("scheduler lease_seconds must be at least 3")
        chosen_now = now or _now()
        expires_at = chosen_now + timedelta(seconds=lease_seconds)
        token = uuid.uuid4().hex
        with self._write(enforce_scheduler_fence=False) as connection:
            existing = connection.execute(
                "SELECT * FROM orch_scheduler_leader WHERE singleton = 1"
            ).fetchone()
            if existing is not None and existing["expires_at"] > _stamp(chosen_now):
                raise LeaseConflict(
                    f"orchestration scheduler is already owned by {existing['owner']}"
                )
            epoch = int(existing["epoch"] if existing is not None else 0) + 1
            connection.execute(
                """
                INSERT INTO orch_scheduler_leader(
                    singleton, owner, token, epoch, expires_at, heartbeat_at
                ) VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    owner = excluded.owner,
                    token = excluded.token,
                    epoch = excluded.epoch,
                    expires_at = excluded.expires_at,
                    heartbeat_at = excluded.heartbeat_at
                """,
                (
                    owner,
                    token,
                    epoch,
                    _stamp(expires_at),
                    _stamp(chosen_now),
                ),
            )
        return token, epoch

    def heartbeat_scheduler_leader(
        self,
        owner: str,
        token: str,
        epoch: int,
        *,
        lease_seconds: int = 15,
        now: Optional[datetime] = None,
    ) -> None:
        chosen_now = now or _now()
        with self._write(enforce_scheduler_fence=False) as connection:
            changed = connection.execute(
                """
                UPDATE orch_scheduler_leader
                SET expires_at = ?, heartbeat_at = ?
                WHERE singleton = 1 AND owner = ? AND token = ? AND epoch = ?
                  AND expires_at > ?
                """,
                (
                    _stamp(chosen_now + timedelta(seconds=lease_seconds)),
                    _stamp(chosen_now),
                    owner,
                    token,
                    int(epoch),
                    _stamp(chosen_now),
                ),
            ).rowcount
            if changed != 1:
                raise LeaseConflict("orchestration scheduler leader lease was lost")

    def release_scheduler_leader(
        self, owner: str, token: str, epoch: int
    ) -> bool:
        chosen_now = _now()
        with self._write(enforce_scheduler_fence=False) as connection:
            changed = connection.execute(
                """
                UPDATE orch_scheduler_leader
                SET expires_at = ?, heartbeat_at = ?
                WHERE singleton = 1 AND owner = ? AND token = ? AND epoch = ?
                """,
                (_stamp(chosen_now), _stamp(chosen_now), owner, token, int(epoch)),
            ).rowcount
        return changed == 1

    def _append_event(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: Optional[str],
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Mapping[str, Any],
        command_id: Optional[str],
    ) -> EventRecord:
        previous_row = connection.execute(
            "SELECT event_hash FROM orch_events ORDER BY sequence_no DESC LIMIT 1"
        ).fetchone()
        previous_hash = previous_row["event_hash"] if previous_row else _ZERO_HASH
        event_id = _id("event")
        created_at = _stamp(_now())
        envelope = self._event_envelope(
            event_id=event_id,
            task_id=task_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            command_id=command_id,
            created_at=created_at,
        )
        event_hash = _event_hash(previous_hash, envelope)
        connection.execute(
            """
            INSERT INTO orch_events(
                id, task_id, aggregate_type, aggregate_id, event_type,
                payload_json, previous_hash, event_hash, command_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                task_id,
                aggregate_type,
                aggregate_id,
                event_type,
                _json(payload),
                previous_hash,
                event_hash,
                command_id,
                created_at,
            ),
        )
        row = connection.execute(
            "SELECT * FROM orch_events WHERE id = ?", (event_id,)
        ).fetchone()
        outbox_id = _id("outbox")
        outbox_payload = {
            "event_id": event_id,
            "sequence": int(row["sequence_no"]),
            "task_id": task_id,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "payload": payload,
            "previous_hash": previous_hash,
            "event_hash": event_hash,
            "created_at": created_at,
        }
        connection.execute(
            """
            INSERT INTO orch_outbox(
                id, event_id, topic, payload_json, available_at, attempts, created_at
            ) VALUES (?, ?, ?, ?, ?, 0, ?)
            """,
            (
                outbox_id,
                event_id,
                f"orchestration.{event_type}",
                _json(outbox_payload),
                created_at,
                created_at,
            ),
        )
        return self._event_from_row(row)

    @staticmethod
    def _event_envelope(
        *,
        event_id: str,
        task_id: Optional[str],
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Mapping[str, Any],
        command_id: Optional[str],
        created_at: str,
    ) -> dict[str, Any]:
        return {
            "id": event_id,
            "task_id": task_id,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "payload": payload,
            "command_id": command_id,
            "created_at": created_at,
        }

    # -- row loading and invariants ----------------------------------------
    def _require_task(
        self, connection: sqlite3.Connection, task_id: str
    ) -> TaskRecord:
        row = connection.execute(
            "SELECT * FROM orch_tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"task not found: {task_id}")
        return self._task_from_row(row)

    def _root_task_id_connection(
        self, connection: sqlite3.Connection, task_id: str
    ) -> str:
        task = self._require_task(connection, task_id)
        seen: set[str] = set()
        while task.parent_task_id:
            if task.id in seen:
                raise IntegrityError("durable task parent cycle detected")
            seen.add(task.id)
            task = self._require_task(connection, task.parent_task_id)
        return task.id

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            title=row["title"],
            objective=row["objective"],
            domain=TaskDomain(row["domain"]),
            workspace=row["workspace"],
            constraints=tuple(_load(row["constraints_json"], [])),
            acceptance_criteria=tuple(_load(row["acceptance_criteria_json"], [])),
            complexity_score=row["complexity_score"],
            complexity_level=(
                ComplexityLevel(row["complexity_level"])
                if row["complexity_level"] is not None
                else None
            ),
            risk_tier=RiskTier(row["risk_tier"]),
            budget=_load(row["budget_json"], {}),
            policy=_load(row["policy_json"], {}),
            input=_load(row["input_json"], {}),
            output=_load(row["output_json"]),
            status=TaskStatus(row["status"]),
            current_stage=OrchestrationStage(row["current_stage"]),
            active_plan_id=row["active_plan_id"],
            parent_task_id=row["parent_task_id"],
            parent_node_id=row["parent_node_id"],
            priority=int(row["priority"]),
            max_parallel_runs=int(row["max_parallel_runs"]),
            version=int(row["version"]),
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
            updated_at=_time(row["updated_at"]),  # type: ignore[arg-type]
            active_brief_id=row["active_brief_id"],
        )

    @staticmethod
    def _brief_draft_from_row(row: sqlite3.Row) -> TaskBriefDraft:
        return TaskBriefDraft(
            title=row["title"],
            objective=row["objective"],
            background=row["background"],
            scope=_load(row["scope_json"], {}),
            instructions=tuple(_load(row["instructions_json"], [])),
            constraints=tuple(_load(row["constraints_json"], [])),
            non_goals=tuple(_load(row["non_goals_json"], [])),
            acceptance_criteria=tuple(
                _load(row["acceptance_criteria_json"], [])
            ),
            deliverables=tuple(_load(row["deliverables_json"], [])),
            result_contract=_load(row["result_contract_json"], {}),
        )

    @classmethod
    def _brief_from_row(cls, row: sqlite3.Row) -> TaskBriefRecord:
        draft = cls._brief_draft_from_row(row)
        return TaskBriefRecord(
            id=row["id"],
            task_id=row["task_id"],
            revision=int(row["revision"]),
            status=BriefStatus(row["status"]),
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
            created_by_task_id=row["created_by_task_id"],
            created_by_run_id=row["created_by_run_id"],
            content_hash=row["content_hash"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
            published_at=_time(row["published_at"]),
        )

    def _require_brief(
        self, connection: sqlite3.Connection, brief_id: str
    ) -> TaskBriefRecord:
        row = connection.execute(
            "SELECT * FROM orch_task_briefs WHERE id = ?", (brief_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"task brief not found: {brief_id}")
        return self._brief_from_row(row)

    @staticmethod
    def _context_ref_from_row(row: sqlite3.Row) -> ContextRefRecord:
        draft = ContextRefDraft(
            requirement=row["requirement"],
            ref_type=row["ref_type"],
            display_name=row["display_name"],
            summary=row["summary"],
            selection_reason=row["selection_reason"],
            locator=_load(row["locator_json"], {}),
            delivery_mode=row["delivery_mode"],
            mime_type=row["mime_type"],
            content_hash=row["content_hash"],
            byte_size=row["byte_size"],
            token_estimate=row["token_estimate"],
            provenance=_load(row["provenance_json"], {}),
            trust_level=row["trust_level"],
        )
        return ContextRefRecord(
            id=row["id"],
            task_id=row["task_id"],
            brief_id=row["brief_id"],
            requirement=ContextRequirement(draft.requirement),
            ref_type=ContextRefType(draft.ref_type),
            display_name=draft.display_name,
            summary=draft.summary,
            selection_reason=draft.selection_reason,
            locator=draft.locator,
            delivery_mode=ContextDeliveryMode(draft.delivery_mode),
            mime_type=draft.mime_type,
            content_hash=draft.content_hash,
            byte_size=draft.byte_size,
            token_estimate=draft.token_estimate,
            provenance=draft.provenance,
            trust_level=draft.trust_level,
            created_by_task_id=row["created_by_task_id"],
            created_by_run_id=row["created_by_run_id"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    def _require_context_ref(
        self, connection: sqlite3.Connection, ref_id: str
    ) -> ContextRefRecord:
        row = connection.execute(
            "SELECT * FROM orch_context_refs WHERE id = ?", (ref_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"context reference not found: {ref_id}")
        return self._context_ref_from_row(row)

    @staticmethod
    def _relation_from_row(row: sqlite3.Row) -> TaskRelationRecord:
        return TaskRelationRecord(
            id=row["id"],
            from_task_id=row["from_task_id"],
            to_task_id=row["to_task_id"],
            relation_type=TaskRelationType(row["relation_type"]),
            metadata=_load(row["metadata_json"], {}),
            created_by_task_id=row["created_by_task_id"],
            created_by_run_id=row["created_by_run_id"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
            removed_at=_time(row["removed_at"]),
        )

    def _require_relation(
        self,
        connection: sqlite3.Connection,
        relation_id: str,
        *,
        include_removed: bool = False,
    ) -> TaskRelationRecord:
        row = connection.execute(
            "SELECT * FROM orch_task_relations WHERE id = ?", (relation_id,)
        ).fetchone()
        if row is None or (row["removed_at"] is not None and not include_removed):
            raise NotFoundError(f"task relation not found: {relation_id}")
        return self._relation_from_row(row)

    @staticmethod
    def _wake_from_row(row: sqlite3.Row) -> WakeRequestRecord:
        return WakeRequestRecord(
            id=row["id"],
            target_task_id=row["target_task_id"],
            target_run_id=row["target_run_id"],
            reason=WakeReason(row["reason"]),
            source_task_id=row["source_task_id"],
            source_run_id=row["source_run_id"],
            source_event_id=row["source_event_id"],
            payload=_load(row["payload_json"], {}),
            dedupe_key=row["dedupe_key"],
            status=WakeStatus(row["status"]),
            coalesced_count=int(row["coalesced_count"]),
            attempts=int(row["attempts"]),
            not_before=_time(row["not_before"]),  # type: ignore[arg-type]
            claimed_by=row["claimed_by"],
            claimed_until=_time(row["claimed_until"]),
            last_error=row["last_error"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
            updated_at=_time(row["updated_at"]),  # type: ignore[arg-type]
            delivered_at=_time(row["delivered_at"]),
            completed_at=_time(row["completed_at"]),
        )

    def _require_wake(
        self, connection: sqlite3.Connection, wake_id: str
    ) -> WakeRequestRecord:
        row = connection.execute(
            "SELECT * FROM orch_wake_requests WHERE id = ?", (wake_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"wake request not found: {wake_id}")
        return self._wake_from_row(row)

    @staticmethod
    def _comment_from_row(row: sqlite3.Row) -> TaskCommentRecord:
        return TaskCommentRecord(
            id=row["id"],
            task_id=row["task_id"],
            sequence=int(row["sequence_no"]),
            author_type=row["author_type"],
            author_id=row["author_id"],
            created_by_run_id=row["created_by_run_id"],
            body_markdown=row["body_markdown"],
            metadata=_load(row["metadata_json"], {}),
            reply_to_comment_id=row["reply_to_comment_id"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    def _require_comment(
        self, connection: sqlite3.Connection, comment_id: str
    ) -> TaskCommentRecord:
        row = connection.execute(
            "SELECT * FROM orch_task_comments WHERE id = ?", (comment_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"task comment not found: {comment_id}")
        return self._comment_from_row(row)

    @staticmethod
    def _work_product_from_row(row: sqlite3.Row) -> WorkProductRecord:
        return WorkProductRecord(
            id=row["id"],
            task_id=row["task_id"],
            run_id=row["run_id"],
            kind=WorkProductKind(row["kind"]),
            title=row["title"],
            summary=row["summary"],
            evidence_id=row["evidence_id"],
            artifact_id=row["artifact_id"],
            uri=row["uri"],
            content_hash=row["content_hash"],
            metadata=_load(row["metadata_json"], {}),
            verification_status=row["verification_status"],
            created_by=row["created_by"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    def _require_work_product(
        self, connection: sqlite3.Connection, product_id: str
    ) -> WorkProductRecord:
        row = connection.execute(
            "SELECT * FROM orch_work_products WHERE id = ?", (product_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"work product not found: {product_id}")
        return self._work_product_from_row(row)

    @staticmethod
    def _stage_from_row(row: sqlite3.Row) -> StageHistoryRecord:
        return StageHistoryRecord(
            id=row["id"],
            task_id=row["task_id"],
            sequence=int(row["sequence_no"]),
            stage=OrchestrationStage(row["stage"]),
            disposition=StageDisposition(row["disposition"]),
            entered_at=_time(row["entered_at"]),  # type: ignore[arg-type]
            exited_at=_time(row["exited_at"]),
            detail=_load(row["detail_json"], {}),
            command_id=row["command_id"],
        )

    @staticmethod
    def _plan_from_row(row: sqlite3.Row) -> PlanRecord:
        return PlanRecord(
            id=row["id"],
            task_id=row["task_id"],
            revision=int(row["revision"]),
            parent_plan_id=row["parent_plan_id"],
            content_hash=row["content_hash"],
            metadata=_load(row["metadata_json"], {}),
            created_by=row["created_by"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    @staticmethod
    def _node_from_row(row: sqlite3.Row) -> NodeRecord:
        retry = _load(row["retry_policy_json"], {})
        return NodeRecord(
            id=row["id"],
            plan_id=row["plan_id"],
            key=row["node_key"],
            title=row["title"],
            instructions=row["instructions"],
            kind=NodeKind(row["kind"]),
            agent=row["agent"],
            model=row["model"],
            input=_load(row["input_json"], {}),
            join_policy=JoinPolicy(row["join_policy"]),
            failure_policy=FailurePolicy(row["failure_policy"]),
            effect_safety=EffectSafety(row["effect_safety"]),
            retry_policy=RetryPolicy(**retry),
            timeout_seconds=int(row["timeout_seconds"]),
            priority=int(row["priority"]),
            concurrency_key=row["concurrency_key"],
            metadata=_load(row["metadata_json"], {}),
        )

    @staticmethod
    def _edge_from_row(row: sqlite3.Row) -> EdgeRecord:
        return EdgeRecord(
            id=row["id"],
            plan_id=row["plan_id"],
            from_node_id=row["from_node_id"],
            to_node_id=row["to_node_id"],
            from_node=row["from_node_key"],
            to_node=row["to_node_key"],
            condition=EdgeCondition(row["condition"]),
            required=bool(row["required"]),
            metadata=_load(row["metadata_json"], {}),
        )

    def _get_plan_graph(
        self, connection: sqlite3.Connection, plan_id: str
    ) -> PlanGraph:
        plan_row = connection.execute(
            "SELECT * FROM orch_plans WHERE id = ?", (plan_id,)
        ).fetchone()
        if plan_row is None:
            raise NotFoundError(f"plan not found: {plan_id}")
        node_rows = connection.execute(
            "SELECT * FROM orch_nodes WHERE plan_id = ? ORDER BY rowid", (plan_id,)
        ).fetchall()
        edge_rows = connection.execute(
            """
            SELECT e.*, source.node_key AS from_node_key,
                   target.node_key AS to_node_key
            FROM orch_edges e
            JOIN orch_nodes source ON source.id = e.from_node_id
            JOIN orch_nodes target ON target.id = e.to_node_id
            WHERE e.plan_id = ? ORDER BY e.rowid
            """,
            (plan_id,),
        ).fetchall()
        return PlanGraph(
            plan=self._plan_from_row(plan_row),
            nodes=tuple(self._node_from_row(row) for row in node_rows),
            edges=tuple(self._edge_from_row(row) for row in edge_rows),
        )

    def _require_run(
        self, connection: sqlite3.Connection, run_id: str
    ) -> RunRecord:
        row = connection.execute(
            """
            SELECT r.*, n.node_key FROM orch_runs r
            JOIN orch_nodes n ON n.id = r.node_id WHERE r.id = ?
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"run not found: {run_id}")
        return self._run_from_row(row)

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            id=row["id"],
            task_id=row["task_id"],
            plan_id=row["plan_id"],
            node_id=row["node_id"],
            node_key=row["node_key"],
            attempt=int(row["attempt"]),
            status=RunStatus(row["status"]),
            session_id=row["session_id"],
            priority=int(row["priority"]),
            ready_at=_time(row["ready_at"]),  # type: ignore[arg-type]
            fencing_token=int(row["fencing_token"]),
            output=_load(row["output_json"]),
            error_kind=row["error_kind"],
            error_message=row["error_message"],
            version=int(row["version"]),
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
            started_at=_time(row["started_at"]),
            finished_at=_time(row["finished_at"]),
            brief_id=row["brief_id"],
        )

    @staticmethod
    def _run_activity_from_row(row: sqlite3.Row) -> RunActivityRecord:
        return RunActivityRecord(
            sequence=int(row["sequence_no"]),
            id=row["id"],
            task_id=row["task_id"],
            run_id=row["run_id"],
            event_key=row["event_key"],
            source_id=row["source_id"],
            kind=row["kind"],
            status=row["status"],
            title=row["title"],
            summary=row["summary"],
            detail=_load(row["detail_json"], {}),
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    def _require_lease(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        token: str,
        fencing_token: int,
    ) -> LeaseRecord:
        row = connection.execute(
            """
            SELECT * FROM orch_leases
            WHERE run_id = ? AND token = ? AND fencing_token = ?
            """,
            (run_id, token, fencing_token),
        ).fetchone()
        if row is None:
            raise LeaseConflict(f"run {run_id} does not hold the current lease")
        lease = self._lease_from_row(row)
        if lease.expires_at <= _now():
            raise LeaseConflict(f"lease for run {run_id} has expired")
        return lease

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> LeaseRecord:
        return LeaseRecord(
            id=row["id"],
            run_id=row["run_id"],
            owner=row["owner"],
            token=row["token"],
            fencing_token=int(row["fencing_token"]),
            expires_at=_time(row["expires_at"]),  # type: ignore[arg-type]
            heartbeat_at=_time(row["heartbeat_at"]),  # type: ignore[arg-type]
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    def _get_claim(
        self, connection: sqlite3.Connection, run_id: str
    ) -> Optional[RunClaim]:
        run = self._require_run(connection, run_id)
        row = connection.execute(
            "SELECT * FROM orch_leases WHERE run_id = ?", (run_id,)
        ).fetchone()
        return RunClaim(run=run, lease=self._lease_from_row(row)) if row else None

    @staticmethod
    def _claim_from_payload(payload: Mapping[str, Any]) -> RunClaim:
        run = payload["run"]
        lease = payload["lease"]
        return RunClaim(
            run=RunRecord(
                id=run["id"],
                task_id=run["task_id"],
                plan_id=run["plan_id"],
                node_id=run["node_id"],
                node_key=run["node_key"],
                attempt=int(run["attempt"]),
                status=RunStatus(run["status"]),
                session_id=run.get("session_id"),
                priority=int(run["priority"]),
                ready_at=_time(run["ready_at"]),  # type: ignore[arg-type]
                fencing_token=int(run["fencing_token"]),
                output=run.get("output"),
                error_kind=run.get("error_kind"),
                error_message=run.get("error_message"),
                version=int(run["version"]),
                created_at=_time(run["created_at"]),  # type: ignore[arg-type]
                started_at=_time(run.get("started_at")),
                finished_at=_time(run.get("finished_at")),
            ),
            lease=LeaseRecord(
                id=lease["id"],
                run_id=lease["run_id"],
                owner=lease["owner"],
                token=lease["token"],
                fencing_token=int(lease["fencing_token"]),
                expires_at=_time(lease["expires_at"]),  # type: ignore[arg-type]
                heartbeat_at=_time(lease["heartbeat_at"]),  # type: ignore[arg-type]
                created_at=_time(lease["created_at"]),  # type: ignore[arg-type]
            ),
        )

    def _require_gate(
        self, connection: sqlite3.Connection, gate_id: str
    ) -> GateRecord:
        row = connection.execute(
            "SELECT * FROM orch_gates WHERE id = ?", (gate_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"gate not found: {gate_id}")
        return self._gate_from_row(row)

    @staticmethod
    def _gate_from_row(row: sqlite3.Row) -> GateRecord:
        return GateRecord(
            id=row["id"],
            task_id=row["task_id"],
            run_id=row["run_id"],
            node_id=row["node_id"],
            kind=GateKind(row["kind"]),
            status=GateStatus(row["status"]),
            source_key=row["source_key"],
            prompt=_load(row["prompt_json"], {}),
            resolution=_load(row["resolution_json"]),
            resolved_by=row["resolved_by"],
            version=int(row["version"]),
            opened_at=_time(row["opened_at"]),  # type: ignore[arg-type]
            published_at=_time(row["published_at"]),
            resolved_at=_time(row["resolved_at"]),
            expires_at=_time(row["expires_at"]),
        )

    def _require_evidence(
        self, connection: sqlite3.Connection, evidence_id: str
    ) -> EvidenceRecord:
        row = connection.execute(
            "SELECT * FROM orch_evidence WHERE id = ?", (evidence_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"evidence not found: {evidence_id}")
        return self._evidence_from_row(row)

    @staticmethod
    def _evidence_from_row(row: sqlite3.Row) -> EvidenceRecord:
        return EvidenceRecord(
            id=row["id"],
            task_id=row["task_id"],
            plan_id=row["plan_id"],
            node_id=row["node_id"],
            run_id=row["run_id"],
            kind=EvidenceKind(row["kind"]),
            mime_type=row["mime_type"],
            content_hash=row["content_hash"],
            payload=_load(row["payload_json"], {}),
            blob_uri=row["blob_uri"],
            created_by=row["created_by"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> EventRecord:
        return EventRecord(
            sequence=int(row["sequence_no"]),
            id=row["id"],
            task_id=row["task_id"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            event_type=row["event_type"],
            payload=_load(row["payload_json"], {}),
            previous_hash=row["previous_hash"],
            event_hash=row["event_hash"],
            command_id=row["command_id"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    @staticmethod
    def _outbox_from_row(row: sqlite3.Row) -> OutboxRecord:
        return OutboxRecord(
            id=row["id"],
            event_id=row["event_id"],
            topic=row["topic"],
            payload=_load(row["payload_json"], {}),
            available_at=_time(row["available_at"]),  # type: ignore[arg-type]
            attempts=int(row["attempts"]),
            locked_by=row["locked_by"],
            locked_until=_time(row["locked_until"]),
            published_at=_time(row["published_at"]),
            dead_lettered_at=_time(row["dead_lettered_at"]),
            last_error=row["last_error"],
            created_at=_time(row["created_at"]),  # type: ignore[arg-type]
        )

    @staticmethod
    def _outbox_requeue_from_row(row: sqlite3.Row) -> OutboxRequeueRecord:
        return OutboxRequeueRecord(
            id=row["id"],
            outbox_id=row["outbox_id"],
            command_id=row["command_id"],
            actor=row["actor"],
            reason=row["reason"],
            snapshot_attempts=int(row["snapshot_attempts"]),
            snapshot_last_error=row["snapshot_last_error"],
            snapshot_dead_lettered_at=_time(
                row["snapshot_dead_lettered_at"]
            ),  # type: ignore[arg-type]
            requeued_at=_time(row["requeued_at"]),  # type: ignore[arg-type]
        )
