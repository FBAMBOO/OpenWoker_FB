"""Transactional SQLite persistence for the orchestration domain.

The store owns an independent WAL database.  All aggregate mutations use
``BEGIN IMMEDIATE`` so command deduplication, optimistic updates, events, and outbox
records commit together.  Runs additionally require a lease token and monotonically
increasing fencing token, preventing a stale worker from committing after reassignment.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence

from .dag import validate_plan
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
        else:
            target = Path(self.path).expanduser()
            target.parent.mkdir(parents=True, exist_ok=True)
            self.path = str(target)
            connection = self._new_connection()
            try:
                apply_migrations(connection)
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

    # -- task aggregate -----------------------------------------------------
    def create_task(
        self, spec: TaskSpec, *, command_id: Optional[str] = None
    ) -> TaskRecord:
        if not spec.idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        if not spec.objective.strip():
            raise ValueError("objective is required")
        if spec.max_parallel_runs < 1:
            raise ValueError("max_parallel_runs must be >= 1")
        command_id = self._command_id(command_id)
        creation = _jsonable(spec)
        creation_hash = _digest(creation)
        with self._write() as connection:
            replay = self._start_command(
                connection, command_id, "task.create", spec.idempotency_key, creation
            )
            if replay is not None:
                return self._require_task(connection, replay["task_id"])

            existing = connection.execute(
                "SELECT * FROM orch_tasks WHERE idempotency_key = ?",
                (spec.idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["creation_hash"] != creation_hash:
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
            self._finish_command(connection, command_id, {"task_id": task_id})
            return self._require_task(connection, task_id)

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
            if chosen_attempt < 1 or chosen_attempt > int(retry.get("max_attempts", 1)):
                raise ConflictError(
                    f"attempt {chosen_attempt} exceeds node max_attempts "
                    f"{retry.get('max_attempts', 1)}"
                )
            run_id = _id("run")
            now = _now()
            connection.execute(
                """
                INSERT INTO orch_runs(
                    id, task_id, plan_id, node_id, attempt, status, session_id,
                    priority, ready_at, fencing_token, version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?)
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
                ),
            )
            self._append_event(
                connection,
                task_id=task_id,
                aggregate_type="run",
                aggregate_id=run_id,
                event_type="run.queued",
                payload={"node_key": node_key, "attempt": chosen_attempt},
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
                    created_at, finished_at
                ) VALUES (?, ?, ?, ?, 1, 'skipped', ?, ?, 0,
                          'edge_condition', ?, 1, ?, ?)
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
                        created_at, finished_at
                    ) VALUES (?, ?, ?, ?, 1, 'skipped', ?, ?, 0,
                              'failure_policy', ?, 1, ?, ?)
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

    def get_run(self, run_id: str) -> RunRecord:
        with self._read() as connection:
            return self._require_run(connection, run_id)

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
            row = connection.execute(
                """
                SELECT r.*, n.node_key, n.concurrency_key, t.priority AS task_priority,
                       t.max_parallel_runs
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
                LIMIT 1
                """,
                (_stamp(chosen_now),),
            ).fetchone()
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

    def fail_run(
        self,
        run_id: str,
        lease_token: str,
        fencing_token: int,
        *,
        error_kind: str,
        error_message: str,
        status: RunStatus = RunStatus.FAILED,
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
            output=None,
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
        )

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
