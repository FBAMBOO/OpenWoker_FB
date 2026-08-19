"""Transactional root budget ledger with reservations, usage and fencing."""

from __future__ import annotations

import json
import uuid
import sqlite3
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, Mapping

from pydantic import Field, model_validator

from ..errors import ConflictError, NotFoundError
from ..store import OrchestrationStore
from .models import (
    BUDGET_DIMENSIONS,
    BudgetLedger,
    BudgetLimits,
    BudgetMode,
    BudgetProfile,
    BudgetStatus,
    QualityModel,
)
from .state_machine import WorkflowEvent, transition_workflow_in_transaction


class BudgetExceeded(ConflictError):
    pass


class ProviderUsage(QualityModel):
    model_calls: int = Field(default=0, ge=0)
    tool_calls: int = Field(default=0, ge=0)
    input_tokens: int = Field(default=0, ge=0)
    cached_input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    reasoning_tokens: int = Field(default=0, ge=0)
    provider_reported_tokens: int | None = Field(default=None, ge=0)
    provider_reported_includes_cached: bool | None = None
    active_seconds: int = Field(default=0, ge=0)
    tool_payload_bytes: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _cached_is_bounded(self) -> "ProviderUsage":
        if self.cached_input_tokens > self.input_tokens:
            raise ValueError("cached input tokens cannot exceed input tokens")
        return self

    @property
    def reported_tokens(self) -> int:
        if self.provider_reported_tokens is not None:
            return self.provider_reported_tokens
        return self.input_tokens + self.output_tokens + self.reasoning_tokens

    def canonical_amounts(self) -> dict[str, int]:
        return {
            "model_calls": self.model_calls,
            "tool_calls": self.tool_calls,
            "reported_tokens": self.reported_tokens,
            "active_seconds": self.active_seconds,
            "tool_payload_bytes": self.tool_payload_bytes,
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _zeroes() -> dict[str, int]:
    return {name: 0 for name in BUDGET_DIMENSIONS}


def _amounts(value: Mapping[str, Any] | None) -> dict[str, int]:
    raw = dict(value or {})
    unknown = sorted(set(raw).difference(BUDGET_DIMENSIONS))
    if unknown:
        raise ValueError("unknown budget dimensions: " + ", ".join(unknown))
    chosen = _zeroes()
    for name, amount in raw.items():
        number = int(amount)
        if number < 0:
            raise ValueError("budget amounts cannot be negative")
        chosen[name] = number
    return chosen


class BudgetService:
    def __init__(self, store: OrchestrationStore) -> None:
        self.store = store

    @staticmethod
    def _event(
        connection,
        ledger_id: str,
        event_type: str,
        *,
        reservation_id: str | None = None,
        run_id: str | None = None,
        dimension: str | None = None,
        amount: int | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        sequence = connection.execute(
            "SELECT COALESCE(MAX(sequence_no), 0) + 1 AS value FROM orch_budget_events WHERE ledger_id=?",
            (ledger_id,),
        ).fetchone()["value"]
        connection.execute(
            """
            INSERT INTO orch_budget_events(
                id, ledger_id, sequence_no, event_type, reservation_id, run_id,
                dimension, amount, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"budget_event_{uuid.uuid4().hex}", ledger_id, sequence, event_type,
                reservation_id, run_id, dimension, amount, _json(dict(payload or {})), _now(),
            ),
        )

    def _move_workflow_to_budget_attention(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
    ) -> None:
        row = connection.execute(
            "SELECT workflow_status FROM orch_tasks WHERE id=?", (task_id,)
        ).fetchone()
        current = str(row["workflow_status"] if row is not None else "")
        if current == "needs_attention":
            return
        if current in {"running", "validating", "reviewing"}:
            event = WorkflowEvent.ATTENTION_REQUIRED
        elif current == "repairing":
            event = WorkflowEvent.REPAIR_EXHAUSTED
        else:
            raise ConflictError(
                f"hard budget exhaustion is inconsistent with workflow state {current or 'missing'}"
            )
        transition_workflow_in_transaction(
            self.store,
            connection,
            task_id=task_id,
            event=event,
            reason_code="budget_exhausted",
            command_id=f"quality-budget-exhausted:{task_id}",
        )

    def create(
        self,
        *,
        task_id: str,
        strategy_id: str,
        profile: BudgetProfile,
        provider_usage_semantics: Mapping[str, Any] | None = None,
        _connection: sqlite3.Connection | None = None,
    ) -> BudgetLedger:
        limits = profile.limits.model_dump(mode="json")
        reserved = _zeroes()
        consumed = _zeroes()
        now = _now()
        ledger_id = f"budget_{uuid.uuid4().hex}"
        with (self.store._write() if _connection is None else nullcontext(_connection)) as connection:
            strategy = connection.execute(
                "SELECT task_id, status FROM orch_execution_strategies WHERE id=?", (strategy_id,)
            ).fetchone()
            if strategy is None or strategy["task_id"] != task_id or strategy["status"] != "published":
                raise ConflictError("budget ledger requires the task's published strategy")
            existing = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE task_id=? AND status='active'",
                (task_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["strategy_id"] == strategy_id
                    and existing["mode"] == profile.mode.value
                    and json.loads(existing["effective_limits_json"]) == limits
                    and existing["source_profile_id"] == profile.id
                ):
                    return self._record(existing)
                raise ConflictError("task already has a different active budget ledger")
            version = connection.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 AS value FROM orch_budget_ledgers WHERE task_id=?",
                (task_id,),
            ).fetchone()["value"]
            connection.execute(
                """
                INSERT INTO orch_budget_ledgers(
                    id, task_id, strategy_id, mode, source_profile_id,
                    effective_limits_json, reserved_json, consumed_json,
                    provider_usage_semantics_json, over_budget, version,
                    fencing_token, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, 'active', ?, ?)
                """,
                (
                    ledger_id, task_id, strategy_id, profile.mode.value, profile.id,
                    _json(limits), _json(reserved), _json(consumed),
                    _json(dict(provider_usage_semantics or {})), version, now, now,
                ),
            )
            self._event(
                connection, ledger_id, "created",
                payload={"mode": profile.mode.value, "source_profile_id": profile.id, "limits": limits},
            )
            projection = (
                BudgetStatus.UNLIMITED.value
                if profile.mode is BudgetMode.UNLIMITED
                else BudgetStatus.WITHIN_BUDGET.value
            )
            connection.execute(
                """
                UPDATE orch_tasks SET active_budget_ledger_id=?, budget_status=?,
                quality_reason_code=NULL WHERE id=?
                """,
                (ledger_id, projection, task_id),
            )
            row = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (ledger_id,)
            ).fetchone()
        return self._record(row)

    def get(self, ledger_id: str) -> BudgetLedger:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (ledger_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"budget ledger {ledger_id} not found")
        return self._record(row)

    @staticmethod
    def _record(row) -> BudgetLedger:
        limits = json.loads(row["effective_limits_json"])
        reserved = json.loads(row["reserved_json"])
        consumed = json.loads(row["consumed_json"])
        remaining = {
            name: (
                None
                if row["mode"] == BudgetMode.UNLIMITED.value or limits.get(name) is None
                else max(0, int(limits[name]) - int(reserved.get(name, 0)) - int(consumed.get(name, 0)))
            )
            for name in BUDGET_DIMENSIONS
        }
        return BudgetLedger(
            id=row["id"], task_id=row["task_id"], strategy_id=row["strategy_id"],
            mode=row["mode"], source_profile_id=row["source_profile_id"],
            effective_limits=BudgetLimits.model_validate(limits), reserved=reserved,
            consumed=consumed, remaining=remaining,
            provider_usage_semantics=json.loads(row["provider_usage_semantics_json"]),
            over_budget=bool(row["over_budget"]), version=row["version"],
            fencing_token=row["fencing_token"],
        )

    def reserve(
        self,
        ledger_id: str,
        *,
        amounts: Mapping[str, int],
        purpose: str,
        run_id: str | None = None,
        reservation_id: str | None = None,
    ) -> tuple[str, int]:
        requested = _amounts(amounts)
        reservation_id = reservation_id or f"reservation_{uuid.uuid4().hex}"
        exhausted = False
        with self.store._write() as connection:
            replay = connection.execute(
                "SELECT * FROM orch_budget_reservations WHERE id=?", (reservation_id,)
            ).fetchone()
            if replay is not None:
                if (
                    replay["ledger_id"] != ledger_id
                    or json.loads(replay["amounts_json"]) != requested
                    or replay["purpose"] != purpose
                    or replay["run_id"] != run_id
                ):
                    raise ConflictError("reservation id was replayed with different inputs")
                return reservation_id, replay["fencing_token"]
            row = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (ledger_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"budget ledger {ledger_id} not found")
            if row["status"] != "active":
                raise BudgetExceeded("budget ledger is not active")
            limits = json.loads(row["effective_limits_json"])
            reserved = json.loads(row["reserved_json"])
            consumed = json.loads(row["consumed_json"])
            exceeded = [
                name for name in BUDGET_DIMENSIONS
                if row["mode"] != BudgetMode.UNLIMITED.value
                and int(reserved.get(name, 0)) + int(consumed.get(name, 0)) + requested[name]
                > int(limits[name])
            ]
            if exceeded and row["mode"] == BudgetMode.HARD.value:
                exhausted = True
                connection.execute(
                    """
                    UPDATE orch_budget_ledgers SET status='exhausted', over_budget=1,
                    fencing_token=fencing_token+1, updated_at=? WHERE id=?
                    """,
                    (_now(), ledger_id),
                )
                self._event(
                    connection, ledger_id, "exhausted", run_id=run_id,
                    payload={"phase": "reservation", "dimensions": exceeded, "requested": requested},
                )
                self._move_workflow_to_budget_attention(
                    connection, task_id=str(row["task_id"])
                )
                connection.execute(
                    """
                    UPDATE orch_tasks SET budget_status='exhausted',
                    quality_reason_code='budget_exhausted' WHERE id=?
                    """,
                    (row["task_id"],),
                )
            else:
                updated = {name: int(reserved.get(name, 0)) + requested[name] for name in BUDGET_DIMENSIONS}
                token = int(row["fencing_token"]) + 1
                connection.execute(
                    """
                    UPDATE orch_budget_ledgers SET reserved_json=?, fencing_token=?, updated_at=?
                    WHERE id=?
                    """,
                    (_json(updated), token, _now(), ledger_id),
                )
                connection.execute(
                    """
                    INSERT INTO orch_budget_reservations(
                        id, ledger_id, run_id, purpose, amounts_json, consumed_json,
                        status, fencing_token, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)
                    """,
                    (
                        reservation_id, ledger_id, run_id, purpose, _json(requested),
                        _json(_zeroes()), token, _now(),
                    ),
                )
                self._event(
                    connection, ledger_id, "reserved", reservation_id=reservation_id,
                    run_id=run_id, payload={"amounts": requested, "purpose": purpose},
                )
        if exhausted:
            raise BudgetExceeded("hard budget reservation would exceed the root ledger")
        return reservation_id, token

    def consume(
        self,
        reservation_id: str,
        *,
        usage: ProviderUsage,
        fencing_token: int,
        final: bool = True,
    ) -> BudgetLedger:
        actual = usage.canonical_amounts()
        with self.store._write() as connection:
            reservation = connection.execute(
                "SELECT * FROM orch_budget_reservations WHERE id=?", (reservation_id,)
            ).fetchone()
            if reservation is None:
                raise NotFoundError(f"budget reservation {reservation_id} not found")
            if reservation["status"] == "consumed":
                if json.loads(reservation["consumed_json"]) != actual:
                    raise ConflictError("consumed reservation was replayed with different usage")
                row = connection.execute(
                    "SELECT * FROM orch_budget_ledgers WHERE id=?", (reservation["ledger_id"],)
                ).fetchone()
                return self._record(row)
            if reservation["status"] != "active" or reservation["fencing_token"] != fencing_token:
                raise ConflictError("stale or inactive budget reservation fencing token")
            row = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (reservation["ledger_id"],)
            ).fetchone()
            limits = json.loads(row["effective_limits_json"])
            root_reserved = json.loads(row["reserved_json"])
            root_consumed = json.loads(row["consumed_json"])
            reserved_amounts = json.loads(reservation["amounts_json"])
            release = reserved_amounts if final else {
                name: min(int(reserved_amounts[name]), actual[name]) for name in BUDGET_DIMENSIONS
            }
            new_reserved = {
                name: max(0, int(root_reserved.get(name, 0)) - int(release[name]))
                for name in BUDGET_DIMENSIONS
            }
            new_consumed = {
                name: int(root_consumed.get(name, 0)) + actual[name]
                for name in BUDGET_DIMENSIONS
            }
            exceeded = [
                name for name in BUDGET_DIMENSIONS
                if row["mode"] != BudgetMode.UNLIMITED.value
                and new_consumed[name] + new_reserved[name] > int(limits[name])
            ]
            mode = BudgetMode(row["mode"])
            status = "exhausted" if exceeded and mode is BudgetMode.HARD else row["status"]
            over_budget = bool(row["over_budget"] or exceeded)
            token = int(row["fencing_token"]) + 1
            connection.execute(
                """
                UPDATE orch_budget_ledgers SET reserved_json=?, consumed_json=?,
                over_budget=?, status=?, fencing_token=?, updated_at=? WHERE id=?
                """,
                (
                    _json(new_reserved), _json(new_consumed), int(over_budget), status,
                    token, _now(), row["id"],
                ),
            )
            connection.execute(
                """
                UPDATE orch_budget_reservations SET consumed_json=?, status=?,
                fencing_token=?, released_at=? WHERE id=?
                """,
                (
                    _json(actual), "consumed" if final else "active", token,
                    _now() if final else None, reservation_id,
                ),
            )
            self._event(
                connection, row["id"], "consumed", reservation_id=reservation_id,
                run_id=reservation["run_id"], payload={
                    "canonical_amounts": actual,
                    "provider_usage": usage.model_dump(mode="json"),
                    "reported_tokens": usage.reported_tokens,
                },
            )
            self._thresholds(connection, row, limits, new_consumed)
            if exceeded and mode is BudgetMode.HARD:
                self._event(
                    connection, row["id"], "exhausted", reservation_id=reservation_id,
                    run_id=reservation["run_id"], payload={"phase": "consume", "dimensions": exceeded},
                )
                self._move_workflow_to_budget_attention(
                    connection, task_id=str(row["task_id"])
                )
                connection.execute(
                    """
                    UPDATE orch_tasks SET budget_status='exhausted',
                    quality_reason_code='budget_exhausted' WHERE id=?
                    """,
                    (row["task_id"],),
                )
            elif exceeded and mode is BudgetMode.SOFT:
                connection.execute(
                    "UPDATE orch_tasks SET budget_status='over_budget', quality_reason_code='soft_budget_overrun' WHERE id=?",
                    (row["task_id"],),
                )
            elif mode is BudgetMode.UNLIMITED:
                connection.execute(
                    "UPDATE orch_tasks SET budget_status='unlimited' WHERE id=?", (row["task_id"],)
                )
            updated = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (row["id"],)
            ).fetchone()
        return self._record(updated)

    def _thresholds(self, connection, row, limits: Mapping[str, Any], consumed: Mapping[str, int]) -> None:
        if row["mode"] == BudgetMode.UNLIMITED.value:
            return
        ratio = max(
            (consumed[name] / int(limits[name]) if int(limits[name]) else (1.0 if consumed[name] else 0.0))
            for name in BUDGET_DIMENSIONS
        )
        for threshold in (0.8, 0.95):
            marker = f"threshold:{threshold:.2f}"
            exists = connection.execute(
                """
                SELECT 1 FROM orch_budget_events
                WHERE ledger_id=? AND event_type='threshold' AND payload_json LIKE ?
                """,
                (row["id"], f'%"marker":"{marker}"%'),
            ).fetchone()
            if ratio >= threshold and exists is None:
                self._event(
                    connection, row["id"], "threshold",
                    payload={"marker": marker, "threshold": threshold, "utilization": ratio},
                )
        if ratio >= 0.8 and row["mode"] == BudgetMode.HARD.value:
            connection.execute(
                "UPDATE orch_tasks SET budget_status='warning' WHERE id=? AND budget_status='within_budget'",
                (row["task_id"],),
            )

    def release(self, reservation_id: str, *, fencing_token: int) -> BudgetLedger:
        with self.store._write() as connection:
            reservation = connection.execute(
                "SELECT * FROM orch_budget_reservations WHERE id=?", (reservation_id,)
            ).fetchone()
            if reservation is None:
                raise NotFoundError(f"budget reservation {reservation_id} not found")
            row = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (reservation["ledger_id"],)
            ).fetchone()
            if reservation["status"] == "released":
                return self._record(row)
            if reservation["status"] != "active" or reservation["fencing_token"] != fencing_token:
                raise ConflictError("stale or inactive budget reservation fencing token")
            amounts = json.loads(reservation["amounts_json"])
            consumed = json.loads(reservation["consumed_json"])
            outstanding = {
                name: max(0, int(amounts[name]) - int(consumed.get(name, 0)))
                for name in BUDGET_DIMENSIONS
            }
            reserved = json.loads(row["reserved_json"])
            reserved = {
                name: max(0, int(reserved.get(name, 0)) - outstanding[name])
                for name in BUDGET_DIMENSIONS
            }
            token = int(row["fencing_token"]) + 1
            connection.execute(
                "UPDATE orch_budget_ledgers SET reserved_json=?, fencing_token=?, updated_at=? WHERE id=?",
                (_json(reserved), token, _now(), row["id"]),
            )
            connection.execute(
                "UPDATE orch_budget_reservations SET status='released', fencing_token=?, released_at=? WHERE id=?",
                (token, _now(), reservation_id),
            )
            self._event(
                connection, row["id"], "released", reservation_id=reservation_id,
                run_id=reservation["run_id"], payload={"released": outstanding},
            )
            updated = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (row["id"],)
            ).fetchone()
        return self._record(updated)

    def extend(
        self,
        ledger_id: str,
        *,
        effective_limits: Mapping[str, int],
        actor_id: str,
        reason: str,
    ) -> BudgetLedger:
        """Create a new finite ledger revision while preserving prior usage."""

        actor = str(actor_id).strip()
        explanation = str(reason).strip()
        if not actor or not explanation:
            raise ValueError("budget extension requires actor_id and reason")
        if set(effective_limits) != set(BUDGET_DIMENSIONS):
            raise ValueError(
                "budget extension must provide every canonical dimension"
            )
        chosen = _amounts(effective_limits)
        with self.store._write() as connection:
            row = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (ledger_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"budget ledger {ledger_id} not found")
            if row["mode"] == BudgetMode.UNLIMITED.value:
                raise ConflictError("an unlimited budget has no business limits to extend")
            if row["status"] not in {"active", "exhausted"}:
                raise ConflictError("only an active or exhausted budget may be extended")
            task = connection.execute(
                "SELECT active_budget_ledger_id FROM orch_tasks WHERE id=?",
                (row["task_id"],),
            ).fetchone()
            if task is None or task["active_budget_ledger_id"] != ledger_id:
                raise ConflictError("only the task's active budget revision may be extended")
            previous = json.loads(row["effective_limits_json"])
            consumed = json.loads(row["consumed_json"])
            reserved = json.loads(row["reserved_json"])
            if any(chosen[name] < int(previous[name]) for name in BUDGET_DIMENSIONS):
                raise ValueError("budget extension cannot reduce an existing limit")
            if not any(chosen[name] > int(previous[name]) for name in BUDGET_DIMENSIONS):
                raise ValueError("budget extension must increase at least one limit")
            if any(chosen[name] < int(consumed.get(name, 0)) for name in BUDGET_DIMENSIONS):
                raise ValueError("extended limits cannot remain below historical consumption")
            ratio = max(
                (
                    int(consumed.get(name, 0)) / chosen[name]
                    if chosen[name]
                    else (1.0 if int(consumed.get(name, 0)) else 0.0)
                )
                for name in BUDGET_DIMENSIONS
            )
            projection = (
                BudgetStatus.WARNING.value
                if row["mode"] == BudgetMode.HARD.value and ratio >= 0.8
                else BudgetStatus.WITHIN_BUDGET.value
            )
            token = int(row["fencing_token"]) + 1
            now = _now()
            new_ledger_id = f"budget_{uuid.uuid4().hex}"
            version = int(
                connection.execute(
                    "SELECT COALESCE(MAX(version), 0) + 1 AS value FROM orch_budget_ledgers WHERE task_id=?",
                    (row["task_id"],),
                ).fetchone()["value"]
            )
            active_reservations = connection.execute(
                "SELECT id FROM orch_budget_reservations WHERE ledger_id=? AND status='active'",
                (ledger_id,),
            ).fetchall()
            # A resumed attempt must reserve again.  Canceling outstanding
            # reservations fences stale workers and prevents carried reservation
            # amounts from being counted twice in the new revision.
            connection.execute(
                """
                UPDATE orch_budget_reservations
                SET status='canceled', fencing_token=?, released_at=?
                WHERE ledger_id=? AND status='active'
                """,
                (token, now, ledger_id),
            )
            new_reserved = _zeroes()
            self._event(
                connection,
                ledger_id,
                "extended",
                payload={
                    "actor_id": actor,
                    "reason": explanation,
                    "previous_limits": previous,
                    "effective_limits": chosen,
                    "utilization": ratio,
                    "new_ledger_id": new_ledger_id,
                    "new_version": version,
                    "canceled_reservation_ids": [item["id"] for item in active_reservations],
                },
            )
            connection.execute(
                """
                UPDATE orch_budget_ledgers
                SET status='superseded', fencing_token=?, updated_at=? WHERE id=?
                """,
                (token, now, ledger_id),
            )
            connection.execute(
                """
                INSERT INTO orch_budget_ledgers(
                    id, task_id, strategy_id, mode, source_profile_id,
                    effective_limits_json, reserved_json, consumed_json,
                    provider_usage_semantics_json, over_budget, version,
                    fencing_token, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'active', ?, ?)
                """,
                (
                    new_ledger_id,
                    row["task_id"],
                    row["strategy_id"],
                    row["mode"],
                    row["source_profile_id"],
                    _json(chosen),
                    _json(new_reserved),
                    _json(consumed),
                    row["provider_usage_semantics_json"],
                    version,
                    token,
                    now,
                    now,
                ),
            )
            self._event(
                connection,
                new_ledger_id,
                "created",
                payload={
                    "mode": row["mode"],
                    "source_profile_id": row["source_profile_id"],
                    "limits": chosen,
                    "carried_consumption": consumed,
                },
            )
            self._event(
                connection,
                new_ledger_id,
                "recovered",
                payload={
                    "actor_id": actor,
                    "reason": explanation,
                    "previous_ledger_id": ledger_id,
                    "previous_version": int(row["version"]),
                    "canceled_reservation_count": len(active_reservations),
                },
            )
            connection.execute(
                """
                UPDATE orch_tasks SET active_budget_ledger_id=?, budget_status=?,
                    quality_reason_code=NULL
                WHERE id=?
                """,
                (new_ledger_id, projection, row["task_id"]),
            )
            updated = connection.execute(
                "SELECT * FROM orch_budget_ledgers WHERE id=?", (new_ledger_id,)
            ).fetchone()
        return self._record(updated)

    def usage_breakdown(self, ledger_id: str) -> dict[str, int | bool | None]:
        totals: dict[str, int | bool | None] = {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
            "provider_reported_tokens": 0,
            "provider_reported_includes_cached": None,
        }
        with self.store._read() as connection:
            ledger = connection.execute(
                "SELECT task_id, version FROM orch_budget_ledgers WHERE id=?",
                (ledger_id,),
            ).fetchone()
            if ledger is None:
                raise NotFoundError(f"budget ledger {ledger_id} not found")
            rows = connection.execute(
                """
                SELECT e.payload_json FROM orch_budget_events e
                JOIN orch_budget_ledgers l ON l.id=e.ledger_id
                WHERE l.task_id=? AND l.version<=? AND e.event_type='consumed'
                ORDER BY l.version, e.sequence_no
                """,
                (ledger["task_id"], ledger["version"]),
            ).fetchall()
        for row in rows:
            usage = json.loads(row["payload_json"]).get("provider_usage") or {}
            for name in (
                "input_tokens", "cached_input_tokens", "output_tokens", "reasoning_tokens",
            ):
                totals[name] = int(totals[name] or 0) + int(usage.get(name) or 0)
            totals["provider_reported_tokens"] = int(totals["provider_reported_tokens"] or 0) + int(
                usage.get("provider_reported_tokens")
                if usage.get("provider_reported_tokens") is not None
                else (usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0) + (usage.get("reasoning_tokens") or 0)
            )
            if usage.get("provider_reported_includes_cached") is not None:
                totals["provider_reported_includes_cached"] = bool(
                    usage["provider_reported_includes_cached"]
                )
        return totals
