"""Application facade for Task Quality V2 draft, read and export APIs."""

from __future__ import annotations

import base64
import binascii
import hashlib
import io
import json
import uuid
import zipfile
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from ..errors import ConflictError, NotFoundError
from ..handoff_models import contains_secret_like
from ..models import TaskBriefDraft, TaskStatus
from .artifact_security import digest_bytes, preview_policy
from .models import (
    ArtifactVersionStatus,
    BudgetProfile,
    ContractStatus,
    Finding,
    TaskContractV2,
    WaiverSubjectType,
    WorkflowStatus,
    content_sha256,
)
from .plan_compiler import compile_strategy_plan
from .repair import RepairCoordinator
from .repository_resolver import TargetResolution
from .state_machine import (
    WorkflowEvent,
    apply_workflow_event,
    transition_workflow_in_transaction,
)


def _now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def _hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _dump(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    return value


def _cursor_token(
    *, stream: str, task_id: str, rowid: int, scope_hash: str = ""
) -> str:
    payload = _json(
        {
            "v": 1,
            "stream": stream,
            "task_id": task_id,
            "after": int(rowid),
            "scope_hash": scope_hash,
        }
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _cursor_payload(
    cursor: str | None,
    *,
    stream: str,
    task_id: str,
    scope_hash: str | None = "",
) -> dict[str, Any] | None:
    if cursor is None:
        return None
    try:
        padding = "=" * (-len(cursor) % 4)
        value = json.loads(base64.urlsafe_b64decode(cursor + padding))
        if (
            not isinstance(value, dict)
            or value.get("v") != 1
            or value.get("stream") != stream
            or value.get("task_id") != task_id
            or (
                scope_hash is not None
                and value.get("scope_hash", "") != scope_hash
            )
            or not isinstance(value.get("after"), int)
            or value["after"] < 0
        ):
            raise ValueError
        return value
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error) as exc:
        raise ValueError("invalid or out-of-scope pagination cursor") from exc


def _cursor_after(
    cursor: str | None, *, stream: str, task_id: str, scope_hash: str = ""
) -> int | None:
    value = _cursor_payload(
        cursor, stream=stream, task_id=task_id, scope_hash=scope_hash
    )
    return int(value["after"]) if value is not None else None


def _page(
    items: list[dict[str, Any]],
    *,
    offset: int,
    limit: int,
    task_id: str | None = None,
    stream: str | None = None,
    cursor: str | None = None,
    scope_hash: str = "",
) -> dict[str, Any]:
    has_more = len(items) > limit
    visible = [dict(item) for item in items[:limit]]
    next_cursor = None
    if task_id is not None and stream is not None and has_more and visible:
        next_cursor = _cursor_token(
            stream=stream,
            task_id=task_id,
            rowid=int(visible[-1]["_cursor_rowid"]),
            scope_hash=scope_hash,
        )
    for item in visible:
        item.pop("_cursor_rowid", None)
    return {
        "items": visible,
        "offset": offset,
        "limit": limit,
        "has_more": has_more,
        "next_offset": offset + limit if has_more and cursor is None else None,
        "cursor": cursor,
        "next_cursor": next_cursor,
        "pagination": "cursor" if cursor is not None else "offset",
    }


class TaskQualityFacade:
    """Keep the additive V2 workflow cohesive without duplicating legacy services."""

    def __init__(self, service: Any) -> None:
        self.service = service
        self.store = service.store
        self.artifacts = service.quality_artifacts
        self.contracts = service.quality_contracts
        self.resolver = service.quality_repository_resolver
        self.snapshots = service.quality_snapshots
        self.strategies = service.quality_strategies
        self.budgets = service.quality_budgets
        self.repairs = RepairCoordinator(self.store, self.artifacts)

    # -- draft workflow -------------------------------------------------
    def create_draft(
        self, payload: Mapping[str, Any], *, idempotency_key: str
    ) -> dict[str, Any]:
        key = str(idempotency_key).strip()
        if not key:
            raise ValueError("Idempotency-Key is required for task draft creation")
        if contains_secret_like(dict(payload)):
            raise ValueError(
                "task draft contains high-confidence secret material; use the runtime secret mechanism"
            )
        request = dict(payload)
        request["idempotency_key"] = key
        request["auto_start"] = False
        request["publish_brief"] = False
        request["input"] = {
            **dict(request.get("input") or {}),
            "task_quality_v2": True,
            "draft_request_hash": _hash(dict(payload)),
        }
        detail = self.service.create_task(request)
        task_id = str(detail["id"])
        task = self.store.get_task(task_id)
        return {
            "task_id": task_id,
            "id": task_id,
            "workflow_status": "draft",
            "prompt_hash": "sha256:"
            + hashlib.sha256(task.objective.encode("utf-8")).hexdigest(),
            "created_at": task.created_at.isoformat().replace("+00:00", "Z"),
        }

    def analyze(
        self,
        task_id: str,
        payload: Mapping[str, Any],
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        key = str(idempotency_key).strip()
        if not key:
            raise ValueError("Idempotency-Key is required for goal analysis")
        task = self.store.get_task(task_id)
        if task.status is not TaskStatus.DRAFT:
            raise ConflictError("only a draft task may be analyzed")
        request_value = {
            "objective": task.objective,
            "workspace": task.workspace,
            "payload": dict(payload),
        }
        request_hash = _hash(request_value)
        with self.store._read() as connection:
            keyed = connection.execute(
                """
                SELECT request_hash FROM orch_task_draft_analyses
                WHERE task_id=? AND idempotency_key=?
                """,
                (task_id, key),
            ).fetchone()
            replay = connection.execute(
                """
                SELECT * FROM orch_task_draft_analyses
                WHERE task_id=? AND request_hash=?
                """,
                (task_id, request_hash),
            ).fetchone()
        if keyed is not None and keyed["request_hash"] != request_hash:
            raise ConflictError(
                "analysis Idempotency-Key was reused with a different request body"
            )
        if replay is not None:
            return self._analysis_payload(replay, cache_hit=True)
        if not task.workspace:
            raise ValueError("a repository-analysis draft requires an existing workspace")
        apply_workflow_event(
            self.store,
            task_id=task_id,
            event=WorkflowEvent.ANALYSIS_REQUESTED,
            command_id=f"quality-analysis:{key}",
        )
        try:
            result = self.service.quality_contract_compiler.compile(
                task_id=task_id,
                objective=task.objective,
                title=str(payload.get("title") or task.title),
                language=str(payload.get("language") or "zh-CN"),
                explicit_permissions={
                    "source_workspace_write": not bool(
                        task.policy.get("read_only", True)
                    ),
                    "task_artifact_write": True,
                    "external_write": bool(task.policy.get("external_writes", False)),
                    "network_access": bool(task.policy.get("network", False)),
                },
                user_criteria=tuple(
                    str(item)
                    for item in (
                        payload.get("acceptance_criteria") or task.acceptance_criteria
                    )
                    if str(item).strip()
                ),
                quality_profile_id=(
                    str(payload["quality_profile_id"])
                    if payload.get("quality_profile_id")
                    else None
                ),
            )
            contract = self.contracts.save_draft(result.contract)
            resolution = self.resolver.resolve(task.workspace, objective=task.objective)
        except Exception:
            apply_workflow_event(
                self.store,
                task_id=task_id,
                event=WorkflowEvent.ANALYSIS_FAILED,
                reason_code="analysis_failed",
                command_id=f"quality-analysis-failed:{key}",
            )
            raise
        status = str(resolution.status)
        analysis_id = f"analysis_{uuid.uuid4().hex}"
        created_at = _now()
        with self.store._write() as connection:
            connection.execute(
                """
                INSERT INTO orch_task_draft_analyses(
                    id, task_id, idempotency_key, request_hash, contract_id,
                    target_resolution_json, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    analysis_id,
                    task_id,
                    key,
                    request_hash,
                    contract.id,
                    _json(resolution.model_dump(mode="json")),
                    status,
                    created_at,
                ),
            )
            if status == "needs_target_selection":
                transition_workflow_in_transaction(
                    self.store,
                    connection,
                    task_id=task_id,
                    event=WorkflowEvent.TARGET_AMBIGUOUS,
                    reason_code="target_ambiguous",
                    command_id=f"quality-analysis-target:{analysis_id}",
                )
            row = connection.execute(
                "SELECT * FROM orch_task_draft_analyses WHERE id=?", (analysis_id,)
            ).fetchone()
        response = self._analysis_payload(row, cache_hit=result.cache_hit)
        response["contract_issues"] = [item.as_dict() for item in result.issues]
        response["contract_conflicts"] = [item.as_dict() for item in result.conflicts]
        response["start_allowed"] = result.start_allowed and status == "resolved"
        return response

    def analysis(self, task_id: str) -> dict[str, Any]:
        self.store.get_task(task_id)
        with self.store._read() as connection:
            row = connection.execute(
                """
                SELECT * FROM orch_task_draft_analyses
                WHERE task_id=? ORDER BY created_at DESC LIMIT 1
                """,
                (task_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"task draft {task_id} has no analysis")
        return self._analysis_payload(row, cache_hit=True)

    def _analysis_payload(self, row: Any, *, cache_hit: bool) -> dict[str, Any]:
        contract = self.contracts.get(str(row["contract_id"]))
        return {
            "id": row["id"],
            "task_id": row["task_id"],
            "request_hash": row["request_hash"],
            "status": row["status"],
            "contract": _dump(contract),
            "contract_etag": contract.content_hash,
            "target_resolution": json.loads(row["target_resolution_json"]),
            "cache_hit": cache_hit,
            "created_at": row["created_at"],
        }

    def update_contract(
        self, task_id: str, payload: Mapping[str, Any], *, if_match: str
    ) -> TaskContractV2:
        current = self.contracts.active_for_task(task_id, include_draft=True)
        if current.status is not ContractStatus.DRAFT:
            raise ConflictError("published contracts are immutable")
        value = current.model_dump(mode="json")
        value.update(dict(payload))
        value.update(
            {
                "id": current.id,
                "task_id": task_id,
                "version": current.version,
                "status": ContractStatus.DRAFT.value,
                "schema_id": "task_contract_v2",
                "schema_version": 2,
                "content_hash": "sha256:" + "0" * 64,
            }
        )
        draft = TaskContractV2.model_validate(value)
        draft = draft.model_copy(update={"content_hash": draft.computed_content_hash()})
        return self.contracts.update_draft(draft, if_match=if_match)

    def resolve_target(
        self, task_id: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        if not task.workspace:
            raise ValueError("target resolution requires a workspace")
        analysis = self.analysis(task_id)
        objective = str(payload.get("objective") or task.objective)
        resolution = self.resolver.resolve(task.workspace, objective=objective)
        status = str(resolution.status)
        with self.store._write() as connection:
            connection.execute(
                """
                UPDATE orch_task_draft_analyses
                SET target_resolution_json=?, status=? WHERE id=?
                """,
                (
                    _json(resolution.model_dump(mode="json")),
                    status,
                    analysis["id"],
                ),
            )
            workflow_row = connection.execute(
                "SELECT workflow_status FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
            workflow = str(workflow_row["workflow_status"] if workflow_row else "")
            if status == "needs_target_selection" and workflow == "analyzing":
                transition_workflow_in_transaction(
                    self.store,
                    connection,
                    task_id=task_id,
                    event=WorkflowEvent.TARGET_AMBIGUOUS,
                    reason_code="target_ambiguous",
                    command_id=f"quality-target:{analysis['id']}",
                )
            elif status != "needs_target_selection" and workflow == "needs_target_selection":
                transition_workflow_in_transaction(
                    self.store,
                    connection,
                    task_id=task_id,
                    event=WorkflowEvent.TARGET_SELECTED,
                    clear_reason=True,
                    command_id=f"quality-target:{analysis['id']}",
                )
        return resolution.model_dump(mode="json")

    def freeze_snapshot(
        self, task_id: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        with self.store._read() as connection:
            task = connection.execute(
                "SELECT active_snapshot_id FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
        if task is None:
            raise NotFoundError(f"task {task_id} not found")
        if task["active_snapshot_id"]:
            return _dump(self.snapshots.get(str(task["active_snapshot_id"])))
        analysis = self.analysis(task_id)
        resolution = TargetResolution.model_validate(analysis["target_resolution"])
        snapshot = self.snapshots.freeze(
            task_id=task_id,
            resolution=resolution,
            candidate_id=(
                str(payload["candidate_id"]) if payload.get("candidate_id") else None
            ),
            selected_ref=(
                str(payload["selected_ref"]) if payload.get("selected_ref") else None
            ),
            snapshot_kind=payload.get("snapshot_kind"),
        )
        return _dump(snapshot)

    def publish_contract(self, task_id: str, *, if_match: str) -> dict[str, Any]:
        contract = self.contracts.active_for_task(task_id, include_draft=True)
        return _dump(self.contracts.publish(contract.id, if_match=if_match))

    def generate_strategy(
        self, task_id: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        with self.store._read() as connection:
            task = connection.execute(
                """
                SELECT active_contract_id, active_snapshot_id, active_strategy_id
                FROM orch_tasks WHERE id=?
                """,
                (task_id,),
            ).fetchone()
        if task is None:
            raise NotFoundError(f"task {task_id} not found")
        if task["active_strategy_id"]:
            return _dump(self.strategies.get(str(task["active_strategy_id"])))
        if not task["active_contract_id"] or not task["active_snapshot_id"]:
            raise ConflictError("publish the contract and freeze a snapshot first")
        contract = self.contracts.get(str(task["active_contract_id"]))
        snapshot = self.snapshots.get(str(task["active_snapshot_id"]))
        strategy = self.strategies.select(
            contract=contract,
            snapshot=snapshot,
            explicit_policy=dict(payload.get("effective_policy") or {}),
            feature_flags=dict(payload.get("feature_flags") or {}),
        )
        return _dump(self.strategies.publish(strategy))

    def start(self, task_id: str) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        with self.store._read() as connection:
            quality = connection.execute(
                """
                SELECT active_contract_id, active_snapshot_id, active_strategy_id,
                       active_budget_ledger_id, workflow_status
                FROM orch_tasks WHERE id=?
                """,
                (task_id,),
            ).fetchone()
        if quality is None or not quality["active_strategy_id"]:
            raise ConflictError("generate an admitted strategy before starting")
        if quality["workflow_status"] == "running":
            self.service.wake()
            return self.service.task_detail(task_id)
        if quality["workflow_status"] != "ready":
            # The public entrypoint commits the required invalid_transition audit
            # before surfacing the conflict.
            apply_workflow_event(
                self.store,
                task_id=task_id,
                event=WorkflowEvent.START_REQUESTED,
                command_id=f"quality-workflow-start-rejected:{task_id}",
            )
        strategy = self.strategies.get(str(quality["active_strategy_id"]))
        raw_profile = dict(strategy.budget_profile)
        raw_profile.pop("source", None)
        profile = BudgetProfile.model_validate(raw_profile)
        plan = compile_strategy_plan(strategy)

        # Budget ledger, immutable plan, queued status, initial wake, and V2
        # workflow projection share one SQLite commit. Any exception (including a
        # process-level crash injected between these calls in tests) rolls the
        # entire start intent back to the pre-start draft.
        with self.store._write() as connection:
            bound = connection.execute(
                """
                SELECT active_contract_id, active_snapshot_id, active_strategy_id,
                       active_budget_ledger_id
                FROM orch_tasks WHERE id=?
                """,
                (task_id,),
            ).fetchone()
            if bound is None:
                raise NotFoundError(f"task {task_id} not found")
            if not all(
                bound[name]
                for name in (
                    "active_contract_id",
                    "active_snapshot_id",
                    "active_strategy_id",
                )
            ):
                raise ConflictError(
                    "start requires published contract, snapshot, and strategy"
                )
            if str(bound["active_strategy_id"]) != strategy.id:
                raise ConflictError("active strategy changed before start")
            current = self.store._require_task(connection, task_id)
            if not current.active_brief_id:
                contract = self.contracts.get(str(bound["active_contract_id"]))
                brief_draft = self._brief_from_contract(contract)
                brief = self.store.create_brief_draft(
                    task_id,
                    brief_draft,
                    command_id=f"quality-brief:{contract.id}:{contract.content_hash}",
                    _connection=connection,
                )
                self.store.publish_brief(
                    task_id,
                    brief.revision,
                    expected_previous_revision=0,
                    command_id=(
                        f"quality-brief-publish:{contract.id}:{contract.content_hash}"
                    ),
                    _connection=connection,
                )
            if not bound["active_budget_ledger_id"]:
                self.budgets.create(
                    task_id=task_id,
                    strategy_id=strategy.id,
                    profile=profile,
                    provider_usage_semantics={
                        "reported_tokens": "provider_total_when_available",
                        "cached_input_tokens": "reported_separately",
                    },
                    _connection=connection,
                )
            current = self.store._require_task(connection, task_id)
            if not current.active_plan_id:
                self.store.create_plan_revision(
                    task_id,
                    plan,
                    expected_task_version=current.version,
                    created_by="task-quality-v2",
                    command_id=f"quality-plan:{strategy.id}:{strategy.content_hash}",
                    _connection=connection,
                )
                current = self.store._require_task(connection, task_id)
            if current.status is TaskStatus.DRAFT:
                self.store.transition_task_status(
                    task_id,
                    TaskStatus.QUEUED,
                    expected_version=current.version,
                    command_id=f"quality-start:{task_id}:{strategy.content_hash}",
                    _connection=connection,
                )
            elif current.status not in {TaskStatus.QUEUED, TaskStatus.RUNNING}:
                raise ConflictError(
                    f"cannot start a task in {current.status.value} state"
                )
            transition_workflow_in_transaction(
                self.store,
                connection,
                task_id=task_id,
                event=WorkflowEvent.START_REQUESTED,
                clear_reason=True,
                command_id=f"quality-workflow-start:{task_id}:{strategy.content_hash}",
            )
        self.service.wake()
        return self.service.task_detail(task_id)

    @staticmethod
    def _brief_from_contract(contract: TaskContractV2) -> TaskBriefDraft:
        """Create the exact legacy scheduler bridge from the published V2 contract."""

        scope = contract.scope.model_dump(mode="json")
        if scope.get("whole_task"):
            scope["reason"] = "Bounded by the immutable Task Quality V2 contract."
        primary = next(item for item in contract.deliverables if item.primary)
        return TaskBriefDraft(
            title=contract.title[:200],
            objective=contract.objective,
            background=contract.background,
            scope=scope,
            instructions=contract.instructions or (contract.objective,),
            constraints=tuple(item.text for item in contract.constraints),
            non_goals=contract.non_goals,
            acceptance_criteria=tuple(
                {
                    "id": item.id,
                    "text": item.text,
                    "required": item.required,
                    "verification": item.verification_method.value,
                    "hard_gate": item.hard_gate,
                }
                for item in contract.requirements
            ),
            deliverables=tuple(
                {
                    "id": item.id,
                    "kind": item.kind,
                    "title": item.filename,
                    "required": item.required,
                    "primary": item.primary,
                    "mime_type": item.mime_type,
                }
                for item in contract.deliverables
            ),
            result_contract={
                "schema_id": primary.result_schema_id,
                "task_contract_id": contract.id,
                "task_contract_hash": contract.content_hash,
            },
        )

    # -- task read model ------------------------------------------------
    def task_projection(self, task_id: str) -> dict[str, Any]:
        with self.store._read() as connection:
            row = connection.execute(
                """
                SELECT id, status, workflow_status, workflow_resume_status,
                       quality_status, artifact_status,
                       budget_status, quality_reason_code, active_contract_id,
                       active_snapshot_id, active_strategy_id,
                       active_budget_ledger_id, primary_artifact_id
                FROM orch_tasks WHERE id=?
                """,
                (task_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"task {task_id} not found")
            is_v2 = bool(row["active_contract_id"])
            primary = (
                connection.execute(
                    "SELECT * FROM orch_artifact_versions WHERE id=?",
                    (row["primary_artifact_id"],),
                ).fetchone()
                if row["primary_artifact_id"]
                else None
            )
            evaluation = (
                connection.execute(
                    """
                    SELECT * FROM orch_quality_evaluations
                    WHERE task_id=? AND evaluation_type='final'
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (task_id,),
                ).fetchone()
                if is_v2
                else None
            )
            score = (
                connection.execute(
                    "SELECT total FROM orch_rubric_scores WHERE id=?",
                    (evaluation["rubric_score_id"],),
                ).fetchone()
                if evaluation is not None and evaluation["rubric_score_id"]
                else None
            )
            run_count = connection.execute(
                "SELECT COUNT(*) AS value FROM orch_runs WHERE task_id=?", (task_id,)
            ).fetchone()["value"]
            repair_count = (
                connection.execute(
                    "SELECT COUNT(*) AS value FROM orch_repair_requests WHERE task_id=?",
                    (task_id,),
                ).fetchone()["value"]
                if is_v2
                else 0
            )
        workflow = (
            row["workflow_status"]
            if is_v2
            else self._legacy_workflow_status(str(row["status"]))
        )
        primary_value = None
        if primary is not None:
            primary_value = {
                "artifact_id": primary["id"],
                "deliverable_id": primary["logical_deliverable_id"],
                "filename": primary["filename"],
                "mime_type": primary["mime_type"],
                "sha256": primary["sha256"],
                "byte_size": primary["byte_size"],
                "version": primary["version"],
                "status": primary["status"],
            }
        verdict = None
        if evaluation is not None:
            verdict = {
                "evaluation_id": evaluation["id"],
                "decision": evaluation["decision"] or (
                    "publish" if evaluation["verdict"] == "pass" else "needs_attention"
                ),
                "rubric_score_id": evaluation["rubric_score_id"],
                "total_score": score["total"] if score is not None else None,
                "finding_ids": json.loads(evaluation["finding_ids_json"]),
                "content_hash": evaluation["content_hash"],
            }
        return {
            "task_quality_v2": is_v2,
            "workflow_status": workflow,
            "workflow_resume_status": (
                row["workflow_resume_status"] if is_v2 else None
            ),
            "quality_status": row["quality_status"] if is_v2 else "unknown",
            "artifact_status": row["artifact_status"] if is_v2 else "none",
            "budget_status": row["budget_status"] if is_v2 else "unconfigured",
            "quality_reason_code": row["quality_reason_code"],
            "primary_deliverable": primary_value,
            "quality_verdict": verdict,
            "run_summary": {"nodes": int(run_count), "repairs": int(repair_count)},
            "effective_budget": self._effective_budget(
                str(row["active_budget_ledger_id"])
                if row["active_budget_ledger_id"]
                else None
            ),
            "quality_refs": {
                "contract_id": row["active_contract_id"],
                "snapshot_id": row["active_snapshot_id"],
                "strategy_id": row["active_strategy_id"],
                "budget_ledger_id": row["active_budget_ledger_id"],
            },
            **(
                {}
                if is_v2
                else {
                    "legacy_quality_projection": True,
                    "quality_projection_warning": (
                        "This historical task has no canonical V2 contract; quality "
                        "and primary-artifact fields are not inferred."
                    ),
                }
            ),
        }

    def task_list_projection(self, task_id: str) -> dict[str, Any]:
        """Return the bounded dashboard projection for all four status axes."""

        projection = self.task_projection(task_id)
        if not projection["task_quality_v2"]:
            return {
                **projection,
                "archetype": None,
                "target": None,
                "quality_score": None,
                "hard_gate_status": "unknown",
                "budget_utilization_percent": None,
                "has_waiver": False,
                "created_by": None,
                "started_at": None,
            }
        refs = projection["quality_refs"]
        contract = self.contracts.get(str(refs["contract_id"]))
        snapshot = (
            self.snapshots.get(str(refs["snapshot_id"]))
            if refs.get("snapshot_id")
            else None
        )
        primary = projection.get("primary_deliverable") or {}
        with self.store._read() as connection:
            gate_counts = connection.execute(
                """
                SELECT
                  SUM(CASE WHEN status='fail' THEN 1 ELSE 0 END) AS failed,
                  SUM(CASE WHEN status='pass' THEN 1 ELSE 0 END) AS passed,
                  COUNT(*) AS total
                FROM orch_gate_results
                WHERE task_id=? AND (?='' OR artifact_id=?)
                """,
                (
                    task_id,
                    str(primary.get("artifact_id") or ""),
                    str(primary.get("artifact_id") or ""),
                ),
            ).fetchone()
            waiver_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS value FROM orch_quality_waivers
                    WHERE task_id=? AND revoked_at IS NULL
                      AND (expires_at IS NULL OR expires_at>?)
                    """,
                    (task_id, _now()),
                ).fetchone()["value"]
            )
            started = connection.execute(
                """
                SELECT MIN(started_at) AS value FROM orch_runs
                WHERE task_id=? AND started_at IS NOT NULL
                """,
                (task_id,),
            ).fetchone()["value"]
            task_row = connection.execute(
                "SELECT input_json, policy_json FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
        failed = int(gate_counts["failed"] or 0)
        total = int(gate_counts["total"] or 0)
        hard_gate_status = "fail" if failed else "pass" if total else "pending"
        budget = projection["effective_budget"]
        limits = dict(budget.get("limit") or {})
        used = dict(budget.get("used") or {})
        ratios = [
            float(used.get(key, 0) or 0) / float(limit)
            for key, limit in limits.items()
            if isinstance(limit, (int, float)) and float(limit) > 0
        ]
        input_value = json.loads(task_row["input_json"]) if task_row else {}
        policy_value = json.loads(task_row["policy_json"]) if task_row else {}
        created_by = str(
            input_value.get("created_by")
            or policy_value.get("created_by")
            or input_value.get("actor_id")
            or ""
        ) or None
        verdict = projection.get("quality_verdict") or {}
        repo_root = str(snapshot.repo_root) if snapshot is not None else ""
        repo_name = (
            repo_root.rstrip("/\\").replace("\\", "/").rsplit("/", 1)[-1]
            if repo_root
            else ""
        )
        return {
            **projection,
            "archetype": contract.archetype.value,
            "target": (
                {
                    "repo": repo_name,
                    "repo_root": repo_root,
                    "snapshot_ref": snapshot.selected_ref,
                    "short_sha": str(snapshot.commit_oid or "")[:12] or None,
                    "dirty": bool(snapshot.dirty),
                    "snapshot_id": snapshot.id,
                }
                if snapshot is not None
                else None
            ),
            "quality_score": verdict.get("total_score"),
            "hard_gate_status": hard_gate_status,
            "budget_utilization_percent": (
                round(max(ratios) * 100, 2) if ratios else None
            ),
            "has_waiver": waiver_count > 0,
            "created_by": created_by,
            "started_at": started,
        }

    @staticmethod
    def _legacy_workflow_status(status: str) -> str:
        return {
            "queued": "running",
            "waiting_human": "needs_attention",
            "waiting_child": "running",
            "paused": "needs_attention",
            "blocked": "needs_attention",
            "canceling": "running",
        }.get(status, status)

    def _effective_budget(self, ledger_id: str | None) -> dict[str, Any]:
        if ledger_id is None:
            return {
                "mode": "unconfigured",
                "source": None,
                "used": {},
                "reserved": {},
                "remaining": {},
                "limit": {},
            }
        ledger = self.budgets.get(ledger_id)
        return {
            "ledger_id": ledger.id,
            "mode": ledger.mode.value,
            "source": ledger.source_profile_id,
            "used": dict(ledger.consumed),
            "reserved": dict(ledger.reserved),
            "remaining": dict(ledger.remaining),
            "limit": ledger.effective_limits.model_dump(mode="json"),
            "provider_usage": self.budgets.usage_breakdown(ledger.id),
            "over_budget": ledger.over_budget,
            "fencing_token": ledger.fencing_token,
        }

    def active_contract(self, task_id: str) -> dict[str, Any]:
        return _dump(self.contracts.active_for_task(task_id, include_draft=False))

    def active_snapshot(self, task_id: str) -> dict[str, Any]:
        identifier = self._active_id(task_id, "active_snapshot_id", "snapshot")
        return _dump(self.snapshots.get(identifier))

    def active_strategy(self, task_id: str) -> dict[str, Any]:
        identifier = self._active_id(task_id, "active_strategy_id", "strategy")
        return _dump(self.strategies.get(identifier))

    def _active_id(self, task_id: str, column: str, noun: str) -> str:
        if column not in {
            "active_snapshot_id",
            "active_strategy_id",
            "active_contract_id",
            "active_budget_ledger_id",
        }:
            raise ValueError("unsupported active quality reference")
        with self.store._read() as connection:
            row = connection.execute(
                f"SELECT {column} AS value FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"task {task_id} not found")
        if not row["value"]:
            raise NotFoundError(f"task {task_id} has no active {noun}")
        return str(row["value"])

    def coverage(
        self,
        task_id: str,
        *,
        offset: int,
        limit: int,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        self.store.get_task(task_id)
        after = _cursor_after(cursor, stream="coverage", task_id=task_id)
        if cursor is not None and offset:
            raise ValueError("offset and cursor pagination cannot be combined")
        with self.store._read() as connection:
            rows = connection.execute(
                """
                SELECT rowid AS _cursor_rowid, * FROM orch_coverage_results
                WHERE task_id=? AND (? IS NULL OR rowid>?)
                ORDER BY rowid LIMIT ? OFFSET ?
                """,
                (task_id, after, after, limit + 1, offset),
            ).fetchall()
        values = [
            {
                "_cursor_rowid": row["_cursor_rowid"],
                "id": row["id"],
                "task_id": row["task_id"],
                "artifact_id": row["artifact_id"],
                "requirement_id": row["requirement_id"],
                "area": row["area"],
                "status": row["status"],
                "claim_ids": json.loads(row["claim_ids_json"]),
                "evidence_count": row["evidence_count"],
                "notes": row["notes"],
                "validator_id": row["validator_id"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]
        return {
            "task_id": task_id,
            "coverage": _page(
                values,
                offset=offset,
                limit=limit,
                task_id=task_id,
                stream="coverage",
                cursor=cursor,
            ),
        }

    def claims(
        self,
        task_id: str,
        *,
        offset: int,
        limit: int,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        self.store.get_task(task_id)
        after = _cursor_after(cursor, stream="claims", task_id=task_id)
        if cursor is not None and offset:
            raise ValueError("offset and cursor pagination cannot be combined")
        with self.store._read() as connection:
            rows = connection.execute(
                """
                SELECT rowid AS _cursor_rowid, * FROM orch_claims
                WHERE task_id=? AND (? IS NULL OR rowid>?)
                ORDER BY rowid LIMIT ? OFFSET ?
                """,
                (task_id, after, after, limit + 1, offset),
            ).fetchall()
        values = []
        for row in rows:
            value = dict(row)
            value["requirement_ids"] = json.loads(value.pop("requirement_ids_json"))
            values.append(value)
        return {
            "task_id": task_id,
            "claims": _page(
                values,
                offset=offset,
                limit=limit,
                task_id=task_id,
                stream="claims",
                cursor=cursor,
            ),
        }

    def evidence(
        self,
        task_id: str,
        *,
        offset: int,
        limit: int,
        claim_id: str | None = None,
        path: str | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        self.store.get_task(task_id)
        scope_hash = _hash({"claim_id": claim_id, "path": path})
        after = _cursor_after(
            cursor,
            stream="evidence",
            task_id=task_id,
            scope_hash=scope_hash,
        )
        if cursor is not None and offset:
            raise ValueError("offset and cursor pagination cannot be combined")
        clauses = ["c.task_id=?"]
        params: list[Any] = [task_id]
        if claim_id:
            clauses.append("e.claim_id=?")
            params.append(claim_id)
        if path:
            clauses.append("e.path=?")
            params.append(path)
        if after is not None:
            clauses.append("e.rowid>?")
            params.append(after)
        params.extend([limit + 1, offset])
        with self.store._read() as connection:
            rows = connection.execute(
                """
                SELECT e.rowid AS _cursor_rowid, e.*, c.artifact_id,
                       c.section_id, c.text AS claim_text
                FROM orch_evidence_refs e
                JOIN orch_claims c ON c.id=e.claim_id
                WHERE """
                + " AND ".join(clauses)
                + " ORDER BY e.rowid LIMIT ? OFFSET ?",
                params,
            ).fetchall()
        return {
            "task_id": task_id,
            "evidence": _page(
                [dict(row) for row in rows],
                offset=offset,
                limit=limit,
                task_id=task_id,
                stream="evidence",
                cursor=cursor,
                scope_hash=scope_hash,
            ),
        }

    def quality(
        self,
        task_id: str,
        *,
        offset: int,
        limit: int,
        gate_cursor: str | None = None,
        finding_cursor: str | None = None,
        evaluation_cursor: str | None = None,
        waiver_cursor: str | None = None,
    ) -> dict[str, Any]:
        projection = self.task_projection(task_id)
        cursors = {
            "gates": gate_cursor,
            "findings": finding_cursor,
            "evaluations": evaluation_cursor,
            "waivers": waiver_cursor,
        }
        if offset and any(cursors.values()):
            raise ValueError("offset and cursor pagination cannot be combined")
        after = {
            stream: _cursor_after(cursor, stream=stream, task_id=task_id)
            for stream, cursor in cursors.items()
        }
        with self.store._read() as connection:
            gates = connection.execute(
                """
                SELECT rowid AS _cursor_rowid, * FROM orch_gate_results
                WHERE task_id=? AND (? IS NULL OR rowid>?)
                ORDER BY rowid LIMIT ? OFFSET ?
                """,
                (task_id, after["gates"], after["gates"], limit + 1, offset),
            ).fetchall()
            findings = connection.execute(
                """
                SELECT rowid AS _cursor_rowid, * FROM orch_quality_findings
                WHERE task_id=? AND (? IS NULL OR rowid>?)
                ORDER BY rowid LIMIT ? OFFSET ?
                """,
                (
                    task_id,
                    after["findings"],
                    after["findings"],
                    limit + 1,
                    offset,
                ),
            ).fetchall()
            evaluations = connection.execute(
                """
                SELECT rowid AS _cursor_rowid, * FROM orch_quality_evaluations
                WHERE task_id=? AND (? IS NULL OR rowid>?)
                ORDER BY rowid LIMIT ? OFFSET ?
                """,
                (
                    task_id,
                    after["evaluations"],
                    after["evaluations"],
                    limit + 1,
                    offset,
                ),
            ).fetchall()
            waivers = connection.execute(
                """
                SELECT rowid AS _cursor_rowid, * FROM orch_quality_waivers
                WHERE task_id=? AND (? IS NULL OR rowid>?)
                ORDER BY rowid LIMIT ? OFFSET ?
                """,
                (task_id, after["waivers"], after["waivers"], limit + 1, offset),
            ).fetchall()
        return {
            "task_id": task_id,
            "quality_status": projection["quality_status"],
            "quality_reason_code": projection["quality_reason_code"],
            "quality_verdict": projection["quality_verdict"],
            "gates": _page(
                [self._decode_row(row) for row in gates],
                offset=offset,
                limit=limit,
                task_id=task_id,
                stream="gates",
                cursor=gate_cursor,
            ),
            "findings": _page(
                [self._decode_row(row) for row in findings],
                offset=offset,
                limit=limit,
                task_id=task_id,
                stream="findings",
                cursor=finding_cursor,
            ),
            "evaluations": _page(
                [self._decode_row(row) for row in evaluations],
                offset=offset,
                limit=limit,
                task_id=task_id,
                stream="evaluations",
                cursor=evaluation_cursor,
            ),
            "waivers": _page(
                [self._decode_row(row) for row in waivers],
                offset=offset,
                limit=limit,
                task_id=task_id,
                stream="waivers",
                cursor=waiver_cursor,
            ),
        }

    @staticmethod
    def _decode_row(row: Any) -> dict[str, Any]:
        value = dict(row)
        for key in tuple(value):
            if key.endswith("_json"):
                target = key.removesuffix("_json")
                try:
                    value[target] = json.loads(value.pop(key))
                except (TypeError, json.JSONDecodeError):
                    pass
        return value

    def deliverables(
        self,
        task_id: str,
        *,
        offset: int,
        limit: int,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        self.store.get_task(task_id)
        if cursor is not None and offset:
            raise ValueError("offset and cursor pagination cannot be combined")
        cursor_value = _cursor_payload(
            cursor,
            stream="deliverables",
            task_id=task_id,
            scope_hash=None,
        )
        after = int(cursor_value["after"]) if cursor_value is not None else None
        projected_primary = self.task_projection(task_id)["primary_deliverable"]
        current_primary_id = (
            str(projected_primary["artifact_id"])
            if projected_primary is not None
            else ""
        )
        frozen_primary_id = (
            str(cursor_value.get("scope_hash") or "")
            if cursor_value is not None
            else current_primary_id
        )
        effective_offset = (
            max(0, offset - 1)
            if cursor is None and offset > 0 and frozen_primary_id
            else offset
        )
        with self.store._read() as connection:
            rows = connection.execute(
                """
                SELECT a.rowid AS _cursor_rowid, a.*,
                       COALESCE(d.is_primary, 0) AS declared_primary,
                       CASE WHEN t.primary_artifact_id=a.id THEN 1 ELSE 0 END AS is_primary
                FROM orch_artifact_versions a
                JOIN orch_tasks t ON t.id=a.task_id
                LEFT JOIN orch_contract_deliverables d
                  ON d.contract_id=t.active_contract_id
                 AND d.id=a.logical_deliverable_id
                WHERE a.task_id=? AND json_extract(a.metadata_json, '$.internal') <> 1
                  AND (?='' OR a.id<>?)
                  AND (? IS NULL OR a.rowid>?)
                ORDER BY a.rowid
                LIMIT ? OFFSET ?
                """,
                (
                    task_id,
                    frozen_primary_id,
                    frozen_primary_id,
                    after,
                    after,
                    limit + 1,
                    effective_offset,
                ),
            ).fetchall()
            primary_row = (
                connection.execute(
                    """
                    SELECT 0 AS _cursor_rowid, a.*,
                           COALESCE(d.is_primary, 0) AS declared_primary,
                           1 AS is_primary
                    FROM orch_artifact_versions a
                    JOIN orch_tasks t ON t.id=a.task_id
                    LEFT JOIN orch_contract_deliverables d
                      ON d.contract_id=t.active_contract_id
                     AND d.id=a.logical_deliverable_id
                    WHERE a.task_id=? AND a.id=?
                      AND json_extract(a.metadata_json, '$.internal') <> 1
                    """,
                    (task_id, frozen_primary_id),
                ).fetchone()
                if cursor is None and offset == 0 and frozen_primary_id
                else None
            )
        values = (
            [self._decode_row(primary_row)] if primary_row is not None else []
        ) + [self._decode_row(row) for row in rows]
        return {
            "task_id": task_id,
            "primary_artifact_id": (
                projected_primary["artifact_id"]
                if projected_primary is not None
                else None
            ),
            "primary_deliverable": projected_primary,
            "deliverables": _page(
                values,
                offset=offset,
                limit=limit,
                task_id=task_id,
                stream="deliverables",
                cursor=cursor,
                scope_hash=frozen_primary_id,
            ),
        }

    # -- artifacts ------------------------------------------------------
    def artifact_metadata(self, artifact_id: str) -> dict[str, Any]:
        artifact = self.artifacts.get(artifact_id)
        with self.store._read() as connection:
            receipts = connection.execute(
                """
                SELECT id, verifier_profile_id, run_id, artifact_hash, ranges_json,
                       covered_bytes, coverage_ratio, candidate_bound_at, completed_at
                FROM orch_artifact_read_receipts WHERE artifact_id=?
                ORDER BY created_at DESC LIMIT 100
                """,
                (artifact_id,),
            ).fetchall()
        value = _dump(artifact)
        policy = preview_policy(artifact.filename, artifact.mime_type)
        value["preview_policy"] = {
            **policy,
            "inline": policy["inline_preview_allowed"],
            "executable": not policy["inline_preview_allowed"],
        }
        value["read_receipts"] = [self._decode_row(row) for row in receipts]
        value["max_read_coverage_ratio"] = max(
            (float(row["coverage_ratio"]) for row in receipts), default=0.0
        )
        return value

    def artifact_content(self, artifact_id: str) -> tuple[Any, bytes]:
        artifact = self.artifacts.get(artifact_id)
        if artifact.status in {
            ArtifactVersionStatus.UPLOADING,
            ArtifactVersionStatus.REJECTED,
        }:
            raise ConflictError("artifact content is unavailable in its current state")
        if not artifact.blob_uri or not artifact.sha256 or artifact.byte_size is None:
            raise ConflictError("artifact content is incomplete")
        try:
            content = self.artifacts.blobs.get(artifact.blob_uri)
        except (OSError, RuntimeError, ValueError) as exc:
            self.artifacts.record_integrity_failure(
                task_id=artifact.task_id,
                artifact_id=artifact.id,
                code="final_blob_integrity",
            )
            raise ConflictError(
                "artifact content failed immutable integrity verification"
            ) from exc
        if len(content) != artifact.byte_size or digest_bytes(content) != artifact.sha256:
            self.artifacts.record_integrity_failure(
                task_id=artifact.task_id,
                artifact_id=artifact.id,
                code="final_blob_hash_mismatch",
            )
            raise ConflictError("artifact content failed immutable integrity verification")
        return artifact, content

    def artifact_diff(self, artifact_id: str, *, base_artifact_id: str) -> str:
        return self.artifacts.diff(artifact_id, base_artifact_id=base_artifact_id)

    # -- repair, waiver and resume -------------------------------------
    def request_repair(
        self, task_id: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        artifact_id = str(
            payload.get("source_artifact_id")
            or (self.task_projection(task_id).get("primary_deliverable") or {}).get(
                "artifact_id"
            )
            or ""
        )
        if not artifact_id:
            raise ValueError("source_artifact_id is required")
        requested_ids = {
            str(item) for item in payload.get("finding_ids") or () if str(item)
        }
        with self.store._read() as connection:
            rows = connection.execute(
                """
                SELECT * FROM orch_quality_findings
                WHERE task_id=? AND artifact_id=? AND status='open'
                ORDER BY created_at, id
                """,
                (task_id, artifact_id),
            ).fetchall()
        findings = tuple(
            self._finding(row)
            for row in rows
            if not requested_ids or row["id"] in requested_ids
        )
        requested_budget = {
            "reported_tokens": 150_000,
            "model_calls": 8,
            "tool_calls": 20,
            "active_seconds": 300,
            "tool_payload_bytes": 16 * 1024 * 1024,
        }
        requested_budget.update(
            {
                str(key): int(value)
                for key, value in dict(
                    payload.get("budget_allocation") or {}
                ).items()
            }
        )
        request = self.repairs.request(
            task_id=task_id,
            source_artifact_id=artifact_id,
            findings=findings,
            budget_allocation=requested_budget,
            budget_available=bool(payload.get("budget_available", True)),
        )
        task = self.store.get_task(task_id)
        if task.status.value in {"paused", "blocked", "needs_reconciliation"}:
            self.service.resume_task(task_id)
        else:
            self.service.wake()
        return _dump(request)

    def _finding(self, row: Any) -> Finding:
        return Finding(
            id=row["id"],
            fingerprint=row["fingerprint"],
            task_id=row["task_id"],
            artifact_id=row["artifact_id"],
            artifact_hash=row["artifact_hash"],
            category=row["category"],
            severity=row["severity"],
            blocking=bool(row["blocking"]),
            repairable=bool(row["repairable"]),
            requirement_id=row["requirement_id"],
            claim_id=row["claim_id"],
            section_id=row["section_id"],
            message=row["message"],
            evidence_refs=tuple(json.loads(row["evidence_refs_json"])),
            suggested_fix=row["suggested_fix"],
            status=row["status"],
            supersedes_finding_id=row["supersedes_finding_id"],
            created_at=row["created_at"],
        )

    def create_waiver(
        self, task_id: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        if str(payload.get("actor_role") or "") not in {
            "admin",
            "quality_owner",
        }:
            raise PermissionError("only an admin or quality owner may create a waiver")
        artifact = self.artifacts.get(str(payload.get("artifact_id") or ""))
        if artifact.task_id != task_id or not artifact.sha256:
            raise PermissionError("waiver artifact is outside the task namespace")
        contract = self.contracts.active_for_task(task_id)
        subject_type = WaiverSubjectType(str(payload.get("subject_type") or ""))
        subject_id = str(payload.get("subject_id") or "")
        subject_version = int(payload.get("subject_version") or 1)
        if not subject_id:
            raise ValueError("waiver subject_id is required")
        self._assert_waivable(
            task_id=task_id,
            artifact_id=artifact.id,
            subject_type=subject_type,
            subject_id=subject_id,
            subject_version=subject_version,
        )
        actor_id = str(payload.get("actor_id") or "").strip()
        reason = str(payload.get("reason") or "").strip()
        if not actor_id or not reason:
            raise ValueError("waiver actor_id and reason are required")
        created_at = _now()
        signature_input = {
            "task_id": task_id,
            "artifact_id": artifact.id,
            "artifact_hash": artifact.sha256,
            "contract_id": contract.id,
            "contract_version": contract.version,
            "subject_type": subject_type.value,
            "subject_id": subject_id,
            "subject_version": subject_version,
            "actor_id": actor_id,
            "reason": reason,
            "reference": payload.get("reference"),
            "expires_at": payload.get("expires_at"),
        }
        signature_hash = content_sha256(signature_input)
        waiver_id = f"waiver_{uuid.uuid4().hex}"
        with self.store._write() as connection:
            existing = connection.execute(
                """
                SELECT * FROM orch_quality_waivers
                WHERE task_id=? AND artifact_id=? AND subject_type=?
                  AND subject_id=? AND subject_version=? AND signature_hash=?
                """,
                (
                    task_id,
                    artifact.id,
                    subject_type.value,
                    subject_id,
                    subject_version,
                    signature_hash,
                ),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO orch_quality_waivers(
                        id, task_id, artifact_id, artifact_hash, contract_id,
                        contract_version, subject_type, subject_id, subject_version,
                        rubric_id, rubric_version, actor_id, reason, reference,
                        expires_at, created_at, signature_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        waiver_id,
                        task_id,
                        artifact.id,
                        artifact.sha256,
                        contract.id,
                        contract.version,
                        subject_type.value,
                        subject_id,
                        subject_version,
                        payload.get("rubric_id"),
                        payload.get("rubric_version"),
                        actor_id,
                        reason,
                        payload.get("reference"),
                        payload.get("expires_at"),
                        created_at,
                        signature_hash,
                    ),
                )
                existing = connection.execute(
                    "SELECT * FROM orch_quality_waivers WHERE id=?", (waiver_id,)
                ).fetchone()
        return self._decode_row(existing)

    def _assert_waivable(
        self,
        *,
        task_id: str,
        artifact_id: str,
        subject_type: WaiverSubjectType,
        subject_id: str,
        subject_version: int,
    ) -> None:
        with self.store._read() as connection:
            if subject_type in {
                WaiverSubjectType.GATE_RESULT,
                WaiverSubjectType.CRITERION,
            }:
                row = connection.execute(
                    """
                    SELECT waivable FROM orch_gate_results
                    WHERE task_id=? AND artifact_id=? AND subject_id=?
                      AND subject_version=? ORDER BY created_at DESC LIMIT 1
                    """,
                    (task_id, artifact_id, subject_id, subject_version),
                ).fetchone()
                allowed = bool(row and row["waivable"])
            elif subject_type is WaiverSubjectType.FINDING:
                row = connection.execute(
                    """
                    SELECT category FROM orch_quality_findings
                    WHERE task_id=? AND artifact_id=? AND id=?
                    """,
                    (task_id, artifact_id, subject_id),
                ).fetchone()
                allowed = bool(
                    row and row["category"] not in {"security", "schema"}
                )
            else:
                allowed = subject_type in {
                    WaiverSubjectType.SEMANTIC_SCORE,
                    WaiverSubjectType.SOFT_BUDGET,
                }
        if not allowed:
            raise PermissionError("the exact quality subject is not waivable")

    def resume(self, task_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        projection = self.task_projection(task_id)
        with self.store._read() as connection:
            workflow_row = connection.execute(
                """
                SELECT workflow_status, workflow_resume_status, quality_reason_code
                FROM orch_tasks WHERE id=?
                """,
                (task_id,),
            ).fetchone()
        if workflow_row is None:
            raise NotFoundError(f"task {task_id} not found")
        workflow = str(workflow_row["workflow_status"])
        remembered = str(workflow_row["workflow_resume_status"] or "")
        reason_code = str(workflow_row["quality_reason_code"] or "")
        if projection["budget_status"] == "exhausted":
            limits = payload.get("effective_limits")
            if not isinstance(limits, Mapping):
                raise ConflictError(
                    "an exhausted task requires explicit increased effective_limits"
                )
            ledger_id = str(projection["effective_budget"].get("ledger_id") or "")
            self.budgets.extend(
                ledger_id,
                effective_limits={str(key): int(value) for key, value in limits.items()},
                actor_id=str(payload.get("actor_id") or ""),
                reason=str(payload.get("reason") or ""),
            )
        task = self.store.get_task(task_id)
        if task.status in {
            TaskStatus.PAUSED,
            TaskStatus.BLOCKED,
            TaskStatus.NEEDS_RECONCILIATION,
        }:
            self.service.resume_task(task_id)
        if workflow == "needs_attention":
            if remembered in {"ready", "running", "validating", "repairing"}:
                target = remembered
            elif reason_code == "budget_exhausted":
                target = "running"
            elif reason_code.startswith("repair_"):
                target = "repairing"
            else:
                target = "ready"
            apply_workflow_event(
                self.store,
                task_id=task_id,
                event=WorkflowEvent.RESUME_REQUESTED,
                server_target=WorkflowStatus(target),
                clear_reason=True,
                command_id=f"quality-resume:{task_id}",
            )
        elif workflow == "recovering":
            apply_workflow_event(
                self.store,
                task_id=task_id,
                event=WorkflowEvent.RECOVERY_SUCCEEDED,
                clear_reason=True,
                command_id=f"quality-recovery:{task_id}",
            )
        elif workflow == "needs_reconciliation":
            apply_workflow_event(
                self.store,
                task_id=task_id,
                event=WorkflowEvent.RECONCILED_RESUME,
                clear_reason=True,
                command_id=f"quality-reconcile:{task_id}",
            )
        self.service.wake()
        return self.service.task_detail(task_id)

    # -- offline export -------------------------------------------------
    def export(self, task_id: str) -> tuple[bytes, str]:
        projection = self.task_projection(task_id)
        primary = projection.get("primary_deliverable")
        if not primary:
            raise ConflictError("task has no published primary deliverable")
        artifact, content = self.artifact_content(str(primary["artifact_id"]))
        payloads: dict[str, Any] = {
            "CONTRACT.json": self.active_contract(task_id),
            "SNAPSHOT.json": self.active_snapshot(task_id),
            "STRATEGY.json": self.active_strategy(task_id),
            "COVERAGE.json": self.coverage(task_id, offset=0, limit=1_000),
            "CLAIMS.json": self.claims(task_id, offset=0, limit=1_000),
            "EVIDENCE_INDEX.json": self.evidence(
                task_id, offset=0, limit=1_000
            ),
            "QUALITY.json": self.quality(task_id, offset=0, limit=1_000),
            "BUDGET_LEDGER.json": projection["effective_budget"],
            "TASK_PROJECTION.json": projection,
        }
        with self.store._read() as connection:
            events = connection.execute(
                """
                SELECT sequence_no, id, aggregate_type, aggregate_id, event_type,
                       payload_json, created_at, previous_hash, event_hash
                FROM orch_events WHERE task_id=? ORDER BY sequence_no
                """,
                (task_id,),
            ).fetchall()
        payloads["EVENT_PROVENANCE.json"] = [
            self._decode_row(row) for row in events
        ]
        manifest = {
            "schema_id": "task_quality_export_v2",
            "schema_version": 2,
            "task_id": task_id,
            "primary_filename": artifact.filename,
            "primary_artifact_id": artifact.id,
            "primary_sha256": artifact.sha256,
            "generated_at": _now(),
            "files": {},
        }
        buffer = io.BytesIO()
        with zipfile.ZipFile(
            buffer, "w", compression=zipfile.ZIP_DEFLATED, strict_timestamps=False
        ) as archive:
            primary_name = artifact.filename
            archive.writestr(primary_name, content)
            if primary_name != "PRIMARY_DELIVERABLE.md":
                archive.writestr("PRIMARY_DELIVERABLE.md", content)
            manifest["files"][primary_name] = {
                "sha256": digest_bytes(content),
                "byte_size": len(content),
            }
            for filename, value in payloads.items():
                encoded = (json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n").encode(
                    "utf-8"
                )
                archive.writestr(filename, encoded)
                manifest["files"][filename] = {
                    "sha256": digest_bytes(encoded),
                    "byte_size": len(encoded),
                }
            archive.writestr(
                "MANIFEST.json",
                json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
            )
        return buffer.getvalue(), f"{task_id}-task-quality-export.zip"
