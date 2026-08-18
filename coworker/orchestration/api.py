"""Versioned FastAPI control plane for the orchestration core."""

from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import APIRouter, Body, Header, HTTPException, Query, Response

from .catalogs import CatalogConflict, CatalogError, CatalogNotFound
from .api_schemas import (
    AddContextRefRequest,
    BlockerSetRequest,
    DelegateTaskRequest,
    HandoffSettingsPayload,
    ResultQuestionRequest,
    TaskBriefPayload,
    TaskCommentRequest,
    TaskRelationRequest,
    WorkProductRequest,
)
from .errors import ConflictError, NotFoundError, OrchestrationError
from .handoff_models import HandoffValidationError
from .models import OrchestrationStage, TaskStatus
from .profiles import AgentRole, ProfileValidationError
from .runtime import RuntimeErrorBase


def _translate(operation: Callable[[], Any]) -> Any:
    try:
        return operation()
    except (CatalogNotFound, NotFoundError, KeyError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except HandoffValidationError as exc:
        raise HTTPException(status_code=422, detail=list(exc.issues)) from exc
    except (CatalogConflict, ConflictError, RuntimeErrorBase) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (ProfileValidationError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (CatalogError, OrchestrationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _etag(value: str | None) -> str:
    if not value:
        raise HTTPException(status_code=428, detail="If-Match is required for draft mutation")
    return value


def _operation_id(
    header_value: Optional[str], *body_values: Optional[str]
) -> Optional[str]:
    """Resolve one mutation key and reject ambiguous retry identities."""

    values = [
        str(value).strip()
        for value in (header_value, *body_values)
        if value is not None and str(value).strip()
    ]
    if len(set(values)) > 1:
        raise HTTPException(
            status_code=409,
            detail="Idempotency-Key header and body operation id disagree",
        )
    if values and len(values[0]) > 256:
        raise HTTPException(status_code=422, detail="idempotency key is too long")
    return values[0] if values else None


def _command_already_completed(service: Any, command_id: Optional[str]) -> bool:
    if not command_id:
        return False
    try:
        return service.store.get_command(command_id).status.value == "completed"
    except NotFoundError:
        return False


def create_orchestration_router(manager: Any) -> APIRouter:
    router = APIRouter(prefix="/v1/orchestration", tags=["orchestration"])
    service = manager.orchestration

    @router.get("/capabilities")
    def capabilities() -> dict[str, Any]:
        return {
            "schema_version": 2,
            "stages": [item.value for item in OrchestrationStage],
            "task_statuses": [item.value for item in TaskStatus],
            "agent_roles": [item.value for item in AgentRole],
            "limits": {
                "max_depth": 3,
                "max_concurrency": service.max_concurrency,
                "max_children_per_agent": 8,
                "max_work_units": 64,
                "max_attempts_per_node": 3,
                "runtime_budget_mode": (
                    "enforced" if service.enforce_runtime_budgets else "unlimited"
                ),
            },
            "features": {
                "durable_resume": True,
                "hash_chained_audit": True,
                "transactional_outbox": True,
                "outbox_dead_letters": True,
                "single_active_scheduler": True,
                "scheduler_write_fencing": True,
                "bounded_audit_pagination": True,
                "bounded_task_detail": True,
                "isolated_workspaces": True,
                "dag_dependencies": True,
                "formal_acceptance": True,
                "runtime_presets": True,
                "structured_handoff": service.handoff_settings.structured_handoff_enabled,
                "structured_handoff_required": service.handoff_settings.structured_handoff_required_for_new_tasks,
                "legacy_spawn_agent": service.handoff_settings.legacy_spawn_agent_enabled,
                "task_briefs": True,
                "versioned_task_briefs": True,
                "context_manifest": True,
                "task_relations": True,
                "durable_wakes": True,
                "task_comments": True,
                "work_products": True,
                "completed_results": True,
                "result_questions": True,
                "explicit_archive": True,
                "run_activity_stream": True,
            },
            "health": service.health_snapshot(),
        }

    @router.get("/health")
    def orchestration_health(response: Response) -> dict[str, Any]:
        snapshot = service.health_snapshot()
        if not snapshot["ready"]:
            response.status_code = 503
        return snapshot

    @router.get("/handoff-settings")
    def handoff_settings() -> dict[str, Any]:
        return service.handoff_settings.to_dict()

    @router.put("/handoff-settings")
    def update_handoff_settings(
        payload: HandoffSettingsPayload = Body(...),
    ) -> dict[str, Any]:
        return _translate(
            lambda: manager.set_orchestration_handoff_settings(
                payload.model_dump()
            )["settings"]
        )

    @router.get("/outbox/dead-letters")
    def outbox_dead_letters(
        offset: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=500),
    ) -> dict[str, Any]:
        return service.outbox_dead_letters(offset=offset, limit=limit)

    @router.get("/outbox/dead-letters/{outbox_id}")
    def outbox_dead_letter_detail(
        outbox_id: str,
        history_offset: int = Query(0, ge=0),
        history_limit: int = Query(100, ge=1, le=1_000),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.outbox_dead_letter_detail(
                outbox_id,
                history_offset=history_offset,
                history_limit=history_limit,
            )
        )

    @router.post("/outbox/dead-letters/{outbox_id}/requeue")
    def requeue_outbox(
        outbox_id: str,
        payload: dict[str, Any] = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        key = str(idempotency_key or "").strip()
        if not key:
            raise HTTPException(
                status_code=428,
                detail="Idempotency-Key is required for dead-letter requeue",
            )
        if len(key) > 256:
            raise HTTPException(status_code=422, detail="idempotency key is too long")
        raw_actor = payload.get("actor")
        raw_reason = payload.get("reason")
        if not isinstance(raw_actor, str):
            raise HTTPException(status_code=422, detail="actor must be a string")
        if not isinstance(raw_reason, str):
            raise HTTPException(status_code=422, detail="reason must be a string")
        actor = raw_actor.strip()
        reason = raw_reason.strip()
        return _translate(
            lambda: service.requeue_outbox(
                outbox_id,
                idempotency_key=key,
                actor=actor,
                reason=reason,
            )
        )

    # -- tasks ------------------------------------------------------------
    @router.get("/tasks")
    def list_tasks(
        status: Optional[list[TaskStatus]] = Query(None),
        limit: int = Query(100, ge=1, le=10_000),
        offset: int = Query(0, ge=0),
    ) -> list[dict[str, Any]]:
        return service.list_tasks(statuses=status, limit=limit, offset=offset)

    @router.post("/tasks", status_code=201)
    def create_task(
        response: Response,
        payload: dict[str, Any] = Body(...),
        idempotency_header: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        body_key = str(payload.get("idempotency_key") or "").strip()
        header_key = str(idempotency_header or "").strip()
        if body_key and header_key and body_key != header_key:
            raise HTTPException(
                status_code=409,
                detail="Idempotency-Key header and body idempotency_key disagree",
            )
        idempotency_key = header_key or body_key
        if not idempotency_key:
            raise HTTPException(
                status_code=428,
                detail=(
                    "Idempotency-Key header or body idempotency_key is required "
                    "for task creation"
                ),
            )
        if len(idempotency_key) > 256:
            raise HTTPException(status_code=422, detail="idempotency key is too long")
        try:
            existed = service.store.get_task_by_idempotency_key(idempotency_key)
        except NotFoundError:
            existed = None
        request = {**payload, "idempotency_key": idempotency_key}
        result = _translate(lambda: service.create_task(request))
        if existed is not None and result.get("id") == existed.id:
            response.status_code = 200
        return result

    @router.get("/tasks/by-idempotency-key")
    def get_task_by_idempotency_key_query(
        idempotency_key: str = Query(..., min_length=1, max_length=256),
    ) -> dict[str, Any]:
        """Recover opaque keys that are unsafe to embed in a URL path segment."""

        task = _translate(
            lambda: service.store.get_task_by_idempotency_key(idempotency_key)
        )
        return service.task_detail(task.id)

    @router.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        return _translate(lambda: service.task_detail(task_id))

    # -- task-centric handoff -------------------------------------------
    @router.get("/tasks/{task_id}/heartbeat-context")
    def heartbeat_context(
        task_id: str,
        after_sequence: int = Query(0, ge=0),
        run_id: Optional[str] = Query(None),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.heartbeat_context(
                task_id, after_sequence=after_sequence, run_id=run_id
            )
        )

    @router.get("/tasks/{task_id}/briefs")
    def list_briefs(task_id: str) -> list[dict[str, Any]]:
        return _translate(lambda: service.list_task_briefs(task_id))

    @router.post("/tasks/{task_id}/briefs/validate")
    def validate_brief(
        task_id: str, payload: TaskBriefPayload = Body(...)
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.validate_task_brief(task_id, payload.model_dump())
        )

    @router.get("/tasks/{task_id}/briefs/{revision}")
    def get_brief(
        task_id: str, revision: int, response: Response
    ) -> dict[str, Any]:
        value = _translate(lambda: service.get_task_brief(task_id, revision))
        response.headers["ETag"] = f'"{value["content_hash"]}"'
        return value

    @router.post("/tasks/{task_id}/briefs", status_code=201)
    def create_brief(
        task_id: str,
        response: Response,
        payload: TaskBriefPayload = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operation = _operation_id(idempotency_key, payload.operation_id)
        replayed = _command_already_completed(service, operation)
        result = _translate(
            lambda: service.create_task_brief_draft(
                task_id,
                payload.model_dump(exclude={"operation_id"}),
                command_id=operation,
            )
        )
        if replayed:
            response.status_code = 200
        return result

    @router.patch("/tasks/{task_id}/briefs/{revision}")
    def update_brief(
        task_id: str,
        revision: int,
        payload: TaskBriefPayload = Body(...),
        if_match: Optional[str] = Header(None, alias="If-Match"),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operation = _operation_id(idempotency_key, payload.operation_id)
        return _translate(
            lambda: service.update_task_brief_draft(
                task_id,
                revision,
                payload.model_dump(exclude={"operation_id"}),
                expected_hash=_etag(if_match),
                command_id=operation,
            )
        )

    @router.post("/tasks/{task_id}/briefs/{revision}/publish")
    def publish_brief(
        task_id: str,
        revision: int,
        if_match: Optional[str] = Header(None, alias="If-Match"),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.publish_task_brief(
                task_id,
                revision,
                expected_hash=_etag(if_match).strip().strip('"'),
                command_id=_operation_id(idempotency_key),
            )
        )

    @router.post("/tasks/{task_id}/delegate", status_code=201)
    def delegate_task(
        task_id: str,
        response: Response,
        payload: DelegateTaskRequest = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        _operation_id(idempotency_key, payload.operation_id)
        result = _translate(
            lambda: service.delegate_task(task_id, payload.model_dump())
        )
        if bool(result.get("replayed")):
            response.status_code = 200
        return result

    @router.get("/tasks/{task_id}/context-refs")
    def list_context_refs(
        task_id: str,
        brief_id: Optional[str] = Query(None),
        requirement: Optional[str] = Query(None),
        ref_type: Optional[str] = Query(None),
        limit: int = Query(1_000, ge=1, le=10_000),
        offset: int = Query(0, ge=0),
    ) -> list[dict[str, Any]]:
        return _translate(
            lambda: service.list_task_context_refs(
                task_id,
                brief_id=brief_id,
                requirement=requirement,
                ref_type=ref_type,
                limit=limit,
                offset=offset,
            )
        )

    @router.post("/tasks/{task_id}/context-refs", status_code=201)
    def add_context_ref(
        task_id: str,
        response: Response,
        payload: AddContextRefRequest = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operation = _operation_id(idempotency_key, payload.operation_id)
        replayed = _command_already_completed(service, operation)
        result = _translate(
            lambda: service.add_task_context_ref(
                task_id,
                payload.model_dump(exclude={"operation_id"}),
                command_id=operation,
            )
        )
        if replayed:
            response.status_code = 200
        return result

    @router.get("/context-refs/{ref_id}")
    def get_context_ref(ref_id: str) -> dict[str, Any]:
        return _translate(lambda: service.get_context_ref_metadata(ref_id))

    @router.get("/context-refs/{ref_id}/content")
    def get_context_ref_content(
        ref_id: str,
        start_line: Optional[int] = Query(None, ge=1),
        end_line: Optional[int] = Query(None, ge=1),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.read_context_ref_content(
                ref_id, start_line=start_line, end_line=end_line
            )
        )

    @router.post("/context-refs/{ref_id}/verify")
    def verify_context_ref(
        ref_id: str,
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.verify_context_ref(
                ref_id, command_id=_operation_id(idempotency_key)
            )
        )

    @router.get("/tasks/{task_id}/relations")
    def task_relations(task_id: str) -> list[dict[str, Any]]:
        return _translate(lambda: service.task_relations(task_id))

    @router.post("/tasks/{task_id}/relations", status_code=201)
    def add_relation(
        task_id: str,
        response: Response,
        payload: TaskRelationRequest = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operation = _operation_id(idempotency_key, payload.operation_id)
        replayed = _command_already_completed(service, operation)
        result = _translate(
            lambda: service.add_task_relation(
                task_id,
                payload.model_dump(exclude={"operation_id"}),
                command_id=operation,
            )
        )
        if replayed:
            response.status_code = 200
        return result

    @router.delete("/tasks/{task_id}/relations/{relation_id}")
    def remove_relation(
        task_id: str,
        relation_id: str,
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.remove_task_relation(
                task_id,
                relation_id,
                command_id=_operation_id(idempotency_key),
            )
        )

    @router.put("/tasks/{task_id}/blockers")
    def replace_blockers(
        task_id: str,
        payload: BlockerSetRequest = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> list[dict[str, Any]]:
        operation = _operation_id(idempotency_key, payload.operation_id)
        return _translate(
            lambda: service.replace_task_blockers(
                task_id,
                payload.model_dump(exclude={"operation_id"}),
                command_id=operation,
            )
        )

    @router.get("/tasks/{task_id}/comments")
    def task_comments(
        task_id: str, after_sequence: int = Query(0, ge=0)
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.task_comments(
                task_id, after_sequence=after_sequence
            )
        )

    @router.get("/tasks/{task_id}/comments/{comment_id}")
    def task_comment(task_id: str, comment_id: str) -> dict[str, Any]:
        return _translate(lambda: service.task_comment(task_id, comment_id))

    @router.post("/tasks/{task_id}/comments", status_code=201)
    def post_comment(
        task_id: str,
        response: Response,
        payload: TaskCommentRequest = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operation = _operation_id(
            idempotency_key, payload.operation_id, payload.command_id
        )
        replayed = _command_already_completed(service, operation)
        value = payload.model_dump(exclude={"operation_id"})
        value["command_id"] = operation
        result = _translate(
            lambda: service.post_operator_comment(task_id, value)
        )
        if replayed:
            response.status_code = 200
        return result

    @router.get("/tasks/{task_id}/work-products")
    def task_work_products(task_id: str) -> list[dict[str, Any]]:
        return _translate(lambda: service.task_work_products(task_id))

    @router.get("/tasks/{task_id}/result-questions")
    def result_questions(task_id: str) -> list[dict[str, Any]]:
        return _translate(lambda: service.result_questions(task_id))

    @router.post("/tasks/{task_id}/result-questions", status_code=201)
    def ask_result_question(
        task_id: str,
        payload: ResultQuestionRequest = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operation = _operation_id(idempotency_key, payload.operation_id)
        return _translate(
            lambda: service.ask_result_question(
                task_id,
                payload.question,
                command_id=operation,
            )
        )

    @router.post("/tasks/{task_id}/work-products", status_code=201)
    def create_work_product(
        task_id: str,
        response: Response,
        payload: WorkProductRequest = Body(...),
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        operation = _operation_id(
            idempotency_key, payload.operation_id, payload.command_id
        )
        replayed = _command_already_completed(service, operation)
        value = payload.model_dump(exclude={"operation_id"})
        value["command_id"] = operation
        result = _translate(
            lambda: service.create_operator_work_product(
                task_id, value
            )
        )
        if replayed:
            response.status_code = 200
        return result

    @router.get("/work-products/{product_id}")
    def get_work_product(product_id: str) -> dict[str, Any]:
        return _translate(lambda: service.get_work_product(product_id))

    @router.post("/work-products/{product_id}/verify")
    def verify_work_product(
        product_id: str,
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.verify_work_product(
                product_id, command_id=_operation_id(idempotency_key)
            )
        )

    @router.get("/tasks/{task_id}/wakes")
    def task_wakes(task_id: str) -> list[dict[str, Any]]:
        return _translate(lambda: service.task_wakes(task_id))

    @router.get("/wakes")
    def list_wakes(
        status: Optional[str] = Query(None),
        limit: int = Query(1_000, ge=1, le=10_000),
        offset: int = Query(0, ge=0),
    ) -> list[dict[str, Any]]:
        return _translate(
            lambda: service.list_wakes(status=status, limit=limit, offset=offset)
        )

    @router.post("/wakes/{wake_id}/retry")
    def retry_wake(
        wake_id: str,
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.retry_wake(
                wake_id, command_id=_operation_id(idempotency_key)
            )
        )

    @router.post("/wakes/{wake_id}/cancel")
    def cancel_wake(
        wake_id: str,
        idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.cancel_wake(
                wake_id, command_id=_operation_id(idempotency_key)
            )
        )

    @router.get("/tasks/by-idempotency-key/{idempotency_key}")
    def get_task_by_idempotency_key(idempotency_key: str) -> dict[str, Any]:
        task = _translate(
            lambda: service.store.get_task_by_idempotency_key(idempotency_key)
        )
        return service.task_detail(task.id)

    @router.post("/tasks/{task_id}/submit")
    def submit_task(task_id: str) -> dict[str, Any]:
        _translate(lambda: service.submit_task(task_id))
        return service.task_detail(task_id)

    @router.post("/tasks/{task_id}/pause")
    def pause_task(task_id: str) -> dict[str, Any]:
        _translate(lambda: service.pause_task(task_id))
        return service.task_detail(task_id)

    @router.post("/tasks/{task_id}/resume")
    def resume_task(task_id: str) -> dict[str, Any]:
        _translate(lambda: service.resume_task(task_id))
        return service.task_detail(task_id)

    @router.post("/tasks/{task_id}/cancel")
    def cancel_task(task_id: str) -> dict[str, Any]:
        _translate(lambda: service.cancel_task(task_id))
        return service.task_detail(task_id)

    @router.post("/tasks/{task_id}/archive")
    def archive_task(task_id: str) -> dict[str, Any]:
        _translate(lambda: service.archive_task(task_id))
        return service.task_detail(task_id)

    @router.post("/tasks/{task_id}/restore")
    def restore_task(task_id: str) -> dict[str, Any]:
        _translate(lambda: service.restore_task(task_id))
        return service.task_detail(task_id)

    def resolve(task_id: str, gate_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        gate = _translate(
            lambda: service.resolve_gate(
                task_id,
                gate_id,
                decision=str(payload.get("decision") or ""),
                response=str(payload.get("response") or ""),
                # The launch token authenticates one local operator today.  Never
                # accept an actor identity from request JSON: it would let any token
                # holder forge the signer recorded in gates/evidence/events.  A future
                # multi-user auth layer can replace this with its verified principal.
                resolved_by="local-user",
                expected_version=(
                    int(payload["expected_version"])
                    if payload.get("expected_version") is not None
                    else None
                ),
                idempotency_key=(
                    str(payload["idempotency_key"])
                    if payload.get("idempotency_key") is not None
                    else None
                ),
                command_id=(
                    str(payload["command_id"])
                    if payload.get("command_id") is not None
                    else None
                ),
            )
        )
        return {"ok": True, "gate": service._gate_payload(gate)}

    @router.post("/tasks/{task_id}/attention/{gate_id}/resolve")
    def resolve_attention(
        task_id: str, gate_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return resolve(task_id, gate_id, payload)

    @router.post("/tasks/{task_id}/gates/{gate_id}/resolve")
    def resolve_gate(
        task_id: str, gate_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return resolve(task_id, gate_id, payload)

    @router.get("/tasks/{task_id}/events")
    def task_events(
        task_id: str,
        after_sequence: int = Query(0, ge=0),
        before_sequence: Optional[int] = Query(None, ge=1),
        latest: bool = Query(True),
        limit: int = Query(1000, ge=1, le=10_000),
    ) -> dict[str, Any]:
        _translate(lambda: service.store.get_task(task_id))
        if after_sequence and before_sequence is not None:
            raise HTTPException(
                status_code=422,
                detail="after_sequence and before_sequence are mutually exclusive",
            )
        backwards = before_sequence is not None or (latest and after_sequence == 0)
        page = service.store.list_events(
            task_id=task_id,
            after_sequence=after_sequence,
            before_sequence=before_sequence,
            newest=backwards,
            limit=limit + 1,
        )
        events = page[-limit:] if backwards else page[:limit]
        has_more = len(page) > limit
        verification = _translate(lambda: service.store.verify_event_page(events))
        next_sequence = None
        if has_more and events:
            next_sequence = events[0].sequence if backwards else events[-1].sequence
        return {
            "events": [
                {
                    "sequence": event.sequence,
                    "id": event.id,
                    "task_id": event.task_id,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": event.aggregate_id,
                    "event_type": event.event_type,
                    "payload": dict(event.payload),
                    "previous_hash": event.previous_hash,
                    "event_hash": event.event_hash,
                    "command_id": event.command_id,
                    "created_at": event.created_at,
                    **service._event_observability_fields(event),
                }
                for event in events
            ],
            # Compatibility field: this now means the bounded page + its immediate
            # global predecessor links were verified. Full genesis-to-tip validation
            # remains a fail-closed startup operation.
            "chain_valid": verification["valid"],
            "chain_verification": verification,
            "has_more": has_more,
            "next_sequence": next_sequence,
            "next_parameter": "before_sequence" if backwards else "after_sequence",
            "order": "oldest_to_newest",
        }

    @router.get("/tasks/{task_id}/runs/{run_id}/transcript")
    def run_transcript(
        task_id: str,
        run_id: str,
        offset: int = Query(0, ge=0),
        limit: int = Query(500, ge=1, le=2_000),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.run_transcript(
                task_id,
                run_id,
                offset=offset,
                limit=limit,
            )
        )

    @router.get("/tasks/{task_id}/runs/{run_id}/activity")
    def run_activity(
        task_id: str,
        run_id: str,
        after_sequence: int = Query(0, ge=0),
        before_sequence: Optional[int] = Query(None, ge=1),
        latest: bool = Query(True),
        limit: int = Query(500, ge=1, le=2_000),
    ) -> dict[str, Any]:
        if after_sequence and before_sequence is not None:
            raise HTTPException(
                status_code=422,
                detail="after_sequence and before_sequence are mutually exclusive",
            )
        backwards = before_sequence is not None or (latest and after_sequence == 0)
        page = _translate(
            lambda: service.store.list_run_activity(
                task_id,
                run_id,
                after_sequence=after_sequence,
                before_sequence=before_sequence,
                newest=backwards,
                limit=limit + 1,
            )
        )
        items = page[-limit:] if backwards else page[:limit]
        has_more = len(page) > limit
        next_sequence = None
        if has_more and items:
            next_sequence = items[0].sequence if backwards else items[-1].sequence
        return {
            "task_id": task_id,
            "run_id": run_id,
            "activity": [
                {
                    "sequence": item.sequence,
                    "id": item.id,
                    "event_key": item.event_key,
                    "source_id": item.source_id,
                    "kind": item.kind,
                    "status": item.status,
                    "title": item.title,
                    "summary": item.summary,
                    "detail": dict(item.detail),
                    "created_at": item.created_at,
                }
                for item in items
            ],
            "has_more": has_more,
            "next_sequence": next_sequence,
            "next_parameter": "before_sequence" if backwards else "after_sequence",
            "order": "oldest_to_newest",
            "privacy": {
                "reasoning": "provider_summary_only",
                "tool_output": "metadata_only",
            },
        }

    @router.get("/tasks/{task_id}/runs")
    def task_runs(
        task_id: str,
        offset: int = Query(0, ge=0),
        limit: int = Query(200, ge=1, le=500),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.task_runs_page(task_id, offset=offset, limit=limit)
        )

    @router.get("/tasks/{task_id}/gates")
    def task_gates(
        task_id: str,
        offset: int = Query(0, ge=0),
        limit: int = Query(200, ge=1, le=500),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.task_gates_page(task_id, offset=offset, limit=limit)
        )

    @router.get("/tasks/{task_id}/evidence")
    def task_evidence(
        task_id: str,
        offset: int = Query(0, ge=0),
        limit: int = Query(200, ge=1, le=500),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.task_evidence_page(task_id, offset=offset, limit=limit)
        )

    @router.get("/blobs/{sha256}")
    def get_blob(sha256: str) -> Response:
        content, mime_type = _translate(lambda: service.get_blob(sha256))
        return Response(
            content=content,
            media_type=mime_type,
            headers={
                "ETag": f'"sha256:{sha256.lower()}"',
                "Cache-Control": "private, immutable, max-age=31536000",
                "X-Content-Type-Options": "nosniff",
            },
        )

    # -- agent profiles ---------------------------------------------------
    @router.get("/agent-profiles")
    def list_profiles() -> list[dict[str, Any]]:
        return service.catalog.list_profiles()

    @router.post("/agent-profiles", status_code=201)
    def create_profile(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return _translate(lambda: service.catalog.create_profile(payload.get("spec") or payload))

    @router.get("/agent-profiles/{profile_id}")
    def get_profile(profile_id: str) -> dict[str, Any]:
        return _translate(lambda: service.catalog.get_profile(profile_id))

    @router.post("/agent-profiles/{profile_id}/clone", status_code=201)
    def clone_profile(
        profile_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.catalog.clone_profile(
                profile_id,
                str(payload.get("new_profile_id") or ""),
                overrides=dict(payload.get("overrides") or {}),
            )
        )

    @router.post("/agent-profiles/{profile_id}/draft")
    def create_profile_draft(
        profile_id: str, payload: dict[str, Any] = Body(default={})
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.catalog.create_profile_draft(
                profile_id,
                base_version=(
                    int(payload["base_version"])
                    if payload.get("base_version") is not None
                    else None
                ),
            )
        )

    @router.put("/agent-profiles/{profile_id}/draft")
    def save_profile_draft(
        profile_id: str,
        payload: dict[str, Any] = Body(...),
        if_match: str | None = Header(default=None, alias="If-Match"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.catalog.save_profile_draft(
                profile_id,
                payload.get("spec") or payload,
                expected_etag=_etag(if_match),
            )
        )

    @router.post("/agent-profiles/{profile_id}/draft/validate")
    def validate_profile(
        profile_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        spec = dict(payload.get("spec") or payload)
        if str(spec.get("profile_id")) != profile_id:
            return {
                "valid": False,
                "errors": [
                    {
                        "code": "profile_id_mismatch",
                        "path": "spec.profile_id",
                        "message": "profile id in path and spec must match",
                    }
                ],
                "warnings": [],
            }
        return service.catalog.validate_profile(spec)

    @router.post("/agent-profiles/{profile_id}/draft/publish")
    def publish_profile(
        profile_id: str,
        if_match: str | None = Header(default=None, alias="If-Match"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.catalog.publish_profile(
                profile_id, expected_etag=_etag(if_match)
            )
        )

    # -- routing policies -------------------------------------------------
    @router.get("/model-policies")
    def list_policies() -> list[dict[str, Any]]:
        return service.catalog.list_policies()

    @router.post("/model-policies", status_code=201)
    def create_policy(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return _translate(lambda: service.catalog.create_policy(payload.get("spec") or payload))

    @router.get("/model-policies/{policy_id}")
    def get_policy(policy_id: str) -> dict[str, Any]:
        return _translate(lambda: service.catalog.get_policy(policy_id))

    @router.post("/model-policies/{policy_id}/clone", status_code=201)
    def clone_policy(
        policy_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.catalog.clone_policy(
                policy_id, str(payload.get("new_policy_id") or "")
            )
        )

    @router.post("/model-policies/{policy_id}/draft")
    def create_policy_draft(
        policy_id: str, payload: dict[str, Any] = Body(default={})
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.catalog.create_policy_draft(
                policy_id,
                base_version=(
                    int(payload["base_version"])
                    if payload.get("base_version") is not None
                    else None
                ),
            )
        )

    @router.put("/model-policies/{policy_id}/draft")
    def save_policy_draft(
        policy_id: str,
        payload: dict[str, Any] = Body(...),
        if_match: str | None = Header(default=None, alias="If-Match"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.catalog.save_policy_draft(
                policy_id,
                payload.get("spec") or payload,
                expected_etag=_etag(if_match),
            )
        )

    @router.post("/model-policies/{policy_id}/draft/validate")
    def validate_policy(
        policy_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        spec = dict(payload.get("spec") or payload)
        if str(spec.get("policy_id")) != policy_id:
            return {
                "valid": False,
                "errors": [
                    {
                        "code": "policy_id_mismatch",
                        "path": "spec.policy_id",
                        "message": "policy id in path and spec must match",
                    }
                ],
                "warnings": [],
            }
        return service.catalog.validate_policy(spec)

    @router.post("/model-policies/{policy_id}/draft/publish")
    def publish_policy(
        policy_id: str,
        if_match: str | None = Header(default=None, alias="If-Match"),
    ) -> dict[str, Any]:
        return _translate(
            lambda: service.catalog.publish_policy(
                policy_id, expected_etag=_etag(if_match)
            )
        )

    @router.post("/model-policies/{policy_id}/draft/simulate")
    def simulate_policy(
        policy_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        policy = dict(payload.get("policy") or {})
        if str(policy.get("policy_id")) != policy_id:
            raise HTTPException(status_code=422, detail="policy id in path and simulation must match")
        return _translate(
            lambda: service.simulate_routing(policy, dict(payload.get("request") or {}))
        )

    @router.get("/model-catalog")
    def model_catalog() -> list[dict[str, Any]]:
        return service.model_catalog()

    @router.get("/subscription-runtimes")
    def subscription_runtimes(
        refresh: bool = Query(False),
    ) -> list[dict[str, Any]]:
        """Inspect local subscription CLI readiness without consuming model quota."""

        return service.subscription_runtime_catalog(refresh=refresh)

    @router.get("/runtime-presets")
    def runtime_presets(
        refresh: bool = Query(False),
    ) -> list[dict[str, Any]]:
        """Inspect role-aware runtime presets and their local readiness."""

        return service.runtime_preset_catalog(refresh=refresh)

    return router
