"""Persistence and optimistic publication for canonical TaskContractV2 values."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from ..errors import ConflictError, NotFoundError
from ..store import OrchestrationStore
from .contract_linter import assert_contract_publishable
from .models import ContractStatus, TaskContractV2
from .state_machine import WorkflowEvent, transition_workflow_in_transaction


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class ContractRepository:
    def __init__(self, store: OrchestrationStore) -> None:
        self.store = store

    def save_draft(self, contract: TaskContractV2) -> TaskContractV2:
        contract.verify_content_hash()
        if contract.status is not ContractStatus.DRAFT:
            raise ValueError("only draft contracts may be initially persisted")
        content = contract.model_dump(mode="json")
        now = _now()
        with self.store._write() as connection:
            if connection.execute(
                "SELECT 1 FROM orch_tasks WHERE id=?", (contract.task_id,)
            ).fetchone() is None:
                raise NotFoundError(f"task {contract.task_id} not found")
            existing = connection.execute(
                "SELECT content_hash FROM orch_quality_contracts WHERE id=?", (contract.id,)
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] != contract.content_hash:
                    raise ConflictError("contract id was replayed with different content")
                return self.get(contract.id)
            connection.execute(
                """
                INSERT INTO orch_quality_contracts(
                    id, task_id, version, schema_id, schema_version, status,
                    title, objective, background, scope_json, instructions_json,
                    original_prompt_hash, archetype, language, constraints_json,
                    non_goals_json, quality_profile_id, compiler_json, content_json,
                    content_hash, etag, created_at
                ) VALUES (?, ?, ?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    contract.id,
                    contract.task_id,
                    contract.version,
                    contract.schema_id,
                    contract.schema_version,
                    contract.title,
                    contract.objective,
                    contract.background,
                    _json(contract.scope.model_dump(mode="json")),
                    _json(list(contract.instructions)),
                    contract.original_prompt_hash,
                    contract.archetype.value,
                    contract.language,
                    _json([item.model_dump(mode="json") for item in contract.constraints]),
                    _json(list(contract.non_goals)),
                    contract.quality_profile_id,
                    _json(contract.compiler.model_dump(mode="json")),
                    _json(content),
                    contract.content_hash,
                    contract.content_hash,
                    now,
                ),
            )
            for position, requirement in enumerate(contract.requirements):
                value = requirement.model_dump(mode="json")
                connection.execute(
                    """
                    INSERT INTO orch_contract_requirements(
                        id, contract_id, position, category, text, required, hard_gate,
                        source, source_span_json, confidence, verification_method,
                        verification_spec_json, waivable
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        requirement.id,
                        contract.id,
                        position,
                        requirement.category.value,
                        requirement.text,
                        int(requirement.required),
                        int(requirement.hard_gate),
                        requirement.source.value,
                        _json(value["source_span"]) if value.get("source_span") else None,
                        requirement.confidence,
                        requirement.verification_method.value,
                        _json(dict(requirement.verification_spec)),
                        int(requirement.waivable),
                    ),
                )
            for position, deliverable in enumerate(contract.deliverables):
                connection.execute(
                    """
                    INSERT INTO orch_contract_deliverables(
                        id, contract_id, position, kind, filename, mime_type, channel,
                        required, is_primary, required_sections_json, result_schema_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        deliverable.id,
                        contract.id,
                        position,
                        deliverable.kind,
                        deliverable.filename,
                        deliverable.mime_type,
                        deliverable.channel,
                        int(deliverable.required),
                        int(deliverable.primary),
                        _json(list(deliverable.required_sections)),
                        deliverable.result_schema_id,
                    ),
                )
        return self.get(contract.id)

    def get(self, contract_id: str) -> TaskContractV2:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT content_json, status FROM orch_quality_contracts WHERE id=?",
                (contract_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"contract {contract_id} not found")
        value = json.loads(row["content_json"])
        value["status"] = row["status"]
        return TaskContractV2.model_validate(value)

    def active_for_task(
        self, task_id: str, *, include_draft: bool = False
    ) -> TaskContractV2:
        """Return the active published contract, or the newest draft during intake."""

        with self.store._read() as connection:
            task = connection.execute(
                "SELECT active_contract_id FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
            if task is None:
                raise NotFoundError(f"task {task_id} not found")
            contract_id = task["active_contract_id"]
            if contract_id is None and include_draft:
                row = connection.execute(
                    """
                    SELECT id FROM orch_quality_contracts
                    WHERE task_id=? AND status='draft'
                    ORDER BY version DESC LIMIT 1
                    """,
                    (task_id,),
                ).fetchone()
                contract_id = row["id"] if row is not None else None
        if contract_id is None:
            raise NotFoundError(f"task {task_id} has no quality contract")
        return self.get(str(contract_id))

    def update_draft(
        self, contract: TaskContractV2, *, if_match: str
    ) -> TaskContractV2:
        """Optimistically replace one draft without mutating a published version."""

        contract.verify_content_hash()
        if contract.status is not ContractStatus.DRAFT:
            raise ValueError("only draft contracts may be updated")
        assert_contract_publishable(contract)
        content = contract.model_dump(mode="json")
        with self.store._write() as connection:
            row = connection.execute(
                """
                SELECT task_id, status, etag FROM orch_quality_contracts WHERE id=?
                """,
                (contract.id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"contract {contract.id} not found")
            if row["task_id"] != contract.task_id:
                raise PermissionError("contract is outside the task namespace")
            if row["status"] != ContractStatus.DRAFT.value:
                raise ConflictError("published contracts are immutable")
            if row["etag"] != if_match:
                raise ConflictError(
                    "contract changed; refresh and retry with the current ETag"
                )
            connection.execute(
                "DELETE FROM orch_contract_requirements WHERE contract_id=?",
                (contract.id,),
            )
            connection.execute(
                "DELETE FROM orch_contract_deliverables WHERE contract_id=?",
                (contract.id,),
            )
            changed = connection.execute(
                """
                UPDATE orch_quality_contracts
                SET title=?, objective=?, background=?, scope_json=?,
                    instructions_json=?, original_prompt_hash=?, archetype=?,
                    language=?, constraints_json=?, non_goals_json=?,
                    quality_profile_id=?, compiler_json=?, content_json=?,
                    content_hash=?, etag=?
                WHERE id=? AND status='draft' AND etag=?
                """,
                (
                    contract.title,
                    contract.objective,
                    contract.background,
                    _json(contract.scope.model_dump(mode="json")),
                    _json(list(contract.instructions)),
                    contract.original_prompt_hash,
                    contract.archetype.value,
                    contract.language,
                    _json(
                        [item.model_dump(mode="json") for item in contract.constraints]
                    ),
                    _json(list(contract.non_goals)),
                    contract.quality_profile_id,
                    _json(contract.compiler.model_dump(mode="json")),
                    _json(content),
                    contract.content_hash,
                    contract.content_hash,
                    contract.id,
                    if_match,
                ),
            ).rowcount
            if changed != 1:
                raise ConflictError(
                    "contract update lost an optimistic concurrency race"
                )
            for position, requirement in enumerate(contract.requirements):
                value = requirement.model_dump(mode="json")
                connection.execute(
                    """
                    INSERT INTO orch_contract_requirements(
                        id, contract_id, position, category, text, required,
                        hard_gate, source, source_span_json, confidence,
                        verification_method, verification_spec_json, waivable
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        requirement.id,
                        contract.id,
                        position,
                        requirement.category.value,
                        requirement.text,
                        int(requirement.required),
                        int(requirement.hard_gate),
                        requirement.source.value,
                        _json(value["source_span"])
                        if value.get("source_span")
                        else None,
                        requirement.confidence,
                        requirement.verification_method.value,
                        _json(dict(requirement.verification_spec)),
                        int(requirement.waivable),
                    ),
                )
            for position, deliverable in enumerate(contract.deliverables):
                connection.execute(
                    """
                    INSERT INTO orch_contract_deliverables(
                        id, contract_id, position, kind, filename, mime_type,
                        channel, required, is_primary, required_sections_json,
                        result_schema_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        deliverable.id,
                        contract.id,
                        position,
                        deliverable.kind,
                        deliverable.filename,
                        deliverable.mime_type,
                        deliverable.channel,
                        int(deliverable.required),
                        int(deliverable.primary),
                        _json(list(deliverable.required_sections)),
                        deliverable.result_schema_id,
                    ),
                )
        return self.get(contract.id)

    def publish(self, contract_id: str, *, if_match: str) -> TaskContractV2:
        contract = self.get(contract_id)
        if contract.status is ContractStatus.PUBLISHED:
            if if_match != contract.content_hash:
                raise ConflictError("contract ETag does not match")
            return contract
        if if_match != contract.content_hash:
            raise ConflictError("contract changed; refresh and retry with the current ETag")
        assert_contract_publishable(contract)
        now = _now()
        content = contract.model_copy(update={"status": ContractStatus.PUBLISHED}).model_dump(
            mode="json"
        )
        with self.store._write() as connection:
            changed = connection.execute(
                """
                UPDATE orch_quality_contracts
                SET status='published', published_at=?, content_json=?
                WHERE id=? AND status='draft' AND etag=?
                """,
                (now, _json(content), contract_id, if_match),
            ).rowcount
            if changed != 1:
                raise ConflictError("contract publication lost an optimistic concurrency race")
            task = connection.execute(
                "SELECT workflow_status FROM orch_tasks WHERE id=?",
                (contract.task_id,),
            ).fetchone()
            connection.execute(
                "UPDATE orch_tasks SET active_contract_id=? WHERE id=?",
                (contract_id, contract.task_id),
            )
            # Direct domain-service callers may publish an already compiled
            # contract without first using the HTTP analyze facade.  That still
            # enters the same canonical event path rather than writing a state.
            if task is not None and task["workflow_status"] == "draft":
                transition_workflow_in_transaction(
                    self.store,
                    connection,
                    task_id=contract.task_id,
                    event=WorkflowEvent.ANALYSIS_REQUESTED,
                    command_id=f"quality-contract-publish:{contract.id}",
                )
        return self.get(contract_id)
