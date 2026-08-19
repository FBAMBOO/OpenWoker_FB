"""Bounded, fingerprinted and section-scoped artifact repair coordination."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Iterable

from ..errors import ConflictError, NotFoundError
from ..store import OrchestrationStore
from .artifacts import ArtifactService
from .models import (
    ArtifactVersion,
    ArtifactVersionStatus,
    Finding,
    FindingStatus,
    RepairRequest,
    RepairRequestStatus,
)
from .state_machine import (
    WorkflowEvent,
    apply_workflow_event,
    transition_workflow_in_transaction,
)


GLOBAL_REPAIR_VALIDATORS = (
    "workspace-integrity@1",
    "baseline-validator@1",
    "citation-validator@1",
    "inventory-reconciliation-validator@1",
    "artifact-contract-validator@1",
    "complete-review-validator@1",
    "schema-integrity-validator@1",
    "budget-integrity-validator@1",
)


class RepairExhausted(ConflictError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _section_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).casefold()).strip("_")


def _markdown_sections(content: bytes) -> tuple[tuple[str, ...], dict[str, str]]:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConflictError("section-scoped repair requires UTF-8 Markdown") from exc
    order: list[str] = []
    sections: dict[str, list[str]] = {"__preamble__": []}
    current = "__preamble__"
    counts: dict[str, int] = {}
    for line in text.splitlines(keepends=True):
        match = re.match(r"^#{1,6}\s+(.+?)\s*$", line.rstrip("\r\n"))
        if match:
            base = _section_key(match.group(1)) or "untitled"
            counts[base] = counts.get(base, 0) + 1
            current = base if counts[base] == 1 else f"{base}__{counts[base]}"
            order.append(current)
            sections[current] = []
        sections[current].append(line)
    return tuple(order), {key: "".join(lines) for key, lines in sections.items()}


class RepairCoordinator:
    def __init__(
        self,
        store: OrchestrationStore,
        artifacts: ArtifactService,
        *,
        max_attempts: int = 2,
    ) -> None:
        self.store = store
        self.artifacts = artifacts
        self.max_attempts = max(0, min(int(max_attempts), 2))

    def request(
        self,
        *,
        task_id: str,
        source_artifact_id: str,
        findings: Iterable[Finding],
        budget_allocation: dict[str, int],
        budget_available: bool,
    ) -> RepairRequest:
        source = self.artifacts.get(source_artifact_id)
        if source.task_id != task_id:
            raise PermissionError("repair artifact is outside the task namespace")
        selected_by_fingerprint = {
            item.fingerprint: item
            for item in findings
            if item.task_id == task_id
            and item.artifact_id == source_artifact_id
            and item.status is FindingStatus.OPEN
            and item.blocking
            and item.repairable
        }
        selected = tuple(sorted(selected_by_fingerprint.values(), key=lambda item: item.id))
        if not selected:
            raise ValueError("repair requires at least one open blocking repairable finding")
        if not budget_available:
            self._record_repair_attention(
                task_id,
                reason="repair_budget_insufficient",
            )
            raise ConflictError("repair budget is insufficient")
        with self.store._read() as connection:
            previous = connection.execute(
                "SELECT COALESCE(MAX(attempt), 0) AS attempt FROM orch_repair_requests WHERE task_id=?",
                (task_id,),
            ).fetchone()
        attempt = int(previous["attempt"]) + 1
        if attempt > self.max_attempts:
            self._record_repair_attention(task_id, reason="repair_exhausted")
            raise RepairExhausted("maximum repair attempts exhausted")
        fingerprint_set = sorted(selected_by_fingerprint)
        finding_set_hash = "sha256:" + hashlib.sha256(_json(fingerprint_set).encode("utf-8")).hexdigest()
        allowed_sections = self._allowed_sections(task_id, selected)
        request = RepairRequest(
            id=f"repair_{uuid.uuid4().hex}",
            task_id=task_id,
            source_artifact_id=source_artifact_id,
            target_version=source.version + 1,
            finding_ids=tuple(item.id for item in selected),
            allowed_sections=allowed_sections,
            required_validators=GLOBAL_REPAIR_VALIDATORS,
            budget_allocation=dict(budget_allocation),
            attempt=attempt,
            status=RepairRequestStatus.PENDING,
        )
        now = _now()
        try:
            with self.store._write() as connection:
                connection.execute(
                    """
                    INSERT INTO orch_repair_requests(
                        id, task_id, source_artifact_id, target_version,
                        finding_ids_json, finding_set_hash, allowed_sections_json,
                        required_validators_json, budget_allocation_json, attempt,
                        status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                    """,
                    (
                        request.id, request.task_id, request.source_artifact_id,
                        request.target_version, _json(list(request.finding_ids)),
                        finding_set_hash, _json(list(request.allowed_sections)),
                        _json(list(request.required_validators)),
                        _json(dict(request.budget_allocation)), request.attempt, now,
                    ),
                )
                connection.execute(
                    """
                    UPDATE orch_quality_findings SET status='repairing'
                    WHERE id IN ({}) AND status='open'
                    """.format(",".join("?" for _ in request.finding_ids)),
                    request.finding_ids,
                )
                task = connection.execute(
                    "SELECT workflow_status FROM orch_tasks WHERE id=?", (task_id,)
                ).fetchone()
                current = str(task["workflow_status"] if task is not None else "")
                if current in {"validating", "reviewing"}:
                    event = WorkflowEvent.REPAIRABLE_FAILURE
                    target = None
                elif current == "completed":
                    event = WorkflowEvent.REPAIR_REQUESTED
                    target = None
                elif current == "needs_attention":
                    event = WorkflowEvent.RESUME_REQUESTED
                    target = "repairing"
                elif current == "repairing":
                    event = None
                    target = None
                else:
                    event = WorkflowEvent.REPAIRABLE_FAILURE
                    target = None
                if event is not None:
                    transition_workflow_in_transaction(
                        self.store,
                        connection,
                        task_id=task_id,
                        event=event,
                        server_target=target,
                        reason_code="repair_requested",
                        command_id=f"quality-repair:{request.id}",
                    )
                connection.execute(
                    "UPDATE orch_tasks SET quality_status='fail' WHERE id=?",
                    (task_id,),
                )
        except sqlite3.IntegrityError:
            with self.store._read() as connection:
                row = connection.execute(
                    """
                    SELECT * FROM orch_repair_requests
                    WHERE task_id=? AND source_artifact_id=? AND finding_set_hash=? AND attempt=?
                    """,
                    (task_id, source_artifact_id, finding_set_hash, attempt),
                ).fetchone()
            if row is None:
                raise
            return self._record(row)
        return request

    def _allowed_sections(
        self,
        task_id: str,
        findings: tuple[Finding, ...],
    ) -> tuple[str, ...]:
        """Expand validator-level locations into bounded artifact section names."""

        with self.store._read() as connection:
            task = connection.execute(
                "SELECT active_contract_id FROM orch_tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            contract_id = str(task["active_contract_id"] or "") if task else ""
            deliverable = (
                connection.execute(
                    """
                    SELECT required_sections_json FROM orch_contract_deliverables
                    WHERE contract_id=? AND is_primary=1
                    """,
                    (contract_id,),
                ).fetchone()
                if contract_id
                else None
            )
            requirement_rows = (
                connection.execute(
                    """
                    SELECT verification_spec_json FROM orch_contract_requirements
                    WHERE contract_id=?
                    """,
                    (contract_id,),
                ).fetchall()
                if contract_id
                else ()
            )
            claim_ids = tuple(
                item.claim_id for item in findings if item.claim_id is not None
            )
            claim_rows = (
                connection.execute(
                    "SELECT section_id FROM orch_claims WHERE id IN ("
                    + ",".join("?" for _ in claim_ids)
                    + ")",
                    claim_ids,
                ).fetchall()
                if claim_ids
                else ()
            )
        required_sections = tuple(
            str(item)
            for item in (
                json.loads(deliverable["required_sections_json"])
                if deliverable is not None
                else ()
            )
        )
        coverage_areas: set[str] = set()
        for row in requirement_rows:
            raw = json.loads(row["verification_spec_json"])
            areas = raw.get("areas") if isinstance(raw, dict) else ()
            if isinstance(areas, list):
                coverage_areas.update(str(item) for item in areas)
        selected: set[str] = {
            str(row["section_id"]) for row in claim_rows if row["section_id"]
        }
        broad_gate = False
        for finding in findings:
            location = str(finding.section_id or "")
            gate_id = str(finding.requirement_id or "")
            if location and not location.startswith("QG-"):
                selected.add(location)
            if gate_id in {"QG-003", "QG-004"}:
                selected.update(coverage_areas)
            elif gate_id == "QG-005":
                selected.add("relationships")
            elif gate_id == "QG-006":
                selected.add("deployment")
            elif gate_id in {"QG-008", "QG-009"}:
                selected.add("limitations")
            elif gate_id == "QG-011":
                selected.update({"architecture_overview", "inventory"})
            elif gate_id in {"QG-007", "QG-010", "QG-012", "QG-013", "QG-014", "QG-015"}:
                broad_gate = True
        if broad_gate or not selected:
            selected.update(required_sections)
        return tuple(sorted(item for item in selected if _section_key(item)))

    def complete(self, repair_id: str, *, result_artifact_id: str) -> RepairRequest:
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_repair_requests WHERE id=?", (repair_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"repair request {repair_id} not found")
        source = self.artifacts.get(row["source_artifact_id"])
        result = self.artifacts.get(result_artifact_id)
        if (
            result.task_id != source.task_id
            or result.parent_artifact_id != source.id
            or result.logical_deliverable_id != source.logical_deliverable_id
            or result.version != row["target_version"]
        ):
            raise ConflictError("repair result is not the required immutable child version")
        if result.status in {ArtifactVersionStatus.UPLOADING, ArtifactVersionStatus.REJECTED}:
            raise ConflictError("repair result is not finalized and reviewable")
        self._assert_section_scope(
            source,
            result,
            allowed_sections=tuple(json.loads(row["allowed_sections_json"])),
        )
        now = _now()
        with self.store._write() as connection:
            connection.execute(
                """
                UPDATE orch_repair_requests
                SET status='completed', result_artifact_id=?, completed_at=?
                WHERE id=? AND status IN ('pending', 'running')
                """,
                (result_artifact_id, now, repair_id),
            )
            connection.execute(
                "UPDATE orch_tasks SET primary_artifact_id=? WHERE id=?",
                (result_artifact_id, source.task_id),
            )
            transition_workflow_in_transaction(
                self.store,
                connection,
                task_id=source.task_id,
                event=WorkflowEvent.REPAIRED_CANDIDATE_CREATED,
                clear_reason=True,
                command_id=f"quality-repair-complete:{repair_id}",
            )
        if source.status is not ArtifactVersionStatus.SUPERSEDED:
            self.artifacts.set_status(source.id, ArtifactVersionStatus.SUPERSEDED)
        with self.store._read() as connection:
            updated = connection.execute(
                "SELECT * FROM orch_repair_requests WHERE id=?", (repair_id,)
            ).fetchone()
        return self._record(updated)

    def mark_validated(self, *, result_artifact_id: str) -> RepairRequest | None:
        """Resolve source findings only after the child version passes adjudication."""

        result = self.artifacts.get(result_artifact_id)
        if result.status is not ArtifactVersionStatus.VERIFIED:
            raise ConflictError("repair findings cannot resolve before the child is verified")
        with self.store._read() as connection:
            row = connection.execute(
                """
                SELECT * FROM orch_repair_requests
                WHERE result_artifact_id=? AND status='completed'
                ORDER BY attempt DESC, created_at DESC LIMIT 1
                """,
                (result_artifact_id,),
            ).fetchone()
        if row is None:
            return None
        finding_ids = tuple(json.loads(row["finding_ids_json"]))
        now = _now()
        if finding_ids:
            with self.store._write() as connection:
                connection.execute(
                    """
                    UPDATE orch_quality_findings
                    SET status='resolved', resolved_at=?
                    WHERE id IN ({}) AND status='repairing'
                    """.format(",".join("?" for _ in finding_ids)),
                    (now, *finding_ids),
                )
        return self._record(row)

    def fail_active(self, repair_id: str, *, reason: str) -> None:
        """Durably terminate a repair plan that exhausted its own run retries."""

        now = _now()
        with self.store._write() as connection:
            row = connection.execute(
                "SELECT task_id FROM orch_repair_requests WHERE id=?",
                (repair_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"repair request {repair_id} not found")
            connection.execute(
                """
                UPDATE orch_repair_requests SET status='failed', completed_at=?
                WHERE id=? AND status IN ('pending', 'running')
                """,
                (now, repair_id),
            )
            transition_workflow_in_transaction(
                self.store,
                connection,
                task_id=str(row["task_id"]),
                event=WorkflowEvent.REPAIR_FAILED,
                reason_code=str(reason)[:255],
                command_id=f"quality-repair-failed:{repair_id}",
            )
            connection.execute(
                "UPDATE orch_tasks SET quality_status='fail' WHERE id=?",
                (row["task_id"],),
            )

    def _record_repair_attention(self, task_id: str, *, reason: str) -> None:
        """Move an active repair path to attention and retain a failed verdict."""

        with self.store._read() as connection:
            row = connection.execute(
                "SELECT workflow_status FROM orch_tasks WHERE id=?", (task_id,)
            ).fetchone()
        current = str(row["workflow_status"] if row is not None else "")
        if current == "repairing":
            event = WorkflowEvent.REPAIR_EXHAUSTED
        elif current in {"validating", "reviewing"}:
            event = WorkflowEvent.ATTENTION_REQUIRED
        else:
            event = None
        if event is not None:
            apply_workflow_event(
                self.store,
                task_id=task_id,
                event=event,
                reason_code=reason,
                command_id=f"quality-repair-attention:{task_id}:{reason}",
            )
        with self.store._write() as connection:
            connection.execute(
                """
                UPDATE orch_tasks SET quality_status='fail', quality_reason_code=?
                WHERE id=?
                """,
                (reason, task_id),
            )

    def _assert_section_scope(
        self,
        source: ArtifactVersion,
        result: ArtifactVersion,
        *,
        allowed_sections: tuple[str, ...],
    ) -> None:
        if not allowed_sections:
            raise ConflictError("repair request has no authorized artifact sections")
        if source.mime_type != "text/markdown" or result.mime_type != "text/markdown":
            raise ConflictError("section-scoped repair currently supports Markdown only")
        allowed = {_section_key(item) for item in allowed_sections if _section_key(item)}
        source_order, source_sections = _markdown_sections(
            self.artifacts.blobs.get(str(source.blob_uri))
        )
        result_order, result_sections = _markdown_sections(
            self.artifacts.blobs.get(str(result.blob_uri))
        )
        changed = {
            key
            for key in set(source_sections).union(result_sections)
            if source_sections.get(key) != result_sections.get(key)
        }
        unauthorized = sorted(
            key
            for key in changed
            if key != "__preamble__" and key.split("__", 1)[0] not in allowed
        )
        if source_sections.get("__preamble__") != result_sections.get("__preamble__"):
            unauthorized.append("__preamble__")
        source_common = tuple(
            key for key in source_order if key.split("__", 1)[0] not in allowed
        )
        result_common = tuple(
            key for key in result_order if key.split("__", 1)[0] not in allowed
        )
        if source_common != result_common:
            unauthorized.append("__section_order__")
        if unauthorized:
            raise ConflictError(
                "repair changed sections outside its authorization: "
                + ", ".join(dict.fromkeys(unauthorized))
            )

    @staticmethod
    def _record(row) -> RepairRequest:
        return RepairRequest(
            id=row["id"], task_id=row["task_id"], source_artifact_id=row["source_artifact_id"],
            target_version=row["target_version"], finding_ids=tuple(json.loads(row["finding_ids_json"])),
            allowed_sections=tuple(json.loads(row["allowed_sections_json"])),
            required_validators=tuple(json.loads(row["required_validators_json"])),
            budget_allocation=json.loads(row["budget_allocation_json"]), attempt=row["attempt"],
            status=row["status"], result_artifact_id=row["result_artifact_id"],
        )
