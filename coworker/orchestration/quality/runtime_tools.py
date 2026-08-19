"""Run-bound Task Quality V2 tools with one canonical artifact read path.

The model never supplies task/run/lease/profile identity.  Every closure below is
bound to a :class:`RunExecutionContext`; mutating calls re-check the live lease and
artifact reads enforce the frozen strategy's direct input bindings.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from ..errors import ConflictError, NotFoundError
from ..handoff_models import jsonable
from ..profiles import AgentRole
from ..store import OrchestrationStore
from .artifact_security import normalize_sha256
from .evidence import EvidenceLedger
from .models import ArtifactVersionStatus
from .repair import RepairCoordinator
from .schemas import validate_model_result


QUALITY_READ_TOOL_NAMES = frozenset(
    {
        "get_task_contract",
        "get_repository_snapshot",
        "get_execution_strategy",
        "get_repository_inventory",
        "list_evidence_bundles",
        "list_artifacts",
        "get_artifact",
        "read_artifact",
        "list_work_products",
        "read_work_product_artifact",
        "read_snapshot_file",
        "search_snapshot",
        "git_snapshot_info",
        "get_repair_request",
    }
)

QUALITY_PRODUCER_TOOL_NAMES = frozenset(
    {
        "create_artifact",
        "append_artifact_chunk",
        "complete_artifact",
        "submit_evidence_bundle",
        "submit_analysis_result",
        "create_repaired_artifact",
    }
)

QUALITY_REVIEW_TOOL_NAMES = frozenset({"submit_quality_findings"})
QUALITY_TOOL_NAMES = (
    QUALITY_READ_TOOL_NAMES | QUALITY_PRODUCER_TOOL_NAMES | QUALITY_REVIEW_TOOL_NAMES
)


def quality_tool_names_for_role(role: AgentRole) -> frozenset[str]:
    """Return task-artifact capabilities without granting source-workspace writes."""

    if role in {AgentRole.WORKER, AgentRole.INTEGRATOR}:
        return QUALITY_READ_TOOL_NAMES | QUALITY_PRODUCER_TOOL_NAMES
    if role is AgentRole.EXPLORER:
        return QUALITY_READ_TOOL_NAMES | frozenset({"submit_evidence_bundle"})
    if role in {
        AgentRole.REVIEWER,
        AgentRole.TESTER,
        AgentRole.EVALUATOR,
        AgentRole.SCORER,
    }:
        return QUALITY_READ_TOOL_NAMES | QUALITY_REVIEW_TOOL_NAMES
    return QUALITY_READ_TOOL_NAMES


def _dump(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    return jsonable(value)


def _text_content(data: bytes, mime_type: str) -> dict[str, Any]:
    textual = mime_type.startswith("text/") or mime_type in {
        "application/json",
        "application/xml",
        "application/yaml",
        "application/x-yaml",
    }
    if textual:
        try:
            return {"encoding": "utf-8", "content": data.decode("utf-8")}
        except UnicodeDecodeError:
            pass
    return {"encoding": "base64", "content": base64.b64encode(data).decode("ascii")}


@dataclass(frozen=True)
class QualityRuntimeDependencies:
    store: OrchestrationStore
    contracts: Any
    snapshots: Any
    strategies: Any
    inventories: Any
    repo_tools: Any
    artifacts: Any


class TaskQualityRunToolFactory:
    """Build canonical V2 tools for one leased native Agent run."""

    def __init__(self, dependencies: QualityRuntimeDependencies) -> None:
        self.dependencies = dependencies

    def build(
        self,
        context: Any,
        report: dict[str, Any],
    ) -> list[Callable[..., Any]]:
        deps = self.dependencies
        store = deps.store
        task_id = context.task.id
        run_id = context.claim.run.id
        lease = context.claim.lease
        profile_id = context.profile.profile_id
        role = context.profile.role
        receipt_ids: dict[str, str] = {}
        evidence_ledger = EvidenceLedger(store, deps.snapshots)

        def assert_lease() -> None:
            store.renew_scheduler_fence()
            store.assert_run_lease(run_id, lease.token, lease.fencing_token)

        def active_identity() -> tuple[Any, Any, Any]:
            with store._read() as connection:
                row = connection.execute(
                    """
                    SELECT active_contract_id, active_snapshot_id, active_strategy_id
                    FROM orch_tasks WHERE id=?
                    """,
                    (task_id,),
                ).fetchone()
            if row is None:
                raise NotFoundError(f"task {task_id} not found")
            if not row["active_contract_id"]:
                raise ConflictError("task has no active published quality contract")
            if not row["active_snapshot_id"]:
                raise ConflictError("task has no active frozen repository snapshot")
            if not row["active_strategy_id"]:
                raise ConflictError("task has no active published execution strategy")
            return (
                deps.contracts.get(row["active_contract_id"]),
                deps.snapshots.get(row["active_snapshot_id"]),
                deps.strategies.get(row["active_strategy_id"]),
            )

        def producer_node_keys() -> set[str]:
            selected: set[str] = set()
            for raw in context.node.input.get("direct_bindings", ()):
                if not isinstance(raw, Mapping) or raw.get("source_type") != "artifact":
                    continue
                selector = raw.get("source_selector")
                if not isinstance(selector, Mapping):
                    continue
                key = str(selector.get("producer_node_key") or "")
                if key:
                    selected.add(key)
                selected.update(
                    str(item)
                    for item in selector.get("producer_node_keys", ())
                    if str(item)
                )
            return selected

        def allowed_artifact_ids() -> set[str]:
            allowed: set[str] = set()
            explicit: set[str] = set()
            for raw in context.node.input.get("direct_bindings", ()):
                if not isinstance(raw, Mapping) or raw.get("source_type") != "artifact":
                    continue
                selector = raw.get("source_selector")
                if isinstance(selector, Mapping) and selector.get("id"):
                    explicit.add(str(selector["id"]))
            allowed.update(explicit)
            keys = producer_node_keys()
            node_ids = {
                item.id for item in context.graph.nodes if item.key in keys
            }
            if node_ids:
                runs = [
                    item
                    for item in store.list_runs(task_id)
                    if item.plan_id == context.graph.plan.id
                    and item.node_id in node_ids
                    and item.status.value == "succeeded"
                ]
                latest_by_node: dict[str, Any] = {}
                for item in runs:
                    previous = latest_by_node.get(item.node_id)
                    if previous is None or (item.attempt, item.created_at, item.id) > (
                        previous.attempt,
                        previous.created_at,
                        previous.id,
                    ):
                        latest_by_node[item.node_id] = item
                run_ids = {item.id for item in latest_by_node.values()}
                for product in store.list_work_products(task_id, limit=10_000):
                    if product.run_id in run_ids and product.artifact_version_id:
                        allowed.add(product.artifact_version_id)
            with store._read() as connection:
                rows = connection.execute(
                    """
                    SELECT id FROM orch_artifact_versions
                    WHERE task_id=? AND run_id=? AND status<>'uploading'
                    """,
                    (task_id, run_id),
                ).fetchall()
                allowed.update(str(row["id"]) for row in rows)
                task = connection.execute(
                    "SELECT primary_artifact_id FROM orch_tasks WHERE id=?",
                    (task_id,),
                ).fetchone()
                if task is not None and task["primary_artifact_id"]:
                    # The active primary is visible only when the strategy explicitly
                    # binds an artifact or this is a repair worker.
                    if keys or role in {AgentRole.WORKER, AgentRole.INTEGRATOR}:
                        allowed.add(str(task["primary_artifact_id"]))
                repair = connection.execute(
                    """
                    SELECT source_artifact_id FROM orch_repair_requests
                    WHERE task_id=? AND status IN ('pending','running')
                    ORDER BY attempt DESC, created_at DESC LIMIT 1
                    """,
                    (task_id,),
                ).fetchone()
                if repair is not None and role in {AgentRole.WORKER, AgentRole.INTEGRATOR}:
                    allowed.add(str(repair["source_artifact_id"]))
            return allowed

        def authorize(artifact_id: str, expected_sha256: str) -> Any:
            artifact = deps.artifacts.get(str(artifact_id))
            if artifact.task_id != task_id:
                # Do not disclose cross-task metadata.
                raise PermissionError("artifact is outside this task namespace")
            if artifact.id not in allowed_artifact_ids():
                raise PermissionError("artifact is not authorized by this node's direct bindings")
            expected = normalize_sha256(expected_sha256)
            if artifact.sha256 != expected:
                raise PermissionError("artifact hash does not match its immutable binding")
            return artifact

        def get_task_contract() -> dict[str, Any]:
            """Return the complete active published Task Quality V2 contract."""

            contract, _snapshot, _strategy = active_identity()
            return _dump(contract)

        def get_repository_snapshot() -> dict[str, Any]:
            """Return frozen target metadata and immutable manifest identity."""

            _contract, snapshot, _strategy = active_identity()
            return _dump(snapshot)

        def get_execution_strategy() -> dict[str, Any]:
            """Return the frozen DAG, direct bindings, policy provenance and budget."""

            _contract, _snapshot, strategy = active_identity()
            return _dump(strategy)

        def get_repository_inventory() -> dict[str, Any]:
            """Return the shared inventory for the active frozen snapshot."""

            _contract, snapshot, _strategy = active_identity()
            with store._read() as connection:
                row = connection.execute(
                    """
                    SELECT id FROM orch_repository_inventories
                    WHERE snapshot_id=? ORDER BY generated_at DESC, id DESC LIMIT 1
                    """,
                    (snapshot.id,),
                ).fetchone()
            if row is None:
                raise NotFoundError("repository inventory has not been built")
            inventory, payload = deps.inventories.get(row["id"])
            return {"inventory": _dump(inventory), "payload": payload}

        def list_evidence_bundles() -> list[dict[str, Any]]:
            """List typed evidence-bundle results authorized to this node."""

            keys = producer_node_keys()
            node_ids = {item.id for item in context.graph.nodes if item.key in keys}
            result: list[dict[str, Any]] = []
            for product in store.list_work_products(task_id, limit=1_000):
                if product.metadata.get("schema_id") != "evidence_bundle_result_v2":
                    continue
                if node_ids and product.run_id:
                    source = store.get_run(product.run_id)
                    if source.node_id not in node_ids:
                        continue
                result.append(jsonable(product))
            return result

        def list_artifacts(
            deliverable_id: Optional[str] = None,
            status: Optional[str] = None,
        ) -> list[dict[str, Any]]:
            """List canonical artifact metadata allowed by direct bindings."""

            statuses = [ArtifactVersionStatus(status)] if status else None
            rows = deps.artifacts.list(
                task_id,
                logical_deliverable_id=deliverable_id,
                statuses=statuses,
                limit=1_000,
            )
            allowed = allowed_artifact_ids()
            return [_dump(item) for item in rows if item.id in allowed]

        def get_artifact(artifact_id: str, expected_sha256: str) -> dict[str, Any]:
            """Return canonical metadata only after task, binding and hash checks."""

            return _dump(authorize(artifact_id, expected_sha256))

        def read_artifact(
            artifact_id: str,
            expected_sha256: str,
            start_byte: int = 0,
            end_byte: Optional[int] = None,
        ) -> dict[str, Any]:
            """Read an exact immutable byte range and server-record review coverage."""

            artifact = authorize(artifact_id, expected_sha256)
            receipt_id: Optional[str] = None
            if role in {
                AgentRole.REVIEWER,
                AgentRole.TESTER,
                AgentRole.EVALUATOR,
                AgentRole.SCORER,
            }:
                receipt_id = receipt_ids.get(artifact.id)
                if receipt_id is None:
                    receipt = deps.artifacts.bind_candidate(
                        run_id=run_id,
                        artifact_id=artifact.id,
                        expected_sha256=expected_sha256,
                        verifier_profile_id=profile_id,
                        caller_task_id=task_id,
                    )
                    receipt_id = receipt.id
                    receipt_ids[artifact.id] = receipt_id
            value = deps.artifacts.read(
                artifact.id,
                expected_sha256=expected_sha256,
                start_byte=int(start_byte),
                end_byte=(int(end_byte) if end_byte is not None else None),
                caller_task_id=task_id,
                caller_run_id=run_id,
                receipt_id=receipt_id,
                allowed_artifact_ids=allowed_artifact_ids(),
            )
            content = bytes(value.pop("content"))
            return {**value, **_text_content(content, artifact.mime_type)}

        def list_work_products() -> list[dict[str, Any]]:
            """List compatibility Work Products without treating summaries as artifacts."""

            allowed = allowed_artifact_ids()
            keys = producer_node_keys()
            node_ids = {item.id for item in context.graph.nodes if item.key in keys}
            result: list[dict[str, Any]] = []
            for product in store.list_work_products(task_id, limit=1_000):
                if product.run_id and node_ids:
                    source = store.get_run(product.run_id)
                    if source.node_id not in node_ids:
                        continue
                if product.artifact_version_id and product.artifact_version_id not in allowed:
                    continue
                result.append(jsonable(product))
            return result

        def read_work_product_artifact(
            product_id: str,
            expected_sha256: str,
            start_byte: int = 0,
            end_byte: Optional[int] = None,
        ) -> dict[str, Any]:
            """Delegate a legacy Work Product read to the canonical artifact reader."""

            product = store.get_work_product(str(product_id))
            if product.task_id != task_id:
                raise PermissionError("work product is outside this task namespace")
            if not product.artifact_version_id:
                raise ConflictError(
                    "work product has no unique canonical artifact_version_id"
                )
            return read_artifact(
                product.artifact_version_id,
                expected_sha256,
                start_byte,
                end_byte,
            )

        def read_snapshot_file(
            path: str,
            start_line: int = 1,
            end_line: Optional[int] = None,
        ) -> dict[str, Any]:
            """Read a fixed-snapshot line range; never read the moving live checkout."""

            _contract, snapshot, _strategy = active_identity()
            return deps.repo_tools.read_snapshot_file(
                snapshot.id,
                path,
                start_line=int(start_line),
                end_line=(int(end_line) if end_line is not None else None),
            )

        def search_snapshot(
            query: str,
            paths: Optional[list[str]] = None,
            mode: str = "literal",
        ) -> dict[str, Any]:
            """Search the fixed snapshot through the shared immutable query cache."""

            _contract, snapshot, _strategy = active_identity()
            return deps.repo_tools.search_snapshot(
                snapshot.id,
                query=query,
                paths=tuple(paths or ()),
                mode=mode,
            )

        def git_snapshot_info() -> dict[str, Any]:
            """Return exact Git/ref/dirty identity for the frozen snapshot."""

            _contract, snapshot, _strategy = active_identity()
            return deps.repo_tools.git_snapshot_info(snapshot.id)

        def create_artifact(
            deliverable_id: str,
            filename: str,
            mime_type: str,
        ) -> dict[str, Any]:
            """Create a task-owned upload; this never writes the source workspace."""

            if role not in {AgentRole.WORKER, AgentRole.INTEGRATOR}:
                raise PermissionError("this role cannot create a deliverable artifact")
            assert_lease()
            return deps.artifacts.create(
                task_id,
                logical_deliverable_id=str(deliverable_id),
                filename=filename,
                mime_type=mime_type,
                run_id=run_id,
                attempt=context.claim.run.attempt,
                producer_profile_id=profile_id,
            )

        def append_artifact_chunk(
            upload_id: str,
            sequence: int,
            content: str,
            chunk_hash: str,
            encoding: str = "utf-8",
        ) -> dict[str, Any]:
            """Append one hash-checked contiguous artifact chunk."""

            assert_lease()
            if encoding == "base64":
                data: bytes | str = base64.b64decode(content, validate=True)
            elif encoding == "utf-8":
                data = content
            else:
                raise ValueError("artifact chunk encoding must be utf-8 or base64")
            return deps.artifacts.append(
                upload_id,
                sequence=int(sequence),
                content=data,
                chunk_hash=chunk_hash,
                caller_task_id=task_id,
                caller_run_id=run_id,
            )

        def complete_artifact(upload_id: str, expected_sha256: str) -> dict[str, Any]:
            """Finalize an upload into an immutable draft artifact version."""

            assert_lease()
            completed = deps.artifacts.complete(
                upload_id,
                expected_sha256=expected_sha256,
                caller_task_id=task_id,
                caller_run_id=run_id,
            )
            repair_id = None
            if completed.parent_artifact_id:
                with store._read() as connection:
                    repair = connection.execute(
                        """
                        SELECT id FROM orch_repair_requests
                        WHERE task_id=? AND source_artifact_id=?
                          AND status IN ('pending','running')
                        ORDER BY attempt DESC, created_at DESC LIMIT 1
                        """,
                        (task_id, completed.parent_artifact_id),
                    ).fetchone()
                if repair is None:
                    raise ConflictError(
                        "repaired artifact finalized without a matching active repair request"
                    )
                RepairCoordinator(store, deps.artifacts).complete(
                    str(repair["id"]),
                    result_artifact_id=completed.id,
                )
                repair_id = str(repair["id"])
            return {**_dump(completed), "repair_request_id": repair_id}

        def record_quality_result(payload: Mapping[str, Any], expected: str) -> dict[str, Any]:
            assert_lease()
            if report.get("quality_result") is not None:
                raise ConflictError("a quality result was already submitted for this run")
            validated = validate_model_result(
                dict(payload),
                expected_schema_id=expected,
                expected_schema_version=2,
            )
            canonical = validated.model_dump(mode="json")
            report["quality_result"] = canonical
            return {
                "ok": True,
                "accepted_for_settlement": True,
                "schema_id": expected,
                "result": canonical,
            }

        def submit_evidence_bundle(
            payload: dict[str, Any],
            records: Optional[dict[str, Any]] = None,
        ) -> dict[str, Any]:
            """Create server-bound evidence records and submit their canonical IDs.

            ``records.claims`` may contain a local ``key``, claim fields, ``evidence``
            file ranges, and ``negative_searches`` backed by a prior cached
            ``search_snapshot`` query. ``records.inventory_metrics`` are stored
            against the one active immutable inventory. The returned ``result`` is
            the strict ``evidence_bundle_result_v2`` payload to use for settlement.
            """

            if role is not AgentRole.EXPLORER:
                raise PermissionError("only the evidence explorer may submit a bundle")
            canonical = dict(payload)
            if records:
                assert_lease()
                contract, snapshot, _strategy = active_identity()
                expected_group = str(context.node.metadata.get("coverage_group") or "")
                if expected_group and canonical.get("coverage_group") != expected_group:
                    raise PermissionError(
                        "evidence bundle coverage_group differs from the frozen node"
                    )
                with store._read() as connection:
                    inventory_row = connection.execute(
                        """
                        SELECT id, artifact_id FROM orch_repository_inventories
                        WHERE snapshot_id=? ORDER BY generated_at DESC, id DESC LIMIT 1
                        """,
                        (snapshot.id,),
                    ).fetchone()
                if inventory_row is None:
                    raise ConflictError("active repository inventory is unavailable")
                requirement_ids = {item.id for item in contract.requirements}
                claim_ids: list[str] = []
                evidence_ids: list[str] = []
                negative_ids: list[str] = []
                local_claims: set[str] = set()
                for index, raw_claim in enumerate(records.get("claims") or ()):
                    claim_value = dict(raw_claim)
                    local_key = str(claim_value.get("key") or f"claim-{index + 1}")
                    if local_key in local_claims:
                        raise ValueError("evidence claim keys must be unique within a bundle")
                    local_claims.add(local_key)
                    chosen_requirements = tuple(
                        str(item) for item in claim_value.get("requirement_ids") or ()
                    )
                    if not set(chosen_requirements).issubset(requirement_ids):
                        raise ValueError("evidence claim references an undeclared requirement")
                    claim = evidence_ledger.create_claim(
                        task_id=task_id,
                        artifact_id=str(
                            claim_value.get("artifact_id") or inventory_row["artifact_id"]
                        ),
                        section_id=str(claim_value.get("section_id") or expected_group),
                        text=str(claim_value.get("text") or ""),
                        claim_type=str(claim_value.get("claim_type") or "fact"),
                        severity=str(claim_value.get("severity") or "info"),
                        confidence=float(claim_value.get("confidence", 1.0)),
                        requirement_ids=chosen_requirements,
                        source_key=f"{run_id}:{local_key}",
                    )
                    claim_ids.append(claim.id)
                    for raw_evidence in claim_value.get("evidence") or ():
                        evidence_value = dict(raw_evidence)
                        evidence = evidence_ledger.create_file_evidence(
                            claim_id=claim.id,
                            snapshot_id=snapshot.id,
                            path=str(evidence_value.get("path") or ""),
                            line_start=int(evidence_value.get("line_start") or 1),
                            line_end=int(
                                evidence_value.get("line_end")
                                or evidence_value.get("line_start")
                                or 1
                            ),
                            support=str(evidence_value.get("support") or "supports"),
                            created_by_run_id=run_id,
                        )
                        evidence_ids.append(evidence.id)
                    for raw_negative in claim_value.get("negative_searches") or ():
                        negative_value = dict(raw_negative)
                        query_key = str(negative_value.get("query_key") or "")
                        with store._read() as connection:
                            cached = connection.execute(
                                """
                                SELECT tool_version, result_hash FROM orch_repo_query_cache
                                WHERE query_key=? AND snapshot_id=? AND complete=1
                                """,
                                (query_key, snapshot.id),
                            ).fetchone()
                        if cached is None:
                            raise ValueError(
                                "negative evidence requires a complete cached snapshot search"
                            )
                        negative = evidence_ledger.create_negative_evidence(
                            claim_id=claim.id,
                            query=str(negative_value.get("query") or ""),
                            tool_version=str(cached["tool_version"]),
                            scope_paths=tuple(
                                str(item)
                                for item in negative_value.get("scope_paths") or ()
                            ),
                            excluded_paths=tuple(
                                str(item)
                                for item in negative_value.get("excluded_paths") or ()
                            ),
                            result_count=int(negative_value.get("result_count") or 0),
                            query_result_hash=str(cached["result_hash"]),
                            limitations=tuple(
                                str(item)
                                for item in negative_value.get("limitations") or ()
                            ),
                        )
                        negative_ids.append(negative.id)
                metric_ids: list[str] = []
                for raw_metric in records.get("inventory_metrics") or ():
                    metric_value = dict(raw_metric)
                    metric = evidence_ledger.record_inventory_metric(
                        inventory_id=str(inventory_row["id"]),
                        name=str(metric_value.get("name") or ""),
                        value=metric_value.get("value", 0),
                        unit=str(metric_value.get("unit") or "count"),
                        query_key=str(
                            metric_value.get("query_key")
                            or f"inventory:{inventory_row['id']}"
                        ),
                        subtotals=dict(metric_value.get("subtotals") or {}),
                        reconciles_to=metric_value.get("reconciles_to"),
                        tolerance=float(metric_value.get("tolerance") or 0),
                    )
                    metric_ids.append(metric.id)
                canonical["claim_ids"] = list(
                    dict.fromkeys((*canonical.get("claim_ids", ()), *claim_ids))
                )
                canonical["evidence_ref_ids"] = list(
                    dict.fromkeys(
                        (*canonical.get("evidence_ref_ids", ()), *evidence_ids)
                    )
                )
                canonical["negative_search_ids"] = list(
                    dict.fromkeys(
                        (*canonical.get("negative_search_ids", ()), *negative_ids)
                    )
                )
                canonical["inventory_metric_ids"] = list(
                    dict.fromkeys(
                        (*canonical.get("inventory_metric_ids", ()), *metric_ids)
                    )
                )

            return record_quality_result(canonical, "evidence_bundle_result_v2")

        def submit_analysis_result(payload: dict[str, Any]) -> dict[str, Any]:
            """Submit the typed primary-artifact result for server settlement."""

            return record_quality_result(payload, "analysis_report_result_v2")

        def submit_quality_findings(
            subject_artifact_id: str,
            subject_hash: str,
            summary: str,
            findings: list[dict[str, Any]],
            criterion_results: list[dict[str, Any]],
            verdict: str,
            rubric_dimension_scores: Optional[list[dict[str, Any]]] = None,
        ) -> dict[str, Any]:
            """Submit typed review findings; read coverage is derived by the server."""

            return record_quality_result(
                {
                    "schema_id": "review_result_v2",
                    "schema_version": 2,
                    "execution_status": "completed",
                    "summary": summary,
                    "subject_artifact_id": subject_artifact_id,
                    "subject_artifact_hash": subject_hash,
                    "criterion_results": criterion_results,
                    "findings": findings,
                    "verdict": verdict,
                    "rubric_dimension_scores": rubric_dimension_scores,
                },
                "review_result_v2",
            )

        def get_repair_request() -> dict[str, Any]:
            """Return the latest active bounded repair request for this task."""

            if role not in {AgentRole.WORKER, AgentRole.INTEGRATOR}:
                raise PermissionError("this role cannot execute artifact repair")
            with store._read() as connection:
                row = connection.execute(
                    """
                    SELECT * FROM orch_repair_requests
                    WHERE task_id=? AND status IN ('pending','running')
                    ORDER BY attempt DESC, created_at DESC LIMIT 1
                    """,
                    (task_id,),
                ).fetchone()
            if row is None:
                raise NotFoundError("task has no active repair request")
            return {
                "id": row["id"],
                "source_artifact_id": row["source_artifact_id"],
                "target_version": row["target_version"],
                "finding_ids": jsonable(json.loads(row["finding_ids_json"])),
                "allowed_sections": jsonable(json.loads(row["allowed_sections_json"])),
                "required_validators": jsonable(json.loads(row["required_validators_json"])),
                "budget_allocation": jsonable(json.loads(row["budget_allocation_json"])),
                "attempt": row["attempt"],
                "status": row["status"],
            }

        def create_repaired_artifact(
            parent_artifact_id: str,
            filename: str,
            mime_type: str,
        ) -> dict[str, Any]:
            """Create the exact immutable child version required by the active repair."""

            request = get_repair_request()
            if request["source_artifact_id"] != parent_artifact_id:
                raise PermissionError("repair parent is not the active request subject")
            parent = authorize(parent_artifact_id, deps.artifacts.get(parent_artifact_id).sha256)
            if filename != parent.filename or mime_type != parent.mime_type:
                raise ValueError(
                    "repair child must preserve the parent deliverable filename and MIME type"
                )
            assert_lease()
            created = deps.artifacts.create(
                task_id,
                logical_deliverable_id=parent.logical_deliverable_id,
                filename=filename,
                mime_type=mime_type,
                run_id=run_id,
                attempt=context.claim.run.attempt,
                producer_profile_id=profile_id,
                parent_artifact_id=parent_artifact_id,
            )
            with store._write() as connection:
                connection.execute(
                    "UPDATE orch_repair_requests SET status='running' WHERE id=? AND status='pending'",
                    (request["id"],),
                )
            return {**created, "repair_request_id": request["id"]}

        return [
            get_task_contract,
            get_repository_snapshot,
            get_execution_strategy,
            get_repository_inventory,
            list_evidence_bundles,
            list_artifacts,
            get_artifact,
            read_artifact,
            list_work_products,
            read_work_product_artifact,
            read_snapshot_file,
            search_snapshot,
            git_snapshot_info,
            create_artifact,
            append_artifact_chunk,
            complete_artifact,
            submit_evidence_bundle,
            submit_analysis_result,
            submit_quality_findings,
            get_repair_request,
            create_repaired_artifact,
        ]
