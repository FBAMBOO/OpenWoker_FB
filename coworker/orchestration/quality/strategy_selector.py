"""Adaptive, provenance-rich execution strategies for Task Quality V2."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from ..errors import ConflictError, NotFoundError
from ..store import OrchestrationStore
from .assessment import assess_task
from .models import (
    Archetype,
    Assessment,
    BindingDeliveryMode,
    BindingRequirement,
    BindingSourceType,
    ExecutionStrategy,
    NodeInputBinding,
    RepositoryInventory,
    RepositorySnapshot,
    StrategyEdge,
    StrategyNode,
    TaskContractV2,
    model_content_sha256,
)
from .rubrics import rubric_for_archetype
from .state_machine import WorkflowEvent, transition_workflow_in_transaction


STRATEGY_VERSION = "strategy-selector@1"
REPO_AREAS = ("entry", "models", "macros", "tests", "seeds", "snapshots", "deployment")


def _locked_effective_policy(
    base: dict[str, Any], overrides: dict[str, Any] | None
) -> dict[str, Any]:
    """Accept confirmations, never request-body authority escalation."""

    for key, value in dict(overrides or {}).items():
        if key not in base:
            raise ValueError(f"unsupported Task Quality effective policy key: {key}")
        if value != base[key]:
            raise PermissionError(
                f"Task Quality effective policy {key} is frozen by security/profile precedence"
            )
    return dict(base)


def _locked_feature_flags(
    base: dict[str, str | bool], overrides: dict[str, str | bool] | None
) -> dict[str, str | bool]:
    """Keep integrity rails immutable while allowing bounded rollout switches."""

    requested = dict(overrides or {})
    allowed_mutable = {
        "auto_repair",
        "auto_repair_enabled",
        "codex_parity_readonly_profile_enabled",
    }
    unknown = sorted(set(requested).difference(base))
    if unknown:
        raise ValueError(
            "unsupported Task Quality feature flags: " + ", ".join(unknown)
        )
    for key, value in requested.items():
        if not isinstance(value, (bool, str)):
            raise ValueError(f"Task Quality feature flag {key} has an invalid value")
        if key not in allowed_mutable and value != base[key]:
            raise PermissionError(
                f"Task Quality integrity flag {key} cannot be overridden"
            )
    if "auto_repair" in requested and not isinstance(requested["auto_repair"], bool):
        raise ValueError("auto_repair must be boolean")
    if "auto_repair_enabled" in requested and not isinstance(
        requested["auto_repair_enabled"], bool
    ):
        raise ValueError("auto_repair_enabled must be boolean")
    requested_auto = {
        bool(requested[key])
        for key in ("auto_repair", "auto_repair_enabled")
        if key in requested
    }
    if len(requested_auto) > 1:
        raise ValueError("auto_repair feature flag aliases disagree")
    auto_repair = next(iter(requested_auto), bool(base.get("auto_repair", False)))
    parity = requested.get(
        "codex_parity_readonly_profile_enabled",
        base["codex_parity_readonly_profile_enabled"],
    )
    if not isinstance(parity, bool):
        raise ValueError("codex_parity_readonly_profile_enabled must be boolean")
    return {
        **base,
        "auto_repair": auto_repair,
        "auto_repair_enabled": auto_repair,
        "codex_parity_readonly_profile_enabled": parity,
    }


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _coverage_groups(workload: int) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if workload < 35:
        return (("repository", REPO_AREAS),)
    if workload < 70:
        return (
            ("architecture", ("entry", "models", "macros")),
            ("quality_control", ("tests", "seeds", "snapshots", "deployment")),
        )
    return (
        ("entry_models", ("entry", "models")),
        ("macros_lifecycle", ("macros",)),
        ("quality_data", ("tests", "seeds", "snapshots")),
        ("control_plane", ("deployment",)),
    )


def _binding(
    consumer: str,
    source_type: BindingSourceType,
    selector: dict[str, Any],
    *,
    max_bytes: int = 262_144,
) -> NodeInputBinding:
    return NodeInputBinding(
        consumer_node_key=consumer,
        source_type=source_type,
        source_selector=selector,
        requirement=BindingRequirement.REQUIRED,
        delivery_mode=BindingDeliveryMode.ON_DEMAND,
        max_bytes=max_bytes,
        must_verify_hash=True,
    )


def _allocation(
    tokens: int,
    *,
    model_calls: int,
    tool_calls: int,
    active_seconds: int,
    tool_payload_bytes: int,
) -> dict[str, int]:
    return {
        "min_reported_tokens": 0 if tokens == 0 else max(1, tokens // 4),
        "reserved_reported_tokens": tokens,
        "max_reported_tokens": max(tokens, int(tokens * 1.35)),
        "model_calls": model_calls,
        "tool_calls": tool_calls,
        "active_seconds": active_seconds,
        "tool_payload_bytes": tool_payload_bytes,
    }


class StrategySelector:
    def __init__(self, store: OrchestrationStore | None = None) -> None:
        self.store = store

    def select(
        self,
        *,
        contract: TaskContractV2,
        snapshot: RepositorySnapshot,
        inventory: RepositoryInventory | None = None,
        assessment: Assessment | None = None,
        version: int = 1,
        explicit_policy: dict[str, Any] | None = None,
        feature_flags: dict[str, str | bool] | None = None,
    ) -> ExecutionStrategy:
        if contract.task_id != snapshot.task_id:
            raise PermissionError("contract and snapshot must belong to the same task")
        if inventory is not None and inventory.snapshot_id != snapshot.id:
            raise ValueError("inventory is not bound to the selected snapshot")
        assessment = assessment or assess_task(
            contract,
            file_count=inventory.file_count if inventory else 0,
            total_bytes=inventory.total_bytes if inventory else 0,
        )
        if contract.archetype is Archetype.FOCUSED_QUESTION:
            return self._focused_strategy(
                contract=contract,
                snapshot=snapshot,
                assessment=assessment,
                version=version,
                explicit_policy=explicit_policy,
                feature_flags=feature_flags,
            )
        if contract.archetype is not Archetype.REPO_ANALYSIS:
            raise ValueError(
                f"adaptive strategy template is not implemented for {contract.archetype.value}"
            )
        return self._repo_analysis_strategy(
            contract=contract,
            snapshot=snapshot,
            inventory=inventory,
            assessment=assessment,
            version=version,
            explicit_policy=explicit_policy,
            feature_flags=feature_flags,
        )

    def _repo_analysis_strategy(
        self,
        *,
        contract: TaskContractV2,
        snapshot: RepositorySnapshot,
        inventory: RepositoryInventory | None,
        assessment: Assessment,
        version: int,
        explicit_policy: dict[str, Any] | None,
        feature_flags: dict[str, str | bool] | None,
    ) -> ExecutionStrategy:
        groups = _coverage_groups(assessment.evidence_workload)
        collectors = tuple(
            StrategyNode(
                key=f"collect_{index + 1}",
                role="explorer",
                kind="evidence_collector",
                coverage_group=name,
                config={"areas": list(areas), "result_schema_id": "evidence_bundle_result_v2"},
            )
            for index, (name, areas) in enumerate(groups)
        )
        nodes = (
            StrategyNode(
                key="resolve_inventory",
                role="service",
                kind="resolve_inventory",
                deterministic=True,
            ),
            *collectors,
            StrategyNode(
                key="synthesize",
                role="worker",
                kind="synthesize_artifact",
                config={"result_schema_id": "analysis_report_result_v2"},
            ),
            StrategyNode(
                key="validate",
                role="service",
                kind="deterministic_validation",
                deterministic=True,
            ),
            StrategyNode(
                key="review",
                role="reviewer",
                kind="independent_review",
                config={"result_schema_id": "review_result_v2", "read_coverage_required": 1.0},
            ),
            StrategyNode(
                key="adjudicate",
                role="service",
                kind="server_adjudication",
                deterministic=True,
            ),
            StrategyNode(
                key="publish",
                role="service",
                kind="publish_verified_artifact",
                deterministic=True,
            ),
        )
        edges: list[StrategyEdge] = []
        for collector in collectors:
            edges.append(StrategyEdge(source="resolve_inventory", target=collector.key))
            edges.append(StrategyEdge(source=collector.key, target="synthesize"))
        edges.extend(
            (
                StrategyEdge(source="synthesize", target="validate"),
                StrategyEdge(source="validate", target="review"),
                StrategyEdge(source="review", target="adjudicate"),
                StrategyEdge(source="adjudicate", target="publish", condition="publish"),
            )
        )
        bindings: list[NodeInputBinding] = []
        for collector in collectors:
            bindings.extend(
                (
                    _binding(collector.key, BindingSourceType.CONTRACT, {"id": contract.id}),
                    _binding(collector.key, BindingSourceType.SNAPSHOT, {"id": snapshot.id}),
                    _binding(
                        collector.key,
                        BindingSourceType.INVENTORY,
                        {"id": inventory.id if inventory else "build_once"},
                    ),
                )
            )
            bindings.append(
                _binding(
                    "synthesize",
                    BindingSourceType.EVIDENCE_BUNDLE,
                    {"producer_node_key": collector.key, "coverage_group": collector.coverage_group},
                    max_bytes=1_048_576,
                )
            )
        bindings.extend(
            (
                _binding("synthesize", BindingSourceType.CONTRACT, {"id": contract.id}),
                _binding("synthesize", BindingSourceType.SNAPSHOT, {"id": snapshot.id}),
                _binding("validate", BindingSourceType.ARTIFACT, {"producer_node_key": "synthesize"}, max_bytes=8_388_608),
                _binding("validate", BindingSourceType.CONTRACT, {"id": contract.id}),
                _binding("validate", BindingSourceType.SNAPSHOT, {"id": snapshot.id}),
                _binding("review", BindingSourceType.ARTIFACT, {"producer_node_key": "synthesize"}, max_bytes=8_388_608),
                _binding("review", BindingSourceType.CONTRACT, {"id": contract.id}),
                _binding("review", BindingSourceType.SNAPSHOT, {"id": snapshot.id}),
                _binding("review", BindingSourceType.EVIDENCE_BUNDLE, {"producer_node_keys": [item.key for item in collectors]}, max_bytes=2_097_152),
                _binding("adjudicate", BindingSourceType.ARTIFACT, {"producer_node_key": "synthesize"}),
                _binding("adjudicate", BindingSourceType.FINDING_SET, {"producer_node_keys": ["validate", "review"]}),
                _binding("publish", BindingSourceType.ARTIFACT, {"producer_node_key": "synthesize"}),
            )
        )
        token_limit = 3_000_000
        collector_total = 1_350_000
        per_collector = collector_total // len(collectors)
        allocations = {
            item.key: _allocation(
                per_collector,
                model_calls=10,
                tool_calls=20,
                active_seconds=150,
                tool_payload_bytes=16 * 1024 * 1024,
            )
            for item in collectors
        }
        allocations["synthesize"] = _allocation(
            1_050_000,
            model_calls=30,
            tool_calls=15,
            active_seconds=300,
            tool_payload_bytes=32 * 1024 * 1024,
        )
        allocations["review"] = _allocation(
            450_000,
            model_calls=20,
            tool_calls=10,
            active_seconds=240,
            tool_payload_bytes=16 * 1024 * 1024,
        )
        for node in nodes:
            if node.deterministic:
                allocations[node.key] = _allocation(
                    0,
                    model_calls=0,
                    tool_calls=0,
                    active_seconds=1,
                    tool_payload_bytes=0,
                )
        effective_policy = _locked_effective_policy({
            "source_workspace_write": {"value": False, "source": "security_ceiling"},
            "task_artifact_write": {"value": True, "source": "archetype_invariant"},
            "network_access": {"value": False, "source": "contract_constraint"},
            "collector_count": {"value": len(collectors), "source": "evidence_assessment"},
            "independent_review": {"value": True, "source": "quality_profile"},
            "semantic_scorer": {"value": "review", "source": "quality_profile"},
            "runtime_multi_agent": {"value": False, "source": "security_default"},
        }, explicit_policy)
        flags = _locked_feature_flags({
            "task_quality_v2_enabled": True,
            "contract_compiler_v2_enabled": True,
            "artifact_v2_enabled": True,
            "repository_snapshot_enabled": True,
            "repository_snapshot_required": True,
            "typed_result_contract_v2_required": True,
            "work_product_artifact_read_enabled": True,
            "adaptive_strategy_enabled": True,
            "semantic_quality_gate_enabled": True,
            "auto_repair": False,
            "auto_repair_enabled": False,
            "runtime_budget_enforcement_mode": "hard",
            "codex_parity_readonly_profile_enabled": False,
        }, feature_flags)
        rubric = rubric_for_archetype(contract.archetype)
        created_at = _now()
        draft = ExecutionStrategy(
            id=f"strategy_{uuid.uuid4().hex}",
            task_id=contract.task_id,
            version=version,
            archetype=contract.archetype,
            template_id="repo-analysis-v2",
            contract_id=contract.id,
            snapshot_id=snapshot.id,
            rubric_id=rubric.id,
            assessment=assessment,
            effective_policy=effective_policy,
            feature_flags=flags,
            nodes=nodes,
            edges=tuple(edges),
            input_bindings=tuple(bindings),
            semantic_scorer_node_key="review",
            budget_profile={
                "id": "repo-analysis-hard-v1",
                "mode": "hard",
                "source": "quality_profile",
                "limits": {
                    "model_calls": 100,
                    "tool_calls": 120,
                    "reported_tokens": token_limit,
                    "active_seconds": 1_200,
                    "tool_payload_bytes": 128 * 1024 * 1024,
                },
                "warning_thresholds": [0.8, 0.95],
                "node_allocations": allocations,
            },
            max_repair_attempts=2,
            content_hash="sha256:" + "0" * 64,
            created_at=created_at,
        )
        return draft.model_copy(update={"content_hash": model_content_sha256(draft)})

    def _focused_strategy(
        self,
        *,
        contract: TaskContractV2,
        snapshot: RepositorySnapshot,
        assessment: Assessment,
        version: int,
        explicit_policy: dict[str, Any] | None,
        feature_flags: dict[str, str | bool] | None,
    ) -> ExecutionStrategy:
        rubric = rubric_for_archetype(contract.archetype)
        nodes = (
            StrategyNode(
                key="answer",
                role="worker",
                kind="focused_answer",
                config={"result_schema_id": "analysis_report_result_v2"},
            ),
            StrategyNode(
                key="validate",
                role="service",
                kind="deterministic_validation",
                deterministic=True,
            ),
            StrategyNode(
                key="review",
                role="reviewer",
                kind="independent_review",
                config={"result_schema_id": "review_result_v2", "read_coverage_required": 1.0},
            ),
            StrategyNode(
                key="adjudicate",
                role="service",
                kind="server_adjudication",
                deterministic=True,
            ),
            StrategyNode(
                key="publish",
                role="service",
                kind="publish_verified_artifact",
                deterministic=True,
            ),
        )
        draft = ExecutionStrategy(
            id=f"strategy_{uuid.uuid4().hex}", task_id=contract.task_id, version=version,
            archetype=contract.archetype, template_id="focused-question-v2",
            contract_id=contract.id, snapshot_id=snapshot.id,
            rubric_id=rubric.id, assessment=assessment,
            effective_policy=_locked_effective_policy({
                "source_workspace_write": {"value": False, "source": "security_ceiling"},
                "task_artifact_write": {"value": True, "source": "archetype_invariant"},
                "network_access": {"value": False, "source": "contract_constraint"},
                "independent_review": {"value": True, "source": "quality_profile"},
                "semantic_scorer": {"value": "review", "source": "quality_profile"},
                "runtime_multi_agent": {"value": False, "source": "security_default"},
            }, explicit_policy),
            feature_flags=_locked_feature_flags({
                "task_quality_v2_enabled": True,
                "contract_compiler_v2_enabled": True,
                "artifact_v2_enabled": True,
                "repository_snapshot_enabled": True,
                "repository_snapshot_required": True,
                "typed_result_contract_v2_required": True,
                "work_product_artifact_read_enabled": True,
                "adaptive_strategy_enabled": True,
                "semantic_quality_gate_enabled": True,
                "auto_repair": False,
                "auto_repair_enabled": False,
                "runtime_budget_enforcement_mode": "hard",
                "codex_parity_readonly_profile_enabled": False,
            }, feature_flags),
            nodes=nodes,
            edges=(
                StrategyEdge(source="answer", target="validate"),
                StrategyEdge(source="validate", target="review"),
                StrategyEdge(source="review", target="adjudicate"),
                StrategyEdge(source="adjudicate", target="publish", condition="publish"),
            ),
            input_bindings=(
                _binding("answer", BindingSourceType.CONTRACT, {"id": contract.id}),
                _binding("answer", BindingSourceType.SNAPSHOT, {"id": snapshot.id}),
                _binding("validate", BindingSourceType.ARTIFACT, {"producer_node_key": "answer"}),
                _binding("validate", BindingSourceType.CONTRACT, {"id": contract.id}),
                _binding("validate", BindingSourceType.SNAPSHOT, {"id": snapshot.id}),
                _binding("review", BindingSourceType.ARTIFACT, {"producer_node_key": "answer"}),
                _binding("review", BindingSourceType.CONTRACT, {"id": contract.id}),
                _binding("review", BindingSourceType.SNAPSHOT, {"id": snapshot.id}),
                _binding("adjudicate", BindingSourceType.ARTIFACT, {"producer_node_key": "answer"}),
                _binding(
                    "adjudicate",
                    BindingSourceType.FINDING_SET,
                    {"producer_node_keys": ["validate", "review"]},
                ),
                _binding("publish", BindingSourceType.ARTIFACT, {"producer_node_key": "answer"}),
            ),
            semantic_scorer_node_key="review",
            budget_profile={
                "id": "focused-hard-v1", "mode": "hard", "source": "archetype",
                "limits": {"model_calls": 8, "tool_calls": 20, "reported_tokens": 200_000,
                           "active_seconds": 300, "tool_payload_bytes": 16 * 1024 * 1024},
                "warning_thresholds": [0.8, 0.95],
                "node_allocations": {
                    "answer": _allocation(
                        120_000,
                        model_calls=4,
                        tool_calls=10,
                        active_seconds=150,
                        tool_payload_bytes=8 * 1024 * 1024,
                    ),
                    "validate": _allocation(
                        0,
                        model_calls=0,
                        tool_calls=0,
                        active_seconds=1,
                        tool_payload_bytes=0,
                    ),
                    "review": _allocation(
                        60_000,
                        model_calls=3,
                        tool_calls=8,
                        active_seconds=120,
                        tool_payload_bytes=7 * 1024 * 1024,
                    ),
                    "adjudicate": _allocation(
                        0,
                        model_calls=0,
                        tool_calls=0,
                        active_seconds=1,
                        tool_payload_bytes=0,
                    ),
                    "publish": _allocation(
                        0,
                        model_calls=0,
                        tool_calls=0,
                        active_seconds=1,
                        tool_payload_bytes=0,
                    ),
                },
            },
            max_repair_attempts=1, content_hash="sha256:" + "0" * 64, created_at=_now(),
        )
        return draft.model_copy(update={"content_hash": model_content_sha256(draft)})

    def publish(self, strategy: ExecutionStrategy) -> ExecutionStrategy:
        if self.store is None:
            raise RuntimeError("strategy persistence requires an OrchestrationStore")
        if strategy.content_hash != model_content_sha256(strategy):
            raise ValueError("strategy content hash mismatch")
        rubric = rubric_for_archetype(strategy.archetype)
        now = _now().isoformat().replace("+00:00", "Z")
        with self.store._write() as connection:
            task = connection.execute(
                """
                SELECT active_contract_id, active_snapshot_id, workflow_status
                FROM orch_tasks WHERE id=?
                """,
                (strategy.task_id,),
            ).fetchone()
            if task is None:
                raise NotFoundError(f"task {strategy.task_id} not found")
            if task["active_contract_id"] != strategy.contract_id:
                raise ConflictError("strategy contract is not the task's active published contract")
            if task["active_snapshot_id"] != strategy.snapshot_id:
                raise ConflictError("strategy snapshot is not the task's active frozen snapshot")
            connection.execute(
                """
                INSERT OR IGNORE INTO orch_quality_rubrics(
                    id, version, name, applicable_archetypes_json, dimensions_json,
                    pass_threshold, content_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rubric.id, rubric.version, rubric.name,
                    _json([item.value for item in rubric.applicable_archetypes]),
                    _json([item.model_dump(mode="json") for item in rubric.dimensions]),
                    rubric.pass_threshold, rubric.content_hash, now,
                ),
            )
            existing = connection.execute(
                "SELECT content_hash FROM orch_execution_strategies WHERE id=?", (strategy.id,)
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] != strategy.content_hash:
                    raise ConflictError("strategy id was replayed with different content")
                return strategy
            connection.execute(
                """
                INSERT INTO orch_execution_strategies(
                    id, task_id, version, status, archetype, template_id, contract_id,
                    snapshot_id, rubric_id, rubric_version, assessment_json,
                    effective_policy_json, policy_provenance_json, feature_flags_json,
                    nodes_json, edges_json, semantic_scorer_node_key, budget_profile_json,
                    max_repair_attempts, content_hash, created_at, published_at
                ) VALUES (?, ?, ?, 'published', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    strategy.id, strategy.task_id, strategy.version, strategy.archetype.value,
                    strategy.template_id, strategy.contract_id, strategy.snapshot_id,
                    rubric.id, rubric.version, _json(strategy.assessment.model_dump(mode="json")),
                    _json(dict(strategy.effective_policy)),
                    _json({key: value.get("source") for key, value in strategy.effective_policy.items() if isinstance(value, dict)}),
                    _json(dict(strategy.feature_flags)),
                    _json([item.model_dump(mode="json") for item in strategy.nodes]),
                    _json([item.model_dump(mode="json") for item in strategy.edges]),
                    strategy.semantic_scorer_node_key, _json(dict(strategy.budget_profile)),
                    strategy.max_repair_attempts, strategy.content_hash, now, now,
                ),
            )
            for position, binding in enumerate(strategy.input_bindings):
                connection.execute(
                    """
                    INSERT INTO orch_node_input_bindings(
                        id, strategy_id, position, consumer_node_key, source_type,
                        source_selector_json, requirement, delivery_mode, max_bytes,
                        must_verify_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"binding_{uuid.uuid4().hex}", strategy.id, position,
                        binding.consumer_node_key, binding.source_type.value,
                        _json(dict(binding.source_selector)), binding.requirement.value,
                        binding.delivery_mode.value, binding.max_bytes,
                        int(binding.must_verify_hash),
                    ),
                )
            connection.execute(
                "UPDATE orch_tasks SET active_strategy_id=? WHERE id=?",
                (strategy.id, strategy.task_id),
            )
            transition_workflow_in_transaction(
                self.store,
                connection,
                task_id=strategy.task_id,
                event=WorkflowEvent.ANALYSIS_READY,
                command_id=f"quality-strategy-publish:{strategy.id}",
            )
        return strategy

    def get(self, strategy_id: str) -> ExecutionStrategy:
        if self.store is None:
            raise RuntimeError("strategy persistence requires an OrchestrationStore")
        with self.store._read() as connection:
            row = connection.execute(
                "SELECT * FROM orch_execution_strategies WHERE id=?", (strategy_id,)
            ).fetchone()
            bindings = connection.execute(
                "SELECT * FROM orch_node_input_bindings WHERE strategy_id=? ORDER BY position",
                (strategy_id,),
            ).fetchall()
        if row is None:
            raise NotFoundError(f"strategy {strategy_id} not found")
        return ExecutionStrategy(
            id=row["id"], task_id=row["task_id"], version=row["version"],
            archetype=row["archetype"], template_id=row["template_id"],
            contract_id=row["contract_id"], snapshot_id=row["snapshot_id"],
            rubric_id=row["rubric_id"], assessment=json.loads(row["assessment_json"]),
            effective_policy=json.loads(row["effective_policy_json"]),
            feature_flags=json.loads(row["feature_flags_json"]),
            nodes=tuple(json.loads(row["nodes_json"])), edges=tuple(json.loads(row["edges_json"])),
            input_bindings=tuple(
                {
                    "consumer_node_key": item["consumer_node_key"],
                    "source_type": item["source_type"],
                    "source_selector": json.loads(item["source_selector_json"]),
                    "requirement": item["requirement"],
                    "delivery_mode": item["delivery_mode"],
                    "max_bytes": item["max_bytes"],
                    "must_verify_hash": bool(item["must_verify_hash"]),
                }
                for item in bindings
            ),
            semantic_scorer_node_key=row["semantic_scorer_node_key"],
            budget_profile=json.loads(row["budget_profile_json"]),
            max_repair_attempts=row["max_repair_attempts"], content_hash=row["content_hash"],
            created_at=row["created_at"],
        )
