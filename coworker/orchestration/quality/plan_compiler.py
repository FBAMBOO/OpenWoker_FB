"""Compile a frozen V2 strategy/proposal into an executable immutable plan DAG."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, model_validator

from ..dag import validate_plan
from ..models import EdgeCondition, EdgeSpec, EffectSafety, NodeKind, NodeSpec, PlanSpec, RetryPolicy
from .models import ExecutionStrategy, NodeInputBinding, QualityModel, StrategyEdge, StrategyNode


class PlanProposalV2(QualityModel):
    schema_id: Literal["plan_proposal_v2"] = "plan_proposal_v2"
    schema_version: Literal[2] = 2
    nodes: tuple[StrategyNode, ...] = Field(min_length=1, max_length=64)
    edges: tuple[StrategyEdge, ...] = ()
    input_bindings: tuple[NodeInputBinding, ...] = ()
    rationale: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def _references(self) -> "PlanProposalV2":
        keys = [item.key for item in self.nodes]
        if len(keys) != len(set(keys)):
            raise ValueError("plan proposal node keys must be unique")
        known = set(keys)
        for edge in self.edges:
            if edge.source not in known or edge.target not in known or edge.source == edge.target:
                raise ValueError("plan proposal edge references an invalid node")
        for binding in self.input_bindings:
            if binding.consumer_node_key not in known:
                raise ValueError("plan proposal binding references an invalid consumer")
        return self


def _node_spec(node: StrategyNode, bindings: tuple[NodeInputBinding, ...]) -> NodeSpec:
    if node.deterministic:
        kind = NodeKind.NOOP
        agent = "orchestrator"
    elif node.role == "reviewer":
        kind, agent = NodeKind.REVIEW, "reviewer"
    elif node.role == "worker":
        kind, agent = NodeKind.EXECUTE, "worker"
    else:
        kind, agent = NodeKind.AGENT, node.role
    direct = [
        item.model_dump(mode="json") for item in bindings if item.consumer_node_key == node.key
    ]
    return NodeSpec(
        key=node.key,
        title=node.kind.replace("_", " ").title(),
        instructions=(
            "Execute only the frozen Task Quality V2 strategy node. Read inputs through "
            "the listed direct bindings; ancestor summaries and private transcripts are not inputs."
        ),
        kind=kind,
        agent=agent,
        input={"direct_bindings": direct, "quality_node_config": dict(node.config)},
        effect_safety=EffectSafety.READ_ONLY,
        retry_policy=RetryPolicy(max_attempts=2 if not node.deterministic else 1),
        metadata={
            "task_quality_v2": True,
            "strategy_kind": node.kind,
            "coverage_group": node.coverage_group,
            "deterministic": node.deterministic,
        },
    )


def compile_strategy_plan(
    strategy: ExecutionStrategy,
    *,
    proposal: PlanProposalV2 | None = None,
) -> PlanSpec:
    """Validate schema, cycle, policy, bindings and budget before freezing a DAG."""

    nodes = proposal.nodes if proposal else strategy.nodes
    edges = proposal.edges if proposal else strategy.edges
    bindings = proposal.input_bindings if proposal else strategy.input_bindings
    if proposal is not None:
        # A model planner may refine configuration but cannot expand roles, remove hard
        # stages, change coverage ownership, or create new authority.
        expected = {item.key: item for item in strategy.nodes}
        submitted = {item.key: item for item in nodes}
        if set(submitted) != set(expected):
            raise ValueError("plan proposal must preserve the frozen strategy node set")
        for key, item in submitted.items():
            baseline = expected[key]
            if (item.role, item.kind, item.coverage_group, item.deterministic) != (
                baseline.role,
                baseline.kind,
                baseline.coverage_group,
                baseline.deterministic,
            ):
                raise PermissionError(f"plan proposal attempted to change authority for node {key}")
    coverage: set[str] = set()
    for node in nodes:
        raw_areas = node.config.get("areas", ())
        for area in raw_areas if isinstance(raw_areas, (list, tuple)) else ():
            name = str(area)
            if name in coverage:
                raise ValueError(f"collector coverage area is not mutually exclusive: {name}")
            coverage.add(name)
    known = {item.key for item in nodes}
    required_consumers = {item.consumer_node_key for item in bindings if item.requirement.value == "required"}
    model_nodes = {item.key for item in nodes if not item.deterministic}
    if not model_nodes.issubset(required_consumers):
        raise ValueError(
            f"model nodes lack required direct input binding: {sorted(model_nodes - required_consumers)}"
        )
    specs = tuple(_node_spec(item, tuple(bindings)) for item in nodes)
    edge_specs = tuple(
        EdgeSpec(
            from_node=item.source,
            to_node=item.target,
            condition=(
                EdgeCondition.SUCCESS if item.condition in {"success", "publish"} else EdgeCondition.ALWAYS
            ),
            required=True,
            metadata={"quality_condition": item.condition},
        )
        for item in edges
    )
    plan = PlanSpec(
        nodes=specs,
        edges=edge_specs,
        metadata={
            "generated": "task-quality-v2",
            "strategy_id": strategy.id,
            "strategy_hash": strategy.content_hash,
            "template_id": strategy.template_id,
            "semantic_scorer_node_key": strategy.semantic_scorer_node_key,
            "effective_policy": dict(strategy.effective_policy),
            "budget_profile": dict(strategy.budget_profile),
        },
    )
    validate_plan(plan)
    limits = dict(strategy.budget_profile.get("limits") or {})
    allocations = dict(strategy.budget_profile.get("node_allocations") or {})
    reserved = sum(int(value.get("reserved_reported_tokens") or 0) for value in allocations.values())
    token_limit = limits.get("reported_tokens")
    if token_limit is not None and reserved > int(token_limit):
        raise ValueError("strategy model token reservations exceed the root budget")
    if strategy.semantic_scorer_node_key not in known:
        raise ValueError("semantic scorer is absent from the compiled DAG")
    return plan
