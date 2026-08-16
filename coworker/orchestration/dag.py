"""Deterministic validation and ordering for immutable plan DAGs."""

from __future__ import annotations

import heapq
from collections import defaultdict
from typing import Iterable, Sequence

from .errors import DAGValidationError
from .models import EdgeSpec, NodeSpec, PlanSpec


MAX_PLAN_WORK_UNITS = 64
MAX_ATTEMPTS_PER_NODE = 3


def validate_dag(
    nodes: Sequence[NodeSpec], edges: Sequence[EdgeSpec]
) -> tuple[str, ...]:
    """Validate a graph with Kahn's algorithm and return a stable topological order."""

    if not nodes:
        raise DAGValidationError("a plan must contain at least one node")
    if len(nodes) > MAX_PLAN_WORK_UNITS:
        raise DAGValidationError(
            f"a plan cannot exceed {MAX_PLAN_WORK_UNITS} work units"
        )

    positions: dict[str, int] = {}
    for index, node in enumerate(nodes):
        if not node.key or node.key.strip() != node.key:
            raise DAGValidationError("node keys must be non-empty and trimmed")
        if node.key in positions:
            raise DAGValidationError(f"duplicate node key: {node.key}")
        if node.retry_policy.max_attempts < 1:
            raise DAGValidationError(f"node {node.key} max_attempts must be >= 1")
        if node.retry_policy.max_attempts > MAX_ATTEMPTS_PER_NODE:
            raise DAGValidationError(
                f"node {node.key} max_attempts cannot exceed {MAX_ATTEMPTS_PER_NODE}"
            )
        if node.retry_policy.initial_delay_seconds < 0:
            raise DAGValidationError(
                f"node {node.key} initial retry delay must be non-negative"
            )
        if node.retry_policy.multiplier < 1:
            raise DAGValidationError(
                f"node {node.key} retry multiplier must be >= 1"
            )
        if node.retry_policy.max_delay_seconds < 0:
            raise DAGValidationError(
                f"node {node.key} maximum retry delay must be non-negative"
            )
        if not 0 <= node.retry_policy.jitter <= 1:
            raise DAGValidationError(
                f"node {node.key} retry jitter must be between 0 and 1"
            )
        if node.timeout_seconds < 1:
            raise DAGValidationError(f"node {node.key} timeout_seconds must be >= 1")
        positions[node.key] = index

    outgoing: dict[str, list[str]] = defaultdict(list)
    indegree = {key: 0 for key in positions}
    seen_pairs: set[tuple[str, str]] = set()
    for edge in edges:
        if edge.from_node not in positions:
            raise DAGValidationError(f"unknown edge source: {edge.from_node}")
        if edge.to_node not in positions:
            raise DAGValidationError(f"unknown edge target: {edge.to_node}")
        if edge.from_node == edge.to_node:
            raise DAGValidationError(f"self edge is not allowed: {edge.from_node}")
        pair = (edge.from_node, edge.to_node)
        if pair in seen_pairs:
            raise DAGValidationError(
                f"duplicate edge: {edge.from_node} -> {edge.to_node}"
            )
        seen_pairs.add(pair)
        outgoing[edge.from_node].append(edge.to_node)
        indegree[edge.to_node] += 1

    ready = [(positions[key], key) for key, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        _, key = heapq.heappop(ready)
        ordered.append(key)
        for child in sorted(outgoing[key], key=positions.__getitem__):
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, (positions[child], child))

    if len(ordered) != len(nodes):
        cyclic = sorted(key for key, degree in indegree.items() if degree > 0)
        raise DAGValidationError(f"plan contains a cycle involving: {', '.join(cyclic)}")
    return tuple(ordered)


def validate_plan(spec: PlanSpec) -> tuple[str, ...]:
    return validate_dag(tuple(spec.nodes), tuple(spec.edges))


def root_nodes(nodes: Sequence[NodeSpec], edges: Sequence[EdgeSpec]) -> tuple[str, ...]:
    validate_dag(nodes, edges)
    targets = {edge.to_node for edge in edges}
    return tuple(node.key for node in nodes if node.key not in targets)


def descendants(start: str, edges: Iterable[EdgeSpec]) -> frozenset[str]:
    outgoing: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        outgoing[edge.from_node].append(edge.to_node)
    found: set[str] = set()
    pending = list(outgoing.get(start, ()))
    while pending:
        node = pending.pop()
        if node in found:
            continue
        found.add(node)
        pending.extend(outgoing.get(node, ()))
    return frozenset(found)
