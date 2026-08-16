from __future__ import annotations

import pytest

from coworker.orchestration import (
    DAGValidationError,
    EdgeSpec,
    NodeKind,
    NodeSpec,
    PlanSpec,
    RetryPolicy,
    descendants,
    root_nodes,
    validate_dag,
    validate_plan,
)


def test_kahn_validation_returns_stable_topological_order():
    nodes = (
        NodeSpec("implement", kind=NodeKind.EXECUTE),
        NodeSpec("review", kind=NodeKind.REVIEW),
        NodeSpec("test", kind=NodeKind.TEST),
        NodeSpec("integrate", kind=NodeKind.INTEGRATE),
    )
    edges = (
        EdgeSpec("implement", "review"),
        EdgeSpec("implement", "test"),
        EdgeSpec("review", "integrate"),
        EdgeSpec("test", "integrate"),
    )
    assert validate_dag(nodes, edges) == (
        "implement",
        "review",
        "test",
        "integrate",
    )
    assert validate_plan(PlanSpec(nodes, edges)) == validate_dag(nodes, edges)
    assert root_nodes(nodes, edges) == ("implement",)
    assert descendants("implement", edges) == {"review", "test", "integrate"}


@pytest.mark.parametrize(
    "nodes,edges,message",
    [
        ((NodeSpec("a"), NodeSpec("a")), (), "duplicate node"),
        ((NodeSpec("a"),), (EdgeSpec("missing", "a"),), "unknown edge source"),
        ((NodeSpec("a"),), (EdgeSpec("a", "missing"),), "unknown edge target"),
        ((NodeSpec("a"),), (EdgeSpec("a", "a"),), "self edge"),
        (
            (NodeSpec("a"), NodeSpec("b")),
            (EdgeSpec("a", "b"), EdgeSpec("a", "b")),
            "duplicate edge",
        ),
        (
            (NodeSpec("a"), NodeSpec("b")),
            (EdgeSpec("a", "b"), EdgeSpec("b", "a")),
            "cycle",
        ),
    ],
)
def test_invalid_dags_are_rejected(nodes, edges, message):
    with pytest.raises(DAGValidationError, match=message):
        validate_dag(nodes, edges)


def test_node_execution_limits_are_validated():
    with pytest.raises(DAGValidationError, match="max_attempts"):
        validate_dag((NodeSpec("a", retry_policy=RetryPolicy(max_attempts=0)),), ())
    with pytest.raises(DAGValidationError, match="cannot exceed 3"):
        validate_dag((NodeSpec("a", retry_policy=RetryPolicy(max_attempts=4)),), ())
    with pytest.raises(DAGValidationError, match="timeout_seconds"):
        validate_dag((NodeSpec("a", timeout_seconds=0),), ())


def test_plan_work_unit_and_retry_timing_rails_are_validated():
    with pytest.raises(DAGValidationError, match="64 work units"):
        validate_dag(tuple(NodeSpec(f"n-{index}") for index in range(65)), ())

    invalid = (
        RetryPolicy(initial_delay_seconds=-1),
        RetryPolicy(multiplier=0.5),
        RetryPolicy(max_delay_seconds=-1),
        RetryPolicy(jitter=1.1),
    )
    for policy in invalid:
        with pytest.raises(DAGValidationError):
            validate_dag((NodeSpec("a", retry_policy=policy),), ())
