from pathlib import Path
from dataclasses import FrozenInstanceError

import pytest

from coworker.orchestration.runtime import (
    BudgetExceededError,
    DEFAULT_ACTIVE_SECONDS_LIMIT,
    DEFAULT_ATTEMPTS_PER_NODE,
    DEFAULT_MODEL_CALL_LIMIT,
    DEFAULT_REPORTED_TOKEN_LIMIT,
    DEFAULT_TASK_BUDGET,
    DEFAULT_TOOL_CALL_LIMIT,
    DEFAULT_WORK_UNIT_LIMIT,
    PermissionEscalationError,
    PermissionSet,
    RootPermission,
    RuntimeBudget,
    RuntimeLimitError,
    RuntimeLimits,
    RuntimeManager,
    RuntimeSpec,
    RuntimeStateError,
    RuntimeStatus,
    intersect_permissions,
)


def spec(runtime_id, *, budget=None, permissions=None, parent_id=None, attempt=1):
    return RuntimeSpec(
        runtime_id=runtime_id,
        profile_id="worker",
        task=f"task for {runtime_id}",
        budget=budget or RuntimeBudget(10, 10, 10_000, 1_000),
        permissions=permissions or PermissionSet(),
        parent_id=parent_id,
        attempt=attempt,
    )


def delegating_permissions(**kwargs):
    return PermissionSet(can_delegate=True, **kwargs)


def test_locked_defaults_are_public_and_applied():
    limits = RuntimeLimits()
    default_spec = RuntimeSpec("root", "orchestrator", "coordinate")

    assert limits.max_depth == 3
    assert limits.max_concurrency == 8
    assert limits.max_work_units == DEFAULT_WORK_UNIT_LIMIT == 64
    assert limits.max_attempts_per_node == DEFAULT_ATTEMPTS_PER_NODE == 3
    assert default_spec.budget == DEFAULT_TASK_BUDGET
    assert str(default_spec.profile_ref) == "orchestrator@1"
    assert DEFAULT_TASK_BUDGET == RuntimeBudget(
        DEFAULT_MODEL_CALL_LIMIT,
        DEFAULT_TOOL_CALL_LIMIT,
        DEFAULT_REPORTED_TOKEN_LIMIT,
        DEFAULT_ACTIVE_SECONDS_LIMIT,
    )


def test_permission_intersection_denies_and_audits_every_escalation(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    parent = PermissionSet(
        tools=frozenset({"read_file"}),
        commands=frozenset({"pytest"}),
        roots=(RootPermission(root, writable=False),),
        mode="plan",
        network=False,
        external_writes=False,
        can_delegate=True,
    )
    requested = PermissionSet(
        tools=None,
        commands=frozenset({"pytest", "rm"}),
        roots=(
            RootPermission(root / "src", writable=True),
            RootPermission(outside, writable=True),
        ),
        mode="auto",
        network=True,
        external_writes=True,
        can_delegate=True,
    )

    effective = intersect_permissions(parent, requested)
    assert effective.tools == frozenset({"read_file"})
    assert effective.commands == frozenset({"pytest"})
    assert effective.roots == (RootPermission(root / "src", writable=False),)
    assert effective.mode == "plan"
    assert effective.network is False
    assert effective.external_writes is False
    assert effective.is_within(parent)
    assert set(requested.escalations_over(parent)) == {
        "tools",
        "commands",
        "roots",
        "mode",
        "network",
        "external_writes",
    }
    with pytest.raises(PermissionEscalationError):
        intersect_permissions(parent, requested, reject_escalation=True)


def test_depth_budget_settlement_and_denied_permissions_are_recorded():
    manager = RuntimeManager()
    root = manager.add_root(
        spec("root", budget=RuntimeBudget(20, 20, 20_000, 2_000), permissions=delegating_permissions(network=False))
    )
    child = manager.spawn_child(
        "root",
        spec("child", budget=RuntimeBudget(10, 10, 10_000, 1_000), permissions=delegating_permissions(network=True)),
    )
    grandchild = manager.spawn_child(
        "child",
        spec("grandchild", budget=RuntimeBudget(5, 5, 5_000, 500), permissions=delegating_permissions()),
    )
    great_grandchild = manager.spawn_child(
        "grandchild",
        spec("great-grandchild", budget=RuntimeBudget(2, 2, 2_000, 200), permissions=delegating_permissions()),
    )

    assert (root.depth, child.depth, grandchild.depth, great_grandchild.depth) == (0, 1, 2, 3)
    assert "network" in child.denied_escalations
    assert child.requested_permissions.network is True
    assert child.spec.permissions == child.effective_permissions
    assert child.spec.permissions.network is False
    with pytest.raises(FrozenInstanceError):
        child.status = RuntimeStatus.SUCCEEDED
    with pytest.raises(FrozenInstanceError):
        child.effective_permissions = PermissionSet(network=True)
    with pytest.raises(RuntimeLimitError, match="depth"):
        manager.spawn_child("great-grandchild", spec("too-deep"))

    manager.start("root")
    manager.start("child")
    manager.start("grandchild")
    manager.start("great-grandchild")
    manager.charge("great-grandchild", RuntimeBudget(1, 1, 100, 10))
    manager.finish("great-grandchild")
    manager.finish("grandchild")
    manager.finish("child")
    assert root.total_usage == RuntimeBudget(1, 1, 100, 10)


def test_dynamic_children_count_toward_work_unit_limit():
    manager = RuntimeManager(limits=RuntimeLimits(max_work_units=2))
    manager.add_root(spec("root", permissions=delegating_permissions()))
    manager.spawn_child("root", spec("child"))
    assert manager.work_unit_count == 2
    assert manager.remaining_work_units == 0
    with pytest.raises(RuntimeLimitError, match="work-unit"):
        manager.spawn_child("root", spec("another"))


def test_concurrency_attempt_and_usage_limits():
    manager = RuntimeManager()
    for index in range(9):
        manager.add_root(spec(f"root-{index}"))
    for index in range(8):
        manager.start(f"root-{index}")
    with pytest.raises(RuntimeLimitError, match="concurrency"):
        manager.start("root-8")

    attempt_manager = RuntimeManager()
    with pytest.raises(RuntimeLimitError, match="attempt"):
        attempt_manager.add_root(spec("retry", attempt=4))

    for attempt in range(1, 4):
        attempt_manager.add_root(
            RuntimeSpec(
                runtime_id=f"unit-attempt-{attempt}",
                profile_id="worker",
                task="retry logical unit",
                attempt=attempt,
                work_unit_id="logical-unit",
            )
        )
    assert attempt_manager.work_unit_count == 1
    assert attempt_manager.runtime_count == 3
    with pytest.raises(RuntimeStateError, match="duplicate attempt"):
        attempt_manager.add_root(
            RuntimeSpec(
                runtime_id="duplicate-attempt",
                profile_id="worker",
                task="duplicate",
                attempt=3,
                work_unit_id="logical-unit",
            )
        )

    budget_manager = RuntimeManager()
    budget_manager.add_root(spec("budget", budget=RuntimeBudget(1, 1, 10, 10)))
    budget_manager.start("budget")
    with pytest.raises(BudgetExceededError):
        budget_manager.charge("budget", RuntimeBudget(model_calls=2))


def test_dependencies_and_cancel_cascade():
    manager = RuntimeManager()
    manager.add_root(spec("dependency"))
    manager.add_root(
        RuntimeSpec(
            "dependent",
            "worker",
            "wait",
            dependencies=("dependency",),
        )
    )
    with pytest.raises(RuntimeStateError, match="cannot finish"):
        manager.finish("dependent")
    with pytest.raises(RuntimeStateError, match="dependencies"):
        manager.start("dependent")
    manager.start("dependency")
    manager.finish("dependency", RuntimeStatus.SUCCEEDED)
    manager.start("dependent")
    assert manager.cancel("dependent") == ("dependent",)


def test_dependency_cycles_are_rejected_when_forward_reference_closes_cycle():
    manager = RuntimeManager()
    manager.add_root(
        RuntimeSpec("cycle-a", "worker", "a", dependencies=("cycle-b",))
    )
    with pytest.raises(RuntimeStateError, match="cycle"):
        manager.add_root(
            RuntimeSpec("cycle-b", "worker", "b", dependencies=("cycle-a",))
        )
