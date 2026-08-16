"""Provider-neutral parent/child runtime constraints for hierarchical agents."""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional

from .profiles import ProfileRef


HARD_MAX_DEPTH = 3
HARD_MAX_CONCURRENCY = 8
HARD_MAX_WORK_UNITS = 64
HARD_MAX_ATTEMPTS_PER_NODE = 3
DEFAULT_WORK_UNIT_LIMIT = 64
DEFAULT_ATTEMPTS_PER_NODE = 3
DEFAULT_MODEL_CALL_LIMIT = 20
DEFAULT_TOOL_CALL_LIMIT = 100
DEFAULT_REPORTED_TOKEN_LIMIT = 1_000_000
DEFAULT_ACTIVE_SECONDS_LIMIT = 7_200
_RUNTIME_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_MODE_RANK = {"discuss": 0, "plan": 0, "interactive": 1, "custom": 2, "auto": 3}


class RuntimeErrorBase(RuntimeError):
    pass


class RuntimeLimitError(RuntimeErrorBase):
    pass


class BudgetExceededError(RuntimeErrorBase):
    pass


class RuntimeStateError(RuntimeErrorBase):
    pass


class PermissionEscalationError(RuntimeErrorBase):
    pass


class RuntimeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUSPENDED = "suspended"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"

    @property
    def terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.CANCELED}


class RuntimeKind(str, Enum):
    """The ledger distinguishes transparent task containers from Agent attempts.

    A task container owns a durable task budget and groups the plan attempts that
    execute it.  It is transparent for Agent depth, concurrency and work-unit
    accounting, so bookkeeping nodes cannot consume the limits they enforce.
    """

    TASK = "task"
    AGENT = "agent"


@dataclass(frozen=True, slots=True)
class RuntimeLimits:
    max_depth: int = HARD_MAX_DEPTH
    max_concurrency: int = HARD_MAX_CONCURRENCY
    max_children_per_node: int = 8
    max_work_units: int = DEFAULT_WORK_UNIT_LIMIT
    max_attempts_per_node: int = DEFAULT_ATTEMPTS_PER_NODE

    def __post_init__(self) -> None:
        if not 0 <= int(self.max_depth) <= HARD_MAX_DEPTH:
            raise RuntimeLimitError(f"max_depth cannot exceed {HARD_MAX_DEPTH}")
        if not 1 <= int(self.max_concurrency) <= HARD_MAX_CONCURRENCY:
            raise RuntimeLimitError(
                f"max_concurrency must be between 1 and {HARD_MAX_CONCURRENCY}"
            )
        if not 0 <= int(self.max_children_per_node) <= HARD_MAX_CONCURRENCY:
            raise RuntimeLimitError(
                f"max_children_per_node cannot exceed {HARD_MAX_CONCURRENCY}"
            )
        if not 1 <= int(self.max_work_units) <= HARD_MAX_WORK_UNITS:
            raise RuntimeLimitError(
                f"max_work_units must be between 1 and {HARD_MAX_WORK_UNITS}"
            )
        if not 1 <= int(self.max_attempts_per_node) <= HARD_MAX_ATTEMPTS_PER_NODE:
            raise RuntimeLimitError(
                "max_attempts_per_node must be between 1 and "
                f"{HARD_MAX_ATTEMPTS_PER_NODE}"
            )
        for name in (
            "max_depth",
            "max_concurrency",
            "max_children_per_node",
            "max_work_units",
            "max_attempts_per_node",
        ):
            object.__setattr__(self, name, int(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class RuntimeBudget:
    model_calls: int = 0
    tool_calls: int = 0
    tokens: int = 0
    wall_seconds: int = 0

    def __post_init__(self) -> None:
        for name in ("model_calls", "tool_calls", "tokens", "wall_seconds"):
            value = int(getattr(self, name))
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
            object.__setattr__(self, name, value)

    def __add__(self, other: "RuntimeBudget") -> "RuntimeBudget":
        return RuntimeBudget(
            self.model_calls + other.model_calls,
            self.tool_calls + other.tool_calls,
            self.tokens + other.tokens,
            self.wall_seconds + other.wall_seconds,
        )

    def __sub__(self, other: "RuntimeBudget") -> "RuntimeBudget":
        if not other.fits_within(self):
            raise BudgetExceededError("budget subtraction would become negative")
        return RuntimeBudget(
            self.model_calls - other.model_calls,
            self.tool_calls - other.tool_calls,
            self.tokens - other.tokens,
            self.wall_seconds - other.wall_seconds,
        )

    def fits_within(self, ceiling: "RuntimeBudget") -> bool:
        return all(
            getattr(self, name) <= getattr(ceiling, name)
            for name in ("model_calls", "tool_calls", "tokens", "wall_seconds")
        )

    def as_dict(self) -> dict[str, int]:
        return {
            "model_calls": self.model_calls,
            "tool_calls": self.tool_calls,
            "tokens": self.tokens,
            "wall_seconds": self.wall_seconds,
        }

    @property
    def reported_tokens(self) -> int:
        return self.tokens

    @property
    def active_seconds(self) -> int:
        return self.wall_seconds


Budget = RuntimeBudget
BudgetUsage = RuntimeBudget
DEFAULT_TASK_BUDGET = RuntimeBudget(
    model_calls=DEFAULT_MODEL_CALL_LIMIT,
    tool_calls=DEFAULT_TOOL_CALL_LIMIT,
    tokens=DEFAULT_REPORTED_TOKEN_LIMIT,
    wall_seconds=DEFAULT_ACTIVE_SECONDS_LIMIT,
)
DEFAULT_RUN_BUDGET = DEFAULT_TASK_BUDGET


@dataclass(frozen=True, slots=True)
class RootPermission:
    path: Path | str
    writable: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", Path(self.path).expanduser().resolve())
        object.__setattr__(self, "writable", bool(self.writable))


def _scope_intersection(
    parent: Optional[frozenset[str]], child: Optional[frozenset[str]]
) -> Optional[frozenset[str]]:
    if parent is None:
        return child
    if child is None:
        return parent
    return parent & child


def _scope_is_within(
    granted: Optional[frozenset[str]], ceiling: Optional[frozenset[str]]
) -> bool:
    if ceiling is None:
        return True
    if granted is None:
        return False
    return granted <= ceiling


def _normalize_roots(
    roots: Optional[Iterable[RootPermission | Mapping[str, Any] | str | Path]],
) -> Optional[tuple[RootPermission, ...]]:
    if roots is None:
        return None
    merged: dict[Path, bool] = {}
    for item in roots:
        if isinstance(item, RootPermission):
            grant = item
        elif isinstance(item, Mapping):
            grant = RootPermission(item["path"], bool(item.get("writable", False)))
        else:
            grant = RootPermission(item, False)
        merged[grant.path] = merged.get(grant.path, False) or grant.writable
    return tuple(RootPermission(path, writable) for path, writable in sorted(merged.items(), key=lambda p: str(p[0])))


def _intersect_roots(
    parent: Optional[tuple[RootPermission, ...]],
    child: Optional[tuple[RootPermission, ...]],
) -> Optional[tuple[RootPermission, ...]]:
    if parent is None:
        return child
    if child is None:
        return parent
    result: dict[Path, bool] = {}
    for p in parent:
        for c in child:
            try:
                c.path.relative_to(p.path)
                path = c.path
            except ValueError:
                try:
                    p.path.relative_to(c.path)
                    path = p.path
                except ValueError:
                    continue
            writable = p.writable and c.writable
            result[path] = result.get(path, False) or writable
    return tuple(
        RootPermission(path, writable)
        for path, writable in sorted(result.items(), key=lambda pair: str(pair[0]))
    )


def _roots_are_within(
    granted: Optional[tuple[RootPermission, ...]],
    ceiling: Optional[tuple[RootPermission, ...]],
) -> bool:
    if ceiling is None:
        return True
    if granted is None:
        return False
    for grant in granted:
        covered = False
        for limit in ceiling:
            try:
                grant.path.relative_to(limit.path)
            except ValueError:
                continue
            if grant.writable and not limit.writable:
                continue
            covered = True
            break
        if not covered:
            return False
    return True


@dataclass(frozen=True, slots=True)
class PermissionSet:
    """A capability ceiling. ``None`` means unrestricted; an empty set means denied."""

    tools: Optional[frozenset[str]] = None
    commands: Optional[frozenset[str]] = None
    roots: Optional[tuple[RootPermission, ...]] = None
    mode: str = "interactive"
    network: bool = False
    external_writes: bool = False
    can_delegate: bool = False

    def __post_init__(self) -> None:
        mode = str(self.mode).strip().lower()
        if mode not in _MODE_RANK:
            raise ValueError(f"unknown permission mode: {mode!r}")
        tools = None if self.tools is None else frozenset(str(v).strip() for v in self.tools if str(v).strip())
        commands = None if self.commands is None else frozenset(str(v).strip() for v in self.commands if str(v).strip())
        object.__setattr__(self, "tools", tools)
        object.__setattr__(self, "commands", commands)
        object.__setattr__(self, "roots", _normalize_roots(self.roots))
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "network", bool(self.network))
        object.__setattr__(self, "external_writes", bool(self.external_writes))
        object.__setattr__(self, "can_delegate", bool(self.can_delegate))

    def intersect(self, requested: "PermissionSet") -> "PermissionSet":
        mode = min((self.mode, requested.mode), key=lambda item: (_MODE_RANK[item], item))
        granted = PermissionSet(
            tools=_scope_intersection(self.tools, requested.tools),
            commands=_scope_intersection(self.commands, requested.commands),
            roots=_intersect_roots(self.roots, requested.roots),
            mode=mode,
            network=self.network and requested.network,
            external_writes=self.external_writes and requested.external_writes,
            can_delegate=self.can_delegate and requested.can_delegate,
        )
        # A failed assertion here would indicate a defect in the intersection logic,
        # never a permission request that callers could use to gain access.
        granted.assert_within(self)
        return granted

    def escalations_over(self, ceiling: "PermissionSet") -> tuple[str, ...]:
        """List requested capabilities that exceed a parent ceiling."""
        escalations: list[str] = []
        if not _scope_is_within(self.tools, ceiling.tools):
            escalations.append("tools")
        if not _scope_is_within(self.commands, ceiling.commands):
            escalations.append("commands")
        if not _roots_are_within(self.roots, ceiling.roots):
            escalations.append("roots")
        if _MODE_RANK[self.mode] > _MODE_RANK[ceiling.mode]:
            escalations.append("mode")
        if self.network and not ceiling.network:
            escalations.append("network")
        if self.external_writes and not ceiling.external_writes:
            escalations.append("external_writes")
        if self.can_delegate and not ceiling.can_delegate:
            escalations.append("can_delegate")
        return tuple(escalations)

    def is_within(self, ceiling: "PermissionSet") -> bool:
        return not self.escalations_over(ceiling)

    def assert_within(self, ceiling: "PermissionSet") -> None:
        escalations = self.escalations_over(ceiling)
        if escalations:
            raise PermissionEscalationError(
                "permission escalation denied: " + ", ".join(escalations)
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "tools": sorted(self.tools) if self.tools is not None else None,
            "commands": sorted(self.commands) if self.commands is not None else None,
            "roots": (
                [
                    {"path": str(root.path), "writable": root.writable}
                    for root in self.roots
                ]
                if self.roots is not None
                else None
            ),
            "mode": self.mode,
            "network": self.network,
            "external_writes": self.external_writes,
            "can_delegate": self.can_delegate,
        }


def intersect_permissions(
    parent: PermissionSet,
    requested: PermissionSet,
    *,
    reject_escalation: bool = False,
) -> PermissionSet:
    """Apply the parent ceiling, optionally rejecting instead of auditing excess requests."""
    if reject_escalation:
        requested.assert_within(parent)
    return parent.intersect(requested)


def _freeze_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_metadata(item) for key, item in value.items()}
        )
    if isinstance(value, (tuple, list)):
        return tuple(_freeze_metadata(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_metadata(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class RuntimeSpec:
    runtime_id: str
    profile_id: str
    task: str
    budget: RuntimeBudget = DEFAULT_TASK_BUDGET
    permissions: PermissionSet = PermissionSet()
    parent_id: Optional[str] = None
    dependencies: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict, compare=False, hash=False)
    attempt: int = 1
    work_unit_id: Optional[str] = None
    profile_version: int = 1
    profile_content_hash: Optional[str] = None
    kind: RuntimeKind = RuntimeKind.AGENT

    def __post_init__(self) -> None:
        runtime_id = str(self.runtime_id).strip()
        if not _RUNTIME_ID.fullmatch(runtime_id):
            raise ValueError(f"invalid runtime_id: {runtime_id!r}")
        if not str(self.profile_id).strip():
            raise ValueError("profile_id must not be empty")
        if not str(self.task).strip():
            raise ValueError("task must not be empty")
        if not isinstance(self.budget, RuntimeBudget):
            raise TypeError("budget must be a RuntimeBudget")
        if not isinstance(self.permissions, PermissionSet):
            raise TypeError("permissions must be a PermissionSet")
        kind = self.kind if isinstance(self.kind, RuntimeKind) else RuntimeKind(self.kind)
        dependencies = tuple(dict.fromkeys(str(v).strip() for v in self.dependencies if str(v).strip()))
        if runtime_id in dependencies:
            raise ValueError("a runtime cannot depend on itself")
        if int(self.attempt) < 1:
            raise ValueError("attempt must be positive")
        profile_version = int(self.profile_version)
        if profile_version < 1:
            raise ValueError("profile_version must be positive")
        profile_content_hash = (
            str(self.profile_content_hash).strip().lower()
            if self.profile_content_hash
            else None
        )
        if profile_content_hash and not re.fullmatch(r"[0-9a-f]{64}", profile_content_hash):
            raise ValueError("profile_content_hash must be a SHA-256 hex digest")
        work_unit_id = (
            str(self.work_unit_id).strip() if self.work_unit_id else runtime_id
        )
        if not _RUNTIME_ID.fullmatch(work_unit_id):
            raise ValueError(f"invalid work_unit_id: {work_unit_id!r}")
        object.__setattr__(self, "runtime_id", runtime_id)
        object.__setattr__(self, "profile_id", str(self.profile_id).strip())
        object.__setattr__(self, "task", str(self.task).strip())
        object.__setattr__(self, "parent_id", str(self.parent_id).strip() if self.parent_id else None)
        object.__setattr__(self, "dependencies", dependencies)
        object.__setattr__(self, "metadata", _freeze_metadata(self.metadata))
        object.__setattr__(self, "attempt", int(self.attempt))
        object.__setattr__(self, "work_unit_id", work_unit_id)
        object.__setattr__(self, "profile_version", profile_version)
        object.__setattr__(self, "profile_content_hash", profile_content_hash)
        object.__setattr__(self, "kind", kind)

    @property
    def profile_ref(self) -> ProfileRef:
        return ProfileRef(self.profile_id, self.profile_version)


AgentRuntimeSpec = RuntimeSpec


@dataclass(frozen=True, slots=True, eq=False)
class RuntimeNode:
    spec: RuntimeSpec
    depth: int
    effective_permissions: PermissionSet
    status: RuntimeStatus = RuntimeStatus.PENDING
    direct_usage: RuntimeBudget = RuntimeBudget()
    denied_escalations: tuple[str, ...] = ()
    requested_permissions: Optional[PermissionSet] = None
    __reservations: dict[str, RuntimeBudget] = field(default_factory=dict, repr=False)
    __settled_children: dict[str, RuntimeBudget] = field(default_factory=dict, repr=False)

    @property
    def runtime_id(self) -> str:
        return self.spec.runtime_id

    @property
    def total_usage(self) -> RuntimeBudget:
        total = self.direct_usage
        for usage in self.__settled_children.values():
            total = total + usage
        return total

    @property
    def committed_budget(self) -> RuntimeBudget:
        total = self.total_usage
        for budget in self.__reservations.values():
            total = total + budget
        return total

    @property
    def remaining_budget(self) -> RuntimeBudget:
        return self.spec.budget - self.committed_budget

    def _transition(self, status: RuntimeStatus) -> None:
        object.__setattr__(self, "status", status)

    def _record_direct_usage(self, usage: RuntimeBudget) -> None:
        object.__setattr__(self, "direct_usage", usage)

    def _reserve(self, runtime_id: str, budget: RuntimeBudget) -> None:
        self.__reservations[runtime_id] = budget

    def _settle(self, runtime_id: str, usage: RuntimeBudget) -> None:
        self.__reservations.pop(runtime_id, None)
        self.__settled_children[runtime_id] = usage

    def _settled_usage(self) -> tuple[RuntimeBudget, ...]:
        return tuple(self.__settled_children.values())

    def _reserved_budgets(self) -> tuple[RuntimeBudget, ...]:
        return tuple(self.__reservations.values())


class RuntimeManager:
    """Thread-safe hierarchy ledger; it does not execute model or tool code."""

    def __init__(self, *, limits: RuntimeLimits = RuntimeLimits()) -> None:
        self.limits = limits
        self._nodes: dict[str, RuntimeNode] = {}
        self._children: dict[str, list[str]] = {}
        self._active: set[str] = set()
        self._attempts: dict[str, set[int]] = {}
        self._lock = threading.RLock()

    def add_root(self, spec: RuntimeSpec) -> RuntimeNode:
        with self._lock:
            if spec.parent_id is not None:
                raise RuntimeStateError("a root spec cannot have parent_id")
            self._ensure_new(spec.runtime_id)
            self._ensure_capacity(spec)
            self._ensure_acyclic(spec)
            self._record_attempt(spec)
            node = RuntimeNode(
                spec=spec,
                depth=0,
                effective_permissions=spec.permissions,
                requested_permissions=spec.permissions,
            )
            self._nodes[spec.runtime_id] = node
            self._children[spec.runtime_id] = []
            return node

    register_root = add_root

    def spawn_child(self, parent_id: str, spec: RuntimeSpec) -> RuntimeNode:
        with self._lock:
            parent = self.get(parent_id)
            self._ensure_new(spec.runtime_id)
            self._ensure_capacity(spec)
            self._ensure_acyclic(spec)
            if parent.status.terminal:
                raise RuntimeStateError("cannot spawn from a terminal parent")
            if not parent.effective_permissions.can_delegate:
                raise RuntimeStateError("parent is not allowed to delegate")
            # Plan attempts owned by a transparent task container are not delegated
            # child Agents.  A real Agent spawning another task/Agent is.
            if (
                parent.spec.kind is RuntimeKind.AGENT
                and len(self._children[parent_id]) >= self.limits.max_children_per_node
            ):
                raise RuntimeLimitError("parent child limit reached")
            if spec.kind is RuntimeKind.TASK:
                depth = parent.depth
            elif parent.spec.kind is RuntimeKind.TASK:
                depth = 0 if parent.spec.parent_id is None else parent.depth + 1
            else:
                depth = parent.depth + 1
            if depth > self.limits.max_depth:
                raise RuntimeLimitError(
                    f"runtime depth {depth} exceeds limit {self.limits.max_depth}"
                )
            if not spec.budget.fits_within(parent.remaining_budget):
                raise BudgetExceededError("child allocation exceeds parent's remaining budget")
            if spec.parent_id not in (None, parent_id):
                raise RuntimeStateError("spec parent_id does not match requested parent")
            denied = spec.permissions.escalations_over(parent.effective_permissions)
            effective = intersect_permissions(parent.effective_permissions, spec.permissions)
            # RuntimeSpec is the executable contract. Store only the intersected grant in
            # it so an executor cannot accidentally treat the original request as authority.
            bound_spec = replace(
                spec,
                parent_id=parent_id,
                permissions=effective,
            )
            node = RuntimeNode(
                spec=bound_spec,
                depth=depth,
                effective_permissions=effective,
                denied_escalations=denied,
                requested_permissions=spec.permissions,
            )
            self._nodes[node.runtime_id] = node
            self._children[node.runtime_id] = []
            self._children[parent_id].append(node.runtime_id)
            self._record_attempt(bound_spec)
            parent._reserve(node.runtime_id, spec.budget)
            return node

    def start(self, runtime_id: str) -> RuntimeNode:
        with self._lock:
            node = self.get(runtime_id)
            if node.status is not RuntimeStatus.PENDING:
                raise RuntimeStateError(f"cannot start runtime in {node.status.value} state")
            incomplete = [
                dep
                for dep in node.spec.dependencies
                if dep not in self._nodes
                or self._nodes[dep].status is not RuntimeStatus.SUCCEEDED
            ]
            if incomplete:
                raise RuntimeStateError("dependencies are not satisfied: " + ", ".join(incomplete))
            if (
                node.spec.kind is RuntimeKind.AGENT
                and len(self._active) >= self.limits.max_concurrency
            ):
                raise RuntimeLimitError("runtime concurrency limit reached")
            node._transition(RuntimeStatus.RUNNING)
            if node.spec.kind is RuntimeKind.AGENT:
                self._active.add(runtime_id)
            return node

    def suspend(self, runtime_id: str) -> RuntimeNode:
        """Release an execution slot while preserving the same durable attempt."""

        with self._lock:
            node = self.get(runtime_id)
            if node.status is RuntimeStatus.SUSPENDED:
                return node
            if node.status is not RuntimeStatus.RUNNING:
                raise RuntimeStateError(
                    f"cannot suspend runtime in {node.status.value} state"
                )
            node._transition(RuntimeStatus.SUSPENDED)
            self._active.discard(runtime_id)
            return node

    def resume(self, runtime_id: str) -> RuntimeNode:
        """Re-acquire an execution slot for a suspended durable attempt."""

        with self._lock:
            node = self.get(runtime_id)
            if node.status is not RuntimeStatus.SUSPENDED:
                raise RuntimeStateError(
                    f"cannot resume runtime in {node.status.value} state"
                )
            if (
                node.spec.kind is RuntimeKind.AGENT
                and len(self._active) >= self.limits.max_concurrency
            ):
                raise RuntimeLimitError("runtime concurrency limit reached")
            node._transition(RuntimeStatus.RUNNING)
            if node.spec.kind is RuntimeKind.AGENT:
                self._active.add(runtime_id)
            return node

    def charge(self, runtime_id: str, usage: RuntimeBudget) -> RuntimeBudget:
        with self._lock:
            node = self.get(runtime_id)
            if node.status is not RuntimeStatus.RUNNING:
                raise RuntimeStateError("usage may only be charged to a running runtime")
            proposed = node.direct_usage + usage
            committed = proposed
            for child_usage in node._settled_usage():
                committed = committed + child_usage
            for reservation in node._reserved_budgets():
                committed = committed + reservation
            if not committed.fits_within(node.spec.budget):
                raise BudgetExceededError("runtime budget exceeded")
            node._record_direct_usage(proposed)
            return node.remaining_budget

    record_usage = charge

    def finish(
        self,
        runtime_id: str,
        status: RuntimeStatus | str = RuntimeStatus.SUCCEEDED,
    ) -> RuntimeNode:
        with self._lock:
            node = self.get(runtime_id)
            terminal = status if isinstance(status, RuntimeStatus) else RuntimeStatus(status)
            if not terminal.terminal:
                raise RuntimeStateError("finish status must be terminal")
            if node.status.terminal:
                return node
            if (
                node.status not in {RuntimeStatus.RUNNING, RuntimeStatus.SUSPENDED}
                and terminal is not RuntimeStatus.CANCELED
            ):
                raise RuntimeStateError(
                    f"cannot finish runtime in {node.status.value} state as {terminal.value}"
                )
            live_children = [
                child_id
                for child_id in self._children[runtime_id]
                if not self._nodes[child_id].status.terminal
            ]
            if live_children:
                raise RuntimeStateError(
                    "cannot finish while children are active: " + ", ".join(live_children)
                )
            node._transition(terminal)
            self._active.discard(runtime_id)
            if node.spec.parent_id:
                parent = self.get(node.spec.parent_id)
                parent._settle(runtime_id, node.total_usage)
            return node

    def cancel(self, runtime_id: str) -> tuple[str, ...]:
        with self._lock:
            canceled: list[str] = []

            def visit(node_id: str) -> None:
                for child_id in tuple(self._children[node_id]):
                    if not self._nodes[child_id].status.terminal:
                        visit(child_id)
                node = self._nodes[node_id]
                if not node.status.terminal:
                    self.finish(node_id, RuntimeStatus.CANCELED)
                    canceled.append(node_id)

            visit(runtime_id)
            return tuple(canceled)

    def get(self, runtime_id: str) -> RuntimeNode:
        with self._lock:
            try:
                return self._nodes[runtime_id]
            except KeyError as exc:
                raise KeyError(f"unknown runtime: {runtime_id}") from exc

    def children_of(self, runtime_id: str) -> tuple[RuntimeNode, ...]:
        with self._lock:
            self.get(runtime_id)
            return tuple(self._nodes[node_id] for node_id in self._children[runtime_id])

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    @property
    def work_unit_count(self) -> int:
        """Unique logical work units, including dynamically spawned children."""
        with self._lock:
            return len(self._attempts)

    @property
    def runtime_count(self) -> int:
        """Registered execution attempts, including retries of one work unit."""
        with self._lock:
            return len(self._nodes)

    @property
    def remaining_work_units(self) -> int:
        return self.limits.max_work_units - self.work_unit_count

    def snapshot(self) -> tuple[dict[str, Any], ...]:
        with self._lock:
            return tuple(
                {
                    "runtime_id": node.runtime_id,
                    "parent_id": node.spec.parent_id,
                    "profile_id": node.spec.profile_id,
                    "profile_version": node.spec.profile_version,
                    "profile_content_hash": node.spec.profile_content_hash,
                    "kind": node.spec.kind.value,
                    "attempt": node.spec.attempt,
                    "work_unit_id": node.spec.work_unit_id,
                    "depth": node.depth,
                    "status": node.status.value,
                    "budget": node.spec.budget.as_dict(),
                    "usage": node.total_usage.as_dict(),
                    "permissions": node.spec.permissions.as_dict(),
                    "requested_permissions": (
                        node.requested_permissions.as_dict()
                        if node.requested_permissions
                        else None
                    ),
                    "denied_escalations": list(node.denied_escalations),
                    "dependencies": list(node.spec.dependencies),
                    "durable_dependencies": list(
                        node.spec.metadata.get(
                            "durable_dependencies", node.spec.dependencies
                        )
                    ),
                }
                for node in sorted(self._nodes.values(), key=lambda item: item.runtime_id)
            )

    def _ensure_new(self, runtime_id: str) -> None:
        if runtime_id in self._nodes:
            raise RuntimeStateError(f"duplicate runtime_id: {runtime_id}")

    def _ensure_capacity(self, spec: RuntimeSpec) -> None:
        if spec.kind is RuntimeKind.TASK:
            return
        attempts = self._attempts.get(spec.work_unit_id, set())
        if not attempts and len(self._attempts) >= self.limits.max_work_units:
            raise RuntimeLimitError("runtime work-unit limit reached")
        if spec.attempt > self.limits.max_attempts_per_node:
            raise RuntimeLimitError(
                f"attempt {spec.attempt} exceeds per-node limit "
                f"{self.limits.max_attempts_per_node}"
            )
        if spec.attempt in attempts:
            raise RuntimeStateError(
                f"duplicate attempt {spec.attempt} for work unit {spec.work_unit_id}"
            )
        if len(attempts) >= self.limits.max_attempts_per_node:
            raise RuntimeLimitError(
                f"work unit {spec.work_unit_id} reached attempt limit "
                f"{self.limits.max_attempts_per_node}"
            )

    def _record_attempt(self, spec: RuntimeSpec) -> None:
        if spec.kind is RuntimeKind.TASK:
            return
        self._attempts.setdefault(spec.work_unit_id, set()).add(spec.attempt)

    def _ensure_acyclic(self, candidate: RuntimeSpec) -> None:
        graph = {
            runtime_id: node.spec.dependencies
            for runtime_id, node in self._nodes.items()
        }
        graph[candidate.runtime_id] = candidate.dependencies
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(runtime_id: str) -> None:
            if runtime_id in visiting:
                raise RuntimeStateError("runtime dependency cycle detected")
            if runtime_id in visited or runtime_id not in graph:
                return
            visiting.add(runtime_id)
            for dependency in graph[runtime_id]:
                visit(dependency)
            visiting.remove(runtime_id)
            visited.add(runtime_id)

        for runtime_id in tuple(graph):
            visit(runtime_id)


OrchestrationRuntime = RuntimeManager
