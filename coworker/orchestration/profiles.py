"""Versioned, immutable agent profiles for the orchestration runtime.

Profiles are deliberately declarative.  They describe an agent's prompt, tool ceiling,
delegation ceiling and model policy, but never construct tools or grant permissions.  The
runtime intersects a profile with the parent's effective permissions before execution.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import threading
from dataclasses import dataclass, field, fields
from enum import Enum
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional


_PROFILE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_PERMISSION_MODES = frozenset({"discuss", "plan", "interactive", "custom", "auto"})


class ProfileValidationError(ValueError):
    """A profile draft, clone or publication is invalid."""


class AgentRole(str, Enum):
    ORCHESTRATOR = "orchestrator"
    PLANNER = "planner"
    WORKER = "worker"
    REVIEWER = "reviewer"
    TESTER = "tester"
    EVALUATOR = "evaluator"
    SCORER = "scorer"
    EXPLORER = "explorer"
    INTEGRATOR = "integrator"


def _role(value: AgentRole | str) -> AgentRole:
    try:
        return value if isinstance(value, AgentRole) else AgentRole(str(value).lower())
    except ValueError as exc:
        raise ProfileValidationError(f"unknown agent role: {value!r}") from exc


def _ordered_unique(values: Iterable[Any]) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        item = str(value).strip()
        if item and item not in result:
            result.append(item)
    return tuple(result)


def _freeze(value: Any) -> Any:
    """Recursively detach and freeze JSON-like metadata supplied by callers."""
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(v) for v in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ProfileValidationError("profile metadata cannot contain NaN or infinity")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ProfileValidationError(
        f"profile metadata must contain only JSON-compatible values, got {type(value).__name__}"
    )


def _jsonable(value: Any) -> Any:
    """Return a detached JSON value from recursively frozen profile data."""
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_jsonable(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, Enum):
        return value.value
    return value


def _content_hash(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        _jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_common(
    *,
    profile_id: str,
    display_name: str,
    instructions: str,
    permission_mode: str,
    model_policy: str,
    max_iterations: int,
    max_children: int,
) -> None:
    if not _PROFILE_ID.fullmatch(profile_id):
        raise ProfileValidationError(
            "profile_id must be a lowercase filesystem-safe slug (max 64 characters)"
        )
    if not display_name.strip():
        raise ProfileValidationError("display_name must not be empty")
    if not instructions.strip():
        raise ProfileValidationError("instructions must not be empty")
    if permission_mode not in _PERMISSION_MODES:
        raise ProfileValidationError(f"unknown permission mode: {permission_mode!r}")
    if not model_policy.strip():
        raise ProfileValidationError("model_policy must not be empty")
    if not 1 <= max_iterations <= 200:
        raise ProfileValidationError("max_iterations must be between 1 and 200")
    if not 0 <= max_children <= 8:
        raise ProfileValidationError("max_children must be between 0 and 8")


@dataclass(frozen=True, slots=True)
class ProfileRef:
    profile_id: str
    version: int

    def __post_init__(self) -> None:
        profile_id = str(self.profile_id).strip()
        version = int(self.version)
        if not _PROFILE_ID.fullmatch(profile_id):
            raise ProfileValidationError(f"invalid profile reference: {profile_id!r}")
        if version < 1:
            raise ProfileValidationError("profile version must be positive")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "version", version)

    def __str__(self) -> str:
        return f"{self.profile_id}@{self.version}"

    def to_dict(self) -> dict[str, Any]:
        return {"profile_id": self.profile_id, "version": self.version}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ProfileRef":
        return cls(profile_id=str(value["profile_id"]), version=int(value["version"]))


@dataclass(frozen=True, slots=True)
class AgentProfileDraft:
    profile_id: str
    display_name: str
    role: AgentRole | str
    instructions: str
    allowed_tools: tuple[str, ...] = ()
    allowed_child_roles: tuple[AgentRole | str, ...] = ()
    permission_mode: str = "interactive"
    model_policy: str = "quality-first"
    max_iterations: int = 12
    max_children: int = 0
    base: Optional[ProfileRef] = None
    metadata: Mapping[str, Any] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        profile_id = str(self.profile_id).strip()
        role = _role(self.role)
        children = tuple(_role(item) for item in self.allowed_child_roles)
        children = tuple(dict.fromkeys(children))
        tools = _ordered_unique(self.allowed_tools)
        permission_mode = str(self.permission_mode).strip().lower()
        model_policy = str(self.model_policy).strip()
        max_iterations = int(self.max_iterations)
        max_children = int(self.max_children)
        _validate_common(
            profile_id=profile_id,
            display_name=str(self.display_name),
            instructions=str(self.instructions),
            permission_mode=permission_mode,
            model_policy=model_policy,
            max_iterations=max_iterations,
            max_children=max_children,
        )
        if children and max_children == 0:
            raise ProfileValidationError(
                "allowed_child_roles requires max_children greater than zero"
            )
        if not children and max_children:
            raise ProfileValidationError(
                "max_children requires at least one allowed_child_role"
            )
        if self.base is not None and not isinstance(self.base, ProfileRef):
            raise ProfileValidationError("base must be a ProfileRef")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "display_name", str(self.display_name).strip())
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "instructions", str(self.instructions).strip())
        object.__setattr__(self, "allowed_tools", tools)
        object.__setattr__(self, "allowed_child_roles", children)
        object.__setattr__(self, "permission_mode", permission_mode)
        object.__setattr__(self, "model_policy", model_policy)
        object.__setattr__(self, "max_iterations", max_iterations)
        object.__setattr__(self, "max_children", max_children)
        object.__setattr__(self, "metadata", _freeze(dict(self.metadata)))

    def publish(self, version: int, *, builtin: bool = False) -> "AgentProfile":
        return AgentProfile(
            profile_id=self.profile_id,
            version=version,
            display_name=self.display_name,
            role=self.role,
            instructions=self.instructions,
            allowed_tools=self.allowed_tools,
            allowed_child_roles=self.allowed_child_roles,
            permission_mode=self.permission_mode,
            model_policy=self.model_policy,
            max_iterations=self.max_iterations,
            max_children=self.max_children,
            builtin=builtin,
            cloned_from=self.base,
            metadata=self.metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "profile_id": self.profile_id,
            "display_name": self.display_name,
            "role": self.role.value,
            "instructions": self.instructions,
            "allowed_tools": list(self.allowed_tools),
            "allowed_child_roles": [role.value for role in self.allowed_child_roles],
            "permission_mode": self.permission_mode,
            "model_policy": self.model_policy,
            "max_iterations": self.max_iterations,
            "max_children": self.max_children,
            "base": self.base.to_dict() if self.base else None,
            "metadata": _jsonable(self.metadata),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "AgentProfileDraft":
        base = value.get("base")
        return cls(
            profile_id=str(value["profile_id"]),
            display_name=str(value["display_name"]),
            role=str(value["role"]),
            instructions=str(value["instructions"]),
            allowed_tools=tuple(value.get("allowed_tools", ())),
            allowed_child_roles=tuple(value.get("allowed_child_roles", ())),
            permission_mode=str(value.get("permission_mode", "interactive")),
            model_policy=str(value.get("model_policy", "quality-first")),
            max_iterations=int(value.get("max_iterations", 12)),
            max_children=int(value.get("max_children", 0)),
            base=ProfileRef.from_dict(base) if isinstance(base, Mapping) else None,
            metadata=dict(value.get("metadata", {})),
        )


@dataclass(frozen=True, slots=True)
class AgentProfile:
    profile_id: str
    version: int
    display_name: str
    role: AgentRole | str
    instructions: str
    allowed_tools: tuple[str, ...] = ()
    allowed_child_roles: tuple[AgentRole | str, ...] = ()
    permission_mode: str = "interactive"
    model_policy: str = "quality-first"
    max_iterations: int = 12
    max_children: int = 0
    builtin: bool = False
    cloned_from: Optional[ProfileRef] = None
    metadata: Mapping[str, Any] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        version = int(self.version)
        if version < 1:
            raise ProfileValidationError("profile version must be positive")
        draft = AgentProfileDraft(
            profile_id=self.profile_id,
            display_name=self.display_name,
            role=self.role,
            instructions=self.instructions,
            allowed_tools=self.allowed_tools,
            allowed_child_roles=self.allowed_child_roles,
            permission_mode=self.permission_mode,
            model_policy=self.model_policy,
            max_iterations=self.max_iterations,
            max_children=self.max_children,
            base=self.cloned_from,
            metadata=self.metadata,
        )
        for name in (
            "profile_id",
            "display_name",
            "role",
            "instructions",
            "allowed_tools",
            "allowed_child_roles",
            "permission_mode",
            "model_policy",
            "max_iterations",
            "max_children",
            "metadata",
        ):
            object.__setattr__(self, name, getattr(draft, name))
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "builtin", bool(self.builtin))

    @property
    def ref(self) -> ProfileRef:
        return ProfileRef(self.profile_id, self.version)

    def to_draft(self) -> AgentProfileDraft:
        return AgentProfileDraft(
            profile_id=self.profile_id,
            display_name=self.display_name,
            role=self.role,
            instructions=self.instructions,
            allowed_tools=self.allowed_tools,
            allowed_child_roles=self.allowed_child_roles,
            permission_mode=self.permission_mode,
            model_policy=self.model_policy,
            max_iterations=self.max_iterations,
            max_children=self.max_children,
            base=self.ref,
            metadata=self.metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the exact published snapshot without mutable references."""
        return {
            "schema_version": 1,
            "profile_id": self.profile_id,
            "version": self.version,
            "display_name": self.display_name,
            "role": self.role.value,
            "instructions": self.instructions,
            "allowed_tools": list(self.allowed_tools),
            "allowed_child_roles": [role.value for role in self.allowed_child_roles],
            "permission_mode": self.permission_mode,
            "model_policy": self.model_policy,
            "max_iterations": self.max_iterations,
            "max_children": self.max_children,
            "builtin": self.builtin,
            "cloned_from": self.cloned_from.to_dict() if self.cloned_from else None,
            "metadata": _jsonable(self.metadata),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "AgentProfile":
        cloned_from = value.get("cloned_from")
        return cls(
            profile_id=str(value["profile_id"]),
            version=int(value["version"]),
            display_name=str(value["display_name"]),
            role=str(value["role"]),
            instructions=str(value["instructions"]),
            allowed_tools=tuple(value.get("allowed_tools", ())),
            allowed_child_roles=tuple(value.get("allowed_child_roles", ())),
            permission_mode=str(value.get("permission_mode", "interactive")),
            model_policy=str(value.get("model_policy", "quality-first")),
            max_iterations=int(value.get("max_iterations", 12)),
            max_children=int(value.get("max_children", 0)),
            builtin=bool(value.get("builtin", False)),
            cloned_from=(
                ProfileRef.from_dict(cloned_from)
                if isinstance(cloned_from, Mapping)
                else None
            ),
            metadata=dict(value.get("metadata", {})),
        )

    @property
    def content_hash(self) -> str:
        """Stable SHA-256 for persistence, cache keys and replay verification."""
        return _content_hash(self.to_dict())


_CLONE_FIELDS = {
    f.name
    for f in fields(AgentProfileDraft)
    if f.name not in {"profile_id", "base"}
}


def clone_profile(
    source: AgentProfile,
    new_profile_id: str,
    **overrides: Any,
) -> AgentProfileDraft:
    """Clone any published profile into a validated, non-builtin draft.

    Version, builtin status and provenance cannot be supplied by callers.  Publication
    chooses the next version and provenance always points at the exact source version.
    """
    normalized_id = str(new_profile_id).strip()
    unknown = set(overrides) - _CLONE_FIELDS
    if unknown:
        raise ProfileValidationError(
            "unsupported clone override(s): " + ", ".join(sorted(unknown))
        )
    if normalized_id == source.profile_id:
        raise ProfileValidationError("a clone must use a new profile_id")
    values = {
        "display_name": f"{source.display_name} copy",
        "role": source.role,
        "instructions": source.instructions,
        "allowed_tools": source.allowed_tools,
        "allowed_child_roles": source.allowed_child_roles,
        "permission_mode": source.permission_mode,
        "model_policy": source.model_policy,
        "max_iterations": source.max_iterations,
        "max_children": source.max_children,
        "metadata": source.metadata,
    }
    values.update(overrides)
    return AgentProfileDraft(
        profile_id=normalized_id,
        base=source.ref,
        **values,
    )


class ProfileCatalog:
    """In-memory version catalog with optimistic publication checks."""

    def __init__(self, *, include_builtins: bool = True) -> None:
        self._lock = threading.RLock()
        self._versions: dict[str, list[AgentProfile]] = {}
        self._drafts: dict[str, AgentProfileDraft] = {}
        if include_builtins:
            for profile in BUILTIN_PROFILES.values():
                self._versions[profile.profile_id] = [profile]

    def save_draft(self, draft: AgentProfileDraft) -> AgentProfileDraft:
        with self._lock:
            current = self.latest(draft.profile_id, default=None)
            if current and current.builtin:
                raise ProfileValidationError("builtin profiles cannot be overwritten")
            if draft.base is not None:
                base = self.get(draft.base.profile_id, draft.base.version)
                if base is None:
                    raise ProfileValidationError(f"clone base does not exist: {draft.base}")
            self._drafts[draft.profile_id] = draft
            return draft

    def draft(self, profile_id: str) -> Optional[AgentProfileDraft]:
        with self._lock:
            return self._drafts.get(profile_id)

    def publish(
        self,
        profile_id: str,
        *,
        expected_previous_version: Optional[int] = None,
    ) -> AgentProfile:
        with self._lock:
            draft = self._drafts.get(profile_id)
            if draft is None:
                raise KeyError(profile_id)
            current = self.latest(profile_id, default=None)
            actual = current.version if current else 0
            if expected_previous_version is not None and expected_previous_version != actual:
                raise ProfileValidationError(
                    f"stale profile draft: expected version {expected_previous_version}, found {actual}"
                )
            profile = draft.publish(actual + 1)
            self._versions.setdefault(profile_id, []).append(profile)
            del self._drafts[profile_id]
            return profile

    def clone(
        self, source_id: str, new_profile_id: str, **overrides: Any
    ) -> AgentProfileDraft:
        with self._lock:
            source = self.latest(source_id)
            draft = clone_profile(source, new_profile_id, **overrides)
            return self.save_draft(draft)

    def get(self, profile_id: str, version: int) -> Optional[AgentProfile]:
        with self._lock:
            return next(
                (p for p in self._versions.get(profile_id, ()) if p.version == version),
                None,
            )

    def latest(
        self, profile_id: str, *, default: Any = ...
    ) -> Optional[AgentProfile]:
        with self._lock:
            versions = self._versions.get(profile_id, ())
            if versions:
                return versions[-1]
            if default is ...:
                raise KeyError(profile_id)
            return default

    def versions(self, profile_id: str) -> tuple[AgentProfile, ...]:
        with self._lock:
            return tuple(self._versions.get(profile_id, ()))

    def list(self) -> tuple[AgentProfile, ...]:
        with self._lock:
            return tuple(self.latest(profile_id) for profile_id in sorted(self._versions))


ProfileRegistry = ProfileCatalog


def _builtin(
    role: AgentRole,
    *,
    instructions: str,
    tools: tuple[str, ...],
    children: tuple[AgentRole, ...] = (),
    mode: str = "interactive",
    max_iterations: int = 12,
    metadata: Optional[Mapping[str, Any]] = None,
) -> AgentProfile:
    return AgentProfile(
        profile_id=role.value,
        version=1,
        display_name=role.value.title(),
        role=role,
        instructions=instructions,
        allowed_tools=tools,
        allowed_child_roles=children,
        permission_mode=mode,
        model_policy="quality-first",
        max_iterations=max_iterations,
        max_children=8 if children else 0,
        builtin=True,
        metadata=dict(metadata or {}),
    )


_BOUNDED_CODE_COMMANDS = [
    # Read-only repository inspection. Candidate mutation uses native file tools,
    # never shell redirects or git state-changing commands.
    "git status",
    "git diff",
    "git log",
    "git show",
    "git grep",
    "git ls-files",
    # Bounded build, lint, and test entry points. PermissionEngine rejects shell
    # operators before prefix matching, so these cannot be chained with a second
    # command. Custom profiles may replace this frozen list explicitly.
    "pytest",
    "python -m pytest",
    "python -m unittest",
    "python -m compileall",
    "python -m ruff",
    "python -m mypy",
    "ruff",
    "mypy",
    "npm test",
    "npm run test",
    "npm run build",
    "npm run lint",
    "pnpm test",
    "pnpm run test",
    "pnpm run build",
    "pnpm run lint",
    "yarn test",
    "yarn build",
    "yarn lint",
    "cargo test",
    "cargo check",
    "cargo build",
    "cargo clippy",
    "go test",
    "go build",
    "go vet",
    "dotnet test",
    "dotnet build",
    "mvn test",
    "mvn package",
    "gradle test",
    "gradle build",
    "./gradlew test",
    "./gradlew build",
]


_BUILTINS = {
    AgentRole.ORCHESTRATOR.value: _builtin(
        AgentRole.ORCHESTRATOR,
        instructions="Decompose work, delegate bounded tasks, and synthesize verified results.",
        tools=("spawn_agent", "wait_agent", "cancel_agent", "ask_user", "todo_write"),
        children=tuple(AgentRole),
        max_iterations=8,
    ),
    AgentRole.PLANNER.value: _builtin(
        AgentRole.PLANNER,
        instructions="Inspect evidence and produce a dependency-aware implementation plan.",
        tools=("list_files", "read_file", "grep", "git_status", "git_diff", "git_log", "spawn_agent", "wait_agent", "cancel_agent"),
        children=(AgentRole.WORKER,),
        mode="plan",
        max_iterations=6,
    ),
    AgentRole.WORKER.value: _builtin(
        AgentRole.WORKER,
        instructions="Complete one scoped assignment and return structured evidence.",
        tools=("list_files", "read_file", "grep", "write_file", "apply_patch", "run_shell", "git_status", "git_diff", "todo_write", "spawn_agent", "wait_agent", "cancel_agent"),
        children=(AgentRole.WORKER, AgentRole.TESTER),
        max_iterations=12,
        metadata={"allowed_commands": _BOUNDED_CODE_COMMANDS},
    ),
    AgentRole.REVIEWER.value: _builtin(
        AgentRole.REVIEWER,
        instructions="Review changes read-only and report prioritized, evidence-backed findings.",
        tools=("list_files", "read_file", "grep", "git_status", "git_diff", "git_log", "submit_verdict"),
        mode="plan",
        max_iterations=6,
    ),
    AgentRole.TESTER.value: _builtin(
        AgentRole.TESTER,
        instructions="Run the bounded verification commands and report reproducible results.",
        tools=("list_files", "read_file", "grep", "run_shell", "git_status", "git_diff", "submit_verdict"),
        max_iterations=8,
        metadata={
            # This is a hard runtime ceiling, not merely an auto-approval list.
            # Shell chaining/redirection is rejected by PermissionEngine before
            # prefix matching, and a human approval cannot exceed this set.
            "allowed_commands": _BOUNDED_CODE_COMMANDS
        },
    ),
    AgentRole.EVALUATOR.value: _builtin(
        AgentRole.EVALUATOR,
        instructions="Compare results with acceptance criteria and recommend accept, retry, replan, or escalate.",
        tools=("list_files", "read_file", "grep", "git_diff", "submit_verdict"),
        mode="plan",
        max_iterations=4,
    ),
    AgentRole.SCORER.value: _builtin(
        AgentRole.SCORER,
        instructions="Score candidate results against explicit criteria and cite the supporting evidence.",
        tools=("list_files", "read_file", "grep", "git_diff", "submit_verdict"),
        mode="plan",
        max_iterations=4,
    ),
    AgentRole.EXPLORER.value: _builtin(
        AgentRole.EXPLORER,
        instructions="Explore the scoped code and evidence, then return concise findings and open questions.",
        tools=("list_files", "read_file", "grep", "git_status", "git_diff", "git_log"),
        mode="plan",
        max_iterations=8,
    ),
    AgentRole.INTEGRATOR.value: _builtin(
        AgentRole.INTEGRATOR,
        instructions="Integrate accepted candidates, resolve bounded conflicts, and verify the combined result.",
        tools=("list_files", "read_file", "grep", "write_file", "apply_patch", "run_shell", "git_status", "git_diff"),
        max_iterations=10,
        metadata={"allowed_commands": _BOUNDED_CODE_COMMANDS},
    ),
}

BUILTIN_PROFILES: Mapping[str, AgentProfile] = MappingProxyType(_BUILTINS)


def builtin_profile(profile_id: str) -> AgentProfile:
    try:
        return BUILTIN_PROFILES[profile_id]
    except KeyError as exc:
        raise KeyError(f"unknown builtin profile: {profile_id}") from exc
