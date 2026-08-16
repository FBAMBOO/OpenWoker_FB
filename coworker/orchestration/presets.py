"""Immutable built-in runtime presets for auditable mixed-Agent routing.

Runtime presets are deliberately additive.  Legacy API callers that omit
``runtime_preset_id`` keep the existing routing behavior, while a client can select a
named, versioned role-to-runtime contract and have that exact snapshot frozen into the
task and plan metadata.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

from .profiles import AgentRole
from .subscription_runtime import (
    CLAUDE_OPUS_5_HIGH,
    CLAUDE_OPUS_5_MAX,
    CODEX_GPT_5_6_SOL_MAX,
)


_PRESET_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,95}$")


def _canonical_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _freeze_json(value: Any) -> Any:
    """Recursively freeze JSON-compatible preset metadata."""

    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"runtime preset metadata is not JSON-compatible: {type(value).__name__}")


def _thaw_json(value: Any) -> Any:
    """Return a detached JSON-compatible copy of recursively frozen metadata."""

    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class RuntimePreset:
    """A versioned role-to-runtime contract, not a mutable provider preference."""

    preset_id: str
    version: int
    display_name: str
    description: str
    role_models: Mapping[AgentRole | str, str]
    domains: tuple[str, ...] = ("code",)
    require_review: bool = True
    require_tests: bool = True
    default_for_domains: tuple[str, ...] = ()
    fallback_mode: str = "strict"
    plan_template: str = "legacy"
    metadata: Mapping[str, Any] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        preset_id = str(self.preset_id).strip()
        if not _PRESET_ID.fullmatch(preset_id):
            raise ValueError("runtime preset id must be a lowercase, filesystem-safe slug")
        version = int(self.version)
        if version < 1:
            raise ValueError("runtime preset version must be positive")
        display_name = str(self.display_name).strip()
        description = str(self.description).strip()
        if not display_name or not description:
            raise ValueError("runtime preset display_name and description are required")
        role_models: dict[AgentRole, str] = {}
        for role, model in self.role_models.items():
            normalized_role = role if isinstance(role, AgentRole) else AgentRole(str(role))
            normalized_model = str(model).strip()
            if not normalized_model:
                raise ValueError(f"runtime preset model is empty for role {normalized_role.value}")
            role_models[normalized_role] = normalized_model
        domains = tuple(dict.fromkeys(str(item).strip() for item in self.domains))
        defaults = tuple(
            dict.fromkeys(str(item).strip() for item in self.default_for_domains)
        )
        if not domains or any(not item for item in domains):
            raise ValueError("runtime preset must support at least one domain")
        if any(item not in domains for item in defaults):
            raise ValueError("default_for_domains must be a subset of domains")
        fallback_mode = str(self.fallback_mode).strip().lower()
        if fallback_mode not in {"strict"}:
            raise ValueError("only strict runtime preset fallback is supported")
        object.__setattr__(self, "preset_id", preset_id)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "description", description)
        object.__setattr__(self, "role_models", MappingProxyType(role_models))
        object.__setattr__(self, "domains", domains)
        object.__setattr__(self, "default_for_domains", defaults)
        object.__setattr__(self, "fallback_mode", fallback_mode)
        object.__setattr__(self, "plan_template", str(self.plan_template).strip())
        object.__setattr__(self, "metadata", _freeze_json(self.metadata))

    def model_for(self, role: AgentRole | str) -> str:
        normalized = role if isinstance(role, AgentRole) else AgentRole(str(role))
        try:
            return self.role_models[normalized]
        except KeyError as exc:
            raise ValueError(
                f"runtime preset {self.preset_id} has no model for role {normalized.value}"
            ) from exc

    def to_dict(self) -> dict[str, Any]:
        value = {
            "schema_version": 1,
            "preset_id": self.preset_id,
            "version": self.version,
            "display_name": self.display_name,
            "description": self.description,
            "role_models": {
                role.value: model
                for role, model in sorted(
                    self.role_models.items(), key=lambda item: item[0].value
                )
            },
            "domains": list(self.domains),
            "require_review": bool(self.require_review),
            "require_tests": bool(self.require_tests),
            "default_for_domains": list(self.default_for_domains),
            "fallback_mode": self.fallback_mode,
            "plan_template": self.plan_template,
            "metadata": _thaw_json(self.metadata),
        }
        return {**value, "content_hash": _canonical_hash(value)}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RuntimePreset":
        return cls(
            preset_id=str(value["preset_id"]),
            version=int(value["version"]),
            display_name=str(value["display_name"]),
            description=str(value["description"]),
            role_models=dict(value.get("role_models") or {}),
            domains=tuple(value.get("domains") or ()),
            require_review=bool(value.get("require_review", True)),
            require_tests=bool(value.get("require_tests", True)),
            default_for_domains=tuple(value.get("default_for_domains") or ()),
            fallback_mode=str(value.get("fallback_mode") or "strict"),
            plan_template=str(value.get("plan_template") or "legacy"),
            metadata=dict(value.get("metadata") or {}),
        )


PRODUCTION_CODEX_LED_MIXED_V1_ID = "production-codex-led-mixed-v1"

PRODUCTION_CODEX_LED_MIXED_V1 = RuntimePreset(
    preset_id=PRODUCTION_CODEX_LED_MIXED_V1_ID,
    version=1,
    display_name="Production · Codex-led mixed Agents",
    description=(
        "Codex owns semantic understanding, repository exploration, planning, "
        "implementation, and integration; isolated Claude Agents own review, "
        "testing, and evidence evaluation."
    ),
    role_models={
        AgentRole.ORCHESTRATOR: CODEX_GPT_5_6_SOL_MAX,
        AgentRole.SCORER: CODEX_GPT_5_6_SOL_MAX,
        AgentRole.EXPLORER: CODEX_GPT_5_6_SOL_MAX,
        AgentRole.PLANNER: CODEX_GPT_5_6_SOL_MAX,
        AgentRole.WORKER: CODEX_GPT_5_6_SOL_MAX,
        AgentRole.INTEGRATOR: CODEX_GPT_5_6_SOL_MAX,
        AgentRole.REVIEWER: CLAUDE_OPUS_5_HIGH,
        AgentRole.TESTER: CLAUDE_OPUS_5_MAX,
        AgentRole.EVALUATOR: CLAUDE_OPUS_5_MAX,
    },
    domains=("code",),
    require_review=True,
    require_tests=True,
    default_for_domains=("code",),
    fallback_mode="strict",
    plan_template="codex-led-code-v1",
    metadata={
        "control_plane_authority": [
            "deterministic_complexity",
            "deterministic_acceptance",
            "human_gate",
            "deterministic_archive",
        ],
        "subscription_fallbacks": [],
        "kimi_subscription_eligible": False,
    },
)

BUILTIN_RUNTIME_PRESETS: Mapping[str, RuntimePreset] = MappingProxyType(
    {PRODUCTION_CODEX_LED_MIXED_V1.preset_id: PRODUCTION_CODEX_LED_MIXED_V1}
)


def runtime_preset(preset_id: str) -> RuntimePreset:
    normalized = str(preset_id).strip()
    try:
        return BUILTIN_RUNTIME_PRESETS[normalized]
    except KeyError as exc:
        raise KeyError(f"unknown runtime preset: {normalized}") from exc


def runtime_presets() -> tuple[RuntimePreset, ...]:
    return tuple(BUILTIN_RUNTIME_PRESETS[key] for key in sorted(BUILTIN_RUNTIME_PRESETS))
