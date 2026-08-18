"""Runtime feature flags and limits for structured Agent handoffs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class HandoffRuntimeSettings:
    structured_handoff_enabled: bool = True
    # Stage C rollout: new explicit Briefs use TCHP, while legacy spawn remains
    # available until operators deliberately advance to the required stage.
    structured_handoff_required_for_new_tasks: bool = False
    legacy_spawn_agent_enabled: bool = True
    default_context_token_budget: int = 8_000
    max_context_refs: int = 50
    max_inline_bytes_per_ref: int = 8_192
    max_inline_bytes_total: int = 32_768
    max_comment_batch: int = 100
    wake_coalesce_window_ms: int = 1_000
    wake_max_attempts: int = 5
    wake_backoff_seconds: int = 1
    context_read_audit_enabled: bool = True
    transcript_sharing_default: bool = False

    def __post_init__(self) -> None:
        if (
            self.structured_handoff_required_for_new_tasks
            and not self.structured_handoff_enabled
        ):
            raise ValueError(
                "structured handoff must be enabled when it is required for new tasks"
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "HandoffRuntimeSettings":
        raw = dict(value or {})
        defaults = cls()

        def flag(name: str, default: bool) -> bool:
            selected = raw.get(name, default)
            if isinstance(selected, str):
                normalized = selected.strip().lower()
                if normalized in {"true", "1", "yes", "on"}:
                    return True
                if normalized in {"false", "0", "no", "off"}:
                    return False
                raise ValueError(f"{name} must be a boolean")
            return bool(selected)

        def bounded(name: str, default: int, minimum: int, maximum: int) -> int:
            selected = int(raw.get(name, default))
            if selected < minimum or selected > maximum:
                raise ValueError(
                    f"{name} must be between {minimum} and {maximum}"
                )
            return selected

        return cls(
            structured_handoff_enabled=flag(
                "structured_handoff_enabled",
                defaults.structured_handoff_enabled,
            ),
            structured_handoff_required_for_new_tasks=flag(
                "structured_handoff_required_for_new_tasks",
                defaults.structured_handoff_required_for_new_tasks,
            ),
            legacy_spawn_agent_enabled=flag(
                "legacy_spawn_agent_enabled", defaults.legacy_spawn_agent_enabled
            ),
            default_context_token_budget=bounded(
                "default_context_token_budget",
                defaults.default_context_token_budget,
                0,
                1_000_000,
            ),
            max_context_refs=bounded(
                "max_context_refs", defaults.max_context_refs, 0, 1_000
            ),
            max_inline_bytes_per_ref=bounded(
                "max_inline_bytes_per_ref",
                defaults.max_inline_bytes_per_ref,
                0,
                65_536,
            ),
            max_inline_bytes_total=bounded(
                "max_inline_bytes_total",
                defaults.max_inline_bytes_total,
                0,
                65_536,
            ),
            max_comment_batch=bounded(
                "max_comment_batch", defaults.max_comment_batch, 1, 1_000
            ),
            wake_coalesce_window_ms=bounded(
                "wake_coalesce_window_ms",
                defaults.wake_coalesce_window_ms,
                0,
                60_000,
            ),
            wake_max_attempts=bounded(
                "wake_max_attempts", defaults.wake_max_attempts, 1, 100
            ),
            wake_backoff_seconds=bounded(
                "wake_backoff_seconds",
                defaults.wake_backoff_seconds,
                1,
                3_600,
            ),
            context_read_audit_enabled=flag(
                "context_read_audit_enabled",
                defaults.context_read_audit_enabled,
            ),
            transcript_sharing_default=flag(
                "transcript_sharing_default", False
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
        }
