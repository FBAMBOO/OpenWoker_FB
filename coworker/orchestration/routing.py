"""Deterministic, capability-safe model selection with replayable audit records."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional


class RoutingError(RuntimeError):
    pass


class NoEligibleModelError(RoutingError):
    def __init__(self, decision: "RoutingDecision") -> None:
        self.decision = decision
        rejected = "; ".join(
            f"{item.model_id}: {', '.join(item.reasons)}"
            for item in decision.evaluations
        )
        super().__init__("no eligible model" + (f" ({rejected})" if rejected else ""))


_KNOWN_PROVIDER_PREFIXES = frozenset(
    {
        "openai",
        "anthropic",
        "gemini",
        "bedrock",
        "vertex",
        "ollama",
        "zai",
        "deepseek",
        "kimi",
        "minimax",
        "qwen",
        "xai",
        "mistral",
        "meta",
        "together",
        "fireworks",
        "openrouter",
        # Full local Agent loops authenticated through the user's existing CLI
        # subscription. These are orchestration runtimes, not ordinary API providers.
        "codex-subscription",
        "claude-code-subscription",
        "kimi-code-subscription",
    }
)


def _recognized_prefix(model_id: str) -> Optional[str]:
    if ":" not in model_id:
        return None
    prefix = model_id.split(":", 1)[0].lower()
    return prefix if prefix in _KNOWN_PROVIDER_PREFIXES else None


def canonical_model_id(model_id: str, provider: Optional[str] = None) -> str:
    """Return the routed id convention used by ProviderRouter.

    OpenAI is the default provider and therefore canonical OpenAI ids are bare.  Other
    providers retain one leading provider prefix; colons inside their vendor model id are
    preserved.
    """
    raw = str(model_id).strip()
    if not raw:
        raise ValueError("model_id must not be empty")
    explicit_provider = str(provider or "").strip().lower()
    prefixed_provider = _recognized_prefix(raw)
    normalized_raw = (
        f"{prefixed_provider}:{raw.split(':', 1)[1]}"
        if prefixed_provider
        else raw
    )
    if explicit_provider and explicit_provider not in _KNOWN_PROVIDER_PREFIXES:
        raise ValueError(f"unknown provider: {explicit_provider!r}")
    if explicit_provider == "openai":
        if prefixed_provider and prefixed_provider != "openai":
            raise ValueError(
                f"model prefix {prefixed_provider!r} conflicts with provider 'openai'"
            )
        if prefixed_provider == "openai":
            stripped = normalized_raw.split(":", 1)[1]
            nested = _recognized_prefix(stripped)
            if nested and nested != "openai":
                raise ValueError(
                    f"model prefix {nested!r} conflicts with provider 'openai'"
                )
            return stripped
        return normalized_raw
    if explicit_provider:
        if prefixed_provider and prefixed_provider != explicit_provider:
            raise ValueError(
                f"model prefix {prefixed_provider!r} conflicts with provider "
                f"{explicit_provider!r}"
            )
        return (
            normalized_raw
            if prefixed_provider
            else f"{explicit_provider}:{normalized_raw}"
        )
    if prefixed_provider == "openai":
        return normalized_raw.split(":", 1)[1]
    return normalized_raw


def provider_for(model_id: str) -> str:
    return _recognized_prefix(model_id) or "openai"


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(v).strip() for v in values if str(v).strip()))


def _json_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ModelCandidate:
    model_id: str
    quality: int
    capabilities: frozenset[str] = frozenset({"tools", "streaming"})
    provider: str = ""
    context_window: Optional[int] = None
    input_microusd_per_million: Optional[int] = None
    output_microusd_per_million: Optional[int] = None
    latency_rank: int = 0
    configured: bool = True
    available: bool = True
    verified: bool = True
    catalog_revision: str = ""

    def __post_init__(self) -> None:
        explicit_provider = str(self.provider).strip().lower()
        routed = canonical_model_id(self.model_id, explicit_provider or None)
        provider = provider_for(routed)
        if explicit_provider and provider != explicit_provider:
            raise ValueError(
                f"canonical model provider {provider!r} conflicts with explicit "
                f"provider {explicit_provider!r}"
            )
        if not 0 <= int(self.quality) <= 100:
            raise ValueError("quality must be between 0 and 100")
        if self.context_window is not None and int(self.context_window) <= 0:
            raise ValueError("context_window must be positive")
        for name in ("input_microusd_per_million", "output_microusd_per_million"):
            value = getattr(self, name)
            if value is not None and int(value) < 0:
                raise ValueError(f"{name} must be non-negative")
        if int(self.latency_rank) < 0:
            raise ValueError("latency_rank must be non-negative")
        object.__setattr__(self, "model_id", routed)
        object.__setattr__(self, "provider", provider)
        object.__setattr__(
            self,
            "capabilities",
            frozenset(str(c).strip().lower() for c in self.capabilities if str(c).strip()),
        )
        object.__setattr__(self, "quality", int(self.quality))
        object.__setattr__(
            self,
            "context_window",
            int(self.context_window) if self.context_window is not None else None,
        )
        for name in (
            "input_microusd_per_million",
            "output_microusd_per_million",
        ):
            value = getattr(self, name)
            object.__setattr__(self, name, int(value) if value is not None else None)
        object.__setattr__(self, "latency_rank", int(self.latency_rank))
        for name in ("configured", "available", "verified"):
            object.__setattr__(self, name, bool(getattr(self, name)))
        object.__setattr__(self, "catalog_revision", str(self.catalog_revision))

    @classmethod
    def from_capabilities(
        cls,
        model_id: str,
        *,
        quality: int,
        capabilities: Any,
        **kwargs: Any,
    ) -> "ModelCandidate":
        names = {
            name
            for name in ("tools", "vision", "pdf", "parallel_tool_calls", "streaming")
            if bool(getattr(capabilities, name, False))
        }
        return cls(model_id=model_id, quality=quality, capabilities=frozenset(names), **kwargs)

    def estimated_cost(self, input_tokens: int, output_tokens: int) -> Optional[int]:
        if self.input_microusd_per_million is None or self.output_microusd_per_million is None:
            return None
        numerator = (
            int(input_tokens) * self.input_microusd_per_million
            + int(output_tokens) * self.output_microusd_per_million
        )
        return int(math.ceil(numerator / 1_000_000))

    def audit_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "provider": self.provider,
            "quality": self.quality,
            "capabilities": sorted(self.capabilities),
            "context_window": self.context_window,
            "input_microusd_per_million": self.input_microusd_per_million,
            "output_microusd_per_million": self.output_microusd_per_million,
            "latency_rank": self.latency_rank,
            "configured": self.configured,
            "available": self.available,
            "verified": self.verified,
            "catalog_revision": self.catalog_revision,
        }


@dataclass(frozen=True, slots=True)
class ModelPolicy:
    policy_id: str = "quality-first"
    version: int = 1
    require_verified: bool = True
    # Unknown prices are acceptable until the request supplies a hard cost ceiling.
    # This keeps locally configured/custom models routable without pretending a quoted
    # price exists; the audit record still carries ``None`` for their estimate.
    allow_unknown_cost: bool = True
    allowed_providers: tuple[str, ...] = ()
    allowed_models: tuple[str, ...] = ()
    blocked_models: tuple[str, ...] = ()
    fallback_limit: int = 2
    fallback_for_explicit: bool = False

    def __post_init__(self) -> None:
        if not str(self.policy_id).strip():
            raise ValueError("policy_id must not be empty")
        if int(self.version) < 1:
            raise ValueError("policy version must be positive")
        if not 0 <= int(self.fallback_limit) <= 8:
            raise ValueError("fallback_limit must be between 0 and 8")
        object.__setattr__(self, "policy_id", str(self.policy_id).strip())
        object.__setattr__(self, "version", int(self.version))
        for name in (
            "require_verified",
            "allow_unknown_cost",
            "fallback_for_explicit",
        ):
            object.__setattr__(self, name, bool(getattr(self, name)))
        object.__setattr__(self, "allowed_providers", _unique(p.lower() for p in self.allowed_providers))
        object.__setattr__(
            self,
            "allowed_models",
            tuple(canonical_model_id(m) for m in _unique(self.allowed_models)),
        )
        object.__setattr__(
            self,
            "blocked_models",
            tuple(canonical_model_id(m) for m in _unique(self.blocked_models)),
        )
        object.__setattr__(self, "fallback_limit", int(self.fallback_limit))

    def audit_dict(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "require_verified": self.require_verified,
            "allow_unknown_cost": self.allow_unknown_cost,
            "allowed_providers": list(self.allowed_providers),
            "allowed_models": list(self.allowed_models),
            "blocked_models": list(self.blocked_models),
            "fallback_limit": self.fallback_limit,
            "fallback_for_explicit": self.fallback_for_explicit,
        }


QUALITY_FIRST_POLICY = ModelPolicy()


@dataclass(frozen=True, slots=True)
class RoutingRequest:
    purpose: str
    required_capabilities: frozenset[str] = frozenset({"tools"})
    input_tokens: int = 0
    reserved_output_tokens: int = 4096
    minimum_context: int = 0
    max_cost_microusd: Optional[int] = None
    requested_model: Optional[str] = None
    preferred_models: tuple[str, ...] = ()
    allowed_providers: tuple[str, ...] = ()
    excluded_models: tuple[str, ...] = ()
    correlation: Mapping[str, str] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        if not str(self.purpose).strip():
            raise ValueError("purpose must not be empty")
        for name in ("input_tokens", "reserved_output_tokens", "minimum_context"):
            if int(getattr(self, name)) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.max_cost_microusd is not None and int(self.max_cost_microusd) < 0:
            raise ValueError("max_cost_microusd must be non-negative")
        object.__setattr__(self, "purpose", str(self.purpose).strip())
        object.__setattr__(
            self,
            "required_capabilities",
            frozenset(str(c).strip().lower() for c in self.required_capabilities if str(c).strip()),
        )
        object.__setattr__(self, "input_tokens", int(self.input_tokens))
        object.__setattr__(self, "reserved_output_tokens", int(self.reserved_output_tokens))
        object.__setattr__(self, "minimum_context", int(self.minimum_context))
        object.__setattr__(
            self,
            "max_cost_microusd",
            int(self.max_cost_microusd)
            if self.max_cost_microusd is not None
            else None,
        )
        object.__setattr__(
            self,
            "requested_model",
            canonical_model_id(self.requested_model) if self.requested_model else None,
        )
        object.__setattr__(
            self,
            "preferred_models",
            tuple(canonical_model_id(m) for m in _unique(self.preferred_models)),
        )
        object.__setattr__(self, "allowed_providers", _unique(p.lower() for p in self.allowed_providers))
        object.__setattr__(
            self,
            "excluded_models",
            tuple(canonical_model_id(m) for m in _unique(self.excluded_models)),
        )
        object.__setattr__(
            self,
            "correlation",
            MappingProxyType(
                dict(sorted((str(k), str(v)) for k, v in self.correlation.items()))
            ),
        )

    def audit_dict(self) -> dict[str, Any]:
        return {
            "purpose": self.purpose,
            "required_capabilities": sorted(self.required_capabilities),
            "input_tokens": self.input_tokens,
            "reserved_output_tokens": self.reserved_output_tokens,
            "minimum_context": self.minimum_context,
            "max_cost_microusd": self.max_cost_microusd,
            "requested_model": self.requested_model,
            "preferred_models": list(self.preferred_models),
            "allowed_providers": list(self.allowed_providers),
            "excluded_models": list(self.excluded_models),
            "correlation": dict(self.correlation),
        }


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    model_id: str
    provider: str
    eligible: bool
    reasons: tuple[str, ...]
    quality: int
    estimated_cost_microusd: Optional[int]
    latency_rank: int
    rank: Optional[int] = None

    def audit_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "provider": self.provider,
            "eligible": self.eligible,
            "reasons": list(self.reasons),
            "quality": self.quality,
            "estimated_cost_microusd": self.estimated_cost_microusd,
            "latency_rank": self.latency_rank,
            "rank": self.rank,
        }


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    decision_id: str
    selected_model: Optional[str]
    fallback_models: tuple[str, ...]
    request: RoutingRequest
    policy: ModelPolicy
    catalog_hash: str
    evaluations: tuple[CandidateEvaluation, ...]
    reason: str

    @property
    def selected_provider(self) -> Optional[str]:
        return provider_for(self.selected_model) if self.selected_model else None

    def audit_record(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "selected_model": self.selected_model,
            "selected_provider": self.selected_provider,
            "fallback_models": list(self.fallback_models),
            "reason": self.reason,
            "request": self.request.audit_dict(),
            "policy": self.policy.audit_dict(),
            "catalog_hash": self.catalog_hash,
            "evaluations": [item.audit_dict() for item in self.evaluations],
        }


class ModelRouter:
    """Pure routing policy: same catalog + request + policy always gives the same decision."""

    def __init__(
        self,
        candidates: Iterable[ModelCandidate],
        *,
        policy: ModelPolicy = QUALITY_FIRST_POLICY,
    ) -> None:
        catalog: dict[str, ModelCandidate] = {}
        for candidate in candidates:
            if candidate.model_id in catalog:
                raise ValueError(f"duplicate model candidate: {candidate.model_id}")
            catalog[candidate.model_id] = candidate
        self._catalog = catalog
        self.policy = policy
        self.catalog_hash = _json_hash(
            [catalog[mid].audit_dict() for mid in sorted(catalog)]
        )

    @property
    def candidates(self) -> tuple[ModelCandidate, ...]:
        return tuple(self._catalog[mid] for mid in sorted(self._catalog))

    def _reasons(
        self, candidate: ModelCandidate, request: RoutingRequest, cost: Optional[int]
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        if (
            request.requested_model
            and candidate.model_id != request.requested_model
            and not self.policy.fallback_for_explicit
        ):
            reasons.append("not explicitly requested")
        if not candidate.configured:
            reasons.append("provider is not configured")
        if not candidate.available:
            reasons.append("model is unavailable")
        if self.policy.require_verified and not candidate.verified:
            reasons.append("model is not verified for automatic routing")
        if self.policy.allowed_providers and candidate.provider not in self.policy.allowed_providers:
            reasons.append("provider is outside policy allowlist")
        if request.allowed_providers and candidate.provider not in request.allowed_providers:
            reasons.append("provider is outside request allowlist")
        if self.policy.allowed_models and candidate.model_id not in self.policy.allowed_models:
            reasons.append("model is outside policy allowlist")
        if candidate.model_id in self.policy.blocked_models:
            reasons.append("model is blocked by policy")
        if candidate.model_id in request.excluded_models:
            reasons.append("model is excluded by request")
        missing = sorted(request.required_capabilities - candidate.capabilities)
        if missing:
            reasons.append("missing capabilities: " + ", ".join(missing))
        required_context = max(
            request.minimum_context,
            request.input_tokens + request.reserved_output_tokens,
        )
        if required_context:
            if candidate.context_window is None:
                reasons.append("context window is unverified")
            elif candidate.context_window < required_context:
                reasons.append(
                    f"context window {candidate.context_window} is below required {required_context}"
                )
        if cost is None and not self.policy.allow_unknown_cost:
            reasons.append("cost is unknown")
        if request.max_cost_microusd is not None:
            if cost is None:
                reasons.append("hard cost budget cannot be verified")
            elif cost > request.max_cost_microusd:
                reasons.append(
                    f"estimated cost {cost} exceeds budget {request.max_cost_microusd}"
                )
        return tuple(reasons)

    def route(self, request: RoutingRequest) -> RoutingDecision:
        raw: list[tuple[ModelCandidate, Optional[int], tuple[str, ...]]] = []
        for candidate in self._catalog.values():
            cost = candidate.estimated_cost(
                request.input_tokens, request.reserved_output_tokens
            )
            raw.append((candidate, cost, self._reasons(candidate, request, cost)))

        preferred = {model: index for index, model in enumerate(request.preferred_models)}

        def rank_key(item: tuple[ModelCandidate, Optional[int], tuple[str, ...]]):
            candidate, cost, _ = item
            # Strictly quality first.  Preferences, known/lower cost and latency only break
            # equal-quality ties; canonical id is the final deterministic tie-breaker. An
            # explicit model is an operator override and therefore precedes fallback rank.
            return (
                bool(request.requested_model and candidate.model_id != request.requested_model),
                -candidate.quality,
                preferred.get(candidate.model_id, len(preferred) + 1),
                cost is None,
                cost if cost is not None else 0,
                candidate.latency_rank,
                candidate.model_id,
            )

        eligible = sorted((item for item in raw if not item[2]), key=rank_key)
        rank_by_model = {item[0].model_id: i + 1 for i, item in enumerate(eligible)}
        evaluations = tuple(
            CandidateEvaluation(
                model_id=candidate.model_id,
                provider=candidate.provider,
                eligible=not reasons,
                reasons=reasons,
                quality=candidate.quality,
                estimated_cost_microusd=cost,
                latency_rank=candidate.latency_rank,
                rank=rank_by_model.get(candidate.model_id),
            )
            for candidate, cost, reasons in sorted(raw, key=lambda item: item[0].model_id)
        )
        selected = eligible[0][0].model_id if eligible else None
        allow_fallback = not request.requested_model or self.policy.fallback_for_explicit
        fallback = (
            tuple(item[0].model_id for item in eligible[1 : 1 + self.policy.fallback_limit])
            if allow_fallback
            else ()
        )
        reason = (
            "explicit model passed all hard constraints"
            if selected and selected == request.requested_model
            else "explicit model failed hard constraints; selected best eligible fallback"
            if selected and request.requested_model
            else "highest-quality eligible model; deterministic tie-breaks applied"
            if selected
            else "no model passed all hard constraints"
        )
        audit_body = {
            "selected_model": selected,
            "fallback_models": list(fallback),
            "request": request.audit_dict(),
            "policy": self.policy.audit_dict(),
            "catalog_hash": self.catalog_hash,
            "evaluations": [item.audit_dict() for item in evaluations],
            "reason": reason,
        }
        decision_id = "route-" + _json_hash(audit_body)[:24]
        return RoutingDecision(
            decision_id=decision_id,
            selected_model=selected,
            fallback_models=fallback,
            request=request,
            policy=self.policy,
            catalog_hash=self.catalog_hash,
            evaluations=evaluations,
            reason=reason,
        )

    def select(self, request: RoutingRequest) -> RoutingDecision:
        decision = self.route(request)
        if decision.selected_model is None:
            raise NoEligibleModelError(decision)
        return decision


def route_model(
    candidates: Iterable[ModelCandidate],
    request: RoutingRequest,
    *,
    policy: ModelPolicy = QUALITY_FIRST_POLICY,
) -> RoutingDecision:
    return ModelRouter(candidates, policy=policy).select(request)
