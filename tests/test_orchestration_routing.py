import pytest

from coworker.orchestration.routing import (
    ModelCandidate,
    ModelPolicy,
    ModelRouter,
    NoEligibleModelError,
    RoutingRequest,
    canonical_model_id,
)


def candidate(model_id, quality, **kwargs):
    context_window = kwargs.pop("context_window", 32_000)
    return ModelCandidate(
        model_id=model_id,
        quality=quality,
        context_window=context_window,
        **kwargs,
    )


def test_quality_is_primary_and_ties_are_deterministic():
    models = [
        candidate("cheap", 80, input_microusd_per_million=1, output_microusd_per_million=1),
        candidate("best", 99),
        candidate("also-best", 99, latency_rank=2),
    ]
    request = RoutingRequest(
        purpose="implementation",
        input_tokens=100,
        preferred_models=("cheap",),
    )
    first = ModelRouter(models).select(request)
    reversed_order = ModelRouter(reversed(models)).select(request)

    assert first.selected_model == "best"
    assert first.fallback_models == ("also-best", "cheap")
    assert reversed_order.selected_model == first.selected_model
    assert reversed_order.decision_id == first.decision_id
    assert first.audit_record()["evaluations"]


def test_capability_context_availability_and_verification_are_hard_filters():
    models = [
        candidate("no-vision", 100),
        candidate("small", 99, capabilities=frozenset({"tools", "vision"}), context_window=100),
        candidate("unverified", 98, capabilities=frozenset({"tools", "vision"}), verified=False),
        candidate("eligible", 90, capabilities=frozenset({"tools", "vision"})),
    ]
    decision = ModelRouter(models).select(
        RoutingRequest(
            purpose="inspect image",
            required_capabilities=frozenset({"tools", "vision"}),
            input_tokens=1_000,
            reserved_output_tokens=500,
        )
    )

    assert decision.selected_model == "eligible"
    rejected = {item.model_id: item.reasons for item in decision.evaluations}
    assert any("missing capabilities" in reason for reason in rejected["no-vision"])
    assert any("context window" in reason for reason in rejected["small"])
    assert any("not verified" in reason for reason in rejected["unverified"])


def test_unknown_cost_is_allowed_without_budget_but_rejected_by_hard_budget():
    router = ModelRouter([candidate("unknown-price", 90)])
    assert router.select(RoutingRequest(purpose="no hard budget")).selected_model == "unknown-price"

    with pytest.raises(NoEligibleModelError) as error:
        router.select(RoutingRequest(purpose="budgeted", max_cost_microusd=100))
    reasons = error.value.decision.evaluations[0].reasons
    assert "hard cost budget cannot be verified" in reasons


def test_explicit_model_can_have_ranked_fallbacks_when_policy_allows():
    router = ModelRouter(
        [candidate("requested", 50), candidate("fallback", 100)],
        policy=ModelPolicy(fallback_for_explicit=True),
    )
    decision = router.select(
        RoutingRequest(purpose="operator override", requested_model="requested")
    )
    assert decision.selected_model == "requested"
    assert decision.fallback_models == ("fallback",)


def test_provider_ids_and_allowlists_are_canonical():
    assert canonical_model_id("openai:gpt-test") == "gpt-test"
    assert canonical_model_id("claude-test", "anthropic") == "anthropic:claude-test"
    router = ModelRouter(
        [
            candidate("gpt-test", 100),
            candidate("claude-test", 90, provider="anthropic"),
        ]
    )
    decision = router.select(
        RoutingRequest(purpose="provider constrained", allowed_providers=("anthropic",))
    )
    assert decision.selected_model == "anthropic:claude-test"


def test_conflicting_provider_prefix_cannot_bypass_allowlist():
    with pytest.raises(ValueError, match="conflicts"):
        candidate("anthropic:claude-test", 100, provider="openai")
    with pytest.raises(ValueError, match="conflicts"):
        candidate("openai:anthropic:claude-test", 100, provider="openai")
    # Colons that are not registered provider prefixes remain valid OpenAI/custom IDs.
    assert candidate("ft:gpt-test:org", 90, provider="openai").provider == "openai"
    uppercase = candidate("ANTHROPIC:claude-test", 90)
    assert uppercase.model_id == "anthropic:claude-test"
    assert uppercase.provider == "anthropic"
