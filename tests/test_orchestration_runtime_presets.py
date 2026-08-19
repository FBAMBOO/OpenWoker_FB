from __future__ import annotations

import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from coworker.orchestration.api import create_orchestration_router
from coworker.orchestration.errors import ConflictError
from coworker.orchestration.models import EffectSafety, JoinPolicy, NodeKind, TaskStatus
from coworker.orchestration.presets import (
    PRODUCTION_CODEX_LED_MIXED_V1,
    PRODUCTION_CODEX_LED_MIXED_V1_ID,
    RuntimePreset,
)
from coworker.orchestration.profiles import AgentRole
from coworker.orchestration.routing import (
    ModelCandidate,
    ModelRouter,
    NoEligibleModelError,
    RoutingRequest,
)
from coworker.orchestration.service import OrchestrationService
from coworker.orchestration.subscription_runtime import (
    CLAUDE_OPUS_5_HIGH,
    CLAUDE_OPUS_5_MAX,
    CODEX_GPT_5_6_SOL_MAX,
    KIMI_K3_MAX,
)


class _Manager:
    def __init__(self, workspace):
        self.default_workspace = str(workspace)
        self.model = "gpt-5.6-sol"

    def _provider_configured(self, _provider: str) -> bool:
        return True

    def get_settings(self):
        return {
            "models": [self.model],
            "model_labels": {self.model: "Test model"},
            "model_context_windows": {self.model: 400_000},
        }


@pytest.fixture
def service(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    instance = OrchestrationService(
        _Manager(workspace), tmp_path / "data", executor=object()
    )
    try:
        yield instance
    finally:
        instance.store.close()
        instance.catalog.close()


def _request(**overrides):
    return {
        "idempotency_key": "preset-task",
        "objective": "Implement the requested change with independent evidence.",
        "domain": "code",
        "runtime_preset_id": PRODUCTION_CODEX_LED_MIXED_V1_ID,
        "acceptance_criteria": ["The implementation is reviewed and tested"],
        "auto_start": False,
        **overrides,
    }


@pytest.mark.parametrize(
    ("objective", "constraints"),
    [
        ("Inspect the repository and do not modify files.", []),
        ("Inspect the repository safely.", ["不要修改任何文件。"]),
    ],
)
def test_read_only_prompt_requires_explicit_permission(
    service, objective, constraints
):
    with pytest.raises(
        ValueError,
        match="require read-only source access.*set read_only=true",
    ):
        service.create_task(
            _request(
                idempotency_key=f"read-only-conflict-{len(constraints)}",
                objective=objective,
                constraints=constraints,
            )
        )


def test_builtin_codex_led_preset_is_versioned_and_exact():
    preset = PRODUCTION_CODEX_LED_MIXED_V1
    snapshot = preset.to_dict()
    restored = RuntimePreset.from_dict(snapshot)

    assert snapshot["preset_id"] == PRODUCTION_CODEX_LED_MIXED_V1_ID
    assert snapshot["version"] == 1
    assert len(snapshot["content_hash"]) == 64
    assert restored.to_dict() == snapshot
    assert preset.default_for_domains == ("code",)
    assert preset.fallback_mode == "strict"
    assert preset.model_for(AgentRole.ORCHESTRATOR) == CODEX_GPT_5_6_SOL_MAX
    assert preset.model_for(AgentRole.SCORER) == CODEX_GPT_5_6_SOL_MAX
    assert preset.model_for(AgentRole.EXPLORER) == CODEX_GPT_5_6_SOL_MAX
    assert preset.model_for(AgentRole.PLANNER) == CODEX_GPT_5_6_SOL_MAX
    assert preset.model_for(AgentRole.WORKER) == CODEX_GPT_5_6_SOL_MAX
    assert preset.model_for(AgentRole.INTEGRATOR) == CODEX_GPT_5_6_SOL_MAX
    assert preset.model_for(AgentRole.REVIEWER) == CLAUDE_OPUS_5_HIGH
    assert preset.model_for(AgentRole.TESTER) == CLAUDE_OPUS_5_MAX
    assert preset.model_for(AgentRole.EVALUATOR) == CLAUDE_OPUS_5_MAX
    assert KIMI_K3_MAX not in preset.role_models.values()
    assert snapshot["metadata"]["subscription_fallbacks"] == []
    snapshot["metadata"]["subscription_fallbacks"].append(KIMI_K3_MAX)
    assert preset.to_dict()["metadata"]["subscription_fallbacks"] == []


def test_preset_generates_codex_led_dag_and_freezes_snapshot(service):
    detail = service.create_task(_request(profile_id="orchestrator"))
    task = service.store.get_task(detail["id"])
    plan = service._plan_spec(task)
    by_key = {node.key: node for node in plan.nodes}

    assert task.status is TaskStatus.DRAFT
    assert task.policy["profile_id"] == "orchestrator"
    assert detail["runtime_preset_id"] == PRODUCTION_CODEX_LED_MIXED_V1_ID
    assert detail["runtime_preset_version"] == 1
    assert task.policy["require_review"] is True
    assert task.policy["require_tests"] is True
    assert task.policy["runtime_preset_hash"] == task.policy[
        "runtime_preset_snapshot"
    ]["content_hash"]
    assert list(by_key) == [
        "understand",
        "explore",
        "plan",
        "execute",
        "review",
        "test",
        "evaluate",
    ]
    assert {
        key: node.model for key, node in by_key.items()
    } == {
        "understand": CODEX_GPT_5_6_SOL_MAX,
        "explore": CODEX_GPT_5_6_SOL_MAX,
        "plan": CODEX_GPT_5_6_SOL_MAX,
        "execute": CODEX_GPT_5_6_SOL_MAX,
        "review": CLAUDE_OPUS_5_HIGH,
        "test": CLAUDE_OPUS_5_MAX,
        "evaluate": CLAUDE_OPUS_5_MAX,
    }
    assert by_key["understand"].kind is NodeKind.AGENT
    assert by_key["plan"].kind is NodeKind.AGENT
    assert by_key["execute"].kind is NodeKind.EXECUTE
    assert by_key["review"].join_policy is JoinPolicy.ALL
    assert by_key["test"].join_policy is JoinPolicy.ALL
    assert by_key["evaluate"].join_policy is JoinPolicy.ALL
    assert {(edge.from_node, edge.to_node) for edge in plan.edges} == {
        ("understand", "explore"),
        ("explore", "plan"),
        ("plan", "execute"),
        ("execute", "review"),
        ("execute", "test"),
        ("review", "evaluate"),
        ("test", "evaluate"),
    }
    assert by_key["review"].metadata["runtime_preset_binding"]["role"] == (
        "reviewer"
    )
    assert plan.metadata["runtime_preset"]["content_hash"] == task.policy[
        "runtime_preset_hash"
    ]


def test_custom_plan_uses_role_defaults_but_preserves_explicit_node_model(service):
    custom_plan = {
        "nodes": [
            {
                "key": "implement",
                "kind": "execute",
                "agent": "worker",
                "model": CLAUDE_OPUS_5_MAX,
                "effect_safety": "idempotent",
            },
            {"key": "review", "kind": "review", "agent": "reviewer"},
            {"key": "test", "kind": "test", "agent": "tester"},
            {"key": "evaluate", "kind": "evaluate", "agent": "evaluator"},
        ],
        "edges": [
            {"from": "implement", "to": "review"},
            {"from": "implement", "to": "test"},
            {"from": "review", "to": "evaluate"},
            {"from": "test", "to": "evaluate"},
        ],
    }
    detail = service.create_task(
        _request(
            idempotency_key="preset-custom-plan",
            profile_id="orchestrator",
            plan=custom_plan,
        )
    )
    task = service.store.get_task(detail["id"])
    plan = service._plan_spec(task)
    by_key = {node.key: node for node in plan.nodes}

    assert task.policy["profile_id"] == "orchestrator"
    assert by_key["implement"].model == CLAUDE_OPUS_5_MAX
    assert by_key["implement"].metadata["runtime_preset_binding"]["source"] == (
        "explicit_node"
    )
    assert by_key["review"].model == CLAUDE_OPUS_5_HIGH
    assert by_key["review"].metadata["runtime_preset_binding"]["source"] == (
        "preset_role"
    )
    assert by_key["test"].model == CLAUDE_OPUS_5_MAX
    assert by_key["evaluate"].model == CLAUDE_OPUS_5_MAX


def test_writable_non_worker_primary_requires_a_validated_worker_execute_node(
    service,
):
    with pytest.raises(
        ValueError,
        match="profile_id='worker'.*validated execute node.*read_only=true",
    ):
        service.create_task(
            {
                "idempotency_key": "orchestrator-legacy-write",
                "objective": "Modify the repository.",
                "domain": "code",
                "profile_id": "orchestrator",
                "acceptance_criteria": ["The change is complete"],
                "auto_start": False,
            }
        )

    no_worker_plan = {
        "nodes": [
            {
                "key": "understand",
                "kind": "agent",
                "agent": "orchestrator",
                "effect_safety": "read_only",
            },
            {"key": "review", "kind": "review", "agent": "reviewer"},
            {"key": "test", "kind": "test", "agent": "tester"},
            {"key": "evaluate", "kind": "evaluate", "agent": "evaluator"},
        ],
        "edges": [
            {"from": "understand", "to": "review"},
            {"from": "understand", "to": "test"},
            {"from": "review", "to": "evaluate"},
            {"from": "test", "to": "evaluate"},
        ],
    }
    with pytest.raises(ValueError, match="validated execute node with a Worker"):
        service.create_task(
            _request(
                idempotency_key="orchestrator-custom-without-worker",
                profile_id="orchestrator",
                plan=no_worker_plan,
            )
        )

    assert service.list_tasks() == []


def test_read_only_orchestrator_and_preset_are_hard_read_only(service):
    direct = service.create_task(
        {
            "idempotency_key": "orchestrator-read-only-direct",
            "objective": "Inspect the repository without changing it.",
            "domain": "code",
            "profile_id": "orchestrator",
            "read_only": True,
            "network": True,
            "acceptance_criteria": ["The inspection is evidence-backed"],
            "auto_start": False,
        }
    )
    direct_task = service.store.get_task(direct["id"])
    task_permissions = service._task_permissions(direct_task)
    profile_permissions = service._profile_permissions(
        direct_task, service.catalog.resolve_profile("orchestrator")
    )

    assert direct_task.policy["profile_id"] == "orchestrator"
    assert task_permissions.mode == "plan"
    assert task_permissions.commands == frozenset()
    assert task_permissions.external_writes is False
    assert task_permissions.roots and task_permissions.roots[0].writable is False
    assert profile_permissions.mode == "plan"
    assert profile_permissions.commands == frozenset()
    assert profile_permissions.external_writes is False
    assert profile_permissions.roots and profile_permissions.roots[0].writable is False
    assert "write_file" not in profile_permissions.tools
    assert "apply_patch" not in profile_permissions.tools
    assert "run_shell" not in profile_permissions.tools

    preset = service.create_task(
        _request(
            idempotency_key="orchestrator-read-only-preset",
            profile_id="orchestrator",
            read_only=True,
        )
    )
    plan = service._plan_spec(service.store.get_task(preset["id"]))
    by_key = {node.key: node for node in plan.nodes}
    execute = by_key["execute"]

    assert execute.agent == "worker"
    assert execute.kind is NodeKind.EXECUTE
    assert execute.effect_safety is EffectSafety.READ_ONLY
    assert execute.concurrency_key is None
    assert "do not modify files or external systems" in execute.instructions.lower()
    assert "no files were changed" in execute.instructions.lower()
    assert "requested read-only deliverable" in by_key["plan"].instructions.lower()
    assert "final read-only deliverable" in by_key["review"].instructions.lower()
    assert "read-only inspection tools" in by_key["test"].instructions.lower()
    assert "shell commands" in by_key["test"].instructions.lower()

    with pytest.raises(ValueError, match="read_only=true.*external_writes=true"):
        service.create_task(
            _request(
                idempotency_key="contradictory-read-only-policy",
                read_only=True,
                external_writes=True,
            )
        )


def test_task_create_api_exposes_the_conditioned_primary_profile_contract(service):
    app = FastAPI()
    app.include_router(
        create_orchestration_router(SimpleNamespace(orchestration=service))
    )

    with TestClient(app) as client:
        preset = client.post(
            "/v1/orchestration/tasks",
            json=_request(
                idempotency_key="api-orchestrator-preset",
                profile_id="orchestrator",
            ),
        )
        read_only = client.post(
            "/v1/orchestration/tasks",
            json={
                "idempotency_key": "api-orchestrator-read-only",
                "objective": "Inspect only.",
                "domain": "code",
                "profile_id": "orchestrator",
                "read_only": True,
                "acceptance_criteria": ["Inspection complete"],
                "auto_start": False,
            },
        )
        rejected = client.post(
            "/v1/orchestration/tasks",
            json={
                "idempotency_key": "api-orchestrator-unsafe-legacy",
                "objective": "Modify files.",
                "domain": "code",
                "profile_id": "orchestrator",
                "acceptance_criteria": ["Modification complete"],
                "auto_start": False,
            },
        )

    assert preset.status_code == 201
    assert read_only.status_code == 201
    assert rejected.status_code == 422
    assert (
        "validated execute node with a Worker profile"
        in rejected.json()["error"]["message"]
    )
    assert rejected.json()["error"]["code"] == "SEMANTIC_VALIDATION_FAILED"


def test_frozen_runtime_preset_snapshot_rejects_tampering(service):
    detail = service.create_task(
        _request(idempotency_key="preset-snapshot-integrity")
    )
    task = service.store.get_task(detail["id"])
    policy = copy.deepcopy(dict(task.policy))
    policy["runtime_preset_snapshot"]["role_models"]["worker"] = (
        CLAUDE_OPUS_5_MAX
    )
    tampered = replace(task, policy=policy)

    with pytest.raises(ConflictError, match="hash mismatch"):
        service._runtime_preset_for_task(tampered)


def test_legacy_task_omitting_preset_keeps_legacy_plan_generation(service):
    detail = service.create_task(
        {
            "idempotency_key": "legacy-without-preset",
            "objective": "Apply one bounded code change.",
            "domain": "code",
            "acceptance_criteria": ["The bounded change is complete"],
            "auto_start": False,
        }
    )
    task = service.store.get_task(detail["id"])
    plan = service._plan_spec(task)

    assert detail["runtime_preset_id"] is None
    assert "runtime_preset_id" not in task.input
    assert task.policy["runtime_preset_snapshot"] is None
    assert "understand" not in {node.key for node in plan.nodes}
    assert "execute" in {node.key for node in plan.nodes}


def test_preset_rejects_uniform_model_wrong_domain_and_insufficient_budget(service):
    with pytest.raises(ValueError, match="mutually exclusive"):
        service.create_task(
            _request(
                idempotency_key="preset-plus-model",
                requested_model=CODEX_GPT_5_6_SOL_MAX,
            )
        )

    with pytest.raises(ValueError, match="does not support domain knowledge"):
        service.create_task(
            _request(
                idempotency_key="preset-knowledge",
                domain="knowledge",
                workspace=None,
            )
        )

    with pytest.raises(ValueError, match="cannot execute 7 DAG nodes"):
        service.create_task(
            _request(
                idempotency_key="preset-low-budget",
                budget={"model_calls": 6},
            )
        )

    assert service.list_tasks() == []


def test_strict_preset_rejects_explicit_fallback_policy(service):
    cloned = service.catalog.clone_policy("quality-first", "unsafe-fallback")
    draft = cloned["draft"]
    spec = dict(draft["spec"])
    spec["fallback_for_explicit"] = True
    saved = service.catalog.save_policy_draft(
        "unsafe-fallback", spec, expected_etag=draft["etag"]
    )
    service.catalog.publish_policy(
        "unsafe-fallback", expected_etag=saved["draft"]["etag"]
    )

    with pytest.raises(ValueError, match="strict.*fallback_for_explicit=true"):
        service.create_task(
            _request(
                idempotency_key="preset-unsafe-policy",
                model_policy_id="unsafe-fallback",
            )
        )


def test_strict_preset_rechecks_policy_when_plan_is_frozen(service):
    cloned = service.catalog.clone_policy("quality-first", "mutable-policy")
    published = service.catalog.publish_policy(
        "mutable-policy", expected_etag=cloned["draft"]["etag"]
    )
    detail = service.create_task(
        _request(
            idempotency_key="preset-policy-changed-before-freeze",
            model_policy_id="mutable-policy",
        )
    )
    draft = service.catalog.create_policy_draft(
        "mutable-policy", base_version=published["current"]["version"]
    )["draft"]
    spec = dict(draft["spec"])
    spec["fallback_for_explicit"] = True
    saved = service.catalog.save_policy_draft(
        "mutable-policy", spec, expected_etag=draft["etag"]
    )
    service.catalog.publish_policy(
        "mutable-policy", expected_etag=saved["draft"]["etag"]
    )

    with pytest.raises(ConflictError, match="cannot freeze.*fallback_for_explicit"):
        service._plan_spec(service.store.get_task(detail["id"]))


def test_unavailable_explicit_codex_fails_closed_instead_of_selecting_claude(
    service,
):
    policy = service.catalog.resolve_policy("quality-first")
    router = ModelRouter(
        (
            ModelCandidate(
                CODEX_GPT_5_6_SOL_MAX,
                quality=100,
                available=False,
            ),
            ModelCandidate(CLAUDE_OPUS_5_MAX, quality=99, available=True),
        ),
        policy=policy,
    )

    with pytest.raises(NoEligibleModelError):
        router.select(
            RoutingRequest(
                purpose="preset-strict-routing",
                requested_model=CODEX_GPT_5_6_SOL_MAX,
            )
        )

    decision = router.route(
        RoutingRequest(
            purpose="preset-strict-routing",
            requested_model=CODEX_GPT_5_6_SOL_MAX,
        )
    )
    assert decision.selected_model is None
    assert decision.fallback_models == ()


def test_runtime_preset_catalog_and_api_report_role_readiness(
    service, monkeypatch
):
    required = {
        CODEX_GPT_5_6_SOL_MAX,
        CLAUDE_OPUS_5_HIGH,
        CLAUDE_OPUS_5_MAX,
    }
    refreshes = []

    def health_snapshot(*, refresh=False):
        refreshes.append(refresh)
        return [
            {
                "runtime_id": runtime_id,
                "health": {
                    "available": runtime_id != CLAUDE_OPUS_5_MAX,
                    "policy_eligible": True,
                    "reason": (
                        "Claude Max is not authenticated"
                        if runtime_id == CLAUDE_OPUS_5_MAX
                        else ""
                    ),
                },
            }
            for runtime_id in required
        ]

    monkeypatch.setattr(
        service.subscription_runtimes, "health_snapshot", health_snapshot
    )
    app = FastAPI()
    app.include_router(
        create_orchestration_router(SimpleNamespace(orchestration=service))
    )

    with TestClient(app) as client:
        response = client.get(
            "/v1/orchestration/runtime-presets", params={"refresh": "true"}
        )

    assert response.status_code == 200
    [item] = response.json()
    assert item["id"] == PRODUCTION_CODEX_LED_MIXED_V1_ID
    assert item["is_default"] is True
    assert item["available"] is False
    assert item["required_runtime_ids"] == sorted(required)
    assert item["unavailable_runtime_ids"] == [CLAUDE_OPUS_5_MAX]
    assert "not authenticated" in item["availability_reason"]
    assert {role["role"] for role in item["roles"]} == {
        role.value for role in PRODUCTION_CODEX_LED_MIXED_V1.role_models
    }
    assert all(role["fresh_session"] for role in item["roles"])
    assert refreshes == [True]
