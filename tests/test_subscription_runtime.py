from __future__ import annotations

import asyncio
import io
import json
import queue
import subprocess
import threading
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

import coworker.orchestration.subscription_runtime as runtime_module
from coworker.orchestration.blobs import ContentAddressedBlobStore
from coworker.orchestration.errors import LeaseConflict
from coworker.orchestration.executor import ExecutionOutcome, RunExecutionContext
from coworker.orchestration.models import NodeSpec, PlanSpec, TaskSpec, TaskStatus
from coworker.orchestration.profiles import builtin_profile
from coworker.orchestration.routing import (
    ModelCandidate,
    ModelRouter,
    RoutingRequest,
)
from coworker.orchestration.store import OrchestrationStore
from coworker.orchestration.subscription_runtime import (
    CLAUDE_OPUS_5_HIGH,
    CLAUDE_OPUS_5_MAX,
    CODEX_GPT_5_6_SOL_MAX,
    KIMI_K3_MAX,
    ClaudeCodeSubscriptionRuntime,
    CodexSubscriptionRuntime,
    KimiCodeSubscriptionRuntime,
    SubscriptionDispatchExecutor,
    SubscriptionRuntimeHealth,
    SubscriptionRuntimeRegistry,
    default_subscription_runtime_specs,
)


_STRUCTURED_RESULT = {
    "summary": "The isolated runtime completed the node.",
    "status": "pass",
    "criteria": {"The node is complete.": "pass"},
    "files_touched": [],
    "checks": ["offline protocol test"],
    "remaining_risks": [],
}

_WIRE_STRUCTURED_RESULT = {
    **_STRUCTURED_RESULT,
    "criteria": [
        {"criterion": "The node is complete.", "status": "pass"},
    ],
}


class _MemorySessionStore:
    def __init__(self) -> None:
        self.records: dict[str, Any] = {}

    def load(self, session_id: str) -> Any:
        return self.records.get(session_id)

    def save(self, record: Any) -> None:
        self.records[record.session_id] = record


@dataclass
class _Harness:
    store: OrchestrationStore
    blob_store: ContentAddressedBlobStore
    manager: Any
    state_dir: Path
    context: RunExecutionContext


@pytest.fixture
def harness(tmp_path: Path) -> Any:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = OrchestrationStore(tmp_path / "orchestration.db")
    task = store.create_task(
        TaskSpec(
            idempotency_key="subscription-runtime-test",
            objective="Exercise an isolated subscription Agent runtime",
            workspace=str(workspace),
            acceptance_criteria=("The node is complete.",),
        )
    )
    task = store.transition_task_status(
        task.id, TaskStatus.QUEUED, expected_version=task.version
    )
    task = store.transition_task_status(
        task.id, TaskStatus.RUNNING, expected_version=task.version
    )
    graph = store.create_plan_revision(
        task.id,
        PlanSpec(
            nodes=(
                NodeSpec(
                    key="execute",
                    title="Execute the node",
                    instructions="Complete the node and report evidence.",
                    agent="worker",
                ),
            )
        ),
        expected_task_version=task.version,
        created_by="test",
    )
    run = store.enqueue_run(task.id, "execute", session_id="__orch__subscription")
    claim = store.claim_next_run("subscription-test-worker")
    assert claim is not None and claim.run.id == run.id
    store.start_run(run.id, claim.lease.token, claim.lease.fencing_token)
    route = ModelRouter(
        (
            ModelCandidate(
                CLAUDE_OPUS_5_HIGH,
                provider="claude-code-subscription",
                quality=98,
                context_window=200_000,
            ),
        )
    ).select(
        RoutingRequest(
            purpose="subscription-runtime-test",
            requested_model=CLAUDE_OPUS_5_HIGH,
        )
    )
    value = _Harness(
        store=store,
        blob_store=ContentAddressedBlobStore(tmp_path / "blobs"),
        manager=SimpleNamespace(session_store=_MemorySessionStore()),
        state_dir=tmp_path / "runtime-state",
        context=RunExecutionContext(
            task=store.get_task(task.id),
            graph=graph,
            node=graph.nodes[0],
            claim=claim,
            profile=builtin_profile("worker"),
            routing=route,
            workspace=workspace,
        ),
    )
    try:
        yield value
    finally:
        store.close()


def _spec(runtime_id: str) -> Any:
    return {
        item.runtime_id: item for item in default_subscription_runtime_specs()
    }[runtime_id]


def _runtime(
    cls: type[Any], runtime_id: str, harness: _Harness
) -> Any:
    return cls(
        _spec(runtime_id),
        harness.manager,
        harness.store,
        harness.blob_store,
        harness.state_dir,
    )


def _healthy(runtime_id: str, provider: str, version: str) -> SubscriptionRuntimeHealth:
    return SubscriptionRuntimeHealth(
        runtime_id=runtime_id,
        provider=provider,
        installed=True,
        authenticated=True,
        available=True,
        policy_eligible=True,
        version=version,
        auth_kind="test_subscription",
        executable=f"/fake/{provider}",
    )


def _context_with_checkpoint(
    harness: _Harness,
    checkpoint: Mapping[str, Any],
) -> RunExecutionContext:
    claim = harness.context.claim
    run = harness.store.checkpoint_active_run(
        claim.run.id,
        claim.lease.token,
        claim.lease.fencing_token,
        checkpoint=checkpoint,
    )
    return replace(harness.context, claim=replace(claim, run=run))


class _FakeTree:
    def __init__(self, proc: Any) -> None:
        self.proc = proc
        self.terminate_calls = 0

    def terminate(self) -> bool:
        self.terminate_calls += 1
        self.proc.return_code = 0
        return True


class _FakeActive:
    def __init__(self, proc: Any) -> None:
        self.run_id = ""
        self.proc = proc
        self.tree = _FakeTree(proc)
        self.write_lock = threading.Lock()
        self.thread_id: str | None = None
        self.turn_id: str | None = None
        self.cleanup_ok: bool | None = None
        self.finished = threading.Event()
        self.interrupt_timer: threading.Timer | None = None

    def send(self, value: Mapping[str, Any]) -> None:
        payload = json.dumps(value, separators=(",", ":")) + "\n"
        with self.write_lock:
            if self.proc.stdin is None or self.proc.poll() is not None:
                raise BrokenPipeError("fake runtime stdin is closed")
            self.proc.stdin.write(payload)
            self.proc.stdin.flush()


class _QueueStdout:
    def __init__(self) -> None:
        self.lines: queue.Queue[str] = queue.Queue()

    def push(self, value: Mapping[str, Any]) -> None:
        self.lines.put(json.dumps(value, separators=(",", ":")) + "\n")

    def readline(self) -> str:
        return self.lines.get(timeout=2)


class _CodexStdin:
    def __init__(self, stdout: _QueueStdout) -> None:
        self.stdout = stdout
        self.messages: list[dict[str, Any]] = []
        self.closed = False

    def write(self, payload: str) -> int:
        for line in payload.splitlines():
            message = json.loads(line)
            self.messages.append(message)
            self._respond(message)
        return len(payload)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    def _respond(self, message: Mapping[str, Any]) -> None:
        method = message.get("method")
        request_id = message.get("id")
        if method == "initialize":
            self.stdout.push(
                {
                    "id": request_id,
                    "result": {"userAgent": "codex-cli/0.146.0"},
                }
            )
        elif method == "thread/start":
            self.stdout.push(
                {
                    "id": request_id,
                    "result": {
                        "thread": {"id": "codex-thread-1", "turns": []},
                        "model": "gpt-5.6-sol",
                        "modelProvider": "openai",
                        "instructionSources": [],
                    },
                }
            )
        elif method == "turn/start":
            self.stdout.push(
                {
                    "id": request_id,
                    "result": {
                        "turn": {"id": "codex-turn-1", "status": "inProgress"}
                    },
                }
            )
            self.stdout.push(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": "codex-thread-1",
                        "turnId": "codex-turn-1",
                        "item": {
                            "id": "message-1",
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": json.dumps(_WIRE_STRUCTURED_RESULT),
                        },
                    },
                }
            )
            self.stdout.push(
                {
                    "method": "thread/tokenUsage/updated",
                    "params": {
                        "threadId": "codex-thread-1",
                        "turnId": "codex-turn-1",
                        "tokenUsage": {
                            "total": {"inputTokens": 23, "outputTokens": 7}
                        },
                    },
                }
            )
            self.stdout.push(
                {
                    "method": "turn/completed",
                    "params": {
                        "threadId": "codex-thread-1",
                        "turn": {"id": "codex-turn-1", "status": "completed"},
                    },
                }
            )


class _FakeCodexProcess:
    def __init__(self) -> None:
        self.stdout = _QueueStdout()
        self.stdin = _CodexStdin(self.stdout)
        self.stderr = io.StringIO("")
        self.return_code: int | None = None

    def poll(self) -> int | None:
        return self.return_code


class _PromptStdin(io.StringIO):
    def close(self) -> None:
        # Keep the captured prompt readable by the assertion after execution.
        self.flush()


class _FakeClaudeProcess:
    def __init__(self, events: list[Mapping[str, Any]]) -> None:
        self.stdin = _PromptStdin()
        self.stdout = io.StringIO(
            "".join(json.dumps(event) + "\n" for event in events)
        )
        self.stderr = io.StringIO("")
        self.return_code: int | None = None

    def poll(self) -> int | None:
        return self.return_code

    def wait(self, timeout: float | None = None) -> int:
        self.return_code = 0
        return 0


def test_default_specs_pin_exact_vendor_model_and_reasoning_effort() -> None:
    specs = {item.runtime_id: item for item in default_subscription_runtime_specs()}

    assert set(specs) == {
        CODEX_GPT_5_6_SOL_MAX,
        CLAUDE_OPUS_5_HIGH,
        CLAUDE_OPUS_5_MAX,
        KIMI_K3_MAX,
    }
    assert (
        specs[CODEX_GPT_5_6_SOL_MAX].cli_model,
        specs[CODEX_GPT_5_6_SOL_MAX].reasoning_effort,
    ) == (
        "gpt-5.6-sol",
        "max",
    )
    assert (
        specs[CLAUDE_OPUS_5_HIGH].cli_model,
        specs[CLAUDE_OPUS_5_HIGH].reasoning_effort,
    ) == (
        "claude-opus-5",
        "high",
    )
    assert (
        specs[CLAUDE_OPUS_5_MAX].cli_model,
        specs[CLAUDE_OPUS_5_MAX].reasoning_effort,
    ) == (
        "claude-opus-5",
        "max",
    )
    assert (specs[KIMI_K3_MAX].cli_model, specs[KIMI_K3_MAX].reasoning_effort) == (
        "kimi-code/k3",
        "max",
    )
    assert specs[KIMI_K3_MAX].interactive_only is True


def test_interactive_catalog_separates_kimi_interactive_and_background_policy(
    harness: _Harness, monkeypatch
) -> None:
    runtimes = [
        SimpleNamespace(spec=_spec(CODEX_GPT_5_6_SOL_MAX)),
        SimpleNamespace(spec=_spec(KIMI_K3_MAX)),
    ]
    registry = SubscriptionRuntimeRegistry(
        harness.manager,
        harness.store,
        harness.blob_store,
        harness.state_dir,
        runtimes=runtimes,
    )
    health_by_id = {
        CODEX_GPT_5_6_SOL_MAX: _healthy(
            CODEX_GPT_5_6_SOL_MAX, "codex-subscription", "0.146.0"
        ),
        KIMI_K3_MAX: SubscriptionRuntimeHealth(
            runtime_id=KIMI_K3_MAX,
            provider="kimi-code-subscription",
            installed=True,
            authenticated=True,
            available=False,
            policy_eligible=False,
            version="0.29.2",
            auth_kind="kimi_managed_oauth",
            executable="/fake/kimi",
            reason=(
                "Kimi Code managed OAuth is interactive-only and blocked for "
                "background DAG execution"
            ),
        ),
    }
    monkeypatch.setattr(
        registry,
        "health",
        lambda runtime_id, *, refresh=False: health_by_id[runtime_id],
    )

    catalog = {item["runtime_id"]: item for item in registry.interactive_catalog()}

    codex = catalog[CODEX_GPT_5_6_SOL_MAX]
    assert codex["interactive_eligible"] is True
    assert codex["background_eligible"] is True

    kimi = catalog[KIMI_K3_MAX]
    assert kimi["label"] == _spec(KIMI_K3_MAX).display_name
    assert kimi["interactive_eligible"] is True
    assert "interactive personal sessions" in kimi["interactive_reason"]
    assert kimi["background_eligible"] is False
    assert kimi["health"]["available"] is False
    assert kimi["health"]["policy_eligible"] is False
    assert "background DAG" in kimi["background_reason"]


def test_result_schema_is_recursively_strict_and_uses_fixed_criteria_items() -> None:
    schema = runtime_module._result_schema()
    criteria = schema["properties"]["criteria"]

    assert criteria["type"] == "array"
    assert criteria["items"]["properties"] == {
        "criterion": {"type": "string"},
        "status": {
            "type": "string",
            "enum": ["pass", "fail", "unknown"],
        },
    }

    def assert_strict_objects(value: Any) -> None:
        if isinstance(value, Mapping):
            if value.get("type") == "object":
                assert value.get("additionalProperties") is False
                assert set(value.get("required", ())) == set(
                    value.get("properties", {})
                )
            for child in value.values():
                assert_strict_objects(child)
        elif isinstance(value, list):
            for child in value:
                assert_strict_objects(child)

    assert_strict_objects(schema)


@pytest.mark.parametrize(
    "payload",
    (
        _WIRE_STRUCTURED_RESULT,
        json.dumps(_WIRE_STRUCTURED_RESULT),
        f"```json\n{json.dumps(_WIRE_STRUCTURED_RESULT)}\n```",
    ),
)
def test_structured_output_normalizes_wire_criteria_to_stable_mapping(
    payload: Any,
) -> None:
    structured = runtime_module._structured(payload)

    assert structured == _STRUCTURED_RESULT
    assert runtime_module._validate_structured(structured) is None


def test_structured_output_preserves_legacy_criteria_mapping() -> None:
    structured = runtime_module._structured(_STRUCTURED_RESULT)

    assert structured == _STRUCTURED_RESULT
    assert runtime_module._validate_structured(structured) is None


def test_structured_output_rejects_duplicate_wire_criteria() -> None:
    duplicate = {
        **_WIRE_STRUCTURED_RESULT,
        "criteria": [
            {"criterion": "The node is complete.", "status": "pass"},
            {"criterion": "The node is complete.", "status": "fail"},
        ],
    }

    structured = runtime_module._structured(duplicate)

    assert structured is not None
    assert runtime_module._validate_structured(structured) == (
        "structured output field 'criteria' is missing or invalid"
    )


@pytest.mark.parametrize("legacy_contract", (None, "dynamic_map", "strict_array"))
@pytest.mark.asyncio
async def test_legacy_v1_sealed_result_recovers_without_model_call(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
    legacy_contract: str | None,
) -> None:
    claude = _runtime(ClaudeCodeSubscriptionRuntime, CLAUDE_OPUS_5_HIGH, harness)
    monkeypatch.setattr(
        claude,
        "probe",
        lambda: _healthy(
            CLAUDE_OPUS_5_HIGH,
            "claude-code-subscription",
            "2.1.220",
        ),
    )
    claim = harness.context.claim
    external_session_id = "legacy-claude-session"
    sealed = harness.blob_store.put_json(
        {
            "schema_version": 1,
            "runtime_id": CLAUDE_OPUS_5_HIGH,
            "run_id": claim.run.id,
            "attempt": claim.run.attempt,
            "terminal_status": "completed",
            "final_text": json.dumps(_STRUCTURED_RESULT),
            "structured": _STRUCTURED_RESULT,
            "external_session_id": external_session_id,
            "external_turn_id": "",
            "events": [],
            "stderr": "",
            "usage": {
                "model_calls": 1,
                "tool_calls": 0,
                "tokens": 17,
                "wall_seconds": 1,
            },
            "runtime_version": "2.1.219",
            "resolved_model": "claude-opus-5",
            "error_kind": None,
            "error_message": None,
            "cleanup_ok": True,
        }
    )
    legacy_checkpoint = {
        "schema_version": 1,
        "runtime_id": CLAUDE_OPUS_5_HIGH,
        "provider": "claude-code-subscription",
        "model": "claude-opus-5",
        "reasoning_effort": "high",
        "protocol": "claude-code-stream-json-v1",
        "run_id": claim.run.id,
        "attempt": claim.run.attempt,
        "external_session_id": external_session_id,
        "external_turn_id": "",
        "runtime_version": "2.1.219",
        "state": "result_sealed",
        "workspace_sha256": runtime_module.hashlib.sha256(
            str(harness.context.workspace.resolve()).encode("utf-8")
        ).hexdigest(),
        "recovery_blob_sha256": sealed.sha256,
        "recovery_blob_size": sealed.size,
        "recovery_blob_mime_type": sealed.mime_type,
    }
    if legacy_contract == "dynamic_map":
        legacy_checkpoint.update(
            {
                "prompt_sha256": runtime_module.hashlib.sha256(
                    runtime_module._v1_dynamic_map_prompt(harness.context).encode(
                        "utf-8"
                    )
                ).hexdigest(),
                "output_schema_sha256": (
                    runtime_module._V1_DYNAMIC_MAP_OUTPUT_SCHEMA_SHA256
                ),
            }
        )
    elif legacy_contract == "strict_array":
        legacy_checkpoint.update(
            {
                "prompt_sha256": runtime_module.hashlib.sha256(
                    runtime_module._v1_strict_array_prompt(harness.context).encode(
                        "utf-8"
                    )
                ).hexdigest(),
                "output_schema_sha256": (
                    runtime_module._V1_STRICT_ARRAY_OUTPUT_SCHEMA_SHA256
                ),
            }
        )
    recovered_context = _context_with_checkpoint(harness, legacy_checkpoint)
    monkeypatch.setattr(
        claude,
        "_spawn",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("legacy sealed recovery must not launch Claude Code")
        ),
    )

    recovered = await claude.execute(recovered_context)

    assert recovered.status == "succeeded", (
        recovered.error_kind,
        recovered.error_message,
    )
    assert recovered.output["structured_result"] == _STRUCTURED_RESULT
    assert recovered.output["subscription_runtime_checkpoint"]["schema_version"] == 2
    assert len(
        recovered.output["subscription_runtime_checkpoint"]["prompt_sha256"]
    ) == 64
    assert len(
        recovered.output["subscription_runtime_checkpoint"][
            "output_schema_sha256"
        ]
    ) == 64


def test_legacy_v1_inflight_checkpoint_is_not_resumed(
    harness: _Harness,
) -> None:
    claude = _runtime(ClaudeCodeSubscriptionRuntime, CLAUDE_OPUS_5_HIGH, harness)
    current = claude._checkpoint(
        harness.context,
        external_session_id="legacy-inflight-session",
        state="session_started",
    )
    legacy = {**current, "schema_version": 1}
    legacy.pop("prompt_sha256")
    legacy.pop("output_schema_sha256")
    legacy_context = _context_with_checkpoint(harness, legacy)

    with pytest.raises(RuntimeError, match="not safely recoverable"):
        claude._load_checkpoint(legacy_context)


@pytest.mark.parametrize("legacy_contract", ("dynamic_map", "strict_array"))
def test_hash_bound_legacy_v1_checkpoint_uses_frozen_contract(
    harness: _Harness,
    legacy_contract: str,
) -> None:
    claude = _runtime(ClaudeCodeSubscriptionRuntime, CLAUDE_OPUS_5_HIGH, harness)
    current = claude._checkpoint(
        harness.context,
        external_session_id="legacy-bound-session",
        state="result_sealed",
    )
    legacy = {**current, "schema_version": 1}
    if legacy_contract == "dynamic_map":
        legacy["prompt_sha256"] = runtime_module.hashlib.sha256(
            runtime_module._v1_dynamic_map_prompt(harness.context).encode("utf-8")
        ).hexdigest()
        legacy["output_schema_sha256"] = (
            runtime_module._V1_DYNAMIC_MAP_OUTPUT_SCHEMA_SHA256
        )
    else:
        legacy["prompt_sha256"] = runtime_module.hashlib.sha256(
            runtime_module._v1_strict_array_prompt(harness.context).encode("utf-8")
        ).hexdigest()
        legacy["output_schema_sha256"] = (
            runtime_module._V1_STRICT_ARRAY_OUTPUT_SCHEMA_SHA256
        )
    legacy_context = _context_with_checkpoint(harness, legacy)

    loaded = claude._load_checkpoint(legacy_context)

    assert loaded == legacy
    assert (
        legacy["prompt_sha256"],
        legacy["output_schema_sha256"],
    ) in runtime_module._v1_known_contract_bindings(harness.context)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("missing_prompt", "binding is incomplete"),
        ("tampered_prompt", "binding mismatch"),
        ("tampered_schema", "binding mismatch"),
        ("cross_dynamic_prompt", "binding mismatch"),
        ("cross_dynamic_schema", "binding mismatch"),
    ),
)
def test_hash_bound_legacy_v1_checkpoint_rejects_partial_or_tampered_binding(
    harness: _Harness,
    mutation: str,
    message: str,
) -> None:
    claude = _runtime(ClaudeCodeSubscriptionRuntime, CLAUDE_OPUS_5_HIGH, harness)
    current = claude._checkpoint(
        harness.context,
        external_session_id="legacy-bound-session",
        state="result_sealed",
    )
    legacy = {**current, "schema_version": 1}
    legacy["prompt_sha256"] = runtime_module.hashlib.sha256(
        runtime_module._v1_strict_array_prompt(harness.context).encode("utf-8")
    ).hexdigest()
    legacy["output_schema_sha256"] = (
        runtime_module._V1_STRICT_ARRAY_OUTPUT_SCHEMA_SHA256
    )
    if mutation == "missing_prompt":
        legacy.pop("prompt_sha256")
    elif mutation == "tampered_prompt":
        legacy["prompt_sha256"] = "0" * 64
    elif mutation == "tampered_schema":
        legacy["output_schema_sha256"] = "0" * 64
    elif mutation == "cross_dynamic_prompt":
        legacy["prompt_sha256"] = runtime_module.hashlib.sha256(
            runtime_module._v1_dynamic_map_prompt(harness.context).encode("utf-8")
        ).hexdigest()
    else:
        legacy["output_schema_sha256"] = (
            runtime_module._V1_DYNAMIC_MAP_OUTPUT_SCHEMA_SHA256
        )
    legacy_context = _context_with_checkpoint(harness, legacy)

    with pytest.raises(RuntimeError, match=message):
        claude._load_checkpoint(legacy_context)


@pytest.mark.parametrize(
    ("field", "mutation", "message"),
    (
        ("prompt_sha256", "missing", "prompt mismatch"),
        ("output_schema_sha256", "missing", "output schema mismatch"),
        ("prompt_sha256", "tampered", "prompt mismatch"),
        ("output_schema_sha256", "tampered", "output schema mismatch"),
    ),
)
def test_v2_checkpoint_requires_exact_prompt_and_schema_bindings(
    harness: _Harness,
    field: str,
    mutation: str,
    message: str,
) -> None:
    claude = _runtime(ClaudeCodeSubscriptionRuntime, CLAUDE_OPUS_5_HIGH, harness)
    checkpoint = claude._checkpoint(
        harness.context,
        external_session_id="bound-session",
        state="session_reserved",
    )
    assert checkpoint["schema_version"] == 2
    assert len(checkpoint["prompt_sha256"]) == 64
    assert len(checkpoint["output_schema_sha256"]) == 64
    mutated = dict(checkpoint)
    if mutation == "missing":
        mutated.pop(field)
    else:
        mutated[field] = "0" * 64
    mutated_context = _context_with_checkpoint(harness, mutated)

    with pytest.raises(RuntimeError, match=message):
        claude._load_checkpoint(mutated_context)


def test_subscription_environment_scrubs_api_and_model_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "CODEX_API_KEY",
        "CODEX_MODEL",
        "OPENAI_API_KEY",
        "OPENAI_MODEL",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_MODEL",
        "MOONSHOT_API_KEY",
        "KIMI_API_KEY",
        "COWORKER_API_TOKEN",
        "GITHUB_TOKEN",
        "AWS_ACCESS_KEY_ID",
        "SSH_AUTH_SOCK",
    ):
        monkeypatch.setenv(name, "must-not-reach-subscription-cli")

    codex_env = runtime_module._safe_environment("codex-subscription")
    claude_env = runtime_module._safe_environment("claude-code-subscription")
    kimi_env = runtime_module._safe_environment("kimi-code-subscription")

    assert "CODEX_API_KEY" not in codex_env
    assert "OPENAI_MODEL" not in codex_env
    assert "ANTHROPIC_API_KEY" not in claude_env
    assert "ANTHROPIC_MODEL" not in claude_env
    assert "MOONSHOT_API_KEY" not in kimi_env
    assert "KIMI_API_KEY" not in kimi_env
    assert "COWORKER_API_TOKEN" not in codex_env
    assert "GITHUB_TOKEN" not in claude_env
    assert "AWS_ACCESS_KEY_ID" not in codex_env
    assert "SSH_AUTH_SOCK" not in claude_env
    assert kimi_env["KIMI_MODEL_THINKING_EFFORT"] == "max"


def test_audit_sanitizer_redacts_credentials_and_hidden_reasoning() -> None:
    sanitized = runtime_module._sanitize_event(
        {
            "type": "assistant",
            "authorization": "Bearer should-not-survive",
            "content": [
                {
                    "type": "thinking",
                    "thinking": "private chain of thought",
                    "signature": "private-signature",
                    "summary": "Public reasoning summary",
                }
            ],
        }
    )

    assert sanitized["authorization"] == "[REDACTED]"
    thinking = sanitized["content"][0]
    assert thinking["thinking"] == "[REDACTED_REASONING]"
    assert thinking["signature"] == "[REDACTED_REASONING]"
    assert thinking["summary"] == "Public reasoning summary"


def test_codex_health_accepts_chatgpt_status_on_stderr_without_model_call(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    codex = _runtime(CodexSubscriptionRuntime, CODEX_GPT_5_6_SOL_MAX, harness)
    monkeypatch.setattr(runtime_module.shutil, "which", lambda _command: "/fake/codex")

    def probe_result(
        _executable: str, args: Any, _provider: str
    ) -> subprocess.CompletedProcess[str]:
        key = tuple(args)
        if key == ("--version",):
            return subprocess.CompletedProcess(args, 0, "codex-cli 0.146.0\n", "")
        if key == ("app-server", "--help"):
            return subprocess.CompletedProcess(args, 0, "--stdio\n", "")
        if key == ("login", "status"):
            return subprocess.CompletedProcess(args, 0, "", "Logged in using ChatGPT\n")
        if key == ("debug", "models"):
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps(
                    {
                        "models": [
                            {
                                "id": "gpt-5.6-sol",
                                "supportedReasoningEfforts": ["high", "max"],
                            }
                        ]
                    }
                ),
                "",
            )
        raise AssertionError(f"unexpected probe: {key}")

    monkeypatch.setattr(runtime_module, "_run_probe", probe_result)

    health = codex.probe()

    assert health.available is True
    assert health.auth_kind == "chatgpt_subscription"


def test_registry_rejects_duplicate_ids_and_blocks_non_loopback_owner(
    harness: _Harness,
) -> None:
    class HealthyRuntime:
        def __init__(self) -> None:
            self.spec = _spec(CLAUDE_OPUS_5_HIGH)

        def probe(self) -> SubscriptionRuntimeHealth:
            return _healthy(
                CLAUDE_OPUS_5_HIGH,
                "claude-code-subscription",
                "2.1.220",
            )

        async def execute(self, context: RunExecutionContext) -> ExecutionOutcome:
            return ExecutionOutcome(status="succeeded", session_id=context.claim.run.id)

        def interrupt(self, _run_id: str) -> None:
            return None

    runtime = HealthyRuntime()
    with pytest.raises(ValueError, match="duplicate subscription runtime id"):
        SubscriptionRuntimeRegistry(
            harness.manager,
            harness.store,
            harness.blob_store,
            harness.state_dir,
            runtimes=(runtime, runtime),
        )
    registry = SubscriptionRuntimeRegistry(
        harness.manager,
        harness.store,
        harness.blob_store,
        harness.state_dir,
        runtimes=(runtime,),
        local_owner_eligible=False,
    )

    health = registry.health(CLAUDE_OPUS_5_HIGH, refresh=True)

    assert health.authenticated is True
    assert health.available is False
    assert health.policy_eligible is False
    assert "loopback-only" in health.reason


def test_active_runtime_checkpoint_rejects_credentials_and_stale_fence(
    harness: _Harness,
) -> None:
    claim = harness.context.claim
    with pytest.raises(ValueError, match="credential-like fields"):
        harness.store.checkpoint_active_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token,
            checkpoint={"runtime_id": CLAUDE_OPUS_5_HIGH, "api_key": "forbidden"},
        )
    with pytest.raises(LeaseConflict):
        harness.store.checkpoint_active_run(
            claim.run.id,
            claim.lease.token,
            claim.lease.fencing_token + 1,
            checkpoint={"runtime_id": CLAUDE_OPUS_5_HIGH, "state": "reserved"},
        )


def test_commands_pin_cli_protocol_model_effort_and_role_ceiling(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        runtime_module.shutil,
        "which",
        lambda command: f"/fake/bin/{command}",
    )
    codex = _runtime(CodexSubscriptionRuntime, CODEX_GPT_5_6_SOL_MAX, harness)
    claude = _runtime(ClaudeCodeSubscriptionRuntime, CLAUDE_OPUS_5_HIGH, harness)
    kimi = _runtime(KimiCodeSubscriptionRuntime, KIMI_K3_MAX, harness)

    assert codex.build_command(harness.context) == [
        "/fake/bin/codex",
        "app-server",
        "--stdio",
        "--strict-config",
    ]

    claude_argv = claude.build_command(harness.context, schema=_STRUCTURED_RESULT)
    assert claude_argv[0] == "/fake/bin/claude"
    assert claude_argv[claude_argv.index("--model") + 1] == "claude-opus-5"
    assert claude_argv[claude_argv.index("--effort") + 1] == "high"
    assert claude_argv[claude_argv.index("--output-format") + 1] == "stream-json"
    assert claude_argv[claude_argv.index("--permission-mode") + 1] == "dontAsk"
    allowed = claude_argv[claude_argv.index("--tools") + 1].split(",")
    denied = claude_argv[claude_argv.index("--disallowedTools") + 1].split(",")
    assert {"Bash", "Edit", "Write"}.issubset(allowed)
    assert {"Agent", "Task"}.issubset(denied)

    kimi_argv = kimi.build_command(harness.context)
    assert kimi_argv[0] == "/fake/bin/kimi"
    assert kimi_argv[kimi_argv.index("--model") + 1] == "kimi-code/k3"
    assert kimi_argv[kimi_argv.index("--output-format") + 1] == "stream-json"
    assert kimi_argv[kimi_argv.index("--session") + 1].startswith("session_")
    assert kimi_argv[kimi_argv.index("--prompt") + 1]
    assert "--effort" not in kimi_argv
    assert runtime_module._safe_environment("kimi-code-subscription")[
        "KIMI_MODEL_THINKING_EFFORT"
    ] == "max"

    read_only_context = replace(
        harness.context,
        task=replace(
            harness.context.task,
            policy={**dict(harness.context.task.policy), "read_only": True},
        ),
    )
    thread_mode, sandbox = codex._sandbox(read_only_context)
    assert thread_mode == "read-only"
    assert sandbox == {"type": "readOnly", "networkAccess": False}
    allowed, denied = claude._tools(read_only_context)
    assert set(allowed.split(",")) == {"Read", "Glob", "Grep"}
    assert {"Bash", "Edit", "Write"}.issubset(set(denied.split(",")))


@pytest.mark.asyncio
async def test_kimi_oauth_background_execution_fails_closed_before_spawn(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    kimi = _runtime(KimiCodeSubscriptionRuntime, KIMI_K3_MAX, harness)
    calls = {"command": 0, "spawn": 0}

    def unexpected_command(*_args: Any, **_kwargs: Any) -> list[str]:
        calls["command"] += 1
        raise AssertionError("Kimi command construction must not run")

    def unexpected_spawn(*_args: Any, **_kwargs: Any) -> Any:
        calls["spawn"] += 1
        raise AssertionError("Kimi process must not start")

    monkeypatch.setattr(kimi, "build_command", unexpected_command)
    monkeypatch.setattr(kimi, "_spawn", unexpected_spawn)

    outcome = await kimi.execute(harness.context)

    assert outcome.status == "failed"
    assert outcome.error_kind == "subscription_noninteractive_automation_forbidden"
    assert calls == {"command": 0, "spawn": 0}


@pytest.mark.asyncio
async def test_codex_jsonl_protocol_persists_thread_and_turn_checkpoint(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    codex = _runtime(CodexSubscriptionRuntime, CODEX_GPT_5_6_SOL_MAX, harness)
    monkeypatch.setattr(
        codex,
        "probe",
        lambda: _healthy(
            CODEX_GPT_5_6_SOL_MAX, "codex-subscription", "codex-cli 0.146.0"
        ),
    )
    process = _FakeCodexProcess()
    active = _FakeActive(process)
    launches: list[tuple[list[str], Path, Mapping[str, str]]] = []

    def fake_spawn(
        argv: Any, *, cwd: Path, env: Mapping[str, str]
    ) -> _FakeActive:
        launches.append((list(argv), cwd, env))
        return active

    monkeypatch.setattr(codex, "_spawn", fake_spawn)

    outcome = await codex.execute(harness.context)

    assert outcome.status == "succeeded"
    assert outcome.summary == _STRUCTURED_RESULT["summary"]
    assert outcome.usage == {
        "model_calls": 1,
        "tool_calls": 0,
        "tokens": 30,
        "wall_seconds": 1,
    }
    assert launches[0][0][1:] == ["app-server", "--stdio", "--strict-config"]
    sent = process.stdin.messages
    assert sent[0]["method"] == "initialize"
    assert sent[1] == {"method": "initialized"}
    thread_start = next(item for item in sent if item.get("method") == "thread/start")
    assert thread_start["params"]["model"] == "gpt-5.6-sol"
    assert thread_start["params"]["config"]["features"]["multi_agent"] is False
    assert {
        "runtimeWorkspaceRoots",
        "selectedCapabilityRoots",
        "allowProviderModelFallback",
        "environments",
    }.isdisjoint(thread_start["params"])
    turn_start = next(item for item in sent if item.get("method") == "turn/start")
    assert turn_start["params"]["model"] == "gpt-5.6-sol"
    assert turn_start["params"]["effort"] == "max"
    assert turn_start["params"]["outputSchema"] == runtime_module._result_schema()
    assert {
        "runtimeWorkspaceRoots",
        "responsesapiClientMetadata",
        "environments",
    }.isdisjoint(turn_start["params"])

    persisted = harness.store.get_run(harness.context.claim.run.id)
    checkpoint = persisted.output["subscription_runtime_checkpoint"]
    assert checkpoint["runtime_id"] == CODEX_GPT_5_6_SOL_MAX
    assert checkpoint["external_session_id"] == "codex-thread-1"
    assert checkpoint["external_turn_id"] == "codex-turn-1"
    assert checkpoint["state"] == "result_sealed"
    assert checkpoint["attempt"] == harness.context.claim.run.attempt
    assert checkpoint["reasoning_effort"] == "max"
    assert len(checkpoint["recovery_blob_sha256"]) == 64
    assert outcome.output["subscription_runtime_checkpoint"][
        "external_session_id"
    ] == "codex-thread-1"
    assert active.finished.is_set()
    # The protocol path performs a bounded reap before publishing the outcome and
    # the finally block defensively repeats it. Both calls are idempotent.
    assert active.tree.terminate_calls >= 1


@pytest.mark.asyncio
async def test_claude_stream_jsonl_is_parsed_without_invoking_a_real_cli(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    claude = _runtime(ClaudeCodeSubscriptionRuntime, CLAUDE_OPUS_5_HIGH, harness)
    monkeypatch.setattr(
        claude,
        "probe",
        lambda: _healthy(
            CLAUDE_OPUS_5_HIGH,
            "claude-code-subscription",
            "2.1.220",
        ),
    )
    expected_session = str(
        runtime_module.uuid.uuid5(
            runtime_module.uuid.NAMESPACE_URL,
            f"openworker:{harness.context.claim.run.id}:{CLAUDE_OPUS_5_HIGH}",
        )
    )
    events = [
        {
            "type": "system",
            "subtype": "init",
            "session_id": expected_session,
            "model": "claude-opus-5",
            "capabilities": {"agents": False},
        },
        {
            "type": "assistant",
            "session_id": expected_session,
            "message": {
                "model": "claude-opus-5",
                "content": [
                    {"type": "tool_use", "id": "tool-1", "name": "Read"},
                    {"type": "text", "text": json.dumps(_WIRE_STRUCTURED_RESULT)},
                ],
            },
        },
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "session_id": expected_session,
            "structured_output": _WIRE_STRUCTURED_RESULT,
            "modelUsage": {
                "claude-opus-5": {"inputTokens": 41, "outputTokens": 9}
            },
        },
    ]
    process = _FakeClaudeProcess(events)
    active = _FakeActive(process)
    launches: list[list[str]] = []

    def fake_spawn(
        argv: Any, *, cwd: Path, env: Mapping[str, str]
    ) -> _FakeActive:
        launches.append(list(argv))
        return active

    monkeypatch.setattr(claude, "_spawn", fake_spawn)

    outcome = await claude.execute(harness.context)

    assert outcome.status == "succeeded"
    assert outcome.output["structured_result"] == _STRUCTURED_RESULT
    assert outcome.usage["tokens"] == 50
    assert outcome.usage["tool_calls"] == 1
    assert process.stdin.getvalue().startswith(
        harness.context.profile.instructions
    )
    assert launches[0][launches[0].index("--model") + 1] == "claude-opus-5"
    assert launches[0][launches[0].index("--effort") + 1] == "high"
    assert json.loads(
        launches[0][launches[0].index("--json-schema") + 1]
    ) == runtime_module._result_schema()
    assert launches[0][launches[0].index("--session-id") + 1] == expected_session
    assert "--resume" not in launches[0]
    checkpoint = harness.store.get_run(harness.context.claim.run.id).output[
        "subscription_runtime_checkpoint"
    ]
    assert checkpoint["external_session_id"] == expected_session
    assert checkpoint["state"] == "result_sealed"
    assert len(checkpoint["recovery_blob_sha256"]) == 64
    assert harness.manager.session_store.records[outcome.session_id].model == (
        CLAUDE_OPUS_5_HIGH
    )

    recovered_run = harness.store.get_run(harness.context.claim.run.id)
    recovered_context = replace(
        harness.context,
        claim=replace(harness.context.claim, run=recovered_run),
    )
    monkeypatch.setattr(
        claude,
        "_spawn",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("a sealed result must not launch Claude Code again")
        ),
    )

    recovered = await claude.execute(recovered_context)

    assert recovered.status == "succeeded"
    assert recovered.output["structured_result"] == _STRUCTURED_RESULT


@pytest.mark.asyncio
async def test_dispatcher_interrupt_targets_the_inflight_subscription_runtime(
    harness: _Harness,
) -> None:
    class BlockingRuntime:
        def __init__(self) -> None:
            self.spec = _spec(CLAUDE_OPUS_5_HIGH)
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.interrupts: list[str] = []

        def probe(self) -> SubscriptionRuntimeHealth:
            return _healthy(
                CLAUDE_OPUS_5_HIGH,
                "claude-code-subscription",
                "2.1.220",
            )

        async def execute(self, context: RunExecutionContext) -> ExecutionOutcome:
            self.started.set()
            await self.release.wait()
            return ExecutionOutcome(
                status="failed",
                session_id=context.claim.run.session_id or "",
                error_kind="interrupted_for_test",
            )

        def interrupt(self, run_id: str) -> None:
            self.interrupts.append(run_id)
            self.release.set()

    class NativeRuntime:
        def __init__(self) -> None:
            self.interrupts: list[str] = []

        async def execute(self, context: RunExecutionContext) -> ExecutionOutcome:
            return ExecutionOutcome(status="succeeded", session_id="native")

        def interrupt(self, run_id: str) -> None:
            self.interrupts.append(run_id)

    subscription = BlockingRuntime()
    native = NativeRuntime()
    registry = SubscriptionRuntimeRegistry(
        harness.manager,
        harness.store,
        harness.blob_store,
        harness.state_dir,
        runtimes=(subscription,),
    )
    dispatcher = SubscriptionDispatchExecutor(native, registry)
    operation = asyncio.create_task(dispatcher.execute(harness.context))
    await asyncio.wait_for(subscription.started.wait(), timeout=1)

    dispatcher.interrupt(harness.context.claim.run.id)
    outcome = await asyncio.wait_for(operation, timeout=1)

    assert outcome.error_kind == "interrupted_for_test"
    assert subscription.interrupts == [harness.context.claim.run.id]
    assert native.interrupts == []
