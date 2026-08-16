"""Session/server integration coverage for interactive subscription Agents.

The native CLIs are deliberately replaced at the adapter boundary.  These tests prove
that normal OpenWorker session semantics (model binding, persistence, reload and stop)
survive the complete-Agent runtime without requiring a Codex/Claude/Kimi installation.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient

from coworker.automation import Schedule, ScheduledTask
from coworker.interactive_subscription import InteractiveSubscriptionProvider
from coworker.orchestration.subscription_runtime import CODEX_GPT_5_6_SOL_MAX
from coworker.providers import AssistantTurn, ModelCapabilities, ProviderClient
from coworker.server import SessionManager, create_app
from coworker.sessions import SessionRecord


class _ApiProvider(ProviderClient):
    """Small fallback provider that records every API-backed completion."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def complete(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        tools: Optional[list[dict[str, Any]]] = None,
        **settings: Any,
    ) -> AssistantTurn:
        self.calls.append({"model": model, "messages": messages, "tools": tools})
        return AssistantTurn(text="API answer", finish_reason="stop")

    def capabilities(self, model: str) -> ModelCapabilities:
        return ModelCapabilities()


def _receive_turn(ws: Any) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    while True:
        event = ws.receive_json()
        events.append(event)
        if event["type"] == "turn_done":
            return events


@pytest.mark.parametrize(
    ("extra_payload", "error_fragment"),
    [
        (
            {
                "attachments": [
                    {
                        "kind": "text",
                        "name": "notes.txt",
                        "mime": "text/plain",
                        "text": "attached evidence",
                    }
                ]
            },
            "attachment",
        ),
        ({"skill": "demo-skill"}, "skill"),
    ],
)
def test_ws_subscription_runtime_rejects_host_only_inputs_before_provider_call(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    extra_payload: dict[str, Any],
    error_fragment: str,
) -> None:
    native_calls: list[str] = []

    def fake_codex(
        self: InteractiveSubscriptionProvider,
        spec: Any,
        prompt: str,
        *,
        resumed: bool,
    ) -> tuple[list[str], str, None]:
        native_calls.append(prompt)
        return ["must not be returned"], "thread", None

    monkeypatch.setattr(InteractiveSubscriptionProvider, "_run_codex", fake_codex)
    manager = SessionManager(
        data_dir=tmp_path / "data", workspace=tmp_path, provider=_ApiProvider()
    )
    monkeypatch.setattr(
        manager, "effective_skill_names", lambda *_args, **_kwargs: {"demo-skill"}
    )
    client = TestClient(create_app(manager))

    with client.websocket_connect("/ws/session/subscription-input") as ws:
        assert ws.receive_json()["type"] == "ready"
        ws.send_json(
            {
                "type": "user_message",
                "text": "use this input",
                "model": CODEX_GPT_5_6_SOL_MAX,
                **extra_payload,
            }
        )
        rejected = ws.receive_json()

    assert rejected["type"] == "input_rejected"
    assert error_fragment in rejected["data"]["error"].lower()
    assert native_calls == []
    assert manager.session_store.load("subscription-input") is None


@pytest.mark.parametrize(
    ("extra_payload", "expected_content"),
    [
        (
            {
                "attachments": [
                    {
                        "kind": "text",
                        "name": "notes.txt",
                        "mime": "text/plain",
                        "text": "attached evidence",
                    }
                ]
            },
            "attached evidence",
        ),
        ({"skill": "demo-skill"}, 'load_skill("demo-skill")'),
    ],
)
def test_ws_api_models_keep_supporting_attachments_and_skills(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    extra_payload: dict[str, Any],
    expected_content: str,
) -> None:
    fallback = _ApiProvider()
    manager = SessionManager(
        data_dir=tmp_path / "data",
        workspace=tmp_path,
        model="api:test-model",
        provider=fallback,
    )
    monkeypatch.setattr(
        manager, "effective_skill_names", lambda *_args, **_kwargs: {"demo-skill"}
    )
    client = TestClient(create_app(manager))

    with client.websocket_connect("/ws/session/__api-input") as ws:
        assert ws.receive_json()["type"] == "ready"
        ws.send_json(
            {
                "type": "user_message",
                "text": "use this input",
                "model": "api:test-model",
                **extra_payload,
            }
        )
        events = _receive_turn(ws)

    assert any(event["type"] == "assistant_message" for event in events)
    assert len(fallback.calls) == 1
    assert expected_content in str(fallback.calls[0]["messages"][-1]["content"])


@pytest.mark.asyncio
async def test_manager_wraps_each_session_but_api_models_still_use_fallback(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fallback = _ApiProvider()
    manager = SessionManager(
        data_dir=tmp_path / "data",
        workspace=tmp_path,
        model="api:test-model",
        provider=fallback,
    )

    interrupted: list[InteractiveSubscriptionProvider] = []
    monkeypatch.setattr(
        InteractiveSubscriptionProvider,
        "interrupt",
        lambda self: interrupted.append(self),
    )
    engine = manager.get_engine("api-session", agent="code")

    assert engine is not None
    assert isinstance(engine.provider, InteractiveSubscriptionProvider)
    assert engine.provider.fallback is fallback
    events = [event async for event in engine.run("hello")]
    assert next(e for e in events if e.type.value == "assistant_message").data[
        "text"
    ] == "API answer"
    assert fallback.calls[0]["model"] == "api:test-model"

    # SessionManager registers the native runtime's cancel hook on the TurnEngine.
    engine.request_interrupt()
    assert interrupted == [engine.provider]


def test_ws_codex_subscription_first_turn_persists_and_reload_resumes(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    invocations: list[dict[str, Any]] = []

    def fake_codex(
        self: InteractiveSubscriptionProvider,
        spec: Any,
        prompt: str,
        *,
        resumed: bool,
    ) -> tuple[list[str], str, None]:
        invocations.append({"prompt": prompt, "resumed": resumed})
        ordinal = len(invocations)
        return [f"subscription answer {ordinal}"], "codex-thread-1", None

    monkeypatch.setattr(InteractiveSubscriptionProvider, "_run_codex", fake_codex)
    manager = SessionManager(
        data_dir=tmp_path / "data",
        workspace=tmp_path,
        provider=_ApiProvider(),
    )
    client = TestClient(create_app(manager))

    with client.websocket_connect("/ws/session/subscription-session") as ws:
        assert ws.receive_json()["type"] == "ready"
        ws.send_json(
            {
                "type": "user_message",
                "text": "inspect this repository",
                "model": CODEX_GPT_5_6_SOL_MAX,
            }
        )
        first_events = _receive_turn(ws)

    first_assistant = next(
        event for event in first_events if event["type"] == "assistant_message"
    )
    assert first_assistant["data"]["text"] == "subscription answer 1"
    assert invocations[0]["resumed"] is False

    record = manager.session_store.load("subscription-session")
    assert record is not None
    assert record.model == CODEX_GPT_5_6_SOL_MAX
    assert record.runtime_state == {
        "schema_version": 1,
        "runtime_id": CODEX_GPT_5_6_SOL_MAX,
        "external_session_id": "codex-thread-1",
        "workspace": str(tmp_path.resolve()),
    }

    # Eviction simulates a process-level engine reload: the next socket must rebuild
    # from ConversationStore, carry the opaque native thread id, and send only the new
    # user turn to the resumed Agent session.
    manager._engines.pop("subscription-session")
    with client.websocket_connect("/ws/session/subscription-session") as ws:
        ready = ws.receive_json()
        assert ready["type"] == "ready"
        assert ready["data"]["model"] == CODEX_GPT_5_6_SOL_MAX
        ws.send_json({"type": "user_message", "text": "continue with tests"})
        second_events = _receive_turn(ws)

    second_assistant = next(
        event for event in second_events if event["type"] == "assistant_message"
    )
    assert second_assistant["data"]["text"] == "subscription answer 2"
    assert invocations[1] == {"prompt": "continue with tests", "resumed": True}
    reloaded = manager.session_store.load("subscription-session")
    assert reloaded is not None
    assert reloaded.runtime_state["external_session_id"] == "codex-thread-1"


def test_native_subscription_tool_events_are_audited_and_reloadable(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Native tool activity must survive beyond the live WebSocket.

    The durable transcript contract is a display-only ``role: native_tool`` record,
    deliberately excluded from provider history.  ``event`` retains the live event
    vocabulary and ``tool_call_id`` correlates start/finish across live, transcript and
    audit views.
    """

    def fake_codex(
        self: InteractiveSubscriptionProvider,
        spec: Any,
        prompt: str,
        *,
        resumed: bool,
    ) -> tuple[list[str], str, None]:
        arguments = {"command": "git status --short"}
        self._tool_event(
            "tool_started",
            name="shell",
            arguments=arguments,
            call_id="native-tool-1",
            status="running",
        )
        self._tool_event(
            "tool_finished",
            name="shell",
            arguments=arguments,
            call_id="native-tool-1",
            status="completed",
        )
        return ["repository inspected"], "codex-thread-tools", None

    monkeypatch.setattr(InteractiveSubscriptionProvider, "_run_codex", fake_codex)
    manager = SessionManager(
        data_dir=tmp_path / "data", workspace=tmp_path, provider=_ApiProvider()
    )
    client = TestClient(create_app(manager))
    session_id = "subscription-native-tools"

    with client.websocket_connect(f"/ws/session/{session_id}") as ws:
        assert ws.receive_json()["type"] == "ready"
        ws.send_json(
            {
                "type": "user_message",
                "text": "inspect repository state",
                "model": CODEX_GPT_5_6_SOL_MAX,
            }
        )
        live_events = _receive_turn(ws)

    live_tools = [
        event
        for event in live_events
        if event["type"] in {"tool_started", "tool_finished"}
    ]
    assert [event["type"] for event in live_tools] == [
        "tool_started",
        "tool_finished",
    ]
    assert all(event["data"]["tool_call_id"] == "native-tool-1" for event in live_tools)

    audit = manager.audit_store.list(session_id=session_id)
    by_stage = {event["stage"]: event for event in audit}
    assert set(by_stage) == {"started", "finished"}
    assert by_stage["started"]["tool"] == "shell"
    assert by_stage["started"]["status"] == "running"
    assert by_stage["started"]["args"] == {"command": "git status --short"}
    assert by_stage["finished"]["status"] == "completed"

    # Drop the live engine so this assertion reads the append-only transcript from disk.
    manager._engines.pop(session_id)
    reloaded = manager.session_messages(session_id)
    native_tools = [message for message in reloaded if message.get("role") == "native_tool"]
    assert [
        {
            "event": message["event"],
            "name": message["name"],
            "tool_call_id": message["tool_call_id"],
            "arguments": message["arguments"],
            "status": message["status"],
        }
        for message in native_tools
    ] == [
        {
            "event": "tool_started",
            "name": "shell",
            "tool_call_id": "native-tool-1",
            "arguments": {"command": "git status --short"},
            "status": "running",
        },
        {
            "event": "tool_finished",
            "name": "shell",
            "tool_call_id": "native-tool-1",
            "arguments": {"command": "git status --short"},
            "status": "completed",
        },
    ]


@pytest.mark.asyncio
async def test_background_delivery_rejects_persisted_subscription_session(
    tmp_path: Any,
) -> None:
    manager = SessionManager(
        data_dir=tmp_path / "data", workspace=tmp_path, provider=_ApiProvider()
    )
    manager.session_store.save(
        SessionRecord(
            session_id="subscription-session",
            workspace=str(tmp_path),
            model=CODEX_GPT_5_6_SOL_MAX,
            mode="interactive",
            agent="code",
            runtime_state={
                "runtime_id": CODEX_GPT_5_6_SOL_MAX,
                "external_session_id": "codex-thread-1",
            },
        )
    )

    with pytest.raises(RuntimeError, match="foreground-only"):
        await manager.deliver_to_session("subscription-session", "background event")


def test_legacy_scheduled_automation_rejects_subscription_model(tmp_path: Any) -> None:
    manager = SessionManager(
        data_dir=tmp_path / "data", workspace=tmp_path, provider=_ApiProvider()
    )
    task = ScheduledTask(
        title="Daily subscription run",
        instructions="inspect the repository",
        schedule=Schedule(kind="cron", cron="0 9 * * *"),
        workspace=str(tmp_path),
        model=CODEX_GPT_5_6_SOL_MAX,
    )

    with pytest.raises(RuntimeError, match="legacy scheduled automations"):
        manager._build_task_engine(task, session_id="__run__subscription")
