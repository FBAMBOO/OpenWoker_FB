from __future__ import annotations

import asyncio

from coworker.engine import DeferredInteraction, TurnEngine
from coworker.events import EventType
from coworker.permissions import PermissionEngine
from coworker.providers import AssistantTurn, ModelCapabilities, ProviderClient, ToolCall
from coworker.tools import ToolRegistry


class ScriptedProvider(ProviderClient):
    def __init__(self, turns):
        self.turns = list(turns)

    def complete(self, *, model, messages, tools=None, **settings):
        return self.turns.pop(0)

    def capabilities(self, model):
        return ModelCapabilities()


def _engine(tmp_path, provider, messages=None, question_asker=None):
    return TurnEngine(
        provider=provider,
        registry=ToolRegistry(),
        permissions=PermissionEngine(workspace_root=tmp_path),
        model="test-model",
        messages=messages,
        question_asker=question_asker,
    )


def test_deferred_interaction_releases_turn_and_resumes_from_messages(tmp_path):
    provider = ScriptedProvider(
        [
            AssistantTurn(
                tool_calls=[
                    ToolCall(
                        id="question-1",
                        name="ask_user",
                        arguments={"question": "Continue?"},
                    )
                ]
            ),
            AssistantTurn(text="continued", finish_reason="stop"),
        ]
    )

    async def defer(_args, _tool_call_id):
        return DeferredInteraction("gate-1", "clarification", {"task_id": "task-1"})

    first = _engine(tmp_path, provider, question_asker=defer)

    async def suspend():
        return [event async for event in first.run("start")]

    events = asyncio.run(suspend())
    assert events[-1].type is EventType.TURN_SUSPENDED
    assert events[-1].data == {
        "interaction_id": "gate-1",
        "kind": "clarification",
        "tool_call_id": "question-1",
        "task_id": "task-1",
    }
    state = first.recovery_state()
    assert state.disposition == "pending_tools"
    assert [call.id for call in state.pending_tool_calls] == ["question-1"]
    assert not any(message.get("role") == "tool" for message in first.messages)

    async def answer(_args, _tool_call_id):
        return {"answer": "yes"}

    rebuilt = _engine(
        tmp_path,
        provider,
        messages=list(first.messages),
        question_asker=answer,
    )

    async def resume():
        return [event async for event in rebuilt.resume()]

    resumed = asyncio.run(resume())
    assert resumed[-1].type is EventType.TURN_END
    assert rebuilt.recovery_state().disposition == "completed"
    assert any(message.get("content") == "continued" for message in rebuilt.messages)

