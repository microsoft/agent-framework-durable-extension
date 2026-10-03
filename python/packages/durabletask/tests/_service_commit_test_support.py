# Copyright (c) Microsoft. All rights reserved.

"""Shared session probes and state assertions for service commit tests."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from _execution_test_support import JsonStateProvider
from agent_framework import AgentResponse, AgentSession, ContextProvider, Message, SessionContext
from agent_framework._sessions import MessageInjectionMiddleware as CoreMessageInjectionMiddleware
from pydantic import BaseModel

from agent_framework_durabletask import DurableAgentState


class Answer(BaseModel):
    answer: int


class _MissingParent(RuntimeError):
    code = "previous_response_not_found"


class _SessionProbe(ContextProvider):
    def __init__(self) -> None:
        super().__init__("probe")
        self.session: AgentSession | None = None
        self.entries: list[dict[str, Any]] = []

    async def before_run(self, *, session: AgentSession, context: SessionContext, **kwargs: Any) -> None:
        self.session = session
        self.entries.append({
            "session": deepcopy(session.to_dict()),
            "inputs": deepcopy([message.to_dict() for message in context.input_messages]),
        })


def _provider(*, service_id: Any = "S0", injection: CoreMessageInjectionMiddleware | None = None) -> JsonStateProvider:
    session = AgentSession(session_id="@review@thread", service_session_id=deepcopy(service_id))
    session.state = {"untouched": {"marker": [False, 0]}, "probe": {}, "audit": {}, "history": {}}
    if injection is not None:
        injection.enqueue_messages(session, [Message("user", ["queued input"], message_id="Q")])
    state = DurableAgentState()
    state.data.session = session.to_dict()
    return JsonStateProvider(state.to_dict(), session_id="thread", entity_name="review")


def _request(correlation: str, *ids: str) -> dict[str, Any]:
    return {
        "message": "contextMessages are authoritative",
        "correlationId": correlation,
        "enable_tool_calls": False,
        "options": {"store": True},
        "contextMessages": [Message("user", [key], message_id=key).to_dict() for key in ids],
        "contextMessageIds": [f"occ-{key}" for key in ids],
    }


def _committed(provider: JsonStateProvider) -> DurableAgentState:
    return DurableAgentState.from_dict(deepcopy(provider.raw))


def _assert_failed(provider: JsonStateProvider, response: AgentResponse, error_code: str, detail: str) -> None:
    assert response.additional_properties["durable_status"] == "error"
    assert detail in response.text
    codes = [
        content.error_code for message in response.messages for content in message.contents if content.type == "error"
    ]
    assert codes == [error_code]
    assert provider.successful_writes == 1
    data = provider.raw["data"]
    assert data["completionReceipts"]["first"]["outcome"] == "failed"
    assert data["terminalResults"]["first"]["outcome"] == "failed"
