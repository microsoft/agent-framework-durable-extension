# Copyright (c) Microsoft. All rights reserved.

"""Payload detachment and compaction reconstruction regressions."""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from _shared_history_test_support import _bound, _CanonicalStateProvider, _request, _stored
from agent_framework import AgentResponse, Content, Message

from agent_framework_durabletask._history_provider import WORKING_BUFFER_KEY, DurableHistoryProvider
from agent_framework_durabletask._shared_agent_state import DurableAgentStateMessage, DurableAgentStateResponse


def test_error_input_details_are_detached_when_staged() -> None:
    details: Any = {"n": []}
    response = AgentResponse(
        messages=[Message("assistant", [Content.from_error(message="failed", error_details=details)])]
    )
    entry = DurableAgentStateResponse.from_run_response("c", response)
    before = entry.to_dict()
    returned_details: Any = response.messages[0].contents[0].error_details
    returned_details["n"].append("consumer")
    assert entry.to_dict() == before


@pytest.mark.parametrize("kind", ["error", "functionResult"])
@pytest.mark.parametrize("profiled", [False, True])
def test_content_projection_cannot_mutate_stored_arbitrary_payload(kind: str, profiled: bool) -> None:
    content: dict[str, Any] = {"$type": kind}
    if kind == "error":
        content.update(message="failed", details={"n": []})
    else:
        content.update(callId="c", result={"n": []})
    if profiled:
        content["pythonCoreFields"] = {"profile": "agent-framework-python.core-fields", "version": 1, "fields": {}}
    stored = DurableAgentStateMessage.from_dict({"role": "tool", "contents": [content]})
    before = stored.to_dict()
    projected = stored.to_chat_message().contents[0]
    payload = projected.error_details if kind == "error" else projected.result
    # Core currently serializes function-result dictionaries to JSON strings.
    if isinstance(payload, str):
        json.loads(payload)["n"].append("consumer")
    else:
        payload["n"].append("consumer")
    assert stored.to_dict() == before


@pytest.mark.parametrize("count", [1, 20])
async def test_flush_rebuilds_full_identity_index_only_at_boundaries(
    count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _CanonicalStateProvider([_request("seed", _stored("prior", message_id="prior"))])
    history = DurableHistoryProvider()
    count_positions = Mock(wraps=history._positions)
    monkeypatch.setattr(history, "_positions", count_positions)
    state: dict[str, Any] = {}
    with _bound(provider):
        await history.get_messages("session", state=state)
        state[WORKING_BUFFER_KEY].extend(
            Message("assistant", [f"new-{i}"], message_id=f"new-{i}") for i in range(count)
        )
        history.flush(state)
        assert count_positions.call_count == 3
        expected = ["prior", *[f"new-{i}" for i in range(count)]]
        assert [m.text for entry in provider.state.data.conversation_history for m in entry.messages] == expected
        before = provider.state.to_dict()
        history.flush(state)
        assert provider.state.to_dict() == before


async def test_summary_revision_repairs_links_for_sources_removed_from_working_buffer() -> None:
    first = _stored("first", message_id="first")
    second = _stored("second", message_id="second")
    first.extension_data = {"_summary_of_message_ids": [], "_summarized_by_summary_id": "summary"}
    second.extension_data = {"_summarized_by_summary_id": "summary"}
    summary = _stored("old summary", role="assistant", message_id="summary")
    summary.extension_data = {"_summary_of_message_ids": ["first"]}
    provider = _CanonicalStateProvider([_request("seed", first, second, summary)])
    history = DurableHistoryProvider(skip_excluded=False)
    state: dict[str, Any] = {}
    with _bound(provider):
        await history.get_messages("session", state=state)
        state[WORKING_BUFFER_KEY] = [m for m in state[WORKING_BUFFER_KEY] if m.message_id != "second"]
        revised = Message(
            "assistant",
            ["new summary"],
            message_id="summary",
            additional_properties={"_summary_of_message_ids": ["second"]},
        )
        state[WORKING_BUFFER_KEY].append(revised)
        history.flush(state)
        assert revised.message_id != "summary"
        assert first.extension_data["_summarized_by_summary_id"] == "summary"
        assert second.extension_data["_summarized_by_summary_id"] == revised.message_id
        assert second.extension_data["_excluded"] is True
        before = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == before
