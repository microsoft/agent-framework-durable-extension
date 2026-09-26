# Copyright (c) Microsoft. All rights reserved.

"""Focused regressions for canonical media replay and response timestamps."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest
from _shared_history_test_support import _bound, _CanonicalStateProvider, _request, _stored
from agent_framework import AgentResponse, Message

from agent_framework_durabletask._history_provider import WORKING_BUFFER_KEY, DurableHistoryProvider
from agent_framework_durabletask._shared_agent_state import DurableAgentState, DurableAgentStateResponse


def _canonical_data_response(*, created_at: str) -> dict[str, Any]:
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": "response",
                    "correlationId": "corr",
                    "createdAt": created_at,
                    "messages": [
                        {
                            "role": "assistant",
                            "messageId": "answer-public",
                            "contents": [
                                {
                                    "$type": "data",
                                    "uri": "data:image/png;base64,AQID",
                                    "mediaType": "image/png",
                                    "extensionData": {"trace": {"preserve": True}},
                                    "future": {"nested": [False, 0, "keep"]},
                                },
                                {
                                    "$type": "uri",
                                    "uri": "https://example.invalid/asset.png",
                                    "mediaType": "image/png",
                                    "extensionData": {"trace": {"kind": "uri"}},
                                },
                            ],
                        }
                    ],
                }
            ],
            "terminalResults": {},
            "completionReceipts": {},
        },
    }


@pytest.mark.parametrize(
    "kind,uri",
    [
        ("data", "data:image/png;base64,AQID"),
        ("uri", "https://example.invalid/asset.png"),
        ("data", "https://example.invalid/asset.png"),
        ("uri", "data:image/png;base64,AQID"),
    ],
)
async def test_cold_history_media_preserves_declared_kind_and_raw_fields(kind: str, uri: str) -> None:
    raw = _canonical_data_response(created_at="2026-09-18T01:02:03.123456789+05:30")
    raw_content = raw["data"]["conversationHistory"][0]["messages"][0]["contents"][0]
    raw_content["$type"] = kind
    raw_content["uri"] = uri
    provider = _CanonicalStateProvider()
    provider.state = DurableAgentState.from_dict(deepcopy(raw))
    history = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(provider):
        messages = await history.get_messages("session", state=working)
        content = messages[0].contents[0]
        assert content.type == kind
        assert content.uri == uri
        assert content.media_type == "image/png"
        assert content.additional_properties == {"trace": {"preserve": True}}
        history.flush(working)
    assert provider.state.to_dict() == raw


def test_canonical_profiled_data_content_cold_roundtrip_preserves_data_uri_and_core_fields() -> None:
    raw = _canonical_data_response(created_at="2026-09-18T01:02:03.123456789+05:30")
    profiled = raw["data"]["conversationHistory"][0]["messages"][0]["contents"][0]
    profiled["pythonCoreFields"] = {
        "profile": "agent-framework-python.core-fields",
        "version": 1,
        "fields": {
            "annotations": [],
            "additional_properties": {"trace": {"preserve": "profile"}},
        },
    }

    state = DurableAgentState.from_dict(deepcopy(raw))
    entry = state.data.conversation_history[0]
    assert isinstance(entry, DurableAgentStateResponse)
    response = DurableAgentStateResponse.to_run_response(entry)
    content = response.messages[0].contents[0]
    assert content.type == "data"
    assert content.uri == "data:image/png;base64,AQID"
    assert content.media_type == "image/png"
    assert content.additional_properties == {"trace": {"preserve": "profile"}}
    assert DurableAgentState.from_json(state.to_json()).to_dict() == raw


@pytest.mark.parametrize("created_at", [datetime(2026, 9, 18, 1, 2, 3), "2026-09-18T01:02:03"])
def test_new_naive_response_timestamp_uses_utc_and_can_be_written(created_at: Any) -> None:
    response = AgentResponse(messages=[Message("assistant", ["answer"])], created_at=created_at)
    replayed = DurableAgentStateResponse.from_run_response("next", response)
    assert replayed.created_at is not None
    assert replayed.created_at.tzinfo == timezone.utc
    assert replayed.to_dict()["createdAt"] == "2026-09-18T01:02:03+00:00"
    state = DurableAgentState()
    state.data.conversation_history.append(replayed)
    assert DurableAgentState.from_json(state.to_json()).data.conversation_history[0].created_at == replayed.created_at
    assert response.created_at == created_at


async def test_summary_follows_unchanged_loaded_occurrence() -> None:
    provider = _CanonicalStateProvider([_request("seed", _stored("first", message_id="first"))])
    history = DurableHistoryProvider(skip_excluded=False)
    state: dict[str, Any] = {}

    with _bound(provider):
        await history.get_messages("session", state=state)
        state[WORKING_BUFFER_KEY].append(
            Message(
                "assistant",
                ["summary"],
                message_id="summary",
                additional_properties={"_summary_of_message_ids": ["first"]},
            )
        )
        history.flush(state)

    persisted = provider.state.data.conversation_history
    assert [entry.json_type for entry in persisted] == ["request", "compaction"]
    assert persisted[0].messages[0].text == "first"
    assert persisted[1].messages[0].text == "summary"
    assert persisted[1].messages[0].message_id != "first"
