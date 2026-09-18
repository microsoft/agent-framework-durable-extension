# Copyright (c) Microsoft. All rights reserved.

"""Review tests for durable history metadata projection and provider ordering."""

from __future__ import annotations

from collections.abc import Awaitable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    BaseChatClient,
    ChatResponse,
    ChatResponseUpdate,
    CompactionProvider,
    Content,
    InMemoryHistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
)
from test_shared_history_provider import _bound, _CanonicalStateProvider, _request, _stored

from agent_framework_durabletask._history_provider import DurableHistoryProvider, ensure_durable_history
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateResponse,
    DurableAgentStateTextContent,
    DurableAgentStateTextReasoningContent,
)


class _CaptureChatClient(BaseChatClient):
    STORES_BY_DEFAULT = False

    def __init__(self) -> None:
        super().__init__()
        self.received_messages: list[list[Message]] = []

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.received_messages.append(deepcopy(list(messages)))
        response = ChatResponse(messages=[Message("assistant", ["answer"])], finish_reason="stop")
        if stream:

            async def updates() -> Any:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=response.messages[0].contents,
                    finish_reason=response.finish_reason,
                )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class _TextReasoningAlias(DurableAgentStateTextReasoningContent):
    def to_ai_content(self) -> Content:
        return Content(type="text_reasoning", text=self.text)


def _assistant_message(*contents: Any, message_id: str = "assistant-id") -> DurableAgentStateMessage:
    return DurableAgentStateMessage("assistant", list(contents), message_id=message_id)


def _response_entry_dict(
    created_at: str,
    *,
    extension_data: dict[str, Any] | None = None,
    contents: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "$type": "response",
        "correlationId": "corr",
        "createdAt": created_at,
        "messages": [{"role": "assistant", "contents": contents or [{"$type": "text", "text": "visible"}]}],
    }
    if extension_data is not None:
        entry["extensionData"] = deepcopy(extension_data)
    return entry


@pytest.mark.parametrize(
    ("stored_message", "expected_texts"),
    [
        pytest.param(
            _assistant_message(DurableAgentStateTextReasoningContent("private"), message_id="reasoning-only"),
            [],
            id="reasoning-only",
        ),
        pytest.param(
            _assistant_message(_TextReasoningAlias("private"), message_id="text-reasoning-only"),
            [],
            id="text-reasoning-only",
        ),
        pytest.param(
            _assistant_message(
                DurableAgentStateTextReasoningContent("private"),
                DurableAgentStateTextContent("visible"),
                message_id="mixed",
            ),
            ["visible"],
            id="mixed",
        ),
        pytest.param(
            DurableAgentStateMessage.from_dict({
                "role": "assistant",
                "messageId": "wire-mixed",
                "contents": [
                    {
                        "$type": "reasoning",
                        "text": "private",
                        "pythonCoreFields": {
                            "profile": "agent-framework-python.core-fields",
                            "version": 1,
                            "fields": {"annotations": []},
                        },
                    },
                    {"$type": "text", "text": "visible"},
                ],
            }),
            ["visible"],
            id="wire-profiled-mixed",
        ),
    ],
)
async def test_get_messages_filters_reasoning_spellings_without_mutating_storage(
    stored_message: DurableAgentStateMessage, expected_texts: list[str]
) -> None:
    provider = _CanonicalStateProvider([_request("seed", stored_message)])
    history = DurableHistoryProvider(skip_excluded=False)
    state: dict[str, Any] = {}
    before = deepcopy(provider.state.to_dict())

    with _bound(provider):
        loaded = await history.get_messages("session", state=state)
        assert [message.text for message in loaded] == expected_texts
        assert all(
            content.type not in {"reasoning", "text_reasoning"} for message in loaded for content in message.contents
        )
        history.flush(state)

    assert provider.state.to_dict() == before


def test_from_run_response_preserves_string_timestamp_with_nanoseconds_and_offset() -> None:
    created_at = "2026-09-18T01:02:03.123456789+05:30"
    response = AgentResponse(messages=[Message("assistant", ["done"])], created_at=created_at)

    entry = DurableAgentStateResponse.from_run_response("corr", response)
    restored = DurableAgentStateResponse.to_run_response(entry)

    assert restored.created_at == created_at


def test_unrelated_response_metadata_edits_do_not_drop_raw_timestamp_precision_on_reload() -> None:
    created_at = "2026-09-18T01:02:03.123456789-04:30"
    entry = DurableAgentStateResponse.from_dict(
        _response_entry_dict(created_at, extension_data={"trace": {"tags": ["stored"]}})
    )

    assert entry.to_dict()["createdAt"] == created_at
    entry.extension_data = {"trace": {"tags": ["stored", "edited"]}}
    restored = DurableAgentStateResponse.to_run_response(entry)
    cold = DurableAgentStateResponse.from_dict(entry.to_dict())

    assert restored.created_at == created_at
    assert cold.to_dict()["createdAt"] == created_at


def test_explicit_typed_created_at_mutation_overrides_the_original_raw_timestamp() -> None:
    entry = DurableAgentStateResponse.from_dict(
        _response_entry_dict("2026-09-18T01:02:03.123456789+00:00", extension_data={"trace": {"tags": ["stored"]}})
    )
    entry.created_at = datetime(2026, 9, 18, 7, 8, 9, 987654, tzinfo=timezone(timedelta(hours=-7)))

    restored = DurableAgentStateResponse.to_run_response(entry)

    assert restored.created_at == entry.created_at.isoformat()


@pytest.mark.parametrize("created_at", [None, "not-a-timestamp"], ids=["missing", "invalid"])
def test_from_run_response_keeps_the_existing_default_now_policy_for_missing_or_invalid_timestamps(
    created_at: str | None,
) -> None:
    response = AgentResponse(messages=[Message("assistant", ["done"])], created_at=created_at)

    entry = DurableAgentStateResponse.from_run_response("corr", response)
    restored = DurableAgentStateResponse.to_run_response(entry)

    assert entry.created_at is not None
    assert entry.created_at.tzinfo is not None
    assert isinstance(restored.created_at, str) and restored.created_at


@pytest.mark.parametrize(
    ("profile", "extension_data", "expected_additional_properties"),
    [
        pytest.param(None, {"trace": {"text": "extension"}}, {"trace": {"text": "extension"}}, id="fallback"),
        pytest.param(
            {
                "profile": "agent-framework-python.core-fields",
                "version": 1,
                "fields": {"additional_properties": {"trace": {"text": "profile"}}},
            },
            {"trace": {"text": "extension"}},
            {"trace": {"text": "profile"}},
            id="recognized-profile-wins-whole-field",
        ),
        pytest.param(
            {"profile": "foreign", "version": 1, "fields": {"additional_properties": {"ignored": True}}},
            {"trace": {"text": "extension"}},
            {"trace": {"text": "extension"}},
            id="foreign-profile-is-inert",
        ),
    ],
)
def test_known_content_extension_data_projects_to_core_additional_properties(
    profile: dict[str, Any] | None,
    extension_data: dict[str, Any],
    expected_additional_properties: dict[str, Any],
) -> None:
    raw_content: dict[str, Any] = {"$type": "text", "text": "visible", "extensionData": extension_data}
    if profile is not None:
        raw_content["pythonCoreFields"] = profile

    stored = DurableAgentStateMessage.from_dict({"role": "assistant", "contents": [raw_content]})
    chat_message = stored.to_chat_message()
    content = chat_message.contents[0]

    assert content.text == "visible"
    assert content.additional_properties == expected_additional_properties
    content.additional_properties["trace"]["text"] = "caller"
    assert stored.to_dict()["contents"][0]["extensionData"] == extension_data


def test_content_extension_data_remains_a_dictionary_even_when_business_keys_match_known_fields() -> None:
    metadata = {"text": "metadata", "callId": "metadata", "type": "metadata"}
    stored = DurableAgentStateMessage.from_dict({
        "role": "assistant",
        "contents": [{"$type": "text", "text": "visible", "extensionData": metadata}],
    })

    content = stored.to_chat_message().contents[0]

    assert content.text == "visible"
    assert content.additional_properties == metadata


def test_unknown_content_wrappers_stay_opaque_controls() -> None:
    stored = DurableAgentStateMessage.from_dict({
        "role": "assistant",
        "contents": [
            {
                "$type": "unknown",
                "content": {"$type": "text", "text": "not replayable", "type": "reasoning"},
                "extensionData": {"trace": {"safe": True}},
            }
        ],
    })

    content = stored.to_chat_message().contents[0]

    assert content.type == "unknown"
    assert content.additional_properties["content"] == {"$type": "text", "text": "not replayable", "type": "reasoning"}


def test_from_run_response_captures_detached_response_additional_properties() -> None:
    response = AgentResponse(
        messages=[Message("assistant", ["done"])],
        additional_properties={"trace": {"tags": ["stored"]}},
    )

    entry = DurableAgentStateResponse.from_run_response("corr", response)
    response.additional_properties["trace"]["tags"].append("caller")

    assert entry.extension_data == {"trace": {"tags": ["stored"]}}


def test_to_run_response_returns_detached_extension_data_for_normal_entries() -> None:
    entry = DurableAgentStateResponse(
        "corr",
        None,
        [DurableAgentStateMessage.from_chat_message(Message("assistant", ["done"]))],
        extension_data={"trace": {"tags": ["stored"]}},
    )

    response = DurableAgentStateResponse.to_run_response(entry)
    assert response.additional_properties == {"trace": {"tags": ["stored"]}}

    response.additional_properties["trace"]["tags"].append("caller")
    assert entry.extension_data == {"trace": {"tags": ["stored"]}}


def test_error_responses_keep_other_metadata_but_force_durable_status_error() -> None:
    entry = DurableAgentStateErrorResponse(
        "corr",
        None,
        [DurableAgentStateMessage.from_chat_message(Message("assistant", ["done"]))],
        extension_data={"trace": {"tags": ["stored"]}, "durable_status": "accepted"},
    )

    response = DurableAgentStateResponse.to_run_response(entry)

    assert response.additional_properties == {
        "trace": {"tags": ["stored"]},
        "durable_status": "error",
    }


async def test_history_after_run_stores_response_entry_metadata() -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider(store_inputs=False, skip_excluded=False)
    response = AgentResponse(
        messages=[Message("assistant", ["done"])],
        additional_properties={"trace": {"tags": ["stored"]}},
    )
    context = SessionContext(input_messages=[Message("user", ["input"])])
    context._response = response

    with _bound(provider, "corr"):
        await history.after_run(agent=None, session=None, context=context, state={})

    assert len(provider.state.data.conversation_history) == 1
    stored = provider.state.data.conversation_history[0]
    assert isinstance(stored, DurableAgentStateResponse)
    assert stored.extension_data == {"trace": {"tags": ["stored"]}}


async def test_ensure_durable_history_loads_history_before_compaction_before_strategy_when_no_primary() -> None:
    seen_by_strategy: list[list[str]] = []
    client = _CaptureChatClient()

    async def before_strategy(messages: list[Message]) -> bool:
        seen_by_strategy.append([message.text for message in messages])
        for message in messages:
            if message.text == "stored context":
                message.additional_properties["_excluded"] = True
        return True

    agent = Agent(
        client=client,
        context_providers=[
            CompactionProvider(
                before_strategy=before_strategy,
                history_source_id=InMemoryHistoryProvider.DEFAULT_SOURCE_ID,
            )
        ],
    )
    prepared = ensure_durable_history(agent)
    assert isinstance(prepared, Agent)
    provider = _CanonicalStateProvider([_request("seed", _stored("stored context", message_id="stored-id"))])
    session = prepared.create_session(session_id="ordering-session")

    with _bound(provider):
        await prepared.run("live input", session=session)

    # Core's before-compaction hook sees prior provider context, not current inputs.
    assert seen_by_strategy == [["stored context"]]
    assert [[message.text for message in batch] for batch in client.received_messages] == [["live input"]]


@pytest.mark.parametrize("extension", [None, False, 0, "opaque", [], {"trace": [False, 0]}])
@pytest.mark.parametrize("fields", [None, {}, {"additional_properties": {}}, {"additional_properties": {"other": [1]}}])
def test_content_metadata_presence_and_profile_precedence(extension: Any, fields: dict[str, Any] | None) -> None:
    raw: dict[str, Any] = {"$type": "text", "text": "visible", "extensionData": extension}
    if fields is not None:
        raw["pythonCoreFields"] = {"profile": "agent-framework-python.core-fields", "version": 1, "fields": fields}
    message = DurableAgentStateMessage.from_dict({"role": "user", "contents": [raw]})
    before = deepcopy(message.to_dict())
    projected = message.to_chat_message().contents[0]
    expected = (
        fields["additional_properties"]
        if fields is not None and "additional_properties" in fields
        else extension
        if isinstance(extension, dict)
        else {}
    )
    assert projected.additional_properties == expected
    projected.additional_properties["consumer-only"] = True
    assert message.to_dict() == before


@pytest.mark.parametrize("before_enabled", [False, True])
async def test_injected_compaction_order_has_explicit_forward_and_reverse_cadence(before_enabled: bool) -> None:
    observed: list[tuple[str, list[str]]] = []

    async def before(messages: list[Message]) -> bool:
        observed.append(("before", [m.text for m in messages]))
        return False

    async def after(messages: list[Message]) -> bool:
        observed.append(("after", [m.text for m in messages]))
        return False

    compaction = CompactionProvider(before_strategy=before if before_enabled else None, after_strategy=after)
    agent = Agent(client=_CaptureChatClient(), context_providers=[compaction])
    original = tuple(agent.context_providers)
    prepared = ensure_durable_history(agent)
    assert isinstance(prepared, Agent)
    provider = _CanonicalStateProvider([_request("seed", _stored("prior", message_id="prior"))])
    session = prepared.create_session()
    with _bound(provider):
        await prepared.run("input", session=session)
        history = next(p for p in prepared.context_providers if isinstance(p, DurableHistoryProvider))
        history.flush(session.state[history.source_id])
    assert observed == (
        [("before", ["prior"]), ("after", ["prior"])] if before_enabled else [("after", ["prior", "input", "answer"])]
    )
    assert [m.text for e in provider.state.data.conversation_history for m in e.messages] == [
        "prior",
        "input",
        "answer",
    ]
    assert tuple(agent.context_providers) == original
