# Copyright (c) Microsoft. All rights reserved.

"""Raw core content fidelity through entity admission, history save and cold storage."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from copy import deepcopy
from inspect import Parameter, signature
from typing import Any

import pytest
from agent_framework import Agent, Content, Message
from test_history_identity_acceptance import _core_fields
from test_history_pipeline_revision import ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask._durable_agent_state import DurableAgentStateMessage

_MESSAGE_ID = "opaque-input"
_OPAQUE: dict[str, Any] = {"nested": [None, False, 0, "", [], {}, {"type": "business", "items": [1, 2.5]}]}


def _content(kind: str, **fields: Any) -> dict[str, Any]:
    return {"type": kind, "additional_properties": {}, **deepcopy(fields)}


def _message(contents: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": "chat_message",
        "role": "user",
        "message_id": _MESSAGE_ID,
        "author_name": "caller",
        "contents": deepcopy(contents),
        "additional_properties": {"source": "caller"},
        "future_message": deepcopy(_OPAQUE),
    }


class _SaveTransform(DurableHistoryProvider):
    def __init__(self, transform: Callable[[Message], None]) -> None:
        super().__init__(prune_excluded=False)
        self.transform = transform
        self.transformed: list[Message] = []

    async def save_messages(
        self, session_id: str | None, messages: Sequence[Message], *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> None:
        copied = deepcopy(list(messages))
        for message in copied:
            if message.message_id == _MESSAGE_ID:
                self.transform(message)
                self.transformed.append(deepcopy(message))
        await super().save_messages(session_id, copied, state=state, **kwargs)


async def _append(
    raw: dict[str, Any], history: DurableHistoryProvider | None = None
) -> tuple[JsonStateProvider, ToolChatClient]:
    request = {
        "message": "logging only, not model input",
        "correlationId": "opaque-turn",
        "contextMessages": [raw],
        "contextMessageIds": ["opaque-occurrence"],
    }
    before = deepcopy(request)
    provider = JsonStateProvider()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[history if history is not None else DurableHistoryProvider(prune_excluded=False)],
    )

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert response.text == "answer-1" and response.additional_properties.get("durable_status") != "error"
    assert provider.writes == 1 and request == before
    assert len(client.received_messages) == 1
    # Core keeps approval control input out of model context. Its durable
    # representation must still be checked independently below.
    approval_only = all(content["type"] == "function_approval_request" for content in raw["contents"])
    assert [message.message_id for message in client.received_messages[0]] == ([] if approval_only else [_MESSAGE_ID])
    return provider, client


def _without_storage_identity(row: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(row)
    result.pop("messageId", None)
    result.pop("originalMessageId", None)
    return result


def _assert_no_runtime_metadata(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert not key.startswith("_durable_"), f"operation-local attribute persisted: {key}"
            assert key not in {"__class__", "__module__", "__qualname__", "py/object"}
            _assert_no_runtime_metadata(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_runtime_metadata(item)


def _assert_persisted(provider: JsonStateProvider, expected_raw: dict[str, Any]) -> dict[str, Any]:
    rows = [row for entry in provider.raw["data"]["conversationHistory"] for row in entry["messages"]]
    inputs = [row for row in rows if row.get("originalMessageId", row.get("messageId")) == _MESSAGE_ID]
    assert len(inputs) == 1
    saved = inputs[0]
    expected = DurableAgentStateMessage.from_core_dict(deepcopy(expected_raw)).to_dict()
    # Compare the complete durable projection, not a consumer's filtered Message.to_dict().
    assert _without_storage_identity(saved) == _without_storage_identity(expected)
    _assert_no_runtime_metadata(provider.raw)
    wire = json.loads(json.dumps(provider.raw, allow_nan=False))
    assert DurableAgentState.from_dict(deepcopy(wire)).to_dict() == wire
    cold = JsonStateProvider(wire)
    assert cold.state.to_dict() == wire and cold.writes == 0
    return saved


@pytest.mark.parametrize("kind", ["search_tool_result", "future_kind"])
async def test_unknown_outer_content_keeps_raw_optional_fields_in_actual_entity_append(kind: str) -> None:
    content = _content(
        kind,
        call_id="canonical-call",
        callId={"business_alias": "not-the-call-id"},
        originalcallId={"opaque": [None, False]},
        result={"type": "business", "value": [1, 2]},
        future_content=_OPAQUE,
        future_null=None,
        future_false=False,
        future_zero=0,
        future_empty="",
        future_list=[],
    )
    raw = _message([content])

    provider, client = await _append(raw)

    assert client.received_messages[0][0].contents[0].call_id == "canonical-call"
    saved = _assert_persisted(provider, raw)
    assert saved["contents"][0]["content"] == content


def _future_content_with_optional_fields() -> dict[str, Any]:
    # Derive optional constructor fields rather than maintaining a second Content schema.
    fields = {
        name: deepcopy(parameter.default)
        for name, parameter in signature(Content).parameters.items()
        if parameter.default is not Parameter.empty
        and parameter.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY)
        and name != "raw_representation"
    }
    fields.update(
        text="original",
        call_id="canonical-call",
        arguments=deepcopy(_OPAQUE),
        result=deepcopy(_OPAQUE),
        additional_properties={"source": "content"},
        future_content=deepcopy(_OPAQUE),
    )
    return _content("future_kind", **fields)


@pytest.mark.parametrize("change_text", [False, True], ids=["unaltered", "save-overrides-known-text"])
async def test_future_kind_retains_optional_nulls_without_overwriting_current_known_text(change_text: bool) -> None:
    raw = _message([_future_content_with_optional_fields()])
    expected = deepcopy(raw)
    history = None
    if change_text:
        expected["contents"][0]["text"] = "changed"
        history = _SaveTransform(lambda message: setattr(message.contents[0], "text", "changed"))

    provider, client = await _append(raw, history)

    assert client.received_messages[0][0].contents[0].text == "original"
    if history is not None:
        assert len(history.transformed) == 1 and history.transformed[0].contents[0].text == "changed"
    saved = _assert_persisted(provider, expected)
    assert saved["contents"][0]["content"] == expected["contents"][0]


_TEXT = _content("text", text="nested text", future_content=_OPAQUE)
_CALL = _content(
    "function_call",
    call_id="nested-call",
    name="lookup",
    arguments={"key": "durable"},
    callId={"business_alias": "not-the-call-id"},
    future_content=_OPAQUE,
)
_NESTED_CONTENT_EDGES = [
    pytest.param(_content("function_result", call_id="call", result="text", items=[_TEXT]), ("items", 0), id="items"),
    pytest.param(
        _content("function_approval_request", id="approval", user_input_request=True, function_call=_CALL),
        ("function_call",),
        id="approval-function-call",
    ),
    pytest.param(_content("code_interpreter_tool_call", call_id="call", inputs=[_TEXT]), ("inputs", 0), id="inputs"),
    *[
        pytest.param(_content(kind, call_id="call", outputs=[_TEXT]), ("outputs", 0), id=kind)
        for kind in ("code_interpreter_tool_result", "shell_tool_result")
    ],
    pytest.param(
        _content(
            "function_result",
            call_id="call",
            result="text",
            items=[_content("code_interpreter_tool_result", call_id="nested-call", outputs=[_TEXT])],
        ),
        ("items", 0, "outputs", 0),
        id="recursive-items-outputs",
    ),
]


def _at(value: Any, path: tuple[str | int, ...]) -> Any:
    for part in path:
        value = value[part]
    return value


@pytest.mark.parametrize(("content", "path"), _NESTED_CONTENT_EDGES)
async def test_nested_content_edges_keep_future_fields_in_persisted_core_overlay(
    content: dict[str, Any], path: tuple[str | int, ...]
) -> None:
    raw = _message([content])

    provider, _ = await _append(raw)

    saved = _assert_persisted(provider, raw)
    root = saved["contents"][0]
    payload = root["content"] if root["$type"] == "unknown" else _core_fields(root)
    assert _at(payload, path) == _at(content, path)
    assert _at(payload, path)["future_content"] == _OPAQUE


@pytest.mark.parametrize("kind", ["function_result", "future_kind"])
@pytest.mark.parametrize("with_extra", [False, True], ids=["known-only-control", "future-nested-field"])
async def test_nested_text_changed_by_save_override_wins_over_original_raw_text(kind: str, with_extra: bool) -> None:
    text = _content("text", text="original")
    if with_extra:
        text["future_content"] = deepcopy(_OPAQUE)
    raw = _message([_content(kind, call_id="call", result="business result", items=[text])])
    expected = deepcopy(raw)
    expected["contents"][0]["items"][0]["text"] = "changed"

    def change(message: Message) -> None:
        items = message.contents[0].items
        assert items is not None and isinstance(items[0], Content)
        items[0].text = "changed"

    history = _SaveTransform(change)
    provider, client = await _append(raw, history)

    model_items = client.received_messages[0][0].contents[0].items
    assert model_items is not None and model_items[0].text == "original"
    assert len(history.transformed) == 1
    _assert_persisted(provider, expected)


@pytest.mark.parametrize("nested", [False, True], ids=["message-contents", "function-result-items"])
async def test_reordered_same_type_contents_keep_future_markers_with_their_original_objects(nested: bool) -> None:
    contents = [_content("text", text=label, future_content={"owner": label}) for label in ("first", "second")]
    raw = _message(
        [_content("function_result", call_id="call", result="business", items=contents)] if nested else contents
    )
    expected = deepcopy(raw)
    expected_contents = expected["contents"][0]["items"] if nested else expected["contents"]
    expected_contents.reverse()

    def reorder(message: Message) -> None:
        if nested:
            items = message.contents[0].items
            assert items is not None
            message.contents[0].items = list(reversed(items))
        else:
            message.contents.reverse()

    history = _SaveTransform(reorder)
    provider, _ = await _append(raw, history)

    assert len(history.transformed) == 1
    saved = _assert_persisted(provider, expected)
    stored = _core_fields(saved["contents"][0])["items"] if nested else saved["contents"]
    assert [item["text"] for item in stored] == ["second", "first"]
    markers = [item["future_content"] if nested else _core_fields(item)["future_content"] for item in stored]
    assert markers == [{"owner": "second"}, {"owner": "first"}]


@pytest.mark.parametrize("nested", [False, True], ids=["message-contents", "function-result-items"])
@pytest.mark.parametrize("change_type", [False, True], ids=["new-same-type-object", "type-changed-in-place"])
async def test_replaced_or_type_changed_content_does_not_inherit_positional_raw_extras(
    nested: bool, change_type: bool
) -> None:
    contents = [_content("text", text=label, future_content={"owner": label}) for label in ("first", "second")]
    raw = _message(
        [_content("function_result", call_id="call", result="business", items=contents)] if nested else contents
    )
    replacement = _content("text_reasoning" if change_type else "text", text="first" if change_type else "replacement")
    expected = deepcopy(raw)
    expected_contents = expected["contents"][0]["items"] if nested else expected["contents"]
    expected_contents[0] = replacement

    def replace(message: Message) -> None:
        items = message.contents[0].items if nested else message.contents
        assert items is not None
        updated = list(items)
        if change_type:
            updated[0].type = "text_reasoning"
        else:
            updated[0] = Content.from_text("replacement")
        if nested:
            message.contents[0].items = updated
        else:
            message.contents = updated

    history = _SaveTransform(replace)
    provider, _ = await _append(raw, history)

    assert len(history.transformed) == 1
    _assert_persisted(provider, expected)


@pytest.mark.parametrize(
    ("kind", "field"),
    [
        ("image_generation_tool_result", "outputs"),
        ("function_result", "result"),
        ("function_call", "arguments"),
        ("text", "additional_properties"),
    ],
)
async def test_business_json_is_not_walked_as_content_even_with_type_and_content_edge_keys(
    kind: str, field: str
) -> None:
    business = {
        "type": "function_call",
        "call_id": "business-call",
        "callId": "business-alias",
        "function_call": {"type": "future_business", "future_content": deepcopy(_OPAQUE)},
        "items": [{"type": "text", "text": "business text", "future_content": deepcopy(_OPAQUE)}],
        "inputs": [{"type": "business", "data": [False, None]}],
        "outputs": [{"not_a_content": [0, "", {}]}],
    }
    value: Any = [business, {"not_a_content": None}] if field == "outputs" else business
    fields: dict[str, Any] = {"text": "visible"} if kind == "text" else {"call_id": "canonical-call"}
    if kind == "function_call":
        fields["name"] = "not_invoked"
    fields[field] = value
    raw = _message([_content(kind, **fields)])

    provider, client = await _append(raw)

    observed = getattr(client.received_messages[0][0].contents[0], field)
    assert observed == value
    observed_business = observed[0] if field == "outputs" else observed
    assert type(observed_business) is dict
    assert type(observed_business["function_call"]) is dict
    assert type(observed_business["items"][0]) is dict
    _assert_persisted(provider, raw)


async def test_content_call_id_alias_is_opaque_data_not_an_active_durable_field() -> None:
    raw = _message([deepcopy(_CALL)])

    provider, client = await _append(raw)

    assert client.received_messages[0][0].contents[0].call_id == "nested-call"
    saved = _assert_persisted(provider, raw)
    assert saved["contents"][0]["callId"] == "nested-call"
    assert _core_fields(saved["contents"][0])["callId"] == _CALL["callId"]
