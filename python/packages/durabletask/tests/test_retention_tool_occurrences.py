# Copyright (c) Microsoft. All rights reserved.

"""Occurrence retention through canonical JSON, real Core grouping and provider replay."""

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from _retention_test_support import JsonStateProvider
from agent_framework import (
    CharacterEstimatorTokenizer,
    Content,
    Message,
    annotate_message_groups,
    included_token_count,
)

from agent_framework_durabletask import _retention as retention
from agent_framework_durabletask._history_provider import (
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    unbind_durable_history,
)
from agent_framework_durabletask._retention import StateCapacityError, enforce_budget
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)

OLD = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _call(kind: str, message_id: str, *, chars: int = 8_000, call_id: str = "shared") -> Message:
    arguments = json.dumps({"query": "x" * chars})
    content = (
        Content.from_function_call(call_id, "lookup", arguments=arguments)
        if kind == "function"
        else Content.from_mcp_server_tool_call(call_id, "lookup", server_name="catalog", arguments=arguments)
    )
    return Message("assistant", [content], message_id=message_id)


def _result(kind: str, message_id: str, *, call_id: str = "shared") -> Message:
    content = (
        Content.from_function_result(call_id, result="done")
        if kind == "function"
        else Content.from_mcp_server_tool_result(call_id, output="done")
    )
    return Message("tool" if kind == "function" else "assistant", [content], message_id=message_id)


def _response(correlation: str, *messages: Message) -> DurableAgentStateResponse:
    return DurableAgentStateResponse(
        correlation, OLD, [DurableAgentStateMessage.from_chat_message(message) for message in messages]
    )


def _request(message_id: str) -> DurableAgentStateRequest:
    return DurableAgentStateRequest(
        message_id,
        OLD,
        [DurableAgentStateMessage.from_chat_message(Message("user", [message_id], message_id=message_id))],
    )


def _state(history: list[DurableAgentStateEntry]) -> DurableAgentState:
    state = DurableAgentState()
    state.data.conversation_history = history
    # In particular, hosted MCP travels through the supported Python content
    # profile. Do not inject raw content that the durable reader treats as opaque.
    return DurableAgentState.from_json(state.to_json())


def _ids(state: DurableAgentState) -> list[str | None]:
    return [message.message_id for entry in state.data.conversation_history for message in entry.messages]


async def _load(state: DurableAgentState, *, skip_excluded: bool = False) -> list[Message]:
    owner = JsonStateProvider(json.loads(state.to_json()))
    token = bind_durable_history(DurableHistoryBinding(owner))
    try:
        return await DurableHistoryProvider(skip_excluded=skip_excluded).get_messages("retention")
    finally:
        unbind_durable_history(token)


async def _eager(state: DurableAgentState) -> DurableAgentState:
    owner = JsonStateProvider(json.loads(state.to_json()))
    token = bind_durable_history(DurableHistoryBinding(owner))
    try:
        provider = DurableHistoryProvider(prune_excluded=True)
        working: dict[str, Any] = {}
        await provider.get_messages("retention", state=working)
        provider.flush(working)
        after = owner.state.to_json()
        provider.flush(working)
        assert owner.state.to_json() == after
        return owner.state
    finally:
        unbind_durable_history(token)


def _exclude(*messages: Message) -> None:
    for message in messages:
        message.additional_properties["_excluded"] = True


@pytest.mark.parametrize("kind", ["function", "mcp"])
@pytest.mark.parametrize("same_message", [False, True])
@pytest.mark.parametrize("result_count", [1, 2])
@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_overlapping_declarations_cannot_be_discharged_by_reused_id(
    kind: str, same_message: bool, result_count: int, mechanism: str
) -> None:
    old = _call(kind, "old-pending")
    newer = _call(kind, "newer-call", chars=0)
    results = [_result(kind, f"result-{index}") for index in range(result_count)]
    if same_message:
        old.contents.extend(newer.contents)
        declarations = [old]
    else:
        declarations = [old, newer]
    if mechanism == "eager":
        _exclude(*declarations, *results)
    history: list[DurableAgentStateEntry] = [_response("old", declarations[0])]
    history.append(_response("newer", *declarations[1:], *results))
    state = _state([*history, _request("current")])
    before = state.to_json()

    # No result contains an occurrence identity. Neither one result nor repeated
    # results establish which overlapping declaration completed. A set of message
    # indices would also lose the two occurrences within a single message.
    if mechanism == "eager":
        state = await _eager(state)
    else:
        with pytest.raises(StateCapacityError) as error:
            await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
        assert error.value.floor_bytes > 8_000
    assert state.to_json() == before
    assert _ids(state) == [message.message_id for message in [*declarations, *results]] + ["current"]
    assert [message.message_id for message in await _load(state)] == _ids(state)


@pytest.mark.parametrize("kind", ["function", "mcp"])
@pytest.mark.parametrize("result_count", [1, 2])
@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_completed_sequential_reuse_is_not_pinned_by_later_pending_call(
    kind: str, result_count: int, mechanism: str
) -> None:
    old = _call(kind, "completed-call")
    results = [_result(kind, f"completed-result-{index}") for index in range(result_count)]
    pending = _call(kind, "pending", chars=0)
    if mechanism == "eager":
        _exclude(old, *results, pending)
    state = _state([_response("old", old, *results), _response("pending", pending), _request("current")])
    pending_json = state.data.conversation_history[1].to_dict()

    if mechanism == "eager":
        state = await _eager(state)
    else:
        removed = await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
        assert removed == 1 + result_count
    assert _ids(state) == ["pending", "current"]
    assert state.data.conversation_history[0].to_dict() == pending_json
    assert state.data.truncation is not None
    assert state.data.truncation["evictedMessageCount"] == 1 + result_count
    assert [message.message_id for message in await _load(state)] == ["pending", "current"]


@pytest.mark.parametrize("pending_kind", ["function", "mcp"])
@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_equal_ids_in_different_tool_families_do_not_complete_each_other(
    pending_kind: str, mechanism: str
) -> None:
    completed_kind = "mcp" if pending_kind == "function" else "function"
    pending = _call(pending_kind, "pending", chars=0)
    completed = _call(completed_kind, "completed")
    result = _result(completed_kind, "result")
    if mechanism == "eager":
        _exclude(pending, completed, result)
    state = _state([_response("pending", pending), _response("completed", completed, result), _request("current")])

    if mechanism == "eager":
        state = await _eager(state)
    else:
        assert await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9) == 2
    assert _ids(state) == ["pending", "current"]
    assert [message.message_id for message in await _load(state)] == ["pending", "current"]


@pytest.mark.parametrize("kind", ["function", "mcp"])
@pytest.mark.parametrize("shape", ["same-message", "adjacent", "gap"])
@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_completed_old_pair_evicts_atomically_without_sacrificing_gap(
    kind: str, shape: str, mechanism: str
) -> None:
    call = _call(kind, "call")
    result = _result(kind, "result")
    if shape == "same-message":
        call.contents.extend(result.contents)
        pair = [call]
    else:
        pair = [call, result]
    if mechanism == "eager":
        _exclude(*pair)
    history: list[DurableAgentStateEntry] = [_response("old", *pair)]
    if shape == "gap":
        history = [_response("call", call), _request("gap"), _response("result", result)]
    state = _state([*history, _request("current")])
    expected = ["gap", "current"] if shape == "gap" else ["current"]

    if mechanism == "eager":
        state = await _eager(state)
    else:
        # Removing just the large call would already fit, so an orphan result
        # cannot hide behind an assertion that only checks the final byte count.
        removed = await enforce_budget(
            state, max_state_bytes=len(state.to_json()) - 1, high_watermark=1, low_watermark=0.99
        )
        assert removed == len(pair)
    assert _ids(state) == expected
    assert [message.message_id for message in await _load(state)] == expected
    assert state.data.truncation is not None
    assert state.data.truncation["evictedMessageCount"] == len(pair)
    after = state.to_json()
    assert await enforce_budget(state, max_state_bytes=20_000) == 0
    assert state.to_json() == after


@pytest.mark.parametrize("kind", ["function", "mcp"])
@pytest.mark.parametrize("excluded_side", ["call", "result"])
async def test_eager_included_partner_preserves_entire_pair(kind: str, excluded_side: str) -> None:
    call = _call(kind, "call")
    result = _result(kind, "result")
    _exclude(call if excluded_side == "call" else result)
    state = _state([_response("call", call), _request("gap"), _response("result", result), _request("current")])
    before = state.to_json()
    included_before = [message.to_dict() for message in await _load(state, skip_excluded=True)]

    state = await _eager(state)

    assert state.to_json() == before
    assert _ids(state) == ["call", "gap", "result", "current"]
    # Storage retention must not silently change the independent replay filter.
    assert [message.to_dict() for message in await _load(state, skip_excluded=True)] == included_before


@pytest.mark.parametrize("kind", ["function", "mcp"])
@pytest.mark.parametrize("result_state", ["missing", "diagnostic", "excluded"])
async def test_pressure_preserves_declaration_unresolved_in_either_replay_view(kind: str, result_state: str) -> None:
    call = _call(kind, "pending")
    history: list[DurableAgentStateEntry] = [_response("call", call)]
    if result_state != "missing":
        result = _result(kind, "result")
        if result_state == "diagnostic":
            stored_result = DurableAgentStateMessage.from_chat_message(result)
            history.append(DurableAgentStateErrorResponse("diagnostic", OLD, [stored_result]))
        else:
            _exclude(result)
            history.append(_response("result", result))
    state = _state([*history, _request("current")])
    before = state.to_json()
    with pytest.raises(StateCapacityError) as error:
        await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
    assert error.value.floor_bytes > 8_000
    assert state.to_json() == before
    assert [message.message_id for message in await _load(state, skip_excluded=True)] == ["pending", "current"]


@pytest.mark.parametrize("kind", ["function", "mcp"])
async def test_latest_result_protects_cross_message_declaration(kind: str) -> None:
    state = _state([
        _response("call", _call(kind, "call")),
        _request("gap"),
        _response("current", _result(kind, "result")),
    ])
    before = state.to_json()
    with pytest.raises(StateCapacityError) as error:
        await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
    assert error.value.floor_bytes > 8_000
    assert state.to_json() == before
    assert [message.message_id for message in await _load(state)] == ["call", "gap", "result"]


@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
@pytest.mark.parametrize("evict", [False, True])
@pytest.mark.parametrize("result_count", [1, 2])
async def test_hosted_mcp_atomicity_at_real_openai_replay_leaf(mechanism: str, evict: bool, result_count: int) -> None:
    # The provider package is already in the workspace's test dependency group.
    # Keep this boundary test optional for package-only installs, never mock the
    # real serializer or use a helper that guesses retention's expected links.
    pytest.importorskip("agent_framework_openai")
    from agent_framework.openai import RawOpenAIChatClient
    from openai import AsyncOpenAI

    call = _call("mcp", "call")
    result = _result("mcp", "result")
    repeated = Message(
        "assistant",
        [Content.from_mcp_server_tool_result("shared", output="later output")],
        message_id="repeated-result",
    )
    if mechanism == "eager":
        _exclude(call)
        if evict or result_count == 2:
            _exclude(result)
        if evict:
            _exclude(repeated)
    history: list[DurableAgentStateEntry] = [_response("call", call), _request("gap"), _response("result", result)]
    if result_count == 2:
        history.extend([_request("between-results"), _response("repeated", repeated)])
    if evict:
        history.append(_request("current"))
    state = _state(history)
    original = state.to_json()
    expected_call = {
        "type": "mcp_call",
        "id": "shared",
        "server_label": "catalog",
        "name": "lookup",
        "arguments": json.dumps({"query": "x" * 8_000}),
        "output": "done",
    }
    expected_gap = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "gap"}]}
    expected_tail: list[dict[str, Any]] = [expected_gap]
    if result_count == 2:
        expected_tail.append({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "between-results"}],
        })
    if evict:
        expected_tail.append({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "current"}],
        })

    async with AsyncOpenAI(api_key="unused", base_url="https://unused.invalid") as sdk:
        client = RawOpenAIChatClient(model="unused", async_client=sdk)
        before = await _load(state)
        assert [content.type for message in before[:3] for content in message.contents] == [
            "mcp_server_tool_call",
            "text",
            "mcp_server_tool_result",
        ]
        assert client._prepare_messages_for_openai(before, request_uses_service_side_storage=False) == [
            expected_call,
            *expected_tail,
        ]
        # A real orphan result is dropped by the provider, establishing why
        # merely retaining its JSON content is insufficient at this boundary.
        orphaned = client._prepare_messages_for_openai(before[1:], request_uses_service_side_storage=False)
        assert orphaned == expected_tail

        if result_count == 2:
            # The real serializer keeps the first output in a full replay. Omit
            # that result to independently prove the second needs the same call.
            expected_repeated: list[dict[str, Any]] = [{**expected_call, "output": "later output"}, *expected_tail]
            repeated_wire = client._prepare_messages_for_openai(
                [message for message in before if message.message_id != "result"],
                request_uses_service_side_storage=False,
            )
            assert repeated_wire == expected_repeated

        if mechanism == "eager":
            state = await _eager(state)
        elif evict:
            removed = await enforce_budget(
                state, max_state_bytes=len(original) - 1, high_watermark=1, low_watermark=0.99
            )
            assert removed == 1 + result_count
        else:
            with pytest.raises(StateCapacityError) as error:
                await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
            assert error.value.floor_bytes > 8_000
        expected_after: list[dict[str, Any]]
        if evict:
            assert _ids(state) == (["gap", "between-results", "current"] if result_count == 2 else ["gap", "current"])
            expected_after = expected_tail
        else:
            assert state.to_json() == original
            expected_after = [expected_call, *expected_tail]
        after = await _load(state)
        assert client._prepare_messages_for_openai(after, request_uses_service_side_storage=False) == expected_after
        if not evict and result_count == 2:
            retained_repeated_wire = client._prepare_messages_for_openai(
                [message for message in after if message.message_id != "result"],
                request_uses_service_side_storage=False,
            )
            assert retained_repeated_wire == [{**expected_call, "output": "later output"}, *expected_tail]


@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_sequential_hosted_mcp_reuse_keeps_outputs_with_their_own_declaration(mechanism: str) -> None:
    pytest.importorskip("agent_framework_openai")
    from agent_framework.openai import RawOpenAIChatClient
    from openai import AsyncOpenAI

    old = _call("mcp", "old-call")
    first = _result("mcp", "first-result")
    repeated = Message(
        "assistant",
        [Content.from_mcp_server_tool_result("shared", output="old repeated output")],
        message_id="repeated-result",
    )
    new = _call("mcp", "new-call", chars=0)
    new_result = Message(
        "assistant",
        [Content.from_mcp_server_tool_result("shared", output="new output")],
        message_id="new-result",
    )
    if mechanism == "eager":
        _exclude(old, first, repeated)
    state = _state([
        _response("old", old, first, repeated),
        _request("gap"),
        _response("current", new, new_result),
    ])
    current_json = state.data.conversation_history[-1].to_dict()
    # Literal wire expectations come from the fixture's two distinct outputs,
    # not retention's links, grouping annotations or the serializer's output.
    expected_after: list[dict[str, Any]] = [
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "gap"}]},
        {
            "type": "mcp_call",
            "id": "shared",
            "server_label": "catalog",
            "name": "lookup",
            "arguments": json.dumps({"query": ""}),
            "output": "new output",
        },
    ]
    expected_before: list[dict[str, Any]] = [
        {
            "type": "mcp_call",
            "id": "shared",
            "server_label": "catalog",
            "name": "lookup",
            "arguments": json.dumps({"query": "x" * 8_000}),
            "output": "done",
        },
        *expected_after,
    ]
    async with AsyncOpenAI(api_key="unused", base_url="https://unused.invalid") as sdk:
        client = RawOpenAIChatClient(model="unused", async_client=sdk)
        before_wire = client._prepare_messages_for_openai(await _load(state), request_uses_service_side_storage=False)
        assert before_wire == expected_before
        if mechanism == "eager":
            state = await _eager(state)
        else:
            assert await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9) == 3
        assert _ids(state) == ["gap", "new-call", "new-result"]
        assert state.data.conversation_history[-1].to_dict() == current_json
        assert state.data.truncation is not None
        assert state.data.truncation["evictedMessageCount"] == 3
        after_wire = client._prepare_messages_for_openai(await _load(state), request_uses_service_side_storage=False)
        assert after_wire == expected_after


@pytest.mark.parametrize("kind", ["function", "mcp"])
async def test_included_repeated_result_holds_old_completed_occurrence(kind: str) -> None:
    call = _call(kind, "call")
    first = _result(kind, "first")
    repeated = _result(kind, "repeated")
    _exclude(call, first)
    state = _state([
        _response("old", call, first),
        _request("gap"),
        _response("repeated", repeated),
        _request("current"),
    ])
    before = state.to_json()

    state = await _eager(state)

    assert state.to_json() == before
    assert _ids(state) == ["call", "first", "gap", "repeated", "current"]
    assert [message.message_id for message in await _load(state)] == ["call", "first", "gap", "repeated", "current"]
    assert [message.message_id for message in await _load(state, skip_excluded=True)] == ["gap", "repeated", "current"]


@pytest.mark.parametrize("old_result_role", ["user", "assistant", "system", "developer", "tool"])
@pytest.mark.parametrize("new_call_role", ["user", "assistant", "system", "developer", "tool"])
@pytest.mark.parametrize("kind", ["function", "mcp"])
@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_sequential_reuse_across_roles_does_not_inflate_the_retained_floor(
    old_result_role: str, new_call_role: str, kind: str, mechanism: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("agent_framework_openai")
    from agent_framework.openai import RawOpenAIChatClient
    from openai import AsyncOpenAI

    now = datetime(2026, 9, 21, 12, 0, 0, 123456, tzinfo=timezone.utc)
    clock = Mock(wraps=datetime)
    clock.now.return_value = now
    monkeypatch.setattr(retention, "datetime", clock)
    old = _call(kind, "A")
    old_result = _result(kind, "B")
    old_result.role = old_result_role
    new = _call(kind, "C", chars=0)
    new.role = new_call_role
    new_result = Message(
        "tool",
        [
            Content.from_function_result("shared", result="new output")
            if kind == "function"
            else Content.from_mcp_server_tool_result("shared", output="new output")
        ],
        message_id="D",
    )
    if mechanism == "eager":
        _exclude(old, old_result)
    state = _state([_response(message.message_id or "", message) for message in (old, old_result, new, new_result)])
    raw = state.to_dict()
    original = state.to_json()
    expected = deepcopy(raw)
    expected["data"]["conversationHistory"] = expected["data"]["conversationHistory"][2:]
    expected["data"]["truncation"] = {
        "evictedMessageCount": 2,
        "firstEvictedAt": now.isoformat(),
        "lastEvictedAt": now.isoformat(),
    }
    assert len(json.dumps(raw)) > 8_000
    assert len(json.dumps(expected)) < 2_700

    # Include the conventional assistant/tool control and the exact false-link
    # case A=assistant, B=developer, C=user, D=tool with no intervening gap.
    # A system B is a real floor. Adjacent B/C/D tool messages also remain one
    # Core physical span, independent of call-ID matching. Neither may be split.
    held = old_result_role == "system" or (old_result_role == "tool" and new_call_role == "tool")
    expected_after = raw if held else expected
    expected_wire: list[dict[str, Any]] = []
    for chars, output in ((8_000, "done"), (0, "new output")):
        if kind == "function":
            expected_wire.extend([
                {
                    "type": "function_call",
                    "id": "fc_shared",
                    "call_id": "shared",
                    "name": "lookup",
                    "arguments": json.dumps({"query": "x" * chars}),
                },
                {"type": "function_call_output", "call_id": "shared", "output": output},
            ])
        else:
            expected_wire.append({
                "type": "mcp_call",
                "id": "shared",
                "server_label": "catalog",
                "name": "lookup",
                "arguments": json.dumps({"query": "x" * chars}),
                "output": output,
            })
    retained_wire = expected_wire if held else expected_wire[(2 if kind == "function" else 1) :]
    async with AsyncOpenAI(api_key="unused", base_url="https://unused.invalid") as sdk:
        client = RawOpenAIChatClient(model="unused", async_client=sdk)
        before = await _load(state)
        assert client._prepare_messages_for_openai(before, request_uses_service_side_storage=False) == expected_wire
        if mechanism == "eager":
            state = await _eager(state)
        else:
            # Independently measure the exact floor, including the truncation
            # record, before testing a budget that fits just the newest pair.
            with pytest.raises(StateCapacityError) as error:
                await enforce_budget(state, max_state_bytes=1, high_watermark=1, low_watermark=0.9)
            assert error.value.floor_bytes == len(json.dumps(expected_after))
            assert state.to_json() == original
            if held:
                with pytest.raises(StateCapacityError):
                    await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
            else:
                assert await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9) == 2
        assert json.dumps(state.to_dict(), sort_keys=True) == json.dumps(expected_after, sort_keys=True)
        assert _ids(state) == (["A", "B", "C", "D"] if held else ["C", "D"])
        assert state.data.conversation_history[-2].to_dict() == raw["data"]["conversationHistory"][-2]
        assert state.data.conversation_history[-1].to_dict() == raw["data"]["conversationHistory"][-1]
        after = await _load(state)
        assert client._prepare_messages_for_openai(after, request_uses_service_side_storage=False) == retained_wire


@pytest.mark.parametrize("tokenize", [False, True])
def test_retention_annotations_preserve_reasoning_and_original_content_tokens(tokenize: bool) -> None:
    # Long IDs make counting the dummy annotation IDs observable. Token costs
    # must still describe real contents, not the temporary grouping projection.
    call_id = "retention_content_0_0" * 100
    messages = [
        Message("assistant", [Content.from_text_reasoning(text="prefix")], message_id="reasoning"),
        _call("function", "A", chars=0, call_id=call_id),
        _result("function", "B", call_id=call_id),
        _call("function", "C", chars=0, call_id=call_id),
        _result("function", "D", call_id=call_id),
    ]
    messages[2].role = "developer"
    messages[3].role = "user"
    original_contents = [[content.to_dict() for content in message.contents] for message in messages]
    control = deepcopy(messages)
    annotate_message_groups(control, force_reannotate=True, tokenizer=CharacterEstimatorTokenizer())

    retention._annotate_retention_groups(messages, tokenizer=CharacterEstimatorTokenizer() if tokenize else None)

    assert [[content.to_dict() for content in message.contents] for message in messages] == original_contents
    assert [message.message_id for message in messages] == ["reasoning", "A", "B", "C", "D"]
    groups = [message.additional_properties["_group"]["id"] for message in messages]
    assert groups[0] == groups[1]
    assert len(set(groups[1:])) == 4
    assert messages[0].additional_properties["_group"]["has_reasoning"] is True
    assert messages[1].additional_properties["_group"]["has_reasoning"] is True
    if tokenize:
        assert included_token_count(messages) == included_token_count(control)
        assert [message.additional_properties["_group"]["token_count"] for message in messages] == [
            message.additional_properties["_group"]["token_count"] for message in control
        ]


@pytest.mark.parametrize("role", ["user", "assistant", "system", "developer", "tool"])
@pytest.mark.parametrize("kind", ["function", "mcp"])
@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_pending_call_in_every_schema_role_survives_at_real_openai_leaf(
    role: str, kind: str, mechanism: str
) -> None:
    pytest.importorskip("agent_framework_openai")
    from agent_framework.openai import RawOpenAIChatClient
    from openai import AsyncOpenAI

    call = _call(kind, "pending")
    call.role = role
    if mechanism == "eager":
        _exclude(call)
    state = _state([_response("old", call), _request("current")])
    original = state.to_json()
    expected_call: dict[str, Any]
    if kind == "function":
        expected_call = {
            "type": "function_call",
            "id": "fc_shared",
            "call_id": "shared",
            "name": "lookup",
            "arguments": json.dumps({"query": "x" * 8_000}),
        }
    else:
        expected_call = {
            "type": "mcp_call",
            "id": "shared",
            "server_label": "catalog",
            "name": "lookup",
            "arguments": json.dumps({"query": "x" * 8_000}),
        }
    expected_wire: list[dict[str, Any]] = [
        expected_call,
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "current"}]},
    ]

    async with AsyncOpenAI(api_key="unused", base_url="https://unused.invalid") as sdk:
        client = RawOpenAIChatClient(model="unused", async_client=sdk)
        before = await _load(state)
        assert before[0].role == role
        assert client._prepare_messages_for_openai(before, request_uses_service_side_storage=False) == expected_wire
        if mechanism == "eager":
            state = await _eager(state)
        else:
            with pytest.raises(StateCapacityError) as error:
                await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
            assert error.value.floor_bytes > 8_000
        assert state.to_json() == original
        assert _ids(state) == ["pending", "current"]
        after = await _load(state)
        assert after[0].role == role
        assert client._prepare_messages_for_openai(after, request_uses_service_side_storage=False) == expected_wire


@pytest.mark.parametrize("role", ["user", "assistant", "system", "developer", "tool"])
@pytest.mark.parametrize("role_side", ["call", "results"])
@pytest.mark.parametrize("kind", ["function", "mcp"])
@pytest.mark.parametrize("result_count", [1, 2])
@pytest.mark.parametrize("evict", [False, True])
@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_tool_pair_in_every_schema_role_is_atomic_at_real_openai_leaf(
    role: str, role_side: str, kind: str, result_count: int, evict: bool, mechanism: str
) -> None:
    pytest.importorskip("agent_framework_openai")
    from agent_framework.openai import RawOpenAIChatClient
    from openai import AsyncOpenAI

    call = _call(kind, "call")
    first = _result(kind, "first-result")
    repeated = Message(
        "tool" if kind == "function" else "assistant",
        [
            Content.from_function_result("shared", result="later output")
            if kind == "function"
            else Content.from_mcp_server_tool_result("shared", output="later output")
        ],
        message_id="repeated-result",
    )
    results = [first, repeated] if result_count == 2 else [first]
    for message in [call] if role_side == "call" else results:
        message.role = role
    if mechanism == "eager":
        _exclude(call, *(results if evict else results[:-1]))
    history: list[DurableAgentStateEntry] = [_response("call", call), _request("gap"), _response("first", first)]
    if result_count == 2:
        history.extend([_request("between-results"), _response("repeated", repeated)])
    if evict:
        history.append(_request("current"))
    state = _state(history)
    original = state.to_json()

    expected_call: dict[str, Any]
    if kind == "function":
        expected_call = {
            "type": "function_call",
            "id": "fc_shared",
            "call_id": "shared",
            "name": "lookup",
            "arguments": json.dumps({"query": "x" * 8_000}),
        }
    else:
        expected_call = {
            "type": "mcp_call",
            "id": "shared",
            "server_label": "catalog",
            "name": "lookup",
            "arguments": json.dumps({"query": "x" * 8_000}),
            "output": "done",
        }
    expected_gap = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "gap"}]}
    expected_survivors: list[dict[str, Any]] = [expected_gap]
    expected_before: list[dict[str, Any]] = [expected_call, expected_gap]
    if kind == "function":
        expected_before.append({"type": "function_call_output", "call_id": "shared", "output": "done"})
    if result_count == 2:
        gap = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "between-results"}]}
        expected_survivors.append(gap)
        expected_before.append(gap)
        if kind == "function":
            expected_before.append({"type": "function_call_output", "call_id": "shared", "output": "later output"})
    if evict:
        current = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "current"}]}
        expected_survivors.append(current)
        expected_before.append(current)

    # System messages have their own floor, including when only a result is
    # system-role. Native projection itself treats all five roles identically.
    should_evict = evict and role != "system"
    async with AsyncOpenAI(api_key="unused", base_url="https://unused.invalid") as sdk:
        client = RawOpenAIChatClient(model="unused", async_client=sdk)
        before = await _load(state)
        expected_roles = {message.message_id: message.role for message in [call, *results]}
        actual_roles = {message.message_id: message.role for message in before if message.message_id in expected_roles}
        assert actual_roles == expected_roles
        assert client._prepare_messages_for_openai(before, request_uses_service_side_storage=False) == expected_before
        if mechanism == "eager":
            state = await _eager(state)
        elif should_evict:
            removed = await enforce_budget(
                state, max_state_bytes=len(original) - 1, high_watermark=1, low_watermark=0.99
            )
            assert removed == 1 + result_count
        else:
            with pytest.raises(StateCapacityError) as error:
                await enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
            assert error.value.floor_bytes > 8_000
        if should_evict:
            assert _ids(state) == (["gap", "between-results", "current"] if result_count == 2 else ["gap", "current"])
            assert state.data.truncation is not None
            assert state.data.truncation["evictedMessageCount"] == 1 + result_count
        else:
            assert state.to_json() == original
        after = await _load(state)
        expected_after = expected_survivors if should_evict else expected_before
        assert client._prepare_messages_for_openai(after, request_uses_service_side_storage=False) == expected_after
        if kind == "mcp" and result_count == 2:
            # The first output wins full MCP replay. Removing it from the input
            # independently demonstrates that the repeated result needs its call.
            expected_repeated: list[dict[str, Any]] = [
                {**expected_call, "output": "later output"},
                *expected_survivors,
            ]
            assert client._prepare_messages_for_openai(
                [message for message in before if message.message_id != "first-result"],
                request_uses_service_side_storage=False,
            ) == expected_repeated
            assert client._prepare_messages_for_openai(
                [message for message in after if message.message_id != "first-result"],
                request_uses_service_side_storage=False,
            ) == (expected_survivors if should_evict else expected_repeated)
