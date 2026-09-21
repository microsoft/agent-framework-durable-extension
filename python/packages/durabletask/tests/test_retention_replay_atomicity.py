# Copyright (c) Microsoft. All rights reserved.

"""Retention must preserve atomic links and failure classification through replay."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient
from agent_framework import Content, Message

from agent_framework_durabletask import AgentEntity
from agent_framework_durabletask import _retention as retention
from agent_framework_durabletask._history_provider import (
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    unbind_durable_history,
)
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUnknownEntry,
)

NOW = datetime(2026, 9, 19, 12, 0, 0, 123456, tzinfo=timezone.utc)
OLD = NOW - timedelta(hours=2)


@pytest.fixture(autouse=True)
def fixed_retention_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = Mock(wraps=datetime)
    clock.now.return_value = NOW
    monkeypatch.setattr(retention, "datetime", clock)


def _call(message_id: str, *, call_id: str = "t", chars: int = 8_000, mixed: bool = False) -> Message:
    contents = [Content.from_function_call(call_id, "lookup", arguments=json.dumps({"payload": "a" * chars}))]
    if mixed:
        contents.insert(0, Content.from_text_reasoning(text="private reasoning"))
    return Message("assistant", contents, message_id=message_id)


def _result(message_id: str, *, call_id: str = "t") -> Message:
    return Message("tool", [Content.from_function_result(call_id, result="result")], message_id=message_id)


def _stored(message: Message) -> DurableAgentStateMessage:
    return DurableAgentStateMessage.from_chat_message(message)


def _pair_history(
    *, diagnostic: str = "error_response", non_contiguous: bool = False, mixed: bool = False
) -> list[DurableAgentStateEntry]:
    history: list[DurableAgentStateEntry] = [
        DurableAgentStateResponse("A", OLD, [_stored(_call("needed-call", mixed=mixed))])
    ]
    if non_contiguous:
        history.append(DurableAgentStateRequest("gap", OLD, [_stored(Message("user", ["gap"], message_id="gap"))]))
    diagnostic_message = _stored(_call("diagnostic-call", chars=0))
    if diagnostic == "error_response":
        history.append(DurableAgentStateErrorResponse("B", OLD, [diagnostic_message]))
    elif diagnostic == "flagged_response":
        entry = DurableAgentStateResponse("B", OLD, [diagnostic_message])
        entry.extension_data = {"durable_status": "error"}
        history.append(entry)
    elif diagnostic == "error_content":
        # Error classification excludes the whole response, not just its error content.
        history.append(
            DurableAgentStateResponse(
                "B",
                OLD,
                [
                    _stored(Message("assistant", [Content.from_error(message="failed")], message_id="failure")),
                    diagnostic_message,
                ],
            )
        )
    else:
        assert diagnostic == "none"
    history.append(DurableAgentStateRequest("C", OLD, [_stored(_result("latest-result"))]))
    return history


def _state(history: list[DurableAgentStateEntry]) -> DurableAgentState:
    state = DurableAgentState()
    state.data.conversation_history = history
    raw = state.to_dict()
    raw["futureRoot"] = {"values": [None, False, 0, 0.0]}
    raw["data"]["futureData"] = {"values": [None, False, 0, 0.0]}
    return DurableAgentState.from_json(json.dumps(raw))


def _snapshot(value: Any) -> str:
    # Unlike dict equality, JSON distinguishes False, 0 and 0.0 in opaque data.
    return json.dumps(value, sort_keys=True, allow_nan=False)


def _view(messages: list[Message]) -> list[tuple[Any, ...]]:
    # Inspect every content item without applying either production replay filter.
    return [
        (
            message.message_id,
            message.role,
            [
                (
                    content.type,
                    getattr(content, "call_id", None),
                    getattr(content, "arguments", None),
                    getattr(content, "result", None),
                    getattr(content, "text", None),
                )
                for content in message.contents
            ],
        )
        for message in messages
    ]


async def _assert_replay(
    state: DurableAgentState, expected: list[Message], *, included: list[Message] | None = None
) -> None:
    # Exercise both real consumers after a JSON reload. Expected messages are
    # specified by each fixture, not selected using retention or grouping helpers.
    owner = JsonStateProvider(json.loads(state.to_json()))
    entity = AgentEntity(NonStreamingAgent(client=RecordingChatClient(), name="retention-replay"), state_provider=owner)
    assert _view(entity._replay_all_messages()) == _view(expected)
    token = bind_durable_history(DurableHistoryBinding(state_provider=owner))
    try:
        for skip_excluded in (False, True):
            loaded = await DurableHistoryProvider(skip_excluded=skip_excluded).get_messages("retention-replay")
            selected = included if skip_excluded and included is not None else expected
            assert _view(loaded) == _view(selected)
    finally:
        unbind_durable_history(token)


def _expected_removal(raw: dict[str, Any], removed_ids: set[str]) -> dict[str, Any]:
    """Independent JSON oracle for these fixtures' bare, nonempty entry envelopes."""
    expected = deepcopy(raw)
    history = expected["data"]["conversationHistory"]
    for entry in history:
        entry["messages"] = [message for message in entry["messages"] if message["messageId"] not in removed_ids]
    expected["data"]["conversationHistory"] = [entry for entry in history if entry["messages"]]
    expected["data"]["truncation"] = {
        "evictedMessageCount": len(removed_ids),
        "firstEvictedAt": NOW.isoformat(),
        "lastEvictedAt": NOW.isoformat(),
    }
    return expected


@pytest.mark.parametrize("diagnostic", ["none", "error_response", "flagged_response", "error_content"])
@pytest.mark.parametrize("non_contiguous", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
async def test_latest_result_protects_its_replayable_declaration(
    diagnostic: str, non_contiguous: bool, mixed: bool
) -> None:
    state = _state(_pair_history(diagnostic=diagnostic, non_contiguous=non_contiguous, mixed=mixed))
    raw = state.to_dict()
    before = _snapshot(raw)
    history = state.data.conversation_history
    entries = tuple(history)
    stored = tuple(tuple(entry.messages) for entry in history)
    expected = [_call("needed-call")]
    if non_contiguous:
        expected.append(Message("user", ["gap"], message_id="gap"))
    expected.append(_result("latest-result"))
    await _assert_replay(state, expected)

    # A's 8 KB declaration cannot fit under 3 KB. B is diagnostic-only, so it
    # cannot replace A when C's protected result is replayed. No deletion is safe.
    with pytest.raises(retention.StateCapacityError) as error:
        await retention.enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)

    assert error.value.floor_bytes > 8_000
    assert _snapshot(state.to_dict()) == before
    assert _snapshot(raw) == before
    assert state.data.conversation_history is history
    assert all(entry is original for entry, original in zip(history, entries, strict=True))
    for entry, messages in zip(history, stored, strict=True):
        assert all(message is original for message, original in zip(entry.messages, messages, strict=True))
    await _assert_replay(state, expected)


def _evictable_history() -> list[DurableAgentStateEntry]:
    # A completed older exchange reuses t. Protect occurrences, not every call
    # that ever used the same identifier as a currently protected result.
    return [
        DurableAgentStateResponse(
            "old-tools",
            OLD,
            [
                _stored(_call("old-call", chars=10_000)),
                _stored(_result("old-result")),
            ],
        ),
        DurableAgentStateErrorResponse(
            "old-error", OLD, [_stored(_call("old-diagnostic", call_id="unrelated", chars=10_000))]
        ),
        DurableAgentStateRequest("old-prose", OLD, [_stored(Message("user", ["p" * 10_000], message_id="old-prose"))]),
    ]


async def test_protected_replay_pair_does_not_pin_unrelated_tools_errors_or_prose() -> None:
    state = _state(_evictable_history() + _pair_history(mixed=True))
    await _assert_replay(
        state,
        [
            _call("old-call", chars=10_000),
            _result("old-result"),
            Message("user", ["p" * 10_000], message_id="old-prose"),
            _call("needed-call"),
            _result("latest-result"),
        ],
    )
    raw = state.to_dict()
    expected = _expected_removal(raw, {"old-call", "old-result", "old-diagnostic", "old-prose"})
    assert len(json.dumps(raw)) > 14_000
    assert len(json.dumps(expected)) < 12_600

    removed = await retention.enforce_budget(state, max_state_bytes=14_000, high_watermark=1, low_watermark=0.9)

    assert removed == 4
    assert _snapshot(state.to_dict()) == _snapshot(expected)
    await _assert_replay(state, [_call("needed-call"), _result("latest-result")])
    after = state.to_json()
    assert await retention.enforce_budget(state, max_state_bytes=14_000, high_watermark=1, low_watermark=0.9) == 0
    assert state.to_json() == after


async def test_old_replay_and_physical_reasoning_links_evict_together_across_a_gap() -> None:
    reasoning = Message("assistant", [Content.from_text_reasoning(text="private prefix")], message_id="reasoning")
    history: list[DurableAgentStateEntry] = [DurableAgentStateResponse("reasoning", OLD, [_stored(reasoning)])]
    history.extend(_pair_history(non_contiguous=True, mixed=True))
    current = Message("user", ["current"], message_id="current")
    history.append(DurableAgentStateRequest("current", OLD, [_stored(current)]))
    state = _state(history)
    gap = Message("user", ["gap"], message_id="gap")
    await _assert_replay(state, [_call("needed-call"), gap, _result("latest-result"), current])
    raw = state.to_dict()
    expected = _expected_removal(raw, {"reasoning", "needed-call", "latest-result"})

    # With no protected member, the replay union is evictable. Neither the
    # intervening user nor the opaque diagnostic belongs to that tool group.
    removed = await retention.enforce_budget(
        state, max_state_bytes=len(json.dumps(raw)) - 1, high_watermark=1, low_watermark=0.99
    )

    assert removed == 3
    assert _snapshot(state.to_dict()) == _snapshot(expected)
    await _assert_replay(state, [gap, current])


@pytest.mark.parametrize("protection", ["system", "current"])
async def test_saved_diagnostic_links_protect_only_their_persisted_group(protection: str) -> None:
    history = _pair_history(mixed=True)
    history[1].messages[0].extension_data = {"_group": {"id": "saved-guard", "future": [False, 0, 0.0]}}
    prose = Message("user", ["p" * 10_000], message_id="eligible")
    history.append(DurableAgentStateRequest("eligible", OLD, [_stored(prose)]))
    guard = Message("system" if protection == "system" else "user", ["guard"], message_id="guard")
    held = _stored(guard)
    held.extension_data = {"_group": {"id": "saved-guard", "future": [False, 0, 0.0]}}
    history.append(DurableAgentStateRequest("guard", OLD, [held]))
    expected_replay = [prose, guard]
    if protection == "system":
        current = Message("user", ["current"], message_id="current")
        history.append(DurableAgentStateRequest("current", OLD, [_stored(current)]))
        expected_replay.append(current)
    state = _state(history)
    await _assert_replay(state, [_call("needed-call"), _result("latest-result"), *expected_replay])
    raw = state.to_dict()
    expected = _expected_removal(raw, {"needed-call", "latest-result"})
    assert len(json.dumps(raw)) > 14_000
    assert len(json.dumps(expected)) < 12_600

    removed = await retention.enforce_budget(state, max_state_bytes=14_000, high_watermark=1, low_watermark=0.9)

    # The guard retains the persisted diagnostic link. Hidden diagnostic
    # content must not create a physical link to the unrelated replay pair.
    assert removed == 2
    assert _snapshot(state.to_dict()) == _snapshot(expected)
    await _assert_replay(state, expected_replay)


async def test_failed_plan_leaves_raw_state_and_original_objects_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _state(_evictable_history() + _pair_history(mixed=True))
    raw = state.to_dict()
    before = _snapshot(raw)
    history = state.data.conversation_history
    entries = tuple(history)
    messages = tuple(message for entry in history for message in entry.messages)
    originals = tuple(_snapshot(message.to_dict()) for message in messages)

    async def no_progress(planned: list[Message]) -> bool:
        planned[0].contents.clear()
        planned[0].additional_properties["planning-only"] = {"mutated": True}
        return False

    strategy = AsyncMock(side_effect=no_progress)
    monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", Mock(return_value=strategy))
    with pytest.raises(retention.StateCapacityError):
        await retention.enforce_budget(state, max_state_bytes=14_000, high_watermark=1, low_watermark=0.9)

    assert strategy.await_count == 3
    assert _snapshot(state.to_dict()) == before
    assert _snapshot(raw) == before
    assert state.data.conversation_history is history
    assert all(entry is original for entry, original in zip(history, entries, strict=True))
    remaining = [message for entry in history for message in entry.messages]
    assert all(message is original for message, original in zip(remaining, messages, strict=True))
    assert tuple(_snapshot(message.to_dict()) for message in messages) == originals
    await _assert_replay(
        state,
        [
            _call("old-call", chars=10_000),
            _result("old-result"),
            Message("user", ["p" * 10_000], message_id="old-prose"),
            _call("needed-call"),
            _result("latest-result"),
        ],
    )


@pytest.mark.parametrize("protected_result", [False, True])
async def test_excluded_duplicate_call_cannot_hide_the_default_replay_link(protected_result: bool) -> None:
    history = _pair_history(diagnostic="none")
    excluded = _call("excluded-call", chars=0)
    excluded.additional_properties["_excluded"] = True
    history.insert(1, DurableAgentStateResponse("B", OLD, [_stored(excluded)]))
    expected = [_call("needed-call"), excluded, _result("latest-result")]
    included = [_call("needed-call"), _result("latest-result")]
    current = Message("user", ["current"], message_id="current")
    if not protected_result:
        history.append(DurableAgentStateRequest("current", OLD, [_stored(current)]))
        expected.append(current)
        included.append(current)
    state = _state(history)
    await _assert_replay(state, expected, included=included)
    raw = state.to_dict()
    before = _snapshot(raw)

    # Default replay sees A, C, but full replay sees two overlapping declarations
    # A, B before C. C cannot establish which declaration completed. Even after
    # the result ages out, ambiguous pending declarations remain a floor.
    with pytest.raises(retention.StateCapacityError) as error:
        await retention.enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
    assert error.value.floor_bytes > 8_000
    assert _snapshot(state.to_dict()) == before
    await _assert_replay(state, expected, included=included)
    assert _snapshot(raw) == before


@pytest.mark.parametrize("protected_partner", [False, True])
async def test_error_classified_response_is_retained_or_evicted_as_a_whole(protected_partner: bool) -> None:
    failure = _stored(Message("assistant", [Content.from_error(message="e" * 8_000)], message_id="E"))
    diagnostic = _stored(Message("assistant", ["diagnostic, not a successful answer"], message_id="D"))
    if protected_partner:
        diagnostic.extension_data = {"_group": {"id": "failure-guard"}}
    failed = DurableAgentStateResponse("failed", OLD, [failure, diagnostic])
    failed.unknown_fields = {"futureEnvelope": {"values": [None, False, 0, 0.0]}}
    failed.extension_data = {"futureMetadata": {"values": [None, False, 0, 0.0]}}
    current = Message("user", ["current"], message_id="current")
    if protected_partner:
        current.additional_properties["_group"] = {"id": "failure-guard"}
    state = _state([failed, DurableAgentStateRequest("current", OLD, [_stored(current)])])
    raw = state.to_dict()
    before = _snapshot(raw)
    assert state.data.conversation_history[0].is_error_response
    assert raw["data"]["conversationHistory"][0]["$type"] == "response"
    await _assert_replay(state, [current])

    if protected_partner:
        with pytest.raises(retention.StateCapacityError):
            await retention.enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
        assert _snapshot(state.to_dict()) == before
        assert state.data.conversation_history[0].is_error_response
    else:
        # Removing E alone fits this target, but would turn D into replayable
        # assistant text. Remove both, without rewriting the response envelope.
        removed = await retention.enforce_budget(
            state, max_state_bytes=len(json.dumps(raw)) - 1, high_watermark=1, low_watermark=0.99
        )
        assert removed == 2
        expected = deepcopy(raw)
        expected["data"]["conversationHistory"][0]["messages"] = []
        expected["data"]["truncation"] = {
            "evictedMessageCount": 2,
            "firstEvictedAt": NOW.isoformat(),
            "lastEvictedAt": NOW.isoformat(),
        }
        assert _snapshot(state.to_dict()) == _snapshot(expected)
    assert _snapshot(raw) == before
    await _assert_replay(state, [current])


@pytest.mark.parametrize("partner_kind", ["included", "error", "system", "opaque", "excluded"])
async def test_eager_prune_saved_group_includes_nonreplayable_physical_partners(partner_kind: str) -> None:
    candidate = Message(
        "user",
        ["excluded candidate"],
        message_id="candidate",
        additional_properties={"_excluded": True, "_group": {"id": "g"}},
    )
    partner = Message(
        "system" if partner_kind == "system" else "assistant",
        ["saved partner"],
        message_id="partner",
        additional_properties={"_group": {"id": "g", "futureGroup": [False, 0, 0.0]}},
    )
    if partner_kind in ("system", "excluded"):
        partner.additional_properties["_excluded"] = True
    saved_partner = _stored(partner)
    if partner_kind in ("error", "opaque"):
        # This profile is valid stored opaque data, but invalid for Core
        # projection. Skipped partners must never enter to_chat_message().
        saved_partner.contents[0].unknown_fields = {
            "pythonCoreFields": {
                "profile": "agent-framework-python.core-fields",
                "version": 1,
                "fields": None,
                "futureProfile": [None, False, 0, 0.0],
            }
        }
    partner_entry = (
        DurableAgentStateErrorResponse("partner", OLD, [saved_partner])
        if partner_kind in ("error", "opaque")
        else DurableAgentStateResponse("partner", OLD, [saved_partner])
    )
    partner_entry.unknown_fields = {"futureEnvelope": [None, False, 0, 0.0]}
    gap = Message("user", ["gap"], message_id="gap")
    current = Message("user", ["current"], message_id="current")
    owner = JsonStateProvider(
        _state([
            DurableAgentStateRequest("candidate", OLD, [_stored(candidate)]),
            DurableAgentStateRequest("gap", OLD, [_stored(gap)]),
            partner_entry,
            DurableAgentStateRequest("current", OLD, [_stored(current)]),
        ]).to_dict()
    )
    if partner_kind == "opaque":
        # Unknown runtime entries may retain a known discriminator. Preserve
        # their physical links without using that discriminator as permission
        # to project or delete their contents.
        original = owner.state.data.conversation_history[2]
        opaque = DurableAgentStateUnknownEntry(original.to_dict())
        opaque.messages = original.messages
        owner.state.data.conversation_history[2] = opaque
    raw = owner.state.to_dict()
    before = _snapshot(raw)
    visible = [candidate, gap]
    included = [gap]
    if partner_kind not in ("error", "opaque"):
        visible.append(partner)
    if partner_kind == "included":
        included.append(partner)
    visible.append(current)
    included.append(current)
    await _assert_replay(owner.state, visible, included=included)
    if partner_kind in ("error", "opaque"):
        # Pressure must not project hidden profiles in either known diagnostics
        # or runtime-opaque entries based on the stored discriminator alone.
        with pytest.raises(retention.StateCapacityError):
            await retention.enforce_budget(owner.state, max_state_bytes=1, high_watermark=1, low_watermark=0.9)
        assert _snapshot(owner.state.to_dict()) == before
    provider = DurableHistoryProvider(prune_excluded=True)
    working: dict[str, Any] = {}
    token = bind_durable_history(DurableHistoryBinding(state_provider=owner))
    try:
        assert _view(await provider.get_messages("session", state=working)) == _view(included)
        provider.flush(working)
        if partner_kind == "excluded":
            expected = deepcopy(raw)
            expected["data"]["conversationHistory"].pop(0)
            expected["data"]["conversationHistory"][1]["messages"] = []
            expected["data"]["truncation"] = {
                "evictedMessageCount": 2,
                "firstEvictedAt": NOW.isoformat(),
                "lastEvictedAt": NOW.isoformat(),
            }
            assert _snapshot(owner.state.to_dict()) == _snapshot(expected)
        else:
            assert _snapshot(owner.state.to_dict()) == before
        after = _snapshot(owner.state.to_dict())
        provider.flush(working)
        assert _snapshot(owner.state.to_dict()) == after
    finally:
        unbind_durable_history(token)
    assert _snapshot(raw) == before
    if partner_kind == "excluded":
        await _assert_replay(owner.state, [gap, current])
    else:
        await _assert_replay(owner.state, visible, included=included)


@pytest.mark.parametrize("diagnostic_kind", ["error_response", "flagged_response", "error_content"])
@pytest.mark.parametrize("profile_version", [None, 1, 999])
@pytest.mark.parametrize("protected", [False, True])
async def test_pressure_treats_hidden_diagnostic_profiles_as_opaque_payloads(
    diagnostic_kind: str, profile_version: int | None, protected: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    failure = _stored(Message("assistant", [Content.from_error(message="failed")], message_id="failure"))
    diagnostic = _stored(Message("assistant", ["d" * 8_000], message_id="diagnostic"))
    profile: dict[str, Any] | None = None
    if profile_version is not None:
        profile = {
            "profile": "agent-framework-python.core-fields",
            "version": profile_version,
            "fields": None,
            "futureProfile": [None, False, 0, 0.0],
        }
    diagnostic.contents[0].unknown_fields = {"pythonCoreFields": profile}
    if protected:
        diagnostic.extension_data = {"_group": {"id": "diagnostic-guard", "future": [False, 0, 0.0]}}
    failed = (
        DurableAgentStateErrorResponse("failed", OLD, [failure, diagnostic])
        if diagnostic_kind == "error_response"
        else DurableAgentStateResponse("failed", OLD, [failure, diagnostic])
    )
    if diagnostic_kind != "error_content":
        # Isolate envelope/status classification from content classification.
        failure.contents = _stored(Message("assistant", ["failed"])).contents
    if diagnostic_kind == "flagged_response":
        failed.extension_data = {"durable_status": "error"}
    failed.unknown_fields = {"futureEnvelope": [None, False, 0, 0.0]}
    prose = Message("user", ["p" * 10_000], message_id="old-prose")
    current = Message("user", ["current"], message_id="current")
    if protected:
        current.additional_properties["_group"] = {"id": "diagnostic-guard"}
    state = _state([
        failed,
        DurableAgentStateRequest("old-prose", OLD, [_stored(prose)]),
        DurableAgentStateRequest("current", OLD, [_stored(current)]),
    ])
    raw = state.to_dict()
    before = _snapshot(raw)
    assert state.data.conversation_history[0].is_error_response
    project = DurableAgentStateMessage.to_chat_message

    def project_replayable(stored: DurableAgentStateMessage) -> Message:
        # Future profiles need the same guarantee even if Core would silently
        # accept them. Keep the real projection for all replayable messages.
        assert stored.message_id not in ("failure", "diagnostic")
        return project(stored)

    monkeypatch.setattr(DurableAgentStateMessage, "to_chat_message", project_replayable)
    await _assert_replay(state, [prose, current])
    diagnostic_entry = state.data.conversation_history[0]
    assert (
        retention._token_budget(
            [(diagnostic_entry, stored) for stored in diagnostic_entry.messages],
            serialized_size=len(json.dumps(raw)),
            evictable_bytes=sum(len(json.dumps(stored.to_dict())) for stored in diagnostic_entry.messages),
            target_bytes=len(json.dumps(raw)),
        )
        > 0
    )

    # Independent JSON floor. The diagnostic envelope is retained even after
    # its entire message batch is evicted because it owns unknown metadata.
    floor = deepcopy(raw)
    floor["data"]["conversationHistory"].pop(1)
    if not protected:
        floor["data"]["conversationHistory"][0]["messages"] = []
    floor["data"]["truncation"] = {
        "evictedMessageCount": 1 if protected else 3,
        "firstEvictedAt": NOW.isoformat(),
        "lastEvictedAt": NOW.isoformat(),
    }
    with pytest.raises(retention.StateCapacityError) as error:
        await retention.enforce_budget(state, max_state_bytes=1, high_watermark=1, low_watermark=0.9)
    assert error.value.floor_bytes == len(json.dumps(floor))
    assert _snapshot(state.to_dict()) == before

    # This target fits either the protected diagnostic or the remaining prose,
    # not both. The budget is unchanged between the two policy cases.
    expected = deepcopy(raw)
    if protected:
        expected["data"]["conversationHistory"].pop(1)
    else:
        expected["data"]["conversationHistory"][0]["messages"] = []
    expected["data"]["truncation"] = {
        "evictedMessageCount": 1 if protected else 2,
        "firstEvictedAt": NOW.isoformat(),
        "lastEvictedAt": NOW.isoformat(),
    }
    assert len(json.dumps(raw)) > 14_000
    assert len(json.dumps(expected)) < 12_600
    removed = await retention.enforce_budget(state, max_state_bytes=14_000, high_watermark=1, low_watermark=0.9)

    assert removed == (1 if protected else 2)
    assert _snapshot(state.to_dict()) == _snapshot(expected)
    assert _snapshot(raw) == before
    await _assert_replay(state, [current] if protected else [prose, current])
    if protected:
        restored = DurableAgentState.from_json(state.to_json())
        assert restored.data.conversation_history[0].is_error_response
        assert _snapshot(restored.data.conversation_history[0].to_dict()) == _snapshot(
            raw["data"]["conversationHistory"][0]
        )
    after = state.to_json()
    assert await retention.enforce_budget(state, max_state_bytes=14_000, high_watermark=1, low_watermark=0.9) == 0
    assert state.to_json() == after


@pytest.mark.parametrize("entry_kind", ["request", "response"])
async def test_pressure_does_not_hide_invalid_replayable_profiles(entry_kind: str) -> None:
    role = "user" if entry_kind == "request" else "assistant"
    invalid = _stored(Message(role, ["p" * 8_000], message_id="invalid"))
    invalid.contents[0].unknown_fields = {
        "pythonCoreFields": {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": None,
        }
    }
    entry = (
        DurableAgentStateRequest("invalid", OLD, [invalid])
        if entry_kind == "request"
        else DurableAgentStateResponse("invalid", OLD, [invalid])
    )
    current = Message("user", ["current"], message_id="current")
    state = _state([entry, DurableAgentStateRequest("current", OLD, [_stored(current)])])
    raw = state.to_dict()
    before = _snapshot(raw)
    history = state.data.conversation_history
    assert not history[0].is_error_response
    owner = JsonStateProvider(json.loads(state.to_json()))
    entity = AgentEntity(NonStreamingAgent(client=RecordingChatClient(), name="invalid-replay"), state_provider=owner)
    expected_error = "The Python core-fields profile requires a fields object"
    # Only entries that replay already excludes are opaque. Invalid visible
    # content must still fail at both real readers and the pressure planner.
    with pytest.raises(ValueError, match=expected_error):
        entity._replay_all_messages()
    token = bind_durable_history(DurableHistoryBinding(state_provider=owner))
    try:
        for skip_excluded in (False, True):
            with pytest.raises(ValueError, match=expected_error):
                await DurableHistoryProvider(skip_excluded=skip_excluded).get_messages("invalid-replay")
    finally:
        unbind_durable_history(token)
    with pytest.raises(ValueError, match=expected_error):
        await retention.enforce_budget(state, max_state_bytes=3_000, high_watermark=1, low_watermark=0.9)
    with pytest.raises(ValueError, match=expected_error):
        retention._token_budget(
            [(history[0], history[0].messages[0])],
            serialized_size=len(json.dumps(raw)),
            evictable_bytes=len(json.dumps(history[0].messages[0].to_dict())),
            target_bytes=3_000,
        )
    assert state.data.conversation_history is history
    assert _snapshot(state.to_dict()) == before
    assert _snapshot(owner.state.to_dict()) == before
    assert _snapshot(raw) == before


@pytest.mark.parametrize("excluded", [False, True])
async def test_eager_prune_keeps_pending_calls_and_never_deletes_included_messages(excluded: bool) -> None:
    pending = _call("pending", call_id="pending", chars=0)
    completed = _call("completed", call_id="completed", chars=0)
    result = _result("result", call_id="completed")
    for message in (pending, completed, result):
        if excluded:
            message.additional_properties["_excluded"] = True
    current = Message("user", ["current"], message_id="current")
    owner = JsonStateProvider(
        _state([
            DurableAgentStateResponse("pending", OLD, [_stored(pending)]),
            DurableAgentStateResponse("completed", OLD, [_stored(completed), _stored(result)]),
            DurableAgentStateRequest("current", OLD, [_stored(current)]),
        ]).to_dict()
    )
    raw = owner.state.to_dict()
    provider = DurableHistoryProvider(prune_excluded=True)
    working: dict[str, Any] = {}
    token = bind_durable_history(DurableHistoryBinding(state_provider=owner))
    try:
        await provider.get_messages("session", state=working)
        provider.flush(working)
        expected = _expected_removal(raw, {"completed", "result"}) if excluded else raw
        assert _snapshot(owner.state.to_dict()) == _snapshot(expected)
        assert [message.message_id for message in working[WORKING_BUFFER_KEY]] == (
            ["pending", "current"] if excluded else ["pending", "completed", "result", "current"]
        )
    finally:
        unbind_durable_history(token)
    await _assert_replay(
        owner.state,
        [pending, current] if excluded else [pending, completed, result, current],
        included=[current] if excluded else None,
    )
