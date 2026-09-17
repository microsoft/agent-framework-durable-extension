# Copyright (c) Microsoft. All rights reserved.

"""Shared completion authority, operation immutability, and inert profile boundaries."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Agent, AgentResponse, Content, ContextProvider, Message
from jsonschema import Draft202012Validator, FormatChecker
from test_history_pipeline_revision import ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState
from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask import _shared_state_validation as validation_module
from agent_framework_durabletask._durable_agent_state import DurableAgentStateData
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._response_utils import (
    _constructor_fields,
    invocation_outcome,
    is_terminal_agent_response,
    load_agent_response,
    serialize_agent_response,
)
from agent_framework_durabletask._shared_state_validation import validate_shared_data, validate_shared_state

NOW = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)
COMPLETED = (NOW - timedelta(hours=1)).isoformat()
DEADLINE = NOW + timedelta(hours=1)
OPAQUE = {"flag": False, "zero": 0, "nested": [None, False, 0, 0.0, "", [], {}]}
ERROR = {"code": "provider_failure", "message": "The original invocation failed.", "details": OPAQUE}
PROFILE_CASES = ("response-fields-null", "message-fields-null", "content-fields-null", "continuation-json-null")
MUTATIONS = (
    "delete-live-result-and-receipt",
    "delete-unavailable-receipt",
    "replace-response",
    "change-both-completed-at",
    "change-both-expiries",
    "change-both-outcomes-and-error",
    "response-false-to-zero",
    "add-expiry-to-no-expiry",
    "remove-both-expiries",
)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, allow_nan=False)


def _same(actual: Any, expected: Any) -> None:
    # Ordinary Python equality would hide a nested false-to-zero rewrite.
    assert _json(actual) == _json(expected)


@pytest.fixture
def validator() -> Draft202012Validator:
    path = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"
    return Draft202012Validator(json.loads(path.read_text(encoding="utf-8")), format_checker=FormatChecker())


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[datetime], None]:
    class Clock(datetime):
        instant = NOW

        @classmethod
        def now(cls, tz: Any = None) -> Clock:
            assert tz is not None
            return cls.fromtimestamp(cls.instant.timestamp(), tz)

    def set_time(instant: datetime) -> None:
        Clock.instant = instant

    monkeypatch.setattr(state_module, "datetime", Clock)
    monkeypatch.setattr(validation_module, "datetime", Clock)
    return set_time


def _response(text: str) -> dict[str, Any]:
    return {
        "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": text}]}],
        "value": deepcopy(OPAQUE),
        "extensionData": {"provider": deepcopy(OPAQUE)},
    }


def _raw(*, expiry: bool = False, unavailable: bool = False, second: bool = False) -> dict[str, Any]:
    data: dict[str, Any] = {
        "conversationHistory": [],
        "terminalResults": {},
        "completionReceipts": {},
        "expirationTimeUtc": None,
        "future": deepcopy(OPAQUE),
    }
    for key in ("A", "B") if second else ("A",):
        common: dict[str, Any] = {"correlationId": key, "outcome": "succeeded", "completedAt": COMPLETED}
        if key == "A" and expiry:
            common["resultExpiresAt"] = DEADLINE.isoformat()
        data["terminalResults"][key] = {**common, "response": _response(f"original-{key}")}
        data["completionReceipts"][key] = {**common, "resultState": "available", "future": deepcopy(OPAQUE)}
    if unavailable:
        del data["terminalResults"]["A"]
        receipt = data["completionReceipts"]["A"]
        receipt.pop("resultExpiresAt", None)
        receipt.update(resultState="unavailable", resultUnavailableAt=NOW.isoformat())
    return {"schemaVersion": "2.0.0", "data": data, "future": deepcopy(OPAQUE)}


def _valid(raw: dict[str, Any], validator: Draft202012Validator) -> None:
    before = _json(raw)
    validator.validate(raw)
    validate_shared_state(raw)
    validate_shared_data(raw["data"])
    assert _json(raw) == before


def _collision(raw: dict[str, Any], case: str) -> str:
    result = raw["data"]["terminalResults"]["A"]
    response = result["response"]
    response["extensionData"].update(durable_outcome="succeeded")
    if case == "success-accepted-metadata":
        response["extensionData"]["durable_status"] = "accepted"
    elif case == "failed-expired-error-code":
        response["messages"][0]["contents"].append({
            "$type": "error",
            "errorCode": "response_expired",
            "message": "Provider error, not a delivery receipt.",
        })
    else:
        response["extensionData"]["durable_status"] = "already_completed"
    outcome = "failed" if case.startswith("failed-") else "succeeded"
    result["outcome"] = raw["data"]["completionReceipts"]["A"]["outcome"] = outcome
    if outcome == "failed":
        result["error"] = deepcopy(ERROR)
    return outcome


def _available(response: AgentResponse[Any], outcome: str, text: str = "original-A") -> None:
    assert response.text == text
    assert invocation_outcome(response) == outcome
    assert is_terminal_agent_response(response) is (outcome == "failed")
    assert response.additional_properties.get("durable_status") not in ("accepted", "already_completed")
    _same(response.value, OPAQUE)
    _same(response.additional_properties["provider"], OPAQUE)


def _unavailable(response: AgentResponse[Any] | None, outcome: str = "succeeded") -> None:
    assert response is not None
    assert response.additional_properties["durable_status"] == "already_completed"
    assert response.additional_properties["durable_outcome"] == outcome
    assert invocation_outcome(response) == outcome
    assert is_terminal_agent_response(response)
    assert "original-A" not in response.text
    assert response.value is None
    assert any(
        content.type == "error" and content.error_code == "response_expired"
        for message in response.messages
        for content in message.contents
    )


@pytest.mark.parametrize("expiry", [False, True], ids=["no-expiry", "live-expiry"])
@pytest.mark.parametrize(
    "case",
    [
        "success-completed-metadata",
        "success-accepted-metadata",
        "failed-expired-error-code",
        "failed-canonical-error-only",
    ],
)
async def test_available_canonical_result_overrides_response_delivery_hints(
    case: str, expiry: bool, validator: Draft202012Validator
) -> None:
    raw = _raw(expiry=expiry)
    outcome = _collision(raw, case)
    before = deepcopy(raw)
    _valid(raw, validator)
    provider = JsonStateProvider(raw)
    client = ToolChatClient(tool_calls=False, fail=True)
    entity = AgentEntity(Agent(client=client), state_provider=provider)

    direct = entity.state.try_get_agent_response("A")
    assert direct is not None
    _available(direct, outcome)
    # Core delivery serialization must not resurrect the untrusted status hints.
    _available(load_agent_response(json.loads(_json(serialize_agent_response(direct)))), outcome)
    duplicate = await entity.run({"message": "must not execute A", "correlationId": "A"})
    _available(duplicate, outcome)
    assert client.received_messages == [] and provider.writes == 0
    _same(entity.state.to_dict(), before)
    _same(provider.raw, before)
    _same(raw, before)
    if outcome == "failed":
        _same(provider.raw["data"]["terminalResults"]["A"]["error"], ERROR)


@pytest.mark.parametrize("case", ["success-completed-metadata", "failed-expired-error-code"])
def test_receipt_and_deadline_control_unavailability_not_response_metadata(
    case: str, clock: Callable[[datetime], None], validator: Draft202012Validator
) -> None:
    raw = _raw(expiry=True)
    outcome = _collision(raw, case)
    before = deepcopy(raw)
    _valid(raw, validator)
    state = DurableAgentState.from_dict(raw)
    clock(DEADLINE)
    _unavailable(state.try_get_agent_response("A"), outcome)
    _same(state.to_dict(), before)
    state.expire_responses(now=DEADLINE)
    expected = deepcopy(before)
    del expected["data"]["terminalResults"]["A"]
    expected["data"]["completionReceipts"]["A"].update(
        resultState="unavailable", resultUnavailableAt=DEADLINE.isoformat()
    )
    _same(state.to_dict(), expected)
    _unavailable(DurableAgentState.from_dict(expected).try_get_agent_response("A"), outcome)
    _same(raw, before)


class _PriorCompletionHook(ContextProvider):
    def __init__(
        self,
        mutation: str,
        phase: str,
        *,
        direct_persist: bool = False,
        clock: Callable[[datetime], None] | None = None,
    ) -> None:
        super().__init__("prior-completion-review")
        self.mutation = mutation
        self.phase = phase
        self.direct_persist = direct_persist
        self.clock = clock
        self.applied = False
        self.direct_error: ValueError | None = None
        self.seen: list[str] = []
        self.before: dict[str, Any] | None = None
        self.after: dict[str, Any] | None = None

    async def before_run(self, **kwargs: Any) -> None:
        self._apply("before")

    async def after_run(self, **kwargs: Any) -> None:
        self._apply("after")

    def _apply(self, phase: str) -> None:
        self.seen.append(phase)
        if phase != self.phase or self.applied:
            return
        binding = current_durable_history_binding()
        assert binding is not None and binding.correlation_id == "B"
        state = binding.state_provider.state
        results, receipts = state.data.response_mailbox, state.data.completed_correlations
        self.before = deepcopy({"terminalResults": results, "completionReceipts": receipts})
        if self.mutation == "delete-live-result-and-receipt":
            del results["A"]
            del receipts["A"]
        elif self.mutation == "delete-unavailable-receipt":
            assert "A" not in results and receipts["A"]["resultState"] == "unavailable"
            del receipts["A"]
        elif self.mutation == "replace-response":
            results["A"]["response"] = _response("replacement-A")
        elif self.mutation == "change-both-completed-at":
            for record in (results["A"], receipts["A"]):
                record["completedAt"] = (NOW - timedelta(minutes=30)).isoformat()
        elif self.mutation in ("change-both-expiries", "add-expiry-to-no-expiry"):
            for record in (results["A"], receipts["A"]):
                if self.mutation == "add-expiry-to-no-expiry":
                    assert "resultExpiresAt" not in record
                record["resultExpiresAt"] = (DEADLINE + timedelta(hours=1)).isoformat()
        elif self.mutation == "remove-both-expiries":
            for record in (results["A"], receipts["A"]):
                del record["resultExpiresAt"]
        elif self.mutation == "change-both-outcomes-and-error":
            results["A"]["outcome"] = receipts["A"]["outcome"] = "failed"
            results["A"]["error"] = deepcopy(ERROR)
        elif self.mutation == "response-false-to-zero":
            assert results["A"]["response"]["value"]["flag"] is False
            results["A"]["response"]["value"]["flag"] = 0
        elif self.mutation == "cleanup":
            assert self.clock is not None
            self.clock(DEADLINE)
            state.expire_responses(now=DEADLINE)
        else:
            raise AssertionError(f"Uncovered mutation: {self.mutation}")
        self.applied = True
        self.after = deepcopy({"terminalResults": results, "completionReceipts": receipts})
        # Every attack remains a valid individual snapshot. Rejection needs prior-state authority.
        validate_shared_data({"conversationHistory": [], **self.after})
        if self.direct_persist:
            try:
                binding.state_provider.persist_state()
            except ValueError as exc:
                self.direct_error = exc
                raise


@pytest.mark.parametrize("mutation", MUTATIONS)
@pytest.mark.parametrize("phase", ["before", "after"])
@pytest.mark.parametrize("direct_persist", [False, True], ids=["entity-commit", "bound-provider-persist"])
async def test_provider_hook_cannot_rewrite_prior_completion_even_with_consistent_maps(
    mutation: str, phase: str, direct_persist: bool, monkeypatch: pytest.MonkeyPatch, validator: Draft202012Validator
) -> None:
    unavailable = mutation == "delete-unavailable-receipt"
    raw = _raw(expiry=mutation != "add-expiry-to-no-expiry", unavailable=unavailable)
    before = deepcopy(raw)
    _valid(raw, validator)
    provider = JsonStateProvider(raw)
    backend = Mock(wraps=provider._set_state_dict)
    monkeypatch.setattr(provider, "_set_state_dict", backend)
    hook = _PriorCompletionHook(mutation, phase, direct_persist=direct_persist)
    client = ToolChatClient(tool_calls=False)
    entity = AgentEntity(Agent(client=client, context_providers=[hook]), state_provider=provider)
    _same(entity.state.to_dict(), before)

    with pytest.raises(ValueError):
        await entity.run({"message": "execute B without rewriting A", "correlationId": "B"})

    assert hook.applied and phase in hook.seen
    assert hook.before is not None and hook.after is not None
    _valid({"schemaVersion": "2.0.0", "data": {"conversationHistory": [], **hook.after}}, validator)
    assert _json(hook.after) != _json(hook.before)
    if mutation == "response-false-to-zero":
        assert hook.after == hook.before, "This case must discriminate JSON types rather than Python equality."
    if direct_persist:
        assert isinstance(hook.direct_error, ValueError), "The bound provider must reject before its backend call."
    backend.assert_not_called()
    assert provider.writes == 0
    _same(provider.raw, before)
    _same(entity.state.to_dict(), before)
    assert entity.state.try_get_agent_response("B") is None
    assert current_durable_history_binding() is None

    cold = JsonStateProvider(provider.raw)
    duplicate_client = ToolChatClient(tool_calls=False, fail=True)
    duplicate = await AgentEntity(Agent(client=duplicate_client), state_provider=cold).run({
        "message": "A must not be re-executed after B rollback",
        "correlationId": "A",
    })
    if unavailable:
        _unavailable(duplicate)
    else:
        _available(duplicate, "succeeded")
    assert duplicate_client.received_messages == [] and cold.writes == 0
    _same(cold.raw, before)
    _same(cold.state.to_dict(), before)
    _same(raw, before)


@pytest.mark.parametrize("phase", ["before", "after"])
@pytest.mark.parametrize("expiry", [False, True], ids=["no-expiry-preserved", "due-result-removed"])
async def test_legitimate_expiry_during_another_turn_preserves_all_other_prior_fields(
    phase: str, expiry: bool, clock: Callable[[datetime], None], validator: Draft202012Validator
) -> None:
    raw = _raw(expiry=expiry)
    before = deepcopy(raw)
    provider = JsonStateProvider(raw)
    hook = _PriorCompletionHook("cleanup", phase, clock=clock)
    client = ToolChatClient(tool_calls=False)
    entity = AgentEntity(Agent(client=client, context_providers=[hook]), state_provider=provider)

    response = await entity.run({"message": "B crosses A's delivery deadline", "correlationId": "B"})

    assert hook.applied and hook.before is not None and hook.after is not None
    expected = deepcopy(hook.before)
    if expiry:
        del expected["terminalResults"]["A"]
        expected["completionReceipts"]["A"].update(resultState="unavailable", resultUnavailableAt=DEADLINE.isoformat())
    _same(hook.after, expected)
    assert response.text == "answer-1" and invocation_outcome(response) == "succeeded"
    assert len(client.received_messages) == 1 and provider.writes == 1
    _valid(provider.raw, validator)
    _same(provider.raw["data"]["completionReceipts"]["A"], expected["completionReceipts"]["A"])
    assert provider.raw["data"]["completionReceipts"]["B"]["outcome"] == "succeeded"
    if expiry:
        assert "A" not in provider.raw["data"]["terminalResults"]
    else:
        _same(provider.raw["data"]["terminalResults"]["A"], before["data"]["terminalResults"]["A"])
        assert "resultExpiresAt" not in provider.raw["data"]["completionReceipts"]["A"]
    cold = JsonStateProvider(provider.raw)
    duplicate_client = ToolChatClient(tool_calls=False, fail=True)
    duplicate_entity = AgentEntity(Agent(client=duplicate_client), state_provider=cold)
    duplicate = await duplicate_entity.run({"message": "do not execute A", "correlationId": "A"})
    if expiry:
        _unavailable(duplicate)
    else:
        _available(duplicate, "succeeded")
    assert (await duplicate_entity.run({"message": "do not execute B", "correlationId": "B"})).text == "answer-1"
    assert duplicate_client.received_messages == [] and cold.writes == 0
    _same(cold.raw, provider.raw)
    _same(raw, before)


def _profile_raw(profile: str, *, expiry: bool = False) -> dict[str, Any]:
    raw = _raw(expiry=expiry, second=True)
    response = raw["data"]["terminalResults"]["A"]["response"]
    if profile == "continuation-json-null":
        response.update(
            continuationToken="bnVsbA==",
            pythonContinuationEncoding={
                "profile": "agent-framework-python.continuation",
                "version": 1,
                "format": "json",
            },
        )
    else:
        targets = {
            "response-fields-null": response,
            "message-fields-null": response["messages"][0],
            "content-fields-null": response["messages"][0]["contents"][0],
        }
        targets[profile]["pythonCoreFields"] = {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": None,
        }
    return raw


@contextmanager
def _forbid_core(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    # Warm reflection before patching constructors, so a failed baseline cannot poison its cache.
    for cls in (AgentResponse, Content, Message):
        _constructor_fields(cls)
    with monkeypatch.context() as patch:
        forbidden: list[Mock] = []
        for cls in (AgentResponse, Content, Message):
            for method in ("__init__", "from_dict", "to_dict"):
                mocked = Mock(side_effect=AssertionError(f"Inert shared state called Core {cls.__name__}.{method}"))
                patch.setattr(cls, method, mocked)
                forbidden.append(mocked)
        yield
        for mocked in forbidden:
            mocked.assert_not_called()


@pytest.mark.parametrize("profile", PROFILE_CASES)
def test_recognized_malformed_profiles_remain_inert_in_root_and_data_roundtrips(
    profile: str, monkeypatch: pytest.MonkeyPatch, validator: Draft202012Validator
) -> None:
    raw = _profile_raw(profile)
    before = deepcopy(raw)
    with _forbid_core(monkeypatch):
        _valid(raw, validator)
        state = DurableAgentState.from_dict(raw)
        data = DurableAgentStateData.from_dict(raw["data"])
        _same(state.to_dict(), before)
        _same(data.to_dict(), before["data"])
        _same(json.loads(state.to_json()), before)
        _same(DurableAgentState.from_json(_json(raw)).to_dict(), before)
        exported = state.to_dict()
        exported["data"]["terminalResults"]["A"]["response"]["value"]["flag"] = 0
        _same(state.to_dict(), before)
    _same(raw, before)


@pytest.mark.parametrize("profile", PROFILE_CASES)
async def test_valid_b_lookup_and_entity_duplicate_do_not_project_malformed_a(
    profile: str, validator: Draft202012Validator
) -> None:
    raw = _profile_raw(profile)
    before = deepcopy(raw)
    _valid(raw, validator)
    provider = JsonStateProvider(raw)
    client = ToolChatClient(tool_calls=False, fail=True)
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    direct = entity.state.try_get_agent_response("B")
    assert direct is not None
    _available(direct, "succeeded", "original-B")
    duplicate = await entity.run({"message": "B already completed", "correlationId": "B"})
    _available(duplicate, "succeeded", "original-B")
    assert client.received_messages == [] and provider.writes == 0
    _same(provider.raw, before)
    _same(entity.state.to_dict(), before)
    _same(raw, before)


@pytest.mark.parametrize("profile", PROFILE_CASES)
def test_available_malformed_profile_fails_only_when_its_result_is_selected(
    profile: str, monkeypatch: pytest.MonkeyPatch, validator: Draft202012Validator
) -> None:
    raw = _profile_raw(profile)
    before = deepcopy(raw)
    _valid(raw, validator)
    with _forbid_core(monkeypatch):
        state = DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError, match="JSON object|recognized Python continuation|profile"):
        state.try_get_agent_response("A")
    _same(state.to_dict(), before)
    other = state.try_get_agent_response("B")
    assert other is not None
    _available(other, "succeeded", "original-B")
    _same(raw, before)


@pytest.mark.parametrize("profile", PROFILE_CASES)
def test_expired_malformed_profile_can_be_reported_and_removed_without_payload_construction(
    profile: str, monkeypatch: pytest.MonkeyPatch, clock: Callable[[datetime], None], validator: Draft202012Validator
) -> None:
    raw = _profile_raw(profile, expiry=True)
    before = deepcopy(raw)
    _valid(raw, validator)
    with _forbid_core(monkeypatch):
        state = DurableAgentState.from_dict(raw)
    clock(DEADLINE)
    with monkeypatch.context() as patch:
        loader = Mock(side_effect=AssertionError("An expired lookup must not construct a stored response."))
        patch.setattr(state_module, "load_terminal_response", loader)
        # The synthetic unavailable reply may construct Core objects, but A's payload must not.
        _unavailable(state.try_get_agent_response("A"))
        loader.assert_not_called()
    expected = deepcopy(before)
    del expected["data"]["terminalResults"]["A"]
    expected["data"]["completionReceipts"]["A"].update(
        resultState="unavailable", resultUnavailableAt=DEADLINE.isoformat()
    )
    with _forbid_core(monkeypatch):
        _same(state.to_dict(), before)
        state.expire_responses(now=DEADLINE)
        _same(state.to_dict(), expected)
        state.expire_responses(now=DEADLINE + timedelta(days=1))
        _same(state.to_dict(), expected)
        restored = DurableAgentState.from_dict(expected)
    _unavailable(restored.try_get_agent_response("A"))
    other = restored.try_get_agent_response("B")
    assert other is not None
    _available(other, "succeeded", "original-B")
    _same(raw, before)


@pytest.mark.parametrize("role", ["user", "assistant", "system", "developer", "tool"])
@pytest.mark.parametrize("wrapped", [False, True], ids=["known-error", "opaque-error-shaped-business-data"])
def test_success_failure_evidence_is_read_from_wire_without_constructing_any_core_response(
    role: str, wrapped: bool, monkeypatch: pytest.MonkeyPatch, validator: Draft202012Validator
) -> None:
    raw = _profile_raw("response-fields-null", expiry=True)
    response = raw["data"]["terminalResults"]["A"]["response"]
    error = {"$type": "error", "message": "Affirmative original failure", "errorCode": "provider_failure"}
    response["messages"] = [{"role": role, "contents": [{"$type": "unknown", "content": error} if wrapped else error]}]
    response["extensionData"].update(durable_status="accepted", durable_outcome="succeeded")
    before = deepcopy(raw)
    _valid(raw, validator)
    with _forbid_core(monkeypatch):
        if role != "tool" and not wrapped:
            readers = (
                lambda: DurableAgentState.from_dict(raw),
                lambda: DurableAgentStateData.from_dict(raw["data"]),
            )
            for read in readers:
                with pytest.raises(ValueError, match="conflict|failure|outcome"):
                    read()
        else:
            _same(DurableAgentState.from_dict(raw).to_dict(), before)
            _same(DurableAgentStateData.from_dict(raw["data"]).to_dict(), before["data"])
    _same(raw, before)


@pytest.mark.parametrize(
    "bad_content",
    [{"$type": "text", "text": None}, {"$type": "error", "message": []}, {"$type": "error", "errorCode": False}],
)
def test_inert_profiles_do_not_relax_validation_of_malformed_known_wire_fields(
    bad_content: dict[str, Any], monkeypatch: pytest.MonkeyPatch, validator: Draft202012Validator
) -> None:
    raw = _profile_raw("response-fields-null")
    raw["data"]["terminalResults"]["A"]["response"]["messages"][0]["contents"] = [deepcopy(bad_content)]
    before = deepcopy(raw)
    assert not validator.is_valid(raw)
    with _forbid_core(monkeypatch):
        with pytest.raises(ValueError):
            validate_shared_state(raw)
        with pytest.raises(ValueError):
            validate_shared_data(raw["data"])
        with pytest.raises(ValueError):
            DurableAgentState.from_dict(raw)
        with pytest.raises(ValueError):
            DurableAgentStateData.from_dict(raw["data"])
    _same(raw, before)
