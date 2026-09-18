# Copyright (c) Microsoft. All rights reserved.

"""Private JSON delivery operations, independent of mutable state and host activation."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, Content, Message

from agent_framework_durabletask import _delivery_state as delivery
from agent_framework_durabletask import _shared_response as codec
from agent_framework_durabletask import _shared_state_validation as validation
from agent_framework_durabletask import _state_reader as reader
from agent_framework_durabletask._shared_response import load_terminal_response, serialize_terminal_response
from agent_framework_durabletask._shared_state_validation import validate_shared_state

NOW = datetime(2026, 9, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)
COMPLETED = "2026-09-17T10:00:00.000000000000000000000001Z"
DEADLINE = "2026-09-17T17:30:00.123456+05:30"
ABSENT = object()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, allow_nan=False)


def _containers(value: Any) -> set[int]:
    if isinstance(value, dict):
        return {id(value)}.union(*(_containers(item) for item in value.values()))
    if isinstance(value, list):
        return {id(value)}.union(*(_containers(item) for item in value))
    return set()


def _same_detached(actual: Any, expected: Any, source: Any) -> None:
    assert _json(actual) == _json(expected)
    assert _containers(actual).isdisjoint(_containers(source))


def _root() -> dict[str, Any]:
    # Literal roots do not depend on a production state model or response producer.
    return {
        "schemaVersion": "2.0.0",
        "extensionData": {"root": [None, False, 0, 0.0]},
        "unknownRoot": {"$runtimeType": "inert.Type", "items": [[], {}, "e\u0301😀", 2**80]},
        "data": {
            "conversationHistory": [
                {"$type": "request", "correlationId": "history-only", "messages": [{"role": "user"}]},
                {
                    "$type": "response",
                    "correlationId": "history-only",
                    "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "not a mailbox"}]}],
                    "unknownHistory": [False, 0],
                },
                {"$type": "compaction", "messages": [], "unknownCompaction": None},
            ],
            "session": {"type": "not_a_constructor", "state": {"opaque": [False, 0, -0.0, None]}},
            "historyBinding": {"profile": "foreign", "version": 999, "owner": [None, False]},
            "ingestedPositions": {"producer": 3},
            "expirationTimeUtc": None,
            "truncation": {
                "evictedMessageCount": 2,
                "firstEvictedAt": "2026-09-17T08:00:00Z",
                "lastEvictedAt": "2026-09-17T09:00:00Z",
                "unknownTruncation": [False, 0],
            },
            "extensionData": {"data": {"keep": None}},
            "unknownData": ["", [], {}, False, 0, 0.0],
            "terminalResults": {
                "c": {
                    "correlationId": "c",
                    "outcome": "succeeded",
                    "completedAt": COMPLETED,
                    "resultExpiresAt": DEADLINE,
                    "unknownResult": {"keep": [False, 0, 0.0]},
                    "response": {
                        "messages": [
                            {
                                "role": "assistant",
                                "authorName": "provider",
                                "messageId": "message-c",
                                "extensionData": {"message": [None]},
                                "unknownMessage": [False, 0],
                                "contents": [
                                    {"$type": "text", "text": "retained answer", "unknownContent": {"keep": None}},
                                    {"$type": "unknown", "content": {"$runtimeType": "inert.Content", "flag": False}},
                                ],
                            }
                        ],
                        "value": {"nested": [None, False, 0, -0.0, "", [], {}]},
                        "responseId": "response-c",
                        "agentId": "agent-c",
                        "createdAt": "2026-09-17T09:59:59.123456789Z",
                        "finishReason": "stop",
                        "continuationToken": "AQID",
                        "usage": {"inputTokenCount": 2**70, "extensionData": {"provider": None}, "unknownUsage": False},
                        "extensionData": {"provider": {"flag": False}},
                        "unknownResponse": {"keep": [False, 0]},
                    },
                }
            },
            "completionReceipts": {
                "c": {
                    "correlationId": "c",
                    "outcome": "succeeded",
                    "completedAt": COMPLETED,
                    "resultExpiresAt": DEADLINE,
                    "resultState": "available",
                    "unknownReceipt": {"keep": [False, 0, 0.0]},
                }
            },
        },
    }


def _result(raw: dict[str, Any]) -> dict[str, Any]:
    return raw["data"]["terminalResults"]["c"]


def _receipt(raw: dict[str, Any]) -> dict[str, Any]:
    return raw["data"]["completionReceipts"]["c"]


def _deadline(raw: dict[str, Any], value: Any) -> None:
    for record in (_result(raw), _receipt(raw)):
        if value is ABSENT:
            record.pop("resultExpiresAt", None)
        else:
            record["resultExpiresAt"] = value


def _new_payload() -> dict[str, Any]:
    return {
        "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "new answer"}]}],
        "createdAt": "2026-09-17T09:00:00.123456789+01:00",
        "value": None,
        "extensionData": {"provider": {"flag": False, "zero": 0}},
        "unknownResponse": {"nested": [None, False, 0, -0.0, "", [], {}]},
    }


def _expired(response: AgentResponse | None, outcome: str = "succeeded") -> None:
    assert response is not None
    assert response.additional_properties == {
        "durable_status": "already_completed",
        "correlation_id": "c",
        "durable_outcome": outcome,
    }
    assert response.value is None
    assert len(response.messages) == 1
    assert response.messages[0].contents[0].error_code == "response_expired"
    assert "retained answer" not in response.text


def _call(operation: str, raw: Any, *, now: Any = NOW, correlation: str = "new") -> Any:
    if operation == "stage":
        return delivery.stage_response(
            raw, correlation, AgentResponse(messages=[]), delivery_window_seconds=60, now=now
        )
    if operation == "expiry":
        return delivery.stage_expiry(raw, now=now)
    if operation == "lookup":
        return delivery.lookup_response(raw, "c", now=now)
    assert operation == "validate"
    return delivery.validate_delivery_state(raw)


@pytest.mark.parametrize("kind", ["success", "non-tool-error", "status-error", "tool-error"])
def test_stage_response_adds_one_canonical_pair_without_rewriting_other_json(kind: str) -> None:
    raw = _root()
    before = _json(raw)
    payload = _new_payload()
    failed = kind in ("non-tool-error", "status-error")
    if kind in ("non-tool-error", "tool-error"):
        payload["messages"].append({
            "role": "tool" if kind == "tool-error" else "system",
            "contents": [{"$type": "error", "errorCode": "provider_failure", "message": "Provider failed."}],
        })
    if kind == "status-error":
        payload["extensionData"]["durable_status"] = "error"
    response = load_terminal_response(payload)
    observed = _json(serialize_terminal_response(response))

    staged = delivery.stage_response(raw, "new", response, delivery_window_seconds=60, now=NOW)

    expected = deepcopy(raw)
    common = {
        "correlationId": "new",
        "outcome": "failed" if failed else "succeeded",
        "completedAt": "2026-09-17T12:00:00.123456+00:00",
        "resultExpiresAt": "2026-09-17T12:01:00.123456+00:00",
    }
    expected["data"]["terminalResults"]["new"] = {**common, "response": payload}
    if failed:
        expected["data"]["terminalResults"]["new"]["error"] = (
            {"code": "provider_failure", "message": "Provider failed."}
            if kind == "non-tool-error"
            else {"code": "agent_error", "message": "The agent invocation failed."}
        )
    expected["data"]["completionReceipts"]["new"] = {**common, "resultState": "available"}
    _same_detached(staged, expected, raw)
    validate_shared_state(staged)
    assert _json(raw) == before
    assert _json(serialize_terminal_response(response)) == observed
    response.additional_properties["provider"]["flag"] = True
    assert _json(staged) == _json(expected)


@pytest.mark.parametrize("status", ["accepted", "already_completed"])
def test_non_result_delivery_status_cannot_create_a_completion(status: str) -> None:
    raw = _root()
    before = _json(raw)
    response = AgentResponse(messages=[], additional_properties={"durable_status": status})
    with pytest.raises(ValueError):
        delivery.stage_response(raw, "new", response, delivery_window_seconds=60, now=NOW)
    assert _json(raw) == before
    assert response.additional_properties == {"durable_status": status}


@pytest.mark.parametrize(
    "value",
    [pytest.param(ABSENT, id="absent"), None, False, 0, -0.0, "", [], {}, {"nested": [False, 0, None, 2**80]}],
)
def test_staging_and_lookup_preserve_value_presence_and_json_types(value: Any) -> None:
    raw = _root()
    payload = _new_payload()
    if value is ABSENT:
        del payload["value"]
    else:
        payload["value"] = value
    staged = delivery.stage_response(raw, "new", load_terminal_response(payload), delivery_window_seconds=1, now=NOW)
    stored = staged["data"]["terminalResults"]["new"]["response"]
    assert _json(stored) == _json(payload)
    response = delivery.lookup_response(staged, "new", now=NOW)
    assert response is not None
    assert _json(serialize_terminal_response(response)) == _json(payload)
    if value is not ABSENT:
        assert type(response.value) is type(value)
        assert _json(response.value) == _json(value)


class _DoNotInspect:
    def __getattribute__(self, name: str) -> Any:
        raise AssertionError(f"Duplicate inspected {name}")

    def __bool__(self) -> bool:
        raise AssertionError("Duplicate inspected truthiness")

    def __repr__(self) -> str:
        raise AssertionError("Duplicate formatted an unused argument")


@pytest.mark.parametrize("kind", ["available", "logically-expired", "unavailable"])
def test_duplicate_is_a_detached_noop_before_response_window_or_clock_inspection(kind: str) -> None:
    raw = _root()
    if kind == "available":
        _deadline(raw, "9999-12-31T23:59:59Z")
    elif kind == "unavailable":
        del raw["data"]["terminalResults"]["c"]
        _receipt(raw).update(resultState="unavailable", resultUnavailableAt=NOW.isoformat())
    before = deepcopy(raw)
    poison: Any = _DoNotInspect()

    staged = delivery.stage_response(raw, "c", poison, delivery_window_seconds=poison, now=poison)

    _same_detached(staged, before, raw)
    assert _json(raw) == _json(before)
    if kind != "available":
        _expired(delivery.lookup_response(staged, "c", now=NOW))


@pytest.mark.parametrize("operation", ["stage", "duplicate", "expiry", "lookup", "validate"])
def test_malformed_later_record_rejects_the_whole_operation_without_partial_mutation(operation: str) -> None:
    raw = _root()  # The first result is due and could be removed by an eager loop.
    bad = deepcopy(_result(raw))
    bad["correlationId"] = "later"
    bad["response"] = {"messages": [{"role": "not-a-role"}]}
    raw["data"]["terminalResults"]["later"] = bad
    raw["data"]["completionReceipts"]["later"] = {**deepcopy(_receipt(raw)), "correlationId": "later"}
    before, identities = _json(raw), _containers(raw)
    with pytest.raises(ValueError):
        if operation in ("stage", "duplicate"):
            poison: Any = _DoNotInspect()
            delivery.stage_response(
                raw, "c" if operation == "duplicate" else "new", poison, delivery_window_seconds=60, now=NOW
            )
        else:
            _call(operation, raw)
    assert _json(raw) == before
    assert _containers(raw) == identities


@pytest.mark.parametrize("seconds", [0, -1, True, False, 1.0, 1.5, None, "60", timedelta(seconds=60)])
def test_new_completion_requires_positive_integer_seconds(seconds: Any) -> None:
    raw = _root()
    before = _json(raw)
    with pytest.raises(ValueError):
        delivery.stage_response(raw, "new", AgentResponse(messages=[]), delivery_window_seconds=seconds, now=NOW)
    assert _json(raw) == before


@pytest.mark.parametrize(
    ("now", "seconds"),
    [
        pytest.param(NOW, 10**1000, id="unrepresentable-duration"),
        pytest.param(datetime.max.replace(tzinfo=timezone.utc), 1, id="unrepresentable-deadline"),
    ],
)
def test_positive_window_overflow_cannot_partially_stage(now: datetime, seconds: int) -> None:
    raw = _root()
    before = _json(raw)
    with pytest.raises((ValueError, OverflowError)):
        delivery.stage_response(raw, "new", AgentResponse(messages=[]), delivery_window_seconds=seconds, now=now)
    assert _json(raw) == before


@pytest.mark.parametrize("now", [NOW.replace(tzinfo=None), True, False, 0, "2026-09-17T12:00:00Z"])
def test_supplied_clock_must_be_an_aware_datetime(now: Any) -> None:
    raw = _root()
    before = _json(raw)
    for operation in ("stage", "expiry", "lookup"):
        with pytest.raises(ValueError):
            _call(operation, raw, now=now)
        assert _json(raw) == before


@pytest.mark.parametrize("offset", [timedelta(hours=5, minutes=30), timedelta(hours=-7)])
def test_stage_normalizes_offset_clock_before_elapsed_window_arithmetic(offset: timedelta) -> None:
    now = NOW.astimezone(timezone(offset))
    staged = delivery.stage_response(_root(), "new", AgentResponse(messages=[]), delivery_window_seconds=60, now=now)
    result = staged["data"]["terminalResults"]["new"]
    assert result["completedAt"] == NOW.isoformat()
    assert result["resultExpiresAt"] == "2026-09-17T12:01:00.123456+00:00"


class _SpringForward(tzinfo):
    """One synthetic forward transition, requiring neither zoneinfo data nor skips."""

    def utcoffset(self, dt: datetime | None) -> timedelta:
        if dt is not None and dt.replace(tzinfo=None) >= datetime(2026, 3, 29, 2):
            return timedelta(hours=1)
        return timedelta(0)

    def dst(self, dt: datetime | None) -> timedelta:
        return self.utcoffset(dt)

    def tzname(self, dt: datetime | None) -> str:
        return "synthetic-spring-forward"

    def fromutc(self, dt: datetime) -> datetime:
        assert dt.tzinfo is self
        return dt + (timedelta(hours=1) if dt.replace(tzinfo=None) >= datetime(2026, 3, 29, 1) else timedelta(0))


def test_delivery_window_is_elapsed_utc_seconds_not_dst_wall_time() -> None:
    local = datetime(2026, 3, 29, 0, 30, tzinfo=_SpringForward())
    assert local.utcoffset() == timedelta(0)
    assert (local + timedelta(hours=2)).utcoffset() == timedelta(hours=1)
    raw = _root()
    staged = delivery.stage_response(raw, "new", AgentResponse(messages=[]), delivery_window_seconds=7200, now=local)
    result = staged["data"]["terminalResults"]["new"]
    assert result["completedAt"] == "2026-03-29T00:30:00+00:00"
    assert result["resultExpiresAt"] == "2026-03-29T02:30:00+00:00"


def test_none_clocks_use_aware_utc_for_staging_lookup_and_transition(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []

    class ClockMeta(type):
        def __instancecheck__(cls, instance: Any) -> bool:
            return isinstance(instance, datetime)

    class Clock(datetime, metaclass=ClockMeta):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> Clock:
            calls.append(tz)
            assert tz is timezone.utc
            return cls(2026, 9, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)

    for module in (delivery, validation, reader):
        monkeypatch.setattr(module, "datetime", Clock)
    staged = delivery.stage_response(_root(), "new", AgentResponse(messages=[]), delivery_window_seconds=1, now=None)
    assert staged["data"]["terminalResults"]["new"]["completedAt"] == NOW.isoformat()
    expired, count = delivery.stage_expiry(_root(), now=None)
    assert count == 1
    assert _receipt(expired)["resultUnavailableAt"] == NOW.isoformat()
    _expired(delivery.lookup_response(_root(), "c", now=None))
    validation.validate_completion_transition(_root(), expired, now=None)
    assert len(calls) >= 4


@pytest.mark.parametrize(
    ("deadline", "now", "count"),
    [
        (DEADLINE, NOW - timedelta(microseconds=1), 0),
        (DEADLINE, NOW, 1),
        (DEADLINE, NOW.astimezone(timezone(timedelta(hours=5, minutes=30))), 1),
        ("2026-09-17T05:00:00.123456-07:00", NOW, 1),
        ("2026-09-17T12:00:00.123456000000000000000000000001Z", NOW, 0),
        ("2026-09-17T12:00:00.123456000000000000000000000001Z", NOW.replace(microsecond=123457), 1),
    ],
)
def test_expiry_and_lookup_compare_exact_fractional_instants(deadline: str, now: datetime, count: int) -> None:
    raw = _root()
    _deadline(raw, deadline)
    before = deepcopy(raw)
    expected = deepcopy(raw)
    if count:
        del expected["data"]["terminalResults"]["c"]
        _receipt(expected).update(
            resultState="unavailable", resultUnavailableAt=now.astimezone(timezone.utc).isoformat()
        )
        _expired(delivery.lookup_response(raw, "c", now=now))
    else:
        response = delivery.lookup_response(raw, "c", now=now)
        assert response is not None and response.text == "retained answer"
    staged, removed = delivery.stage_expiry(raw, now=now)
    assert type(removed) is int and removed == count
    _same_detached(staged, expected, raw)
    assert _json(raw) == _json(before)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_expiry_retains_outcome_exact_original_timestamps_and_an_immutable_receipt(outcome: str) -> None:
    raw = _root()
    for record in (_result(raw), _receipt(raw)):
        record["outcome"] = outcome
    if outcome == "failed":
        _result(raw)["error"] = {"code": "failure", "message": "Original failure.", "details": [False, 0]}
    # Time-based expiry must retain a second available record with no deadline.
    other_result, other_receipt = deepcopy(_result(raw)), deepcopy(_receipt(raw))
    for record in (other_result, other_receipt):
        record["correlationId"] = "forever"
        del record["resultExpiresAt"]
    raw["data"]["terminalResults"]["forever"] = other_result
    raw["data"]["completionReceipts"]["forever"] = other_receipt
    before = _json(raw)
    expected = deepcopy(raw)
    del expected["data"]["terminalResults"]["c"]
    _receipt(expected).update(resultState="unavailable", resultUnavailableAt=NOW.isoformat())
    staged, count = delivery.stage_expiry(raw, now=NOW)
    assert count == 1
    _same_detached(staged, expected, raw)
    _expired(delivery.lookup_response(staged, "c", now=NOW + timedelta(days=1)), outcome)
    again, count = delivery.stage_expiry(staged, now=NOW + timedelta(days=1))
    assert count == 0
    _same_detached(again, expected, staged)
    assert _receipt(again)["completedAt"] == COMPLETED
    assert _receipt(again)["resultExpiresAt"] == DEADLINE
    assert _json(raw) == before


@pytest.mark.parametrize("due", [False, True])
def test_validation_and_expiry_never_decode_even_recognized_but_unloadable_profiles(
    monkeypatch: pytest.MonkeyPatch, due: bool
) -> None:
    raw = _root()
    if not due:
        _deadline(raw, ABSENT)
    _result(raw)["response"].update({
        "continuationToken": "bnVsbA==",  # JSON null, not a resumable dictionary.
        "pythonContinuationEncoding": {
            "profile": "agent-framework-python.continuation",
            "version": 1,
            "format": "json",
        },
        "pythonCoreFields": {"profile": "agent-framework-python.core-fields", "version": 1, "fields": []},
    })
    before = _json(raw)
    blocked = Mock(side_effect=AssertionError("Raw-state operation decoded a response"))
    for module in (delivery, codec, reader):
        if hasattr(module, "load_terminal_response"):
            monkeypatch.setattr(module, "load_terminal_response", blocked)
    delivery.validate_delivery_state(raw)
    if due:
        _expired(delivery.lookup_response(raw, "c", now=NOW))
    staged, count = delivery.stage_expiry(raw, now=NOW)
    assert count == int(due)
    assert _json(raw) == before
    if due:
        _expired(delivery.lookup_response(staged, "c", now=NOW))
    else:
        _same_detached(staged, raw, raw)
    blocked.assert_not_called()


@pytest.mark.parametrize("version", [None, False, 2, "1.1.0", "2.0.1", "2.1.0", "3.0.0", "2.0.0+build"])
def test_private_delivery_requires_exact_declared_v2_not_a_legacy_or_future_revision(version: Any) -> None:
    raw = _root()
    raw["schemaVersion"] = version
    if version == "1.1.0":
        raw = {"schemaVersion": version, "data": {"conversationHistory": []}}
    before = _json(raw)
    for operation in ("stage", "expiry", "lookup", "validate"):
        with pytest.raises(ValueError):
            _call(operation, raw)
        assert _json(raw) == before


@pytest.mark.parametrize(
    ("location", "field", "value"),
    [
        ("receipt", "outcome", "pending"),
        ("receipt", "resultState", "future-availability"),
        ("history", "$type", "future-entry"),
        ("message", "role", "future-role"),
        ("content", "$type", "future-content"),
        ("data", "terminalResults", []),
    ],
)
def test_unsupported_discriminators_and_known_field_types_are_not_opaque_extensions(
    location: str, field: str, value: Any
) -> None:
    raw = _root()
    message = _result(raw)["response"]["messages"][0]
    targets = {
        "receipt": _receipt(raw),
        "history": raw["data"]["conversationHistory"][0],
        "message": message,
        "content": message["contents"][0],
        "data": raw["data"],
    }
    targets[location][field] = value
    before = _json(raw)
    with pytest.raises(ValueError):
        delivery.validate_delivery_state(raw)
    assert _json(raw) == before


def test_lookup_uses_only_exact_mailbox_identity_and_detaches_consumer_metadata() -> None:
    raw = _root()
    _deadline(raw, ABSENT)
    before = _json(raw)
    assert delivery.lookup_response(raw, "history-only", now=NOW) is None
    assert delivery.lookup_response(raw, "C", now=NOW) is None
    response = delivery.lookup_response(raw, "c", now=NOW)
    assert response is not None
    assert _json(serialize_terminal_response(response)) == _json(_result(raw)["response"])
    response.messages[0].contents[0].text = "consumer edit"
    response.additional_properties["provider"]["flag"] = True
    value = response.value
    assert isinstance(value, dict)
    value["nested"].append("consumer edit")
    assert _json(raw) == before
    again = delivery.lookup_response(raw, "c", now=NOW)
    assert again is not None
    assert _json(serialize_terminal_response(again)) == _json(_result(raw)["response"])


def test_new_response_serialization_failure_leaves_all_existing_json_untouched() -> None:
    raw = _root()
    before = _json(raw)
    unsupported = object()
    response = AgentResponse[Any](messages=[Message("assistant", [Content.from_text("answer")])], value=unsupported)
    with pytest.raises(ValueError):
        delivery.stage_response(raw, "new", response, delivery_window_seconds=60, now=NOW)
    assert _json(raw) == before
    assert response.value is unsupported


@pytest.mark.parametrize("operation", ["stage", "expiry"])
def test_staging_does_not_edit_unknown_siblings_that_alias_delivery_containers(operation: str) -> None:
    raw = _root()
    raw["unknownRoot"]["mailboxMirror"] = raw["data"]["terminalResults"]
    raw["data"]["session"]["receiptMirror"] = _receipt(raw)
    before = _json(raw)
    # A JSON round trip deliberately gives the oracle independent container paths.
    expected = json.loads(before)
    if operation == "expiry":
        del expected["data"]["terminalResults"]["c"]
        _receipt(expected).update(resultState="unavailable", resultUnavailableAt=NOW.isoformat())
        staged, count = delivery.stage_expiry(raw, now=NOW)
        assert count == 1
    else:
        payload = _new_payload()
        common = {
            "correlationId": "new",
            "outcome": "succeeded",
            "completedAt": NOW.isoformat(),
            "resultExpiresAt": "2026-09-17T12:01:00.123456+00:00",
        }
        expected["data"]["terminalResults"]["new"] = {**common, "response": payload}
        expected["data"]["completionReceipts"]["new"] = {**common, "resultState": "available"}
        staged = delivery.stage_response(
            raw, "new", load_terminal_response(payload), delivery_window_seconds=60, now=NOW
        )
    _same_detached(staged, expected, raw)
    assert _json(raw) == before


@pytest.mark.parametrize("raw", [None, False, [], {}, {"schemaVersion": "2.0.0", "data": []}])
def test_delivery_operations_reject_noncanonical_root_shapes_without_repair(raw: Any) -> None:
    before = _json(raw)
    for operation in ("stage", "expiry", "lookup", "validate"):
        with pytest.raises(ValueError):
            _call(operation, raw)
        assert _json(raw) == before


@pytest.mark.parametrize("correlation", [None, False, "", " ", "c\x00", "c" * 257])
def test_stage_and_lookup_reject_invalid_correlation_identifiers(correlation: Any) -> None:
    raw = _root()
    before = _json(raw)
    with pytest.raises(ValueError):
        delivery.stage_response(raw, correlation, AgentResponse(messages=[]), delivery_window_seconds=60, now=NOW)
    with pytest.raises(ValueError):
        delivery.lookup_response(raw, correlation, now=NOW)
    assert _json(raw) == before
