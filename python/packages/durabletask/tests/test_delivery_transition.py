# Copyright (c) Microsoft. All rights reserved.

"""Completion transition invariants using independently constructed canonical roots."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from agent_framework_durabletask._shared_state_validation import (
    validate_completion_transition,
    validate_shared_state,
)

NOW = datetime(2026, 9, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)
COMPLETED = "2026-09-17T10:00:00.000000000000000000000001Z"
DEADLINE = "2026-09-17T12:00:00.123456Z"
ABSENT = object()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, allow_nan=False)


def _root() -> dict[str, Any]:
    return {
        "schemaVersion": "2.0.0",
        "extensionData": {"root": [False, 0, 0.0]},
        "unknownRoot": {"$runtimeType": "inert.Root", "keep": [None, [], {}]},
        "data": {
            "conversationHistory": [
                {"$type": "request", "correlationId": "c", "messages": [{"role": "user"}]},
                {"$type": "response", "correlationId": "c", "messages": [], "unknownHistory": False},
            ],
            "session": {"state": {"opaque": [False, 0]}},
            "historyBinding": {"version": 999, "profile": "inert", "opaque": None},
            "ingestedPositions": {"producer": 3},
            "expirationTimeUtc": None,
            "extensionData": {"data": [None]},
            "unknownData": {"keep": [False, 0]},
            "terminalResults": {
                "c": {
                    "correlationId": "c",
                    "outcome": "failed",
                    "completedAt": COMPLETED,
                    "resultExpiresAt": DEADLINE,
                    "unknownResult": {"flag": False, "zero": 0, "items": [False, 0]},
                    "extensionData": {"mailbox": [None, False]},
                    "error": {
                        "code": "provider_failure",
                        "message": "Original failure.",
                        "details": {"flag": False, "items": [None, 0]},
                        "unknownError": [None, False],
                    },
                    "response": {
                        "messages": [
                            {
                                "role": "assistant",
                                "messageId": "message-c",
                                "extensionData": {"message": {"flag": False}},
                                "unknownMessage": [False, 0],
                                "contents": [
                                    {"$type": "text", "text": "partial answer", "unknownContent": {"flag": False}}
                                ],
                            }
                        ],
                        "value": False,
                        "responseId": "response-c",
                        "createdAt": "2026-09-17T09:59:59.123456789Z",
                        "usage": {"inputTokenCount": 1, "extensionData": {"provider": [False, 0]}},
                        "extensionData": {"provider": {"flag": False}},
                        "unknownResponse": {"flag": False},
                    },
                }
            },
            "completionReceipts": {
                "c": {
                    "correlationId": "c",
                    "outcome": "failed",
                    "completedAt": COMPLETED,
                    "resultExpiresAt": DEADLINE,
                    "resultState": "available",
                    "unknownReceipt": {"flag": False, "items": [False, 0]},
                    "extensionData": {"receipt": [None, False]},
                },
                "gone": {
                    "correlationId": "gone",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-17T08:00:00+00:00",
                    "resultState": "unavailable",
                    "resultUnavailableAt": "2026-09-17T09:00:00.123456789+00:00",
                    "unknownReceipt": {"flag": False},
                },
            },
        },
    }


def _result(raw: dict[str, Any]) -> dict[str, Any]:
    return raw["data"]["terminalResults"]["c"]


def _receipt(raw: dict[str, Any]) -> dict[str, Any]:
    return raw["data"]["completionReceipts"]["c"]


def _replace(raw: Any, path: tuple[Any, ...], value: Any) -> None:
    for key in path[:-1]:
        raw = raw[key]
    if value is ABSENT:
        del raw[path[-1]]
    else:
        raw[path[-1]] = deepcopy(value)


def _expire(raw: dict[str, Any], unavailable: str = DEADLINE) -> dict[str, Any]:
    current = deepcopy(raw)
    del current["data"]["terminalResults"]["c"]
    _receipt(current).update(resultState="unavailable", resultUnavailableAt=unavailable)
    return current


def _valid_pair(previous: dict[str, Any], current: dict[str, Any]) -> None:
    # A temporal test must not pass just because one snapshot is malformed.
    validate_shared_state(previous)
    validate_shared_state(current)


def _rejected(previous: dict[str, Any], current: dict[str, Any], *, now: Any = NOW) -> None:
    _valid_pair(previous, current)
    before = (_json(previous), _json(current))
    with pytest.raises(ValueError):
        validate_completion_transition(previous, current, now=now)
    assert (_json(previous), _json(current)) == before


@pytest.mark.parametrize("same_object", [False, True])
def test_unchanged_complete_snapshot_is_valid_and_observational(same_object: bool) -> None:
    previous = _root()
    current = previous if same_object else deepcopy(previous)
    _valid_pair(previous, current)
    before = _json(previous)
    validate_completion_transition(previous, current, now=NOW)
    assert _json(previous) == before
    assert _json(current) == before


@pytest.mark.parametrize(
    ("path", "value"),
    [
        pytest.param(("response", "value"), 0, id="false-is-not-zero"),
        pytest.param(("response", "value"), None, id="false-is-not-null"),
        pytest.param(("response", "value"), ABSENT, id="value-presence"),
        pytest.param(("unknownResult", "zero"), 0.0, id="integer-is-not-float"),
        pytest.param(("unknownResult", "items"), [0, False], id="array-order-and-types"),
        pytest.param(("unknownResult",), ABSENT, id="unknown-result-removal"),
        pytest.param(("unknownAdded",), False, id="unknown-result-addition"),
        pytest.param(("extensionData", "mailbox"), [], id="mailbox-metadata"),
        pytest.param(("error", "code"), "changed_failure", id="error-code"),
        pytest.param(("error", "message"), "Changed failure.", id="error-message"),
        pytest.param(("error", "details", "flag"), 0, id="error-details"),
        pytest.param(("error", "unknownError"), [None, 0], id="unknown-error"),
        pytest.param(("response", "responseId"), "different", id="response-id"),
        pytest.param(("response", "unknownResponse", "flag"), 0, id="unknown-response"),
        pytest.param(("response", "extensionData", "provider", "flag"), 0, id="response-metadata"),
        pytest.param(("response", "usage", "extensionData", "provider"), [0, 0], id="usage-metadata"),
        pytest.param(("response", "messages", 0, "unknownMessage"), [0, 0], id="unknown-message"),
        pytest.param(("response", "messages", 0, "contents", 0, "text"), "replacement", id="content-text"),
        pytest.param(("response", "messages", 0, "contents", 0, "unknownContent", "flag"), 0, id="unknown-content"),
    ],
)
def test_every_part_of_an_existing_terminal_result_is_immutable(path: tuple[Any, ...], value: Any) -> None:
    previous = _root()
    current = deepcopy(previous)
    _replace(_result(current), path, value)
    _rejected(previous, current)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        pytest.param(("unknownReceipt", "flag"), 0, id="false-is-not-zero"),
        pytest.param(("unknownReceipt", "items"), [0, False], id="ordered-unknown-json"),
        pytest.param(("unknownReceipt",), ABSENT, id="unknown-removal"),
        pytest.param(("unknownAdded",), None, id="unknown-addition"),
        pytest.param(("extensionData", "receipt"), [], id="receipt-metadata"),
    ],
)
def test_existing_receipt_unknown_json_and_metadata_are_immutable(path: tuple[Any, ...], value: Any) -> None:
    previous = _root()
    current = deepcopy(previous)
    _replace(_receipt(current), path, value)
    _rejected(previous, current)


@pytest.mark.parametrize("change", ["outcome", "completedAt", "resultExpiresAt", "expiry-added", "expiry-removed"])
def test_rewriting_both_members_of_a_valid_pair_does_not_bypass_immutability(change: str) -> None:
    previous = _root()
    if change == "expiry-added":
        del _result(previous)["resultExpiresAt"]
        del _receipt(previous)["resultExpiresAt"]
    current = deepcopy(previous)
    for record in (_result(current), _receipt(current)):
        if change == "outcome":
            record["outcome"] = "succeeded"
        elif change == "completedAt":
            record["completedAt"] = "2026-09-17T10:00:00.000000000000000000000002Z"
        elif change == "expiry-removed":
            del record["resultExpiresAt"]
        else:
            record["resultExpiresAt"] = "2026-09-17T12:01:00Z"
    if change == "outcome":
        del _result(current)["error"]
    _rejected(previous, current)


@pytest.mark.parametrize(
    ("unavailable", "now"),
    [
        (DEADLINE, NOW),
        ("2026-09-17T17:30:00.123456+05:30", NOW),
        ("2026-09-17T12:00:00.123456000000000000000000000001Z", NOW.replace(microsecond=123457)),
    ],
)
def test_expiry_allows_a_complete_available_to_unavailable_transition(unavailable: str, now: datetime) -> None:
    previous = _root()
    current = _expire(previous, unavailable)
    _valid_pair(previous, current)
    before = (_json(previous), _json(current))
    validate_completion_transition(previous, current, now=now)
    assert (_json(previous), _json(current)) == before
    assert _receipt(current)["outcome"] == "failed"
    assert _receipt(current)["completedAt"] == COMPLETED
    assert _receipt(current)["resultExpiresAt"] == DEADLINE


@pytest.mark.parametrize("change", ["before-deadline", "future-unavailable", "no-stored-deadline", "rewrite-on-expiry"])
def test_removal_cannot_invent_a_deadline_or_use_a_future_or_premature_clock(change: str) -> None:
    previous = _root()
    now = NOW
    if change == "no-stored-deadline":
        del _result(previous)["resultExpiresAt"]
        del _receipt(previous)["resultExpiresAt"]
    current = _expire(previous)
    if change == "before-deadline":
        now -= timedelta(microseconds=1)
    elif change == "future-unavailable":
        _receipt(current)["resultUnavailableAt"] = "2026-09-17T12:00:00.123456000000000000000000000001Z"
    elif change == "rewrite-on-expiry":
        _receipt(current)["unknownReceipt"]["flag"] = 0
    _rejected(previous, current, now=now)


def test_unavailable_timestamp_must_reach_exact_fractional_deadline_not_rounded_microsecond() -> None:
    previous = _root()
    for record in (_result(previous), _receipt(previous)):
        record["resultExpiresAt"] = "2026-09-17T12:00:00.123456000000000000000000000001Z"
    current = _expire(previous, DEADLINE)
    before = (_json(previous), _json(current))
    validate_shared_state(previous)
    with pytest.raises(ValueError):
        validate_shared_state(current)
    with pytest.raises(ValueError):
        validate_completion_transition(previous, current, now=NOW.replace(microsecond=123457))
    assert (_json(previous), _json(current)) == before


@pytest.mark.parametrize(
    "change", ["reopen", "delete-receipt", "delete-pair", "change-unavailable-time", "change-outcome"]
)
def test_committed_completion_cannot_be_reopened_deleted_or_rewritten(change: str) -> None:
    previous = _root()
    if change != "delete-pair":
        previous = _expire(previous)
    current = deepcopy(previous)
    if change == "reopen":
        current["data"]["terminalResults"]["c"] = _root()["data"]["terminalResults"]["c"]
        _receipt(current)["resultState"] = "available"
        del _receipt(current)["resultUnavailableAt"]
    elif change in ("delete-receipt", "delete-pair"):
        del current["data"]["completionReceipts"]["c"]
        current["data"]["terminalResults"].pop("c", None)
    elif change == "change-unavailable-time":
        _receipt(current)["resultUnavailableAt"] = (NOW + timedelta(microseconds=1)).isoformat()
    else:
        _receipt(current)["outcome"] = "succeeded"
    _rejected(previous, current, now=NOW + timedelta(seconds=1))


def test_downgrading_to_an_individually_valid_legacy_snapshot_cannot_discard_receipts() -> None:
    previous = _root()
    current = {"schemaVersion": "1.1.0", "data": {"conversationHistory": []}}
    _rejected(previous, current)


def test_non_delivery_state_can_change_and_new_pairs_can_be_added_without_rewriting_committed_pairs() -> None:
    previous = _root()
    current = deepcopy(previous)
    current["data"]["conversationHistory"] = []
    current["data"]["session"] = {"state": {"new": [False, 0]}}
    current["data"]["ingestedPositions"]["producer"] = 4
    current["data"]["historyBinding"] = [None, False, {"version": 999}]
    current["data"]["expirationTimeUtc"] = "2026-09-18T12:00:00Z"
    current["data"]["extensionData"]["data"] = ["changed"]
    current["unknownRoot"]["keep"] = ["changed"]
    current["data"]["terminalResults"]["new"] = {
        "correlationId": "new",
        "outcome": "succeeded",
        "completedAt": NOW.isoformat(),
        "response": {"messages": [], "value": None, "unknownResponse": [False, 0]},
    }
    current["data"]["completionReceipts"]["new"] = {
        "correlationId": "new",
        "outcome": "succeeded",
        "completedAt": NOW.isoformat(),
        "resultState": "available",
    }
    _valid_pair(previous, current)
    before = (_json(previous), _json(current))
    validate_completion_transition(previous, current, now=NOW)
    assert (_json(previous), _json(current)) == before


@pytest.mark.parametrize("now", [NOW.replace(tzinfo=None), False, True, "2026-09-17T12:00:00Z"])
def test_transition_rejects_non_datetime_and_naive_clocks(now: Any) -> None:
    previous = _root()
    _rejected(previous, _expire(previous), now=now)


@pytest.mark.parametrize(
    "change",
    ["orphan-result", "missing-result", "key-mismatch", "outcome-mismatch", "completion-fraction", "expiry-presence"],
)
def test_shared_snapshot_validation_still_requires_an_exact_complete_pair(change: str) -> None:
    raw = _root()
    if change == "orphan-result":
        del raw["data"]["completionReceipts"]["c"]
    elif change == "missing-result":
        del raw["data"]["terminalResults"]["c"]
    elif change == "key-mismatch":
        _receipt(raw)["correlationId"] = "C"
    elif change == "outcome-mismatch":
        _receipt(raw)["outcome"] = "succeeded"
    elif change == "completion-fraction":
        _receipt(raw)["completedAt"] = "2026-09-17T10:00:00.000000000000000000000002Z"
    else:
        del _receipt(raw)["resultExpiresAt"]
    before = _json(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    assert _json(raw) == before


def test_pair_matching_compares_instants_without_normalizing_original_strings() -> None:
    raw = _root()
    _receipt(raw)["completedAt"] = "2026-09-17T15:30:00.000000000000000000000001+05:30"
    _receipt(raw)["resultExpiresAt"] = "2026-09-17T05:00:00.123456000000000000000000000000-07:00"
    before = _json(raw)
    validate_shared_state(raw)
    validate_completion_transition(raw, deepcopy(raw), now=NOW)
    assert _json(raw) == before


@pytest.mark.parametrize("invalid_side", ["previous", "current"])
def test_transition_validates_later_records_before_accepting_an_earlier_expiry(invalid_side: str) -> None:
    previous = _root()
    current = _expire(previous)
    target = previous if invalid_side == "previous" else current
    target["data"]["completionReceipts"]["later"] = {
        "correlationId": "later",
        "outcome": "succeeded",
        "completedAt": "2026-09-17T10:00:00Z",
        "resultState": "available",  # No matching result.
    }
    before = (_json(previous), _json(current))
    with pytest.raises(ValueError):
        validate_completion_transition(previous, current, now=NOW)
    assert (_json(previous), _json(current)) == before


@pytest.mark.parametrize(
    "change", ["identity", "timestamp-spelling", "unavailable-expiry-added", "unavailable-unknown"]
)
def test_exact_committed_identity_and_receipt_json_survive_even_equivalent_snapshot_rewrites(change: str) -> None:
    previous = _root()
    current = deepcopy(previous)
    if change == "identity":
        for name in ("terminalResults", "completionReceipts"):
            record = current["data"][name].pop("c")
            record["correlationId"] = "C"
            current["data"][name]["C"] = record
    elif change == "timestamp-spelling":
        for record in (_result(current), _receipt(current)):
            record["completedAt"] = "2026-09-17T15:30:00.000000000000000000000001+05:30"
    elif change == "unavailable-expiry-added":
        current["data"]["completionReceipts"]["gone"]["resultExpiresAt"] = "2026-09-17T09:00:00Z"
    else:
        current["data"]["completionReceipts"]["gone"]["unknownReceipt"]["flag"] = 0
    _rejected(previous, current)
