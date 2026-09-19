# Copyright (c) Microsoft. All rights reserved.

"""Canonical shared-state acceptance tests, independent of the old prototype layout.

Schema examples exercise the public runtime validators. Reader and producer tests
add whole-JSON preservation and lifecycle assertions that schema validation alone
cannot establish. No migration from the superseded prototype is implied.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from agent_framework import Agent, AgentResponse, AgentSession, Content, Message
from clock_helpers import ClockDateTime
from jsonschema import Draft202012Validator, FormatChecker
from test_history_pipeline_revision import NonStreamingAgent, ToolChatClient
from test_revision_contract import ExternalHistory, JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState
from agent_framework_durabletask import _delivery_state as delivery_module
from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask import _shared_state_validation as validation_module
from agent_framework_durabletask._shared_state_validation import (
    validate_identifier,
    validate_shared_data,
    validate_shared_state,
)

SCHEMAS = Path(__file__).resolve().parents[4] / "schemas"
SCHEMA = json.loads((SCHEMAS / "durable-agent-entity-state.json").read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
VERSIONS = tuple(SCHEMA["properties"]["schemaVersion"]["enum"])
ENTRY_KINDS = tuple(kind.value for kind in state_module.DurableAgentStateEntryJsonType)
FIXTURES = sorted((SCHEMAS / "fixtures").glob("*.json"))
CASE_GROUPS = [
    (path.name, group)
    for path in sorted((SCHEMAS / "tests").glob("*-cases.json"))
    for group in json.loads(path.read_text(encoding="utf-8"))
]
CASES = [
    pytest.param(group["schema"]["$ref"], case, id=f"{name}/{group['description']}/{case['description']}")
    for name, group in CASE_GROUPS
    for case in group["tests"]
]
NOW = datetime(2026, 9, 16, tzinfo=timezone.utc)
COMPLETED = "2026-09-16T00:00:00Z"
EXPIRES = "2026-09-16T00:01:00Z"
OPAQUE: dict[str, Any] = {
    "$runtimeType": "inert.Type",
    "nested": [None, False, 0, -0.0, "", [], {}, 2**80, "e\u0301😀"],
}
JSON_VALUES: list[Any] = [None, False, 0, -0.0, "", [], {}, OPAQUE]
ABSENT = object()


def _json(value: Any) -> str:
    # Python equality alone would mistake false for zero, including nested values.
    return json.dumps(value, ensure_ascii=True, sort_keys=True, allow_nan=False)


def _same(actual: Any, expected: Any) -> None:
    assert _json(actual) == _json(expected)


def _response(state: DurableAgentState, key: str = "c") -> AgentResponse[Any]:
    response = state.try_get_agent_response(key)
    assert response is not None
    return response


def _empty(version: str = "2.0.0") -> dict[str, Any]:
    data: dict[str, Any] = {"conversationHistory": []}
    if version == "2.0.0":
        data.update(terminalResults={}, completionReceipts={})
    return {"schemaVersion": version, "data": data}


def _mailbox(*, outcome: str = "succeeded", expiry: bool = True) -> dict[str, Any]:
    raw = _empty()
    common: dict[str, Any] = {"correlationId": "c", "outcome": outcome, "completedAt": COMPLETED}
    if expiry:
        common["resultExpiresAt"] = EXPIRES
    result = {
        **common,
        "response": {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "payload"}]}]},
    }
    if outcome == "failed":
        result["error"] = {"code": "synthetic", "message": "Synthetic failure", "details": deepcopy(OPAQUE)}
    raw["data"]["terminalResults"]["c"] = result
    raw["data"]["completionReceipts"]["c"] = {**common, "resultState": "available"}
    return raw


def _valid(raw: dict[str, Any]) -> None:
    before = _json(raw)
    VALIDATOR.validate(raw)
    validate_shared_state(raw)
    validate_shared_data(raw["data"], version=raw["schemaVersion"])
    assert _json(raw) == before


def _roundtrip(raw: dict[str, Any]) -> DurableAgentState:
    before = deepcopy(raw)
    _valid(raw)
    state = DurableAgentState.from_dict(raw)
    _same(state.to_dict(), before)
    _same(json.loads(state.to_json()), before)
    _same(DurableAgentState.from_json(json.dumps(raw, allow_nan=False)).to_dict(), before)
    _same(raw, before)
    _valid(state.to_dict())
    return state


def _case_root(reference: str, value: Any) -> dict[str, Any]:
    fragment = reference.partition("#")[2]
    if not fragment:
        return deepcopy(value)
    raw = _empty()
    if fragment == "/$defs/v2ChatMessage":
        raw["data"]["conversationHistory"] = [{"$type": "response", "messages": [deepcopy(value)]}]
    elif fragment == "/$defs/terminalResponse":
        # A response-only case must get a consistent result AND receipt, otherwise
        # a semantic failure could conceal a structural validator regression.
        raw = _mailbox(expiry=False)
        raw["data"]["terminalResults"]["c"]["response"] = deepcopy(value)
    elif fragment == "/$defs/data/properties/ingestedPositions":
        raw["data"]["ingestedPositions"] = deepcopy(value)
    else:
        pytest.fail(f"Add an explicit, semantically valid envelope for {reference}")
    return raw


def test_corpus_enumeration_and_exact_schema_versions() -> None:
    Draft202012Validator.check_schema(SCHEMA)
    assert len(CASES) == 96
    assert len(FIXTURES) == 4
    assert VERSIONS == ("1.0.0", "1.1.0", "1.2.0", "2.0.0")
    assert {group["schema"]["$ref"].partition("#")[2] for _, group in CASE_GROUPS} == {
        "",
        "/$defs/v2ChatMessage",
        "/$defs/terminalResponse",
        "/$defs/data/properties/ingestedPositions",
    }
    assert {group["schema"]["$ref"].partition("#")[0] for _, group in CASE_GROUPS} == {SCHEMA["$id"]}
    entries = SCHEMA["allOf"][0]["then"]["properties"]["data"]["properties"]["conversationHistory"]["items"]["oneOf"]
    assert set(ENTRY_KINDS) == {
        SCHEMA["$defs"][reference["$ref"].rsplit("/", 1)[-1]]["properties"]["$type"]["const"] for reference in entries
    }


@pytest.mark.parametrize(("reference", "case"), CASES)
def test_all_language_neutral_cases_match_public_runtime_validation(reference: str, case: dict[str, Any]) -> None:
    fragment = reference.partition("#")[2]
    schema = {"$defs": SCHEMA["$defs"], "$ref": f"#{fragment}"} if fragment else SCHEMA
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    assert validator.is_valid(case["data"]) is case["valid"]
    raw = _case_root(reference, case["data"])
    before = deepcopy(raw)
    assert VALIDATOR.is_valid(raw) is case["valid"]
    if case["valid"]:
        _valid(raw)
    else:
        with pytest.raises(ValueError):
            validate_shared_state(raw)
        with pytest.raises(ValueError):
            validate_shared_data(raw["data"], version=raw["schemaVersion"])
    _same(raw, before)


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda path: path.name)
def test_actual_reader_roundtrips_each_complete_fixture_without_autofill(fixture: Path) -> None:
    _roundtrip(json.loads(fixture.read_text(encoding="utf-8")))


@pytest.mark.parametrize("version", VERSIONS)
def test_exact_versions_roundtrip_without_upgrade_or_new_optional_fields(version: str) -> None:
    raw = _empty(version)
    if version != "2.0.0":
        raw["data"] = {}
    state = _roundtrip(raw)
    assert state.schema_version == version


@pytest.mark.parametrize("kind", ENTRY_KINDS)
@pytest.mark.parametrize("messages", [ABSENT, [], [{"role": "assistant"}], [{"role": "assistant", "contents": []}]])
def test_sparse_actual_state_preserves_missing_created_at_and_optional_fields(kind: str, messages: Any) -> None:
    raw = _empty()
    entry: dict[str, Any] = {"$type": kind}
    if messages is not ABSENT:
        entry["messages"] = deepcopy(messages)
    raw["data"]["conversationHistory"] = [entry]
    state = _roundtrip(raw)
    assert state.data.conversation_history[0].created_at is None
    assert "createdAt" not in state.to_dict()["data"]["conversationHistory"][0]


def _rich_raw() -> dict[str, Any]:
    raw = _mailbox(outcome="failed")
    response = raw["data"]["terminalResults"]["c"]["response"]
    response.update({
        "value": deepcopy(OPAQUE),
        "createdAt": "2026-09-16T00:00:00.123456789123456789Z",
        "responseId": "response",
        "agentId": "agent",
        "finishReason": "stop",
        "continuationToken": "AP8B",
        "usage": {"inputTokenCount": 2**80, "outputTokenCount": 0.0, "extensionData": deepcopy(OPAQUE)},
    })
    response["messages"][0].update({
        "messageId": "",
        "authorName": "",
        "createdAt": "2026-09-16T05:30:00.123456789+05:30",
    })
    raw["data"]["conversationHistory"] = [{"$type": "request", "messages": deepcopy(response["messages"])}]
    raw["data"].update({
        "historyBinding": deepcopy(OPAQUE),
        "session": {"type": "foreign-session", "state": deepcopy(OPAQUE), "messages": {"not": "model input"}},
        "expirationTimeUtc": None,
        "ingestedPositions": {},
        "truncation": {"evictedMessageCount": 1, "firstEvictedAt": COMPLETED, "lastEvictedAt": COMPLETED},
    })
    # Unknown siblings and explicit metadata deliberately collide by name, but
    # never overwrite one another. Do not test just a selected subset of fields.
    result = raw["data"]["terminalResults"]["c"]
    entry = raw["data"]["conversationHistory"][0]
    for node in [
        raw,
        raw["data"],
        entry,
        entry["messages"][0],
        entry["messages"][0]["contents"][0],
        result,
        result["error"],
        raw["data"]["completionReceipts"]["c"],
        response,
        response["messages"][0],
        response["messages"][0]["contents"][0],
        response["usage"],
        raw["data"]["truncation"],
    ]:
        node["future"] = deepcopy(OPAQUE)
        node["extensionData"] = {"future": ["explicit", deepcopy(OPAQUE)]}
    return raw


def test_original_unknown_nested_values_remain_at_every_original_location() -> None:
    raw = _rich_raw()
    state = _roundtrip(raw)
    raw["data"]["terminalResults"]["c"]["response"]["value"]["nested"].append("caller mutation")
    _same(state.to_dict(), _rich_raw())
    exported = state.to_dict()
    exported["future"]["nested"].append("export mutation")
    _same(state.to_dict(), _rich_raw())


@pytest.mark.parametrize("value", JSON_VALUES)
def test_content_extension_data_is_arbitrary_json_not_an_object_requirement(value: Any) -> None:
    raw = _rich_raw()
    data = raw["data"]
    for content in [
        data["conversationHistory"][0]["messages"][0]["contents"][0],
        data["terminalResults"]["c"]["response"]["messages"][0]["contents"][0],
    ]:
        content["extensionData"] = deepcopy(value)
    _roundtrip(raw)


@pytest.mark.parametrize("value", JSON_VALUES)
def test_history_binding_accepts_any_json_and_session_contents_are_opaque(value: Any) -> None:
    raw = _empty()
    raw["data"].update(historyBinding=deepcopy(value), session={"opaque": deepcopy(value), "$runtimeType": "inert"})
    _roundtrip(raw)


def test_raw_shadow_changes_only_the_edited_projection_not_absence_or_timestamp_precision() -> None:
    raw = _rich_raw()
    state = _roundtrip(raw)
    content = state.data.conversation_history[0].messages[0].contents[0]
    assert isinstance(content, state_module.DurableAgentStateTextContent)
    content.text = "edited"
    expected = deepcopy(raw)
    expected["data"]["conversationHistory"][0]["messages"][0]["contents"][0]["text"] = "edited"
    _same(state.to_dict(), expected)
    _roundtrip(state.to_dict())


def test_raw_shadow_detects_nested_false_to_zero_change() -> None:
    raw = _empty()
    raw["data"]["conversationHistory"] = [
        {
            "$type": "response",
            "messages": [
                {"role": "tool", "contents": [{"$type": "functionResult", "callId": "tool", "result": {"n": False}}]}
            ],
        }
    ]
    state = _roundtrip(raw)
    content = state.data.conversation_history[0].messages[0].contents[0]
    assert isinstance(content, state_module.DurableAgentStateFunctionResultContent)
    assert isinstance(content.result, dict)
    content.result["n"] = 0
    expected = deepcopy(raw)
    expected["data"]["conversationHistory"][0]["messages"][0]["contents"][0]["result"]["n"] = 0
    _same(state.to_dict(), expected)


# Paths are rooted in a complete, valid mailbox. ABSENT deletes exactly one field.
RESULT = ("data", "terminalResults", "c")
RECEIPT = ("data", "completionReceipts", "c")
RESPONSE = (*RESULT, "response")
STRUCTURAL_CASES = [
    ("missing-root-version", ("schemaVersion",), ABSENT),
    ("missing-data", ("data",), ABSENT),
    ("null-data", ("data",), None),
    ("missing-transcript", ("data", "conversationHistory"), ABSENT),
    ("missing-results", ("data", "terminalResults"), ABSENT),
    ("missing-receipts", ("data", "completionReceipts"), ABSENT),
    ("array-results", ("data", "terminalResults"), []),
    ("null-receipts", ("data", "completionReceipts"), None),
    ("root-metadata-not-object", ("extensionData",), []),
    ("data-metadata-not-object", ("data", "extensionData"), False),
    ("session-not-object", ("data", "session"), []),
    ("fractional-position", ("data", "ingestedPositions"), {"p": 0.5}),
    ("boolean-position", ("data", "ingestedPositions"), {"p": True}),
    ("missing-outcome", (*RECEIPT, "outcome"), ABSENT),
    ("unknown-outcome", (*RECEIPT, "outcome"), "unknown"),
    ("null-outcome", (*RESULT, "outcome"), None),
    ("unknown-availability", (*RECEIPT, "resultState"), "pending"),
    ("missing-availability", (*RECEIPT, "resultState"), ABSENT),
    ("available-with-removal-time", (*RECEIPT, "resultUnavailableAt"), EXPIRES),
    ("success-with-error", (*RESULT, "error"), {"code": "bad", "message": "bad"}),
    ("success-with-null-error", (*RESULT, "error"), None),
    ("missing-response", RESPONSE, ABSENT),
    ("missing-response-messages", (*RESPONSE, "messages"), ABSENT),
    ("bad-response-messages", (*RESPONSE, "messages"), None),
    ("blank-response-id", (*RESPONSE, "responseId"), " "),
    ("bad-finish-reason", (*RESPONSE, "finishReason"), False),
    ("bad-base64", (*RESPONSE, "continuationToken"), "AQID\n"),
    ("large-base64", (*RESPONSE, "continuationToken"), "A" * 16388),
    ("bad-response-metadata-key", (*RESPONSE, "extensionData"), {"bad\x7f": 1}),
    ("boolean-usage", (*RESPONSE, "usage"), {"inputTokenCount": True}),
    ("fractional-usage", (*RESPONSE, "usage"), {"outputTokenCount": 0.5}),
    ("null-usage-metadata", (*RESPONSE, "usage"), {"extensionData": None}),
    ("unknown-message-role", (*RESPONSE, "messages", 0, "role"), "future"),
    ("bad-message-metadata", (*RESPONSE, "messages", 0, "extensionData"), []),
    ("bad-message-id", (*RESPONSE, "messages", 0, "messageId"), 0),
    ("unwrapped-wire-content", (*RESPONSE, "messages", 0, "contents", 0), {"$type": "future", "content": {}}),
    ("malformed-known-text", (*RESPONSE, "messages", 0, "contents", 0), {"$type": "text", "text": False}),
    ("malformed-known-error", (*RESPONSE, "messages", 0, "contents", 0), {"$type": "error", "message": []}),
    ("missing-opaque-content", (*RESPONSE, "messages", 0, "contents", 0), {"$type": "unknown"}),
    ("unknown-entry", ("data", "conversationHistory"), [{"$type": "future"}]),
    ("missing-entry-type", ("data", "conversationHistory"), [{}]),
    ("compaction-correlation", ("data", "conversationHistory"), [{"$type": "compaction", "correlationId": "c"}]),
]
SEMANTIC_CASES = [
    ("result-key-case-mismatch", (*RESULT, "correlationId"), "C"),
    ("receipt-key-case-mismatch", (*RECEIPT, "correlationId"), "C"),
    ("result-without-receipt", RECEIPT, ABSENT),
    ("available-without-result", RESULT, ABSENT),
    ("different-outcome", (*RECEIPT, "outcome"), "failed"),
    ("different-completion", (*RECEIPT, "completedAt"), "2026-09-16T00:00:00.000000001Z"),
    ("different-expiry", (*RECEIPT, "resultExpiresAt"), "2026-09-16T00:01:00.000000001Z"),
    ("missing-receipt-expiry", (*RECEIPT, "resultExpiresAt"), ABSENT),
    ("missing-result-expiry", (*RESULT, "resultExpiresAt"), ABSENT),
    ("expiry-before-completion", (*RESULT, "resultExpiresAt"), "2026-09-15T23:59:59.999999999Z"),
    (
        "unavailable-with-result",
        RECEIPT,
        {
            "correlationId": "c",
            "outcome": "succeeded",
            "completedAt": COMPLETED,
            "resultState": "unavailable",
            "resultExpiresAt": EXPIRES,
            "resultUnavailableAt": EXPIRES,
        },
    ),
    (
        "backwards-truncation",
        ("data", "truncation"),
        {
            "evictedMessageCount": 1,
            "firstEvictedAt": EXPIRES,
            "lastEvictedAt": COMPLETED,
        },
    ),
]
# jsonschema's date-time checker needs optional format dependencies. Runtime
# validation must enforce timestamps even when that optional checker is absent.
FORMAT_CASES = [
    ("bad-idle-ttl", ("data", "expirationTimeUtc"), "tomorrow"),
    ("bad-message-time", (*RESPONSE, "messages", 0, "createdAt"), "2026-09-16"),
]


def _changed(raw: dict[str, Any], path: tuple[Any, ...], value: Any) -> dict[str, Any]:
    changed = deepcopy(raw)
    node: Any = changed
    for key in path[:-1]:
        node = node[key]
    if value is ABSENT:
        del node[path[-1]]
    else:
        node[path[-1]] = deepcopy(value)
    return changed


@pytest.mark.parametrize(("name", "path", "value"), STRUCTURAL_CASES, ids=[c[0] for c in STRUCTURAL_CASES])
def test_structural_negatives_start_from_valid_semantics_and_reject_without_rewriting(
    name: str, path: tuple[Any, ...], value: Any
) -> None:
    baseline = _mailbox()
    _roundtrip(baseline)
    raw = _changed(baseline, path, value)
    before = deepcopy(raw)
    assert not VALIDATOR.is_valid(raw), name
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    _same(raw, before)


@pytest.mark.parametrize(("name", "path", "value"), SEMANTIC_CASES, ids=[c[0] for c in SEMANTIC_CASES])
def test_schema_valid_cross_map_and_temporal_inconsistencies_are_rejected(
    name: str, path: tuple[Any, ...], value: Any
) -> None:
    baseline = _mailbox()
    _roundtrip(baseline)
    raw = _changed(baseline, path, value)
    before = deepcopy(raw)
    VALIDATOR.validate(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    with pytest.raises(ValueError):
        validate_shared_data(raw["data"])
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    _same(raw, before)


@pytest.mark.parametrize("expires", [False, True])
@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_unavailable_time_is_not_before_completion_or_stored_expiry(expires: bool, offset: int) -> None:
    raw = _mailbox(expiry=expires)
    raw["data"]["terminalResults"].clear()
    receipt = raw["data"]["completionReceipts"]["c"]
    deadline = NOW + timedelta(seconds=60 if expires else 0)
    receipt.update(resultState="unavailable", resultUnavailableAt=(deadline + timedelta(seconds=offset)).isoformat())
    VALIDATOR.validate(raw)
    if offset < 0:
        with pytest.raises(ValueError):
            validate_shared_state(raw)
        with pytest.raises(ValueError):
            DurableAgentState.from_dict(raw)
    else:
        _roundtrip(raw)


@pytest.mark.parametrize("field", ["code", "message"])
@pytest.mark.parametrize("value", [ABSENT, None, False, "", " \t "])
def test_failed_result_requires_valid_known_error_fields_not_an_opaque_wrapper(field: str, value: Any) -> None:
    raw = _changed(_mailbox(outcome="failed"), (*RESULT, "error", field), value)
    assert not VALIDATOR.is_valid(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("error", [ABSENT, None, {}, {"$type": "unknown", "content": {"message": "failure"}}])
def test_failed_result_cannot_omit_or_wrap_the_terminal_error(error: Any) -> None:
    raw = _changed(_mailbox(outcome="failed"), (*RESULT, "error"), error)
    assert not VALIDATOR.is_valid(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)


@pytest.mark.parametrize("version", [None, False, 2, "", "1.2.1", "2.0", "2.0.0-preview", "2.0.0\n", "2.7.3", "3.0.0"])
def test_unlisted_versions_are_rejected_exactly_before_core_construction(
    version: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _empty()
    raw["schemaVersion"] = version
    before = deepcopy(raw)
    assert not VALIDATOR.is_valid(raw)
    forbidden = Mock(side_effect=AssertionError("Unsupported wire state must not construct core objects"))
    monkeypatch.setattr(AgentResponse, "__init__", forbidden)
    monkeypatch.setattr(Content, "__init__", forbidden)
    monkeypatch.setattr(Message, "__init__", forbidden)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    with pytest.raises(ValueError):
        validate_shared_data(raw["data"], version=version)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    forbidden.assert_not_called()
    _same(raw, before)


@pytest.mark.parametrize("version", VERSIONS[:-1])
@pytest.mark.parametrize("field", ["terminalResults", "completionReceipts", "historyBinding"])
@pytest.mark.parametrize("value", [{}, None])
def test_legacy_versions_forbid_new_maps_and_binding_even_when_empty(version: str, field: str, value: Any) -> None:
    raw = _empty(version)
    raw["data"][field] = value
    assert not VALIDATOR.is_valid(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("identifier", ["a", " id ", "😀" * 256, "e\u0301" * 128, "a\u00a0", "a\u200b", "é", "e\u0301"])
def test_identifiers_use_unicode_code_points_without_normalization(identifier: str) -> None:
    identifier_schema = {"$defs": SCHEMA["$defs"], "$ref": "#/$defs/identifier"}
    Draft202012Validator(identifier_schema).validate(identifier)
    validate_identifier(identifier)
    raw = _mailbox(expiry=False)
    for field in ("terminalResults", "completionReceipts"):
        record = raw["data"][field].pop("c")
        record["correlationId"] = identifier
        raw["data"][field][identifier] = record
    _roundtrip(raw)


@pytest.mark.parametrize("identifier", [None, False, 0, "", " \u00a0\u2003", "😀" * 257, "e\u0301" * 129])
def test_identifier_limits_reject_non_strings_blank_and_overlong_values(identifier: Any) -> None:
    assert not Draft202012Validator(SCHEMA["$defs"]["identifier"]).is_valid(identifier)
    with pytest.raises(ValueError):
        validate_identifier(identifier)


@pytest.mark.parametrize("codepoint", [*range(0x20), *range(0x7F, 0xA0)])
def test_every_c0_and_c1_control_is_forbidden_in_identifiers(codepoint: int) -> None:
    identifier = f"valid{chr(codepoint)}suffix"
    assert not Draft202012Validator(SCHEMA["$defs"]["identifier"]).is_valid(identifier)
    with pytest.raises(ValueError):
        validate_identifier(identifier)


def test_unicode_equivalent_but_different_map_keys_do_not_match() -> None:
    raw = _mailbox()
    for field, key in [("terminalResults", "é"), ("completionReceipts", "e\u0301")]:
        record = raw["data"][field].pop("c")
        record["correlationId"] = key
        raw["data"][field][key] = record
    VALIDATOR.validate(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)


@pytest.mark.parametrize("number", [0, -1, 2**100, -(2**100), 0.0, 1.0, 1e30])
def test_schema_integer_counts_preserve_integral_floats_and_large_signed_integers(number: Any) -> None:
    raw = _mailbox()
    raw["data"]["terminalResults"]["c"]["response"]["usage"] = {
        "inputTokenCount": number,
        "extensionData": {"fraction": 0.125, "nullable": None},
    }
    _roundtrip(raw)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), (1, 2), {1: "key"}, object()])
def test_public_validators_reject_non_json_even_in_opaque_data_without_mutation(value: Any) -> None:
    raw = _empty()
    raw["future"] = value
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    assert raw["future"] is value
    raw["data"]["historyBinding"] = value
    with pytest.raises(ValueError):
        validate_shared_data(raw["data"])
    assert raw["data"]["historyBinding"] is value


def test_public_validation_rejects_cycles_but_accepts_shared_subtrees() -> None:
    raw = _empty()
    shared: list[Any] = [False, None]
    raw["data"].update(historyBinding=shared, session={"same": shared})
    _valid(raw)
    shared.append(shared)
    with pytest.raises(ValueError, match="cycle"):
        validate_shared_state(raw)
    with pytest.raises(ValueError, match="cycle"):
        validate_shared_data(raw["data"])
    assert shared[-1] is shared


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-09-16",
        "2026-09-16T00:00:00",
        "2026-09-16 00:00:00Z",
        "2026-02-30T00:00:00Z",
        "2026-09-16T00:00:00+24:00",
        "2026-09-16T00:00:00+00:60",
        "2026-09-16T00:00:00Z\n",
        "２０２６-09-16T00:00:00Z",
        "2026-09-16T00:00:60Z",
    ],
)
def test_timestamps_require_ascii_offset_bearing_rfc3339_without_leap_seconds(timestamp: str) -> None:
    raw = _mailbox()
    _valid(raw)
    raw["data"]["completionReceipts"]["c"]["completedAt"] = timestamp
    before = deepcopy(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    with pytest.raises(ValueError):
        validate_shared_data(raw["data"])
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    _same(raw, before)


@pytest.mark.parametrize(("name", "path", "value"), FORMAT_CASES, ids=[c[0] for c in FORMAT_CASES])
def test_runtime_checks_formats_independently_of_optional_schema_extras(
    name: str, path: tuple[Any, ...], value: Any
) -> None:
    baseline = _mailbox()
    _valid(baseline)
    raw = _changed(baseline, path, value)
    before = deepcopy(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    with pytest.raises(ValueError):
        validate_shared_data(raw["data"])
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    _same(raw, before)


@pytest.mark.parametrize("fraction", ["1", "123456789", "123456789012345678901234567890123456789"])
def test_equal_offset_instants_and_fractional_spelling_roundtrip_exactly(fraction: str) -> None:
    raw = _mailbox()
    result, receipt = raw["data"]["terminalResults"]["c"], raw["data"]["completionReceipts"]["c"]
    result["completedAt"] = f"2026-09-16t00:00:00.{fraction}z"
    receipt["completedAt"] = f"2026-09-16T05:30:00.{fraction}0+05:30"
    result["resultExpiresAt"] = "2026-09-16T01:01:00+01:00"
    _roundtrip(raw)


def test_fraction_comparison_does_not_round_under_decimal_context() -> None:
    raw = _mailbox(expiry=False)
    raw["data"]["terminalResults"]["c"]["completedAt"] = "2026-09-16T00:00:00.1234567890123456789012345678901Z"
    raw["data"]["completionReceipts"]["c"]["completedAt"] = "2026-09-16T00:00:00.1234567890123456789012345678902Z"
    VALIDATOR.validate(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[datetime], None]:
    class Clock(ClockDateTime):
        instant = NOW

        @classmethod
        def now(cls, tz: Any = None) -> Clock:
            assert tz is not None, "These contract tests must not use a naive wall clock"
            return cls.fromtimestamp(cls.instant.timestamp(), tz)

    def set_time(instant: datetime) -> None:
        Clock.instant = instant

    for module in (state_module, delivery_module, validation_module):
        monkeypatch.setattr(module, "datetime", Clock)
    return set_time


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_record_response_produces_canonical_atomic_maps_and_immutable_completion(outcome: str) -> None:
    response = AgentResponse[Any](
        messages=[
            Message(
                "assistant",
                [
                    Content.from_error(message="Synthetic failure", error_code="synthetic")
                    if outcome == "failed"
                    else Content.from_text("original")
                ],
            )
        ],
        value=False,
    )
    state = DurableAgentState()
    state.record_response("c", response, delivery_window_seconds=60, now=NOW)
    raw = state.to_dict()
    _roundtrip(raw)
    data = raw["data"]
    assert "responseMailbox" not in data and "completedCorrelations" not in data
    result, receipt = data["terminalResults"]["c"], data["completionReceipts"]["c"]
    assert result["outcome"] == receipt["outcome"] == outcome
    assert receipt["resultState"] == "available" and "resultUnavailableAt" not in receipt
    for field in ("correlationId", "completedAt", "resultExpiresAt"):
        assert result[field] == receipt[field]
    assert ("error" in result) is (outcome == "failed")
    assert result["response"]["value"] is False
    assert "type" not in result["response"]
    assert result["response"]["messages"][0]["contents"][0]["$type"] == ("error" if outcome == "failed" else "text")
    response.messages.clear()
    state.record_response("c", AgentResponse(messages=[]), delivery_window_seconds=999, now=NOW + timedelta(days=1))
    _same(state.to_dict(), raw)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_logical_expiry_then_cleanup_retains_receipt_timestamps_outcome_and_unknowns(
    outcome: str, clock: Callable[[datetime], None]
) -> None:
    raw = _mailbox(outcome=outcome)
    raw["future"] = deepcopy(OPAQUE)
    raw["data"]["completionReceipts"]["c"]["future"] = deepcopy(OPAQUE)
    state = _roundtrip(raw)
    clock(NOW + timedelta(seconds=59))
    assert _response(state).text == "payload"
    clock(NOW + timedelta(seconds=60))
    expired = state.try_get_agent_response("c")
    assert expired is not None
    assert expired.additional_properties["durable_status"] == "already_completed"
    assert expired.additional_properties["durable_outcome"] == outcome
    assert "payload" not in expired.text
    _same(state.to_dict(), raw)  # Logical expiry does not rewrite an available receipt.
    cleanup = NOW + timedelta(seconds=65)
    state.expire_responses(now=cleanup)
    expected = deepcopy(raw)
    expected["data"]["terminalResults"].clear()
    expected["data"]["completionReceipts"]["c"].update(
        resultState="unavailable", resultUnavailableAt=cleanup.isoformat()
    )
    _same(state.to_dict(), expected)
    _roundtrip(expected)
    state.expire_responses(now=NOW + timedelta(days=1))
    state.record_response("c", AgentResponse(messages=[]), delivery_window_seconds=60, now=NOW + timedelta(days=1))
    _same(state.to_dict(), expected)
    assert state.try_get_agent_response("unknown") is None


@pytest.mark.parametrize("idle_ttl", [ABSENT, None, "2026-09-15T00:00:00Z"])
def test_absent_result_expiry_never_acquires_an_automatic_ttl(idle_ttl: Any, clock: Callable[[datetime], None]) -> None:
    raw = _mailbox(expiry=False)
    if idle_ttl is not ABSENT:
        raw["data"]["expirationTimeUtc"] = idle_ttl
    state = _roundtrip(raw)
    future = NOW + timedelta(days=3650)
    clock(future)
    state.expire_responses(now=future)
    assert _response(state).text == "payload"
    _same(state.to_dict(), raw)


@pytest.mark.parametrize("operation", ["lookup", "cleanup"])
def test_fractional_expiry_is_not_truncated_before_the_logical_deadline(
    operation: str, clock: Callable[[datetime], None]
) -> None:
    raw = _mailbox()
    for field in ("terminalResults", "completionReceipts"):
        raw["data"][field]["c"]["resultExpiresAt"] = "2026-09-16T00:00:00.000001001Z"
    state = _roundtrip(raw)
    clock(NOW + timedelta(microseconds=1))
    if operation == "lookup":
        assert _response(state).text == "payload"
    else:
        state.expire_responses(now=NOW + timedelta(microseconds=1))
    _same(state.to_dict(), raw)
    state.expire_responses(now=NOW + timedelta(microseconds=2))
    _valid(state.to_dict())
    assert state.to_dict()["data"]["completionReceipts"]["c"]["resultState"] == "unavailable"


def test_v2_lookup_never_infers_completion_from_transcript(clock: Callable[[datetime], None]) -> None:
    raw = _mailbox(expiry=False)
    raw["data"]["conversationHistory"] = [
        {
            "$type": "response",
            "correlationId": key,
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "not the terminal result"}]}],
        }
        for key in ("c", "transcript-only")
    ]
    state = _roundtrip(raw)
    assert _response(state).text == "payload"
    assert state.try_get_agent_response("transcript-only") is None
    state.data.conversation_history.clear()
    assert _response(state).text == "payload"


class _RichAgent(NonStreamingAgent):
    """Enrich a real core/client response at the agent's public response boundary."""

    def __init__(self, *, value: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.structured_value = value

    def run(self, *args: Any, **kwargs: Any) -> Any:
        pending = super().run(*args, **kwargs)

        async def finish() -> AgentResponse[Any]:
            response = await pending
            fields: dict[str, Any] = {}
            if self.structured_value is not ABSENT:
                fields["value"] = deepcopy(self.structured_value)
            return AgentResponse[Any](
                messages=response.messages,
                response_id="real-response",
                agent_id="real-agent",
                created_at="2026-09-16T00:00:00.123456789Z",
                finish_reason="stop",
                usage_details=cast(
                    Any, {"input_token_count": 2**80, "output_token_count": 0, "provider": deepcopy(OPAQUE)}
                ),
                additional_properties={"provider": deepcopy(OPAQUE)},
                continuation_token=cast(Any, {"cursor": deepcopy(OPAQUE)}),
                **fields,
            )

        return finish()


@pytest.mark.parametrize("owner", ["default", "service", "external"])
@pytest.mark.parametrize("failed", [False, True], ids=["success", "failure"])
async def test_real_entity_producer_uses_canonical_maps_for_each_history_owner(owner: str, failed: bool) -> None:
    client = ToolChatClient(tool_calls=False, fail=failed)
    external = ExternalHistory() if owner == "external" else None
    agent = Agent(client=client, name="contract", context_providers=[external] if external else None)
    provider = JsonStateProvider()
    callback = Mock()
    callback.on_agent_response = AsyncMock()
    callback.on_streaming_response_update = AsyncMock()
    entity = AgentEntity(agent, callback=callback, state_provider=provider)
    request = {"message": "input", "correlationId": "c", "options": {"store": owner == "service"}}
    response = await entity.run(request)
    assert len(client.received_messages) == 1 and provider.writes == 1
    _roundtrip(provider.raw)
    data = provider.raw["data"]
    result, receipt = data["terminalResults"]["c"], data["completionReceipts"]["c"]
    assert result["outcome"] == receipt["outcome"] == ("failed" if failed else "succeeded")
    assert receipt["resultState"] == "available"
    assert ("error" in result) is failed
    assert "responseMailbox" not in data and "completedCorrelations" not in data
    assert "type" not in result["response"] and "messages" in result["response"]
    assert response.additional_properties.get("durable_status") == ("error" if failed else None)
    if owner != "default":
        assert data["conversationHistory"] == []
    if owner == "service" and not failed:
        assert data["session"]["service_session_id"] == "service-thread"
    if external is not None and not failed:
        assert [message.text for message in external.messages] == ["input", "answer-1"]
    before = deepcopy(provider.raw)
    cold = JsonStateProvider(before)
    duplicate = await AgentEntity(agent, state_provider=cold).run(request)
    assert duplicate.text == response.text
    assert len(client.received_messages) == 1 and cold.writes == 0
    _same(cold.raw, before)


@pytest.mark.parametrize("owner", ["default", "service", "external"])
@pytest.mark.parametrize("value", [ABSENT, False, 0, -0.0, "", [], {}, OPAQUE])
async def test_real_entity_preserves_falsey_values_and_rich_response_metadata(owner: str, value: Any) -> None:
    client = ToolChatClient(tool_calls=False)
    external = ExternalHistory() if owner == "external" else None
    agent = _RichAgent(value=value, client=client, context_providers=[external] if external else None)
    provider = JsonStateProvider()
    await AgentEntity(agent, state_provider=provider).run({
        "message": "input",
        "correlationId": "c",
        "options": {"store": owner == "service"},
    })
    assert len(client.received_messages) == 1 and provider.writes == 1
    state = _roundtrip(provider.raw)
    response = provider.raw["data"]["terminalResults"]["c"]["response"]
    assert ("value" in response) is (value is not ABSENT)
    if value is not ABSENT:
        _same(response["value"], value)
        _same(_response(state).value, value)
    assert response["responseId"] == "real-response" and response["agentId"] == "real-agent"
    assert response["createdAt"] == "2026-09-16T00:00:00.123456789Z"
    assert response["usage"]["inputTokenCount"] == 2**80
    _same(response["usage"]["extensionData"]["provider"], OPAQUE)
    _same(response["extensionData"], {"provider": OPAQUE})
    _same(_response(state).continuation_token, {"cursor": OPAQUE})


@pytest.mark.parametrize("value", JSON_VALUES)
def test_present_wire_value_including_null_is_distinct_from_absence(value: Any) -> None:
    raw = _mailbox(expiry=False)
    raw["data"]["terminalResults"]["c"]["response"]["value"] = deepcopy(value)
    state = _roundtrip(raw)
    _same(_response(state).value, value)


@pytest.mark.parametrize(
    ("name", "path", "value"),
    [*STRUCTURAL_CASES, *SEMANTIC_CASES, *FORMAT_CASES],
    ids=[c[0] for c in [*STRUCTURAL_CASES, *SEMANTIC_CASES, *FORMAT_CASES]],
)
async def test_bad_state_rejects_before_model_callback_storage_or_registration_mutation(
    name: str, path: tuple[Any, ...], value: Any
) -> None:
    raw = _changed(_mailbox(), path, value)
    before = deepcopy(raw)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(client=client)
    callback = Mock()
    callback.on_agent_response = AsyncMock()
    callback.on_streaming_response_update = AsyncMock()
    provider = JsonStateProvider(raw)
    entity = AgentEntity(agent, callback=callback, state_provider=provider)
    registration = entity.agent
    providers = agent.context_providers
    provider_items = tuple(providers)
    options = deepcopy(agent.default_options)
    middleware = client.chat_middleware
    middleware_items = tuple(middleware)
    configuration = deepcopy(client.function_invocation_configuration)
    request = {"message": "must not run", "correlationId": "fresh"}
    with pytest.raises(ValueError):
        await entity.run(request)
    assert provider.writes == 0 and client.received_messages == []
    callback.on_agent_response.assert_not_called()
    callback.on_streaming_response_update.assert_not_called()
    _same(provider.raw, before)
    assert entity.agent is registration and agent.client is client
    assert agent.context_providers is providers and tuple(providers) == provider_items
    assert agent.default_options == options
    assert client.chat_middleware is middleware and tuple(middleware) == middleware_items
    assert client.function_invocation_configuration == configuration
    assert request == {"message": "must not run", "correlationId": "fresh"}


@pytest.mark.parametrize("operation", ["lookup", "record", "expire", "prepare", "persist"])
def test_in_memory_map_corruption_is_rejected_before_partial_mutation_or_write(operation: str) -> None:
    provider = JsonStateProvider(_mailbox())
    state = provider.state
    state.data.completed_correlations["c"]["outcome"] = "failed"
    before_maps = deepcopy((state.data.response_mailbox, state.data.completed_correlations))
    before_storage = deepcopy(provider.raw)
    with pytest.raises(ValueError):
        if operation == "lookup":
            state.try_get_agent_response("c")
        elif operation == "record":
            state.record_response("fresh", AgentResponse(messages=[]), delivery_window_seconds=60, now=NOW)
        elif operation == "expire":
            state.expire_responses(now=NOW + timedelta(days=1))
        elif operation == "prepare":
            state.prepare_for_write(delivery_window_seconds=60)
        else:
            provider.persist_state()
    _same([state.data.response_mailbox, state.data.completed_correlations], list(before_maps))
    _same(provider.raw, before_storage)
    assert provider.writes == 0


@pytest.mark.parametrize("kind", ["content", "entry"])
def test_unknown_wire_discriminators_fail_before_core_constructors(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _mailbox()
    if kind == "content":
        raw["data"]["terminalResults"]["c"]["response"]["messages"][0]["contents"] = [
            {"$type": "future", "content": deepcopy(OPAQUE)}
        ]
    else:
        raw["data"]["conversationHistory"] = [{"$type": "future", "messages": []}]
    before = deepcopy(raw)
    forbidden = Mock(side_effect=AssertionError("No core construction before admission"))
    for core_type in (AgentResponse, Message, Content, AgentSession):
        monkeypatch.setattr(core_type, "__init__", forbidden)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    forbidden.assert_not_called()
    _same(raw, before)


def test_public_validation_never_constructs_core_objects_or_interprets_runtime_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _rich_raw()
    raw["data"]["historyBinding"] = {"version": "not a version", "ownerKind": False, "$runtimeType": "inert"}
    before = deepcopy(raw)
    forbidden = Mock(side_effect=AssertionError("Shared validation must remain structural and semantic only"))
    for core_type in (AgentResponse, Message, Content, AgentSession):
        monkeypatch.setattr(core_type, "__init__", forbidden)
    monkeypatch.setattr(importlib, "import_module", forbidden)
    validate_shared_state(raw)
    validate_shared_data(raw["data"])
    forbidden.assert_not_called()
    _same(raw, before)
