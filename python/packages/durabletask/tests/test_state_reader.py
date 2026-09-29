# Copyright (c) Microsoft. All rights reserved.

"""Read-only shared-state tests built from literal wire JSON, not mutable v2 writers."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse

from agent_framework_durabletask import _state_reader as reader_module
from agent_framework_durabletask._durable_agent_state import DurableAgentState
from agent_framework_durabletask._response_utils import is_terminal_agent_response
from agent_framework_durabletask._shared_state_validation import validate_shared_state
from agent_framework_durabletask._state_reader import SharedAgentStateReader, read_agent_state

SCHEMAS = Path(__file__).resolve().parents[4] / "schemas"
SCHEMA = json.loads((SCHEMAS / "durable-agent-entity-state.json").read_text(encoding="utf-8"))
VERSIONS = tuple(SCHEMA["properties"]["schemaVersion"]["enum"])
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
NOW = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)
COMPLETED = "2026-09-16T11:00:00Z"
EXPIRES = "2026-09-16T13:00:00Z"
OPAQUE: dict[str, Any] = {
    "$runtimeType": "not.a.RuntimeType",
    "type": "not_a_constructor",
    "nested": [None, False, 0, -0.0, "", [], {}, 2**80, "e\u0301😀"],
}
ABSENT = object()
JSON_VALUES: list[Any] = [ABSENT, None, False, 0, -0.0, "", [], {}, OPAQUE]


def _json(value: Any) -> str:
    # Equality alone conflates false, zero and floating-point zero.
    return json.dumps(value, ensure_ascii=True, sort_keys=True, allow_nan=False)


def _same(actual: Any, expected: Any) -> None:
    assert _json(actual) == _json(expected)


def _empty() -> dict[str, Any]:
    return json.loads(
        '{"schemaVersion":"2.0.0","data":{"conversationHistory":[],"terminalResults":{},"completionReceipts":{}}}'
    )


def _raw(*, outcome: str = "succeeded", expiry: bool = False, unavailable: bool = False) -> dict[str, Any]:
    raw = _empty()
    common: dict[str, Any] = {"correlationId": "c", "outcome": outcome, "completedAt": COMPLETED}
    if expiry:
        common["resultExpiresAt"] = EXPIRES
    receipt = {**common, "resultState": "unavailable" if unavailable else "available"}
    raw["data"]["completionReceipts"]["c"] = receipt
    if unavailable:
        receipt["resultUnavailableAt"] = "2026-09-16T14:00:00Z"
    else:
        result = {
            **common,
            "response": {
                "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "canonical result"}]}],
            },
        }
        if outcome == "failed":
            result["error"] = {"code": "provider_failure", "message": "Original failure.", "details": deepcopy(OPAQUE)}
        raw["data"]["terminalResults"]["c"] = result
    return raw


def _payload(raw: dict[str, Any]) -> dict[str, Any]:
    return raw["data"]["terminalResults"]["c"]["response"]


def _reader(raw: dict[str, Any]) -> SharedAgentStateReader:
    reader = read_agent_state(raw)
    assert isinstance(reader, SharedAgentStateReader)
    return reader


def _response(reader: SharedAgentStateReader, key: str = "c", *, now: datetime = NOW) -> AgentResponse[Any]:
    response = reader.try_get_agent_response(key, now=now)
    assert response is not None
    return response


def _available(response: AgentResponse, outcome: str) -> None:
    assert response.additional_properties.get("durable_status") not in ("accepted", "already_completed")
    assert is_terminal_agent_response(response) is (outcome == "failed")
    if "durable_outcome" in response.additional_properties:
        assert response.additional_properties["durable_outcome"] == outcome


def _expired(response: AgentResponse, outcome: str, key: str = "c") -> None:
    assert response.additional_properties == {
        "durable_status": "already_completed",
        "correlation_id": key,
        "durable_outcome": outcome,
    }
    assert is_terminal_agent_response(response)
    assert response.value is None
    assert len(response.messages) == 1
    assert response.messages[0].role == "system"
    assert response.messages[0].contents[0].type == "error"
    assert response.messages[0].contents[0].error_code == "response_expired"
    assert "canonical result" not in response.text


def _case_root(reference: str, value: Any) -> dict[str, Any]:
    fragment = reference.partition("#")[2]
    if not fragment:
        return deepcopy(value)
    raw = _empty()
    if fragment == "/$defs/v2ChatMessage":
        raw["data"]["conversationHistory"] = [{"$type": "response", "messages": [deepcopy(value)]}]
    elif fragment == "/$defs/terminalResponse":
        raw = _raw()
        raw["data"]["terminalResults"]["c"]["response"] = deepcopy(value)
    elif fragment == "/$defs/data/properties/ingestedPositions":
        raw["data"]["ingestedPositions"] = deepcopy(value)
    else:
        pytest.fail(f"Add a valid envelope for shared schema fragment {fragment}")
    return raw


def test_shared_corpus_enumeration_and_exact_versions() -> None:
    assert len(FIXTURES) == 4
    assert len(CASES) == 96
    assert VERSIONS == ("1.0.0", "1.1.0", "1.2.0", "2.0.0")
    assert {group["schema"]["$ref"].partition("#")[0] for _, group in CASE_GROUPS} == {SCHEMA["$id"]}
    assert {group["schema"]["$ref"].partition("#")[2] for _, group in CASE_GROUPS} == {
        "",
        "/$defs/v2ChatMessage",
        "/$defs/terminalResponse",
        "/$defs/data/properties/ingestedPositions",
    }


@pytest.mark.parametrize(("reference", "case"), CASES)
def test_shared_corpus_validator_is_separate_from_legacy_loader(reference: str, case: dict[str, Any]) -> None:
    raw = _case_root(reference, case["data"])
    before = deepcopy(raw)
    if case["valid"]:
        validate_shared_state(raw)
    else:
        with pytest.raises(ValueError):
            validate_shared_state(raw)
    _same(raw, before)
    # Legacy schema validity does not imply that the old mutable model can parse
    # every historical discriminator or preserve all optional and unknown fields.
    if raw.get("schemaVersion") == "2.0.0":
        if case["valid"]:
            _same(_reader(raw).to_dict(), before)
        else:
            with pytest.raises(ValueError):
                read_agent_state(raw)


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda path: path.name)
def test_all_fixtures_validate_but_only_v2_views_promise_lossless_snapshots(fixture: Path) -> None:
    raw = json.loads(fixture.read_text(encoding="utf-8"))
    before = deepcopy(raw)
    validate_shared_state(raw)
    if raw["schemaVersion"] == "2.0.0":
        reader = _reader(raw)
        _same(reader.to_dict(), before)
        _same(json.loads(reader.to_json()), before)
        reloaded = read_agent_state(reader.to_json())
        assert isinstance(reloaded, SharedAgentStateReader)
        _same(reloaded.to_dict(), before)
    else:
        # The real 1.2 fixture contains errorResponse/compaction, which main's
        # existing legacy enum does not support. Dispatch must not silently upgrade it.
        assert raw["schemaVersion"] == "1.2.0"
        with pytest.raises(ValueError, match="errorResponse"):
            read_agent_state(raw)
    _same(raw, before)


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("as_json", [False, True])
def test_version_dispatch_keeps_mutable_default_legacy(version: str, as_json: bool) -> None:
    raw = _empty() if version == "2.0.0" else {"schemaVersion": version, "data": {"conversationHistory": []}}
    state = read_agent_state(json.dumps(raw) if as_json else raw)
    assert state.schema_version == version
    assert isinstance(state, SharedAgentStateReader) is (version == "2.0.0")
    assert isinstance(state, DurableAgentState) is (version != "2.0.0")
    assert DurableAgentState().schema_version == "1.1.0"


@pytest.mark.parametrize("raw", [{}, "{}"])
def test_only_empty_object_deliberately_requests_fresh_state(raw: Any) -> None:
    state = read_agent_state(raw)
    assert isinstance(state, DurableAgentState)
    assert state.schema_version == "1.1.0"
    assert state.message_count == 0


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_legacy_loader_still_accepts_existing_nullable_usage_counts(version: str) -> None:
    raw = {
        "schemaVersion": version,
        "data": {
            "conversationHistory": [
                {
                    "$type": "response",
                    "correlationId": "legacy",
                    "createdAt": COMPLETED,
                    "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "legacy result"}]}],
                    "usage": {"inputTokenCount": None, "outputTokenCount": 1, "totalTokenCount": None},
                }
            ],
        },
    }
    # Do not impose v2 usage-count constraints on the existing legacy loader.
    for value in (raw, json.dumps(raw)):
        state = read_agent_state(value)
        assert isinstance(state, DurableAgentState)
        response = state.try_get_agent_response("legacy")
        assert response is not None and response.text == "legacy result"


@pytest.mark.parametrize("version", [ABSENT, None, 2, False, {}, [], "", "2", "2.0", "2.0.1", "1.3.0", "3.0.0"])
def test_missing_invalid_and_future_versions_fail_closed(version: Any) -> None:
    raw: dict[str, Any] = {"data": {}}
    if version is not ABSENT:
        raw["schemaVersion"] = version
    with pytest.raises(ValueError, match="schemaVersion"):
        read_agent_state(raw)


@pytest.mark.parametrize("raw", [None, [], 0, False, "null", "[]", "0", '"state"', "", "{invalid"])
def test_non_object_and_malformed_json_roots_fail(raw: Any) -> None:
    with pytest.raises(ValueError):
        read_agent_state(raw)


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("data", [ABSENT, None, False, [], "data"])
def test_all_version_envelopes_require_object_data(version: str, data: Any) -> None:
    raw: dict[str, Any] = {"schemaVersion": version}
    if data is not ABSENT:
        raw["data"] = data
    with pytest.raises(ValueError):
        read_agent_state(raw)


def test_view_constructor_rejects_legacy_state() -> None:
    with pytest.raises(ValueError, match="requires schemaVersion 2.0.0"):
        SharedAgentStateReader({"schemaVersion": "1.1.0", "data": {}})


@pytest.mark.parametrize("field", ["conversationHistory", "terminalResults", "completionReceipts"])
def test_v2_requires_all_canonical_collections_without_private_layout_conversion(field: str) -> None:
    raw = _raw()
    del raw["data"][field]
    raw["data"]["responseMailbox"] = {"c": {"type": "agent_response", "messages": []}}
    raw["data"]["completedCorrelations"] = {"c": {"completedAt": COMPLETED}}
    with pytest.raises(ValueError, match=field):
        read_agent_state(raw)


def test_missing_lookup_never_uses_transcript_and_count_means_entries() -> None:
    raw = _empty()
    raw["data"]["conversationHistory"] = [
        {
            "$type": "response",
            "correlationId": "c",
            "messages": [
                {"role": "assistant", "contents": [{"$type": "text", "text": "transcript only"}]},
                {"role": "assistant", "contents": []},
            ],
        },
        {"$type": "compaction"},
    ]
    reader = _reader(raw)
    assert reader.message_count == 2
    assert reader.try_get_agent_response("c") is None
    assert reader.try_get_agent_response("C") is None


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("expiry", [False, True])
def test_available_results_use_canonical_outcome_and_not_transcript(outcome: str, expiry: bool) -> None:
    raw = _raw(outcome=outcome, expiry=expiry)
    raw["data"]["conversationHistory"] = [
        {"$type": "response", "correlationId": "c", "messages": [{"role": "assistant", "contents": []}]}
    ]
    reader = _reader(raw)
    response = _response(reader)
    assert response.text == "canonical result"
    _available(response, outcome)
    if outcome == "failed":
        assert response.additional_properties["correlation_id"] == "c"
        assert response.messages[-1].contents[0].error_code == "provider_failure"
    _same(reader.to_dict(), raw)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("expiry", [False, True])
def test_unavailable_receipts_never_resurrect_transcript(outcome: str, expiry: bool) -> None:
    raw = _raw(outcome=outcome, expiry=expiry, unavailable=True)
    raw["data"]["conversationHistory"] = [
        {
            "$type": "response",
            "correlationId": "c",
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "stale transcript"}]}],
        }
    ]
    reader = _reader(raw)
    _expired(_response(reader), outcome)
    _same(reader.to_dict(), raw)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize(
    ("deadline", "now", "expired"),
    [
        ("2026-09-16T12:00:00Z", NOW - timedelta(microseconds=1), False),
        ("2026-09-16T12:00:00Z", NOW, True),
        ("2026-09-16T17:30:00+05:30", NOW, True),
        ("2026-09-16T12:00:00.000000000000000000000000000001Z", NOW, False),
        ("2026-09-16T12:00:00.123456000000000000000000000001Z", NOW.replace(microsecond=123456), False),
        ("2026-09-16T12:00:00.123456000000000000000000000001Z", NOW.replace(microsecond=123457), True),
    ],
)
def test_logical_expiry_is_exact_and_never_prunes_raw(
    outcome: str, deadline: str, now: datetime, expired: bool
) -> None:
    raw = _raw(outcome=outcome, expiry=True)
    for record in (raw["data"]["terminalResults"]["c"], raw["data"]["completionReceipts"]["c"]):
        record["resultExpiresAt"] = deadline
    reader = _reader(raw)
    response = _response(reader, now=now)
    if expired:
        _expired(response, outcome)
    else:
        assert response.text == "canonical result"
        _available(response, outcome)
    _same(reader.to_dict(), raw)


def test_default_clock_is_current_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = Mock()
    clock.now.return_value = NOW + timedelta(hours=1)
    monkeypatch.setattr(reader_module, "datetime", clock)
    response = _reader(_raw(expiry=True)).try_get_agent_response("c")
    assert response is not None
    _expired(response, "succeeded")
    clock.now.assert_called_once_with(timezone.utc)


def test_naive_expiry_clock_is_rejected() -> None:
    reader = _reader(_raw(expiry=True))
    with pytest.raises(ValueError, match="now"):
        reader.try_get_agent_response("c", now=NOW.replace(tzinfo=None))


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("hint", ["accepted", "already_completed"])
def test_live_receipt_overrides_provider_delivery_hints_only_in_projection(outcome: str, hint: str) -> None:
    raw = _raw(outcome=outcome)
    _payload(raw)["extensionData"] = {
        "durable_status": hint,
        "durable_outcome": "failed" if outcome == "succeeded" else "succeeded",
        "provider": deepcopy(OPAQUE),
    }
    reader = _reader(raw)
    response = _response(reader)
    assert response.additional_properties.get("durable_status") not in ("accepted", "already_completed")
    assert response.additional_properties["durable_outcome"] == outcome
    _available(response, outcome)
    _same(response.additional_properties["provider"], OPAQUE)
    _same(reader.to_dict(), raw)


@pytest.mark.parametrize("code", ["provider_failure", "response_expired"])
def test_live_failed_receipt_overrides_provider_expired_error(code: str) -> None:
    raw = _raw(outcome="failed")
    raw["data"]["terminalResults"]["c"]["error"]["code"] = code
    _payload(raw)["messages"][0]["contents"].append({
        "$type": "error",
        "errorCode": "response_expired",
        "message": "Provider failure, not delivery expiry.",
    })
    _payload(raw)["extensionData"] = {"durable_status": "already_completed", "durable_outcome": "succeeded"}
    reader = _reader(raw)
    response = _response(reader)
    _available(response, "failed")
    assert response.additional_properties["durable_status"] == "error"
    assert response.messages[0].contents[-1].error_code == ("agent_error" if code == "response_expired" else code)
    _same(reader.to_dict(), raw)


@pytest.mark.parametrize("evidence", ["status", "content", "expired-content"])
def test_succeeded_conflicting_failure_evidence_rejected_without_profile_activation(evidence: str) -> None:
    raw = _raw()
    if evidence == "status":
        _payload(raw)["extensionData"] = {"durable_status": "error"}
    else:
        _payload(raw)["messages"][0]["contents"].append({
            "$type": "error",
            "errorCode": "response_expired" if evidence == "expired-content" else "failure",
        })
    before = deepcopy(raw)
    # Snapshot shape alone intentionally does not classify error evidence.
    validate_shared_state(raw)
    with pytest.raises(ValueError, match="succeeded terminal result conflicts"):
        read_agent_state(raw)
    _same(raw, before)


def test_tool_error_does_not_change_succeeded_invocation() -> None:
    raw = _raw()
    _payload(raw)["messages"].append({"role": "tool", "contents": [{"$type": "error", "errorCode": "tool_error"}]})
    response = _response(_reader(raw))
    _available(response, "succeeded")


@pytest.mark.parametrize("value", JSON_VALUES)
def test_falsey_structured_values_preserve_presence_and_type(value: Any) -> None:
    raw = _raw()
    if value is not ABSENT:
        _payload(raw)["value"] = deepcopy(value)
    reader = _reader(raw)
    response = _response(reader)
    if value is ABSENT:
        assert response.value is None
        assert "value" not in _payload(reader.to_dict())
    else:
        _same(response.value, value)
        assert "value" in _payload(reader.to_dict())
    _same(reader.to_dict(), raw)


def _rich_raw() -> dict[str, Any]:
    raw = _raw(outcome="failed")
    response = _payload(raw)
    response.update(
        value=deepcopy(OPAQUE),
        createdAt="2026-09-16T12:00:00.123456789123456789Z",
        responseId="response-id",
        agentId="agent-id",
        finishReason="stop",
        continuationToken="AP8B",
        usage={"inputTokenCount": 2**80, "outputTokenCount": 0.0, "extensionData": deepcopy(OPAQUE)},
    )
    response["messages"][0].update(messageId="", authorName="", createdAt="2026-09-16T17:30:00.123456789+05:30")
    response["messages"][0]["contents"].extend([
        {"$type": "unknown", "content": deepcopy(OPAQUE)},
        {"$type": "functionCall", "callId": "call", "name": "tool", "arguments": ' { "partial": '},
    ])
    raw["data"].update(
        conversationHistory=[{"$type": "request", "messages": deepcopy(response["messages"])}],
        historyBinding=deepcopy(OPAQUE),
        session={"type": "foreign-session", "state": deepcopy(OPAQUE)},
        expirationTimeUtc=None,
        ingestedPositions={"p": 3},
        truncation={"evictedMessageCount": 1, "firstEvictedAt": COMPLETED, "lastEvictedAt": COMPLETED},
    )
    entry = raw["data"]["conversationHistory"][0]
    for node in [
        raw,
        raw["data"],
        entry,
        entry["messages"][0],
        entry["messages"][0]["contents"][0],
        raw["data"]["terminalResults"]["c"],
        raw["data"]["terminalResults"]["c"]["error"],
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


def test_complete_snapshot_detaches_input_exports_projections_and_synthesized_error_details() -> None:
    raw = _rich_raw()
    expected = deepcopy(raw)
    reader = _reader(raw)
    raw["data"]["session"]["state"]["nested"].append("input edit")
    exported = reader.to_dict()
    exported["future"]["nested"].append("export edit")
    _payload(exported)["value"]["nested"].append("value edit")
    response = _response(reader)
    assert isinstance(response.value, dict)
    response.value["nested"].append("consumer edit")
    response.additional_properties["future"].append("metadata edit")
    response.messages[0].contents[0].text = "consumer text"
    assert isinstance(response.messages[-1].contents[0].error_details, dict)
    response.messages[-1].contents[0].error_details["nested"].append("error edit")
    response.messages.clear()
    _same(reader.to_dict(), expected)
    _same(json.loads(reader.to_json()), expected)
    again = _response(reader)
    assert again.text == "canonical result"
    _same(again.value, OPAQUE)
    _same(again.messages[-1].contents[0].error_details, OPAQUE)


@pytest.mark.parametrize("name", ["data", "schema_version", "message_count"])
def test_view_has_no_public_mutable_state(name: str) -> None:
    reader = _reader(_empty())
    assert not hasattr(reader, "data")
    assert not hasattr(reader, "record_response")
    assert not hasattr(reader, "expire_responses")
    assert not hasattr(reader, "__dict__")
    with pytest.raises(AttributeError):
        setattr(reader, name, {})


@pytest.mark.parametrize("profile", JSON_VALUES[1:])
def test_unknown_history_profiles_and_session_payloads_are_inert_json(profile: Any) -> None:
    raw = _raw()
    raw["data"].update(
        historyBinding=deepcopy(profile), session={"$runtimeType": "inert", "payload": deepcopy(profile)}
    )
    _payload(raw)["messages"][0]["contents"].append({"$type": "unknown", "content": deepcopy(profile)})
    reader = _reader(raw)
    response = _response(reader)
    assert response.messages[0].contents[-1].type == "unknown"
    _same(response.messages[0].contents[-1].additional_properties["content"], profile)
    assert not response.user_input_requests
    _same(reader.to_dict(), raw)


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("profile_case", ["response", "message", "content", "continuation"])
def test_native_profiles_stay_inert_until_a_live_targeted_lookup(
    expired: bool, profile_case: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _raw(expiry=expired)
    # This identified profile is schema-valid but cannot be projected. Storage
    # and missing/expired lookups must not decode it as a global precondition.
    payload = _payload(raw)
    if profile_case == "continuation":
        payload["continuationToken"] = "bnVsbA=="
        payload["pythonContinuationEncoding"] = {
            "profile": "agent-framework-python.continuation",
            "version": 1,
            "format": "json",
        }
    else:
        node = payload
        if profile_case in ("message", "content"):
            node = payload["messages"][0]
        if profile_case == "content":
            node = node["contents"][0]
        node["pythonCoreFields"] = {"profile": "agent-framework-python.core-fields", "version": 1, "fields": None}
    raw["data"]["terminalResults"]["other"] = {
        "correlationId": "other",
        "outcome": "succeeded",
        "completedAt": COMPLETED,
        "response": {"messages": []},
    }
    raw["data"]["completionReceipts"]["other"] = {
        "correlationId": "other",
        "outcome": "succeeded",
        "completedAt": COMPLETED,
        "resultState": "available",
    }
    decoder = Mock(wraps=reader_module.load_terminal_response)
    monkeypatch.setattr(reader_module, "load_terminal_response", decoder)
    reader = _reader(raw)
    assert reader.try_get_agent_response("missing") is None
    _same(reader.to_dict(), raw)
    _same(json.loads(reader.to_json()), raw)
    decoder.assert_not_called()
    if expired:
        _expired(_response(reader, now=NOW + timedelta(hours=1)), "succeeded")
        decoder.assert_not_called()
    _response(reader, "other")
    decoder.assert_called_once_with({"messages": []})
    if not expired:
        with pytest.raises(ValueError):
            _response(reader)
    _same(reader.to_dict(), raw)


def test_native_error_projection_cannot_reclassify_a_succeeded_receipt() -> None:
    raw = _raw()
    _payload(raw)["messages"][0]["contents"].append({
        "$type": "unknown",
        "content": {"type": "error", "error_code": "native_failure", "message": "Native profile error."},
        "pythonContentEncoding": {"profile": "agent-framework-python.content", "version": 1},
    })
    reader = _reader(raw)
    assert reader.try_get_agent_response("missing") is None
    with pytest.raises(ValueError, match="succeeded terminal result conflicts"):
        _response(reader)
    _same(reader.to_dict(), raw)


@pytest.mark.parametrize("token", ["", "AQID", "AP8B", "eyJmb3JlaWduIjpmYWxzZX0="])
def test_foreign_continuation_tokens_are_not_resumable_core_tokens(token: str) -> None:
    raw = _raw()
    _payload(raw)["continuationToken"] = token
    _payload(raw)["pythonContinuationEncoding"] = {"profile": "foreign", "version": 99}
    reader = _reader(raw)
    assert _response(reader).continuation_token is None
    assert _payload(reader.to_dict())["continuationToken"] == token
    _same(reader.to_dict(), raw)


@pytest.mark.parametrize("identifier", [None, False, 1, "", " ", "id\n", "id\u0085", "x" * 257])
def test_public_lookup_identifiers_are_validated(identifier: Any) -> None:
    with pytest.raises(ValueError):
        _reader(_empty()).try_get_agent_response(identifier)


@pytest.mark.parametrize("identifier", ["Case", "case", " padded ", "x" * 256, "😀"])
def test_public_lookup_identifiers_are_exact_not_trimmed_or_case_folded(identifier: str) -> None:
    raw = _raw()
    for field in ("terminalResults", "completionReceipts"):
        record = raw["data"][field].pop("c")
        record["correlationId"] = identifier
        raw["data"][field][identifier] = record
    reader = _reader(raw)
    assert _response(reader, identifier).text == "canonical result"
    assert reader.try_get_agent_response("missing") is None
    if identifier == "Case":
        assert reader.try_get_agent_response("case") is None
    if identifier == " padded ":
        assert reader.try_get_agent_response("padded") is None


@pytest.mark.parametrize("field", ["responseId", "agentId", "finishReason", "extensionData"])
@pytest.mark.parametrize("identifier", ["", " ", "id\n", "id\u0085", "x" * 257])
def test_terminal_public_identifiers_reject_invalid_values(field: str, identifier: str) -> None:
    raw = _raw()
    _payload(raw)[field] = {identifier: None} if field == "extensionData" else identifier
    with pytest.raises(ValueError):
        read_agent_state(raw)


@pytest.mark.parametrize("number", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("as_json", [False, True])
def test_nonfinite_unknown_values_fail_before_export_or_lookup(number: float, as_json: bool) -> None:
    raw = _raw()
    raw["future"] = {"nested": [number]}
    with pytest.raises(ValueError, match="finite"):
        read_agent_state(json.dumps(raw) if as_json else raw)


@pytest.mark.parametrize("value", [{1: "non-string key"}, ("tuple",), object()])
def test_non_json_opaque_values_are_rejected(value: Any) -> None:
    raw = _raw()
    raw["future"] = value
    with pytest.raises(ValueError):
        read_agent_state(raw)


def test_cyclic_opaque_values_are_rejected_before_deepcopy() -> None:
    raw = _raw()
    raw["future"] = raw
    with pytest.raises(ValueError, match="cycles"):
        read_agent_state(raw)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"messages": None},
        {"messages": {}},
        {"messages": [{"role": "assistant", "contents": [{"type": "text", "text": "Core is not shared wire"}]}]},
    ],
)
def test_malformed_shared_response_envelopes_are_not_reinterpreted(payload: dict[str, Any]) -> None:
    raw = _raw()
    raw["data"]["terminalResults"]["c"]["response"] = payload
    with pytest.raises(ValueError):
        read_agent_state(raw)


@pytest.mark.parametrize(
    "defect", ["outcome", "key", "missing-result", "unavailable-with-result", "completion", "expiry"]
)
def test_entire_snapshot_is_validated_before_any_lookup(defect: str) -> None:
    raw = _raw(expiry=True)
    receipt = raw["data"]["completionReceipts"]["c"]
    if defect == "outcome":
        receipt["outcome"] = "failed"
    elif defect == "key":
        receipt["correlationId"] = "different"
    elif defect == "missing-result":
        raw["data"]["terminalResults"].clear()
    elif defect == "unavailable-with-result":
        receipt.update(resultState="unavailable", resultUnavailableAt="2026-09-16T14:00:00Z")
    elif defect == "completion":
        receipt["completedAt"] = "2026-09-16T11:00:01Z"
    else:
        receipt["resultExpiresAt"] = "2026-09-16T13:00:00.000000000000000000000000000001Z"
    with pytest.raises(ValueError):
        read_agent_state(raw)
