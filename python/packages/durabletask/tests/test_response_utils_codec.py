# Copyright (c) Microsoft. All rights reserved.

"""Compatibility checks for the fixed base-response codec, without host integration."""

import base64
import builtins
import importlib
import json
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, Content, Message
from pydantic import BaseModel, ConfigDict, Field, Json, RootModel, ValidationError

from agent_framework_durabletask._response_utils import (
    ensure_response_format,
    load_agent_response,
    serialize_agent_response,
)
from agent_framework_durabletask._shared_response import load_terminal_response, serialize_terminal_response
from agent_framework_durabletask._shared_state_validation import validate_shared_data, validate_shared_state

SCHEMAS = Path(__file__).resolve().parents[4] / "schemas"


class AliasCount(BaseModel):
    count: int = Field(alias="aliasCount")


class JsonValue(BaseModel):
    document: Json[list[int]]


class NullValue(RootModel[None]):
    pass


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"type": "agent_response"},
        {"type": "custom_response"},
        {"messages": None},
        {"messages": []},
        {"value": False},
        {"value": None},
        {"response_id": "response", "additional_properties": {"provider": [None, False]}},
    ],
)
def test_sparse_legacy_payloads_still_match_the_base_constructor(payload: dict[str, Any]) -> None:
    before = deepcopy(payload)
    try:
        expected = AgentResponse.from_dict(deepcopy(payload))
    except (ValueError, TypeError) as exc:
        with pytest.raises(type(exc)):
            load_agent_response(payload)
        assert payload == before
        return
    actual = load_agent_response(payload)
    assert type(actual) is AgentResponse
    assert actual.to_dict() == expected.to_dict()
    assert actual.value == expected.value
    assert payload == before


@pytest.mark.parametrize("response_type", [None, "", False, 0, [], {}, "provider.CustomResponse", True, 1])
def test_present_response_type_requires_the_fixed_base_discriminator(response_type: Any) -> None:
    payload = {"type": response_type, "messages": [{"role": "assistant", "contents": ["answer"]}]}
    before = deepcopy(payload)
    with pytest.raises(ValueError, match="Response type"):
        load_agent_response(payload)
    assert payload == before


@pytest.mark.parametrize("shape", ["single", "list", "tuple", "mapping"])
def test_existing_message_inputs_and_raw_host_payloads_remain_readable(shape: str) -> None:
    message = Message("assistant", ["answer"], additional_properties={"provider": {"n": False}})
    inputs: dict[str, Any] = {
        "single": message,
        "list": [message],
        "tuple": (message,),
        "mapping": [message.to_dict()],
    }
    actual = load_agent_response({"messages": inputs[shape]})
    assert actual.to_dict() == AgentResponse(messages=[message]).to_dict()
    # Hosts retain their existing Core serializer rather than invoking either new serializer.
    assert load_agent_response(AgentResponse(messages=[message]).to_dict()).to_dict() == actual.to_dict()


@pytest.mark.parametrize("data", [b"", b"\x00\xff", b"AQID"])
def test_legacy_inline_data_still_uses_the_fixed_core_data_factory(data: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    raw = {"type": "data", "data": data, "media_type": "application/octet-stream"}
    expected = Content.from_dict(raw)
    forbidden = Mock(side_effect=AssertionError("Do not use polymorphic deserialization"))
    for cls in (AgentResponse, Message, Content):
        monkeypatch.setattr(cls, "from_dict", forbidden)
    loaded = load_agent_response({"messages": [{"role": "assistant", "contents": [raw]}]})
    assert type(loaded.messages[0].contents[0]) is Content
    assert loaded.messages[0].contents[0].to_dict() == expected.to_dict()
    forbidden.assert_not_called()


def test_legacy_inline_data_still_rejects_text_instead_of_decoding_it_as_bytes() -> None:
    raw = {"type": "data", "data": "AQID", "media_type": "application/octet-stream"}
    with pytest.raises(TypeError):
        Content.from_dict(raw)
    with pytest.raises(TypeError):
        load_agent_response({"messages": [{"role": "assistant", "contents": [raw]}]})


@pytest.mark.parametrize("response_format", [AliasCount, {"type": "object"}])
def test_caller_supplied_legacy_response_format_keeps_lazy_parsing(response_format: Any) -> None:
    payload = {
        "messages": [{"role": "assistant", "contents": ['{"aliasCount":7}']}],
        "response_format": response_format,
    }
    expected = AgentResponse.from_dict(deepcopy(payload))
    loaded = load_agent_response(payload)
    assert loaded.value == expected.value
    assert type(loaded.value) is type(expected.value)


def test_existing_instance_is_returned_unchanged_and_none_still_fails() -> None:
    response = AgentResponse()
    assert load_agent_response(response) is response
    with pytest.raises(ValueError, match="cannot be None"):
        load_agent_response(None)


@pytest.mark.parametrize("payload", [[], "response", 1])
def test_unsupported_top_level_inputs_still_fail(payload: Any) -> None:
    with pytest.raises(TypeError, match="Unsupported type"):
        load_agent_response(payload)


@pytest.mark.parametrize("version", [None, True, 0, 2, "1", {}, []])
def test_invalid_optional_delivery_versions_fail_without_mutation(version: Any) -> None:
    payload = {"type": "agent_response", "_durable_response_version": version}
    before = deepcopy(payload)
    with pytest.raises(ValueError, match="Unsupported durable response version"):
        load_agent_response(payload)
    assert payload == before


def test_supported_delivery_version_is_read_only_and_not_written() -> None:
    response = load_agent_response({"type": "agent_response", "_durable_response_version": 1})
    assert "_durable_response_version" not in serialize_agent_response(response)


@pytest.mark.parametrize(
    "value",
    [AliasCount(aliasCount=7), JsonValue(document="[1,2]"), NullValue(root=None)],
    ids=["alias", "json-field", "null-root"],
)
def test_structured_values_survive_inline_json_delivery_without_reparsing_text(value: BaseModel) -> None:
    response = AgentResponse(messages=[Message("assistant", ["not the structured value"])], value=value)
    payload = json.loads(json.dumps(serialize_agent_response(response), allow_nan=False))
    assert payload["value"] == value.model_dump(mode="json", by_alias=True, round_trip=True)
    loaded = load_agent_response(payload)
    ensure_response_format(type(value), "codec", loaded)
    assert type(loaded.value) is type(value)
    assert loaded.value == value


@pytest.mark.parametrize("defaulted", [False, True])
def test_distinct_serialization_aliases_roundtrip_through_shared_profile(defaulted: bool) -> None:
    default: Any = 0 if defaulted else ...

    class SeparateAliases(BaseModel):
        count: int = Field(default=default, validation_alias="inputCount", serialization_alias="outputCount")

    class NestedValue(BaseModel):
        child: SeparateAliases = Field(alias="nestedChild")
        values: list[SeparateAliases]

    value = NestedValue(nestedChild={"inputCount": 7}, values=[SeparateAliases(inputCount=8)])
    snapshot = serialize_agent_response(AgentResponse[Any](value=value))
    assert snapshot["value"] == {"child": {"count": 7}, "values": [{"count": 8}]}
    assert snapshot["_durable_value_by_name"] is True
    loaded = load_terminal_response(serialize_terminal_response(snapshot))
    ensure_response_format(NestedValue, "codec", loaded)
    assert loaded.value == value


def test_strict_retained_values_are_validated_as_json() -> None:
    class StrictValue(BaseModel):
        model_config = ConfigDict(strict=True)
        day: date
        coordinates: tuple[int, int]

    value = StrictValue(day=date(2026, 9, 16), coordinates=(1, 2))
    loaded = load_agent_response(serialize_agent_response(AgentResponse[Any](value=value)))
    ensure_response_format(StrictValue, "codec", loaded)
    assert loaded.value == value


@pytest.mark.parametrize("value", [None, False, 0, -0.0, "", [], {}, {"type": "text", "future": [None]}])
def test_retained_value_presence_precedes_conflicting_valid_message_text(value: Any) -> None:
    loaded = load_agent_response({"messages": [{"role": "assistant", "contents": ["42"]}], "value": value})
    snapshot = serialize_agent_response(loaded)
    assert "value" in snapshot and type(snapshot["value"]) is type(value)
    ensure_response_format(RootModel[Any], "codec", loaded)
    assert isinstance(loaded.value, RootModel)
    assert loaded.value.root == value
    assert type(loaded.value.root) is type(value)


def test_explicit_null_is_not_replaced_by_conflicting_text() -> None:
    loaded = load_agent_response({
        "messages": [{"role": "assistant", "contents": ['{"aliasCount":7}']}],
        "value": None,
    })
    with pytest.raises(ValidationError):
        ensure_response_format(AliasCount, "codec", loaded)


def test_absent_value_uses_the_requested_format_without_evaluating_the_old_format() -> None:
    response = AgentResponse(messages=[Message("assistant", ['{"aliasCount":7}'])], response_format=JsonValue)
    ensure_response_format(AliasCount, "codec", response)
    assert response.value == AliasCount(aliasCount=7)


def test_no_format_is_a_noop_and_matching_model_is_retained() -> None:
    value = AliasCount(aliasCount=7)
    response = AgentResponse(value=value)
    before = dict(vars(response))
    ensure_response_format(None, "codec", response)
    assert vars(response) == before
    ensure_response_format(AliasCount, "codec", response)
    assert response.value is value


@pytest.mark.parametrize("valid", [False, True])
def test_legacy_tool_errors_do_not_bypass_structured_text_validation(valid: bool) -> None:
    response = AgentResponse(
        messages=[
            Message("tool", [Content.from_error(message="retryable")]),
            Message("assistant", ['{"aliasCount":7}' if valid else "invalid JSON"]),
        ]
    )
    loaded = load_agent_response(response.to_dict())
    if valid:
        ensure_response_format(AliasCount, "codec", loaded)
        assert loaded.value == AliasCount(aliasCount=7)
    else:
        with pytest.raises(ValueError):
            ensure_response_format(AliasCount, "codec", loaded)


@pytest.mark.parametrize("kind", ["error", "already_completed", "accepted", "approval"])
def test_non_result_responses_do_not_trigger_typed_parsing(kind: str) -> None:
    content = (
        Content.from_function_approval_request("approval", Content.from_function_call("call", "tool"))
        if kind == "approval"
        else Content.from_text("not JSON")
    )
    response = AgentResponse(
        messages=[Message("assistant", [content])],
        response_format=AliasCount,
        additional_properties={} if kind == "approval" else {"durable_status": kind},
    )
    before = dict(vars(response))
    snapshot = serialize_agent_response(response)
    assert "value" not in snapshot
    assert vars(response) == before
    loaded = load_agent_response(snapshot)
    if kind == "approval":
        # An ordinary legacy inline approval did not bypass required formatting.
        with pytest.raises(ValueError, match="required format"):
            ensure_response_format(AliasCount, "codec", loaded)
        return
    ensure_response_format(AliasCount, "codec", loaded)
    assert loaded.value is None


def test_base_projection_ignores_unknown_fields_without_mutating_raw_json() -> None:
    raw = {
        "type": "agent_response",
        "future_response": [None],
        "messages": [
            {
                "type": "untrusted.Message",
                "role": "assistant",
                "future_message": [False],
                "contents": [{"type": "text", "text": "answer", "future_content": [0]}],
            }
        ],
        "value": {"type": "text", "business": [None]},
        "additional_properties": {"future": [False]},
    }
    before = deepcopy(raw)
    loaded = load_agent_response(raw)
    assert not hasattr(loaded, "future_response")
    assert not hasattr(loaded.messages[0], "future_message")
    assert not hasattr(loaded.messages[0].contents[0], "future_content")
    loaded.messages[0].contents[0].text = "edited"
    loaded.additional_properties["future"].append(0)
    assert isinstance(loaded.value, dict)
    loaded.value["business"].append(False)
    assert raw == before


@pytest.mark.parametrize("recognized", [False, True])
def test_continuations_and_business_markers_never_import_or_decode_pickle(
    recognized: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A protocol-0 reduction targeting the test sentinel must remain opaque.
    opaque_pickle = base64.b64encode(b"cbuiltins\ncodec_probe\n(tR.").decode("ascii")
    business = {"type": "untrusted.Payload", "__pickled__": opaque_pickle, "$runtimeType": "untrusted.Type"}
    profile = {"profile": "agent-framework-python.continuation", "version": 1, "format": "json"}
    payload: dict[str, Any] = {
        "messages": [{"role": "assistant", "contents": [{"$type": "unknown", "content": business}]}],
        "value": business,
        "continuationToken": "e30=" if recognized else opaque_pickle,
        "pythonContinuationEncoding": profile if recognized else {"profile": "foreign", "version": 1},
    }
    forbidden = Mock(side_effect=AssertionError("No runtime type imports or pickle activation"))
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name in ("pickle", "_pickle", "cloudpickle", "dill") or name.startswith("untrusted"):
            return forbidden(name)
        return original_import(name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(builtins, "__import__", guarded_import)
        patch.setattr(importlib, "import_module", forbidden)
        patch.setattr(builtins, "codec_probe", forbidden, raising=False)
        for cls in (AgentResponse, Message, Content):
            patch.setattr(cls, "from_dict", forbidden)
        loaded = load_terminal_response(payload)
        assert loaded.continuation_token == ({} if recognized else None)
        assert serialize_terminal_response(loaded) == payload
        assert loaded.value == business
    forbidden.assert_not_called()


@pytest.mark.parametrize("fixture", sorted((SCHEMAS / "fixtures").glob("shared-durable-agent-state-*.json")))
def test_snapshot_validator_accepts_existing_literal_fixtures_without_runtime_projection(fixture: Path) -> None:
    raw = json.loads(fixture.read_text(encoding="utf-8"))
    before = json.dumps(raw, sort_keys=True, allow_nan=False)
    validate_shared_state(raw)
    validate_shared_data(raw["data"], version=raw["schemaVersion"])
    assert json.dumps(raw, sort_keys=True, allow_nan=False) == before


def _snapshot() -> dict[str, Any]:
    common = {"correlationId": "c", "outcome": "succeeded", "completedAt": "2026-09-16T00:00:00Z"}
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "terminalResults": {"c": {**common, "response": {"messages": [], "value": None}}},
            "completionReceipts": {"c": {**common, "resultState": "available"}},
        },
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("correlationId", "C"),
        ("outcome", "failed"),
        ("completedAt", "2026-09-16T00:00:00.000000001Z"),
        ("resultExpiresAt", "2026-09-16T00:01:00Z"),
        ("resultState", "unavailable"),
    ],
)
def test_snapshot_validator_rejects_inconsistent_receipts_without_mutation(field: str, value: Any) -> None:
    raw = _snapshot()
    validate_shared_state(raw)
    raw["data"]["completionReceipts"]["c"][field] = value
    before = deepcopy(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    assert raw == before


@pytest.mark.parametrize("fraction", ["1", "123456789", "123456789012345678901234567890123456789"])
def test_snapshot_timestamp_comparison_retains_exact_fraction_and_offset(fraction: str) -> None:
    raw = _snapshot()
    raw["data"]["terminalResults"]["c"]["completedAt"] = f"2026-09-16t00:00:00.{fraction}z"
    raw["data"]["completionReceipts"]["c"]["completedAt"] = f"2026-09-16T05:30:00.{fraction}0+05:30"
    before = deepcopy(raw)
    validate_shared_state(raw)
    assert raw == before


def test_snapshot_validation_preserves_opaque_profiles_without_loading_them(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _snapshot()
    raw["data"]["terminalResults"]["c"]["response"].update({
        "continuationToken": "bm90IEpTT04=",
        "pythonContinuationEncoding": {
            "profile": "agent-framework-python.continuation",
            "version": 1,
            "format": "json",
        },
        "pythonCoreFields": {"profile": "agent-framework-python.core-fields", "version": 1, "fields": None},
    })
    before = deepcopy(raw)
    forbidden = Mock(side_effect=AssertionError("Snapshot validation must not construct response projections"))
    for cls in (AgentResponse, Message, Content):
        monkeypatch.setattr(cls, "__init__", forbidden)
    validate_shared_state(raw)
    assert raw == before
    forbidden.assert_not_called()


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_sparse_legacy_snapshot_is_not_upgraded_or_filled(version: str) -> None:
    raw = {"schemaVersion": version, "data": {}}
    validate_shared_state(raw)
    assert raw == {"schemaVersion": version, "data": {}}


def test_snapshot_json_rejects_cycles_but_not_shared_subtrees() -> None:
    raw = _snapshot()
    shared: list[Any] = [None, False]
    raw["data"].update(session={"same": shared}, historyBinding=shared)
    validate_shared_state(raw)
    shared.append(shared)
    with pytest.raises(ValueError, match="cycle"):
        validate_shared_state(raw)
    assert shared[-1] is shared
