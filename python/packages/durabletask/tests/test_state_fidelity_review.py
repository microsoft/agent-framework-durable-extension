# Copyright (c) Microsoft. All rights reserved.

"""Transcript and mailbox fidelity across a real JSON storage boundary."""

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast, get_args, get_type_hints

import jsonschema
import pytest
from agent_framework import AgentResponse, Content, Message
from pydantic import BaseModel, Field

from agent_framework_durabletask import DurableHistoryProvider
from agent_framework_durabletask._constants import ContentTypes
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateContent,
    DurableAgentStateEntryJsonType,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateTextContent,
    DurableAgentStateUnknownContent,
    DurableAgentStateUsage,
)
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._models import RunRequest
from agent_framework_durabletask._response_utils import ensure_response_format

NOW = datetime(2026, 9, 9, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def schema() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _state(message: Message) -> DurableAgentState:
    state = DurableAgentState()
    state.data.conversation_history = [
        DurableAgentStateResponse.from_run_response(
            "response", AgentResponse(messages=[message], created_at=NOW.isoformat())
        )
    ]
    return state


def _stored_message(state: DurableAgentState) -> DurableAgentStateMessage:
    return state.data.conversation_history[0].messages[0]


def _cold(state: DurableAgentState, schema: dict[str, Any]) -> DurableAgentState:
    payload = json.loads(state.to_json())
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(payload)
    return DurableAgentState.from_dict(payload)


def _core_fields(raw: dict[str, Any]) -> dict[str, Any]:
    profile = raw["pythonCoreFields"]
    assert profile["profile"] == "agent-framework-python.core-fields"
    assert type(profile["version"]) is int and profile["version"] == 1
    assert isinstance(profile["fields"], dict)
    return profile["fields"]


@pytest.mark.parametrize("kind", get_args(get_type_hints(Content.__init__)["type"]))
def test_every_core_kind_preserves_metadata_in_transcript(kind: Any, schema: dict[str, Any]) -> None:
    citation = {
        "type": "citation",
        "title": "Source",
        "url": "https://example.test/source",
        "annotated_regions": [{"type": "text_span", "start_index": 0, "end_index": 4, "future": [1]}],
        "additional_properties": {"type": "opaque", "nested": [2]},
    }
    # Exercise cross-subtype fields too. Core's constructor, not a copied list of kinds,
    # defines the category; typed durable fields must coexist with the remaining fields.
    content = Content(
        kind,
        text="body",
        uri="data:image/png;base64,AA==",
        call_id="call",
        name="lookup",
        file_id="file",
        vector_store_id="vector",
        usage_details={"input_token_count": 0},
        protected_data="protected",
        informational_only=True,
        id="content",
        exception="retry",
        annotations=[cast(Any, citation)],
        additional_properties={"type": "text", "future": {"keep": [3]}},
        raw_representation=object(),
    )
    original = Message(
        "developer",
        [content],
        author_name="author",
        message_id="id",
        additional_properties={"nested": {"keep": [4]}},
        raw_representation=object(),
    )
    restored = _stored_message(_cold(_state(original), schema)).to_chat_message()

    assert restored.to_dict() == original.to_dict()
    restored.additional_properties["nested"]["keep"].append(5)
    assert original.additional_properties["nested"]["keep"] == [4]


def test_typed_content_mapping_covers_shared_schema_kinds(schema: dict[str, Any]) -> None:
    known = {value for key, value in vars(ContentTypes).items() if key.isupper()}
    branches = schema["$defs"]["v2ChatContentItem"]["oneOf"]

    def definition(branch: dict[str, Any]) -> dict[str, Any]:
        while "$ref" in branch:
            branch = schema["$defs"][branch["$ref"].split("/")[-1]]
        return branch

    declared = {definition(branch)["properties"]["$type"]["const"] for branch in branches}
    assert declared == known
    assert all("$ref" in branch for branch in branches), "v2 admits only declared discriminators"
    assert {cls.type for cls in DurableAgentStateContent.__subclasses__() if cls.type} == known
    for kind in known - {"unknown"}:
        core_kind = {
            "functionCall": "function_call",
            "functionResult": "function_result",
            "hostedFile": "hosted_file",
            "hostedVectorStore": "hosted_vector_store",
            "reasoning": "text_reasoning",
        }.get(kind, kind)
        content = Content(
            core_kind,
            text="text",
            uri="https://example.test",
            call_id="c",
            name="f",
            file_id="file",
            vector_store_id="vector",
            usage_details={},
        )
        stored = DurableAgentStateContent.from_ai_content(content)
        assert stored.type == kind
        assert not isinstance(stored, DurableAgentStateUnknownContent)


def test_function_result_retains_binary_and_text_items_without_a_transcript_mirror(schema: dict[str, Any]) -> None:
    content = Content.from_function_result(
        "call",
        result=[Content.from_text("answer"), Content.from_data(b"\x00\xff", "image/png")],
        exception="recoverable",
        additional_properties={"future": [1]},
    )
    original = Message("tool", [content])
    state = _state(original)
    raw = _stored_message(state).to_dict()["contents"][0]

    assert raw["$type"] == "functionResult"
    assert raw["result"] == content.result
    overlay = _core_fields(raw)
    assert overlay["items"] == content.to_dict()["items"]
    assert not {"type", "call_id", "result"} & overlay.keys()
    assert not {"coreMessage", "core_message"} & _stored_message(state).to_dict().keys()
    restored = _stored_message(_cold(state, schema)).to_chat_message()
    assert restored.to_dict() == original.to_dict()
    assert restored.contents[0].items is not None
    assert isinstance(restored.contents[0].items[1], Content)


def test_current_known_content_changes_win_over_metadata(schema: dict[str, Any]) -> None:
    original = Message("assistant", [Content.from_text("original", additional_properties={"nested": [1]})])
    cold = _cold(_state(original), schema)
    stored = _stored_message(cold)
    assert isinstance(stored.contents[0], DurableAgentStateTextContent)
    raw = stored.to_dict()["contents"][0]
    assert "text" not in _core_fields(raw)
    stored.contents[0].text = "edited"
    restored = _stored_message(_cold(cold, schema)).to_chat_message()
    assert restored.text == "edited"
    assert restored.contents[0].additional_properties == {"nested": [1]}
    restored.contents[0].additional_properties["nested"].append(2)
    assert _core_fields(stored.to_dict()["contents"][0])["additional_properties"] == {"nested": [1]}


@pytest.mark.parametrize("arguments", [None, {}, {"type": "text", "opaque": [1]}, '{"x":1}', "{unfinished"])
def test_arguments_remain_exact_not_reparsed_or_reformatted(arguments: Any, schema: dict[str, Any]) -> None:
    original = Message("assistant", [Content.from_function_call("call", "f", arguments=arguments)])
    restored = _stored_message(_cold(_state(original), schema)).to_chat_message()
    assert restored.to_dict() == original.to_dict()


def test_uri_without_media_type_and_partial_usage_validate(schema: dict[str, Any]) -> None:
    original = Message(
        "user", [Content.from_uri("https://example.test/file"), Content.from_usage({"input_token_count": 0})]
    )
    restored = _cold(_state(original), schema)
    assert _stored_message(restored).to_chat_message().to_dict() == original.to_dict()
    usage = DurableAgentStateUsage.from_dict({"inputTokenCount": 0, "future": {"nested": [1]}})
    assert usage.to_dict() == {"inputTokenCount": 0, "future": {"nested": [1]}}
    assert usage.to_usage_details() == {"input_token_count": 0}


@pytest.mark.parametrize("kind", list(DurableAgentStateEntryJsonType))
def test_unknown_fields_are_owned_by_actual_entry_subtype(kind: str, schema: dict[str, Any]) -> None:
    entry: dict[str, Any] = {
        "$type": kind,
        "createdAt": NOW.isoformat(),
        "messages": [
            {
                "role": "assistant",
                "futureMessage": {"nested": [1]},
                "contents": [
                    {"$type": "text", "text": "hello", "callId": {"future": [2]}, "futureContent": {"nested": [3]}}
                ],
            }
        ],
        "futureEntry": {"nested": [4]},
    }
    if kind != "request":
        entry.update(responseType="future-format", responseSchema={"future": [5]}, orchestrationId="future-id")
    if kind not in ("response", "errorResponse"):
        entry["usage"] = {"future": {"nested": [6]}}
    else:
        entry["usage"] = {"inputTokenCount": 1, "future": {"nested": [6]}}
    payload = {
        "schemaVersion": "2.0.0",
        "data": {"conversationHistory": [entry], "terminalResults": {}, "completionReceipts": {}},
    }
    before = deepcopy(payload)
    loaded = DurableAgentState.from_dict(payload)
    assert _cold(loaded, schema).to_dict() == before
    serialized = loaded.to_dict()
    serialized["data"]["conversationHistory"][0]["messages"][0]["futureMessage"]["nested"].append(9)
    assert payload == before
    assert loaded.to_dict() == before


def test_core_context_preserves_nested_future_items_before_consumer_filtering(schema: dict[str, Any]) -> None:
    raw: dict[str, Any] = {
        "role": "tool",
        "future_message": {"nested": [1]},
        "contents": [
            {
                "type": "function_result",
                "call_id": "call",
                "result": "text",
                "items": [
                    {"type": "text", "text": "text", "future_content": {"nested": [2]}},
                    Content.from_data(b"data", "image/png").to_dict(),
                ],
                "future_outer": {"nested": [3]},
            }
        ],
        "additional_properties": {"nested": [4]},
    }
    request = RunRequest("", "c", context_messages=[raw])
    entry = DurableAgentStateRequest.from_run_request(request)
    assert entry.messages[0].message_id is None
    assert entry.messages[0].ingestion_identity == message_identity(entry.messages[0].to_chat_message())
    state = DurableAgentState()
    state.data.conversation_history = [entry]
    loaded = _cold(state, schema)
    stored = _stored_message(loaded).to_dict()
    assert stored["future_message"] == raw["future_message"]
    overlay = _core_fields(stored["contents"][0])
    assert overlay["items"] == raw["contents"][0]["items"]
    assert overlay["future_outer"] == {"nested": [3]}
    items = _stored_message(loaded).to_chat_message().contents[0].items
    assert items is not None
    assert items[0].text == "text"
    assert loaded.to_dict() == state.to_dict()


@pytest.mark.parametrize("level", ["entry", "content"])
def test_v2_rejects_unknown_discriminators_without_losing_the_original_snapshot(
    level: str, schema: dict[str, Any]
) -> None:
    state = _state(Message("assistant", ["hello"]))
    raw = state.to_dict()
    if level == "entry":
        raw["data"]["conversationHistory"].append({"$type": "futureEntry", "messages": [], "future": [None]})
    else:
        raw["data"]["conversationHistory"][0]["messages"][0]["contents"].append({
            "$type": "futureContent",
            "payload": None,
            "items": {"futureShape": [2]},
        })
    before = deepcopy(raw)
    assert not jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).is_valid(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_json(json.dumps(raw))
    assert raw == before


@pytest.mark.parametrize("level", ["history", "messages", "contents"])
@pytest.mark.parametrize("malformed", [None, False, 7, "text", {}, [None], [42], ["text"]])
def test_malformed_transcript_containers_fail_instead_of_dropping_data(level: str, malformed: Any) -> None:
    raw = _state(Message("assistant", ["hello"])).to_dict()
    data = raw["data"]
    if level == "history":
        data["conversationHistory"] = malformed
    elif level == "messages":
        data["conversationHistory"][0]["messages"] = malformed
    else:
        data["conversationHistory"][0]["messages"][0]["contents"] = malformed
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("number", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("location", ["root", "session", "content"])
def test_nonfinite_json_is_rejected_including_unknown_fields(number: float, location: str) -> None:
    raw = _state(Message("assistant", ["hello"])).to_dict()
    target = raw
    if location == "session":
        raw["data"]["session"] = target = {}
    elif location == "content":
        target = raw["data"]["conversationHistory"][0]["messages"][0]["contents"][0]
    target["future"] = {"nested": [number]}
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_json(json.dumps(raw))


def _mailbox() -> dict[str, Any]:
    state = DurableAgentState()
    state.record_response(
        "c", AgentResponse(messages=[Message("assistant", ["42"])]), delivery_window_seconds=60, now=NOW
    )
    raw = state.to_dict()
    for field in ("terminalResults", "completionReceipts"):
        raw["data"][field]["c"].update(completedAt="2026-09-09T00:00:00Z", resultExpiresAt="2099-01-01T00:00:00Z")
    return raw


def test_poll_uses_versioned_loader_preserving_null_and_future_envelope_fields(schema: dict[str, Any]) -> None:
    raw = _mailbox()
    response = raw["data"]["terminalResults"]["c"]["response"]
    response.update(value=None, future_response={"nested": [1]})
    response["messages"][0]["future_message"] = {"nested": [2]}
    response["messages"][0]["contents"][0]["future_content"] = {"nested": [3]}
    loaded = _cold(DurableAgentState.from_dict(raw), schema)
    result = loaded.try_get_agent_response("c")
    assert type(result) is AgentResponse
    assert result.value is None
    result.messages[0].contents[0].text = "changed"
    assert loaded.to_dict() == raw
    loaded.expire_responses(now=datetime(2100, 1, 1, tzinfo=timezone.utc))
    expired = loaded.try_get_agent_response("c")
    assert expired is not None
    assert expired.additional_properties["durable_status"] == "already_completed"


def test_poll_preserves_value_by_name_marker(schema: dict[str, Any]) -> None:
    class Aliased(BaseModel):
        count: int = Field(validation_alias="inputCount", serialization_alias="outputCount")

    raw = _mailbox()
    response = raw["data"]["terminalResults"]["c"]["response"]
    response.update(
        value={"count": 7},
        pythonCoreFields={
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": {"_durable_value_by_name": True},
        },
    )
    result = _cold(DurableAgentState.from_dict(raw), schema).try_get_agent_response("c")
    assert result is not None
    ensure_response_format(Aliased, "c", result)
    assert result.value == Aliased(inputCount=7)


@pytest.mark.parametrize("collection", ["terminalResults", "completionReceipts"])
@pytest.mark.parametrize("field", ["resultExpiresAt", "completedAt"])
@pytest.mark.parametrize(
    "timestamp",
    [
        None,
        "2026-09-09",
        "2026-09-09T00:00:00",
        "2026-09-09T00:00:00+00:00\n",
        "2026-02-30T00:00:00Z",
        "2026-09-09T00:00:00+00:60",
    ],
)
def test_new_delivery_timestamps_require_valid_rfc3339(collection: str, field: str, timestamp: Any) -> None:
    raw = _mailbox()
    raw["data"][collection]["c"][field] = timestamp
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_legacy_missing_timestamp_and_scalar_state_remain_unchanged(version: str) -> None:
    raw = {
        "schemaVersion": version,
        "data": {
            "ingestedPositions": {"source": 7},
            "conversationHistory": [
                {"$type": "request", "messages": []},
            ],
        },
    }
    state = DurableAgentState.from_dict(raw)
    before = state.to_dict()
    assert before == raw and state.data.conversation_history[0].created_at is None
    with pytest.raises(ValueError, match="delivery evidence"):
        state.prepare_for_write(delivery_window_seconds=60)
    assert state.to_dict() == before


def test_undated_uncorrelated_history_gets_a_deterministic_identity_without_fabricated_time() -> None:
    raw = {"$type": "request", "messages": [{"role": "user", "contents": [{"$type": "text", "text": "old"}]}]}
    first = DurableAgentStateRequest.from_dict(deepcopy(raw))
    second = DurableAgentStateRequest.from_dict(json.loads(json.dumps(raw)))
    identity = DurableHistoryProvider._synthetic_message_id(first, 0)
    assert isinstance(identity, str) and identity
    assert DurableHistoryProvider._synthetic_message_id(second, 0) == identity
    assert DurableHistoryProvider._synthetic_message_id(second, 1) != identity
    assert first.created_at is second.created_at is None
    assert first.to_dict() == second.to_dict() == raw


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0", "2.0.0"])
@pytest.mark.parametrize("timestamp", [None, "2026-09-09", "2026-09-09T00:00:00", "not-a-date"])
def test_present_invalid_transcript_timestamp_is_never_replaced_with_now(version: str, timestamp: Any) -> None:
    raw = {
        "schemaVersion": version,
        "data": {
            "conversationHistory": [{"$type": "request", "createdAt": timestamp, "messages": []}],
            **({"terminalResults": {}, "completionReceipts": {}} if version == "2.0.0" else {}),
        },
    }
    before = deepcopy(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_json(json.dumps(raw))
    assert raw == before


@pytest.mark.parametrize(
    ("kind", "field"),
    [
        ("search_tool_result", "items"),
        ("code_interpreter_tool_call", "inputs"),
        ("code_interpreter_tool_result", "outputs"),
        ("shell_tool_result", "outputs"),
        ("function_approval_request", "function_call"),
        ("function_approval_response", "function_call"),
    ],
)
def test_nested_core_edges_are_reconstructed_in_transcript(kind: str, field: str, schema: dict[str, Any]) -> None:
    nested = Content.from_function_result(
        "call", result=[Content.from_text("nested"), Content.from_data(b"data", "image/png")]
    )
    content_class: Any = Content
    content: Content = content_class(kind, **{field: nested if field == "function_call" else [nested]})
    original = Message("tool", [content])
    restored = _stored_message(_cold(_state(original), schema)).to_chat_message()
    assert restored.to_dict() == original.to_dict()
    inner = getattr(restored.contents[0], field)
    inner = inner if field == "function_call" else inner[0]
    assert isinstance(inner, Content)
    assert inner.items is not None
    assert isinstance(inner.items[1], Content)


@pytest.mark.parametrize("invalid", [None, "text", {}, [None], ["text"], [42]])
def test_mailbox_shared_contents_reject_malformed_containers(invalid: Any) -> None:
    raw = _mailbox()
    raw["data"]["terminalResults"]["c"]["response"]["messages"][0]["contents"] = invalid
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)


def test_z_delivery_timestamp_is_normalized_before_fromisoformat(monkeypatch: pytest.MonkeyPatch) -> None:
    import agent_framework_durabletask._durable_agent_state as state_module

    class Python310Datetime(datetime):
        @classmethod
        def fromisoformat(cls, value: str) -> "Python310Datetime":
            assert not value.endswith(("Z", "z")), "Python 3.10 does not accept Z directly"
            return super().fromisoformat(value)

    raw = _mailbox()
    monkeypatch.setattr(state_module, "datetime", Python310Datetime)
    loaded = DurableAgentState.from_dict(raw)
    assert loaded.try_get_agent_response("c") is not None
    loaded.expire_responses(now=datetime(2100, 1, 1, tzinfo=timezone.utc))
    assert not loaded.data.response_mailbox
