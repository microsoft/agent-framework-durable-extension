# Copyright (c) Microsoft. All rights reserved.

"""Nullable mapped fields preserve literal Core input presence on the first write."""

import json
from copy import deepcopy
from typing import Any

import pytest

from agent_framework_durabletask._response_utils import load_agent_response, preserve_input_envelope
from agent_framework_durabletask._shared_agent_state import DurableAgentStateMessage

CONTENT_FIELDS = [
    ("function_result", "result", "functionResult", "result", {"call_id": "call"}),
    ("error", "error_details", "error", "details", {"message": "failed"}),
]
VALUES: tuple[Any, ...] = (None, False, 0, 0.0, -0.0, "", [], {}, {"nested": [None, False, 0]})


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, allow_nan=False)


@pytest.mark.parametrize(("kind", "core_field", "shared_kind", "shared_field", "required"), CONTENT_FIELDS)
@pytest.mark.parametrize(("present", "value"), [(False, None), *((True, value) for value in VALUES)])
@pytest.mark.parametrize("entry_point", ["literal", "attached"])
def test_core_mapped_presence_survives_first_write_and_cold_read(
    kind: str,
    core_field: str,
    shared_kind: str,
    shared_field: str,
    required: dict[str, Any],
    value: Any,
    present: bool,
    entry_point: str,
) -> None:
    content = {"type": kind, **required, "future": {"keep": [False, 0]}}
    if present:
        content[core_field] = deepcopy(value)
    raw: dict[str, Any] = {"role": "tool", "contents": [content]}
    before = _json(raw)
    if entry_point == "literal":
        stored = DurableAgentStateMessage.from_core_dict(raw)
    else:
        message = load_agent_response({"messages": [raw]}).messages[0]
        preserve_input_envelope(message, raw)
        stored = DurableAgentStateMessage.from_chat_message(message)

    first_write = stored.to_dict()
    wire_content = first_write["contents"][0]
    assert wire_content["$type"] == shared_kind
    assert (shared_field in wire_content) is present
    if present:
        assert _json(wire_content[shared_field]) == _json(value)
    assert wire_content["pythonCoreFields"]["fields"]["future"] == {"keep": [False, 0]}
    assert core_field not in wire_content["pythonCoreFields"]["fields"]
    assert _json(raw) == before

    # Exercise mutations before the shared reader creates its own raw shadow.
    setattr(stored.contents[0], shared_field, {"first-write-change": False})
    assert stored.to_dict()["contents"][0][shared_field] == {"first-write-change": False}

    for _ in range(2):
        stored = DurableAgentStateMessage.from_dict(json.loads(_json(first_write)))
        # Projection may normalize Core defaults but must not change stored presence.
        stored.to_chat_message()
        assert _json(stored.to_dict()) == _json(first_write)

    setattr(stored.contents[0], shared_field, {"changed": [False, 0]})
    changed = stored.to_dict()
    assert changed["contents"][0][shared_field] == {"changed": [False, 0]}
    assert _json(raw) == before
    changed["contents"][0][shared_field]["changed"].append("export-only")
    assert stored.to_dict()["contents"][0][shared_field] == {"changed": [False, 0]}


@pytest.mark.parametrize(("kind", "core_field", "shared_kind", "shared_field", "required"), CONTENT_FIELDS)
@pytest.mark.parametrize("value", VALUES)
def test_plain_core_contents_keep_canonical_nullable_defaults(
    kind: str,
    core_field: str,
    shared_kind: str,
    shared_field: str,
    required: dict[str, Any],
    value: Any,
) -> None:
    # No attached literal envelope means Core has no absence bit for these fields.
    message = load_agent_response({
        "messages": [{"role": "tool", "contents": [{"type": kind, **required, core_field: deepcopy(value)}]}]
    }).messages[0]
    assert not hasattr(message.contents[0], "_durable_original_core_content")
    stored = DurableAgentStateMessage.from_chat_message(message)
    wire = stored.to_dict()["contents"][0]
    assert wire["$type"] == shared_kind
    assert shared_field in wire
    assert _json(wire[shared_field]) == _json(value)


@pytest.mark.parametrize(("kind", "core_field", "shared_kind", "shared_field", "required"), CONTENT_FIELDS)
def test_plain_core_omitted_nullable_field_keeps_canonical_null(
    kind: str, core_field: str, shared_kind: str, shared_field: str, required: dict[str, Any]
) -> None:
    message = load_agent_response({"messages": [{"role": "tool", "contents": [{"type": kind, **required}]}]}).messages[
        0
    ]
    wire = DurableAgentStateMessage.from_chat_message(message).to_dict()["contents"][0]
    assert wire["$type"] == shared_kind
    assert shared_field in wire and wire[shared_field] is None


@pytest.mark.parametrize(("kind", "core_field", "shared_kind", "shared_field", "required"), CONTENT_FIELDS)
def test_current_null_keeps_presence_from_original_nonnull_field(
    kind: str, core_field: str, shared_kind: str, shared_field: str, required: dict[str, Any]
) -> None:
    raw: dict[str, Any] = {"role": "tool", "contents": [{"type": kind, **required, core_field: "before"}]}
    message = load_agent_response({"messages": [raw]}).messages[0]
    preserve_input_envelope(message, raw)
    setattr(message.contents[0], core_field, None)

    wire = DurableAgentStateMessage.from_chat_message(message).to_dict()["contents"][0]

    assert wire["$type"] == shared_kind
    assert shared_field in wire and wire[shared_field] is None
    assert raw["contents"][0][core_field] == "before"


@pytest.mark.parametrize(("kind", "core_field", "shared_kind", "shared_field", "required"), CONTENT_FIELDS)
@pytest.mark.parametrize("value", VALUES)
def test_current_core_value_overrides_attached_absence(
    kind: str,
    core_field: str,
    shared_kind: str,
    shared_field: str,
    required: dict[str, Any],
    value: Any,
) -> None:
    raw: dict[str, Any] = {"role": "tool", "contents": [{"type": kind, **required}]}
    message = load_agent_response({"messages": [raw]}).messages[0]
    preserve_input_envelope(message, raw)
    setattr(message.contents[0], core_field, deepcopy(value))

    wire = DurableAgentStateMessage.from_chat_message(message).to_dict()["contents"][0]

    assert wire["$type"] == shared_kind
    # Setting None is indistinguishable from the original absent Core default.
    assert (shared_field in wire) is (value is not None)
    if value is not None:
        assert _json(wire[shared_field]) == _json(value)


@pytest.mark.parametrize(("kind", "core_field", "shared_kind", "shared_field", "required"), CONTENT_FIELDS)
@pytest.mark.parametrize("present", [False, True])
def test_shared_raw_shadow_preserves_nullable_presence(
    kind: str,
    core_field: str,
    shared_kind: str,
    shared_field: str,
    required: dict[str, Any],
    present: bool,
) -> None:
    content: dict[str, Any] = {"$type": shared_kind, "future": False}
    if kind == "function_result":
        content["callId"] = required["call_id"]
    if present:
        content[shared_field] = None
    raw: dict[str, Any] = {"role": "tool", "contents": [content]}
    stored = DurableAgentStateMessage.from_dict(deepcopy(raw))
    stored.to_chat_message()
    assert _json(stored.to_dict()) == _json(raw)
    assert _json(DurableAgentStateMessage.from_dict(json.loads(_json(raw))).to_dict()) == _json(raw)
