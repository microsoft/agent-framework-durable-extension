# Copyright (c) Microsoft. All rights reserved.

"""The persisted state must match the shared cross-language schema.

``schemas/durable-agent-entity-state.json`` is the contract between the Python and .NET hosting
layers. Nothing enforced it before, so fields this runtime persisted (``messageId`` and
``extensionData``, both load-bearing for context management) went undeclared and a .NET
implementer reading the schema would not have known to round-trip them.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from agent_framework import Message

from agent_framework_durabletask import (
    DurableAgentState,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)

jsonschema = pytest.importorskip("jsonschema")

SCHEMA_PATH = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"


@pytest.fixture(scope="module")
def schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _validate(payload: dict[str, Any], schema: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator(schema).validate(payload)


def _populated_state() -> DurableAgentState:
    """Build state exercising every field this runtime persists."""
    now = datetime.now(tz=timezone.utc)
    request = DurableAgentStateRequest(
        correlation_id="c0",
        created_at=now,
        messages=[
            DurableAgentStateMessage.from_chat_message(
                Message(role="user", contents=["hello"], message_id="wf_input_0")
            )
        ],
    )
    response = DurableAgentStateResponse(
        correlation_id="c0",
        created_at=now,
        messages=[
            DurableAgentStateMessage.from_chat_message(
                Message(role="assistant", contents=["hi"], message_id="wf_writer_1")
            )
        ],
    )
    # Annotations are what carry compaction state across a round-trip.
    response.messages[0].extension_data = {"_excluded": True, "_excluded_reason": "sliding_window"}

    state = DurableAgentState()
    state.data.conversation_history.extend([request, response])
    state.data.session = {"type": "session", "session_id": "@dafx-writer@run-1", "state": {"compaction": {}}}
    state.data.ingested_positions = {"input": 0, "writer": 1}
    return state


def test_the_schema_itself_is_valid(schema: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator.check_schema(schema)


def test_empty_state_validates(schema: dict[str, Any]) -> None:
    _validate(DurableAgentState().to_dict(), schema)


def test_populated_state_validates(schema: dict[str, Any]) -> None:
    _validate(_populated_state().to_dict(), schema)


def test_message_identity_and_annotations_are_declared(schema: dict[str, Any]) -> None:
    """Both are load-bearing for compaction, so an implementer must be told to round-trip them."""
    properties = schema["$defs"]["chatMessage"]["properties"]

    assert "messageId" in properties
    assert "extensionData" in properties


def test_the_ingestion_watermark_is_declared(schema: dict[str, Any]) -> None:
    """It is how a repeated workflow node recognizes context it already recorded."""
    assert "ingestedPositions" in schema["$defs"]["data"]["properties"]


def test_session_is_left_opaque(schema: dict[str, Any]) -> None:
    """The two runtimes serialize sessions differently, so the shared schema must not fix a shape.

    .NET produces ``conversationId`` plus ``stateBag``. Python produces ``session_id``,
    ``service_session_id`` and ``state``. Declaring either one would make the other invalid.
    """
    session = schema["$defs"]["data"]["properties"]["session"]

    assert "properties" not in session, "the schema pins one runtime's session shape"

    dotnet_shaped = {
        "schemaVersion": DurableAgentState.SCHEMA_VERSION,
        "data": {"conversationHistory": [], "session": {"conversationId": "abc", "stateBag": {}}},
    }
    _validate(dotnet_shaped, schema)


def test_state_survives_a_round_trip_through_the_schema(schema: dict[str, Any]) -> None:
    """Serialize, validate, restore, and confirm the compaction-critical fields came back."""
    payload = _populated_state().to_dict()
    _validate(payload, schema)

    restored = DurableAgentState.from_dict(payload)
    stored = restored.data.conversation_history[1].messages[0]

    assert stored.message_id == "wf_writer_1"
    assert (stored.extension_data or {}).get("_excluded") is True
    assert restored.data.ingested_positions == {"input": 0, "writer": 1}
