# Copyright (c) Microsoft. All rights reserved.

"""Migration cursor attribution is independent of opaque public message identities."""

from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from agent_framework import Message

from agent_framework_durabletask import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._entities import AgentEntity
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_agent_state import DurableAgentState, DurableAgentStateMessage

VERSIONS = ("1.0.0", "1.1.0", "1.2.0")
IDS = ("custom", "wf_source_0", "wf_source_9", "wf_source_-1", "wf_source_true", "wf__3", "wf_source_3\n")
NOW = datetime(2026, 9, 17, tzinfo=timezone.utc)


def _source(identity: str | None, *, version: str = "1.1.0") -> dict[str, Any]:
    return {
        "schemaVersion": version,
        "data": {
            "conversationHistory": []
            if identity is None
            else [{"$type": "request", "messages": [{"role": "user", "messageId": identity, "contents": []}]}]
        },
    }


def _delivery(source: dict[str, Any], messages: list[Message], **extra: Any) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "original-accepted-inputs",
        "complete": True,
        "messages": [message.to_dict() for message in messages],
        **extra,
    }


def _migrate(source: dict[str, Any], delivery: dict[str, Any] | None = None) -> DurableAgentState:
    return migrate_legacy_state(
        source,
        source_digest=state_snapshot_digest(source),
        source_session_id="old-agent:session",
        migration_id="migration",
        ownership_transfer_id="authorized-transfer",
        delivery_window_seconds=60,
        delivery_evidence=delivery,
        # Independent controlled fixture: these accepted inputs have no completions.
        completion_evidence={
            "sourceDigest": state_snapshot_digest(source),
            "evidenceId": "original-completions",
            "complete": True,
            "results": [],
        },
        now=NOW,
    )


def _cold(state: DurableAgentState) -> DurableAgentState:
    restored = DurableAgentState.from_json(state.to_json())
    assert restored.to_dict() == state.to_dict()
    return restored


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("identity", IDS)
def test_retained_id_only_compatibility_is_independent_of_public_spelling(version: str, identity: str) -> None:
    source = _source(identity, version=version)
    before = deepcopy(source)
    state = _cold(_migrate(source))
    assert state.data.ingested_messages == {identity: None}
    receiver: Any = SimpleNamespace(state=state)
    message = DurableAgentStateMessage.from_chat_message(Message("user", ["later body"], message_id=identity))
    assert AgentEntity._drop_already_stored(receiver, [message]) == []
    assert state.to_dict()["data"]["conversationHistory"] == before["data"]["conversationHistory"]
    assert source == before


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("identity", IDS)
@pytest.mark.parametrize("explicit", [False, True])
def test_unpositioned_journal_preserves_exact_revisions_for_every_public_spelling(
    version: str, identity: str, explicit: bool
) -> None:
    source = _source(identity, version=version)
    first = Message("user", ["accepted"], message_id=identity)
    revised = Message("user", ["accepted revision"], message_id=identity)
    evidence = _delivery(source, [first, revised], **({"messagePositions": [None, None]} if explicit else {}))
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    state = _cold(_migrate(source, evidence))
    assert state.data.ingested_messages == {identity: [message_identity(first), message_identity(revised)]}
    next_revision = Message("user", ["not yet accepted"], message_id=identity)
    messages = [DurableAgentStateMessage.from_chat_message(item) for item in (first, revised, next_revision)]
    receiver: Any = SimpleNamespace(state=state)
    assert AgentEntity._drop_already_stored(receiver, messages) == messages[-1:]
    assert source == before_source and evidence == before_evidence


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("identity", IDS)
def test_complete_journal_cannot_hide_retained_ids_behind_workflow_spelling(version: str, identity: str) -> None:
    source = _source(identity, version=version)
    evidence = _delivery(source, [])
    before = deepcopy(source), deepcopy(evidence)
    with pytest.raises(ValueError, match="every retained legacy.*request message ID"):
        _migrate(source, evidence)
    assert (source, evidence) == before


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("generated_id", ["wf_source_3", "opaque-positioned-input", "wf_different_99"])
def test_sparse_maxima_use_only_explicit_acceptance_attribution(version: str, generated_id: str) -> None:
    source = _source("wf_source_9", version=version)
    source["data"]["ingestedPositions"] = {"source": 3, "other": 0}
    inputs = [
        Message("user", ["accepted 3"], message_id=generated_id),
        Message("user", ["custom, not position 9"], message_id="wf_source_9"),
        Message("user", ["accepted 1"], message_id="wf_source_1"),
        Message("user", ["accepted 0"], message_id="another opaque id"),
        Message("user", ["accepted 3 revised"], message_id=generated_id),
    ]
    attribution = [
        {"producer": "source", "position": 3},
        None,
        {"producer": "source", "position": 1},
        {"producer": "other", "position": 0},
        {"producer": "source", "position": 3},
    ]
    evidence = _delivery(source, inputs, messagePositions=attribution)
    before = deepcopy(source), deepcopy(evidence)
    state = _cold(_migrate(source, evidence))
    assert state.data.ingested_positions == {"source": 3, "other": 0}
    expected: dict[str, list[str]] = {}
    for item in inputs:
        assert item.message_id is not None
        expected.setdefault(item.message_id, []).append(message_identity(item))
    assert state.data.ingested_messages == expected
    assert "wf_source_2" not in expected
    gap = Message("user", ["unseen gap"], message_id="wf_source_2")
    receiver: Any = SimpleNamespace(state=state)
    delivered = [DurableAgentStateMessage.from_chat_message(item) for item in (*inputs, gap)]
    assert AgentEntity._drop_already_stored(receiver, delivered) == delivered[-1:]
    # Simulate complete transcript pruning: committed exact ingestion is independent.
    state.data.conversation_history.clear()
    receiver.state = _cold(state)
    assert AgentEntity._drop_already_stored(receiver, delivered) == []
    assert (source, evidence) == before


@pytest.mark.parametrize("version", VERSIONS)
def test_matching_id_and_producer_are_not_proof_of_cursor_attribution(version: str) -> None:
    source = _source(None, version=version)
    source["data"]["ingestedPositions"] = {"source": 3}
    evidence = _delivery(source, [Message("user", ["accepted"], message_id="wf_source_3")])
    before = deepcopy(source), deepcopy(evidence)
    with pytest.raises(ValueError, match="messagePositions"):
        _migrate(source, evidence)
    assert (source, evidence) == before


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("position", [0.0, 3.0, 9007199254740992.0])
def test_integral_json_positions_are_preserved_while_comparing_normalized_maxima(version: str, position: float) -> None:
    source = _source(None, version=version)
    source["data"]["ingestedPositions"] = {"producer": position}
    message = Message("user", ["accepted"], message_id="opaque")
    evidence = _delivery(source, [message], messagePositions=[{"producer": "producer", "position": int(position)}])
    before = deepcopy(source), deepcopy(evidence)
    state = _cold(_migrate(source, evidence))
    raw_position = state.to_dict()["data"]["ingestedPositions"]["producer"]
    assert raw_position == position and type(raw_position) is float
    assert (source, evidence) == before


@pytest.mark.parametrize(
    "positions",
    [
        None,
        False,
        {},
        [],
        [None, None],
        [False],
        ["source:3"],
        [{}],
        [{"producer": "source"}],
        [{"position": 3}],
        [{"producer": "source", "position": 3, "extra": True}],
        [{"producer": " ", "position": 3}],
        [{"producer": 1, "position": 3}],
        [{"producer": "source", "position": True}],
        [{"producer": "source", "position": -1}],
        [{"producer": "source", "position": 3.0}],
        [{"producer": "source", "position": "3"}],
        [{"producer": "source", "position": float("inf")}],
        [{"producer": "source", "position": float("nan")}],
    ],
)
def test_malformed_attribution_is_rejected_without_mutation(positions: Any) -> None:
    source = _source(None)
    source["data"]["ingestedPositions"] = {"source": 3}
    evidence = _delivery(source, [Message("user", ["accepted"], message_id="wf_source_3")], messagePositions=positions)
    before = deepcopy(source), deepcopy(evidence)
    with pytest.raises(ValueError):
        _migrate(source, evidence)
    assert (source, evidence) == before


@pytest.mark.parametrize(
    "positions", [[None], [{"producer": "other", "position": 3}], [{"producer": "source", "position": 2}]]
)
def test_cursor_consistency_uses_attribution_not_an_id_that_appears_to_match(positions: list[Any]) -> None:
    source = _source(None)
    source["data"]["ingestedPositions"] = {"source": 3}
    evidence = _delivery(source, [Message("user", ["accepted"], message_id="wf_source_3")], messagePositions=positions)
    with pytest.raises(ValueError, match="producers and maximum positions"):
        _migrate(source, evidence)


def test_nonempty_attribution_cannot_invent_an_absent_scalar_cursor() -> None:
    source = _source(None)
    evidence = _delivery(
        source,
        [Message("user", ["accepted"], message_id="opaque")],
        messagePositions=[{"producer": "source", "position": 0}],
    )
    with pytest.raises(ValueError, match="producers and maximum positions"):
        _migrate(source, evidence)


@pytest.mark.parametrize("journal", [False, True])
def test_reconciliation_identity_is_not_the_public_migration_receipt_key(journal: bool) -> None:
    source = _source("wf_source_0")
    source["data"]["conversationHistory"][0]["messages"][0].update({
        "pythonHistoryId": "internal-reconciliation",
        "pythonHistoryIdentity": {"profile": "agent-framework-python.history-identity", "version": 1},
    })
    public = Message("user", ["accepted"], message_id="wf_source_0")
    evidence = _delivery(source, [public]) if journal else None
    state = _cold(_migrate(source, evidence))
    assert state.data.ingested_messages == {"wf_source_0": [message_identity(public)] if journal else None}
    assert state.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
