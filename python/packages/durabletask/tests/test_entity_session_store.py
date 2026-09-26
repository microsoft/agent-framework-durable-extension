# Copyright (c) Microsoft. All rights reserved.

"""Direct, in-memory contract tests for the operation-scoped session adapter."""

import importlib
import warnings
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest
from agent_framework import AgentSession, SessionStore
from agent_framework import _sessions as core_sessions
from agent_framework._serialization import SerializationMixin

from agent_framework_durabletask import _session_store
from agent_framework_durabletask._session_store import EntitySessionStore
from agent_framework_durabletask._shared_agent_state import DurableAgentStateData, DurableAgentStateUnknownEntry

KEY = "@agent@entity"


class _TransientPoison:
    def __deepcopy__(self, memo: Any) -> Any:
        raise AssertionError("transient buffer must not be copied")

    def to_dict(self) -> Any:
        raise AssertionError("transient buffer must not be serialized")


def _snapshot() -> dict[str, Any]:
    return {
        "type": "session",
        "session_id": "logical-id",
        "service_session_id": {"remote": {"ids": ["service-id"]}},
        "state": {"application": {"values": [False, 0, None]}},
    }


async def test_core_store_uses_current_operation_data_without_warnings() -> None:
    data = DurableAgentStateData(session=_snapshot())
    other = DurableAgentStateData()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        store = EntitySessionStore(data, KEY)
        assert isinstance(store, SessionStore)
        first = await store.get(KEY)
        assert first is not None and first.session_id == "logical-id"
        data.session = {**_snapshot(), "session_id": "replacement"}
        second = await store.get(KEY)
        assert second is not None and second.session_id == "replacement"
        assert await EntitySessionStore(other, KEY).get(KEY) is None
        await store.set(KEY, second)
        await store.delete(KEY)
    assert caught == []
    assert other.session is None


def test_constructor_rejects_empty_key_without_mutating_data() -> None:
    data = DurableAgentStateData(session=_snapshot())
    before = deepcopy(vars(data))
    with pytest.raises(ValueError):
        EntitySessionStore(data, "")
    assert vars(data) == before


@pytest.mark.parametrize("operation", ["get", "set", "delete"])
@pytest.mark.parametrize("key", ["", "other-entity"], ids=["empty", "mismatch"])
async def test_invalid_operation_key_does_not_mutate(operation: str, key: str) -> None:
    data = DurableAgentStateData(session=_snapshot())
    prior, before = data.session, deepcopy(vars(data))
    store = EntitySessionStore(data, KEY)
    args = (key, _TransientPoison()) if operation == "set" else (key,)
    with pytest.raises(ValueError):
        await getattr(store, operation)(*args)
    assert data.session is prior
    assert vars(data) == before


@pytest.mark.parametrize("snapshot", [None, {}, {"state": {"values": [0]}}], ids=["none", "empty", "missing-id"])
async def test_get_missing_snapshot_does_not_mutate(snapshot: Any) -> None:
    data = DurableAgentStateData(session=snapshot)
    before = deepcopy(vars(data))
    assert await EntitySessionStore(data, KEY).get(KEY) is None
    assert data.session is snapshot
    assert vars(data) == before


async def test_get_set_isolate_structured_service_id_and_nested_state() -> None:
    expected = _snapshot()
    session = AgentSession.from_dict(deepcopy(expected))
    data = DurableAgentStateData()
    store = EntitySessionStore(data, KEY)
    await store.set(KEY, session)
    assert data.session == expected  # The access key must not replace the logical ID.
    assert isinstance(session.service_session_id, dict)
    session.service_session_id["remote"]["ids"].append("live")
    session.state["application"]["values"].append("live")
    restored = await store.get(KEY)
    assert restored is not None and restored is not session
    assert restored.to_dict() == expected
    assert isinstance(restored.service_session_id, dict)
    restored.service_session_id["remote"]["ids"].append("restored")
    restored.state["application"]["values"].append("restored")
    again = await store.get(KEY)
    assert again is not None and again is not restored
    assert again.to_dict() == data.session == expected


async def test_get_registers_loaded_decoder_without_importing_payload_type(monkeypatch: pytest.MonkeyPatch) -> None:
    class LoadedState(SerializationMixin):
        def __init__(self, values: list[Any]) -> None:
            self.values = values

        @classmethod
        def _get_type_identifier(cls, value: Any = None) -> str:
            return "entity_store_loaded_state"

        @classmethod
        def from_dict(cls, data: Any, **kwargs: Any) -> Any:
            return cls(data.pop("values"))  # A consuming decoder must not mutate the stored snapshot.

    monkeypatch.setattr(core_sessions, "_STATE_TYPE_REGISTRY", {})
    monkeypatch.setattr(core_sessions, "_STATE_CLASS_REGISTRY", {})
    monkeypatch.setattr(_session_store, "_registered_state_types", set())
    raw = _snapshot()
    raw["state"] = {
        "nested": [{"type": "entity_store_loaded_state", "values": [{"count": 1}]}],
        "unknown": {"type": "uninstalled_entity_store_plugin.State", "values": [0]},
    }
    before = deepcopy(raw)
    data = DurableAgentStateData(session=raw)

    def unexpected_import(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("snapshot type names must not trigger module imports")

    monkeypatch.setattr(importlib, "import_module", unexpected_import)
    restored = await EntitySessionStore(data, KEY).get(KEY)
    assert restored is not None
    decoded = restored.state["nested"][0]
    assert isinstance(decoded, LoadedState)
    assert decoded.values == [{"count": 1}]
    assert restored.state["unknown"] == before["state"]["unknown"]
    decoded.values[0]["count"] = 2
    assert data.session is raw and raw == before


async def test_set_filters_only_selected_top_level_transients() -> None:
    nested: dict[str, Any] = {"messages": [False, 0], "_positions": {"keep": None}}
    transient = {"messages": [_TransientPoison()], "_positions": _TransientPoison(), "custom": nested}
    session = AgentSession(session_id="logical-id")
    session.state = {"history": transient, "external": deepcopy(nested), "messages": ["application"]}
    data = DurableAgentStateData(session={**_snapshot(), "future": {"values": [1]}})
    previous = data.session
    await EntitySessionStore(data, KEY, transient_history_source="history").set(KEY, session)
    assert session.state["history"] is transient
    assert data.session is not None and previous is not None
    assert data.session["state"] == {"history": {"custom": nested}, "external": nested, "messages": ["application"]}
    assert data.session["future"] == {"values": [1]}
    previous["future"]["values"].append(2)
    nested["messages"].append("live")
    assert data.session["future"] == {"values": [1]}
    assert data.session["state"]["history"]["custom"]["messages"] == [False, 0]


@pytest.mark.parametrize("source", [None, "absent"])
async def test_set_without_matching_source_preserves_history(source: str | None) -> None:
    session = AgentSession(session_id="logical-id")
    session.state = {"history": {"messages": ["keep"], "_positions": {"cursor": 1}}}
    expected = deepcopy(session.to_dict())
    data = DurableAgentStateData()
    await EntitySessionStore(data, KEY, transient_history_source=source).set(KEY, session)
    assert data.session == expected
    assert session.to_dict() == expected


async def test_set_omits_transient_only_bag_and_restores_reference() -> None:
    transient = {"messages": [_TransientPoison()], "_positions": _TransientPoison()}
    session = AgentSession(session_id="logical-id")
    session.state = {"history": transient}
    data = DurableAgentStateData()
    await EntitySessionStore(data, KEY, transient_history_source="history").set(KEY, session)
    assert data.session is not None and data.session["state"] == {}
    assert session.state["history"] is transient


@pytest.mark.parametrize(
    "failure",
    ["serializer", object(), float("nan"), float("inf"), -float("inf")],
    ids=["serializer", "object", "nan", "positive-infinity", "negative-infinity"],
)
async def test_set_failure_preserves_prior_snapshot_and_live_buffer(failure: Any) -> None:
    transient = {"messages": [_TransientPoison()], "_positions": _TransientPoison(), "cursor": 3}
    state = {"history": transient}

    def serialize() -> dict[str, Any]:
        assert state["history"] == {"cursor": 3}
        if failure == "serializer":
            raise RuntimeError("serializer failed")
        return {**_snapshot(), "state": {"bad": failure}}

    data = DurableAgentStateData(session=_snapshot())
    prior, before = data.session, deepcopy(vars(data))
    duck: Any = SimpleNamespace(state=state, to_dict=serialize)
    error = RuntimeError if failure == "serializer" else ValueError
    with pytest.raises(error, match="serializer failed|JSON-compatible"):
        await EntitySessionStore(data, KEY, transient_history_source="history").set(KEY, duck)
    assert data.session is prior and vars(data) == before
    assert duck.state is state and state["history"] is transient


async def test_set_duck_payload_wins_and_final_snapshot_is_independent() -> None:
    data = DurableAgentStateData(session={**_snapshot(), "future": {"old": [1]}, "opaque": [2]})
    payload: dict[str, Any] = {"session_id": "duck-id", "state": {}, "future": {"new": [3]}}
    duck: Any = SimpleNamespace(to_dict=lambda: payload)
    await EntitySessionStore(data, KEY).set(KEY, duck)
    assert data.session is not None
    assert data.session == {**payload, "opaque": [2]}  # Omitted known fields must not be resurrected.
    payload["future"]["new"].append(4)
    assert data.session["future"] == {"new": [3]}


@pytest.mark.parametrize(
    "session", [None, object(), SimpleNamespace(to_dict=None)], ids=["none", "absent", "noncallable"]
)
async def test_set_without_callable_serializer_is_noop(session: Any) -> None:
    data = DurableAgentStateData(session=_snapshot())
    prior, before = data.session, deepcopy(vars(data))
    await EntitySessionStore(data, KEY).set(KEY, session)
    assert data.session is prior and vars(data) == before


@pytest.mark.parametrize("previous", [None, _snapshot()], ids=["no-prior", "prior-mapping"])
async def test_none_serializer_payload_retains_inherited_behavior(previous: Any) -> None:
    data = DurableAgentStateData(session=previous)
    before = deepcopy(previous)
    duck: Any = SimpleNamespace(to_dict=lambda: None)
    store = EntitySessionStore(data, KEY)
    if previous is None:
        await store.set(KEY, duck)
    else:
        with pytest.raises(TypeError):  # Existing opaque-envelope merging requires a mapping.
            await store.set(KEY, duck)
    assert data.session is previous and data.session == before


async def test_delete_only_session_preserves_history_results_receipts_and_unknowns() -> None:
    history = {"$type": "future-entry", "messages": [{"text": "keep"}], "opaque": [False]}
    data = DurableAgentStateData(
        session=_snapshot(),
        conversation_history=[DurableAgentStateUnknownEntry(history)],
        response_mailbox={"request": {"response": {"messages": []}}},
        completed_correlations={"request": {"outcome": "succeeded"}},
        ingested_positions={"executor": 2},
        ingested_messages={"message": None},
        extension_data={"owner": {"value": 1}},
        truncation={"keep": [1]},
    )
    data.unknown_fields = {"migration": {"destinationSessionId": KEY}, "future": [0]}
    siblings = {key: value for key, value in vars(data).items() if key != "session"}
    history_entry = data.conversation_history[0]
    before = deepcopy({key: value for key, value in siblings.items() if key != "conversation_history"})
    store = EntitySessionStore(data, KEY)
    for _ in range(2):
        await store.delete(KEY)
        assert data.session is None and await store.get(KEY) is None
        assert all(getattr(data, key) is value for key, value in siblings.items())
        assert data.conversation_history == [history_entry]
        assert history_entry.to_dict() == history
        assert {key: getattr(data, key) for key in before} == before
