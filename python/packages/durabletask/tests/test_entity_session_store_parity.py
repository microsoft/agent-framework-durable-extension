# Copyright (c) Microsoft. All rights reserved.

"""Black-box session parity controls for both the baseline and the extraction.

The keyed external store is an in-memory Redis-style fake, not a backend integration.
"""

from collections.abc import Sequence
from copy import deepcopy
from typing import Any, cast

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient
from agent_framework import Agent, AgentSession, HistoryProvider, Message
from test_runtime_sessions import _cold, _request

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider


class _FactorySession(AgentSession):
    pass


class _DuckSession:
    def __init__(self, *, session_id: str, service_session_id: Any) -> None:
        self.session_id = session_id
        self.service_session_id = service_session_id
        self.state: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "session",
            "session_id": self.session_id,
            "service_session_id": deepcopy(self.service_session_id),
            "state": deepcopy(self.state),
        }


class _FactoryAgent(NonStreamingAgent):
    def __init__(self, *, session_type: type = _FactorySession, service_id: Any = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.session_type = session_type
        self.service_id = service_id
        self.factory_attribute = object()
        self.created: list[Any] = []
        self.seen: list[tuple[Any, dict[str, Any]]] = []

    def create_session(self, **kwargs: Any) -> Any:
        session = self.session_type(service_session_id=deepcopy(self.service_id), **kwargs)
        session.factory_attribute = self.factory_attribute
        session.state = {"factory_only": {"keep": True}, "external": {"factory_default": True}}
        self.created.append(session)
        return session

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")
        session = kwargs["session"]
        self.seen.append((session, deepcopy(session.state)))
        return super().run(*args, **kwargs)


class _Client(RecordingChatClient):
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        super().__init__(fail=fail)
        self.events = events

    def _inner_get_response(self, **kwargs: Any) -> Any:
        self.events.append("model")
        return super()._inner_get_response(**kwargs)


class _ExternalHistory(HistoryProvider):
    def __init__(
        self, store: dict[str, list[Message]], events: list[str], *, source_id: str = "external", load: bool = True
    ) -> None:
        super().__init__(source_id, load_messages=load)
        self.store = store
        self.events = events
        self.calls: list[tuple[str, str | None, dict[str, Any]]] = []
        self.partial_failure = False

    async def before_run(self, **kwargs: Any) -> None:
        self.events.append(f"{self.source_id}.before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.events.append(f"{self.source_id}.after")
        await super().after_run(**kwargs)

    async def get_messages(self, session_id: str | None, *, state: Any = None, **kwargs: Any) -> list[Message]:
        assert isinstance(state, dict)
        self.events.append(f"{self.source_id}.load")
        self.calls.append(("load", session_id, deepcopy(state)))
        state["loads"] = state.get("loads", 0) + 1
        return deepcopy(self.store.get(f"{self.source_id}:{session_id}", []))

    async def save_messages(
        self, session_id: str | None, messages: Sequence[Message], *, state: Any = None, **kwargs: Any
    ) -> None:
        assert isinstance(state, dict)
        self.events.append(f"{self.source_id}.save")
        self.calls.append(("save", session_id, deepcopy(state)))
        state["saves"] = state.get("saves", 0) + 1
        accepted = messages[:1] if self.partial_failure else messages
        self.store.setdefault(f"{self.source_id}:{session_id}", []).extend(deepcopy(list(accepted)))
        if self.partial_failure:
            raise OSError("partial external save")


def _seed(state: dict[str, Any], **siblings: Any) -> JsonStateProvider:
    initial = DurableAgentState()
    initial.data.session = {
        "type": "session",
        "session_id": "previous-session-id",
        "service_session_id": {"remote": "saved"},
        "state": deepcopy(state),
        **siblings,
    }
    return JsonStateProvider(initial.to_dict())


@pytest.mark.parametrize("session_type", [_FactorySession, _DuckSession], ids=["subclass", "duck"])
@pytest.mark.parametrize(
    ("factory_id", "expected_id"),
    [(None, {"remote": "saved"}), ("", ""), ({}, {}), ({"remote": "factory"}, {"remote": "factory"})],
    ids=["none-restores", "empty-string-wins", "empty-mapping-wins", "mapping-wins"],
)
async def test_factory_session_and_shallow_state_merge_survive_cold_restore(
    session_type: type, factory_id: Any, expected_id: Any
) -> None:
    provider = _seed({"external": {"cursor": 7}, "application": [False, 0, None]})
    client = RecordingChatClient()
    agent = _FactoryAgent(client=client, name="parity", session_type=session_type, service_id=factory_id)
    expected_state = {
        "factory_only": {"keep": True},
        "external": {"cursor": 7},
        "application": [False, 0, None],
    }
    for correlation, reply in (("first", "reply-1"), ("second", "reply-2")):
        entity, provider = _cold(agent, provider)
        assert (await entity.run(_request(correlation, "hello"))).text == reply
        session, before = agent.seen[-1]
        assert session is agent.created[-1]
        assert type(session) is session_type
        assert cast(Any, session).factory_attribute is agent.factory_attribute
        assert before == expected_state
        assert provider.raw["data"]["session"] == {
            "type": "session",
            "session_id": "@runtime@runtime-session",
            "service_session_id": expected_id,
            "state": expected_state,
        }
    assert len(agent.created) == len(agent.seen) == len(client.received_messages) == 2
    assert agent.created[0] is not agent.created[1]


async def test_external_history_and_load_disabled_audit_restore_bags_and_hook_order() -> None:
    events: list[str] = []
    store = {"external:@runtime@runtime-session": [Message("user", ["prior"])]}
    provider = _seed({"external": {"cursor": "kept"}, "audit": {"tag": "sink"}})
    for correlation in ("first", "second"):
        external = _ExternalHistory(store, events)
        audit = _ExternalHistory(store, events, source_id="audit", load=False)
        client = _Client(events)
        agent = Agent(client=client, name="parity", context_providers=[external, audit])
        entity, provider = _cold(agent, provider)
        assert (await entity.run(_request(correlation, correlation))).text == "reply-1"
        assert [call[:2] for call in external.calls] == [
            ("load", "@runtime@runtime-session"),
            ("save", "@runtime@runtime-session"),
        ]
        assert [call[:2] for call in audit.calls] == [("save", "@runtime@runtime-session")]
    expected_events = [
        "external.before",
        "external.load",
        "model",
        "audit.after",
        "audit.save",
        "external.after",
        "external.save",
    ] * 2
    assert events == expected_events
    assert [message.text for message in client.received_messages[0]] == ["prior", "first", "reply-1", "second"]
    assert external.calls[0][2] == {"cursor": "kept", "loads": 1, "saves": 1}
    assert external.calls[1][2] == {"cursor": "kept", "loads": 2, "saves": 1}
    assert audit.calls[0][2] == {"tag": "sink", "saves": 1}
    assert provider.raw["data"]["session"]["state"] == {
        "external": {"cursor": "kept", "loads": 2, "saves": 2},
        "audit": {"tag": "sink", "saves": 2},
    }
    assert [message.text for message in store["audit:@runtime@runtime-session"]] == [
        "first",
        "reply-1",
        "second",
        "reply-1",
    ]


@pytest.mark.parametrize("audit_only", [False, True], ids=["primary", "load-disabled-audit"])
async def test_capture_filters_only_durable_buffer_and_preserves_opaque_envelope(audit_only: bool) -> None:
    nested = {"type": "parity-opaque", "messages": [False, 0], "_positions": {"x": None}}
    provider = _seed({"history": {"custom": nested, "_excluded": False}, "external": nested}, future={"nested": nested})
    history = DurableHistoryProvider("history")
    history.load_messages = not audit_only
    providers: list[HistoryProvider] = [history]
    if audit_only:
        providers.insert(0, _ExternalHistory({}, []))
    agent = _FactoryAgent(client=RecordingChatClient(), name="parity", context_providers=providers)
    for correlation, reply in (("first", "reply-1"), ("second", "reply-2")):
        entity, provider = _cold(agent, provider)
        assert (await entity.run(_request(correlation, "hello"))).text == reply
        payload = provider.raw["data"]["session"]
        assert payload["future"] == {"nested": nested}
        assert payload["state"]["history"] == {"custom": nested, "_excluded": False}
        for key in ("type", "messages", "_positions"):
            assert payload["state"]["external"][key] == nested[key]
        live_bag = agent.seen[-1][0].state["history"]
        assert "messages" in live_bag and "_positions" in live_bag
        assert [message.text for message in live_bag["messages"]][-2:] == ["hello", reply]


@pytest.mark.parametrize("failure", ["partial-save", "application"])
async def test_application_failure_commits_session_and_error_not_host_failure(failure: str) -> None:
    events: list[str] = []
    store: dict[str, list[Message]] = {}
    external = _ExternalHistory(store, events)
    external.partial_failure = failure == "partial-save"
    client = _Client(events, fail=failure == "application")
    provider = _seed({"external": {"cursor": "kept"}})
    entity = AgentEntity(Agent(client=client, context_providers=[external]), state_provider=provider)
    response = await entity.run(_request("failed", "hello"))
    assert response.additional_properties["durable_status"] == "error"
    assert provider.successful_writes == provider.attempted_writes == 1
    assert entity.state.data.completed_correlations["failed"]["outcome"] == "failed"
    expected_bag = {"cursor": "kept", "loads": 1}
    if failure == "partial-save":
        expected_bag["saves"] = 1
        assert "partial external save" in response.text
        assert [message.text for message in store["external:@runtime@runtime-session"]] == ["hello"]
    else:
        assert "model failed before history persistence" in response.text
        assert store == {}
    assert provider.raw["data"]["session"] == {
        "type": "session",
        "session_id": "@runtime@runtime-session",
        "service_session_id": {"remote": "saved"},
        "state": {"external": expected_bag},
    }


async def test_escaping_host_write_failure_rolls_back_entity_not_external_store() -> None:
    store: dict[str, list[Message]] = {}
    external = _ExternalHistory(store, [])
    provider = _seed({"external": {"cursor": "kept"}})
    before = deepcopy(provider.raw)
    provider.fail_before_write = True
    entity = AgentEntity(Agent(client=RecordingChatClient(), context_providers=[external]), state_provider=provider)
    with pytest.raises(OSError, match="injected commit failure"):
        await entity.run(_request("failed-write", "hello"))
    assert provider.raw == before
    assert entity.state.to_dict() == before
    assert provider.attempted_writes == 1 and provider.successful_writes == 0
    assert [message.text for message in store["external:@runtime@runtime-session"]] == ["hello", "reply-1"]


@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
@pytest.mark.parametrize("fail", [False, True], ids=["success", "error"])
async def test_duplicate_skips_factory_pipeline_and_write(cold: bool, fail: bool) -> None:
    client = RecordingChatClient(fail=fail)
    agent = _FactoryAgent(client=client, name="parity")
    provider = JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider)
    first = await entity.run(_request("same", "hello"))
    assert (first.additional_properties.get("durable_status") == "error") is fail
    if cold:
        entity, provider = _cold(agent, provider)
    before = deepcopy(provider.raw)
    writes = provider.attempted_writes, provider.successful_writes
    duplicate = await entity.run(_request("same", "hello"))
    assert duplicate.to_dict() == first.to_dict()
    assert len(agent.created) == len(agent.seen) == len(client.received_messages) == 1
    assert provider.raw == before
    assert (provider.attempted_writes, provider.successful_writes) == writes


async def test_json_compatible_custom_serializer_does_not_require_deepcopy_support() -> None:
    class JsonOnlyDict(dict[str, Any]):
        def __deepcopy__(self, memo: Any) -> Any:
            raise TypeError("JSON snapshot does not support deepcopy")

    class JsonSession(AgentSession):
        def to_dict(self) -> dict[str, Any]:
            result = super().to_dict()
            if self.state.get("finished"):
                return JsonOnlyDict(result)
            return result

    class JsonAgent(_FactoryAgent):
        def run(self, *args: Any, **kwargs: Any) -> Any:
            result = super().run(*args, **kwargs)

            async def finish() -> Any:
                response = await result
                kwargs["session"].state["finished"] = True
                return response

            return finish()

    provider = JsonStateProvider()
    client = RecordingChatClient()
    agent = JsonAgent(client=client, session_type=JsonSession)
    entity = AgentEntity(agent, state_provider=provider)
    response = await entity.run(_request("custom-json", "hello"))
    assert response.text == "reply-1"
    assert provider.successful_writes == 1
    assert provider.raw["data"]["session"]["state"]["finished"] is True
    assert entity.state.data.completed_correlations["custom-json"]["outcome"] == "succeeded"


@pytest.mark.parametrize("invalid", [{1: "numeric", "1": "string"}, (False, 0, None)], ids=["keys", "tuple"])
async def test_custom_session_payload_remains_strict_json_before_normalization(invalid: Any) -> None:
    class InvalidSession(AgentSession):
        def to_dict(self) -> dict[str, Any]:
            result = super().to_dict()
            result["future"] = invalid
            return result

    provider = JsonStateProvider()
    original = deepcopy(provider.raw)
    client = RecordingChatClient()
    agent = _FactoryAgent(client=client, session_type=InvalidSession)
    entity = AgentEntity(agent, state_provider=provider)
    with pytest.raises(ValueError):
        await entity.run(_request("invalid-json", "hello"))
    assert provider.raw == original
    assert provider.attempted_writes == provider.successful_writes == 0
    assert entity.state.data.session is None
    assert "invalid-json" not in entity.state.data.completed_correlations
    assert len(client.received_messages) == 1
