# Copyright (c) Microsoft. All rights reserved.

"""Exercise the session adapter through real entity operations and Core hooks."""

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from agent_framework import Agent, SessionStore
from test_entity_session_store_parity import _ExternalHistory, _seed
from test_reset_primary_boundaries import _seed as _reset_seed
from test_runtime_sessions import _request
from test_runtime_transactions import _ControlProvider

from agent_framework_durabletask import AgentEntity, DurableHistoryProvider
from agent_framework_durabletask._session_store import EntitySessionStore


def _spy(monkeypatch: pytest.MonkeyPatch, provider: JsonStateProvider) -> list[tuple[str, Any]]:
    calls: list[tuple[str, Any]] = []
    snapshots: list[tuple[Any, Any]] = []

    def wrap(name: str) -> None:
        original = getattr(EntitySessionStore, name)

        async def observe(store: EntitySessionStore, key: str, *args: Any) -> Any:
            assert isinstance(store, SessionStore)
            assert store._data is provider.state.data
            assert key == provider.core_session_id == "@runtime@runtime-session"
            writes = provider.attempted_writes, provider.successful_writes
            raw = deepcopy(provider.raw)
            snapshots.append((store._data.session, deepcopy(store._data.session)))
            calls.append((name, store._data))
            result = await original(store, key, *args)
            assert (provider.attempted_writes, provider.successful_writes) == writes
            assert provider.raw == raw
            assert all(previous == frozen for previous, frozen in snapshots)
            return result

        monkeypatch.setattr(EntitySessionStore, name, observe)

    for name in ("get", "set", "delete"):
        wrap(name)
    return calls


@pytest.mark.parametrize("external", [False, True], ids=["durable-history", "external-history"])
async def test_run_routes_through_store_and_duplicate_bypasses_it(
    monkeypatch: pytest.MonkeyPatch, external: bool
) -> None:
    provider = _seed({"control": {"before_runs": 4, "after_runs": 3}})
    client, control = RecordingChatClient(), _ControlProvider()
    events: list[str] = []
    history = _ExternalHistory({}, events) if external else DurableHistoryProvider()
    entity = AgentEntity(Agent(client=client, context_providers=[history, control]), state_provider=provider)
    original, frozen = entity.state, deepcopy(entity.state.to_dict())
    calls = _spy(monkeypatch, provider)
    first = await entity.run(_request("same", "hello"))
    assert first.text == "reply-1"
    assert [name for name, _ in calls] == ["get", "set"]
    assert calls[0][1] is calls[1][1] is entity.state.data
    assert calls[0][1] is not original.data and original.to_dict() == frozen
    assert control.loaded == [{"before_runs": 4, "after_runs": 3}]
    assert provider.raw["data"]["session"]["state"]["control"] == {"before_runs": 5, "after_runs": 4}
    assert len(control.responses) == len(client.received_messages) == 1
    if external:
        assert events == ["external.before", "external.load", "external.after", "external.save"]
    committed = deepcopy(provider.raw)
    duplicate = await entity.run(_request("same", "must not execute"))
    assert duplicate.to_dict() == first.to_dict()
    assert [name for name, _ in calls] == ["get", "set"]
    assert len(control.loaded) == len(control.responses) == len(client.received_messages) == 1
    assert provider.raw == committed
    assert provider.attempted_writes == provider.successful_writes == 1


async def test_reused_entity_rebinds_after_success_application_failure_and_host_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _seed({"control": {"before_runs": 0, "after_runs": 0}})
    client, control = RecordingChatClient(), _ControlProvider()
    entity = AgentEntity(Agent(client=client, context_providers=[control]), state_provider=provider)
    calls = _spy(monkeypatch, provider)
    phases = [("ok", False, False), ("error", True, False), ("retry", False, True), ("retry", False, False)]
    for index, (correlation, fail_model, fail_write) in enumerate(phases, 1):
        original, frozen = entity.state, deepcopy(entity.state.to_dict())
        committed = deepcopy(provider.raw)
        expected = deepcopy(committed["data"]["session"]["state"]["control"])
        client.fail, provider.fail_before_write = fail_model, fail_write
        if fail_write:
            with pytest.raises(OSError, match="injected commit failure"):
                await entity.run(_request(correlation, "hello"))
            assert provider.raw == committed and entity.state.to_dict() == frozen
        else:
            response = await entity.run(_request(correlation, "hello"))
            assert (response.additional_properties.get("durable_status") == "error") is fail_model
            if fail_model:
                assert "model failed before history persistence" in response.text
                duplicate = await entity.run(_request(correlation, "must not execute"))
                assert duplicate.to_dict() == response.to_dict()
            else:
                assert response.text == f"reply-{index}"
        assert original.to_dict() == frozen
        assert control.loaded[-1] == expected
        assert [name for name, _ in calls] == ["get", "set"] * index
        assert calls[-2][1] is calls[-1][1] and calls[-1][1] is not original.data
        assert all(calls[-1][1] is not data for _, data in calls[:-2])
        assert provider.attempted_writes == index
        assert provider.successful_writes == index - int(index >= 3)
    assert len(control.loaded) == len(client.received_messages) == 4
    assert len(control.responses) == 3
    assert provider.raw["data"]["session"]["state"]["control"] == {"before_runs": 3, "after_runs": 2}


async def test_agent_without_context_pipeline_does_not_use_store(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, client = JsonStateProvider(), RecordingChatClient()
    agent: Any = SimpleNamespace(run=Agent(client=client).run)
    entity = AgentEntity(agent, state_provider=provider)
    calls = _spy(monkeypatch, provider)
    assert (await entity.run(_request("plain", "hello"))).text == "reply-1"
    assert calls == [] and entity.state.data.session is None
    assert len(client.received_messages) == 1
    assert provider.attempted_writes == provider.successful_writes == 1


def test_reset_is_synchronous_and_preserves_control_state_without_store_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = JsonStateProvider(_reset_seed(None).to_dict())
    client = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    original, frozen = entity.state, deepcopy(entity.state.to_dict())
    expected = deepcopy(provider.raw)
    expected["data"]["conversationHistory"] = []
    del expected["data"]["session"]
    calls = _spy(monkeypatch, provider)
    entity.reset()
    assert calls == [] and client.received_messages == []
    assert provider.raw == entity.state.to_dict() == expected
    assert original.to_dict() == frozen
    assert provider.attempted_writes == provider.successful_writes == 1
