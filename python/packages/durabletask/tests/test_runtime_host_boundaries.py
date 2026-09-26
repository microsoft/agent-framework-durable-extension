# Copyright (c) Microsoft. All rights reserved.

"""Real SDK state and host registration boundaries for runtime execution."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from _execution_test_support import NonStreamingAgent, RecordingChatClient
from agent_framework import Agent, AgentExecutor, BaseChatClient, ChatResponse, Message, Workflow, WorkflowBuilder
from agent_framework_azurefunctions import AgentFunctionApp
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableAIAgentWorker, load_agent_response

UTC_NOW = datetime(2026, 9, 18, 12, 0, 0, tzinfo=timezone.utc)


class _CoreClient(BaseChatClient):
    STORES_BY_DEFAULT = False

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Any:
        del stream, options, kwargs
        return _chat_response(f"reply:{messages[-1].text}")


class _RecordingRunAgent(NonStreamingAgent):
    def __init__(self, client: BaseChatClient, *, name: str = "runtime-agent") -> None:
        super().__init__(client=client, name=name)
        self.observed_messages: list[list[Message]] = []

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")
        messages = kwargs.get("messages", [])
        self.observed_messages.append(deepcopy(list(messages)))
        return super().run(*args, **kwargs)


def _chat_response(text: str) -> Any:
    async def get() -> ChatResponse:
        return ChatResponse(
            messages=[Message("assistant", [text])], response_id=f"response:{text}", finish_reason="stop"
        )

    return get()


def _sdk_provider(state_json: str | None, *, entity_name: str = "dafx-runtime", session_id: str = "session-1") -> Any:
    converter = JsonDataConverter()
    shim = StateShim(state_json, converter, is_serialized=True)
    entity_id = EntityInstanceId(entity_name, session_id)
    context = EntityContext("orchestration", "operation", shim, entity_id, converter)
    entity = DurableAIAgentWorker(Mock(), deployment_mode="isolated_v2")._DurableAIAgentWorker__create_agent_entity(  # type: ignore[attr-defined]
        Agent(client=RecordingChatClient(), name="bootstrap"),
        None,
        entity_id="bootstrap",
    )()
    entity._initialize_entity_context(context)
    return entity, shim


def _request(correlation_id: str, message: str, **kwargs: Any) -> dict[str, Any]:
    return {"message": message, "correlationId": correlation_id, **kwargs}


def _delivery_state(
    *, correlation_id: str = "c1", text: str = "done", expires_at: datetime | None = None
) -> dict[str, Any]:
    state = DurableAgentState()
    response = load_agent_response({"messages": [{"role": "assistant", "contents": [{"type": "text", "text": text}]}]})
    state.record_response(
        correlation_id,
        response,
        delivery_window_seconds=60,
        now=UTC_NOW if expires_at is None else expires_at - timedelta(seconds=60),
    )
    raw = state.to_dict()
    if expires_at is not None:
        stamp = expires_at.isoformat()
        raw["data"]["terminalResults"][correlation_id]["resultExpiresAt"] = stamp
        raw["data"]["completionReceipts"][correlation_id]["resultExpiresAt"] = stamp
    return raw


def _agent(name: str, *, client: BaseChatClient | None = None) -> Agent:
    return Agent(client=client or RecordingChatClient(), name=name)


def _workflow(name: str, executor_id: str, *, agent_name: str | None = None) -> Workflow:
    executor = AgentExecutor(agent=_agent(agent_name or executor_id), id=executor_id)
    return WorkflowBuilder(name=name, start_executor=executor, output_from=[executor]).build()


async def test_sdk_state_cold_reload_preserves_session_continuity_and_canonical_history() -> None:
    recorded = _RecordingRunAgent(_CoreClient(), name="runtime")
    provider, shim = _sdk_provider(None, entity_name="dafx-runtime", session_id="thread")
    entity = AgentEntity(recorded, state_provider=provider)

    first = await entity.run(_request("c1", "hello"))
    assert first.text == "reply:hello"
    raw = shim.encode_state()
    assert isinstance(raw, str)

    cold_provider, cold_shim = _sdk_provider(raw, entity_name="dafx-runtime", session_id="thread")
    cold_agent = _RecordingRunAgent(_CoreClient(), name="runtime")
    second = await AgentEntity(cold_agent, state_provider=cold_provider).run(_request("c2", "again"))

    assert second.text == "reply:again"
    assert cold_agent.observed_messages[0][0].text == "again"
    persisted = json.loads(cold_shim.encode_state())
    assert [entry["correlationId"] for entry in persisted["data"]["conversationHistory"]] == ["c1", "c1", "c2", "c2"]
    assert persisted["data"]["session"]["session_id"] == "@dafx-runtime@thread"


async def test_sdk_state_duplicate_completion_is_suppressed_without_live_map_mutation() -> None:
    raw = _delivery_state(correlation_id="dup", text="kept")
    raw["data"]["terminalResults"]["dup"].pop("resultExpiresAt")
    raw["data"]["completionReceipts"]["dup"].pop("resultExpiresAt")
    provider, shim = _sdk_provider(json.dumps(raw), entity_name="dafx-runtime", session_id="dup-session")
    entity = AgentEntity(_agent("runtime"), state_provider=provider)
    before = json.loads(shim.encode_state())
    mailbox = entity.state.data.response_mailbox
    receipts = entity.state.data.completed_correlations

    duplicate = await entity.run(_request("dup", "ignored"))

    assert duplicate.text == "kept"
    assert json.loads(shim.encode_state()) == before
    assert entity.state.data.response_mailbox is mailbox
    assert entity.state.data.completed_correlations is receipts


async def test_sdk_state_expiry_maintenance_and_reset_preserve_immutable_completion_receipts() -> None:
    raw = _delivery_state(
        correlation_id="done", text="payload", expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    provider, shim = _sdk_provider(json.dumps(raw), entity_name="dafx-runtime", session_id="expiry")
    entity = AgentEntity(_agent("runtime"), state_provider=provider)

    removed = entity.expire_responses()
    after_expiry = json.loads(shim.encode_state())

    assert removed == 1
    assert "done" not in after_expiry["data"]["terminalResults"]
    assert after_expiry["data"]["completionReceipts"]["done"]["outcome"] == "succeeded"
    assert after_expiry["data"]["completionReceipts"]["done"]["resultState"] == "unavailable"

    entity.reset()
    after_reset = json.loads(shim.encode_state())

    assert after_reset["data"]["conversationHistory"] == []
    assert after_reset["data"]["terminalResults"] == {}
    assert after_reset["data"]["completionReceipts"]["done"]["outcome"] == "succeeded"
    assert after_reset["data"]["completionReceipts"]["done"]["resultState"] == "unavailable"


def test_worker_add_agent_registers_real_factory_and_binds_sdk_context() -> None:
    native = Mock()
    native.add_entity = MagicMock(return_value="dafx-runtime-agent")
    worker = DurableAIAgentWorker(native, deployment_mode="isolated_v2", response_delivery_window_seconds=91)
    agent = _agent("runtime-agent", client=RecordingChatClient())

    worker.add_agent(agent)

    registered_class = native.add_entity.call_args.args[0]
    instance = registered_class()
    context = EntityContext(
        "orchestration",
        "operation",
        StateShim(None, JsonDataConverter(), is_serialized=True),
        EntityInstanceId("dafx-runtime-agent", "stateful"),
        JsonDataConverter(),
    )
    instance._initialize_entity_context(context)

    assert type(instance).__name__ == "dafx-runtime-agent"
    assert worker._registered_agents["runtime-agent"] is agent
    assert instance._agent_entity.agent.client is agent.client
    assert instance._agent_entity._response_delivery_window_seconds == 91
    assert instance.core_session_id == "@dafx-runtime-agent@stateful"


@pytest.mark.parametrize(
    ("value", "valid"),
    [
        pytest.param(True, False, id="bool-true"),
        pytest.param(False, False, id="bool-false"),
        pytest.param(12, True, id="int"),
        pytest.param("12", False, id="non-int-string"),
    ],
)
def test_worker_delivery_window_configuration_matrix(value: Any, valid: bool) -> None:
    native = Mock()
    if valid:
        worker = DurableAIAgentWorker(native, deployment_mode="isolated_v2", response_delivery_window_seconds=value)
        worker.add_agent(_agent("runtime-agent"), response_delivery_window_seconds=value)
        registered_class = native.add_entity.call_args.args[0]
        instance = registered_class()
        assert worker._response_delivery_window_seconds == value
        assert instance._agent_entity._response_delivery_window_seconds == value
    else:
        with pytest.raises(ValueError, match="positive integer"):
            DurableAIAgentWorker(native, deployment_mode="isolated_v2", response_delivery_window_seconds=value)  # type: ignore[arg-type]


def test_worker_rejects_case_insensitive_workflow_name_collision_before_partial_registration() -> None:
    native = Mock()
    worker = DurableAIAgentWorker(native, deployment_mode="isolated_v2")

    worker.configure_workflow(_workflow("Orders", "assistant", agent_name="Reviewer"))

    with pytest.raises(ValueError, match="case-insensitively"):
        worker.configure_workflow(_workflow("orders", "assistant-2", agent_name="Reviewer"))

    assert [call.args[0].__name__ for call in native.add_orchestrator.call_args_list] == ["dafx-Orders"]
    assert worker.registered_workflow_names == ["Orders"]
    assert "Orders" not in worker.registered_agent_names


def test_worker_rejects_shared_workflow_callback_mismatch_before_partial_registration() -> None:
    native = Mock()
    worker = DurableAIAgentWorker(native, deployment_mode="isolated_v2")
    workflow = _workflow("shared", "assistant")

    worker.configure_workflow(workflow, callback=Mock())

    with pytest.raises(ValueError, match="different settings"):
        worker.configure_workflow(workflow, callback=Mock())

    assert len(native.add_orchestrator.call_args_list) == 1
    assert len(native.add_entity.call_args_list) == 1


def test_worker_rejects_shared_workflow_delivery_window_mismatch_before_partial_registration() -> None:
    native = Mock()
    worker = DurableAIAgentWorker(native, deployment_mode="isolated_v2")
    workflow = _workflow("shared", "assistant")

    worker.configure_workflow(workflow, response_delivery_window_seconds=30)

    with pytest.raises(ValueError, match="different settings"):
        worker.configure_workflow(workflow, response_delivery_window_seconds=31)

    assert len(native.add_orchestrator.call_args_list) == 1
    assert len(native.add_entity.call_args_list) == 1


def test_app_constructor_requires_explicit_isolated_v2_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    with pytest.raises(ValueError, match="Schema 2 requires an isolated task hub/deployment"):
        AgentFunctionApp(enable_health_check=False)


@pytest.mark.parametrize("value", ["legacy", "isolated", "", "ISOLATED_V2"])
def test_app_constructor_rejects_invalid_deployment_mode_values(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    with pytest.raises(ValueError, match="no other deployment mode is accepted"):
        AgentFunctionApp(enable_health_check=False, deployment_mode=value)


def test_app_constructor_accepts_explicit_isolated_v2_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    app = AgentFunctionApp(enable_health_check=False, deployment_mode="isolated_v2")
    assert app._deployment_mode == "isolated_v2"


@pytest.mark.parametrize("value", ["legacy", "isolated", "", "ISOLATED_V2"])
def test_app_constructor_rejects_invalid_deployment_environment(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", value)
    with pytest.raises(ValueError, match="no other deployment mode is accepted"):
        AgentFunctionApp(enable_health_check=False)


@pytest.mark.parametrize(
    ("value", "valid"),
    [
        pytest.param(True, False, id="bool-true"),
        pytest.param(False, False, id="bool-false"),
        pytest.param(17, True, id="int"),
        pytest.param("17", False, id="string"),
    ],
)
def test_app_constructor_delivery_window_matrix_matches_configuration(value: Any, valid: bool) -> None:
    if valid:
        app = AgentFunctionApp(
            enable_health_check=False, deployment_mode="isolated_v2", response_delivery_window_seconds=value
        )
        assert app._response_delivery_window_seconds == value
    else:
        with pytest.raises(ValueError, match="positive integer"):
            AgentFunctionApp(
                enable_health_check=False, deployment_mode="isolated_v2", response_delivery_window_seconds=value
            )  # type: ignore[arg-type]
