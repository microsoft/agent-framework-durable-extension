# Copyright (c) Microsoft. All rights reserved.

"""Real Core regressions for bounded retries, input receipts and safe diagnostics."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any, cast

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient
from agent_framework import (
    Agent,
    AgentContext,
    AgentMiddleware,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    BaseChatClient,
    ChatMiddlewareLayer,
    ChatResponse,
    HistoryProvider,
    Message,
)
from durabletask.task import CompletableTask
from pydantic import BaseModel, ValidationError, field_validator
from test_runtime_invocation_progress import _ScriptedNonStreamingClient
from test_service_commit_boundaries_review import (
    Answer,
    _assert_failed,
    _committed,
    _MissingParent,
    _provider,
    _request,
    _SessionProbe,
)

from agent_framework_durabletask import AgentEntity, DurableHistoryProvider, RunRequest
from agent_framework_durabletask import _entities as entities
from agent_framework_durabletask._callbacks import AgentCallbackContext
from agent_framework_durabletask._executors import DurableAgentTask
from agent_framework_durabletask._response_utils import serialize_agent_response


@pytest.fixture(autouse=True)
def _zero_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entities, "_REJECTED_ID_BACKOFF_SECONDS", 0)


class _RefusingClient(BaseChatClient):
    STORES_BY_DEFAULT = True

    def __init__(self, action: Callable[[Sequence[Message], Mapping[str, Any]], None] | None = None) -> None:
        super().__init__()
        self.action = action
        self.calls: list[dict[str, Any]] = []

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse]:
        assert not stream
        self.calls.append({"messages": deepcopy(list(messages)), "options": dict(options)})

        async def respond() -> ChatResponse:
            if len(self.calls) == 1:
                if self.action is not None:
                    self.action(messages, options)
                raise _MissingParent("first observed refusal")
            text = '{"answer": 42}' if options.get("response_format") is not None else "ok"
            return ChatResponse(messages=[Message("assistant", [text])], conversation_id="S1")

        return respond()


def _entity(
    provider: JsonStateProvider, client: Any, *, probe: _SessionProbe | None = None, middleware: Any = None
) -> AgentEntity:
    return AgentEntity(
        NonStreamingAgent(
            client=client,
            name="review",
            default_options={"store": True},
            context_providers=[probe] if probe is not None else [],
            middleware=middleware,
        ),
        state_provider=provider,
    )


@pytest.mark.parametrize("mutation", ["unchanged", "input", "session", "continuation", "options", "schema"])
async def test_real_core_refusal_retries_only_unchanged_invocation(mutation: str) -> None:
    provider = _provider()
    probe = _SessionProbe()

    def mutate(messages: Sequence[Message], options: Mapping[str, Any]) -> None:
        assert probe.session is not None
        if mutation == "input":
            messages[0].contents[0].text = "changed input"
        elif mutation == "session":
            probe.session.state["untouched"]["marker"].append("changed")
        elif mutation == "continuation":
            probe.session.service_session_id = "changed-parent"
        elif mutation == "options":
            options["metadata"]["nested"].append("changed")
        elif mutation == "schema":
            options["response_format"]["json_schema"]["name"] = "changed"

    client = _RefusingClient(mutate)
    entity = _entity(provider, client, probe=probe)
    request = _request("first", "B")
    request["options"].update({
        "metadata": {"nested": ["original"]},
        "response_format": {"type": "json_schema", "json_schema": {"name": "original"}},
    })

    response = await entity.run(request)

    if mutation == "unchanged":
        assert response.value == {"answer": 42}
        assert len(client.calls) == len(probe.entries) == 2
        assert client.calls[0]["options"] == client.calls[1]["options"]
        assert set(_committed(provider).data.ingested_messages) == {"occ-B"}
    else:
        _assert_failed(provider, response, "_MissingParent", "first observed refusal")
        assert len(client.calls) == len(probe.entries) == 1
        assert _committed(provider).data.ingested_messages == {}


class _FailBeforeClient(AgentMiddleware):
    def __init__(self) -> None:
        self.attempts = 0

    async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.attempts += 1
        if self.attempts > 1:
            raise _MissingParent("outer middleware refused before dispatch")
        await call_next()


async def test_previous_attempt_observation_cannot_authorize_retry_before_client_dispatch() -> None:
    provider = _provider()
    middleware = _FailBeforeClient()
    client = _RefusingClient()

    response = await _entity(provider, client, middleware=[middleware]).run(_request("first", "B"))

    _assert_failed(provider, response, "_MissingParent", "outer middleware refused before dispatch")
    assert middleware.attempts == 2
    assert len(client.calls) == 1
    assert _committed(provider).data.ingested_messages == {}


@pytest.mark.parametrize("mutation", ["unchanged", "session", "options"])
async def test_backoff_rechecks_state_before_dispatch(mutation: str, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _provider()
    probe = _SessionProbe()
    client = _RefusingClient()
    request = _request("first", "B")
    request["options"]["metadata"] = {"nested": ["original"]}
    sleeps: list[float] = []

    async def backoff(delay: float) -> None:
        sleeps.append(delay)
        assert probe.session is not None
        if mutation == "session":
            probe.session.state["untouched"]["marker"].append("during backoff")
        elif mutation == "options":
            request["options"]["metadata"]["nested"].append("during backoff")

    monkeypatch.setattr(entities.asyncio, "sleep", backoff)
    response = await _entity(provider, client, probe=probe).run(request)

    assert len(sleeps) == 1
    assert len(client.calls) == (2 if mutation == "unchanged" else 1)
    if mutation == "unchanged":
        assert response.text == "ok"
    else:
        _assert_failed(provider, response, "_MissingParent", "first observed refusal")


async def test_backoff_abort_reports_most_recent_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _provider()
    probe = _SessionProbe()

    def first() -> ChatResponse:
        raise _MissingParent("first refusal")

    def second() -> ChatResponse:
        raise _MissingParent("latest refusal")

    client = _ScriptedNonStreamingClient([first, second])
    sleeps = 0

    async def backoff(delay: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == 2:
            assert probe.session is not None
            probe.session.state["untouched"]["marker"].append("changed")

    monkeypatch.setattr(entities.asyncio, "sleep", backoff)
    response = await _entity(provider, client, probe=probe).run(_request("first", "B"))

    _assert_failed(provider, response, "_MissingParent", "latest refusal")
    assert sleeps == len(client.received_messages) == 2


@pytest.mark.parametrize("unsupported", [object(), float("nan")], ids=["opaque", "nonfinite"])
@pytest.mark.parametrize("refuse", [False, True])
async def test_non_json_options_disable_retry_without_rejecting_first_invocation(
    unsupported: Any, refuse: bool
) -> None:
    provider = _provider()
    client: Any = _RefusingClient() if refuse else RecordingChatClient(conversation_id="S1")
    request = _request("first", "B")
    request["options"]["custom"] = unsupported

    response = await _entity(provider, client).run(request)

    if refuse:
        _assert_failed(provider, response, "_MissingParent", "first observed refusal")
        assert len(client.calls) == 1
    else:
        assert response.text == "reply-1"
        assert len(client.received_messages) == 1
        assert client.received_options[0]["custom"] is unsupported


async def test_unchanged_pydantic_response_class_allows_typed_retry() -> None:
    def refuse() -> ChatResponse:
        raise _MissingParent("typed refusal")

    client = _ScriptedNonStreamingClient([
        refuse,
        ChatResponse(messages=[Message("assistant", ['{"answer": 42}'])], conversation_id="S1"),
    ])
    provider = _provider()
    response = await _entity(provider, client).run(
        RunRequest(message="answer", correlation_id="first", response_format=Answer, options={"store": True})
    )

    assert response.additional_properties.get("durable_status") != "error"
    assert response.value == {"answer": 42}
    assert len(client.received_options) == 2
    assert all(options["response_format"] is Answer for options in client.received_options)
    child: CompletableTask[Any] = CompletableTask()
    task = DurableAgentTask(child, Answer, "review")
    child.complete(serialize_agent_response(response))
    assert task.get_result().value == Answer(answer=42)


def test_retry_snapshot_keeps_schema_identity_and_all_other_options() -> None:
    class OtherAnswer(BaseModel):
        answer: int

    session = AgentSession()
    options: dict[str, Any] = {"response_format": Answer, "custom": {"nested": [1]}}
    kwargs = {"messages": [Message("user", ["hello"])], "options": options}
    before = entities._retry_snapshot(session, kwargs, set())
    assert before is not None
    assert before == entities._retry_snapshot(session, kwargs, set())
    options["response_format"] = OtherAnswer
    assert before != entities._retry_snapshot(session, kwargs, set())
    options["response_format"] = Answer
    options["custom"]["nested"].append(2)
    assert before != entities._retry_snapshot(session, kwargs, set())


class _OpaqueAgent:
    name = "opaque"
    default_options = {"store": True}

    def __init__(self) -> None:
        self.context_providers: list[Any] = []
        self.effects: list[str] = []

    def create_session(self, *, session_id: str) -> AgentSession:
        return AgentSession(session_id=session_id)

    async def run(self, *, session: AgentSession, messages: list[Message], options: dict[str, Any]) -> AgentResponse:
        self.effects.append("accepted opaque side effect")
        raise _MissingParent("opaque refusal with unchanged session")


async def test_opaque_non_agent_refusal_never_retries_unchanged_state() -> None:
    provider = _provider()
    agent = _OpaqueAgent()
    response = await AgentEntity(cast(Any, agent), state_provider=provider).run(_request("first", "B"))

    _assert_failed(provider, response, "_MissingParent", "opaque refusal with unchanged session")
    assert agent.effects == ["accepted opaque side effect"]
    assert _committed(provider).data.ingested_messages == {}


class _OpaqueClient:
    def __init__(self, inner: Any) -> None:
        self.inner = inner

    def get_response(self, **kwargs: Any) -> Any:
        return self.inner.get_response(**kwargs)


@pytest.mark.parametrize("refuse", [False, True])
async def test_opaque_client_does_not_claim_acceptance_or_retry(refuse: bool) -> None:
    provider = _provider()
    inner: Any = _RefusingClient() if refuse else RecordingChatClient(conversation_id="S1")
    response = await _entity(provider, _OpaqueClient(inner)).run(_request("first", "B"))

    assert _committed(provider).data.ingested_messages == {}
    if refuse:
        _assert_failed(provider, response, "_MissingParent", "first observed refusal")
        assert len(inner.calls) == 1
    else:
        assert response.text == "reply-1"
        assert len(inner.received_messages) == 1


class _FilterInputs(AgentMiddleware):
    async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
        context.messages = [message for message in context.messages if message.message_id == "B"]
        await call_next()


class _ExternalHistory(HistoryProvider):
    def __init__(self, *, store_inputs: bool, stored: list[Message]) -> None:
        super().__init__("external", store_inputs=store_inputs)
        self.stored = stored

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return deepcopy(self.stored)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.stored.extend(deepcopy(list(messages)))


class _HistoryClient(ChatMiddlewareLayer, RecordingChatClient):
    """Execute Core's per-service-call history middleware, not just the leaf."""


@pytest.mark.parametrize("external", [False, True], ids=["durable", "external"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("store_inputs", [False, True], ids=["no-input-retention", "retain-inputs"])
@pytest.mark.parametrize("filtered", [False, True], ids=["unchanged", "filtered"])
async def test_success_receipts_follow_actual_client_owned_inputs(
    external: bool, per_call: bool, store_inputs: bool, filtered: bool
) -> None:
    stored: list[Message] = []

    def build(provider: JsonStateProvider, *, filter_inputs: bool) -> tuple[AgentEntity, _SessionProbe]:
        primary = (
            _ExternalHistory(store_inputs=store_inputs, stored=stored)
            if external
            else DurableHistoryProvider("history", store_inputs=store_inputs)
        )
        probe = _SessionProbe()
        agent = NonStreamingAgent(
            client=_HistoryClient(),
            name="review",
            context_providers=[probe, primary],
            middleware=[_FilterInputs()] if filter_inputs else [],
            require_per_service_call_history_persistence=per_call,
        )
        return AgentEntity(agent, state_provider=provider), probe

    provider = _provider(service_id=None)
    entity, probe = build(provider, filter_inputs=filtered)
    request = _request("first", "A", "B")
    request["options"]["store"] = False
    response = await entity.run(request)

    expected = ["B"] if filtered else ["A", "B"]
    assert response.text == "reply-1"
    assert [message["message_id"] for message in probe.entries[0]["inputs"]] == expected
    assert set(_committed(provider).data.ingested_messages) == {f"occ-{key}" for key in expected}
    assert provider.successful_writes == 1

    cold = JsonStateProvider(deepcopy(provider.raw), session_id="thread", entity_name="review")
    cold_entity, cold_probe = build(cold, filter_inputs=False)
    request = _request("second", "A", "B", "C")
    request["options"]["store"] = False
    assert (await cold_entity.run(request)).text == "reply-1"
    # New inputs are distinct from loaded model context, which may include B.
    assert [message["message_id"] for message in cold_probe.entries[0]["inputs"]] == (["A", "C"] if filtered else ["C"])
    assert set(_committed(cold).data.ingested_messages) == {"occ-A", "occ-B", "occ-C"}


_PRIVATE_OUTPUT = "private-model-output-sentinel-4926"


class _EchoValidator(BaseModel):
    answer: str

    @field_validator("answer")
    @classmethod
    def reject(cls, value: str) -> str:
        raise ValueError(f"validator included {value}")


@pytest.mark.parametrize("schema", [Answer, _EchoValidator], ids=["field-error", "custom-validator"])
async def test_validation_failure_has_safe_diagnostics_and_cold_typed_delivery(
    schema: type[BaseModel], caplog: pytest.LogCaptureFixture
) -> None:
    provider = _provider()
    client = _ScriptedNonStreamingClient([
        ChatResponse(messages=[Message("assistant", [json.dumps({"answer": _PRIVATE_OUTPUT})])], conversation_id="S1")
    ])
    request = RunRequest(
        message="public question", correlation_id="first", response_format=schema, options={"store": True}
    )
    caplog.set_level(logging.ERROR, logger="agent_framework.durabletask")

    response = await _entity(provider, client).run(request)

    safe = "Validation failed with 1 error(s). Input details omitted."
    _assert_failed(provider, response, "ValidationError", safe)
    wire = serialize_agent_response(response)
    terminal = provider.raw["data"]["terminalResults"]["first"]
    assert terminal["error"] == {"code": "ValidationError", "message": safe}
    assert _PRIVATE_OUTPUT not in json.dumps(wire)
    assert _PRIVATE_OUTPUT not in json.dumps(terminal)
    records = [record for record in caplog.records if record.name == "agent_framework.durabletask"]
    assert len(records) == 1
    assert safe in records[0].getMessage()
    assert records[0].exc_info is None
    assert _PRIVATE_OUTPUT not in caplog.text

    child: CompletableTask[Any] = CompletableTask()
    task = DurableAgentTask(child, schema, "review")
    child.complete(wire)
    delivered = task.get_result()
    assert delivered.value is None
    assert serialize_agent_response(delivered) == wire
    cold = JsonStateProvider(deepcopy(provider.raw), session_id="thread", entity_name="review")
    cold_client = _ScriptedNonStreamingClient([])
    duplicate = await _entity(cold, cold_client).run(request)
    assert serialize_agent_response(duplicate) == wire
    assert cold.successful_writes == 0
    assert cold_client.received_messages == []


@pytest.mark.parametrize("error_type", [ValueError, OSError])
async def test_ordinary_errors_keep_details_and_traceback(
    error_type: type[Exception], caplog: pytest.LogCaptureFixture
) -> None:
    def fail() -> ChatResponse:
        raise error_type("ordinary error detail")

    provider = _provider()
    caplog.set_level(logging.ERROR, logger="agent_framework.durabletask")
    response = await _entity(provider, _ScriptedNonStreamingClient([fail])).run(_request("first", "B"))

    _assert_failed(provider, response, error_type.__name__, "ordinary error detail")
    record = next(record for record in caplog.records if record.name == "agent_framework.durabletask")
    assert record.exc_info is not None
    assert "ordinary error detail" in caplog.text


class _FailingCallback:
    def __init__(self, validation: bool) -> None:
        self.validation = validation
        self.calls: list[str] = []

    def fail(self) -> None:
        if self.validation:
            _EchoValidator.model_validate({"answer": _PRIVATE_OUTPUT})
        raise RuntimeError("ordinary callback detail")

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: AgentCallbackContext) -> None:
        self.calls.append("stream")
        self.fail()

    async def on_agent_response(self, response: AgentResponse, context: AgentCallbackContext) -> None:
        self.calls.append("final")
        self.fail()


@pytest.mark.parametrize("validation", [False, True], ids=["ordinary", "validation"])
async def test_callback_warnings_sanitize_only_pydantic_failures(
    validation: bool, caplog: pytest.LogCaptureFixture
) -> None:
    # Check the real validator includes the sentinel, including in its custom msg.
    with pytest.raises(ValidationError, match=_PRIVATE_OUTPUT):
        _EchoValidator.model_validate({"answer": _PRIVATE_OUTPUT})
    callback = _FailingCallback(validation)
    provider = JsonStateProvider()
    caplog.set_level(logging.WARNING, logger="agent_framework.durabletask")
    response = await AgentEntity(
        Agent(client=RecordingChatClient(), name="callback"), callback=callback, state_provider=provider
    ).run(RunRequest(message="public question", correlation_id="first"))

    assert response.text == "reply-1"
    assert callback.calls == ["stream", "final"]
    assert provider.successful_writes == 1
    records = [record for record in caplog.records if record.name == "agent_framework.durabletask"]
    assert len(records) == 2
    if validation:
        assert all("Input details omitted" in record.getMessage() and record.exc_info is None for record in records)
        assert _PRIVATE_OUTPUT not in caplog.text
    else:
        assert all(
            "ordinary callback detail" in record.getMessage() and record.exc_info is not None for record in records
        )
