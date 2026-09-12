# Copyright (c) Microsoft. All rights reserved.

"""Maintenance operations through the synchronous Functions wrapper and real AgentEntity."""

import hashlib
import json
from collections.abc import AsyncIterable, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import azure.durable_functions as df
import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    HistoryProvider,
    Message,
    ResponseStream,
)
from agent_framework_durabletask import DurableAgentState, serialize_agent_response
from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask import _entities as entities_module
from agent_framework_durabletask import _retention as retention_module
from agent_framework_durabletask import _state_migration as migration_module
from agent_framework_durabletask._message_identity import message_identity
from typing_extensions import Self

from agent_framework_azurefunctions import _entities as af_entities
from agent_framework_azurefunctions._entities import AzureFunctionEntityStateProvider, create_agent_entity

NOW = datetime(2040, 1, 1, 12, tzinfo=timezone.utc)
SOURCE_ID = "@dafx-maintenance@legacy-source"
DESTINATION_ID = "@dafx-maintenance@destination"


class _ClockType(type):
    def __instancecheck__(cls, instance: Any) -> bool:
        # Parsed timestamps remain real datetime objects, not instances of the test subclass.
        return isinstance(instance, datetime)


class Clock(datetime, metaclass=_ClockType):
    current = NOW

    @classmethod
    def now(cls, tz: Any = None) -> Self:
        return cls.fromtimestamp(cls.current.timestamp(), tz=tz)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> type[Clock]:
    monkeypatch.setattr(Clock, "current", NOW)
    for module in (state_module, entities_module, retention_module, migration_module):
        monkeypatch.setattr(module, "datetime", Clock)
    return Clock


def _wire(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


def _digest(raw: dict[str, Any]) -> str:
    text = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _size(raw: dict[str, Any]) -> int:
    return len(json.dumps(raw, allow_nan=False))


class Model:
    def __init__(self) -> None:
        self.additional_properties: dict[str, Any] = {}
        self.calls: list[list[Message]] = []

    def get_response(self, messages: Any, *, stream: bool = False, **kwargs: Any) -> Any:
        self.calls.append(list(messages))
        text = f"reply-{len(self.calls)}"

        async def complete() -> ChatResponse:
            return ChatResponse(messages=[Message("assistant", [text])])

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            yield ChatResponseUpdate(role="assistant", contents=[Content.from_text(text)])

        def finalize(items: Sequence[ChatResponseUpdate]) -> ChatResponse:
            return ChatResponse.from_updates(items)

        return ResponseStream(updates(), finalizer=finalize) if stream else complete()


class Hooks(ContextProvider):
    def __init__(self) -> None:
        super().__init__("maintenance-probe")
        self.calls: list[str] = []

    async def before_run(self, **kwargs: Any) -> None:
        self.calls.append("before")

    async def after_run(self, **kwargs: Any) -> None:
        self.calls.append("after")


class ExternalHistory(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("external")
        self.calls: list[tuple[str, str | None]] = []
        self.rows: dict[str | None, list[Message]] = {
            SOURCE_ID: [Message("user", ["preexisting external input"], message_id="external-old")]
        }

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append(("get", session_id))
        return deepcopy(self.rows.get(session_id, []))

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        self.calls.append(("save", session_id))
        self.rows.setdefault(session_id, []).extend(deepcopy(list(messages)))


class Host:
    def __init__(
        self,
        raw: dict[str, Any] | None = None,
        *,
        external: ExternalHistory | None = None,
        **settings: Any,
    ) -> None:
        self.raw = _wire(raw or {})
        self.writes = 0
        self.fail_writes = False
        self.model = Model()
        self.hooks = Hooks()
        self.callback = Mock(spec=["on_streaming_response_update", "on_agent_response"])
        client: Any = self.model
        providers: list[Any] = [self.hooks] if external is None else [external, self.hooks]
        agent = Agent(client=client, name="maintenance", context_providers=providers)
        self.handler = create_agent_entity(agent, callback=self.callback, deployment_mode="isolated_v2", **settings)
        self.contexts: list[Mock] = []

    def _write(self, raw: dict[str, Any]) -> None:
        if self.fail_writes:
            raise OSError("injected commit failure")
        self.raw = _wire(raw)
        self.writes += 1

    def invoke(self, operation: str, request: Any = None) -> Any:
        # Fresh context/provider on every invocation, but the actual wrapper owns dispatch.
        context = Mock(spec=df.DurableEntityContext)
        context.entity_name = "dafx-maintenance"
        context.entity_key = "destination"
        context.operation_name = operation
        context.get_input.return_value = request
        context.get_state.side_effect = lambda *args, **kwargs: _wire(self.raw)
        context.set_state.side_effect = self._write
        self.contexts.append(context)
        self.handler(context)
        context.set_result.assert_called_once()
        return context.set_result.call_args.args[0]

    def assert_quiet(self) -> None:
        assert self.model.calls == [] and self.hooks.calls == [] and self.callback.mock_calls == []


def _source() -> dict[str, Any]:
    return {
        "schemaVersion": "1.1.0",
        "futureRoot": {"keep": ["雪", None]},
        "data": {
            "conversationHistory": [
                {
                    "$type": kind,
                    "correlationId": "legacy-done",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "messages": [
                        {
                            "role": role,
                            "contents": [{"$type": "text", "text": text}],
                            "messageId": f"legacy-{kind}",
                        }
                    ],
                }
                for kind, role, text in (
                    ("request", "user", "retained legacy input"),
                    ("response", "assistant", "retained legacy answer"),
                )
            ],
            "session": {"session_id": SOURCE_ID, "state": {"opaque": {"keep": [1, 3]}}},
            "futureData": {"keep": [False, 0]},
        },
    }


def _request(*, evidence: bool = False) -> dict[str, Any]:
    source = _source()
    if evidence:
        source["data"]["ingestedPositions"] = {"upstream": 3}
        source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = "wf_upstream_3"
    digest = _digest(source)
    request: dict[str, Any] = {
        "source": source,
        "sourceDigest": digest,
        "sourceSessionId": SOURCE_ID,
        "destinationSessionId": DESTINATION_ID,
        "migrationId": "migration-1",
        "ownershipTransferId": "operator-transfer-1",
    }
    if evidence:
        request["deliveryEvidence"] = {
            "sourceDigest": digest,
            "evidenceId": "operator-journal-1",
            "complete": True,
            "messages": [
                Message("user", [f"accepted {position}"], message_id=f"wf_upstream_{position}").to_dict()
                for position in (1, 3)
            ],
        }
    return request


def _mailboxes() -> dict[str, Any]:
    raw = _source()
    raw["schemaVersion"] = "2.0.0"
    state = DurableAgentState.from_dict(raw)
    state.data.ingested_messages = {"old-input": ["a" * 64]}
    state.data.completed_correlations["long-gone"] = {"completedAt": "2020-01-01T00:00:00+00:00"}
    for correlation, error in (("expired-success", False), ("expired-error", True), ("live", False)):
        response = AgentResponse[Any](
            messages=[
                Message(
                    "assistant",
                    [Content.from_error(message="original failure", error_code="OriginalError")]
                    if error
                    else [Content.from_text("original result", additional_properties={"keep": ["雪"]})],
                    message_id=f"answer-{correlation}",
                )
            ],
            response_id=f"response-{correlation}",
            additional_properties={"durable_status": "error" if error else "success", "nested": {"keep": [1]}},
            value=None if error else {"original": [1, 2]},
        )
        state.record_response(
            correlation,
            response,
            delivery_window_seconds=60,
            now=NOW if correlation == "live" else NOW - timedelta(minutes=2),
        )
        assert state.data.response_mailbox[correlation]["response"] == serialize_agent_response(response)
    return _wire(state.to_dict())


def _without_expired(raw: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(raw)
    for correlation in ("expired-success", "expired-error"):
        del result["data"]["responseMailbox"][correlation]
    return result


@pytest.mark.parametrize(
    ("operation", "correlation"),
    [("run", "new"), ("run", "legacy-done"), ("run_agent", "legacy-done"), ("reset", None), ("expire_responses", None)],
)
def test_af_legacy_lookup_allowed_but_actual_writer_rejected_before_model_hooks_or_write(
    operation: str, correlation: str | None
) -> None:
    raw = _source()
    lookup = DurableAgentState.from_dict(raw).try_get_agent_response("legacy-done")
    assert lookup is not None and lookup.text == "retained legacy answer"
    host = Host(raw)
    result = host.invoke(operation, {"message": "blocked", "correlationId": correlation})
    assert result["status"] == "error" and "read-only" in result["error"] and "Legacy" in result["error"]
    assert host.raw == raw and host.writes == 0
    host.contexts[-1].set_state.assert_not_called()
    host.assert_quiet()


@pytest.mark.parametrize("evidence", [False, True])
def test_af_migrate_uses_actual_destination_raw_digest_and_optional_evidence(
    clock: type[Clock], evidence: bool
) -> None:
    request = _request(evidence=evidence)
    before = deepcopy(request)
    assert request["sourceDigest"] != _digest(DurableAgentState.from_dict(request["source"]).to_dict())
    host = Host(response_delivery_window_seconds=17)
    result = host.invoke("migrate", request)
    assert result == {"status": "migrated", "migrationId": "migration-1", "sessionId": DESTINATION_ID}
    assert host.writes == 1 and request == before
    state = DurableAgentState.from_dict(host.raw)
    assert state.schema_version == "2.0.0"
    assert state.data.session == request["source"]["data"]["session"]
    expected_history = DurableAgentState.from_dict(request["source"]).to_dict()["data"]["conversationHistory"]
    assert host.raw["data"]["conversationHistory"] == expected_history
    metadata = state.data.unknown_fields["migration"]
    assert metadata == {
        "id": "migration-1",
        "sourceDigest": request["sourceDigest"],
        "sourceSessionId": SOURCE_ID,
        "destinationSessionId": DESTINATION_ID,
        "requestDigest": _digest(request),
        "ownershipTransferId": "operator-transfer-1",
        "createdAt": NOW.isoformat(),
        **({"evidenceId": "operator-journal-1"} if evidence else {}),
    }
    assert state.data.response_mailbox["legacy-done"]["expiresAt"] == (NOW + timedelta(seconds=17)).isoformat()
    assert state.data.completed_correlations["legacy-done"]["legacy"] is True
    if evidence:
        assert state.data.ingested_messages == {
            message["message_id"]: [message_identity(Message.from_dict(deepcopy(message)))]
            for message in request["deliveryEvidence"]["messages"]
        }
        assert "wf_upstream_2" not in state.data.ingested_messages
    assert host.raw["futureRoot"] == request["source"]["futureRoot"]
    assert host.raw["data"]["futureData"] == request["source"]["data"]["futureData"]
    host.assert_quiet()


def test_af_exact_cold_retry_after_v2_run_does_not_rewrite_or_refresh_expiry(clock: type[Clock]) -> None:
    request = _request()
    before_request = deepcopy(request)
    host = Host(response_delivery_window_seconds=17)
    result = host.invoke("migrate", request)
    assert result["status"] == "migrated"
    clock.current = NOW + timedelta(seconds=5)
    assert host.invoke("run", {"message": "new turn", "correlationId": "v2-done"})["type"] == "agent_response"
    assert len(host.model.calls) == 1
    before = deepcopy(host.raw)
    hooks, callbacks = list(host.hooks.calls), list(host.callback.mock_calls)
    clock.current = NOW + timedelta(days=1)
    assert host.invoke("migrate", deepcopy(request)) == result
    host.contexts[-1].set_state.assert_not_called()
    assert host.raw == before and host.writes == 2
    assert host.invoke("migrate", deepcopy(request)) == result
    assert host.raw == before and host.writes == 2
    assert len(host.model.calls) == 1 and host.hooks.calls == hooks and host.callback.mock_calls == callbacks
    assert host.invoke("expire_responses") == {"expired": 2}
    expected = deepcopy(before)
    del expected["data"]["responseMailbox"]
    assert host.raw == expected and host.writes == 3
    assert request == before_request


@pytest.mark.parametrize(
    "invalid", ["sourceDigest", "destinationSessionId", "sourceSessionId", "migrationId", "ownershipTransferId"]
)
def test_af_migration_invalid_identity_or_digest_returns_error_without_write(invalid: str) -> None:
    request = _request()
    if invalid == "sourceDigest":
        request[invalid] = _digest(DurableAgentState.from_dict(request["source"]).to_dict())
    elif invalid == "destinationSessionId":
        request[invalid] = "@dafx-other@destination"
    elif invalid == "sourceSessionId":
        request[invalid] = DESTINATION_ID
    else:
        request[invalid] = " \t"
    before = deepcopy(request)
    host = Host()
    result = host.invoke("migrate", request)
    assert result["status"] == "error" and result["error"]
    assert host.raw == {} and host.writes == 0 and request == before
    host.contexts[-1].set_state.assert_not_called()
    host.assert_quiet()


@pytest.mark.parametrize("change", ["source", "ownershipTransferId", "migrationId"])
def test_af_mismatched_retry_never_replaces_committed_migration(clock: type[Clock], change: str) -> None:
    request = _request()
    host = Host()
    assert host.invoke("migrate", request)["status"] == "migrated"
    before = deepcopy(host.raw)
    changed = deepcopy(request)
    if change == "source":
        changed["source"]["futureRoot"]["keep"].append("changed source")
        changed["sourceDigest"] = _digest(changed["source"])
    else:
        changed[change] += "-different"
    result = host.invoke("migrate", changed)
    assert result["status"] == "error" and "empty" in result["error"]
    assert host.raw == before and host.writes == 1
    host.contexts[-1].set_state.assert_not_called()
    host.assert_quiet()


def test_af_nonempty_destination_without_migration_is_not_overwritten(clock: type[Clock]) -> None:
    raw = _mailboxes()
    host = Host(raw)
    result = host.invoke("migrate", _request())
    assert result["status"] == "error" and "empty" in result["error"]
    assert host.raw == raw and host.writes == 0
    host.contexts[-1].set_state.assert_not_called()
    host.assert_quiet()


@pytest.mark.parametrize("correlation", ["expired-success", "expired-error"])
def test_af_expired_duplicate_removes_physical_mailbox_without_reexecution(
    clock: type[Clock], correlation: str
) -> None:
    raw = _mailboxes()
    host = Host(raw)
    result = host.invoke("run", {"message": "duplicate", "correlationId": correlation})
    assert result["type"] == "agent_response"
    assert result["additional_properties"] == {
        "durable_status": "already_completed",
        "correlation_id": correlation,
        "durable_outcome": "failed" if correlation == "expired-error" else "succeeded",
    }
    assert result["messages"][0]["contents"][0]["error_code"] == "response_expired"
    assert raw["data"]["responseMailbox"][correlation]["response"]["response_id"] == f"response-{correlation}"
    assert host.raw == _without_expired(raw) and host.writes == 1
    host.assert_quiet()


def test_af_idle_expiry_preserves_live_original_history_and_forever_completion_receipts(clock: type[Clock]) -> None:
    raw = _mailboxes()
    host = Host(raw)
    assert host.invoke("expire_responses") == {"expired": 2}
    assert host.raw == _without_expired(raw) and host.writes == 1
    assert host.invoke("expire_responses") == {"expired": 0}
    host.contexts[-1].set_state.assert_not_called()
    assert host.writes == 1
    live = host.invoke("run", {"message": "duplicate", "correlationId": "live"})
    assert live == raw["data"]["responseMailbox"]["live"]["response"]
    assert host.writes == 1
    clock.current = NOW + timedelta(days=36500)
    assert host.invoke("expire_responses") == {"expired": 1}
    expected = _without_expired(raw)
    del expected["data"]["responseMailbox"]
    assert host.raw == expected and host.writes == 2
    for correlation in raw["data"]["completedCorrelations"]:
        result = host.invoke("run", {"message": "old duplicate", "correlationId": correlation})
        assert result["additional_properties"]["durable_status"] == "already_completed"
    assert host.raw == expected and host.writes == 2
    host.assert_quiet()


@pytest.mark.parametrize("operation", ["migrate", "expire_responses"])
def test_af_failed_maintenance_commit_restores_real_provider_cache_and_retries(
    clock: type[Clock], monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    providers: list[AzureFunctionEntityStateProvider] = []
    originals: list[DurableAgentState] = []

    def capture(context: Any) -> AzureFunctionEntityStateProvider:
        provider = AzureFunctionEntityStateProvider(context)
        providers.append(provider)
        originals.append(provider.state)
        return provider

    # Observe the actual state provider, never replace AgentEntity or its maintenance methods.
    monkeypatch.setattr(af_entities, "AzureFunctionEntityStateProvider", capture)
    raw = {} if operation == "migrate" else _mailboxes()
    host = Host(raw)
    request = _request(evidence=True)
    before_request = deepcopy(request)
    host.fail_writes = True
    result = host.invoke(operation, request)
    assert result == {"status": "error", "error": "injected commit failure"}
    assert providers[-1].state is originals[-1]
    assert providers[-1].state.to_dict() == (raw or DurableAgentState().to_dict())
    assert host.raw == raw and host.writes == 0
    host.contexts[-1].set_state.assert_called_once()
    host.fail_writes = False
    result = host.invoke(operation, request)
    expected = (
        {"status": "migrated", "migrationId": "migration-1", "sessionId": DESTINATION_ID}
        if operation == "migrate"
        else {"expired": 2}
    )
    assert result == expected and host.writes == 1 and request == before_request
    host.assert_quiet()


@pytest.mark.parametrize("operation", ["migrate", "expire_responses"])
def test_af_strict_maintenance_budget_includes_full_retained_floor_and_metadata(
    clock: type[Clock], operation: str
) -> None:
    raw = {} if operation == "migrate" else _mailboxes()
    request = _request()
    request["source"]["data"]["conversationHistory"][0]["messages"][0]["contents"][0]["text"] = "雪" * 500
    request["sourceDigest"] = _digest(request["source"])
    before_request = deepcopy(request)
    sizing = Host(raw)
    expected_result = sizing.invoke(operation, request)
    if operation == "migrate":
        assert expected_result["status"] == "migrated"
    else:
        assert expected_result == {"expired": 2}
    expected = deepcopy(sizing.raw)
    full_size = _size(expected)
    if operation == "migrate":
        assert full_size > _size(request["source"])
        normalized = DurableAgentState.from_dict(request["source"]).to_dict()
        assert expected["data"]["conversationHistory"] == normalized["data"]["conversationHistory"]
    else:
        assert expected == _without_expired(raw)
        assert full_size > _size(expected["data"]["responseMailbox"])
    rejected = Host(raw, max_state_bytes=full_size - 1)
    result = rejected.invoke(operation, request)
    assert result["status"] == "error" and "max_state_bytes" in result["error"]
    assert rejected.raw == raw and rejected.writes == 0
    rejected.contexts[-1].set_state.assert_not_called()
    accepted = Host(raw, max_state_bytes=full_size)
    assert accepted.invoke(operation, request) == expected_result
    assert accepted.raw == expected and accepted.writes == 1
    assert request == before_request and "truncation" not in accepted.raw["data"]
    rejected.assert_quiet()
    accepted.assert_quiet()


def test_af_external_get_and_save_use_logical_source_identity_on_cold_destination_after_retry(
    clock: type[Clock],
) -> None:
    external = ExternalHistory()
    host = Host(external=external)
    request = _request()
    before_request = deepcopy(request)
    rows = {key: [message.to_dict() for message in messages] for key, messages in external.rows.items()}
    assert host.invoke("migrate", request)["status"] == "migrated"
    # Input IDs do not authorize or prove external transfer; the operator owns that boundary.
    assert external.calls == []
    assert {key: [message.to_dict() for message in messages] for key, messages in external.rows.items()} == rows
    for index in range(2):
        if index:
            clock.current = NOW + timedelta(seconds=10)
            before = deepcopy(host.raw)
            assert host.invoke("migrate", request)["status"] == "migrated"
            assert host.raw == before
            host.contexts[-1].set_state.assert_not_called()
        response = host.invoke("run", {"message": f"new turn {index}", "correlationId": f"new-{index}"})
        assert response["type"] == "agent_response"
        assert host.raw["data"]["session"]["session_id"] == SOURCE_ID
    assert external.calls == [(phase, SOURCE_ID) for _ in range(2) for phase in ("get", "save")]
    assert set(external.rows) == {SOURCE_ID}
    assert [message.text for message in external.rows[SOURCE_ID]] == [
        "preexisting external input",
        "new turn 0",
        "reply-1",
        "new turn 1",
        "reply-2",
    ]
    assert "retained legacy input" not in [message.text for batch in host.model.calls for message in batch]
    assert request == before_request
