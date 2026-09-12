# Copyright (c) Microsoft. All rights reserved.

"""Occurrence ambiguity, serialized forwarding and declared HITL reconstruction."""

import json
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, get_args
from unittest.mock import Mock, patch

import pytest
from agent_framework import (
    AgentExecutor,
    AgentExecutorResponse,
    AgentResponse,
    AgentSession,
    Content,
    Executor,
    Message,
    Workflow,
    WorkflowContext,
    WorkflowExecutor,
    handler,
    response_handler,
)
from agent_framework._types import ContentType
from pydantic import BaseModel

from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from agent_framework_durabletask._workflows.context import WorkflowOrchestrationContext
from agent_framework_durabletask._workflows.orchestrator import (
    SOURCE_HITL_RESPONSE,
    TaskType,
    _match_occurrences,
    _prepare_activity_task,
    _prepare_agent_task,
    _prepare_subworkflow_task,
    _process_activity_result,
    _route_result_messages,
    _WorkflowDeliveryLedger,
)
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_INPUT_KEY,
    deserialize_value,
    reconstruct_to_type,
    serialize_value,
)


class _Agent:
    name = "target"
    id = "target"
    description = None

    def create_session(self, **kwargs: Any) -> AgentSession:
        return AgentSession(**kwargs)

    async def run(self, messages: Any = None, **kwargs: Any) -> AgentResponse:
        raise AssertionError("These tests schedule tasks without invoking a model")


def _agent(**kwargs: Any) -> AgentExecutor:
    agent: Any = _Agent()
    return AgentExecutor(agent, id="target", **kwargs)


def _host(instance_id: str = "review-run") -> Any:
    host = Mock(spec=WorkflowOrchestrationContext)
    host.instance_id = instance_id
    host.prepare_activity_task.side_effect = lambda name, payload: payload
    host.call_sub_orchestrator.side_effect = lambda name, payload, **kwargs: payload
    return host


def _envelope(messages: list[Message], latest: list[Message]) -> AgentExecutorResponse:
    return AgentExecutorResponse("producer", AgentResponse(messages=latest), list(messages))


def _dispatch(host: Any, executor: AgentExecutor, source: Any, ledger: _WorkflowDeliveryLedger) -> tuple[Any, Any]:
    _prepare_agent_task(host, executor, executor.id, source, "review", ledger)
    call = host.prepare_agent_task.call_args
    messages = json.loads(json.dumps(call.args[3]))
    return messages, call.kwargs["context_message_ids"]


@pytest.mark.parametrize("latest_alias", [False, True])
@pytest.mark.parametrize("selection", [[0], [0, 2]])
def test_sparse_reused_alias_never_suppresses_a_previously_unsent_position(
    latest_alias: bool, selection: list[int]
) -> None:
    shared = Message("assistant", ["same"], message_id="opaque")
    original = [shared, shared, Message("user", ["other"])]
    source = _envelope(original, [shared] if latest_alias else [])
    positions = list(selection)
    target = _agent(context_mode="custom", context_filter=lambda values: [values[i] for i in positions])
    host, ledger = _host(), _WorkflowDeliveryLedger(instance_id="review-run")

    first, first_ids = _dispatch(host, target, source, ledger)
    positions[0] = 1
    second, second_ids = _dispatch(host, target, source, ledger)

    assert first == [original[i].to_dict() for i in selection]
    assert second == [shared.to_dict()]
    assert len(second_ids) == 1
    assert set(first_ids).isdisjoint(second_ids)
    assert shared.message_id == "opaque"
    assert source.full_conversation[0] is source.full_conversation[1] is shared


@pytest.mark.parametrize("detached", [False, True])
def test_whole_list_positions_keep_repeated_aliases_and_equal_detached_copies(detached: bool) -> None:
    shared = Message("assistant", ["same"], message_id="opaque")
    source = _envelope([shared, shared], [shared])
    host, ledger = _host(), _WorkflowDeliveryLedger(instance_id="review-run")
    target = _agent(context_mode="custom", context_filter=lambda values: deepcopy(values) if detached else list(values))

    first, ids = _dispatch(host, target, source, ledger)
    repeated, repeated_ids = _dispatch(host, target, source, ledger)

    assert first == [shared.to_dict(), shared.to_dict()]
    assert len(set(ids)) == 2
    assert repeated == repeated_ids == []


def test_alias_reused_more_than_source_multiplicity_is_a_new_handoff_occurrence() -> None:
    shared = Message("assistant", ["same"], message_id="opaque")
    source = _envelope([shared], [shared])
    target = _agent(context_mode="custom", context_filter=lambda values: [values[0], values[0]])
    messages, ids = _dispatch(_host(), target, source, _WorkflowDeliveryLedger())
    assert messages == [shared.to_dict(), shared.to_dict()]
    assert len(set(ids)) == 2


def test_equal_but_wrong_position_aliases_are_not_a_whole_list_copy() -> None:
    first = Message("user", ["same"])
    second = Message("user", ["same"])
    assert _match_occurrences([first, first], [first, second], ["first", "second"]) == ["first", None]
    assert _match_occurrences([first, second], [first, second], ["first", "second"]) == ["first", "second"]
    detached = deepcopy(first)
    assert _match_occurrences([detached, detached], [first, second], ["first", "second"]) == [None, None]


def test_previously_unique_global_alias_does_not_collapse_a_later_repeated_history() -> None:
    shared = Message("user", ["same"], message_id="opaque")
    host, ledger = _host(), _WorkflowDeliveryLedger(instance_id="review-run")
    _dispatch(host, _agent(), _envelope([shared], []), ledger)
    repeated = _envelope([shared, shared], [])
    sent, ids = _dispatch(host, _agent(), repeated, ledger)
    assert sent == [shared.to_dict(), shared.to_dict()]
    assert len(set(ids)) == 2


class _Relay(Executor):
    def __init__(self, mode: str = "unchanged") -> None:
        super().__init__(id="relay")
        self.mode = mode

    @handler
    async def relay(self, message: AgentExecutorResponse, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
        if self.mode == "new-response":
            # The same producer, response ID, application ID and text are not an event ID.
            latest = deepcopy(message.agent_response.messages)
            message = AgentExecutorResponse(
                message.executor_id,
                AgentResponse(messages=latest, response_id=message.agent_response.response_id),
                [*message.full_conversation[: -len(latest)], *latest],
            )
        elif self.mode == "replace-message":
            # Even reusing the AgentResponse does not prove a replacement is forwarding.
            latest = deepcopy(message.agent_response.messages)
            message.agent_response.messages = latest
            message.full_conversation = [*message.full_conversation[: -len(latest)], *latest]
        elif self.mode == "changed-value":
            message.agent_response.messages[-1].contents = [Content.from_text("changed")]
        await ctx.send_message(message, target_id="target")


_ADDRESS = {"root_instance_id": "review-run", "root_workflow_name": "review", "request_path_prefix": ""}


@pytest.mark.parametrize("child_dispatch", [False, True])
def test_failed_forwarding_dispatch_does_not_commit_provenance(child_dispatch: bool) -> None:
    latest = Message("assistant", ["same"], message_id="opaque")
    source = _envelope([latest], [latest])
    ledger = _WorkflowDeliveryLedger(instance_id="review-run")
    host = _host()
    host.prepare_activity_task.side_effect = OSError("prepare failed")
    host.call_sub_orchestrator.side_effect = OSError("prepare failed")
    child = Mock(spec=WorkflowExecutor)
    child.workflow = Mock()
    child.workflow.name = "inner"
    with pytest.raises(OSError, match="prepare failed"):
        if child_dispatch:
            _prepare_subworkflow_task(host, child, source, "child", _ADDRESS, ledger)
        else:
            _prepare_activity_task(host, "relay", source, "producer", None, "review", _ADDRESS, ledger)
    assert ledger == _WorkflowDeliveryLedger(instance_id="review-run")
    assert not hasattr(source.agent_response, "_durable_workflow_forwarding")
    assert latest.message_id == "opaque"


def _relay_result(host: Any, ledger: _WorkflowDeliveryLedger, source: Any, relay: _Relay) -> Any:
    payload = _prepare_activity_task(host, relay.id, source, "producer", None, "review", _ADDRESS, ledger)
    raw = execute_workflow_activity(relay, payload)
    result = _process_activity_result(raw, relay.id, None, [])
    result.source_message = source
    return result


def _routed(result: Any, ledger: _WorkflowDeliveryLedger) -> AgentExecutorResponse:
    workflow = Mock(spec=Workflow)
    workflow.edge_groups = []
    pending: dict[str, list[tuple[Any, str]]] = {}
    _route_result_messages(result, workflow, pending, defaultdict(dict), ledger)
    response = pending["target"][0][0]
    assert isinstance(response, AgentExecutorResponse)
    return response


@pytest.mark.parametrize("mode", ["unchanged", "new-response", "replace-message", "changed-value"])
@pytest.mark.parametrize("context_mode", ["full", "last_agent"])
def test_real_activity_checkpoint_forwarding_distinguishes_new_identical_producer_events(
    mode: str, context_mode: str
) -> None:
    latest = Message("assistant", ["same"], message_id="opaque")
    source = _envelope([Message("user", ["question"], message_id="question"), latest], [latest])
    before = [message.to_dict() for message in source.full_conversation]
    response_before = source.agent_response.to_dict()

    def replay() -> tuple[Any, Any]:
        host, ledger = _host(), _WorkflowDeliveryLedger(instance_id="review-run")
        target = _agent(context_mode=context_mode)
        _, first_ids = _dispatch(host, target, source, ledger)
        result = _relay_result(host, ledger, source, _Relay(mode))
        forwarded = _routed(result, ledger)
        assert forwarded is not source
        assert forwarded.agent_response is not source.agent_response
        assert forwarded.full_conversation[-1] is not latest
        sent, sent_ids = _dispatch(host, target, forwarded, ledger)
        if mode == "unchanged":
            assert sent == sent_ids == []
        else:
            expected = Message("assistant", ["changed"], message_id="opaque") if mode == "changed-value" else latest
            assert sent == [expected.to_dict()]
            assert len(sent_ids) == 1
            assert set(first_ids).isdisjoint(sent_ids)
        return sent, sent_ids

    assert replay() == replay()
    assert [message.to_dict() for message in source.full_conversation] == before
    assert source.agent_response.to_dict() == response_before
    assert not hasattr(source.agent_response, "_durable_workflow_forwarding")


def test_unchanged_relay_preserves_repeated_anonymous_history_positions() -> None:
    shared = Message("user", ["same"])
    latest = Message("assistant", ["answer"])
    source = _envelope([shared, shared, latest], [latest])
    host, ledger = _host(), _WorkflowDeliveryLedger(instance_id="review-run")
    _, first_ids = _dispatch(host, _agent(), source, ledger)
    forwarded = _routed(_relay_result(host, ledger, source, _Relay()), ledger)
    sent, sent_ids = _dispatch(host, _agent(), forwarded, ledger)
    assert len(set(first_ids)) == 3
    assert sent == sent_ids == []


@pytest.mark.parametrize("relay_only", [False, True])
def test_child_keeps_inherited_prefix_receipts_but_scopes_fresh_equal_outputs(relay_only: bool) -> None:
    shared = Message("user", ["same"], message_id="shared")
    latest = Message("assistant", ["same"], message_id="opaque")
    source = _envelope([shared, shared, latest], [latest])
    host, ledger = _host(), _WorkflowDeliveryLedger(instance_id="review-run")
    _, original_ids = _dispatch(host, _agent(), source, ledger)
    child = Mock(spec=WorkflowExecutor)
    child.workflow = Mock()
    child.workflow.name = "inner"
    new_ids: list[str] = []
    for child_id in ["review-run::child::0", "review-run::child::1"]:
        payload = _prepare_subworkflow_task(host, child, source, child_id, _ADDRESS, ledger)
        child_input = unwrap_workflow_input(payload)
        inherited = deserialize_value(child_input[SUBWORKFLOW_INPUT_KEY])
        assert isinstance(inherited, AgentExecutorResponse)
        child_ledger = _WorkflowDeliveryLedger(instance_id=child_id)
        if relay_only:
            # A real child activity re-dispatch must retain the parent's witness.
            result = _relay_result(_host(child_id), child_ledger, inherited, _Relay())
        else:
            fresh = Message("assistant", ["same"], message_id="opaque")
            produced = _envelope([*inherited.full_conversation, fresh], [fresh])
            result = _process_activity_result(
                json.dumps({"sent_messages": [{"message": serialize_value(produced), "target_id": "target"}]}),
                "child",
                None,
                [],
            )
        result.source_message = source
        result.child_instance_id = child_id
        result.task_type = TaskType.SUBWORKFLOW
        output = _routed(result, ledger)
        sent, sent_ids = _dispatch(host, _agent(), output, ledger)
        if relay_only:
            assert sent == sent_ids == []
        else:
            assert sent == [latest.to_dict()]
            assert len(sent_ids) == 1
            assert set(original_ids + new_ids).isdisjoint(sent_ids)
            new_ids.extend(sent_ids)
    assert [message.message_id for message in source.full_conversation] == ["shared", "shared", "opaque"]


@dataclass
class _Request:
    prompt: str


class _ValidatedReply(BaseModel):
    approved: bool


class _HumanGate(Executor):
    def __init__(self) -> None:
        super().__init__(id="human-gate")
        self.seen: list[tuple[str | None, Any]] = []

    @handler
    async def start(self, message: str, ctx: WorkflowContext) -> None:
        await ctx.request_info(_Request(message), Content, request_id="request-1")

    @response_handler
    async def content_reply(self, original_request: _Request, response: Content, ctx: WorkflowContext) -> None:
        self.seen.append((ctx.request_id, response))

    @response_handler
    async def message_reply(self, original_request: _Request, response: Message, ctx: WorkflowContext) -> None:
        self.seen.append((ctx.request_id, response))

    @response_handler
    async def validated_reply(
        self, original_request: _Request, response: _ValidatedReply, ctx: WorkflowContext
    ) -> None:
        self.seen.append((ctx.request_id, response))


def _hitl_input(value: Any, response_type: type) -> str:
    return json.dumps({
        "message": serialize_value({
            "request_id": "request-1",
            "original_request": serialize_value(_Request("Review")),
            "response": value,
            "response_type": f"{response_type.__module__}:{response_type.__name__}",
        }),
        "source_executor_ids": [f"{SOURCE_HITL_RESPONSE}_request-1"],
    })


@pytest.mark.parametrize("reply_type", [Content, Message])
def test_external_framework_reply_reconstructs_and_receives_request_id(reply_type: type) -> None:
    payload: dict[str, Any] = {
        "type": "image_generation_tool_result",
        "outputs": [{"type": "untrusted.module:Class", "items": [{"type": "application_data"}]}],
        "additional_properties": {"opaque": {"type": "untrusted.module:Class"}},
    }
    if reply_type is Message:
        payload = {"role": "user", "message_id": "application-id", "contents": [payload]}
    before = deepcopy(payload)
    executor = _HumanGate()
    result = json.loads(execute_workflow_activity(executor, _hitl_input(payload, reply_type)))
    assert result["pending_request_info_events"] == []
    assert len(executor.seen) == 1
    request_id, reply = executor.seen[0]
    assert request_id == "request-1"
    assert isinstance(reply, reply_type)
    content = reply.contents[0] if isinstance(reply, Message) else reply
    assert isinstance(content, Content)
    assert content.outputs == [{"type": "untrusted.module:Class", "items": [{"type": "application_data"}]}]
    assert content.additional_properties == {"opaque": {"type": "untrusted.module:Class"}}
    assert payload == before


def test_request_id_is_preserved_for_an_already_supported_pydantic_reply() -> None:
    executor = _HumanGate()
    execute_workflow_activity(executor, _hitl_input({"approved": True}, _ValidatedReply))
    assert executor.seen == [("request-1", _ValidatedReply(approved=True))]


@pytest.mark.parametrize(
    ("value", "reply_type"),
    [
        pytest.param({"wrong_field": True}, _ValidatedReply, id="invalid-model"),
        pytest.param("wrong-runtime-type", Content, id="unmatched-handler"),
        pytest.param({"contents": []}, Message, id="missing-message-role"),
        pytest.param({"type": ""}, Content, id="invalid-content-type"),
        pytest.param({"__pickled__": "not-a-pickle", "__type__": "untrusted:Class"}, Content, id="markers"),
    ],
)
def test_invalid_hitl_reply_fails_activity_instead_of_silently_completing(value: Any, reply_type: type) -> None:
    executor = _HumanGate()
    with pytest.raises((TypeError, ValueError)):
        execute_workflow_activity(executor, _hitl_input(value, reply_type))
    assert executor.seen == []


@pytest.mark.parametrize("reply_type", [Content, Message])
def test_declared_framework_reconstruction_does_not_import_payload_type_names(reply_type: type) -> None:
    payload: dict[str, Any] = {"type": "untrusted.module:Class", "additional_properties": {"type": "other:Class"}}
    if reply_type is Message:
        payload = {"role": "user", "contents": [payload]}
    with patch("importlib.import_module", side_effect=AssertionError("Payload type names must remain data")):
        restored = reconstruct_to_type(payload, reply_type)
    assert isinstance(restored, reply_type)


def test_content_known_nested_envelopes_are_rebuilt_but_application_results_stay_dicts() -> None:
    payload = {
        "type": "function_approval_response",
        "approved": True,
        "function_call": {"type": "function_call", "call_id": "call", "name": "lookup", "arguments": "{}"},
        "result": {"type": "application_result", "items": [False, None, 0]},
    }
    restored = reconstruct_to_type(payload, Content)
    assert isinstance(restored, Content)
    assert isinstance(restored.function_call, Content)
    assert restored.function_call.call_id == "call"
    assert restored.result == payload["result"]


@pytest.mark.parametrize("content_type", get_args(ContentType))
def test_all_declared_core_content_kinds_reconstruct_without_a_copied_kind_allowlist(content_type: str) -> None:
    restored = reconstruct_to_type({"type": content_type}, Content)
    assert isinstance(restored, Content)
    assert restored.type == content_type
