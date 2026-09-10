# Copyright (c) Microsoft. All rights reserved.

"""Source-side workflow deltas, driven through projection and generator dispatch."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Generator
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock

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
)
from agent_framework._workflows._edge import EdgeGroup, FanInEdgeGroup, FanOutEdgeGroup, SingleEdgeGroup

from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._workflows.orchestrator import (
    _AGENT_TASK_MESSAGE_PREVIEW_LIMIT,
    _build_context_messages,
    _prepare_agent_task,
    _WorkflowDeliveryLedger,
    build_agent_executor_response,
    run_workflow_orchestrator,
)
from agent_framework_durabletask._workflows.serialization import deserialize_value, serialize_value


class _StubAgent:
    name = "stub"
    id = "stub"
    description = None

    def create_session(self, **kwargs: Any) -> AgentSession:
        return AgentSession(**kwargs)

    async def run(self, messages: Any = None, **kwargs: Any) -> AgentResponse:
        raise AssertionError("The recording host must not invoke a model")


def _agent(executor_id: str = "target", **kwargs: Any) -> AgentExecutor:
    agent: Any = _StubAgent()
    return AgentExecutor(agent, id=executor_id, **kwargs)


def _message(position: int, producer: str = "source", text: str | None = None) -> Message:
    return Message(
        "assistant", [text if text is not None else f"{producer}-{position}"], message_id=f"wf_{producer}_{position}"
    )


def _response(
    messages: list[Message], producer: str = "source", *, latest: list[Message] | None = None
) -> AgentExecutorResponse:
    return AgentExecutorResponse(
        executor_id=producer,
        agent_response=AgentResponse(messages=messages[-1:] if latest is None else latest),
        full_conversation=list(messages),
    )


def _ids(call: dict[str, Any]) -> list[str | None]:
    """Read application IDs, which are independent of delivery occurrences."""
    assert call["contextMessages"] is not None
    return [message.get("message_id") for message in call["contextMessages"]]


def _occurrences(call: dict[str, Any]) -> list[str]:
    ids = call["contextMessageIds"]
    assert isinstance(ids, list)
    assert len(ids) == len(call["contextMessages"])
    assert all(isinstance(value, str) and value.startswith("wf:occurrence:") for value in ids)
    return ids


def _texts(call: dict[str, Any]) -> list[str]:
    assert call["contextMessages"] is not None
    return [Message.from_dict(message).text for message in call["contextMessages"]]


class _RecordingHost:
    """Return recorded task outcomes while capturing the adapter-boundary payloads."""

    supports_event_streaming = False
    current_utc_datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def __init__(
        self,
        *,
        instance_id: str = "run",
        is_replaying: bool = False,
        activities: dict[str, list[dict[str, Any]]] | None = None,
        agent_reply: str | None = None,
    ) -> None:
        self.instance_id = instance_id
        self.is_replaying = is_replaying
        self.calls: list[dict[str, Any]] = []
        self.activity_inputs: list[dict[str, Any]] = []
        self.waited_for: list[str] = []
        self.batch_sizes: list[int] = []
        self.statuses: list[Any] = []
        self.fail_prepare = False
        self._activities = {name: iter(results) for name, results in (activities or {}).items()}
        self._agent_reply = agent_reply

    def prepare_agent_task(
        self,
        executor_id: str,
        message: str,
        orchestration_instance_id: str,
        context_messages: list[dict[str, Any]] | None = None,
        context_message_ids: list[str] | None = None,
    ) -> AgentResponse:
        assert (context_messages is None) == (context_message_ids is None)
        if context_messages is not None:
            assert context_message_ids is not None
            assert len(context_messages) == len(context_message_ids)
        # JSON round-trip the complete adapter arguments, not just a count of messages.
        self.calls.append(
            json.loads(
                json.dumps(
                    {
                        "executorId": executor_id,
                        "message": message,
                        "instanceId": orchestration_instance_id,
                        "contextMessages": context_messages,
                        "contextMessageIds": context_message_ids,
                    },
                    allow_nan=False,
                )
            )
        )
        if self.fail_prepare:
            raise OSError("injected preparation failure")
        reply = self._agent_reply if self._agent_reply is not None else f"reply-{len(self.calls)}"
        return AgentResponse(messages=[Message("assistant", [reply])])

    def prepare_activity_task(self, activity_name: str, input_json: str) -> str:
        payload = json.loads(input_json)
        self.activity_inputs.append(payload)
        return json.dumps(next(self._activities[payload["executor_id"]]))

    def call_sub_orchestrator(self, name: str, input: Any, instance_id: str | None = None) -> Any:
        raise AssertionError("These workflows have no child orchestrations")

    def task_all(self, tasks: list[Any]) -> list[Any]:
        self.batch_sizes.append(len(tasks))
        return tasks

    def task_any(self, tasks: list[Any]) -> Any:
        raise AssertionError("These workflows do not race tasks")

    def wait_for_external_event(self, name: str) -> str:
        self.waited_for.append(name)
        return "approved"

    def create_timer(self, fire_at: datetime) -> Any:
        raise AssertionError("These workflows have no timers")

    def set_custom_status(self, status: Any) -> None:
        self.statuses.append(deepcopy(status))

    def new_uuid(self) -> str:
        raise AssertionError("Message identity must not require UUIDs")

    def cancel_task(self, task: Any) -> None:
        raise AssertionError("These workflows do not cancel tasks")

    def get_task_result(self, task: Any) -> Any:
        return task


def _dispatch(
    host: _RecordingHost, executor: AgentExecutor, message: Any, ledger: _WorkflowDeliveryLedger
) -> dict[str, Any]:
    _prepare_agent_task(host, executor, executor.id, message, "delta", ledger)
    return host.calls[-1]


def _workflow(nodes: list[Any], edges: list[EdgeGroup], *, max_iterations: int = 20) -> Any:
    # The graph container is passive here. Real executors and edge groups exercise
    # the orchestrator's production classification, routing and task grouping.
    workflow = Mock(spec=Workflow)
    workflow.name = "delta"
    workflow.start_executor_id = nodes[0].id
    workflow.executors = {node.id: node for node in nodes}
    workflow.edge_groups = edges
    workflow.max_iterations = max_iterations
    return workflow


def _activity(executor_id: str) -> Mock:
    executor = Mock(spec=Executor)
    executor.id = executor_id
    executor.input_types = [str]
    return executor


def _activity_result(messages: list[Any], target: str | None = "target", *, request: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {
        "sent_messages": [{"message": serialize_value(message), "target_id": target} for message in messages]
    }
    if request:
        result["pending_request_info_events"] = [
            {"request_id": "approval", "source_executor_id": "gate", "data": "review"}
        ]
    return result


def _finish(orchestration: Generator[Any, Any, Any], yielded: Any) -> Any:
    while True:
        try:
            yielded = orchestration.send(yielded)
        except StopIteration as completed:
            return completed.value


def _run(host: _RecordingHost, workflow: Any) -> Any:
    orchestration = run_workflow_orchestrator(host, workflow, "start")
    return _finish(orchestration, next(orchestration))


def test_message_identity_uses_canonical_full_message_json() -> None:
    original = Message(
        "assistant",
        [{"type": "function_call", "call_id": "call", "name": "lookup", "arguments": {"b": 2, "a": 1}}],
        message_id="custom-id",
        author_name="author",
        additional_properties={"nested": {"z": "世界", "a": 1}},
        raw_representation=object(),
    )
    reordered = Message(
        "assistant",
        [{"arguments": {"a": 1, "b": 2}, "name": "lookup", "call_id": "call", "type": "function_call"}],
        message_id="custom-id",
        author_name="author",
        additional_properties={"nested": {"a": 1, "z": "世界"}},
    )
    canonical = json.dumps(
        original.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    assert message_identity(original) == expected == message_identity(reordered)
    assert message_identity(Message.from_dict(json.loads(original.to_json()))) == expected
    assert original.message_id == "custom-id"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("message_id", "other-id"),
        ("role", "system"),
        ("author_name", "other-author"),
        ("contents", [{"type": "text", "text": "changed"}]),
        ("contents", [{"type": "text", "text": "second"}, {"type": "text", "text": "first"}]),
        ("additional_properties", {"_is_summary": True}),
    ],
)
def test_message_identity_detects_meaningful_changes(field: str, value: Any) -> None:
    original = Message("assistant", ["first", "second"], message_id="custom-id", author_name="author")
    modified = original.to_dict()
    modified[field] = value

    assert message_identity(original) != message_identity(Message.from_dict(modified))


@pytest.mark.parametrize("mode", ["full", "last_agent", "custom"])
def test_empty_projection_is_explicit_and_does_not_leak_raw_response(mode: str) -> None:
    secret = Message("assistant", ["unselected secret " * 10_000], message_id="secret")
    upstream = _response([] if mode == "full" else [_message(1)], latest=[] if mode == "last_agent" else [secret])
    executor = _agent(context_mode=mode, context_filter=(lambda messages: []) if mode == "custom" else None)
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()

    assert _build_context_messages(executor, upstream) == []
    call = _dispatch(host, executor, upstream, ledger)

    assert call["contextMessages"] == []
    assert call["message"] == ""
    assert "secret" not in json.dumps(call)
    assert ledger.sent == {}


def test_projection_remains_stateless_and_does_not_stamp_filter_input() -> None:
    original = Message("user", ["anonymous"])
    upstream = _response([original])
    executor = _agent(context_mode="custom", context_filter=lambda messages: [m for m in messages if not m.message_id])
    expected = [original.to_dict()]

    assert _build_context_messages(executor, upstream) == expected
    call = _dispatch(_RecordingHost(), executor, upstream, _WorkflowDeliveryLedger())
    assert _ids(call) == [None]
    assert len(_occurrences(call)) == 1
    assert _build_context_messages(executor, upstream) == expected
    assert original.message_id is None


def test_no_context_raw_requests_are_not_deduplicated_or_truncated() -> None:
    executor = _agent()
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    prompt = "a new request " * 1000

    assert _build_context_messages(executor, prompt) is None
    for _ in range(2):
        call = _dispatch(host, executor, prompt, ledger)
        assert call["contextMessages"] is None
        assert call["message"] == prompt
    assert ledger.sent == {}


def test_last_agent_delta_preserves_all_selected_assistant_and_tool_messages() -> None:
    executor = _agent(context_mode="last_agent")
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    first = Message("assistant", ["answer"], message_id="wf_source_5")
    second = Message(
        "tool", [{"type": "function_result", "call_id": "call", "result": "result"}], message_id="wf_source_6"
    )
    upstream = _response([_message(0), first, second], latest=[first, second])

    call = _dispatch(host, executor, upstream, ledger)
    assert call["contextMessages"] == [first.to_dict(), second.to_dict()]
    assert call["message"] == ""
    assert _ids(_dispatch(host, executor, upstream, ledger)) == []
    assert ledger.sent == {
        "target": set(zip(_occurrences(call), [message_identity(first), message_identity(second)], strict=True))
    }


def test_missing_custom_filter_fails_instead_of_forwarding_unfiltered_input() -> None:
    executor = _agent(context_mode="custom", context_filter=lambda messages: messages)
    executor._context_filter = None
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()

    with pytest.raises(ValueError, match="context_filter"):
        _dispatch(host, executor, _response([_message(1)]), ledger)
    assert host.calls == []
    assert ledger == _WorkflowDeliveryLedger()


def test_empty_selection_does_not_mark_unselected_positions_delivered() -> None:
    executor = _agent(context_mode="custom", context_filter=lambda messages: [] if len(messages) == 1 else messages[:1])
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()

    assert _ids(_dispatch(host, executor, _response([_message(1)]), ledger)) == []
    assert _ids(_dispatch(host, executor, _response([_message(1), _message(2)]), ledger)) == ["wf_source_1"]


def test_sparse_custom_selection_delivers_previously_skipped_lower_positions() -> None:
    positions = [0, 2]
    executor = _agent(context_mode="custom", context_filter=lambda messages: [messages[i] for i in positions])
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    upstream = _response([_message(i) for i in [1, 2, 3, 4]])

    first = _dispatch(host, executor, upstream, ledger)
    positions[:] = [1, 3]
    second = _dispatch(host, executor, upstream, ledger)
    repeated = _dispatch(host, executor, upstream, ledger)

    assert _ids(first) == ["wf_source_1", "wf_source_3"]
    assert _ids(second) == ["wf_source_2", "wf_source_4"]
    assert _ids(repeated) == []
    assert repeated["message"] == ""
    assert len(set(_occurrences(first) + _occurrences(second))) == 4
    assert ledger.sent == {
        "target": {
            (occurrence, message_identity(Message.from_dict(message)))
            for call in [first, second]
            for occurrence, message in zip(_occurrences(call), call["contextMessages"], strict=True)
        }
    }


def test_reordered_projection_preserves_new_message_order_without_a_cursor() -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    positions = [2, 3]
    executor = _agent(context_mode="custom", context_filter=lambda messages: [messages[i] for i in positions])
    upstream = _response([_message(i) for i in [4, 2, 3, 1]])

    first = _dispatch(host, executor, upstream, ledger)
    positions[:] = [0, 1, 2, 3]
    call = _dispatch(host, executor, upstream, ledger)

    assert _ids(call) == ["wf_source_4", "wf_source_2"]
    assert set(_occurrences(first)).isdisjoint(_occurrences(call))
    assert call["message"] == "source-2"


def test_fanout_delivery_is_independent_for_each_target() -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    left, right = _agent("left"), _agent("right")
    first = _response([_message(1), _message(3)])
    next_projection = _response([_message(2), _message(4), first.full_conversation[0]], latest=[])

    left_first = _dispatch(host, left, first, ledger)
    right_next = _dispatch(host, right, next_projection, ledger)
    left_next = _dispatch(host, left, next_projection, ledger)
    right_first = _dispatch(host, right, first, ledger)
    assert _ids(left_first) == ["wf_source_1", "wf_source_3"]
    assert _ids(right_next) == ["wf_source_2", "wf_source_4", "wf_source_1"]
    assert _ids(left_next) == ["wf_source_2", "wf_source_4"]
    assert _ids(right_first) == ["wf_source_3"]
    assert _occurrences(left_first) == [_occurrences(right_next)[-1], *_occurrences(right_first)]
    assert _occurrences(left_next) == _occurrences(right_next)[:2]


def test_fanin_tracks_each_messages_producer_not_the_immediate_sender() -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()
    common = _message(0, "input")
    first = [
        _response([common, _message(100, "A")], "relay"),
        _response([common, _message(1, "B")], "relay"),
    ]
    second = [
        _response([common, _message(99, "A"), first[0].full_conversation[-1]], "other-relay", latest=[]),
        _response([common, _message(0, "B"), first[1].full_conversation[-1]], "other-relay", latest=[]),
    ]

    projected = [m.to_dict() for response in first for m in response.full_conversation]
    assert _build_context_messages(executor, first) == projected
    assert _ids(_dispatch(host, executor, first, ledger)) == ["wf_input_0", "wf_A_100", "wf_B_1"]
    assert _ids(_dispatch(host, executor, second, ledger)) == ["wf_A_99", "wf_B_0"]


@pytest.mark.parametrize(
    "message_id",
    [
        "wf_source_7",
        "wf:external:" + "a" * 64,
        "wf:projection:" + "b" * 64,
        "custom-id",
        "wf_not_a_position",
        "wf:external:not-a-hash",
        "wf:projection:not-a-hash",
    ],
)
def test_same_id_content_changes_are_delivered_and_exact_repeats_are_not(message_id: str) -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()
    original = Message("assistant", ["old"], message_id=message_id)
    changed = Message("assistant", ["new"], message_id=message_id)

    source = _response([original])
    first = _dispatch(host, executor, source, ledger)
    assert _texts(first) == ["old"]
    copied = _agent(context_mode="custom", context_filter=lambda messages: deepcopy(messages))
    assert _occurrences(_dispatch(host, copied, source, ledger)) == []
    redacted = _agent(context_mode="custom", context_filter=lambda messages: [deepcopy(changed)])
    call = _dispatch(host, redacted, source, ledger)
    assert _texts(call) == ["new"]
    assert _ids(call) == [message_id]
    assert _occurrences(call) == _occurrences(first)
    assert _occurrences(_dispatch(host, redacted, source, ledger)) == []
    assert _occurrences(_dispatch(host, executor, source, ledger)) == []
    independent = _dispatch(host, executor, _response([deepcopy(original)]), ledger)
    assert _ids(independent) == [message_id]
    assert set(_occurrences(first)).isdisjoint(_occurrences(independent))
    assert original.message_id == changed.message_id == message_id


def test_nontext_updates_and_repeated_ids_with_different_contents_are_not_lost() -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()
    original = Message(
        "tool", [{"type": "function_result", "call_id": "call", "result": {"answer": 1}}], message_id="m"
    )
    changed = Message("tool", [{"type": "function_result", "call_id": "call", "result": {"answer": 2}}], message_id="m")
    source = _response([original, deepcopy(original), changed])
    call = _dispatch(host, executor, source, ledger)

    assert call["contextMessages"] == [message.to_dict() for message in source.full_conversation]
    assert _ids(call) == ["m"] * 3
    assert len(set(_occurrences(call))) == 3
    assert call["message"] == ""
    assert _occurrences(_dispatch(host, executor, source, ledger)) == []
    assert original.message_id == changed.message_id == "m"


def test_distinct_custom_ids_do_not_globally_deduplicate_equal_text() -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()

    occurrences: list[str] = []
    for message_id in ["first-request", "second-request"]:
        call = _dispatch(host, executor, _response([Message("user", ["again"], message_id=message_id)]), ledger)
        assert _ids(call) == [message_id]
        occurrences.extend(_occurrences(call))
    assert len(set(occurrences)) == 2


@pytest.mark.parametrize("mode", ["full", "last_agent", "custom"])
@pytest.mark.parametrize("batch", [False, True])
def test_equal_custom_ids_from_different_producers_have_distinct_transport_identities(mode: str, batch: bool) -> None:
    original = Message(
        "assistant",
        ["approved"],
        message_id="custom-id",
        author_name="reviewer",
        additional_properties={"nested": {"decision": "approved"}},
    )
    before = original.to_dict()
    sources = [_response([deepcopy(original)], producer) for producer in ["left", "right"]]
    executor = _agent(
        context_mode=mode,
        context_filter=(lambda messages: [m for m in messages if m.message_id == "custom-id"])
        if mode == "custom"
        else None,
    )
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    deliveries: list[Any] = [sources] if batch else sources
    calls = [_dispatch(host, executor, message, ledger) for message in deliveries]
    transported = [message for call in calls for message in call["contextMessages"]]

    assert transported == [before, before]
    # Equal application payloads still represent two independently produced events.
    assert len({identity for call in calls for identity in _occurrences(call)}) == 2
    assert len({message_identity(Message.from_dict(message)) for message in transported}) == 1
    assert _ids(_dispatch(host, executor, list(reversed(sources)), ledger)) == []
    assert [source.full_conversation[0].to_dict() for source in sources] == [before, before]
    assert _build_context_messages(executor, sources) == [before, before]


def test_custom_id_scopes_use_unambiguous_producer_and_id_addresses() -> None:
    sources = [
        _response([Message("assistant", ["approved"], message_id=message_id)], producer)
        for producer, message_id in [("left_part", "id"), ("left", "part_id")]
    ]
    call = _dispatch(_RecordingHost(), _agent(), sources, _WorkflowDeliveryLedger())

    assert _ids(call) == ["id", "part_id"]
    assert len(set(_occurrences(call))) == 2


def test_mixed_custom_and_anonymous_messages_keep_each_producers_identity() -> None:
    custom = Message("assistant", ["approved"], message_id="custom-id")
    anonymous = Message("user", ["same"])
    sources = [_response(deepcopy([custom, anonymous]), producer) for producer in ["left", "right"]]
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()

    first = _dispatch(host, executor, sources, ledger)
    assert _ids(first) == ["custom-id", None, "custom-id", None]
    assert len(set(_occurrences(first))) == 4
    assert _ids(_dispatch(host, executor, sources, ledger)) == []
    assert custom.message_id == "custom-id"
    assert anonymous.message_id is None


def test_custom_source_identity_survives_chained_copies_and_serialization() -> None:
    original = Message("assistant", ["approved"], message_id="custom-id", additional_properties={"label": "original"})
    before = original.to_dict()
    upstream = _response([original], "origin")
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()

    first = _dispatch(host, executor, upstream, ledger)
    assert _ids(first) == ["custom-id"]
    forwarded = build_agent_executor_response("relay", "reply", None, upstream)
    forwarded = deserialize_value(json.loads(json.dumps(serialize_value(forwarded))))
    ledger.identify(forwarded, upstream)
    assert _ids(_dispatch(host, executor, forwarded, ledger)) == ["wf_relay_1"]
    assert ledger.identify(forwarded)[0][0] == _occurrences(first)[0]
    next_hop = build_agent_executor_response("next", "reply", None, forwarded)
    assert _ids(_dispatch(host, executor, next_hop, ledger)) == ["wf_next_2"]
    assert forwarded.full_conversation[0].to_dict() == next_hop.full_conversation[0].to_dict() == before
    assert original.to_dict() == upstream.full_conversation[0].to_dict() == before


@pytest.mark.parametrize("message_id", ["wf_origin_7", "wf:external:" + "a" * 64, "wf:projection:" + "b" * 64])
def test_workflow_shaped_application_ids_do_not_conflate_independent_relay_outputs(message_id: str) -> None:
    original = Message("assistant", ["approved"], message_id=message_id)
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()

    first = _dispatch(host, executor, _response([original], "left"), ledger)
    assert _ids(first) == [message_id]
    copied = Message.from_dict(host.calls[-1]["contextMessages"][0])
    source = _response([copied], "right")
    second = _dispatch(host, executor, source, ledger)
    assert _ids(second) == [message_id]
    assert set(_occurrences(first)).isdisjoint(_occurrences(second))
    assert _occurrences(_dispatch(host, executor, source, ledger)) == []
    forwarded = build_agent_executor_response("relay", "reply", None, _response([copied], "right"))
    assert forwarded.full_conversation[0].message_id == message_id
    assert original.message_id == message_id


def test_anonymous_equal_text_is_identified_by_source_position_without_mutating_callers() -> None:
    executor = _agent(context_mode="custom", context_filter=lambda messages: messages)
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    originals = [Message("user", ["again"]) for _ in range(3)]
    before = [m.to_dict() for m in originals]

    first = _dispatch(host, executor, _response(originals[:2], latest=[]), ledger)
    source = _response(originals, latest=[])
    second = _dispatch(host, executor, source, ledger)
    assert _ids(first) == [None, None]
    assert _ids(second) == [None]
    assert len(set(_occurrences(first) + _occurrences(second))) == 3
    copied = _agent(context_mode="custom", context_filter=lambda messages: deepcopy(messages))
    assert _occurrences(_dispatch(host, copied, source, ledger)) == []
    assert [m.to_dict() for m in originals] == before
    assert all(m.message_id is None for m in originals)


def test_detached_anonymous_copies_do_not_guess_positions_from_equal_text() -> None:
    executor = _agent(
        context_mode="custom", context_filter=lambda messages: [Message.from_dict(messages[-1].to_dict())]
    )
    originals = [Message("user", ["again"]), Message("user", ["again"])]

    def replay() -> list[dict[str, Any]]:
        host = _RecordingHost()
        ledger = _WorkflowDeliveryLedger()
        _dispatch(host, executor, _response(originals[:1]), ledger)
        _dispatch(host, executor, _response(originals), ledger)
        return host.calls

    calls = replay()
    assert [_texts(call) for call in calls] == [["again"], ["again"]]
    assert _ids(calls[0]) == _ids(calls[1]) == [None]
    assert _occurrences(calls[0]) != _occurrences(calls[1])
    assert calls == replay()
    assert all(m.message_id is None for m in originals)


def test_anonymous_projection_reordering_uses_original_positions() -> None:
    def project(messages: list[Message]) -> list[Message]:
        return [messages[2], messages[0]] if len(messages) == 3 else [messages[3], messages[1]]

    executor = _agent(
        context_mode="custom",
        context_filter=project,
    )
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    originals = [Message("user", [str(i)]) for i in range(4)]

    first = _dispatch(host, executor, _response(originals[:3], latest=[]), ledger)
    source = _response(originals, latest=[])
    second = _dispatch(host, executor, source, ledger)
    assert _texts(first) == ["2", "0"]
    assert _texts(second) == ["3", "1"]
    assert _ids(first) == _ids(second) == [None, None]
    assert len(set(_occurrences(first) + _occurrences(second))) == 4
    assert _occurrences(_dispatch(host, executor, source, ledger)) == []
    assert all(m.message_id is None for m in originals)


def test_reused_anonymous_object_at_two_source_positions_keeps_both_occurrences() -> None:
    original = Message("user", ["again"])
    call = _dispatch(_RecordingHost(), _agent(), _response([original, original]), _WorkflowDeliveryLedger())

    assert _ids(call) == [None, None]
    assert len(set(_occurrences(call))) == 2
    assert original.message_id is None


def test_anonymous_same_position_in_different_producers_does_not_collide() -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()
    sources = [_response([Message("user", ["same"])], producer) for producer in ["left", "right"]]

    first = _dispatch(host, executor, sources, ledger)
    assert _ids(first) == [None, None]
    assert len(set(_occurrences(first))) == 2
    assert _ids(_dispatch(host, executor, list(reversed(sources)), ledger)) == []
    assert all(source.full_conversation[0].message_id is None for source in sources)


def test_anonymous_ids_remain_stable_when_forwarded_around_a_cycle() -> None:
    original = Message("user", ["source input"])
    upstream = _response([original], "origin")
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()

    first = _dispatch(host, executor, upstream, ledger)
    assert _ids(first) == [None]
    forwarded = build_agent_executor_response("relay", "reply", None, upstream)
    assert _ids(_dispatch(host, executor, forwarded, ledger)) == ["wf_relay_1"]
    assert ledger.identify(forwarded)[0][0] == _occurrences(first)[0]
    assert original.message_id is None
    assert forwarded.full_conversation[0].message_id is None


def test_synthesized_anonymous_messages_are_distinct_per_handoff_and_replay_stable() -> None:
    executor = _agent(
        context_mode="custom",
        context_filter=lambda messages: [Message("system", ["summary"]), Message("system", ["summary"])],
    )
    upstream = _response([_message(1)])

    def replay() -> list[dict[str, Any]]:
        host = _RecordingHost()
        ledger = _WorkflowDeliveryLedger()
        for _ in range(2):
            _dispatch(host, executor, deepcopy(upstream), ledger)
        return host.calls

    first, repeated = replay()
    assert _ids(first) == _ids(repeated) == [None, None]
    assert len(set(_occurrences(first) + _occurrences(repeated))) == 4
    assert [first, repeated] == replay()
    assert _texts(first) == _texts(repeated) == ["summary", "summary"]


def test_synthesized_message_with_explicit_id_is_a_new_occurrence_each_handoff() -> None:
    executor = _agent(
        context_mode="custom",
        context_filter=lambda messages: [Message("system", [f"summary-{len(messages)}"], message_id="summary")],
    )
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()

    source = _response([_message(1)])
    first = _dispatch(host, executor, source, ledger)
    repeated = _dispatch(host, executor, source, ledger)
    changed = _dispatch(host, executor, _response([_message(1), _message(2)]), ledger)
    assert _texts(first) == _texts(repeated) == ["summary-1"]
    assert _texts(changed) == ["summary-2"]
    assert all(_ids(call) == ["summary"] for call in [first, repeated, changed])
    assert len({identity for call in [first, repeated, changed] for identity in _occurrences(call)}) == 3


def test_last_agent_projection_without_original_position_gets_a_stable_handoff_identity() -> None:
    latest = Message("assistant", ["response absent from full_conversation"])
    upstream = _response([], latest=[latest])
    executor = _agent(context_mode="last_agent")

    first = _dispatch(_RecordingHost(), executor, upstream, _WorkflowDeliveryLedger())
    replay = _dispatch(_RecordingHost(), executor, deepcopy(upstream), _WorkflowDeliveryLedger())
    assert first == replay
    assert len(_occurrences(first)) == 1
    assert _ids(first) == [None]
    assert latest.message_id is None


def test_preparation_failure_does_not_mark_delivery_or_consume_synthetic_ordinal() -> None:
    executor = _agent(
        context_mode="custom", context_filter=lambda messages: [Message("system", ["summary"]), *messages]
    )
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    upstream = _response([_message(1)])
    host.fail_prepare = True

    with pytest.raises(OSError, match="preparation failure"):
        _dispatch(host, executor, upstream, ledger)
    assert ledger == _WorkflowDeliveryLedger()

    host.fail_prepare = False
    retried = _dispatch(host, executor, upstream, ledger)
    assert retried == host.calls[0]
    assert len(ledger.sent["target"]) == 2
    assert ledger.handoffs == {"target": 1}


@pytest.mark.parametrize("bad_value", [float("inf"), float("nan")])
def test_serialization_failure_does_not_partially_record_a_batch(bad_value: Any) -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()
    invalid = Message("user", ["bad"], message_id="invalid", additional_properties={"nested": {"value": bad_value}})

    with pytest.raises((TypeError, ValueError)):
        _dispatch(host, executor, _response([_message(1), invalid]), ledger)
    assert host.calls == []
    assert ledger == _WorkflowDeliveryLedger()
    assert _ids(_dispatch(host, executor, _response([_message(1)]), ledger)) == ["wf_source_1"]


def test_projection_can_exclude_non_json_source_values() -> None:
    invalid = Message("user", ["bad"], additional_properties={"nested": {"value": float("nan")}})
    selected = Message("user", ["selected"])
    executor = _agent(
        context_mode="custom", context_filter=lambda messages: [Message.from_dict(messages[-1].to_dict())]
    )

    call = _dispatch(_RecordingHost(), executor, _response([invalid, selected]), _WorkflowDeliveryLedger())
    assert len(_occurrences(call)) == 1
    assert _ids(call) == [None]
    assert _texts(call) == ["selected"]


def test_preview_uses_only_new_selected_text_and_never_the_large_raw_response() -> None:
    selected = _message(1, text="selected")
    excluded = _message(2, text="unselected secret " * 10_000)
    upstream = _response([selected, excluded], latest=[excluded])
    executor = _agent(context_mode="custom", context_filter=lambda messages: messages[:1])
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()

    first = _dispatch(host, executor, upstream, ledger)
    assert first["message"] == "selected"
    assert "secret" not in json.dumps(first)
    repeated = _dispatch(host, executor, upstream, ledger)
    assert repeated["contextMessages"] == []
    assert repeated["message"] == ""
    assert len(json.dumps(repeated)) < 200


def test_large_new_context_retains_its_contents_but_has_a_bounded_preview() -> None:
    latest = _message(1, text="large selected input " * 10_000)
    call = _dispatch(_RecordingHost(), _agent(), _response([latest]), _WorkflowDeliveryLedger())

    assert len(call["message"]) == _AGENT_TASK_MESSAGE_PREVIEW_LIMIT
    assert call["message"] == latest.text[:_AGENT_TASK_MESSAGE_PREVIEW_LIMIT]
    assert call["contextMessages"] == [latest.to_dict()]


def test_eight_hundred_turn_payload_contains_only_new_context_and_a_bounded_envelope() -> None:
    host = _RecordingHost()
    ledger = _WorkflowDeliveryLedger()
    executor = _agent()
    upstream: Any = "initial prompt"
    for turn in range(800):
        upstream = build_agent_executor_response("source", f"turn-{turn}:" + "x" * 700, None, upstream)
        _dispatch(host, executor, upstream, ledger)

    final_call = host.calls[-1]
    latest = upstream.full_conversation[-1]
    latest_bytes = len(json.dumps([latest.to_dict()]).encode("utf-8"))
    payload_bytes = len(json.dumps(final_call).encode("utf-8"))
    projected = _build_context_messages(executor, upstream)
    full_bytes = len(json.dumps(projected).encode("utf-8"))
    assert final_call["contextMessages"] == [latest.to_dict()]
    assert len(_occurrences(final_call)) == 1
    assert payload_bytes <= latest_bytes + _AGENT_TASK_MESSAGE_PREVIEW_LIMIT + 200
    assert full_bytes > 100 * payload_bytes
    assert "initial prompt" not in json.dumps(final_call)

    repeated = _dispatch(host, executor, upstream, ledger)
    assert repeated["contextMessages"] == []
    assert repeated["message"] == ""
    assert len(json.dumps(repeated).encode("utf-8")) < 200


def test_generator_preserves_independent_events_between_parallel_and_sequential_agent_tasks() -> None:
    projections = [_response([_message(i) for i in positions]) for positions in ([1, 3], [2, 4], [4, 1])]
    workflow = _workflow([_activity("source"), _agent()], [])
    host = _RecordingHost(activities={"source": [_activity_result(projections)]})

    assert _run(host, workflow) == []
    assert host.batch_sizes == [1, 1]
    assert [_ids(call) for call in host.calls] == [
        ["wf_source_1", "wf_source_3"],
        ["wf_source_2", "wf_source_4"],
        ["wf_source_4", "wf_source_1"],
    ]
    assert len({identity for call in host.calls for identity in _occurrences(call)}) == 6
    assert host.calls[-1]["message"] == "source-1"


@pytest.mark.parametrize("representation", ["typed", "serialized", "restored"])
@pytest.mark.parametrize("sequential", [False, True])
@pytest.mark.parametrize(
    ("status", "error_code", "include_text", "reason"),
    [
        ("error", "ValueError", True, "a terminal runtime error"),
        (None, "ValueError", True, "a terminal runtime error"),
        (None, "", False, "a terminal runtime error"),
        ("error", None, True, "a terminal runtime error"),
        ("already_completed", "response_expired", False, "an expired durable response"),
        ("already_completed", None, False, "an expired durable response"),
        (None, "response_expired", False, "an expired durable response"),
    ],
)
def test_generator_terminal_agent_result_stops_pending_and_downstream_dispatch(
    representation: str, sequential: bool, status: str | None, error_code: str | None, include_text: bool, reason: str
) -> None:
    secret = "private request and exception details"
    contents = (
        [
            Content.from_error(
                message=secret,
                error_code=error_code,
                error_details=secret,
                additional_properties={"future_error_metadata": {"opaque": [secret]}},
            )
        ]
        if error_code is not None
        else []
    )
    if include_text:
        contents.append(Content.from_text(f"ValueError: {secret}"))
    properties: dict[str, Any] = {"correlation_id": "retained-call", "future_metadata": {"opaque": [secret]}}
    if status is not None:
        properties["durable_status"] = status
    response = AgentResponse(
        messages=[Message("system" if reason == "an expired durable response" else "assistant", contents)],
        additional_properties=properties,
    )
    wire = json.loads(response.to_json())
    assert wire["type"] == "agent_response"
    restored = AgentResponse.from_dict(deepcopy(wire))
    assert restored.to_dict() == wire
    assert restored.additional_properties == properties
    payload: Any = {"typed": response, "serialized": wire, "restored": restored}[representation]
    before = deepcopy(wire)

    workflow = _workflow([_activity("source"), _agent("A"), _agent("B")], [SingleEdgeGroup("A", "B")])
    host = _RecordingHost(activities={"source": [_activity_result([secret, "second", "must not run"], "A")]})
    orchestration = run_workflow_orchestrator(host, workflow, "start")
    yielded = orchestration.send(next(orchestration))
    if sequential:
        orchestration.send(yielded)

    with pytest.raises(RuntimeError) as failure:
        orchestration.send(payload if sequential else [payload])

    assert str(failure.value) == f"Agent executor 'A' returned {reason}."
    assert secret not in str(failure.value)
    assert [call["executorId"] for call in host.calls] == ["delta-A"] * (2 if sequential else 1)
    assert [call["message"] for call in host.calls] == ([secret, "second"] if sequential else [secret])
    assert wire == before == response.to_dict() == restored.to_dict()
    with pytest.raises(StopIteration):
        next(orchestration)


def test_generator_checks_raw_error_before_deserializing_unknown_wire_fields() -> None:
    response = AgentResponse(
        messages=[Message("assistant", [Content.from_error(error_code="ValueError"), "exception text"])]
    )
    wire = json.loads(response.to_json())
    wire["future_response_field"] = {"opaque": True}
    wire["messages"][0]["contents"][0]["future_content_field"] = {"opaque": True}
    before = deepcopy(wire)
    host = _RecordingHost()
    workflow = _workflow([_agent("A"), _agent("B")], [SingleEdgeGroup("A", "B")])
    orchestration = run_workflow_orchestrator(host, workflow, "start")
    next(orchestration)

    with pytest.raises(RuntimeError, match="Agent executor 'A' returned a terminal runtime error"):
        orchestration.send([wire])

    assert [call["executorId"] for call in host.calls] == ["delta-A"]
    assert wire == before


@pytest.mark.parametrize("serialized", [False, True])
@pytest.mark.parametrize("tool_error", [False, True])
def test_generator_normal_response_and_recovered_tool_errors_still_flow(serialized: bool, tool_error: bool) -> None:
    messages: list[Message] = []
    if tool_error:
        error = Content.from_error(message="recoverable tool error", error_code="ValueError")
        messages.append(
            Message(
                "tool",
                [error, Content.from_function_result("call", result=[error], exception="recoverable tool error")],
            )
        )
        # A tool result may also appear in an assistant message, still as tool data.
        messages.append(Message("assistant", [Content.from_function_result("call", result=[error])]))
    messages.append(Message("assistant", ["approved"]))
    response = AgentResponse(messages=messages, additional_properties={"future_metadata": {"error": "not a status"}})
    before = response.to_dict()
    payload = json.loads(response.to_json()) if serialized else response
    host = _RecordingHost()
    workflow = _workflow([_agent("A"), _agent("B")], [SingleEdgeGroup("A", "B")])
    orchestration = run_workflow_orchestrator(host, workflow, "start")
    next(orchestration)

    assert _finish(orchestration, orchestration.send([payload])) == []
    assert [call["executorId"] for call in host.calls] == ["delta-A", "delta-B"]
    assert host.calls[-1]["contextMessages"] == [Message("user", ["start"]).to_dict(), *before["messages"]]
    assert _ids(host.calls[-1]) == [None] * (1 + len(messages))
    assert len(set(_occurrences(host.calls[-1]))) == 1 + len(messages)
    assert response.to_dict() == before


@pytest.mark.parametrize("response_type", [None, "application_result"])
@pytest.mark.parametrize("structured", [False, True])
def test_generator_lightweight_dict_is_not_mistaken_for_a_durable_failure(
    response_type: str | None, structured: bool
) -> None:
    payload: dict[str, Any] = {
        "text": "ValueError: ordinary application text",
        "error": "application data",
        "additional_properties": {"durable_status": "error"},
        "messages": [Message("assistant", [Content.from_error(error_code="ValueError")]).to_dict()],
    }
    if response_type is not None:
        payload["type"] = response_type
    if structured:
        payload["value"] = {"error": "ordinary structured output"}
    before = deepcopy(payload)
    host = _RecordingHost()
    workflow = _workflow([_agent("A"), _agent("B")], [SingleEdgeGroup("A", "B")])
    orchestration = run_workflow_orchestrator(host, workflow, "start")
    next(orchestration)

    assert _finish(orchestration, orchestration.send([payload])) == []
    assert [call["executorId"] for call in host.calls] == ["delta-A", "delta-B"]
    assert _texts(host.calls[-1]) == ["start", json.dumps(payload["value"]) if structured else payload["text"]]
    assert payload == before


@pytest.mark.parametrize("repeat_input", [False, True])
@pytest.mark.parametrize("pause_between", [False, True])
def test_generator_independent_strings_deliver_equal_outputs_as_new_turns(
    repeat_input: bool, pause_between: bool
) -> None:
    inputs = ["first request", "first request" if repeat_input else "second request"]
    results = (
        [_activity_result(inputs[:1], "A", request=True), _activity_result(inputs[1:], "A")]
        if pause_between
        else [_activity_result(inputs, "A")]
    )
    workflow = _workflow(
        [_activity("gate"), _agent("A"), _agent("B", context_mode="last_agent")],
        [SingleEdgeGroup("A", "B")],
    )
    live = _RecordingHost(activities={"gate": results}, agent_reply="approved")
    replay = _RecordingHost(is_replaying=True, activities={"gate": results}, agent_reply="approved")

    assert _run(live, workflow) == _run(replay, workflow) == []
    assert live.calls == replay.calls
    producer_calls = [call for call in live.calls if call["executorId"] == "delta-A"]
    consumer_calls = [call for call in live.calls if call["executorId"] == "delta-B"]
    assert [call["message"] for call in producer_calls] == inputs
    assert all(call["contextMessages"] is None for call in producer_calls)
    assert [_ids(call) for call in consumer_calls] == [[None], [None]]
    assert len({identity for call in consumer_calls for identity in _occurrences(call)}) == 2
    assert [_texts(call) for call in consumer_calls] == [["approved"], ["approved"]]
    assert live.waited_for == replay.waited_for == (["approval"] if pause_between else [])


def test_generator_output_positions_survive_shorter_and_empty_incoming_conversations() -> None:
    inputs = [_response([_message(position) for position in range(length)]) for length in [0, 4, 1, 0, 8]]
    workflow = _workflow(
        [_activity("source"), _agent("A"), _agent("B", context_mode="last_agent")],
        [SingleEdgeGroup("A", "B")],
    )
    activities = {"source": [_activity_result(inputs, "A")]}
    live = _RecordingHost(activities=activities, agent_reply="approved")
    replay = _RecordingHost(is_replaying=True, activities=activities, agent_reply="approved")

    assert _run(live, workflow) == _run(replay, workflow) == []
    assert live.calls == replay.calls
    consumer_calls = [call for call in live.calls if call["executorId"] == "delta-B"]
    assert [_ids(call) for call in consumer_calls] == [[None]] * len(inputs)
    assert len({identity for call in consumer_calls for identity in _occurrences(call)}) == len(inputs)
    assert [_texts(call) for call in consumer_calls] == [["approved"]] * len(inputs)


def test_generator_same_producer_on_independent_branches_assigns_distinct_output_positions() -> None:
    workflow = _workflow(
        [_agent("source"), _agent("left"), _agent("right"), _agent("A"), _agent("B", context_mode="last_agent")],
        [
            FanOutEdgeGroup("source", ["left", "right"]),
            SingleEdgeGroup("left", "A"),
            SingleEdgeGroup("right", "A"),
            SingleEdgeGroup("A", "B"),
        ],
    )
    live = _RecordingHost(agent_reply="approved")
    replay = _RecordingHost(is_replaying=True, agent_reply="approved")

    assert _run(live, workflow) == _run(replay, workflow) == []
    assert live.calls == replay.calls
    producer_calls = [call for call in live.calls if call["executorId"] == "delta-A"]
    assert [_ids(call) for call in producer_calls] == [[None, None, None], [None]]
    assert [_texts(call) for call in producer_calls] == [["start", "approved", "approved"], ["approved"]]
    assert len({identity for call in producer_calls for identity in _occurrences(call)}) == 4
    consumer_calls = [call for call in live.calls if call["executorId"] == "delta-B"]
    assert [_ids(call) for call in consumer_calls] == [[None], [None]]
    assert len({identity for call in consumer_calls for identity in _occurrences(call)}) == 2
    assert [_texts(call) for call in consumer_calls] == [["approved"], ["approved"]]


def test_generator_fanin_keeps_repeated_outputs_from_each_producer_and_replays_identically() -> None:
    workflow = _workflow(
        [_activity("source"), _agent("left"), _agent("right"), _agent("join", context_mode="last_agent")],
        [FanOutEdgeGroup("source", ["left", "right"]), FanInEdgeGroup(["left", "right"], "join")],
    )
    activities = {"source": [_activity_result(["first request", "second request"], None)]}
    live = _RecordingHost(activities=activities, agent_reply="approved")
    replay = _RecordingHost(is_replaying=True, activities=activities, agent_reply="approved")

    assert _run(live, workflow) == _run(replay, workflow) == []
    assert live.calls == replay.calls
    assert live.batch_sizes == [1, 2, 1]
    joined = [call for call in live.calls if call["executorId"] == "delta-join"]
    assert len(joined) == 1
    assert _ids(joined[0]) == [None] * 4
    assert len(set(_occurrences(joined[0]))) == 4
    assert _texts(joined[0]) == ["approved"] * 4


@pytest.mark.parametrize("batch", [False, True])
def test_generator_custom_id_collisions_are_scoped_on_the_wire_and_replay_stable(batch: bool) -> None:
    sources = [
        _response([Message("assistant", ["approved"], message_id="custom-id")], producer)
        for producer in ["left", "right"]
    ]
    deliveries: list[Any] = [sources] if batch else sources
    workflow = _workflow([_activity("source"), _agent(context_mode="last_agent")], [])
    activities = {"source": [_activity_result(deliveries)]}
    live = _RecordingHost(activities=activities)
    replay = _RecordingHost(is_replaying=True, activities=activities)

    assert _run(live, workflow) == _run(replay, workflow) == []
    assert live.calls == replay.calls
    assert [message_id for call in live.calls for message_id in _ids(call)] == [
        "custom-id",
        "custom-id",
    ]
    assert len({identity for call in live.calls for identity in _occurrences(call)}) == 2
    assert [text for call in live.calls for text in _texts(call)] == ["approved", "approved"]
    assert [source.full_conversation[0].message_id for source in sources] == ["custom-id", "custom-id"]


def _cycle_workflow() -> Any:
    return _workflow(
        [_agent("A"), _agent("B")],
        [
            SingleEdgeGroup("A", "B", condition=lambda response: len(response.full_conversation) < 6),
            SingleEdgeGroup("B", "A", condition=lambda response: len(response.full_conversation) < 6),
        ],
    )


def test_generator_replay_rebuilds_the_same_cycle_delta_sequence() -> None:
    workflow = _cycle_workflow()
    live, replay = _RecordingHost(), _RecordingHost(is_replaying=True)

    assert _run(live, workflow) == _run(replay, workflow) == []
    assert live.calls == replay.calls
    assert live.calls[0]["contextMessages"] is None
    assert [_ids(call) for call in live.calls[1:]] == [[None] * count for count in [2, 3, 2, 2]]
    assert [_texts(call) for call in live.calls[1:]] == [
        ["start", "reply-1"],
        ["start", "reply-1", "reply-2"],
        ["reply-2", "reply-3"],
        ["reply-3", "reply-4"],
    ]
    first_b, first_a, next_b, next_a = [_occurrences(call) for call in live.calls[1:]]
    assert first_a[:2] == first_b
    assert next_b[0] == first_a[-1]
    assert next_a[0] == next_b[-1]
    assert set(first_b).isdisjoint(next_b)
    assert set(first_a).isdisjoint(next_a)
    assert live.statuses
    assert replay.statuses == []


def test_interleaved_live_runs_do_not_share_delivery_on_retained_executors() -> None:
    workflow = _cycle_workflow()
    first, second = _RecordingHost(instance_id="first"), _RecordingHost(instance_id="second")
    first_run = run_workflow_orchestrator(first, workflow, "start")
    second_run = run_workflow_orchestrator(second, workflow, "start")
    first_yield = next(first_run)
    second_yield = next(second_run)
    first_yield = first_run.send(first_yield)
    second_yield = second_run.send(second_yield)

    assert _finish(first_run, first_yield) == _finish(second_run, second_yield) == []
    assert [call["contextMessages"] for call in first.calls] == [call["contextMessages"] for call in second.calls]
    assert all(call["instanceId"] == "first" for call in first.calls)
    assert all(call["instanceId"] == "second" for call in second.calls)
    assert len(_ids(first.calls[-1])) == len(_ids(second.calls[-1])) == 2
    assert {identity for call in first.calls[1:] for identity in _occurrences(call)}.isdisjoint(
        identity for call in second.calls[1:] for identity in _occurrences(call)
    )


def test_generator_fanout_fanin_and_cycle_preserve_producer_identity() -> None:
    workflow = _workflow(
        [_agent("source"), _agent("left"), _agent("right"), _agent("join")],
        [
            FanOutEdgeGroup("source", ["left", "right"]),
            FanInEdgeGroup(["left", "right"], "join"),
            SingleEdgeGroup("join", "join", condition=lambda response: len(response.full_conversation) < 8),
        ],
    )
    host = _RecordingHost()

    assert _run(host, workflow) == []
    assert host.batch_sizes == [1, 2, 1, 1]
    assert [_ids(call) for call in host.calls[1:]] == [[None] * count for count in [2, 2, 4, 1]]
    assert [_texts(call) for call in host.calls[1:]] == [
        ["start", "reply-1"],
        ["start", "reply-1"],
        ["start", "reply-1", "reply-2", "reply-3"],
        ["reply-4"],
    ]
    left, right, joined, repeated = [_occurrences(call) for call in host.calls[1:]]
    assert left == right == joined[:2]
    assert len(set(joined + repeated)) == 5


def test_generator_hitl_resume_keeps_independent_activity_events_distinct_on_replay() -> None:
    workflow = _workflow([_activity("gate"), _agent()], [])
    results = [
        _activity_result([_response([_message(1), _message(3)])], request=True),
        _activity_result([_response([_message(3), _message(2), _message(4), _message(1)])]),
    ]
    live = _RecordingHost(activities={"gate": results})
    replay = _RecordingHost(is_replaying=True, activities={"gate": results})

    assert _run(live, workflow) == _run(replay, workflow) == []
    assert live.calls == replay.calls
    assert [_ids(call) for call in live.calls] == [
        ["wf_source_1", "wf_source_3"],
        ["wf_source_3", "wf_source_2", "wf_source_4", "wf_source_1"],
    ]
    assert set(_occurrences(live.calls[0])).isdisjoint(_occurrences(live.calls[1]))
    assert live.waited_for == replay.waited_for == ["approval"]
    assert deserialize_value(live.activity_inputs[1]["message"])["response"] == "approved"
    assert live.activity_inputs[1]["source_executor_ids"] == ["__hitl_response___approval"]
    assert any(status["state"] == "waiting_for_human_input" for status in live.statuses)
