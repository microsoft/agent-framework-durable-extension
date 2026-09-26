# Copyright (c) Microsoft. All rights reserved.

"""Native SDK histories with registered entity/activity producers, not live hosts.

Only the external service's event delivery is constructed. Both orchestrators,
both entity registrations, Core's tool approvals, and checkpoint activities run
unchanged. No SDK resumption, recursion limit, task result or queue is replaced.
"""

import json
import pickle
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from _execution_test_support import NonStreamingAgent
from _workflow_agent_approval_test_support import (
    CHECKPOINT,
    ENTITY,
    _ApprovalClient,
    _functions,
    _History,
    _workflow,
)
from agent_framework import AgentExecutor, Content, WorkflowBuilder, WorkflowExecutor
from agent_framework_durabletask import DurableAIAgentWorker, load_agent_response
from agent_framework_durabletask._workflows import orchestrator as engine
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
@pytest.mark.parametrize("sibling_first", [False, True], ids=["retained-sibling", "aggregated-sibling"])
@pytest.mark.parametrize(("kind", "count"), [("null", 1100), ("wrong-id", 3), ("malformed-envelope", 3)])
def test_native_agent_rejection_backlog_keeps_approvals_and_replays_without_execution(
    functions_host: bool, sibling_first: bool, kind: str, count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow, client, effects = _workflow(sibling_first=sibling_first)
    history = _History(functions_host, workflow)
    first = history.complete_entity(history.replay())
    approvals: dict[str, Content] = {}
    for request in load_agent_response(first).user_input_requests:
        assert request.function_call is not None and isinstance(request.function_call.call_id, str)
        approvals[request.function_call.call_id] = request
    assert set(approvals) == {"a", "b"}
    a, b = approvals["a"], approvals["b"]
    assert isinstance(a.id, str) and isinstance(b.id, str) and a.id != b.id
    answer_a = a.to_function_approval_response(True).to_dict()
    answer_b = b.to_function_approval_response(True).to_dict()
    invalid = {
        "null": None,
        "wrong-id": {**answer_a, "id": "not-the-pending-approval"},
        "malformed-envelope": {"__type__": ["unloaded:Untrusted"], "private": "PRIVATE_REPLY"},
    }[kind]
    assert len(client.calls) == len(history.entity_inputs) == 1 and effects == []
    initial_entity_state = history.entity_state

    # All replies arrive before the initial entity's completion, so the real
    # SDK must consume distinct, equal-valued buffered occurrences. Keep the
    # service event identity separate from the (identical) application payload.
    completion = history.rows.pop()
    if sibling_first:
        # Both names are buffered. Put b first in the real producer's request
        # order too: SDK task_any chooses the first already-ready wait, not the
        # globally earliest arrival across different event names.
        history.event(b.id, answer_b, 9000)
    backlog_start = len(history.rows)
    for index in range(count):
        history.event(a.id, invalid, 10000 + index)
    history.event(a.id, answer_a, 10000 + count)
    backlog = history.rows[backlog_start : backlog_start + count]
    if functions_host:
        assert len({row["EventId"] for row in backlog}) == count
        assert len({row["Input"] for row in backlog}) == 1
    else:
        assert len({row.eventId for row in backlog}) == count
        assert len({row.eventRaised.input.value for row in backlog}) == 1
    history.rows.append(completion)

    no_decode = Mock(side_effect=AssertionError("Agent rejection must not decode arbitrary response classes"))
    with monkeypatch.context() as guard:
        guard.setattr(pickle, "loads", no_decode)
        guard.setattr(engine, "_deserialize_hitl_response", no_decode)
        before = history.replay()
        wire = history.checkpoint_input(before, 1)
        assert json.loads(json.loads(wire)) == {"request_id": a.id, "status": "invalidreply"}
        assert "PRIVATE_REPLY" not in wire and "__type__" not in wire
        pending = deepcopy(before["customStatus"]["pending_requests"])
        assert set(pending) == ({a.id} if sibling_first else {a.id, b.id})
        assert not before["isDone"]
        assert len(history.waits[a.id]) == len(history.waits[b.id]) == 1

        history.complete_checkpoint(1, wire)
        after = history.replay()
        assert history.checkpoint_input(after, 2) == wire
        assert after["customStatus"] == before["customStatus"]
        assert len(history.waits[a.id]) == 2 and len(history.waits[b.id]) == 1
        # Build linear histories with real activity results instead of running
        # 1100 quadratic cold episodes. The final SDK pass checks the schedule.
        for index in range(2, count + 1):
            history.complete_checkpoint(index, wire)
        ready = history.replay()
        assert not ready["isDone"] and history.checkpoints == count
        assert len(history.waits[a.id]) == count + 1 and len(history.waits[b.id]) == 1
        assert history.entity_state == initial_entity_state
        assert len(client.calls) == len(history.entity_inputs) == 1 and effects == []
        if not sibling_first:
            assert ready["customStatus"]["pending_requests"] == {b.id: pending[b.id]}
            if functions_host:
                assert len(ready["scheduled"]) == count + 1
            else:
                assert ready["actions"] == []
            history.event(b.id, answer_b, 20000)
            ready = history.replay()
        no_decode.assert_not_called()

    # Only once every matching approval is accumulated does a second registered
    # entity run execute the real Core tools and make the next model request.
    second = history.complete_entity(ready)
    assert load_agent_response(second).text == "done"
    assert len(history.entity_inputs) == len(client.calls) == 2
    assert sorted(effects) == ["a", "b"] and len(effects) == 2
    delivered = history.entity_inputs[1]["contextMessages"]
    assert len(delivered) == 1 and delivered[0]["role"] == "user"
    expected = [answer_b, answer_a] if sibling_first else [answer_a, answer_b]
    assert delivered[0]["contents"] == expected
    committed = history.entity_state
    with monkeypatch.context() as guard:
        guard.setattr(pickle, "loads", no_decode)
        guard.setattr(engine, "_deserialize_hitl_response", no_decode)
        for _ in range(2):
            cold = history.replay()
            assert cold["isDone"] and not cold["customStatus"].get("pending_requests")
            assert deserialize_workflow_output(cold["output"])[0].text == "done"
            assert len(history.waits[a.id]) == count + 1 and len(history.waits[b.id]) == 1
            if functions_host:
                assert [action["actionType"] for action in cold["scheduled"]] == [7, *([0] * count), 7]
                assert all(action["input"] == wire for action in cold["scheduled"][1:-1])
            else:
                scheduled = [event.taskScheduled for event in history.rows if event.HasField("taskScheduled")]
                assert [task.name for task in scheduled] == [CHECKPOINT] * count
                assert all(task.input.value == wire for task in scheduled)
                assert sum(event.HasField("taskCompleted") for event in history.rows) == count
                invoked = [e for e in cold["customStatus"]["events"] if e["type"] == "executor_invoked"]
                assert [event["iteration"] for event in invoked] == [0, 1]
            assert history.entity_state == committed and history.checkpoints == count
            assert len(history.entity_inputs) == len(client.calls) == 2 and sorted(effects) == ["a", "b"]
        no_decode.assert_not_called()


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
def test_valid_agent_approval_batch_keeps_entity_only_schedule(functions_host: bool) -> None:
    workflow, client, effects = _workflow()
    history = _History(functions_host, workflow)
    first = history.complete_entity(history.replay())
    requests = load_agent_response(first).user_input_requests
    assert len(requests) == 2
    pending = deepcopy(history.replay()["customStatus"]["pending_requests"])
    for index, request in enumerate(reversed(requests)):
        assert isinstance(request.id, str)
        history.event(request.id, request.to_function_approval_response(True).to_dict(), 30000 + index)
        ready = history.replay()
        if index == 0:
            assert set(ready["customStatus"]["pending_requests"]) == set(pending) - {request.id}
            assert len(history.entity_inputs) == len(client.calls) == 1 and effects == []
    second = history.complete_entity(ready)
    assert load_agent_response(second).text == "done"
    final = history.replay()
    assert final["isDone"] and history.checkpoints == 0
    assert len(history.entity_inputs) == len(client.calls) == 2 and sorted(effects) == ["a", "b"]
    if functions_host:
        assert [action["actionType"] for action in final["scheduled"]] == [7, 7]
    else:
        assert not any(event.HasField("taskScheduled") for event in history.rows)


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
def test_checkpoint_registration_is_per_agent_workflow_and_deduplicates_nested_reuse(functions_host: bool) -> None:
    inner, _, _ = _workflow()
    another = AgentExecutor(NonStreamingAgent(client=_ApprovalClient(), name="other"), id="other")
    inner.executors[another.id] = another
    left, right = WorkflowExecutor(inner, id="left"), WorkflowExecutor(inner, id="right")
    outer = WorkflowBuilder(name="outer", start_executor=left, output_from=[left]).build()
    # Registration visits all registered nodes, including disconnected nodes.
    outer.executors[right.id] = right
    if functions_host:
        functions = _functions(outer)
        assert [name for name in functions if name.startswith("dafx__hitl-")] == [CHECKPOINT]
        assert ENTITY in functions and "dafx-agent-backlog-other" in functions
    else:
        native = Mock()
        worker = DurableAIAgentWorker(native, deployment_mode="isolated_v2")
        worker.configure_workflow(outer)
        worker.configure_workflow(inner)
        names = [call.args[0].__name__ for call in native.add_activity.call_args_list]
        assert names == [CHECKPOINT]
        assert [call.args[0].__name__ for call in native.add_entity.call_args_list] == [
            ENTITY,
            "dafx-agent-backlog-other",
        ]


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
@pytest.mark.parametrize("instruction", [None, {}, {"request_id": "a", "status": "accepted"}])
def test_registered_checkpoint_rejects_untrusted_instruction_shapes(functions_host: bool, instruction: Any) -> None:
    workflow, client, effects = _workflow()
    if functions_host:
        function = _functions(workflow)[CHECKPOINT]
    else:
        native = Mock()
        DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
        activities = {call.args[0].__name__: call.args[0] for call in native.add_activity.call_args_list}

        def function(value: str) -> str:
            return str(activities[CHECKPOINT](None, value))

    with pytest.raises(ValueError, match="internal HITL checkpoint instruction"):
        function(json.dumps(instruction))
    assert client.calls == effects == []
