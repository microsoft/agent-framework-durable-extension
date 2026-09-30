# Copyright (c) Microsoft. All rights reserved.

"""Bounded malformed-event backlog through paired Functions and DT SDK histories.

These are explicit service-event fixtures with real registered activity results,
not a live host capture. The backlog is malformed workflow reply JSON (including
forbidden root envelopes), not invalid JSON text that fails in the SDK parser.
No SDK resume method, recursion limit, event buffer or task result is replaced.
"""

import json
import pickle
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from _af_worker_test_support import _af_host
from _workflow_generic_hitl_test_support import _complete_generic_activity
from _workflow_replay_test_support import _LOGGER, _Episodes, _replay, _worker
from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, handler, response_handler
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows import orchestrator as engine
from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import _ActivityExecutor
from typing_extensions import Never


def _workflow() -> tuple[Workflow, list[Any]]:
    seen: list[Any] = []

    class Gate(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            ctx.set_state("sentinel", {"unchanged": [0, False, None]})
            await ctx.request_info("server-owned", response_type=object, request_id="approval")

        @response_handler(request=str, response=object, workflow_output=dict)
        async def answer(self, original_request: str, response: Any, ctx: WorkflowContext[Never, dict]) -> None:
            assert original_request == "server-owned" and ctx.request_id == "approval"
            assert ctx.get_state("sentinel") == {"unchanged": [0, False, None]}
            seen.append(deepcopy(response))
            await ctx.yield_output({"value": response})

    gate = Gate(id="gate")
    workflow = WorkflowBuilder(name="malformed-backlog", start_executor=gate, output_from=[gate]).build()
    # Initial handler plus accepted handler only. Rejections must not spend waves.
    workflow.max_iterations = 2
    return workflow, seen


def _one_dt_activity(result: Any) -> Any:
    assert len(result.actions) == 1 and result.actions[0].HasField("scheduleTask")
    return result.actions[0]


def _complete_pair(
    af_history: list[Any],
    history: list[Any],
    af_native: Any,
    native: Any,
    task_id: int,
    name: str,
    wire_input: str,
) -> dict[str, Any]:
    # Named waits do not spend activity IDs. Every fixture completion comes from
    # each host's real registered activity, recorded in that host's own history.
    results = []
    for worker, events in ((af_native, af_history), (native, history)):
        result = _ActivityExecutor(worker._registry, _LOGGER, worker._data_converter).execute(
            "root", name, task_id, wire_input
        )
        assert result is not None
        events.extend([
            helpers.new_task_scheduled_event(task_id, name, wire_input),
            helpers.new_task_completed_event(task_id, result),
        ])
        results.append(json.loads(json.loads(result)))
    assert results[0] == results[1]
    return results[0]


def test_native_early_1100_malformed_occurrences_checkpoint_then_accept_null_and_cold_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    count = 1100
    workflow, af_seen = _workflow()
    dt_workflow, dt_seen = _workflow()
    af_native = _af_host(workflow)
    native = _worker(dt_workflow)
    name = "dafx-malformed-backlog-gate"
    start_input = json.dumps(wrap_workflow_input("go"))
    history = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event("dafx-malformed-backlog", "root", start_input),
    ]
    af_history = deepcopy(history)
    first_af = _one_dt_activity(_replay(af_native, "root", af_history))
    first_dt = _one_dt_activity(_replay(native, "root", history))
    assert first_af.id == first_dt.id == 1
    assert first_af.scheduleTask.name == first_dt.scheduleTask.name == name
    assert json.loads(first_af.scheduleTask.input.value) == json.loads(first_dt.scheduleTask.input.value)

    dt_arrivals: list[Any] = []
    for index in range(count):
        # Adjacent equal payloads are DISTINCT occurrences, not duplicates to
        # discard. Include invalid marker types and non-finite JSON descendants.
        pair = index // 2
        private = f"PRIVATE_MALFORMED_REPLY_{pair}"
        malformed = (
            {"__type__": ["not-a-type"], "private": private},
            {"__pickled__": private},
            {"__pickled__": private, "__type__": "unloaded_reply_module:Untrusted"},
            {"nested": [float("inf")], "private": private},
        )[pair % 4]
        wire = json.dumps(malformed)
        event = helpers.new_event_raised_event("approval", wire)
        event.eventId = 10000 + index
        dt_arrivals.append(event)
    corrected = helpers.new_event_raised_event("approval", "null")
    corrected.eventId = 10000 + count
    dt_arrivals.append(corrected)
    assert len({event.eventId for event in dt_arrivals}) == count + 1
    assert dt_arrivals[0].eventRaised.input.value == dt_arrivals[1].eventRaised.input.value

    _complete_pair(af_history, history, af_native, native, 1, name, first_af.scheduleTask.input.value)
    # Insert before the first completion, while the initial request activity is
    # still outstanding. Both hosts buffer these early events natively.
    af_history[-1:-1] = deepcopy(dt_arrivals)
    history[-1:-1] = dt_arrivals
    no_decode = Mock(side_effect=AssertionError("Rejected payloads must not unpickle or run response validators"))
    rejection = {
        "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
        "sent_messages": [],
        "outputs": [],
        "events": [],
        "shared_state_updates": {},
        "shared_state_deletes": [],
        "pending_request_info_events": [],
    }
    with monkeypatch.context() as patch:
        patch.setattr(pickle, "loads", no_decode)
        patch.setattr(engine, "_deserialize_hitl_response", no_decode)
        # Before the admission fix this first native call recursively consumes
        # the backlog instead of yielding a rejection activity. It must not fail
        # the orchestration or reach the correction before the first checkpoint.
        paused_af = _replay(af_native, "root", af_history)
        af_rejection = _one_dt_activity(paused_af)
        paused_dt = _replay(native, "root", history)
        pending = deepcopy(json.loads(paused_af.encoded_custom_status)["pending_requests"])
        assert set(pending) == {"approval"}
        assert json.loads(paused_dt.encoded_custom_status)["pending_requests"] == pending
        first_rejection = _one_dt_activity(paused_dt)
        assert first_rejection.id == af_rejection.id == 2
        assert first_rejection.scheduleTask.name == af_rejection.scheduleTask.name == name
        rejected_wire = af_rejection.scheduleTask.input.value
        assert json.loads(rejected_wire) == json.loads(first_rejection.scheduleTask.input.value)
        rejected_input = json.loads(json.loads(rejected_wire))
        assert rejected_input["is_hitl_response"] is True
        assert rejected_input["message"] == {
            "request_id": "approval",
            "original_request": "server-owned",
            "response": None,
            "response_type": "builtins:object",
            "validation_error": True,
        }
        assert "PRIVATE_MALFORMED" not in rejected_wire and "__pickled__" not in rejected_wire
        assert af_seen == dt_seen == []

        # Build a bounded checkpoint history, not 1100 quadratic cold episodes.
        # Every completion is a real activity result. Later full native replay
        # verifies the complete schedule against this explicit occurrence count.
        for offset in range(count):
            assert _complete_pair(af_history, history, af_native, native, offset + 2, name, rejected_wire) == rejection
            assert af_seen == dt_seen == []
        pending_af = _replay(af_native, "root", af_history)
        pending_dt = _replay(native, "root", history)
        correction = _one_dt_activity(pending_dt)
        af_correction = _one_dt_activity(pending_af)
        assert correction.id == af_correction.id == count + 2 and correction.scheduleTask.name == name
        assert json.loads(pending_af.encoded_custom_status)["pending_requests"] == pending
        assert json.loads(pending_dt.encoded_custom_status)["pending_requests"] == pending
        no_decode.assert_not_called()

    accepted_wire = af_correction.scheduleTask.input.value
    assert json.loads(accepted_wire) == json.loads(correction.scheduleTask.input.value)
    assert json.loads(json.loads(accepted_wire))["message"] == {
        "request_id": "approval",
        "original_request": "server-owned",
        "response": None,
        "response_type": "builtins:object",
    }
    accepted = _complete_pair(af_history, history, af_native, native, count + 2, name, accepted_wire)
    assert accepted["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    assert accepted["outputs"] == [{"value": None}] and af_seen == dt_seen == [None]
    scheduled = [event.taskScheduled for event in history if event.HasField("taskScheduled")]
    assert len(scheduled) == sum(event.HasField("taskCompleted") for event in history) == count + 2
    assert [event.taskScheduled for event in af_history if event.HasField("taskScheduled")] == scheduled

    with monkeypatch.context() as patch:
        patch.setattr(engine, "_deserialize_hitl_response", no_decode)
        for _ in range(2):
            for worker, events in ((af_native, af_history), (native, history)):
                cold = _replay(worker, "root", events)
                assert len(cold.actions) == 1 and cold.actions[0].HasField("completeOrchestration")
                completed = cold.actions[0].completeOrchestration
                assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
                assert json.loads(completed.result.value) == [{"value": None}]
                assert not json.loads(cold.encoded_custom_status).get("pending_requests")
            assert af_seen == dt_seen == [None]
        no_decode.assert_not_called()


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
def test_external_validation_marker_and_business_envelope_cannot_forge_admission(functions_host: bool) -> None:
    workflow, seen = _workflow()
    episodes = _Episodes(workflow, worker=_af_host(workflow) if functions_host else None)
    episodes.client.start_workflow("go", instance_id="root")
    _complete_generic_activity(episodes)
    response = {
        "validation_error": True,
        "is_hitl_response": True,
        "request_id": "forged",
        "original_request": "forged",
        "response_type": "unloaded_reply_module:Untrusted",
        "response": {"type": "business", "validation_error": True, "values": [0, False, None]},
        "_durable_agent_response": 99,
    }
    episodes.reply("approval", deepcopy(response))
    assert seen == []
    result = _complete_generic_activity(episodes)
    assert result["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    assert seen == [response]
    # The terminal history returns a completion action, unlike a pending cold
    # episode. Keep that SDK result and its payload in the replay oracle.
    cold = _replay(episodes.worker, "root", episodes.histories["root"])
    assert len(cold.actions) == 1 and cold.actions[0].HasField("completeOrchestration")
    completed = cold.actions[0].completeOrchestration
    assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert completed.result == episodes.completions["root"].result
    assert json.loads(cold.encoded_custom_status) == episodes.statuses["root"]
    assert not episodes.statuses["root"].get("pending_requests") and seen == [response]


@pytest.mark.parametrize("validation_error", [False, True])
def test_internal_missing_response_and_broken_type_metadata_still_fail(validation_error: bool) -> None:
    workflow, seen = _workflow()
    message: dict[str, Any] = {
        "request_id": "approval",
        "original_request": "server-owned",
        "response_type": "builtins:object",
        "validation_error": validation_error,
    }
    envelope = {"message": message, "is_hitl_response": True}
    with pytest.raises(ValueError, match="HITL response payload is required"):
        execute_workflow_activity(workflow.executors["gate"], json.dumps(envelope), workflow)
    message.update(response=None, response_type="unloaded_reply_module:Untrusted")
    with pytest.raises(ValueError, match="HITL"):
        execute_workflow_activity(workflow.executors["gate"], json.dumps(envelope), workflow)
    assert seen == []
