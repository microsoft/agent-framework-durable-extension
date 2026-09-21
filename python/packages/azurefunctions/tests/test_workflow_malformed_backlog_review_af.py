# Copyright (c) Microsoft. All rights reserved.

"""Bounded malformed-event backlog through native AF and paired DT SDK histories.

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
from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, handler, response_handler
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows import orchestrator as engine
from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import _ActivityExecutor
from test_workflow_buffered_events_review_af import _event
from test_workflow_generic_hitl_review import _complete_generic_activity
from test_workflow_mixed_hitl_review import _atomic_actions, _Episodes
from test_workflow_recorded_replay_review import _LOGGER, _af_replay, _replay, _worker
from typing_extensions import Never

from agent_framework_azurefunctions import AgentFunctionApp


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


def _functions(workflow: Workflow) -> dict[str, Any]:
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    result: dict[str, Any] = {}
    for function in app.get_functions():
        name = function.get_function_name()
        assert name is not None
        result[name] = function.get_user_function()
    return result


def _native_af(rows: list[dict[str, Any]], function: Any) -> dict[str, Any]:
    context = DurableOrchestrationContext(
        rows,
        instanceId="root",
        isReplaying=True,
        parentInstanceId=None,
        input=json.dumps(wrap_workflow_input("go")),
        upperSchemaVersion=ReplaySchema.V3.value,
    )
    result = TaskOrchestrationExecutor().execute(context, context.histories, function.orchestrator_function)
    return json.loads(result)


def _activities(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [action for action in _atomic_actions(state["actions"]) if action["actionType"] == 0]


def _one_dt_activity(result: Any) -> Any:
    assert len(result.actions) == 1 and result.actions[0].HasField("scheduleTask")
    return result.actions[0]


def _complete_pair(
    rows: list[dict[str, Any]],
    history: list[Any],
    functions: dict[str, Any],
    native: Any,
    af_id: int,
    name: str,
    wire_input: str,
) -> dict[str, Any]:
    # AF IDs start at zero, DT at one. Named waits do not spend activity IDs.
    # Every fixture completion comes from each host's real registered activity.
    af_result = functions[name](json.loads(wire_input))
    dt_result = _ActivityExecutor(native._registry, _LOGGER, native._data_converter).execute(
        "root", name, af_id + 1, wire_input
    )
    assert dt_result is not None
    decoded = json.loads(af_result)
    assert json.loads(json.loads(dt_result)) == decoded
    rows.extend([
        _event(4, af_id, Name=name, Input=wire_input),
        _event(5, TaskScheduledId=af_id, Result=json.dumps(af_result)),
    ])
    history.extend([
        helpers.new_task_scheduled_event(af_id + 1, name, wire_input),
        helpers.new_task_completed_event(af_id + 1, dt_result),
    ])
    return decoded


def test_native_early_1100_malformed_occurrences_checkpoint_then_accept_null_and_cold_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    count = 1100
    workflow, af_seen = _workflow()
    dt_workflow, dt_seen = _workflow()
    functions = _functions(workflow)
    native = _worker(dt_workflow)
    name = "dafx-malformed-backlog-gate"
    orchestrator = functions["dafx-malformed-backlog"]
    start_input = json.dumps(wrap_workflow_input("go"))
    rows = [_event(12), _event(0, Name="dafx-malformed-backlog", Input=start_input)]
    history = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event("dafx-malformed-backlog", "root", start_input),
    ]
    first_af = _activities(_native_af(rows, orchestrator))
    first_dt = _one_dt_activity(_replay(native, "root", history))
    assert len(first_af) == 1 and first_af[0]["functionName"] == name
    assert first_dt.id == 1 and first_dt.scheduleTask.name == name
    assert json.loads(first_af[0]["input"]) == json.loads(first_dt.scheduleTask.input.value)

    arrivals: list[dict[str, Any]] = []
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
        arrivals.append(_event(15, 10000 + index, Name="approval", Input=wire))
        event = helpers.new_event_raised_event("approval", wire)
        event.eventId = 10000 + index
        dt_arrivals.append(event)
    arrivals.append(_event(15, 10000 + count, Name="approval", Input="null"))
    corrected = helpers.new_event_raised_event("approval", "null")
    corrected.eventId = 10000 + count
    dt_arrivals.append(corrected)
    assert len({row["EventId"] for row in arrivals}) == count + 1
    assert arrivals[0]["Input"] == arrivals[1]["Input"]

    _complete_pair(rows, history, functions, native, 0, name, first_af[0]["input"])
    # Insert before the first completion, while the initial request activity is
    # still outstanding. This is native AF early buffering, not a DT translation.
    rows[-1:-1] = arrivals
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
        paused_af = _native_af(rows, orchestrator)
        af_calls = _activities(paused_af)
        assert not paused_af["isDone"] and len(af_calls) == 2
        paused_dt = _replay(native, "root", history)
        pending = deepcopy(paused_af["customStatus"]["pending_requests"])
        assert set(pending) == {"approval"}
        assert json.loads(paused_dt.encoded_custom_status)["pending_requests"] == pending
        first_rejection = _one_dt_activity(paused_dt)
        assert first_rejection.id == 2
        assert first_rejection.scheduleTask.name == af_calls[-1]["functionName"] == name
        rejected_wire = af_calls[-1]["input"]
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
            assert _complete_pair(rows, history, functions, native, offset + 1, name, rejected_wire) == rejection
            assert af_seen == dt_seen == []
        pending_af = _native_af(rows, orchestrator)
        pending_dt = _replay(native, "root", history)
        correction = _one_dt_activity(pending_dt)
        assert correction.id == count + 2 and correction.scheduleTask.name == name
        assert not pending_af["isDone"] and len(_activities(pending_af)) == count + 2
        assert pending_af["customStatus"]["pending_requests"] == pending
        assert json.loads(pending_dt.encoded_custom_status)["pending_requests"] == pending
        no_decode.assert_not_called()

    accepted_wire = _activities(pending_af)[-1]["input"]
    assert json.loads(accepted_wire) == json.loads(correction.scheduleTask.input.value)
    assert json.loads(json.loads(accepted_wire))["message"] == {
        "request_id": "approval",
        "original_request": "server-owned",
        "response": None,
        "response_type": "builtins:object",
    }
    accepted = _complete_pair(rows, history, functions, native, count + 1, name, accepted_wire)
    assert accepted["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    assert accepted["outputs"] == [{"value": None}] and af_seen == dt_seen == [None]
    scheduled = [event.taskScheduled for event in history if event.HasField("taskScheduled")]
    assert len(scheduled) == sum(event.HasField("taskCompleted") for event in history) == count + 2
    assert sum(row["EventType"] == 5 for row in rows) == count + 2

    with monkeypatch.context() as patch:
        patch.setattr(engine, "_deserialize_hitl_response", no_decode)
        for _ in range(2):
            cold_af = _native_af(rows, orchestrator)
            cold_dt = _replay(native, "root", history)
            assert cold_af["isDone"] and cold_af["output"] == [{"value": None}]
            assert len(cold_dt.actions) == 1 and cold_dt.actions[0].HasField("completeOrchestration")
            completed = cold_dt.actions[0].completeOrchestration
            assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
            assert json.loads(completed.result.value) == [{"value": None}]
            assert not cold_af["customStatus"].get("pending_requests")
            assert not json.loads(cold_dt.encoded_custom_status).get("pending_requests")
            actions = _atomic_actions(cold_af["actions"])
            assert [action["externalEventName"] for action in actions if action["actionType"] == 6] == ["approval"] * (
                count + 1
            )
            assert [(action["functionName"], json.loads(action["input"])) for action in _activities(cold_af)] == [
                (task.name, json.loads(task.input.value)) for task in scheduled
            ]
            assert af_seen == dt_seen == [None]
        no_decode.assert_not_called()


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
def test_external_validation_marker_and_business_envelope_cannot_forge_admission(functions_host: bool) -> None:
    workflow, seen = _workflow()
    functions = _functions(workflow) if functions_host else None
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    _complete_generic_activity(episodes, functions=functions)
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
    result = _complete_generic_activity(episodes, functions=functions)
    assert result["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    assert seen == [response]
    replay = _af_replay(episodes.histories["root"], workflow, instance="root")
    assert replay["isDone"] and seen == [response]
    # The terminal history returns a completion action, unlike a pending cold
    # episode. Keep that SDK result and its payload in the replay oracle.
    cold = _replay(episodes.worker, "root", episodes.histories["root"])
    assert len(cold.actions) == 1 and cold.actions[0].HasField("completeOrchestration")
    completed = cold.actions[0].completeOrchestration
    assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert completed.result == episodes.completions["root"].result
    assert json.loads(cold.encoded_custom_status) == episodes.statuses["root"]
    assert not replay["customStatus"].get("pending_requests") and seen == [response]


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
