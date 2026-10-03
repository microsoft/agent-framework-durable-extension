# Copyright (c) Microsoft. All rights reserved.

"""Generic HITL workflows, annotations and registered SDK replay trials."""

import asyncio
import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal, get_origin

import pytest
from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, handler, response_handler
from pydantic import BaseModel, field_validator
from typing_extensions import Never

from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output


class _Models:
    class Decision(BaseModel):
        count: int
        verdict: Literal["approve", "deny"]


def _generic_workflow(requested: Any, *, request_id: str = "approval") -> tuple[Workflow, list[Any]]:
    seen: list[Any] = []

    class Gate(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("decision", response_type=requested, request_id=request_id)

        # Any is deliberately broader than the actual request. Admission must
        # enforce the producer's annotation, not this handler's permissiveness.
        @response_handler(request=str, response=Any, workflow_output=dict)
        async def answer(self, original_request: str, response: Any, ctx: WorkflowContext[Never, dict]) -> None:
            assert original_request == "decision" and ctx.request_id == request_id
            seen.append(response)
            await ctx.yield_output({"value": response})

    gate = Gate(id="gate")
    return WorkflowBuilder(name="generic-hitl", start_executor=gate, output_from=[gate]).build(), seen


async def _core_trial(requested: Any, answer: Any) -> tuple[bool, list[Any]]:
    workflow, seen = _generic_workflow(requested)
    started = await workflow.run("go")
    assert started.get_request_info_events()[0].response_type == requested
    try:
        await workflow.run(responses={"approval": deepcopy(answer)})
    except (TypeError, ValueError) as exc:
        assert "Response type mismatch" in str(exc)
        assert not seen
        assert set(await workflow._runner.context.get_pending_request_info_events()) == {"approval"}
        return False, seen
    return True, seen


# Leaf delivery is intentionally independent of pending snapshots. These cases
# exercise old, absent and malformed snapshots without changing event buffering.
_PENDING_STATUS_CASES = [
    pytest.param(None, "approval", False, id="absent-status"),
    pytest.param([], "approval", False, id="non-object-status"),
    pytest.param({"state": "running"}, "approval", False, id="not-yet-published"),
    pytest.param({"pending_requests": []}, "approval", False, id="non-object-pending"),
    pytest.param({"pending_requests": {"approval": None}}, "approval", False, id="non-object-record"),
    pytest.param({"pending_requests": {"other": {}}}, "approval", False, id="unknown-id"),
    pytest.param({"pending_requests": {"approval": {}}}, "approval", True, id="legacy-id-fallback"),
    pytest.param(
        {"state": "running", "pending_requests": {"approval": {"request_id": "approval"}}},
        "approval",
        True,
        id="pending-while-other-work-runs",
    ),
    pytest.param(
        {"pending_requests": {"storage-key": {"request_id": "approval"}}},
        "approval",
        True,
        id="published-id",
    ),
    pytest.param(
        {"pending_requests": {"storage-key": {"request_id": "approval"}}},
        "storage-key",
        False,
        id="unpublished-map-key",
    ),
    pytest.param({"pending_requests": {"approval": {"request_id": None}}}, "approval", False, id="null-id"),
    pytest.param({"pending_requests": {"auto::0": {}}}, "auto::0", True, id="legacy-auto-id"),
    pytest.param(
        {"pending_requests": {"approval": {"response_type": "worker_only_types:Decision"}}},
        "approval",
        True,
        id="caller-need-not-load-worker-types",
    ),
]


_VALIDATOR_CALLS: list[int] = []


class _CountedDecision(BaseModel):
    count: int

    @field_validator("count")
    @classmethod
    def count_validations(cls, value: int) -> int:
        _VALIDATOR_CALLS.append(value)
        # Non-idempotent output makes duplicate validation visible, not just a
        # harmless repeated side effect that equality assertions would miss.
        return value + len(_VALIDATOR_CALLS)


@dataclass
class _CountedDataclass:
    count: int

    def __post_init__(self) -> None:
        _VALIDATOR_CALLS.append(self.count)
        self.count += len(_VALIDATOR_CALLS)


def _complete_generic_activity(episodes: Any, *, functions: dict[str, Any] | None = None) -> dict[str, Any]:
    from _workflow_replay_test_support import _LOGGER
    from durabletask.internal import helpers
    from durabletask.worker import _ActivityExecutor

    assert len(episodes.actions["root"]) == 1
    task_id, action = episodes.actions["root"].popitem()
    task = action.scheduleTask
    if functions is None:
        executor = _ActivityExecutor(episodes.worker._registry, _LOGGER, episodes.worker._data_converter)
        result = executor.execute("root", task.name, task_id, task.input.value)
    else:
        # Use the exact scheduled name and real Functions activity registration.
        # DT history stores converter-wrapped strings. Functions takes the inner
        # activity JSON string and returns the same shared-body result string.
        result = json.dumps(functions[task.name](json.loads(task.input.value)))
    assert result is not None
    decoded: dict[str, Any] = json.loads(json.loads(result))
    episodes.episode("root", helpers.new_task_completed_event(task_id, result))
    episodes.flush()
    return decoded


_CONCRETE_REPLAY_CASES = [
    pytest.param(int, 123, 456, id="int-valid"),
    pytest.param(int, True, 456, id="bool-is-int-subclass"),
    pytest.param(int, "123", 456, id="int-numeric-string-rejected"),
    pytest.param(int, "not-an-int", 456, id="int-invalid-string-rejected"),
    pytest.param(int, 1.0, 456, id="int-float-rejected"),
    pytest.param(int, None, 456, id="int-null-rejected"),
    pytest.param(str, "123", "corrected", id="numeric-string-stays-string"),
    pytest.param(str, 123, "corrected", id="str-int-rejected"),
    pytest.param(str, None, "corrected", id="str-null-rejected"),
    pytest.param(float, 7, 8.0, id="int-to-float"),
    pytest.param(float, True, 8.0, id="bool-to-float-core-version-dependent"),
]


def _concrete_replay_trial(requested: type, answer: Any, correction: Any, *, functions_host: bool = False) -> None:
    from _workflow_replay_test_support import _af_replay, _Episodes, _replay

    # The public Core workflow is independent of durable's descriptor and
    # admission helpers. In particular bool->float differs in Core 1.13/1.16.
    accepted, oracle = asyncio.run(_core_trial(requested, answer))
    corrected, correction_oracle = asyncio.run(_core_trial(requested, correction))
    assert corrected
    workflow, seen = _generic_workflow(requested)
    functions: dict[str, Any] | None = None
    if functions_host:
        from agent_framework_azurefunctions import AgentFunctionApp

        app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
        functions = {}
        for item in app.get_functions():
            name = item.get_function_name()
            assert name is not None
            functions[name] = item.get_user_function()
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    _complete_generic_activity(episodes, functions=functions)
    pending = deepcopy(episodes.statuses["root"]["pending_requests"])
    assert set(pending) == {"approval"}

    def cold_pending() -> None:
        cold = _replay(episodes.worker, "root", episodes.histories["root"])
        assert list(cold.actions) == []
        assert json.loads(cold.encoded_custom_status)["pending_requests"] == pending
        if functions_host:
            af_cold = _af_replay(episodes.histories["root"], workflow, instance="root")
            assert not af_cold["isDone"] and af_cold["customStatus"]["pending_requests"] == pending
        assert not seen

    for _ in range(1 if accepted else 2):
        episodes.reply("approval", deepcopy(answer))
        assert not seen and len(episodes.actions["root"]) == 1
        assert episodes.statuses["root"]["pending_requests"] == pending
        cold_pending()  # A scheduled activity is not an acknowledged admission.
        result = _complete_generic_activity(episodes, functions=functions)
        assert seen == oracle
        if accepted:
            assert result["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
        else:
            assert result == {
                "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
                "sent_messages": [],
                "outputs": [],
                "events": [],
                "shared_state_updates": {},
                "shared_state_deletes": [],
                "pending_request_info_events": [],
            }
            assert "root" not in episodes.completions and not episodes.actions["root"]
            assert episodes.statuses["root"]["pending_requests"] == pending
            cold_pending()
    if not accepted:
        episodes.reply("approval", deepcopy(correction))
        cold_pending()
        result = _complete_generic_activity(episodes, functions=functions)
        assert result["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    expected = oracle if accepted else correction_oracle
    assert seen == expected and type(seen[0]) is type(expected[0])
    assert "root" in episodes.completions and not episodes.pending()
    scheduled = [event for event in episodes.histories["root"] if event.HasField("taskScheduled")]
    completed = [event for event in episodes.histories["root"] if event.HasField("taskCompleted")]
    assert len(scheduled) == len(completed) == (2 if accepted else 4)
    for _ in range(2):
        cold = _replay(episodes.worker, "root", episodes.histories["root"])
        assert len(cold.actions) == 1 and cold.actions[0].HasField("completeOrchestration")
        assert deserialize_workflow_output(json.loads(cold.actions[0].completeOrchestration.result.value)) == [
            {"value": expected[0]}
        ]
        if functions_host:
            af_cold = _af_replay(episodes.histories["root"], workflow, instance="root")
            assert af_cold["isDone"] and af_cold["output"] == [{"value": expected[0]}]
        assert seen == expected and len(seen) == 1


def _validator_replay_trial(annotation: Any, *, functions_host: bool = False) -> None:
    from _workflow_replay_test_support import _af_replay, _Episodes, _replay

    _VALIDATOR_CALLS.clear()
    workflow, seen = _generic_workflow(annotation)
    functions: dict[str, Any] | None = None
    if functions_host:
        from agent_framework_azurefunctions import AgentFunctionApp

        app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
        functions = {}
        for item in app.get_functions():
            name = item.get_function_name()
            assert name is not None
            functions[name] = item.get_user_function()
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    _complete_generic_activity(episodes, functions=functions)
    # A concrete model/dataclass is supported on both Core versions. A union's
    # fallback permits the generic control to finish even on older Core versions.
    wire: Any = [{"count": 7}] if get_origin(annotation) is not None else {"count": 7}
    episodes.reply("approval", wire)
    assert not _VALIDATOR_CALLS and not seen  # No user validation in the generator.
    for _ in range(2):
        replayed = _replay(episodes.worker, "root", episodes.histories["root"])
        assert list(replayed.actions) == [] and not _VALIDATOR_CALLS
        if functions_host:
            assert not _af_replay(episodes.histories["root"], workflow, instance="root")["isDone"]
            assert not _VALIDATOR_CALLS
    result = _complete_generic_activity(episodes, functions=functions)
    if result["hitl_admission"]["status"] == "invalidreply":
        assert annotation == (list[_CountedDecision] | str) and not _VALIDATOR_CALLS
        episodes.reply("approval", "fallback")
        result = _complete_generic_activity(episodes, functions=functions)
        assert seen == ["fallback"]
    else:
        assert _VALIDATOR_CALLS == [7]
        actual = seen[0][0] if isinstance(seen[0], list) else seen[0]
        assert actual.count == 8
    assert result["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    checkpoint = deepcopy(_VALIDATOR_CALLS)
    for _ in range(3):
        replayed = _replay(episodes.worker, "root", episodes.histories["root"])
        assert len(replayed.actions) == 1 and replayed.actions[0].HasField("completeOrchestration")
        if functions_host:
            assert _af_replay(episodes.histories["root"], workflow, instance="root")["isDone"]
        assert checkpoint == _VALIDATOR_CALLS and len(seen) == 1


def _invalid_generic_replay_trial(*, functions_host: bool = False) -> None:
    from _workflow_replay_test_support import _af_replay, _Episodes, _replay

    workflow, seen = _generic_workflow(list[int])
    functions: dict[str, Any] | None = None
    if functions_host:
        from agent_framework_azurefunctions import AgentFunctionApp

        app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
        functions = {}
        for item in app.get_functions():
            name = item.get_function_name()
            assert name is not None
            functions[name] = item.get_user_function()
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    _complete_generic_activity(episodes, functions=functions)
    episodes.reply("approval", ["bad"])
    rejected = _complete_generic_activity(episodes, functions=functions)
    assert rejected["hitl_admission"]["status"] == "invalidreply" and not seen
    assert episodes.pending() == {"approval"}
    for _ in range(2):
        cold = _replay(episodes.worker, "root", episodes.histories["root"])
        assert list(cold.actions) == []
        assert set(json.loads(cold.encoded_custom_status)["pending_requests"]) == {"approval"}
        if functions_host:
            af_cold = _af_replay(episodes.histories["root"], workflow, instance="root")
            assert not af_cold["isDone"]
            assert set(af_cold["customStatus"]["pending_requests"]) == {"approval"}
    episodes.reply("approval", [7])
    accepted = _complete_generic_activity(episodes, functions=functions)
    assert accepted["hitl_admission"]["status"] == "accepted"
    assert seen == [[7]] and "root" in episodes.completions
    if functions_host:
        done = _af_replay(episodes.histories["root"], workflow, instance="root")
        assert done["isDone"] and done["output"] == [{"value": [7]}]
        assert seen == [[7]]


def _handler_failure_trial(*, functions_host: bool, output_failure: bool) -> None:
    from _workflow_replay_test_support import _LOGGER, _af_replay, _Episodes
    from durabletask.internal import helpers
    from durabletask.internal import orchestrator_service_pb2 as pb
    from durabletask.worker import _ActivityExecutor

    seen: list[int] = []

    class Broken(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("decision", response_type=int, request_id="approval")

        @response_handler(request=str, response=int, workflow_output=dict)
        async def answer(self, original_request: str, response: int, ctx: WorkflowContext[Never, dict]) -> None:
            seen.append(response)
            if output_failure:
                await ctx.yield_output({1: "invalid workflow transport key"})
            else:
                raise ValueError("Application handler failed")

    gate = Broken(id="gate")
    workflow = WorkflowBuilder(name="failure-hitl", start_executor=gate, output_from=[gate]).build()
    functions: dict[str, Any] | None = None
    if functions_host:
        from agent_framework_azurefunctions import AgentFunctionApp

        app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
        functions = {}
        for item in app.get_functions():
            name = item.get_function_name()
            assert name is not None
            functions[name] = item.get_user_function()
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    _complete_generic_activity(episodes, functions=functions)
    episodes.reply("approval", 7)
    task_id, action = episodes.actions["root"].popitem()
    task = action.scheduleTask
    with pytest.raises(ValueError) as failure:
        if functions is None:
            executor = _ActivityExecutor(episodes.worker._registry, _LOGGER, episodes.worker._data_converter)
            executor.execute("root", task.name, task_id, task.input.value)
        else:
            functions[task.name](json.loads(task.input.value))
    assert seen == [7]
    if output_failure:
        assert "string keys" in str(failure.value)
    else:
        assert str(failure.value) == "Application handler failed"
    episodes.episode("root", helpers.new_task_failed_event(task_id, failure.value))
    assert episodes.completions["root"].orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    if functions_host:
        # The real Functions SDK exposes failed orchestration state on its
        # exception, rather than returning an invalidreply control record.
        with pytest.raises(Exception) as terminal:
            _af_replay(episodes.histories["root"], workflow, instance="root")
        terminal_state = json.loads(str(terminal.value).split("$OutOfProcData$:", 1)[1])
        # Functions' isDone denotes successful invocation, not failure. The
        # populated error and raised out-of-proc exception are its failure contract.
        assert terminal_state["error"]
        expected_error = "string keys" if output_failure else "Application handler failed"
        assert expected_error in terminal_state["error"]
    assert seen == [7]
