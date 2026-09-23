# Copyright (c) Microsoft. All rights reserved.

"""Keep ordinary child output JSON opaque to Functions SDK custom-object hooks.

Registered generated workflows produce the actual completion payloads, including
through a nested WorkflowExecutor. The shared harness replays constructed host
histories through fresh native SDK contexts, not a live Functions host. Only
benign sentinel imports are observed, and the unloaded sentinel is never imported.
"""

import importlib
import json
import sys
from collections import defaultdict
from collections.abc import Callable, Generator
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, WorkflowExecutor, handler
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.serialization import SUBWORKFLOW_RESULT_KEY
from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions.models.history.HistoryEvent import HistoryEvent
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.Task import AtomicTask, TaskState
from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor
from azure.functions import _durable_functions as sdk_codec

from agent_framework_azurefunctions import _workflow_af_context as adapter_module
from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext

_DECODER_CALLS: list[Any] = []
_UNLOADED_MODULE = "_af_child_completion_unloaded_sentinel"


class _ChildCompletionProbe:
    @classmethod
    def from_json(cls, value: Any) -> Any:
        _DECODER_CALLS.append(deepcopy(value))
        return {"sdk_decoded": value}


def _metadata(module: str) -> dict[str, Any]:
    return {
        "__class__": "_ChildCompletionProbe",
        "__module__": module,
        "__data__": {"value": 7},
        "ordinary_extra": [False, None, "世界"],
    }


def _payload(module: str) -> dict[str, Any]:
    return {
        "business": [False, None, 0, 1.25, "世界", {"nested": [_metadata(module)]}],
        "empty_object": {},
        "empty_array": [],
        # A completion guard must not decode a JSON-looking string twice.
        "json_text": json.dumps(_metadata(module)),
    }


@pytest.fixture
def _decode_attempts(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    _DECODER_CALLS.clear()
    assert sys.modules[__name__].__dict__["_ChildCompletionProbe"] is _ChildCompletionProbe
    assert _UNLOADED_MODULE not in sys.modules
    attempts: list[str] = []
    original = importlib.import_module

    def observe(name: str, package: str | None = None) -> Any:
        if name in (__name__, _UNLOADED_MODULE):
            attempts.append(name)
        if name == _UNLOADED_MODULE:
            raise AssertionError("Child completion attempted a payload-selected module import")
        return original(name, package)

    # Observe both entry points without replacing the SDK object_hook. The
    # loaded probe only records a call and returns JSON, with no external effects.
    monkeypatch.setattr(importlib, "import_module", observe)
    monkeypatch.setattr(sdk_codec, "import_module", observe)
    return attempts


@pytest.fixture
def _af_harness(monkeypatch: pytest.MonkeyPatch) -> Any:
    # The sibling harness imports its shared Durable Task test helpers. Scope
    # both paths to this fixture so selecting this module alone can import it,
    # without changing conftest or relying on another test's collection order.
    tests = Path(__file__).resolve().parent
    monkeypatch.syspath_prepend(str(tests.parents[1] / "durabletask" / "tests"))
    monkeypatch.syspath_prepend(str(tests))
    import test_workflow_child_provenance_af

    assert Path(test_workflow_child_provenance_af.__file__).resolve() == tests / "test_workflow_child_provenance_af.py"
    return test_workflow_child_provenance_af


class _Echo(Executor):
    def __init__(self) -> None:
        super().__init__(id="echo")
        self.seen: list[Any] = []

    @handler(input=object, workflow_output=object)
    async def handle(self, message: Any, ctx: WorkflowContext) -> None:
        self.seen.append(deepcopy(message))
        await ctx.yield_output(message)


class _JsonTextEcho(Executor):
    """Produce structured JSON in the activity, not the native parent's input."""

    def __init__(self) -> None:
        super().__init__(id="echo")
        self.seen: list[Any] = []

    @handler(input=str, workflow_output=dict)
    async def handle(self, message: str, ctx: WorkflowContext) -> None:
        value = json.loads(message)
        self.seen.append(deepcopy(value))
        await ctx.yield_output(value)


def _leaf(echo: Executor) -> Workflow:
    # This is also the generated child called by the harness's native-parent.
    return WorkflowBuilder(name="provenance-leaf", start_executor=echo, output_from=[echo]).build()


def _tree(depth: int) -> tuple[Workflow, _Echo]:
    echo = _Echo()
    inner = _leaf(echo)
    if depth == 2:
        grand = WorkflowExecutor(inner, id="grand", allow_direct_output=True)
        inner = WorkflowBuilder(name="completion-middle", start_executor=grand, output_from=[grand]).build()
    else:
        assert depth == 1
    child = WorkflowExecutor(inner, id="child", allow_direct_output=True)
    root = WorkflowBuilder(name="completion-root", start_executor=child, output_from=[child]).build()
    return root, echo


def _assert_terminal(state: dict[str, Any], expected: Any) -> None:
    assert state["isDone"] is True and not state.get("error")
    assert state["output"] == expected
    # JSON equality also distinguishes bool/number types that Python == merges.
    assert json.dumps(state["output"], sort_keys=True, allow_nan=False) == json.dumps(
        expected, sort_keys=True, allow_nan=False
    )


@pytest.mark.parametrize("depth", [1, 2], ids=["child", "grandchild"])
@pytest.mark.parametrize("module", [__name__, _UNLOADED_MODULE], ids=["loaded", "unloaded"])
def test_generated_af_child_completion_preserves_metadata_without_sdk_construction(
    depth: int, module: str, _af_harness: Any, _decode_attempts: list[str]
) -> None:
    workflow, echo = _tree(depth)
    host = _af_harness._AFStarts(workflow)
    value = _payload(module)
    original = deepcopy(value)
    root = "completion-root::世界"
    instance = root
    state = host.start("dafx-completion-root", root, wrap_workflow_input(value))
    parents: list[str] = []

    for hop in range(depth):
        action = _af_harness._last_action(state, 2)
        parent = instance
        parents.append(parent)
        # Every generated parent starts with its WorkflowExecutor, so its
        # single child task is task 0 in that parent's independent history.
        instance, state = host.child(parent, action, 0)
        executor_id = "child" if hop == 0 else "grand"
        assert instance == subworkflow_instance_id(parent, executor_id, 0)
        assert host.starts[instance]["parentInstanceId"] == parent
        assert host.starts[instance]["history"][1]["Version"] == ""
        assert echo.seen == [] and _DECODER_CALLS == [] and _decode_attempts == []

    leaf_instance = instance
    action = _af_harness._last_action(state, 0)
    assert json.loads(json.loads(action["input"]))["message"] == original
    state = host.complete_activity(leaf_instance, action)
    envelope = {SUBWORKFLOW_RESULT_KEY: True, "outputs": [original], "events": []}
    _assert_terminal(state, envelope)
    assert echo.seen == [original] and value == original
    assert _DECODER_CALLS == [] and _decode_attempts == []
    assert host.replay(leaf_instance) == state
    assert echo.seen == [original] and _DECODER_CALLS == [] and _decode_attempts == []

    for parent in reversed(parents):
        # Use the exact producer output, not a hand-made completion envelope.
        # The unprotected SDK currently decodes Result before the generated
        # parent resumes and before its subworkflow result validator can run.
        child_output = deepcopy(state["output"])
        state = host.complete_child(parent, 0, state["output"])
        assert _DECODER_CALLS == [], "SDK constructed an object while receiving a generated child completion"
        assert _decode_attempts == [], "SDK imported a module while receiving a generated child completion"
        assert json.loads(host.starts[parent]["history"][-1]["Result"]) == child_output
        _assert_terminal(state, [original] if parent == root else envelope)
        assert echo.seen == [original] and value == original

    recorded = deepcopy(host.starts)
    for _ in range(2):
        # Each invocation builds a fresh native context and task registry. No
        # child activity is rerun, and every completed ancestor retains its JSON.
        for replay_instance in [leaf_instance, *reversed(parents)]:
            terminal = host.replay(replay_instance)
            _assert_terminal(terminal, [original] if replay_instance == root else envelope)
        assert host.starts == recorded
        assert echo.seen == [original] and _DECODER_CALLS == [] and _decode_attempts == []
        assert _UNLOADED_MODULE not in sys.modules


def test_unrelated_native_af_parent_retains_sdk_custom_child_completion_decoding(
    _af_harness: Any, _decode_attempts: list[str]
) -> None:
    echo = _JsonTextEcho()
    host = _af_harness._AFStarts(_leaf(echo))
    value = _payload(__name__)
    original = deepcopy(value)
    root = "native-completion-root"
    # The native parent's get_input sees a string, not metadata. Its real
    # generated child activity converts the string to ordinary JSON output.
    state = host.start("native-parent", root, json.dumps(value))
    child_id, child = host.child(root, _af_harness._last_action(state, 2), 0)
    child = host.complete_activity(child_id, _af_harness._last_action(child, 0))
    _assert_terminal(child, [original])
    assert echo.seen == [original] and _DECODER_CALLS == [] and _decode_attempts == []

    expected = deepcopy(original)
    expected["business"][-1]["nested"][0] = {"sdk_decoded": {"value": 7}}
    state = host.complete_child(root, 0, child["output"])
    _assert_terminal(state, [expected])
    assert _DECODER_CALLS == [{"value": 7}] and _decode_attempts == [__name__]
    assert value == original and child["output"] == [original]

    recorded = deepcopy(host.starts)
    for replay_count in (2, 3):
        _assert_terminal(host.replay(root), [expected])
        expected_calls = [{"value": 7}] * replay_count
        assert expected_calls == _DECODER_CALLS
        assert _decode_attempts == [__name__] * replay_count
        assert echo.seen == [original] and host.starts == recorded


def _event(kind: int, event_id: int = -1, **fields: Any) -> dict[str, Any]:
    return {
        "EventType": kind,
        "EventId": event_id,
        "IsPlayed": False,
        "Timestamp": "2026-09-22T00:00:00Z",
        "Version": "",
        **fields,
    }


def _prefix() -> list[dict[str, Any]]:
    return [_event(12), _event(0, Name="completion-parent", Input="null")]


def _context(rows: list[dict[str, Any]], *, cold: bool = False, schema: ReplaySchema = ReplaySchema.V3) -> Any:
    return DurableOrchestrationContext(
        [{**row, "IsPlayed": cold} for row in rows],
        instanceId="completion-parent",
        isReplaying=cold,
        parentInstanceId=None,
        input="null",
        upperSchemaVersion=schema.value,
        maximumShortTimerDuration="00:05:00",
        longRunningTimerIntervalDuration="00:03:00",
    )


def _execute(
    rows: list[dict[str, Any]],
    run: Callable[..., Any],
    *,
    cold: bool = False,
    schema: ReplaySchema = ReplaySchema.V3,
) -> tuple[dict[str, Any], Any]:
    context = _context(rows, cold=cold, schema=schema)
    return json.loads(TaskOrchestrationExecutor().execute(context, context.histories, run)), context


def _actions(groups: list[Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for item in groups:
        if isinstance(item, list):
            actions.extend(_actions(item))
        elif "compoundActions" in item:
            actions.extend(_actions(item["compoundActions"]))
        else:
            actions.append(item)
    return actions


_JSON_VALUES = [
    None,
    False,
    True,
    0,
    -7,
    1.25,
    "",
    "plain 世界",
    "null",
    json.dumps(_metadata(__name__)),
    {},
    [],
    [None],
    _metadata(__name__),
    _metadata(_UNLOADED_MODULE),
    _payload(__name__),
    _payload(_UNLOADED_MODULE),
]


@pytest.mark.parametrize("value", _JSON_VALUES)
@pytest.mark.parametrize("early", [False, True], ids=["waiting", "buffered"])
def test_native_sdk_child_json_types_restore_once_before_resume(
    value: Any, early: bool, _decode_attempts: list[str]
) -> None:
    wire = json.dumps(value)
    completion = _event(8, 101, TaskScheduledId=1, Result=wire)
    ack = _event(5, TaskScheduledId=0, Result="null")
    rows = [
        *_prefix(),
        _event(4, 0, Name="gate", Input="null"),
        _event(7, 1, Name="child", InstanceId="chosen-child", Input="null"),
        *([completion, ack] if early else [ack, completion]),
    ]
    original = deepcopy(rows)

    def run(native: Any) -> Generator[Any, Any, Any]:
        adapter = AzureFunctionsWorkflowContext(native)
        yield adapter.prepare_activity_task("gate", "null")
        child = adapter.call_sub_orchestrator("child", None, instance_id="chosen-child")
        result = yield child
        assert child.id == 1 and child.result is result
        assert child.state is TaskState.SUCCEEDED
        assert json.dumps(result) == wire
        assert _DECODER_CALLS == [] and _decode_attempts == []
        return [result]  # noqa: B901 - Keep a null result visible in SDK output.

    # The buffered variant is a defensive reordered completion fixture. The
    # generated-workflow tests above supply service-ordered producer results.
    for cold in (False, True, True):
        state, _ = _execute(rows, run, cold=cold)
        _assert_terminal(state, [value])
        actions = _actions(state["actions"])
        assert [a["actionType"] for a in actions] == [0, 2]
        assert actions[1]["instanceId"] == "chosen-child" and actions[1]["input"] == "null"
        assert rows == original and _DECODER_CALLS == [] and _decode_attempts == []


def test_sdk_absent_child_result_preserves_native_none(_decode_attempts: list[str]) -> None:
    def run(native: Any) -> Generator[Any, Any, Any]:
        child = AzureFunctionsWorkflowContext(native).call_sub_orchestrator("child", None)
        assert (yield child) is None and child.state is TaskState.SUCCEEDED
        return [None]  # noqa: B901

    state, context = _execute([*_prefix(), _event(8, TaskScheduledId=0, Result=None)], run)
    _assert_terminal(state, [None])
    assert context.histories[-1].Result is None and _DECODER_CALLS == [] and _decode_attempts == []


@pytest.mark.parametrize("wire", [json.dumps(_metadata(__name__)) + " trailing", '{"incomplete":', ""])
def test_malformed_child_json_cannot_run_a_hook_before_parse_failure(wire: str, _decode_attempts: list[str]) -> None:
    context = _context([*_prefix(), _event(8, TaskScheduledId=0, Result=wire)])
    child = AzureFunctionsWorkflowContext(context).call_sub_orchestrator("child", None)
    context._add_to_open_tasks(child)
    executor = TaskOrchestrationExecutor()
    executor.context = context
    with pytest.raises(json.JSONDecodeError):
        executor.set_task_value(context.histories[-1], True, "TaskScheduledId")
    assert child.state is TaskState.RUNNING and child.result is None
    assert _DECODER_CALLS == [] and _decode_attempts == []


@pytest.mark.parametrize("schema", [ReplaySchema.V1, ReplaySchema.V2, ReplaySchema.V3])
def test_child_wrapper_preserves_native_action_identity_ids_failure_and_cancellation(
    schema: ReplaySchema, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(_prefix(), schema=schema)
    adapter = AzureFunctionsWorkflowContext(context)
    registry = context.open_tasks
    original = context.call_sub_orchestrator
    created: list[Any] = []

    def capture(*args: Any, **kwargs: Any) -> Any:
        task = original(*args, **kwargs)
        created.append(task)
        return task

    monkeypatch.setattr(context, "call_sub_orchestrator", capture)
    child = adapter.call_sub_orchestrator("child", {"keep": [None, False]}, "chosen-child")
    native = created[0]
    assert type(native) is AtomicTask and isinstance(child, AtomicTask) and child is not native
    assert child.id is native.id is None and child.action_repr is native.action_repr
    assert child.state is native.state is TaskState.RUNNING and child.parent is native.parent is None
    assert child._api_name == native._api_name == "CallSubOrchestratorAction"
    assert not child._is_scheduled and not native._is_scheduled
    AzureFunctionsWorkflowContext(context)
    assert context.open_tasks is registry
    # Atomic child tasks have no cancellation API. The adapter must not turn
    # cancellation into task completion or acquire timer cancellation semantics.
    adapter.cancel_task(child)
    assert not hasattr(native, "cancel") and not hasattr(child, "cancel")
    context._add_to_open_tasks(child)
    assert child.id == 0 and context.open_tasks[0] is child
    later = context.call_activity("next", None)
    context._add_to_open_tasks(later)
    assert later.id == 1
    failure = RuntimeError("same exception object")
    child.set_value(is_error=True, value=failure)
    with pytest.raises(RuntimeError, match="same exception object") as error:
        adapter.get_task_result(child)
    assert error.value is failure and child.result is failure and child.state is TaskState.FAILED
    timer = adapter.create_timer(context.current_utc_datetime)
    adapter.cancel_task(timer)
    assert timer.is_cancelled
    completed_timer = adapter.create_timer(context.current_utc_datetime)
    completed_timer.set_value(is_error=False, value=None)
    with pytest.raises(ValueError, match="completed"):
        adapter.cancel_task(completed_timer)


@pytest.mark.parametrize("schema", [ReplaySchema.V1, ReplaySchema.V2, ReplaySchema.V3])
@pytest.mark.parametrize("child_first", [False, True])
def test_mixed_composites_retain_losing_tasks_and_unrelated_native_decoders(
    schema: ReplaySchema, child_first: bool, _decode_attempts: list[str]
) -> None:
    protected = _metadata(__name__)
    reply = _metadata(_UNLOADED_MODULE)
    child_done = _event(8, 101, TaskScheduledId=0, Result=json.dumps(protected))
    reply_done = _event(15, 102, Name="reply", Input=json.dumps(reply))
    first, second = (child_done, reply_done) if child_first else (reply_done, child_done)
    rows = [
        *_prefix(),
        _event(7, 0, Name="framework-child", InstanceId="framework-child-id", Input="null"),
        _event(10, 1, FireAt="2026-09-22T00:00:00Z"),
        first,
        _event(7, 2, Name="native-child", InstanceId="native-child-id", Input="null"),
        _event(4, 3, Name="native-activity", Input="null"),
        second,
        _event(8, 103, TaskScheduledId=2, Result=json.dumps(protected)),
        _event(5, 104, TaskScheduledId=3, Result=json.dumps(protected)),
    ]
    expected = [protected, reply, {"sdk_decoded": {"value": 7}}, {"sdk_decoded": {"value": 7}}]

    def native_control(native: Any) -> Generator[Any, Any, Any]:
        child = native.call_sub_orchestrator("framework-child", instance_id="framework-child-id")
        waiting = native.wait_for_external_event("reply")
        timer = native.create_timer(native.current_utc_datetime)
        winner = yield native.task_any([child, waiting, timer])
        assert winner is (child if child_first else waiting)
        timer.cancel()
        other = native.call_sub_orchestrator("native-child", instance_id="native-child-id")
        activity = native.call_activity("native-activity")
        return (yield native.task_all([child, waiting, other, activity]))  # noqa: B901

    control_rows = deepcopy(rows)
    for row in control_rows:
        if row["EventType"] in (5, 8):
            row["Result"] = "null"
        elif row["EventType"] == 15:
            row["Input"] = "null"
    control, _ = _execute(control_rows, native_control, schema=schema)
    _assert_terminal(control, [None] * 4)
    for cold in (False, True, True):
        _DECODER_CALLS.clear()
        _decode_attempts.clear()
        observed: dict[str, Any] = {}

        def run(native: Any, *, observed: dict[str, Any] = observed) -> Generator[Any, Any, Any]:
            adapter = AzureFunctionsWorkflowContext(native)
            child = adapter.call_sub_orchestrator("framework-child", None, "framework-child-id")
            waiting = adapter.wait_for_external_event("reply")
            timer = adapter.create_timer(native.current_utc_datetime)
            race = adapter.task_any([child, waiting, timer])
            winner = yield race
            assert winner is (child if child_first else waiting)
            loser = waiting if child_first else child
            assert loser.state is TaskState.RUNNING
            assert _DECODER_CALLS == [] and _decode_attempts == []
            adapter.cancel_task(timer)
            native_child = native.call_sub_orchestrator("native-child", instance_id="native-child-id")
            activity = native.call_activity("native-activity")
            children = [child, waiting, native_child, activity]
            joined = adapter.task_all(children)
            assert joined.children is children and loser in joined.pending_tasks
            observed.update(children=children, race=race, joined=joined, timer=timer)
            values = yield joined
            assert all(task.state is TaskState.SUCCEEDED for task in children)
            assert [task.id for task in children] == [0, "reply", 2, 3]
            assert all(task.result is value for task, value in zip(children, values))
            assert timer.id == 1 and timer.is_cancelled
            return values  # noqa: B901

        state, context = _execute(rows, run, cold=cold, schema=schema)
        _assert_terminal(state, expected)
        # V1 repeats list-based child actions when a task joins another
        # composite. Preserve the real SDK's per-schema graph, not an invented
        # deduplication rule. Inputs and names match this native control exactly.
        assert state["actions"] == control["actions"]
        actions = _actions(state["actions"])
        assert [a["actionType"] for a in actions] == (
            [2, 6, 5, 2, 6, 2, 0] if schema is ReplaySchema.V1 else [2, 6, 5, 2, 0]
        )
        assert actions[2]["isCanceled"] is True
        assert _DECODER_CALLS == [{"value": 7}] * 2 and _decode_attempts == [__name__] * 2
        assert context.histories[-2].Result == rows[-2]["Result"]
        assert context.histories[-1].Result == rows[-1]["Result"]
        assert observed["joined"].pending_tasks == set()


@pytest.mark.parametrize("composite", ["direct", "all", "any"])
def test_sdk_child_failure_propagates_the_native_exception_without_json_decoding(
    composite: str, _decode_attempts: list[str]
) -> None:
    reason = json.dumps(_metadata(_UNLOADED_MODULE))
    rows = [*_prefix(), _event(9, TaskScheduledId=0, Reason=reason, Details="child failed")]

    def run(native: Any) -> Generator[Any, Any, Any]:
        adapter = AzureFunctionsWorkflowContext(native)
        child = adapter.call_sub_orchestrator("child", None)
        waiting = adapter.wait_for_external_event("loser")
        task = child
        if composite == "all":
            task = adapter.task_all([child, waiting])
        elif composite == "any":
            task = adapter.task_any([child, waiting])
        try:
            result = yield task
            assert composite == "any" and result is child
            adapter.get_task_result(result)
        except Exception as error:
            assert error is child.result and type(error) is Exception
            assert str(error) == f"{reason} \n child failed"
            assert child.state is TaskState.FAILED and waiting.state is TaskState.RUNNING
            with pytest.raises(Exception) as raised:
                adapter.get_task_result(child)
            assert raised.value is error
        else:
            pytest.fail("The child failure was swallowed")
        return "caught"  # noqa: B901

    for cold in (False, True):
        state, context = _execute(rows, run, cold=cold)
        _assert_terminal(state, "caught")
        assert context.histories[-1].Reason == reason and context.histories[-1].Details == "child failed"
        assert _DECODER_CALLS == [] and _decode_attempts == []


@pytest.mark.parametrize(
    "layout", ["dict", "none", "wrong-factory", "subclass", "missing", "history-none", "history-tuple"]
)
def test_unknown_child_context_layout_fails_closed_before_native_call(
    layout: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FutureRegistry(defaultdict):
        pass

    context = _context(_prefix())
    if layout == "missing":
        del context.open_tasks
    elif layout.startswith("history"):
        context._histories = None if layout == "history-none" else tuple(context.histories)
    else:
        context.open_tasks = {
            "dict": {},
            "none": None,
            "wrong-factory": defaultdict(set),
            "subclass": FutureRegistry(list),
        }[layout]
    call = Mock(wraps=context.call_sub_orchestrator)
    monkeypatch.setattr(context, "call_sub_orchestrator", call)
    with pytest.raises(RuntimeError, match="Unsupported Durable Functions workflow child"):
        AzureFunctionsWorkflowContext(context).call_sub_orchestrator("child", None)
    call.assert_not_called()
    assert _actions(context._actions) == []


@pytest.mark.parametrize("layout", ["object", "subclass", "assigned-id", "completed", "scheduled", "parent", "api"])
def test_unknown_child_task_layout_never_falls_back_to_native(layout: str, monkeypatch: pytest.MonkeyPatch) -> None:
    class FutureTask(AtomicTask):
        pass

    context = _context(_prefix())
    task = context.call_sub_orchestrator("child")
    if layout == "object":
        task = object()
    elif layout == "subclass":
        task = FutureTask(task.id, task.action_repr)
    elif layout == "assigned-id":
        task.id = 42
    elif layout == "completed":
        task.set_value(is_error=False, value=None)
    elif layout == "scheduled":
        task._set_is_scheduled(True)
    elif layout == "parent":
        task.parent = context.task_all([task])
    else:
        task._api_name = "FutureChildAction"
    call = Mock(return_value=task)
    monkeypatch.setattr(context, "call_sub_orchestrator", call)
    with pytest.raises(RuntimeError, match="Unsupported Durable Functions workflow child task representation"):
        AzureFunctionsWorkflowContext(context).call_sub_orchestrator("child", None)
    call.assert_called_once_with("child", input_=None, instance_id=None)
    assert not context.open_tasks and _actions(context._actions) == []


@pytest.mark.parametrize("key", ["0", False, 0.0, None])
def test_unknown_child_completion_task_id_fails_closed_before_sdk_decode(key: Any, _decode_attempts: list[str]) -> None:
    wire = json.dumps(_metadata(_UNLOADED_MODULE))
    context = _context([*_prefix(), _event(8, TaskScheduledId=key, Result=wire)])
    child = AzureFunctionsWorkflowContext(context).call_sub_orchestrator("child", None)
    context.open_tasks[key] = child
    executor = TaskOrchestrationExecutor()
    executor.context = context
    with pytest.raises(RuntimeError, match="Unsupported Durable Functions workflow child task ID representation"):
        executor.set_task_value(context.histories[-1], True, "TaskScheduledId")
    assert child.state is TaskState.RUNNING and _DECODER_CALLS == [] and _decode_attempts == []


@pytest.mark.parametrize("raw", [{}, [], b"{}", 7])
def test_unknown_child_result_layout_fails_closed(raw: Any, _decode_attempts: list[str]) -> None:
    context = _context([*_prefix(), _event(8, TaskScheduledId=0, Result=raw)])
    child = AzureFunctionsWorkflowContext(context).call_sub_orchestrator("child", None)
    context._add_to_open_tasks(child)
    executor = TaskOrchestrationExecutor()
    executor.context = context
    with pytest.raises(RuntimeError, match="Unsupported Durable Functions workflow child Result representation"):
        executor.set_task_value(context.histories[-1], True, "TaskScheduledId")
    assert child.state is TaskState.RUNNING and _DECODER_CALLS == [] and _decode_attempts == []


@pytest.mark.parametrize("change", ["append", "replacement", "shrink", "non-list"])
def test_child_completion_index_reuses_history_references_and_invalidates_with_event_index(
    change: str, _decode_attempts: list[str]
) -> None:
    wire = json.dumps(_payload(_UNLOADED_MODULE))
    context = _context([
        *_prefix(),
        _event(15, 100, Name="reply", Input="null"),
        _event(8, 101, TaskScheduledId=0, Result=wire),
        _event(8, 102, TaskScheduledId=1, Result=wire),
    ])
    adapter = AzureFunctionsWorkflowContext(context)
    # The existing event index must index children too in its one traversal.
    adapter.wait_for_external_event("reply")
    index = context._workflow_event_inputs
    original_list = context.histories
    first = original_list[3]
    assert index.child_results[0].events[0] is first
    new_event = HistoryEvent(**_event(8, 103, TaskScheduledId=1, Result=wire))
    if change == "append":
        original_list.append(new_event)
    elif change == "replacement":
        context._histories = [*original_list[:4], new_event]
    elif change == "shrink":
        del original_list[4:]
        new_event = first
    else:
        context._histories = None
        with pytest.raises(RuntimeError, match="workflow child history representation"):
            adapter.call_sub_orchestrator("child", None)
        assert context._workflow_event_inputs is None
        original_list[-1] = new_event
        context._histories = original_list
    child = adapter.call_sub_orchestrator("child", None)
    child.id = new_event.TaskScheduledId
    context.open_tasks[child.id] = child
    executor = TaskOrchestrationExecutor()
    executor.context = context
    executor.set_task_value(new_event, True, "TaskScheduledId")
    assert child.state is TaskState.SUCCEEDED and child.result == _payload(_UNLOADED_MODULE)
    updated = context._workflow_event_inputs
    assert updated.child_results[child.id].events[-1] is new_event
    assert updated.buckets["reply"].events[0] is context.histories[2]
    assert _DECODER_CALLS == [] and _decode_attempts == []


@pytest.mark.parametrize("framework_first", [False, True])
def test_selected_integer_task_controls_only_matching_child_results(
    framework_first: bool, _decode_attempts: list[str]
) -> None:
    wire = json.dumps(_metadata(__name__))
    unused = json.dumps(_metadata(_UNLOADED_MODULE))
    rows = [
        *_prefix(),
        _event(8, 100, TaskScheduledId=0, Result=wire, Input=unused),
        _event(8, 101, TaskScheduledId=0, Result=wire, Input=unused),
        _event(8, 102, TaskScheduledId=99, Result=unused),
        _event(5, 103, TaskScheduledId=0, Result=unused),
        _event(9, 104, TaskScheduledId=0, Reason=unused, Details=unused, Result=unused),
        _event(15, 105, Name="0", Input=unused),
        _event(14, 0, Input=unused),
    ]
    context = _context(rows)
    adapter = AzureFunctionsWorkflowContext(context)
    executor = TaskOrchestrationExecutor()
    executor.context = context
    for offset, framework in enumerate((framework_first, not framework_first)):
        selected = (
            adapter.call_sub_orchestrator("child", None) if framework else context.call_sub_orchestrator("native")
        )
        loser = adapter.call_sub_orchestrator("speculative", None)
        selected.id = loser.id = 0
        tasks = [loser, selected]
        context.open_tasks[0] = tasks
        executor.set_task_value(context.histories[2 + offset], True, "TaskScheduledId")
        assert selected.result == (_metadata(__name__) if framework else {"sdk_decoded": {"value": 7}})
        assert selected.state is TaskState.SUCCEEDED and loser.state is TaskState.RUNNING
        assert context.open_tasks[0] is tasks and tasks == [loser]
        del context.open_tasks[0]
    assert _DECODER_CALLS == [{"value": 7}] and _decode_attempts == [__name__]
    # Identical numeric values in other event kinds are not child completions.
    assert [event.Input for event in context.histories[2:4]] == [unused, unused]
    assert context.histories[4].Result == unused and context.histories[5].Result == unused
    assert context.histories[6].Result == unused and context.histories[6].Reason == unused
    assert context.histories[6].Details == unused
    assert context.histories[7].Input == unused and context.histories[8].Input == unused
    assert set(context._workflow_event_inputs.buckets) == {"0"}
    assert set(context._workflow_event_inputs.child_results) == {0, 99}


def test_child_projection_is_idempotent_and_lookalike_cache_cannot_skip_it(_decode_attempts: list[str]) -> None:
    wire = json.dumps(_payload(_UNLOADED_MODULE))
    context = _context([*_prefix(), _event(8, TaskScheduledId=0, Result=wire)])
    context._workflow_event_inputs = SimpleNamespace(
        histories=context.histories, indexed=len(context.histories), child_results={}
    )
    adapter = AzureFunctionsWorkflowContext(context)
    child = adapter.call_sub_orchestrator("child", None)
    context._add_to_open_tasks(child)
    assert context.open_tasks.pop(0) is child
    index = context._workflow_event_inputs
    projected = context.histories[-1].Result
    assert json.loads(projected) == [wire]
    for number in range(100):
        AzureFunctionsWorkflowContext(context)
        context.open_tasks[0] = child
        assert context.open_tasks.pop(0) is child
        # Default-value pops must not allocate buckets for absent task IDs.
        assert context.open_tasks.pop(1000 + number, None) is None
        assert context._workflow_event_inputs is index and context.histories[-1].Result is projected
    context.open_tasks[0] = child
    executor = TaskOrchestrationExecutor()
    executor.context = context
    executor.set_task_value(context.histories[-1], True, "TaskScheduledId")
    assert child.result == _payload(_UNLOADED_MODULE) and child.state is TaskState.SUCCEEDED
    assert set(index.child_results) == {0} and not index.buckets
    assert _DECODER_CALLS == [] and _decode_attempts == []


class _CountedWire(str):
    comparisons: list[int]

    def __new__(cls, wire: str, comparisons: list[int]) -> "_CountedWire":
        value = super().__new__(cls, wire)
        value.comparisons = comparisons
        return value

    def __eq__(self, other: object) -> bool:
        self.comparisons[0] += 1
        return str.__eq__(self, other)

    def __ne__(self, other: object) -> bool:
        self.comparisons[0] += 1
        return str.__ne__(self, other)

    __hash__ = str.__hash__


def _measure_children(monkeypatch: pytest.MonkeyPatch, count: int, *, early: bool) -> dict[str, int]:
    value = _payload(_UNLOADED_MODULE)
    comparisons = [0]
    wire = _CountedWire(json.dumps(value), comparisons)
    completions = [_event(8, 1000 + i, TaskScheduledId=2 * i + 1, Result=wire) for i in range(count)]
    checkpoints = [_event(5, TaskScheduledId=2 * i + 2, Result="null") for i in range(count)]
    gate = _event(5, TaskScheduledId=0, Result="null")
    unused = [_event(15, 2000 + i, Name=f"unused-{i}", Input=wire) for i in range(count)]
    rows = [*_prefix(), *unused]
    if early:
        rows.extend([*completions, gate, *checkpoints])
    else:
        rows.append(gate)
        for completion, checkpoint in zip(completions, checkpoints):
            rows.extend([completion, checkpoint])
    context = _context(rows)
    history_ids = {id(event) for event in context.histories}
    counts = {"event_type": 0, "Name": 0, "Input": 0, "Result": 0, "TaskScheduledId": 0, "plain_json": 0}
    original_getattribute = HistoryEvent.__getattribute__

    def read(event: HistoryEvent, name: str) -> Any:
        if id(event) in history_ids and name in counts:
            counts[name] += 1
        return original_getattribute(event, name)

    def loads(raw: Any, *args: Any, **kwargs: Any) -> Any:
        counts["plain_json"] += 1
        return json.loads(raw, *args, **kwargs)

    children: list[Any] = []

    def run(native: Any) -> Generator[Any, Any, None]:
        adapter = AzureFunctionsWorkflowContext(native)
        yield adapter.prepare_activity_task("gate", "null")
        for number in range(count):
            # Reusing the adapter/cache and negative event lookups must not
            # rescan child buckets or cause the old event guard to do more work.
            adapter = AzureFunctionsWorkflowContext(native)
            adapter.wait_for_external_event(f"absent-{number}")
            child = adapter.call_sub_orchestrator("child", None, f"child-{number}")
            children.append(child)
            assert (yield child) == value and child.id == 2 * number + 1
            yield adapter.prepare_activity_task("checkpoint", "null")
        yield adapter.wait_for_external_event("finish")
        pytest.fail("There is no finish event")

    with monkeypatch.context() as observer:
        observer.setattr(HistoryEvent, "__getattribute__", read)
        observer.setattr(adapter_module, "json", SimpleNamespace(loads=loads, dumps=json.dumps))
        state = json.loads(TaskOrchestrationExecutor().execute(context, context.histories, run))
    assert not state["isDone"] and len(children) == count
    assert all(child.state is TaskState.SUCCEEDED for child in children)
    assert comparisons == [0] and counts["plain_json"] == count
    actions = _actions(state["actions"])
    assert sum(a["actionType"] == 2 for a in actions) == count
    assert sum(a["actionType"] == 0 for a in actions) == count + 1
    assert sum(a["actionType"] == 6 for a in actions) == 1
    return counts


@pytest.mark.parametrize("early", [False, True])
def test_child_completion_index_and_plain_decoder_work_are_linear(
    early: bool, monkeypatch: pytest.MonkeyPatch, _decode_attempts: list[str]
) -> None:
    small = _measure_children(monkeypatch, 32, early=early)
    large = _measure_children(monkeypatch, 64, early=early)
    for operation in small:
        assert large[operation] <= 2 * small[operation] + 30, (operation, small, large)
        assert large[operation] <= 40 * 64 + 100, (operation, large)
    assert _DECODER_CALLS == [] and _decode_attempts == []
