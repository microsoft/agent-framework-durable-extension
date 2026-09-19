# Copyright (c) Microsoft. All rights reserved.

"""Exercise standalone sample setup and the real workflow streaming client offline."""

import asyncio
import importlib.util
import json
import logging
import re
import sys
import threading
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from agent_framework import WorkflowEvent
from dotenv import dotenv_values
from test_durable_history_provider import RecordingChatClient

from agent_framework_durabletask._configuration import validate_runtime_deployment
from agent_framework_durabletask._workflows.naming import validate_workflow_name, workflow_orchestrator_name
from agent_framework_durabletask._workflows.protocol import wrap_workflow_input
from agent_framework_durabletask._workflows.serialization import serialize_workflow_event

SAMPLES = Path(__file__).resolve().parents[3] / "samples"
ENV_SAMPLES = sorted(path.parent for path in SAMPLES.glob("1[34]_*/.env.example"))


def test_history_sample_environment_examples_are_covered():
    assert {folder.name for folder in ENV_SAMPLES} == {"13_conversation_compaction", "14_external_history_redis"}


def _load(path: Path, name: str, monkeypatch: pytest.MonkeyPatch):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("folder", ENV_SAMPLES, ids=lambda folder: folder.name)
def test_standalone_sample_requires_explicit_documented_acknowledgement(folder, monkeypatch):
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    supplied = dotenv_values(folder / ".env.example")
    assert supplied["DURABLE_AGENTS_DEPLOYMENT_MODE"] == ""
    for key, value in supplied.items():
        if value is not None:
            monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match="operator acknowledgement"):
        validate_runtime_deployment(None)

    readme = (folder / "README.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```dotenv\n(.*?)```", readme, flags=re.DOTALL)
    settings = [dotenv_values(stream=StringIO(block)) for block in blocks]
    documented = next(values for values in settings if "DURABLE_AGENTS_DEPLOYMENT_MODE" in values)
    assert documented["TASKHUB"] and documented["TASKHUB"] != "default"
    for key, value in documented.items():
        assert value is not None
        monkeypatch.setenv(key, value)
    # The real gate returns None on success, not a truthy acknowledgement value.
    validate_runtime_deployment(None)
    assert "not runtime proof of isolation" in readme
    assert "old engine" in readme


def test_streaming_sample_workflow_name_is_stable(monkeypatch):
    monkeypatch.setattr("dotenv.load_dotenv", Mock(return_value=False))
    worker = _load(SAMPLES / "10_workflow_streaming" / "worker.py", "sample_stream_worker", monkeypatch)
    monkeypatch.setattr(worker, "_create_chat_client", RecordingChatClient)

    first = worker.create_workflow()
    second = worker.create_workflow()

    assert first.name == second.name == "content_pipeline"
    validate_workflow_name(first.name)


async def test_streaming_sample_schedules_once_streams_and_waits_off_loop(monkeypatch, caplog):
    monkeypatch.setattr("dotenv.load_dotenv", Mock(return_value=False))
    folder = SAMPLES / "10_workflow_streaming"
    worker = _load(folder / "worker.py", "sample_stream_contract_worker", monkeypatch)
    monkeypatch.setattr(worker, "_create_chat_client", RecordingChatClient)
    workflow = worker.create_workflow()
    client = _load(folder / "client.py", "sample_stream_client", monkeypatch)
    loop_thread = threading.get_ident()
    events = [
        WorkflowEvent(type="executor_completed", executor_id="publish"),
        WorkflowEvent(type="output", executor_id="publish", data="Published: offline"),
    ]
    state = SimpleNamespace(
        name=workflow_orchestrator_name(workflow.name),
        runtime_status=SimpleNamespace(name="COMPLETED"),
        serialized_custom_status=json.dumps({"events": [serialize_workflow_event(event) for event in events]}),
        serialized_output=json.dumps(["Published: offline"]),
    )
    calls: list[str] = []

    def schedule(*args, **kwargs):
        assert threading.get_ident() != loop_thread
        calls.append("schedule")
        return "sample-instance"

    def poll(instance_id):
        assert threading.get_ident() != loop_thread
        calls.append("poll")
        return state

    def wait(instance_id, *, timeout):
        assert threading.get_ident() != loop_thread
        calls.append("wait")
        return state

    native = Mock()
    native.schedule_new_orchestration.side_effect = schedule
    native.get_orchestration_state.side_effect = poll
    native.wait_for_orchestration_completion.side_effect = wait
    monkeypatch.setattr(client, "get_client", lambda: native)

    # Keep DurableWorkflowClient and all three of its public methods real.
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(client.main(), timeout=5)

    native.schedule_new_orchestration.assert_called_once_with(
        workflow_orchestrator_name(workflow.name),
        input=wrap_workflow_input("Write a short note about durable workflows."),
        instance_id=None,
    )
    native.get_orchestration_state.assert_called_once_with("sample-instance")
    native.wait_for_orchestration_completion.assert_called_once_with("sample-instance", timeout=300)
    assert calls == ["schedule", "poll", "wait"]
    assert "[executor_completed] publish" in caplog.text
    assert "[output] from publish: Published: offline" in caplog.text
    assert "Final output: ['Published: offline']" in caplog.text
