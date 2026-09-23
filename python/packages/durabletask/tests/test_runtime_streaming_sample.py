# Copyright (c) Microsoft. All rights reserved.

"""Exercise workflow and HTTP streaming sample contracts offline."""

import ast
import asyncio
import importlib.util
import json
import logging
import re
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from _execution_test_support import RecordingChatClient
from agent_framework import WorkflowEvent
from packaging.requirements import Requirement

from agent_framework_durabletask._workflows.naming import validate_workflow_name, workflow_orchestrator_name
from agent_framework_durabletask._workflows.protocol import wrap_workflow_input
from agent_framework_durabletask._workflows.serialization import serialize_workflow_event

SAMPLES = Path(__file__).resolve().parents[3] / "samples"


def test_external_history_sample_declares_its_direct_authentication_dependency():
    sample = SAMPLES / "14_external_history_redis"
    for name in ("worker.py", "client.py"):
        tree = ast.parse((sample / name).read_text(encoding="utf-8"))
        assert any(
            isinstance(node, ast.ImportFrom) and node.module in ("azure.identity", "azure.identity.aio")
            for node in ast.walk(tree)
        )
    requirements = {
        Requirement(line.split("#", 1)[0].strip()).name
        for line in (sample / "requirements.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith(("#", "-e"))
    }
    assert "azure-identity" in requirements


def test_reliable_streaming_http_demo_uses_accepted_session_id():
    demo = (SAMPLES / "azure_functions" / "03_reliable_streaming" / "demo.http").read_text(encoding="utf-8")
    start_request = demo.split("# @name trip\n", 1)[1].split("\n###", 1)[0].splitlines()

    assert "@sessionId = {{trip.response.body.$.session_id}}" in demo.splitlines()
    # A plain-text POST needs both settings for a non-blocking JSON response.
    assert start_request[0] == "POST {{baseUrl}}/api/agents/{{agentName}}/run?wait_for_response=false"
    assert "Content-Type: text/plain" in start_request
    assert "Accept: application/json" in start_request
    assert [line for line in demo.splitlines() if line.startswith("GET {{baseUrl}}/api/agent/stream/")] == [
        "GET {{baseUrl}}/api/agent/stream/{{sessionId}}",
        "GET {{baseUrl}}/api/agent/stream/{{sessionId}}",
        "GET {{baseUrl}}/api/agent/stream/{{sessionId}}?cursor={cursor_id}",
    ]


@pytest.mark.parametrize(("sample", "expected_count"), [("01_single_agent", 1), ("02_multi_agent", 2)])
def test_agent_http_sample_accepted_examples_use_session_id(sample, expected_count):
    source = (SAMPLES / "azure_functions" / sample / "function_app.py").read_text(encoding="utf-8")
    examples = re.findall(r"HTTP/1\.1 202 Accepted\n(\{.*?\n\})", source, re.DOTALL)
    assert len(examples) == expected_count
    for example in examples:
        payload = json.loads(example)
        assert payload["status"] == "accepted"
        assert "session_id" in payload
        assert payload["session_id"] == "<guid>"
        assert "conversation_id" not in payload


def _load(path: Path, name: str, monkeypatch: pytest.MonkeyPatch):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


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
