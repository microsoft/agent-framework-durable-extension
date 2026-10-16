# Copyright (c) Microsoft. All rights reserved.

"""Real Scheduler legacy continuation, evidence-backed conversion and cold replay.

Only model I/O is substituted. Requires the local DTS emulator, no model credentials.
Worker replacement is exclusive; this does not test an incompatible overlapping fleet.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Event
from typing import Any

import pytest
from agent_framework import (
    Agent,
    CompactionProvider,
    CompactionStrategy,
    Executor,
    Message,
    WorkflowBuilder,
    WorkflowContext,
    handler,
    response_handler,
)
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.client import OrchestrationStatus
from durabletask.entities import EntityInstanceId
from live_retention_worker import AGENT_NAME, RecordingModel, live_retention_call
from test_15_dt_live_retention import (
    _snapshot,
)
from test_15_dt_live_retention import (
    live_client as live_client,
)
from test_15_dt_live_retention import (
    live_taskhub as live_taskhub,
)

from agent_framework_durabletask import (
    DurableAIAgentWorker,
    DurableWorkflowClient,
    LegacyMigrationEvidence,
    RunRequest,
    state_snapshot_digest,
)
from agent_framework_durabletask._shared_response import serialize_terminal_response

pytestmark = [pytest.mark.integration, pytest.mark.requires_dts, pytest.mark.timeout(180)]


class _Human(Executor):
    @handler(input=str)
    async def begin(self, value: str, ctx: WorkflowContext) -> None:
        await ctx.request_info(value, response_type=int, request_id="decision")

    @response_handler(request=str, response=int, workflow_output=str)
    async def finish(self, request: str, response: int, ctx: WorkflowContext[Any, str]) -> None:
        await ctx.yield_output(f"{request}:{response}")


@contextmanager
def _host(
    endpoint: str,
    hub: str,
    artifacts: Path,
    *,
    mode: str | None = None,
    evidence: Callable[[dict[str, Any]], LegacyMigrationEvidence | None] | None = None,
    after_strategy: CompactionStrategy | None = None,
) -> Iterator[RecordingModel]:
    artifacts.mkdir()
    model = RecordingModel(artifacts, "")
    worker = DurableTaskSchedulerWorker(
        host_address=endpoint,
        taskhub=hub,
        token_credential=None,
        secure_channel=False,
    )
    host = DurableAIAgentWorker(worker, deployment_mode=mode, legacy_migration_evidence=evidence)
    host.add_agent(
        Agent(
            client=model,
            name=AGENT_NAME,
            default_options={"store": False},
            context_providers=[CompactionProvider(after_strategy=after_strategy)]
            if after_strategy is not None
            else None,
        )
    )
    host.configure_workflow(WorkflowBuilder(name="live-compatibility", start_executor=_Human(id="human")).build())
    worker.add_orchestrator(live_retention_call)
    try:
        host.start()
        yield model
    finally:
        host.stop()


def _wait(probe: Callable[[], Any], description: str) -> Any:
    deadline = time.monotonic() + 30
    pause = Event()
    while time.monotonic() < deadline:
        if result := probe():
            return result
        pause.wait(min(0.1, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"Scheduler did not expose {description}")


def _finished(client: DurableTaskSchedulerClient, instance: str) -> Any:
    state = client.get_orchestration_state(instance)
    if state is not None:
        state.raise_if_failed()
        if state.runtime_status == OrchestrationStatus.COMPLETED:
            return state
    return None


def _turn(
    client: DurableTaskSchedulerClient,
    entity: EntityInstanceId,
    correlation: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    request = RunRequest(
        "",
        correlation_id=correlation,
        context_messages=[Message("user", [correlation], message_id=correlation).to_dict()],
    )
    instance = client.schedule_new_orchestration(
        "live_retention_call",
        input={"key": entity.key, "request": request.to_dict()},
    )
    completed = _wait(lambda: _finished(client, instance), f"agent completion {correlation}")
    response = json.loads(completed.serialized_output)
    assert response["messages"][0]["contents"][0]["text"] == f"answer:{correlation}"
    return response, _snapshot(client, entity)["state"]


def test_live_legacy_compaction_survives_exclusive_worker_replacement(
    live_client: DurableTaskSchedulerClient,
    dts_endpoint: str,
    live_taskhub: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    entity = EntityInstanceId(entity=f"dafx-{AGENT_NAME}", key="legacy-compaction")

    async def compact(messages: list[Message]) -> bool:
        for message in messages:
            if message.text in ("first", "answer:first"):
                message.additional_properties["_excluded"] = True
        messages.insert(0, Message("system", ["summary"]))
        return True

    with _host(dts_endpoint, live_taskhub, tmp_path / "compact", mode="legacy", after_strategy=compact):
        _, source = _turn(live_client, entity, "first")
        assert source["schemaVersion"] == "1.1.0"
        assert source["data"]["extensionData"]["_dafx_legacy_history"]["generated"]
    with _host(dts_endpoint, live_taskhub, tmp_path / "cold", mode="legacy") as cold:
        _, committed = _turn(live_client, entity, "second")
        captured = json.loads((cold.artifacts / "model-1.json").read_text(encoding="utf-8"))
        assert [Message.from_dict(message).text for message in captured] == ["summary", "second"]
        _, duplicate = _turn(live_client, entity, "first")
        assert duplicate == committed and cold.calls == 1


def test_live_legacy_continuation_migration_and_cold_replay(
    live_client: DurableTaskSchedulerClient,
    dts_endpoint: str,
    live_taskhub: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    entity = EntityInstanceId(entity=f"dafx-{AGENT_NAME}", key="compatibility-session")
    results: list[dict[str, Any]] = []

    def record(correlation: str, response: dict[str, Any], source: dict[str, Any]) -> None:
        # Outcome comes from the acknowledged successful call and its exact output.
        # Only the original timestamp is read from the committed legacy response.
        stored = next(
            entry
            for entry in source["data"]["conversationHistory"]
            if entry["$type"] == "response" and entry["correlationId"] == correlation
        )
        results.append({
            "correlationId": correlation,
            "outcome": "succeeded",
            "completedAt": stored["createdAt"],
            "response": serialize_terminal_response(response),
        })

    def waiting() -> Any:
        state = live_client.get_orchestration_state("legacy-human")
        if state is None:
            return None
        state.raise_if_failed()
        status = json.loads(state.serialized_custom_status or "{}")
        return status if status.get("state") == "waiting_for_human_input" else None

    with _host(dts_endpoint, live_taskhub, tmp_path / "legacy", mode="legacy") as legacy:
        response, source = _turn(live_client, entity, "first")
        assert source["schemaVersion"] == "1.1.0"
        assert legacy.calls == 1
        record("first", response, source)
        client = DurableWorkflowClient(live_client, workflow_name="live-compatibility", deployment_mode="legacy")
        client.start_workflow("question", instance_id="legacy-human")
        assert _wait(waiting, "legacy human wait")["pending_requests"]["decision"]["response_type"] == "builtins:int"

    # No deployment setting and no evidence: new workers continue old entities.
    with _host(dts_endpoint, live_taskhub, tmp_path / "bridge") as bridge:
        response, source = _turn(live_client, entity, "second")
        assert source["schemaVersion"] == "1.1.0"
        captured = json.loads((bridge.artifacts / "model-1.json").read_text(encoding="utf-8"))
        assert [message["contents"][0]["text"] for message in captured] == ["first", "answer:first", "second"]
        record("second", response, source)
        client = DurableWorkflowClient(live_client, workflow_name="live-compatibility")
        client.send_hitl_response("legacy-human", "decision", 17)
        completed = _wait(lambda: _finished(live_client, "legacy-human"), "cold legacy human completion")
        assert json.loads(completed.serialized_output) == ["question:17"]

    evidence = LegacyMigrationEvidence(
        completion_evidence={
            "sourceDigest": state_snapshot_digest(source),
            "evidenceId": "recorded-successful-calls",
            "complete": True,
            "results": results,
        }
    )
    with _host(dts_endpoint, live_taskhub, tmp_path / "convert", evidence=lambda _: evidence) as converted:
        _, committed = _turn(live_client, entity, "third")
        assert converted.calls == 1
        assert committed["schemaVersion"] == "2.0.0"
        assert committed["data"]["migration"]["mode"] == "in_place"
        assert set(committed["data"]["completionReceipts"]) == {"first", "second", "third"}

    with _host(dts_endpoint, live_taskhub, tmp_path / "cold") as cold:
        _, duplicate = _turn(live_client, entity, "third")
        assert duplicate == committed
        assert cold.calls == 0
        _, continued = _turn(live_client, entity, "fourth")
        assert continued["schemaVersion"] == "2.0.0"
        assert cold.calls == 1
