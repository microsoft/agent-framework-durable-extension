# Copyright (c) Microsoft. All rights reserved.

"""Test-only DTS host with real core history and a deterministic, recording model.

The stdin/stdout protocol carries bounded control records only. Full model inputs
and simulated external effects stay in the parent test's temporary directory.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import AsyncIterable, Awaitable, Generator, Mapping, Sequence
from pathlib import Path
from threading import Event, Lock
from typing import Any

from agent_framework import Agent, BaseChatClient, ChatResponse, ChatResponseUpdate, Content, Message, ResponseStream
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.entities import EntityInstanceId
from durabletask.task import OrchestrationContext
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Sum

import agent_framework_durabletask
from agent_framework_durabletask import DurableAIAgentWorker, DurableHistoryProvider

AGENT_NAME = "live-retention"
MAX_STATE_BYTES = 50_000
DELIVERY_WINDOW_SECONDS = 3600
CONTROL_TIMEOUT = 45
_output_lock = Lock()


def emit(event: str, **fields: Any) -> None:
    record = json.dumps({"event": event, **fields}, ensure_ascii=True)
    if len(record) > 2048:
        raise ValueError("Control record exceeds its bound")
    with _output_lock:
        sys.stdout.write(record + "\n")
        sys.stdout.flush()


class RecordingModel(BaseChatClient):
    """Only model I/O is replaced. Agent streaming and history hooks are real."""

    def __init__(self, artifacts: Path, blocked_message_id: str) -> None:
        super().__init__()
        self.artifacts = artifacts
        self.blocked_message_id = blocked_message_id
        self.release = Event()
        self.calls = 0

    def _capture(self, messages: Sequence[Message], current_id: str) -> None:
        self.calls += 1
        ordinal = self.calls
        captured = [message.to_dict() for message in messages]
        (self.artifacts / f"model-{ordinal}.json").write_text(json.dumps(captured), encoding="utf-8")
        # This file is the simulated nontransactional external effect, not entity state.
        (self.artifacts / f"effect-{ordinal}.json").write_text(
            json.dumps({"message_id": current_id, "ordinal": ordinal}), encoding="utf-8"
        )
        emit("model_entered", ordinal=ordinal, message_id=current_id)

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        async def response_update() -> ChatResponseUpdate:
            await self._validate_options(options)
            current_id = next(message.message_id for message in reversed(messages) if message.role == "user")
            if not current_id:
                raise ValueError("The test requires a current user message ID")
            await asyncio.to_thread(self._capture, messages, current_id)
            if current_id == self.blocked_message_id:
                released = await asyncio.to_thread(self.release.wait, CONTROL_TIMEOUT)
                if not released:
                    # Do not turn a missed test barrier into a committed agent error response.
                    raise asyncio.CancelledError("Test model barrier timed out")
            return ChatResponseUpdate(
                role="assistant",
                author_name="retention-model",
                contents=[Content.from_text(f"answer:{current_id}")],
                message_id=f"{current_id}-answer",
                response_id=f"response:{current_id}",
                finish_reason="stop",
            )

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            yield await response_update()

        async def response() -> ChatResponse:
            return ChatResponse.from_updates([await response_update()])

        return ResponseStream(updates(), finalizer=ChatResponse.from_updates) if stream else response()


def live_retention_duplicate(context: OrchestrationContext, payload: dict[str, Any]) -> Generator[Any, Any, Any]:
    """A same-sender signal then call supplies an acknowledged duplicate barrier."""
    entity = EntityInstanceId(entity=f"dafx-{AGENT_NAME}", key=payload["key"])
    context.signal_entity(entity, "run", payload["request"])
    result = yield context.call_entity(entity, "run", payload["request"])
    return result  # noqa: B901


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--taskhub", required=True)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--block-message-id", default="")
    args = parser.parse_args()
    expected_package = Path(__file__).resolve().parents[2] / "agent_framework_durabletask"
    if Path(agent_framework_durabletask.__file__).resolve().parent != expected_package:
        raise RuntimeError("Worker imported durabletask extension from a different checkout")
    logging.basicConfig(level=logging.WARNING)
    reader = InMemoryMetricReader()
    meters = MeterProvider(metric_readers=[reader], shutdown_on_exit=False)
    set_meter_provider(meters)
    model = RecordingModel(args.artifacts, args.block_message_id)
    worker = DurableTaskSchedulerWorker(
        host_address=args.endpoint, taskhub=args.taskhub, token_credential=None, secure_channel=False
    )
    host = DurableAIAgentWorker(
        worker,
        deployment_mode="isolated_v2",
        retention="keep_all",
        max_state_bytes=MAX_STATE_BYTES,
        response_delivery_window_seconds=DELIVERY_WINDOW_SECONDS,
    )
    host.add_agent(
        Agent(
            client=model,
            name=AGENT_NAME,
            id=AGENT_NAME,
            default_options={"store": False},
            context_providers=[DurableHistoryProvider()],
        )
    )
    worker.add_orchestrator(live_retention_duplicate)
    try:
        host.start()
        # start() launches the SDK background thread. Only a backend receipt proves readiness.
        emit("started")
        for line in sys.stdin:
            command = json.loads(line)["command"]
            if command == "release":
                model.release.set()
            elif command == "metrics":
                data = reader.get_metrics_data()
                rows: list[dict[str, Any]] = []
                if data is not None:
                    for resource in data.resource_metrics:
                        for scope in resource.scope_metrics:
                            for metric in scope.metrics:
                                if metric.name == "durable.retention.removed_messages":
                                    assert isinstance(metric.data, Sum) and metric.data.is_monotonic
                                    rows.extend(
                                        {"value": point.value, "attributes": dict(point.attributes or {})}
                                        for point in metric.data.data_points
                                    )
                (args.artifacts / "metrics.json").write_text(json.dumps(rows), encoding="utf-8")
                emit("metrics")
            elif command == "stop":
                break
            else:
                raise ValueError("Unknown test control command")
    finally:
        model.release.set()
        try:
            host.stop()
        finally:
            meters.shutdown()


if __name__ == "__main__":
    main()
