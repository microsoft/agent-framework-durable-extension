# Copyright (c) Microsoft. All rights reserved.

"""Media retention through core hooks and JSON cold starts, not live model services."""

import hashlib
import json
import struct
import zlib
from collections.abc import Awaitable, Iterator
from copy import deepcopy
from typing import Any

import pytest
from agent_framework import GROUP_ANNOTATION_KEY, ChatResponse, CompactionProvider, Content, Message
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Sum
from test_durable_history_provider import RecordingChatClient
from test_history_pipeline_revision import NonStreamingAgent
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask import _retention_telemetry as telemetry
from agent_framework_durabletask._retention import RetentionMode, StateCapacityError

MEDIA_CASES = ("inline-png", "inline-text", "uri-image", "hosted-file", "mixed-tool", "large-tool")


def _png() -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))

    # A valid grayscale PNG with incompressible pixels. The binary data, not just
    # text padding, must contribute materially to pressure and protected-floor checks.
    width, height = 128, 64
    pixels = hashlib.shake_256(b"durable-media-pressure").digest(width * height)
    rows = b"".join(b"\x00" + pixels[row * width : (row + 1) * width] for row in range(height))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )


PNG = _png()


@pytest.fixture
def media_metrics(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader], shutdown_on_exit=False)
    monkeypatch.setattr(telemetry, "get_meter", meter_provider.get_meter)
    telemetry._instruments.cache_clear()
    try:
        yield reader
    finally:
        telemetry._instruments.cache_clear()
        meter_provider.shutdown()


def _removed_counter(reader: InMemoryMetricReader) -> int:
    data = reader.get_metrics_data()
    total = 0
    if data is not None:
        for resource in data.resource_metrics:
            for scope in resource.scope_metrics:
                for metric in scope.metrics:
                    if metric.name != "durable.retention.removed_messages":
                        continue
                    assert isinstance(metric.data, Sum) and metric.data.is_monotonic
                    for point in metric.data.data_points:
                        assert point.attributes is not None
                        assert point.attributes["outcome"] == "staged"
                        assert point.attributes["commit_status"] == "not_attempted"
                        assert isinstance(point.value, int)
                        total += point.value
    return total


def _payload_messages(kind: str, turn: str) -> list[Message]:
    application = {"type": "text", "nested": {"type": "error", "labels": [turn, "界", 0, False, None]}}
    media = {
        "inline-png": Content.from_data(PNG, "image/png"),
        "inline-text": Content.from_data(("inline document 界\n" * 400).encode(), "text/plain"),
        "uri-image": Content.from_uri("https://example.test/image.png?version=1", media_type="image/png"),
        "hosted-file": Content("hosted_file", file_id=f"file-{turn}", additional_properties=deepcopy(application)),
    }.get(kind, Content.from_text("Use the tool payload"))
    arguments: Any = {"query": turn, "metadata": deepcopy(application)}
    result: Any = {"records": [turn], "metadata": deepcopy(application)}
    if kind == "mixed-tool":
        result = [
            Content.from_text("tool text 界", additional_properties=deepcopy(application)),
            Content.from_data(PNG, "image/png"),
            Content.from_data(b"inline tool document", "text/plain"),
        ]
    elif kind == "large-tool":
        # Preserve the original JSON string, including its whitespace, not a reparsed equivalent.
        arguments = json.dumps({"query": "界🚀" * 700, "metadata": application}, ensure_ascii=False, indent=2)
        result = {"records": ["result 界🚀" * 700], "metadata": deepcopy(application)}
    return [
        Message(
            "user",
            [Content.from_text(f"{turn}: " + "context " * 400), media],
            message_id=f"{turn}-input",
            author_name="media-user",
            additional_properties=deepcopy(application),
        ),
        Message(
            "assistant",
            [
                Content.from_text_reasoning(
                    id=f"{turn}-reasoning",
                    text="Retain this reasoning summary",
                    protected_data=f"opaque-{turn}",
                    additional_properties=deepcopy(application),
                ),
                Content.from_function_call(f"{turn}-call", "lookup", arguments=arguments),
            ],
            message_id=f"{turn}-call-message",
            author_name="planner",
            additional_properties=deepcopy(application),
        ),
        Message(
            "tool",
            [Content.from_function_result(f"{turn}-call", result=result, additional_properties=deepcopy(application))],
            message_id=f"{turn}-result-message",
            author_name="lookup",
            additional_properties=deepcopy(application),
        ),
    ]


class _MediaClient(RecordingChatClient):
    """Core Agent still owns history hooks, only the model response is deterministic."""

    def get_response(self, messages: Any, *, stream: bool = False, **kwargs: Any) -> Awaitable[ChatResponse]:
        if stream:
            raise TypeError("stream is not supported")
        self.received_messages.append(deepcopy(list(messages)))
        self._counter += 1
        counter = self._counter

        async def get() -> ChatResponse:
            return ChatResponse(
                messages=[
                    Message(
                        "assistant",
                        [Content.from_text(f"answer-{counter}", additional_properties={"json": {"keep": [0, False]}})],
                        message_id=f"answer-{counter}",
                        author_name="media-client",
                        additional_properties={"json": {"type": "text", "keep": [counter, None]}},
                    )
                ],
                response_id=f"response-{counter}",
            )

        return get()


def _agent(client: _MediaClient, **kwargs: Any) -> NonStreamingAgent:
    chat_client: Any = client
    return NonStreamingAgent(client=chat_client, **kwargs)


def _request(correlation: str, messages: list[Message]) -> dict[str, Any]:
    return {
        "message": "projected media turn",
        "correlationId": correlation,
        "contextMessages": [deepcopy(message.to_dict()) for message in messages],
    }


def _messages(raw: dict[str, Any]) -> list[Message]:
    cold = DurableAgentState.from_json(json.dumps(raw))
    return [message.to_chat_message() for entry in cold.data.conversation_history for message in entry.messages]


@pytest.mark.parametrize("kind", MEDIA_CASES)
@pytest.mark.parametrize("retention", ["keep_all", "follow_compaction"])
@pytest.mark.parametrize("pressure", [False, True], ids=["no-budget", "pressure"])
async def test_media_payloads_survive_policy_matrix_json_reload_and_next_model_call(
    kind: str, retention: RetentionMode, pressure: bool, media_metrics: InMemoryMetricReader
) -> None:
    provider = JsonStateProvider()
    client = _MediaClient()
    seed_agent: Any = _agent(client=client, name="media")
    seed = AgentEntity(seed_agent, state_provider=provider)
    originals: dict[str, dict[str, Any]] = {}
    inputs: dict[str, list[Message]] = {}
    atomic_pairs: list[set[str]] = []
    for index in range(8):
        turn = f"seed-{index}"
        inputs[turn] = _payload_messages(kind, turn)
        response = await seed.run(_request(turn, inputs[turn]))
        for message in [*inputs[turn], *response.messages]:
            assert message.message_id is not None
            originals[message.message_id] = deepcopy(message.to_dict())
        atomic_pairs.append({f"{turn}-call-message", f"{turn}-result-message"})

    persisted_seed = json.loads(json.dumps(provider.raw))
    assert [message.to_dict() for message in _messages(persisted_seed)] == list(originals.values())
    assert provider.writes == 8
    # The request payloads dominate the small answer mailboxes, leaving a reachable floor.
    # Use the actual complete JSON size so all media forms exercise pressure without a guessed cap.
    budget = int(len(json.dumps(persisted_seed)) * 0.8) if pressure else None
    excluded = {message.message_id for message in inputs["seed-0"]} | {"answer-1", "seed-1-call-message"}
    strategy_calls: list[int] = []
    core_groups: dict[str, Any] = {}

    async def exclude_old(messages: list[Message]) -> bool:
        strategy_calls.append(len(messages))
        changed = False
        for message in messages:
            assert message.message_id is not None
            # Core adds grouping annotations before invoking a custom strategy. Preserve that
            # generated metadata separately from the independent original-payload oracle.
            core_groups[message.message_id] = deepcopy(message.additional_properties[GROUP_ANNOTATION_KEY])
            if message.message_id in excluded and not message.additional_properties.get("_excluded"):
                message.additional_properties["_excluded"] = True
                changed = True
        return changed

    history = DurableHistoryProvider()
    current_agent: Any = _agent(
        client=client,
        name="media",
        context_providers=[
            history,
            CompactionProvider(after_strategy=exclude_old, history_source_id=history.source_id),
        ],
    )
    current_provider = JsonStateProvider(persisted_seed)
    entity = AgentEntity(current_agent, state_provider=current_provider, retention=retention, max_state_bytes=budget)
    current_inputs = _payload_messages(kind, "current")
    response = await entity.run(_request("current", current_inputs))
    for message in [*current_inputs, *response.messages]:
        assert message.message_id is not None
        originals[message.message_id] = deepcopy(message.to_dict())
    for message_id in excluded:
        assert message_id is not None
        originals[message_id]["additional_properties"]["_excluded"] = True
    for message_id, group in core_groups.items():
        originals[message_id]["additional_properties"][GROUP_ANNOTATION_KEY] = group
        # Core compaction explicitly materializes False on included messages.
        originals[message_id]["additional_properties"].setdefault("_excluded", False)
    assert strategy_calls, "the fixture must execute real core compaction hooks"
    assert current_provider.writes == 1

    raw = json.loads(json.dumps(current_provider.raw))
    retained = _messages(raw)
    retained_ids = {message.message_id for message in retained}
    removed = set(originals) - retained_ids
    assert [message.to_dict() for message in retained] == [
        payload for message_id, payload in originals.items() if message_id in retained_ids
    ]
    newest_ids = {message.message_id for message in [*current_inputs, *response.messages]}
    assert newest_ids <= retained_ids, "the entire newest exchange is protected, including non-text payloads"
    for pair in atomic_pairs:
        assert pair <= retained_ids or pair.isdisjoint(retained_ids), "no half tool-call/result group may be deleted"
    if pressure:
        assert len(removed) > 4, "pressure must remove more than the one fully excluded exchange"
        assert budget is not None and len(json.dumps(raw)) < budget * 0.85
    elif retention == "follow_compaction":
        assert removed == {message.message_id for message in inputs["seed-0"]} | {"answer-1"}
        assert {"seed-1-call-message", "seed-1-result-message"} <= retained_ids
    else:
        assert removed == set()
    truncation = raw["data"].get("truncation") or {}
    assert truncation.get("evictedMessageCount", 0) == len(removed)
    assert _removed_counter(media_metrics) == len(removed)
    if removed:
        assert truncation["firstEvictedAt"] and truncation["lastEvictedAt"]

    # A new provider and core Agent must reconstruct only committed, included payloads.
    # Resending an old projected input also checks that eviction did not erase its receipt.
    cold_client = _MediaClient()
    cold_client._counter = client._counter
    cold_agent: Any = _agent(client=cold_client, name="media", context_providers=[DurableHistoryProvider()])
    cold_provider = JsonStateProvider(raw)
    cold = AgentEntity(cold_agent, state_provider=cold_provider, retention=retention, max_state_bytes=budget)
    next_input = Message("user", ["next model call"], message_id="next-input", additional_properties={"json": [1]})
    duplicate = await cold.run(_request("current", current_inputs))
    assert duplicate.to_dict() == response.to_dict()
    assert cold_client.received_messages == [] and cold_provider.writes == 0
    await cold.run(_request("next", [*inputs["seed-0"], next_input]))
    expected = [message.to_dict() for message in retained if not message.additional_properties.get("_excluded")]
    for payload in expected:
        # HistoryProvider.before_run contributes source attribution to model copies only.
        payload["additional_properties"]["_attribution"] = {
            "source_id": DurableHistoryProvider.DEFAULT_SOURCE_ID,
            "source_type": "DurableHistoryProvider",
        }
    assert len(cold_client.received_messages) == 1
    assert [message.to_dict() for message in cold_client.received_messages[0]] == [*expected, next_input.to_dict()]
    cold_ids = {message.message_id for message in _messages(cold_provider.raw)}
    assert removed.isdisjoint(cold_ids), "a cold flush or replayed transport input must not resurrect deleted payloads"
    for pair in atomic_pairs:
        assert pair <= cold_ids or pair.isdisjoint(cold_ids)
    assert cold_provider.raw["data"]["ingestedMessages"] == {
        **raw["data"]["ingestedMessages"],
        "next-input": cold_provider.raw["data"]["ingestedMessages"]["next-input"],
    }
    assert cold_provider.writes == 1
    final_removed = len(originals) + 2 - len(_messages(cold_provider.raw))
    assert (cold_provider.raw["data"].get("truncation") or {}).get("evictedMessageCount", 0) == final_removed
    assert _removed_counter(media_metrics) == final_removed


@pytest.mark.parametrize("kind", MEDIA_CASES)
async def test_newest_media_floor_cannot_be_deleted_to_make_a_commit_fit(kind: str) -> None:
    client = _MediaClient()
    agent: Any = _agent(client=client, name="protected-media")
    probe_provider = JsonStateProvider()
    probe = AgentEntity(agent, state_provider=probe_provider)
    request = _request("protected", _payload_messages(kind, "protected"))
    await probe.run(request)
    full_size = len(json.dumps(probe_provider.raw))
    # The same turn cannot fit at this budget unless its newest protected payload is deleted.
    budget = int(full_size * 0.8)
    provider = JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider, max_state_bytes=budget)
    before = entity.state.to_dict()

    with pytest.raises(StateCapacityError) as error:
        await entity.run(request)

    assert error.value.floor_bytes >= budget * 0.85
    assert len(client.received_messages) == 2, "the model succeeded before the commit was rejected"
    assert entity.state.to_dict() == before and provider.raw == {} and provider.writes == 0
    assert entity.state.try_get_agent_response("protected") is None
