# Copyright (c) Microsoft. All rights reserved.

"""Callback snapshots must not expose live Core or SDK raw graphs."""

from collections.abc import AsyncIterable, Awaitable, Iterator, Mapping, Sequence
from typing import Any

import pytest
from _execution_test_support import JsonStateProvider, ToolChatClient
from agent_framework import (
    Agent,
    AgentResponse,
    AgentResponseUpdate,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    Message,
    ResponseStream,
    tool,
)

from agent_framework_durabletask import AgentEntity
from agent_framework_durabletask._callbacks import AgentCallbackContext

_CORE_TYPES = (AgentResponse, AgentResponseUpdate, ChatResponse, ChatResponseUpdate, Message, Content)


class OpaqueSdkRaw:
    def __init__(self) -> None:
        self.payload = {"key": "original", "tags": ["sdk"]}
        self.copy_attempts = 0

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        self.copy_attempts += 1
        raise TypeError("SDK objects cannot be copied")


def _walk(value: Any, seen: set[int] | None = None) -> Iterator[Any]:
    """Walk callback-visible public and raw fields independently of the snapshot implementation."""
    if seen is None:
        seen = set()
    if id(value) in seen:
        return
    seen.add(id(value))
    yield value
    if isinstance(value, dict):
        children = [*value.keys(), *value.values()]
    elif isinstance(value, (list, tuple, set, frozenset)):
        children = list(value)
    elif isinstance(value, (*_CORE_TYPES, OpaqueSdkRaw)):
        children = list(vars(value).values())
    else:
        return
    for child in children:
        yield from _walk(child, seen)


def _mutate_every_reachable_payload(value: Any) -> None:
    for item in _walk(value):
        if isinstance(item, Content):
            if item.type == "function_call":
                item.arguments = '{"key":"changed"}'
            if item.type == "text":
                item.text = "changed"
        if isinstance(item, dict):
            if "key" in item:
                item["key"] = "changed"
            if isinstance(item.get("tags"), list):
                item["tags"].append("changed")


class RecordingCallback:
    def __init__(self, *, mutate: bool = False) -> None:
        self.mutate = mutate
        self.updates: list[AgentResponseUpdate] = []
        self.responses: list[AgentResponse] = []
        self.contexts: list[AgentCallbackContext] = []

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: AgentCallbackContext) -> None:
        self.updates.append(update)
        self.contexts.append(context)
        if self.mutate:
            _mutate_every_reachable_payload(update)

    async def on_agent_response(self, response: AgentResponse, context: AgentCallbackContext) -> None:
        self.responses.append(response)
        self.contexts.append(context)
        if self.mutate:
            _mutate_every_reachable_payload(response)


def _entity(callback: Any) -> AgentEntity:
    return AgentEntity(
        Agent(client=ToolChatClient(tool_calls=False)), callback=callback, state_provider=JsonStateProvider()
    )


@pytest.mark.parametrize("kind", ["stream", "final"])
@pytest.mark.parametrize("opaque", [False, True])
async def test_callback_omits_raw_at_every_core_depth_but_preserves_public_graph(kind: str, opaque: bool) -> None:
    sdk = OpaqueSdkRaw()
    live_call = Content.from_function_call(call_id="call-1", name="lookup", arguments={"key": "original"})
    raw_leaf = ChatResponseUpdate(contents=[live_call], raw_representation=sdk)
    raw_graph: dict[str, Any] = {"nested": [Message("assistant", [live_call], raw_representation=raw_leaf)]}
    raw_graph["cycle"] = raw_graph
    provider_raw = sdk if opaque else raw_graph

    content = Content.from_text(
        "original answer",
        annotations=[{"type": "citation", "url": "https://example.com", "title": "source"}],
        additional_properties={"tags": ["content"]},
        raw_representation=provider_raw,
    )
    nested_message = Message(
        "assistant", [content], additional_properties={"tags": ["message"]}, raw_representation=provider_raw
    )
    nested_update = ChatResponseUpdate(contents=[content], raw_representation=provider_raw)
    metadata: dict[str, Any] = {
        "tags": ["metadata"],
        "nested": (nested_message, nested_update),
        "response": ChatResponse(messages=[nested_message], raw_representation=provider_raw),
        "none": None,
        "false": False,
        "zero": 0,
    }
    metadata["cycle"] = metadata
    payload: AgentResponse[Any] | AgentResponseUpdate
    if kind == "stream":
        payload = AgentResponseUpdate(
            contents=[content],
            role="assistant",
            response_id="response-1",
            message_id="message-1",
            finish_reason="stop",
            additional_properties=metadata,
            raw_representation=provider_raw,
        )
    else:
        payload = AgentResponse[Any](
            messages=[nested_message],
            response_id="response-1",
            finish_reason="stop",
            usage_details={"input_token_count": 3, "output_token_count": 2},
            value={"key": "original", "nested": nested_update},
            additional_properties=metadata,
            raw_representation=provider_raw,
        )
    callback = RecordingCallback()
    entity = _entity(callback)
    context = AgentCallbackContext("agent", "correlation", "session", "question")

    snapshot: AgentResponse[Any] | AgentResponseUpdate
    if isinstance(payload, AgentResponseUpdate):
        await entity._notify_stream_update(payload, context)
        assert len(callback.updates) == 1 and callback.responses == []
        snapshot = callback.updates[0]
    else:
        await entity._notify_final_response(payload, context)
        assert len(callback.responses) == 1 and callback.updates == []
        snapshot = callback.responses[0]

    assert callback.contexts == [context] and callback.contexts[0] is context
    assert snapshot is not payload and type(snapshot) is type(payload)
    assert snapshot.response_id == "response-1" and snapshot.finish_reason == "stop"
    assert snapshot.text == "original answer"
    assert snapshot.raw_representation is None
    assert all(item.raw_representation is None for item in _walk(snapshot) if isinstance(item, _CORE_TYPES))
    copied_metadata = snapshot.additional_properties
    assert copied_metadata is not None
    assert copied_metadata["tags"] == ["metadata"]
    assert copied_metadata["none"] is None and copied_metadata["false"] is False and copied_metadata["zero"] == 0
    assert copied_metadata["cycle"]["cycle"] is copied_metadata["cycle"]
    copied_message, copied_update = copied_metadata["nested"]
    assert copied_message is copied_metadata["response"].messages[0]
    assert copied_message.contents[0] is copied_update.contents[0]
    assert copied_message.contents[0].annotations == content.annotations
    assert copied_message.contents[0].additional_properties == {"tags": ["content"]}
    if isinstance(payload, AgentResponseUpdate):
        assert isinstance(snapshot, AgentResponseUpdate)
        assert snapshot.message_id == "message-1"
        assert snapshot.contents[0] is copied_message.contents[0]
    else:
        assert isinstance(snapshot, AgentResponse)
        assert snapshot.messages[0] is copied_message
        assert snapshot.usage_details == {"input_token_count": 3, "output_token_count": 2}
        assert isinstance(snapshot.value, dict)
        assert snapshot.value["key"] == "original"
        assert snapshot.value["nested"] is copied_update

    _mutate_every_reachable_payload(snapshot)

    assert payload.text == "original answer"
    assert payload.raw_representation is provider_raw
    assert content.raw_representation is provider_raw and nested_message.raw_representation is provider_raw
    assert nested_update.raw_representation is provider_raw
    assert metadata["tags"] == ["metadata"]
    assert nested_message.additional_properties == {"tags": ["message"]}
    assert content.additional_properties == {"tags": ["content"]}
    assert live_call.arguments == {"key": "original"}
    assert sdk.payload == {"key": "original", "tags": ["sdk"]}
    assert sdk.copy_attempts == 0
    if isinstance(payload, AgentResponse):
        assert isinstance(payload.value, dict)
        assert payload.value["key"] == "original"


class RawToolClient(ToolChatClient):
    """Use Core's actual streaming function-invocation layer with a scripted model."""

    def __init__(self) -> None:
        super().__init__()
        self.model_calls = 0
        self.live_call = Content.from_function_call(call_id="call-1", name="lookup", arguments='{"key":"original"}')
        self.sdk = OpaqueSdkRaw()
        self.raw = ChatResponseUpdate(contents=[self.live_call], raw_representation=self.sdk)
        self.raw.raw_representation = {"sdk": self.sdk, "cycle": self.raw}
        self.tool_results: list[str] = []

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        assert stream is True
        self.model_calls += 1
        first = self.model_calls == 1
        self.tool_results.extend(
            content.result for message in messages for content in message.contents if content.type == "function_result"
        )

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            yield ChatResponseUpdate(
                role="assistant",
                contents=[self.live_call] if first else [Content.from_text("original answer")],
                response_id=f"response-{self.model_calls}",
                finish_reason="tool_calls" if first else "stop",
                additional_properties={"tags": ["model"]},
                raw_representation=self.raw,
            )

        return ResponseStream(updates(), finalizer=ChatResponse.from_updates)


async def test_stream_callback_cannot_change_actual_core_tool_arguments_or_delivered_response() -> None:
    invoked: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        invoked.append(key)
        return f"value:{key}"

    client = RawToolClient()
    callback = RecordingCallback(mutate=True)
    provider = JsonStateProvider()
    entity = AgentEntity(
        Agent(client=client, name="raw-isolation", tools=[lookup]), callback=callback, state_provider=provider
    )

    response = await entity.run({"message": "Use lookup", "correlationId": "raw-isolation"})

    assert invoked == ["original"]
    assert client.model_calls == 2
    assert client.tool_results == ["value:original"]
    assert client.live_call.arguments == '{"key":"original"}'
    assert response.text == "original answer"
    assert response.additional_properties.get("durable_status") != "error"
    assert response.additional_properties["tags"] == ["model"]
    assert len(callback.updates) == 3 and len(callback.responses) == 1
    assert callback.responses[0].text == "changed"
    assert client.sdk.copy_attempts == 0
    assert client.sdk.payload == {"key": "original", "tags": ["sdk"]}
    assert provider.successful_writes == 1
    stored = entity.state.try_get_agent_response("raw-isolation")
    assert stored is not None and stored.text == "original answer"
    assert callback.updates[0].additional_properties == {"tags": ["model", "changed"]}


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_callback_exceptions_are_logged_once_per_notification_without_escaping(
    asynchronous: bool, caplog: pytest.LogCaptureFixture
) -> None:
    attempts: list[str] = []

    def fail(kind: str, *_: Any) -> None:
        attempts.append(kind)
        raise RuntimeError("callback failed")

    async def fail_async(kind: str, *args: Any) -> None:
        fail(kind, *args)

    class Callback:
        def on_streaming_response_update(self, *args: Any) -> Any:
            return fail_async("stream", *args) if asynchronous else fail("stream", *args)

        def on_agent_response(self, *args: Any) -> Any:
            return fail_async("final", *args) if asynchronous else fail("final", *args)

    entity = _entity(Callback())
    context = AgentCallbackContext("agent", "correlation")
    await entity._notify_stream_update(AgentResponseUpdate(contents=[Content.from_text("update")]), context)
    await entity._notify_final_response(AgentResponse(messages=[Message("assistant", ["answer"])]), context)

    assert attempts == ["stream", "final"]
    assert caplog.text.count("Streaming callback raised an exception: callback failed") == 1
    assert caplog.text.count("Response callback raised an exception: callback failed") == 1


@pytest.mark.parametrize("missing", ["callback", "context"])
async def test_missing_callback_or_context_does_not_attempt_a_snapshot(missing: str) -> None:
    callback = RecordingCallback()
    entity = _entity(None if missing == "callback" else callback)
    context = None if missing == "context" else AgentCallbackContext("agent", "correlation")
    uncopiable = OpaqueSdkRaw()
    await entity._notify_stream_update(AgentResponseUpdate(additional_properties={"value": uncopiable}), context)
    await entity._notify_final_response(AgentResponse(additional_properties={"value": uncopiable}), context)
    assert uncopiable.copy_attempts == 0
    assert callback.updates == [] and callback.responses == []
