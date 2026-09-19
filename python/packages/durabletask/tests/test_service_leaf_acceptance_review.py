# Copyright (c) Microsoft. All rights reserved.

"""Exact Core acceptance and opaque-client completion, without network calls."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from types import MethodType
from typing import Any

import pytest
from agent_framework import (
    Agent,
    BaseChatClient,
    ChatContext,
    ChatMiddleware,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    FunctionInvocationLayer,
    HistoryProvider,
    Message,
    ResponseStream,
)
from agent_framework.observability import ChatTelemetryLayer
from test_invocation_safety import ToolChatClient, _DelegatingClient, lookup

from agent_framework_durabletask._invocation_safety import (
    DurableServiceAcceptance,
    DurableServiceClient,
    DurableToolGuard,
    InvocationProgress,
)


class _Record:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.received: list[list[Message]] = []
        self.instances: list[Any] = []
        self.streams: list[ResponseStream[ChatResponseUpdate, ChatResponse]] = []
        self.completed: list[ChatResponse] = []
        self.accepted: list[list[Message]] = []
        self.progress = InvocationProgress()

    def accept(self, messages: Sequence[Message]) -> None:
        self.events.append("accept")
        self.accepted.append(list(messages))

    def on_completed(self, response: ChatResponse) -> None:
        self.events.append("completed")
        self.completed.append(response)
        self.progress.service_completed = True


class _LeafClient(BaseChatClient):
    def __init__(
        self,
        record: _Record,
        *,
        deferred_stream: bool = False,
        failure: str | None = None,
        mutate: bool = False,
        **kwargs: Any,
    ) -> None:
        record.events.append("constructor")
        super().__init__(**kwargs)
        self.record = record
        self.deferred_stream = deferred_stream
        self.failure = failure
        self.mutate = mutate
        self.local_calls = 0
        self.client = _OpaqueSdk()

    def __copy__(self) -> Any:
        raise AssertionError("must not invoke caller copy hooks")

    def __deepcopy__(self, memo: Any) -> Any:
        raise AssertionError("must not deep-copy caller resources")

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.local_calls += 1
        self.record.instances.append(self)
        self.record.events.append("leaf")
        self.record.received.append(deepcopy(list(messages)))
        if self.failure == "dispatch":
            raise RuntimeError("dispatch failed")
        if self.mutate:
            messages[0].contents[0].text = "provider-mutated"
            messages[0].additional_properties["nested"]["values"].append("provider-mutated")
        # The real OpenAI retrieval path removes a settled token from this same
        # options dictionary. Acceptance must use the value from before the call.
        if options.get("continuation_token") is not None and isinstance(options, dict):
            options.pop("continuation_token")

        response = ChatResponse(
            messages=[Message("assistant", ["answer"])],
            response_id="response-final",
            conversation_id="conversation-final",
        )
        if not stream:

            async def get() -> ChatResponse:
                if self.failure == "await":
                    raise RuntimeError("await failed")
                return response

            return get()

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            self.record.events.append("pull")
            # IDs intentionally exist only in the provider's finalizer.
            yield ChatResponseUpdate(role="assistant", contents=[Content.from_text("answer")])
            if self.failure == "stream":
                raise RuntimeError("stream failed")

        def finalize(updates: Sequence[ChatResponseUpdate]) -> ChatResponse:
            self.record.events.append("finalizer")
            assert len(updates) == 1
            if self.failure == "finalizer":
                raise RuntimeError("finalizer failed")
            return response

        def provider_result(value: ChatResponse) -> ChatResponse:
            self.record.events.append("provider-result")
            assert value is response
            return value

        result = ResponseStream(
            updates(),
            finalizer=finalize,
            cleanup_hooks=[lambda: self.record.events.append("cleanup")],
            result_hooks=[provider_result],
        )
        result.with_transform_hook(lambda update: self._transform(update))
        self.record.streams.append(result)
        if self.deferred_stream:

            async def resolve() -> Any:
                self.record.events.append("resolve-stream")
                return result

            return resolve()
        return result

    def _transform(self, update: ChatResponseUpdate) -> ChatResponseUpdate:
        self.record.events.append("transform")
        return update


class _OpaqueSdk:
    @property
    def inner(self) -> Any:
        raise AssertionError("must not inspect SDK delegation")

    @property
    def __wrapped__(self) -> Any:
        raise AssertionError("must not inspect SDK delegation")

    def get_response(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("must not call the SDK to check completion")


class _ProtocolClient:
    """Standalone protocol client with its own filtering, state, and finalization."""

    def __init__(self, record: _Record, *, deferred_stream: bool = False, failure: str | None = None) -> None:
        self.record = record
        self.deferred_stream = deferred_stream
        self.failure = failure
        self.error = RuntimeError(f"{failure} failed")
        self.local_calls = 0
        self.additional_properties = {"opaque": True}
        self.function_invocation_configuration = {"enabled": False}
        self.arguments: list[tuple[Sequence[Message], dict[str, Any]]] = []
        self.responses: list[ChatResponse] = []

    def get_response(self, messages: Sequence[Message], *, stream: bool = False, **kwargs: Any) -> Any:
        self.local_calls += 1
        self.record.instances.append(self)
        self.record.events.append("opaque-call")
        self.arguments.append((messages, kwargs))
        # No BaseChatClient implementation participates in this projection.
        self.record.received.append([message for message in messages if message.message_id != "A"])
        if self.failure == "dispatch":
            raise self.error
        response = ChatResponse(
            messages=[Message("assistant", [f"answer-{self.local_calls}"])],
            conversation_id=f"S{self.local_calls}",
            response_id=f"R{self.local_calls}",
        )
        self.responses.append(response)
        if not stream:

            async def get() -> ChatResponse:
                self.record.events.append("opaque-await")
                if self.failure == "await":
                    raise self.error
                return response

            return get()

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            self.record.events.append("opaque-pull")
            yield ChatResponseUpdate(role="assistant", contents=[Content.from_text(response.text)])
            if self.failure == "stream":
                raise self.error

        def transform(update: ChatResponseUpdate) -> ChatResponseUpdate:
            self.record.events.append("opaque-transform")
            return update

        def finalize(updates: Sequence[ChatResponseUpdate]) -> ChatResponse:
            self.record.events.append("opaque-finalizer")
            assert len(updates) == 1
            if self.failure == "finalizer":
                raise self.error
            return response

        def result_hook(value: ChatResponse) -> ChatResponse:
            self.record.events.append("opaque-result")
            if self.failure == "result-hook":
                raise self.error
            return value

        result = ResponseStream(
            updates(),
            finalizer=finalize,
            transform_hooks=[transform],
            cleanup_hooks=[lambda: self.record.events.append("opaque-cleanup")],
            result_hooks=[result_hook],
        )
        self.record.streams.append(result)
        if self.deferred_stream:

            async def resolve() -> ResponseStream[ChatResponseUpdate, ChatResponse]:
                self.record.events.append("opaque-resolve")
                if self.failure == "resolve":
                    raise self.error
                return result

            return resolve()
        return result


class _LayeredClient(FunctionInvocationLayer, ChatMiddlewareLayer, ChatTelemetryLayer, _LeafClient):
    def __init__(self, record: _Record, **kwargs: Any) -> None:
        super().__init__(record=record, **kwargs)


class _Audit(ChatMiddleware):
    def __init__(self, record: _Record, *, fail: bool = False, short_circuit: bool = False) -> None:
        self.record = record
        self.fail = fail
        self.short_circuit = short_circuit

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.record.events.append("audit-before")
        if self.short_circuit:
            response = ChatResponse(messages=[Message("assistant", ["cached"])])
            if context.stream:

                async def cached() -> AsyncIterable[ChatResponseUpdate]:
                    yield ChatResponseUpdate(role="assistant", contents=[Content.from_text("cached")])

                context.result = ResponseStream(cached(), finalizer=lambda _: response)
            else:
                context.result = response
            return
        await call_next()

        def audit(response: ChatResponse) -> ChatResponse:
            self.record.events.append("audit-after")
            assert self.record.progress.service_completed
            assert self.record.completed[-1].conversation_id == "conversation-final"
            if self.fail:
                raise RuntimeError("audit failed after provider completion")
            return response

        if isinstance(context.result, ResponseStream):
            context.result.with_result_hook(audit)
        else:
            assert isinstance(context.result, ChatResponse)
            audit(context.result)


async def _exclude_a(messages: list[Message]) -> bool:
    # Core's real apply_compaction annotates and projects this list after chat
    # middleware has captured its context. A must never appear in acceptance.
    for message in messages:
        if message.message_id == "A":
            message.additional_properties["_excluded"] = True
    return True


def _inputs() -> list[Message]:
    return [
        Message("user", ["A"], message_id="A"),
        Message("user", ["B"], message_id="B", additional_properties={"nested": {"values": ["original"]}}),
    ]


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("layered", [False, True])
async def test_acceptance_matches_actual_core_compaction_leaf(stream: bool, layered: bool) -> None:
    record = _Record()
    client_type = _LayeredClient if layered else _LeafClient
    original = client_type(record)
    before = dict(vars(original))
    client = DurableServiceClient(original, record.accept, record.on_completed)
    assert client.exact_acceptance is True

    result = client.get_response(_inputs(), stream=stream, compaction_strategy=_exclude_a)
    response = await result.get_final_response() if stream else await result

    assert response.text == "answer"
    assert len(record.completed) == 1
    assert record.completed[0].conversation_id == "conversation-final"
    assert record.completed[0].response_id == "response-final"
    assert [[message.text for message in batch] for batch in record.received] == [["B"]]
    assert [[message.to_dict() for message in batch] for batch in record.accepted] == [
        [message.to_dict() for message in batch] for batch in record.received
    ]
    # The function loop may aggregate updates into a different response. The
    # callback must retain the provider's final-only IDs, not that aggregation.
    if not (layered and stream):
        assert record.completed[0] is response
    assert record.progress.service_completed
    assert original.local_calls == 0
    assert vars(original) == before
    assert len(record.instances) == 1
    assert record.instances[0] is not original
    assert record.instances[0].client is original.client
    assert record.events.count("constructor") == 1
    assert "_inner_get_response" not in vars(original)


@pytest.mark.parametrize("stream", [False, True])
async def test_completed_leaf_is_observed_before_outer_audit_failure(stream: bool) -> None:
    record = _Record()
    original = _LayeredClient(record, middleware=[_Audit(record, fail=True)])
    client = DurableServiceClient(original, record.accept, record.on_completed)

    with pytest.raises(RuntimeError, match="audit failed"):
        result = client.get_response(_inputs(), stream=stream, compaction_strategy=_exclude_a)
        if stream:
            await result.get_final_response()
        else:
            await result

    assert [[message.text for message in batch] for batch in record.accepted] == [["B"]]
    assert [response.conversation_id for response in record.completed] == ["conversation-final"]
    assert record.progress.service_completed
    assert record.events.index("leaf") < record.events.index("accept") < record.events.index("completed")
    assert record.events.index("completed") < record.events.index("audit-after")
    assert record.events.count("leaf") == record.events.count("accept") == record.events.count("completed") == 1
    if stream:
        assert record.events.index("finalizer") < record.events.index("completed")
        assert all(update.conversation_id is None for update in record.streams[0].updates)
    assert original._cached_chat_middleware_pipeline is None


@pytest.mark.parametrize("deferred", [False, True])
async def test_stream_is_not_drained_and_preserves_original_finalizer_and_hooks(deferred: bool) -> None:
    record = _Record()
    client = DurableServiceClient(_LeafClient(record, deferred_stream=deferred), record.accept, record.on_completed)
    stream = client.get_response(_inputs(), stream=True)
    assert isinstance(stream, ResponseStream)
    assert record.events == ["constructor", "leaf"]
    assert not record.progress.service_completed
    if not deferred:
        assert stream is record.streams[0]

    await stream  # Core awaits stream setup, but this must not drain updates.
    assert "pull" not in record.events
    update = await anext(stream)
    assert update.response_id is None
    assert record.accepted == record.completed == []
    response = await stream.get_final_response()
    assert await stream.get_final_response() is response
    assert record.completed == [response]
    assert response.response_id == "response-final"
    assert response.conversation_id == "conversation-final"
    for event in ("pull", "transform", "cleanup", "finalizer", "provider-result", "accept", "completed"):
        assert record.events.count(event) == 1
    assert record.events.index("provider-result") < record.events.index("accept")


@pytest.mark.parametrize("stream", [False, True])
async def test_prepared_inputs_are_detached_before_provider_mutation(stream: bool) -> None:
    record = _Record()
    client = DurableServiceClient(_LeafClient(record, mutate=True), record.accept, record.on_completed)
    result = client.get_response(_inputs(), stream=stream, compaction_strategy=_exclude_a)
    if stream:
        await result.get_final_response()
    else:
        await result
    assert [message.text for message in record.accepted[0]] == ["B"]
    assert record.accepted[0][0].additional_properties["nested"] == {"values": ["original"]}


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("token", [None, {}, {"response_id": "previous"}])
async def test_retrieval_completion_does_not_accept_new_inputs(stream: bool, token: Any) -> None:
    record = _Record()
    client = DurableServiceClient(_LeafClient(record), record.accept, record.on_completed)
    options = {"continuation_token": token}
    result = client.get_response(_inputs(), stream=stream, options=options)
    response = await result.get_final_response() if stream else await result
    assert len(record.received) == 1  # No completion query or additional provider call.
    assert record.completed == [response]
    assert record.progress.service_completed
    assert len(record.accepted) == (1 if token is None else 0)


@pytest.mark.parametrize("stream", [False, True])
async def test_short_circuit_does_not_claim_a_service_completion(stream: bool) -> None:
    record = _Record()
    original = _LayeredClient(record, middleware=[_Audit(record, short_circuit=True)])
    client = DurableServiceClient(original, record.accept, record.on_completed)
    result = client.get_response(_inputs(), stream=stream)
    response = await result.get_final_response() if stream else await result
    assert response.text == "cached"
    assert record.received == record.accepted == record.completed == []
    assert not record.progress.service_completed


@pytest.mark.parametrize(
    ("stream", "failure"),
    [(False, "dispatch"), (False, "await"), (True, "dispatch"), (True, "stream"), (True, "finalizer")],
)
async def test_failed_or_unfinished_leaf_does_not_claim_acceptance(stream: bool, failure: str) -> None:
    record = _Record()
    client = DurableServiceClient(_LeafClient(record, failure=failure), record.accept, record.on_completed)
    with pytest.raises(RuntimeError, match=f"{failure} failed"):
        result = client.get_response(_inputs(), stream=stream)
        if stream:
            await result.get_final_response()
        else:
            await result
    assert record.accepted == record.completed == []
    assert not record.progress.service_completed


class _WrappedClient:
    def __init__(self, client: Any) -> None:
        self.__wrapped__ = client
        self.forwarded: list[Any] = []
        self.local_calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__wrapped__, name)

    def get_response(self, messages: Sequence[Message], **kwargs: Any) -> Any:
        self.local_calls += 1
        self.forwarded.append(self)
        return self.__wrapped__.get_response(messages=messages, **kwargs)


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
async def test_declared_wrapper_chain_retains_real_function_loop_and_guard(stream: bool, enabled: bool) -> None:
    record = _Record()
    inner = ToolChatClient()
    delegated = _DelegatingClient(inner)
    outer = _WrappedClient(delegated)
    configuration = deepcopy(inner.function_invocation_configuration)
    before = dict(vars(inner))
    client = DurableServiceClient(outer, record.accept, record.on_completed)
    agent: Any = Agent(client=client, tools=[lookup])
    result = agent.run(
        "question",
        stream=stream,
        client_kwargs={"middleware": [DurableToolGuard(record.progress, enabled=enabled)]},
    )
    response = await result.get_final_response() if stream else await result

    assert response.text == "answer-2"
    assert len(record.accepted) == len(record.completed) == len(inner.received_messages) == 2
    assert [[message.to_dict() for message in batch] for batch in record.accepted] == [
        [message.to_dict() for message in batch] for batch in inner.received_messages
    ]
    assert record.progress.function_started is enabled
    assert record.progress.service_completed
    assert len(outer.forwarded) == len(delegated.forwarded) == 1
    assert outer.forwarded[0] is not outer
    assert outer.forwarded[0].__wrapped__ is not delegated
    assert outer.local_calls == 0
    assert outer.__wrapped__ is delegated
    assert delegated.inner is inner
    assert inner.function_invocation_configuration == configuration
    assert delegated.inner_configurations == [configuration]
    assert vars(inner) == before
    tool_results = [
        content.result
        for message in record.accepted[1]
        for content in message.contents
        if content.type == "function_result"
    ]
    assert tool_results == ["value:durable" if enabled else "Tool execution is disabled for this invocation."]


@pytest.mark.parametrize("stream", [False, True])
async def test_instance_bound_entry_and_leaf_overrides_rebind_to_clone(stream: bool) -> None:
    record = _Record()
    original: Any = _LayeredClient(record)
    wrapper: Any = _WrappedClient(original)

    def entry(self: Any, messages: Sequence[Message], **kwargs: Any) -> Any:
        self.record.events.append("entry-override")
        self.local_calls += 10
        return _LayeredClient.get_response(self, messages=messages, **kwargs)

    def leaf(self: Any, **kwargs: Any) -> Any:
        self.record.events.append("leaf-override")
        self.local_calls += 100
        return _LeafClient._inner_get_response(self, **kwargs)

    def delegated_entry(self: Any, messages: Sequence[Message], **kwargs: Any) -> Any:
        self.local_calls += 10
        return _WrappedClient.get_response(self, messages=messages, **kwargs)

    original.get_response = MethodType(entry, original)
    original._inner_get_response = MethodType(leaf, original)
    wrapper.get_response = MethodType(delegated_entry, wrapper)
    original_entry = original.get_response
    original_leaf = original._inner_get_response
    wrapper_entry = wrapper.get_response
    client = DurableServiceClient(wrapper, record.accept, record.on_completed)
    result = client.get_response(_inputs(), stream=stream, compaction_strategy=_exclude_a)
    if stream:
        await result.get_final_response()
    else:
        await result
    assert record.events.count("entry-override") == record.events.count("leaf-override") == 1
    assert record.instances[0].local_calls == 111
    assert wrapper.forwarded[0].local_calls == 11
    assert original.local_calls == wrapper.local_calls == 0
    assert original.get_response is original_entry
    assert original._inner_get_response is original_leaf
    assert wrapper.get_response is wrapper_entry
    assert [[message.text for message in batch] for batch in record.accepted] == [["B"]]


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("kind", ["opaque", "sdk-only", "computed", "ambiguous", "closure", "leaf-closure"])
async def test_unsupported_layouts_execute_original_without_acceptance(kind: str, stream: bool) -> None:
    record = _Record()
    leaf: Any = _LeafClient(record)

    class Computed(_ProtocolClient):
        @property
        def inner(self) -> Any:
            raise AssertionError("must not resolve computed delegation")

    original: Any
    if kind == "computed":
        original = Computed(record)
    elif kind in {"closure", "leaf-closure"}:
        original = leaf
        name = "get_response" if kind == "closure" else "_inner_get_response"
        bound_method = getattr(original, name)
        setattr(original, name, lambda *args, **kwargs: bound_method(*args, **kwargs))
    else:
        original = _ProtocolClient(record)
        if kind == "sdk-only":
            original.client = _OpaqueSdk()
        elif kind == "ambiguous":
            original.inner = leaf
            original.__wrapped__ = _LeafClient(record)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    assert client.exact_acceptance is False
    assert record.received == record.accepted == record.completed == []
    result = client.get_response(_inputs(), stream=stream)
    response = await result.get_final_response() if stream else await result
    assert record.completed == [response]
    assert record.accepted == []
    assert record.instances == [original]
    assert original.local_calls == 1


@pytest.mark.parametrize("kind", ["missing", "no-method", "non-callable", "cycle", "indirect-cycle"])
def test_invalid_clients_and_detected_cycles_fail_before_dispatch(kind: str) -> None:
    record = _Record()
    original: Any = _ProtocolClient(record)
    match = "callable get_response"
    if kind == "missing":
        original = None
    elif kind == "no-method":
        original = object()
    elif kind == "non-callable":
        original.get_response = None
    else:
        match = "delegation chain contains a cycle"
        if kind == "cycle":
            original.inner = original
        else:
            other: Any = _ProtocolClient(record)
            original.inner = other
            other.__wrapped__ = original
    with pytest.raises(ValueError, match=match):
        DurableServiceClient(original, record.accept, record.on_completed)
    assert record.instances == record.received == record.accepted == record.completed == []


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("deferred", [False, True])
async def test_generic_protocol_preserves_original_state_options_and_outer_completion(
    stream: bool, deferred: bool
) -> None:
    record = _Record()
    original = _ProtocolClient(record, deferred_stream=deferred)
    inputs = _inputs()
    options = {"conversation_id": "previous", "custom": object()}
    client_kwargs = {"opaque_option": object()}

    async def invoke(call: int) -> None:
        # A new run-local observer must not reset the original's scalar state.
        returned = False

        def completed(response: ChatResponse) -> None:
            assert not returned
            assert response is original.responses[call - 1]
            assert response.conversation_id == f"S{call}"
            record.on_completed(response)

        client = DurableServiceClient(original, record.accept, completed)
        assert client.exact_acceptance is False
        assert client.local_calls == call - 1
        assert client.additional_properties is original.additional_properties
        assert client.function_invocation_configuration is original.function_invocation_configuration
        result = client.get_response(inputs, stream=stream, options=options, client_kwargs=client_kwargs)
        assert len(record.completed) == call - 1
        response = await result.get_final_response() if stream else await result
        returned = True
        assert response is original.responses[call - 1]
        assert response.text == f"answer-{call}"
        assert record.completed[-1] is response
        assert client.local_calls == original.local_calls == call
        assert original.arguments[-1][0] is inputs
        assert original.arguments[-1][1] == {"options": options, "client_kwargs": client_kwargs}
        assert original.arguments[-1][1]["options"] is options
        assert original.arguments[-1][1]["client_kwargs"] is client_kwargs
        if stream:
            assert await result.get_final_response() is response

    for call in (1, 2):
        await invoke(call)

    assert record.instances == [original, original]
    assert [response.conversation_id for response in record.completed] == ["S1", "S2"]
    assert [[message.text for message in batch] for batch in record.received] == [["B"], ["B"]]
    assert record.accepted == []
    assert record.events.count("completed") == record.events.count("opaque-call") == 2


@pytest.mark.parametrize("deferred", [False, True])
async def test_generic_stream_preserves_hooks_without_early_or_duplicate_completion(deferred: bool) -> None:
    record = _Record()
    original = _ProtocolClient(record, deferred_stream=deferred)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    stream = client.get_response(_inputs(), stream=True)
    assert isinstance(stream, ResponseStream)
    assert record.events == ["opaque-call"]
    if not deferred:
        assert stream is record.streams[0]
    assert await stream is stream
    assert "opaque-pull" not in record.events
    update = await anext(stream)
    assert update.conversation_id is None
    assert record.accepted == record.completed == []
    response = await stream.get_final_response()
    assert await stream.get_final_response() is response
    assert response is original.responses[0]
    assert response.conversation_id == "S1"
    assert record.completed == [response]
    assert record.accepted == []
    for event in (
        "opaque-call",
        "opaque-pull",
        "opaque-transform",
        "opaque-cleanup",
        "opaque-finalizer",
        "opaque-result",
    ):
        assert record.events.count(event) == 1
    assert record.events.count("completed") == 1
    assert record.events.index("opaque-result") < record.events.index("completed")


@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("fail_outer", [False, True])
async def test_generic_completion_precedes_outer_result_finalization(deferred: bool, fail_outer: bool) -> None:
    record = _Record()
    original = _ProtocolClient(record, deferred_stream=deferred)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    stream = client.get_response(_inputs(), stream=True)

    def finalize_outer(updates: Sequence[ChatResponseUpdate]) -> ChatResponse:
        assert len(updates) == 1
        assert record.completed == [original.responses[0]]
        assert record.completed[0].conversation_id == "S1"
        assert record.accepted == []
        record.events.append("outer-finalizer")
        if fail_outer:
            raise RuntimeError("outer finalization failed")
        return ChatResponse(messages=[Message("assistant", ["outer answer"])])

    outer = stream.map(lambda update: update, finalize_outer)
    if fail_outer:
        with pytest.raises(RuntimeError, match="outer finalization failed"):
            await outer.get_final_response()
    else:
        assert (await outer.get_final_response()).text == "outer answer"
    assert record.events.index("completed") < record.events.index("outer-finalizer")
    assert record.events.count("completed") == original.local_calls == 1


@pytest.mark.parametrize(
    ("stream", "deferred", "failure"),
    [
        (False, False, "dispatch"),
        (False, False, "await"),
        (True, False, "dispatch"),
        (True, True, "resolve"),
        (True, False, "stream"),
        (True, True, "stream"),
        (True, False, "finalizer"),
        (True, True, "finalizer"),
        (True, False, "result-hook"),
        (True, True, "result-hook"),
    ],
)
async def test_generic_failure_propagates_without_receipt_completion_or_rerun(
    stream: bool, deferred: bool, failure: str
) -> None:
    record = _Record()
    original = _ProtocolClient(record, deferred_stream=deferred, failure=failure)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    with pytest.raises(RuntimeError) as error:
        result = client.get_response(_inputs(), stream=stream)
        if stream:
            await result.get_final_response()
        else:
            await result
    assert error.value is original.error
    assert client.exact_acceptance is False
    assert original.local_calls == 1
    assert record.accepted == record.completed == []
    assert not record.progress.service_completed


@pytest.mark.parametrize("stream", [False, True])
async def test_generic_completion_callback_is_optional(stream: bool) -> None:
    record = _Record()
    original = _ProtocolClient(record)
    client = DurableServiceClient(original, record.accept)
    result = client.get_response(_inputs(), stream=stream)
    response = await result.get_final_response() if stream else await result
    assert response is original.responses[0]
    assert record.accepted == record.completed == []
    assert original.local_calls == 1


@pytest.mark.parametrize("deferred", [False, True])
async def test_generic_partial_stream_does_not_claim_completion(deferred: bool) -> None:
    record = _Record()
    original = _ProtocolClient(record, deferred_stream=deferred)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    stream = client.get_response(_inputs(), stream=True)
    await anext(stream)
    producer: Any = record.streams[0]._stream_source
    await producer.aclose()
    assert record.accepted == record.completed == []
    assert not record.progress.service_completed
    assert "opaque-finalizer" not in record.events
    assert original.local_calls == 1


@pytest.mark.parametrize("stream", [False, True])
async def test_async_protocol_entry_executes_once_and_preserves_its_result(stream: bool) -> None:
    record = _Record()

    class AsyncProtocol(_ProtocolClient):
        async def get_response(self, messages: Sequence[Message], *, stream: bool = False, **kwargs: Any) -> Any:
            result = super().get_response(messages, stream=stream, **kwargs)
            return result if stream else await result

    original = AsyncProtocol(record)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    result = client.get_response(_inputs(), stream=stream)
    assert original.local_calls == 0
    response = await result.get_final_response() if stream else await result
    assert response is original.responses[0]
    assert original.local_calls == 1
    assert record.completed == [response]
    assert record.accepted == []
    assert client.exact_acceptance is False


@pytest.mark.parametrize("deferred", [False, True])
async def test_generic_invalid_stream_shape_does_not_fake_completion(deferred: bool) -> None:
    record = _Record()

    class InvalidStream(_ProtocolClient):
        def get_response(self, messages: Sequence[Message], **kwargs: Any) -> Any:
            self.local_calls += 1
            response = ChatResponse(messages=[Message("assistant", ["not a stream"])])
            if deferred:

                async def resolve() -> ChatResponse:
                    return response

                return resolve()
            return response

    original = InvalidStream(record)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    with pytest.raises(ValueError, match="requires a ResponseStream"):
        await client.get_response(_inputs(), stream=True).get_final_response()
    assert record.accepted == record.completed == []
    assert original.local_calls == 1


async def test_generic_callback_failure_is_once_only_and_does_not_rerun_original() -> None:
    record = _Record()
    original = _ProtocolClient(record)
    failure = RuntimeError("completion callback failed")

    def completed(response: ChatResponse) -> None:
        record.on_completed(response)
        raise failure

    client = DurableServiceClient(original, record.accept, completed)
    stream = client.get_response(_inputs(), stream=True)
    with pytest.raises(RuntimeError) as error:
        await stream.get_final_response()
    assert error.value is failure
    # Core may revisit finalization while unwinding. Do not call the callback twice.
    assert await stream.get_final_response() is original.responses[0]
    assert record.completed == original.responses
    assert record.accepted == []
    assert original.local_calls == 1


def test_unrelated_clone_value_error_is_not_misclassified_as_unsupported() -> None:
    record = _Record()
    failure = ValueError("client reconstruction failed")

    class FailingClone(_LeafClient):
        @property
        def clone_guard(self) -> Any:
            return self.__dict__["clone_guard"]

        @clone_guard.setter
        def clone_guard(self, value: Any) -> None:
            raise failure

    original = FailingClone(record)
    vars(original)["clone_guard"] = True
    with pytest.raises(ValueError) as error:
        DurableServiceClient(original, record.accept, record.on_completed)
    assert error.value is failure
    assert record.received == record.accepted == record.completed == []


def test_original_entry_lookup_error_is_not_masked_as_an_invalid_client() -> None:
    record = _Record()
    failure = ValueError("client entry lookup failed")

    class BrokenLookup:
        @property
        def get_response(self) -> Any:
            raise failure

    with pytest.raises(ValueError) as error:
        DurableServiceClient(BrokenLookup(), record.accept, record.on_completed)
    assert error.value is failure
    assert record.received == record.accepted == record.completed == []


@pytest.mark.parametrize("stream", [False, True])
async def test_direct_acceptance_middleware_keeps_compatibility_semantics(stream: bool) -> None:
    record = _Record()
    original: Any = _LayeredClient(record, middleware=[DurableServiceAcceptance(record.accept)])
    result = original.get_response(_inputs(), stream=stream)
    if stream:
        await result.get_final_response()
    else:
        await result
    assert [[message.text for message in batch] for batch in record.accepted] == [["A", "B"]]


async def test_completion_flag_is_still_set_when_acceptance_callback_fails() -> None:
    record = _Record()

    def fail_accept(messages: Sequence[Message]) -> None:
        raise RuntimeError("acceptance persistence failed")

    client = DurableServiceClient(_LeafClient(record), fail_accept, record.on_completed)
    with pytest.raises(RuntimeError, match="acceptance persistence failed"):
        await client.get_response(_inputs())
    assert record.progress.service_completed
    assert len(record.received) == len(record.completed) == 1


class _HistoryAudit(HistoryProvider):
    def __init__(self, record: _Record, *, fail: bool) -> None:
        super().__init__("audit", load_messages=False)
        self.record = record
        self.fail = fail

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        raise AssertionError("store-only audit must not query history")

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.record.events.append("history-audit")
        assert self.record.progress.service_completed
        assert [message.text for message in self.record.accepted[0]] == ["B"]
        assert self.record.completed[0].conversation_id == "conversation-final"
        if self.fail:
            raise RuntimeError("history audit failed")


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("fail_audit", [False, True])
async def test_real_per_call_history_persistence_runs_after_leaf_observer(stream: bool, fail_audit: bool) -> None:
    record = _Record()
    original = _LayeredClient(record)
    agent: Any = Agent(
        client=DurableServiceClient(original, record.accept, record.on_completed),
        default_options={"store": True},
        context_providers=[_HistoryAudit(record, fail=fail_audit)],
        require_per_service_call_history_persistence=True,
        compaction_strategy=_exclude_a,
    )

    async def run() -> Any:
        result = agent.run(_inputs(), stream=stream, session=agent.create_session(session_id="leaf-review"))
        return await result.get_final_response() if stream else await result

    if fail_audit:
        with pytest.raises(RuntimeError, match="history audit failed"):
            await run()
    else:
        assert (await run()).text == "answer"
    assert len(record.received) == len(record.accepted) == len(record.completed) == 1
    assert record.events.index("completed") < record.events.index("history-audit")
    # Core can revisit a failed streaming finalization hook while unwinding its
    # outer stream. Durable observation must remain once-only in either case.
    assert record.events.count("history-audit") == (2 if stream and fail_audit else 1)
    assert original.local_calls == 0


async def test_slotted_delegation_and_constructor_free_allocation() -> None:
    record = _Record()

    class SlottedWrapper:
        __slots__ = ("inner",)

        def __new__(cls, inner: Any) -> Any:
            record.events.append("wrapper-new")
            return super().__new__(cls)

        def __init__(self, inner: Any) -> None:
            record.events.append("wrapper-init")
            self.inner = inner

        def get_response(self, messages: Sequence[Message], **kwargs: Any) -> Any:
            return self.inner.get_response(messages=messages, **kwargs)

    original = _LeafClient(record)
    wrapper = SlottedWrapper(original)
    client = DurableServiceClient(wrapper, record.accept, record.on_completed)
    response = await client.get_response(_inputs())
    assert response.text == "answer"
    assert record.events.count("wrapper-new") == record.events.count("wrapper-init") == 1
    assert wrapper.inner is original
    assert record.instances[0] is not original
    assert original.local_calls == 0


async def test_two_run_local_clients_keep_observers_and_scalar_state_separate() -> None:
    record = _Record()
    first = _Record()
    second = _Record()
    original = _LeafClient(record)
    clients = [
        DurableServiceClient(original, first.accept, first.on_completed),
        DurableServiceClient(original, second.accept, second.on_completed),
    ]
    streams = [
        client.get_response([Message("user", [text])], stream=True) for client, text in zip(clients, ["one", "two"])
    ]
    await anext(streams[0])
    await streams[1].get_final_response()
    assert first.accepted == first.completed == []
    assert [[message.text for message in batch] for batch in second.accepted] == [["two"]]
    await streams[0].get_final_response()
    assert [[message.text for message in batch] for batch in first.accepted] == [["one"]]
    assert record.instances[0] is not record.instances[1]
    assert [instance.local_calls for instance in record.instances] == [1, 1]
    assert original.local_calls == 0


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("callback_kind", ["bound-method", "instance-function"])
async def test_exact_receipt_capability_does_not_isolate_cached_middleware_callbacks(
    stream: bool, callback_kind: str
) -> None:
    # Review control for the existing clone, not a full compatibility guarantee.
    # Cached pipelines and referenced callback containers are still borrowed.
    record = _Record()
    original: Any = _LayeredClient(record)
    callback_instances: list[Any] = []

    async def audit(self: Any, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        callback_instances.append(self)
        self.local_calls += 10
        await call_next()

    if callback_kind == "bound-method":
        original.audit = MethodType(audit, original)
    else:

        async def instance_audit(context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
            await audit(original, context, call_next)

        original.audit = instance_audit
    original.chat_middleware = [original.audit]
    await original.get_response(_inputs())
    cached = original._cached_chat_middleware_pipeline
    assert cached is not None
    assert original.local_calls == 11

    client = DurableServiceClient(original, record.accept, record.on_completed)
    result = client.get_response(_inputs(), stream=stream)
    response = await result.get_final_response() if stream else await result
    clone = record.instances[-1]
    assert client.exact_acceptance is True
    assert clone is not original
    assert clone._cached_chat_middleware_pipeline is cached
    assert callback_instances == [original, original]
    assert original.local_calls == 21
    assert clone.local_calls == 12
    assert len(record.accepted) == len(record.completed) == 1
    assert response.text == "answer"


async def test_custom_attribute_lookup_uses_original_without_claiming_exact_acceptance() -> None:
    record = _Record()

    class OpaqueLookup(_LeafClient):
        def __getattribute__(self, name: str) -> Any:
            return super().__getattribute__(name)

    original = OpaqueLookup(record)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    assert client.exact_acceptance is False
    response = await client.get_response(_inputs())
    assert record.instances == [original]
    assert record.completed == [response]
    assert record.accepted == []
    assert original.local_calls == 1


async def test_partial_stream_and_producer_close_do_not_count_as_completion() -> None:
    record = _Record()
    client = DurableServiceClient(_LeafClient(record), record.accept, record.on_completed)
    stream = client.get_response(_inputs(), stream=True)
    await anext(stream)
    assert record.accepted == record.completed == []
    assert not record.progress.service_completed
    # Close the fake producer without asking Core to drain or finalize the result.
    await stream._stream_source.aclose()
    assert record.events.count("pull") == 1
    assert "finalizer" not in record.events
    assert not record.progress.service_completed


@pytest.mark.parametrize("deferred", [False, True])
async def test_invalid_stream_shape_is_not_accepted(deferred: bool) -> None:
    record = _Record()
    original: Any = _LeafClient(record)

    def invalid_stream(self: Any, **kwargs: Any) -> Any:
        self.record.events.append("leaf")
        response = ChatResponse(messages=[Message("assistant", ["not a stream"])])
        if deferred:

            async def resolve() -> ChatResponse:
                return response

            return resolve()
        return response

    original._inner_get_response = MethodType(invalid_stream, original)
    client = DurableServiceClient(original, record.accept, record.on_completed)
    with pytest.raises(ValueError, match="requires a ResponseStream"):
        result = client.get_response(_inputs(), stream=True)
        await result.get_final_response()
    assert record.events.count("leaf") == 1
    assert record.accepted == record.completed == []
    assert not record.progress.service_completed
