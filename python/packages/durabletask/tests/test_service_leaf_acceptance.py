# Copyright (c) Microsoft. All rights reserved.

"""Original-client observation against real Core preparation and stream pipelines."""

from __future__ import annotations

from collections import UserDict, UserList
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from types import MethodType, SimpleNamespace
from typing import Any, cast

import agent_framework
import pytest
from _invocation_test_support import ToolChatClient, lookup
from agent_framework import (
    Agent,
    AgentContext,
    AgentMiddleware,
    BaseChatClient,
    ChatContext,
    ChatMiddleware,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    CompactionStrategy,
    Content,
    FunctionInvocationLayer,
    HistoryProvider,
    Message,
    ResponseStream,
)
from agent_framework import chat_middleware as as_chat_middleware
from agent_framework.observability import ChatTelemetryLayer

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


class _Leaf(BaseChatClient):
    def __init__(
        self,
        record: _Record,
        *,
        mutate: bool = False,
        deferred: bool = False,
        failure: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.record = record
        self.local_calls = 0
        self.mutate = mutate
        self.deferred = deferred
        self.failure = failure
        self.error = RuntimeError(f"{failure} failed")
        self.hidden_alias = self
        self.cached_counter = lambda: self.local_calls
        self.options: list[Mapping[str, Any]] = []

    def __copy__(self) -> Any:
        raise AssertionError("Do not copy clients")

    def __deepcopy__(self, memo: Any) -> Any:
        raise AssertionError("Do not deep-copy clients")

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Any:
        assert self.hidden_alias is self
        self.local_calls += 1
        assert self.cached_counter() == self.local_calls
        self.record.instances.append(self)
        self.record.events.append("leaf")
        self.record.received.append(deepcopy(list(messages)))
        self.options.append(options)
        if self.failure == "dispatch":
            raise self.error
        if self.mutate and messages:
            messages[-1].contents[0].text = "provider-mutated"
            messages[-1].additional_properties["nested"]["values"].append("provider-mutated")
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
                    raise self.error
                return response

            return get()

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            self.record.events.append("pull")
            yield ChatResponseUpdate(role="assistant", contents=[Content.from_text("answer")])
            if self.failure == "stream":
                raise self.error

        def finalize(updates: Sequence[ChatResponseUpdate]) -> ChatResponse:
            self.record.events.append("finalizer")
            assert len(updates) == 1
            if self.failure == "finalizer":
                raise self.error
            return response

        def result_hook(value: ChatResponse) -> ChatResponse:
            self.record.events.append("provider-result")
            assert value is response
            if self.failure == "result-hook":
                raise self.error
            return value

        def transform(update: ChatResponseUpdate) -> ChatResponseUpdate:
            self.record.events.append("transform")
            return update

        result = ResponseStream(
            updates(),
            finalizer=finalize,
            transform_hooks=[transform],
            cleanup_hooks=[lambda: self.record.events.append("cleanup")],
            result_hooks=[result_hook],
        )
        self.record.streams.append(result)
        if self.deferred:

            async def resolve() -> Any:
                self.record.events.append("resolve")
                return result

            return resolve()
        return result


class _Layered(FunctionInvocationLayer, ChatMiddlewareLayer, ChatTelemetryLayer, _Leaf):
    def __init__(self, record: _Record, **kwargs: Any) -> None:
        super().__init__(record=record, **kwargs)


class _ChatOnly(ChatMiddlewareLayer, _Leaf):
    def __init__(self, record: _Record, **kwargs: Any) -> None:
        super().__init__(record=record, **kwargs)


class _Pass(ChatMiddleware):
    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()


class _Audit(ChatMiddleware):
    def __init__(self, record: _Record, *, fail: bool = False, short_circuit: bool = False) -> None:
        self.record = record
        self.fail = fail
        self.short_circuit = short_circuit

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        if self.short_circuit:
            response = ChatResponse(messages=[Message("assistant", ["cached"])])
            if context.stream:

                async def updates() -> AsyncIterable[ChatResponseUpdate]:
                    yield ChatResponseUpdate(role="assistant", contents=[Content.from_text("cached")])

                context.result = ResponseStream(updates(), finalizer=lambda _: response)
            else:
                context.result = response
            return
        await call_next()

        def audit(response: ChatResponse) -> ChatResponse:
            assert self.record.progress.service_completed
            assert self.record.completed[-1].conversation_id == "conversation-final"
            self.record.events.append("audit")
            if self.fail:
                raise RuntimeError("audit failed")
            return response

        if isinstance(context.result, ResponseStream):
            context.result.with_result_hook(audit)
        else:
            assert isinstance(context.result, ChatResponse)
            audit(context.result)


async def _compact(messages: list[Message]) -> bool:
    for message in messages:
        if message.message_id == "A":
            message.additional_properties["_excluded"] = True
    if not any(message.message_id == "summary" for message in messages):
        messages.insert(0, Message("system", ["summary"], message_id="summary"))
    return True


def _inputs() -> list[Message]:
    return [
        Message("user", ["A"], message_id="A"),
        Message("user", ["B"], message_id="B", additional_properties={"nested": {"values": ["original"]}}),
    ]


def _serialized(batches: Sequence[Sequence[Message]]) -> list[list[dict[str, Any]]]:
    return [[message.to_dict() for message in batch] for batch in batches]


async def _finish(result: Any, stream: bool) -> ChatResponse:
    return await result.get_final_response() if stream else await result


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("layered", [False, True])
@pytest.mark.parametrize("source", ["configured", "per-call", "override"])
async def test_real_core_compaction_matches_unwrapped_dispatch(stream: bool, layered: bool, source: str) -> None:
    # Independent oracle: the real unwrapped Core client dispatch, not a copied
    # exclusion predicate or the observer's projection helper.
    baseline = _Record()
    observed = _Record()
    calls: list[str] = []

    async def configured(messages: list[Message]) -> bool:
        calls.append("configured")
        return await _compact(messages)

    async def per_call(messages: list[Message]) -> bool:
        calls.append("per-call")
        return await _compact(messages)

    def make(record: _Record) -> _Leaf:
        kwargs: dict[str, Any] = {"compaction_strategy": configured if source != "per-call" else None}
        return _Layered(record, middleware=[_Pass()], **kwargs) if layered else _Leaf(record, **kwargs)

    original = make(observed)
    control: Any = make(baseline)
    observer = DurableServiceClient(original, observed.accept, observed.on_completed)
    assert not observer.observed_request
    assert not observer.exact_acceptance
    kwargs = {"compaction_strategy": per_call} if source != "configured" else {}
    await _finish(control.get_response(_inputs(), stream=stream, **kwargs), stream)
    response = await _finish(observer.get_response(_inputs(), stream=stream, **kwargs), stream)

    assert response.text == "answer"
    assert [m.text for m in observed.received[0]] == ["summary", "B"]
    assert _serialized(observed.accepted) == _serialized(observed.received) == _serialized(baseline.received)
    assert calls == (["configured"] * 2 if source == "configured" else ["per-call"] * 2)
    assert observed.instances == [original]
    assert original.local_calls == original.cached_counter() == 1
    assert original.compaction_strategy is (configured if source != "per-call" else None)
    assert "get_response" not in vars(original)
    assert "_inner_get_response" not in vars(original)
    assert observer.observed_request and observer.exact_acceptance
    assert len(observed.completed) == 1
    assert observed.completed[0].response_id == "response-final"


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("layered", [False, True])
async def test_no_strategy_keeps_preexcluded_messages_and_annotations(stream: bool, layered: bool) -> None:
    record = _Record()
    original = _Layered(record, middleware=[_Pass()]) if layered else _Leaf(record)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    messages = _inputs()
    messages[0].additional_properties["_excluded"] = True
    before = _serialized([messages])
    await _finish(observer.get_response(messages, stream=stream), stream)
    assert _serialized(record.received) == _serialized(record.accepted) == before
    assert [m.text for m in record.received[0]] == ["A", "B"]
    assert original.compaction_strategy is None
    assert observer.observed_request


@pytest.mark.parametrize("stream", [False, True])
async def test_receipt_precedes_real_outer_middleware_audit_failure(stream: bool) -> None:
    record = _Record()
    original = _Layered(record, middleware=[_Audit(record, fail=True)], compaction_strategy=_compact)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    with pytest.raises(RuntimeError, match="audit failed"):
        await _finish(observer.get_response(_inputs(), stream=stream), stream)
    assert _serialized(record.accepted) == _serialized(record.received)
    assert record.events.index("leaf") < record.events.index("accept") < record.events.index("completed")
    assert record.events.index("completed") < record.events.index("audit")
    assert record.events.count("completed") == record.events.count("accept") == original.local_calls == 1
    assert record.progress.service_completed


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("compact", [False, True])
async def test_snapshot_precedes_provider_mutation(stream: bool, compact: bool) -> None:
    record = _Record()
    original = _Leaf(record, mutate=True, compaction_strategy=_compact if compact else None)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    await _finish(observer.get_response(_inputs(), stream=stream), stream)
    assert _serialized(record.accepted) == _serialized(record.received)
    assert record.accepted[0][-1].text == "B"
    assert record.accepted[0][-1].additional_properties["nested"] == {"values": ["original"]}


@pytest.mark.parametrize("stream", [False, True])
async def test_compaction_failure_leaves_dynamic_evidence_false_and_does_not_dispatch(stream: bool) -> None:
    record = _Record()
    original = _Leaf(record)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    await _finish(observer.get_response(_inputs(), stream=stream), stream)
    assert observer.observed_request
    record.accepted.clear()
    record.completed.clear()

    async def fail(messages: list[Message]) -> bool:
        assert not observer.observed_request
        raise RuntimeError("compaction failed")

    with pytest.raises(RuntimeError, match="compaction failed"):
        await _finish(observer.get_response(_inputs(), stream=stream, compaction_strategy=fail), stream)
    assert not observer.observed_request
    assert not observer.exact_acceptance
    assert original.local_calls == 1
    assert record.accepted == record.completed == []


@pytest.mark.parametrize("stream", [False, True])
async def test_strategy_can_verify_evidence_is_not_claimed_before_it_returns(stream: bool) -> None:
    record = _Record()
    original = _Leaf(record)
    observer = DurableServiceClient(original, record.accept, record.on_completed)

    async def strategy(messages: list[Message]) -> bool:
        assert not observer.observed_request
        return await _compact(messages)

    result = observer.get_response(_inputs(), stream=stream, compaction_strategy=strategy)
    assert not observer.observed_request
    await _finish(result, stream)
    assert observer.observed_request


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("token", [None, {}, {"response_id": "previous"}])
async def test_retrieval_completion_does_not_accept_new_inputs(stream: bool, token: Any) -> None:
    record = _Record()
    original = _Leaf(record)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    await _finish(observer.get_response(_inputs(), stream=stream, options={"continuation_token": token}), stream)
    assert original.local_calls == len(record.completed) == 1
    assert len(record.accepted) == (1 if token is None else 0)
    assert observer.observed_request


@pytest.mark.parametrize("stream", [False, True])
async def test_short_circuit_never_reaches_observer_or_claims_completion(stream: bool) -> None:
    record = _Record()
    original = _Layered(record, middleware=[_Audit(record, short_circuit=True)], compaction_strategy=_compact)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    response = await _finish(observer.get_response(_inputs(), stream=stream), stream)
    assert response.text == "cached"
    assert record.received == record.accepted == record.completed == []
    assert original.local_calls == 0
    assert not observer.observed_request
    assert not record.progress.service_completed


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("layered", [False, True])
async def test_tokenizer_only_preserves_core_preparation_but_claims_no_exact_receipt(
    stream: bool, layered: bool
) -> None:
    class Tokenizer:
        def __init__(self) -> None:
            self.calls = 0

        def count_tokens(self, text: str) -> int:
            self.calls += 1
            return 7

    records = [_Record(), _Record()]
    tokenizers = [Tokenizer(), Tokenizer()]
    clients = [
        _Layered(record, middleware=[_Pass()], tokenizer=tokenizer) if layered else _Leaf(record, tokenizer=tokenizer)
        for record, tokenizer in zip(records, tokenizers)
    ]
    observer = DurableServiceClient(clients[1], records[1].accept, records[1].on_completed)
    target: Any
    for target in (clients[0], observer):
        messages = _inputs()
        messages[0].additional_properties["_excluded"] = True
        await _finish(target.get_response(messages, stream=stream), stream)
    assert _serialized(records[0].received) == _serialized(records[1].received)
    assert [m.text for m in records[1].received[0]] == ["A", "B"]
    assert tokenizers[0].calls == tokenizers[1].calls
    assert records[1].accepted == []
    assert len(records[1].completed) == 1
    assert not observer.observed_request


@pytest.mark.parametrize("stream", [False, True])
async def test_original_scalar_state_and_cached_callback_survive_repeated_invocations(stream: bool) -> None:
    record = _Record()
    original = _Leaf(record)
    saved_callback = original.cached_counter
    for expected in (1, 2):
        observer = DurableServiceClient(original, record.accept, record.on_completed)
        await _finish(observer.get_response(_inputs(), stream=stream), stream)
        assert original.local_calls == saved_callback() == expected
        assert original.hidden_alias is original
    assert record.instances == [original, original]
    assert original.cached_counter is saved_callback


class _Opaque:
    def __init__(self, target: _Leaf, *, fail_after: bool = False) -> None:
        self.inner = target
        self.hidden_alias = target.get_response
        self.local_calls = 0
        self.fail_after = fail_after
        self.kwargs: list[dict[str, Any]] = []
        self.error = RuntimeError("previous_response_not_found")

    def get_response(self, messages: Sequence[Message], **kwargs: Any) -> Any:
        self.local_calls += 1
        self.kwargs.append(kwargs)
        if not self.fail_after:
            return self.hidden_alias(messages=messages, **kwargs)

        async def invoke() -> Any:
            result = self.hidden_alias(messages=messages, **kwargs)
            await _finish(result, bool(kwargs.get("stream")))
            raise self.error

        return invoke()


@pytest.mark.parametrize("stream", [False, True])
async def test_wrapper_hidden_alias_executes_original_and_is_completion_only(stream: bool) -> None:
    record = _Record()
    original = _Leaf(record)
    wrapper = _Opaque(original)
    observer = DurableServiceClient(wrapper, record.accept, record.on_completed)
    options = {"custom": object()}
    client_kwargs = {"opaque": object()}
    await _finish(observer.get_response(_inputs(), stream=stream, options=options, client_kwargs=client_kwargs), stream)
    assert record.instances == [original]
    assert original.local_calls == wrapper.local_calls == 1
    assert wrapper.inner is original
    assert wrapper.kwargs[0]["client_kwargs"] is client_kwargs
    assert wrapper.kwargs[0]["options"] is options
    assert record.accepted == []
    assert len(record.completed) == 1
    assert not observer.observed_request
    assert not observer.exact_acceptance


@pytest.mark.parametrize("stream", [False, True])
async def test_parent_retry_contract_does_not_restart_opaque_wrapper_after_hidden_completion(stream: bool) -> None:
    # Consumer-contract probe only. Parent AgentEntity integration is out of scope.
    record = _Record()
    original = _Leaf(record)
    wrapper = _Opaque(original, fail_after=True)
    observer = DurableServiceClient(wrapper, record.accept, record.on_completed)
    attempts = 0
    with pytest.raises(RuntimeError, match="previous_response_not_found"):
        for _ in range(2):
            attempts += 1
            try:
                await _finish(observer.get_response(_inputs(), stream=stream), stream)
            except RuntimeError:
                if not observer.observed_request or record.progress.service_completed:
                    raise
    assert attempts == wrapper.local_calls == original.local_calls == 1
    assert record.accepted == record.completed == []
    assert not observer.observed_request


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("override", ["bound", "closure", "class", "preparation", "handler"])
@pytest.mark.parametrize("compact", [False, True])
async def test_custom_entry_and_preparation_keep_original_behavior_without_false_capability(
    stream: bool, override: str, compact: bool
) -> None:
    record = _Record()
    original: Any = _ChatOnly(record, middleware=[_Pass()])
    entry = original.get_response
    if override == "bound":

        def custom(self: Any, messages: Sequence[Message], **kwargs: Any) -> Any:
            self.local_calls += 10
            return entry(messages=messages, **kwargs)

        original.get_response = MethodType(custom, original)
    elif override == "closure":
        original.get_response = lambda *args, **kwargs: entry(*args, **kwargs)
    elif override == "class":

        class Custom(_ChatOnly):
            def get_response(self, messages: Sequence[Message], **kwargs: Any) -> Any:
                return super().get_response(messages=messages, **kwargs)

        original = Custom(record, middleware=[_Pass()])
    elif override == "preparation":
        prepare = original._prepare_messages_for_model_call

        async def custom_prepare(self: Any, messages: Sequence[Message], **kwargs: Any) -> Any:
            return await prepare(messages, **kwargs)

        original._prepare_messages_for_model_call = MethodType(custom_prepare, original)
    else:
        handler = original._middleware_handler
        original._middleware_handler = lambda context: handler(context)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    await _finish(
        observer.get_response(_inputs(), stream=stream, compaction_strategy=_compact if compact else None), stream
    )
    assert record.instances == [original]
    assert original.local_calls == (11 if override == "bound" else 1)
    assert not observer.observed_request
    assert record.accepted == []
    assert len(record.completed) == 1


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
async def test_existing_pipeline_preserves_real_function_loop_and_guard(stream: bool, enabled: bool) -> None:
    record = _Record()
    original = ToolChatClient()
    pass_through = _Pass()
    original.chat_middleware = [pass_through]
    configuration = original.function_invocation_configuration
    before = deepcopy(configuration)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    agent: Any = Agent(client=observer, tools=[lookup])
    guard = DurableToolGuard(record.progress, enabled=enabled)
    runtime = {"middleware": [guard]}
    result = agent.run("question", stream=stream, client_kwargs=runtime)
    response = await _finish(result, stream)
    assert response.text == "answer-2"
    assert len(record.accepted) == len(record.completed) == len(original.received_messages) == 2
    assert _serialized(record.accepted) == _serialized(original.received_messages)
    assert record.progress.function_started is enabled
    assert original.function_invocation_configuration is configuration
    assert configuration == before
    assert original.chat_middleware == [pass_through]
    assert runtime == {"middleware": [guard]}
    assert observer.observed_request


def _compact_once(working_lists: list[list[Message]]) -> CompactionStrategy:
    async def compact(messages: list[Message]) -> bool:
        working_lists.append(messages)
        return await _compact(messages) if len(working_lists) == 1 else False

    return compact


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("source", ["configured", "per-call"])
async def test_real_core_first_chat_middleware_loses_loop_compaction_insertions(stream: bool, source: str) -> None:
    # Dependency oracle for Core 1.13/1.16. Neither arm uses DurableServiceClient
    # or an imitation of the function loop. A passive PUBLIC per-call middleware
    # is the only pipeline difference, including when a function guard is present.
    # If Core changes this ownership behavior, revisit the completion-only gate.
    baseline = ToolChatClient()
    with_pipeline = ToolChatClient()
    working_lists: list[list[list[Message]]] = [[], []]
    client: Any
    for client, lists, chat_middleware in (
        (baseline, working_lists[0], []),
        (with_pipeline, working_lists[1], [_Pass()]),
    ):
        strategy = _compact_once(lists)
        kwargs: dict[str, Any] = {}
        if source == "configured":
            client.compaction_strategy = strategy
        else:
            kwargs["compaction_strategy"] = strategy
        guard = DurableToolGuard(InvocationProgress(), enabled=True)
        messages = _inputs()
        before = _serialized([messages])
        response = await _finish(
            client.get_response(
                messages,
                stream=stream,
                options={"tools": [lookup]},
                client_kwargs={"middleware": [guard, *chat_middleware]},
                **kwargs,
            ),
            stream,
        )
        assert response.text == "answer-2"
        assert len(lists) == len(client.received_messages) == 2
        assert lists[0] is not messages
        assert _serialized([messages]) == before
        assert client.chat_middleware == []

    # The no-pipeline strategy receives the SAME loop-owned list each time.
    # ChatContext receives a new shallow list per leaf, so only the Message
    # exclusions survive into the second call, not the inserted summary.
    assert working_lists[0][0] is working_lists[0][1]
    assert working_lists[1][0] is not working_lists[1][1]
    assert _serialized([baseline.received_messages[0]]) == _serialized([with_pipeline.received_messages[0]])
    assert [m.text for m in baseline.received_messages[0]] == ["summary", "B"]
    for client, expected_summary_counts in ((baseline, [1, 1]), (with_pipeline, [1, 0])):
        assert [
            sum(message.message_id == "summary" for message in batch) for batch in client.received_messages
        ] == expected_summary_counts
        assert all(message.message_id != "A" for batch in client.received_messages for message in batch)
        assert any(
            content.type == "function_result" and content.result == "value:durable"
            for message in client.received_messages[1]
            for content in message.contents
        )


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("source", ["configured", "explicit-none", "per-call", "override"])
@pytest.mark.parametrize("fail_on_call", [None, 2])
async def test_compaction_tool_loop_without_pipeline_preserves_inserted_summary(
    stream: bool, source: str, fail_on_call: int | None
) -> None:
    # Compatibility control only. Preserving unwrapped dispatch does not satisfy
    # the separate requirement to accept each completed leaf before a later failure.
    baseline = ToolChatClient(fail_on_call=fail_on_call)
    original = ToolChatClient(fail_on_call=fail_on_call)
    record = _Record()
    observer = DurableServiceClient(original, record.accept, record.on_completed)

    target: Any
    for client, target in ((baseline, baseline), (original, observer)):
        kwargs: dict[str, Any] = {}
        strategy = _compact_once([])
        if source in {"configured", "explicit-none"}:
            client.compaction_strategy = strategy
        if source == "explicit-none":
            kwargs["compaction_strategy"] = None
        elif source in {"per-call", "override"}:
            kwargs["compaction_strategy"] = strategy
            if source == "override":
                client.compaction_strategy = _compact
        if fail_on_call:
            with pytest.raises(RuntimeError, match="service parent is not visible"):
                await _finish(
                    target.get_response(_inputs(), stream=stream, options={"tools": [lookup]}, **kwargs), stream
                )
        else:
            await _finish(target.get_response(_inputs(), stream=stream, options={"tools": [lookup]}, **kwargs), stream)
    assert _serialized(original.received_messages) == _serialized(baseline.received_messages)
    assert len(original.received_messages) == 2
    assert all(any(m.message_id == "summary" for m in batch) for batch in original.received_messages)
    assert record.accepted == []
    assert len(record.completed) == (0 if fail_on_call else 1)
    assert not observer.observed_request
    assert not observer.exact_acceptance
    assert original.chat_middleware == []


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("fail_on_call", [None, 2])
@pytest.mark.parametrize("middleware_source", ["none", "client_kwargs", "middleware"])
async def test_plain_function_loop_observes_each_completed_leaf_without_reconstructing_client(
    stream: bool, fail_on_call: int | None, middleware_source: str
) -> None:
    baseline = ToolChatClient(fail_on_call=fail_on_call)
    original = ToolChatClient(fail_on_call=fail_on_call)
    record = _Record()
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    configuration = original.function_invocation_configuration
    before = deepcopy(configuration)
    target: Any
    for target in (baseline, observer):
        guard = DurableToolGuard(record.progress, enabled=True)
        runtime = {"middleware": [guard]}
        kwargs: dict[str, Any] = {"compaction_strategy": None, "tokenizer": None}
        if middleware_source == "client_kwargs":
            kwargs["client_kwargs"] = runtime
        elif middleware_source == "middleware":
            kwargs["middleware"] = runtime["middleware"]
        messages = _inputs()
        messages[0].additional_properties["_excluded"] = True
        if fail_on_call:
            with pytest.raises(RuntimeError, match="service parent is not visible"):
                await _finish(
                    target.get_response(messages, stream=stream, options={"tools": [lookup]}, **kwargs), stream
                )
        else:
            response = await _finish(
                target.get_response(messages, stream=stream, options={"tools": [lookup]}, **kwargs), stream
            )
            assert response.text == "answer-2"
        assert runtime == {"middleware": [guard]}
    completed = 1 if fail_on_call else 2
    assert _serialized(original.received_messages) == _serialized(baseline.received_messages)
    assert _serialized(record.accepted) == _serialized(original.received_messages[:completed])
    assert len(record.completed) == completed
    assert [response.response_id for response in record.completed] == [f"response-{i + 1}" for i in range(completed)]
    assert [m.text for m in record.accepted[0]] == ["A", "B"]
    assert record.progress.service_completed and observer.observed_request
    assert original.function_invocation_configuration is configuration
    assert configuration == before
    assert original.chat_middleware == []
    assert original.compaction_strategy is original.tokenizer is None
    assert "get_response" not in vars(original)
    assert "_inner_get_response" not in vars(original)


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("source", ["configured", "explicit-none", "per-call", "override"])
async def test_plain_function_loop_tokenizer_only_keeps_unknown_acceptance(stream: bool, source: str) -> None:
    class Tokenizer:
        def __init__(self) -> None:
            self.calls = 0

        def count_tokens(self, text: str) -> int:
            self.calls += 1
            return 7

    clients = [ToolChatClient(), ToolChatClient()]
    tokenizers = [Tokenizer(), Tokenizer()]
    record = _Record()
    observer = DurableServiceClient(clients[1], record.accept, record.on_completed)
    target: Any
    for client, tokenizer, target in zip(clients, tokenizers, [clients[0], observer]):
        kwargs: dict[str, Any] = {}
        if source in {"configured", "explicit-none"}:
            client.tokenizer = tokenizer
        if source == "explicit-none":
            kwargs["tokenizer"] = None
        elif source in {"per-call", "override"}:
            kwargs["tokenizer"] = tokenizer
            if source == "override":
                client.tokenizer = Tokenizer()
        await _finish(target.get_response(_inputs(), stream=stream, options={"tools": [lookup]}, **kwargs), stream)
        assert client.chat_middleware == []
    assert tokenizers[0].calls == tokenizers[1].calls > 0
    assert _serialized(clients[0].received_messages) == _serialized(clients[1].received_messages)
    assert record.accepted == []
    assert len(record.completed) == 1
    assert not observer.observed_request


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(
    "container",
    ["mapping", "sequence", "bare", "callable", "nested-strategy"]
    + (["bundle"] if hasattr(agent_framework, "MiddlewareBundle") else []),
)
async def test_plain_function_loop_unknown_runtime_preserves_dispatch_without_receipts(
    stream: bool, container: str
) -> None:
    clients = [ToolChatClient(), ToolChatClient()]
    record = _Record()
    observer = DurableServiceClient(clients[1], record.accept, record.on_completed)

    @as_chat_middleware
    async def pass_through(context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()

    target: Any
    for target in (clients[0], observer):
        guard = DurableToolGuard(InvocationProgress(), enabled=True)
        runtime: Any = {"middleware": [guard]}
        if container == "mapping":
            runtime = UserDict(runtime)
        elif container == "sequence":
            runtime["middleware"] = UserList([guard])
        elif container == "bare":
            runtime["middleware"] = guard
        elif container == "bundle":
            core: Any = agent_framework
            runtime["middleware"] = [core.MiddlewareBundle([guard])]
        elif container == "callable":
            runtime["middleware"] = [pass_through]
        else:
            runtime["compaction_strategy"] = _compact
        response = await _finish(
            target.get_response(_inputs(), stream=stream, options={"tools": [lookup]}, client_kwargs=runtime), stream
        )
        assert response.text == "answer-2"
    assert _serialized(clients[0].received_messages) == _serialized(clients[1].received_messages)
    assert record.accepted == []
    assert len(record.completed) == 1
    assert not observer.observed_request


@pytest.mark.parametrize("stream", [False, True])
async def test_reset_before_agent_attempt_discards_evidence_when_agent_middleware_fails(stream: bool) -> None:
    record = _Record()
    original = ToolChatClient()
    observer = DurableServiceClient(original, record.accept, record.on_completed)

    class FailBeforeClient(AgentMiddleware):
        fail = False

        async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
            if self.fail:
                assert not observer.observed_request
                raise RuntimeError("agent middleware failed")
            await call_next()

    middleware = FailBeforeClient()
    agent: Any = Agent(client=observer, tools=[lookup], middleware=[middleware])
    await _finish(agent.run("first", stream=stream), stream)
    assert observer.observed_request
    assert len(record.accepted) == len(record.completed) == len(original.received_messages) == 2
    middleware.fail = True
    # Consumer contract: this reset is needed even when get_response is not reached.
    observer.reset_observation()
    with pytest.raises(RuntimeError, match="agent middleware failed"):
        await _finish(agent.run("second", stream=stream), stream)
    assert not observer.observed_request
    assert not observer.exact_acceptance
    assert len(record.accepted) == len(record.completed) == len(original.received_messages) == 2


class _SDKStream:
    def __init__(self, events: list[Any]) -> None:
        self.events = events

    async def __aenter__(self) -> _SDKStream:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    async def __aiter__(self) -> AsyncIterable[Any]:
        for event in self.events:
            yield event


class _ResponsesSDK:
    """Double only the SDK transport. Stock clients still encode and parse."""

    def __init__(self, fail_on_call: int | None, record: _Record | None = None) -> None:
        self.requests: list[dict[str, Any]] = []
        self.fail_on_call = fail_on_call
        self.record = record
        self.responses = SimpleNamespace(with_raw_response=self)
        self.base_url = "https://offline.invalid/"
        self.error = RuntimeError("offline second SDK call failed")

    async def create(self, **kwargs: Any) -> Any:
        from openai.types.responses import Response as OpenAIResponse
        from openai.types.responses import ResponseStreamEvent
        from pydantic import TypeAdapter

        call = len(self.requests) + 1
        if self.record is not None:
            # The first completed leaf must be visible BEFORE dispatching the next.
            assert len(self.record.accepted) == len(self.record.completed) == call - 1
        self.requests.append(deepcopy(kwargs))
        if call == self.fail_on_call:
            raise self.error
        item = (
            {
                "type": "function_call",
                "id": "fc-1",
                "call_id": "call-1",
                "name": "lookup",
                "arguments": '{"key":"durable"}',
                "status": "completed",
            }
            if call == 1
            else {
                "type": "message",
                "id": "msg-2",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": "offline answer", "annotations": []}],
            }
        )
        response = OpenAIResponse.model_validate({
            "id": f"resp_{call}",
            "created_at": 1.0,
            "model": "offline-model",
            "object": "response",
            "status": "completed",
            "parallel_tool_calls": False,
            "tool_choice": "auto",
            "tools": [],
            "output": [item],
        })
        if not kwargs["stream"]:
            return SimpleNamespace(headers={}, parse=lambda: response)
        delta = (
            {
                "type": "response.function_call_arguments.delta",
                "item_id": "fc-1",
                "output_index": 0,
                "delta": '{"key":"durable"}',
                "sequence_number": 2,
            }
            if call == 1
            else {
                "type": "response.output_text.delta",
                "item_id": "msg-2",
                "output_index": 0,
                "content_index": 0,
                "delta": "offline answer",
                "logprobs": [],
                "sequence_number": 2,
            }
        )
        events = [
            {"type": "response.created", "response": response, "sequence_number": 0},
            {"type": "response.output_item.added", "item": item, "output_index": 0, "sequence_number": 1},
            delta,
            {"type": "response.completed", "response": response, "sequence_number": 3},
        ]
        parsed: list[Any] = [TypeAdapter(ResponseStreamEvent).validate_python(event) for event in events]
        return SimpleNamespace(headers={}, parse=lambda: _SDKStream(parsed))


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("provider", ["openai", "foundry"])
@pytest.mark.parametrize("fail_on_call", [None, 2])
async def test_stock_responses_clients_observe_no_compaction_tool_leaves_at_offline_sdk_boundary(
    stream: bool, provider: str, fail_on_call: int | None
) -> None:
    from agent_framework.exceptions import ChatClientException
    from agent_framework_foundry import FoundryChatClient
    from agent_framework_openai import OpenAIChatClient

    record = _Record()
    sdks = [_ResponsesSDK(fail_on_call), _ResponsesSDK(fail_on_call, record)]
    clients: list[Any] = []
    for sdk in sdks:
        sdk_arg: Any = sdk
        project: Any = SimpleNamespace(get_openai_client=lambda sdk=sdk: sdk)
        client = (
            OpenAIChatClient(model="offline-model", async_client=sdk_arg)
            if provider == "openai"
            else FoundryChatClient(model="offline-model", project_client=project)
        )
        assert type(client) is (OpenAIChatClient if provider == "openai" else FoundryChatClient)
        clients.append(client)
    observer = DurableServiceClient(clients[1], record.accept, record.on_completed)
    configuration = clients[1].function_invocation_configuration
    before = deepcopy(configuration)
    for target in (clients[0], observer):
        agent: Any = Agent(client=target, tools=[lookup], default_options={"store": False})
        if fail_on_call:
            with pytest.raises(ChatClientException, match="offline second SDK call failed"):
                await _finish(agent.run(_inputs(), stream=stream), stream)
        else:
            response = await _finish(agent.run(_inputs(), stream=stream), stream)
            assert response.text == "offline answer"
    assert sdks[0].requests == sdks[1].requests
    assert len(sdks[1].requests) == 2
    assert [item["content"][0]["text"] for item in sdks[1].requests[0]["input"]] == ["A", "B"]
    assert any(
        item.get("type") == "function_call_output" and item["output"] == "value:durable"
        for item in sdks[1].requests[1]["input"]
    )
    assert len(record.accepted) == len(record.completed) == (1 if fail_on_call else 2)
    assert [m.text for m in record.accepted[0]] == ["A", "B"]
    assert observer.observed_request and record.progress.service_completed
    assert clients[1].client is sdks[1]
    if provider == "foundry":
        assert clients[1].project_client.get_openai_client() is sdks[1]
    assert clients[1].function_invocation_configuration is configuration
    assert configuration == before
    assert clients[1].chat_middleware == []
    assert "get_response" not in vars(clients[1])
    assert "_inner_get_response" not in vars(clients[1])


def test_stock_chat_completion_override_is_not_whitelisted() -> None:
    from agent_framework_openai import OpenAIChatCompletionClient

    from agent_framework_durabletask._invocation_safety import _core_layout

    sdk: Any = SimpleNamespace(base_url="https://offline.invalid/")
    client = OpenAIChatCompletionClient(model="offline-model", async_client=sdk)
    assert _core_layout(client) is None


@pytest.mark.parametrize("stream", [False, True])
async def test_plain_chat_fast_path_preserves_callers_compaction_list(stream: bool) -> None:
    record = _Record()
    original = _ChatOnly(record, compaction_strategy=_compact)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    messages = _inputs()
    await _finish(observer.get_response(messages, stream=stream), stream)
    assert messages[0].message_id == "summary"
    assert _serialized(record.accepted) == _serialized(record.received)
    assert original.chat_middleware == []
    assert observer.observed_request


async def test_unclassified_runtime_middleware_never_uses_an_outer_input_snapshot() -> None:
    record = _Record()
    original = _ChatOnly(record)

    async def cached(context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        context.result = ChatResponse(messages=[Message("assistant", ["cached"])])

    observer = DurableServiceClient(original, record.accept, record.on_completed)
    response = await observer.get_response(_inputs(), client_kwargs={"middleware": [cached]})
    assert response.text == "cached"
    assert original.local_calls == 0
    assert record.accepted == []
    assert not observer.observed_request


@pytest.mark.parametrize("stream", [False, True])
async def test_cached_middleware_closure_and_scalar_updates_belong_to_original(stream: bool) -> None:
    record = _Record()
    original = _Layered(record)
    callbacks: list[Any] = []

    async def audit(context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        callbacks.append(original)
        original.local_calls += 10
        await call_next()

    original.chat_middleware = [audit]
    await original.get_response(_inputs())
    assert original.local_calls == 11
    record.accepted.clear()
    record.completed.clear()
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    await _finish(observer.get_response(_inputs(), stream=stream), stream)
    assert callbacks == [original, original]
    assert record.instances == [original, original]
    assert original.local_calls == original.cached_counter() == 22
    assert original.chat_middleware == [audit]
    assert len(record.accepted) == len(record.completed) == 1


@pytest.mark.parametrize("stream", [False, True])
async def test_two_observers_do_not_share_request_snapshots(stream: bool) -> None:
    record = _Record()
    first, second = _Record(), _Record()
    original = _Leaf(record)
    observers = [
        DurableServiceClient(original, first.accept, first.on_completed),
        DurableServiceClient(original, second.accept, second.on_completed),
    ]
    pending = [
        observer.get_response([Message("user", [text])], stream=stream)
        for observer, text in zip(observers, ["one", "two"])
    ]
    await _finish(pending[1], stream)
    assert first.accepted == first.completed == []
    assert second.accepted[0][0].text == "two"
    await _finish(pending[0], stream)
    assert first.accepted[0][0].text == "one"
    assert record.instances == [original, original]
    assert original.local_calls == 2


@pytest.mark.parametrize("deferred", [False, True])
async def test_stream_keeps_original_finalizer_hooks_and_lazy_updates(deferred: bool) -> None:
    record = _Record()
    original = _Leaf(record, deferred=deferred)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    result = observer.get_response(_inputs(), stream=True)
    assert "pull" not in record.events
    assert record.accepted == record.completed == []
    await result
    assert "pull" not in record.events
    await anext(result)
    assert record.accepted == record.completed == []
    response = await result.get_final_response()
    assert await result.get_final_response() is response
    assert response.conversation_id == "conversation-final"
    assert response.response_id == "response-final"
    for name in ("leaf", "pull", "transform", "cleanup", "finalizer", "provider-result", "accept", "completed"):
        assert record.events.count(name) == 1
    assert record.events.index("provider-result") < record.events.index("completed")


@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("opaque", [False, True])
async def test_prefinalized_stream_does_not_repeat_provider_finalizer_or_result_hooks(
    deferred: bool, opaque: bool
) -> None:
    record = _Record()
    producer = _Leaf(record)
    finalized = producer.get_response(_inputs(), stream=True)
    response = await finalized.get_final_response()
    before = list(record.events)

    class Prepared(_Leaf):
        def _inner_get_response(self, **kwargs: Any) -> Any:
            self.local_calls += 1
            if deferred:

                async def resolve() -> Any:
                    return finalized

                return resolve()
            return finalized

    original = Prepared(record)
    target = _Opaque(original) if opaque else original
    observer = DurableServiceClient(target, record.accept, record.on_completed)
    result = observer.get_response(_inputs(), stream=True)
    assert await result.get_final_response() is response
    assert await result.get_final_response() is response
    assert await finalized.get_final_response() is response
    assert record.events[: len(before)] == before
    assert record.events.count("finalizer") == record.events.count("provider-result") == 1
    assert record.completed == [response]
    assert len(record.accepted) == (0 if opaque else 1)
    assert original.local_calls == 1


@pytest.mark.parametrize(
    ("stream", "failure"),
    [
        (False, "dispatch"),
        (False, "await"),
        (True, "dispatch"),
        (True, "stream"),
        (True, "finalizer"),
        (True, "result-hook"),
    ],
)
async def test_failed_provider_does_not_claim_acceptance_or_restart(stream: bool, failure: str) -> None:
    record = _Record()
    original = _Leaf(record, failure=failure)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    with pytest.raises(RuntimeError) as error:
        await _finish(observer.get_response(_inputs(), stream=stream), stream)
    assert error.value is original.error
    assert record.accepted == record.completed == []
    assert original.local_calls == 1
    assert observer.observed_request
    assert not record.progress.service_completed


async def test_partial_stream_is_not_drained_or_accepted() -> None:
    record = _Record()
    original = _Leaf(record)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    result = observer.get_response(_inputs(), stream=True)
    await anext(result)
    await cast(Any, record.streams[0]._stream_source).aclose()
    assert record.accepted == record.completed == []
    assert record.events.count("pull") == 1
    assert "finalizer" not in record.events


async def test_completion_still_notifies_when_acceptance_callback_raises() -> None:
    record = _Record()

    def reject(messages: Sequence[Message]) -> None:
        raise RuntimeError("acceptance failed")

    observer = DurableServiceClient(_Leaf(record), reject, record.on_completed)
    with pytest.raises(RuntimeError, match="acceptance failed"):
        await observer.get_response(_inputs())
    assert record.progress.service_completed
    assert len(record.completed) == 1


@pytest.mark.parametrize("stream", [False, True])
async def test_optional_completion_callback_and_legacy_middleware(stream: bool) -> None:
    record = _Record()
    observer = DurableServiceClient(_Leaf(record), record.accept)
    await _finish(observer.get_response(_inputs(), stream=stream), stream)
    assert [m.text for m in record.accepted[0]] == ["A", "B"]
    assert record.completed == []
    record.accepted.clear()
    original: Any = _ChatOnly(record, middleware=[DurableServiceAcceptance(record.accept)])
    await _finish(original.get_response(_inputs(), stream=stream), stream)
    assert [m.text for m in record.accepted[0]] == ["A", "B"]


class _HistoryAudit(HistoryProvider):
    def __init__(self, record: _Record) -> None:
        super().__init__("audit", load_messages=False)
        self.record = record

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        raise AssertionError("store-only provider")

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        assert self.record.progress.service_completed
        assert [m.text for m in self.record.accepted[0]] == ["summary", "B"]
        assert self.record.completed[0].conversation_id == "conversation-final"
        self.record.events.append("history-audit")


@pytest.mark.parametrize("stream", [False, True])
async def test_real_per_call_history_audit_runs_after_receipt(stream: bool) -> None:
    record = _Record()
    original = _Layered(record)
    observer = DurableServiceClient(original, record.accept, record.on_completed)
    agent: Any = Agent(
        client=observer,
        default_options={"store": True},
        context_providers=[_HistoryAudit(record)],
        require_per_service_call_history_persistence=True,
        compaction_strategy=_compact,
    )
    result = agent.run(_inputs(), stream=stream, session=agent.create_session(session_id="receipt-review"))
    assert (await _finish(result, stream)).text == "answer"
    assert record.events.index("completed") < record.events.index("history-audit")
    assert original.local_calls == 1
    assert _serialized(record.accepted) == _serialized(record.received)
    assert observer.observed_request
