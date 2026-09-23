# Copyright (c) Microsoft. All rights reserved.

"""Characterize nested preparation at real Core chat/Base dispatch boundaries.

The bare Core client is the compatibility oracle. Literal message expectations
and independent counters also distinguish unused provider kwargs from preparation.
These tests do not authorize acceptance for an unclassified invocation scope.
"""

from collections import UserDict
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

import pytest
from agent_framework import (
    BaseChatClient,
    ChatContext,
    ChatMiddleware,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    Message,
    ResponseStream,
)

from agent_framework_durabletask._invocation_safety import DurableServiceClient, _core_layout


class _Strategy:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[list[str | None]] = []

    async def __call__(self, messages: list[Message]) -> bool:
        self.calls.append([message.message_id for message in messages])
        messages[0].additional_properties["_excluded"] = True
        messages.insert(0, Message("system", [f"{self.name}-summary"], message_id=f"{self.name}-summary"))
        return True


class _Tokenizer:
    def __init__(self, value: int) -> None:
        self.value = value
        self.calls: list[str] = []

    def count_tokens(self, text: str) -> int:
        self.calls.append(text)
        return self.value


class _RawProviderValue:
    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        raise AssertionError("Provider kwargs must not be deep-copied")


class _Pass(ChatMiddleware):
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        # Capture the genuine Core context before the durable observer runs.
        self.calls.append(dict(context.kwargs))
        await call_next()


class _ChatClient(ChatMiddlewareLayer, BaseChatClient):
    """Override only the provider leaf, not any classified Core seam."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.messages: list[list[Message]] = []
        self.kwargs: list[dict[str, Any]] = []
        self.streams: list[bool] = []

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.messages.append(deepcopy(list(messages)))
        # Keep provider values by reference. Copying kwargs would mask ownership.
        self.kwargs.append(kwargs)
        self.streams.append(stream)
        response = ChatResponse(messages=[Message("assistant", ["answer"])], response_id="nested-review")
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(role="assistant", contents=[Content.from_text("answer")])

            return ResponseStream(updates(), finalizer=lambda _: response)

        async def get() -> ChatResponse:
            return response

        return get()


def _serialized(batches: Sequence[Sequence[Message]]) -> list[list[dict[str, Any]]]:
    return [[message.to_dict() for message in batch] for batch in batches]


async def _compare_pair(
    *,
    stream: bool,
    pipeline: str,
    nested: str,
    top: str,
    defaults: bool = False,
    unknown_mapping: bool = False,
    expected_strategy: str | None,
    expected_tokenizer: str | None,
    expected_observed: bool,
) -> None:
    clients: list[_ChatClient] = []
    strategy_counts: list[dict[str, list[list[str | None]]]] = []
    tokenizer_counts: list[dict[str, list[str]]] = []
    accepted: list[list[Message]] = []
    completed: list[ChatResponse] = []

    def accept(messages: Sequence[Message]) -> None:
        accepted.append(list(messages))

    # Fresh clients, inputs, callbacks, and kwargs for BOTH arms. Never run the
    # control through DurableServiceClient or reuse its mutated Message objects.
    for wrapped in (False, True):
        strategies = {name: _Strategy(name) for name in ("nested", "default", "top")}
        tokenizers = {name: _Tokenizer(value) for name, value in (("nested", 7), ("default", 11), ("top", 13))}
        middleware = _Pass()
        raw = _RawProviderValue()
        provider_values = ["original"]
        provider_nested = {"values": provider_values}
        provider_payload = {"raw": raw, "nested": provider_nested}
        runtime: dict[str, Any] = {"provider_payload": provider_payload}
        if nested in {"strategy", "both"}:
            runtime["compaction_strategy"] = strategies["nested"]
        if nested in {"tokenizer", "both"}:
            runtime["tokenizer"] = tokenizers["nested"]
        if pipeline == "per-call":
            runtime["middleware"] = [middleware]
        runtime_before = dict(runtime)
        supplied_runtime: Mapping[str, Any] = UserDict(runtime) if unknown_mapping else runtime
        call_kwargs: dict[str, Any] = {"client_kwargs": supplied_runtime}
        if top == "none":
            call_kwargs.update(compaction_strategy=None, tokenizer=None)
        elif top == "override":
            call_kwargs.update(compaction_strategy=strategies["top"], tokenizer=tokenizers["top"])
        else:
            assert top == "omitted"
            assert "compaction_strategy" not in call_kwargs and "tokenizer" not in call_kwargs

        client = _ChatClient(
            middleware=[middleware] if pipeline == "configured" else [],
            compaction_strategy=strategies["default"] if defaults else None,
            tokenizer=tokenizers["default"] if defaults else None,
        )
        clients.append(client)
        configured_middleware = client.chat_middleware
        assert _core_layout(client) == (True, False)
        assert type(client).get_response is ChatMiddlewareLayer.get_response
        observer = None
        target: Any = client
        if wrapped:
            observer = DurableServiceClient(client, accept, completed.append)
            assert not observer.observed_request and not observer.exact_acceptance
            target = observer
        messages = [
            Message("user", ["A"], message_id="A", additional_properties={"_excluded": True}),
            Message("user", ["B"], message_id="B"),
        ]
        result = target.get_response(messages, stream=stream, **call_kwargs)
        response = await result.get_final_response() if stream else await result
        assert response.text == "answer" and response.response_id == "nested-review"

        # This independent literal expectation makes parity alone insufficient.
        expected_messages = (
            [("system", f"{expected_strategy}-summary", f"{expected_strategy}-summary"), ("user", "B", "B")]
            if expected_strategy is not None
            else [("user", "A", "A"), ("user", "B", "B")]
        )
        assert len(client.messages) == len(client.kwargs) == 1
        assert [(message.role, message.text, message.message_id) for message in client.messages[0]] == expected_messages
        assert client.streams == [stream]
        for name, strategy in strategies.items():
            assert strategy.calls == ([["A", "B"]] if name == expected_strategy else [])
        for name, tokenizer in tokenizers.items():
            assert len(tokenizer.calls) == (2 if name == expected_tokenizer else 0)
        strategy_counts.append({name: strategy.calls for name, strategy in strategies.items()})
        tokenizer_counts.append({name: tokenizer.calls for name, tokenizer in tokenizers.items()})
        if expected_tokenizer is not None:
            last = client.messages[0][-1]
            assert last.additional_properties["_group"]["token_count"] == tokenizers[expected_tokenizer].value
        if expected_strategy is None and expected_tokenizer is None:
            assert [message.additional_properties for message in client.messages[0]] == [{"_excluded": True}, {}]

        leaf_kwargs = client.kwargs[0]
        assert leaf_kwargs["provider_payload"] is provider_payload
        assert provider_payload["raw"] is raw
        assert provider_payload["nested"] is provider_nested
        assert provider_nested["values"] is provider_values
        assert provider_payload == {"raw": raw, "nested": {"values": ["original"]}}
        assert supplied_runtime == runtime == runtime_before
        assert call_kwargs["client_kwargs"] is supplied_runtime
        for key, value in runtime_before.items():
            assert supplied_runtime[key] is value
        if pipeline == "per-call":
            assert supplied_runtime["middleware"] == [middleware]
        cached_pipeline = client._cached_chat_middleware_pipeline
        assert cached_pipeline is not None
        if pipeline == "none":
            # The fast path forwards these as raw provider kwargs, by identity.
            # Seeing a compaction/tokenizer key at the leaf is NOT its execution.
            assert middleware.calls == []
            assert leaf_kwargs.keys() == runtime.keys()
            for key, value in runtime.items():
                assert leaf_kwargs[key] is value
            assert not cached_pipeline.has_middlewares
        else:
            assert len(middleware.calls) == 1
            assert cached_pipeline.has_middlewares
            assert leaf_kwargs == {"provider_payload": provider_payload}
            assert middleware.calls[0]["provider_payload"] is provider_payload
            for key in ("compaction_strategy", "tokenizer"):
                if top == "override":
                    assert middleware.calls[0][key] is call_kwargs[key]
                elif key in runtime:
                    assert middleware.calls[0][key] is runtime[key]
                else:
                    assert key not in middleware.calls[0]
        assert client.chat_middleware is configured_middleware
        assert client.chat_middleware == ([middleware] if pipeline == "configured" else [])
        assert client.compaction_strategy is (strategies["default"] if defaults else None)
        assert client.tokenizer is (tokenizers["default"] if defaults else None)
        assert _core_layout(client) == (True, False)
        if wrapped:
            assert observer is not None
            assert observer.observed_request is expected_observed
            assert observer.exact_acceptance is expected_observed
            assert completed == [response]
        else:
            assert accepted == completed == []

    assert _serialized(clients[0].messages) == _serialized(clients[1].messages)
    assert strategy_counts[0] == strategy_counts[1]
    assert tokenizer_counts[0] == tokenizer_counts[1]
    if expected_observed:
        assert _serialized(accepted) == _serialized(clients[0].messages)
    else:
        assert accepted == []


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("top", ["omitted", "none"])
@pytest.mark.parametrize("nested", ["strategy", "tokenizer", "both"])
async def test_no_pipeline_nested_preparation_is_unused_provider_data(stream: bool, top: str, nested: str) -> None:
    await _compare_pair(
        stream=stream,
        pipeline="none",
        nested=nested,
        top=top,
        expected_strategy=None,
        expected_tokenizer=None,
        expected_observed=True,
    )


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(("pipeline", "top"), [("configured", "omitted"), ("per-call", "none")])
@pytest.mark.parametrize(
    ("nested", "strategy", "tokenizer", "observed"),
    [("strategy", "nested", None, True), ("tokenizer", None, "nested", False), ("both", "nested", "nested", True)],
)
async def test_active_pipeline_promotes_nested_preparation_before_observation(
    stream: bool, pipeline: str, top: str, nested: str, strategy: str | None, tokenizer: str | None, observed: bool
) -> None:
    await _compare_pair(
        stream=stream,
        pipeline=pipeline,
        nested=nested,
        top=top,
        expected_strategy=strategy,
        expected_tokenizer=tokenizer,
        expected_observed=observed,
    )


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(
    ("pipeline", "top", "winner"),
    [
        ("none", "omitted", "default"),
        ("none", "none", "default"),
        ("none", "override", "top"),
        ("configured", "omitted", "nested"),
        ("configured", "none", "nested"),
        ("configured", "override", "top"),
    ],
)
async def test_nested_preparation_keeps_core_top_level_and_default_precedence(
    stream: bool, pipeline: str, top: str, winner: str
) -> None:
    await _compare_pair(
        stream=stream,
        pipeline=pipeline,
        nested="both",
        top=top,
        defaults=True,
        expected_strategy=winner,
        expected_tokenizer=winner,
        expected_observed=True,
    )


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(
    ("pipeline", "strategy", "tokenizer"), [("none", None, None), ("configured", "nested", "nested")]
)
async def test_unclassified_mapping_preserves_core_dispatch_without_claiming_acceptance(
    stream: bool, pipeline: str, strategy: str | None, tokenizer: str | None
) -> None:
    await _compare_pair(
        stream=stream,
        pipeline=pipeline,
        nested="both",
        top="omitted",
        unknown_mapping=True,
        expected_strategy=strategy,
        expected_tokenizer=tokenizer,
        expected_observed=False,
    )
