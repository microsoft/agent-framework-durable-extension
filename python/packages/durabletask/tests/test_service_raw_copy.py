# Copyright (c) Microsoft. All rights reserved.

"""Native Core copy contracts at the two durable observation snapshot sites."""

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

from agent_framework_durabletask._invocation_safety import DurableServiceAcceptance, DurableServiceClient


class _PoisonRaw:
    def __init__(self) -> None:
        self.copy_calls = 0

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        self.copy_calls += 1
        raise AssertionError("raw payload was traversed")


def _message(message_raw: _PoisonRaw, content_raw: _PoisonRaw) -> Message:
    message = Message(
        "user",
        [
            Content.from_text(
                "original",
                additional_properties={"nested": {"values": ["content-original"]}},
                raw_representation=content_raw,
            )
        ],
        additional_properties={"nested": {"values": ["message-original"]}},
        raw_representation=message_raw,
    )
    message._durable_ingestion_receipt = ("occurrence", "a" * 64)  # type: ignore[attr-defined]
    return message


def _assert_snapshot(snapshot: Message, original: Message, message_raw: _PoisonRaw, content_raw: _PoisonRaw) -> None:
    assert type(snapshot) is type(original) is Message
    assert type(snapshot.contents[0]) is type(original.contents[0]) is Content
    assert snapshot is not original
    assert snapshot.contents[0] is not original.contents[0]
    assert snapshot.text == "original"
    assert snapshot.additional_properties["nested"] == {"values": ["message-original"]}
    assert snapshot.contents[0].additional_properties["nested"] == {"values": ["content-original"]}
    assert snapshot.raw_representation is message_raw
    assert snapshot._durable_ingestion_receipt == ("occurrence", "a" * 64)  # type: ignore[attr-defined]
    # Core 1.13 keeps Content.raw_representation by reference. Core 1.16 drops
    # it. Neither policy traverses the raw object, and the original is intact.
    assert snapshot.contents[0].raw_representation is content_raw or snapshot.contents[0].raw_representation is None
    assert original.raw_representation is message_raw
    assert original.contents[0].raw_representation is content_raw
    assert message_raw.copy_calls == content_raw.copy_calls == 0


def test_poison_raw_really_raises_when_deepcopied() -> None:
    raw = _PoisonRaw()
    with pytest.raises(AssertionError, match="raw payload was traversed"):
        deepcopy(raw)
    assert raw.copy_calls == 1


def test_native_message_and_content_deepcopy_do_not_traverse_raw_payloads() -> None:
    message_raw = _PoisonRaw()
    content_raw = _PoisonRaw()
    original = _message(message_raw, content_raw)
    copied_content = deepcopy(original.contents[0])
    assert copied_content is not original.contents[0]
    assert copied_content.raw_representation is content_raw or copied_content.raw_representation is None
    snapshot = deepcopy(original)
    original.contents[0].text = "changed"
    original.contents[0].additional_properties["nested"]["values"].append("changed")
    original.additional_properties["nested"]["values"].append("changed")
    _assert_snapshot(snapshot, original, message_raw, content_raw)


class _NoCopyLeaf(BaseChatClient):
    """Record references and scalars only so the probe cannot mask the copy site."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.received: list[Message] = []
        self.received_texts: list[str] = []
        self.events: list[str] = []

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.events.append("leaf")
        self.received.extend(messages)
        self.received_texts.extend(message.text for message in messages)
        for message in messages:
            message.contents[0].text = "leaf-mutated"
            message.contents[0].additional_properties["nested"]["values"].append("leaf-mutated")
            message.additional_properties["nested"]["values"].append("leaf-mutated")
        response = ChatResponse(messages=[Message("assistant", ["answer"])])
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(role="assistant", contents=[Content.from_text("answer")])

            def finalize(updates: Sequence[ChatResponseUpdate]) -> ChatResponse:
                self.events.append("completed")
                return response

            return ResponseStream(updates(), finalizer=finalize)

        async def get() -> ChatResponse:
            self.events.append("completed")
            return response

        return get()


class _ChatNoCopyLeaf(ChatMiddlewareLayer, _NoCopyLeaf):
    pass


class _Pass(ChatMiddleware):
    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("compact", [False, True], ids=["no-preparation", "compaction-tap"])
@pytest.mark.parametrize("seam", ["acceptance-middleware", "receipt-direct", "receipt-middleware"])
async def test_observation_snapshots_skip_native_raw_payloads_and_detach_mutable_fields(
    stream: bool, compact: bool, seam: str
) -> None:
    message_raw = _PoisonRaw()
    content_raw = _PoisonRaw()
    original = _message(message_raw, content_raw)
    accepted: list[list[Message]] = []
    strategy_calls: list[str] = []
    client: _NoCopyLeaf

    async def strategy(messages: list[Message]) -> bool:
        strategy_calls.append("compact")
        assert messages[0] is original
        assert messages[0].raw_representation is message_raw
        assert messages[0].contents[0].raw_representation is content_raw
        return False

    def accept(messages: Sequence[Message]) -> None:
        assert client.events == ["leaf", "completed"]
        client.events.append("accept")
        accepted.append(list(messages))  # No deepcopy or serialization in this probe's sink.

    kwargs: dict[str, Any] = {"compaction_strategy": strategy if compact else None}
    if seam == "acceptance-middleware":
        client = _ChatNoCopyLeaf(middleware=[DurableServiceAcceptance(accept)], **kwargs)
        target: Any = client
    elif seam == "receipt-middleware":
        client = _ChatNoCopyLeaf(middleware=[_Pass()], **kwargs)
        target = DurableServiceClient(client, accept)
    else:
        client = _NoCopyLeaf(**kwargs)
        target = DurableServiceClient(client, accept)

    result = target.get_response([original], stream=stream)
    response = await result.get_final_response() if stream else await result

    assert response.text == "answer"
    assert client.events == ["leaf", "completed", "accept"]
    assert len(client.received) == 1 and client.received[0] is original
    assert client.received_texts == ["original"]
    assert original.text == "leaf-mutated"
    assert strategy_calls == (["compact"] if compact else [])
    assert len(accepted) == 1 and len(accepted[0]) == 1
    _assert_snapshot(accepted[0][0], original, message_raw, content_raw)
    if isinstance(target, DurableServiceClient):
        assert target.observed_request
