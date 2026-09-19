# Copyright (c) Microsoft. All rights reserved.

"""Run-local safeguards at core's function invocation boundary."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass
from inspect import getattr_static, isawaitable
from types import FunctionType, MemberDescriptorType, MethodType
from typing import Any, cast

from agent_framework import (
    BaseChatClient,
    ChatContext,
    ChatMiddleware,
    ChatResponse,
    ChatResponseUpdate,
    FunctionInvocationContext,
    FunctionMiddleware,
    Message,
    ResponseStream,
)


@dataclass
class InvocationProgress:
    """Track observable progress that makes restarting a whole agent run unsafe."""

    stream_started: bool = False
    function_started: bool = False
    service_completed: bool = False


class DurableToolGuard(FunctionMiddleware):
    """Prevent callable execution even when a wrapper delegates to an inner core loop.

    This uses core's public per-run middleware contract. Arbitrary custom agents or
    clients that execute tools outside that contract remain responsible for their own
    side effects. No portable wrapper can sandbox their implementation.
    """

    def __init__(self, progress: InvocationProgress, *, enabled: bool) -> None:
        self.progress = progress
        self.enabled = enabled

    async def process(self, context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        if not self.enabled:
            context.result = "Tool execution is disabled for this invocation."
            return
        self.progress.function_started = True
        await call_next()


class DurableServiceAcceptance(ChatMiddleware):
    """Observe middleware inputs for direct callers, not exact post-compaction inputs.

    Kept for compatibility. Production acceptance uses ``DurableServiceClient``
    because even the innermost chat middleware runs before BaseChatClient compaction.
    """

    def __init__(self, accept: Callable[[Sequence[Message]], None]) -> None:
        self._accept = accept

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        inputs = deepcopy(list(context.messages))
        await call_next()

        def completed(response: ChatResponse) -> ChatResponse:
            # A completed provider response affirms receipt of these inputs, even
            # when the invocation outcome is failed. Interrupted streams do not.
            self._accept(inputs)
            return response

        if isinstance(context.result, ResponseStream):
            context.result.with_result_hook(completed)
        elif isinstance(context.result, ChatResponse):
            completed(context.result)


class DurableServiceClient:
    """Observe exact Core inputs when possible, otherwise only outer completion.

    Exact acceptance uses an isolated BaseChatClient chain with ordinary Python
    wrappers delegating through stored ``inner`` or ``__wrapped__`` attributes.
    Constructors and copy hooks are not run. SDK clients and other referenced
    resources remain borrowed, not recursively cloned.

    Generic SupportsChatGetResponse clients and unsupported layouts execute their
    original get_response, preserving their state and options. Only a completed
    outer ChatResponse is observed, never acceptance of opaque inputs. An outer
    failure cannot reveal whether an internal service call already completed.
    Invalid clients and detected delegation cycles are rejected before dispatch.

    ``exact_acceptance`` describes receipt capability, not a full compatibility
    guarantee. The parent must disable whole-agent retry when it is false, even
    for structured refusals. Exact acceptance describes prepared Core inputs, not
    provider wire transformations or a durable remote commit. Continuation-token
    retrieval accepts no new inputs.
    """

    def __init__(
        self,
        client: Any,
        accept: Callable[[Sequence[Message]], None],
        on_completed: Callable[[ChatResponse], None] | None = None,
    ) -> None:
        self.__wrapped__ = client
        if not callable(getattr(client, "get_response", None)):
            raise ValueError("A service client must expose a callable get_response method.")
        self._on_completed = on_completed
        try:
            self._client = _clone_service_client(client, accept, on_completed)
        except _UnsupportedServiceClient as exc:
            if not exc.allow_fallback:
                raise
            self._client = client
            self._exact_acceptance = False
        else:
            self._exact_acceptance = True

    @property
    def exact_acceptance(self) -> bool:
        """Whether a typed Core leaf was cloned for exact input acceptance.

        False for generic clients or unsupported layouts. The parent must disable
        whole-agent retry in that case, including on structured refusals, since
        hidden service completion cannot be ruled out. This receipt capability
        flag is not a guarantee of compatibility with arbitrary client behavior.
        """
        return self._exact_acceptance

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__wrapped__, name)

    def get_response(self, messages: Sequence[Message], **kwargs: Any) -> Any:
        result = self._client.get_response(messages=messages, **kwargs)
        if self._exact_acceptance:
            return result
        return _observe_outer_completion(result, stream=bool(kwargs.get("stream")), on_completed=self._on_completed)


class _UnsupportedServiceClient(ValueError):
    """A recognized observation limitation, distinct from arbitrary client errors."""

    def __init__(self, message: str, *, allow_fallback: bool) -> None:
        super().__init__(message)
        self.allow_fallback = allow_fallback


def _observe_outer_completion(result: Any, *, stream: bool, on_completed: Callable[[ChatResponse], None] | None) -> Any:
    """Observe the original call once, without inferring receipt of opaque inputs."""
    notified = False

    def completed(response: Any) -> Any:
        nonlocal notified
        if isinstance(response, ChatResponse) and not notified:
            notified = True
            if on_completed is not None:
                on_completed(cast("ChatResponse[Any]", response))
        return cast(Any, response)

    def observe(value: Any) -> Any:
        # ResponseStream is awaitable, but awaiting it only sets up the stream.
        # Retain its own finalizer and hooks, and observe only its final response.
        if isinstance(value, ResponseStream):
            return cast("ResponseStream[ChatResponseUpdate, ChatResponse]", value).with_result_hook(completed)
        return completed(value)

    if isinstance(result, ResponseStream):
        return observe(result)
    if isawaitable(result):
        pending = result
        if stream:

            async def resolve_stream() -> ResponseStream[ChatResponseUpdate, ChatResponse]:
                resolved = await pending
                if not isinstance(resolved, ResponseStream):
                    raise ValueError("Streaming completion observation requires a ResponseStream from the client.")
                return cast("ResponseStream[ChatResponseUpdate, ChatResponse]", resolved).with_result_hook(completed)

            return ResponseStream[ChatResponseUpdate, ChatResponse].from_awaitable(resolve_stream())

        async def resolve_response() -> Any:
            return observe(await pending)

        return resolve_response()
    if stream:
        raise ValueError("Streaming completion observation requires a ResponseStream from the client.")
    return observe(result)


def _unsupported_client(client: Any, reason: str, *, allow_fallback: bool = True) -> _UnsupportedServiceClient:
    return _UnsupportedServiceClient(
        f"Cannot observe exact service acceptance for {type(client).__name__}: {reason}. "
        "Exact acceptance requires a BaseChatClient with a rebindable _inner_get_response method, optionally "
        "behind wrappers delegating through stored inner or __wrapped__ attributes.",
        allow_fallback=allow_fallback,
    )


def _client_state(client: Any) -> dict[str, Any]:
    """Read stored state only, without evaluating properties or SDK attributes."""
    try:
        state = dict(cast("dict[str, Any]", object.__getattribute__(client, "__dict__")))
    except AttributeError:
        state = {}
    client_type = cast("type[Any]", type(client))
    for cls in client_type.__mro__:
        for name, descriptor in vars(cls).items():
            if isinstance(descriptor, MemberDescriptorType):
                # An uninitialized slot must stay uninitialized.
                with suppress(AttributeError):
                    state[name] = descriptor.__get__(client, client_type)
    return state


def _client_method(client: Any, name: str, state: dict[str, Any]) -> Callable[..., Any]:
    """Resolve a Python method without silently discarding instance overrides."""
    method: Any = getattr_static(client, name, None)
    if isinstance(method, MethodType) and method.__self__ is client:
        return method.__func__
    if isinstance(method, FunctionType) and name not in state:
        return method
    raise _unsupported_client(client, f"{name} is not a Python method bound to this client")


def _clone_service_client(
    client: Any,
    accept: Callable[[Sequence[Message]], None],
    on_completed: Callable[[ChatResponse], None] | None,
) -> Any:
    # Only these declared delegation edges are followed. In particular, a provider's
    # SDK `client` attribute is never searched for a chat-client-shaped object.
    chain: list[tuple[Any, dict[str, Any], list[str]]] = []
    seen: set[int] = set()
    current = client
    while True:
        if id(current) in seen:
            raise _unsupported_client(client, "the delegation chain contains a cycle", allow_fallback=False)
        seen.add(id(current))
        current_type = cast("type[Any]", type(current))
        if getattr_static(current_type, "__getattribute__") is not object.__getattribute__:
            raise _unsupported_client(client, "custom attribute lookup can bypass the isolated provider leaf")
        state = _client_state(current)
        _client_method(current, "get_response", state)
        if isinstance(current, BaseChatClient):
            current = cast(Any, current)
            leaf = _client_method(current, "_inner_get_response", state)
            chain.append((current, state, []))
            break
        links = [name for name in ("inner", "__wrapped__") if name in state and state[name] is not None]
        if not links:
            raise _unsupported_client(client, "no stored delegation chain reaches BaseChatClient")
        target = state[links[0]]
        if any(state[name] is not target for name in links):
            raise _unsupported_client(client, "inner and __wrapped__ designate different clients")
        chain.append((current, state, links))
        current = target

    clones: dict[int, Any] = {}
    try:
        for original, _, _ in chain:
            # Bypass user __new__, __init__, __copy__ and serialization hooks.
            original_type = cast("type[Any]", type(original))
            clones[id(original)] = object.__new__(original_type)
        for original, state, links in chain:
            clone = clones[id(original)]
            for name, value in state.items():
                if name in links:
                    value = clones[id(value)]
                elif isinstance(value, MethodType) and id(value.__self__) in clones:
                    value = MethodType(value.__func__, clones[id(value.__self__)])
                object.__setattr__(clone, name, value)
        leaf_clone = clones[id(current)]
        bound_leaf = MethodType(leaf, leaf_clone)

        def observed_leaf(
            *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
        ) -> Any:
            # Snapshot before the provider can mutate messages or polling options.
            inputs = deepcopy(list(messages)) if options.get("continuation_token") is None else None
            notified = False

            def completed(response: ChatResponse) -> ChatResponse:
                nonlocal notified
                if not isinstance(response, ChatResponse):
                    raise ValueError("Exact service acceptance requires a finalized ChatResponse from the provider.")
                if not notified:
                    notified = True
                    try:
                        if inputs is not None:
                            accept(inputs)
                    finally:
                        if on_completed is not None:
                            on_completed(response)
                return response

            result = bound_leaf(messages=messages, stream=stream, options=options, **kwargs)
            # ResponseStream is itself awaitable. Do not await or reconstruct it:
            # its own finalizer and existing hooks must produce the observed result.
            if isinstance(result, ResponseStream):
                return cast("ResponseStream[ChatResponseUpdate, ChatResponse]", result).with_result_hook(completed)
            if isawaitable(result):
                pending = result
                if stream:

                    async def resolve_stream() -> ResponseStream[ChatResponseUpdate, ChatResponse]:
                        resolved = await pending
                        if not isinstance(resolved, ResponseStream):
                            raise ValueError(
                                "Streaming service acceptance requires a ResponseStream from the provider."
                            )
                        return cast("ResponseStream[ChatResponseUpdate, ChatResponse]", resolved).with_result_hook(
                            completed
                        )

                    return ResponseStream[ChatResponseUpdate, ChatResponse].from_awaitable(resolve_stream())

                async def resolve_response() -> ChatResponse:
                    return completed(await pending)

                return resolve_response()
            if stream:
                raise ValueError("Streaming service acceptance requires a ResponseStream from the provider.")
            return completed(result)

        object.__setattr__(leaf_clone, "_inner_get_response", observed_leaf)
    except (AttributeError, TypeError) as exc:
        raise _unsupported_client(client, "its instance state cannot be isolated without reconstruction") from exc
    return clones[id(client)]
