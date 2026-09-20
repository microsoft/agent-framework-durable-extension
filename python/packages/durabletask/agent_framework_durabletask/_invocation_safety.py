# Copyright (c) Microsoft. All rights reserved.

"""Run-local observation without reconstructing caller-owned clients."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from inspect import getattr_static, isawaitable
from typing import Any, cast

from agent_framework import (
    BaseChatClient,
    ChatContext,
    ChatMiddleware,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    FunctionInvocationContext,
    FunctionInvocationLayer,
    FunctionMiddleware,
    Message,
    ResponseStream,
)
from agent_framework._compaction import project_included_messages
from agent_framework.observability import ChatTelemetryLayer


@dataclass
class InvocationProgress:
    """Track observable progress that makes restarting a whole agent run unsafe."""

    stream_started: bool = False
    function_started: bool = False
    service_completed: bool = False


class DurableToolGuard(FunctionMiddleware):
    """Prevent callable execution through Core's public per-run middleware contract.

    Custom clients executing tools outside that contract own their side effects.
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
    """Retain the legacy pre-compaction middleware-input observation contract."""

    def __init__(self, accept: Callable[[Sequence[Message]], None]) -> None:
        self._accept = accept

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        inputs = deepcopy(list(context.messages))
        await call_next()
        context.result = _observe_completion(
            context.result, stream=context.stream, on_completed=lambda _: self._accept(inputs)
        )


@dataclass
class _RequestObservation:
    observed: bool = False


class DurableServiceClient:
    """Observe supported Core requests while always executing the original client.

    A final per-call chat middleware observes an existing Core pipeline. A plain
    Base call, or a standard chain without an active function loop or middleware,
    can instead be observed directly. A compaction tap runs the original effective
    strategy, then snapshots Core's own projection before provider dispatch.

    No strategy is inserted when none was configured. Tokenizer-only preparation,
    custom entry/preparation methods, and opaque wrappers are completion-only.
    A standard function loop can add its first chat observer only when neither
    compaction nor a tokenizer is effective. Otherwise, without an existing chat
    pipeline it remains completion-only: completed early leaves are not reported
    if a later leaf fails.

    Core 1.13 and 1.16 pass their loop-local prepared_messages list directly to
    each service call. The first chat middleware copies that list, so compaction
    insertions no longer reach the next iteration although shared message
    exclusions do. Neither the outer caller's input nor the session is that
    loop-local list. Supporting this path without changing its compaction behavior
    needs an observation seam that preserves the loop's list ownership.
    Completion-only fallbacks do not claim acceptance or authorize whole-agent retry.

    No client attributes are written. Core may update its own counters and caches
    as usual. Wrapper closures and hidden aliases are never inspected or rebound.
    This is a run-local observer, not a concurrency primitive.
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
        self._accept = accept
        self._on_completed = on_completed
        self._request = _RequestObservation()

    @property
    def observed_request(self) -> bool:
        """Whether the latest attempt reached a supported request snapshot.

        Read AFTER a failure, together with InvocationProgress, before considering
        retry. False means unknown, including short circuits and failed compaction.
        True describes prepared Core inputs, not remote receipt or wire encoding.
        A completed response is separately required before accepting those inputs.
        """
        return self._request.observed

    @property
    def exact_acceptance(self) -> bool:
        """Compatibility alias for dynamic observed_request, not a capability flag."""
        return self.observed_request

    def reset_observation(self) -> None:
        """Discard prior evidence before a new whole-agent invocation begins.

        Agent middleware can fail before the client is reached. Resetting only
        inside get_response would then leave stale evidence from an earlier try.
        """
        self._request = _RequestObservation()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__wrapped__, name)

    def get_response(self, messages: Sequence[Message], **kwargs: Any) -> Any:
        request = self._request = _RequestObservation()
        client = self.__wrapped__
        layout = _core_layout(client)
        raw_runtime = kwargs.get("client_kwargs")
        if raw_runtime is None:
            raw_runtime = cast("dict[str, Any]", {})
        if layout is not None and type(raw_runtime) is dict:
            runtime = cast("dict[str, Any]", raw_runtime)
            has_chat, has_functions = layout
            call_middleware = runtime.get("middleware", [])
            extra_middleware = kwargs.get("middleware")
            if extra_middleware is None:
                extra_middleware = cast("list[Any]", [])
            # Unknown middleware containers keep Core's original normalization.
            # Only known chat instances prove a runtime pipeline is active.
            if type(call_middleware) in (list, tuple) and type(extra_middleware) in (list, tuple):
                configured = getattr_static(client, "chat_middleware", [])
                active_chat = has_chat and (
                    bool(configured)
                    or any(isinstance(item, ChatMiddleware) for item in [*call_middleware, *extra_middleware])
                )
                configuration = getattr_static(client, "function_invocation_configuration", {})
                strategy, tokenizer = _effective_preparation(client, kwargs)
                plain_function_loop = (
                    has_chat
                    and has_functions
                    and configuration.get("enabled", True) is not False
                    and strategy is None
                    and tokenizer is None
                    # Installing a first chat pipeline would promote these nested
                    # provider kwargs to Core preparation overrides. Do not do so.
                    and runtime.get("compaction_strategy") is None
                    and runtime.get("tokenizer") is None
                    and all(isinstance(item, FunctionMiddleware) for item in [*call_middleware, *extra_middleware])
                )
                if active_chat or plain_function_loop:
                    observer = _FinalServiceObserver(client, request, self._accept, self._on_completed)
                    kwargs = dict(kwargs)
                    if extra_middleware:
                        kwargs["middleware"] = [*extra_middleware, observer]
                    else:
                        kwargs["client_kwargs"] = {**runtime, "middleware": [*call_middleware, observer]}
                    # Do not observe outer synthetic/short-circuit responses here.
                    return client.get_response(messages=messages, **kwargs)
                # Unclassified runtime callables/bundles may create a pipeline
                # that rewrites or short-circuits inputs. Never snapshot outside it.
                plain_dispatch = not has_chat or (not call_middleware and not extra_middleware)
                if plain_dispatch and (not has_functions or configuration.get("enabled", True) is False):
                    receipt = _ServiceReceipt(request, self._accept, self._on_completed)
                    prepared_kwargs = dict(kwargs)
                    receipt.prepare(client, messages, prepared_kwargs, kwargs.get("options"))
                    return receipt.observe(
                        client.get_response(messages=messages, **prepared_kwargs), stream=bool(kwargs.get("stream"))
                    )

        # Preserve original arguments, scalar state, aliases, and return behavior.
        result = client.get_response(messages=messages, **kwargs)
        return _observe_completion(result, stream=bool(kwargs.get("stream")), on_completed=self._on_completed)


def _core_layout(client: Any) -> tuple[bool, bool] | None:
    """Recognize actual Core method identities, never module-name lookalikes."""
    if not isinstance(client, BaseChatClient):
        return None
    client = cast("BaseChatClient[Any]", client)
    client_type = cast("type[Any]", type(client))
    if getattr_static(client_type, "__getattribute__") is not object.__getattribute__:
        return None
    state = cast("dict[str, Any]", object.__getattribute__(client, "__dict__"))
    if "get_response" in state:
        return None
    layers = (BaseChatClient, ChatMiddlewareLayer, FunctionInvocationLayer, ChatTelemetryLayer)
    entry = getattr_static(client, "get_response", None)
    if not any(entry is vars(layer)["get_response"] for layer in layers):
        return None
    chain: list[type[Any]] = []
    for cls in client_type.__mro__:
        if "get_response" in vars(cls):
            if cls not in layers:
                return None
            chain.append(cls)
        if cls is BaseChatClient:
            break
    if not chain or chain[-1] is not BaseChatClient:
        return None
    has_chat = ChatMiddlewareLayer in chain
    has_functions = FunctionInvocationLayer in chain
    if has_chat and has_functions and chain.index(FunctionInvocationLayer) > chain.index(ChatMiddlewareLayer):
        return None

    # A known public entry is insufficient if it dispatches an overridden helper.
    seams: list[tuple[type[Any], tuple[str, ...]]] = [
        (BaseChatClient, ("_resolve_compaction_overrides", "_prepare_messages_for_model_call")),
    ]
    if has_chat:
        seams.append((ChatMiddlewareLayer, ("_middleware_handler", "_get_chat_middleware_pipeline")))
        if not isinstance(getattr_static(client, "chat_middleware", None), list):
            return None
    if has_functions:
        seams.append((
            FunctionInvocationLayer,
            (
                "_get_response_with_function_invocation",
                "_stream_response_with_function_invocation",
                "_get_function_middleware_pipeline",
            ),
        ))
        if not isinstance(getattr_static(client, "function_invocation_configuration", None), dict):
            return None
    for layer, names in seams:
        for name in names:
            if name in state or getattr_static(client, name, None) is not vars(layer)[name]:
                return None
    for name in ("compaction_strategy", "tokenizer"):
        # A computed default can change between observation and Core resolution.
        descriptor_type = cast("type[Any]", type(getattr_static(client_type, name, None)))
        if hasattr(descriptor_type, "__set__"):
            return None
        default_type = cast("type[Any]", type(getattr_static(client, name, None)))
        if name not in state and hasattr(default_type, "__get__"):
            return None
    return has_chat, has_functions


def _effective_preparation(client: Any, kwargs: Mapping[str, Any]) -> tuple[Any, Any]:
    """Resolve standard Core overrides, where None preserves the client default."""
    strategy = kwargs.get("compaction_strategy")
    if strategy is None:
        strategy = getattr_static(client, "compaction_strategy", None)
    tokenizer = kwargs.get("tokenizer")
    if tokenizer is None:
        tokenizer = getattr_static(client, "tokenizer", None)
    return strategy, tokenizer


class _ServiceReceipt:
    def __init__(
        self,
        request: _RequestObservation,
        accept: Callable[[Sequence[Message]], None],
        on_completed: Callable[[ChatResponse], None] | None,
    ) -> None:
        self._request = request
        self._accept = accept
        self._on_completed = on_completed
        self._inputs: list[Message] | None = None
        self._retrieval = False

    def prepare(
        self, client: Any, messages: Sequence[Message], kwargs: dict[str, Any], options: Mapping[str, Any] | None
    ) -> None:
        self._request.observed = False
        self._retrieval = options is not None and options.get("continuation_token") is not None
        strategy, tokenizer = _effective_preparation(client, kwargs)
        if strategy is not None:
            original_strategy = strategy

            async def tapped_strategy(working_messages: list[Message]) -> bool:
                changed = await original_strategy(working_messages)
                # Core repeats this pure projection immediately after the strategy
                # returns. Snapshot now, before provider or outer middleware writes.
                self._snapshot(project_included_messages(working_messages))
                return cast(bool, changed)

            kwargs["compaction_strategy"] = tapped_strategy
        elif tokenizer is None:
            # Core skips preparation entirely here, INCLUDING exclusion filtering.
            self._snapshot(messages)
        # Tokenizer-only preparation mutates annotations after this seam. Do not
        # insert a strategy or manufacture an "exact" pre-annotation snapshot.

    def _snapshot(self, messages: Sequence[Message]) -> None:
        if not self._retrieval:
            self._inputs = deepcopy(list(messages))
        self._request.observed = True

    def observe(self, result: Any, *, stream: bool) -> Any:
        return _observe_completion(result, stream=stream, on_completed=self._completed)

    def _completed(self, response: ChatResponse) -> None:
        try:
            if self._inputs is not None:
                self._accept(self._inputs)
        finally:
            if self._on_completed is not None:
                self._on_completed(response)


class _FinalServiceObserver(ChatMiddleware):
    def __init__(
        self,
        client: Any,
        request: _RequestObservation,
        accept: Callable[[Sequence[Message]], None],
        on_completed: Callable[[ChatResponse], None] | None,
    ) -> None:
        self._client = client
        self._request = request
        self._accept = accept
        self._on_completed = on_completed

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self._request.observed = False
        if context.client is not self._client or _core_layout(context.client) is None:
            await call_next()
            return
        receipt = _ServiceReceipt(self._request, self._accept, self._on_completed)
        receipt.prepare(context.client, context.messages, context.kwargs, context.options)
        await call_next()
        context.result = receipt.observe(context.result, stream=context.stream)
        # Resolve only stream setup, never drain. Already-finalized streams use a
        # deferred getter and must notify before outer middleware resumes.
        result = context.result
        if isinstance(result, ResponseStream):
            await cast("ResponseStream[ChatResponseUpdate, ChatResponse]", result)


def _observe_completion(result: Any, *, stream: bool, on_completed: Callable[[ChatResponse], None] | None) -> Any:
    """Observe once without draining streams or replacing provider finalization."""
    notified = False

    def completed(response: Any) -> Any:
        nonlocal notified
        if isinstance(response, ChatResponse) and not notified:
            notified = True
            if on_completed is not None:
                on_completed(cast("ChatResponse[Any]", response))
        return cast(Any, response)

    def observe(value: Any) -> Any:
        if isinstance(value, ResponseStream):
            response_stream = cast("ResponseStream[ChatResponseUpdate, ChatResponse]", value)
            # Core has no public completion flag. Read this single flag only:
            # with_result_hook clears it and would rerun a completed finalizer.
            if getattr_static(response_stream, "_finalized", False) is True:

                async def observe_finalized() -> ResponseStream[ChatResponseUpdate, ChatResponse]:
                    completed(await response_stream.get_final_response())
                    return response_stream

                return ResponseStream[ChatResponseUpdate, ChatResponse].from_awaitable(observe_finalized())
            return response_stream.with_result_hook(completed)
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
                return cast("ResponseStream[ChatResponseUpdate, ChatResponse]", observe(resolved))

            return ResponseStream[ChatResponseUpdate, ChatResponse].from_awaitable(resolve_stream())

        async def resolve_response() -> Any:
            return observe(await pending)

        return resolve_response()
    if stream:
        raise ValueError("Streaming completion observation requires a ResponseStream from the client.")
    return observe(result)
