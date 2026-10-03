# Copyright (c) Microsoft. All rights reserved.

"""Shared append-boundary observation and copy-failure helpers for history tests."""

from collections.abc import Callable, Sequence
from typing import Any

from _history_atomicity_test_support import _reference_check, _snapshot
from agent_framework import AgentResponse, Message

from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    DurableHistoryProvider,
)
from agent_framework_durabletask._response_utils import serialize_input_message

CORRELATION = "append-review"


class _CopyFailure:
    """Harmless copy probe. Its only side effect is an in-memory counter."""

    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.calls = 0

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        self.calls += 1
        raise self.error


def _append_snapshot(
    binding: DurableHistoryBinding,
    state: dict[str, Any] | None,
    messages: Sequence[Message],
    response: AgentResponse | None,
) -> Callable[[], None]:
    """Capture after a successful flush, without copying opaque caller payloads."""
    owner = binding.state_provider
    canonical = owner.state
    captured = canonical.to_dict()
    before = _snapshot(captured)
    binding_fields = dict(vars(binding))
    objects: list[Any] = [owner, canonical, canonical.data]
    for entry in canonical.data.conversation_history:
        objects.append(entry)
        for stored in entry.messages:
            objects.extend((stored, *stored.contents))
    caller_messages = list(messages)
    buffered = state.get(WORKING_BUFFER_KEY, []) if state is not None else []
    for message in [*caller_messages, *buffered, *binding.pending_inputs]:
        objects.extend((message, *message.contents))
    if response is not None:
        objects.append(response)
    check_references = _reference_check(state, messages, *binding_fields.values(), *(vars(value) for value in objects))
    caller_wire = _snapshot([serialize_input_message(message) for message in caller_messages])
    buffer_wire = _snapshot([serialize_input_message(message) for message in buffered])

    def check() -> None:
        assert owner.state is canonical
        # after_run restores its temporary response marker in finally. Check that
        # separately at the caller, while keeping every other binding field exact.
        assert vars(binding).keys() == binding_fields.keys()
        for name, value in binding_fields.items():
            if name != "append_response":
                assert getattr(binding, name) is value, name
        check_references()
        assert _snapshot(canonical.to_dict()) == before
        assert _snapshot(captured) == before
        assert _snapshot([serialize_input_message(message) for message in caller_messages]) == caller_wire
        assert _snapshot([serialize_input_message(message) for message in buffered]) == buffer_wire

    return check


class _ObservedAppend(DurableHistoryProvider):
    """Observe the real append entry point without replacing any of its work."""

    check_last_append: Callable[[], None] | None = None

    def _append_messages(
        self,
        binding: DurableHistoryBinding,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None,
        response: AgentResponse | None = None,
    ) -> None:
        self.check_last_append = _append_snapshot(binding, state, messages, response)
        super()._append_messages(binding, messages, state=state, response=response)

    def assert_unchanged(self) -> None:
        assert self.check_last_append is not None, "the real append boundary was not reached"
        self.check_last_append()


async def _working(provider: DurableHistoryProvider, mode: str) -> dict[str, Any] | None:
    if mode == "none":
        return None
    state: dict[str, Any] = {"caller": {"keep": [None, False, 0, 0.0]}}
    if mode == "loaded":
        await provider.get_messages("session", state=state)
    else:
        assert mode == "absent" and WORKING_BUFFER_KEY not in state and POSITIONS_KEY not in state
    return state
