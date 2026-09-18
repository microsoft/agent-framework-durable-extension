# Copyright (c) Microsoft. All rights reserved.

"""Review tests for durable compaction ID scoping and restoration."""

from __future__ import annotations

import pytest
from agent_framework import CompactionProvider, Message, SessionContext

from agent_framework_durabletask._history_provider import _DurableCompactionProvider


def _message(text: str, *, public_id: str | None, durable_id: str | None) -> Message:
    message = Message("user", [text], message_id=public_id)
    if durable_id is not None:
        message._durable_history_id = durable_id  # type: ignore[attr-defined]
    return message


@pytest.mark.parametrize("hook", ["before", "after"])
async def test_compaction_scope_restores_original_objects_after_id_rename_insertion_and_removal(hook: str) -> None:
    original = _message("original", public_id="public-original", durable_id="private-original")
    removed = _message("removed", public_id="public-removed", durable_id="private-removed")
    inserted = _message("inserted", public_id="public-inserted", durable_id=None)
    replacement = _message("replacement", public_id="public-replacement", durable_id=None)
    strategy_called = False

    async def before_strategy(messages: list[Message]) -> bool:
        nonlocal strategy_called
        strategy_called = True
        assert [message.message_id for message in messages] == ["private-original", "private-removed"]
        messages[0].message_id = "private-original-renamed"
        messages.pop(1)
        replacement.message_id = "private-original"
        messages.extend([inserted, replacement])
        return True

    async def after_strategy(messages: list[Message]) -> bool:
        nonlocal strategy_called
        strategy_called = True
        assert [message.message_id for message in messages] == ["private-original", "private-removed"]
        messages[0].message_id = "private-original-renamed"
        messages.pop(1)
        replacement.message_id = "private-removed"
        messages.extend([replacement, inserted])
        return True

    provider = _DurableCompactionProvider(
        CompactionProvider(
            before_strategy=before_strategy if hook == "before" else None,
            after_strategy=after_strategy if hook == "after" else None,
            history_source_id="durable",
        )
    )

    context = SessionContext(input_messages=[])
    if hook == "before":
        context.extend_messages("durable", [original, removed])
        # Check restoration on the context-owned copies passed to the strategy.
        original, removed = context.get_messages(sources={"durable"})
        await provider.before_run(agent=None, session=None, context=context, state={})
    else:
        parked = [original, removed]
        session = type("Session", (), {"state": {"durable": {"messages": parked}}})()
        await provider.after_run(agent=None, session=session, context=context, state={})

    assert strategy_called
    assert original.message_id == "public-original"
    assert removed.message_id == "public-removed"
    assert inserted.message_id == "public-inserted"
    assert replacement.message_id == ("private-removed" if hook == "after" else "private-original")


@pytest.mark.parametrize("hook", ["before", "after"])
async def test_compaction_scope_restores_removed_original_objects_when_strategy_raises(hook: str) -> None:
    original = _message("original", public_id="public-original", durable_id="private-original")
    removed = _message("removed", public_id="public-removed", durable_id="private-removed")
    strategy_called = False

    async def strategy(messages: list[Message]) -> bool:
        nonlocal strategy_called
        strategy_called = True
        assert [message.message_id for message in messages] == ["private-original", "private-removed"]
        messages[0].message_id = "private-original-renamed"
        messages.pop()
        raise RuntimeError("boom")

    provider = _DurableCompactionProvider(
        CompactionProvider(
            before_strategy=strategy if hook == "before" else None,
            after_strategy=strategy if hook == "after" else None,
            history_source_id="durable",
        )
    )

    context = SessionContext(input_messages=[])
    with pytest.raises(RuntimeError, match="boom"):
        if hook == "before":
            context.extend_messages("durable", [original, removed])
            # Check restoration on the context-owned copies passed to the strategy.
            original, removed = context.get_messages(sources={"durable"})
            await provider.before_run(agent=None, session=None, context=context, state={})
        else:
            parked = [original, removed]
            session = type("Session", (), {"state": {"durable": {"messages": parked}}})()
            await provider.after_run(agent=None, session=session, context=context, state={})

    assert strategy_called
    assert original.message_id == "public-original"
    assert removed.message_id == "public-removed"
