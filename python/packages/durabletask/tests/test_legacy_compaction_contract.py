# Copyright (c) Microsoft. All rights reserved.

"""Legacy final-buffer compaction preserves history, identities and cold replay."""

from collections.abc import Sequence
from copy import deepcopy
from typing import Any

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from _history_pipeline_test_support import ToolChatClient, lookup
from agent_framework import (
    GROUP_ANNOTATION_KEY,
    SUMMARIZED_BY_SUMMARY_ID_KEY,
    SUMMARY_OF_MESSAGE_IDS_KEY,
    Agent,
    CompactionProvider,
    ContextProvider,
    InMemoryHistoryProvider,
    Message,
    included_token_count,
)
from test_agent_compatibility_prototype import _evidence, _legacy

from agent_framework_durabletask import (
    CompatibleAgentEntity,
    DurableAgentState,
    LegacyMigrationEvidence,
    RunRequest,
    read_agent_state,
    serialize_agent_response,
    state_snapshot_digest,
)
from agent_framework_durabletask._shared_response import serialize_terminal_response


class _LateHistoryInsert(ContextProvider):
    def __init__(self, enabled: bool) -> None:
        super().__init__("late")
        self.enabled = enabled

    async def after_run(self, *, session: Any, **kwargs: Any) -> None:
        if self.enabled:
            session.state[InMemoryHistoryProvider.DEFAULT_SOURCE_ID]["messages"].append(Message("system", ["late"]))


@pytest.mark.parametrize("late", [False, True])
async def test_late_buffer_insertions_survive_cold_continuation(late: bool) -> None:
    async def compact(messages: list[Message]) -> bool:
        return False

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                _LateHistoryInsert(late),
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await entity.run(RunRequest("new", correlation_id="first"))
    assert not result.additional_properties.get("durable_status")
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client), state_provider=JsonStateProvider(provider.raw), deployment_mode="legacy"
    )
    await cold.run(RunRequest("next", correlation_id="next"))
    expected = ["old question", "old answer", "new", "reply-1", *(["late"] if late else []), "next"]
    assert [message.text for message in client.received_messages[0]] == expected


@pytest.mark.parametrize("current", [False, True])
@pytest.mark.parametrize("nested", [False, True])
async def test_in_place_summary_keeps_original_source_and_derived_identities_distinct(
    current: bool, nested: bool
) -> None:
    original_text = "new" if current else "old question"

    async def compact(messages: list[Message]) -> bool:
        source = next(message for message in messages if message.text == original_text)
        original_id = source.message_id
        source.role = "system"
        source.contents = Message("system", ["summary"]).contents
        group = source.additional_properties[GROUP_ANNOTATION_KEY] if nested else source.additional_properties
        group[SUMMARY_OF_MESSAGE_IDS_KEY] = [original_id]
        group[SUMMARIZED_BY_SUMMARY_ID_KEY] = original_id
        return True

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await entity.run(RunRequest("new", correlation_id="first"))
    assert not result.additional_properties.get("durable_status")
    raw = provider.raw
    assert raw["data"]["extensionData"]["_dafx_legacy_history"]["generated"]
    reference = raw["data"]["extensionData"]["_dafx_legacy_history"]["generated"][0]
    index, offset = (int(part) for part in reference.split(":"))
    summary = raw["data"]["conversationHistory"][index]["messages"][offset]
    group = summary["extensionData"][GROUP_ANNOTATION_KEY] if nested else summary["extensionData"]
    source_id = group[SUMMARY_OF_MESSAGE_IDS_KEY][0]
    assert source_id != f"dafx_legacy:{reference}"
    index, offset = (int(part) for part in source_id.removeprefix("dafx_legacy:").split(":"))
    archived = raw["data"]["conversationHistory"][index]["messages"][offset]
    assert archived["contents"][0]["text"] == original_text
    assert archived["extensionData"]["_excluded"] is True
    source_properties = archived["extensionData"][GROUP_ANNOTATION_KEY] if nested else archived["extensionData"]
    assert source_properties[SUMMARIZED_BY_SUMMARY_ID_KEY] == f"dafx_legacy:{reference}"
    assert SUMMARIZED_BY_SUMMARY_ID_KEY not in group


@pytest.mark.parametrize("same_text", [False, True])
async def test_in_place_summary_revisions_keep_their_complete_source_chain(same_text: bool) -> None:
    async def rewrite(messages: list[Message], text: str) -> bool:
        source = messages[0]
        identity = source.message_id
        source.role = "system"
        source.contents = Message("system", [text]).contents
        source.additional_properties[GROUP_ANNOTATION_KEY].update({
            SUMMARY_OF_MESSAGE_IDS_KEY: [identity],
            SUMMARIZED_BY_SUMMARY_ID_KEY: identity,
        })
        return True

    async def first(messages: list[Message]) -> bool:
        return await rewrite(messages, "summary-one")

    async def second(messages: list[Message]) -> bool:
        return await rewrite(messages, "summary-one" if same_text else "summary-two")

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=second, history_source_id=history.source_id, source_id="second"),
                CompactionProvider(after_strategy=first, history_source_id=history.source_id, source_id="first"),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await entity.run(RunRequest("new", correlation_id="first"))
    assert not result.additional_properties.get("durable_status")
    raw = provider.raw
    references = raw["data"]["extensionData"]["_dafx_legacy_history"]["generated"]
    assert len(references) == 2
    chain = ["0:0", *references]
    for source_reference, summary_reference in zip(chain, chain[1:]):
        source_index, source_offset = (int(part) for part in source_reference.split(":"))
        summary_index, summary_offset = (int(part) for part in summary_reference.split(":"))
        source = raw["data"]["conversationHistory"][source_index]["messages"][source_offset]
        summary = raw["data"]["conversationHistory"][summary_index]["messages"][summary_offset]
        assert source["extensionData"]["_excluded"] is True
        assert source["extensionData"][GROUP_ANNOTATION_KEY][SUMMARIZED_BY_SUMMARY_ID_KEY] == (
            f"dafx_legacy:{summary_reference}"
        )
        assert summary["extensionData"][GROUP_ANNOTATION_KEY][SUMMARY_OF_MESSAGE_IDS_KEY] == [
            f"dafx_legacy:{source_reference}"
        ]


@pytest.mark.parametrize("compact", [False, True])
async def test_after_compaction_state_survives_legacy_cold_replay(compact: bool) -> None:
    async def summarize(messages: list[Message]) -> bool:
        if not compact:
            return False
        for message in messages:
            if message.text in ("old question", "old answer"):
                message.additional_properties["_excluded"] = True
        messages.insert(0, Message("system", ["retained summary"]))
        return True

    def providers() -> list[CompactionProvider | InMemoryHistoryProvider]:
        history = InMemoryHistoryProvider()
        return [
            CompactionProvider(after_strategy=summarize, history_source_id=history.source_id),
            history,
        ]

    reference = Agent(client=RecordingChatClient(), context_providers=providers())
    session = reference.create_session()
    source_id = InMemoryHistoryProvider.DEFAULT_SOURCE_ID
    session.state[source_id] = {"messages": [Message("user", ["old question"]), Message("assistant", ["old answer"])]}
    await reference.run("new", session=session)
    persisted: Sequence[Message] = session.state[source_id]["messages"]
    expected = [message.text for message in persisted if not message.additional_properties.get("_excluded")] + ["next"]
    assert len(persisted) == (5 if compact else 4)
    assert [message.text for message in persisted if message.additional_properties.get("_excluded")] == (
        ["old question", "old answer"] if compact else []
    )

    provider = JsonStateProvider(_legacy())
    first = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), context_providers=providers()),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await first.run(RunRequest("new", correlation_id="first"))
    assert not result.additional_properties.get("durable_status")
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    result = await cold.run(RunRequest("next", correlation_id="next"))
    assert not result.additional_properties.get("durable_status")
    assert [message.text for message in client.received_messages[0]] == expected


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("public_ids", [False, True])
@pytest.mark.parametrize("skip_excluded", [False, True])
async def test_lossless_legacy_compaction_preserves_opaque_archive_and_lineage(
    version: str, public_ids: bool, skip_excluded: bool
) -> None:
    source = _legacy(version=version)
    original = deepcopy(source)
    for entry in source["data"]["conversationHistory"]:
        entry["futureEntry"] = {"keep": [False, 0, None]}
        entry["messages"][0]["futureMessage"] = {"opaque": [0.0, False]}
        if public_ids:
            entry["messages"][0]["messageId"] = "shared"
    before = deepcopy(source)
    seen: list[list[str | None]] = []

    async def summarize(messages: list[Message]) -> bool:
        seen.append([message.message_id for message in messages])
        targets = [message for message in messages if message.text in ("old question", "old answer")]
        for message in targets:
            message.additional_properties["_excluded"] = True
            message.additional_properties[GROUP_ANNOTATION_KEY][SUMMARIZED_BY_SUMMARY_ID_KEY] = "shared"
        messages.insert(
            1,
            Message(
                "system",
                ["summary"],
                message_id="shared",
                additional_properties={
                    GROUP_ANNOTATION_KEY: {
                        SUMMARY_OF_MESSAGE_IDS_KEY: [message.message_id for message in targets],
                        "futureGroup": {"keep": True},
                    }
                },
            ),
        )
        return True

    history = InMemoryHistoryProvider(skip_excluded=skip_excluded)
    provider = JsonStateProvider(source)
    first = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=summarize, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await first.run(RunRequest("new", correlation_id="first"))
    assert not result.additional_properties.get("durable_status")
    assert source == before
    assert seen and all(seen[0]) and len(set(seen[0])) == len(seen[0])
    raw = provider.raw
    assert raw["schemaVersion"] == version and raw["futureRoot"] == original["futureRoot"]
    assert raw["data"]["futureData"] == original["data"]["futureData"]
    for index in (0, 1):
        retained = raw["data"]["conversationHistory"][index]
        assert retained["futureEntry"] == before["data"]["conversationHistory"][index]["futureEntry"]
        assert retained["messages"][0]["futureMessage"] == {"opaque": [0.0, False]}
        assert (
            retained["messages"][0]["contents"]
            == before["data"]["conversationHistory"][index]["messages"][0]["contents"]
        )
        assert retained["messages"][0].get("messageId") == ("shared" if public_ids else None)
        assert retained["messages"][0]["extensionData"]["_excluded"] is True
    declared = raw["data"]["extensionData"]["_dafx_legacy_history"]
    assert len(declared["generated"]) == 1
    derived = next(
        message
        for entry in raw["data"]["conversationHistory"]
        for message in entry["messages"]
        if message["contents"][0].get("text") == "summary"
    )
    assert derived["messageId"] == "shared"
    assert derived["extensionData"][GROUP_ANNOTATION_KEY]["futureGroup"] == {"keep": True}
    assert derived["extensionData"][GROUP_ANNOTATION_KEY][SUMMARY_OF_MESSAGE_IDS_KEY] == [
        "dafx_legacy:0:0",
        "dafx_legacy:1:0",
    ]
    historical = read_agent_state(raw).try_get_agent_response("old")
    assert historical is not None and historical.text == "old answer"
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client, context_providers=[InMemoryHistoryProvider(skip_excluded=skip_excluded)]),
        state_provider=JsonStateProvider(raw),
        deployment_mode="legacy",
    )
    await cold.run(RunRequest("next", correlation_id="next"))
    expected = ["summary", "new", "reply-1", "next"]
    if not skip_excluded:
        expected = ["old question", "summary", "old answer", "new", "reply-1", "next"]
    assert [message.text for message in client.received_messages[0]] == expected


@pytest.mark.parametrize("change", ["remove", "rewrite", "reorder"])
async def test_final_buffer_changes_do_not_delete_original_legacy_bodies(change: str) -> None:
    async def compact(messages: list[Message]) -> bool:
        if change == "remove":
            messages[:] = [message for message in messages if message.text not in ("old question", "old answer")]
            messages.insert(0, Message("system", ["summary"]))
        elif change == "rewrite":
            messages[0].contents[0].text = "rewritten"
        else:
            messages[0], messages[1] = messages[1], messages[0]
        return True

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await entity.run(RunRequest("new", correlation_id="first"))
    assert not result.additional_properties.get("durable_status")
    assert [
        entry["messages"][0]["contents"][0]["text"] for entry in provider.raw["data"]["conversationHistory"][:2]
    ] == ["old question", "old answer"]
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client), state_provider=JsonStateProvider(provider.raw), deployment_mode="legacy"
    )
    await cold.run(RunRequest("next", correlation_id="next"))
    prefix = (
        ["summary"]
        if change == "remove"
        else ["rewritten", "old answer"]
        if change == "rewrite"
        else ["old answer", "old question"]
    )
    assert [message.text for message in client.received_messages[0]] == [*prefix, "new", "reply-1", "next"]


async def test_generated_summary_is_not_a_delivery_receipt_during_conversion() -> None:
    async def summarize(messages: list[Message]) -> bool:
        for message in messages[:2]:
            message.additional_properties["_excluded"] = True
        messages.insert(0, Message("system", ["summary"], message_id="generated-not-delivered"))
        return True

    history = InMemoryHistoryProvider()
    source = _legacy()
    prefix = {
        "$type": "request",
        "correlationId": "empty",
        "createdAt": source["data"]["conversationHistory"][0]["createdAt"],
        "messages": [],
        "futureEnvelope": {"keep": [False, 0, None]},
    }
    source["data"]["conversationHistory"].insert(0, prefix)
    provider = JsonStateProvider(source)
    first = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=summarize, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    response = await first.run(RunRequest("new", correlation_id="first"))
    assert not response.additional_properties.get("durable_status")
    completed_at = provider.raw["data"]["conversationHistory"][-1]["createdAt"]

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        completion = _evidence(raw).completion_evidence
        assert completion is not None
        completion["results"].append({
            "correlationId": "first",
            "outcome": "succeeded",
            "completedAt": completed_at,
            "response": serialize_terminal_response(serialize_agent_response(response)),
        })
        return LegacyMigrationEvidence(
            completion_evidence=completion,
            delivery_evidence={
                "sourceDigest": state_snapshot_digest(raw),
                "evidenceId": "no-public-inputs",
                "complete": True,
                "messages": [],
            },
        )

    client = RecordingChatClient()
    converted = CompatibleAgentEntity(Agent(client=client), state_provider=provider, legacy_migration_evidence=evidence)
    result = await converted.run(RunRequest("convert", correlation_id="convert"))
    assert not result.additional_properties.get("durable_status")
    assert provider.raw["schemaVersion"] == "2.0.0"
    assert provider.raw["data"]["conversationHistory"][0] == prefix
    assert [message.text for message in client.received_messages[0]] == ["summary", "new", "reply-1", "convert"]
    assert DurableAgentState.from_dict(provider.raw).data.ingested_messages == {}


@pytest.mark.parametrize("copied", [False, True])
async def test_copied_summary_is_a_new_occurrence_without_changing_public_source_ids(copied: bool) -> None:
    async def compact(messages: list[Message]) -> bool:
        summary = deepcopy(messages[0]) if copied else Message("system", ["summary"])
        summary.role = "system"
        summary.contents = Message("system", ["summary"]).contents
        if copied:
            summary.additional_properties[GROUP_ANNOTATION_KEY] = {
                SUMMARY_OF_MESSAGE_IDS_KEY: [message.message_id for message in messages[:2]]
            }
        for message in messages[:2]:
            message.additional_properties["_excluded"] = True
            if copied:
                message.additional_properties[GROUP_ANNOTATION_KEY][SUMMARIZED_BY_SUMMARY_ID_KEY] = summary.message_id
        messages.insert(0, summary)
        return True

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    response = await entity.run(RunRequest("new", correlation_id="first"))
    assert not response.additional_properties.get("durable_status")
    if copied:
        generated = provider.raw["data"]["extensionData"]["_dafx_legacy_history"]["generated"][0]
        for entry in provider.raw["data"]["conversationHistory"][:2]:
            assert entry["messages"][0]["extensionData"][GROUP_ANNOTATION_KEY][SUMMARIZED_BY_SUMMARY_ID_KEY] == (
                f"dafx_legacy:{generated}"
            )
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client), state_provider=JsonStateProvider(provider.raw), deployment_mode="legacy"
    )
    await cold.run(RunRequest("next", correlation_id="next"))
    assert [message.text for message in client.received_messages[0]] == ["summary", "new", "reply-1", "next"]


@pytest.mark.parametrize("reorder", [False, True])
async def test_order_materialization_preserves_empty_terminal_envelopes(reorder: bool) -> None:
    async def compact(messages: list[Message]) -> bool:
        if reorder and any(message.text == "three" for message in messages):
            messages.reverse()
            return True
        return False

    history = InMemoryHistoryProvider(store_outputs=False)
    provider = JsonStateProvider(_legacy(used=False))
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    results: list[dict[str, Any]] = []
    for value in ("one", "two", "three"):
        response = await entity.run(RunRequest(value, correlation_id=value))
        assert not response.additional_properties.get("durable_status")
        results.append({
            "correlationId": value,
            "outcome": "succeeded",
            "completedAt": provider.raw["data"]["conversationHistory"][-1]["createdAt"],
            "response": serialize_terminal_response(serialize_agent_response(response)),
        })
    original_empty = [entry for entry in provider.raw["data"]["conversationHistory"] if not entry["messages"]]

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        return LegacyMigrationEvidence(
            completion_evidence={
                "sourceDigest": state_snapshot_digest(raw),
                "evidenceId": "observed-completions",
                "complete": True,
                "results": results,
            }
        )

    client = RecordingChatClient()
    converted = CompatibleAgentEntity(
        Agent(client=client, context_providers=[InMemoryHistoryProvider(store_outputs=False)]),
        state_provider=provider,
        legacy_migration_evidence=evidence,
    )
    response = await converted.run(RunRequest("next", correlation_id="next"))
    assert not response.additional_properties.get("durable_status")
    assert provider.raw["schemaVersion"] == "2.0.0"
    expected = ["three", "two", "one", "next"] if reorder else ["one", "two", "three", "next"]
    assert [message.text for message in client.received_messages[0]] == expected
    actual_empty = [
        entry
        for entry in provider.raw["data"]["conversationHistory"]
        if not entry["messages"] and entry.get("correlationId") in ("one", "two", "three")
    ]
    assert actual_empty == original_empty


async def test_new_compaction_group_identities_do_not_collide_across_turns() -> None:
    async def compact(messages: list[Message]) -> bool:
        return False

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy(used=False))
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    for value in ("one", "two"):
        response = await entity.run(RunRequest(value, correlation_id=value))
        assert not response.additional_properties.get("durable_status")
    groups = [
        message["extensionData"][GROUP_ANNOTATION_KEY]["id"]
        for entry in provider.raw["data"]["conversationHistory"]
        for message in entry["messages"]
    ]
    assert len(groups) == len(set(groups)) == 4


class _CountingTokenizer:
    def __init__(self) -> None:
        self.calls = 0

    def count_tokens(self, text: str) -> int:
        self.calls += 1
        return 1


class _TokenAwareStrategy:
    def __init__(self, tokenizer: _CountingTokenizer) -> None:
        self.tokenizer = tokenizer

    async def __call__(self, messages: list[Message]) -> bool:
        assert included_token_count(messages) > 0
        return False


@pytest.mark.parametrize("before", [False, True])
@pytest.mark.parametrize("override", [False, True])
async def test_strategy_tokenizer_fallback_and_provider_precedence_match_core(before: bool, override: bool) -> None:
    reference_tokens = [_CountingTokenizer(), _CountingTokenizer(), _CountingTokenizer()]
    legacy_tokens = [_CountingTokenizer(), _CountingTokenizer(), _CountingTokenizer()]

    def providers(tokens: list[_CountingTokenizer]) -> list[InMemoryHistoryProvider | CompactionProvider]:
        history = InMemoryHistoryProvider()
        return [
            history,
            CompactionProvider(
                before_strategy=_TokenAwareStrategy(tokens[0]) if before else None,
                after_strategy=_TokenAwareStrategy(tokens[1]),
                tokenizer=tokens[2] if override else None,
                history_source_id=history.source_id,
            ),
        ]

    reference = Agent(client=RecordingChatClient(), context_providers=providers(reference_tokens))
    session = reference.create_session()
    session.state[InMemoryHistoryProvider.DEFAULT_SOURCE_ID] = {
        "messages": [Message("user", ["old question"]), Message("assistant", ["old answer"])]
    }
    await reference.run("new", session=session)
    provider = JsonStateProvider(_legacy())
    original = providers(legacy_tokens)
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), context_providers=original),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await entity.run(RunRequest("new", correlation_id="first"))
    assert not result.additional_properties.get("durable_status")
    assert [tokenizer.calls for tokenizer in legacy_tokens] == [tokenizer.calls for tokenizer in reference_tokens]
    assert isinstance(original[1], CompactionProvider)
    assert original[1].tokenizer is (legacy_tokens[2] if override else None)


@pytest.mark.parametrize("per_call", [False, True])
async def test_compaction_insertions_have_unique_occurrence_ids_across_tool_iterations(per_call: bool) -> None:
    seen: list[list[str | None]] = []

    async def compact(messages: list[Message]) -> bool:
        seen.append([message.message_id for message in messages])
        assert all(seen[-1]) and len(set(seen[-1])) == len(seen[-1])
        if not any(message.text == "summary" for message in messages):
            messages.insert(0, Message("system", ["summary"]))
        return True

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    compaction = CompactionProvider(after_strategy=compact, history_source_id=history.source_id)
    entity = CompatibleAgentEntity(
        Agent(
            client=ToolChatClient(),
            tools=[lookup],
            require_per_service_call_history_persistence=per_call,
            context_providers=[
                compaction,
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    response = await entity.run(RunRequest("new", correlation_id="first"))
    assert not response.additional_properties.get("durable_status")
    assert len(seen) == 1
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client), state_provider=JsonStateProvider(provider.raw), deployment_mode="legacy"
    )
    await cold.run(RunRequest("next", correlation_id="next"))
    assert [message.text for message in client.received_messages[0]].count("summary") == 1


async def test_later_compaction_hook_can_identify_anonymous_insertions() -> None:
    seen: list[list[str | None]] = []

    async def insert(messages: list[Message]) -> bool:
        messages.insert(0, Message("system", ["summary"]))
        return True

    async def inspect(messages: list[Message]) -> bool:
        seen.append([message.message_id for message in messages])
        assert all(seen[-1]) and len(set(seen[-1])) == len(seen[-1])
        return False

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=inspect, history_source_id=history.source_id, source_id="inspect"),
                CompactionProvider(after_strategy=insert, history_source_id=history.source_id, source_id="insert"),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await entity.run(RunRequest("new", correlation_id="first"))
    assert not result.additional_properties.get("durable_status") and len(seen) == 1


async def test_removed_current_sources_keep_lossless_summary_backlinks() -> None:
    async def compact(messages: list[Message]) -> bool:
        source_ids = [message.message_id for message in messages]
        for message in messages:
            message.additional_properties[GROUP_ANNOTATION_KEY][SUMMARIZED_BY_SUMMARY_ID_KEY] = "summary"
        messages[:] = [
            Message(
                "system",
                ["summary"],
                message_id="summary",
                additional_properties={GROUP_ANNOTATION_KEY: {SUMMARY_OF_MESSAGE_IDS_KEY: source_ids}},
            )
        ]
        return True

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    response = await entity.run(RunRequest("new", correlation_id="first"))
    assert not response.additional_properties.get("durable_status")
    raw = provider.raw
    profile = raw["data"]["extensionData"]["_dafx_legacy_history"]
    summary_reference = profile["generated"][0]
    entry_index, message_index = (int(value) for value in summary_reference.split(":"))
    summary = raw["data"]["conversationHistory"][entry_index]["messages"][message_index]
    source_ids = summary["extensionData"][GROUP_ANNOTATION_KEY][SUMMARY_OF_MESSAGE_IDS_KEY]
    assert len(source_ids) == 4 and all("pending" not in source_id for source_id in source_ids)
    for source_id in source_ids:
        index, offset = (int(value) for value in source_id.removeprefix("dafx_legacy:").split(":"))
        source = raw["data"]["conversationHistory"][index]["messages"][offset]["extensionData"]
        assert source["_excluded"] is True
        assert source[GROUP_ANNOTATION_KEY][SUMMARIZED_BY_SUMMARY_ID_KEY] == f"dafx_legacy:{summary_reference}"


@pytest.mark.parametrize("skip_excluded", [False, True])
async def test_service_bootstrap_honors_compacted_legacy_replay_order(skip_excluded: bool) -> None:
    async def compact(messages: list[Message]) -> bool:
        for message in messages[:2]:
            message.additional_properties["_excluded"] = True
        messages.insert(0, Message("system", ["summary"]))
        return True

    history = InMemoryHistoryProvider()
    provider = JsonStateProvider(_legacy())
    first = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    await first.run(RunRequest("new", correlation_id="first"))
    client = RecordingChatClient(conversation_id="remote")
    cold = CompatibleAgentEntity(
        Agent(client=client, context_providers=[InMemoryHistoryProvider(skip_excluded=skip_excluded)]),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    await cold.run(RunRequest("remote", correlation_id="remote", options={"store": True}))
    expected = ["summary", "new", "reply-1", "remote"]
    if not skip_excluded:
        expected = ["summary", "old question", "old answer", "new", "reply-1", "remote"]
    assert [message.text for message in client.received_messages[0]] == expected


@pytest.mark.parametrize(
    "profile",
    [
        None,
        {},
        {"profile": "foreign", "entryCount": 2, "order": ["0:0", "1:0"], "generated": []},
        {"profile": "dafx-legacy-history-v1", "entryCount": True, "order": ["0:0", "1:0"], "generated": []},
        {"profile": "dafx-legacy-history-v1", "entryCount": 2, "order": ["0:0", "0:0"], "generated": []},
        {"profile": "dafx-legacy-history-v1", "entryCount": 2, "order": ["0:0"], "generated": []},
        {"profile": "dafx-legacy-history-v1", "entryCount": 2, "order": ["0:0", "1:0"], "generated": ["1:0"]},
    ],
)
async def test_invalid_history_order_profile_rejects_before_execution(profile: Any) -> None:
    source = _legacy()
    source["data"]["extensionData"]["_dafx_legacy_history"] = profile
    client = RecordingChatClient()
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(Agent(client=client), state_provider=provider, deployment_mode="legacy")
    with pytest.raises(ValueError, match="Legacy history|Generated legacy"):
        await entity.run(RunRequest("new", correlation_id="new"))
    assert provider.raw == source and provider.attempted_writes == 0 and client.received_messages == []


@pytest.mark.parametrize("fail", [False, True])
async def test_legacy_compaction_reset_and_failure_preserve_metadata_atomically(fail: bool) -> None:
    async def compact(messages: list[Message]) -> bool:
        messages[0].additional_properties["_excluded"] = True
        messages.insert(0, Message("system", ["summary"]))
        if fail:
            raise RuntimeError("compaction failed")
        return True

    history = InMemoryHistoryProvider()
    source = _legacy()
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[
                CompactionProvider(after_strategy=compact, history_source_id=history.source_id),
                history,
            ],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await entity.run(RunRequest("new", correlation_id="first"))
    if fail:
        assert result.additional_properties["durable_status"] == "error"
        assert provider.raw["data"]["conversationHistory"][:2] == source["data"]["conversationHistory"]
        assert "_dafx_legacy_history" not in provider.raw["data"]["extensionData"]
    else:
        assert not result.additional_properties.get("durable_status")
        provider.raw["data"]["extensionData"]["_dafx_legacy_history"]["future"] = {"keep": [False, 0]}
        cold_provider = JsonStateProvider(provider.raw)
        cold = CompatibleAgentEntity(
            Agent(client=RecordingChatClient()), state_provider=cold_provider, deployment_mode="legacy"
        )
        cold.reset()
        profile = cold_provider.raw["data"]["extensionData"]["_dafx_legacy_history"]
        assert profile["entryCount"] == 0 and profile["order"] == profile["generated"] == []
        assert profile["future"] == {"keep": [False, 0]}
