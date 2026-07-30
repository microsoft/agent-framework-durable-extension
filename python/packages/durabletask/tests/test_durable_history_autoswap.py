# Copyright (c) Microsoft. All rights reserved.

"""Tests for automatic durable history backing (ADR-0032).

A user should be able to take an agent that already works in core, register it with the
durable runtime, and get durable conversation history with no configuration change.
These tests cover the substitution rules and confirm the user's agent is never mutated.
"""

from typing import Any

from agent_framework import (
    Agent,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
)

from agent_framework_durabletask import AgentEntity, AgentEntityStateProviderMixin, DurableHistoryProvider
from agent_framework_durabletask._history_provider import ensure_durable_history


class _StubClient:
    """Chat client stand-in that stores history locally (the common case)."""

    STORES_BY_DEFAULT = False

    def __init__(self) -> None:
        self.additional_properties: dict[str, Any] = {}


class _ServiceStoringClient(_StubClient):
    """Chat client whose service keeps the conversation server-side."""

    STORES_BY_DEFAULT = True


class _ExternalHistoryProvider(HistoryProvider):
    """Stand-in for Cosmos/Redis/file-backed history the user chose deliberately."""

    def __init__(self) -> None:
        super().__init__(source_id="external")

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return []

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        return None


class _InMemoryStateProvider(AgentEntityStateProviderMixin):
    def __init__(self, *, session_id: str = "autoswap-session") -> None:
        self._session_id = session_id
        self._state_dict: dict[str, Any] = {}

    def _get_state_dict(self) -> dict[str, Any]:
        return self._state_dict

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self._state_dict = state

    def _get_session_id_from_entity(self) -> str:
        return self._session_id


def _history_providers(agent: Any) -> list[Any]:
    return [p for p in agent.context_providers if isinstance(p, HistoryProvider)]


class TestAutomaticDurableHistory:
    """The durable runtime substitutes durable-backed history where appropriate."""

    def test_agent_without_providers_gets_durable_history(self) -> None:
        agent = Agent(client=_StubClient(), name="a")

        prepared = ensure_durable_history(agent)

        providers = _history_providers(prepared)
        assert len(providers) == 1
        assert isinstance(providers[0], DurableHistoryProvider)
        # Uses the source id core's auto-injected provider would have, so a
        # default-configured CompactionProvider still resolves it.
        assert providers[0].source_id == InMemoryHistoryProvider.DEFAULT_SOURCE_ID

    def test_in_memory_history_is_replaced_preserving_source_id(self) -> None:
        agent = Agent(
            client=_StubClient(),
            name="a",
            context_providers=[InMemoryHistoryProvider(source_id="custom_slot", skip_excluded=True)],
        )

        prepared = ensure_durable_history(agent)

        providers = _history_providers(prepared)
        assert len(providers) == 1
        replacement = providers[0]
        assert isinstance(replacement, DurableHistoryProvider)
        # Preserving these is what keeps an existing CompactionProvider wired up.
        assert replacement.source_id == "custom_slot"
        assert replacement.skip_excluded is True

    def test_external_history_provider_is_left_alone(self) -> None:
        """The user deliberately chose their own storage; durable must not override it."""
        external = _ExternalHistoryProvider()
        agent = Agent(client=_StubClient(), name="a", context_providers=[external])

        prepared = ensure_durable_history(agent)

        assert prepared is agent
        assert _history_providers(prepared) == [external]

    def test_service_managed_history_is_left_alone(self) -> None:
        agent = Agent(client=_ServiceStoringClient(), name="a")

        prepared = ensure_durable_history(agent)

        assert prepared is agent
        assert not _history_providers(prepared)

    def test_existing_durable_provider_is_untouched(self) -> None:
        """Explicit configuration (for example to enable pruning) wins."""
        explicit = DurableHistoryProvider(prune_excluded=True)
        agent = Agent(client=_StubClient(), name="a", context_providers=[explicit])

        prepared = ensure_durable_history(agent)

        assert prepared is agent
        assert _history_providers(prepared) == [explicit]

    def test_agent_without_context_pipeline_is_left_alone(self) -> None:
        """Custom agents that do not expose context_providers keep legacy replay."""

        class _CustomAgent:
            name = "custom"

            async def run(self, *args: Any, **kwargs: Any) -> Any: ...

        agent = _CustomAgent()

        assert ensure_durable_history(agent) is agent  # type: ignore[arg-type]


class TestUserAgentIsNotMutated:
    """Substitution must not change the object the caller handed us."""

    def test_original_agent_keeps_its_providers(self) -> None:
        original_provider = InMemoryHistoryProvider()
        agent = Agent(client=_StubClient(), name="a", context_providers=[original_provider])
        original_list = agent.context_providers

        prepared = ensure_durable_history(agent)

        assert prepared is not agent
        assert agent.context_providers is original_list
        assert agent.context_providers == [original_provider]

    def test_entity_construction_does_not_mutate_the_agent(self) -> None:
        agent = Agent(client=_StubClient(), name="a", context_providers=[InMemoryHistoryProvider()])

        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider())

        assert isinstance(_history_providers(entity.agent)[0], DurableHistoryProvider)
        assert isinstance(_history_providers(agent)[0], InMemoryHistoryProvider)
