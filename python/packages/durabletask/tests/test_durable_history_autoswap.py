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

    def test_store_false_overrides_a_service_storing_client(self) -> None:
        """``store=False`` puts history back in the client's hands, so durable must back it.

        Mirrors core's precedence: an explicit ``store`` wins over ``STORES_BY_DEFAULT``. Without
        this, an agent using the Responses API with ``store=False`` would keep a plain in-memory
        provider that the durable runtime never persists, silently losing the conversation.
        """
        agent = Agent(client=_ServiceStoringClient(), name="a", default_options={"store": False})

        prepared = ensure_durable_history(agent)

        providers = _history_providers(prepared)
        assert len(providers) == 1
        assert isinstance(providers[0], DurableHistoryProvider)

    def test_store_true_keeps_history_with_the_service(self) -> None:
        agent = Agent(client=_StubClient(), name="a", default_options={"store": True})

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


class TestPruneHistoryOptIn:
    """Pruning is a deployment-level retention policy, set at registration."""

    def test_off_by_default(self) -> None:
        agent = Agent(client=_StubClient(), name="a")

        prepared = ensure_durable_history(agent)

        assert _history_providers(prepared)[0].prune_excluded is False

    def test_enabled_via_registration(self) -> None:
        agent = Agent(client=_StubClient(), name="a", context_providers=[InMemoryHistoryProvider()])

        prepared = ensure_durable_history(agent, prune_history=True)

        assert _history_providers(prepared)[0].prune_excluded is True

    def test_entity_forwards_the_flag(self) -> None:
        agent = Agent(client=_StubClient(), name="a")

        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider(), prune_history=True)

        assert _history_providers(entity.agent)[0].prune_excluded is True

    def test_explicit_provider_configuration_wins(self) -> None:
        """A hand-configured provider is never overridden by the registration flag."""
        explicit = DurableHistoryProvider(prune_excluded=False)
        agent = Agent(client=_StubClient(), name="a", context_providers=[explicit])

        prepared = ensure_durable_history(agent, prune_history=True)

        assert _history_providers(prepared)[0] is explicit
        assert explicit.prune_excluded is False


class TestServiceManagedSessions:
    """Service-backed agents let the service own the conversation."""

    async def test_only_new_messages_are_sent(self) -> None:
        """History must not be replayed locally when the service already holds it."""
        recorded: list[list[Message]] = []

        class _ServiceAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(self, messages: Any = None, *, stream: bool = False, **kwargs: Any) -> Any:
                from agent_framework import AgentResponse

                if stream:
                    raise TypeError("stream is not supported")
                recorded.append(list(messages or []))
                return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])

        entity = AgentEntity(_ServiceAgent(), state_provider=_InMemoryStateProvider())  # type: ignore[arg-type]

        await entity.run({"message": "first", "correlationId": "c0"})
        await entity.run({"message": "second", "correlationId": "c1"})

        # Each turn delivers only its own message; the service supplies the rest.
        assert len(recorded[1]) == 1
        assert recorded[1][0].text == "second"

    async def test_service_conversation_id_is_persisted_and_restored(self) -> None:
        """Without this the service would start a new thread on every turn."""
        seen_ids: list[str | None] = []

        class _ThreadingAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(
                self,
                messages: Any = None,
                *,
                stream: bool = False,
                session: Any = None,
                **kwargs: Any,
            ) -> Any:
                from agent_framework import AgentResponse

                if stream:
                    raise TypeError("stream is not supported")
                seen_ids.append(getattr(session, "service_session_id", None))
                # The service issues (or confirms) the thread id on the session.
                session.service_session_id = "svc-thread-1"
                return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])

        provider = _InMemoryStateProvider()
        entity = AgentEntity(_ThreadingAgent(), state_provider=provider)  # type: ignore[arg-type]

        await entity.run({"message": "first", "correlationId": "c0"})
        await entity.run({"message": "second", "correlationId": "c1"})

        assert seen_ids[0] is None  # first turn has no thread yet
        assert seen_ids[1] == "svc-thread-1"  # second turn continues the same thread
        assert provider._get_state_dict()["data"]["serviceSessionId"] == "svc-thread-1"
