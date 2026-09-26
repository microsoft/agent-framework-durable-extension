# Copyright (c) Microsoft. All rights reserved.

"""Offline deployment isolation checks using the Redis sample's actual factories."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

SAMPLE = Path(__file__).resolve().parents[3] / "samples" / "14_external_history_redis"


def _load(filename, name, monkeypatch):
    spec = importlib.util.spec_from_file_location(name, SAMPLE / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def sample_factory(monkeypatch):
    dotenv = Mock(return_value=False)
    monkeypatch.setattr("dotenv.load_dotenv", dotenv)
    monkeypatch.setattr("logging.basicConfig", Mock())
    monkeypatch.setenv("FOUNDRY_PROJECT_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("FOUNDRY_MODEL", "offline")
    monkeypatch.setenv("REDIS_CONNECTION_STRING", "redis://example.invalid")
    monkeypatch.setenv("TASKHUB", "EnvironmentHub")
    monkeypatch.setenv("ENDPOINT", "http://localhost:8080")
    provider = _load("redis_history_provider.py", "redis_history_provider", monkeypatch)
    redis = Mock(side_effect=AssertionError("No Redis connection is allowed during setup"))
    monkeypatch.setattr(provider.aioredis, "from_url", redis)
    worker = _load("worker.py", "worker", monkeypatch)
    history = Mock(wraps=provider.RedisHistoryProvider)
    monkeypatch.setattr(worker, "RedisHistoryProvider", history)
    monkeypatch.setattr(worker, "FoundryChatClient", Mock())
    monkeypatch.setattr(worker, "AsyncAzureCliCredential", Mock())
    monkeypatch.setattr(worker, "AzureCliCredential", Mock())
    agent = Mock(side_effect=lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(worker, "Agent", agent)
    native = MagicMock()
    native.__enter__.return_value = native
    scheduler = Mock(return_value=native)
    monkeypatch.setattr(worker, "DurableTaskSchedulerWorker", scheduler)
    registration = Mock()
    monkeypatch.setattr(worker, "DurableAIAgentWorker", registration)
    yield SimpleNamespace(
        worker=worker,
        provider=provider,
        history=history,
        agent=agent,
        scheduler=scheduler,
        native=native,
        registration=registration,
    )
    dotenv.assert_called_once_with()
    redis.assert_not_called()


@pytest.mark.parametrize(("first", "second"), [("HubA", "HubB"), ("HubA", "huba"), ("a:b", "a%3Ab")])
def test_factory_separates_hubs_for_the_same_session(sample_factory, first, second):
    worker = sample_factory.worker
    first_history = worker.create_archivist_agent(first).context_providers[0]
    second_history = worker.create_archivist_agent(second).context_providers[0]

    assert first_history._key("same-session") != second_history._key("same-session")
    for history, call in zip((first_history, second_history), sample_factory.history.call_args_list):
        assert call.args == ("redis://example.invalid",)
        assert call.kwargs == {"key_prefix": history.key_prefix}
        assert history.key_prefix.startswith("durable_sample:history:hub:")


def test_namespace_cannot_be_confused_with_a_session_suffix(sample_factory):
    first = sample_factory.worker.create_archivist_agent("Hub").context_providers[0]
    second = sample_factory.worker.create_archivist_agent("Hub:part").context_providers[0]

    assert first._key("part:session") != second._key("session")


def test_factory_restores_the_same_keys_without_rereading_taskhub(sample_factory, monkeypatch):
    first = sample_factory.worker.create_archivist_agent("HubA").context_providers[0]
    monkeypatch.setenv("TASKHUB", "UnrelatedHub")
    restored = sample_factory.worker.create_archivist_agent("HubA").context_providers[0]

    assert first is not restored
    # An independent known encoding also detects case folding or a random namespace.
    expected_key = "durable_sample:history:hub:48756241:same-session"
    assert first._key("same-session") == restored._key("same-session") == expected_key


@pytest.mark.parametrize("from_env", [False, True], ids=["explicit-hub", "environment-hub"])
def test_worker_and_registration_share_the_resolved_hub(sample_factory, monkeypatch, from_env):
    worker = sample_factory.worker
    monkeypatch.setenv("TASKHUB", "MixedCaseHub" if from_env else "OtherHub")
    hub = worker._resolve_taskhub(None if from_env else "MixedCaseHub")
    native = worker.get_worker(taskhub=hub)
    monkeypatch.setenv("TASKHUB", "ChangedAfterWorkerCreation")
    worker.setup_worker(native, taskhub=hub)

    assert sample_factory.scheduler.call_args.kwargs["taskhub"] == "MixedCaseHub"
    sample_factory.registration.assert_called_once_with(native)
    agent = sample_factory.registration.return_value.add_agent.call_args.args[0]
    assert agent.context_providers[0].key_prefix == "durable_sample:history:hub:4d6978656443617365487562"


async def test_worker_entrypoint_keeps_the_resolved_hub_during_setup(sample_factory, monkeypatch):
    def create_scheduler(**kwargs):
        monkeypatch.setenv("TASKHUB", "ChangedDuringStartup")
        return sample_factory.native

    sample_factory.scheduler.side_effect = create_scheduler
    monkeypatch.setattr(sample_factory.worker.asyncio, "sleep", AsyncMock(side_effect=KeyboardInterrupt()))
    await sample_factory.worker.main()

    assert sample_factory.scheduler.call_args.kwargs["taskhub"] == "EnvironmentHub"
    agent = sample_factory.registration.return_value.add_agent.call_args.args[0]
    assert agent.context_providers[0].key_prefix == "durable_sample:history:hub:456e7669726f6e6d656e74487562"
    sample_factory.native.__exit__.assert_called_once()


def test_combined_entrypoint_uses_one_hub_for_worker_history_and_client(sample_factory, monkeypatch):
    def create_scheduler(**kwargs):
        monkeypatch.setenv("TASKHUB", "ChangedDuringStartup")
        return sample_factory.native

    sample_factory.scheduler.side_effect = create_scheduler
    client = ModuleType("client")
    get_client = Mock()
    monkeypatch.setattr(client, "get_client", get_client, raising=False)
    monkeypatch.setattr(client, "run_client", Mock(), raising=False)
    monkeypatch.setitem(sys.modules, "client", client)
    sample = _load("sample.py", "redis_deployment_sample", monkeypatch)
    sample.main()

    assert sample_factory.scheduler.call_args.kwargs["taskhub"] == "EnvironmentHub"
    assert get_client.call_args.kwargs["taskhub"] == "EnvironmentHub"
    agent = sample_factory.registration.return_value.add_agent.call_args.args[0]
    assert agent.context_providers[0].key_prefix == "durable_sample:history:hub:456e7669726f6e6d656e74487562"
    sample_factory.native.__exit__.assert_called_once()


def test_plain_provider_keeps_generic_default_and_supports_explicit_prefix(sample_factory):
    provider = sample_factory.provider.RedisHistoryProvider
    generic = provider("redis://example.invalid")
    explicit = provider("redis://example.invalid", key_prefix="deployment-b:history")

    assert generic._key("same-session") == "durable_sample:history:same-session"
    assert explicit._key("same-session") == "deployment-b:history:same-session"
