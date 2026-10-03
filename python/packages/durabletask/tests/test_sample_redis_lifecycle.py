# Copyright (c) Microsoft. All rights reserved.

"""Exercise the Redis sample's provider and entrypoints with loop-affine offline clients."""

import asyncio
import importlib.util
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from agent_framework import Message

SAMPLE = Path(__file__).resolve().parents[3] / "samples" / "14_external_history_redis"


def _load(filename, name, monkeypatch):
    spec = importlib.util.spec_from_file_location(name, SAMPLE / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def redis_service(monkeypatch):
    module = _load("redis_history_provider.py", "redis_history_provider", monkeypatch)
    service = SimpleNamespace(clients=[], rows={}, events=[], fail=None)

    class RedisClient:
        def __init__(self):
            # Like redis.asyncio, construction need not run on a loop. First use binds it.
            self.loop: asyncio.AbstractEventLoop | None = None
            self.closed = False
            service.clients.append(self)
            service.events.append("redis-open")

        def check_loop(self):
            if self.loop is None:
                self.loop = asyncio.get_running_loop()
            assert asyncio.get_running_loop() is self.loop
            assert not self.closed

        async def __aenter__(self):
            self.check_loop()
            return self

        async def __aexit__(self, *args):
            await self.aclose()

        async def aclose(self):
            self.check_loop()
            self.closed = True
            service.events.append("redis-close")

        async def lrange(self, key, start, stop):
            self.check_loop()
            assert (start, stop) == (0, -1)
            service.events.append("read")
            if service.fail == "read":
                raise RuntimeError("read failed")
            return service.rows.get(key, [])[:]

        async def rpush(self, key, *values):
            self.check_loop()
            service.events.append("append")
            service.rows.setdefault(key, []).extend(values)
            if service.fail == "append":
                # Model an accepted external append whose acknowledgement is lost.
                raise RuntimeError("append failed")

    factory = Mock(side_effect=lambda *args, **kwargs: RedisClient())
    monkeypatch.setattr(module.aioredis, "from_url", factory)
    return module, service, factory


def test_redis_provider_opens_no_connections_during_setup(redis_service):
    module, service, factory = redis_service
    module.RedisHistoryProvider("redis://example.invalid")
    factory.assert_not_called()
    assert service.clients == []


@pytest.mark.parametrize("operation", ["read", "append"])
@pytest.mark.parametrize("fails", [False, True], ids=["normal", "error"])
async def test_redis_history_operations_close_on_their_own_loop(redis_service, operation, fails):
    module, service, factory = redis_service
    history = module.RedisHistoryProvider("redis://example.invalid")
    message = Message("user", ["remember this"])
    service.rows["durable_sample:history:session"] = [message.to_json()]
    service.fail = operation if fails else None

    async def run():
        if operation == "read":
            messages = await history.get_messages("session")
            assert [item.text for item in messages] == ["remember this"]
        else:
            await history.save_messages("session", [message])

    if fails:
        with pytest.raises(RuntimeError, match=f"{operation} failed"):
            await run()
    else:
        await run()

    factory.assert_called_once_with("redis://example.invalid", decode_responses=True)
    assert service.events == ["redis-open", operation, "redis-close"]
    assert all(client.closed for client in service.clients)


async def test_redis_empty_append_opens_no_connection(redis_service):
    module, _, factory = redis_service
    history = module.RedisHistoryProvider("redis://example.invalid")
    await history.save_messages("session", [])
    factory.assert_not_called()


def test_redis_provider_can_be_used_from_successive_worker_loops(redis_service):
    module, service, _ = redis_service
    history = module.RedisHistoryProvider("redis://example.invalid")
    message = Message("user", ["kept across loops"])

    asyncio.run(history.save_messages("session", [message]))
    messages = asyncio.run(history.get_messages("session"))

    assert [item.text for item in messages] == ["kept across loops"]
    assert len(service.clients) == 2
    assert service.clients[0].loop is not service.clients[1].loop
    assert all(client.closed for client in service.clients)


async def test_redis_retry_keeps_ordinary_external_append_semantics(redis_service):
    module, service, _ = redis_service
    history = module.RedisHistoryProvider("redis://example.invalid")
    message = Message("user", ["repeatable append"])
    service.fail = "append"
    with pytest.raises(RuntimeError, match="append failed"):
        await history.save_messages("session", [message])
    service.fail = None
    await history.save_messages("session", [message])
    messages = await history.get_messages("session")
    assert [item.text for item in messages] == ["repeatable append", "repeatable append"]
    assert all(client.closed for client in service.clients)


@pytest.fixture
def entrypoint(redis_service, monkeypatch):
    provider_module, service, _ = redis_service
    monkeypatch.setattr("dotenv.load_dotenv", Mock(return_value=False))
    monkeypatch.setenv("FOUNDRY_PROJECT_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("FOUNDRY_MODEL", "offline")
    monkeypatch.setenv("REDIS_CONNECTION_STRING", "redis://example.invalid")
    monkeypatch.setenv("TASKHUB", "RedisLifecycleHub")
    worker = _load("worker.py", "worker", monkeypatch)
    providers = []
    state = SimpleNamespace(fail=None)

    def create_provider(*args, **kwargs):
        provider = provider_module.RedisHistoryProvider(*args, **kwargs)
        providers.append(provider)
        return provider

    def create_agent(**kwargs):
        assert kwargs["context_providers"] == [providers[-1]]
        if state.fail == "agent_setup":
            raise RuntimeError("agent_setup failed")
        return SimpleNamespace(name="Archivist")

    def add_agent(agent):
        service.events.append("register")
        if state.fail == "registration":
            raise RuntimeError("registration failed")

    monkeypatch.setattr(worker, "RedisHistoryProvider", create_provider)
    monkeypatch.setattr(worker, "FoundryChatClient", Mock())
    monkeypatch.setattr(worker, "AsyncAzureCliCredential", Mock())
    monkeypatch.setattr(worker, "Agent", create_agent)
    monkeypatch.setattr(worker, "DurableAIAgentWorker", Mock(return_value=SimpleNamespace(add_agent=add_agent)))

    class Worker:
        def __enter__(self):
            service.events.append("enter")
            return self

        def __exit__(self, *args):
            # No Redis pool can outlive the operation or require cross-loop shutdown.
            assert all(client.closed for client in service.clients)
            service.events.append("stop")

        def start(self):
            service.events.append("start")

            async def operation():
                await providers[0].save_messages("session", [Message("user", ["offline turn"])])
                assert (await providers[0].get_messages("session"))[0].text == "offline turn"

            # The SDK's work runs on a different loop from worker.main's asyncio.run loop.
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(asyncio.run, operation()).result(timeout=5)
            if state.fail == "start":
                raise RuntimeError("start failed")

    native = Worker()
    monkeypatch.setattr(worker, "get_worker", lambda **kwargs: native)
    return worker, service, providers, state


@pytest.mark.parametrize("failure", [None, "client", "client_setup", "agent_setup", "registration", "start"])
def test_redis_combined_entrypoint_releases_resources_on_all_exit_paths(entrypoint, monkeypatch, failure):
    _, service, providers, state = entrypoint
    state.fail = failure

    def get_client(**kwargs):
        service.events.append("client-create")
        if failure == "client_setup":
            raise RuntimeError("client_setup failed")
        return object()

    def run_client(client):
        service.events.append("client-run")
        if failure == "client":
            raise RuntimeError("client failed")

    client = ModuleType("client")
    monkeypatch.setattr(client, "get_client", get_client, raising=False)
    monkeypatch.setattr(client, "run_client", run_client, raising=False)
    monkeypatch.setitem(sys.modules, "client", client)
    # Avoid the sample's process-wide logging reconfiguration interfering with pytest.
    monkeypatch.setattr("logging.basicConfig", Mock())
    sample = _load("sample.py", "redis_combined_sample", monkeypatch)

    if failure in {"client_setup", "agent_setup", "registration", "start"}:
        with pytest.raises(RuntimeError, match=f"{failure} failed"):
            sample.main()
    else:
        sample.main()

    assert len(providers) == 1
    assert service.events[0] == "enter" and service.events[-1] == "stop"
    assert service.events.count("stop") == 1
    assert all(client.closed for client in service.clients)
    if failure in {"agent_setup", "registration"}:
        assert service.clients == []
        assert "start" not in service.events
    else:
        assert len(service.clients) == 2
        assert service.events.index("redis-close") < service.events.index("stop")


@pytest.mark.parametrize("failure", [None, "agent_setup", "registration", "start", "running", "cancelled"])
async def test_redis_worker_stops_on_interrupt_setup_failure_and_cancellation(entrypoint, monkeypatch, failure):
    worker, service, providers, state = entrypoint
    state.fail = failure
    ending: BaseException = KeyboardInterrupt()
    if failure == "cancelled":
        ending = asyncio.CancelledError()
    elif failure == "running":
        ending = RuntimeError("running failed")
    monkeypatch.setattr(worker.asyncio, "sleep", AsyncMock(side_effect=ending))

    if failure == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await worker.main()
    elif failure is not None:
        with pytest.raises(RuntimeError, match=f"{failure} failed"):
            await worker.main()
    else:
        await worker.main()

    assert len(providers) == 1
    assert service.events[0] == "enter" and service.events[-1] == "stop"
    assert service.events.count("stop") == 1
    assert all(client.closed for client in service.clients)
    if failure in {"agent_setup", "registration"}:
        assert service.clients == []
        assert "start" not in service.events
    else:
        assert len(service.clients) == 2
        assert all(client.loop is not asyncio.get_running_loop() for client in service.clients)
