# Copyright (c) Microsoft. All rights reserved.

"""Exercise the real compaction sample factories and standalone worker lifetime offline."""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from agent_framework import Agent, CompactionProvider, InMemoryHistoryProvider, Message
from durabletask.worker import TaskHubGrpcWorker

from agent_framework_durabletask import AgentEntity, DurableHistoryProvider

SAMPLES = Path(__file__).resolve().parents[3] / "samples"


def _sample(kind: str, monkeypatch: pytest.MonkeyPatch) -> tuple[ModuleType, Mock]:
    monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", "isolated_v2")
    monkeypatch.setenv("FOUNDRY_PROJECT_ENDPOINT", "https://offline.example.invalid")
    monkeypatch.setenv("FOUNDRY_MODEL", "offline")
    monkeypatch.setattr("dotenv.load_dotenv", Mock(return_value=False))
    monkeypatch.setattr("logging.basicConfig", Mock())
    monkeypatch.setattr("azure.identity.aio.AzureCliCredential", Mock(return_value=object()))
    monkeypatch.setattr("azure.identity.AzureCliCredential", Mock(return_value=object()))
    models = Mock(return_value=RecordingChatClient())
    monkeypatch.setattr("agent_framework.foundry.FoundryChatClient", models)
    relative = (
        "13_conversation_compaction/worker.py"
        if kind == "standalone"
        else "azure_functions/14_conversation_compaction/function_app.py"
    )
    path = SAMPLES / relative
    name = f"_sample_compaction_order_{kind}"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    assert module.__file__ is not None and Path(module.__file__).resolve() == path.resolve()
    return module, models


@pytest.mark.parametrize("kind", ["standalone", "functions"])
@pytest.mark.parametrize("history_first", [False, True], ids=["actual-sample", "old-order-control"])
async def test_sample_fourth_model_call_observes_four_prior_groups_after_cold_reloads(
    kind: str, history_first: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    module, models = _sample(kind, monkeypatch)
    factory = module.create_historian_agent if kind == "standalone" else module._create_agent
    assert module.KEEP_LAST_GROUPS == 4
    raw: dict[str, Any] | None = None
    calls: list[list[str | None]] = []
    for turn in range(1, 5):
        model = RecordingChatClient(response_message_id=f"a{turn}")
        models.return_value = model
        agent = factory()
        assert isinstance(agent, Agent) and agent.client is model and agent.default_options["store"] is False
        history = next(p for p in agent.context_providers if type(p) is InMemoryHistoryProvider)
        compaction = next(p for p in agent.context_providers if isinstance(p, CompactionProvider))
        assert compaction.before_strategy is None and compaction.after_strategy is not None
        assert compaction.history_source_id == history.source_id and history.skip_excluded is True
        if history_first:
            # Change only order. The real Core strategy and both sample providers remain intact.
            agent.context_providers = [history, compaction]
        configured = agent.context_providers
        providers = tuple(configured)
        sources = [p.source_id for p in configured]
        store = JsonStateProvider(raw, session_id="sample", entity_name="dafx-historian")
        entity = AgentEntity(agent, state_provider=store, retention="keep_all", max_state_bytes=None)
        assert isinstance(entity.agent, Agent)
        assert [p.source_id for p in entity.agent.context_providers] == sources
        assert sum(isinstance(p, DurableHistoryProvider) for p in entity.agent.context_providers) == 1
        response = await entity.run({
            "message": f"question-{turn}",
            "correlationId": f"turn-{turn}",
            "contextMessages": [Message("user", [f"question-{turn}"], message_id=f"u{turn}").to_dict()],
        })
        assert response.text == "reply-1" and response.additional_properties.get("durable_status") != "error"
        assert len(model.received_messages) == 1
        received = [message for message in model.received_messages[0] if message.role != "system"]
        assert received[-1].message_id == f"u{turn}" and received[-1].text == f"question-{turn}"
        calls.append([message.message_id for message in received])
        assert store.attempted_writes == store.successful_writes == 1
        assert agent.context_providers is configured
        assert all(a is b for a, b in zip(configured, providers, strict=True))
        raw = json.loads(json.dumps(store.raw, allow_nan=False))
        assert raw is not None
        data = raw["data"]
        rows = [message for entry in data["conversationHistory"] for message in entry["messages"]]
        assert [row["messageId"] for row in rows] == [
            mid for index in range(1, turn + 1) for mid in (f"u{index}", f"a{index}")
        ]
        assert len(data["terminalResults"]) == len(data["completionReceipts"]) == turn
        assert "truncation" not in data

    assert calls[:3] == [["u1"], ["u1", "a1", "u2"], ["u1", "a1", "u2", "a2", "u3"]]
    expected = ["u1", "a1", "u2", "a2", "u3", "a3", "u4"] if history_first else ["u2", "a2", "u3", "a3", "u4"]
    assert calls[3] == expected, "Compare literal leaf IDs, not the sample's configured provider order"


@pytest.mark.parametrize("failure", ["setup", "start", "running", "cancelled", "interrupt"])
async def test_compaction_worker_stops_once_and_preserves_escaping_failures(
    failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module, _ = _sample("standalone", monkeypatch)
    error: BaseException = (
        asyncio.CancelledError("worker cancelled")
        if failure == "cancelled"
        else KeyboardInterrupt()
        if failure == "interrupt"
        else RuntimeError(f"{failure} failed")
    )
    worker = MagicMock()
    # Delegate the SDK's actual context-manager methods without constructing or starting a worker.
    worker.__enter__.side_effect = lambda: TaskHubGrpcWorker.__enter__(cast(Any, worker))
    worker.__exit__.side_effect = lambda *args: TaskHubGrpcWorker.__exit__(cast(Any, worker), *args)
    get_worker = Mock(return_value=worker)
    setup = Mock(side_effect=error if failure == "setup" else None)
    if failure == "start":
        worker.start.side_effect = error
    suspend = AsyncMock(side_effect=error)
    monkeypatch.setattr(module, "get_worker", get_worker)
    monkeypatch.setattr(module, "setup_worker", setup)
    monkeypatch.setattr(module, "asyncio", SimpleNamespace(sleep=suspend))
    if failure == "interrupt":
        await module.main()
    else:
        with pytest.raises(type(error)) as caught:
            await module.main()
        assert caught.value is error
    get_worker.assert_called_once_with()
    setup.assert_called_once_with(worker)
    worker.__enter__.assert_called_once_with()
    worker.__exit__.assert_called_once()
    assert worker.__exit__.call_args.args[1] is error
    if failure == "setup":
        worker.start.assert_not_called()
    else:
        worker.start.assert_called_once_with()
    if failure in ("setup", "start"):
        suspend.assert_not_awaited()
    else:
        suspend.assert_awaited_once_with(1)
    worker.stop.assert_called_once_with()
