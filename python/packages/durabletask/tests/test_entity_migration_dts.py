# Copyright (c) Microsoft. All rights reserved.

"""Bounded local-emulator rollout check, not a managed-service availability guarantee."""

import json
import os
import re
import time
import uuid
from typing import Any

import pytest
from _execution_test_support import NonStreamingAgent, RecordingChatClient
from agent_framework import AgentResponse, Message
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.entities import DurableEntity, EntityInstanceId

from agent_framework_durabletask import (
    AgentSessionId,
    DurableAgentSession,
    DurableAIAgentClient,
    DurableAIAgentWorker,
    LegacyDurableAgentState,
    RunRequest,
)
from agent_framework_durabletask._durable_agent_state import DurableAgentStateRequest as LegacyRequest
from agent_framework_durabletask._durable_agent_state import DurableAgentStateResponse as LegacyResponse

pytestmark = pytest.mark.integration


def _local_emulator_endpoint(endpoint: str) -> str:
    match = re.fullmatch(r"(?:localhost|127\.0\.0\.1):([0-9]{1,5})", endpoint)
    if match is None or not 1 <= int(match.group(1)) <= 65535:
        raise ValueError("This test accepts only a local emulator host:port, never a cloud scheduler.")
    return endpoint


@pytest.mark.parametrize("populated", [False, True])
def test_local_dts_two_phase_upgrade_at_original_entity_address(populated: bool) -> None:
    endpoint = os.getenv("DTS_ENTITY_MIGRATION_ENDPOINT")
    if not endpoint:
        pytest.skip("Set DTS_ENTITY_MIGRATION_ENDPOINT to a task-owned local DTS emulator.")
    endpoint = _local_emulator_endpoint(endpoint)

    def worker() -> DurableTaskSchedulerWorker:
        return DurableTaskSchedulerWorker(
            host_address=endpoint, taskhub="default", token_credential=None, secure_channel=False
        )

    client = DurableTaskSchedulerClient(
        host_address=endpoint, taskhub="default", token_credential=None, secure_channel=False
    )
    key = f"schema-{uuid.uuid4().hex}"
    agent_name = f"assistant-{uuid.uuid4().hex}"
    original = EntityInstanceId(f"dafx-{agent_name}", key)
    legacy_state = LegacyDurableAgentState()
    if populated:
        legacy_state.data.conversation_history.extend([
            LegacyRequest.from_run_request(RunRequest(message="old request", correlation_id="old-turn")),
            LegacyResponse.from_run_response("old-turn", AgentResponse(messages=[Message("assistant", ["old reply"])])),
        ])
    legacy = json.loads(legacy_state.to_json())

    class LegacyWriter(DurableEntity):
        def seed(self) -> None:
            self.set_state(legacy)

    LegacyWriter.__name__ = original.entity
    old = worker()
    old.add_entity(LegacyWriter)
    phase_one, phase_two = worker(), worker()
    model_one, model_two = RecordingChatClient(), RecordingChatClient()
    DurableAIAgentWorker(phase_one, deployment_mode="two_phase").add_agent(
        NonStreamingAgent(client=model_one, name=agent_name)
    )
    DurableAIAgentWorker(phase_two, deployment_mode="two_phase", process_legacy_entities=True).add_agent(
        NonStreamingAgent(client=model_two, name=agent_name)
    )

    def wait_for_state(address: EntityInstanceId, version: str) -> dict[str, Any]:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            metadata = client.get_entity(address)
            if metadata is not None:
                state = json.loads(metadata.get_state())
                if state.get("schemaVersion") == version:
                    assert metadata.id == address
                    return state
            time.sleep(0.1)
        pytest.fail(f"Timed out waiting for {version} at {address}.")

    running: list[DurableTaskSchedulerWorker] = []
    try:
        old.start()
        running.append(old)
        client.signal_entity(original, "seed")
        assert wait_for_state(original, "1.1.0") == legacy
        phase_one.start()
        running.append(phase_one)
        # A finite settling interval is not a reservation or outage guarantee.
        time.sleep(2)
        wrapped = DurableAIAgentClient(client, entity_version=2, max_poll_retries=200, poll_interval_seconds=0.1)
        new = wrapped.get_agent(agent_name).create_session()
        assert wrapped.get_agent(agent_name).run("new-name traffic", session=new).text == "reply-1"
        assert len(model_one.received_messages) == 1
        assert wait_for_state(original, "1.1.0") == legacy
        old.stop()
        running.remove(old)
        phase_one.stop()
        running.remove(phase_one)
        phase_two.start()
        running.append(phase_two)
        old_session = DurableAgentSession(durable_session_id=AgentSessionId(agent_name, key))
        assert wrapped.get_agent(agent_name).run("upgrade original", session=old_session).text == "reply-1"
        upgraded = wait_for_state(original, "2.0.0")
        assert upgraded["data"]["session"]["session_id"] == f"@dafx-{agent_name}@{key}"
        assert upgraded["dafxSchema"] == {"contract": "dafx.agent-state", "version": "2.0.0"}
        assert "migration" not in upgraded["data"]
        assert "old-turn" not in upgraded["data"]["completionReceipts"]
        if populated:
            assert any(message.text == "old request" for message in model_two.received_messages[0])
        assert len(model_two.received_messages) == 1
        # No copied entity was created at the new name with the original key.
        assert client.get_entity(EntityInstanceId(f"dafx-v2-{agent_name}", key)) is None
    finally:
        for active in reversed(running):
            active.stop()
        client.close()
