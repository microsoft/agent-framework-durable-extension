# Copyright (c) Microsoft. All rights reserved.

"""Initial agent input identity through a cycle, with real cold entity ingestion."""

import asyncio
from copy import deepcopy
from typing import Any

from _execution_test_support import JsonStateProvider, RecordingChatClient
from agent_framework import Agent, AgentExecutor, Executor, WorkflowBuilder, WorkflowContext, handler
from test_workflow_admission_review import _registered_run
from test_workflow_protocol_review import _complete, _drain
from typing_extensions import Never

from agent_framework_durabletask import AgentEntity, DurableAgentState, serialize_agent_response


def test_nonempty_initial_string_keeps_occurrence_receipt_when_cycle_returns() -> None:
    clients = {name: RecordingChatClient() for name in ("first", "second")}
    agents = {name: Agent(client=client, name=name) for name, client in clients.items()}
    nodes = {name: AgentExecutor(agent, id=name) for name, agent in agents.items()}
    workflow = (
        WorkflowBuilder(name="initial-cycle", start_executor=nodes["first"], output_from=[nodes["first"]])
        .add_edge(nodes["first"], nodes["second"], condition=lambda response: response.agent_response.text == "reply-1")
        .add_edge(nodes["second"], nodes["first"])
        .build()
    )
    providers = {
        name: JsonStateProvider(session_id="root-run", entity_name=f"dafx-initial-cycle-{name}") for name in nodes
    }
    requests: dict[str, list[dict[str, Any]]] = {name: [] for name in nodes}

    def entity_call(entity_id: Any, operation: str, request: dict[str, Any]) -> Any:
        name = entity_id.entity.removeprefix("dafx-initial-cycle-")
        assert name in nodes and operation == "run" and entity_id.key == "root-run"
        requests[name].append(deepcopy(request))
        response = asyncio.run(AgentEntity(agents[name], state_provider=providers[name]).run(deepcopy(request)))
        assert response.additional_properties.get("durable_status") != "error"
        return _complete(serialize_agent_response(response))

    _drain(_registered_run(workflow, entity_call)[0])
    assert len(requests["first"]) == 2 and len(requests["second"]) == 1
    # One initial user occurrence must remain one occurrence after returning.
    assert [sum(message.text == "go" for message in batch) for batch in clients["first"].received_messages] == [1, 1]
    first, returned = requests["first"]
    assert len(first["contextMessages"]) == len(first["contextMessageIds"]) == 1
    initial_id = first["contextMessageIds"][0]
    assert initial_id in requests["second"][0]["contextMessageIds"]
    assert initial_id not in returned["contextMessageIds"]
    assert all(message["role"] != "user" for message in returned["contextMessages"])
    persisted = DurableAgentState.from_dict(deepcopy(providers["first"].raw))
    assert initial_id in persisted.data.ingested_messages
    assert sum(message.text == "go" for entry in persisted.data.conversation_history for message in entry.messages) == 1


def test_nonagent_initial_string_and_loop_messages_stay_raw() -> None:
    seen: list[str] = []

    class Raw(Executor):
        @handler(input=str, output=str, workflow_output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str, str]) -> None:
            assert type(message) is str
            seen.append(message)
            if message == "go":
                await ctx.send_message("back")
            else:
                await ctx.yield_output(message)

    class Return(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str, Never]) -> None:
            assert type(message) is str and message == "back"
            await ctx.send_message(message)

    raw, back = Raw(id="raw"), Return(id="back")
    workflow = (
        WorkflowBuilder(name="raw-cycle", start_executor=raw, output_from=[raw])
        .add_edge(raw, back)
        .add_edge(back, raw)
        .build()
    )
    assert _drain(_registered_run(workflow)[0]) == ["back"]
    assert seen == ["go", "back"]
