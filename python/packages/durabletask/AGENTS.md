# Durable Task Package (agent-framework-durabletask)

Durable execution support for long-running agent workflows using Azure Durable Functions.

## Main Classes

### Client Side

- **`DurableAIAgentClient`** - Client for invoking durable agents
- **`DurableAIAgent`** - Shim for creating durable agents

### Worker Side

- **`DurableAIAgentWorker`** - Worker that executes durable agent tasks
- **`DurableAgentExecutor`** - Executes agent logic within durable context
- **`AgentEntity`** - Durable entity for agent state management

### State Management

- **`DurableAgentState`** - State container for durable agents
- **`DurableAgentSession`** - Session management for durable agents
- **`DurableAIAgentOrchestrationContext`** - Orchestration context

### Callbacks

- **`AgentCallbackContext`** - Context for agent callbacks
- **`AgentResponseCallbackProtocol`** - Protocol for response callbacks

## Usage

For the PR #59 prototype, first configure a separate version-2 endpoint and hub with compatible workers and clients. Only then acknowledge that setup with `deployment_mode="isolated_v2"`. The gate applies to samples and tests too. Localhost does not prove isolation, and old workers and workflow histories must remain on the old engine. See the [deployment warning](README.md#version-2-deployment-warning).

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIChatCompletionClient
from agent_framework_durabletask import DurableAIAgentClient, DurableAIAgentWorker
from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker

# Client side
dt_client = TaskHubGrpcClient(host_address="localhost:4001")
agent_client = DurableAIAgentClient(dt_client)
durable_agent = agent_client.get_agent("assistant")

# Worker side
dt_worker = TaskHubGrpcWorker(host_address="localhost:4001")
agent_worker = DurableAIAgentWorker(dt_worker, deployment_mode="isolated_v2")

# Create a chat client for the agent
chat_client = OpenAIChatCompletionClient()
my_agent = Agent(client=chat_client, name="assistant")
agent_worker.add_agent(my_agent)
```

## Import Path

```python
from agent_framework_durabletask import DurableAIAgentClient, DurableAIAgentWorker
```
