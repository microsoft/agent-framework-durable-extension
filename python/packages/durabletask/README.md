# Get Started with Microsoft Agent Framework Durable Task

[![PyPI](https://img.shields.io/pypi/v/agent-framework-durabletask)](https://pypi.org/project/agent-framework-durabletask/)

Please install this package via pip:

```bash
pip install agent-framework-durabletask --pre
```

## Durable Task Integration

The durable task integration lets you host Microsoft Agent Framework agents using the [Durable Task](https://github.com/microsoft/durabletask-python) framework so they can persist state, replay conversation history, and recover from failures automatically.

### Reader-first phased rollout

The current local implementation adds canonical `2.0.0` readers, not a released capability.
Mutable `DurableAgentState` still defaults to `1.1.0`. Exact `1.0.0`, `1.1.0` and `1.2.0`
inputs use the existing legacy model. This does not promise a full raw-preserving `1.2.0`
round trip.

Public `agent_framework_durabletask.read_agent_state(raw)` accepts a dictionary or JSON string.
For `2.0.0`, it returns a detached `SharedAgentStateReader`. Its `to_dict()` preserves the
original JSON, including unknown fields and profiles. `try_get_agent_response(correlation_id)`
looks up canonical results and completion receipts without lifecycle writes. Unknown profiles
remain inert in storage. A targeted unsupported profile projection may fail without changing
the original snapshot. Typed shared results require a present `value` and a JSON-preserving
projection. Consumers do not infer it from text, coerce types or discard unknown value fields.
`serialize_agent_response()` carries these restrictions through Core JSON using the versioned
`_durable_value_policy` marker. It does not duplicate the value, identify a runtime class or
establish completion authority. Use the matching loader when consuming that snapshot.

Both Durable Task and Azure Functions polling consumers read canonical results and retain
known completion outcomes when response delivery expires or is unavailable. Durable Task
storage reads use bounded polling retries.

V2 snapshots are raw-only, read-only state. `AgentEntity.run()`, `reset()`, state assignment
and `persist_state()` reject v2 backing state, even with empty history and result maps or a
duplicate request. SDK, HTTP and MCP run APIs still signal a command before polling.
**Read-only views are not read-only run APIs.** Do not use those APIs to execute against
v2-backed entities with this implementation or to guarantee that no command was dispatched.

Roll out reader support first. A matching future writer is required before targeting a v2
deployment. Do not roll back v2 state to an older lossy reader/writer. This phase adds no
automatic activation, deployment gate or environment switch and makes no historical workflow
changes. V2 readers do not resume provider sessions or workflow history.

The Core requirement remains `agent-framework-core>=1.13.0,<2`. This package directly requires
`pydantic>=2.11,<3` for structured response handling.

### Basic Usage Example

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIChatCompletionClient
from agent_framework_durabletask import DurableAIAgentWorker
from durabletask.worker import TaskHubGrpcWorker

# Create the worker
worker = TaskHubGrpcWorker(host_address="localhost:4001")
agent_worker = DurableAIAgentWorker(worker)

chat_client = OpenAIChatCompletionClient()
my_agent = Agent(client=chat_client, name="assistant")
agent_worker.add_agent(my_agent)
```

For more details, review the standalone [Durable Task samples](https://github.com/microsoft/agent-framework-durable-extension/tree/main/python/samples) and the full [Agent Framework Python documentation](https://github.com/microsoft/agent-framework/tree/main/python).
