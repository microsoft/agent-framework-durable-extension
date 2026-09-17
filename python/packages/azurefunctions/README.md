# Get Started with Microsoft Agent Framework Durable Functions

[![PyPI](https://img.shields.io/pypi/v/agent-framework-azurefunctions)](https://pypi.org/project/agent-framework-azurefunctions/)

Please install this package via pip:

```bash
pip install agent-framework-azurefunctions --pre
```

## Durable Agent Extension

The durable agent extension lets you host Microsoft Agent Framework agents on Azure Durable Functions so they can persist state, replay conversation history, and recover from failures automatically.

### Reader-first phased rollout

The current local implementation adds canonical `2.0.0` readers, not a released capability.
Mutable `DurableAgentState` still defaults to `1.1.0`. Exact `1.0.0`, `1.1.0` and `1.2.0`
inputs use the existing legacy model, without a full raw-preserving `1.2.0` round-trip guarantee.

Public `agent_framework_durabletask.read_agent_state(raw)` accepts a dictionary or JSON string.
For `2.0.0`, it returns a detached `SharedAgentStateReader`. Its `to_dict()` preserves original
JSON, including unknown fields and profiles. `try_get_agent_response(correlation_id)` reads
canonical results and completion receipts without lifecycle writes. Unknown profiles stay
inert in storage. A targeted unsupported projection may fail without altering the snapshot.

Durable Task and Azure Functions polling read canonical results and retain known outcomes
after response expiry or unavailability. Azure Functions HTTP returns `410` with the retained
`outcome`. Decoded v2 responses include an `agent_response` snapshot in JSON that preserves
structured values, including explicit null.
Typed shared results require a present `value` and a JSON-preserving projection rather than
text inference, type coercion or discarded fields.
Available failed results and state decode errors return `500`. Transient storage reads retry
within the bounded polling limit.

V2 is raw-only, read-only state. `AgentEntity.run()`, `reset()`, state assignment and
`persist_state()` reject v2 backing state, even with empty history and result maps or duplicate
requests. SDK, HTTP and MCP run APIs still signal before polling. **Read-only views are not
read-only run APIs.** They cannot execute against v2-backed entities with this implementation
or guarantee that no command was dispatched.

Roll out reader support first. A matching future writer is required before targeting a v2
deployment. Do not roll back v2 state to an older lossy reader/writer. This phase adds no
automatic activation, deployment gate or environment switch and makes no historical workflow
changes. V2 readers do not resume provider sessions or workflow history.

The Core requirement remains `agent-framework-core>=1.13.0,<2`. The Durable Task dependency
directly requires `pydantic>=2.11,<3` for structured response handling.

### Basic Usage Example

See the durable functions integration sample in the repository to learn how to:

```python
from agent_framework_azurefunctions import AgentFunctionApp

_app = AgentFunctionApp()
```

- Register agents with `AgentFunctionApp`
- Post messages using the generated `/api/agents/{agent_name}/run` endpoint

For more details, review the Python [README](https://github.com/microsoft/agent-framework/tree/main/python/README.md) and the samples directory.
