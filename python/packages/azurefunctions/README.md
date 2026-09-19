# Get Started with Microsoft Agent Framework Durable Functions

[![PyPI](https://img.shields.io/pypi/v/agent-framework-azurefunctions)](https://pypi.org/project/agent-framework-azurefunctions/)

Please install this package via pip:

```bash
pip install agent-framework-azurefunctions --pre
```

## Durable Agent Extension

The durable agent extension lets you host Microsoft Agent Framework agents on Azure Durable Functions so they can persist state, replay conversation history, and recover from failures automatically.

### Current Runtime Contract On This Branch

This README describes the current `python-runtime-protocol` branch. It documents unreleased
behavior and should not be read as a released compatibility promise.

The Azure Functions host participates in the same canonical `DurableAgentState` schema `2.0.0`
runtime as the standalone Durable Task host. `read_agent_state(raw)` remains explicitly backward
compatible with legacy `1.x` payloads. It accepts a dictionary or JSON string and returns:

- `LegacyDurableAgentState` for exact `1.0.0`, `1.1.0`, and `1.2.0` inputs
- `SharedAgentStateReader` for exact `2.0.0` inputs

Other schema versions are rejected. Within an accepted `2.0.0` snapshot, unknown fields and
unknown protocol content remain preserved in detached raw JSON until a caller explicitly asks for
a narrower projection.

The v2 reader preserves the original JSON, including unknown fields, shared-value metadata,
session state, receipts, and protocol details. `try_get_agent_response(correlation_id)` reads
canonical results and receipts without writing back lifecycle changes. When a response is expired
or unavailable, the HTTP polling surface returns `410 Gone` with the retained completion `outcome`.
Available failed results and state decode errors return `500`. Transient storage reads retry only
within the bounded polling limit.

Known response snapshots preserve the explicit shared-value policy. Typed projections require a
present `value` field and a JSON-preserving decode. Consumers do not infer values from text,
coerce missing payloads, or discard unknown structured fields. The JSON `agent_response` snapshot
carries the versioned `_durable_value_policy` marker so the matching loader can preserve the same
rules after HTTP transport. It does not prove execution authority, duplicate the value, or identify
a runtime-native class by itself.

`AgentFunctionApp` now requires an explicit isolated v2 acknowledgement. Pass
`deployment_mode="isolated_v2"` or set `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2`.
There is no automatic probe and no other deployment mode is accepted. This is an operator
acknowledgement that the task hub is isolated and the clients are upgraded. It is not runtime
proof of isolation and it cannot detect peer workers. Existing workflows and historical runs
must stay on the old engine. Do not update a live hub in place and then replay historical runs
through the new runtime. Use new isolated hubs and upgraded clients.

The current layer includes canonical transaction state, session capture, and the workflow
protocol boundary. Migration and retention automation are not implemented yet. Treat any move
from legacy hubs to isolated v2 hubs as an operator-managed upgrade path, not as proof that an
in-place migration or replayed history is supported.

Workflow start input wrapping is not universal. `DurableWorkflowClient` and generated HTTP
start routes wrap new inputs for host-generated workflows. Application-owned native orchestrators
retain their original input contracts. Do not wrap every orchestration payload in this envelope.

There is no automatic transcript recovery for a rejected service conversation ID. A specifically
rejected parent response can receive up to three additional identical-request retries for a
visibility delay, but only before any output, tool execution or session advance. The saved parent
ID is not cleared. Old recorded workflow histories must remain on the old engine.

Retention defaults currently preserve every physical message. Response delivery availability is
time-bounded, with a default expiry window of `60` seconds, but receipt records are retained and
never deleted by response expiry alone. `reset` clears local transcript and session state while
preserving completion receipts and live results. External-primary reset requires a provider-owned
clear operation and is rejected rather than silently clearing only local state. There are no runtime retention knobs
yet for automatic transcript pruning or receipt cleanup. The application remains responsible for
its own maintenance policy. Per-agent and per-workflow delivery windows are configurable through
the host registration APIs.

The Core requirement remains `agent-framework-core>=1.13.0,<2`. The Durable Task dependency
directly requires `pydantic>=2.11,<3` for structured response handling.

### Basic Usage Example

See the durable functions integration sample in the repository to learn how to:

```python
from agent_framework_azurefunctions import AgentFunctionApp

_app = AgentFunctionApp(deployment_mode="isolated_v2")
```

- Register agents with `AgentFunctionApp`
- Post messages using the generated `/api/agents/{agent_name}/run` endpoint

For more details, review the Python [README](https://github.com/microsoft/agent-framework/tree/main/python/README.md) and the samples directory.
