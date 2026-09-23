# Get Started with Microsoft Agent Framework Durable Functions

[![PyPI](https://img.shields.io/pypi/v/agent-framework-azurefunctions)](https://pypi.org/project/agent-framework-azurefunctions/)

Please install this package via pip:

```bash
pip install agent-framework-azurefunctions --pre
```

## Durable Agent Extension

The durable agent extension lets you host Microsoft Agent Framework agents on Azure Durable Functions so they can persist state, replay conversation history, and recover from failures automatically.

### Current Runtime Contract On This Unreleased Stack

This unreleased host uses the [canonical Python runtime contract](../durabletask/README.md#current-runtime-contract-on-this-unreleased-stack)
for schema `2.0.0` writes, legacy readers, response values, migration, sessions and workflow replay.
This guide covers the Functions-specific behavior, not a separate compatibility promise.

`AgentFunctionApp` requires `deployment_mode="isolated_v2"` or
`DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2`. No other mode or automatic probe is supported.
This acknowledges an isolated hub with upgraded workers and clients. It cannot prove isolation
or detect peers. Use a new isolated hub and **fresh workflow instances, even after earlier v2 builds**.
Protocol `2` does not guarantee replay compatibility. Keep old runs on their original workers and hub.
The [ADR rollout gates](../../../docs/decisions/0032-durable-thread-compaction.md#state-evolution-and-compatibility) remain in force.

Migration is a privileged backend entity operation, with no generated HTTP or MCP endpoint.
Follow the shared [migration contract and request example](../durabletask/README.md#migration),
including operator quiescence, fencing, evidence and a separate empty destination.

### HTTP Routes and Responses

With the default `/api` prefix, generated routes are

| Method | Route | Purpose |
| --- | --- | --- |
| POST | `/api/agents/{agent_name}/run` | Send an agent message. |
| POST | `/api/workflow/{name}/run` | Start a top-level workflow. |
| GET | `/api/workflow/{name}/status/{instanceId}` | Read status, output and pending HITL requests. |
| POST | `/api/workflow/{name}/respond/{instanceId}/{requestId}` | Deliver a JSON HITL reply. |

`http_auth_level` defaults to `FUNCTION`. `enable_http_endpoints` controls agent run routes and
`enable_health_check` controls `/api/health` (both default true). `enable_mcp_tool_trigger` opts
agents into native MCP tools (default false).

Agent requests accept a JSON `message` with optional `session_id`, `role`, `response_format` and
`enable_tool_calls`, or a plain-text body. `session_id` also works in the query string. The deprecated
`thread_id` alias remains accepted, but conflicting nonblank IDs are rejected. Reuse the returned
`session_id` for later turns. JSON requests or `Accept: application/json` select JSON responses.
Agent `wait_for_response` defaults to true. A valid `x-ms-wait-for-response` header takes precedence
over query and body settings. With JSON responses, false returns `202` with `session_id` and `correlation_id`.

Workflow start query parameters are `runId`, `waitForResponse` (default false) and `timeoutSeconds` (default 10,
range 1-200 when waiting). A valid wait header overrides the query. The generic Functions `runId`
validator is Unicode-aware, with a 1-100-character limit, no leading `@`, no `/`, `\`, `#`, `?` or
control characters. It does not apply the standalone DTS ASCII-only rule or rewrite IDs.

Agent polling returns `200` for an available successful result, `410 Gone` for an expired or
unavailable v2 result with its retained `outcome`, and `500` for available failed v2 results or state
decode errors. Transient storage reads retry within `max_poll_retries` and `poll_interval_seconds`.
Polling does not persist cleanup. JSON `agent_response` snapshots retain the `_durable_value_policy`
marker under the shared value rules. Text responses expose unavailable outcomes in
`x-ms-durable-outcome`. See [delivery and maintenance](../durabletask/README.md#delivery-and-maintenance).

### Retention and State Budgets

Defaults are `retention="keep_all"`, `max_state_bytes=None`, `high_watermark=0.85`,
`low_watermark=0.70` and `response_delivery_window_seconds=60`. Eager pruning and pressure eviction
are independent opt-ins under the [shared retention contract](../durabletask/README.md#retention-and-state-budgets),
including protected groups, capacity failures, ownership, reset and receipt-growth limits.
Functions rejects `max_state_bytes="backend_limit"`, even with DTS. Use a positive integer or `None`.

`add_agent()` accepts per-agent retention, budget, watermark and delivery-window overrides.
`configure_workflow()` applies them to that workflow's agent entities and nested workflows, not
an aggregate workflow budget. App-level workflow defaults are `workflow_retention`,
`workflow_max_state_bytes`, `workflow_high_watermark`, `workflow_low_watermark` and
`workflow_response_delivery_window_seconds`. For budget overrides, omission or `INHERIT` from
`agent_framework_durabletask` inherits the enclosing default, while explicit `None` disables
pressure eviction. For other overrides, `None` inherits. Shared registrations require matching settings.

### Retention Metrics

Functions uses the [shared retention metrics](../durabletask/README.md#retention-metrics).
Deletion counts describe staged state, and `set_state` leaves commit status unknown even on return.
Applications configure the OpenTelemetry SDK, reader and exporter. Metrics never confirm a durable commit.

### Workflow HITL and Mixed Parent/Child Execution

Functions uses the shared [HITL activity and scheduler contract](../durabletask/README.md#workflow-hitl-and-mixed-parentchild-execution)
and [workflow start and child identity rules](../durabletask/README.md#workflow-starts-and-child-identity).
Only top-level workflows receive HTTP routes. Follow returned `respondUrl` values and actual child
IDs in `subworkflows` status maps, not physical ID parsing. Qualified `~` request paths remain unchanged.

The respond endpoint accepts early replies for known fixed IDs before their waits are published.
HTTP success acknowledges event delivery, not validation or handler success. Nested replies require
a recorded active child path. Status/respond routes reject other workflows' instances with `404`,
and replies to terminal top-level instances return `409`. A terminal child path returns `404`.
Pending waits survive other replies and mixed waves.
Functions exposes status and final output, not the standalone workflow event-streaming endpoint.

### JSON runtime boundary

Generated entities decode state and `run`/`migrate` inputs as plain JSON. Framework agent results,
generated workflow starts, child results and HITL values also use scoped JSON decoding. Start,
agent-result and child-result guards reject unsupported SDK layouts rather than use native decoding.
Unrelated native calls retain SDK behavior. Manually wrapping an entity factory does not install
the generated-entity boundary, and internal checkpoints still require trusted workers and storage.
See the [host coverage and SDK constraints](../../../docs/features/python-durable-json-boundaries.md#covered-host-paths).

Requires Python 3.10+, `agent-framework-core>=1.13.0,<2`, `azure-functions>=1.24.0,<2` and
`azure-functions-durable>=1.3.1,<2`. The shared dependency requires `durabletask>=1.7.1,<2`
and `pydantic>=2.11,<3`. Functions uses its own SDK's parent metadata.

### Basic Usage Example

```python
from agent_framework_azurefunctions import AgentFunctionApp

# Use only after isolating the hub and upgrading all workers and clients.
_app = AgentFunctionApp(deployment_mode="isolated_v2")
# Register an existing agent with _app.add_agent(agent).
```

See the [Functions samples](../../samples/azure_functions/README.md) for host settings, setup and
HTTP examples, and the [Agent Framework Python documentation](https://github.com/microsoft/agent-framework/tree/main/python).
