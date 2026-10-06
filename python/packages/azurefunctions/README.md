# Get Started with Microsoft Agent Framework Durable Functions

[![PyPI](https://img.shields.io/pypi/v/agent-framework-azurefunctions)](https://pypi.org/project/agent-framework-azurefunctions/)

Please install this package via pip:

```bash
pip install agent-framework-azurefunctions --pre
```

## Durable Agent Extension

The durable agent extension lets you host Microsoft Agent Framework agents on Azure Durable Functions so they can persist state, replay conversation history, and recover from failures automatically.

<a id="current-runtime-contract-on-this-branch"></a>

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

### Entity Operations

Generated entities use the same operation contract as the standalone Python host: `run`, `reset`,
`expire_responses`, `migrate` and administrative `delete`. Raw state setters and internal helpers
are rejected. These operations are not arbitrary commands exposed by the generated HTTP/MCP routes.

The deprecated `run_agent` alias is removed. Direct callers must use `run`. Maintenance results
also change from the 1.x Functions adapter: `reset` returns no value instead of `{"status":"reset"}`,
and `expire_responses` returns an integer instead of `{"expired":count}`. Failures use native SDK
operation failures, not the old error-result wrapper. See the shared
[maintenance and deletion contract](../durabletask/README.md#delivery-and-maintenance), including
receipt preservation, retry-count semantics and deletion's loss of duplicate-execution protection.

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

Every orchestrator and entity worker the app registers, including your own functions and
blueprints, decodes framework payloads as plain JSON. That covers agent state and `run`/`migrate`
inputs, framework agent results, generated workflow starts, child results and HITL values. The
generated agent entity also decodes non-framework operation input as plain JSON, including typed
helper inputs that the dispatcher will reject. State and migration targets keep exact counters.
Other functions keep the Functions converter's behavior, so an unannotated native input still gets
its object reconstruction. Registration fails if a function's durable worker can't be located.
Internal checkpoints still require trusted workers and storage.
See the [host coverage and SDK constraints](../../../docs/features/python-durable-json-boundaries.md#covered-host-paths).

Requires Python 3.13+, `agent-framework-core>=1.19.0,<2`, `azure-functions>=2.3.0,<3` and
`azure-functions-durable>=2.0.0rc2,<3`, which brings `durabletask>=1.11.0`. The shared dependency
requires `durabletask>=1.7.1,<2` and `pydantic>=2.11,<3`.

Use the GA `Microsoft.Azure.Functions.ExtensionBundle` with version range `[4.38.1, 5.0.0)`.
Local Azure Storage development requires Azurite 3.37.0 or later, without an API-validation bypass.
The Python Durable Functions SDK remains prerelease; the extension bundle does not need to be Preview.

### Optional Blob Payload Offloading

SDK-managed offloading is available in `azure-functions-durable` 2.0.0rc2. It is disabled
unless you configure a payload store. Install the optional dependencies with:

```bash
pip install "agent-framework-azurefunctions[azure-blob-payloads]" --pre
```

Configure the inherited SDK method once at app startup, before any invocations. It may
be called after registering agents, workflows and blueprints:

```python
import os

from agent_framework_azurefunctions import AgentFunctionApp
from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

app = AgentFunctionApp(deployment_mode="isolated_v2")
app.configure_large_payloads(
	payload_store=BlobPayloadStore(
		BlobPayloadStoreOptions(
			connection_string=os.environ["PAYLOAD_STORAGE_CONNECTION_STRING"],
			container_name="durable-payloads",
			threshold_bytes=256 * 1024,
		)
	)
)
```

Supply a full Blob connection string in `PAYLOAD_STORAGE_CONNECTION_STRING`, including
the Blob endpoint when using Azurite. `UseDevelopmentStorage=true` is not a full connection
string for the Python Blob SDK. Configuration does not automatically discover or reuse
`AzureWebJobsStorage`. Use the store's credential options for identity-based deployments.

Payloads above the threshold are stored as blobs and replaced with references. This includes
agent entity state, inputs and results, workflow inputs and outputs, custom status, external
events, sub-orchestrations and continue-as-new. The default stored-payload limit is 10 MiB;
`max_stored_payload_bytes` can change it. Configured SDK clients hydrate these references,
but unconfigured clients and host management HTTP endpoints may expose reference strings.

There is one store per Python worker process, shared by all durable functions and clients,
including imported blueprints. Registering the same store object again is allowed; a different
object raises `ValueError`. Keep the store open for the process lifetime, and configure all
scaled-out workers and subsequent deployments with access to the same backing storage.
This is separate from the Azure Storage backend's automatic large-message handling.

Offloading does not reduce decoded state or model context, and does not change retention,
pressure budgets or the isolated-hub migration requirements. Keep explicit state budgets
where needed. Reset, response expiry, entity deletion and orchestration purge are not SDK
payload-blob cleanup. Retain blobs while any state or history needed for replay references
them, and manage storage lifecycle separately.

> [!WARNING]
> Store-recognized whole strings are reserved references, even below the threshold. For the
> Blob store, this includes raw or JSON-quoted `blob:v1:<container>:<blobName>` values. Wrap
> literal references in an object, such as `{"reference": "blob:v1:container:blob"}`, and
> retain that wrapper across durable boundaries. References are not authorization checks.
> `container_name` selects uploads, not the containers credentials may read. Use least-privilege
> storage credentials and reject or validate references from untrusted callers before durable
> API calls. Framework plain-JSON decoding does not remove this transport trust requirement.

> [!WARNING]
> Blob failures that escape storage retries can fail durable invocations, including causing
> terminal orchestration failure. Activity retry policies do not cover all transport I/O,
> and the SDK does not guarantee host abandonment and redelivery after storage failure.

`get_agent()` needs a two-argument `(context, input)` orchestrator. A one-argument orchestrator
receives the 1.x compatibility context, which cannot call agents. Session keys can't contain `@`,
because durabletask rejects it in entity keys.

### Basic Usage Example

```python
from agent_framework_azurefunctions import AgentFunctionApp

# Use only after isolating the hub and upgrading all workers and clients.
_app = AgentFunctionApp(deployment_mode="isolated_v2")
# Register an existing agent with _app.add_agent(agent).
```

See the [Functions samples](../../samples/azure_functions/README.md) for host settings, setup and
HTTP examples, and the [Agent Framework Python documentation](https://github.com/microsoft/agent-framework/tree/main/python).
