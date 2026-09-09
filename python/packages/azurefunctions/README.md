# Get Started with Microsoft Agent Framework Durable Functions

[![PyPI](https://img.shields.io/pypi/v/agent-framework-azurefunctions)](https://pypi.org/project/agent-framework-azurefunctions/)

Please install this package via pip:

```bash
pip install agent-framework-azurefunctions --pre
```

Requires Python 3.10+ and `agent-framework-core>=1.13.0,<2`. Recorded local validation used
core 1.13.0 and 1.16.0. The local unit matrix also covers Python 3.10 and 3.13.

## Version 2 deployment warning

The settings below describe the local PR #59 implementation, not release readiness or the contents
of an already published package.

> **Breaking persisted-state change.** New writes use `schemaVersion="2.0.0"`. Python reads legacy
> `1.x` and revised `2.x` layouts, but the current .NET converter rejects major `2` and has no
> mailbox response lookup. The cross-runtime release gate is **not satisfied**. Do not deploy these
> writers where incompatible workers or polling clients can access converted entities. Rollback
> requires versions that preserve both version-2 response lookup and write behavior.

Legacy state without scalar ingestion cursors converts at an entity operation boundary. Surviving
responses receive a fresh delivery grace window, and known custom IDs retain legacy markers.
Conversion does not recover previously removed or altered original results. Non-empty legacy
`ingestedPositions` rejects conversion rather than guessing which positions were delivered.
In-flight legacy workflows using those cursors need a version-specific migration that is not
implemented. See [ADR-0032](../../../docs/decisions/0032-durable-thread-compaction.md#current-local-implementation-status)
for the contract, recorded validation and remaining gates. Its latest live DTS/Redis result does
not establish Azure Functions live-host validation.

## Durable Agent Extension

The durable agent extension lets you host Microsoft Agent Framework agents on Azure Durable Functions so they can persist state, replay conversation history, and recover from failures automatically.

### Basic Usage Example

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIChatCompletionClient
from agent_framework_azurefunctions import AgentFunctionApp

assistant = Agent(client=OpenAIChatCompletionClient(), name="assistant")
app = AgentFunctionApp(agents=[assistant])
```

Post messages using the generated `/api/agents/{agent_name}/run` endpoint.

### History and retention settings

`AgentFunctionApp` uses the same Python agent entity and history-provider integration as the direct
Durable Task worker. In-memory primary history is replaced, and durable history is injected when
no load-enabled primary exists. Substitution preserves `source_id`, `skip_excluded` and core storage
flags without enabling compaction. External primaries and store-only sinks retain their own policies.
Multiple load-enabled primaries are rejected. The selected provider owns appends through core's
hooks, followed by a final durable-provider flush. Only agents without a context pipeline use direct
entity transcript appends.

Eager pruning and pressure eviction are independent. The matrix assumes no explicit provider
`prune_excluded` override.

| `retention` | `max_state_bytes=None` | Positive integer byte budget |
| --- | --- | --- |
| `"keep_all"` (default) | No transcript deletion (default) | Evict eligible oldest groups only under pressure |
| `"follow_compaction"` | Prune eligible compaction exclusions only | Prune exclusions, then evict under pressure if needed |

- `max_state_bytes` defaults to `None`. Azure Functions cannot resolve its backend's hard limit,
  so `"backend_limit"` is rejected at registration. Use an explicit positive integer to enable
  pressure eviction. A budget does not enable blob offload or raise a backend limit. `"auto"` is
  no longer a retention mode.
- Watermarks default to `high_watermark=0.85` and `low_watermark=0.70`, with
  `0 < low_watermark < high_watermark <= 1`. The whole serialized entity counts, including mailbox,
  completion, session and ingestion state. Protected data can prevent a commit even after pruning.
- `response_delivery_window_seconds` defaults to `60` and must be a positive integer. Delivery
  expiry is independent of transcript retention.
- `add_agent()` overrides app defaults. Constructor `workflow_*` settings supply workflow defaults,
  and `configure_workflow()` can override them for newly registered nodes, including nested workflows.
  Omitted budgets or `INHERIT` inherit the enclosing default. Explicit `None` disables that budget.
- Explicit `prune_excluded=False` on `DurableHistoryProvider` disables eager pruning even with
  `follow_compaction`. It does not disable pressure eviction or change an external store's policy.

As an alternative to the default app above, configure an explicit budget for standalone agents and
disable it for an existing named `workflow`. The sample byte budget is an application choice, not
an inferred Functions backend limit.

```python
from agent_framework_durabletask import INHERIT

app = AgentFunctionApp(max_state_bytes=800_000, workflow_max_state_bytes=None)
app.add_agent(assistant, retention="follow_compaction", max_state_bytes=INHERIT)
app.configure_workflow(workflow)
```

`follow_compaction` only prunes exclusions produced by configured compaction. Workflow `full`,
`last_agent` and `custom` projection runs before per-target delta transport. Custom filters execute
during orchestration replay and must be synchronous, deterministic and side-effect-free, but need
not select monotonically increasing positions. Durable owns transport identities and ingestion
receipts without imposing a new core ID requirement.

### Service ownership, delivery and reset

Effective `store` follows run options, then agent defaults, then the client's `STORES_BY_DEFAULT`.
For example, `options={"store": False}` selects client-owned history even on a service-storing
client. Explicit `False` excludes saved or supplied service conversation IDs from that invocation
and its history hooks. A later service-owned run can reuse the saved service ID without importing
the intervening client-owned transcript. Switching branches does not migrate or merge history.
External and service-owned runs create no local request-message mirror.

HTTP polling uses independent original response snapshots in `responseMailbox`, including
serializable metadata and structured `value`. Transcript pruning or reset cannot change those
results. Expiry leaves `completedCorrelations` receipts and returns an already-completed status with
`response_expired`, never a reconstructed transcript response or another agent invocation.

Local reset clears session and transcript context but preserves live mailbox payloads, completion
receipts and ingestion evidence. Normal delivery expiry still applies. Reset with an external
primary raises `NotImplementedError` until a provider-owned clear operation is available.

Entity-local state commits once per operation. Model/runtime failures are not retried through a
generic non-streaming fallback. Only an unsupported-stream `TypeError` takes that fallback path.
Uncommitted model/tool effects and external appends can repeat after failure. Completion receipts
last until entity deletion and can exhaust capacity. A bounded receipt protocol and optional
retry-safe external-history adapters remain deferred, with no mandatory core API changes or
exactly-once guarantee for uncommitted effects.

For more details, review the Python [README](https://github.com/microsoft/agent-framework/tree/main/python/README.md) and the samples directory.
