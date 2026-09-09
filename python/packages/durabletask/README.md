# Get Started with Microsoft Agent Framework Durable Task

[![PyPI](https://img.shields.io/pypi/v/agent-framework-durabletask)](https://pypi.org/project/agent-framework-durabletask/)

Please install this package via pip:

```bash
pip install agent-framework-durabletask --pre
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
for the contract, recorded validation and remaining gates.

## Durable Task Integration

The durable task integration lets you host Microsoft Agent Framework agents using the [Durable Task](https://github.com/microsoft/durabletask-python) framework so they can persist state, replay conversation history, and recover from failures automatically.

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

### History and retention settings

Registration injects durable history when no load-enabled primary exists, or replaces an in-memory
primary without changing its `source_id`, `skip_excluded` or core storage flags. External primaries
and store-only sinks keep their own storage policies. Multiple load-enabled primaries are rejected.
Registration does not enable compaction. The selected provider owns appends through core's hooks,
with a final durable-provider flush after the run. Only agents without a context pipeline use direct
entity transcript appends.

Eager pruning and pressure eviction are independent. The matrix assumes no explicit provider
`prune_excluded` override.

| `retention` | `max_state_bytes=None` | Positive byte budget or `"backend_limit"` |
| --- | --- | --- |
| `"keep_all"` (default) | No transcript deletion (default) | Evict eligible oldest groups only under pressure |
| `"follow_compaction"` | Prune eligible compaction exclusions only | Prune exclusions, then evict under pressure if needed |

- `max_state_bytes` defaults to `None`. Direct DTS resolves `"backend_limit"` to 1,048,576 bytes
  (1 MiB). A positive integer sets an application budget, not a larger backend limit. `"auto"` is
  no longer a retention mode.
- Watermarks default to `high_watermark=0.85` and `low_watermark=0.70`, with
  `0 < low_watermark < high_watermark <= 1`. The whole serialized entity counts, including mailbox,
  completion, session and ingestion state. Protected data can prevent a commit even after pruning.
- `response_delivery_window_seconds` defaults to `60` and must be a positive integer. Delivery
  expiry is independent of transcript retention.
- `add_agent()` and `configure_workflow()` accept overrides. Omitted budgets or `INHERIT` use the
  worker default. Explicit `None` disables that inherited budget. Workflow settings apply to newly
  registered agent nodes, including nested workflows.
- Explicit `prune_excluded=False` on `DurableHistoryProvider` disables eager pruning even with
  `follow_compaction`. It does not disable pressure eviction. Neither retention control configures
  an external store's retention policy.

As an alternative to the default registration above, use an unregistered worker, `my_agent` and an
existing named `workflow` to opt into a DTS budget while disabling it for workflow nodes.

```python
from agent_framework_durabletask import INHERIT

agent_worker = DurableAIAgentWorker(worker, max_state_bytes="backend_limit")
agent_worker.add_agent(my_agent, retention="follow_compaction", max_state_bytes=INHERIT)
agent_worker.configure_workflow(workflow, max_state_bytes=None)
```

`follow_compaction` only prunes exclusions produced by configured compaction. Without a strategy,
there are no exclusions to prune. Workflow `full`, `last_agent` and `custom` projection runs before
per-target delta transport. Custom filters must be synchronous, deterministic and side-effect-free,
but need not select monotonically increasing positions. Durable owns transport identities and
ingestion receipts without imposing a new core ID requirement.

### Service ownership, delivery and reset

Effective `store` follows run options, then agent defaults, then the client's `STORES_BY_DEFAULT`.
For example, `options={"store": False}` selects client-owned history even on a service-storing
client. Explicit `False` excludes saved or supplied service conversation IDs from that invocation
and its history hooks. A later service-owned run can reuse the saved service ID without importing
the intervening client-owned transcript. Switching branches does not migrate or merge history.
External and service-owned runs create no local request-message mirror.

`responseMailbox` holds independent original serializable response snapshots, including metadata
and structured `value`, rather than rebuilding results from the mutable transcript. After delivery
expiry, `completedCorrelations` prevents reinvocation and returns an already-completed status with
`response_expired`. Version-2 lookup never falls back to a transcript response.

Local reset clears session and transcript context but preserves live mailbox payloads, completion
receipts and ingestion evidence. Normal delivery expiry still applies. Reset with an external
primary raises `NotImplementedError` until a provider-owned clear operation is available.

Entity-local state commits once per operation. Model/runtime failures are not retried through a
generic non-streaming fallback. Only an unsupported-stream `TypeError` takes that fallback path.
Uncommitted model/tool effects and external appends can repeat after failure. Completion receipts
last until entity deletion and can exhaust capacity. A bounded receipt protocol and optional
retry-safe external-history adapters remain deferred, with no mandatory core API changes or
exactly-once guarantee for uncommitted effects.

For more details, review the standalone [Durable Task samples](https://github.com/microsoft/agent-framework-durable-extension/tree/main/python/samples) and the full [Agent Framework Python documentation](https://github.com/microsoft/agent-framework/tree/main/python).
