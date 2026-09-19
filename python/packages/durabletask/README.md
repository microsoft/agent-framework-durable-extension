# Get Started with Microsoft Agent Framework Durable Task

[![PyPI](https://img.shields.io/pypi/v/agent-framework-durabletask)](https://pypi.org/project/agent-framework-durabletask/)

Please install this package via pip:

```bash
pip install agent-framework-durabletask --pre
```

## Durable Task Integration

The durable task integration lets you host Microsoft Agent Framework agents using the [Durable Task](https://github.com/microsoft/durabletask-python) framework so they can persist state, replay conversation history, and recover from failures automatically.

### Current Runtime Contract On This Unreleased Stack

This README describes the current unreleased schema-v2 runtime, not a released compatibility
promise.

The public mutable state model is now canonical `DurableAgentState` schema `2.0.0`.
`read_agent_state(raw)` remains explicitly backward compatible with legacy `1.x` payloads.
It accepts a dictionary or JSON string and returns:

- `LegacyDurableAgentState` for exact `1.0.0`, `1.1.0`, and `1.2.0` inputs
- `SharedAgentStateReader` for exact `2.0.0` inputs

Other schema versions are rejected. Within an accepted `2.0.0` snapshot, unknown fields and
unknown protocol content remain preserved in detached raw JSON until a caller explicitly asks for
a narrower projection.

The v2 reader preserves the original JSON, including unknown fields, shared-value metadata,
session state, receipts, and protocol details. `to_dict()` returns detached original JSON.
`try_get_agent_response(correlation_id)` reads canonical results and receipts without writing
back lifecycle changes. If the response is expired or otherwise unavailable, polling consumers
retain the known completion outcome instead of deleting or rewriting the receipt. Durable Task
storage reads use bounded polling retries.

Known response snapshots preserve the explicit shared-value policy. Typed projections require a
present `value` field and a JSON-preserving decode. Consumers do not infer values from text,
coerce missing payloads, or discard unknown structured fields. The Core transport snapshot carries
the versioned `_durable_value_policy` marker so the matching loader can preserve the same rules
after JSON transport. It does not prove execution authority, duplicate the value, or identify a
runtime-native class by itself.

Host construction now requires an explicit isolated v2 acknowledgement. Pass
`deployment_mode="isolated_v2"` or set `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2`.
There is no automatic probe and no other deployment mode is accepted. This is an operator
acknowledgement that the task hub is isolated and the clients are upgraded. It is not runtime
proof of isolation and it cannot detect peer workers. Existing workflows and historical runs
must stay on the old engine. Do not update a live hub in place and then replay historical runs
through the new runtime. Use new isolated hubs and upgraded clients.

The current layer includes canonical transaction state, session capture, the workflow
protocol boundary, and a privileged manual migration path. Migration is a backend entity
operation only. There is no generated HTTP or MCP migration endpoint. The helper stages a
detached result with no model, tool, provider, storage, or transcript I/O. The operator must
quiesce the legacy owner, fence it, authorize ownership transfer, and commit the staged result
exactly once into a separate empty isolated v2 destination. The migration evidence cannot prove
that authority or completeness on its own.

The request contract is explicit and complete. `migrate(request)` requires `source`,
`sourceDigest`, `sourceSessionId`, `destinationSessionId`, `migrationId`, and
`ownershipTransferId`. It optionally accepts `deliveryEvidence`, `completionEvidence`, and
`requireKnownOutcomes`. The destination must match the current entity, must be separately
addressed, and must still be empty. This is never an in-place rewrite and never a replay into a
used destination. The same exact request may be retried idempotently. The runtime records a
`requestDigest` and accepts only an identical retry after commit, without rewriting results or
refreshing their grace period. Changed requests are rejected, including after acknowledgement
loss or a later host run. If the state setter raises, only local staged/cache state rolls back.
The backend may have committed, so the next operation reloads authoritative state before proceeding.

`source` accepts only valid published shared legacy `1.0.0`, `1.1.0`, or `1.2.0` state. All
version `2.x` sources are rejected. There is no private prototype v2 converter. The migration
path does not claim that nullable legacy fields are normalized, that lossy legacy data becomes
canonical automatically, or that unversioned private shapes are accepted. An existing
`source.data.migration` field is rejected, never overwritten. Any `source.data.pythonIngestion`
field is rejected before legacy parsing, including null, empty, foreign and recognized profiles.
It is neither relocated nor carried into v2 where it could become active bookkeeping. Other
unknown application fields remain at their original locations.

Nonempty legacy `ingestedPositions` requires `deliveryEvidence`. When supplied, this evidence
must contain `sourceDigest` matching the source snapshot, a nonblank `evidenceId`, `complete=true`,
and canonical `messages` from `Message.to_dict()`. It must cover all accepted inputs, including
evicted inputs and every accepted revision. The only additional field is `messagePositions`,
required when legacy `ingestedPositions` is nonempty and optional otherwise. When present, it must
align one-for-one with `messages`. Each entry is `null` for an unpositioned input or exactly
`producer` (a nonblank string) and `position` (a nonnegative integer, not a boolean). Attributed
producers and their maximum positions must match `ingestedPositions`. This checks consistency,
not completeness. Positions are never inferred from public message IDs or naming heuristics.

A complete, independent `completionEvidence` journal is required for any retained history,
a `truncation` field, nonempty session state or `ingestedPositions`, or nonempty supplied delivery
`messages`. An empty delivery journal alone does not require it. The journal must contain exactly
`sourceDigest` matching the source snapshot, a nonblank `evidenceId`, `complete=true`, and `results`,
including pruned original terminal results. Each result needs `correlationId`, the original
`outcome`, `completedAt`, and `response` with `messages`, plus `error` for failures.
`resultExpiresAt` is forbidden. An empty `results` list asserts there were no completions and is
valid only without retained responses. `requireKnownOutcomes=false` cannot waive these requirements.
Transcript fragments, accepted inputs, and later acknowledgements do not establish original outcomes.
Imported results receive a new bounded delivery grace period from the migration clock plus the
configured delivery window. Their original `completedAt` values remain unchanged, and identical
retries never extend the deadline.

Full request example for a fresh source. Before submitting, replace the `sourceDigest` placeholder
with `agent_framework_durabletask.state_snapshot_digest(source)` computed from the unmodified
`source` object.

```json
{
  "source": {
    "schemaVersion": "1.1.0",
    "data": {
      "conversationHistory": []
    }
  },
  "sourceDigest": "<computed source digest>",
  "sourceSessionId": "legacy-provider:source-session",
  "destinationSessionId": "@agent@dest-session",
  "migrationId": "migration-1",
  "ownershipTransferId": "transfer-1"
}
```

If the legacy source carries external session state, its original `session_id` is preserved and
used by later host runs only after the migration is committed into the destination. Migration is
the boundary that transfers that destination binding. A nonblank saved `session_id` must match
`sourceSessionId`. An absent, null or blank saved ID is filled from `sourceSessionId`.
Within a session object, `state` must be an object when present. Only omission defaults to `{}`,
not null, a list or a scalar. `service_session_id` may be absent, null, any string or a JSON object,
matching Core 1.13 and 1.16's `str | ServiceSessionId | None` contract, where `ServiceSessionId`
is `Mapping[str, Any]`. Empty strings and objects are accepted. Object keys must be strings, but
no particular keys are required and values may be any strict JSON, including unknown metadata.
Service IDs, state values (including registered-type JSON), and unknown session siblings are
preserved without trimming, Core session deserialization, provider imports or provider calls
during migration. Cold session restoration preserves structured IDs. Continuation still requires
a compatible agent/provider, since Core's generic chat `Agent` requires a string service ID.

Workflow start input wrapping is not universal. `DurableWorkflowClient` and generated HTTP
start routes wrap new inputs for host-generated workflows. Application-owned native orchestrators
retain their original input contracts. Do not wrap every orchestration payload in this envelope.

There is no automatic transcript recovery for a rejected service conversation ID. A specifically
rejected parent response can receive up to three additional identical-request retries for a
visibility delay, but only before any output, tool execution or session advance. The saved parent
ID is not cleared. Old recorded workflow histories must remain on the old engine.

The optional migration helper budget `max_state_bytes` is a neutral admission limit, not a
pressure policy. If the fully staged destination exceeds that bound, migration fails with
`StateCapacityError`. It does not prune transcript, receipts, session state, or unknown JSON to
fit. Runtime transcript retention is configured separately below.

The Core requirement remains `agent-framework-core>=1.13.0,<2`. This package directly requires
`pydantic>=2.11,<3` for structured response handling.

The history bridge requires an exact `2.0.0` snapshot. Even `get_messages()` may repair missing
or duplicate internal IDs, so it is not a read-only inspection path. Mutation-capable hooks reject
legacy and unsupported versions before changing stored history or working state. Use
`read_agent_state()` for legacy inspection instead.

Explicit durable and external primary providers retain the user's hook order, so place history
before a matching before-compaction provider when that strategy must see loaded history.
Provider namespaces must be unique, including store-only sinks, and conflicts with an injected
history source raise an error rather than automatically reassigning a namespace.

Canonical media keeps its declared content kind rather than inferring it from the URI scheme.
New response timestamps without an offset are interpreted as UTC. Valid stored timestamps retain
their original offset and fractional precision.

### Retention and State Budgets

The defaults are `retention="keep_all"` and `max_state_bytes=None`. They preserve physical
transcript messages and impose no local pressure cap. Backend limits still apply.

Eager pruning and pressure eviction are independent opt-ins.

- `retention="follow_compaction"` physically removes eligible compaction exclusions when durable
  history is flushed, even without a pressure budget. It does not configure a compaction strategy.
- A positive integer `max_state_bytes` enables pressure eviction, including with `keep_all`.
  At `high_watermark=0.85`, the runtime plans removal of oldest eligible atomic message groups
  toward `low_watermark=0.70`, or the protected floor if it is higher but still below the high
  watermark. Watermarks must be finite and satisfy `0 < low_watermark < high_watermark <= 1`.
- `max_state_bytes=None` disables pressure eviction, not eager pruning. Booleans, zero and
  negative budgets are rejected.

The estimate covers the whole entity using Python's default JSON serialization with ASCII
escaping, including non-text content, delivery records, session state, metadata and truncation
records. It excludes transport framing and is not a backend acceptance guarantee.
`max_state_bytes="backend_limit"` is a 1 MiB (`1_048_576` bytes) convenience only when the wrapped
worker is a `DurableTaskSchedulerWorker`. A generic `TaskHubGrpcWorker` cannot resolve it.
`AgentFunctionApp` rejects it, even when Functions uses DTS. Use an explicit positive budget or
`None` there.

Pressure eviction leaves terminal results, completion and ingestion receipts, session/control
state, opaque unknown entries and retained entry metadata intact. System messages and the latest
exchange are protected, and linked tool, reasoning and persisted atomic groups are not split.
Eager pruning also protects pending tool calls and groups with any included member. These
protections can prevent the requested reduction. A protected floor at or above the high watermark,
or an unreachable safe target, raises `StateCapacityError` without applying a partial pressure plan.
The enclosing run restores its local staged/cache state on failure. This does not undo model,
tool or external-provider side effects. If a host state write raises, backend acknowledgement is
unknown, not proof of rollback. The next operation reloads authoritative state.

#### Registration and History Ownership

`DurableAIAgentWorker` sets host defaults. `add_agent()` and `configure_workflow()` accept
`retention`, `max_state_bytes`, both watermarks and `response_delivery_window_seconds` overrides.
Workflow settings apply to its agent entities and nested workflows, not one aggregate workflow
budget. For budget overrides, omission or `INHERIT` from `agent_framework_durabletask` uses the
host default, while explicit `None` disables pressure eviction. For the other overrides, `None`
inherits. Shared workflow registrations must have matching settings.

A hand-configured `DurableHistoryProvider(prune_excluded=...)` keeps its explicit value, including
`False`, ahead of the inherited eager-pruning policy. This does not disable a pressure budget.
An exact built-in in-memory primary is replaced with durable history while preserving its
`source_id` and storage settings. An external primary, including an in-memory subclass, stays
primary without a second durable primary. More than one load-enabled primary is rejected.
Local retention does not delete external-provider history.

History ownership is resolved per run. A service-owned conversation bypasses durable history
loading and eager compaction flushes. A configured pressure budget still checks local entity
state and can evict eligible older local transcript, but it neither compacts the remote
conversation nor clears the saved service branch ID.

#### Delivery and Maintenance

Response availability has a default `response_delivery_window_seconds=60`. Transcript deletion
does not delete the independent result or its completion outcome. Response expiry removes delivery
payloads without deleting completion or ingestion receipts. There is no bounded receipt cleanup,
so receipt growth alone can exhaust a configured budget. Runs perform expiry checks, but idle
entities need application-owned maintenance to invoke `expire_responses()`. Polling reads do not
persist cleanup. `reset` clears local transcript and session state while preserving completion
receipts and live results. External-primary reset is rejected without a provider-owned clear
operation.

### Retention Metrics

The `agent_framework.durabletask` meter emits `durable.retention.*` metrics for evaluations,
resolved budgets, before/after JSON size, staged message and entry removals, reclaimed bytes,
capacity failures, write attempts and observed run operations. Labels are bounded categories,
not agent, session, correlation or message identifiers, content or exception text.

Deletion measurements describe staged local state with `commit_status="not_attempted"`.
Write and operation observations distinguish `outcome="returned"` from `"failed"`. After a
`set_state` attempt, `commit_status="unknown"` applies even when the call returns. Neither
status confirms a durable commit, and staged deletions may later roll back locally.

Instrumentation uses only the OpenTelemetry API. The package does not configure an SDK, metric
reader or exporter. Applications own that setup, and telemetry failures do not replace the
operation's result or error.

### Basic Usage Example

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIChatCompletionClient
from agent_framework_durabletask import DurableAIAgentWorker
from durabletask.worker import TaskHubGrpcWorker

# Create the worker
worker = TaskHubGrpcWorker(host_address="localhost:4001")

# Only use isolated_v2 for a new isolated hub with upgraded clients.
# Do not point this host at an existing hub and replay historical runs after updating.
agent_worker = DurableAIAgentWorker(worker, deployment_mode="isolated_v2")

chat_client = OpenAIChatCompletionClient()
my_agent = Agent(client=chat_client, name="assistant")
agent_worker.add_agent(my_agent)
```

For more details, review the standalone [Durable Task samples](https://github.com/microsoft/agent-framework-durable-extension/tree/main/python/samples) and the full [Agent Framework Python documentation](https://github.com/microsoft/agent-framework/tree/main/python).
