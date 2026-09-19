# Get Started with Microsoft Agent Framework Durable Task

[![PyPI](https://img.shields.io/pypi/v/agent-framework-durabletask)](https://pypi.org/project/agent-framework-durabletask/)

Please install this package via pip:

```bash
pip install agent-framework-durabletask --pre
```

## Durable Task Integration

The durable task integration lets you host Microsoft Agent Framework agents using the [Durable Task](https://github.com/microsoft/durabletask-python) framework so they can persist state, replay conversation history, and recover from failures automatically.

### Current Runtime Contract On This Unreleased Stack

This README describes the current `impl/python-evidence-migration` worktree. It documents
unreleased behavior and should not be read as a released compatibility promise.

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

Retention defaults currently preserve every physical message. Response delivery availability is
time-bounded, with a default expiry window of `60` seconds, but receipt records are retained and
never deleted by response expiry alone. `reset` clears local transcript and session state while
preserving completion receipts and live results. External-primary reset requires a provider-owned
clear operation and is rejected rather than silently clearing only local state.

The optional migration helper budget `max_state_bytes` is a neutral admission limit, not a
pressure policy. If the fully staged destination exceeds that bound, migration fails with
`StateCapacityError`. It does not prune transcript, receipts, session state, or unknown JSON to
fit. There is no documented runtime pressure policy yet.

There are no retention controls yet for automatic transcript pruning or receipt cleanup. The
application remains responsible for its own maintenance policy. Per-agent and per-workflow
delivery windows are configurable through the host registration APIs.

The Core requirement remains `agent-framework-core>=1.13.0,<2`. This package directly requires
`pydantic>=2.11,<3` for structured response handling.

The history bridge requires an exact `2.0.0` snapshot. Even `get_messages()` may repair missing
or duplicate internal IDs, so it is not a read-only inspection path. Mutation-capable hooks reject
legacy and unsupported versions before changing stored history or working state. Use
`read_agent_state()` for legacy inspection instead.

External primary providers, service-owned history and store-only audit sinks keep separate roles.
Preparation permits only one `DurableHistoryProvider`, including an injected or replaced primary.
Different source IDs do not create separate durable transcripts. Multiple durable adapters are
rejected before model execution, even when loading or all store flags are disabled, because loading
and flushing can still update shared history. Use an ordinary store-only `HistoryProvider` with a
distinct source ID for an independent audit store.
On a service-owned turn, the inactive external primary's custom hooks are also suppressed because
they may load or persist history directly. Ownership-independent work belongs in a separate context
provider or store-only sink. Client-owned turns retain the original primary's hooks and resources.
Compaction aimed at an inactive external history source does not run its stored-history after
hook. Its before hook still operates on unrelated current context, as Core specifies.
Ordinary input and response IDs are preserved. Newly generated compaction summary occurrences get
unique IDs when a strategy reuses a candidate ID, so both summary revisions and their links survive.
With no primary, history is injected before a matching before-compaction provider. Core runs before
hooks forward and after hooks in reverse, so that provider's after hook sees the previously stored
history. After-only compaction retains the existing append-then-compact order. Per-service-call
history uses Core's middleware cadence and does not run compaction hooks per model call.
Injection honors a single configured compaction history source. Conflicting sources require an
explicit primary instead of silently selecting a source that cannot serve all configured hooks.
All explicit primary providers retain the user's hook order, including a built-in in-memory provider
replaced with durable history. With `[compaction, history]`, before compaction cannot see history
loaded later, while reverse after hooks append the current input and output before compaction.
With `[history, compaction]`, before compaction sees loaded history, while the ordinary per-run
after compaction runs before the current turn is appended. Choose the order explicitly for the
strategy's intended inputs. Preparation does not mutate the caller's agent or provider list.
Provider namespaces must be unique, including store-only sinks, and conflicts with an injected
history source raise an error rather than automatically reassigning a namespace.

Canonical media keeps its declared content kind rather than inferring it from the URI scheme.
Literal Core input preserves absent versus explicit-null function results and error details on
the first canonical write and later cold reads. Plain Core objects without an attached input
envelope keep canonical nullable defaults. Existing shared JSON retains its raw field presence.
New response timestamps without an offset are interpreted as UTC. Valid stored timestamps retain
their original offset and fractional precision.

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
