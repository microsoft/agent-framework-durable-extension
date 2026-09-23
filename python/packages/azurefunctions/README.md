# Get Started with Microsoft Agent Framework Durable Functions

[![PyPI](https://img.shields.io/pypi/v/agent-framework-azurefunctions)](https://pypi.org/project/agent-framework-azurefunctions/)

Please install this package via pip:

```bash
pip install agent-framework-azurefunctions --pre
```

## Durable Agent Extension

The durable agent extension lets you host Microsoft Agent Framework agents on Azure Durable Functions so they can persist state, replay conversation history, and recover from failures automatically.

### Current Runtime Contract On This Unreleased Stack

This README describes the current unreleased schema-v2 runtime, not a released compatibility
promise.

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
proof of isolation and it cannot detect peer workers. A new isolated hub with matching upgraded
workers and clients is the recommended way to keep old workers and histories off this deployment.

**Start fresh workflow instances after this update, including upgrades from earlier v2 builds.**
Workflow protocol `2` is unchanged, but start admission is stricter and checkpointed HITL admission
and mixed parent/child scheduling change the replay action graph, including generated child IDs.
Existing v2 in-flight instances and recorded histories are not supported by this runtime and may
fail. The version marker checks start-envelope admission, not feature or replay compatibility.
Older v2 envelopes can still pass that check. There is no replay migration or compatibility fallback.
If old runs must finish, keep them on their original workers and hub, including published histories
with concatenated child IDs. Do not resume them here.

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
`source.data.migration` field is rejected, never overwritten.
Any `source.data.pythonIngestion` field is rejected before parsing, including null, empty,
foreign and recognized profiles, so legacy metadata cannot become active v2 bookkeeping.

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

Generated root starts take public application JSON, not internal checkpoint data. The workflow
client and generated HTTP routes remove reserved child markers. Application input is sanitized
before typed reconstruction.
Generated Functions workflow starts are read as plain JSON without the SDK's custom-object decoder
before the provenance check. Generated Functions parents also receive child results as plain JSON,
without SDK custom-object construction or global decoder changes. Native orchestration input
decoding is unchanged.
At generated workflow entries, internal `__subworkflow_input__` and `__subworkflow_address__`
markers require an actual SDK-reported parent and a child address consistent with both SDK parent
and current instance IDs. Missing or mismatched provenance is rejected before checkpoint decoding.
A native application parent may still call a generated workflow with ordinary application JSON
inside `wrap_workflow_input(...)`. Without internal child markers, that payload remains application
input, even though the SDK reports a parent.

SDK parent metadata authenticates only the immediate parent/child relationship, not the full
ancestry or claimed root's authority. It does not protect against a malicious application parent
supplying an otherwise consistent envelope. Trusted worker and application deployments remain
required. The internal checkpoint codec still uses pickle and is not safe for arbitrary untrusted
input.

Generated child-ID scheme `1` always produces a physical instance ID of exactly 74 ASCII characters,
`dafxsw_v1_` followed by the full 64-character SHA-256 hex digest. The digest covers a domain separator
and length-framed UTF-8 values for the parent's actual instance ID, the exact executor ID and the
dispatch ordinal in decimal. Dispatch and provenance helpers use this same derivation at every hop.
The hash checks address consistency, not authentication.
Bounded physical IDs do not imply unlimited nesting, as logical paths and payloads still grow with
depth and their limits still apply.

Original executor IDs and workflow names, `~`-qualified HITL paths and the root notification address
remain unchanged. Clients follow actual child IDs in the parent's `subworkflows` status map at each
hop. They must not parse physical child IDs or reconstruct addresses from their names.

The Functions generic route validator retains its existing 100-character limit and Unicode-aware
rules, not an ASCII-only rule, because its provider is unknown. For a known standalone
`DurableTaskSchedulerClient`, an explicit root ID must be nonblank, contain 1-100 printable ASCII
characters and not start with `@`. It is validated, never rewritten or hashed. Generic `TaskHubGrpcClient`
behavior is preserved because its backend is unknown. Application-owned native orchestrator calls
are unchanged.

There is no automatic transcript recovery for a rejected service conversation ID. A specifically
rejected parent response can receive up to three additional identical-request retries for a
visibility delay, but only before any output, tool execution or session advance. The saved parent
ID is not cleared.

The optional migration helper budget `max_state_bytes` is a neutral admission limit, not a
pressure policy. If the fully staged destination exceeds that bound, migration fails with
`StateCapacityError`. It does not prune transcript, receipts, session state, or unknown JSON to
fit. Runtime transcript retention is configured separately below.

The shared Durable Task package now requires `durabletask>=1.7.1,<2` for parent-instance metadata
in standalone hosting. Functions uses its own SDK's parent metadata. The existing lock already
selects Durable Task `1.7.2`, so this raises the supported minimum without changing the locked SDK
version. The Core requirement remains `agent-framework-core>=1.13.0,<2`. The Durable Task dependency
directly requires `pydantic>=2.11,<3` for structured response handling.

### Retention and State Budgets

`AgentFunctionApp` defaults to `retention="keep_all"` and `max_state_bytes=None`, preserving
physical transcript messages with no local pressure cap. Backend limits still apply.
`retention="follow_compaction"` opts into eager removal of eligible compaction exclusions.
Independently, a positive integer `max_state_bytes` enables oldest-group pressure eviction,
including with `keep_all`. `None` disables pressure eviction only. Booleans, zero and negative
budgets are rejected. Default watermarks are `high_watermark=0.85` and `low_watermark=0.70`, with
`0 < low_watermark < high_watermark <= 1` and finite values required.

The budget measures whole-entity Python JSON with ASCII escaping, not just transcript text.
Transport framing is excluded, so it is not a backend acceptance guarantee.
Functions rejects `max_state_bytes="backend_limit"` because it cannot infer the backend limit,
even when using DTS. The 1 MiB convenience belongs only to a standalone
`DurableAIAgentWorker` wrapping `DurableTaskSchedulerWorker`.

`add_agent()` accepts per-agent retention, budget, watermark and delivery-window overrides.
`configure_workflow()` applies overrides to that workflow's agent entities and nested workflows.
The app also accepts `workflow_retention`, `workflow_max_state_bytes`,
`workflow_high_watermark`, `workflow_low_watermark` and
`workflow_response_delivery_window_seconds` defaults. A workflow budget is per agent entity,
not an aggregate workflow cap. For budgets, omission or `INHERIT` from
`agent_framework_durabletask` inherits the enclosing default, while explicit `None` disables
pressure eviction. For the other overrides, `None` inherits. Shared registrations require matching
settings.

Explicit `DurableHistoryProvider(prune_excluded=False)` overrides inherited eager pruning without
disabling pressure eviction. Explicit `True` also wins. External primary history providers remain
primary, and replacement of a built-in in-memory primary preserves its `source_id` and storage
settings. Local retention does not delete external history. Service-owned runs skip durable
history loading and eager flushes, but a configured budget still checks local state and may evict
eligible older local transcript. It does not compact remote history or clear a saved service ID.

Pressure eviction preserves terminal results, completion and ingestion receipts, session/control
state, opaque unknown entries and retained entry metadata. System messages and the latest exchange
are protected, and atomic groups are not split. Eager pruning also protects pending tool calls
and groups containing included messages. The low-watermark target may rise to the protected floor
only while remaining below the high watermark. Otherwise, or when no safe target is reachable,
`StateCapacityError` rejects the pressure plan. The enclosing run restores local staged/cache state,
not external side effects. A failed host write leaves backend acknowledgement unknown and requires
an authoritative reload on the next operation.

Response availability defaults to `response_delivery_window_seconds=60`, independently of
transcript retention. Expiry removes delivery payloads, not completion outcomes or ingestion
receipts. There is no bounded receipt cleanup, so receipts alone can exhaust the budget. Idle
entities need application-owned `expire_responses()` maintenance. Polling does not persist cleanup.
`reset` preserves completion receipts and live results while clearing local transcript and session
state. External-primary reset is rejected without a provider-owned clear operation.

See the [shared retention contract](../durabletask/README.md#retention-and-state-budgets) for details.

### Retention Metrics

The shared `agent_framework.durabletask` meter emits `durable.retention.*` metrics for local
retention, capacity failures and host write/operation outcomes. Deletion counts and reclaimed bytes
describe staged state with `commit_status="not_attempted"`, not durable deletion. Write and
operation `outcome` is `"returned"` or `"failed"`. A `set_state` attempt leaves
`commit_status="unknown"` even on return. No metric confirms a durable commit.

Only the OpenTelemetry API is used. Applications configure their own SDK, metric reader and
exporter. Labels use bounded categories without identifiers, content or exception text.
See the [shared metric semantics](../durabletask/README.md#retention-metrics).

### Workflow HITL and Mixed Parent/Child Execution

The Functions host uses the same response activity and scheduler as the standalone Durable Task
host. Non-agent `request_info` replies are reconstructed and validated against the request's
recorded type in that activity. Its result checkpoints `accepted` or `invalidreply`, so orchestration
replay consumes the outcome instead of rerunning reply validators. Rejected replies stay pending for
a correction. Handler and output-serialization failures remain activity failures. Activity retries
or redelivery can still rerun application code.

Response-type descriptors retain declared types and generic arguments for supported `list`, `dict`,
`tuple`, `set`, union and `Any` annotations. Coercion and type checks follow the installed Core
version, so nested model/dataclass and tuple/set reconstruction from JSON is Core-dependent.
Custom types must already be loaded under their recorded module and qualified name. This does not
make every Python annotation a supported reply type.

The respond endpoint preserves early delivery for known fixed request IDs, even before their waits
are published. A successful HTTP response acknowledges event delivery, not type validation or
handler success. Nested replies still require an active child path recorded by the parent. Pending
event waits survive other replies and mixed waves, including valid replies already buffered for them.

Parent and child HITL requests can remain active together. Ready parent handlers and downstream
work proceed without waiting for paused children. A ready child's downstream work can answer an
earlier paused child. Within a ready wave, results route in dispatch order and preserve each
result's message order. Local tasks read the dispatched state snapshot, and their reported updates
and deletes merge in dispatch order before downstream work or reply handlers run. Across waves,
recorded readiness defines order, not original child invocation order. This preserves durable
snapshots, not Core's in-process visibility of other executors' uncommitted writes.

### JSON runtime boundary

`AgentFunctionApp` decodes state and operation inputs as plain JSON for generated agent entities.
Framework agent results, generated workflow starts, child results and HITL event values also
use scoped JSON decoding. Native co-hosted calls retain their SDK behavior. Manually wrapping
entity factories does not install the generated-entity boundary. The Durable Task dependency
requires `durabletask>=1.7.1,<2`. The Functions SDK
floor remains `azure-functions-durable>=1.3.1,<2`.
See [Python durable JSON boundaries](../../../docs/features/python-durable-json-boundaries.md).

### Basic Usage Example

See the durable functions integration sample in the repository to learn how to:

```python
from agent_framework_azurefunctions import AgentFunctionApp

_app = AgentFunctionApp(deployment_mode="isolated_v2")
```

- Register agents with `AgentFunctionApp`
- Post messages using the generated `/api/agents/{agent_name}/run` endpoint

For more details, review the Python [README](https://github.com/microsoft/agent-framework/tree/main/python/README.md) and the samples directory.
