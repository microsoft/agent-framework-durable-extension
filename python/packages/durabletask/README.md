# Get Started with Microsoft Agent Framework Durable Task

[![PyPI](https://img.shields.io/pypi/v/agent-framework-durabletask)](https://pypi.org/project/agent-framework-durabletask/)

Please install this package via pip:

```bash
pip install agent-framework-durabletask --pre
```

## Durable Task Integration

The durable task integration lets you host Microsoft Agent Framework agents using the [Durable Task](https://github.com/microsoft/durabletask-python) framework so they can persist state, replay conversation history, and recover from failures automatically.

<a id="current-runtime-contract-on-this-branch"></a>

### Current Runtime Contract On This Unreleased Stack

This README describes the current unreleased schema-v2 runtime, not a released compatibility
promise. It is the common runtime contract for both Python hosts. See the
[Azure Functions guide](../azurefunctions/README.md) for host-specific routes and settings.

The public mutable state model is now canonical `DurableAgentState` schema `2.0.0`.
`read_agent_state(raw)` remains explicitly backward compatible with legacy `1.x` payloads.
It accepts a dictionary or JSON string and returns:

- `LegacyDurableAgentState` for exact `1.0.0`, `1.1.0`, and `1.2.0` inputs
- `SharedAgentStateReader` for exact `2.0.0` inputs

Other schema versions are rejected. Within an accepted `2.0.0` snapshot, unknown fields and
unknown protocol content remain preserved in detached raw JSON until a caller explicitly asks for
a narrower projection.

Named shared counters are bounded to signed Int64. Usage token counts and ingested
positions accept `0..9223372036854775807`. A present `evictedMessageCount` accepts
`1..9223372036854775807`. Readers and writers reject invalid counts, and eviction
increments reject overflow before changing state. These bounds also apply when
loading legacy snapshots. Legacy nullable usage fields remain readable, and nullable
Core usage counts retain their existing metadata representation. Other invalid known
counts cannot be moved into metadata to bypass validation.

Integral JSON numbers such as `1.0` remain accepted. Exact integer tokens are not
rounded through floating point. These bounds do not restrict opaque user payloads,
provider metadata, session data, or unknown fields.

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

#### Agent compatibility prototype

This worktree contains a **Python compatibility prototype, not a release-ready upgrade contract**.
Both host factories use `CompatibleAgentEntity`; direct `AgentEntity` remains the existing v2 executor.
Host construction defaults to v2 without an acknowledgement setting. The explicit
`deployment_mode="isolated_v2"` value remains accepted. Set `deployment_mode="legacy"` or
`DURABLE_AGENTS_DEPLOYMENT_MODE=legacy` to keep all writes legacy, including fresh schema-1.1 sessions.
Invalid or empty values are rejected. Resolve the policy before registering/starting the host.

```python
# Default: new sessions use v2; existing supported legacy sessions can continue.
host = DurableAIAgentWorker(worker)
# Alternatively, for an explicitly legacy-writing deployment:
# host = DurableAIAgentWorker(worker, deployment_mode="legacy")
host.add_agent(agent)
```

Both hosts support legacy state versions 1.0, 1.1 and 1.2, entity-owned transcripts,
external history providers and service-owned conversations. They restore serialized provider
state and the original logical session ID, preserve unknown session siblings, and retain an
inactive service continuation across local-history turns. Registered providers are not mutated.
If a legacy session lacks a saved ID, restoration supplies the resolved entity identity on a
detached snapshot, without changing committed continuation before a successful run.
Used sessions without authoritative migration evidence continue on the legacy execution path.
Legacy history is never pressure-pruned; an enabled byte budget rejects oversized writes instead.
New legacy completions retain their complete canonical Core delivery snapshots in the declared
`extensionData._dafx_legacy_delivery` profile, preserving structured values and response metadata
for cold duplicates. Historical transcript projections are not promoted into these snapshots.
Both legacy readers and public client polling consult the same validated delivery profile.
Functions includes the full `agentResponse`/`agent_response` fields for declared full deliveries,
while historical text-only payloads and legacy status behavior remain unchanged.
Legacy validation failures use the same sanitized diagnostic-first logging as v2, including
wrapped and grouped Pydantic errors, without logging their private input values or tracebacks.
The profile is bounded by the entity-state budget, retained with legacy history, and cleared by
legacy reset. It does not establish a complete historical completion journal; conversion evidence
must preserve any full snapshots that are known.
A nonempty delivery cache counts as used state even if retained history or session data is absent,
so it cannot bypass conversion evidence or cause a known completion to execute again.

An empty legacy profile can be converted automatically. Used profiles require a trusted
`legacy_migration_evidence` callback returning `LegacyMigrationEvidence` with the complete,
source-digest-bound journals described below, or `None` to defer conversion. The callback receives
a detached source snapshot; invalid evidence is an error, not permission to silently fall back.
Valid delivery-only evidence can authorize legacy context delivery without forcing conversion
when completion evidence is missing.
Conversion is staged at the same entity address and shares the next v2 turn's state setter.
The active, immutable `data.migration` binding records `mode="in_place"`, the physical
`sourceEntitySessionId`/`destinationSessionId`, original logical `sourceSessionId`, source schema,
source digest and request digest. Cold v2 operations validate this binding and restore the original
provider identity. This is **not** evidence of fleet isolation or fencing.
The source is preserved when staging or a pre-write failure occurs; uncertain acknowledgements
are reconciled from the subsequently read state.

Legacy mode can return committed v2 duplicates but cannot run new v2 turns, reset v2 state,
expire v2 results or perform migration. It never downgrades receipts. Legacy cleanup is a no-op,
and legacy reset retains its schema and unrelated metadata.
Legacy reset rejects an external primary history provider, matching v2. The runtime provides no
generic provider-owned clear operation, so it must not report a local-only reset as complete.
The committed legacy owner is checked before continuation or conversion, so completion evidence
never authorizes switching providers. Disabling tool calls uses the same invocation-local policy
in both paths, removing configured/request tools without mutating registered agents or clients.

Legacy workflow context uses explicit delivery occurrences and content fingerprints, not public
message IDs or cursor maxima as inferred provenance. The declared `_dafx_legacy_ingestion`
extension records bridge-era acceptances; `_dafx_legacy_owner` binds the selected provider.
A present ingestion field must contain a declared object profile; explicit null is not absence.
The ingestion profile's v2 form stores historical public lookups in `publicMessages`, separate
from accepted occurrences in `messages`. Its v1 form remains readable without activating unknown
siblings. Public lookups are consulted only when explicit occurrence IDs are absent.
Promotion to the v2 ingestion profile rejects an existing opaque v1 `publicMessages` sibling
before execution rather than overwriting it.
Existing nonempty scalar cursors require trusted delivery evidence before accepting new context
when no bridge ledger exists. Failed runs do not advance that ledger. Conversion merges its
acceptances into v2. Remote provider/model/tool effects remain at-least-once: a local state rollback
cannot undo a remote side effect.
New context receipts follow post-middleware acceptance by the primary history owner, or a completed
observed service request. Filtering an input does not consume its delivery occurrence, even when
input transcript storage is disabled. Replay is recorded separately from successful primary history
writes, preserving edited content, context selection, storage flags and tool-call ordering. Receipt
fingerprints still describe the original delivered input, not the edited replay representation.
External save payloads and response classification are captured at the completed save boundary.
Built-in memory bodies remain live until session persistence, while their classification stays fixed.
Disabling output history does not remove the full duplicate-response delivery cache, and storage
flags do not retroactively erase historical bodies. Observation uses the same history/service hooks
as v2 without binding a v2 writer to legacy state. Opaque custom hooks and unobserved service requests
do not establish acceptance or provider writes by inference.
Only memory providers substituted for `DurableHistoryProvider` are transient. Caller-configured
store-only `InMemoryHistoryProvider` audit state retains its prior and newly stored messages across
cold continuation.
Failed context deliveries also remove their staged request from replayable local history while
retaining the terminal error, so a cold redelivery does not include the uncommitted input twice.
Fingerprints use the original input identity, including message extras, rather than a transcript
projection that may omit them.
Validated delivery-only evidence is carried into legacy fallback even without scalar cursors or
when bridge receipts already exist. Its public lookups never consume an explicit delivery occurrence.
Successful ordinary turns retain supplied delivery evidence even when `context_messages` is omitted.
History substitution preserves configured `after_run_once_per_turn` metadata and Core's iteration cadence.
Session-capable custom agents without a context pipeline receive prior history directly rather than
relying on a nonexistent provider to load it.
Entity-owned legacy after-run compaction reconciles its final buffer before transient memory is
stripped. Original request/response envelopes and bodies remain available, including excluded
messages under `keep_all`. The declared `extensionData._dafx_legacy_history` profile records
message-slot replay order and generated summary/revision slots without changing public message IDs
or creating input receipts. Anonymous and duplicate public IDs use distinct transient reconciliation
identities. Unknown profile siblings and untouched historical JSON are retained.
Late buffer insertions receive identities at final reconciliation. In-place summary revisions retain
distinct archived source identities and backlinks, including sources from the current turn's writes.
Cold legacy turns honor that order even without a compaction provider. Reset clears only the
profile's owned ordering fields. Migration materializes the order as canonical v2 history after
validating the original source and journals; generated summaries never become delivered-input
evidence. Older binaries do not gain this new profile's semantics merely by reading legacy JSON.
Automatic in-place conversion seeds active occurrence receipts only from the bridge ledger.
Historical delivery journals validate accepted public lookups and cursor consistency; their
public message IDs do not establish transport occurrence IDs. Exact public journal receipts are
preserved separately in `extensionData._dafx_legacy_public_ingestion` and consulted only when a
request falls back to public IDs instead of supplying explicit occurrence IDs. Neither public
transcript IDs nor public journal lookup IDs become occurrence receipts. Exact bridge fingerprints
never expand into acceptance of every future revision. This does not change the manual transfer helper.

Workflow compatibility is pinned to the published unversioned engine from
`v1.16.0-preview.260922.1`, alongside this branch's current v2 engine. Both hosts dispatch
recorded unversioned starts and activities through that predecessor, retaining its action ordering,
HITL behavior and concatenated child IDs. Default client/HTTP starts use v2; legacy mode emits the
old raw wire shape. Legacy child routing reads each child's recorded public ordinal instead of
renumbering the remaining child list.
The predecessor retains its checkpoint codec: later v2 response-envelope keys remain literal
legacy application data, including in recorded activity results and client/HTTP output decoding.

An optional trusted `workflow_execution_resolver(WorkflowStart)` selects `"legacy"` or `"v2"`
using the backend instance ID, immediate parent ID and detached recorded input. Use it when
historical application data collides with the reserved v2 envelope shape. Configure the matching
resolver on `DurableWorkflowClient` when reading old results/events too; `AgentFunctionApp`
uses its resolver for both execution and result reads. This also covers already-completed legacy
records that cannot acquire a new protocol marker. Parent identity can be unavailable in read
metadata, in which case the resolver receives `None`. Invalid explicit v2
envelopes fail rather than silently falling back; internal child payloads require SDK parent
provenance. **Earlier unreleased v2 action graphs are not covered merely because their marker is
also `2`.** Start fresh runs or retain the matching old workers/hub for those histories.

Prevent incompatible workers from consuming the hub before enabling v2 writes. The prototype
does not detect peers, fence old consumers, certify real-backend atomicity, or reconstruct missing
historical outcomes. The local replay fixtures exercise both SDKs, not arbitrary captured
production histories.

From the repository root, run the focused executable examples:

```powershell
uv run --frozen --no-sync --project .\python python -m pytest -c .\python\pyproject.toml .\python\packages\durabletask\tests\test_agent_compatibility_prototype.py .\python\packages\durabletask\tests\test_complete_compatibility.py -o addopts=-ra -q
```

#### Migration

In addition to automatic in-place conversion above, the runtime retains a privileged manual
transfer operation. Manual `migrate` is a backend entity operation only, with no generated HTTP
or MCP endpoint. The helper stages a
detached result with no model, tool, provider, storage, or transcript I/O. The operator must
quiesce the legacy owner, fence it, authorize ownership transfer, and commit the staged result
exactly once into a separate empty isolated v2 destination. The migration evidence cannot prove
that authority or completeness on its own.

The request contract is explicit and complete. `migrate(request)` requires `source`,
`sourceDigest`, `sourceSessionId`, `destinationSessionId`, `migrationId`, and
`ownershipTransferId`. It optionally accepts `deliveryEvidence`, `completionEvidence`, and
`requireKnownOutcomes`. The destination must match the current entity, must be separately
addressed, and must still be empty. This manual operation is never an in-place rewrite or replay into a
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

The optional migration helper budget `max_state_bytes` is a neutral admission limit, not a
pressure policy. If the fully staged destination exceeds that bound, migration fails with
`StateCapacityError`. It does not prune transcript, receipts, session state, or unknown JSON to
fit. Runtime transcript retention is configured separately below.

#### Session Restoration

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

#### Workflow Starts and Child Identity

Workflow start input wrapping is not universal. `DurableWorkflowClient` and generated HTTP
start routes wrap new inputs for host-generated workflows. Application-owned native orchestrators
retain their original input contracts. Do not wrap every orchestration payload in this envelope.

Generated root starts take public application JSON, not internal checkpoint data. The workflow
client and generated HTTP routes remove reserved child markers. Application input is sanitized
before typed reconstruction.
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

For a known standalone `DurableTaskSchedulerClient`, an explicit root ID must be nonblank, contain
1-100 printable ASCII characters and not start with `@`. It is validated, never rewritten or hashed. Generic
`TaskHubGrpcClient` behavior is preserved because its backend is unknown. The Functions generic route
validator retains its existing 100-character limit and Unicode-aware rules, not an ASCII-only rule,
because its provider is unknown. Application-owned native orchestrator calls are unchanged.

#### Invocation and State Boundaries

There is no automatic transcript recovery for a rejected service conversation ID. A specifically
rejected parent response can receive up to three additional identical-request retries for a
visibility delay, but only before any output, tool execution or session advance. The saved parent
ID is not cleared.

Streaming fallback requires an immediate capability refusal, not awaited setup, iteration or finalization errors.
Async streaming remains supported. Activity state deltas compare a detached receiving-worker encoding, not producer
pickle bytes. Explicit writes retain their intent, and in-place JSON changes remain type-sensitive.

Requires Python 3.10+, `durabletask>=1.7.1,<2` for scoped JSON decoding and SDK parent metadata,
`agent-framework-core>=1.19.0,<2` and `pydantic>=2.11,<3` for structured response handling.

#### History Provider Integration

Ownership is resolved per run from its `store` option, then the agent's `default_options`, then
the client's default. On client-owned runs, the active primary's load and storage flags select
history and appends. `skip_excluded=True` omits compaction exclusions from later model context.

Append staging is atomic in memory, undoing lazy internal-ID repairs and restoring the transcript,
working buffers, position indexes and append ordinal if staging fails. Earlier successful saves
and flushes, including the preliminary flush in `save_messages()`, remain intact. This does not
commit to the backend or roll back external provider writes.

Requests and responses are checked as stored transcript JSON before publication. Failed-run
finalization retains pending tool results if filtering or staging fails, allowing a retry.

Core sessions are loaded and staged through a private entity-backed `SessionStore` adapter.
It uses the existing entity session field, not a separate backend or commit. The agent still
creates its working session, and only the durable history adapter's temporary message/index
buffers are omitted from its snapshot. Snapshot copies isolate saved state from failing custom
decoders and retained working-session references. External history providers keep their own transcripts
and provider state. This does not make external writes transactional or migrate history between
providers. The adapter relies on Core's experimental store contract and is not a public storage
configuration API.

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
A single load-disabled durable adapter may audit an external primary into the canonical transcript.
Its stored messages and public IDs do not acknowledge input delivery. Only the active primary's
acceptance evidence contributes ingestion receipts, so an audit cannot suppress a later retry.
On a service-owned turn, the inactive external primary's custom hooks are also suppressed because
they may load or persist history directly. Ownership-independent work belongs in a separate context
provider or store-only sink. Client-owned turns retain the original primary's hooks and resources.
External-primary observation uses its current storage flags without rebinding custom hooks.
Compaction aimed at an inactive external history source does not run its stored-history after
hook. Its before hook still operates on unrelated current context, as Core specifies.
Ordinary input and response IDs are preserved. Newly generated compaction summary occurrences get
unique IDs when a strategy reuses a candidate ID, so both summary revisions and their links survive.
Reconciliation distinguishes loaded occurrence IDs from unallocated summary IDs and compares
summary revisions with JSON-exact payloads.
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

### Retention and State Budgets

The retention default is `retention="keep_all"`. When wrapping `DurableTaskSchedulerWorker`,
an omitted `max_state_bytes` now enables its known 1 MiB pressure budget. Old eligible transcript
groups are removed only under pressure, not merely because model compaction excluded them.
Pass `max_state_bytes=None` to preserve the previous disabled-budget behavior. Generic workers
and Azure Functions remain disabled by default and require an explicit budget.

This default change can delete stored history in existing standalone DTS applications that omit
the budget. Eager pruning remains opt-in and independent of pressure eviction.

- `retention="follow_compaction"` physically removes eligible compaction exclusions when durable
  history is flushed, even without a pressure budget. It does not configure a compaction strategy.
- A positive integer `max_state_bytes` enables pressure eviction, including with `keep_all`.
  At `high_watermark=0.85`, the runtime plans removal of oldest eligible atomic message groups
  toward `low_watermark=0.70`, or the protected floor if it is higher but still below the high
  watermark. Watermarks must be finite and satisfy `0 < low_watermark < high_watermark <= 1`.
- Explicit `max_state_bytes=None` disables pressure eviction, not eager pruning. Booleans, zero and
  negative budgets are rejected.

The estimate covers the whole entity using Python's default JSON serialization with ASCII
escaping, including non-text content, delivery records, session state, metadata and truncation
records. It excludes transport framing and is not a backend acceptance guarantee.
`max_state_bytes="backend_limit"` is a 1 MiB (`1_048_576` bytes) convenience only when the wrapped
worker is a `DurableTaskSchedulerWorker`. A generic `TaskHubGrpcWorker` cannot resolve it.
`AgentFunctionApp` rejects it, even when Functions uses DTS. Use an explicit positive budget or
`None` there.

The local emulator tests compare actual persisted JSON bytes with the estimate, exercise
near-limit default/opt-out behavior, and verify incoming entity-operation payload rejection.
The tested emulator did not reject oversized entity state with budgeting disabled. These tests
do not establish Azure's hard state-rejection boundary, and a state budget cannot prevent an
oversized input or output from exceeding its separate transport limit.

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

`DurableAIAgentWorker` sets host defaults. At construction, omission or `INHERIT` uses the known
DTS budget when available, otherwise no budget. Explicit `None` always disables pressure eviction.
`add_agent()` and `configure_workflow()` accept
`retention`, `max_state_bytes`, both watermarks and `response_delivery_window_seconds` overrides.
Workflow settings apply to its agent entities and nested workflows, not one aggregate workflow
budget. For budget overrides, omission or `INHERIT` from `agent_framework_durabletask` uses the
host default, while explicit `None` disables pressure eviction. For the other overrides, `None`
inherits. Shared workflow registrations must have matching settings.

`AgentRegistrationSettings` retains its positional delivery-window and callback parameters.
Its added retention and budget fields are keyword-only and default to non-evicting behavior.

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

Canonical `terminalResults` hold immutable original response envelopes by correlation ID,
separately from transcript history. Matching `completionReceipts` retain `correlationId`, known
`outcome` (`succeeded` or `failed`) and `completedAt`. Results include the full `response`, including
messages, metadata and an optional explicit `value`, even null or falsey. Failures require `error.code`
and `error.message`, while successes forbid `error`. `resultState="available"` requires a matching result.
`resultState="unavailable"` forbids a result and requires `resultUnavailableAt`. Completion facts
and any `resultExpiresAt` agree across the pair. See the
[shared wire invariants](../../../schemas/README.md#proposed-wire-concepts-and-semantic-invariants).

Staging rejects unsupported live response fields before Core serialization, without invoking their
conversion hooks. Supported lazy values keep their existing policy. Duplicate completions remain
no-ops without inspecting the replacement producer.

Response availability has a default `response_delivery_window_seconds=60`. Transcript deletion
does not delete the independent result or its completion outcome. Response expiry removes delivery
payloads without deleting completion or ingestion receipts. There is no bounded receipt cleanup,
so receipt growth alone can exhaust a configured budget. Runs perform expiry checks, but idle
entities need application-owned maintenance to invoke `expire_responses()`. Polling reads do not
persist cleanup. `reset` clears local transcript and session state while preserving completion
receipts and live results. External-primary reset is rejected without a provider-owned clear
operation.

At or after a delivery deadline, lookup reports completion with its retained outcome, even before
physical cleanup. It never returns the expired payload, reports pending work, reruns the request or
reconstructs an original from compacted or external history. Shared v2 has no `unknown` outcome.

### Retention Metrics

The `agent_framework.durabletask` meter emits `durable.retention.*` metrics for evaluations,
resolved budgets, before/after JSON size, staged message and entry removals, reclaimed bytes,
capacity failures, write attempts and observed run operations. Labels are bounded categories,
not agent, session, correlation or message identifiers, content or exception text.

Deletion measurements describe staged local state with `commit_status="not_attempted"`.
Write observations distinguish `stage="serialization"` from `stage="set_state"`, each with
`outcome="returned"` or `"failed"`. Successful serialization and transition validation record
`"returned"` with `commit_status="not_attempted"` before the host call. A serialization failure
records only that stage. After a `set_state` attempt, write and operation observations use
`commit_status="unknown"` even when the call returns. Neither status confirms a durable commit,
and staged deletions may later roll back locally.

Instrumentation uses only the OpenTelemetry API. The package does not configure an SDK, metric
reader or exporter. Applications own that setup, and telemetry failures do not replace the
operation's result or error.

### Workflow HITL and Mixed Parent/Child Execution

Non-agent `request_info` replies are reconstructed and validated inside the registered response
activity against the request's recorded type. The activity result checkpoints `accepted` or
`invalidreply`, so orchestration replay consumes that outcome instead of rerunning reply validators.
Rejected replies leave the request pending for a correction. Handler and output-serialization
failures remain activity failures, not invalid replies. Activity retries or redelivery can still
rerun application code.

Response-type descriptors retain declared types and generic arguments for supported `list`, `dict`,
`tuple`, `set`, union and `Any` annotations. Coercion and type checks follow the installed Core
version, so nested model/dataclass and tuple/set reconstruction from JSON is Core-dependent.
Custom types must already be loaded under their recorded module and qualified name. This does not
make every Python annotation a supported reply type.
Tuple descriptors preserve the declared argument shape, including Python 3.10's explicit empty-tuple
argument. They do not assume all tuple aliases admit the same JSON or native tuple values.

Valid replies for known fixed request IDs can arrive before their waits are published and remain
buffered by the service. Sending a reply acknowledges event delivery, not type validation or handler
success. Nested replies still require an active child path recorded by the parent. Pending event
waits survive other replies and mixed waves, including valid replies already buffered for them.

Parent and child HITL requests can remain active together. Ready parent handlers and downstream
work proceed without waiting for paused children. A ready child's downstream work can answer an
earlier paused child. Within a ready wave, results route in dispatch order and preserve each
result's message order. Local tasks read the dispatched state snapshot, and their reported updates
and deletes merge in dispatch order before downstream work or reply handlers run. Across waves,
recorded readiness defines order, not original child invocation order. This preserves durable
snapshots, not Core's in-process visibility of other executors' uncommitted writes.

### JSON runtime boundary

The current runtime writes canonical `2.0.0` state and supports the explicit migration above.
Construct `DurableAIAgentWorker` before starting the SDK worker. Generated entity state and
`run`/`migrate` inputs, blocking framework agent results, generated workflow starts, child results
and external-event values use plain JSON rather than SDK custom-object reconstruction.
Native co-hosted work keeps its original converter behavior. The
[integrated JSON boundary guide](../../../docs/features/python-durable-json-boundaries.md)
details both hosts, custom-converter constraints and separate checkpoint trust requirements.

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
