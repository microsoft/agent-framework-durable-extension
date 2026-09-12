# Microsoft.Agents.AI.DurableTask

The Microsoft Agent Framework provides a programming model for building agents and agent workflows in .NET. This package, the *Durable Task extension for the Agent Framework*, extends the Agent Framework programming model with the following capabilities:

- Stateful, durable execution of agents in distributed environments
- Automatic conversation history management
- Long-running agent workflows as "durable orchestrator" functions
- Tools and dashboards for managing and monitoring agents and agent workflows

These capabilities are implemented using foundational technologies from the Durable Task technology stack:

- [Durable Entities](https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-entities) for stateful, durable execution of agents
- [Durable Orchestrations](https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-orchestrations) for long-running agent workflows
- The [Durable Task Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/choose-orchestration-framework) for managing durable task execution and observability at scale

This package can be used by itself or in conjunction with the `Microsoft.Agents.AI.Hosting.AzureFunctions` package, which provides additional features via Azure Functions integration.

## Install the package

From the command-line:

```bash
dotnet add package Microsoft.Agents.AI.DurableTask
```

Or directly in your project file:

```xml
<ItemGroup>
  <PackageReference Include="Microsoft.Agents.AI.DurableTask" Version="[CURRENTVERSION]" />
</ItemGroup>
```

You can alternatively just reference the `Microsoft.Agents.AI.Hosting.AzureFunctions` package if you're hosting your agents and orchestrations in the Azure Functions .NET Isolated worker.

## Usage Examples

For a comprehensive tour of all the functionality, concepts, and APIs, check out the [.NET Durable Task samples](https://github.com/microsoft/agent-framework-durable-extension/tree/main/dotnet/samples).

## Durable completion and delivery

An invocation correlation ID is an idempotency key, not a transcript position. Do not reuse it for
different work. Legacy state resolves terminal responses and errors from the retained transcript.
Schema 2.0 resolves only the result mailbox and permanent completion receipts: pruning the transcript
does not make a completed correlation runnable again. A recorded completion with an expired or removed
payload is **completed but result unavailable**, not pending and not a new invocation.

| Recorded state | Client behavior |
| --- | --- |
| No terminal evidence | Pending; a polling handle continues waiting |
| Successful completion with a result | Return the original response and its retained metadata |
| Supported committed terminal failure | Throw `DurableAgentTerminalException` with its code and details |
| Completion without an available payload | Throw `DurableAgentResultUnavailableException`, retaining whether the completion succeeded or failed |
| Inconsistent or unsupported state | Fail closed; do not invoke the model as a recovery fallback |

These failure semantics also apply to direct orchestration calls and workflow agent executors.
A committed duplicate failure (including a legacy `errorResponse` and an empty retry) throws before
validation, agent construction, history/tool work, or migration; it cannot become downstream success.
The Durable Task SDK serializes exception type, message, and inner failures, but drops custom exception
properties. The entity therefore includes a versioned metadata snapshot in a
`DurableAgentFailureMetadataException` inner exception. `DurableAIAgent` restores the typed terminal or
unavailable exception from either `EntityOperationFailedException` or `TaskFailedException`, retaining
the SDK failure as a nested cause. Only these framework exception types select this contract;
model/user text is never used to infer outcomes. Unsupported metadata remains an SDK failure.
This is an additive failure-only transport change: `Run`/`RunAgentAsync` operation names, request and
successful-response wire formats, and entity-state schemas are unchanged. Older orchestration clients
still receive an SDK exception, not an ordinary successful response. Historical message-only failures
remain typed failures, without inventing metadata that was not recorded.

One successful outer entity operation stages the immutable result and its receipt in an independent
working copy with the existing session/continuation, ingestion, entity transcript, whole-entity TTL,
binding, and other local state. Publishing that copy participates in the Durable Task entity commit.
Appending transcript text alone is not delivery acknowledgement. External history-provider writes and
tool effects are **not** part of this entity-local transaction; tool implementations still need their
own idempotency guarantees.

Only a successfully committed outer invocation creates a new success receipt. Validation failures,
cancellation, model/provider errors, serialization/capacity failures, and failed entity commits remain
retryable; they are not converted into terminal receipts. Existing explicitly committed terminal
failure evidence can be read and migrated, but a transient exception does not establish such a contract.
Legacy conversion is evidence-only and idempotent. It never calls the model or tools and cannot
reconstruct receipts for results already evicted from legacy state.

Completion receipts last until entity deletion. `DurableAgentsOptions.ResultRetentionPeriod` is optional
and defaults to no payload expiry. Result-payload retention and whole-entity TTL are separate policies;
there is no implicit 60-second mailbox expiry. Deleting the entity also deletes its
idempotency evidence. Keep schema 2.0 writes disabled until the shared rollout gates are agreed and every
participating reader/worker is mailbox-aware or explicitly rejects the new major version.
Producer activation and receipt-deleting entity TTL are internal test gates only, disabled by default.
`HistoryRetentionMode.Auto` does not activate schema 2 production writes. Automatic transcript retention
requires a mailbox-aware state writer, but that rollout remains internal until Python, dashboards, pollers,
and every other participating reader can safely consume or reject schema 2. Auto therefore fails closed when
the internal writer gate is disabled or legacy migration is not explicitly authorized from independently
authoritative complete history.
Legacy TTL behavior is preserved, but old deadlines cannot delete schema-2 receipts without a separately agreed
deletion policy. Unknown-field
preservation by an older worker is not sufficient. See [state compatibility](State/README.md).

Under the internal mailbox-writer gate, a successful new run also removes already-expired mailbox
payloads and maintains one logical entity-local `CheckAndExpireResults` schedule for the earliest
remaining expiry. The optional version-1 runtime profile
`extensionData["Microsoft.Agents.AI.DurableTask.resultExpiry"]` stores the entity identity, UTC scheduled
deadline, and an unpredictable token. It uses the existing root extension map, not a new shared schema
field or `historyBinding`. The replacement state, profile and signal outbox participate in the same
entity operation commit.
Cleanup preserves each completion's outcome, completion/expiry timestamps and unknown receipt metadata,
marks its result unavailable, and records the first cleanup time. It does not delete receipts, invoke
the agent/factory/tools, create a session, or refresh entity TTL. The independent entity-deletion gate
is not required for payload cleanup.

Entity operations are serialized. New runs reuse a pending check that already covers the earliest
deadline; an earlier deadline replaces it once with a new token. Moving the earliest expiry later reuses
the earlier check, which will schedule one successor when consumed. Superseded physical signals may
still arrive, but only an exact entity/token/deadline match can consume the logical schedule. Stale,
duplicate, timestamp-only pre-profile, and previous-generation signals are no-ops: no state setter,
outgoing signal, model invocation or TTL update. A signal against a deleted entity does not recreate it.
A matching cleanup deep-clones and validates authoritative current state and uses the host's
`TimeProvider`, not the signal, to decide expiry. Consuming it atomically clears or rotates the token.
Early matching checks schedule at most one successor, at least one minute after both the current clock
and the prior scheduled check, avoiding repeated scheduling at the same timestamp when the worker
clock moves backward. Duplicates of that early check cannot advance the chain again. This scheduling floor
is **not** a default retention period. Physical cleanup can lag logical expiry by that floor and scheduler
delivery/clock skew; polling reports unavailability at the recorded expiry without mutating state.
Repeated successful cleanup preserves the original unavailable timestamp.

Serialization, scheduling, cancellation, and commit failures leave hydrated state unchanged; operation
errors propagate rather than being acknowledged as cleanup. Recovery/retry uses the same
idempotent operation. There is no background entity scan: for an imported state with no signal, or a
signal lost/failed during rollout, the host can explicitly invoke the `CheckAndExpireResults` entity
operation (no input required) on the known entity. One successful turn sweeps its existing due payloads
and installs a missing schedule or supersedes an overdue/stuck token once. A valid future schedule is
reused, so repeated recovery cannot multiply the chain. An early failed check retains its token and
can be retried with the same input, or recovered without input once its persisted deadline is due.
A later successful **new** run also performs this recovery. A host
requiring a cleanup-time bound for otherwise idle imported entities must enqueue that operation as part
of import/recovery and monitor failed operations. Read-only polling and terminal duplicate calls do not
provide durable cleanup. Cleanup never migrates legacy state and rejects mailbox writes while the gate
is off.

Malformed/unsupported scheduling profiles fail closed before a new model invocation or cleanup.
Other extensions and unknown fields in a supported profile are preserved. Non-relying result reads
remain opaque. Compatible writers must preserve this profile and honor its scheduling contract;
preserving unknown JSON alone does not make an older scheduler safe to deploy alongside this writer.

**Merge and release gate:** this draft must not merge or release until the actual real-backend atomicity
test passes in an explicitly isolated environment. A clearly documented gated skip is acceptable only
for draft readiness, not for merge or release. Schema-2 production writing remains inaccessible in this
layer, even when public
`ResultRetentionPeriod` is set. Activation requires coordinated reader/writer/rollback compatibility,
late-duplicate/deletion policy agreement, and an executed real-backend atomicity test in an explicitly
isolated environment. The [gated integration test](../../tests/Microsoft.Agents.AI.DurableTask.IntegrationTests/README.md)
checks state plus outgoing delayed signals, worker restart, duplicate delivery and a failure after
state/outbox staging. A skipped test or passing mock tests **does not verify backend atomicity** and
does not satisfy either the merge gate or the release gate.

`response.GetDurableResult()` returns the canonical retained terminal-response JSON for a durable
delivery, including optional `value` and unknown metadata that the native `AgentResponse` cannot
represent. An absent value remains absent, not explicit null. The registered `DurableDataConverter`
transports this JSON-only snapshot in additive namespaced response metadata, preserving native response
fields and plain legacy response reads. Direct native serialization does not preserve this association.
The metadata is result data, never a workflow control envelope or a runtime type selector.
When complete legacy history is independently authorized for promotion, the retained mailbox snapshot
also preserves declared response extension data and unknown response fields independently of the
transcript. First delivery, cold polling, and repeated duplicates retain that canonical metadata;
JSON-looking response text never supplies a missing canonical `value`.

## Workflow output trust boundary

Agent/model output and request-port responses are data, never workflow control envelopes. The framework
wraps their exact text in result-only values, including text that happens to be valid JSON or matches an
activity envelope. Only trusted regular activity/subworkflow results may supply state updates, scope
clears, events, routed messages, or halt requests. Invalid known activity-envelope fields fail closed to
plain result text without applying partial controls. Legacy plain-text activity results remain supported.
Each trusted activity or child-workflow `sentMessages` entry must contain a nonblank string `typeName`
and `data`; null entries or missing/null/empty/whitespace fields reject the **whole** collection before
any routing. Activity JSON also rejects ambiguous repeated known fields and invalid field kinds.
An invalid child collection discards all its messages, events and halt controls, matching the
all-or-nothing activity trust boundary. Its exact `Result` text, not the serialized invalid envelope,
is the fallback; it is never decoded again as controls. Child shared state is always isolated.
These are CLR string fields: serialized JSON payloads such as `null`, `false`, `0`, and `""` remain valid
inside the `data` string. Payload text is not recursively interpreted as controls or required to resolve
a runtime type during envelope validation. Unknown fields cannot override known controls. A nonblank
unknown type name is structurally valid and travels unchanged to the target activity. If that activity
cannot resolve it or match a registered input type by name, it fails rather than choosing the first
handler. Type resolution is not added to orchestration code. Existing registered-name/version matching,
untyped input, and supported string/string-array adaptation remain unchanged.
The child runner tags its non-empty final result as a CLR string when routing it to parent successors,
so an executor supporting several input types receives the original text through its string handler
even when another supported type is listed first. Child result-only fallback (including legacy missing,
null or empty message collections) also retains string provenance. Whitespace-only `Result` text is
preserved exactly, even though a whitespace-only typed `data` field is invalid. Null/empty results do
not enqueue a fallback message. Legacy absent collections preserve trusted events/halt; invalid entries
discard those controls. Valid typed child halt requests and superstep limits are unchanged.

This is a structural provenance boundary, not a signing/authenticity mechanism. No new discriminator or
entity-state schema field is needed. The C# workflow output format is not asserted to match Python's
workflow format; shared entity-state fixture compatibility is a separate contract.

## C# history ownership profile

The shared schema 2 `historyBinding` remains optional, provisional configuration metadata. It does not
pin an effective owner or prohibit Python or other runtimes from supporting per-run transitions.
The C# durable-agent runtime applies a stricter profile: after the first successful turn it marks and
seals one logical owner using the binding version, owner kind, and a stable non-secret provider key.
Later C# turns must restore the same continuation and resolve the same identity; mismatches fail instead
of silently resetting, migrating, or starting another logical conversation.

The default in-memory history pipeline is entity-owned and appends its model transcript to
`conversationHistory`. Custom providers, model services, and opaque `CurrentRequestOnly` agents remain
authoritative for their own transcripts. They append no new request or response mirrors to
`conversationHistory`; durable delivery still uses the schema 2 terminal-result mailbox and completion
receipts, and the opaque serialized session preserves provider state, conversation IDs, approvals, and
other continuation. Mailbox results are never replayed as model history.

Configure non-entity owners with a stable logical key:

```csharp
services.ConfigureDurableAgents(options =>
{
    options.AddAIAgent(agent);
    options.SetHistoryProviderKey(agent.Name!, "contoso.support-history.v1");
});
```

The key must not contain credentials or be inferred from CLR type names, process instances, or opaque
session keys. Legacy non-entity adoption requires owner-specific public evidence: a normal service
conversation ID or a custom provider's declared `StateKeys`. Opaque `CurrentRequestOnly` and legacy
per-service-call sessions cannot prove their prior owner through the pinned public contracts and require
a new durable session.

Provider/model/session work, mailbox completion, TTL preparation, and entity transcript updates share
one isolated working-state commit boundary. Remote services can still observe a call before a later
ownership or serialization failure; those transition errors report that limitation explicitly.
Local per-service-call provider persistence remains unsupported because public callbacks do not identify
the final outer tool-loop response.

Directly discoverable stateful `CompactionProvider` configurations and in-memory reducers are rejected.
Explicitly configured `InMemoryChatHistoryProvider` instances are also rejected because the pinned public
API does not expose their initializer and message-filter delegates for faithful transfer to the durable adapter.
Use the implicit default in-memory provider for entity-owned history, or a custom external provider with a key.
The pinned Agent Framework API cannot universally inspect builder-installed or privately nested provider
decorators. Hidden stateful-compaction pipelines are unsupported but cannot be reliably rejected before
side effects without an upstream public discovery hook; this implementation does not use reflection,
type-name scanning, guessed session keys, or factory double invocation.

Pressure retention is opt-in. `KeepAll` is the default and performs no proactive history eviction; backend or
provider size limits can still reject a write. Select `Auto` and configure its positive serialized-state budget
when bounded transcript storage is preferred:

```csharp
services.ConfigureDurableAgents(options =>
{
    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
    options.MaxStateBytes = 1_048_576;
    options.AddAIAgent(agent);
});
```

`MaxStateBytes` is active only in `Auto`. The 85% high watermark starts a retention attempt, which removes the
oldest eligible transcript groups toward the 70% low watermark. The measured payload is the complete JSON state
produced by this extension, including terminal-result mailboxes, completion receipts, fixed history binding,
opaque provider or agent continuation, TTL, ingestion and workflow bookkeeping, truncation evidence, media, and
metadata. Durable Task backends can add envelope bytes outside this measurement.

Selecting `Auto` configures retention policy only; it does not activate mailbox-aware schema 2 writes.
When the internal rollout gate is enabled, existing legacy sessions are migrated only when the configured
migration authorization confirms independently authoritative complete history. Otherwise the operation fails
before model or provider side effects.

Only `conversationHistory` transcript entries are eligible for pressure eviction. Mailbox result envelopes,
completion receipts, fixed history binding, serialized continuation, TTL, and other execution controls are
protected. Correlation IDs connect transcript request/response entries, and stable tool-call IDs connect calls
with results even across entries or correlations. Duplicate non-empty tool IDs are conservatively connected;
missing or empty IDs create no cross-entry edge. System-message groups and the newest transcript exchange are
also protected.

Schema 2 mailbox results remain authoritative after their transcript copies are removed, so duplicate execution
and polling return the same retained result. Legacy state is converted to schema 2 before entity retention once
history ownership can be resolved. Retention itself fails closed if legacy transcript terminals are still the
only completion evidence.

If all eligible transcript is removed and the protected floor still reaches the high watermark,
`DurableAgentStateSizeLimitExceededException` fails the operation without committing the working state. Auto
does not expire mailbox payloads; delivery expiry is a separate mailbox policy. Large inline image and
tool-result offload is not part of this implementation.

Retention is separate from model-context compaction: retention destructively removes durable history only under
storage pressure, while compaction changes the context supplied to the model. `Auto` is not
`FollowCompaction`, and stateful compaction remains unsupported.

### Retention metrics

The package emits automatic-retention metrics through the
`Microsoft.Agents.AI.DurableTask` meter, with the package assembly version as its instrumentation scope version.
Applications can subscribe by using the public `DurableAgentTelemetry.MeterName` constant. The OpenTelemetry SDK
and exporter remain application choices; the product package depends only on `System.Diagnostics.Metrics`.

| Instrument | Type | Unit | Tags | Meaning |
| --- | --- | --- | --- | --- |
| `durable.agent.history.evicted.entries` | Counter | `{entry}` | `agent.name`, `reason` | Transcript entries removed, including entries that contain no messages. |
| `durable.agent.history.evicted.messages` | Counter | `{message}` | `agent.name`, `reason` | Transcript messages removed. |
| `durable.agent.history.reclaimed.bytes` | Counter | `By` | `agent.name`, `reason` | Positive net serialized state bytes reclaimed by transcript eviction. |
| `durable.agent.history.state.size.before` | Histogram | `By` | `agent.name`, `outcome` | Exact serialized state size when an `Auto` check reaches the high watermark. |
| `durable.agent.history.state.size.after` | Histogram | `By` | `agent.name`, `outcome` | Exact serialized state size after that pressure-retention attempt. |
| `durable.agent.history.retention.operations` | Counter | `{operation}` | `agent.name`, `outcome` | Automatic retention checks by final outcome. |

The bounded `outcome` values are `no_action`, `transcript_evicted`, and
`protected_state_capacity_failure`; the bounded `reason` value is `transcript_pressure`.
Removing a zero-message entry increments the entry counter without incrementing the message counter, and
reclaimed bytes are emitted only for a positive net reduction so truncation metadata never creates a negative
measurement. `KeepAll` emits no retention metrics. Session IDs, correlation IDs, message IDs, content,
exception text, and provider paths are never tags.

These are **attempt-level operational metrics**, not durable-state truth. Retention is evaluated before the
entity operation commits, so a later scheduling, persistence, or retry failure can leave measurements for state
that was not committed; retries can also record an attempt more than once. Exporters can buffer or drop
telemetry. Reload persisted state and inspect model input or mailbox outcomes when validating committed behavior;
do not rely on emitted counters alone or exact-once metric delivery. A metric observation is never evidence that
the corresponding retained state committed.

## Feedback & Contributing

We welcome feedback and contributions in [our GitHub repo](https://github.com/microsoft/agent-framework-durable-extension).
