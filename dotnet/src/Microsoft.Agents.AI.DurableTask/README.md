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
Producer activation and receipt-deleting entity TTL are internal test gates only, disabled by default;
this draft exposes no public schema-2 activation API. Legacy TTL behavior is preserved, but old deadlines
cannot delete schema-2 receipts without a separately agreed deletion policy. Unknown-field
preservation by an older worker is not sufficient. See [state compatibility](State/README.md).

Under the internal mailbox-writer gate, a successful new run also removes already-expired mailbox
payloads and schedules an entity-local `CheckAndExpireResults` self-signal for the earliest remaining
expiry. The replacement state and signal outbox participate in the same entity operation commit.
Cleanup preserves each completion's outcome, completion/expiry timestamps and unknown receipt metadata,
marks its result unavailable, and records the first cleanup time. It does not delete receipts, invoke
the agent/factory/tools, create a session, or refresh entity TTL. The independent entity-deletion gate
is not required for payload cleanup.

Entity operations are serialized. Each cleanup turn deep-clones and validates authoritative current
state and uses the host's `TimeProvider`, not an old signal, to decide expiry. Early, duplicate, and
previous-generation signals cannot remove a currently unexpired result, even if a correlation was reused
after deletion; a signal against a deleted entity does not recreate it. Early checks schedule at most
one successor, at least one minute after both the current clock and the prior scheduled check, avoiding
repeated scheduling at the same timestamp when the worker clock moves backward. This scheduling floor
is **not** a default retention period. Physical cleanup can lag logical expiry by that floor and scheduler
delivery/clock skew; polling reports unavailability at the recorded expiry without mutating state.
Repeated successful cleanup preserves the original unavailable timestamp.

Serialization, scheduling, cancellation, and commit failures leave hydrated state unchanged; operation
errors are logged and propagated rather than acknowledged as cleanup. Recovery/retry uses the same
idempotent operation. There is no background entity scan: for an imported state with no signal, or a
signal lost/failed during rollout, the host can explicitly invoke the `CheckAndExpireResults` entity
operation (no input required) on the known entity. One successful turn sweeps its existing due payloads
and restarts the scheduling chain. A later successful **new** run also performs this recovery. A host
requiring a cleanup-time bound for otherwise idle imported entities must enqueue that operation as part
of import/recovery and monitor failed operations. Read-only polling and terminal duplicate calls do not
provide durable cleanup. Cleanup never migrates legacy state and rejects mailbox writes while the gate
is off.

`response.GetDurableResult()` returns the canonical retained terminal-response JSON for a durable
delivery, including optional `value` and unknown metadata that the native `AgentResponse` cannot
represent. An absent value remains absent, not explicit null. The registered `DurableDataConverter`
transports this JSON-only snapshot in additive namespaced response metadata, preserving native response
fields and plain legacy response reads. Direct native serialization does not preserve this association.
The metadata is result data, never a workflow control envelope or a runtime type selector.

## Workflow output trust boundary

Agent/model output and request-port responses are data, never workflow control envelopes. The framework
wraps their exact text in result-only values, including text that happens to be valid JSON or matches an
activity envelope. Only trusted regular activity/subworkflow results may supply state updates, scope
clears, events, routed messages, or halt requests. Invalid known activity-envelope fields fail closed to
plain result text without applying partial controls. Legacy plain-text activity results remain supported.
Each trusted activity `sentMessages` entry must explicitly contain a nonblank string `typeName` and
`data`; missing/null/invalid fields or ambiguous repeated known fields reject the **whole** envelope.
These are CLR string fields: serialized JSON payloads such as `null`, `false`, `0`, and `""` remain valid
inside the `data` string. Payload text is not recursively interpreted as controls or required to resolve
a runtime type during envelope validation. Unknown fields cannot override known controls.

This is a structural provenance boundary, not a signing/authenticity mechanism. No new discriminator or
entity-state schema field is needed. The C# workflow output format is not asserted to match Python's
workflow format; shared entity-state fixture compatibility is a separate contract.

## Feedback & Contributing

We welcome feedback and contributions in [our GitHub repo](https://github.com/microsoft/agent-framework-durable-extension).
