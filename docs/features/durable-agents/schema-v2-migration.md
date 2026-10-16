# Upgrading durable agents to Schema v2

Schema v2 (`2.0.0`) separates **request completion tracking** (terminal result mailboxes and completion receipts) from **conversation history**. This lets durable agents prune old conversation turns under storage pressure without forgetting which requests have already completed.

## Quick start: What happens when I upgrade?

| Language | Default behavior on upgrade | How to enable Schema v2 | How to enable automatic history pruning |
| --- | --- | --- | --- |
| **.NET (C#)** | **No breaking change.** Writes Schema `1.2.0` (`Legacy`) and keeps all history (`KeepAll`). Can read both `1.x` and `2.0` state. | Set `options.DeploymentMode = DurableAgentDeploymentMode.SchemaV2` or `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2`. | Set `options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto` (defaults to a 1 MiB budget). |
| **Python** | **Requires v2 opt-in.** Writes Schema `2.0.0`. `read_agent_state()` can still read `1.x` state. | Pass `deployment_mode="isolated_v2"` or set `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2`. | Set `max_state_bytes=1_048_576` (enabled by default on standalone `DurableTaskSchedulerWorker`; opt-in on Azure Functions). |

## Rolling out Schema v2 safely

Schema v2 does not migrate active `1.x` sessions in place. When moving an application to Schema v2:

1. **Isolate a compatible fleet before the first v2 write.** Use a fresh isolated task hub if you cannot guarantee compatible workers, clients, pollers, readers, and other consumers through fleet control and routing. Reuse an existing hub only after excluding incompatible consumers from v2 work; a new hub name alone does not prove isolation.
2. **Let existing `1.x` sessions and in-flight workflows finish on your existing deployment.** Do not point v2 workers at active `1.x` workflow histories.
3. **Start new sessions for v2 after isolating the fleet.** Fresh session IDs avoid reusing old state; they do not prevent old workers on the same hub from receiving v2 work. In C#, a `SchemaV2` worker rejects existing `1.x` sessions rather than rewriting them in place.
4. **Keep external history stores intact.** If your agent uses a custom `ChatHistoryProvider` or a model-service conversation ID (such as Foundry threads), local entity retention never deletes records from your external store.

Code-selected `SchemaV2` and `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` are operator acknowledgements, not runtime checks of fleet compatibility or routing.

## Enabling Schema v2 (`DeploymentMode` and `DURABLE_AGENTS_DEPLOYMENT_MODE`)

You can enable Schema v2 in code or through the shared environment variable:

```text
DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2
```

### C# configuration and environment rules

In C#, `DeploymentMode` defaults to `Legacy` (Schema `1.2.0`) unless configured in code or via `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2`:

| Code (`options.DeploymentMode`) | `DURABLE_AGENTS_DEPLOYMENT_MODE` | Effective mode |
| --- | --- | --- |
| Unspecified | Unset | `Legacy` (Schema `1.2.0`) |
| Unspecified | `isolated_v2` | `SchemaV2` (Schema `2.0.0`) |
| `Legacy` | Unset | `Legacy` (Schema `1.2.0`) |
| `SchemaV2` | Unset or `isolated_v2` | `SchemaV2` (Schema `2.0.0`) |
| `Legacy` | `isolated_v2` | Rejected when completed options are finalized (conflicting configuration) |
| Any | Empty, whitespace, wrong case, or any other value | Rejected when completed options are finalized (invalid value) |

### C# registration and validation timing

`ConfigureDurableAgents`, `ConfigureDurableOptions`, and `ConfigureDurableWorkflows` compose additively. Their configuration callbacks execute immediately during registration, including worker/client builder configuration callbacks. Complete **all registration calls before resolving services or starting the host**.

Composite validation and the deployment environment snapshot are deferred until the completed agent options are first finalized through dependency injection, the normal SDK options dependencies, or host startup. Setting `Auto` in one registration and `SchemaV2` in a later registration is supported, as is the reverse order. Only the completed combination is validated. Invalid enum values, nonpositive byte budgets, and other individual property-domain violations can still throw when assigned.

Do not rely on a registration callback, `BuildServiceProvider()`, or `IHost.Build()` to perform composite validation. Normal SDK worker/client initialization and registered agent factories require finalized valid options before running; invalid final settings also fail host startup when no SDK builders are registered. Validation does not wait for the first model or provider call.

The deployment environment is captured once at finalization, not during each registration callback. Changing it afterward does not change the resolved mode. To change deployment settings, stop admission and configure a new host deliberately.

### Python configuration rules

In Python, `DurableAIAgentWorker` and `AgentFunctionApp` require `deployment_mode="isolated_v2"` (or `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` when `deployment_mode` is omitted).

## Configuring history retention

### C#: `KeepAll` (default) vs `Auto`

```csharp
// Option 1: Default Legacy (Schema 1.2) + KeepAll
services.ConfigureDurableAgents(options =>
{
    options.DeploymentMode = DurableAgentDeploymentMode.Legacy;
    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;
    options.AddAIAgent(agent);
});

// Option 2: Schema v2 + KeepAll (no automatic transcript pruning)
services.ConfigureDurableAgents(options =>
{
    options.DeploymentMode = DurableAgentDeploymentMode.SchemaV2;
    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;
    options.AddAIAgent(agent);
});

// Option 3: Schema v2 + Auto (automatic byte-budget history pruning)
services.ConfigureDurableAgents(options =>
{
    options.DeploymentMode = DurableAgentDeploymentMode.SchemaV2;
    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
    // Defaults to 1 MiB (1,048,576 bytes); optionally override:
    // options.MaxStateBytes = 32 * 1024;
    options.HistoryRetentionHighWatermark = 0.85;
    options.HistoryRetentionLowWatermark = 0.70;
    options.AddAIAgent(agent);
});
```

Key C# behavior:
- `Auto` requires `SchemaV2` and defaults to a **1 MiB (`1,048,576` bytes)** state budget (`HighWatermark = 0.85`, `LowWatermark = 0.70`, where `0 < low < high <= 1`). Setting `MaxStateBytes = null` uses the 1 MiB default.
- Under `KeepAll`, `MaxStateBytes` and watermark settings are ignored—you can switch from `Auto` back to `KeepAll` without clearing your budget settings.
- For full details on C# retention and provider behavior, see [Automatic history retention in .NET](automatic-history-retention.md).

### Python: `keep_all`, `follow_compaction`, and `max_state_bytes`

```python
from agent_framework_durabletask import DurableAIAgentWorker

# Schema v2 with pressure eviction disabled:
agents = DurableAIAgentWorker(
    worker,
    deployment_mode="isolated_v2",
    retention="keep_all",
    max_state_bytes=None,
)
agents.add_agent(agent)

# Schema v2 with a byte budget (evicts oldest eligible history groups above 85%):
agents = DurableAIAgentWorker(
    worker,
    deployment_mode="isolated_v2",
    retention="keep_all",
    max_state_bytes=32 * 1024,
    high_watermark=0.85,
    low_watermark=0.70,
)
```

For Azure Functions, pass the same settings to `AgentFunctionApp(deployment_mode="isolated_v2", retention="keep_all", max_state_bytes=...)`.

Key Python behavior:
- `max_state_bytes` controls byte-budget eviction independently of `retention`:
  - On standalone `DurableTaskSchedulerWorker`, omitting `max_state_bytes` defaults to a **1 MiB (`1,048,576` bytes)** budget. Pass `max_state_bytes=None` to disable byte-budget eviction.
  - On generic `TaskHubGrpcWorker` and `AgentFunctionApp` (Azure Functions), `max_state_bytes` defaults to `None` (disabled). Pass a positive integer (e.g., `1_048_576`) to enable it.
- `retention="follow_compaction"` eagerly removes messages excluded by a `CompactionProvider`, even when `max_state_bytes=None`.

## Using custom or service-managed history providers

Both `KeepAll` and `Auto` support external `ChatHistoryProvider` implementations and service-managed conversations (such as Foundry or OpenAI conversation IDs). Local retention never trims your external store or remote conversation.

In C#, register a stable, non-secret `ProviderKey` so the session verifies it is using the same logical history provider across restarts:

```csharp
options.AddAIAgent(
    externalOrServiceAgent,
    timeToLive: null,
    configureHistory: history =>
        history.ProviderKey = new DurableAgentHistoryProviderKey("application.history.v1"));
```

### Make retries safe in your application

Durable entity state and self-signals commit atomically, but external `ChatHistoryProvider` callbacks, model calls, and tools execute outside the entity transaction. An unrecovered external load or store callback failure propagates without creating a framework failed result or completion receipt. Local working state and staged outgoing signals roll back to the last committed state and continuation; remote effects do not roll back. This is the same contract under `KeepAll` and `Auto` ([#143](https://github.com/microsoft/agent-framework-durable-extension/issues/143)).

- **Load failure before model or tool execution:** Those model/tool calls have not run, but the provider may already have contacted or changed its remote store.
- **Store failure after execution:** Model, tool, or business effects may already have occurred. A remote history write may also have succeeded before the callback threw, leaving partial or duplicate history updates and an ambiguous outcome.

Without a committed receipt, the same correlation ID may execute again if the request is retried. This does not mean every exception is automatically retried or proves that nothing happened. Cancellation, timeout, response loss, and uncertain entity acknowledgement can also leave remote effects ambiguous. A committed completion still deduplicates its correlation after transcript pruning or result-payload expiry, but cannot protect effects from an earlier execution that never committed a receipt. This is an at-least-once execution boundary, not an exactly-once guarantee.

To make retries safe when using external history providers or side-effecting tools:

1. **Pass a stable application operation ID.** Use the durable correlation where your integration exposes it; otherwise supply a business operation ID in `ChatMessage.AdditionalProperties` and keep it across retries. Do not assign a new key to hide an ambiguous retry:

   ```csharp
   ChatMessage message = new(ChatRole.User, "Process the approved order")
   {
       AdditionalProperties = new() { ["operationId"] = operationId },
   };
   RunRequest request = new([message]);
   ```

2. **Forward that idempotency key to tools and external APIs.** Your custom `ChatHistoryProvider` can read `InvokedContext.RequestMessages` and `AdditionalProperties["operationId"]`, and tools can pass it to downstream APIs:

   ```csharp
   using HttpRequestMessage call = new(HttpMethod.Post, approvedEndpoint);
   call.Headers.TryAddWithoutValidation("Idempotency-Key", operationId);
   ```

3. **Persist completed results and operation status.** Enforce uniqueness for the operation key in your application ledger. Return its recorded result on duplicate requests, and track states such as `Pending`, `Completed`, and `NeedsReconciliation`, together with remote operation IDs. Local execution starting is not proof of completion.
4. **Use deterministic message IDs in external history stores.** Include the session, operation key, role, and occurrence in keys such as `{sessionId}:{operationId}:assistant:0`. Upsert those records or make repeated writes duplicate-safe no-ops instead of appending duplicates.
5. **Check remote status before retrying non-idempotent operations.** Query the external system by the stable key or consult your ledger before repeating an ambiguous operation. A remote success followed by a local ledger-write failure must be reconciled, not treated as proof that the remote operation failed.
6. **Use an outbox only with a real transaction.** If an application-owned database transaction covers its result ledger and outbox, use it to arrange eventual external delivery. The durable entity commit does not make an arbitrary external provider write transactional.
7. **Reconcile effects that cannot be queried or deduplicated.** Route them to manual reconciliation; avoid non-idempotent side effects unless duplicate execution is acceptable or independently protected.

## What is preserved vs. pruned in Schema v2

| Component | Behavior under Schema v2 |
| --- | --- |
| **Conversation transcript (`conversationHistory`)** | Stores model context when the entity owns history. Under `Auto` (or a Python byte budget), the oldest eligible turns are evicted as whole atomic groups (never splitting a turn or a tool-call/result pair). System messages and the newest turn are always protected. |
| **Terminal result mailbox (`terminalResults`)** | Holds the caller-visible response or error for the delivery window (`ResultRetentionPeriod`, 60 seconds by default). Protected from transcript eviction. |
| **Completion receipts (`completionReceipts`)** | Permanent record that a correlation ID completed (`succeeded` or `failed`). Survives both transcript pruning and result payload expiry so completed requests are never re-executed on duplicate delivery. |
| **History binding & session state (`historyBinding`, `session`)** | Protected across turns and worker restarts. |
| **Oversized protected state** | If protected state alone (such as a large newest message, tool output, or accumulated receipts) exceeds the high watermark after all eligible history is removed, the write fails without corrupting entity state. |

## Rollback and troubleshooting

- **Switching from `Auto` to `KeepAll` (C#):** Keep `DeploymentMode = DurableAgentDeploymentMode.SchemaV2` and set `HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll`. Existing v2 sessions continue normally without pruning.
- **Reverting `DeploymentMode` to `Legacy` (C#):** Existing v2 sessions remain readable for polling and duplicate-delivery checks, but new turns against a v2 session are rejected while the worker is in `Legacy` mode.
- **Rolling back to an older v1-only package:** Point the v1 deployment at its original v1 task hub or pre-upgrade backup. Do not route Schema `2.0.0` entities to a `1.x`-only worker.
