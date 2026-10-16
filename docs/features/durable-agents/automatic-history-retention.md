# Automatic history retention in .NET

Long-running agent sessions accumulate conversation history over time. **Automatic history retention (`Auto`)** keeps your durable entity state within a target byte budget by automatically pruning the oldest conversation turns when state size crosses a high watermark, while preserving completion receipts, active results, and recent context.

> [!NOTE]
> Automatic retention requires **Schema v2** (`DurableAgentDeploymentMode.SchemaV2` or `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2`). By default, .NET uses `Legacy` (Schema `1.2.0`) and `KeepAll` (no pruning). For upgrade and task-hub rollout steps, see the [Schema v2 migration guide](schema-v2-migration.md).

## Configuration

```csharp
services.ConfigureDurableAgents(options =>
{
    options.DeploymentMode = DurableAgentDeploymentMode.SchemaV2;
    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;

    // Optional: defaults to 1 MiB (1,048,576 bytes), 0.85 high watermark, 0.70 low watermark
    options.MaxStateBytes = 32 * 1024;
    options.HistoryRetentionHighWatermark = 0.85;
    options.HistoryRetentionLowWatermark = 0.70;

    options.AddAIAgent(agent);
});
```

You can also enable Schema v2 via the environment variable `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` instead of setting `options.DeploymentMode` in code.

Configuration callbacks run immediately and compose across registrations; complete all registrations before resolving services or starting the host. Completed settings and the deployment environment are validated and captured at first options finalization, before normal SDK worker/client initialization or registered agent factories run. See [C# registration and validation timing](schema-v2-migration.md#c-registration-and-validation-timing).

## How budgeting and pruning work

- **Whole-entity byte budget:** `Auto` measures the full serialized entity state (UTF-8 JSON storage envelope), not token count.
- **Default budget and watermarks:** `MaxStateBytes` defaults to **1 MiB (`1,048,576` bytes)** when omitted or `null`. Watermarks default to `0.85` (high) and `0.70` (low), and must satisfy `0 < low < high <= 1` when `Auto` is active.
- **Eviction behavior:** When the serialized state reaches `HistoryRetentionHighWatermark`, the runtime removes the oldest eligible conversation turns in atomic groups until the state fits below `HistoryRetentionLowWatermark` (or until only protected entries remain below the high watermark).
- **Protected state (never pruned):**
  - System messages
  - The newest user/assistant exchange
  - Connected tool-call and tool-result chains (turns and linked tool calls are evicted atomically—never half a turn)
  - Terminal result mailbox payloads, completion receipts, session continuation, and history binding metadata
- **When protected state exceeds the budget:** If a single large message, tool result, or accumulated receipts push the protected state over the high watermark even after all eligible history is removed, the turn fails with an `InvalidOperationException` rather than corrupting state or deleting completion receipts.
- **Switching back to `KeepAll`:** Under `KeepAll`, `MaxStateBytes` and watermark values are ignored. You can switch `HistoryRetentionMode` from `Auto` to `KeepAll` without removing your budget settings.

## History ownership and providers

| History configuration | Behavior under `SchemaV2` (`KeepAll` and `Auto`) |
| --- | --- |
| **Default (`ChatClientAgent` / entity-owned history)** | Conversation history is stored in the entity transcript; `Auto` prunes the oldest eligible turns under byte pressure. |
| **Custom `ChatHistoryProvider`** | Requires `history.ProviderKey` and persisted `StateKeys` continuation. History lives in your external store; the entity does not mirror or delete it. |
| **Service-managed conversation (e.g., Foundry thread ID)** | Requires `history.ProviderKey`. History lives in the model service; the entity stores only the conversation ID. |
| **`CurrentRequestOnly` / opaque session** | Requires `history.ProviderKey`. Only the current turn is sent; opaque session state is preserved. |
| **Explicit `InMemoryChatHistoryProvider` with custom reducers/filters** | Not supported under `SchemaV2` (rejected before execution). Use default entity history or a custom `ChatHistoryProvider`. |

> [!TIP]
> **Compaction vs. retention:** Agent Framework's `CompactionProvider` reduces which messages are sent to the model on each turn, whereas `Auto` retention prunes the durable entity state when approaching storage size limits. See [Sample 10](../../../dotnet/samples/DurableAgents/ConsoleApps/10_AutoHistoryRetention/README.md) for a runnable `Auto` retention example.

## External history providers and retry safety

External `ChatHistoryProvider` callbacks, model calls, and tools run outside the durable entity transaction. Under both `KeepAll` and `Auto`, an unrecovered external load/store callback failure propagates without creating a framework failed result or receipt; local working state and staged outgoing signals roll back, but remote effects do not ([#143](https://github.com/microsoft/agent-framework-durable-extension/issues/143)). A load failure before model execution leaves those model/tool calls unstarted. A store failure after execution may follow model/tool effects or a successful remote write, so retrying the same uncommitted correlation may execute again. See [Make retries safe in your application](schema-v2-migration.md#make-retries-safe-in-your-application) for result ledgers, duplicate-safe history writes, and reconciliation guidance.
