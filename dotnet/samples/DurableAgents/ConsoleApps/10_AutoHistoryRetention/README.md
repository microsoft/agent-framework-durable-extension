# Automatic History Retention Sample

> [!NOTE]
> This sample enables **Schema v2** (`DurableAgentDeploymentMode.SchemaV2`) and **Automatic history retention** (`DurableAgentHistoryRetentionMode.Auto`) for a new durable agent session. For production upgrade and task-hub rollout steps, see the [Schema v2 migration guide](../../../../../docs/features/durable-agents/schema-v2-migration.md).

The sample configures a 32 KiB state budget so you can observe automatic history pruning after just a few turns:

```csharp
services.ConfigureDurableAgents(options =>
{
    options.DeploymentMode = DurableAgentDeploymentMode.SchemaV2; // Or set DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2
    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
    options.MaxStateBytes = 32 * 1024; // Demo override; Auto otherwise defaults to 1 MiB.
    options.HistoryRetentionHighWatermark = 0.85;
    options.HistoryRetentionLowWatermark = 0.70;
    options.AddAIAgent(agent);
});
```

## Scenario

The sample generates a random ASCII marker in its first project note, adds seven moderate turns
without printing their filler text, and asks the model to recall the exact marker after pressure
retention. Agent instructions keep every response under 20 words and `MaxOutputTokens = 64`
enforces a service-request bound so protected result mailboxes do not dominate the state floor.

The offline SDK tests check the 32 KiB budget against schema 2 state containing bounded terminal results,
completion receipts, the fixed history binding, serialized agent session, and bookkeeping.
Seven 4 KiB inputs plus their state envelopes cross the 85% high watermark, while the protected
mailbox and session floor still fits below it. `Auto` can therefore remove old model-transcript
groups and return state below the pressure threshold without deleting durable completion evidence.
The sample does not wait for wall-clock expiry and does not depend on a soft delivery window.

The random-marker answer is illustrative only:

- An exact marker match is classified as `Present`.
- Only `UNKNOWN` with optional final punctuation is classified as `Unavailable`.
- Every other response is `Inconclusive`.

Both the scenario helper and diagnostic classifier reject null, empty, or whitespace-only markers
with an argument exception. The scenario validates before invoking callbacks or the agent, so
invalid markers cannot cause model calls or mutate the session. Valid markers are preserved
unchanged and compared using exact, case-sensitive matching.

The real `Microsoft.Agents.AI.DurableTask.UnitTests` project provides white-box coverage through the
SDK transactional entity batch dispatcher. It captures later model input, inspects serialized and
reloaded state, retrieves the
original completed result from the mailbox, and redelivers the original correlation while checking
that the model is not executed again.

## What Auto retains

Pressure retention removes eligible model transcript. It does not remove authoritative terminal
results, completion receipts, the history-owner binding, serialized session continuation, or other
durable bookkeeping. Receipt-bearing mailbox entities are not TTL-deleted under the current public
policy. Entries connected by correlation and tool-call identity are treated as one atomic transcript
group, so retention does not leave half of an exchange behind.

This is durable entity **pressure retention**, not Microsoft Agent Framework stateful context
compaction or `FollowCompaction`. It does not summarize old messages. It removes old transcript
from later model input while durable execution and delivery evidence remains available.

By default, .NET uses `Legacy` (Schema 1.2) and `KeepAll`. When `SchemaV2` is enabled, `Auto` defaults to a 1 MiB (`1,048,576` bytes) budget (`HighWatermark = 0.85`, `LowWatermark = 0.70`), which this sample overrides to 32 KiB. To stop automatic pruning, keep `SchemaV2` and switch `HistoryRetentionMode` to `KeepAll`.

`Auto` cannot make every payload fit. A single oversized protected newest inline `DataContent`,
tool result, terminal result, or other protected state can still exceed the high watermark. The
operation then fails rather than persisting oversized state.

## OpenTelemetry metrics

The sample registers the `agent_framework.durabletask` meter (`DurableAgentTelemetry.MeterName`) with the OpenTelemetry console exporter:

```csharp
services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(DurableAgentTelemetry.MeterName)
        .AddConsoleExporter(...));
```

Emitted `durable.retention.*` instruments:

- `durable.retention.evaluations`
- `durable.retention.budget`
- `durable.retention.state.size`
- `durable.retention.removed_messages`
- `durable.retention.removed_entries`
- `durable.retention.reclaimed_bytes`
- `durable.retention.capacity_failures`
- `durable.retention.write_attempts`
- `durable.retention.operations`

Metrics are recorded while staging state before the entity commits, so a failed or retried commit may still emit attempt metrics.

## Run the sample

Start the local Durable Task Scheduler emulator and set your Foundry model environment variables:

```powershell
$env:FOUNDRY_PROJECT_ENDPOINT = "https://YOUR-ENDPOINT/api/projects/YOUR-PROJECT"
$env:FOUNDRY_MODEL = "YOUR-MODEL-DEPLOYMENT"
Set-Location dotnet\samples\DurableAgents\ConsoleApps\10_AutoHistoryRetention
dotnet restore --source https://packagefeedproxy.microsoft.io/nuget/v3/index.json
dotnet run --framework net10.0 --no-restore
```

`DURABLE_TASK_SCHEDULER_CONNECTION_STRING` is optional and defaults to `Endpoint=http://localhost:8080;TaskHub=default;Authentication=None`. Enter a project topic (up to 80 characters) when prompted; the app creates a new durable session, sends seven turns, and prints whether the initial marker was recalled (`Present`, `Unavailable`, or `Inconclusive`).

## Tests

```powershell
dotnet test --project tests\10_AutoHistoryRetention.Tests.csproj -c Release -f net10.0
```

To run the package unit tests for public `SchemaV2` and `Auto` retention:

```powershell
dotnet test --project ..\..\..\..\tests\Microsoft.Agents.AI.DurableTask.UnitTests\Microsoft.Agents.AI.DurableTask.UnitTests.csproj `
    -c Release -f net10.0 `
    --filter-class "*AgentEntityPublicRetentionTests"
```
