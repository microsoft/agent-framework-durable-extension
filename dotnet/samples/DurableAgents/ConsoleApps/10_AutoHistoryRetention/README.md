# Opt-in Pressure Retention Sample

This sample demonstrates a durable Azure OpenAI agent that explicitly selects
`DurableAgentHistoryRetentionMode.Auto` with a deliberately small `MaxStateBytes` budget.
It also explicitly sets `EnableMailboxWrites = true`, activating schema 2 terminal results and
completion receipts through the supported public registration API. `KeepAll` is the default and
does not proactively delete model transcript under pressure.

## Scenario

The sample generates a random ASCII marker in its first project note, adds seven moderate turns
without printing their filler text, and asks the model to recall the exact marker after pressure
retention. Agent instructions keep every response under 20 words and `MaxOutputTokens = 64`
enforces a service-request bound so protected result mailboxes do not dominate the state floor.

The 32 KiB budget was checked against schema 2 state containing seven bounded terminal results,
completion receipts, the fixed history binding, serialized agent session, and bookkeeping.
Seven 4 KiB inputs plus their state envelopes cross the 85% high watermark, while the protected
mailbox and session floor still fits below it. `Auto` can therefore remove old model-transcript
groups and return state below the pressure threshold without deleting durable completion evidence.
The sample does not wait for wall-clock expiry and does not depend on a soft delivery window.

The random-marker answer is illustrative only:

- An exact marker match is classified as `Present`.
- Only `UNKNOWN` with optional final punctuation is classified as `Unavailable`.
- Every other response is `Inconclusive`.

Deterministic tests provide the correctness proof. They capture later model input, inspect
persisted and reloaded entity state, retrieve the original completed result from the mailbox, and
redeliver the original correlation while checking that the model is not executed again.

## What Auto retains

Pressure retention removes eligible model transcript. It does not remove authoritative terminal
results, completion receipts, the history-owner binding, serialized session continuation, or other
durable bookkeeping. Receipt-bearing mailbox entities are not TTL-deleted under the current public
policy. Entries connected by correlation and tool-call identity are treated as one atomic transcript
group, so retention does not leave half of an exchange behind.

This is durable entity **pressure retention**, not Microsoft Agent Framework stateful context
compaction or `FollowCompaction`. It does not summarize old messages. It removes old transcript
from later model input while durable execution and delivery evidence remains available.

Choose `KeepAll` when preserving the complete transcript is more important than proactive state
bounding. With `KeepAll`, `MaxStateBytes` is inactive and state can continue growing toward backend
limits.

`Auto` cannot make every payload fit. A single oversized protected newest inline `DataContent`,
tool result, terminal result, or other protected state can still exceed the high watermark. The
operation then fails rather than persisting oversized state. That protected-state failure is
different from cumulative transcript pressure, and application-level payload references or
transport limits remain separate concerns.

## OpenTelemetry metrics

The sample uses the standard OpenTelemetry metrics pipeline and console exporter:

```csharp
services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(DurableAgentTelemetry.MeterName)
        .AddConsoleExporter(...));
```

The meter is `Microsoft.Agents.AI.DurableTask`. It reports retention operations, evicted transcript
entries and messages, reclaimed bytes, and state size before and after a pressure-retention
attempt. The eviction reason is `transcript_pressure`. Outcomes are `no_action`,
`transcript_evicted`, and `protected_state_capacity_failure`.

These measurements are emitted for attempts before entity commit. A later persistence failure or
retry can roll back or duplicate what telemetry observed. Metrics are operational evidence, not
durable committed truth or an exact-once state query. Pair them with durable state and application
behavior when correctness matters.

## Run the sample

See the [ConsoleApps README](../README.md) for Foundry, authentication, and Durable Task Scheduler
setup. Then run:

```bash
cd dotnet/samples/DurableAgents/ConsoleApps/10_AutoHistoryRetention
dotnet run --framework net10.0
```

Enter a project topic of 80 characters or fewer. The sample prints the original random marker,
concise progress for each turn, the diagnostic question and response, an honest observation, and
standard OpenTelemetry console-exporter output when metrics flush.

## Tests

```powershell
dotnet test --project tests\10_AutoHistoryRetention.Tests.csproj -c Release -f net10.0
```

The sample-local tests use the same entity execution seam as the durable-agent product tests, with
an in-memory fake model and opaque state persisted and reloaded between operations. They do not use
credentials, a DTS service, private reflection, or the private retention algorithm. The tests prove
that:

- `Auto` is explicitly configured while `KeepAll` remains the default.
- The first marker and connected tool group leave model transcript, while the newest transcript
  remains.
- The original mailbox result and completion receipt remain available after eviction.
- Redelivery of the completed correlation returns the original result without another model call.
- The same pressure under `KeepAll` does not proactively delete transcript.
- An oversized protected newest payload fails without committing state.
- Real product retention metrics are exported alongside the persisted-state assertions.
- The production OpenTelemetry console registration observes the durable meter.
