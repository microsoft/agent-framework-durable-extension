# Opt-in Pressure Retention Sample

> [!IMPORTANT]
> This is a draft/experimental sample. Automatic transcript retention requires schema 2 mailbox
> state, whose writer is internal and disabled by default. The runnable entry point fails closed
> before reading credentials or contacting the model or DTS. Do not enable this scenario in
> mixed-runtime production until Python, the dashboard, pollers, rollback tooling, and every other
> reader meet the schema 2 rollout floor.

The public sample source preserves the intended prompts, bounded model options, marker diagnostic,
and OpenTelemetry registration. It does **not** configure a retention mode or state budget because
those activation settings are internal. The real package unit tests apply the draft profile through
the existing internal, default-disabled test hook.

## Scenario

The sample generates a random ASCII marker in its first project note, adds seven moderate turns
without printing their filler text, and asks the model to recall the exact marker after pressure
retention. Agent instructions keep every response under 20 words and `MaxOutputTokens = 64`
enforces a service-request bound so protected result mailboxes do not dominate the state floor.

The 32 KiB budget was checked against schema 2 state containing eight bounded terminal results,
completion receipts, the fixed history binding, serialized agent session, and bookkeeping.
Seven 4 KiB inputs plus their state envelopes cross the 85% high watermark, while the protected
mailbox and session floor still fits below it. `Auto` can therefore remove old model-transcript
groups and return state below the pressure threshold without deleting durable completion evidence.
The sample does not wait for wall-clock expiry and does not depend on a soft delivery window.

The random-marker answer is illustrative only:

- An exact marker match is classified as `Present`.
- Only `UNKNOWN` with optional final punctuation is classified as `Unavailable`.
- Every other response is `Inconclusive`.

The real `Microsoft.Agents.AI.DurableTask.UnitTests` project provides the white-box correctness
proof. It captures later model input, inspects persisted and reloaded entity state, retrieves the
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

This release exposes no public retention-mode selection. Internal `KeepAll` remains the
default-off policy and performs no proactive transcript deletion.

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

The shared meter is `agent_framework.durabletask`, exposed through
`DurableAgentTelemetry.MeterName`. Corrected retention builds emit the cross-runtime
`durable.retention.*` instruments:

- `durable.retention.evaluations`
- `durable.retention.budget`
- `durable.retention.state.size`
- `durable.retention.removed_messages`
- `durable.retention.removed_entries`
- `durable.retention.reclaimed_bytes`
- `durable.retention.capacity_failures`
- `durable.retention.write_attempts`
- `durable.retention.operations`

Retention dimensions are bounded operational fields such as `mechanism`, `outcome`,
`commit_status`, `phase`, `stage`, and `deletion_staged`. They do not include agent, session,
correlation, message, or content dimensions. Outcomes include `below_threshold`, `staged`,
`protected_floor`, `returned`, and `failed`.

These measurements are emitted for attempts before entity commit. A later persistence failure or
retry can roll back or duplicate what telemetry observed. Metrics are operational evidence, not
durable committed truth or an exact-once state query. Pair them with durable state and application
behavior when correctness matters.

## Run the sample

The entry point is intentionally unsupported and exits before accessing credentials or services:

```bash
cd dotnet/samples/DurableAgents/ConsoleApps/10_AutoHistoryRetention
dotnet run --framework net10.0
```

It prints the draft-gate explanation and exits with code 2. A future public activation design must
be reviewed before this becomes runnable.

## Tests

```powershell
dotnet test --project tests\10_AutoHistoryRetention.Tests.csproj -c Release -f net10.0
```

The sample-local tests use only public APIs. They cover bounded agent options, prompt construction,
the no-wait marker diagnostic, conservative result classification, and real OpenTelemetry meter
registration/export.

The white-box retention regression lives in the real package unit-test project, which legitimately
has friend access:

```powershell
dotnet test --project ..\..\..\..\tests\Microsoft.Agents.AI.DurableTask.UnitTests\Microsoft.Agents.AI.DurableTask.UnitTests.csproj `
    -c Release -f net10.0 `
    --filter-method "*DraftSampleProfileEvictsTranscriptButPreservesCompletionAndIdempotencyAsync"
```

That regression applies the draft profile through the internal test gate and proves transcript
eviction, connected tool grouping, mailbox and receipt preservation, persisted-state reload,
idempotent duplicate delivery, later model-input removal, and product retention telemetry.
