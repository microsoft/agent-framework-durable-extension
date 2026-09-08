# Automatic History Retention Sample

This sample demonstrates a durable Azure OpenAI agent configured with
`DurableAgentHistoryRetentionMode.Auto` and a deliberately small `MaxStateBytes` budget.

## Scenario

The sample generates a random ASCII marker in its first project note, adds several moderate turns
without printing their filler text, and then asks the model to recall the exact marker after
automatic retention has applied pressure to durable state.

The durable-state budget is exactly 32 KiB (32,768 bytes). Seven turns each include 4 KiB of ASCII
filler, so the filler alone reaches 28,672 bytes (87.5% of the budget), deterministically crossing
the 85% high watermark. The first four turns become eligible before the final three are added,
providing enough removable content to target the 70% low watermark.

Successful durable responses are delivered through a signal plus polling and are normally protected
for 60 seconds so the caller can retrieve them. The sample waits 61 seconds after its first four
turns so those old exchanges become normally eligible for eviction. A hard state budget can override
the delivery window as a fallback, which can require callers still polling an evicted response to
retry, but this scenario provides enough old eligible history to exercise normal `Auto` eviction.

Automatic retention starts when serialized extension state crosses the 85% high watermark and
targets the 70% low watermark. It preserves system messages and the newest exchange. Choose
`KeepAll` when retaining every exchange is more important than bounded durable state, understanding
that state can then grow until a backend limit is reached.

This is durable entity **pressure retention**, not Microsoft Agent Framework stateful context
compaction or `FollowCompaction`. It does not summarize old messages. It removes eligible exchanges
from durable state. Automatic retention also cannot make a single oversized protected newest
`DataContent` item or tool result fit within the configured budget; that operation fails instead of
persisting unsafe oversized state.

## OpenTelemetry metrics

The sample uses the standard OpenTelemetry metrics pipeline and console exporter:

```csharp
services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(DurableAgentTelemetry.MeterName)
        .AddConsoleExporter(...));
```

The meter is `Microsoft.Agents.AI.DurableTask`. It reports retention operations, evicted entries and
messages, reclaimed bytes, and state size before and after a pressure-retention attempt. Reasons
distinguish normal `pressure` from `delivery_protection_override`; outcomes distinguish `no_action`,
`evicted`, `forced_delivery_eviction`, and `failed_protected_state`.

These measurements are emitted for attempts before entity commit. A later persistence failure or
retry can roll back or duplicate what telemetry observed. Metrics are operational evidence, not
durable committed truth or an exact-once state query. The model's answer is only a human-readable
illustration of the effect. Product-layer tests cover the internal retention mechanics.

## Run the sample

See the [ConsoleApps README](../README.md) for Foundry, authentication, and Durable Task Scheduler
setup. Then run:

```bash
cd dotnet/samples/DurableAgents/ConsoleApps/10_AutoHistoryRetention
dotnet run --framework net10.0
```

Enter a project topic of 80 characters or fewer. The sample prints:

- The original random marker.
- Why it waits 61 seconds.
- The diagnostic question and model response.
- An honest `Present`, `Unavailable`, or `Inconclusive` observation.
- Standard OpenTelemetry console-exporter output when metrics flush.

The diagnostic prompt tells the model to answer only `UNKNOWN` when the marker is absent. An exact
marker match means the marker is present. Only `UNKNOWN` with optional final punctuation is classified
as unavailable. Every other response is inconclusive.

## Tests

```bash
dotnet test --project tests/10_AutoHistoryRetention.Tests.csproj
```

The sample-local tests stay on supported public boundaries. They verify that public durable options
select `Auto` with an exact 32 KiB (32,768-byte) budget, a public fake `AIAgent` receives the marker-first scenario,
moderate turns, an injected 61-second delay, and the diagnostic question, and the marker
classification avoids false passes. They also emit a real measurement and prove that the registered
meter and exporter collect it, including the production console-exporter path.

Internal eviction, watermarks, delivery protection, truncation metadata, and retention telemetry are
covered by the product-layer retention tests. The repository sample verifier runs this real sample
only when its Azure OpenAI and Durable Task Scheduler environment variables are available.
