# Custom History Provider Sample

This sample demonstrates a durable Azure OpenAI agent whose cumulative conversation history is
stored by a custom JSON-file `ChatHistoryProvider`.

> [!WARNING]
> This is a draft, experimental sample. Schema 2 mailbox writes remain behind an internal,
> default-disabled C# runtime gate, so the executable exits before reading credentials or contacting
> the model or DTS. Do not enable this scenario in mixed-runtime production until Python, the
> dashboard, pollers, and every other reader meet the schema 2 rollout floor.

## Key Concepts Demonstrated

- Persisting the full transcript outside the durable entity.
- Binding the durable session to the stable logical provider key `sample-json-file-history.v1`.
- Restoring the same external history reference with a compatible provider instance after a host restart.
- Growing external history beyond 1 MiB with many simulated 4 KiB records.
- Projecting only the newest 12 records, capped at 32 KiB of UTF-8 text, into model history.

The sample limits the user-provided marker to 1,024 UTF-8 bytes before it creates a durable session
or sends a request. The 1 KiB marker limit is intentionally smaller than each simulated 4 KiB
history record and leaves conservative space for prompt and serialization overhead, so the sample
does not send one message larger than 1 MiB. A single oversized request or tool result can exceed
the Durable Task Scheduler message boundary before the provider can process it. The seeded records
represent ordinary conversation accumulated over time.

The durable entity retains execution/delivery records, the fixed provider binding, and the opaque
provider continuation/reference. It does not keep a second external-owned request/response transcript
in `conversationHistory`, and mailbox results are not replayed into model input. External provider
writes are not transactionally coupled to the durable entity commit.

External ownership requires schema 2 terminal results and completion receipts, but activation is
deliberately unavailable through the public options API. Deterministic tests use the existing
internal test hook; the sample itself does not expose or bypass that gate. It separately keeps
`HistoryRetentionMode.KeepAll`, so mailbox activation remains independent of automatic retention.

This JSON implementation reads the full file before selecting the bounded model-history suffix.
It is text-focused, local to one process/filesystem, and not a distributed, idempotent, exactly-once,
claim-check, or payload-offload adapter. Production stores need their own concurrency, paging,
indexing, retry, item-size, retention, transaction, and eviction design, and must serialize every
content type and message property the application uses.

This is a sample storage provider, not a production design.

## Environment Setup

See the [README.md](../README.md) file in the parent directory for Foundry, authentication, and
Durable Task Scheduler setup.

## Running the Sample

```bash
cd dotnet/samples/DurableAgents/ConsoleApps/09_CustomHistoryProvider
dotnet run --framework net10.0
```

The source retains the intended flow: enter a short marker, create more than 1 MiB of cumulative
external history, ask the agent to remember the marker, restart the host with a compatible provider
instance, verify marker recall and the same external history reference, and delete the exact
sample-local JSON directory. Until the rollout gate is approved, invoking the executable prints the
draft-gate message and exits before performing those operations.

## Tests

```bash
dotnet test --project tests/09_CustomHistoryProvider.Tests.csproj
```

The sample-local tests cover marker validation at the 1,024-byte boundary (including oversized
ASCII and multibyte input), the stable logical key, external storage above 1 MiB, bounded model-history
projection, provider-reference restoration, framework-filtered persistence, unsupported content
failure, and cancellation without requiring Foundry or DTS. The durable runtime registration tests
exercise the configured keyed proxy across a cold host restart, verify schema 2 mailbox and completion
state without a transcript mirror through the internal test hook, and verify a missing mailbox
activation fails before provider or model callbacks. Additional durable runtime tests
`AgentEntityHistoryTests.RecreatedExternalProviderWithSameLogicalKeyContinuesWithoutTranscriptMirrorAsync`,
`AgentEntityHistoryTests.ChangedExternalProviderKeyRejectsBeforeProviderOrModelCallbacksAsync`,
`AgentEntityHistoryTests.CustomProviderOwnsTranscriptAndEntityStoresOnlyMailboxAndContinuationAsync`,
and the provider failure/cancellation tests cover cold entity restart, binding rejection, zero
transcript mirroring, mailbox availability, replay exclusion, and commit isolation.
