# Durable Foundry Managed Agent

This sample wraps a server-managed, versioned Microsoft Foundry agent as a Microsoft Agent Framework `FoundryAgent`, registers it as a durable `AIAgent`, and restores its opaque service continuation across a host restart.

> [!WARNING]
> This is a draft, experimental sample. Schema 2 mailbox writes remain behind an internal,
> default-disabled C# runtime gate, so the executable exits before creating any Foundry resource.
> Do not enable this scenario in mixed-runtime production until Python, the dashboard, pollers, and
> every other reader meet the schema 2 rollout floor.

## Architecture

```text
Console application
  |
  | keyed AIAgent proxy + DurableAgentSession
  v
Durable Task Scheduler
  |-- owns invocation, retry, durable entity state, and entity lifetime
  |-- binds the durable session to a stable logical service-owner key
  |-- persists execution/delivery records and the opaque inner AgentSession
  v
FoundryAgent -> ChatClientAgent -> Foundry Responses API
  |-- owns the versioned agent definition
  `-- owns the server-side conversation and its transcript
```

The application:

1. Creates a uniquely named Foundry agent version with `CreateAgentVersionAsync`.
2. Wraps that exact version with `AIProjectClient.AsAIAgent`, producing a `FoundryAgent`.
3. Registers the `FoundryAgent` with `ConfigureDurableAgents`, keeps `HistoryRetentionMode.KeepAll`, and sets the stable logical key `foundry-managed-service.v1`. Deterministic tests activate schema 2 through the runtime's internal test hook; the sample does not expose or bypass that gate.
4. Resolves the keyed durable `AIAgent` proxy and creates a `DurableAgentSession`.
5. Sends a generated marker on the first turn. Foundry establishes the service conversation, and the durable entity persists the inner `ChatClientAgent` session containing that conversation identity.
6. Stops the host, creates a fresh `FoundryAgent` wrapper and host, and sends a second turn using the same `DurableAgentSession`. The durable entity restores the opaque inner session. The deterministic registration test verifies that the same service conversation ID survives serialization and restoration; the live marker recall is an additional smoke signal, not the proof of identity.
7. Stops the host before deleting the exact server agent version in `finally`.

The host restart is intentional: it demonstrates that continuity comes from durable entity state, not from an in-memory `FoundryAgent` or `AgentSession`.

## History ownership

After Foundry establishes a conversation, Foundry owns the transcript. The durable entity keeps the fixed logical owner binding, execution/delivery records, and opaque inner-session continuation. It does not keep or replay a duplicate transcript in `conversationHistory`.

Schema 2 terminal results and completion receipts are required because the service-owned transcript is not mirrored into durable state. Activation is deliberately unavailable through the public options API. The deterministic tests use the existing internal test hook, while this sample remains gated and retains `HistoryRetentionMode.KeepAll` rather than opting into automatic transcript eviction.

This fixed ownership is an intentional C# durable runtime contract. `HistorySampleRegistrationTests.FoundryAgentRegistrationRestoresServiceContinuationAcrossProxyRestartAsync` invokes the actual versioned `FoundryAgent` through the registered durable proxy and verifies the entity-to-service transition, mailbox-only state, cold-host continuation, and a second call without credentials or network access. `FoundryAgentRegistrationTests.ServerManagedFoundryAgentRestoresFixedServiceOwnershipAsync` separately verifies wrapper discovery and the stable provider key. `AgentEntityHistoryTests.ServiceManagedConversationStoresOnlyMailboxAndContinuationAsync`, `AgentEntityHistoryTests.FirstServiceManagedTurnDoesNotLeaveEntityOwnedTranscriptAsync`, and `AgentEntityHistoryTests.WrappedServerManagedSessionSurvivesColdEntityInvocationAsync` cover the durable entity state shape and cold continuation.

This versioned `FoundryAgent` path does **not** enable `ChatClientAgentOptions.RequirePerServiceCallChatHistoryPersistence`, so the sample intentionally does not call `SetServiceManagedPerServiceCallHistory`. That declaration is only needed when a service-backed `ChatClientAgent` explicitly enables per-service-call persistence; it is otherwise ignored.

For arbitrary server-hosted `AIAgent` implementations that do not expose a discoverable `ChatClientAgent` pipeline, configure `DurableAgentHistoryReplayMode.CurrentRequestOnly` explicitly. This sample does not need that fallback because `FoundryAgent` exposes its inner `ChatClientAgent` through `GetService<ChatClientAgent>()`.

## Prerequisites and configuration

- .NET 10 SDK
- Azure CLI authenticated with `az login`
- Permission to create, invoke, and delete agents in the Foundry project
- A Durable Task Scheduler endpoint, such as the local DTS emulator

Set:

```powershell
$env:FOUNDRY_PROJECT_ENDPOINT = "https://<resource>.services.ai.azure.com/api/projects/<project>"
$env:FOUNDRY_MODEL = "<OpenAI-model-deployment>"
$env:DURABLE_TASK_SCHEDULER_CONNECTION_STRING = "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
```

Under this sample's model policy, configure an OpenAI model deployment that is supported by Foundry prompt agents and the Responses API. No credentials are stored in tracked files. `DefaultAzureCredential` is convenient for local development; production applications should prefer a specific credential such as `ManagedIdentityCredential` to avoid unintended credential probing and latency.

## Run

The source is retained as the intended end-to-end flow, but it is not currently runnable. Invoking it
prints the draft-gate message and exits before reading credentials, creating a Foundry agent version,
or contacting DTS:

```powershell
cd dotnet\samples\DurableAgents\ConsoleApps\08_FoundryManagedAgent
dotnet run --framework net10.0
```

Once the cross-runtime rollout floor is met and the production gate is approved, a successful run is
expected to print both responses and `Marker recall after durable restart: PASS`. Model wording is
nondeterministic. The marker check is only a smoke signal; the deterministic tests prove the fixed
binding and restored service conversation ID.

## Cleanup

The durable host is stopped before cleanup. Because the sample creates a unique server agent name, its `finally` block deletes only the exact version it created. Cleanup failures are reported and cause a failed process exit when there was no earlier failure; they never hide the original sample failure.

## Known limitation

A caller still cannot seed a new `DurableAgentSession` from an existing, pre-durable Foundry conversation. That unsupported scenario is isolated in the [server-managed agent reproduction branch](https://github.com/microsoft/agent-framework-durable-extension/tree/tamirdresher-microsoft-server-managed-agent-repro).
