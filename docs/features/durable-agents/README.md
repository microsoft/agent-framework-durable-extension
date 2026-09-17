# Durable agents

## Overview

Durable agents extend the standard Microsoft Agent Framework with **durable execution state** powered by the Durable Task framework. Ordinary agents can already use in-memory, external-provider or service-owned history. Durable hosting persists execution and session control so that compatible workers can continue sessions across process restarts and scale-out.

| Capability | Ordinary agent | Durable agent |
| --- | --- | --- |
| Conversation history | Selected provider or model service | Selected owner, with durable-backed local history when configured |
| Failure recovery | Application-owned | Persisted orchestration and entity state; uncommitted external effects can repeat |
| Multi-instance scale-out | Application-owned coordination | Compatible workers serialize access to each entity |
| Multi-agent orchestrations | Manual coordination | Deterministic, checkpointed workflows |
| Human-in-the-loop | Must keep process alive | Can wait days/weeks with zero compute |
| Hosting | Any process | Console app, Azure Functions, or any Durable Task–compatible host |

> [!NOTE]
> For a step-by-step tutorial and deployment guidance, see [Azure Functions (Durable)](https://learn.microsoft.com/agent-framework/integrations/azure-functions) on Microsoft Learn.

## How durable agents work

Durable agents are implemented on top of [Durable Entities](https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-entities) (also called "virtual actors"). Each **agent session** maps to one entity instance. Transcript ownership and response storage depend on the runtime version and selected history provider. When you send a message to a durable agent, the following happens:

1. The message is dispatched to the entity identified by an `AgentSessionId` (a composite of the agent name and a unique session key).
2. The entity loads its persisted `DurableAgentState` and session control.
3. The underlying agent obtains context from its configured history path and executes the request.
4. Entity-local changes are persisted. External provider writes and tool effects are not part of a distributed transaction.

> [!WARNING]
> The [Python prototype in PR #59](https://github.com/microsoft/agent-framework-durable-extension/pull/59) uses main's canonical schema `2.0.0` with `terminalResults` and `completionReceipts`. The unreleased private `2.0.0` layout and its in-flight runs are abandoned. Start fresh, isolated runs, with no private-prototype detection, conversion or resume path. The explicit `isolated_v2` gate still applies to every host, including samples and tests. Schema validation never activates writes or establishes deployment compatibility. Do not mix incompatible readers/writers or replay old workflow histories through the new Python engine. No .NET interoperability is claimed. See the [deployment constraints](../../../python/packages/durabletask/README.md#version-2-deployment-warning).

The architecture in [ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88) was accepted and merged on September 15, 2026 into `feature/python-durable-thread-compaction` at `7d90e8f`. PR #59 remains an integrated prototype, not a merge-as-is implementation or production drop-in. Shared-wire adoption is available on the prototype branch at `567087f8`, not in a released package. It uses one canonical wire validator, not parallel private and shared contracts. The [validation record](../../../python/samples/README.md#prototype-validation) separates current local measurements from historical prototype evidence and remaining release gaps.

The entity framework processes concurrent messages to one session one at a time. This does not serialize external effects across entities or make external providers transactional.

### Agent session identity

Every durable agent session is identified by an `AgentSessionId`, which has two components:

- **Name** – the registered name of the agent (case-insensitive).
- **Key** – a unique session key (case-sensitive), typically a GUID.

The session ID is mapped to an underlying Durable Task entity ID with a `dafx-` prefix (e.g., `dafx-joker`). This naming convention is consistent across both .NET and Python implementations.

## Architecture

### .NET

The .NET implementation consists of two NuGet packages:

| Package | Purpose |
| --- | --- |
| `Microsoft.Agents.AI.DurableTask` | Core durable agent types: `DurableAIAgent`, `AgentEntity`, `DurableAgentSession`, `AgentSessionId`, `DurableAgentsOptions`, and the state model. |
| `Microsoft.Agents.AI.Hosting.AzureFunctions` | Azure Functions hosting integration: auto-generated HTTP endpoints, MCP tool triggers, entity function triggers, and the `ConfigureDurableAgents` extension method on `FunctionsApplicationBuilder`. |

Key types:

- **`DurableAIAgent`** – A subclass of `AIAgent` used *inside orchestrations*. Obtained via `context.GetAgent("agentName")`, it routes `RunAsync` calls through the orchestration's entity APIs so that each call is checkpointed.
- **`DurableAIAgentProxy`** – A subclass of `AIAgent` used *outside orchestrations* (e.g., from HTTP triggers or console apps). It signals the entity via `DurableTaskClient` and polls for the response.
- **`AgentEntity`** – The `TaskEntity<DurableAgentState>` that hosts the real agent. It loads the registered `AIAgent` by name, wraps it in an `EntityAgentWrapper`, feeds it the full conversation history, and persists the result.
- **`DurableAgentSession`** – An `AgentSession` subclass that carries the `AgentSessionId`.
- **`DurableAgentsOptions`** – Builder for registering agents and configuring TTL.

### Python

The core Python implementation is in the `agent-framework-durabletask` package (`python/packages/durabletask`). Azure Functions hosting for Python is provided by the `agent-framework-azurefunctions` package (`python/packages/azurefunctions`).

Key types:

- **`DurableAIAgent`** – A generic proxy (`DurableAIAgent[TaskT]`) implementing `SupportsAgentRun`. Returns a `TaskT` from `run()` — either an `AgentResponse` (client context) or a `DurableAgentTask` (orchestration context, must be `yield`ed).
- **`DurableAIAgentWorker`** – Wraps a `TaskHubGrpcWorker` and registers agents as durable entities via `add_agent()`.
- **`DurableAIAgentClient`** – Wraps a `TaskHubGrpcClient` for external callers. `get_agent()` returns a `DurableAIAgent[AgentResponse]`.
- **`DurableAIAgentOrchestrationContext`** – Wraps an `OrchestrationContext` for use inside orchestrations. `get_agent()` returns a `DurableAIAgent[DurableAgentTask]`.
- **`AgentEntity`** – Platform-agnostic agent execution logic that manages state, invokes the agent, handles streaming, and calls response callbacks.

Canonical `messageId` remains the public producer ID, including repeated IDs. Version-1 `pythonHistoryIdentity` uses `pythonHistoryId` for internal occurrence identity instead of rewriting public IDs. Configured `CompactionProvider` hooks temporarily see internal IDs for summary/source links, while normal model calls, other providers and audit sinks see public IDs. Arbitrary hooks that depend on public IDs or substitute different input IDs are not guaranteed transparent behavior. Reconciliation metadata is not exposed as application message metadata.

Full raw JSON preservation is separate from Core projection. Canonical metadata and unknown properties stay at their original object locations, with explicit `extensionData` kept separate. Versioned Python extensions include `pythonIngestion` (profile `agent-framework-python.ingestion`, version 1) for exact message receipts, `pythonCoreFields` for the `core-fields` profile and `pythonContinuationEncoding` for JSON-dictionary/base64 continuation encoding. Foreign profiles remain inert for readers. A runtime relying on a profile must validate it or block dependent restoration and writes. `historyBinding` is optional opaque JSON, not a shared fixed-owner policy. JSON preservation and continuation encoding do not prove .NET or provider interoperability. See [shared JSON and Python profiles](../../../python/packages/durabletask/README.md#shared-json-and-python-runtime-profiles).

The default durable history after-run hook calls public `save_messages()` separately for request and response batches, allowing overrides to validate, transform or reject each batch. One core hook can therefore produce two saves for provenance. Completed external-primary default save hooks and completed service-owned `ChatResponse` values affirm accepted inputs even after a later failure. Opaque custom after-run hooks, interrupted saves and store-only sinks cannot establish primary acceptance by inference. Existing core and provider APIs are unchanged. See [Python history integration](../../../python/packages/durabletask/README.md#history-and-retention-settings) for the contract and branch-isolation limits.

`terminalResults` stores full immutable response envelopes independently of history. Results and `completionReceipts` require authoritative `outcome` (`succeeded` or `failed`) and `completedAt`, with matching correlation identities and optional `resultExpiresAt`. A failed result requires a canonical error as well as its inline response. Receipt `resultState` is required. `available` requires a matching result, while `unavailable` forbids one and requires `resultUnavailableAt`. Expired lookup retains the outcome without delivering the payload or reopening work, even before physical cleanup. There is no `unknown` v2 outcome. Results and receipts must commit atomically with the operation's other entity-local state. See the [delivery contract](../../../python/packages/durabletask/README.md#service-ownership-delivery-and-reset).

Legacy reads are limited to exact `1.0.0`, `1.1.0` and `1.2.0` snapshots and remain read-only. Unsupported future versions are rejected. Both Python hosts implement privileged backend `migrate` with source-bound completion evidence. The request requires `source`, `sourceDigest`, `sourceSessionId`, `destinationSessionId`, `migrationId` and `ownershipTransferId`. Only `deliveryEvidence`, `completionEvidence` and the deprecated boolean `requireKnownOutcomes` are optional keys. Migration requires an empty, separately addressed destination, a quiesced old owner and authorized ownership transfer. The source digest is `state_snapshot_digest(source)` for the unmodified export, and the destination ID must match the receiving entity and differ from the source ID.

`completionEvidence` has exactly `sourceDigest`, a stable nonblank `evidenceId`, `complete: true` and `results`. The matching digest binds the journal to the source. Results are original canonical `terminalResults` objects without `resultExpiresAt`, even null, with `correlationId`, a known `outcome`, authoritative original `completedAt` and full inline `response.messages`. Optional `value`, metadata and unknown JSON are preserved independently of Core projection. Failures require canonical errors, and success forbids `error`. The journal must cover every completion, including results lost from history. Any nonempty history (even request-only), nonempty session-only state, a `truncation` field, nonempty scalar `ingestedPositions` or a supplied nonempty accepted-input journal requires completion evidence. A used source with no completed requests needs an explicit complete source-bound journal with `results: []`. That assertion is rejected if any response or `errorResponse` remains. No retained responses alone does not prove no completions. Only a fresh source with none of these signs of use can omit completion evidence.

`deliveryEvidence` requires matching `sourceDigest`, a stable nonblank `evidenceId`, `complete: true` and `messages`. Its only optional field is `messagePositions`, an array aligned one-for-one with `messages`. Messages remain complete, canonical, losslessly round-trippable `Message.to_dict()` accepted inputs, including `message_id`, all accepted revisions and evicted inputs.

Nonempty scalar `ingestedPositions` requires both this journal and the sidecar. Each sidecar member is `null` for no cursor participation or an object with exactly `producer` (nonblank string) and `position` (nonnegative integer, never a boolean), recording independently authoritative accepted-input cursor attribution. Producer maxima from explicit attributions alone must match the legacy map. This checks consistency, not a delivered prefix. Omit the sidecar only with an empty or absent scalar map, treating every input as unpositioned. Never derive attribution from public message IDs, guessed producer names or a retained partial transcript.

Every retained nonblank public request message ID must occur in a provided complete journal, regardless of `wf_` spelling. Internal reconciliation IDs do not substitute. Without an input journal, retained-ID-only compatibility markers apply equally to opaque and `wf_`-looking public IDs. These markers are not exact fingerprint evidence and cannot reconstruct evicted inputs. Journal authority and completeness are privileged operator assertions, not independently proved by `sourceDigest`. The accepted-input and completion journals remain independent.

Neither partial transcript projections nor source `createdAt` supply missing original responses, outcomes or completion times. Missing, duplicate or contradictory evidence blocks migration, and `requireKnownOutcomes=False` cannot waive the requirement. The migrator preserves original journal `completedAt` strings and sets matching result and receipt `resultExpiresAt` to migration time plus configured `response_delivery_window_seconds`, never before completion. Exact request retries return the recorded migration without rewriting state, even after cold reload or later runs, so grace is not refreshed. Migration does not copy external history or move workflow histories. No generated HTTP/MCP migration endpoint is provided. See [migration requirements](../../../python/packages/durabletask/README.md#explicit-legacy-migration).

Python retention defaults remain nondeleting with `keep_all` and `max_state_bytes=None`. Eager compaction pruning and pressure eviction are independent opt-ins. External/service-owned history needs no local mirror, and an invalid service conversation must not silently fall back to a new conversation or local history. External appends and uncommitted tool effects can repeat, so application-level idempotency is still required. Entity TTL/deletion also removes receipts and needs an explicit late-duplicate policy.

## Hosting models

### Azure Functions

For Azure Functions hosting, a single call to `ConfigureDurableAgents` (C#) or `AgentFunctionApp` (Python) automatically:

- Registers agent entities with the Durable Task worker.
- Generates HTTP endpoints at `/api/agents/{agentName}/run` for each registered agent.
- Supports the `session_id` query parameter / JSON field and the `x-ms-session-id` response header for session continuity.
- Supports fire-and-forget via the `x-ms-wait-for-response: false` header (returns HTTP 202).
- Optionally exposes agents as MCP tools.

> [!NOTE]
> Preview versions of this extension used `thread_id` instead of `session_id`. The `session_id` parameter is now preferred over the deprecated `thread_id`, and responses only ever return `session_id` / `x-ms-session-id`. If both names are provided they must carry the same value; conflicting values are rejected with HTTP 400.

**C# example:**

```csharp
using IHost app = FunctionsApplication
    .CreateBuilder(args)
    .ConfigureFunctionsWebApplication()
    .ConfigureDurableAgents(options => options.AddAIAgent(agent))
    .Build();
app.Run();
```

To host durable workflows alongside agents, add a `ConfigureDurableWorkflows` call. The two methods compose, in any order and any number of times:

```csharp
using IHost app = FunctionsApplication
    .CreateBuilder(args)
    .ConfigureFunctionsWebApplication()
    .ConfigureDurableAgents(agents => agents.AddAIAgent(agent))
    .ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(workflow))
    .Build();
app.Run();
```

Alternatively, `ConfigureDurableOptions` configures both from a single delegate and can be freely mixed with the methods above.

**Python example:**

Configure a separate Functions task hub/deployment with compatible version-2 workers and clients first. The explicit deployment mode below is the operator's acknowledgement of that verified setup, not automatic isolation.

```python
app = AgentFunctionApp(agents=[agent], deployment_mode="isolated_v2")
```

### Console apps / generic hosts

For self-hosted or non-serverless scenarios, register durable agents via `IServiceCollection.ConfigureDurableAgents` (.NET) or `DurableAIAgentWorker` (Python) with explicit Durable Task worker and client configuration.

**C# example:**

```csharp
IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.ConfigureDurableAgents(
            options => options.AddAIAgent(agent),
            workerBuilder: b => b.UseDurableTaskScheduler(connectionString),
            clientBuilder: b => b.UseDurableTaskScheduler(connectionString));
    })
    .Build();
```

**Python example:**

Configure the endpoint and hub as a separate version-2 deployment with compatible workers and clients before acknowledging the deployment mode. A localhost address alone does not isolate the prototype.

```python
worker = DurableAIAgentWorker(TaskHubGrpcWorker(host_address="localhost:4001"), deployment_mode="isolated_v2")
worker.add_agent(agent)
worker.start()
```

## Deterministic multi-agent orchestrations

Durable agents can be composed into deterministic, checkpointed workflows using Durable Task orchestrations. The orchestration framework replays orchestrator code on failure, so completed agent calls are not re-executed.

### Patterns

| Pattern | Description |
| --- | --- |
| **Sequential (chaining)** | Call agents one after another, passing outputs forward. |
| **Parallel (fan-out/fan-in)** | Run multiple agents concurrently and aggregate results. |
| **Conditional** | Branch orchestration logic based on structured agent output. |
| **Human-in-the-loop** | Pause for external events (approvals, feedback) with optional timeouts. |

### Using agents in orchestrations

Inside an orchestration function, obtain a `DurableAIAgent` via the orchestration context. Each agent gets its own session (created with `CreateSessionAsync` / `create_session`), and you can call the same agent multiple times on the same session to maintain conversation context across sequential invocations.

**C#:**

```csharp
static async Task<string> WritingOrchestration(TaskOrchestrationContext context)
{
    // Get a durable agent reference — works in any host (console app, Azure Functions, etc.)
    DurableAIAgent writer = context.GetAgent("WriterAgent");

    // Create a session to maintain conversation context across multiple calls
    AgentSession session = await writer.CreateSessionAsync();

    // First call: generate an initial draft
    AgentResponse<TextResponse> draft = await writer.RunAsync<TextResponse>(
        message: "Write a concise inspirational sentence about learning.",
        session: session);

    // Second call: refine the draft — the agent sees the full conversation history
    AgentResponse<TextResponse> refined = await writer.RunAsync<TextResponse>(
        message: $"Improve this further while keeping it under 25 words: {draft.Result.Text}",
        session: session);

    return refined.Result.Text;
}
```

**Python:**

```python
def writing_orchestration(context, _):
    agent_ctx = DurableAIAgentOrchestrationContext(context)

    # Get a durable agent reference — works in any host (standalone worker, Azure Functions, etc.)
    writer = agent_ctx.get_agent("WriterAgent")

    # Create a session to maintain conversation context across multiple calls
    session = writer.create_session()

    # First call: generate an initial draft
    draft = yield writer.run(
        messages="Write a concise inspirational sentence about learning.",
        session=session,
    )

    # Second call: refine the draft — the agent sees the full conversation history
    refined = yield writer.run(
        messages=f"Improve this further while keeping it under 25 words: {draft.text}",
        session=session,
    )

    return refined.text
```

> [!IMPORTANT]
> In .NET, `DurableAIAgent.RunAsync<T>` deliberately avoids `ConfigureAwait(false)` because the Durable Task Framework uses a custom synchronization context — all continuations must run on the orchestration thread.

## Streaming and response callbacks

Durable agents do not support true end-to-end streaming because entity operations are request/response. However, **reliable streaming** is supported via response callbacks:

- **`IAgentResponseHandler`** (.NET) or **`AgentResponseCallbackProtocol`** (Python) – Implement this interface to receive streaming updates as the underlying agent generates them (e.g., push tokens to a Redis Stream for client consumption).
- The entity still returns the complete `AgentResponse` after the stream is fully consumed.
- Clients can reconnect and resume reading from a cursor-based stream (e.g., Redis Streams) without losing messages.

In Python, callbacks report execution progress, not confirmed persistence. A final callback or host write return is not confirmation that the terminal result and receipt committed. Callback delivery does not make external effects exactly once.

See the **Reliable Streaming** samples for a complete implementation using Redis Streams.

## Session TTL (Time-To-Live)

Durable agent sessions support automatic cleanup via configurable TTL. See [Session TTL](durable-agents-ttl.md) for details on configuration, behavior, and best practices.

## Observability

When using the [Durable Task Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler) as the durable backend, you get built-in observability through its dashboard:

- **Conversation history** – Inspect the history retained in durable state. External/service-owned history and evicted transcript content are not a complete local mirror.
- **Orchestration visualization** – See multi-agent execution flows, including parallel branches and conditional logic.
- **Performance metrics** – Monitor agent response times, token usage, and orchestration duration.
- **Debugging** – Trace tool invocations and external event handling.

## Samples

- **.NET** – [Console app samples](../../../dotnet/samples/DurableAgents/ConsoleApps/) and [Azure Functions samples](../../../dotnet/samples/DurableAgents/AzureFunctions/) covering single-agent, chaining, concurrency, conditionals, human-in-the-loop, long-running tools, MCP tool exposure, and reliable streaming.
- **Python** – [Durable Task samples](../../../python/samples/) covering single-agent, multi-agent, streaming, chaining, concurrency, conditionals, and human-in-the-loop.

## Packages

| Language | Package | Source |
| --- | --- | --- |
| .NET | `Microsoft.Agents.AI.DurableTask` | [`dotnet/src/Microsoft.Agents.AI.DurableTask`](../../../dotnet/src/Microsoft.Agents.AI.DurableTask) |
| .NET | `Microsoft.Agents.AI.Hosting.AzureFunctions` | [`dotnet/src/Microsoft.Agents.AI.Hosting.AzureFunctions`](../../../dotnet/src/Microsoft.Agents.AI.Hosting.AzureFunctions) |
| Python | `agent-framework-durabletask` | [`python/packages/durabletask`](../../../python/packages/durabletask) |

## Further reading

- [Azure Functions (Durable) — Microsoft Learn](https://learn.microsoft.com/agent-framework/integrations/azure-functions)
- [Durable Task Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
- [Durable Entities](https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-entities)
- [Session TTL](durable-agents-ttl.md)
