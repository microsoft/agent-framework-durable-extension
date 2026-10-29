# 11 — Durable GitHub Copilot Agent

This sample demonstrates capabilities that **only** `Microsoft.Agents.AI.DurableTask` adds on top of Microsoft Agent Framework's `GitHubCopilotAgent` (`Microsoft.Agents.AI.GitHub.Copilot` / `GitHub.Copilot.SDK`):

1. **Scenario 1 — Concurrent Webhook / Alert Bursts Serialized by the Durable Agent Mailbox (`Task.WhenAll` on the same `AgentSession`):**
   - In raw `GitHub.Copilot.SDK`, calling `SendAsync` concurrently on the same session is unsupported and requires custom distributed locks (such as Redis).
   - With `ConfigureDurableAgents`, firing `Task.WhenAll(durableAgent.RunAsync(alert1, session), durableAgent.RunAsync(alert2, session))` at the same `DurableAgentSession` automatically queues and executes each turn strictly one at a time inside the Durable Entity mailbox (`@dafx-v2-copilotagent@{key}`) on the same underlying `GitHubCopilotAgentSession.SessionId`.
2. **Scenario 2 — Long-Running HITL Orchestration with SLA Timer (`Task.WhenAny(WaitForExternalEvent, CreateTimer)`) + Full Worker Shutdown While Paused:**
   - Starts `RunIncidentOrchestrationAsync` on **Host 1**, runs Turn 1 on `context.GetAgent("CopilotAgent")` to propose a remediation plan, and pauses waiting for `"Approval"` bounded by a durable SLA timer (`context.CreateTimer`).
   - **Stops and disposes Host 1 completely** while the orchestration is waiting—holding zero processes, sockets, or memory open during human review.
   - Starts **Host 2**, raises the `"Approval"` event, replays the orchestration, cancels the SLA timer, rehydrates `@dafx-v2-copilotagent@{key}`, and resumes the **exact same `GitHubCopilotAgentSession.SessionId`** for Turn 2.
3. **Scenario 3 — Durable Workflows (`ConfigureDurableWorkflows` / `WorkflowBuilder`):**
   - Executes `GitHubCopilotAgent` as a stateful node inside a Durable Workflow superstep graph.

---

## Why Use Durable Task with `GitHubCopilotAgent`?

 While `GitHub.Copilot.SDK` already persists its own session history under `COPILOT_HOME/session-state/{sessionId}` and supports `ResumeSessionAsync(sessionId)`, wrapping `GitHubCopilotAgent` in Durable Task provides distributed systems guarantees that the raw SDK does not:

1. **Per-Session Actor Serialization & Mailbox Queuing:** Durable Entities (`@dafx-v2-copilotagent@{key}`) serialize concurrent requests to the same session one turn at a time without requiring custom Redis distributed locks, and retain idempotent completion receipts (`TerminalResults` / `CompletionReceipts`) indexed by `CorrelationId`.
2. **Long-Running Human-in-the-Loop (`WaitForExternalEvent`) & Durable Timers:** Orchestrations can ask `GitHubCopilotAgent` to diagnose an incident or propose a plan on Turn 1, dehydrate to storage for hours or days waiting for an operator approval event at **zero compute cost**, and then resume the **exact same `GitHubCopilotAgentSession`** on Turn 2.
3. **Crash Recovery & Automatic Retries for Background / Webhook Workloads:** If a worker crashes mid-orchestration or between turns, Durable Task automatically replays the orchestration and resumes the persisted `GitHubCopilotAgentSession.SessionId` on a healthy worker.
4. **Multi-Agent & Workflow Composition:** `GitHubCopilotAgent` can participate in `WorkflowBuilder` graphs alongside `ChatClientAgent` (Azure OpenAI / Anthropic / Ollama) and `FoundryAgent`, with conditional routing, fan-out/fan-in supersteps, and request ports.
5. **Centralized Observability in the Durable Task Scheduler (DTS) Dashboard:** Every turn, orchestration step, custom status, and entity state transition is inspectable in the DTS web UI.

---

## Required Configuration & Known Limitations

### 1. Configure `ReplayMode = CurrentRequestOnly` and `ProviderKey` (Required)

`GitHubCopilotAgent` derives directly from `AIAgent` (not `ChatClientAgent`), so `DurableAgentHistoryOwnershipResolver` classifies it as `NoContextPipeline`.

- If you leave the default `ReplayMode = DurableAgentHistoryReplayMode.PreloadEntityHistory`, `AgentEntity` prepends all prior turns from `ConversationHistory` into `inputMessages`, **and** `GitHubCopilotAgent` simultaneously calls `ResumeSessionAsync(typedSession.SessionId)` and joins all `inputMessages` via `string.Join("\n", messages.Select(m => m.Text))`. On Turn 2+, that re-sends the entire prior conversation transcript inside the new user prompt to an already-resumed Copilot session.
- Always configure `DurableAgentHistoryReplayMode.CurrentRequestOnly` together with an explicit `DurableAgentHistoryProviderKey`:

```csharp
options.AddAIAgent(
    copilotAgent,
    timeToLive: TimeSpan.FromHours(1),
    configureHistory: history =>
    {
        history.ReplayMode = DurableAgentHistoryReplayMode.CurrentRequestOnly;
        history.ProviderKey = new DurableAgentHistoryProviderKey("github-copilot-session.v1");
    });
```

With `CurrentRequestOnly` + `ProviderKey`:
- `AgentEntity` passes only the current turn's `request.Messages` to `GitHubCopilotAgent`.
- `GitHubCopilotAgentSession.SessionId` is persisted in `workingState.Data.Session` (`{"sessionId":"<guid>"}`) and automatically passed to `CopilotClient.ResumeSessionAsync` on Turn 2+.
- `workingState.Data.ConversationHistory` stays empty, avoiding duplicate transcript storage.

### 2. Use an Alphanumeric Agent `Name` (No Spaces or Hyphens) and `id: null`

- `GitHubCopilotAgent`'s default name is `"GitHub Copilot Agent"` (with spaces).
- When an `AIAgent` is added to a `WorkflowBuilder`, `AIAgentExtensions.GetDescriptiveId()` sanitizes any non-alphanumeric characters (`[^0-9A-Za-z]+`) to `_`, turning `"GitHub Copilot Agent"` into `"GitHub_Copilot_Agent_<32hex>"`. At runtime, `WorkflowNamingHelper.GetExecutorName(...)` strips the `_<32hex>` suffix and looks up `"GitHub_Copilot_Agent"`, which does not match `"GitHub Copilot Agent"`.
- Similarly, `WorkflowNamingHelper.GetExecutorName` expects the suffix after the last `_` to be a 32-character hex GUID (`AIAgent`'s default `Id` when `id: null`).
- **Best practice:** Always pass a single-word alphanumeric `name` (such as `"CopilotAgent"`) and leave `id: null`:

```csharp
AIAgent copilotAgent = copilotClient.AsAIAgent(
    sessionConfig: sessionConfig,
    ownsClient: false,
    id: null,
    name: "CopilotAgent",
    description: "Durable GitHub Copilot agent");
```

### 3. Multi-Worker / Multi-Machine Deployments

Because `GitHubCopilotAgent` resumes sessions via `CopilotClient.ResumeSessionAsync(sessionId)`, any worker that executes Turn 2+ of a session must have access to that Copilot session's state:

- **Option A (Shared Copilot Runtime Endpoint):** Set `COPILOT_RUNTIME_URL` (`CopilotClientOptions.Connection = RuntimeConnection.ForUri(...)`) so all workers connect to a shared Copilot runtime service.
- **Option B (Shared Filesystem Mount):** Set `COPILOT_BASE_DIRECTORY` (`CopilotClientOptions.BaseDirectory`) to a shared volume mounted across workers so `COPILOT_HOME/session-state/{sessionId}` is accessible from any replica.
- *Note:* In `Microsoft.Agents.AI.GitHub.Copilot` `1.23.0`, `GitHubCopilotAgent.CopyResumeSessionConfig` does not yet copy `SessionConfig.CreateSessionFsProvider` onto `ResumeSessionConfig`, so custom `ISessionFsHandler` providers are not preserved across `ResumeSessionAsync`.

### 4. Per-Request `DurableAgentRunOptions` Are Ignored by `GitHubCopilotAgent`

`EntityAgentWrapper` applies per-request `DurableAgentRunOptions` (`EnableToolCalls`, `EnableToolNames`, and `ResponseFormat` / `RunAsync<T>`) via `ChatClientAgentRunOptions.ChatClientFactory`. Because `GitHubCopilotAgent` does not use `IChatClient` and ignores the `AgentRunOptions` parameter in `RunCoreStreamingAsync`, configure tools and instructions on `SessionConfig` when constructing `GitHubCopilotAgent`.

---

## Running the Sample

### Prerequisites

1. [.NET 10.0 SDK](https://dotnet.microsoft.com/download/dotnet)
2. [Durable Task Scheduler Emulator](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/develop-with-durable-task-scheduler):
   ```bash
   docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
   ```
3. Either an installed and authenticated `copilot` CLI on `PATH`, or `COPILOT_RUNTIME_URL` pointing to a running Copilot runtime server.

### Run the Console App

```bash
cd dotnet/samples/DurableAgents/ConsoleApps/11_GitHubCopilotAgent
dotnet run --framework net10.0
```

### Run the Automated Tests (No External CLI or Emulator Required)

The `tests/` directory includes deterministic xUnit tests backed by an in-process loopback TCP JSON-RPC server (`FakeCopilotJsonRpcServer`):

```bash
dotnet test dotnet/samples/DurableAgents/ConsoleApps/11_GitHubCopilotAgent/tests/11_GitHubCopilotAgent.Tests.csproj
```
