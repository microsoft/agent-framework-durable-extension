# Result-expiry backend atomicity merge and release gate

`ResultExpiryAtomicityTests.StateAndOutboxSurviveRestartAndRollbackFailedCleanupAsync` uses
`ConfigureDurableAgents`, the actual Durable Task Scheduler client and worker, and the production
`AgentEntity`. Its deterministic local `AIAgent` needs no LLM, Foundry or cloud credentials.

**Not run by default.** Missing explicit opt-in or endpoint produces an xUnit **skip**, not a passing
early return. This repository's ordinary integration helpers may fall back to shared localhost:8080;
this test never does. It does not invoke Docker, provision resources, discover credentials, or stop
other workers/containers. Only its own hosts are stopped/disposed.

Before enabling, independently arrange and verify an **existing isolated emulator** on a non-default
HTTP loopback port. Do not enable against a shared/fixed emulator or cloud service. Each invocation
generates a fresh `expiry-{32-character GUID}` task hub and entity key; only its own worker restart
reuses that hub. The test does not delete shared hubs or containers.

From `dotnet`, after a Release build, explicitly opt in for this process:

```powershell
$env:DURABLE_AGENT_EXPIRY_INTEGRATION = '1'
$env:DURABLE_AGENT_EXPIRY_EMULATOR_ENDPOINT = 'http://127.0.0.1:<your-isolated-port>'
dotnet tests\Microsoft.Agents.AI.DurableTask.IntegrationTests\bin\Release\net10.0\Microsoft.Agents.AI.DurableTask.IntegrationTests.dll --filter-class '*ResultExpiryAtomicityTests' --timeout 5m
```

The placeholder is intentional; there is no suggested shared port. Opt-in with an invalid endpoint
fails before host construction. The connection uses `Authentication=None` and the fresh test hub.
Allow about 2–3 minutes (four-minute cancellation bound): the test deliberately waits for actual
delayed backend delivery rather than treating intercepted signals as proof of an outbox commit.

## Assertions and observability

1. Two successful runs persist two mailbox results/receipts but stage exactly one delayed signal.
2. A cleanup operation stages a payload sweep, a token replacement and one successor, then a test
   decorator throws **after** the real `AgentEntity` has staged state and signals. A real orchestration
   catches the SDK's `EntityOperationFailedException` only for the injected cleanup marker
   (matching entity, operation, error type and message, without an inner failure). Unrelated failures
   propagate. Backend state must equal its pre-operation serialization.
3. The first host is stopped/disposed; a new worker/client starts against the same unique hub.
   The original delayed backend signal (not another client signal) must sweep the first result and
   commit exactly one successor.
4. Three explicitly duplicated deliveries through real entity calls must produce zero state setters
   and zero outgoing signals, with identical persisted state. Unit tests separately check **100 runs**
   and **100 duplicate deliveries**, asserting exact counts on each step.
5. The committed successor must arrive automatically and remove the remaining payload while retaining
   both receipts. A ten-second observation window beyond both successor deadlines must contain **zero**
   deliveries of the token staged by the failed operation, even if such a leaked signal would be stale.
   Expected totals: eight dispatches, two model calls, three attempted outgoing signals (one rolled
   back, two committed). The observation window is bounded evidence, not a guarantee about arbitrary
   future backend delays.

The observer forwards every state/context operation to the real runtime object; it is not a mocked
entity operation or backend. `ConfigureDurableAgents` configures the production agent services/options
and real Scheduler client. The worker is registered separately using public `AddDurableTaskWorker`,
`AddTasks`, and `DurableTaskRegistry.AddEntity(factory)` APIs. That test-only factory constructs the
actual production `AgentEntity` through the existing friend-assembly access, then wraps it in the
forwarding `ITaskEntity` observer. No private SDK reflection or entity-registry replacement is used.
The actual AgentEntity executes against real SDK operation/context/state objects and the real
worker/backend outbox; there is no mock fallback.

## Local orchestration regression

`ResultExpiryOrchestrationTests` executes the same test-orchestration handler without a backend.
Only this unit proof substitutes the entity call. It uses the actual SDK exception and JSON-reloaded
`TaskFailureDetails` from the injected exception: success returns `true`, the expected injected cleanup
failure returns `false`, and mismatched entity/operation/type/message/inner failures, ordinary task
failures and cancellation propagate. It is not gated and is separate from the actual backend Fact:

```powershell
dotnet tests\Microsoft.Agents.AI.DurableTask.IntegrationTests\bin\Release\net10.0\Microsoft.Agents.AI.DurableTask.IntegrationTests.dll --filter-class '*ResultExpiryOrchestrationTests' --timeout 2m
```

Passing this local regression proves exception handling, not backend rollback or outbox atomicity.

## Rollout restriction

On a machine without the explicit isolated-backend configuration, report this test as **NOT RUN
(gated skip)**. A compiled/skipped test and passing unit mocks do **not** establish backend atomicity.
An executed passing isolated run is mandatory before **either merge or release**. A clearly documented
gated skip is acceptable for **draft readiness only**; do not merge or release this correction until
the real-backend test passes. Independent review acceptance does not waive this requirement.
Shared schema/reader/writer/rollback agreement and late-duplicate/deletion policy remain additional
rollout gates. Schema-2 writing/deletion remain internal test-only,
default-off and inaccessible to production public options in this layer.
