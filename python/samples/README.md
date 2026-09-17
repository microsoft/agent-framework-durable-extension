# Durable Task Samples

This directory contains samples for durable agent hosting using the Durable Task Scheduler. These samples demonstrate the worker-client architecture pattern, enabling distributed agent execution with persistent conversation state.

## PR #59 prototype scope

The architecture in [ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88) was accepted and merged on September 15, 2026 into `feature/python-durable-thread-compaction` at `7d90e8f`. [PR #59](https://github.com/microsoft/agent-framework-durable-extension/pull/59) remains the integrated prototype, not a merge-as-is implementation or production drop-in. Shared-wire adoption is implemented and locally validated, but not published. The local implementation uses main's canonical `terminalResults` and `completionReceipts` instead of the private `responseMailbox` and `completedCorrelations` layout, with one canonical wire validator rather than parallel contracts. No .NET interoperability is claimed.

The prototype's version-2 runtime requires `deployment_mode="isolated_v2"` on `DurableAIAgentWorker`,
`AgentFunctionApp` and the standalone Functions entity factory, or
`DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` when the argument is omitted/`None`. Configure the sample
host environment accordingly. This is operator acknowledgement, not proof of isolation. Use a
separate hub/deployment with compatible workers and clients. Old workers and workflow histories,
including paused legacy HITL, must stay on the old engine.
This gate applies to every prototype host, including samples and tests. A sample, localhost endpoint or passing schema validation never establishes isolation or activates version-2 writes.

Only canonical `2.0.0` entity state is writable. Exact legacy `1.0.0`, `1.1.0` and `1.2.0` snapshots
remain read-only. Unsupported versions, including future `2.x`, are rejected. The unreleased private
`2.0.0` state and in-flight runs are abandoned. Start fresh, isolated runs, with no private-prototype
detection, conversion or resume path. Names are unchanged, so reusing an old `@name@key` on an empty
new hub is not migration or permission to redeliver old work.

Explicit legacy migration needs an empty, separately addressed destination, a quiesced old owner
and authorized ownership transfer. Both hosts implement backend `migrate` with required request keys
`source`, `sourceDigest`, `sourceSessionId`, `destinationSessionId`, `migrationId` and
`ownershipTransferId`. Only `deliveryEvidence`, `completionEvidence` and the deprecated boolean
`requireKnownOutcomes` are optional keys. `sourceDigest` is `state_snapshot_digest(source)` for the
unmodified export. The destination ID must match the receiving entity and differ from the source ID.

`completionEvidence` has exactly `sourceDigest`, a stable nonblank `evidenceId`, `complete: true`
and `results`. Its digest must match the source. Results are original canonical `terminalResults`
objects without `resultExpiresAt`, not even null. Each has `correlationId`, known `outcome`, original
authoritative `completedAt` and full inline `response.messages`. Optional `value`, metadata and
unknown JSON are preserved without a lossy Core projection. Failure requires a canonical error,
while success forbids one. The journal covers all completions, including results lost from history.

Completion evidence is required for any nonempty history (even request-only), nonempty session-only
state, a `truncation` field, nonempty scalar `ingestedPositions` or a supplied nonempty accepted-input
journal. If such a source has no completed requests, supply an explicit complete source-bound journal
with `results: []`. It is rejected if any retained response or `errorResponse` remains. No retained
responses alone does not prove no completions. Only a fresh source with none of these signs of use
can omit completion evidence.

Nonempty scalar `ingestedPositions` additionally requires complete `deliveryEvidence` with exactly
`sourceDigest`, `evidenceId`, `complete: true` and `messages`. This independent journal contains
lossless `Message.to_dict()` accepted inputs, including evicted inputs and all accepted revisions.
Exact identities preserve gaps, and matching maxima do not establish a delivered prefix. Assertions
and matching digests do not establish journal authority. Missing, duplicate or contradictory evidence
blocks migration, with no invented or `unknown` outcome or `requireKnownOutcomes=False` waiver.
Partial transcript projections and source `createdAt` cannot supply missing original results or
completion times. If the required journals are unavailable, keep the session on the old engine.

The migrator preserves original journal `completedAt` strings and sets matching result and receipt
`resultExpiresAt` to migration time plus configured `response_delivery_window_seconds`, never before
completion. Exact request retries return the recorded migration without rewriting state, including
after cold reload or later runs, so they do not refresh grace. Migration does not copy external
history or move workflow histories. No generated HTTP/MCP migration endpoint is provided. See
[migration requirements](../packages/durabletask/README.md#explicit-legacy-migration).

The workflow client, generated start routes and child dispatch wrap new starts with protocol version
2. Native custom schedulers must use public `wrap_workflow_input` for new instances. Old/raw starts
reject before revised actions execute. Rewrapping old starts is not history migration.

## Prototype validation

### Current adoption status

Canonical shared-wire adoption, including source-bound completion-evidence migration, is implemented
and locally validated as of September 16, 2026. These results cover the working tree based on
`31293f2` plus main `45b7fd8`, not a published implementation or remote CI result. The final
implementation commit will record this working tree after the documentation update. The complete
canonical schema tree equals main `45b7fd8`, and the accepted ADR is unchanged.

| Current local check | Result | Time |
| --- | --- | --- |
| Python 3.13 / core 1.16 | 5,724 passed, 0 failures/errors/skips | 135.712 s |
| Python 3.13 / real cached core 1.13 | 5,704 passed, 0 failures/errors/skips | 130.802 s |
| Python 3.10 / core 1.16 | 5,724 passed, 0 failures/errors/skips | 144.628 s |
| Full DTS integration suite | 45 passed | 368.370 s |
| Full Functions / Azure Storage integration suite | 45 passed | 908.867 s |

The 20-case core-version difference comes from conditional middleware forms unavailable on core
1.13, not skipped tests. Shared structural coverage includes 96 cases and four fixtures with full
raw JSON round-trips, already included in the unit totals.

Both source Pyright checks reported zero errors. Linux MyPy checked 89 Durable Task and 40 Azure
Functions files with zero errors. Ruff reported zero findings, formatting passed for 174 files,
the 148-package offline lock check passed, and both packages' wheels and source distributions built
successfully. Markdown and local-link checks passed with no new Markdown lint findings before this
documentation-only update.

Host validation used local DTS and Azure Functions with Azure Storage through Azurite. It covers
real Foundry text scenarios and deterministic binary-media/model-boundary scenarios, not hosted-model
media acceptance. It does not establish compiled .NET interoperability, actual scheduler-limit or
offload behavior, or a resume path for old private `2.0.0` runs. Exact Pydantic 2.11 remains unverified
because its earlier artifact download failed with a TLS handshake error. Local validation does not
make PR #59 mergeable as-is or establish production readiness. The historical records below remain
separate from these current results.

### Historical post-acceptance record

The following local results belong to historical published baseline `31293f2`, recorded when
shared-wire adoption began on September 16, 2026. They are not current-revision or green CI results.
CI at that head was skipped because of a merge conflict.

| Check | Result |
| --- | --- |
| Python 3.13 / core 1.16 | 4,116 passed, zero skipped |
| Python 3.13 / real cached core 1.13 | 4,096 passed, zero skipped |
| Python 3.10 / core 1.16 | 4,116 passed, zero skipped |
| Direct DTS integration | 45 passed |
| Functions / Azure Storage integration | 45 passed |
| Package lint, 165-file format check, both source analyzers and Linux test typing | Passed |
| Offline lock, wheel and source distributions for both packages | Passed |

The earlier record attributes the 20-case difference on core 1.13 to middleware singleton/bundle
forms that the older core does not expose. Its shared list-form cases ran on both versions. That coverage included
literal workflow dictionaries, explicit null routing, public history IDs, acknowledged-input
receipts, nested opaque input metadata, middleware composition, cold mailbox reads, conservative
legacy outcome enrichment, MCP timeouts and sample entrypoints. Real host tests retain the limits
described below, rather than proving hosted-model media support or cross-runtime interoperability.

The Functions launch environment explicitly selected the isolated test interpreter. An initial
attempt without that child-interpreter configuration could not start the sample hosts. That was
corrected in the local test launcher without changing product behavior. Exact Pydantic 2.11 remains
unverified because its isolated artifact download failed with a TLS handshake error.

### Historical outcome and retention validation

The results below are the historical local outcome, media, failure-boundary and telemetry follow-up to
[prototype baseline 9b4550d](https://github.com/microsoft/agent-framework-durable-extension/commit/9b4550d), retained in this record on September 16, 2026, not the post-acceptance follow-up above.
These pinned counts are not current remote CI status or a claim of release readiness. See
[PR #59 checks](https://github.com/microsoft/agent-framework-durable-extension/pull/59/checks)
for remote results.

| Local check | Result |
| --- | --- |
| Python 3.13 / core 1.16 | 3,427 passed, zero skipped |
| Python 3.13 / real cached core 1.13 | 3,427 passed, zero skipped |
| Python 3.10 / core 1.16 | 3,427 passed, zero skipped |
| Media retention units | 30 passed, six content kinds across four policies plus six protected-floor cases |
| Cancellation/failure units and Functions consumers | 11 + 3 passed |
| Retention OTel units | 20 passed |
| Completion-outcome units | 52 passed, including formatted and unformatted acceptance-only regressions |
| Existing consumer parameterizations | Eight additional cases passed |
| Direct DTS integration suite | 45 passed in 354.65 seconds, prior 42 plus three new cases |
| Azure Functions integration suite | 45 passed in 649.44 seconds, prior 43 plus two media cases |
| Ruff lint/format, Pyright, MyPy, offline lock and both package builds | Passed |

The focused unit counts are subsets of each 3,427-test run, not additional tests. Media cases cover
inline PNG, inline text files, image URIs, hosted files, mixed binary/text tool results and large
tool payloads. They check all retention/budget combinations, JSON cold reload, exact subsequent
model input, atomic tool groups, protected floors and staged deletion measurements. Failure cases
cover cancellation at provider/model/retention boundaries, warm rollback, lost write acknowledgement
and provider failure combined with rejected error persistence and bounded polling. Caller polling
cancellation does not cancel the entity. Outcome tests cover retained success/failure, unknown
legacy receipts, strict migration and rejection of fresh acceptance-only completion records.

The three new direct tests use real DTS persistence and process restarts with a deterministic
`BaseChatClient`, not Foundry. Two exercise PNG and inline-file pressure with persisted-state
readback, exact next model input and matching truncation/OTel counts. The third hard-kills a worker
before commit, observes repeated simulated external effects on retry, then kills after confirmed
scheduler readback and verifies duplicate suppression. These are not live graceful execution
cancellation tests. The two new Functions cases use the production entity handler and actual
`DurableEntityContext` with Azure Storage via Azurite. They verify PNG/inline-file pressure,
persisted JSON, a restarted host, exact subsequent model input and staged OTel measurements.
Inline media bytes dominate the live pressure cases, rather than text padding alone. The existing
42 direct tests and 43 Functions tests remain text-based and include Foundry-backed scenarios.

The Functions rerun required the local test Azurite setting `--skipApiVersionCheck`. The initial
36 failures and seven passes were caused by unsupported Storage API `2026-02-06`, not product
changes. The corrected final run passed all 45 tests. Coverage percentage was not remeasured here.

Mutation checks reject disabled pressure/eager pruning, lost content metadata, missing rollback,
missing binding cleanup and missing telemetry. Live DTS cases fail when pressure is disabled.
Functions cases fail on actual stored byte size when the configured budget is deliberately inflated.
Restored runs pass. Mutation changes stayed in fresh process memory or generated temporary test apps.

Graceful-shutdown-specific host behavior and hosted-model media acceptance are not established by
these tests. Remaining release validation includes actual scheduler-limit/offload behavior as those
capabilities are enabled, exact Pydantic 2.11 runtime validation (artifact downloads
remain blocked), and shared reader/writer, client, replay and rollback compatibility. The reduced
live budget is not a scheduler-limit test. No compiled C# or cross-runtime schema acceptance is
claimed. Shared-wire adoption does not establish .NET compatibility or allow legacy workflow
history replay. These gaps do not replace or defer the ADR's required validation.

## Import convention

These samples import the durable hosting types **directly from the extension packages**,
`agent_framework_durabletask` and `agent_framework_azurefunctions`:

```python
from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient
from agent_framework_azurefunctions import AgentFunctionApp
```

For backward compatibility these entry-point types are also re-exported from
`agent_framework.azure` in the core `agent-framework` package, so existing
`from agent_framework.azure import ...` code keeps working. **New and updated samples should use
the direct package imports shown above**, the self-contained path for this repo,
rather than routing through the `agent_framework.azure` shim.

## Quick Prerequisites Checklist

Install and verify these tools before [Running the Samples](#running-the-samples):

- **[Docker](https://docs.docker.com/get-docker/)** – run the Durable Task Scheduler emulator locally
- **[uv](https://docs.astral.sh/uv/)** – manage Python dependencies (optional but recommended)
- **[Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)** – authenticate with `az login` for `AzureCliCredential`

**Windows (PowerShell):**

```powershell
winget install Docker.DockerDesktop
irm https://astral.sh/uv/install.ps1 | iex
winget install Microsoft.AzureCLI
```

**macOS / Linux:**

```bash
# Docker: https://docs.docker.com/get-docker/
curl -LsSf https://astral.sh/uv/install.sh | sh
# Azure CLI: https://learn.microsoft.com/cli/azure/install-azure-cli
```

**Verify:**

```bash
docker --version
uv --version
az account show
```

## Sample Catalog

### Basic Patterns
- **[01_single_agent](01_single_agent/)**: Host a single conversational agent and interact with it via a client. Demonstrates basic worker-client architecture and agent state management.
- **[02_multi_agent](02_multi_agent/)**: Host multiple domain-specific agents (physicist and chemist) and route requests to the appropriate agent based on the question topic.
- **[03_single_agent_streaming](03_single_agent_streaming/)**: Enable reliable, resumable streaming using Redis Streams with agent response callbacks. Demonstrates non-blocking agent execution and cursor-based resumption for disconnected clients.

### Orchestration Patterns
- **[04_single_agent_orchestration_chaining](04_single_agent_orchestration_chaining/)**: Chain multiple invocations of the same agent using durable orchestration, preserving conversation context across sequential runs.
- **[05_multi_agent_orchestration_concurrency](05_multi_agent_orchestration_concurrency/)**: Run multiple agents concurrently within an orchestration, aggregating their responses in parallel.
- **[06_multi_agent_orchestration_conditionals](06_multi_agent_orchestration_conditionals/)**: Implement conditional branching in orchestrations with spam detection and email assistant agents. Demonstrates structured outputs with Pydantic models and activity functions for side effects.
- **[07_single_agent_orchestration_hitl](07_single_agent_orchestration_hitl/)**: Human-in-the-loop pattern with external event handling, timeouts, and iterative refinement based on human feedback. Shows long-running workflows with external interactions.

### Workflow Hosting Patterns
- **[08_workflow](08_workflow/)**: Host a MAF `Workflow` as a durable orchestration on a standalone worker via `DurableAIAgentWorker.configure_workflow`. Demonstrates conditional routing and mixing AI agents with non-agent executors.
- **[09_workflow_hitl](09_workflow_hitl/)**: A workflow that pauses for human approval using `ctx.request_info` / `@response_handler`, with the client discovering and answering the pending request.
- **[10_workflow_streaming](10_workflow_streaming/)**: Stream a hosted workflow's events as typed `WorkflowEvent` objects by polling the orchestration's custom status.
- **[11_subworkflow](11_subworkflow/)**: Compose workflows by embedding an inner `Workflow` as a node via `WorkflowExecutor`. On the durable host the inner workflow runs as its own child orchestration, and a single `configure_workflow` call registers both.
- **[12_subworkflow_hitl](12_subworkflow_hitl/)**: A human-in-the-loop pause that lives **inside a sub-workflow**. The nested request surfaces to the client with a qualified request id (`{executor}~{ordinal}~{requestId}`) behind a single top-level addressing surface.

These workflow samples and their Azure Functions counterparts explicitly set
`WorkflowBuilder(output_from=[...])` to the executors that produce their final results.
For composed workflows, the inner workflow selects the result forwarded to its parent,
and the outer workflow selects its final report or publication message. Agent responses
still travel along the graph edges but are not additional results in these samples.
For a workflow intended to return agent responses, include those agents in `output_from`
or use `output_from="all"`.

### Conversation History

History providers own transcript writes according to their storage flags. External and
service-managed history do not get a local transcript mirror. The entity keeps response delivery
payloads and completion receipts separately from model history. Retention defaults to `keep_all`
with `max_state_bytes=None`. Eager pruning and pressure eviction are separate opt-ins, not a promise
of unlimited capacity.

Only exact built-in in-memory providers are substituted. Subclasses retain custom hooks and session
transcripts in the protected floor, outside durable transcript eviction. Service-owned runs
intentionally suppress both load and store hooks on the inactive primary, including per-call hooks.
That differs from core 1.16 behavior. Use a distinct store-only sink to audit both service/client
branches. Do not assume universal unchanged-hook semantics or retry-safe external effects.

The default `DurableHistoryProvider.after_run()` calls public `save_messages()` for request and response batches. Overrides can validate, transform or reject each batch, and one core hook can produce two saves for provenance. A completed save through an external primary's default hook, or a completed service-owned `ChatResponse`, preserves affirmative input acceptance after a later invocation failure. Opaque custom after-run hooks and interrupted saves do not permit inferred acknowledgement. Store-only sinks do not establish primary acceptance. Existing core and provider APIs are unchanged.

Canonical `messageId` remains the public producer ID, including repeated IDs. Version-1 `pythonHistoryIdentity` uses `pythonHistoryId` for internal occurrences without exposing reconciliation metadata as application message metadata. Public history loads, normal model calls, other providers and audits use public IDs. Configured `CompactionProvider` hooks temporarily see internal IDs and must use those IDs for internal summary/source links. Arbitrary custom hooks that depend on public IDs or substitute different input IDs are not guaranteed transparent behavior.

Full raw JSON preservation is separate from Core projection. Keep canonical metadata and unknown
fields at their original locations, separate from explicit `extensionData`, without loading types
from stored data. Versioned `pythonIngestion` uses profile `agent-framework-python.ingestion`,
version 1, for exact message receipts that preserve delivery gaps. `pythonCoreFields` uses the
`core-fields` profile, and `pythonContinuationEncoding` identifies JSON-dictionary/base64 encoding.
Foreign profiles remain inert for readers. A runtime relying on one must validate it or block
dependent restoration and writes. Optional opaque `historyBinding` imposes no shared fixed-owner
policy. Raw preservation and continuation encoding are not .NET or provider interoperability claims.
See [shared JSON and Python profiles](../packages/durabletask/README.md#shared-json-and-python-runtime-profiles).

Workflow delta transport uses parallel occurrence IDs, not public `Message.message_id` rewrites.
The full selected logical conversation and all response messages remain available for downstream
projection. Private forwarding provenance is internal checkpoint data, not application metadata.
Typed/cache-only requests, agent approval/HITL and output-designated agents are supported locally.

`terminalResults` retains immutable canonical response envelopes independently of model history.
Results and `completionReceipts` require authoritative `outcome` (`succeeded` or `failed`) and
`completedAt`, with matching correlation identities and optional `resultExpiresAt`. Results include
the full inline response and canonical metadata, with an error required for failure. Receipt
`resultState` is required. `available` requires a matching result, while `unavailable` forbids one
and requires `resultUnavailableAt`. Results and receipts must commit atomically with all other
entity-local changes for the operation. No v2 `unknown` outcome or transcript-based success inference
is permitted. Acceptance-only and fire-and-forget responses do not establish terminal completion.

When expiry is configured, delivery expires logically even while an idle entity retains its payload.
Lookup reports completed-but-result-unavailable with the retained outcome, without returning an
expired payload, reporting pending or rerunning work. Cleanup removes the result and records
`resultUnavailableAt` no earlier than completion and, for expiry, no earlier than the deadline.
New runs, duplicate runs, reset and backend `expire_responses` clean expired payloads without erasing
receipts. Idle physical cleanup needs an application-owned schedule or explicit backend signal/manual
operation. No public HTTP/MCP cleanup endpoint is generated.

Receipts can exhaust capacity. Whole-entity TTL/deletion also removes duplicate protection and needs
an explicit late-duplicate policy. Entity commits do not make external appends or tool effects
transactional or exactly once. Callbacks and host write returns are not confirmation of persisted
completion. Applications need their own external idempotency strategy. An invalid service conversation
must fail without silently falling back to a new conversation or local history.

The shared [retention telemetry](../packages/durabletask/README.md#retention-telemetry) measures staged
deletion, not committed deletion. A `set_state` return or failure leaves commit status unknown.
Pair separate persisted readback with subsequent model input. Applications own SDK/exporter setup.
`"backend_limit"` remains a non-normative Python-only Scheduler convenience, outside the portable
`None` or positive-integer budget contract and without assumed shared-review agreement.

- **[13_conversation_compaction](13_conversation_compaction/)**: Compact client-owned history with `InMemoryHistoryProvider` and `CompactionProvider`. Keep excluded history by default and choose transcript pruning or a state budget independently.
- **[14_external_history_redis](14_external_history_redis/)**: Use an ordinary Redis history provider with a stable session id and no local transcript mirror. The minimal blind-append provider documents interrupted-retry duplicates and unsupported portable reset.

### Azure Functions Hosting

These samples host workflows and agents on Azure Durable Functions (`func start`) instead of the worker-client model above. Each has its own setup steps in its README, and shared environment setup lives in [azure_functions/README.md](azure_functions/README.md).

- **[azure_functions/01_single_agent](azure_functions/01_single_agent/)**: Host a single AI agent on Azure Functions with direct HTTP API access for interactive conversations.
- **[azure_functions/02_multi_agent](azure_functions/02_multi_agent/)**: Host multiple AI agents on Azure Functions, each reachable via its own HTTP endpoint.
- **[azure_functions/03_reliable_streaming](azure_functions/03_reliable_streaming/)**: Reliable, resumable streaming for durable agents using Redis Streams with cursor-based reconnection.
- **[azure_functions/04_single_agent_orchestration_chaining](azure_functions/04_single_agent_orchestration_chaining/)**: Chain two invocations of the same agent inside a Durable Functions orchestration, preserving conversation state between runs.
- **[azure_functions/05_multi_agent_orchestration_concurrency](azure_functions/05_multi_agent_orchestration_concurrency/)**: Run two agents in parallel inside a Durable Functions orchestration and merge their responses.
- **[azure_functions/06_multi_agent_orchestration_conditionals](azure_functions/06_multi_agent_orchestration_conditionals/)**: Conditional orchestration that screens emails with a spam-detector agent and drafts replies with an email assistant agent.
- **[azure_functions/07_single_agent_orchestration_hitl](azure_functions/07_single_agent_orchestration_hitl/)**: Human-in-the-loop orchestration where a writer agent iterates until a reviewer approves or the attempt limit is reached.
- **[azure_functions/08_mcp_server](azure_functions/08_mcp_server/)**: Expose agents as both HTTP endpoints and [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) tools.
- **[azure_functions/09_workflow_shared_state](azure_functions/09_workflow_shared_state/)**: Run a MAF `Workflow` with `SharedState` on Azure Durable Functions.
- **[azure_functions/10_workflow_no_shared_state](azure_functions/10_workflow_no_shared_state/)**: Run a MAF `Workflow` on Azure Durable Functions without SharedState.
- **[azure_functions/11_workflow_parallel](azure_functions/11_workflow_parallel/)**: Parallel execution of executors and agents in an Azure Durable Functions workflow.
- **[azure_functions/12_workflow_hitl](azure_functions/12_workflow_hitl/)**: The workflow human-in-the-loop pattern on Azure Durable Functions, with the reviewer notified from inside the workflow via `WorkflowHitlContext`.
- **[azure_functions/13_subworkflow_hitl](azure_functions/13_subworkflow_hitl/)**: A human-in-the-loop pause inside a sub-workflow on Azure Durable Functions, exposed through a single top-level respond surface.
- **[azure_functions/14_conversation_compaction](azure_functions/14_conversation_compaction/)**: Compact client-owned history on Azure Functions with independent retention and explicit byte-budget options. The Functions counterpart to [13_conversation_compaction](13_conversation_compaction/).

## Running the Samples

These samples are designed to be run locally in a cloned repository.

### Prerequisites

The following prerequisites are required to run the samples:

- [Python 3.10 or later](https://www.python.org/downloads/), `agent-framework-core>=1.13.0,<2` and `pydantic>=2.11,<3`
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) installed and authenticated (`az login`)
- [Microsoft Foundry project](https://learn.microsoft.com/azure/foundry/how-to/create-projects) with a deployed model, configured through `FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL` (gpt-4o-mini or better is recommended)
- [Durable Task Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/develop-with-durable-task-scheduler) (local emulator or Azure-hosted)
- [Docker](https://docs.docker.com/get-docker/) installed if running the Durable Task Scheduler emulator locally

### Configuring RBAC Permissions for Azure OpenAI

These samples are configured to use the Azure OpenAI service with RBAC permissions to access the model. You'll need to configure the RBAC permissions for the Azure OpenAI service to allow the Python app to access the model.

Below is an example of how to configure the RBAC permissions for the Azure OpenAI service to allow the current user to access the model.

Bash (Linux/macOS/WSL):

```bash
az role assignment create \
  --assignee "yourname@contoso.com" \
  --role "Cognitive Services OpenAI User" \
  --scope /subscriptions/<your-subscription-id>/resourceGroups/<your-resource-group-name>/providers/Microsoft.CognitiveServices/accounts/<your-openai-resource-name>
```

PowerShell:

```powershell
az role assignment create `
  --assignee "yourname@contoso.com" `
  --role "Cognitive Services OpenAI User" `
  --scope /subscriptions/<your-subscription-id>/resourceGroups/<your-resource-group-name>/providers/Microsoft.CognitiveServices/accounts/<your-openai-resource-name>
```

More information on how to configure RBAC permissions for Azure OpenAI can be found in the [Azure OpenAI documentation](https://learn.microsoft.com/azure/ai-services/openai/how-to/create-resource?pivots=cli).

### Start Durable Task Scheduler

Most samples use the Durable Task Scheduler (DTS) to support hosted agents and durable orchestrations. DTS also allows you to view the status of orchestrations and their inputs and outputs from a web UI.

To run the Durable Task Scheduler locally, you can use the following `docker` command:

```bash
docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
```

The DTS dashboard will be available at `http://localhost:8082`.

### Environment Configuration

First configure an isolated version-2 hub/deployment and point every sample worker and client at it. Only after verifying that setup should the operator set `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` for samples that omit an explicit deployment mode. Do not enable acknowledgement automatically for an existing or shared production hub. Starting the emulator alone does not establish isolation.

Each sample reads configuration from environment variables. You'll need to set the following environment variables:

Bash (Linux/macOS/WSL):

```bash
export FOUNDRY_PROJECT_ENDPOINT="https://your-project.services.ai.azure.com/api/projects/your-project"
export FOUNDRY_MODEL="your-deployment-name"
```

PowerShell:

```powershell
$env:FOUNDRY_PROJECT_ENDPOINT="https://your-project.services.ai.azure.com/api/projects/your-project"
$env:FOUNDRY_MODEL="your-deployment-name"
```

### Installing Dependencies

Navigate to the sample directory and install dependencies. For example:

```bash
cd samples/01_single_agent
pip install -r requirements.txt
```

If you're using `uv` for package management:

```bash
uv pip install -r requirements.txt
```

### Running the Samples

Each sample follows a worker-client architecture. Most samples provide separate `worker.py` and `client.py` files, though some include a combined `sample.py` for convenience.

**Running with separate worker and client:**

In one terminal, start the worker:

```bash
python worker.py
```

In another terminal, run the client:

```bash
python client.py
```

**Running with combined sample:**

```bash
python sample.py
```

### Viewing the Sample Output

The sample output is displayed directly in the terminal where you ran the Python script. Agent responses are printed to stdout with log formatting for better readability.

You can also see the state of agents and orchestrations in the Durable Task Scheduler dashboard at `http://localhost:8082`.
