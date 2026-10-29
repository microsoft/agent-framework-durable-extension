# Python entity schema migration

This opt-in path upgrades supported agent state at its original entity name and key. It does not replace orchestrations, create successor instance IDs, use ContinueAsNew, or migrate arbitrary business workflows.

## Deployment prerequisites

Schema interpretation and dispatch are separate contracts. `dafxSchema` admits a state format. Backend work-item filters decide which worker receives work. A Python exception is not a routing mechanism.

Use `deployment_mode="two_phase"` only with Durable Task Scheduler (DTS). Standalone hosting requires `DurableTaskSchedulerWorker` and `durabletask>=1.11.0`. The wrapper calls `use_work_item_filters()` so the SDK advertises the complete registered entity, orchestration and activity names when the worker starts. Do not clear or replace these filters with broader filters afterward.

Keep a compatible filtered fleet connected while legacy workers coexist. Name-based filtering is not an indefinite reservation. The [released Functions filtering documentation](https://github.com/Azure/azure-functions-durable-extension/blob/v3.13.1/samples/workitem-filters/README.md#key-behaviors) explicitly describes timeout and fallback to the general queue after a filtered worker disconnects. Bounded emulator observations are not managed-service guarantees. If compatible fleet availability cannot be maintained, stop legacy workers before submitting new-name traffic or use a separate hub.

`isolated_v2` remains available with its existing names and client defaults. It does not turn on the two-phase path. Both modes can be selected explicitly or through `DURABLE_AGENTS_DEPLOYMENT_MODE`.

## Phase 1

Leave `process_legacy_entities=False`. New workers register only `dafx-v2-{agent}` entities. New agent clients explicitly select address generation 2.

```python
from agent_framework_durabletask import DurableAIAgentClient, DurableAIAgentWorker

# worker is a configured DurableTaskSchedulerWorker.
host = DurableAIAgentWorker(worker, deployment_mode="two_phase")
host.add_agent(agent)

# client is a configured DurableTaskSchedulerClient.
agents = DurableAIAgentClient(client, entity_version=2)
session = agents.get_agent(agent.name).create_session()
```

Existing clients retain generation 1 by default. Existing `DurableAgentSession` addresses take precedence over a client's new-session generation. Serialize sessions with `to_dict()` and restore with `from_dict()`. New serialized sessions include `durable_entity_version=2`. A missing field means the historical Python logical-name format, even if the agent name itself contains `dafx-`. Do not strip prefixes from old serialized sessions or relabel them.

New external generation-2 session strings have the form `@dafx-v2-{agent}@{key}`. Historical Python strings use `@{agent}@{key}`. Explicit raw keys remain historical addresses in generated HTTP and MCP endpoints. Reuse the complete returned session ID for generation-2 calls. Unknown external address generations are rejected.

Historical formatting, serialization, default clients and `isolated_v2` keep valid logical names such as `v2-assistant`, `V3-customer` and `v2-orders` unchanged. Only two-phase host registration rejects logical names beginning with `v2-` (case-insensitive), because their historical `dafx-v2-*` addresses already occupy the new routing namespace. Keep these identities on an isolated deployment rather than renaming or retargeting their existing state. Other version-looking names are not globally reserved. Registration still checks collisions between the actual derived entity, orchestration, activity and Functions names before installing handlers.

When a supplied logical agent name exactly matches the embedded session name, parsing preserves the historical generation. For example, `@dafx-v2-assistant@key` for the logical agent `dafx-v2-assistant` still addresses `dafx-dafx-v2-assistant`, not a new conversation. Explicit serialized generation metadata takes precedence. A new generation-2 session for that agent uses `@dafx-v2-dafx-v2-assistant@key`.

Generated workflows need the same isolation for their entry point. `DurableWorkflowClient(client, workflow_name="orders", entity_version=2)` starts **new** workflows under `dafx-v2-orders`. It preserves a caller-supplied orchestration instance ID. Their agent entities use `dafx-v2-orders-{executor}`. Activity names and workflow logic are unchanged. Children inherit the entry point's generation without rewriting their existing deterministic IDs.

Historical generated workflow entries remain `dafx-orders`. They select historical agent entity names on every replay, including after phase 2. Selecting routing from `is_replaying`, a current environment variable, or fleet configuration would change recorded dispatch and is not supported. For hand-written orchestrations, the additive `entity_version` on `DurableAIAgentOrchestrationContext` or Functions `get_agent` must be selected from a replay-stable recorded input/version. Keep the default 1 for existing histories.

## Phase 2

**Stop every old worker before enabling historical registration.** Name filters cannot establish that the old owner has stopped, fence a peer, or prevent both generations from handling the same historical name after opt-in. This is a customer deployment prerequisite.

```python
host = DurableAIAgentWorker(
    worker,
    deployment_mode="two_phase",
    process_legacy_entities=True,
    legacy_evidence_provider=evidence_provider,  # Optional authoritative historical recovery.
)
host.add_agent(agent)
```

New workers now register `dafx-{agent}` alongside `dafx-v2-{agent}` and both generated workflow entry names. There is no state scan or bulk rewrite. A historical entity upgrades on its next successful operation at the **same address**. Reading or staging alone does not write. The normal host entity commit persists the upgrade. A duplicate request whose authoritative completion is imported also commits the upgrade without invoking the agent again.

Conversion receives detached JSON. Failed validation or conversion leaves raw state unchanged and does not decode the operation request or call the agent, tools or providers. An operation that fails before its host commit can be converted again on retry/restart. This does not make model calls or external provider writes exactly once. Existing invocation safety and backend commit semantics still apply.

State budgets include the persisted `dafxSchema` metadata during retention and maintenance checks. In-place conversion measures the final marked snapshot, excluding the export API's temporary ownership-transfer record. The enclosing operation still applies its normal budget before committing any additional state. For the separate export/import API, an otherwise empty destination initialized by reset remains empty even with validated schema metadata. Application fields, session state, completion receipts and existing migration bindings still prevent replacement.

## Supported historical state and evidence

The historical Python writer in `_durable_agent_state.py` persists `schemaVersion`, `data.conversationHistory`, optional `extensionData`, and the documented legacy request/response shapes. Its default is `1.1.0`. The shared reader additionally recognizes exact `1.0.0` and `1.2.0` shapes. Current v2 state uses `terminalResults` and `completionReceipts`, with authoritative outcomes and completion timestamps. These are not present in legacy transcripts.

The in-place adapter reuses `migrate_legacy_state` with an explicit completion policy, rather than inventing historical results:

- Without completion evidence, eligible empty or populated retained conversations use the **cutover policy**. Validated history and session JSON survive, but historical `terminalResults` and `completionReceipts` start empty. Conversion does not invent old outcomes, completion timestamps or receipts.
- The legacy Python runtime reran duplicate invocation correlations. A late retry of a pre-upgrade invocation can therefore execute again, even if its response remains in history. New duplicate protection begins with the successful entity commit that records an operation under v2, including the operation that commits conversion. Its recorded success or failure is protected on subsequent retries and cold restarts through the normal v2 receipt rules. A failed or unacknowledged backend commit does not establish exactly-once model or provider effects.
- If an authoritative completion journal is supplied, the **historical policy** imports its validated results and receipts. A duplicate with an imported result can return without invoking the agent. Retained response timestamps alone are not completion timestamps. A retained successful-looking message does not establish a complete completion journal.
- Truncation metadata is not accepted by cutover conversion. The approved ledger boundary does not authorize reconstruction of missing conversation history or reset data. The historical evidence path retains its existing validation requirements.
- Nonempty `ingestedPositions` are not accepted by cutover conversion. The historical evidence path additionally requires a complete accepted-input journal, including original message-to-position attribution. A cursor maximum or an ID naming pattern is not proof of accepted input.
- Reserved legacy `pythonIngestion`, malformed metadata, unsupported session layouts, inconsistent source identity, unknown versions and lossy target representations are rejected. Current v2 state is validated, not re-migrated. No newer version is silently downgraded.

An optional `legacy_evidence_provider(snapshot, context)` returns a mapping with only `delivery_evidence` and/or `completion_evidence`. Their values follow the existing [export migration contract](README.md#migration), including `sourceDigest`, `evidenceId`, `complete=True` and authoritative original data. Use `state_snapshot_digest(snapshot)` to bind evidence to the exact source. The provider receives the original `entity_name`, `entity_key` and unchanged `session_id`. It must be effect-free and return stable recorded evidence. It is not called for current state or after a committed upgrade.

Omitting the provider or returning an empty mapping intentionally selects cutover. A provider returning a list, null or another non-mapping value is invalid. If either evidence key is present, its value must be a non-null journal object and pass the existing evidence validators. Explicit null, malformed or partial evidence never becomes an omitted journal or falls back to cutover, even for an empty legacy entity. Rejection precedes request decoding, model calls and state mutation.

This API does not recover missing evidence. Eligible retained history can migrate at the approved cutover boundary without a historical completion journal. State that still requires unavailable cursor, truncation or other authoritative evidence is **not supported for conversion**. Do not manufacture results, timestamps, receipts or journals. Keep unsupported data unmodified and choose an explicit operational recovery outside this migration path.

Session state and service continuation are preserved as JSON. Provider compatibility with an old continuation token remains provider-specific. The export operation's separate-destination ownership-transfer binding is not added to an in-place conversion.

Scalar ingestion cursors protect a different behavior from invocation duplicate suppression. Clearing them or replacing them with guessed fingerprints could replay already accepted upstream messages or suppress inputs never accepted. This implementation does neither. Retained public message IDs without authoritative accepted-input evidence remain identity-only compatibility markers, not reconstructed acceptance fingerprints. Missing reset history cannot be recovered from an empty snapshot, and the converter makes no retroactive promise about it.

The separate export/import API keeps `completion_policy="historical"` by default. `require_known_outcomes=False` still does not waive that policy. Explicit `completion_policy="cutover"` selects the same constrained ledger boundary used by in-place upgrades and cannot be combined with completion evidence or `require_known_outcomes=True`. Supplying malformed or partial authoritative evidence never silently falls back to cutover.

### Derivable facts and alternative evidence sources

The retained JSON establishes its message contents, public IDs, correlation IDs, ordering and recorded metadata. Conversion preserves those values instead of generating replacement messages or identities. Explicit `errorResponse` entries and recognized non-tool failure content constrain the outcome to failure. The absence of failure content does not establish a successful terminal invocation.

The historical Python response writer takes `createdAt` from the provider response timestamp, with a fallback when absent. It is not a recorded durable completion time. Its in-memory `is_error` flag is not serialized. Reset can replace the entire history. These distinctions prevent a transcript-only converter from establishing the complete historical completion set or assigning original completion timestamps.

Operators should check recorded orchestration histories, callback journals and application audit records before concluding that evidence is unavailable. The released SDK's `get_orchestration_history()` exposes entity call inputs and completion or failure records correlated by request ID. Validate the original target address, operation and input correlation ID when joining these records. A successful entity operation alone does not establish agent success because the historical agent can return an error response as a successful entity operation. An event timestamp records the backend event, not necessarily the original invocation completion instant. Do not silently relabel it as that instant.

Orchestration histories cover recorded orchestration calls, not every direct HTTP, MCP or client signal. Entity `lastModifiedTime` describes the latest state write, not each invocation's completion. Callback logs can establish only the callbacks actually recorded, and a final callback can precede the entity commit. None of these sources automatically proves journal completeness. Assemble and validate an authoritative snapshot before starting the new worker, then expose stable evidence through `legacy_evidence_provider`; do not fetch changing records or perform effects during conversion.

The approved cutover policy preserves the historical Python invocation delivery behavior rather than claiming stronger retroactive protection. Only authoritative recovery selects historical completion results. The absence of that evidence selects empty historical completion maps, not an inferred success, failure or completion time.

## Schema and compatibility contracts

Two-phase writes include root metadata:

```json
{
  "schemaVersion": "2.0.0",
  "dafxSchema": {
    "contract": "dafx.agent-state",
    "version": "2.0.0"
  }
}
```

The complete state also contains its normal `data` object. `schemaVersion` and metadata must agree. Metadata must contain exactly the recognized contract and version. Historical unmarked states are admitted only through the registry's explicit compatibility list. Readers and mutation paths reject incompatible reserved metadata before version-sensitive projection.

`EntitySchemaRegistry` freezes explicit validators and conversion edges. Every registered version must have an acyclic path to its declared current version. Every edge declares its output schema, which is validated before further interpretation. No minor-version range is assumed safe. Converters and validators receive detached JSON, and converters must not perform effects. Future implementations can register tested formats and conversion paths without inventing production v3 semantics today.

The supplied `agent_entity_schema_registry` targets exactly `2.0.0`. The public generic registry can be exercised with synthetic contracts, but registering a synthetic version does not make the production agent writer understand it.

## Azure Functions

Functions does **not** stream work from `DurableTaskSchedulerWorker`. `azure-functions-durable==2.0.0rc2` uses a host-driven `DurableFunctionsWorker`. Calling its inherited `use_work_item_filters()` does not configure host dispatch.

The released [Durable Functions extension v3.13.1](https://github.com/Azure/azure-functions-durable-extension/tree/v3.13.1/samples/workitem-filters) documents actual host/provider filtering. That release's [dependency manifest](https://github.com/Azure/azure-functions-durable-extension/blob/v3.13.1/Directory.Packages.props) includes `Microsoft.DurableTask.AzureManagedBackend` 1.3.0. Deploy a host extension with that support or later, not merely a newer Python wheel, and configure:

```json
{
  "version": "2.0",
  "extensions": {
    "durableTask": {
      "storageProvider": {
        "type": "azureManaged",
        "connectionStringName": "DURABLE_TASK_SCHEDULER_CONNECTION_STRING",
        "workItemFilteringEnabled": true
      }
    }
  }
}
```

```python
from agent_framework_azurefunctions import AgentFunctionApp

app = AgentFunctionApp(
    agents=[agent],
    deployment_mode="two_phase",
    acknowledge_host_filtering=True,
    process_legacy_entities=False,
)
```

`acknowledge_host_filtering=True` asserts that the operator configured the deployed host. It does not install a host extension, validate an extension bundle, inspect effective environment overrides, or prove that DTS is filtering. Verify the resolved extension/provider and the host's filtering startup log before coexistence. Azure Storage and other providers are not supported for this strategy. Generated entity handlers use the same schema admission and conversion as standalone. HTTP, MCP and generated workflows version new addresses consistently. Phase 2 uses the same explicit old-worker shutdown prerequisite.

## Replay and validation limits

Customers own replay compatibility of workflow logic and customer-defined data contracts. DAFX owns its dispatch and persistence compatibility. This change keeps existing supported workflow names, instance IDs, actions and entity addresses when replaying them. It does not make incompatible historical engines replayable. In particular, the pre-existing Python protocol/HITL and SDK nested-race limitations in the [runtime guide](README.md#current-runtime-contract-on-this-unreleased-stack) still apply to affected older histories.

Tests drive real released SDK entity executors and generated Functions protobuf handlers, including cold retries, failed writes, malformed metadata, unsupported future state and replay dispatch controls. Registered HTTP and MCP handler tests check new-session routing and historical address preservation. A bounded local DTS emulator acceptance test exercises mixed-worker new-name traffic, old-worker shutdown and conversion at the original address.

The `Python entity migration (local DTS emulator)` PR job in `.github/workflows/python-merge-tests.yml` explicitly runs both empty and populated cases from `tests/test_entity_migration_dts.py`. It starts a task-owned digest-pinned emulator with loopback-only ports, waits for its HTTP listener, requires both cases to execute without skips, and collects container logs before cleanup even on failure. No Azure credentials or cloud endpoint are used. To run locally against an independently owned emulator, set `DTS_ENTITY_MIGRATION_ENDPOINT=127.0.0.1:8080` and run `uv run --no-sync pytest -m integration packages/durabletask/tests/test_entity_migration_dts.py` from `python`. The test rejects non-loopback endpoints. Without that variable, ordinary developer runs skip the test; the explicit CI gate supplies it and rejects skips.

A separate local probe exercised the actual Functions host, not just generated handlers. Core Tools 4.12.0 with released extension bundle 4.39.1 loaded Durable extension 3.15.0 and AzureManagedBackend 1.10.0. The host logged effective filtering and registered one entity during phase 1, then two during phase 2. Five generation-2 HTTP calls completed alongside six legacy SDK operations. After the legacy worker stopped, the restarted Functions host upgraded genuinely empty legacy state at its original address without creating a copied entity. Standard app-local Python dependency resolution was necessary to load the released Python dependencies without modifying them.

A follow-up on the same released host versions exercised populated legacy writer output. It preserved the retained request and response, migrated at the original address, passed the retained request to the model, and created no completion receipt for the old correlation during conversion. A late retry of that old correlation then executed once and gained a new receipt. Repeating it did not call the model again. Standalone emulator integration also covers both empty and populated state.

These probes did not exercise the documented minimum extension versions or an authenticated managed service. They also emitted an AzureWebJobsStorage health warning because general-purpose Functions storage was not configured, despite successful entity dispatch and commits. They are bounded coexistence evidence, not a fully healthy deployment, scale-out test, filter-expiry guarantee or outage guarantee. Validate your actual host/provider deployment and replay-compatible workflow histories before rollout.
