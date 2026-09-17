# Get Started with Microsoft Agent Framework Durable Functions

[![PyPI](https://img.shields.io/pypi/v/agent-framework-azurefunctions)](https://pypi.org/project/agent-framework-azurefunctions/)

Please install this package via pip:

```bash
pip install agent-framework-azurefunctions --pre
```

Requires Python 3.10+ and `agent-framework-core>=1.13.0,<2`. The Durable Task dependency requires
`pydantic>=2.11,<3`. Shared-wire adoption is available on the prototype branch at `567087f8`,
not in a released package.
See [prototype validation](../../samples/README.md#prototype-validation) for current measurements,
separate historical evidence and remaining gaps.

## Version 2 deployment warning

The architecture in [ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88) was accepted and merged on September 15, 2026 into `feature/python-durable-thread-compaction` at `7d90e8f`. [PR #59](https://github.com/microsoft/agent-framework-durable-extension/pull/59) remains an integrated prototype, not a merge-as-is implementation or the contents of a published package. The locally validated implementation uses main's canonical `terminalResults` and `completionReceipts`, replacing the private `responseMailbox` and `completedCorrelations` layout. It uses one canonical wire validator, not parallel private and shared contracts. No .NET interoperability is claimed.

> [!WARNING]
> **Breaking deployment and state contract.** `AgentFunctionApp` and standalone `create_agent_entity`
> require `deployment_mode="isolated_v2"`, or `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` when the
> argument is omitted/`None`. This is operator acknowledgement, not a handshake, security boundary
> or proof of isolation. Use a separate hub/deployment with compatible workers and all clients.
> Keep old workers and workflow histories on the old engine. The gate applies to every prototype
> host, including samples and tests. Passing schema validation never activates version-2 writes.
> ADR acceptance does not make this a production drop-in.

Only canonical `schemaVersion="2.0.0"` state is writable. Legacy reads are limited to exact
`1.0.0`, `1.1.0` and `1.2.0` snapshots, which remain read-only. Unsupported versions, including
future `2.x` versions, are rejected. Normal operations never upgrade legacy state. Rollback requires
compatible workers, clients and workflow protocol, not a transcript-only writer.

Unreleased private prototype `2.0.0` state and in-flight runs are abandoned. Start fresh, isolated
runs with the canonical implementation. There is no private-prototype detection, conversion or
resume path. A shared version label does not make those layouts compatible. Names are unchanged.
Reusing an old `@name@key` on an empty new hub is not migration or permission to redeliver old work.

Generated start routes and internal child dispatch wrap new starts of generated framework workflows
with workflow engine version 2. A custom scheduler starting one of those generated orchestrators
must pass `wrap_workflow_input(input)` because the generated entry point calls `unwrap_workflow_input`.
Those entry points reject unwrapped or legacy starts before revised actions execute.

Do not apply this envelope to ordinary native orchestrations registered with a Functions orchestration
trigger. They receive their application input directly unless their own entry point explicitly
implements the matching unwrap protocol. Wrapping does not authorize input or migrate old histories.

Explicit legacy migration needs an empty, separately addressed destination, a quiesced old owner
and authorized ownership transfer. The backend `migrate` operation implements source-bound
completion evidence. Its required request keys are `source`, `sourceDigest`, `sourceSessionId`,
`destinationSessionId`, `migrationId` and `ownershipTransferId`. Only `deliveryEvidence`,
`completionEvidence` and the deprecated boolean `requireKnownOutcomes` are optional keys.
`sourceDigest` is `state_snapshot_digest(source)` for the unmodified export. The destination ID must
match the receiving entity and differ from the source ID. IDs and assertions do not establish authority.

`completionEvidence` contains exactly `sourceDigest`, a stable nonblank `evidenceId`, `complete: true`
and `results`. The digest must match the source. Results are original canonical `terminalResults`
objects without `resultExpiresAt`, not even `null`. They require known `outcome`, original
`completedAt`, `correlationId` and full inline `response.messages`, preserving optional `value`,
metadata and unknown JSON without a lossy Core projection. Failures require canonical errors.
Success forbids `error`. Every completion must be covered, including results lost from history.

Completion evidence is required for any nonempty history, including request-only history, a
nonempty session-only source, a `truncation` field, nonempty scalar `ingestedPositions`, or a supplied
nonempty accepted-input journal. If such a source has no completed requests, an explicit complete
source-bound journal with `results: []` asserts that fact. It is rejected if any retained response or
`errorResponse` remains. An empty retained response set alone does not prove no completions.
Only a fresh source with none of these signs of use can omit completion evidence.

`deliveryEvidence` requires matching `sourceDigest`, a stable nonblank `evidenceId`, `complete: true`
and `messages`.
Its only optional field is `messagePositions`, an array aligned one-for-one with `messages`.
Messages remain complete, canonical, losslessly round-trippable `Message.to_dict()` accepted inputs,
including `message_id`, all accepted revisions and evicted inputs.

Nonempty scalar `ingestedPositions` requires both this journal and the sidecar. Each sidecar member
is `null` for no cursor participation or an object with exactly `producer` (nonblank string) and
`position` (nonnegative integer, never a boolean), recording independently authoritative accepted-input
cursor attribution. Producer maxima from explicit attributions alone must match the legacy map.
This checks consistency, not a delivered prefix. Omit the sidecar only with an empty or absent scalar
map, treating every input as unpositioned. Never derive attribution from public message IDs, guessed
producer names or a retained partial transcript.

Every retained nonblank public request message ID must occur in a provided complete journal,
regardless of `wf_` spelling. Internal reconciliation IDs do not substitute. Without an input journal,
retained-ID-only compatibility markers apply equally to opaque and `wf_`-looking public IDs. These
markers are not exact fingerprint evidence and cannot reconstruct evicted inputs. Journal authority
and completeness are privileged operator assertions, not independently proved by `sourceDigest`.
The accepted-input and completion journals remain independent.

Neither transcript fragments nor source `createdAt` can supply missing original responses or
completion times. Missing, duplicate or contradictory evidence blocks migration. There is no
invented or `unknown` v2 outcome, and `requireKnownOutcomes=False` cannot waive the requirement.
Without the required authoritative journals, keep the session on the old engine.

The migrator preserves original journal `completedAt` strings and sets matching result and receipt
`resultExpiresAt` to migration time plus configured `response_delivery_window_seconds`, never before
completion. The source does not choose that grace deadline. Exact request retries return the recorded
migration without rewriting state, including after cold reload or later runs. Migration retains the
logical external-history session identity without copying that store or moving workflow histories.
No generated HTTP/MCP migration endpoint is provided. See
[migration requirements](../durabletask/README.md#explicit-legacy-migration).

The [shared JSON and Python profiles](../durabletask/README.md#shared-json-and-python-runtime-profiles)
apply to both hosts. Full raw JSON preservation is separate from Core projection, including canonical
metadata, unknown sibling fields and absent versus null values. Python uses versioned `pythonIngestion`,
`pythonHistoryIdentity`, `pythonCoreFields` and `pythonContinuationEncoding` extensions. Foreign profiles
remain inert for readers. A runtime relying on one must validate it or block dependent restoration
and writes. Optional opaque `historyBinding` imposes no shared fixed-owner policy. Neither raw
round-tripping nor base64 continuation encoding proves .NET or provider interoperability.

## Durable Agent Extension

The durable agent extension lets you host Microsoft Agent Framework agents on Azure Durable Functions so they can persist state, replay conversation history, and recover from failures automatically.

### Basic Usage Example

Before running this example, configure a separate Functions task hub/deployment with compatible version-2 workers and clients. Set `deployment_mode="isolated_v2"` only after the operator has verified that setup.

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIChatCompletionClient
from agent_framework_azurefunctions import AgentFunctionApp

assistant = Agent(client=OpenAIChatCompletionClient(), name="assistant")
# Configure the Functions task hub/deployment separately from the old worker
app = AgentFunctionApp(agents=[assistant], deployment_mode="isolated_v2")
```

Post messages using the generated `/api/agents/{agent_name}/run` endpoint.

### History and retention settings

`AgentFunctionApp` uses the same entity/history integration as the direct Durable Task worker.
Automatic durable history is appended after existing providers to match core's reverse after-hook
order. Only exact built-in `InMemoryHistoryProvider` instances are replaced, preserving `source_id`,
`skip_excluded`, storage flags and optional `after_run_once_per_turn` metadata. Core 1.13 does not
require that hint. Custom in-memory subclasses retain their hooks/session transcripts in the
protected floor, outside durable transcript eviction. Other custom durable-provider JSON state
persists except the transient message buffer and position index.

Registration does not enable compaction. External primaries and store-only sinks retain their
policies, subject to the intentional service-branch restriction below. Multiple primaries or
duplicate `source_id` values are rejected. Providers append through core hooks, followed by a final
durable flush. Only agents without a context pipeline use direct entity transcript appends.

The default `DurableHistoryProvider.after_run()` calls public `save_messages()` for separate request and response batches, so overrides can validate, transform or reject them. A single core hook can produce two saves for provenance, not an unchanged combined-batch cadence. Existing core and provider APIs are unchanged.

Canonical `messageId` remains the public producer ID, including repeated IDs. Version-1 `pythonHistoryIdentity` uses `pythonHistoryId` for internal occurrences without exposing reconciliation metadata as application message metadata. The durable adapter presents internal IDs only during configured `CompactionProvider` hooks. Strategies use those IDs for internal summary/source links, while normal model calls, other providers and audits see public IDs. Arbitrary custom hooks that depend on public IDs or substitute different input IDs are not guaranteed transparent behavior. Raw unknown input fields remain inert JSON independently of Core projection. See [history integration](../durabletask/README.md#history-and-retention-settings).

Eager pruning and pressure eviction are independent. The matrix assumes no explicit provider
`prune_excluded` override.

| `retention` | `max_state_bytes=None` | Positive integer byte budget |
| --- | --- | --- |
| `"keep_all"` (default) | No transcript deletion (default) | Evict eligible oldest groups only under pressure |
| `"follow_compaction"` | Prune eligible compaction exclusions only | Prune exclusions, then evict under pressure if needed |

- `max_state_bytes` defaults to `None`. Azure Functions cannot resolve its backend's hard limit,
  so `"backend_limit"` is rejected at registration. Use an explicit positive integer to enable
  pressure eviction. A budget does not enable blob offload or raise a backend limit. `"auto"` is
  no longer a retention mode.
- The direct Scheduler host's `"backend_limit"` remains a non-normative Python-only convenience,
  not part of the portable `None` or positive-integer contract or an agreed shared API.
- Watermarks default to `high_watermark=0.85` and `low_watermark=0.70`, with
  `0 < low_watermark < high_watermark <= 1`. The whole serialized entity counts, including terminal
  results, completion receipts, session and ingestion state. Protected data can prevent a commit even after pruning.
- `response_delivery_window_seconds` defaults to `60` and must be a positive integer. Delivery
  expiry is independent of transcript retention.
- `add_agent()` overrides app defaults. Constructor `workflow_*` settings supply workflow defaults,
  and `configure_workflow()` can override them for newly registered nodes, including nested workflows.
  Omitted budgets or `INHERIT` inherit the enclosing default. Explicit `None` disables that budget.
- Explicit `prune_excluded=False` on `DurableHistoryProvider` disables eager pruning even with
  `follow_compaction`. It does not disable pressure eviction or change an external store's policy.

As an alternative to the default app above, configure an explicit budget for standalone agents and
disable it for an existing named `workflow`. The sample byte budget is an application choice, not
an inferred Functions backend limit.
The same verified isolated deployment and explicit operator acknowledgement are required.

```python
from agent_framework_durabletask import INHERIT

app = AgentFunctionApp(deployment_mode="isolated_v2", max_state_bytes=800_000, workflow_max_state_bytes=None)
app.add_agent(assistant, retention="follow_compaction", max_state_bytes=INHERIT)
app.configure_workflow(workflow)
```

`follow_compaction` only prunes exclusions produced by configured compaction. Workflow `full`,
`last_agent` and `custom` projection runs before per-target delta transport. Custom filters execute
during orchestration replay and must be synchronous, deterministic and side-effect-free, but need
not select monotonically increasing positions. Parallel `contextMessageIds` carry occurrence hashes
without rewriting public message IDs. Private forwarding provenance stays in internal checkpoints,
not application metadata. The outgoing logical conversation includes the full selection and all
response messages, not just the delta. Typed/cache-only requests, agent approval/HITL and
output-designated agents use the same contract.

Generated agent outputs and intermediate events use portable response snapshots. HTTP workflow
results retain structured `value`, including null and falsey values, and response metadata.
External clients do not need the worker's Pydantic class. Worker-side conditions and activities
still receive the locally declared model. Arbitrary activity outputs keep the existing checkpoint
codec and its importable-type requirements. Parent designations also gate direct child outputs.

### Service ownership, delivery and reset

Effective `store` follows run options, then agent defaults, then the client's `STORES_BY_DEFAULT`.
For example, `options={"store": False}` selects client-owned history even on a service-storing
client. Explicit `False` excludes saved or supplied service conversation IDs from that invocation
and its history hooks. A later service-owned run can reuse the saved service ID without importing
the intervening client-owned transcript. Switching branches does not migrate or merge history.
External and service-owned runs create no local request-message mirror.

Durable deliberately suppresses **both load and store hooks** on the inactive external/custom
primary during service-owned runs, including per-service-call persistence. Core 1.16 can still save
to a configured primary on such runs. This branch-isolation restriction is not universal unchanged
hook semantics. Use a distinct store-only sink with its own `source_id` to audit both branches.
Its configured storage flags still apply.

A completed save through an external primary's default after-run hook or a completed service-owned `ChatResponse` affirms the inputs actually accepted, even if the invocation later fails. This is accepted-input evidence, not invocation success or a distributed commit. Opaque custom `after_run()` overrides and interrupted saves do not permit inferred acknowledgement. A store-only sink cannot establish acceptance by the primary.

HTTP polling uses immutable original envelopes in `terminalResults`, not reconstructed transcript
responses. Every result and `completionReceipts` entry requires `correlationId`, authoritative
`outcome` (`succeeded` or `failed`) and `completedAt`. The result carries a full shared response,
including `messages`, optional structured `value` and canonical metadata. Failure additionally
requires a canonical error, while success forbids one. Receipt metadata does not alter the original
response payload. Raw JSON remains independent of the Core objects returned to clients.

Receipt `resultState` is required. `available` requires a matching result, while `unavailable`
forbids a result and requires `resultUnavailableAt`. Map keys must equal embedded correlation IDs
exactly. Outcome, completion time and optional `resultExpiresAt`, including absence, must agree
between result and receipt. Expiry cannot precede completion. Reads and cleanup reject contradictions,
even after expiry. There is no `unknown` v2 outcome or inferred success from a pruned transcript.

At or after a configured expiry, HTTP/MCP consumers must report completed-but-result-unavailable
with the retained outcome, not deliver the payload, report pending or rerun work. This applies even
before physical cleanup. Cleanup atomically removes the result and records `resultUnavailableAt`,
no earlier than completion and, for time expiry, no earlier than expiry. Completion facts and expiry
policy remain unchanged. No receipt means only no recorded completion, not proof of pending work.
Acceptance-only and fire-and-forget responses cannot establish completion.

Expiry is optional in the shared wire contract and independent of transcript retention. New runs,
duplicates, reset and backend `expire_responses` clean time-expired payloads without executing the
model, tools or providers for cleanup. Idle physical cleanup needs an application-owned schedule or
explicit backend signal/manual operation. No public HTTP/MCP cleanup endpoint is generated. Expiry,
cleanup and reset retain completion receipts. See the [delivery contract](../durabletask/README.md#service-ownership-delivery-and-reset).

Local reset clears session and transcript context but preserves live terminal results, completion
receipts and ingestion evidence. Normal delivery expiry still applies. Reset with a non-durable
custom/external primary raises `NotImplementedError` until provider-owned clearing is available.

Results and receipts must commit atomically with the operation's entity-local session, ingestion,
transcript and other control state. Only structured `previous_response_not_found` on a
service-owned run permits bounded retries, and only before a stream update, function execution or
service-session advancement. An invalid service conversation must fail without silently falling
back to a new conversation or local history. Provider-hook side
effects are not guaranteed safe or identical on retry. There is no generic non-streaming retry after
runtime failure. Only matching unsupported-stream `TypeError` before consumption negotiates fallback.
Final callbacks receive deep copies preserving Pydantic fields. Opaque SDK `raw_representation`
detachment is best effort and that field is omitted if it cannot be copied.
Callbacks and host write returns are not confirmation of persisted completion. Uncommitted model/tool
effects and external appends can repeat after failure, so applications need their own external
idempotency strategy. Completion receipts last until entity deletion and can exhaust capacity.
Whole-entity TTL/deletion removes duplicate protection and needs an explicit late-duplicate policy.
A bounded receipt protocol and optional
retry-safe external-history adapters remain deferred, with no mandatory core API changes or
guarantee of a distributed transaction or exactly-once uncommitted effects.

### Retention telemetry

The shared runtime emits the [retention instruments and bounded attributes](../durabletask/README.md#retention-telemetry)
under scope `agent_framework.durabletask`. Only the OpenTelemetry API is a direct runtime dependency
for this instrumentation. The SDK remains a development dependency, with application-owned meter
providers and exporters. Metrics contain no payloads or session, request or message IDs.

Removal counts describe staged changes, not confirmed deletion. Host `set_state` returns and
failures both leave commit status `unknown`. Separate persisted-state readback and subsequent model
input are needed to validate retention. Telemetry does not change warm-state rollback or protect
uncommitted external effects from repetition.

For more details, review the Python [README](https://github.com/microsoft/agent-framework/tree/main/python/README.md) and the samples directory.
