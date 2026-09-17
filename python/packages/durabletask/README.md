# Get Started with Microsoft Agent Framework Durable Task

[![PyPI](https://img.shields.io/pypi/v/agent-framework-durabletask)](https://pypi.org/project/agent-framework-durabletask/)

Please install this package via pip:

```bash
pip install agent-framework-durabletask --pre
```

Requires Python 3.10+, `agent-framework-core>=1.13.0,<2` and `pydantic>=2.11,<3`.
Shared-wire adoption is implemented and locally validated, but not published. See
[prototype validation](../../samples/README.md#prototype-validation) for current measurements,
separate historical evidence and remaining gaps.

## Version 2 deployment warning

The architecture in [ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88) was accepted and merged on September 15, 2026 into `feature/python-durable-thread-compaction` at `7d90e8f`. [PR #59](https://github.com/microsoft/agent-framework-durable-extension/pull/59) remains an integrated prototype, not a merge-as-is implementation or the contents of a published package. The locally validated implementation uses main's canonical shared wire contract, replacing the private `responseMailbox` and `completedCorrelations` layout with `terminalResults` and `completionReceipts`. It uses one canonical wire validator, not parallel private and shared contracts. No .NET interoperability is claimed.

> [!WARNING]
> **Breaking deployment and state contract.** `DurableAIAgentWorker` requires
> `deployment_mode="isolated_v2"`, or `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` when the argument
> is omitted/`None`. This is operator acknowledgement, not a handshake, security boundary or proof
> of isolation. Use a separate hub/deployment with compatible workers and all clients. Keep old
> workers and workflow histories on the old engine. The gate applies to every prototype host,
> including samples and tests. Passing schema validation never activates version-2 writes.
> ADR acceptance does not make this a production drop-in.

Only canonical `schemaVersion="2.0.0"` state is writable. Legacy reads are limited to exact
`1.0.0`, `1.1.0` and `1.2.0` snapshots, which remain read-only. Unsupported versions, including
future `2.x` versions, are rejected. Normal operations never upgrade legacy state. Rollback
requires compatible workers, clients and workflow protocol, not a transcript-only writer.

Unreleased private prototype `2.0.0` state and its in-flight runs are abandoned. Start fresh,
isolated runs with the canonical implementation. There is no private-prototype detection,
conversion or resume path, and the shared version label does not make the layouts compatible.
Names are unchanged. Reusing an old `@name@key` on an empty new hub is not migration or permission
to redeliver old work.

`DurableWorkflowClient` and internal child dispatch wrap new starts with workflow engine version 2.
Raw/legacy starts reject before revised actions execute. Native custom scheduling must use public
`wrap_workflow_input` for new instances. It does not authorize input or migrate old action histories.

### Explicit legacy migration

Legacy migration is a privileged backend operation into an empty, separately addressed destination
after quiescing the old owner and authorizing ownership transfer. It is not a private `2.0.0`
migration path. Both Python hosts implement the backend `migrate` operation with source-bound
completion evidence. This does not establish full shared-wire or deployment validation.

The request requires `source`, `sourceDigest`, `sourceSessionId`, `destinationSessionId`,
`migrationId` and `ownershipTransferId`. Its only optional keys are `deliveryEvidence`,
`completionEvidence` and the deprecated boolean `requireKnownOutcomes`. `source` is the unmodified
legacy export, and `sourceDigest` is `state_snapshot_digest(source)`, computed before defaults or
projection. `destinationSessionId` must match the destination entity and differ from `sourceSessionId`.
The deployment owner must authorize the export, journals and ownership transfer. Supplying IDs
does not authorize migration or fence the old owner.

`completionEvidence` has exactly `sourceDigest`, `evidenceId`, `complete` and `results`.
Its digest must match the source digest, `evidenceId` is a stable nonblank journal identity,
`complete` must be `true`, and `results` is a list of original canonical `terminalResults` objects
without `resultExpiresAt`, not even `null`. Each result requires `correlationId`, a known `outcome`
(`succeeded` or `failed`), authoritative original `completedAt` and the full inline `response`,
including `messages`. Optional structured `value`, including null or falsey values, metadata and
unknown JSON fields stay at their original locations. Failure also requires canonical `error.code`
and `error.message`. Success forbids `error`.

A complete completion journal is required for any used source. That includes any nonempty
`conversationHistory` (even request-only history), a nonempty `session` object without history,
a `truncation` field, a nonempty scalar `ingestedPositions` map, or a supplied nonempty accepted-input
journal. It must cover every terminal completion, including results lost from retained history.
For a used source with no completed requests, supply the same source-bound journal with
`complete: true` and `results: []` as an explicit assertion that no requests completed. That empty
journal is rejected if any response or `errorResponse` remains. No retained responses alone does
not prove that no requests completed. Only a fresh source with none of these signs of use can omit
completion evidence.

Every retained response correlation must be covered, and failure evidence must agree with the
journal outcome. Partial transcript responses, typed `errorResponse` entries and accepted-input
journals cannot supply missing original results or completion times. Neither absence of errors nor
source `createdAt` proves success or completion time. Migration preserves canonical journal JSON
without restoring a partial Core projection and presenting it as the original response. Missing,
duplicate or contradictory evidence blocks migration. There is no invented or `unknown` v2 outcome,
and `requireKnownOutcomes=False` cannot waive the evidence requirement.

Nonempty scalar `ingestedPositions` additionally requires `deliveryEvidence` with exactly
`sourceDigest`, `evidenceId`, `complete: true` and `messages`. The messages are complete, losslessly
round-trippable `Message.to_dict()` inputs, including `message_id`, all accepted revisions and
evicted inputs. Exact identities preserve gaps. Matching producer maxima checks consistency only,
not a delivered prefix. This accepted-input journal is independent of the completion journal.
Completeness assertions and digest checks do not establish authority by themselves. Without the
required journals, keep the session on the old engine.

The migrator owns delivery grace. It preserves the original journal `completedAt` strings and sets
matching result and receipt `resultExpiresAt` to migration time plus the configured
`response_delivery_window_seconds`. The new deadline cannot precede completion. Source expiry is
not accepted as the new grace policy. An exact request retry returns the recorded migration without
rewriting state, including after cold reload or later runs, so it does not refresh grace. Migration
retains the logical session identity used by external history without copying that store or moving
workflow action histories. No generated HTTP/MCP migration endpoint is provided. See
[ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88) for the accepted
architecture and [prototype validation](../../samples/README.md#prototype-validation) for current
and historical checks and remaining gaps.

### Shared JSON and Python runtime profiles

The raw canonical JSON must survive read/write independently of its projection into Core objects.
Unknown properties remain at their original object locations, separate from explicit `extensionData`.
This includes nested response, message, content and usage metadata. Preserve exact string contents,
array order, numeric values and absent versus null fields. Function arguments retain their original
object or string form. A Core projection that cannot expose a field must not erase its raw value.
This is JSON-value preservation, not byte-identical formatting or a promise that .NET or a model
provider can consume every projected value. Malformed known fields are rejected, not hidden as
unknown metadata. Stored type names never authorize dynamic type loading.

Python-specific bookkeeping uses separately versioned runtime extensions, not private replacements
for canonical fields.

- `pythonIngestion` uses profile `agent-framework-python.ingestion`, version 1, with exact message
  receipts that preserve delivery gaps independently of transcript retention. Scalar legacy cursors
  keep their historical meaning.
- `pythonHistoryIdentity`, version 1, uses `pythonHistoryId` for internal occurrence identity.
  Canonical `messageId` remains the public producer ID, including repeated IDs.
- `pythonCoreFields` uses the versioned `core-fields` profile for Python Core fields that need
  explicit round-trip metadata, separate from the shared envelope.
- `pythonContinuationEncoding` identifies the versioned encoding for a JSON dictionary carried as
  base64 continuation bytes. This does not make the token resumable by another runtime or provider.

Readers preserve foreign profiles as inert JSON. A runtime relying on a profile must validate its
identity, supported version and required fields before use, or block restoration and writes that
depend on it. Schema validation alone is not profile approval. `historyBinding` is optional opaque
JSON, not a shared fixed-owner policy or permission to infer an owner from session contents.

## Durable Task Integration

The durable task integration lets you host Microsoft Agent Framework agents using the [Durable Task](https://github.com/microsoft/durabletask-python) framework so they can persist state, replay conversation history, and recover from failures automatically.

### Basic Usage Example

Before running this example, configure the endpoint and hub as a separate version-2 deployment with compatible workers and clients. Set `deployment_mode="isolated_v2"` only after the operator has verified that setup. A localhost endpoint alone is not isolation.

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIChatCompletionClient
from agent_framework_durabletask import DurableAIAgentWorker
from durabletask.worker import TaskHubGrpcWorker

# Connect only to the separately configured version-2 deployment
worker = TaskHubGrpcWorker(host_address="localhost:4001")
agent_worker = DurableAIAgentWorker(worker, deployment_mode="isolated_v2")

chat_client = OpenAIChatCompletionClient()
my_agent = Agent(client=chat_client, name="assistant")
agent_worker.add_agent(my_agent)
```

### History and retention settings

Registration appends durable history when no load-enabled primary exists, matching core's automatic
injection and reverse after-hook order. Only the exact built-in `InMemoryHistoryProvider` is replaced,
preserving `source_id`, `skip_excluded`, storage flags and `after_run_once_per_turn` when available.
Core 1.13 does not require that optional hint. Custom in-memory subclasses keep their hooks and
session transcripts. Their state is a protected floor, not managed by durable transcript eviction.
Custom durable-provider JSON state persists except the transient message buffer and position index.

External primaries and store-only sinks keep their storage policies, subject to the intentional
service-branch restriction below. Multiple load-enabled primaries or duplicate `source_id` values
are rejected. Registration does not enable compaction. The provider owns appends through core hooks,
with a final durable flush after all after-run callbacks. Only agents without a context pipeline use
direct entity transcript appends.

The default `DurableHistoryProvider.after_run()` calls public `save_messages()` for the selected request and response batches. An override can validate, transform or reject each batch. One core after-run hook can therefore produce two saves to preserve request/response provenance, rather than core's combined-batch cadence. Existing core and provider APIs are unchanged.

Public `Message.message_id` values remain unchanged in canonical `messageId`, even when repeated. The versioned `pythonHistoryIdentity` extension uses `pythonHistoryId` for internal occurrence identity instead of rewriting public IDs. Internal reconciliation metadata is not exposed as application message metadata. Raw unknown message/content fields remain inert JSON independently of Core projection.

The durable adapter invokes configured `CompactionProvider` hooks with internal unique IDs only for the hook duration. Normal model calls, other providers and audit sinks see public IDs. Compaction strategies must use the IDs presented inside those hooks for summary/source links, which belong to the internal reconciliation domain, not the public-ID domain. This does not promise transparent behavior for arbitrary custom hooks that depend on application IDs or substitute different input IDs.

Eager pruning and pressure eviction are independent. The matrix assumes no explicit provider
`prune_excluded` override.

| `retention` | `max_state_bytes=None` | Positive byte budget or `"backend_limit"` |
| --- | --- | --- |
| `"keep_all"` (default) | No transcript deletion (default) | Evict eligible oldest groups only under pressure |
| `"follow_compaction"` | Prune eligible compaction exclusions only | Prune exclusions, then evict under pressure if needed |

- `max_state_bytes` defaults to `None`. `"backend_limit"` resolves to 1,048,576 bytes (1 MiB) only
  for `DurableTaskSchedulerWorker`, not a generic `TaskHubGrpcWorker`. An unresolved limit is
  rejected. A positive integer sets an application budget, not a larger backend limit. `"auto"`
  is no longer a retention mode.
- `"backend_limit"` remains a non-normative Python-only convenience, outside the portable `None`
  or positive-integer contract. It does not account for transport overhead or imply shared-review
  agreement.
- Watermarks default to `high_watermark=0.85` and `low_watermark=0.70`, with
  `0 < low_watermark < high_watermark <= 1`. The whole serialized entity counts, including terminal
  results, completion receipts, session and ingestion state. Protected data can prevent a commit even after pruning.
- `response_delivery_window_seconds` defaults to `60` and must be a positive integer. Delivery
  expiry is independent of transcript retention.
- `add_agent()` and `configure_workflow()` accept overrides. Omitted budgets or `INHERIT` use the
  worker default. Explicit `None` disables that inherited budget. Workflow settings apply to newly
  registered agent nodes, including nested workflows.
- Explicit `prune_excluded=False` on `DurableHistoryProvider` disables eager pruning even with
  `follow_compaction`. It does not disable pressure eviction. Neither retention control configures
  an external store's retention policy.

As an alternative to the default registration above, use an unregistered worker, `my_agent` and an
existing named `workflow` to set an explicit byte budget while disabling it for workflow nodes.
The same verified isolated deployment and explicit operator acknowledgement are required.

```python
from agent_framework_durabletask import INHERIT

agent_worker = DurableAIAgentWorker(worker, deployment_mode="isolated_v2", max_state_bytes=800_000)
agent_worker.add_agent(my_agent, retention="follow_compaction", max_state_bytes=INHERIT)
agent_worker.configure_workflow(workflow, max_state_bytes=None)
```

`follow_compaction` only prunes exclusions produced by configured compaction. Without a strategy,
there are no exclusions to prune. Workflow `full`, `last_agent` and `custom` projection runs before
per-target delta transport. Custom filters must be synchronous, deterministic and side-effect-free,
but need not select monotonically increasing positions. Parallel `contextMessageIds` carry occurrence
hashes without rewriting public message IDs. Private forwarding provenance stays in internal
checkpoints, not application metadata. Outgoing context retains the full selected conversation plus
all response messages, not only the delta. Typed/cache-only requests, agent approval/HITL and
output-designated agents use the same workflow contract.

Generated agent outputs and intermediate events use portable response snapshots. External clients
receive base `AgentResponse` objects with JSON structured values, without importing worker-local
response models. Worker-side conditions and activities still receive the locally declared model.
Arbitrary custom activity outputs retain the existing checkpoint codec and its importable-type
requirements. Parent output designations also apply to direct child-workflow outputs.

### Service ownership, delivery and reset

Effective `store` follows run options, then agent defaults, then the client's `STORES_BY_DEFAULT`.
For example, `options={"store": False}` selects client-owned history even on a service-storing
client. Explicit `False` excludes saved or supplied service conversation IDs from that invocation
and its history hooks. A later service-owned run can reuse the saved service ID without importing
the intervening client-owned transcript. Switching branches does not migrate or merge history.
External and service-owned runs create no local request-message mirror.

On service-owned runs, durable deliberately suppresses **both load and store hooks** on the inactive
external/custom primary, including per-service-call persistence. Core 1.16 can still save to a
configured primary on such runs. This restriction avoids mixing service/client branches, but is not
universal unchanged-hook parity. Use a distinct store-only sink with its own `source_id` to audit
both branches. Its configured storage flags still apply.

A completed save through an external primary's default after-run hook affirms acceptance of the inputs actually saved. A completed service-owned `ChatResponse` likewise affirms its inputs, even if the invocation later fails. These observations preserve accepted-input evidence, not a successful invocation outcome or a distributed commit. Opaque custom `after_run()` overrides and interrupted saves do not permit inferred acknowledgement. A store-only audit sink is not the primary and cannot establish its acceptance.

`terminalResults` holds immutable original response envelopes independently of the transcript.
Every result and receipt requires `correlationId`, authoritative `outcome` (`succeeded` or `failed`)
and `completedAt`. Results contain the full shared `response`, including `messages`, optional
structured `value` and canonical metadata. A failed result also requires sanitized `error.code`
and `error.message`, while success forbids `error`. Explicit null and falsey structured values
remain present values. Receipt metadata is not injected into the original response payload.

`completionReceipts` additionally requires `resultState`. An `available` receipt has exactly one
matching result. An `unavailable` receipt has no result and requires `resultUnavailableAt`. Exact,
case-sensitive map keys must equal embedded correlation IDs. Result and receipt outcome,
completion time and optional `resultExpiresAt`, including its absence, must agree. Contradictions
are rejected before delivery or cleanup, even after expiry. These are runtime semantic checks,
not guarantees from schema validation alone.

`resultExpiresAt` is optional in the shared contract. When present, it cannot precede `completedAt`.
At or after that deadline, lookup reports completed-but-result-unavailable with the retained
`succeeded` or `failed` outcome, never an expired payload, pending work or permission to rerun.
Cleanup atomically removes the result and changes availability to `unavailable`, recording
`resultUnavailableAt` no earlier than completion and, for expiry, no earlier than the deadline.
Identity, outcome, completion time and expiry policy do not change. There is no v2 `unknown` outcome.
Absence of expiry does not authorize arbitrary removal or promise unlimited retention.

Version-2 lookup never reconstructs results or infers success from the transcript. No receipt means
only no recorded terminal completion, not proof that a request is pending. Acceptance-only and
fire-and-forget responses cannot establish completion. An approval response can complete its
invocation without proving that the guarded action executed.

Expiry is a logical deadline, not an idle timer. New runs, duplicates, reset and backend
`expire_responses` clean time-expired payloads without model/tool/provider execution for cleanup.
Idle physical cleanup needs an application-owned schedule or explicit backend signal/manual
operation. No public HTTP/MCP cleanup endpoint is generated. Expiry, cleanup and reset retain receipts.

Local reset clears session and transcript context but preserves live terminal results, completion
receipts and ingestion evidence. Normal delivery expiry still applies. Reset with a non-durable
custom/external primary raises `NotImplementedError` until provider-owned clearing is available.

Results and receipts must commit atomically with the operation's entity-local session, ingestion,
transcript and other control state, not as a separate delivery write. Only structured `previous_response_not_found` on a
service-owned run permits bounded retries, and only before a stream update, function execution or
service-session advancement. An invalid service conversation must fail without silently falling
back to a new conversation or local history. Provider-hook side
effects are not guaranteed safe or identical on retry. There is no generic non-streaming retry after
runtime failure. Only matching unsupported-stream `TypeError` before consumption negotiates fallback.
Final callbacks receive deep copies preserving Pydantic fields. Opaque SDK `raw_representation`
detachment is best effort and that field is omitted if it cannot be copied.
Callbacks and host write returns are not confirmation of persisted completion. Uncommitted model/tool
effects and external appends can repeat after failure. Applications need their own external
idempotency strategy. Completion receipts last until entity deletion and can exhaust capacity.
Whole-entity TTL/deletion also removes duplicate protection and needs an explicit late-duplicate
policy. A bounded receipt protocol and optional
retry-safe external-history adapters remain deferred, with no mandatory core API changes or
guarantee of a distributed transaction or exactly-once uncommitted effects.

### Retention telemetry

Both Python hosts use OpenTelemetry scope `agent_framework.durabletask`. The package directly
depends only on `opentelemetry-api` for these instruments. The SDK is a development dependency,
and applications own their meter provider, readers and exporters. The runtime configures none.

| Instrument | Kind | Unit |
| --- | --- | --- |
| `durable.retention.evaluations` | Counter | `{evaluation}` |
| `durable.retention.budget` | Histogram | `By` |
| `durable.retention.state.size` | Histogram | `By` |
| `durable.retention.removed_messages` | Counter | `{message}` |
| `durable.retention.removed_entries` | Counter | `{entry}` |
| `durable.retention.reclaimed_bytes` | Counter | `By` |
| `durable.retention.capacity_failures` | Counter | `{failure}` |
| `durable.retention.write_attempts` | Counter | `{attempt}` |
| `durable.retention.operations` | Counter | `{operation}` |

Attributes are bounded and apply only to the relevant instruments.

| Attribute | Values |
| --- | --- |
| `mechanism` | `eager`, `pressure` |
| `outcome` | Retention uses `below_threshold`, `staged`, `protected_floor`, `unreachable_target`, `protected`. Write/operation observations use `returned`, `failed`. |
| `commit_status` | `not_attempted`, `unknown` |
| `phase` | `before`, `after` |
| `stage` | `serialization`, `set_state` |
| `deletion_staged` | `true`, `false` |

No payloads or session, request or message IDs are recorded in these metrics. The budget is the
resolved whole-entity budget, and sizes describe serialized JSON at the retention boundary.
Removal counts and nonnegative reclaimed bytes describe staged changes, not detached trial plans
or committed deletion. Serialization failure leaves commit status `not_attempted`. A host
`set_state` return or failure leaves it `unknown`, since either can follow a staged write without
confirming persistence. Separate authoritative persisted-state readback is needed, paired with
subsequent model input when validating retention. Metrics do not change warm-state rollback or
make external effects transactional.

For more details, review the standalone [Durable Task samples](https://github.com/microsoft/agent-framework-durable-extension/tree/main/python/samples) and the full [Agent Framework Python documentation](https://github.com/microsoft/agent-framework/tree/main/python).
