# Get Started with Microsoft Agent Framework Durable Functions

[![PyPI](https://img.shields.io/pypi/v/agent-framework-azurefunctions)](https://pypi.org/project/agent-framework-azurefunctions/)

Please install this package via pip:

```bash
pip install agent-framework-azurefunctions --pre
```

Requires Python 3.10+ and `agent-framework-core>=1.13.0,<2`. The Durable Task dependency requires
`pydantic>=2.11,<3`. Full unit runs passed on Python 3.13/core 1.16, Python 3.13/core 1.13 and
Python 3.10/core 1.16. Pydantic 2.11 runtime validation remains blocked by dependency artifact
downloads. The offline lock, lint, typing and both package builds passed for this follow-up.
See [prototype validation](../../samples/README.md#prototype-validation)
for recorded results and limitations.

## Version 2 deployment warning

The settings below describe the [PR #59 prototype](https://github.com/microsoft/agent-framework-durable-extension/pull/59),
not an approved design or the contents of a published package. Design review belongs in
[ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88).
The outcome, retention telemetry and validation follow-up builds on prototype baseline `9b4550d`.
It does not establish shared-schema acceptance or a released package contract.
After ADR approval, the agreed implementation will be submitted as stacked PRs rather than merged
from this prototype as-is.

> **Breaking deployment and state contract.** `AgentFunctionApp` and standalone `create_agent_entity`
> require `deployment_mode="isolated_v2"`, or `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` when the
> argument is omitted/`None`. This is operator acknowledgement, not a handshake, security boundary
> or proof of isolation. Use a separate hub/deployment with compatible workers and all clients.
> Keep old workers and workflow histories on the old engine. The current .NET reader rejects version 2.

Only `schemaVersion="2.0.0"` is writable. Legacy `1.x.y` and supported later `2.x.y` state can be
read/round-tripped, but `run`, `reset` and `expire_responses` reject those layouts. No operation
silently upgrades legacy state. Rollback requires compatible version-2 workers, clients and workflow
protocol. Names are unchanged. Reusing an old `@name@key` on an empty new hub is not migration.

A matching version label does not prove layout compatibility. The shared Python reader rejects
known alternate `data.terminalResults` or `data.completionReceipts` containers instead of treating
their completed requests as new work. Unrelated optional metadata remains opaque, including nested
uses of those names. This guard is not a general format detector or a schema conversion.

Generated workflow start routes and internal child dispatch wrap new starts with workflow engine
version 2. Raw/legacy starts reject before revised actions execute. Native custom scheduling must use
public `wrap_workflow_input` for new instances. It does not authorize input or migrate old histories.

Both hosts expose privileged backend `AgentEntity.migrate`, supported by the pure
`migrate_legacy_state` helper. The request requires `source`, `sourceDigest`, `sourceSessionId`,
`destinationSessionId`, `migrationId` and `ownershipTransferId`, with optional `deliveryEvidence`
and `requireKnownOutcomes`.
Use an empty, separately addressed destination after quiescing and authorizing transfer from the
old owner. Nonempty scalar `ingestedPositions` requires a complete accepted-message journal,
including evicted inputs. `complete=True` is an operator assertion. Digest/max-position checks do
not prove authority/completeness or justify inferring a delivered prefix. Without the journal, keep
the old session on the old engine.

Only recorded responses receive legacy completion backfill and a delivery grace window. Surviving
transcript payloads may be partial, so absence of error content does not prove success. Existing
original mailbox records keep their payload and expiry. A missing matching receipt gets its
`completedAt` from the mailbox's `createdAt`, not migration time. `requireKnownOutcomes=True` on
the entity request, or `require_known_outcomes=True` on the helper, rejects imports without
trustworthy known outcomes. The default legacy-compatible path preserves unknown completion
evidence and duplicate suppression rather than inventing an outcome or rerunning completed work.
Whole-request digest idempotency prevents grace refresh after an exact retry, cold reload or
subsequent run. The original logical session ID is retained for external history. Migration does
not copy that store or move workflow action histories.
No generated HTTP/MCP migration endpoint is provided. These are prototype constraints, not an agreed
cross-runtime migration contract. See [ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88)
for the design discussion and [prototype validation](../../samples/README.md#prototype-validation)
for recorded checks and remaining gaps.

## Durable Agent Extension

The durable agent extension lets you host Microsoft Agent Framework agents on Azure Durable Functions so they can persist state, replay conversation history, and recover from failures automatically.

### Basic Usage Example

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
  `0 < low_watermark < high_watermark <= 1`. The whole serialized entity counts, including mailbox,
  completion, session and ingestion state. Protected data can prevent a commit even after pruning.
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

HTTP polling uses independent original response snapshots in `responseMailbox`, including
serializable metadata and structured `value`. Transcript pruning or reset cannot change those
results. New `completedCorrelations` receipts retain `completedAt` and `outcome` (`succeeded` or
`failed`) after payload expiry. Expired lookup returns `response_expired` with
`durable_status="already_completed"` and `durable_outcome` set to `succeeded`, `failed` or `unknown`.
Older timestamp-only receipts still suppress duplicates when the outcome is unknown. Cleanup can
backfill known outcomes from independent original mailboxes before removing them, even after the
delivery deadline. A possibly pruned legacy transcript without error content is not success evidence.

For expired delivery, HTTP returns 410. JSON includes top-level `outcome` and
`agent_response.additional_properties.durable_outcome`. Plain text carries `x-ms-durable-outcome`,
and the MCP error includes the invocation outcome. These additions do not modify retained original
response payloads or the standalone SDK API. Acceptance alone cannot create a new completion
receipt. A fresh response with no known invocation outcome raises before either delivery map
changes, while legacy-compatible receipts and fire-and-forget acceptance remain supported.

Expiry is a logical deadline, not an idle timer. New runs, duplicates and reset remove expired
payloads. Both hosts also expose backend `expire_responses` without model/tool/provider execution.
Idle physical cleanup needs an application-owned schedule or explicit backend signal/manual
operation. No public HTTP/MCP cleanup endpoint is generated. Completion receipts are never removed
by expiry, cleanup or reset.

Local reset clears session and transcript context but preserves live mailbox payloads, completion
receipts and ingestion evidence. Normal delivery expiry still applies. Reset with a non-durable
custom/external primary raises `NotImplementedError` until provider-owned clearing is available.

Entity-local state commits once per operation. Only structured `previous_response_not_found` on a
service-owned run permits bounded retries, and only before a stream update, function execution or
service-session advancement. Otherwise fail without restarting the conversation. Provider-hook side
effects are not guaranteed safe or identical on retry. There is no generic non-streaming retry after
runtime failure. Only matching unsupported-stream `TypeError` before consumption negotiates fallback.
Final callbacks receive deep copies preserving Pydantic fields. Opaque SDK `raw_representation`
detachment is best effort and that field is omitted if it cannot be copied.
Uncommitted model/tool effects and external appends can repeat after failure. Completion receipts
last until entity deletion and can exhaust capacity. A bounded receipt protocol and optional
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
