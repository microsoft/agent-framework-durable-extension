# Get Started with Microsoft Agent Framework Durable Task

[![PyPI](https://img.shields.io/pypi/v/agent-framework-durabletask)](https://pypi.org/project/agent-framework-durabletask/)

Please install this package via pip:

```bash
pip install agent-framework-durabletask --pre
```

Requires Python 3.10+, `agent-framework-core>=1.13.0,<2` and `pydantic>=2.11,<3`.
The full unit suite passed on Python 3.13/core 1.16, Python 3.13/core 1.13 and Python 3.10/core 1.16.
Pydantic 2.11 runtime validation remains blocked by dependency artifact downloads.
The offline lock, lint, typing and both package builds passed for this follow-up.
See [prototype validation](../../samples/README.md#prototype-validation) for recorded results and limitations.

## Version 2 deployment warning

The settings below describe the [PR #59 prototype](https://github.com/microsoft/agent-framework-durable-extension/pull/59),
not an approved design or the contents of a published package. Design review belongs in
[ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88).
The outcome, retention telemetry and validation follow-up builds on prototype baseline `9b4550d`.
It does not establish shared-schema acceptance or a released package contract.
After ADR approval, the agreed implementation will be submitted as stacked PRs rather than merged
from this prototype as-is.

> **Breaking deployment and state contract.** `DurableAIAgentWorker` requires
> `deployment_mode="isolated_v2"`, or `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` when the argument
> is omitted/`None`. This is operator acknowledgement, not a handshake, security boundary or proof
> of isolation. Use a separate hub/deployment with compatible workers and all clients. Keep old
> workers and workflow histories on the old engine. The current .NET reader rejects version 2.

Only `schemaVersion="2.0.0"` is writable. Legacy `1.x.y` and supported later `2.x.y` state can be
read/round-tripped, but `run`, `reset` and `expire_responses` reject those layouts. No operation
silently upgrades legacy state. Rollback requires compatible version-2 workers, clients and workflow
protocol. Names are unchanged. Reusing an old `@name@key` on an empty new hub is not migration.

A matching version label does not prove layout compatibility. The reader rejects known alternate
`data.terminalResults` or `data.completionReceipts` containers instead of treating their completed
requests as new work. Unrelated optional metadata remains opaque, including nested uses of those
names. This guard is not a general format detector or a conversion between proposed schemas.

`DurableWorkflowClient` and internal child dispatch wrap new starts with workflow engine version 2.
Raw/legacy starts reject before revised actions execute. Native custom scheduling must use public
`wrap_workflow_input` for new instances. It does not authorize input or migrate old action histories.

Both hosts expose privileged backend `AgentEntity.migrate`, supported by the pure
`migrate_legacy_state` helper. The request requires `source`, `sourceDigest`, `sourceSessionId`,
`destinationSessionId`, `migrationId` and `ownershipTransferId`, with optional `deliveryEvidence`
and `requireKnownOutcomes`.
Use an empty, separately addressed destination after quiescing and authorizing transfer from the
old owner. Nonempty scalar `ingestedPositions` requires a complete accepted-message journal,
including evicted inputs. `complete=True` is an operator assertion. Digest/max-position validation
does not prove authority or completeness, and no delivered prefix is inferred. Without that journal,
keep the old session on the old engine.

Only recorded responses receive legacy completion backfill and a delivery grace window. Surviving
transcript payloads may be partial, so absence of error content does not prove success. Existing
original mailbox records keep their payload and expiry. If a matching receipt is missing, its
`completedAt` comes from the mailbox's `createdAt`, not migration time. `requireKnownOutcomes=True`
on the entity request, or `require_known_outcomes=True` on the helper, rejects imports without
trustworthy known outcomes. The default legacy-compatible path preserves unknown completion
evidence and duplicate suppression rather than inventing an outcome or rerunning completed work.
Whole-request digest idempotency prevents grace refresh after an exact retry, cold reload or
subsequent run. Migration retains the original logical session ID for external history and does
not copy that store or migrate workflow histories. No generated HTTP/MCP migration endpoint is
provided. These are prototype constraints, not an agreed
cross-runtime migration contract. See [ADR PR #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88)
for the design discussion and [prototype validation](../../samples/README.md#prototype-validation)
for recorded checks and remaining gaps.

## Durable Task Integration

The durable task integration lets you host Microsoft Agent Framework agents using the [Durable Task](https://github.com/microsoft/durabletask-python) framework so they can persist state, replay conversation history, and recover from failures automatically.

### Basic Usage Example

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
  `0 < low_watermark < high_watermark <= 1`. The whole serialized entity counts, including mailbox,
  completion, session and ingestion state. Protected data can prevent a commit even after pruning.
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

`responseMailbox` holds independent original serializable response snapshots, including metadata
and structured `value`, rather than rebuilding results from the mutable transcript. New
`completedCorrelations` receipts retain `completedAt` and the invocation `outcome`, either
`succeeded` or `failed`. After delivery expiry, lookup returns `durable_status="already_completed"`
with `response_expired` and `additional_properties["durable_outcome"]` set to `succeeded`, `failed`
or `unknown`. An older timestamp-only receipt with no trustworthy outcome still prevents
reinvocation. Cleanup can backfill a known outcome from an independent original mailbox before
removing its payload, even if the delivery deadline has passed. Version-2 lookup never uses the
possibly pruned transcript to infer success or reconstruct a result.

The standalone SDK API is unchanged. Retained original responses are returned unmodified rather
than having receipt metadata injected into their payloads. Acceptance alone is not completion.
A fresh `record_response()` with no known invocation outcome raises before changing either delivery
map. Legacy-compatible receipts and fire-and-forget acceptance behavior remain supported. An
approval response can complete its invocation without proving that the guarded action executed.

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
