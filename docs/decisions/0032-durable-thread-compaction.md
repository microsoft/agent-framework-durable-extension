---
status: proposed
contact: ahmedmuhsin
date: 2026-07-27
deciders:
consulted:
informed:
---

# Thread Compaction for Durable Agents and Workflows

> **How to read this.** The decision comes first. The later sections record the Python prototype
> and the gaps it exposed. **.NET is not implemented yet.**
>
> **Naming.** .NET's `ChatHistoryProvider` and Python's `HistoryProvider` are the same concept. The
> decision sections use the .NET name, and the implementation sections use the Python one.

## Context and Problem Statement

Long-running **durable** agents and workflows accumulate conversation history in durable
storage and replay it on every turn. Durable agents persist a full `ConversationHistory` in
entity state (`AgentEntity` → `DurableAgentState`). Durable workflows persist inter-executor
messages (`AgentExecutor.full_conversation`) as checkpointed envelopes. An in-memory agent keeps its
history in process RAM, where it disappears when the process recycles. Durable history instead
survives restarts and is reloaded on later turns.

It helps to separate **three distinct pressures**, because they have different owners.

| Pressure | What bounds it | Same in core? | Owner |
| --- | --- | --- | --- |
| **Context window**, the model's max input per call | the model | **Yes**, identical in core and durable | Compaction (in-run filter) |
| **Token cost / latency**, resending history each turn | tokens billed / round-trip | **Yes**, same mechanism | Compaction (in-run filter) |
| **Storage capacity**, the cumulative persisted state | backend state-size limit | **No**, durable-only | Backend offload and durable retention |

The first two are per-operation and identical in both runtimes. The third is cumulative.
`ConversationHistory` is one blob appended to every turn and re-persisted whole, so it is bounded by
what the backend will store, and the two backends fail differently. **Durable Task Scheduler caps a
message at 1 MB.** The Azure Storage backend has no hard cap, because it compresses anything over
45 KB into a `<taskhub>-largemessages` blob, but it pays for size in CPU, I/O and memory. So one
backend stops working at the limit and the other degrades toward it, while a core process is bounded
only by RAM and resets on restart.

**Storage capacity is an infrastructure concern, not a context-window concern.** It is relieved
first by raising the ceiling where blob offload is available and only then by deleting. A tool for
bounding what the model reads is not a tool for bounding what the backend holds.

Core MAF already has a compaction system ([ADR-0019](https://github.com/microsoft/agent-framework/blob/main/docs/decisions/0019-python-context-compaction-strategy.md),
.NET `Microsoft.Agents.AI.Compaction`, Python `agent_framework._compaction`) with **two hooks**.

1. **In-run filter.** A `CompactionProvider` (`AIContextProvider`) / `compaction_strategy` runs
   before each model call. It is **non-lossy**. It filters the projection sent to the model and
   stores incremental group state in the `AgentSession.StateBag`, leaving the underlying store
   untouched. This hook works with **any** history provider, since it acts on the messages already
   loaded into the invocation context.
2. **Store reducer.** **Lossily** rewrites the stored conversation, applying the same strategies at
   the store instead of at the model call. Unlike the in-run filter, this hook is tied to a specific
   storage mechanism in both languages. .NET exposes an `IChatReducer` on `InMemoryChatHistoryProvider`
   only (bridged from any strategy by `strategy.AsChatReducer()`), and Python's
   `CompactionProvider.after_strategy` reads the messages out of session state. Neither offers it to
   a provider backed by anything else - see "Core Interface Gaps" below.

The durable layer benefited from **neither**, because `AgentEntity` **bypassed the history
provider**: it created a fresh session per operation (so the StateBag - and any history provider
store or reducer in it - was discarded) and fed `ConversationHistory` directly as input messages.
Both the in-run filter's incremental state and the store reducer were thrown away every turn.

The goal is **configuration parity**: a user's core compaction config must carry over to a durable
entity or workflow **unchanged**, reusing the same strategies and hooks on the durable runtime,
without a parallel durable compaction API. Storage retention is a separate deployment policy because
it has no meaning for an in-memory agent.

**How should core compaction be reused on the durable runtime, in both .NET and Python, so the same
agent configuration bounds model input and the persisted store can be bounded separately?**

## Decision Drivers

- **Configuration parity.** The same core compaction config (strategies, `CompactionProvider`,
  `IChatReducer`) must apply unchanged when moving core → durable entity → durable workflow. No
  parallel durable-only API.
- **Reuse existing core hooks.** Do not reinvent triggers, strategies or grouping. Reuse the in-run
  filter and the store reducer.
- **Separate storage capacity from context management.** Bound the model input with compaction
  (parity with core), raise backend capacity where possible, and use observable deletion only as a
  fallback.
- **Deleting is a last resort, and never silent.** Entity state is a state bag, not an immutable
  system of record, so deleting from it is legitimate. But deletion should happen only when capacity
  demands it, should remove no more than capacity demands, and should always be observable.
- **Determinism and idempotency.** Durable entity operations can be retried, so a lossy reducer
  (especially LLM summarization) must not corrupt or diverge persisted state across retries.
- **Message-list correctness.** Preserve atomic groups (assistant tool-call plus tool-result, and
  reasoning pairings) so the model input stays valid.
- **Cover both surfaces.** Durable agents **and** durable workflows, in **both** languages. Core's
  compaction system is agent-level, so a workflow agent node inherits it unchanged. The conversation
  chained *between* nodes is governed by `AgentExecutor`'s `context_mode` / `context_filter` seam,
  which is a plain callable rather than the compaction system. That difference is real and is called
  out rather than papered over.
- **Defer when the model provider owns the conversation.** When the chat client keeps history on the
  service, the client holds no history for core compaction. "Service" here means the model provider,
  not the durable entity. The entity's own durable record still follows its retention policy.

## Considered Options

- **Option 1, in-run filter only (rejected).** Reuse the agent's core compaction without changing
  durable history. This bounds model input but not persisted state.
- **Option 2, bespoke pre-write compaction in the agent entity.** Add durable-specific code that
  compacts `ConversationHistory` inside the entity operation before checkpoint. Rejected because it
  duplicates core's store-reducer behavior.
- **Option 3, on-storage maintenance compaction (deferred).** Compact persisted history from a
  separate entity operation. This may suit expensive summarization but does not prevent in-turn
  growth.
- **Option 4, workflow context projection (chosen).** Honor `AgentExecutor.context_mode` and
  `context_filter` for the `full_conversation` chained between executors.
- **Option 5, auto-derive a durable store reducer (rejected as default).** Derive a lossy reducer
  from a configured in-run strategy. The explicit equivalent is `follow_compaction`. The default
  must also protect agents with no compaction strategy.
- **Option 6, durable store as a `ChatHistoryProvider` (chosen).** Back the durable entity's
  persisted conversation with a core `ChatHistoryProvider` implementation, so both core hooks apply
  on the durable runtime from the user's unchanged configuration. The in-run filter runs in the
  agent pipeline (L1), and a user-configured reducer or strategy can bound the durable store (L2,
  opt-in). External history providers also rejoin the context pipeline, but the entity still keeps
  its own conversation record, so they do not currently remove the need for retention.
- **Option 7, offload large payloads to blob storage (chosen where available).** Raise the ceiling
  instead of reducing content, using the Durable Task Scheduler [large payload
  extension](https://learn.microsoft.com/azure/durable-task/scheduler/durable-task-scheduler-large-payloads).
  Non-lossy, and the same technique the Azure Storage backend has always used internally.

## Decision Outcome

Chosen option: **Option 6, express durable conversation storage as a core `ChatHistoryProvider`**,
combined with workflow context projection (Option 4). The two solve different surfaces.

| Surface | Mechanism | Behavior |
| --- | --- | --- |
| **L1, agent context** | The user's configured core `CompactionProvider` / `compaction_strategy` | Non-lossy projection of model input. The same agent configuration works durably. |
| **L2, eager store pruning** | Core compaction annotations plus `retention="follow_compaction"` | Opt-in deletion of messages the user's strategy excluded. Python only today because .NET is blocked by duplicated compaction state (gap 4). |
| **L3, workflow context** | Existing `AgentExecutor.context_mode` / `context_filter` projection | Controls `full_conversation` passed between executors. This is not a core compaction hook. |
| **Capacity fallback** | Durable retention | Bounds entity state independently of whether compaction is configured. |

This gives agent-level **configuration parity**, not byte-for-byte parity in every workflow cycle.
Durable workflow nodes intentionally deduplicate repeated upstream context before persisting it. The
L3 section explains the measured difference.

Capacity is handled in this order: raise the ceiling non-lossily where blob offload is available,
honor an explicit `follow_compaction` choice, then evict under pressure. An exclusion normally means
only "do not send this to the model". It means "delete this" only under `follow_compaction`.

### Retention

| Mode | Behavior |
| --- | --- |
| `keep_all` | Never delete. The entity may reach the backend limit and fail. |
| `auto` **(default)** | Delete only under storage pressure, targeting the low watermark. |
| `follow_compaction` | Delete whatever compaction excluded every turn, then use the same pressure eviction as `auto` if the remaining state is still too large. |

**How pressure eviction works.** After the turn is recorded and before the state is persisted, the
entity measures its serialized state. `auto` uses only this path. `follow_compaction` uses it after
eager pruning. Below the high watermark, nothing happens. Above it, the entity targets the low
watermark using detached message copies with existing exclusions cleared and
`TokenBudgetComposedStrategy(strategies=[])`. Clearing exclusions makes the budget reflect what is
stored, while the empty strategy list bypasses the user's context policy and uses core's
deterministic oldest-group fallback. System messages, atomic tool groups, and the newest exchange
are protected. The entity remeasures after each pass and logs what it removes.

`max_state_bytes` defaults to `1_048_576`. High and low watermarks of `0.85` and `0.70` provide
hysteresis and room for estimation error. Measuring a 1 MB prototype state took about 8 ms.

**Why not simply reduce the store by default.** A default-on reducer only helps agents that already
configured compaction, because nothing else marks messages excludable. It would leave every other
configuration exposed while changing behavior for only a subset of users.

**Why not rely on blob offload alone.** It raises the ceiling roughly tenfold and does not remove it.
It is also unreachable on the Durable Functions Python path today (gap 6). Retention is therefore
the fallback that works on every host.

**Service-managed model context** is outside compaction scope, mirroring ADR-0019. When the model
provider owns the conversation, the client holds no history to compact. Entity retention still
applies. See "Service-managed conversations".

**Why workflows largely come "for free."** Durable workflow agent execution
(`DurableExecutorDispatcher.ExecuteAgentAsync`) runs an agent through the same
`DurableAIAgent → AgentEntity → inner agent` path as standalone durable agents, so **L1, L2 and
retention are inherited by workflow agent executors**. The workflow's own `full_conversation` between
executors does not pass through the agent, so it needs the separate **L3** hook.

### Consequences

- **Configuration parity.** Existing agent compaction configuration works durably without changing
  the agent. Retention does not choose the current model projection.
- **Broad capacity protection.** Because pressure eviction lives in the entity, it covers external
  providers, service-managed agents, and agents with no context pipeline. A single oversized newest
  exchange can still fail because the current result is never evicted.
- **Proportionate deletion.** Under `auto`, the budget decides how much to remove. The user's context
  strategy does not.
- **Larger entity change.** The history-provider design must preserve response polling and the
  entity's conversation record.
- **Python-only eager pruning.** .NET would duplicate the transcript if it persisted current
  compaction state (gap 4), so a .NET implementation could use pressure retention but not L2 yet.
- **Core workarounds.** Python must publish and reconcile a working buffer because core binds
  store-side compaction to session state (gaps 1 and 2).
- **Threshold behavior.** `auto` changes behavior only near the capacity limit. This is less uniform
  than always pruning, but it avoids changing unaffected conversations.

### Validation

**Done (Python).** Unit tests cover provider substitution, annotation round-trips, synthetic summary
insertion and reconciliation, all retention modes, session persistence, and workflow projection and
deduplication. The retention test drives a real agent through twenty turns against a reduced budget.
`keep_all` is the control proving the same run exceeds it. Scheduler integration covers persisted
annotations and message ids, external-provider session identity, schema conformance, and downstream
workflow context.

**Outstanding.** Not covered yet.

- Retention crossing the real scheduler limit against a live backend, rather than a reduced budget
  in process.
- The .NET realization and its schema parity (gap 3), and the .NET compaction-state blocker (gap 4).
- Blob offload (Option 7) against a real scheduler. It remains unreachable through Durable Functions
  Python 1.x and the 2.x preview (gap 6).
- Idempotency of an LLM-based reducer across simulated entity retries.

## Cross-Cutting Design Details

- Honor the user's configured reducer trigger. Durable registration must not change compaction
  cadence.
- Reuse core grouping so tool-call/result and reasoning groups remain atomic.
- Pressure eviction is deterministic and uses the estimator tokenizer without a model call. Any
  future LLM reducer must give summaries stable identities and be tested across retries.
- The durable history provider belongs in `AgentEntity`. Workflow projection belongs at the
  existing `AgentExecutor.context_mode` / `context_filter` seam.

## Core Interface Gaps for Pluggable History Providers

Prototyping the Python `DurableHistoryProvider` surfaced places where the current contracts assume a
*session-state-backed* history provider. They are recorded here because they affect **any** provider
whose store is not session state (Cosmos, Valkey, durable), not just this one. The prototype works
around them, but the cleaner fix is upstream.

1. **Store-side compaction is bound to session state rather than to the provider.** `CompactionProvider`
   has two hooks, but only `before_strategy` works with any provider because it acts on invocation
   context. `after_strategy` mutates `session.state[history_source_id]["messages"]` and assumes that
   mutation rewrites storage. External providers can therefore bound model input but cannot use
   core to rewrite their stores. .NET similarly exposes `IChatReducer` only on
   `InMemoryChatHistoryProvider`.

   *Workaround:* the durable provider publishes a working buffer under the session-state key core
   expects. *Upstream fix:* put store-rewrite compaction on the provider abstraction.

2. **`save_messages()` is append-only.** The other half of the same open question. It receives only
   new messages, so changes to existing messages and inserted summaries have no path back to storage.
   *Workaround:* the durable provider reconciles its working buffer **by `message_id`** during
   `after_run`. *Upstream fix:* add an explicit replace/flush operation alongside append.

3. **Message-level metadata was not persisted (durable schema).** Python wrote
  `extension_data` asymmetrically, so annotations disappeared on round-trip. This is fixed. The
   shared schema now declares `messageId` and `extensionData` as round-trip-required, describes
   `session` as runtime-discriminated, and has a conformance test. A validator would not previously
  have rejected these fields because the schema permits extra properties. The defect was an
   under-declared contract.

   .NET still loses `ChatMessage.AdditionalProperties` and `MessageId` in
   `FromChatMessage`/`ToChatMessage`. Its `[JsonExtensionData]` property is only an overflow bucket for
   unmapped JSON. `ChatMessage.MessageId` **does** exist in the pinned package and needs mapping.
   Exclusions themselves live on `CompactionMessageGroup.IsExcluded`, while
   `AdditionalProperties` carries the `_is_summary` marker, so mapping both fields is necessary but
   not sufficient for .NET compaction parity.

4. **.NET compaction state cannot be persisted without duplicating the transcript.** This is the
   blocker behind "L2 is Python-only today". `CompactionProvider.State` is documented as living in
   `AgentSession.StateBag`, holds `List<CompactionMessageGroup>`, and each group serializes its full
   `ChatMessage` objects. Every run rewrites it wholesale. That leaves three unappealing choices for a
   durable provider that also persists the session:

   | Choice | Consequence |
   | --- | --- |
   | Persist the session | The conversation is stored twice, in `ConversationHistory` and again in the state bag, so entity state roughly doubles instead of being bounded |
   | Omit the provider state | Exclusions and summaries are discarded and summarization can re-run |
   | Return only included messages | `CompactionMessageIndex.Update()` sees a trimmed front and rebuilds from scratch, losing the incremental state |

   None of this is inherent to the history-provider approach. It resolves if core can persist
   lightweight compaction metadata keyed by `MessageId` rather than whole message copies. Until then
  a .NET implementation can bound entity state through the retention path, which is independent of
  `CompactionProvider`.

5. **Provider cadence splits under per-service-call persistence.** With
   `require_per_service_call_history_persistence=True`, history providers run per model call while
   `CompactionProvider` remains once per run. Compaction can then annotate after the last history
   flush, delaying persistence until the next flush. Only `HarnessAgent` enables this today, so the
   gap is latent.

6. **Blob offload is unreachable on the Durable Functions Python path.** Not a core gap but an
   upstream gap. Version 1.x, which this package pins (`>=1.3.1,<2`), does not use the durabletask
   SDK, so it has no Python-side payload-store seam. The 2.x previews (`2.0.0b1` and `2.0.0b2`, Python
   3.13+) depend on `durabletask>=1.9.0`, but `DurableFunctionsWorker` and
   `DurableFunctionsClient` do not expose the base types' `payload_store` parameter. The direct
   durabletask path does. *Upstream fix:* expose `payload_store` on both Functions types.

Two more core gaps are described where they matter: the process-local state-type registry under
session persistence, and the lack of a public resolved history-ownership decision under
service-managed conversations.

## L3 Realization: Workflow Context Parity

In-process workflows give a downstream `AgentExecutor` the upstream conversation through
`AgentExecutorResponse.full_conversation`, governed by `context_mode` (`full` | `last_agent` |
`custom` + `context_filter`). The durable orchestrator previously flattened that to the **last
message's text**, so a downstream agent lost everything earlier nodes produced.

Agent-level compaction needs no workflow-specific work: `AgentExecutor` passes its own session to
`agent.run()`, so the agent's `CompactionProvider` runs normally. Inter-executor context has no core
compaction hook. Durable instead honors the existing `context_mode` and invokes `context_filter` for
`custom` mode, then sends the projection as `RunRequest.context_messages`. Those messages become part
of the request entry and are visible to agent-level compaction.

Cycles need deduplication because a node receives the accumulated upstream conversation again on
each visit. The orchestrator stamps each forwarded message as `wf_{executor}_{position}`. The entity
stores the highest ingested position per executor and drops older positions, keeping the newest
message as input when everything repeats. Per-executor watermarks are required because fan-out
branches can share a position.

Stored-id comparison is insufficient: retention removes old ids, after which a cycle would re-ingest
exactly what was evicted and oscillate instead of converging. The small position map survives
deletion. Once content is evicted, the node no longer sees it. Re-ingesting it would defeat
retention.

This intentionally differs from core in one measured case. On the third visit of a `full`-mode
cycle, core in-process sends 11 messages with repeated context while durable sends 8 after dedup. In
`last_agent` mode they are identical. Each durable node also keeps history keyed by workflow instance
and executor, so its memory survives restarts independently of the workflow envelope.

## Zero-Configuration Registration

Registration must not require edits to an agent that already works in core. The entity therefore
substitutes history at construction time. It shallow-copies the agent when substitution is needed,
so the caller's instance remains unchanged.

| User configured | Durable behavior |
| --- | --- |
| Nothing | Inject a durable history provider, using the `source_id` core's auto-injected provider would have, so default-wired compaction still resolves. No compaction by default (same as core). |
| `InMemoryHistoryProvider` (± compaction) | Replace with the durable provider, **preserving `source_id` and `skip_excluded`** so any attached `CompactionProvider` keeps working untouched. |
| Cosmos / Redis / file / custom provider | **Leave alone.** The user chose where their conversation lives, and durable still supplies execution durability. |
| Service-managed history | **Leave alone.** The model service owns the conversation. Decided by core's precedence, explicit `store` first and then the client's `STORES_BY_DEFAULT`. |
| Agent without the core context pipeline | **Leave alone.** Falls back to replaying persisted history. |

Preserving `source_id` is the load-bearing detail. `CompactionProvider` locates history through
`history_source_id` (default `"in_memory"`), so a provider swapped in under the same id is invisible
to the rest of the configuration. An explicit `DurableHistoryProvider` takes precedence.

### Entity Context Ownership

1. **Who supplies conversation context?** If the agent exposes core's context-provider pipeline,
   the providers do, so the entity passes a session and delivers **only the new messages**. This
   holds whether history lives in durable state, an external store, or the model service.
2. **Who bounds entity state?** Retention does, for every configuration, because the entity records
  the conversation even when another provider owns model context.

The entity therefore replays its own persisted history in exactly one case, an agent that does not
expose the context pipeline. Passing a session re-engages external providers and core's in-run
filter. It does not let core rewrite an external store (gap 1). The session id is derived from the
full entity identity, name plus key, so workflow nodes cannot share an external-provider key.

### The session is persisted, not just its conversation id

Providers use session state for data that must survive turns, including pending approvals. Because
the entity creates a session per operation, it persists the **whole serialized session** rather than
selecting fields. Two details prevent duplication and type loss:

- The service-issued conversation id needs no bespoke field of its own - it is already part of
  `AgentSession.to_dict()`.
- The durable history provider's own slice is **excluded** before persisting. It is derived from
  `conversationHistory`, so storing it would duplicate the transcript.

Restore applies the stored state onto a session created by the agent's own `create_session()`, so
the agent's session type is preserved. Core's state-type registry is process-local, so the entity
pre-registers serializable types already loaded in the process before restore. Pydantic state remains
a core gap because broad subclass discovery would be collision-prone.

### Service-managed conversations

When the model service stores the conversation, it identifies the thread with an id. The entity
creates a fresh session per operation, so that id is **persisted in durable state and restored on
the next turn** as part of the serialized session. Without it, every turn would start a new thread.

Whether the service owns history is decided with **core's precedence, not the client class alone**.
An explicit `store` in the agent's options wins, and only when it is unset does the client's
`STORES_BY_DEFAULT` apply. This matters because clients that store by default (such as the Responses
API) are routinely put back into client-side mode with `store=False`. Consulting only
`STORES_BY_DEFAULT` would leave such an agent with a plain in-memory provider that the durable
runtime never persists, silently losing the conversation between turns.

Core resolves this rule inside `Agent._run` and does not expose the result, so this layer
**re-derives it** and can drift if core changes. *Upstream fix:* expose the resolved decision. The
integration sample covers `store=False` against a store-by-default client.

### Retention is a deployment policy, not agent configuration

Compaction annotates, it does not delete. Deletion is configured at **registration** (an app-level
default with a per-agent override) rather than on the agent, so the agent definition stays portable:
the same agent runs in-memory where retention has no meaning. `auto` applies no context policy, but
deletion necessarily shortens future available history. Only `follow_compaction` treats a compaction
exclusion as permission to delete. Under `auto`, the storage budget alone chooses what is removed.

## Out of Scope: Entity Lifetime

Idle TTL and cleanup bound how many abandoned entities remain. They do not bound an actively used
entity because each interaction extends its lifetime. Cross-language TTL parity is a separate
decision.

## More Information

- Builds on [ADR-0019](https://github.com/microsoft/agent-framework/blob/main/docs/decisions/0019-python-context-compaction-strategy.md) (context compaction strategy),
  which defines the in-run / pre-write / on-existing-storage compaction points and the atomic-group
  constraint.
- Core reference mechanisms reused: `CompactionProvider` (in-run filter), `InMemoryChatHistoryProvider`
  + `IChatReducer` (store reducer), `strategy.AsChatReducer()` bridge, and the existing external
  `ChatHistoryProvider` implementations (`CosmosChatHistoryProvider`, `ValkeyChatHistoryProvider`).
- Relevant durable code: `AgentEntity` and `DurableAgentState` (durable agents),
  `DurableExecutorDispatcher.ExecuteAgentAsync` (durable workflow agent execution), and
  `AgentExecutor` (`context_mode` / `context_filter`, `full_conversation`).
