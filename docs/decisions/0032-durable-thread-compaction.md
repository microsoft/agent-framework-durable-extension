---
# These are optional elements. Feel free to remove any of them.
status: proposed
contact: ahmedmuhsin
date: 2026-07-27
deciders:
consulted:
informed:
---

# Thread Compaction for Durable Agents and Workflows

> **How to read this.** Everything through "Pros and Cons of the Options" is the proposed design.
> Everything after it records how that design was prototyped in Python and what the prototype
> surfaced. **.NET is not implemented yet.**
>
> **Naming.** .NET's `ChatHistoryProvider` and Python's `HistoryProvider` are the same concept. The
> decision sections use the .NET name, and the implementation sections use the Python one.

## Context and Problem Statement

Long-running **durable** agents and workflows accumulate conversation history in durable
storage and replay it on every turn. Durable agents persist a full `ConversationHistory` in
entity state (`AgentEntity` → `DurableAgentState`). Durable workflows persist inter-executor
messages (`AgentExecutor.full_conversation`) as checkpointed envelopes. An in-memory agent keeps its
history in process RAM, where it disappears when the process recycles. This history is instead
**persisted, reloaded every turn, and permanent**.

It helps to separate **three distinct pressures**, because they have different owners.

| Pressure | What bounds it | Same in core? | Owner |
| --- | --- | --- | --- |
| **Context window**, the model's max input per call | the model | **Yes**, identical in core and durable | Compaction (in-run filter) |
| **Token cost / latency**, resending history each turn | tokens billed / round-trip | **Yes**, same mechanism | Compaction (in-run filter) |
| **Storage capacity**, the cumulative persisted state | backend state-size limit | **No**, durable-only | Storage backend (built-in limit or external store) |

The first two are per-operation and identical in both runtimes. The third is cumulative.
`ConversationHistory` is one blob appended to every turn and re-persisted whole, so it is bounded by
what the backend will store, and the two backends fail differently. **Durable Task Scheduler caps a
message at 1 MB.** The Azure Storage backend has no hard cap, because it compresses anything over
45 KB into a `<taskhub>-largemessages` blob, but it pays for size in CPU, I/O and memory. So one
backend stops working at the limit and the other degrades toward it, while a core process is bounded
only by RAM and resets on restart.

**Storage capacity is an infrastructure concern, not a context-window concern.** It is relieved
first by raising the ceiling (blob offload, an external store) and only then by deleting. The two
are kept separate throughout this document, because a tool for bounding what the model reads is not
a tool for bounding what the backend holds.

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
without a parallel durable-only API.

**How should core compaction (both hooks) be reused on the durable runtime, in both .NET and
Python, so that the model input is bounded identically to core and the persisted store can be
bounded when the user opts into it?**

## Decision Drivers

- **Configuration parity.** The same core compaction config (strategies, `CompactionProvider`,
  `IChatReducer`) must apply unchanged when moving core → durable entity → durable workflow. No
  parallel durable-only API.
- **Reuse existing core hooks.** Do not reinvent triggers, strategies or grouping. Reuse the in-run
  filter and the store reducer.
- **Separate storage capacity from context management.** Bound the model input with compaction
  (parity with core), and relieve persisted-storage capacity with infrastructure (backend limits,
  external stores) rather than by silently trimming.
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
  service (a `ConversationId` or `service_session_id` is set), the client holds nothing to compact.
  "Service" here means the model provider. The durable entity is not the service in this sense, even
  though it is also storage someone else manages.

## Considered Options

- **Option 1, in-run filter only.** Register the core `CompactionProvider` / `compaction_strategy`
  on the inner agent and change nothing else in the durable layer.
- **Option 2, bespoke pre-write compaction in the agent entity.** Add durable-specific code that
  compacts `ConversationHistory` inside the entity operation before checkpoint.
- **Option 3, on-storage maintenance compaction.** Compact persisted history from a separate
  entity signal or operation, decoupled from the request path.
- **Option 4, workflow-level compaction hook.** Apply a strategy at the `AgentExecutor`
  `context_mode` / `context_filter` boundary that governs the `full_conversation` chained between
  agent executors.
- **Option 5, auto-derive a durable store reducer.** When only an in-run filter is configured,
  automatically derive a lossy store reducer (`strategy.AsChatReducer()`) so durable storage is
  bounded even without an explicit reducer.
- **Option 6, durable store as a `ChatHistoryProvider` (chosen).** Back the durable entity's
  persisted conversation with a core `ChatHistoryProvider` implementation, so both core hooks apply
  on the durable runtime from the user's unchanged configuration. The in-run filter runs in the
  agent pipeline (L1), and a user-configured reducer or strategy bounds the store (L2, opt-in). The
  same seam makes external storage backends (Cosmos, Valkey, blob) pluggable for capacity.
- **Option 7, offload large payloads to blob storage.** Raise the ceiling instead of reducing the
  content, using the Durable Task Scheduler [large payload
  extension](https://learn.microsoft.com/azure/durable-task/scheduler/durable-task-scheduler-large-payloads).
  Non-lossy, and the same technique the Azure Storage backend has always used internally.

## Decision Outcome

Chosen option: **Option 6, express durable conversation storage as a core `ChatHistoryProvider`**,
combined with the workflow hook (Option 4). This makes core's two compaction hooks apply on the
durable runtime with **no config change**, and cleanly separates context management from storage
capacity.

Compaction applies at **three layers**, mapped directly onto the core hooks. Retention, described
below, is a fourth and separate concern: it bounds storage and never touches the model input.

| Layer | Core mechanism reused | Lossy? | Role |
| --- | --- | --- | --- |
| **L1, in-run filter** | `CompactionProvider` in the agent pipeline | No | Always-on. Bounds the **model input** (context window, token cost). Identical to core. |
| **L2, store reducer** | the store-rewrite hook applied to the durable provider's store | Yes | **Opt-in** (`follow_compaction`). Bounds the **persisted store** from the user's own strategy. Python only today, since the hook is bound to session state upstream and .NET cannot persist its compaction state without duplicating the transcript (see "Core Interface Gaps"). |
| **L3, workflow hook** | the same strategy at the `AgentExecutor` `context_filter` seam | Yes | Bounds the inter-executor `full_conversation`. A plainer seam than L1 and L2. |

**Two accumulation surfaces.**

| Surface | Where it accumulates | Covered by |
| --- | --- | --- |
| **In-agent** | the agent's model input, and the persisted `AgentEntity` store | L1 for the model input, retention for the store, L2 when opted into |
| **Inter-executor (workflow)** | `AgentExecutor.full_conversation`, checkpointed as envelopes | L3 |

**Strict parity for context, capacity handled separately.** Durable honors exactly the compaction
hooks the user configured, so the model input is identical in both runtimes. It does **not** infer a
storage policy from a context policy: an exclusion means "do not send this to the model", never
"this is safe to delete". Those are two of the three pressures above and conflating them would let a
token-cost decision quietly destroy records the user never agreed to lose.

Capacity is therefore its own axis, with three answers applied in order.

1. **Raise the ceiling first.** Blob offload (Option 7) or an external provider. Non-lossy.
2. **Honor an explicit retention choice.** `follow_compaction` is the user authorizing exclusion to
   mean deletion (Option 5, in opt-in form).
3. **Evict as a last resort.** Under storage pressure, delete the minimum needed to stay alive.

### Retention

One setting, because a single question ("who deleted my message?") should have a single answer.

| Mode | Behavior |
| --- | --- |
| `keep_all` | Never delete. The entity may reach the backend limit and fail. The honest choice when the complete record matters more than availability. |
| `auto` **(default)** | Delete only under storage pressure, and only down to the low watermark. |
| `follow_compaction` | Delete whatever compaction excluded, every turn. The previous `prune_history=True`. |

**How `auto` works.** After the turn is recorded and before the state is persisted, the entity
serializes the state and measures it. Under the high watermark, nothing happens. Over it, the entity
builds a detached view of the stored messages **with context exclusions cleared**, hands it to core's
`TokenBudgetComposedStrategy` with no strategies of its own, and deletes whatever that marks.

Each part earns its place.

- **The entity triggers it, not the history provider.** `AgentEntity` appends to
  `ConversationHistory` in every configuration, including external providers, service-managed agents
  and agents with no context pipeline. A trigger inside the provider would protect only the
  configurations that already have `follow_compaction` available, and miss the ones with no other
  mitigation.
- **Exclusions are cleared on the detached view.** The strategy budgets over *included* messages, so
  leaving a user's exclusions in place makes an over-budget conversation look empty and nothing is
  evicted. Clearing them makes the budget reflect what is stored. The stored annotations are
  untouched, so the user's context decisions survive.
- **No strategies are passed to the budget strategy.** With `early_stop`, a configured sliding window
  would satisfy the budget immediately and everything it had excluded would be deleted, which is the
  over-deletion this design exists to avoid. An empty strategy list goes straight to core's
  deterministic oldest-group eviction, which preserves system messages and keeps tool-call groups
  intact.
- **Deletion reuses the existing prune path**, which removes by identity and drops entries left
  empty. No second deletion mechanism exists.
- **No summarization.** A model call on the request path re-runs on retry and can diverge. Eviction
  is deterministic.

**Values.** `max_state_bytes` defaults to `1_048_576`, the scheduler limit, and should be raised when
blob offload is configured. The high watermark is `0.85` and the low watermark `0.70`. The gap is
hysteresis: evicting to just under the trigger would evict again every subsequent turn. `0.85` rather
than `0.90` because the budget is approximate twice over, once in the byte-to-token estimate and once
because reasoning content is stripped from the candidate view. The byte budget converts to a token
budget using the ratio of content characters to serialized bytes measured on the spot, rather than a
guessed overhead constant.

Measuring costs about 8 ms on a conversation at the 1 MB limit, against a turn dominated by a model
call, and `to_dict()` already runs on every persist regardless.

**Why not simply reduce the store by default.** A default-on reducer only helps agents that already
configured compaction, because nothing else marks messages excludable, and those are the agents least
likely to hit the limit. It would leave every other configuration exactly as exposed as before while
changing behavior for users who were never at risk.

**Why not rely on blob offload alone.** It raises the ceiling roughly tenfold and does not remove it.
It is preview, it needs a storage account, and on the Durable Functions Python path it is currently
unreachable. `azure-functions-durable` 1.x does not depend on the durabletask SDK at all, because the
host extension owns persistence, so there is no Python-side seam to configure. The 2.x preview does
depend on `durabletask>=1.9.0`, whose worker and client both accept `payload_store`, but its
`DurableFunctionsWorker.__init__` takes no parameters and hardcodes the `super().__init__` call, and
`DurableFunctionsClient.__init__` takes only a connection string. Neither forwards `**kwargs`, so the
inherited capability cannot be reached without an upstream change (gap 6).

**Service-managed storage** is out of scope, mirroring ADR-0019. When the model provider owns the
conversation the client holds no history to compact. See "Service-managed conversations".

**Why workflows largely come "for free."** Durable workflow agent execution
(`DurableExecutorDispatcher.ExecuteAgentAsync`) runs an agent through the same
`DurableAIAgent → AgentEntity → inner agent` path as standalone durable agents, so **L1, L2 and
retention are inherited by workflow agent executors**. The workflow's own `full_conversation` between
executors does not pass through the agent, so it needs the separate **L3** hook.

### Consequences

- Good: **configuration parity for context.** The same core strategies and hooks apply on the durable
  runtime with no changes, and retention applies no context policy of its own.
- Good: **every configuration is protected from the capacity limit**, including external providers,
  service-managed agents and agents with no context pipeline, because retention lives in the entity
  rather than in the history provider.
- Good: **deletion is proportionate.** Under `auto` the amount removed is set by the budget, not by
  how much a context strategy happened to exclude.
- Neutral: a larger entity change than a bespoke compaction pass, and it must preserve the existing
  `ConversationHistory` consumer contract (`AgentRunHandle` response polling, audit/replay, TTL).
- Bad: **L2 is Python-only today.** In .NET, `CompactionProvider` persists its `CompactionMessageIndex`
  into `AgentSession.StateBag` with full `ChatMessage` copies, so a durable provider that also persists
  the session would store the transcript twice. See "Core Interface Gaps".
- Bad: L2 carries workaround code in Python because upstream binds the store-rewrite hook to session
  state. That code is deletable if the gap closes.
- Bad: retention under `auto` behaves differently above and below the watermark, which is harder to
  explain than uniform behavior. Accepted because the alternative for those users is the entity
  failing.
- Bad: an opt-in LLM-based reducer under `follow_compaction` runs inside the entity operation and
  re-runs on retry, mitigated by stable summary identity and optionally by Option 3 to move heavy
  summarization off the request path. Eviction under `auto` is deterministic and unaffected.

### Validation

**Done (Python).** Unit tests cover the provider substitution rules, compaction annotations
surviving a state round-trip, summary insertion, pruning, service-managed skip, session-state
persistence, and workflow context projection. Integration tests run against a real scheduler and
assert that annotations and message ids survive entity serialization, that an external provider
keeps a whole conversation under one key, and that a downstream workflow agent can reference the
upstream conversation.

**Outstanding.** Not covered yet.

- **Retention.** All three modes are built and covered, including an end-to-end test that drives a
  real agent through the entity for twenty turns against a small budget, with `keep_all` as the
  control proving the same run exceeds it. What is not yet covered is a conversation crossing the
  real scheduler limit against a real backend, rather than a lowered one in-process.
- The .NET realization and its schema parity (gap 3), and the .NET compaction-state blocker (gap 4).
- Blob offload (Option 7) against a real scheduler. Whether the Durable Functions Python path can
  reach it is now answered: it cannot, in either 1.x or the 2.x preview (gap 6).
- An external history provider storing history beyond the built-in state-size limit.
- Idempotency of an LLM-based reducer across simulated entity retries.

## Pros and Cons of the Options

The full argument is in **Decision Outcome** above. This is the summary.

- **Option 1 - In-run filter only.** Existing core feature, almost no new code, bounds the model
  input including long tool loops, and applies to workflow agent executors too. But it is non-lossy
  by design, so the persisted store keeps growing and the filter's incremental state is discarded
  and recomputed every turn.
- **Option 2 - Bespoke pre-write compaction in the entity.** Directly bounds persisted state, but is
  new durable-only code duplicating what core's store-reducer path already does, needs a
  `DurableAgentStateMessage` ⇄ message conversion, and gives no external-storage pluggability.
- **Option 3 - On-storage maintenance compaction.** Keeps expensive summarization off the request
  path and maps to ADR-0019's "on existing storage" point, and can layer on top of Option 6 later
  without rework. Adds scheduling machinery, leaves a window where state is un-compacted, and does
  not bound in-turn growth.
- **Option 4 - Workflow-level hook.** Bounds the inter-executor `full_conversation` that agent-level
  compaction never sees, reusing the existing `context_filter` seam. Only relevant to multi-agent
  workflows, and must reuse core grouping or a naive filter breaks atomic groups. **Adopted
  alongside Option 6 as L3.**
- **Option 5 - Auto-derive a store reducer.** Would bound durable storage without an explicit
  reducer, but as a *default* it only reaches agents that already configured compaction, since
  nothing else marks messages excludable, and it treats a context decision as consent to delete.
  **Adopted in opt-in form as the `follow_compaction` retention mode**, not as the default.
- **Option 6 - Durable store as a history provider (chosen).** The user's configuration carries over
  unchanged, and the same abstraction makes external backends pluggable, so one seam delivers both
  the opt-in reducer and pluggable storage. Costs a larger entity change that must preserve the
  `ConversationHistory` consumer contract (response polling, audit, TTL). L2 also does not come free:
  in Python the store-rewrite hook is bound to session state, so the provider publishes a working
  buffer and reconciles it itself, and **in .NET L2 is blocked outright** until core can persist
  compaction metadata without duplicating the transcript (see "Core Interface Gaps").
- **Option 7 - Blob offload.** Raises the ceiling roughly tenfold with no data loss, needs no code
  from this layer since the payload store is passed to the worker and client the caller already
  builds, and mirrors what the Azure Storage backend does internally. But it is preview, needs a
  storage account, does not remove the ceiling, and is unreachable on the Durable Functions Python
  path in both 1.x and the 2.x preview (gap 6). **Adopted as the first capacity answer on the
  durabletask path, ahead of any deletion.**

## Cross-Cutting Design Details

- **Reducer trigger.** Honor the configured `ReducerTriggerEvent`. `AfterMessageAdded`
  (compact-on-write, before checkpoint) is the natural durable default so the checkpoint is already
  bounded. `BeforeMessagesRetrieval` also works (reduce-on-load, then persist).
- **Determinism & idempotency.** An opt-in lossy reducer runs inside the entity operation and
  re-runs on retry. Give any generated summary a **stable identity** (derived from the ids of the
  messages it replaces) so retries do not re-summarize or duplicate. Reduced content becomes
  **permanent** durable state (same indirect-prompt-injection caution core flags on
  `ChatReducerCompactionStrategy` / `SummarizationCompactionStrategy`).
- **Message-list correctness.** Reuse core grouping so atomic tool-call/result and reasoning
  pairings are preserved at every layer.
- **Token counting.** Triggers must work without a live model call, so use the estimator tokenizer
  (`CharacterEstimatorTokenizer` / equivalent) unless a real tokenizer is supplied.
- **Placement.** The durable history provider backs `AgentEntity` in both languages. L3 lives in the
  `AgentExecutor` context handling.

## Core Interface Gaps for Pluggable History Providers

Prototyping the Python `DurableHistoryProvider` surfaced places where the current contracts assume a
*session-state-backed* history provider. They are recorded here because they affect **any** provider
whose store is not session state (Cosmos, Valkey, durable), not just this one. The prototype works
around them, but the cleaner fix is upstream.

1. **Store-side compaction is bound to session state rather than to the provider.** `CompactionProvider`
   has two hooks and only one of them is coupled.

   - `before_strategy` runs on messages already in the invocation context, whichever provider loaded
     them. Every provider gets this, so **in-run context bounding already works for external stores**.
   - `after_strategy` is documented as operating on "the accumulated messages stored by a history
     provider in session state", and "requires `history_source_id` to locate the messages in session
     state". It reads `session.state[history_source_id]["messages"]` and mutates that list in place,
     treating mutation as persistence - which only holds when the store *is* session state.

   So the missing capability is narrower than it first appears: an external provider can bound what
   the model sees, but cannot have the framework rewrite its store.

   Whether that is a defect depends on **who owns the store**. For a user-owned store (Cosmos, Redis)
   the framework arguably *should not* rewrite it implicitly. For a framework-owned store (in-memory,
   and durable entity state) rewriting is squarely in scope. Durable is the first framework-owned
   store that is not session state, which is what turns this from a defensible omission into a real
   problem.

   It is also unresolved rather than decided. ADR-0019 names three compaction points (in-run,
   pre-write, on existing storage), explicitly scopes in "local storage (e.g. `InMemoryHistoryProvider`,
   Redis, Cosmos)", and then leaves the mechanism open:

   > Should pre-write and existing-storage compaction share one unified configuration/setup to reduce
   > duplicate strategy wiring, and then either: each write overrides the full storage, or only new
   > messages are compacted while a separate interface can be called to compact the existing storage?

   That question shipped unanswered, and the languages then diverged on where the hook lives. .NET
   puts store reduction on the provider (`IChatReducer`) but only on `InMemoryChatHistoryProvider`,
   and `CosmosChatHistoryProvider` has none. Python puts it in `CompactionProvider` reaching into
   session state. **Neither language offers it to external providers.**

   *Workaround:* the provider publishes its loaded messages as a working buffer under the expected
   session-state key. *Upstream fix:* bind the store-rewrite hook to the provider abstraction instead
   of to session state as a storage mechanism, since .NET's shape generalizes and Python's does not.

2. **`save_messages()` is append-only.** The other half of the same open question. It receives only
   the newly produced messages, so mutations that compaction applies to *already stored* messages
   (setting `_excluded`, inserting a summary) have no defined path back to the store.
   *Workaround (implemented):* the provider overrides `after_run` and reconciles the working buffer
   itself **by `message_id`**, updating annotations on known messages and inserting ones compaction
   added. This required persisting `messageId` in durable state, which also gives summaries the
   **stable identity** the idempotency requirement needs. *Upstream fix:* add an explicit
   replace/flush operation alongside append so every external provider does not have to re-implement
   this reconciliation.

3. **Message-level metadata was not persisted (durable schema).** `DurableAgentStateMessage.to_dict()`
   dropped `extension_data` while `from_dict()` read it, a write-lossy asymmetry that silently
   discarded compaction annotations on every state round-trip. Since annotations are what carry
   compaction state, this had to be fixed for any of this to work. This one is ours rather than
   core's. The Python side now serializes it.

   The shared schema also under-declared what is persisted. `messageId` and `extensionData` are both
   load-bearing for compaction and neither appeared in `chatMessage`, so an implementer reading the
   contract had no way to know they must round-trip. Nothing would have *failed* validation, since
   the schema permits extra properties, which is precisely why it went unnoticed. They are declared
   now, `session` is described as an opaque runtime-discriminated payload rather than pinning
   Python's shape onto .NET, and a test validates real persisted state against the schema so the two
   cannot drift apart again silently.

   **.NET needs the same treatment, and looks deceptively fine.** Its `DurableAgentStateMessage`
   already has an `ExtensionData` property, but it is `[JsonExtensionData]`, System.Text.Json's
   overflow bucket for *unmapped JSON properties*, not a mapping of `ChatMessage.AdditionalProperties`.
   `FromChatMessage`/`ToChatMessage` copy neither `AdditionalProperties` nor `MessageId`, so both are
   lost at the **conversion** boundary rather than the JSON one. Anyone checking for "is extension
   data persisted?" will see the property and wrongly conclude parity is done.

   Two clarifications, because the reason this matters is not the obvious one. `ChatMessage.MessageId`
   **does** exist in the pinned Microsoft.Extensions.AI.Abstractions and is used throughout .NET, so
   it only needs mapping, not inventing. And .NET does **not** keep exclusion state in
   `AdditionalProperties` (it lives on `CompactionMessageGroup.IsExcluded`), so mapping these two
   fields is necessary but not sufficient. What `AdditionalProperties` does carry is the summary
   marker `_is_summary`, which is how a rebuilt index recognises an existing summary instead of
   re-summarizing it.

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
   .NET can bound entity state only through the retention path, which is deliberately independent of
   `CompactionProvider` and therefore unaffected.

5. **Provider cadence splits under per-service-call persistence.** With
   `require_per_service_call_history_persistence=True`, the agent's once-per-run loop skips history
   providers because the per-service-call middleware drives `before_run`/`after_run` itself, once per
   **model call** instead of once per run. `CompactionProvider` is not a `HistoryProvider`, so it
   stays on the once-per-run path. The pair is therefore split across two cadences, and compaction
   annotates the buffer *after* the history provider last flushed it, so annotations would not reach
   storage until the following flush. Only `HarnessAgent` sets this flag today, so this is latent
   rather than live. It is recorded because the symptom would be missing annotations rather than an
   error.

6. **Blob offload is unreachable on the Durable Functions Python path.** Not a core gap but an
   upstream one, recorded here because it is what forces this layer to own a capacity answer at all.
   `azure-functions-durable` 1.x, which this package pins (`>=1.3.1,<2`), does not depend on the
   durabletask SDK, since the host extension owns persistence. There is no Python-side seam to
   configure and the word payload does not appear in the package. The 2.x preview (`2.0.0b1`,
   `2.0.0b2`, both requiring Python 3.13+) does depend on `durabletask>=1.9.0`, and
   `DurableFunctionsWorker` subclasses `TaskHubGrpcWorker`, whose constructor accepts
   `payload_store`. But `DurableFunctionsWorker.__init__` takes no parameters and hardcodes its
   `super().__init__` arguments, and `DurableFunctionsClient.__init__` takes only a connection
   string. Neither forwards `**kwargs`, so the inherited capability is unreachable. The durabletask
   path has no such problem, because the caller constructs the worker and client and can pass
   `payload_store` directly. *Upstream fix:* expose `payload_store` on `DurableFunctionsWorker` and
   `DurableFunctionsClient`.

Two further core gaps are recorded with the decisions they affect: the process-local **state type
registry** (see "The session is persisted, not just its conversation id") and the absence of a public
way to ask whether **the service owns history for a run** (see "Service-managed conversations"). Both
forced this layer to re-implement logic core already has.

Consequence for ordering: core runs `before_run` forward and `after_run` in **reverse**. With
`[history, compaction]`, compaction annotates the buffer *before* the history provider flushes it
(convenient), but it sees history only as of the **previous** turn - so context reaches a steady
state rather than shrinking immediately. This is expected, not a defect.

## L3 Realization: Workflow Context Parity

In-process workflows give a downstream `AgentExecutor` the upstream conversation through
`AgentExecutorResponse.full_conversation`, governed by `context_mode` (`full` | `last_agent` |
`custom` + `context_filter`). The durable orchestrator previously flattened that to the **last
message's text**, so a downstream agent lost everything earlier nodes produced.

**L3 is a weaker seam than L1 and L2, and should not be described as parity with them.** Core's
compaction system is agent-level, so a workflow agent node inherits L1 unchanged: the in-process
`AgentExecutor` holds its own `AgentSession` and passes it to `agent.run()`, so any `CompactionProvider`
on the agent runs exactly as it would standalone. The inter-executor conversation has no equivalent.
`context_filter` is a synchronous callable returning a filtered list, not a strategy that annotates
groups, so L3 reuses the same *strategy* at a different, plainer seam rather than reusing the same
hook.

Durable now projects the same conversation and delivers it to the agent entity:

- The orchestrator reads the executor's `context_mode`/`context_filter` and projects
  `full_conversation` accordingly.
- The projection travels as `RunRequest.context_messages` (serialized `Message` values) and becomes
  the request entry's messages, so it is persisted like any other conversation content and is
  visible to compaction.
- A node that runs more than once (a cycle) receives the whole upstream conversation again, so the
  entity **drops the part it has already recorded**, keeping at least the latest message so the
  agent always has an input.

**Dedup is tracked by position, not by stored identity.** Comparing against the ids currently in
`ConversationHistory` breaks the moment retention evicts any of them: their ids leave the comparison
set, the orchestrator re-sends them on the next visit because its own conversation is never evicted,
and the node re-records exactly what was just deleted. That oscillates rather than converges, since
the re-ingested volume is proportional to what was evicted.

The entity therefore keeps a small map of `executor_id` to the highest conversation position it has
ingested, and drops anything at or below that mark. It is a handful of integers, it is unaffected by
deletion, and it is per executor rather than global because a fan-out gives two branches the same
position. Consequence worth stating: once a message is evicted the node stops seeing it, where the
broken behavior would re-feed it. That is intended. Re-ingesting evicted content defeats the
eviction.

**Alternatives measured and rejected.** Not persisting the forwarded context, and treating the
orchestrator's conversation as authoritative, both looked cleaner on paper. Measuring what actually
reaches the model showed otherwise. Core in-process sends 11 messages on the third visit of a
`full`-mode cycle, with heavy duplication, while durable today sends 8, because this dedup removes
repeats before they reach the model. For `last_agent` the two are identical. So the current design
already matches core where core is sane and improves on it where core is not, and the alternatives
would have reordered the conversation or dropped context the node should keep.

Behavior difference that remains, by design: each agent node also keeps its **own durable history**
(keyed by workflow instance + executor), so per-agent memory survives restarts and is compacted
independently - a superset of the in-process behavior rather than a strict match.

## Zero-Configuration Registration

The parity goal is only met if a user can take an agent that **already works in core**, register it
with `AgentFunctionApp` (or the worker, or as a workflow node), and get durable behavior with **no
edits to the agent**. Requiring them to add a durable-specific provider would just relocate the
configuration burden.

So the durable entity substitutes the history provider at construction time - covering every
registration path, since both the worker and the Azure Functions host build the same entity. The
agent is never mutated: when a substitution is needed, a shallow copy with its own provider list is
used, so the caller's agent still behaves normally in-process.

| User configured | Durable behavior |
| --- | --- |
| Nothing | Inject a durable history provider, using the `source_id` core's auto-injected provider would have, so default-wired compaction still resolves. No compaction by default (same as core). |
| `InMemoryHistoryProvider` (± compaction) | Replace with the durable provider, **preserving `source_id` and `skip_excluded`** so any attached `CompactionProvider` keeps working untouched. |
| Cosmos / Redis / file / custom provider | **Leave alone.** The user chose where their conversation lives, and durable still supplies execution durability. |
| Service-managed history | **Leave alone.** The model service owns the conversation. Decided by core's precedence, explicit `store` first and then the client's `STORES_BY_DEFAULT`. |
| Agent without the core context pipeline | **Leave alone.** Falls back to replaying persisted history. |

Preserving `source_id` is the load-bearing detail. `CompactionProvider` locates history through
`history_source_id` (default `"in_memory"`), so a provider swapped in under the same id is invisible
to the rest of the user's configuration. Because the injected provider is a `HistoryProvider` with
`load_messages=True`, core's own auto-injection sees a provider present and stands down, leaving no
duplicate provider.

An explicit `DurableHistoryProvider` remains supported as an advanced escape hatch, and takes
precedence over anything the runtime would inject.

### When the entity manages history itself

Two distinct decisions drive the entity, and conflating them caused bugs.

1. **Who supplies conversation context?** If the agent exposes core's context-provider pipeline,
   the providers do, so the entity passes a session and delivers **only the new messages**. This
   holds whether history lives in durable state, an external store, or the model service.
2. **Should durable state be bound?** Retention decides this, at the entity, for every
   configuration. It is deliberately not tied to whether a `DurableHistoryProvider` is present,
   because the entity records the conversation either way.

The entity therefore replays its own persisted history in exactly one case, an agent that does not
expose the context pipeline at all (for example a fully custom agent). Routing external-store or
service-backed agents down that path was incorrect, because it either bypassed their provider
entirely or re-sent history the service already had.

**Consequence:** passing a session is what re-engages the pipeline, so external history providers
(Cosmos, Redis, file) now function under the durable runtime. Previously they were silently
ignored because no session was ever created. They get the in-run filter like any other provider.
What they do not get is the framework rewriting their store, which no language offers today (core
interface gap 1 above).

That session must also carry the entity's **stable** session id rather than a generated one.
External providers key their storage on `session.session_id`, so a per-operation id would make them
read and write a different key every turn - the conversation would silently restart each time with
nothing to indicate a problem.

### The session is persisted, not just its conversation id

Core documents the per-provider `state` dict handed to `before_run`/`after_run` as durable for the
life of the session, and persists it through `AgentSession.to_dict()`. The entity builds a fresh
session per operation, so anything providers keep there was previously discarded at the end of every
turn: tool approval rules and **queued approval requests**, todo lists, background-task state, memory
extraction state. On .NET the same bag (`AgentSessionStateBag`) is a first-class part of the
`AIContextProvider` contract via `StateKeys`, so the gap is wider there.

That is a poor fit for a durable runtime whose headline scenario is long-running human-in-the-loop:
an approval flow that spans turns cannot work if the pending requests are dropped between them.

So the entity persists the **whole serialized session** rather than individual fields. Two
consequences:

- The service-issued conversation id needs no bespoke field of its own - it is already part of
  `AgentSession.to_dict()`. This replaces a hand-rolled `serviceSessionId` state field and its
  capture/restore helpers with one general mechanism that matches core's own serialization contract.
- The durable history provider's own slice is **excluded** before persisting. It is derived from
  `conversationHistory` on every turn, so storing it would duplicate the transcript and let the copy
  drift from the record of truth.

Restore applies the stored state onto a session created by the agent's own `create_session()`, so
the agent's session type is preserved.

**Restoring values as their own types.** Core deserializes state through a type registry that it
seeds with exactly one entry (`Message`). Anything else must be registered explicitly, and the
registry is process-local. `to_dict`-based types are never auto-registered, and only Pydantic models
are, and then only as a side effect of serializing. A durable entity routinely restores in a process
that never serialized the value, so state would come back as plain dicts instead of its own classes.

Before restoring, the entity therefore registers the serializable types **already loaded in the
process**. Nothing is imported from persisted data, so this cannot load code the application has not
already loaded itself, and that is sufficient in practice, because whoever put a value in the state
bag had to import its class to construct it. The walk is over `SerializationMixin` subclasses and
costs tens of microseconds.

Residual gaps, both better fixed in core.

- Pydantic values in state are keyed by `cls.__name__.lower()` and are not covered, since walking
  every `BaseModel` subclass in the process would be broad and collision-prone.
- Core could seed the registry with the state types it ships, which would make this unnecessary.
  `register_state_type()` is already public and its documentation names cold-start restore as the
  motivating case, yet nothing calls it today.

### Service-managed conversations

When the model service stores the conversation, it identifies the thread with an id. The entity
creates a fresh session per operation, so that id is **persisted in durable state and restored on
the next turn** (as part of the serialized session, above). Without it the service would start a new
thread every turn. The durable history provider additionally no-ops (neither loading nor flushing)
for service-managed sessions.

Whether the service owns history is decided with **core's precedence, not the client class alone**.
An explicit `store` in the agent's options wins, and only when it is unset does the client's
`STORES_BY_DEFAULT` apply. This matters because clients that store by default (such as the Responses
API) are routinely put back into client-side mode with `store=False`. Consulting only
`STORES_BY_DEFAULT` would leave such an agent with a plain in-memory provider that the durable
runtime never persists, silently losing the conversation between turns.

Core resolves this rule inside `Agent._run` and does not expose the result, so this layer
**re-derives it** and can drift from core if the rule changes, with silent conversation loss as the
symptom, which is exactly the bug this rule was written to fix. The unit tests here only pin *our*
logic. The end-to-end net is the compaction sample, which runs `store=False` against a
store-by-default client and asserts recall. *Upstream fix:* expose the resolved decision.

### Retention is a deployment policy, not agent configuration

Compaction annotates, it does not delete. Deletion is configured at **registration** (an app-level
default with a per-agent override) rather than on the agent, so the agent definition stays portable:
the same agent runs in-memory where retention would be meaningless, and the setting sits next to its
natural sibling, entity lifetime and TTL.

The three modes are described under "Retention" in the Decision Outcome. Two properties are worth
restating here, because they are what make retention safe to have on by default.

- **It applies no context policy.** Retention decides what durable state can hold, never what the
  model should read. Filtering the model's view remains entirely L1's job. What retention cannot
  avoid is that a deleted message is gone for every reader, including the history provider that
  loads context from `ConversationHistory`. Eviction therefore shortens the model's available
  history as a consequence of deletion, not as a policy of its own, and only from the point where
  the record would otherwise have stopped being writable at all.
- **An exclusion is not consent to delete.** `follow_compaction` is the only mode where a compaction
  exclusion causes deletion, and it is opt-in. Under `auto` a user's exclusions are left untouched
  and the amount deleted is set by the storage budget alone.

## Related Concern: Entity Lifetime (TTL) and Cleanup

Compaction bounds the *size* of a conversation. Entity **lifetime**, when the persisted state is
deleted, is a separate axis. It is out of scope for the decision above, but is recorded here
because it is the natural sibling of the retention setting introduced by this ADR, and because it
has a notable cross-language parity gap in this repository.

**TTL does not substitute for retention.** The .NET mechanism is a sliding idle timer: every
interaction pushes `ExpirationTimeUtc` forward, so an actively used conversation never expires and
grows until it reaches the backend limit. TTL reclaims *abandoned* entities, which bounds how many
exist and what they cost in aggregate. It does nothing about how large a single live entity gets,
which is the failure this ADR's retention design addresses.

- **.NET agents:** `DurableAgentsOptions.DefaultTimeToLive` (default 14 days) provides a global TTL,
  with a per-agent override via `AddAIAgent(agent, ttl)`. Idle entities self-delete via an
  `ExpirationTimeUtc` + `CheckAndDeleteIfExpired` self-signal.
- **.NET workflows:** workflow agent executors are auto-registered *without* a TTL
  (`DurableWorkflowOptions` calls `AddAIAgent(agent)`) and inherit the global default. There is **no
  workflow-scoped TTL option**, and each agent-node invocation spawns a fresh, single-use entity that
  then lingers for the full default (14 days) - far longer than needed for throwaway per-node state.
- **Python (agents *and* workflows):** there is **no TTL/cleanup mechanism at all** - no global
  default, no per-agent option, no `expirationTimeUtc` in the state schema, and no deletion. Entities
  persist indefinitely until manually deleted. This is a **.NET/Python parity gap**.

Follow-ups (tracked separately from the compaction decision):

1. **Port the TTL mechanism to Python** - a global default TTL, per-agent override, an
   `expirationTimeUtc` state field (for cross-language schema parity), and idle-based self-deletion.
2. **Expose a configurable global TTL consistently** across both languages, for agents and workflows.
3. **Give workflow-spawned agent entities a sensible lifetime** - a short workflow-scoped default TTL,
   or deterministic cleanup when the workflow completes, instead of the 14-day agent default (with an
   idle-TTL backstop for workflows that pause or never reach a terminal state).

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
- Suggested realization order: express the durable store as a `ChatHistoryProvider` (Option 6) →
  verify L1 filter parity → wire L3 workflow hook → add external storage backends → evaluate
  Option 3 for heavy summarization.
