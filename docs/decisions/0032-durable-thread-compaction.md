---
# These are optional elements. Feel free to remove any of them.
status: proposed
contact: ahmedmuhsin
date: 2026-07-27
deciders: ahmedmuhsin
consulted: eavanvalkenburg
informed:
---

# Thread Compaction for Durable Agents and Workflows

## Context and Problem Statement

Long-running **durable** agents and workflows accumulate conversation history in durable
storage and replay it on every turn. Durable agents persist a full `ConversationHistory` in
entity state (`AgentEntity` → `DurableAgentState`); durable workflows persist inter-executor
messages (`AgentExecutor.full_conversation`) as checkpointed envelopes. Unlike an in-memory agent
— whose history lives in process RAM (gigabytes) and disappears when the process recycles — this
history is **persisted, reloaded every turn, and permanent**.

It helps to separate **three distinct pressures**, because they have different owners:

| Pressure | What bounds it | Same in core? | Owner |
| --- | --- | --- | --- |
| **Context window** — the model's max input per call | the model | **Yes** — identical in core and durable | Compaction (in-run filter) |
| **Token cost / latency** — resending history each turn | tokens billed / round-trip | **Yes** — same mechanism | Compaction (in-run filter) |
| **Storage capacity** — the cumulative persisted state | backend state-size limit | **No** — durable-only | Storage backend (built-in limit or external store) |

The first two are **per-operation** (what a single turn sends to the model) and are **identical in
core and durable** — the model's context window is the same regardless of runtime. The third is
**cumulative across all runs**: `ConversationHistory` is a single blob appended to every turn and
re-persisted whole, so it is bounded by the durable backend's state-size limit (backend-specific;
e.g. classic Azure Storage ~1 MB/entity), whereas a core process is bounded only by RAM and resets
on restart. **Storage capacity is an infrastructure concern, not a context-window concern** — it is
relieved by raising the limit or moving to an external store, not by trimming what the model sees.

Core MAF already has a compaction system ([ADR-0019](https://github.com/microsoft/agent-framework/blob/main/docs/decisions/0019-python-context-compaction-strategy.md);
.NET `Microsoft.Agents.AI.Compaction`; Python `agent_framework._compaction`) with **two hooks**:

1. **In-run filter** — a `CompactionProvider` (`AIContextProvider`) / `compaction_strategy` runs
   before each model call. It is **non-lossy**: it filters the projection sent to the model and
   stores incremental group state in the `AgentSession.StateBag`; the underlying store is untouched.
2. **Store reducer** — an `IChatReducer` on a `ChatHistoryProvider` (e.g. `InMemoryChatHistoryProvider`)
   **lossily** rewrites the stored conversation. `strategy.AsChatReducer()` bridges any core strategy
   into this hook, so it is the **same strategies** applied at the store instead of the model call.

The durable layer benefits from **neither** today, because `AgentEntity` **bypasses the
`ChatHistoryProvider`**: it creates a fresh session per operation (so the StateBag — and any history
provider store or reducer in it — is discarded) and feeds `ConversationHistory` directly as input
messages. So both the in-run filter's incremental state and the store reducer are thrown away each
turn.

The goal is **configuration parity**: a user's core compaction config must carry over to a durable
entity or workflow **unchanged**, reusing the same strategies and hooks on the durable runtime,
without a parallel durable-only API.

**How should core compaction (both hooks) be reused on the durable runtime, in both .NET and
Python, so that the model input is bounded identically to core and the persisted store can be
bounded when the user opts into it?**

## Decision Drivers

- **Configuration parity** — the same core compaction config (strategies, `CompactionProvider`,
  `IChatReducer`) must apply unchanged when moving core → durable entity → durable workflow. No
  parallel durable-only API.
- **Reuse existing core hooks** — do not reinvent triggers/strategies/grouping; reuse the in-run
  filter and the store reducer.
- **Separate storage capacity from context management** — bound the model input with compaction
  (parity with core); relieve persisted-storage capacity with infrastructure (backend limits /
  external stores), not by silently trimming.
- **No silent data loss in the durable record** — a durable system of record must not quietly
  truncate history; lossy reduction is explicit opt-in, and hard capacity limits should surface a
  clear error/warning.
- **Determinism / idempotency** — durable entity operations can be retried; a lossy reducer
  (especially LLM summarization) must not corrupt or diverge persisted state across retries.
- **Message-list correctness** — preserve atomic groups (assistant tool-call + tool-result, and
  reasoning pairings) so the model input stays valid.
- **Cover both surfaces** — durable agents **and** durable workflows, in **both** languages.
- **No-op for service-managed storage** — when the service owns the conversation (a
  `ConversationId`/`service_session_id` is set), the client has no history to compact.

## Considered Options

- **Option 1 — In-run filter only.** Register the core `CompactionProvider` / `compaction_strategy`
  on the inner agent; change nothing else in the durable layer.
- **Option 2 — Bespoke pre-write compaction in the agent entity.** Add durable-specific code that
  compacts `ConversationHistory` inside the entity operation before checkpoint.
- **Option 3 — On-storage maintenance compaction.** Compact persisted history from a separate
  entity signal/operation, decoupled from the request path.
- **Option 4 — Workflow-level compaction hook.** Apply a strategy at the `AgentExecutor`
  `context_mode` / `context_filter` boundary that governs the `full_conversation` chained between
  agent executors.
- **Option 5 — Auto-derive a durable store reducer.** When only an in-run filter is configured,
  automatically derive a lossy store reducer (`strategy.AsChatReducer()`) so durable storage is
  bounded even without an explicit reducer.
- **Option 6 — Durable store as a `ChatHistoryProvider` (chosen).** Back the durable entity's
  persisted conversation with a core `ChatHistoryProvider` implementation, so **both** core hooks
  apply on the durable runtime unchanged: the in-run filter runs in the agent pipeline (L1), and a
  user-configured `IChatReducer` bounds the store (L2, opt-in). The same seam makes external storage
  backends (Cosmos, Valkey, blob) pluggable for capacity.

## Decision Outcome

Chosen option: **Option 6 — express durable conversation storage as a core `ChatHistoryProvider`**,
combined with the workflow hook (Option 4). This makes core's two compaction hooks apply on the
durable runtime with **no config change**, and cleanly separates context management from storage
capacity.

Compaction applies at **three layers**, mapped directly onto the core hooks:

| Layer | Core mechanism reused | Lossy? | Role |
| --- | --- | --- | --- |
| **L1 — in-run filter** | `CompactionProvider` in the agent pipeline | No | Always-on. Bounds the **model input** (context window, token cost). Identical to core. |
| **L2 — store reducer** | `IChatReducer` on the durable `ChatHistoryProvider` | Yes | **Opt-in.** Bounds the **persisted store**, only when the user configures a reducer. Identical to core. |
| **L3 — workflow hook** | the same strategy as the `AgentExecutor` `context_filter` | Yes | Bounds the inter-executor `full_conversation`. |

**Two accumulation surfaces:**

| Surface | Where it accumulates | Covered by |
| --- | --- | --- |
| **In-agent** | the agent's model input, and the persisted `AgentEntity` store | L1 (filter) + L2 (reducer, opt-in) |
| **Inter-executor (workflow)** | `AgentExecutor.full_conversation`, checkpointed as envelopes | L3 |

**Why Option 6 over bespoke entity compaction (Option 2).** Making the durable store a
`ChatHistoryProvider` means L2 is core's existing `IChatReducer` path — not new compaction code —
and the same abstraction is the seam for **external storage backends** (Cosmos/Valkey/blob) that
relieve capacity. One abstraction delivers both the opt-in reducer and pluggable storage, all
reused from core.

**Strict parity — no auto-derive (Option 5 rejected).** Durable honors exactly the hooks the user
configured. If only an in-run filter is configured, durable trims the model input just like core
and the store still grows — because the context window (which compaction addresses) is identical in
both runtimes, and storage capacity is a separate concern. Auto-deriving a lossy reducer would use a
context-window tool to solve a storage problem and **silently destroy the durable record**, breaking
both the "no data loss" driver and parity. Storage capacity is instead addressed by the backend:
the built-in store enforces a limit (surface a clear error/warning as it is approached), and an
external `ChatHistoryProvider` raises the ceiling for those who need unbounded durable records.

**Ideal durable default:** keep the full record in a (possibly external) durable `ChatHistoryProvider`
and apply the L1 in-run filter to the model input — never lose the record, always bound what the
model sees. A lossy L2 reducer is a deliberate opt-in, not a durable surprise.

**Why workflows largely come "for free."** Durable workflow agent execution
(`DurableExecutorDispatcher.ExecuteAgentAsync`) runs an agent through the same
`DurableAIAgent → AgentEntity → inner agent` path as standalone durable agents, so **L1 and L2 are
inherited by workflow agent executors**. The workflow's own `full_conversation` between executors
does not pass through the agent, so it needs the separate **L3** hook.

**Service-managed storage** remains out of scope (mirrors ADR-0019): when the service owns the
conversation, the client holds no history to compact.

### Consequences

- Good: **configuration parity** — the same core strategies/hooks apply on the durable runtime with
  no changes; the model input is bounded identically to core.
- Good: **no reinvention** — L2 is core's `IChatReducer` path; the `ChatHistoryProvider` seam also
  makes external storage backends pluggable for capacity.
- Good: **no silent data loss** — the durable record is only reduced when the user opts into a
  reducer; capacity limits surface explicitly.
- Good: durable workflows inherit L1+L2; L3 reuses the existing `context_filter` seam.
- Neutral: making the durable store a `ChatHistoryProvider` is a larger change to the entity than a
  bespoke compaction pass would be, and must preserve the existing `ConversationHistory` consumer
  contract (`AgentRunHandle` response polling, audit/replay, TTL).
- Bad: an opt-in LLM-based reducer runs inside the entity operation and re-runs on retry; mitigated
  by stable summary identity and (optionally) Option 3 to move heavy summarization off the request
  path.

### Validation

- **Unit tests (both languages):** a core `CompactionProvider` on a durable agent bounds the model
  input; a configured `IChatReducer` bounds the persisted store; with no reducer the store is not
  silently truncated; atomic groups preserved; reducer idempotent across simulated entity retries;
  service-managed sessions skipped.
- **Integration tests:** the same agent config produces equivalent compaction behavior in core and
  durable; a fan-out/chained durable workflow keeps `full_conversation` bounded via L3; an external
  `ChatHistoryProvider` stores history beyond the built-in limit.

## Pros and Cons of the Options

### Option 1 — In-run filter only

- Good, because it is the existing core feature with (almost) no new code, and bounds the model
  input within a run (including long tool loops).
- Good, because it applies to workflow agent executors too (shared agent path).
- Neutral, because it is non-lossy — by design it does not bound the persisted store.
- Bad, because the persisted `ConversationHistory` still grows and its incremental StateBag is
  discarded each operation (recomputed every turn), so on its own it does not address storage.

### Option 2 — Bespoke pre-write compaction in the agent entity

- Good, because it directly bounds persisted state and can reuse the static `CompactAsync`.
- Neutral, because it requires a `DurableAgentStateMessage` ⇄ `ChatMessage` conversion.
- Bad, because it is **new durable-specific code** that duplicates what core's `IChatReducer` path
  already does, and it does not give external-storage pluggability.

### Option 3 — On-storage maintenance compaction

- Good, because it keeps expensive summarization off the request/response path and maps to the
  "on existing storage" point from ADR-0019.
- Neutral, because it can layer on top of Option 6 later without rework.
- Bad, because it adds scheduling/trigger machinery and a window where state is temporarily
  un-compacted; on its own it does not bound in-turn growth.

### Option 4 — Workflow-level compaction hook

- Good, because it bounds the inter-executor `full_conversation` that agent-level compaction never
  sees, reusing the existing `context_filter` seam.
- Neutral, because it is only relevant to multi-agent workflows.
- Bad, because a naive filter could break atomic groups if it does not reuse the core grouping.

### Option 5 — Auto-derive a durable store reducer

- Good, because it would bound durable storage automatically even for in-run-filter-only configs.
- Bad, because it **conflates storage with context management** — using a lossy tool to solve a
  capacity problem — and **silently truncates the durable record**, breaking parity and the
  no-data-loss driver. Rejected.

### Option 6 — Durable store as a `ChatHistoryProvider` (chosen)

- Good, because **both** core hooks apply unchanged: L1 filter in the pipeline, L2 reducer on the
  store — full configuration parity.
- Good, because the same abstraction makes external storage backends (Cosmos/Valkey/blob) pluggable,
  relieving capacity without touching compaction.
- Good, because it is core reuse rather than durable-specific compaction code.
- Neutral, because L2 is opt-in — a store is only reduced when the user configures a reducer.
- Bad, because it is a larger entity change and must preserve the `ConversationHistory` consumer
  contract (response polling, audit, TTL).

## Cross-Cutting Design Details

- **Configuration parity (discovery over new API).** The durable runtime honors the compaction the
  user already configured on the agent — the `CompactionProvider` in the pipeline (L1) and any
  `IChatReducer` on the history provider (L2). A durable-specific option exists at most as an
  optional override, never as the required path. Moving core → durable entity → durable workflow
  requires no reconfiguration.
- **Two hooks, mapped.** In-run filter (`CompactionProvider`) → L1, non-lossy, bounds the model
  input. Store reducer (`IChatReducer` on the durable `ChatHistoryProvider`) → L2, lossy, opt-in,
  bounds the persisted store. Both accept the same `CompactionStrategy` (via `strategy.AsChatReducer()`).
- **Reducer trigger.** Honor the configured `ReducerTriggerEvent`; `AfterMessageAdded`
  (compact-on-write, before checkpoint) is the natural durable default so the checkpoint is already
  bounded. `BeforeMessagesRetrieval` also works (reduce-on-load, then persist).
- **Storage capacity is separate.** The built-in entity store is bounded by the backend state-size
  limit; approaching it should surface a clear error/warning, not silent truncation. An external
  `ChatHistoryProvider` (Cosmos/Valkey/blob) raises the ceiling for unbounded durable records and
  is enabled by the same Option 6 seam.
- **Determinism & idempotency.** An opt-in lossy reducer runs inside the entity operation and
  re-runs on retry. Give any generated summary a **stable identity** (derived from the ids of the
  messages it replaces) so retries do not re-summarize or duplicate. Reduced content becomes
  **permanent** durable state (same indirect-prompt-injection caution core flags on
  `ChatReducerCompactionStrategy` / `SummarizationCompactionStrategy`).
- **Message-list correctness.** Reuse core grouping so atomic tool-call/result and reasoning
  pairings are preserved at every layer.
- **Token counting.** Triggers must work without a live model call; use the estimator tokenizer
  (`CharacterEstimatorTokenizer` / equivalent) unless a real tokenizer is supplied.
- **Placement.** The durable `ChatHistoryProvider` backs `AgentEntity` (.NET) / `AgentEntity` in
  `_entities.py` (Python). L3 lives in the `AgentExecutor` context handling in both languages.

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
