---
status: proposed
contact: cgillum
date: 2026-05-22
deciders: cgillum, kshyju, larohra, gavin-aguiar, greenie-msft
consulted:
informed:
---

# Durable Agents: Pluggable Conversation Storage

## Context and Problem Statement

The Microsoft Agent Framework's Durable Agents extension (`Microsoft.Agents.AI.DurableTask` for .NET and `agent-framework-durabletask` for Python) currently stores the full conversation history for every agent session inside Durable Task framework (DTF) entity state. Each `AgentEntity` owns a `DurableAgentState` whose `Data.ConversationHistory` is a chronologically-ordered list of every request and response message ever exchanged in the session. Every turn, the entity loads this list, replays it to the wrapped `AIAgent`, appends the new request/response pair, and persists it back to DTF storage.

This design is simple and atomic — the entity-state checkpoint captures both "what the user said" and "what the agent replied" in one transaction — but it has two adoption-blocking problems for production users:

1. **Hard size ceiling.** The Durable Task Scheduler (DTS) limits a single gRPC payload to ~1 MB by default. Because each agent turn re-checkpoints the full conversation history, a session that accumulates ~1 MB of message JSON cannot continue. The `LargePayloadStorage` interceptor in `durabletask-dotnet` raises this ceiling to ~10 MB by externalizing oversize payloads to a customer-owned blob container, but the conversation can still outgrow that ceiling and the underlying problem (unbounded growth inside entity state) is unchanged.

2. **No customer ownership of conversation data.** Several user segments (regulated industries, internal compliance audits, customers with existing chat-analytics pipelines) require conversation transcripts to live in storage that they own and control — a Cosmos DB account they administer, a SQL database in their compliance boundary, etc. Today, the only place a durable conversation lives is inside DTF entity state, which is opaque to the customer's data plane and cannot be queried alongside their other application data.

The Agent Framework already exposes `ChatHistoryProvider` (.NET, in `Microsoft.Agents.AI.Abstractions`) and `HistoryProvider` (Python, in `agent_framework`) — abstract base classes for retrieving and storing conversation history with concrete implementations including `InMemoryChatHistoryProvider` / `InMemoryHistoryProvider` and `CosmosChatHistoryProvider`. However, attaching such a provider to the inner `ChatClientAgent` used by an `AgentEntity` does not solve either problem: the entity continues to write the full untruncated history to DTF entity state in parallel (`AgentEntity.cs:49,68-72,105-106`). The history is effectively double-stored, and the entity copy is the one that bumps into the DTS payload limit.

This ADR proposes a single new public extension point — **pluggable conversation storage** — that lets customers redirect durable-agent conversation history into a store they own and operate, while preserving today's default in-entity-state behavior for users who don't need it. **Entity-level compaction is explicitly out of scope** for this ADR; it will be addressed in a follow-up ADR once the store cursor/version model proposed here is settled.

## Decision Drivers

- **A. Remove the unbounded-growth hard ceiling.** A correctly-configured durable agent must be able to run indefinitely without the 1 MB / 10 MB DTS payload limit becoming a wall.
- **B. Allow customer ownership of conversation data.** Customers must be able to direct durable-agent conversation history into their own store with the same compliance and operational surface as the rest of their data.
- **C. Backward compatibility.** Existing apps must continue to work with no code changes and no behavior change. No on-the-wire schema break for the default configuration.
- **D. Reuse, don't reinvent.** The design must reuse the existing `ChatHistoryProvider` / `HistoryProvider` primitive as the most natural bridge to customer storage.
- **E. Atomicity & idempotency under entity retry.** Entity operations can crash and be redelivered. The store contract must make duplicate execution and duplicate writes safe across retries. The strongest (exactly-once-storage) guarantee is achievable only when the store can persist an idempotency marker in its own backend atomically with the turn; stores bridged from the existing `ChatHistoryProvider` contract (which has no such primitive) can offer only at-least-once turn writes. The ADR must make this distinction explicit rather than promise exactly-once for all stores.
- **F. Observability preservation.** The DTF dashboard's ability to surface conversation history in the entity inspector is a core debugging advantage. When history moves out of entity state, an explicit read API and non-content metadata in entity state must keep the debugging story coherent without leaking conversation content for compliance-sensitive customers.
- **G. Cross-language parity.** Whatever is decided for .NET must be expressible idiomatically in Python so the two SDKs stay aligned.
- **H. Forward compatibility with compaction.** Whatever shape this ADR ships must compose cleanly with the entity-level compaction work tracked for a follow-up ADR — the store contract must accommodate full-history rewrites without redesign.
- **I. No new mandatory dependencies.** The default path must not require any new Azure resources. External-store implementations are opt-in.

## Considered Options

1. **Status quo + document `LargePayloadStorage`.** Do not change `Microsoft.Agents.AI.DurableTask`. Add documentation pointing users at the existing `durabletask-dotnet` blob interceptor for ~1 MB → ~10 MB relief.
2. **Pluggable external conversation store.** Introduce a `DurableAgentConversationStore` extension point on `DurableAgentsOptions` (.NET) / `DurableAIAgentWorker` (Python). Ship three things: an `EntityStateConversationStore` (today's behavior, default); at least one **first-class backend store** (e.g., a Cosmos-backed store written directly against the contract) that persists the idempotency marker atomically and so delivers exactly-once storage; and a best-effort `ChatHistoryProviderConversationStore` / `HistoryProviderConversationStore` adapter that bridges to any existing `ChatHistoryProvider` / `HistoryProvider` at an at-least-once guarantee.
3. **Sharded entity state.** Split each session across a chain of entities (`agent@session.0`, `.1`, …) so each shard stays under the payload limit.
4. **Per-content externalization.** Add an entity-internal hook that externalizes individual large `AIContent` items (e.g., large tool results, base64 images) to a side blob store, leaving content pointers in the entity state.

## Decision Outcome

**Chosen option: Option 2 — pluggable external conversation store, with default behavior unchanged.**

Option 1 alone is insufficient (does not solve driver B, only partially solves driver A). Option 3 forces the storage problem into the public addressing surface (callers must reason about which shard they signal) and provides no benefit over Option 2. Option 4 is complementary, not competing — it can be layered as a decorator over any `DurableAgentConversationStore` in a follow-up.

In addition to Option 2, this ADR formally **documents Option 1** (the `LargePayloadStorage` interceptor) as the recommended immediate-mitigation path for users hitting the payload limit today and unwilling to wait for Option 2.

### Design Overview

The store is configured on `DurableAgentsOptions` (.NET) / `DurableAIAgentWorker` (Python). When configured to a non-default store, the entity:

- **Reads** conversation history from the store at the start of each turn (instead of from `DurableAgentState.Data.ConversationHistory`).
- **Buffers** the in-flight request in memory while invoking the LLM.
- **Writes** the request + response together as one atomic turn after the LLM call completes successfully.
- **Persists** only metadata (TTL, `OrchestrationId`, store-private cursor state, last-committed turn marker) in entity state — no conversation content. The store-private cursor state must hold references (e.g., a conversation id), not content; see idempotency guarantee 5.

The idempotency marker that makes turn writes safe under crash-recovery must be persisted **in the store's own backend, atomically with the turn's messages** — not in the entity-state cursor, which is rolled back on a mid-operation crash. Stores that cannot do this (the `ChatHistoryProvider` bridge) are at-least-once rather than exactly-once; see the idempotency contract for the full analysis.

The store interface centers on a single `CommitTurnAsync(correlationId, request, response)` primitive. Splitting that into separate "append request" / "append response" calls was rejected because it exposes a partial-turn window that breaks idempotency under entity retry (see "Pros and Cons of the Options" §2 for the analysis).

```text
AgentEntity.Run(request)  // request carries CorrelationId
  │
  ├─ if store.TryGetTurnAsync(sessionId, request.CorrelationId) returns response:
  │     return response                            ← replay short-circuit: turn already committed
  │
  ├─ history = store.LoadAsync(sessionId, storeState)
  │
  ├─ response = agentWrapper.RunStreamingAsync(history + request, session)
  │
  ├─ newStoreState = store.CommitTurnAsync(sessionId, request.CorrelationId, request, response, storeState)
  │                                                ← idempotent on (sessionId, correlationId)
  │
  └─ entity-state checkpoint:
       { LastCommittedCorrelationId = request.CorrelationId,
         StoreState = newStoreState,
         ExpirationTimeUtc, OrchestrationId,
         // ConversationHistory omitted (external) or preserved (default store) }
```

#### New public types (.NET)

```csharp
namespace Microsoft.Agents.AI.DurableTask;

public abstract class DurableAgentConversationStore
{
    /// <summary>Load the full conversation history for a session.</summary>
    /// <param name="storeState">Opaque store-private state previously returned by CommitTurnAsync, or default for a new session.</param>
    public abstract ValueTask<IReadOnlyList<ChatMessage>> LoadAsync(
        AgentSessionId sessionId,
        JsonElement storeState,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// If a turn with this correlation id has already been committed, return the committed response;
    /// otherwise return null. Used by the entity to short-circuit replay after a crash.
    /// </summary>
    /// <remarks>
    /// CRITICAL: this query must be answered from the same durable medium that <see cref="CommitTurnAsync"/>
    /// writes to — NOT from <paramref name="storeState"/>. <paramref name="storeState"/> is checkpointed in
    /// entity state, which is rolled back if the worker crashes after the store write but before the entity
    /// checkpoint commits. A correct implementation persists a per-correlation-id marker in its own backend
    /// (atomically with the turn's messages) and reads that marker back here. Stores that cannot persist such
    /// a marker in their backend cannot honor this contract; see the idempotency section below.
    /// </remarks>
    public abstract ValueTask<AgentResponse?> TryGetTurnAsync(
        AgentSessionId sessionId,
        string correlationId,
        JsonElement storeState,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically append both the request and response for a single turn.
    /// Must be idempotent on (sessionId, correlationId): a duplicate call with the same
    /// correlationId must produce no additional writes and must return store state equivalent
    /// to the first successful call.
    /// </summary>
    /// <remarks>
    /// The idempotency marker (the committed correlation id) MUST be persisted in the store's own backend
    /// within the same atomic write as the turn's messages, so that <see cref="TryGetTurnAsync"/> can observe
    /// the commit even when the entity-state checkpoint that would have recorded <paramref name="storeState"/>
    /// was lost to a crash. Recording the marker only in the returned <paramref name="storeState"/> (which lives
    /// in entity state) is insufficient and will produce duplicate writes on crash-recovery — see the idempotency
    /// section. The default <see cref="EntityStateConversationStore"/> satisfies this trivially because its backend
    /// IS the entity state, so the marker and the messages share the entity-operation transaction.
    /// </remarks>
    /// <returns>Opaque store-private state to be checkpointed in entity state and supplied to the next LoadAsync/TryGetTurnAsync call. This is an optimization hint and a cursor, NOT the source of truth for idempotency.</returns>
    public abstract ValueTask<JsonElement> CommitTurnAsync(
        AgentSessionId sessionId,
        string correlationId,
        IReadOnlyList<ChatMessage> requestMessages,
        AgentResponse response,
        JsonElement storeState,
        CancellationToken cancellationToken = default);

    /// <summary>Atomically replace the full conversation. Required for compaction (future ADR); default throws.</summary>
    public virtual ValueTask<JsonElement> ReplaceAsync(
        AgentSessionId sessionId,
        IReadOnlyList<ChatMessage> compactedMessages,
        JsonElement baseStoreState,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException("This store does not support full-history replacement.");

    /// <summary>Delete all session state from the external store. Called by the entity's TTL cleanup.</summary>
    public abstract ValueTask DeleteAsync(
        AgentSessionId sessionId,
        JsonElement storeState,
        CancellationToken cancellationToken = default);
}

public sealed class EntityStateConversationStore : DurableAgentConversationStore { /* default; preserves today's behavior */ }

// Best-effort bridge. See "Bridging to ChatHistoryProvider" and the idempotency section for its
// reduced guarantee (at-least-once turn writes under crash-recovery) and eligibility constraints.
public sealed class ChatHistoryProviderConversationStore : DurableAgentConversationStore
{
    public ChatHistoryProviderConversationStore(ChatHistoryProvider provider) { /* ... */ }
}

public sealed class DurableAgentsOptions
{
    public DurableAgentsOptions UseConversationStore(DurableAgentConversationStore store);
    public DurableAgentsOptions UseConversationStore(Func<IServiceProvider, DurableAgentConversationStore> factory);
}
```

> Implementation note: `DurableAgentsOptions` is currently a non-`partial` `public sealed class`
> (`DurableAgentsOptions.cs`). Adding `UseConversationStore` is a straightforward member addition;
> no `partial` split is required.

#### Strong-guarantee stores vs. the best-effort bridge

`DurableAgentConversationStore` implementations fall into two categories, and the ADR is explicit
about which guarantee each can offer:

- **First-class backend stores (recommended for production).** A store that has direct control over its
  backend (e.g., a Cosmos-backed store written directly against the `DurableAgentConversationStore`
  contract) can persist the committed-correlation-id marker in the same atomic write as the turn's
  messages, and answer `TryGetTurnAsync` by reading that marker back. These stores deliver the full
  exactly-once-storage guarantee of the idempotency contract below. Phase 2 ships at least one such
  store so the production data-ownership story (driver B) does not rest on the bridge alone.
- **The `ChatHistoryProviderConversationStore` bridge (convenience, best-effort).** Bridging to an
  arbitrary existing `ChatHistoryProvider` cannot deliver the full guarantee — see the next subsection.
  It is offered as a low-friction adapter with explicitly documented at-least-once turn-write semantics.

#### Bridging to `ChatHistoryProvider` (mechanics and limits)

The existing `ChatHistoryProvider` (.NET) is an *interception* abstraction, not a load/save one: it exposes
`InvokingAsync(InvokingContext)` (returns history merged with the caller's request messages) and
`InvokedAsync(InvokedContext)` (stores the new request/response messages), and it keeps per-session state in
`AgentSession.StateBag`. The bridge therefore does real, non-trivial work:

- It reconstructs an `AgentSession` whose `StateBag` is rehydrated from `storeState`, calls
  `InvokingAsync` with an empty request-message set to obtain history (`LoadAsync`), and re-extracts the
  updated `StateBag` after `InvokedAsync` to return as the new `storeState` (`CommitTurnAsync`).
- It must account for `ChatHistoryProvider`'s source-stamping and filtering: `InvokingCoreAsync` stamps
  returned messages with `AgentRequestMessageSourceType.ChatHistory`, and the default
  `storeInputRequestMessageFilter` *excludes* such messages on store. The bridge must orchestrate this so
  the genuine new user/request message is stored while loaded history is not re-stored.

Two hard limits the bridge cannot overcome through this abstraction:

1. **Dependency on evaluation-only APIs.** The `InvokingContext`/`InvokedContext` constructors the bridge
   must call are marked `[Experimental]` (diagnostic `MAAI001`). The bridge takes a dependency on types that
   are explicitly subject to change or removal.
2. **No correlation-keyed idempotency primitive.** `ChatHistoryProvider` has no way to (a) write a
   committed-correlation-id marker into the provider's backend atomically with the messages, or (b) query
   "was correlation id X already committed?". Concrete providers also generate non-deterministic message ids
   (e.g., `CosmosChatHistoryProvider` uses `Guid.NewGuid()` with `CreateItemAsync`, which has no native
   dedup). Consequently the bridge can only gate re-invocation using a marker it keeps in `storeState` — and
   `storeState` is lost in the crash-after-write-before-checkpoint window. The bridge is therefore
   **at-least-once** for turn writes: a crash in that window causes the turn's messages to be written twice.

A new public extension method exposes the read-back API to callers outside the entity:

```csharp
namespace Microsoft.Agents.AI.DurableTask;

public static class DurableTaskClientAgentExtensions
{
    public static Task<IReadOnlyList<ChatMessage>> GetAgentConversationHistoryAsync(
        this DurableTaskClient client,
        AgentSessionId sessionId,
        CancellationToken cancellationToken = default);
}
```

This avoids making the currently-`internal` `IDurableAgentClient` interface public. The extension method reads from whichever store is configured: for `EntityStateConversationStore`, it queries the entity directly; for external stores, it must first read the per-session `storeState` (the store cursor — e.g., the Cosmos conversation id) from entity state, then call `store.LoadAsync(sessionId, storeState)`. It cannot call the store "blind" because the store needs that per-session cursor to locate the conversation.

#### Python parity

```python
class DurableAgentConversationStore(ABC):
    @abstractmethod
    async def load(
        self, session_id: AgentSessionId, store_state: dict[str, Any]
    ) -> list[Message]: ...

    @abstractmethod
    async def try_get_turn(
        self, session_id: AgentSessionId, correlation_id: str, store_state: dict[str, Any]
    ) -> AgentResponse | None: ...

    @abstractmethod
    async def commit_turn(
        self,
        session_id: AgentSessionId,
        correlation_id: str,
        request_messages: list[Message],
        response: AgentResponse,
        store_state: dict[str, Any],
    ) -> dict[str, Any]: ...

    async def replace(
        self,
        session_id: AgentSessionId,
        compacted_messages: list[Message],
        base_store_state: dict[str, Any],
    ) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def delete(
        self, session_id: AgentSessionId, store_state: dict[str, Any]
    ) -> None: ...


class EntityStateConversationStore(DurableAgentConversationStore): ...
class HistoryProviderConversationStore(DurableAgentConversationStore): ...


# Configuration follows the existing worker pattern (constructor arg, matches `callback`):
worker = DurableAIAgentWorker(
    dt_worker,
    conversation_store=my_store,   # new
)
```

Note that the Python type for messages is `Message` (not `ChatMessage`), the response type is `AgentResponse`, and the Python store opaque state is a `dict[str, Any]` rather than a `JsonElement`, matching existing conventions. The Python `HistoryProviderConversationStore` bridges to the Python `HistoryProvider` (`agent_framework._sessions.HistoryProvider`), whose interception methods are `get_messages(session_id, *, state=...)` and `save_messages(session_id, messages, *, state=...)` — neither of which carries a correlation/idempotency key, so the same at-least-once caveat described for the .NET bridge applies (see the idempotency section).

> Cross-language gap (driver G): the bridge has **no production-grade external provider in Python today.**
> Core Python ships only `InMemoryHistoryProvider` (which stores full message content in session state —
> see the eligibility note below) and `FileHistoryProvider` (local disk, unusable from a distributed durable
> worker). There is no Python equivalent of `CosmosChatHistoryProvider`. So the data-ownership scenario
> (driver B) is realizable on .NET first; Python parity for the *bridge* is structural only until a
> reference-holding Python provider (or a first-class Python store) ships. This is called out again under
> Cross-language notes.

Python parity for the read-back API extends `DurableAIAgentClient`:

```python
async def get_conversation_history(
    self, session_id: AgentSessionId
) -> list[Message]: ...
```

#### Idempotency contract (the core correctness guarantee)

The entity orchestrates each turn around the store's `(TryGetTurnAsync, CommitTurnAsync)` pair as follows. This protocol must hold in every implementation:

1. **`CommitTurnAsync` must be idempotent on `(sessionId, correlationId)`.** A duplicate call with the same `correlationId` must not produce duplicate stored messages and must return store state equivalent (in terms of subsequent `LoadAsync` results) to the first successful call.
2. **`TryGetTurnAsync` must return the committed response for any `correlationId` previously passed to a successful `CommitTurnAsync`.** This lets the entity short-circuit replay so the LLM and tools are never executed twice for the same turn.
3. **The idempotency marker must live in the store's backend, not in entity state.** `CommitTurnAsync` must persist the committed `correlationId` durably in the *same backend and same atomic write* as the turn's messages, and `TryGetTurnAsync` must answer from that backend. The opaque `storeState` returned to the entity is a cursor/optimization hint that is checkpointed in entity state; it is **not** the source of truth for idempotency, because entity state is rolled back if the worker crashes after the store write but before the entity-operation checkpoint commits. A marker kept only in `storeState` (e.g., a ring buffer of recent correlation ids) is therefore lost in exactly the window where it is needed, and re-invocation produces duplicate writes.
4. **Stores without a correlation-keyed backend primitive cannot honor guarantees 1–3; they are at-least-once.** Existing `ChatHistoryProvider` / `HistoryProvider` implementations (including `CosmosChatHistoryProvider`) have no way to write a correlation-id marker atomically with messages and no way to query "was correlation id X committed?". They also assign non-deterministic message ids (`Guid.NewGuid()`), so the bridge cannot dedup by id either. The `ChatHistoryProviderConversationStore` bridge therefore provides **at-least-once turn writes**: correct under normal operation and exactly-once except in the crash-after-store-write-before-entity-checkpoint window, where a turn's messages may be written twice. This is acceptable for many history-analytics use cases but is **not** the exactly-once-storage guarantee. Customers needing exactly-once storage should use a first-class backend store (above) that persists the marker atomically. The earlier claim that the bridge could enforce dedup via a ring buffer in `storeState` and deterministic message ids was incorrect and has been removed.
5. **Only reference-holding store state qualifies for size relief (driver A).** The `storeState` / `SessionStateBag` snapshot is checkpointed in entity state. If the configured store keeps conversation *content* there, the content lands back in entity state and the 1 MB ceiling returns. `CosmosChatHistoryProvider.State` holds only `{ ConversationId, TenantId, UserId }` (references) and is fine; `InMemoryChatHistoryProvider` / Python `InMemoryHistoryProvider` store the **entire message list** in session state and therefore do **not** relieve the size ceiling when bridged — they are valid for testing/parity only. A store is "size-relieving" only if its checkpointed state holds references, not content.

On the entity side, the checkpointed `LastCommittedCorrelationId` is a single string (the most recent successful commit). The entity uses it as a fast-path replay check: if the incoming `request.CorrelationId` equals it, the turn already committed and the entity reads back via `TryGetTurnAsync`. If it doesn't match, the entity still calls `TryGetTurnAsync` (since prior turns from queued duplicate signals may have committed but not been seen by this entity instance), then proceeds to a fresh turn if no match. Whether that read-back actually finds the committed turn depends on the store's guarantee class (below).

The replay outcomes differ by store. For a store that satisfies guarantees 1–4 (the default `EntityStateConversationStore`, where the backend *is* the entity-operation transaction, and any first-class backend store that persists the marker atomically):

| Failure window | Outcome on replay (exactly-once store) |
| --- | --- |
| Crash before LLM call | `TryGetTurnAsync` returns null; LLM runs normally; `CommitTurnAsync` is the first successful write. |
| Crash during LLM call | Same as above. |
| Crash after LLM but before `CommitTurnAsync` | `TryGetTurnAsync` returns null; the LLM call runs again on retry (possibly producing a different response); `CommitTurnAsync` commits the new response. **Tool side effects can execute twice in this window.** See "Known limitation: tool side effects on retry". |
| Crash after `CommitTurnAsync` but before entity checkpoint | `TryGetTurnAsync` returns the committed response (read from the backend marker); entity short-circuits without re-running the LLM. |
| Crash after entity checkpoint | `LastCommittedCorrelationId` is set; entity fast-paths into `TryGetTurnAsync`; same outcome. |

For the `ChatHistoryProviderConversationStore` bridge (at-least-once; the marker is not durable independently of the entity checkpoint), one row changes — and it is the dangerous one:

| Failure window | Outcome on replay (bridge / at-least-once) |
| --- | --- |
| Crash after `CommitTurnAsync` (messages written to the provider) but before entity checkpoint | The marker in `storeState` is rolled back with the entity state, but the provider's messages persist. `TryGetTurnAsync` cannot see the commit, returns null, the LLM runs again, and `CommitTurnAsync` writes the turn's messages a **second** time. The conversation history contains a duplicated turn. |

This is the concrete reason the bridge is documented as at-least-once and a first-class store is recommended for exactly-once needs.

> Streaming interaction: when `TryGetTurnAsync` short-circuits a replayed turn, the entity returns the
> committed response *without* re-driving the `IAgentResponseHandler` streaming path
> (`AgentEntity.OnStreamingResponseUpdateAsync`). An interactive client that renders live streaming updates
> will receive no incremental updates for a short-circuited turn (the final response is still available via
> the run handle / read-back API). This is an acceptable trade (it avoids re-streaming a turn whose side
> effects already ran) but consumers of the streaming handler must tolerate a turn arriving only as a final
> response on replay.

#### Known limitation: tool side effects on retry

The atomic `CommitTurnAsync` primitive guarantees (for strong-guarantee stores; at-least-once for the bridge) that **conversation storage** is never *corrupted* by a retry — no half-written turns and no LLM-prompt poisoning on replay (the bridge's only failure mode is a duplicated turn, not a corrupted one). It does not, and cannot, guarantee that **tool side effects** are executed exactly once.

Consider a turn where the agent calls a `send_email` tool, the email is sent, the LLM then produces a final response, and the entity crashes before `CommitTurnAsync` completes. On retry:

1. `TryGetTurnAsync` returns null (the turn was never committed).
2. The entity re-invokes the underlying agent with the same prompt.
3. The agent's tool-calling loop runs again from scratch. The model may or may not choose to call `send_email` again. If it does, the email is sent a second time.

This window exists because individual tool calls inside a single agent turn are not themselves checkpointed — the unit of durability is the whole turn, not each tool invocation. Closing the window would require persisting the tool-call execution log between LLM calls inside a turn, which is a much larger design and would couple the durable-agent contract tightly to the tool-calling loop's internals. We are deliberately not solving this in this ADR.

What this means in practice:

- **Idempotent or read-only tools** (most retrieval, calculation, and lookup tools) are unaffected.
- **Side-effecting tools** (`send_email`, `charge_card`, `post_to_slack`, `create_ticket`, etc.) must be designed to be idempotent on a caller-supplied key, or must accept that a process crash during the turn can cause a duplicate side effect. This is the same constraint that applies to any retried agent invocation today; the durable-agents extension does not change it.
- **The default expectation is that tool authors handle this** the same way they would for any retry-prone caller — by accepting an idempotency key parameter (e.g., a stable per-turn identifier) or by using the side-effect system's own dedup features.

A future ADR may add per-tool checkpointing inside a turn to close this window, but it is not a prerequisite for shipping pluggable conversation storage.

#### Entity-state schema impact

`DurableAgentState.SchemaVersion` bumps from `1.1.0` to **`1.2.0`** (additive, not a major version). The existing `DurableAgentStateJsonConverter` only checks for major version `1`, so old code can still deserialize new state. The new fields are:

- `LastCommittedCorrelationId: string?` — the correlation id of the most recent successfully committed turn (any store).
- `StoreState: JsonElement` — opaque store-private state. Owned and shaped by whichever `DurableAgentConversationStore` is configured. Defaults to `JsonElement` of `null`. The entity never introspects this field.
- `SessionStateBag: JsonElement?` — checkpointed snapshot of `DurableAgentSession.StateBag`. Today the entity creates a fresh session each turn (`AgentEntity.cs` calls `CreateSessionAsync` per `Run`), so the `StateBag` is discarded between turns; checkpointing it is genuinely new plumbing and is required for `ChatHistoryProvider`-style providers, which rely on `AgentSession.StateBag` for provider-private bookkeeping (e.g., Cosmos stores its `ConversationId` there). **Caveat:** this field is checkpointed in entity state, so it is subject to driver-A guarantee 5 above — if a provider keeps conversation *content* in its `StateBag` (e.g., `InMemoryChatHistoryProvider`), that content lands in entity state and the size ceiling is not relieved.

When the default `EntityStateConversationStore` is used, `Data.ConversationHistory` continues to be the source of truth (so existing entities load transparently and no migration is needed). When a non-default store is configured, `Data.ConversationHistory` is omitted from new writes — the store is the source of truth.

#### Migration when switching from default store to external store

A customer running today has populated `Data.ConversationHistory` in entity state. When they redeploy with `UseConversationStore(externalStore)`, the entity uses **lazy migration** on the next turn:

1. Entity loads. `Data.ConversationHistory` is non-empty, but a non-default store is configured.
2. Entity replays each historical turn pair into the store via a one-time `store.CommitTurnAsync(...)`, pairing each request entry with its response entry by the correlation id carried on both (`DurableAgentStateRequest.CorrelationId` and `DurableAgentStateResponse.CorrelationId`). Because `CommitTurnAsync` is idempotent (on a strong-guarantee store), a partial-migration retry resumes correctly; on the at-least-once bridge, a retried migration may duplicate already-migrated turns. **Edge cases:** the migration must tolerate request entries with no paired response (an errored or still-in-flight turn) and responses dropped during persistence by `HasSerializableContent` (empty responses are not stored). Such unpaired entries are migrated as request-only turns or skipped per a documented rule, not assumed to be clean request/response pairs.
3. On full success, entity sets `Data.Migrated = true` and clears `Data.ConversationHistory` in the same checkpoint as the new turn's commit.
4. From then on, the entity reads exclusively from the external store.

Customers with strict compliance requirements who do not want implicit data movement can opt into a stricter behavior via `DurableAgentsOptions.MigrationPolicy`:

- **`Lazy`** (default) — the flow above.
- **`Explicit`** — the entity refuses to start with a clear exception until the customer invokes an explicit `DurableTaskClient.MigrateAgentConversationAsync(sessionId)` operation. Lets compliance customers audit the data movement.

#### Observability mitigation

For external-store sessions, the entity persists non-content diagnostic metadata in entity state so the DTF dashboard remains useful:

- `StoreType: string` — fully-qualified type name of the configured store (e.g., `"ChatHistoryProviderConversationStore"`).
- `MessageCount: int` — total committed turns (request + response) on this session.
- `LastTurnUtc: DateTime` — timestamp of the most recent `CommitTurnAsync`.
- `LastStoreError: string?` — message + timestamp of the most recent store failure, if any. Cleared on the next successful operation.

**Conversation content** (messages, summaries) is **never** mirrored into the diagnostic metadata above when an external store is configured. This is non-negotiable for the compliance use case that motivated driver B. (Note this is distinct from the `SessionStateBag` field: a poorly-chosen provider that keeps content in its `StateBag` can still leak content into entity state via that field — see guarantee 5. Size-relieving, compliance-suitable stores must keep only references in their session state.) If customers need richer entity-state diagnostics, that is an opt-in feature for a future ADR.

#### TTL / delete semantics

Entity deletion (TTL expiry or explicit delete) becomes a two-phase operation when an external store is configured:

1. **External delete first.** Entity calls `store.DeleteAsync(sessionId, storeState)`. If this throws, the entity retries on a backoff (using the existing `ScheduleDeletionCheck` machinery) — the entity stays alive until the external delete succeeds, ensuring no orphaned transcripts.
2. **Entity self-delete.** Once external delete succeeds (or returns "already gone"), the entity sets `State = null` to delete itself.

`DeleteAsync` is required to be idempotent on `sessionId` — calling it on an already-deleted session must succeed silently.

### Consequences

- **Good**, because backward compatibility is preserved: the default `EntityStateConversationStore` makes Option 2 a no-op for existing apps (driver C). The on-the-wire schema for default-store entities is unchanged in content, only adds optional fields.
- **Good**, because driver A is solved: external stores remove the entity-state ceiling entirely. The orthogonal entity-level compaction work tracked for a follow-up ADR will also solve driver A for users who prefer to stay in entity state.
- **Good**, because driver B is solved for production via a first-class backend store (e.g., a direct Cosmos store) that persists the idempotency marker atomically and delivers exactly-once storage. The `ChatHistoryProviderConversationStore` bridge additionally lets every existing `ChatHistoryProvider` be adapted with low friction, at a documented at-least-once guarantee.
- **Good**, because for strong-guarantee stores (the default `EntityStateConversationStore` and first-class backend stores) the `CommitTurnAsync` atomic primitive plus the `TryGetTurnAsync` replay-check eliminates LLM-replay divergence under entity retry (driver E). For the bridge this is reduced to at-least-once turn writes (see the idempotency section).
- **Good**, because the design surfaces the same shape in .NET and Python (driver G), with the Python sketch using idiomatic constructor-arg configuration and `Message`/`dict` types.
- **Good**, because the default path requires no new Azure resources (driver I).
- **Good**, because the store contract reserves `ReplaceAsync` for the compaction follow-up (driver H) without committing to a compaction strategy here.
- **Bad**, because the public API surface grows (new abstraction, two concrete stores, options method, extension method). Mitigated by deriving the new abstraction shape directly from the existing `ChatHistoryProvider` mental model.
- **Bad**, because the DTF dashboard's "see the conversation inside the entity" view stops working for external-store sessions (driver F regression). Mitigated by `DurableTaskClient.GetAgentConversationHistoryAsync` and by the non-content diagnostic metadata persisted in entity state.
- **Bad**, because tool side effects can execute twice if the process crashes after the LLM call completes but before the turn is committed to the store. This is inherent to checkpointing turns rather than individual tool calls and is not introduced by this ADR; documented above under "Known limitation: tool side effects on retry."
- **Bad**, because lazy migration implicitly moves customer data into the new store on first turn after redeploy. Mitigated by the opt-in `MigrationPolicy.Explicit` for compliance users.
- **Bad**, because the `ChatHistoryProviderConversationStore` bridge can only offer at-least-once turn writes (a crash after the provider write but before the entity checkpoint duplicates the turn), and it depends on the `[Experimental]` (`MAAI001`) `InvokingContext`/`InvokedContext` constructors, which are subject to change or removal. Mitigated by recommending a first-class backend store for exactly-once needs and by isolating the experimental dependency inside the bridge.
- **Bad**, because cross-language parity for the bridge is incomplete: Python ships no reference-holding external `HistoryProvider` (only in-memory and local-file), so the data-ownership scenario lands on .NET first (driver G partially deferred).
- **Bad**, because streaming consumers (`IAgentResponseHandler`) receive a short-circuited (replayed) turn only as a final response, not as incremental updates, since the `TryGetTurnAsync` fast-path bypasses re-streaming.
- **Neutral**, because the `LargePayloadStorage` interceptor remains available and is documented as an orthogonal mitigation for users who do not want to adopt a new abstraction.

## Validation

- Unit tests in `dotnet/tests/Microsoft.Agents.AI.DurableTask.UnitTests` covering:
  - **Backward-compat path:** default store, no store configured, schema unchanged in content, existing entities deserialize without migration.
  - **External-store round-trip** via a first-class backend store and (separately) via `ChatHistoryProviderConversationStore` over an in-memory `ChatHistoryProvider`.
  - **Exactly-once `CommitTurnAsync` (first-class store):** duplicate calls with the same `correlationId` produce no duplicate writes; `TryGetTurnAsync` answers from the backend marker (not from `storeState`).
  - **At-least-once bridge window (negative test):** simulate crash after the provider write but before the entity checkpoint; assert the bridge re-writes the turn (documenting the duplicate) and that the entity does not silently claim exactly-once. This pins the documented guarantee so a future change can't regress it unnoticed.
  - **`TryGetTurnAsync` recovery:** for a strong-guarantee store, entity replay after crash-after-`CommitTurnAsync`-before-checkpoint reads back the committed response and does not re-invoke the LLM.
  - **Migration:** entity with non-empty `ConversationHistory` and a configured external store migrates lazily on next turn; idempotent on retry; `Data.ConversationHistory` cleared on success.
  - **`MigrationPolicy.Explicit`** throws a clear exception until `MigrateAgentConversationAsync` is called.
  - **TTL delete two-phase:** external delete is retried until success before entity self-deletes; entity stays alive across transient external-store failures.
  - **`SessionStateBag` checkpointing:** provider-private state in `DurableAgentSession.StateBag` survives a turn boundary.
- Integration tests in `dotnet/tests/IntegrationTests` running the existing `DurableAgents` samples with both the default store and a Cosmos store, verifying behavioural parity.
- Equivalent Python tests under `python/packages/durabletask/tests/`.
- Documentation updates in `docs/features/durable-agents/`:
  - **New sibling document `durable-agents-conversation-storage.md`** — the store abstraction, the idempotency contract, the bridge limitations, the migration flow, and the `LargePayloadStorage` mitigation as a Phase-1 alternative.
  - **README updates** linking out to the new document.

## Pros and Cons of the Options

### Option 1 — Status quo + document `LargePayloadStorage`

- Good, because zero AF code change and zero ongoing maintenance cost.
- Good, because customers can adopt today without waiting for a new release.
- Good, because the customer's blob account already provides data ownership at the blob level.
- Neutral, because it raises the ceiling from ~1 MB to ~10 MB but does not remove it (driver A only partially satisfied).
- Bad, because it does not satisfy driver B in any structured sense — conversation data is in opaque blob payloads, not queryable as conversation data in the customer's data plane.
- Bad, because it leaves the dual-storage problem in place: every turn still re-checkpoints the full history.

### Option 2 — Pluggable external store (chosen)

- Good, because directly solves driver B (data ownership) — fully via a first-class backend store, and with low friction (at-least-once) via the bridge to existing `ChatHistoryProvider` implementations (driver D).
- Good, because removes the entity-state size ceiling entirely (driver A) when a *reference-holding* external store is configured. (Stores that keep content in their session state — e.g., `InMemoryChatHistoryProvider` — do not relieve the ceiling; see idempotency guarantee 5.)
- Good, because additive and backward-compatible (driver C) — the default store preserves today's behavior exactly.
- Good, because the `CommitTurnAsync` atomic primitive eliminates the partial-turn windows that a split `AppendRequest` + `AppendResponse` shape would expose. Specifically:
  - There is never a window where the entity has persisted a request but not its response, so on replay the load never sees the in-flight request twice.
  - For strong-guarantee stores, the `TryGetTurnAsync` short-circuit handles crash-after-commit-before-checkpoint cleanly. For the bridge, that window is the documented at-least-once duplicate-write case.
- Bad, because the bridge cannot reach exactly-once over the existing `ChatHistoryProvider` contract (no correlation-keyed backend primitive) and depends on `[Experimental]` context types; exactly-once requires a first-class backend store.
- Bad, because users who want to stay on entity-state storage but are bumping the 1 MB limit get no relief from this ADR alone — they need the follow-up compaction ADR or the Option 1 mitigation.
- Bad, because external stores require strong customer-side operational ownership: backup, recovery, access control, retention.

### Option 3 — Sharded entity state

- Good, because preserves the single-source-of-truth-in-DTF model.
- Bad, because forces the size problem into the entity addressing surface: callers must reason about which shard they signal, complicating `IDurableAgentClient` and the DTF dashboard.
- Bad, because does nothing for driver B.
- Bad, because cross-shard reads (full conversation history) require fan-out, which is slow and fragile.
- Bad, because adds significant complexity to TTL, deletion, and reliable-streaming flows.

### Option 4 — Per-content externalization

- Good, because targeted at a real problem (single tool result with a large attachment / image / document).
- Good, because complementary to Option 2 — can be added as a transparent decorator over any store.
- Neutral, because does nothing for cumulative-growth scenarios (many small messages adding up).
- Bad, because as a standalone solution it ignores drivers A and B in the general case.

Deferred to a future ADR as a layered enhancement on top of Option 2.

## More Information

### Implementation phasing

1. **Phase 1 (immediate, no new ADR work):** Update `docs/features/durable-agents/README.md` to document the `LargePayloadStorage` interceptor as the recommended mitigation for users hitting the 1 MB limit today.
2. **Phase 2 (this ADR):** Ship `DurableAgentConversationStore`, `EntityStateConversationStore` (default), at least one first-class backend store (e.g., Cosmos) delivering exactly-once storage, the best-effort `ChatHistoryProviderConversationStore` bridge, the `UseConversationStore` option, the `DurableTaskClient.GetAgentConversationHistoryAsync` extension method, the lazy migration flow, the `MigrationPolicy.Explicit` opt-in, and the schema bump to `1.2.0`. Default behavior unchanged.
3. **Phase 3 (follow-up ADR — entity-level compaction):** Plug the existing `CompactionStrategy` infrastructure into the entity with a bytes-based trigger, using the `ReplaceAsync` primitive reserved in this ADR. Compose with any store.
4. **Phase 4 (follow-up ADR — per-content externalization):** Layer Option 4 as a decorator over any `DurableAgentConversationStore`.

### Cross-language notes

- **Message type names differ:** .NET `ChatMessage` vs. Python `Message`. The .NET sketch uses `ChatMessage`; the Python sketch uses `Message`.
- **Opaque store state shape:** .NET uses `JsonElement` (matching existing entity-state types); Python uses `dict[str, Any]` (matching `_durable_agent_state.py` conventions).
- **Worker configuration style:** .NET uses fluent options on `DurableAgentsOptions.UseConversationStore(...)`; Python uses a constructor arg on `DurableAIAgentWorker(..., conversation_store=...)`, matching the existing `callback=` precedent in `_worker.py`. Both languages get a method-style alternative for late-binding scenarios (`worker.set_conversation_store(...)` in Python) but the constructor-arg form is the documented default.
- **Schema migration:** Both languages need to extend their `from_dict`/`Deserialize` logic to accept the new optional fields (`LastCommittedCorrelationId`, `StoreState`, `SessionStateBag`, `Migrated`, diagnostic metadata) and to handle the schema-version bump. The two languages currently behave **differently** and this must be reconciled: .NET's `DurableAgentStateJsonConverter` rejects any state whose major version is not `1`, whereas Python's `from_dict` does **not** compare versions at all — it only warns and **resets state (discarding all history)** when the `schemaVersion` field is *missing*, and otherwise loads whatever version it finds. The earlier claim that "Python's `from_dict` already warns on version mismatch" was inaccurate. For the additive `1.2.0` bump: .NET accepts it (major still `1`); Python accepts it (no check). Both should treat missing new fields as defaults. As a follow-up, Python should adopt a major-version guard equivalent to .NET's so the cross-language contract is symmetric, and the missing-version reset path (silent history loss) should be re-examined.
- **No reference-holding external `HistoryProvider` in Python yet.** Core Python ships `InMemoryHistoryProvider` (full content in session state) and `FileHistoryProvider` (local disk). Neither relieves the size ceiling for a distributed worker nor provides customer-owned cloud storage, so driver B is realized on .NET first. Python parity requires either a reference-holding `HistoryProvider` (e.g., Cosmos/Redis) or a first-class Python `DurableAgentConversationStore` before the bridge is production-meaningful there.

### Open questions deferred to implementation

- **Bridge `storeState` shape for `ChatHistoryProviderConversationStore`.** The bridge `storeState` carries the wrapped provider's per-session `AgentSession.StateBag` (e.g., the Cosmos `ConversationId`) plus a per-provider extension blob. It does **not** carry a correlation-id dedup buffer as a correctness mechanism — as established in the idempotency section, a marker in `storeState` is rolled back in the dangerous crash window, so the bridge is at-least-once. (A `storeState`-resident recent-correlation-id cache may still be kept as a best-effort optimization to avoid obvious double-writes during normal operation, but it must not be presented as a correctness guarantee.) The exact extension-blob bounds are settled at implementation time.
- **First-class store backend choice and marker schema.** What backend ships first (Cosmos is the leading candidate) and the exact shape of the atomically-written committed-correlation-id marker (e.g., a marker document keyed by `(sessionId, correlationId)` written in the same transaction/batch as the turn's messages) are to be finalized during Phase 2 implementation.
- **Whether to expose the `MigrationPolicy.Explicit` migration call as a CLI tool** in `Microsoft.Agents.AI.Hosting.AzureFunctions` for bulk-migrating tenants. Out of scope for this ADR.
- **Whether `SessionStateBag` checkpointing should be opt-out** for stores that explicitly don't use it. Default behavior is to always checkpoint; opt-out is a perf-only optimization that can be added later if measured to matter.

### Related ADRs

- [ADR-0018](0018-agentthread-serialization.md) — Agent session serialization. Defines the `AgentSession`/`StateBag` shape that this ADR's `SessionStateBag` checkpointing depends on.
- [ADR-0019](0019-python-context-compaction-strategy.md) — Context compaction strategy for long-running agents (Python). Establishes the compaction primitives that the follow-up entity-level compaction ADR will plug into the store contract reserved here.
- [ADR-0022](0022-chat-history-persistence-consistency.md) — Chat history persistence consistency. Establishes the per-run vs per-service-call persistence model that this ADR's `CommitTurnAsync` atomic boundary aligns with.

### References

- `dotnet/src/Microsoft.Agents.AI.DurableTask/AgentEntity.cs` — current entity implementation.
- `dotnet/src/Microsoft.Agents.AI.DurableTask/State/DurableAgentStateData.cs` — current entity-state schema.
- `dotnet/src/Microsoft.Agents.AI.DurableTask/State/DurableAgentState.cs` — `SchemaVersion` field.
- `dotnet/src/Microsoft.Agents.AI.DurableTask/State/DurableAgentStateJsonConverter.cs` — schema-version compatibility check.
- `dotnet/src/Microsoft.Agents.AI.Abstractions/ChatHistoryProvider.cs` — existing .NET provider abstraction to bridge.
- `dotnet/src/Microsoft.Agents.AI.CosmosNoSql/CosmosChatHistoryProvider.cs` — concrete example provider; bridge will adapt this without modifying it.
- `python/packages/core/agent_framework/_sessions.py` — Python `HistoryProvider` and `AgentSession`.
- `python/packages/durabletask/agent_framework_durabletask/_entities.py` — Python `AgentEntity` (same dual-storage problem as .NET today).
- `python/packages/durabletask/agent_framework_durabletask/_worker.py` — Python `DurableAIAgentWorker` (new `conversation_store=` constructor arg).
- `python/packages/durabletask/agent_framework_durabletask/_durable_agent_state.py` — Python entity-state schema and `from_dict` migration logic.
- `durabletask-dotnet/src/Extensions/AzureBlobPayloads/` — the `LargePayloadStorage` interceptor recommended as the Phase-1 mitigation.
