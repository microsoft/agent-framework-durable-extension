# Conversation Compaction with Durable Agents

Shows an agent whose conversation history is **persisted durably** and **compacted as it grows**,
using the same configuration you would write for in-process Agent Framework.

## What this demonstrates

The agent is built with a plain `InMemoryHistoryProvider` and a `CompactionProvider`:

```python
history = InMemoryHistoryProvider(skip_excluded=True)
compaction = CompactionProvider(
    after_strategy=SlidingWindowStrategy(keep_last_groups=4),
    history_source_id=history.source_id,
)
agent = Agent(
    client=...,
    name="Historian",
    default_options={"store": False},
    context_providers=[history, compaction],
)
```

Registering that agent with the durable runtime changes nothing about how you configure it, but:

- **History becomes durable.** The runtime swaps the in-memory provider for a durable-backed one,
  preserving its `source_id` and storage flags. The provider owns transcript appends according to
  `store_inputs`, `store_outputs`, `store_context_messages`, and `store_context_from`. This sample's
  stored inputs and outputs survive worker restarts in the agent's durable entity.
- **Compaction state is persisted.** Annotations produced by the strategy are stored alongside the
  messages and are available on later turns.
- **The history window stays small.** Only the history groups the strategy keeps are sent to the
  model on the next turn. Individual messages can still be large.

With this sample's storage flags and explicit `retention="keep_all", max_state_bytes=None`,
compaction does not delete the local transcript. Original responses are also retained temporarily
in `responseMailbox`, keyed by correlation id, independently of the model's compacted history.
`completedCorrelations` records completion even after response delivery expires.

### Retention and state budgets

Compaction selects model context. `retention` controls eager deletion of eligible compaction
exclusions. `max_state_bytes` independently controls pressure eviction of local transcript groups.
Set these on `DurableAIAgentWorker` or override them with `add_agent`.

| Mode | Behavior |
| --- | --- |
| `keep_all` (default) | Does not eagerly delete compaction exclusions. An explicitly configured byte budget can still evict eligible transcript groups. |
| `follow_compaction` | Eagerly deletes eligible exclusions from local durable history, protecting system messages and the newest/current exchange. It does not enable a byte budget. |

`max_state_bytes=None` is the default and disables pressure eviction, not the backend's size limit.
On the standalone DTS worker, `max_state_bytes="backend_limit"` resolves to 1,048,576 bytes (1 MiB).
An explicit positive integer is also accepted. Azure Functions cannot infer its backend limit, so
it requires an integer to enable pressure eviction and rejects `"backend_limit"`.

For example, to retain compaction exclusions until state pressure requires eviction, use
`retention="keep_all", max_state_bytes=1_048_576, high_watermark=0.85, low_watermark=0.70`.
Use `retention="follow_compaction"` to opt into eager pruning as well. The watermark defaults are
`0.85` and `0.70`, with `0 < low_watermark < high_watermark <= 1`. Pressure eviction starts at the
high watermark and aims for the low watermark, or the protected state size if that is larger.

The budget measures the whole serialized entity, including live mailbox payloads, completion
receipts, session state, and metadata. Pressure eviction preserves protected state and evicts whole
atomic transcript groups. If protected state alone reaches the high watermark, the operation fails with
`StateCapacityError` rather than discarding responses still owed to callers. Size a budget for those
delivery obligations and the backend limit. A burst of turns can fill a small budget even after
transcript pruning. Do not shorten delivery expiry just to make a demo fit.

Neither retention mode provides unlimited capacity. Completion receipts persist until entity
deletion, and mailbox payloads expire independently of transcript retention. Retention does not
prune an external store or service-managed history.

### Client-side vs service-managed history

Compaction only applies to history the **client** owns. When Foundry or the Responses API owns a
turn's history, the durable history provider neither loads nor appends a local transcript. The
entity still persists session state, response delivery payloads, and completion receipts, not a
second conversation record. Existing local history is not erased when ownership changes.

Ownership is resolved for each run from its `store` option, then the agent's `default_options`,
then the client's default. This sample sets `store=False` so the client-side history provider and
compaction control model context.

## Running the sample

1. Start the Durable Task Scheduler emulator:

   ```bash
   docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
   ```

2. Copy `.env.example` to `.env` and set `FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`.

3. Sign in for `AzureCliCredential`:

   ```bash
   az login
   ```

4. Install dependencies and start the worker:

   ```bash
   pip install -r requirements.txt
   python worker.py
   ```

5. In another terminal, run the client:

   ```bash
   python client.py
   ```

## What to look for

The client runs a multi-turn conversation and then asks the agent to recall a fact from a **recent**
turn. The fact should still be in the retained window.

A sliding window leaves older turns out of model context, so the model may no longer recall their
facts. With the sample's `keep_all` and disabled pressure budget, those messages remain in local
durable storage, marked as excluded. Opting into eager pruning or a byte budget can delete eligible
history. Summarization is an alternative when older details need to stay in context.
