# Conversation Compaction Sample (Python)

This sample demonstrates hosting an agent whose conversation history is **persisted durably** and
**compacted as it grows**, using the same configuration you would write for in-process Agent
Framework. It is the Azure Functions counterpart to the standalone
[`13_conversation_compaction`](../../13_conversation_compaction) sample.

## Key Concepts Demonstrated

- Configuring compaction the ordinary core way, an `InMemoryHistoryProvider` plus a
  `CompactionProvider`, with **no durable-specific configuration on the agent**.
- The durable runtime swapping the in-memory provider for a durable-backed one at registration,
  preserving its `source_id` and storage flags. The provider owns appends according to
  `store_inputs`, `store_outputs`, `store_context_messages`, and `store_context_from`.
- Compaction annotations being persisted alongside the stored messages for later turns.
- Limiting the number of history groups sent to the model, not the size of individual messages
  or the whole entity.

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

app = AgentFunctionApp(
    agents=[agent],
    enable_health_check=True,
    retention="keep_all",
    max_state_bytes=None,
)
```

This sample stores inputs and outputs and keeps them with explicit `retention="keep_all"` and
`max_state_bytes=None`, which are also the host defaults. Original responses live independently
in the correlation-keyed `responseMailbox` until delivery expiry. `completedCorrelations` keeps
completion evidence after those payloads expire.

### Retention and state budgets

`retention="keep_all"` disables eager deletion of compaction exclusions. It does not disable an
explicit byte budget. `retention="follow_compaction"` prunes eligible exclusions from local durable
history, protecting system messages and the newest/current exchange, but does not enable pressure
eviction by itself.

`max_state_bytes=None` disables pressure eviction, not the backend's capacity limit. To opt in,
pass a positive integer chosen for your backend and workload, for example
`max_state_bytes=1_048_576, high_watermark=0.85, low_watermark=0.70`. These watermark values are the
defaults and must satisfy `0 < low_watermark < high_watermark <= 1`. The budget is independent of
`retention` and can be used with either mode. Configure them on `AgentFunctionApp` or override
them with `add_agent`.

Functions cannot infer the backend's limit and rejects `max_state_bytes="backend_limit"`. That
option resolves to 1,048,576 bytes (1 MiB) only on the standalone DTS worker.

Pressure eviction measures the whole serialized entity. It starts at the high watermark and aims
for the low watermark, or the protected state size if larger. Live responses, completion receipts,
session state, metadata, and protected transcript groups all need space. If the protected state
reaches the high watermark, the operation fails with `StateCapacityError` rather than deleting
responses still owed to callers. A burst of turns can fill a small budget even after history is
pruned. Do not shorten delivery expiry to force the sample to fit.

Neither mode gives unlimited capacity. Completion receipts persist until entity deletion, and
mailbox expiry is separate from transcript retention. These settings do not prune an external
store or service-managed history.

### Client-side vs service-managed history

Compaction only applies to history the **client** owns. On a service-owned turn, the durable history
provider neither loads nor appends a local transcript. Session state, response delivery payloads,
and completion receipts are still persisted, not a second conversation record. Switching ownership
does not erase existing local history.

The runtime resolves ownership from the run's `store` option, then the agent's `default_options`,
then the client's default. This sample sets `store=False` so client-side history and compaction
control model context rather than Foundry's service-managed history.

## Prerequisites

Follow the common setup steps in `../README.md` to install tooling, configure Foundry
credentials, and install the Python dependencies for this sample. This sample uses
`FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`.

## Running the Sample

Send several turns using the **same** session id so they form one conversation. `demo.http` contains
a ready-made sequence, and the equivalent with `curl` is:

```bash
curl -X POST http://localhost:7071/api/agents/Historian/run \
     -H "Content-Type: application/json" \
     -d '{"message": "My project codename is BLUEHERON.", "session_id": "compaction-demo-001"}'

curl -X POST http://localhost:7071/api/agents/Historian/run \
     -H "Content-Type: application/json" \
     -d '{"message": "What is my project codename? Reply with just the codename.", "session_id": "compaction-demo-001"}'
```

## What to look for

The agent answers correctly from a **recent** turn while older turns fall outside the retained
window.

A sliding window leaves older turns out of model context, so the model may no longer recall their
facts. With the sample's `keep_all` and disabled pressure budget, those messages remain in local
durable storage, marked as excluded. Opting into eager pruning or a byte budget can delete eligible
history. Summarization is an alternative when older details need to stay in context.
