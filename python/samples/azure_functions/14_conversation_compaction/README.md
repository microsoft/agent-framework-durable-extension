# Conversation Compaction Sample (Python)

This sample demonstrates hosting an agent whose conversation history is **persisted durably** and
**compacted as it grows**, using the same configuration you would write for in-process Agent
Framework. It is the Azure Functions counterpart to the standalone
[`13_conversation_compaction`](../../13_conversation_compaction) sample.

## Key Concepts Demonstrated

- Configuring compaction the ordinary core way, an `InMemoryHistoryProvider` plus a
  `CompactionProvider`, with **no durable-specific configuration on the agent**.
- The durable runtime swapping the in-memory provider for a durable-backed one at registration,
  preserving its `source_id` so the compaction provider stays wired to it.
- Compaction annotations being persisted alongside the messages, so compaction state is not
  recomputed from scratch on every turn.
- Context growth being bounded: only the messages the strategy keeps are sent to the model.

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

app = AgentFunctionApp(agents=[agent], enable_health_check=True)
```

The full conversation remains in durable storage, and compaction bounds what the *model* sees. To
also bound what is *stored*, opt in at registration with `AgentFunctionApp(..., prune_history=True)`,
which is lossy and therefore off by default.

### Client-side vs service-managed history

Compaction only applies to history the **client** owns. When a chat client keeps the conversation on
the service (Foundry and the Responses API both do so by default), the service owns the model's
context, the durable entity keeps the transcript purely as a record, and the durable history provider
stays out of the way. This sample sets `store=False` so history is client-side and compaction has
something to compact.

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

The trade-off is the point of the sample: a sliding window keeps context bounded by *dropping* older
turns from what the model sees, so facts from long-past turns are genuinely no longer available to
the model. Those messages are **not deleted**. They remain in durable storage, marked as excluded,
so the conversation record stays complete and auditable. Choose a strategy accordingly: use
summarization if old details must survive in the model's context, and a sliding window when only
recent context matters.
