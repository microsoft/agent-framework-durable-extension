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
  preserving its `source_id` so the compaction provider stays wired to it. Conversation state lives
  in the agent's durable entity and survives worker restarts.
- **Compaction state is persisted.** Annotations produced by the strategy are stored alongside the
  messages, so compaction is not recomputed from scratch on every turn.
- **Context stays bounded.** Only the messages the strategy keeps are sent to the model, so a long
  conversation does not grow the per-turn context without limit.

The full conversation remains in durable storage, and compaction bounds what the *model* sees. To
also bound what is *stored*, opt in at registration with `add_agent(agent, prune_history=True)`,
which is lossy and therefore off by default.

### Client-side vs service-managed history

Compaction only applies to history the **client** owns. When a chat client keeps the conversation on
the service (Foundry and the Responses API both do so by default), the service owns the model's
context, the durable entity keeps the transcript purely as a record, and the durable history provider
stays out of the way. This sample sets `store=False` so history is client-side and compaction has
something to compact.

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
turn, which it answers correctly.

The trade-off is the point of the sample: a sliding window keeps context bounded by *dropping* older
turns from what the model sees, so facts from long-past turns are genuinely no longer available to
the model. Those messages are **not deleted**. They remain in durable storage, marked as excluded,
so the conversation record stays complete and auditable. Choose a strategy accordingly: use
summarization if old details must survive in the model's context, and a sliding window when only
recent context matters.
