# External Conversation History (Redis) with Durable Agents

Shows an agent whose conversation history lives in a **user-chosen external store** rather than in
durable entity state, using the same configuration you would write for in-process Agent Framework.

## What this demonstrates

The agent is built with an ordinary `HistoryProvider` that happens to be backed by Redis:

```python
history = RedisHistoryProvider("redis://localhost:6379")
agent = Agent(
    client=...,
    name="Archivist",
    default_options={"store": False},
    context_providers=[history],
)
```

Registering that agent with the durable runtime changes nothing about how you configure it:

- **Your provider is left alone.** Unlike an `InMemoryHistoryProvider` — which is swapped for a
  durable-backed one (see [13_conversation_compaction](../13_conversation_compaction)) — a provider
  you chose deliberately is never substituted. You picked where the conversation lives.
- **It receives a stable session id.** The durable entity creates a fresh session per operation but
  gives it the entity's own session id, so the provider reads and writes the same key every turn.
  Without that, an externally keyed store would start a new conversation on each turn.
- **Execution is still durable.** Retries, restarts, and orchestration guarantees are unchanged, and
  durable state still records the conversation for audit.

`redis_history_provider.py` is deliberately small — roughly "read a list, append to a list" — to show
how little a bring-your-own-store provider needs. The same shape applies to Cosmos DB, a file, or any
other backend.

## Running the sample

1. Start the Durable Task Scheduler emulator and Redis:

   ```bash
   docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
   docker run -d --name redis -p 6379:6379 redis:latest
   ```

2. Copy `.env.example` to `.env` and set `AZURE_OPENAI_ENDPOINT` and `AZURE_OPENAI_MODEL`.

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

The client states a fact and then asks for it back in a later turn. The agent answers correctly,
which is only possible if Redis served the earlier turn back into the model's context — the durable
runtime itself never replays history for this agent.

To see it directly, inspect the Redis key while the sample runs:

```bash
docker exec -it redis redis-cli KEYS 'durable_sample:history:*'
```
