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

- **Your provider stays active for this client-owned run.** The exact built-in
   `InMemoryHistoryProvider` is swapped for a durable-backed one (see
   [13_conversation_compaction](../13_conversation_compaction)), but this Redis provider keeps its
   hooks and storage. On a service-owned run, the inactive primary is wrapped to suppress load/store
   hooks. Use a distinct store-only sink to audit both branches.
- **It receives a stable session id.** The durable entity creates a fresh session per operation but
  gives it the entity's own session id, so the provider reads and writes the same key every turn.
  Without that, an externally keyed store would start a new conversation on each turn.
- **The provider owns transcript writes.** Core calls it according to `store_inputs`,
  `store_outputs`, `store_context_messages`, and `store_context_from`. This sample uses the default
  input/output storage flags and `store=False` so the provider supplies model context.
- **Delivery is separate from history.** Fresh durable entity state has an empty
  `conversationHistory`, not metadata-only exchange envelopes or a local transcript mirror. It
  stores session state, original responses in `responseMailbox` by correlation id, and completion
  evidence in `completedCorrelations`. Delivery payloads expire independently of Redis history.

The [Redis provider](redis_history_provider.py) is deliberately small, using a list read and a blind
`RPUSH`. It is not an exactly-once storage implementation. If Redis accepts an append but the durable
operation is interrupted before its local state commits, retrying can append the same messages
again. A stable session id does not prevent that. A production provider needs its own idempotency
policy for external writes.

Portable durable `reset` is unsupported for this external provider. Clearing its history requires a
provider-owned operation and coordination with the caller. The sample does not implement one.

The default `retention="keep_all"` and `max_state_bytes=None` do not prune Redis and do not enable
local pressure eviction. `follow_compaction` or an explicit local byte budget does not manage Redis
retention either. External storage does not give the entity unlimited capacity. Live response
payloads, session state, and completion receipts still need space, and completion receipts persist
until entity deletion. Existing local history from before an ownership change is not erased.

## Running the sample

1. Start the Durable Task Scheduler emulator and Redis:

   ```bash
   docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
   docker run -d --name redis -p 6379:6379 redis:latest
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

The client states a fact and then asks for it back in a later turn. The agent answers correctly,
which is only possible if Redis served the earlier turn back into the model's context. The durable
runtime itself never replays history for this agent.

To see it directly, inspect the Redis key while the sample runs:

```bash
docker exec -it redis redis-cli KEYS 'durable_sample:history:*'
```
