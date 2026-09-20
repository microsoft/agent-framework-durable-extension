# External Conversation History (Redis) with Durable Agents

Shows an agent whose conversation history lives in a **user-chosen external store** rather than in
durable entity state, using the same configuration you would write for in-process Agent Framework.

> [!WARNING]
> Use a new isolated task hub shared only with compatible canonical `2.0.0` workers and upgraded
> clients. Explicitly set `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` only after verifying that
> isolation. This is an operator acknowledgement, not runtime proof of isolation or a migration
> step. Keep old workflow histories on the old engine. Schema validation and localhost do not
> prove isolation, and this sample does not establish .NET interoperability.

## What this demonstrates

The agent is built with an ordinary `HistoryProvider` that happens to be backed by Redis:

```python
# taskhub is the resolved, case-preserved hub used to create this worker.
key_prefix = f"durable_sample:history:hub:{taskhub.encode('utf-8').hex()}"
history = RedisHistoryProvider("redis://localhost:6379", key_prefix=key_prefix)
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
   The worker also supplies a hub-specific prefix so reusing a session id in another hub does not
   select the same Redis transcript.
- **The provider owns transcript writes.** Core calls it according to `store_inputs`,
  `store_outputs`, `store_context_messages`, and `store_context_from`. This sample uses the default
  input/output storage flags and `store=False` so the provider supplies model context.
- **Delivery is separate from history.** Fresh durable entity state has an empty `conversationHistory`, not metadata-only exchange envelopes or a local transcript mirror. It stores session state, original response envelopes in canonical `terminalResults` by correlation ID, and matching `completionReceipts`. Delivery expiry is independent of Redis history.

Each result and receipt has `correlationId`, known `outcome` (`succeeded` or `failed`) and `completedAt`.
The result includes the full inline `response`, including `messages`, optional structured `value`
(even null or falsey values) and metadata. Failure also requires canonical `error.code` and
`error.message`. Success forbids `error`. Receipt `resultState="available"` requires a matching
result. `unavailable` forbids a result and requires `resultUnavailableAt`. Completion facts and
optional `resultExpiresAt` must agree between result and receipt.

The Python runtime assigns a delivery deadline, though `resultExpiresAt` is optional in the shared
contract. At or after a configured deadline, lookup reports completed-but-result-unavailable with
the retained outcome, even before physical cleanup. It does not return the expired payload, report
pending work, rerun the request or reconstruct an original response from Redis history. Cleanup
removes the result and records `resultUnavailableAt` without changing completion facts. Idle physical
cleanup needs an application-owned schedule or backend operation. There is no `unknown` v2 outcome.
See the [shared state contract](../../../schemas/README.md).

### Deployment namespace

Both sample entrypoints pass the same resolved `TASKHUB` to worker creation and agent setup.
Redis keys use `durable_sample:history:hub:<UTF-8 hex of TASKHUB>:<session id>`. Encoding the hub
preserves casing and keeps delimiters in a hub name from merging with the session suffix. Restarting
with the same hub, session id, and Redis store selects the same key. This is key isolation, not a
metric label or proof that the durable deployment is isolated.

The namespace does not include the scheduler endpoint. If hub names are reused across schedulers,
use a **distinct Redis store or an explicit deployment-specific `key_prefix`** in the agent factory.
Do not share the sample's hub-derived prefix in that case. Keep the chosen namespace stable across
restarts. Hex is an encoding, not a way to hide the hub name.

The plain `RedisHistoryProvider` keeps its generic `durable_sample:history` default for compatibility
and still accepts an explicit `key_prefix`. Only this sample's agent factory opts into hub scoping.
Existing keys under the old generic prefix are not read or migrated automatically.

### External write lifecycle

The [Redis provider](redis_history_provider.py) is deliberately small, using a list read and a blind
`RPUSH`. It is not an exactly-once storage implementation. If Redis accepts an append but the durable
operation is interrupted before its local state commits, retrying can append the same messages
again. A stable session id does not prevent that. A production provider needs its own idempotency
policy for external writes.

Each read or append creates and closes its own async Redis client on the event loop
running that operation. The provider keeps no connection pool between calls, so neither
entry point needs to close a worker-loop pool from its main loop during shutdown. This
trades connection reuse for a small, loop-safe example. Both entry points scope worker
setup and execution in the worker's context manager so it stops on normal exit or failure.

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
   docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 -e DTS_USE_DYNAMIC_TASK_HUBS=true mcr.microsoft.com/dts/dts-emulator:latest
   docker run -d --name redis -p 6379:6379 redis:latest
   ```

2. Copy `.env.example` to `.env` and set `FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`.

   Choose a **new isolated task hub** for `TASKHUB`, shared only with compatible canonical `2.0.0`
   workers and clients. Use the same hub for the worker and client. Keep old
   workflow histories on the old engine, not on this hub. If another scheduler reuses that hub
   name, use a separate Redis store or change the factory's prefix as described above.

   Only after verifying those conditions, explicitly set the following in `.env`.
   Replace the example hub name with your new hub's name.

   ```dotenv
   TASKHUB=DurableAgentsV2Sample
   DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2
   ```

   The supplied acknowledgement is deliberately blank. This is an operator acknowledgement,
   not runtime proof of isolation or a migration step. Do not set it for an arbitrary
   production hub or incompatible peers.

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

The client states a fact and then asks for it back in a later turn. The agent should recall it from
the Redis-backed history supplied to the model. Recall alone does not prove storage behavior.
Inspect Redis to check that the provider stored the earlier turn. The durable runtime does not
replay a local transcript for this agent.

To see it directly, inspect the Redis key while the sample runs:

```bash
docker exec -it redis redis-cli --scan --pattern 'durable_sample:history:hub:*'
```
