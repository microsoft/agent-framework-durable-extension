# Conversation Compaction with Durable Agents

Shows an agent whose conversation history is **persisted durably** and **compacted as it grows**,
using the same configuration you would write for in-process Agent Framework.

> [!WARNING]
> Use a new isolated task hub shared only with compatible canonical `2.0.0` workers and upgraded
> clients. Explicitly set `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` only after verifying that
> isolation. This is an operator acknowledgement, not runtime proof of isolation or a migration
> step. Keep old workflow histories on the old engine. Schema validation and localhost do not
> prove isolation, and this sample does not establish .NET interoperability.

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
    context_providers=[compaction, history],
)
```

Core runs after hooks in reverse order. Putting this after-only compaction provider first
lets history append the current input and answer before the strategy keeps its four groups
for the next turn. A group is not necessarily a whole turn.

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
compaction does not delete the local transcript. Original response delivery is independent of
history. See the common [delivery, expiry and maintenance contract](../../packages/durabletask/README.md#delivery-and-maintenance).

### Retention and state budgets

Configure `DurableAIAgentWorker` or override with `add_agent()`.

- `retention="keep_all"` leaves compaction exclusions stored. `follow_compaction` opts into eager
   removal of eligible exclusions without enabling a byte budget.
- `max_state_bytes=None` disables pressure eviction, not backend limits. A positive integer enables
   it independently of retention. This standalone DTS worker also accepts `"backend_limit"` (1 MiB).

For pressure eviction without eager pruning, use `retention="keep_all"`, `max_state_bytes=1_048_576`,
`high_watermark=0.85` and `low_watermark=0.70`. See the
[shared retention contract](../../packages/durabletask/README.md#retention-and-state-budgets) for
watermark validation, whole-entity accounting, protected atomic groups and `StateCapacityError`.
Budget for live responses, receipts and session state as well as history. Do not shorten delivery
expiry to make the demo fit. Neither option bounds receipt growth or cleans external/service history.

### Client-side vs service-managed history

This sample sets `store=False` so client-side history and compaction control model context.
On service-owned turns, durable history neither loads nor appends a local transcript. Session and
delivery state still persist, and switching ownership does not erase old local history. See
[history ownership and provider ordering](../../packages/durabletask/README.md#history-provider-integration).

## Running the sample

1. Start the Durable Task Scheduler emulator:

   ```bash
   docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 -e DTS_USE_DYNAMIC_TASK_HUBS=true mcr.microsoft.com/dts/dts-emulator:latest
   ```

2. Copy `.env.example` to `.env` and set `FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`.

   Choose a **new isolated task hub** for `TASKHUB`, shared only with compatible canonical `2.0.0`
   workers and clients. Use the same hub for the worker and client. Keep old
   workflow histories on the old engine, not on this hub.

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

The client runs a multi-turn conversation and then asks the agent to recall a fact from a **recent**
turn. The fact should still be in the retained window.

A sliding window leaves older turns out of model context, so the model may no longer recall their
facts. With the sample's `keep_all` and disabled pressure budget, those messages remain in local
durable storage, marked as excluded. Opting into eager pruning or a byte budget can delete eligible
history. Summarization is an alternative when older details need to stay in context.
