# Conversation Compaction Sample (Python)

This sample demonstrates hosting an agent whose conversation history is **persisted durably** and
**compacted as it grows**, using the same configuration you would write for in-process Agent
Framework.

> [!WARNING]
> Use a new, empty, uniquely named task hub reserved for this deployment, with compatible
> canonical `2.0.0` workers and upgraded clients. Do not use the `default` hub or share it with
> old or unrelated workers. Keep existing instances and recorded workflow histories on their
> original hub and engine. Set `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` only after verifying
> isolation. This is an operator acknowledgement, not proof of isolation, automatic history
> migration, or a cross-runtime compatibility guarantee. See the
> [common deployment requirements](../README.md#3-running-the-samples).

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
    context_providers=[compaction, history],
)

app = AgentFunctionApp(
    agents=[agent],
    enable_health_check=True,
    retention="keep_all",
    max_state_bytes=None,
)
```

Core runs after hooks in reverse order. Putting this after-only compaction provider first
lets history append the current input and answer before the strategy keeps its four groups
for the next turn. A group is not necessarily a whole turn.

This sample stores inputs and outputs and keeps them with explicit `retention="keep_all"` and
`max_state_bytes=None`, which are also the host defaults. Original response delivery is independent
of history. See the common [delivery, expiry and maintenance contract](../../../packages/durabletask/README.md#delivery-and-maintenance).

### Retention and state budgets

Configure `AgentFunctionApp` or override with `add_agent()`.

- `retention="keep_all"` leaves compaction exclusions stored. `follow_compaction` opts into eager
  removal of eligible exclusions without enabling a byte budget.
- `max_state_bytes=None` disables pressure eviction, not backend limits. Opt in independently with
  a positive integer, for example `max_state_bytes=1_048_576, high_watermark=0.85, low_watermark=0.70`.
  Functions rejects `"backend_limit"`, even when using DTS.

See the [Functions settings and shared contract](../../../packages/azurefunctions/README.md#retention-and-state-budgets)
for watermark validation, whole-entity accounting, protected atomic groups and `StateCapacityError`.
Budget for live responses, receipts and session state as well as history. Do not shorten delivery
expiry to make the demo fit. Neither option bounds receipt growth or cleans external/service history.

### Client-side vs service-managed history

This sample sets `store=False` so client-side history and compaction control model context.
On service-owned turns, durable history neither loads nor appends a local transcript. Session and
delivery state still persist, and switching ownership does not erase old local history. See
[history ownership and provider ordering](../../../packages/durabletask/README.md#history-provider-integration).

## Prerequisites

Follow the [common setup steps](../README.md) to install tooling, configure Foundry
credentials, and install the Python dependencies for this sample. This sample uses
`FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`.

As shipped, [host.json](host.json) has no `storageProvider` selection, so this sample uses the
**Azure Storage backend**, with local Azurite through `AzureWebJobsStorage`. The DTS connection
string in [local.settings.json.template](local.settings.json.template) does not select DTS.
No DTS emulator is required for this default configuration.

Before starting the host, replace `TASKHUB_NAME=durablesamplev2UNIQUE` in the template with a new,
empty, uniquely named hub in that storage account or Azurite instance. Use an alphanumeric name,
not `default`. `TASKHUB_NAME` supplies the hub through [host.json](host.json). Any host-level hub
override and all compatible clients must target the same hub and backend.

If you choose DTS for a new deployment, follow the
[optional backend setup](../README.md#optional-durable-task-scheduler-backend). It requires an
explicit `storageProvider.type="azureManaged"` and a supporting host extension. Keep the connection
string's `TaskHub` identical to `TASKHUB_NAME` and any host override. Provision the hub first for an
Azure-hosted scheduler, or enable dynamic hubs in the local emulator. Do not switch an existing
deployment's backend in place. The sample's default configuration remains Azure Storage.

Only after verifying isolation, set `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2`.
Its template value is deliberately blank. Leaving the acknowledgement unset or blank intentionally
fails host initialization. The sample does not set it in code or automatically isolate the deployment.
Copy the configured template to the local settings file as described in the common setup steps.

## Running the Sample

Send several turns using the **same** session id so they form one conversation. [demo.http](demo.http)
contains a ready-made sequence, and the equivalent with `curl` is:

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
