# Get Started with Microsoft Agent Framework Durable Task

[![PyPI](https://img.shields.io/pypi/v/agent-framework-durabletask)](https://pypi.org/project/agent-framework-durabletask/)

Please install this package via pip:

```bash
pip install agent-framework-durabletask --pre
```

## Durable Task Integration

The durable task integration lets you host Microsoft Agent Framework agents using the [Durable Task](https://github.com/microsoft/durabletask-python) framework so they can persist state, replay conversation history, and recover from failures automatically.

### Reader-first phased rollout

The current local implementation adds canonical `2.0.0` readers, not a released capability.
Mutable `DurableAgentState` still defaults to `1.1.0`. Exact `1.0.0`, `1.1.0` and `1.2.0`
inputs use the existing legacy model. This does not promise a full raw-preserving `1.2.0`
round trip.

Public `agent_framework_durabletask.read_agent_state(raw)` accepts a dictionary or JSON string.
For `2.0.0`, it returns a detached `SharedAgentStateReader`. Its `to_dict()` preserves the
original JSON, including unknown fields and profiles. `try_get_agent_response(correlation_id)`
looks up canonical results and completion receipts without lifecycle writes. Unknown profiles
remain inert in storage. A targeted unsupported profile projection may fail without changing
the original snapshot. Typed shared results require a present `value` and a JSON-preserving
projection. Consumers do not infer it from text, coerce types or discard unknown value fields.
`serialize_agent_response()` carries these restrictions through Core JSON using the versioned
`_durable_value_policy` marker. It does not duplicate the value, identify a runtime class or
establish completion authority. Use the matching loader when consuming that snapshot.

Both Durable Task and Azure Functions polling consumers read canonical results and retain
known completion outcomes when response delivery expires or is unavailable. Durable Task
storage reads use bounded polling retries.

V2 snapshots are raw-only, read-only state. `AgentEntity.run()`, `reset()`, state assignment
and `persist_state()` reject v2 backing state, even with empty history and result maps or a
duplicate request. SDK, HTTP and MCP run APIs still signal a command before polling.
**Read-only views are not read-only run APIs.** Do not use those APIs to execute against
v2-backed entities with this implementation or to guarantee that no command was dispatched.

Roll out reader support first. A matching future writer is required before targeting a v2
deployment. Do not roll back v2 state to an older lossy reader/writer. This phase adds no
automatic activation, deployment gate or environment switch and makes no historical workflow
changes. V2 readers do not resume provider sessions or workflow history.

The Core requirement remains `agent-framework-core>=1.13.0,<2`. This package directly requires
`pydantic>=2.11,<3` for structured response handling.

### Private delivery staging

The next stack layer adds private delivery operations over complete canonical v2 JSON snapshots.
Recording stages an original result with its matching receipt. Expiry stages removal only when
the stored deadline is due and retains the original completion outcome and timestamps. Duplicate
correlations do not replace results or refresh their delivery window. Failed staging leaves the
input snapshot unchanged, including unknown JSON.

These functions do not persist state or activate a v2 entity writer. The mutable writer still
uses legacy `1.1.0`, and v2 execution and mutation remain rejected. A later runtime layer must
validate its complete candidate against an independent committed baseline before storage.
Read-only consumers reuse the same lookup rules, without transcript fallback or cleanup writes.

### Private canonical history bridge

The next private layer adds a typed canonical transcript and a Core history-provider bridge.
It preserves unknown JSON, session state and original public message IDs separately from
internal reconciliation identities. Delivery methods reuse the private staging operations
without replacing typed history objects. History hooks stage appends and compaction annotations
in memory. Excluded messages can be omitted from model input but are not physically deleted.

Append staging is atomic in memory, undoing lazy internal-ID repairs and restoring the transcript,
working buffers, position indexes and append ordinal if staging fails. Earlier successful saves
and flushes, including the preliminary flush in `save_messages()`, remain intact. This does not
commit to the backend or roll back external provider writes.

Requests and responses are checked as stored transcript JSON before publication. Failed-run
finalization retains pending tool results if filtering or staging fails, allowing a retry.

The private bridge requires an exact `2.0.0` snapshot. Even `get_messages()` may repair missing
or duplicate internal IDs, so it is not a read-only inspection path. Mutation-capable hooks reject
legacy and unsupported versions before changing stored history or working state. Use
`read_agent_state()` for legacy inspection instead.

External primary providers, service-owned history and store-only audit sinks keep separate roles.
Preparation permits only one `DurableHistoryProvider`, including an injected or replaced primary.
Different source IDs do not create separate durable transcripts. Multiple durable adapters are
rejected before model execution, even when loading or all store flags are disabled, because loading
and flushing can still update shared history. Use an ordinary store-only `HistoryProvider` with a
distinct source ID for an independent audit store.
A single load-disabled durable adapter may audit an external primary into the canonical transcript.
Its stored messages and public IDs do not acknowledge input delivery. Only the active primary's
acceptance evidence contributes ingestion receipts, so an audit cannot suppress a later retry.
On a service-owned turn, the inactive external primary's custom hooks are also suppressed because
they may load or persist history directly. Ownership-independent work belongs in a separate context
provider or store-only sink. Client-owned turns retain the original primary's hooks and resources.
Compaction aimed at an inactive external history source does not run its stored-history after
hook. Its before hook still operates on unrelated current context, as Core specifies.
Ordinary input and response IDs are preserved. Newly generated compaction summary occurrences get
unique IDs when a strategy reuses a candidate ID, so both summary revisions and their links survive.
With no primary, history is injected before a matching before-compaction provider. Core runs before
hooks forward and after hooks in reverse, so that provider's after hook sees the previously stored
history. After-only compaction retains the existing append-then-compact order. Per-service-call
history uses Core's middleware cadence and does not run compaction hooks per model call.
Injection honors a single configured compaction history source. Conflicting sources require an
explicit primary instead of silently selecting a source that cannot serve all configured hooks.
All explicit primary providers retain the user's hook order, including a built-in in-memory provider
replaced with durable history. With `[compaction, history]`, before compaction cannot see history
loaded later, while reverse after hooks append the current input and output before compaction.
With `[history, compaction]`, before compaction sees loaded history, while the ordinary per-run
after compaction runs before the current turn is appended. Choose the order explicitly for the
strategy's intended inputs. Preparation does not mutate the caller's agent or provider list.
Provider namespaces must be unique, including store-only sinks, and conflicts with an injected
history source raise an error rather than automatically reassigning a namespace.

Canonical media keeps its declared content kind rather than inferring it from the URI scheme.
Literal Core input preserves absent versus explicit-null function results and error details on
the first canonical write and later cold reads. Plain Core objects without an attached input
envelope keep canonical nullable defaults. Existing shared JSON retains its raw field presence.
New response timestamps without an offset are interpreted as UTC. Valid stored timestamps retain
their original offset and fractional precision.

These modules are not exported or connected to either host. Public `DurableAgentState` remains
the legacy writer, and the existing v2 mutation guards remain active. There is no new supported
deployment mode, migration operation, retention policy or workflow engine in this layer.
Host activation, transactional session capture and the versioned workflow start boundary must
land together in the later runtime layer. The private model alone does not enforce a committed
storage baseline or make provider side effects transactional.

### JSON runtime boundary

The local runtime requires `durabletask>=1.7.1,<2`. Construct `DurableAIAgentWorker` before
starting the SDK worker. Framework-selected reads for generated agents and workflows use plain
JSON rather than SDK custom-object reconstruction. Native co-hosted work keeps its original
converter behavior. See
[Python durable JSON boundaries](../../../docs/features/python-durable-json-boundaries.md) for
covered paths, custom-converter constraints and separate checkpoint trust requirements.

### Basic Usage Example

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIChatCompletionClient
from agent_framework_durabletask import DurableAIAgentWorker
from durabletask.worker import TaskHubGrpcWorker

# Create the worker
worker = TaskHubGrpcWorker(host_address="localhost:4001")
agent_worker = DurableAIAgentWorker(worker)

chat_client = OpenAIChatCompletionClient()
my_agent = Agent(client=chat_client, name="assistant")
agent_worker.add_agent(my_agent)
```

For more details, review the standalone [Durable Task samples](https://github.com/microsoft/agent-framework-durable-extension/tree/main/python/samples) and the full [Agent Framework Python documentation](https://github.com/microsoft/agent-framework/tree/main/python).
