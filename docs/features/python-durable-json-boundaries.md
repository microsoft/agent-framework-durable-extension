# Python durable JSON boundaries

This guide describes the current unreleased Python runtime stack through
[PR #112](https://github.com/microsoft/agent-framework-durable-extension/pull/112), not just the
earlier reader-first stage. It is not a released compatibility promise. The
[rollout gates](../decisions/0032-durable-thread-compaction.md#state-evolution-and-compatibility)
still apply. Plain-JSON SDK decoding and trusted checkpoint reconstruction are separate boundaries.

## Wire JSON is data

At the plain-JSON payload decoding steps below, every object key is data. Names such as
`__durabletask_autoobject__`, `__module__`, `__class__`, `__data__`, `__type__` and `__pickled__`
do not select Python types, import modules or invoke SDK custom-object constructors.
JSON-looking strings remain strings after the required transport layers are decoded.

Plain parsing preserves the received shape before consumer validation. It does not coerce an array
or string into a state object. This is not a promise that reserved fields are ignored by later
consumers. State, response and workflow envelopes still have schema, shape and profile checks.
Unknown JSON metadata does not activate an unsupported profile. Shared response projection uses
fixed `AgentResponse`, `Message` and `Content` constructors. `ensure_response_format()` takes its
requested Pydantic type from calling code. Separately, the existing `RunRequest.response_format`
transport can resolve module-qualified models. The SDK boundary does not replace that transport.

## Standalone Durable Task

`DurableAIAgentWorker` installs a worker-local decoder during construction, before SDK startup.
Trusted registration annotations and explicit task/state calls select its private `JsonPayload`
target. Payload metadata cannot select that target. The covered reads are

- Generated agent operation inputs, including `run`, and backing state.
- Blocking agent-call results through `OrchestrationAgentExecutor`.
- Generated workflow start inputs, child-orchestration results and external-event/HITL values.

The target is selected by input annotations, state `intended_type`, task `return_type` and event
`data_type`. State object-shape and schema validation happen after plain decoding, not inside an
SDK custom-object hook.

Other target types retain the original converter's decoding behavior. Serialization and
value-level coercion also delegate to that converter. Native co-hosted orchestrations, activities
and entities are not switched to a new JSON policy merely by sharing the worker.

## Azure Functions

`AgentFunctionApp` parses backing state and generated agent operation inputs as plain JSON,
including for agents registered as workflow executors. State is parsed before constructing the SDK
entity context. Operation input unwraps the SDK's two JSON layers without custom-object hooks.
The SDK's entity batch executor still processes the operations.

Scoped JSON decoding also covers

- Generated workflow start inputs, before protocol and child-provenance checks.
- Child results and external-event/HITL values consumed by `AzureFunctionsWorkflowContext`.
- Blocking framework agent-call results in generated workflows and standalone public agent proxies.

`AgentFunctionApp.get_agent()` and direct `DurableAIAgent` calls backed by
`AzureFunctionsAgentExecutor` use the same result guard.

The task adapters keep structured payloads opaque to SDK custom-object hooks, then restore plain
JSON before framework projection. They preserve native task scheduling and correlation. Guarded
start, agent-result and child-result paths reject unknown SDK representations rather than silently
falling back to custom-object reconstruction.

Unmarked native entity registrations retain the SDK path. Manually wrapping `create_agent_entity()`
with the native SDK does not install the generated-entity boundary. Unrelated native orchestration
inputs, activities, entity calls and event waits retain their SDK decoding behavior. These are
registration- and call-scoped boundaries, not an app-wide decoder or a global SDK patch.

The exported handler matches the native SDK's unannotated rich-binding input and optional
`body` wrapper. Direct batch tests and Python worker indexing test different boundaries.

## Dependencies and custom converters

The active upstream Durable Task SDK floor is `durabletask>=1.7.1,<2` for target-aware input/result
decoding, deferred state reads and parent-instance metadata. The reader integration already needed
this floor, independently of the later workflow changes.

Both packages require Python 3.10+ and `agent-framework-core>=1.13.0,<2`. Durable Task directly
requires `pydantic>=2.11,<3`. Functions keeps `azure-functions-durable>=1.3.1,<2`.

Custom converters can still serve native co-hosted work. For framework traffic, serializers must
preserve the expected JSON wire shape. Non-JSON encodings and custom rewrites of that shape are
unsupported. Plain decoding does not fall back to custom-object reconstruction on malformed JSON.

## Trusted checkpoints are separate

Internal workflow activity and child checkpoints use a separate Core codec that can unpickle
Python objects. Their transport is parsed as plain JSON and validated before checkpoint decoding.
Valid JSON shape alone is not permission to use that decoder. Generated agent yields instead use
the versioned `_durable_agent_response` envelope with base-response JSON. Known response and HITL
type profiles have their own validation rules, not payload-selected SDK type construction.

External start and HITL values are sanitized before typed reconstruction. Internal child markers
require SDK-reported parent metadata and a consistent child address before checkpoint decoding.
That check establishes immediate-parent consistency, not full ancestry or caller authorization.
Trusted workers, application parents and storage remain required. Plain JSON parsing does not make
checkpoint data safe to load from an untrusted source, and sanitized application JSON is not itself
an encoded checkpoint.

<a id="reader-first-limits"></a>

## Current runtime and rollout limits

Public mutable `DurableAgentState` now defaults to exact schema `2.0.0`, and the current runtime
writes canonical v2 state. `read_agent_state()` remains the read-only inspection path for supported
legacy `1.x` and v2 snapshots. Legacy backing state is not writable by the new runtime. The earlier
`1.1.0` writer and reader-only limits belong to the
[historical reader stage](../decisions/0032-durable-thread-compaction.md#python-reader-stage-runtime-note-2026-09-23).
This PR112 layer does not implement migration or automatic retention.

Workflow protocol remains `2`, but this runtime requires fresh workflow instances, including after
an upgrade from an earlier v2 build. Old in-flight instances and recorded histories are unsupported
and may fail. A matching start-envelope version is not replay compatibility. Keep old runs on their
original workers and hub if they must finish. The `isolated_v2` acknowledgement does not prove hub
isolation or compatible peers.

The package guides retain the rollout and polling contracts

- [Durable Task](../../python/packages/durabletask/README.md#current-runtime-contract-on-this-unreleased-stack)
- [Azure Functions](../../python/packages/azurefunctions/README.md#current-runtime-contract-on-this-unreleased-stack)

This source-level guide makes no new live-host or supported-version validation claim.