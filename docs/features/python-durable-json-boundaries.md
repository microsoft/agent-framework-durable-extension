# Python durable JSON boundaries

This note describes the local reader-first implementation, not a released capability. The
[rollout gates](../decisions/0032-durable-thread-compaction.md#state-evolution-and-compatibility)
still apply. Plain-JSON decoding does not enable schema `2.0.0` writes or introduce a workflow
protocol.

## Wire JSON is data

At the plain-JSON payload decoding steps below, every object key is data. Names such as
`__durabletask_autoobject__`, `__module__`, `__class__`, `__data__`, `__type__` and `__pickled__`
do not select Python types, import modules or invoke SDK custom-object constructors.
JSON-looking strings remain strings after the required transport layers are decoded.

This is not a promise that reserved fields are ignored by later consumers. State and response
envelopes still have defined meanings and validation rules. Shared v2 readers retain unknown
JSON without activating profiles. Shared response projection uses fixed `AgentResponse`, `Message`
and `Content` constructors. `ensure_response_format()` takes its requested Pydantic type from
calling code. Separately, the existing `RunRequest.response_format` transport can resolve
module-qualified models. This SDK boundary does not replace that transport or the checkpoint codec.

## Standalone Durable Task

`DurableAIAgentWorker` installs a worker-local decoder during construction, before SDK startup.
Framework annotations and explicit task/state calls select its private `JsonPayload` target.
Payload metadata cannot select that target. The covered reads are

- Generated agent `run` inputs and backing state.
- Blocking agent-call results through `OrchestrationAgentExecutor`.
- Generated workflow start inputs, child-orchestration results and external-event values.

Other target types retain the original converter's decoding behavior. Serialization and
value-level coercion also delegate to that converter. Native co-hosted orchestrations, activities
and entities are not switched to a new JSON policy merely by sharing the worker.

## Azure Functions

`AgentFunctionApp` selects plain JSON only for agent entity functions it generates, including
agents registered as workflow executors. State is parsed before constructing the SDK context.
Operation input unwraps the SDK's two JSON layers without custom-object hooks. The SDK's entity
batch executor still processes the operations.

Unmarked native entity registrations retain the SDK path. Manually wrapping `create_agent_entity()`
with the native SDK does not install this boundary. This reader-stage change does not replace
Functions workflow start, child-result or external-event decoding. It is not an app-wide decoder.

The exported handler matches the native SDK's unannotated rich-binding input and optional
`body` wrapper. Direct batch tests and Python worker indexing test different boundaries.

## Dependencies and custom converters

The local Durable Task package requires `durabletask>=1.7.1,<2` for target-aware SDK input/result
decoding and deferred state reads. This floor belongs to the reader-stage integration, not only
to later workflow changes.

Both packages require Python 3.10+ and `agent-framework-core>=1.13.0,<2`. Durable Task directly
requires `pydantic>=2.11,<3`. Functions keeps `azure-functions-durable>=1.3.1,<2`.

Custom converters can still serve native co-hosted work. For framework traffic, serializers must
preserve the expected JSON wire shape. Non-JSON encodings and custom rewrites of that shape are
unsupported. Plain decoding does not fall back to custom-object reconstruction on malformed JSON.

## Trusted checkpoints are separate

Workflow checkpoints use a separate internal Core codec that can unpickle Python objects.
Plain JSON parsing does not make checkpoints safe to load from untrusted input or storage.
Existing external-input sanitization and trusted worker/storage assumptions remain in force.
Do not treat inert SDK metadata as permission to decode a checkpoint. This note adds no caller
authorization, child-provenance or native-orchestrator policy.

## Reader-first limits

Mutable `DurableAgentState` still defaults to `1.1.0` and rejects v2 backing state. V2 snapshots
remain raw, read-only views. These decoder changes do not activate a v2 writer, migration or
provider-session restoration, and do not change checkpoint semantics or retention policy.

The package guides retain the rollout and polling contracts

- [Durable Task](../../python/packages/durabletask/README.md#reader-first-phased-rollout)
- [Azure Functions](../../python/packages/azurefunctions/README.md#reader-first-phased-rollout)

This source-level note makes no new live-host or supported-version validation claim.