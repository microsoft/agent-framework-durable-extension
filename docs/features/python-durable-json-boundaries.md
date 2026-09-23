# Python durable JSON boundaries

This guide describes the current unreleased Python runtime, including canonical `2.0.0` writes,
explicit legacy migration and workflow protocol `2`. The
[package contract](../../python/packages/durabletask/README.md#current-runtime-contract-on-this-unreleased-stack)
and [ADR rollout gates](../decisions/0032-durable-thread-compaction.md#state-evolution-and-compatibility)
apply. Scoped JSON decoding does not establish deployment isolation or replay compatibility.

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

## Covered host paths

| Read boundary | Standalone Durable Task | Azure Functions |
| --- | --- | --- |
| Generated agent state and operation inputs | Plain JSON for state and registered `run`/`migrate` inputs. | Plain state and two-layer operation JSON through the generated entity wrapper, including `migrate`. |
| Blocking framework agent results | `OrchestrationAgentExecutor` selects a JSON result target. | Scoped result guard for standalone framework agent calls and generated workflows, before Core response loading. |
| Generated workflow starts | Registered input target selects JSON before provenance checks. | Raw SDK input is parsed as JSON before provenance checks. |
| Generated parent receiving a child result | Child call selects a JSON result target. | Selected successful child `Result` is protected before SDK decoding. |
| Generated workflow HITL values | Event wait selects a JSON target. | Matching event values use the scoped JSON guard, including buffered events. |
| Unrelated native co-hosted calls | Original converter behavior. | Original SDK decoding, including native orchestrator inputs and unrelated entity, activity, child and event results. |

## Standalone Durable Task

`DurableAIAgentWorker` installs a worker-local decoder during construction, before SDK startup.
Framework annotations and explicit task/state calls select its private `JsonPayload` target.
Payload metadata cannot select that target. Other target types, serialization and value-level
coercion delegate to the original converter. Sharing the worker does not change native decoding.

## Azure Functions

`AgentFunctionApp` selects the entity ingress boundary only for generated agents, including workflow
executors. State is parsed before constructing the SDK context. Operation input unwraps both SDK
JSON layers without custom-object hooks, then the SDK batch executor processes the operations.
Unmarked native entity registrations retain the SDK path. Manually wrapping `create_agent_entity()`
with the native SDK does not install this ingress boundary.

The exported handler matches the native SDK's unannotated rich-binding input and optional
`body` wrapper. Direct batch tests and Python worker indexing test different boundaries.

The separate orchestration result guard also covers standalone framework agent calls through
`AgentFunctionApp.get_agent()` or `AzureFunctionsAgentExecutor`. It protects the entity envelope
and inner result before SDK object construction, without adapting unrelated native waits.
Generated workflow start, agent-result and child-result guards reject unsupported SDK layouts.
These context-local adapters rely on raw input/history and task-registry contracts, not an
app-wide or global SDK decoder replacement.

## Dependencies and custom converters

Durable Task requires `durabletask>=1.7.1,<2` for target-aware decoding, deferred state reads and
parent-instance metadata, plus `pydantic>=2.11,<3`. Both packages require Python 3.10+ and
`agent-framework-core>=1.13.0,<2`. Functions requires `azure-functions>=1.24.0,<2` and
`azure-functions-durable>=1.3.1,<2` and uses its own SDK's parent metadata.

Custom converters can still serve native co-hosted work. For framework traffic, serializers must
preserve the expected JSON wire shape. Non-JSON encodings and custom rewrites of that shape are
unsupported. Plain decoding does not fall back to custom-object reconstruction on malformed JSON.

## Trusted checkpoints are separate

Workflow checkpoints use a separate internal Core codec that can unpickle Python objects.
Plain JSON parsing does not make checkpoints safe to load from untrusted input or storage.
Existing external-input sanitization and trusted worker/storage assumptions remain in force.
Do not treat inert SDK metadata as permission to decode a checkpoint. Generated child markers
require SDK immediate-parent/address consistency before checkpoint decoding, not proof of full
ancestry or protection against a malicious application parent. See the shared
[workflow start contract](../../python/packages/durabletask/README.md#workflow-starts-and-child-identity).

<a id="reader-first-limits"></a>

## Current runtime and rollout

Mutable `DurableAgentState` now defaults to `2.0.0`. Legacy `1.x` inspection remains read-only
through `read_agent_state()`, and use by the writer requires [explicit migration](../../python/packages/durabletask/README.md#migration).
Migration preserves opaque JSON for digest and identity checks, but decoding does not authorize
ownership transfer. Operators still own quiescence, fencing and the separate empty destination.
Use fresh workflow instances, including after earlier v2 builds. Old histories stay on their
original deployment. The [Functions guide](../../python/packages/azurefunctions/README.md#http-routes-and-responses)
covers HTTP behavior. Retention, delivery and session restoration follow the common package contract.

This source-level guide makes no new live-host or supported-version validation claim.