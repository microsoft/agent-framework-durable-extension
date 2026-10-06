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
| Generated agent state and operation inputs | Plain JSON for state and registered `run`/`migrate` inputs. | The same entity class and JSON targets, plus plain JSON for its untagged operation inputs. |
| Blocking framework agent results | `OrchestrationAgentExecutor` selects a JSON result target. | The same executor through `AgentFunctionApp.get_agent()` and generated workflows. |
| Generated workflow starts | Registered input target selects JSON before provenance checks. | The same registered input target. |
| Generated parent receiving a child result | Child call selects a JSON result target. | The same child call target. |
| Generated workflow HITL values | Event wait selects a JSON target. | The same event wait target, including buffered events. |
| Unrelated native co-hosted calls | Original converter behavior. | The Functions converter's behavior, including object reconstruction for unannotated native inputs. |

## Standalone Durable Task

`DurableAIAgentWorker` installs a worker-local decoder during construction, before SDK startup.
Framework annotations and explicit task/state calls select its private `JsonPayload` target, or
`JsonState` and `JsonMigration` for entity state and migration input, which keep exact counters.
Payload metadata cannot select that target. Other target types, serialization and value-level
coercion delegate to the original converter. Sharing the worker does not change native decoding.

## Azure Functions

azure-functions-durable 2.x runs each orchestrator and entity function on its own durabletask
worker. That worker's converter rebuilds objects from `__class__`, `__module__` and `__data__`
envelopes. `AgentFunctionApp` wraps it with the same framework decoder on every durable function
it registers, including user functions, blueprints and the SDK's built-in functions. The generated
agent entity also decodes non-framework operation input as plain JSON, including typed helper
inputs before unsupported-operation rejection. State and migration targets keep exact counters. Other
functions change only for `JsonPayload`, `JsonState` and `JsonMigration` targets, so their native
payload types keep the Functions converter's behavior.

The worker is found in the generated invocation handler. Registration fails if it can't be found,
rather than falling back to object reconstruction. The workflow HTTP endpoints read serialized
orchestration output and custom status as plain JSON instead of through the Functions converter.
`get_agent()` needs a two-argument `(context, input)` orchestrator, which receives the durabletask
`OrchestrationContext`.

Both Python hosts restrict generated agent entities to `run`, `reset`, `expire_responses`, `migrate`
and administrative `delete`. Internal helpers and raw state setters are not operations, and the
deprecated `run_agent` alias is removed. This does not restrict unrelated user entity classes.
See the [maintenance and deletion contract](../../python/packages/durabletask/README.md#delivery-and-maintenance).

## Dependencies and custom converters

Durable Task requires Python 3.10+ and `durabletask>=1.7.1,<2` for target-aware decoding, deferred
state reads and parent-instance metadata, plus `pydantic>=2.11,<3`. Both packages require
`agent-framework-core>=1.19.0,<2`. Functions requires Python 3.13+, `azure-functions>=2.3.0,<3`
and `azure-functions-durable>=2.0.0rc2,<3`, which brings `durabletask>=1.11.0`.

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

## Response codec contracts

The response bridge has three separate contracts, not one general object decoder.

| Boundary | Contract |
| --- | --- |
| Stored JSON validation | Validate the shared shape and result/receipt agreement. Preserve unknown JSON and profile bytes without constructing runtime objects. A storage-valid response need not be projectable by this Python version. |
| Core projection and delivery | Interpret only recognized profiles using fixed base `AgentResponse`, `Message` and `Content` constructors. Preserve explicit value presence and original JSON separately. Foreign continuation bytes are inert, not resumable tokens. Modified projections cannot silently overwrite unprojectable fields. |
| Caller-selected typed value | Apply only the Pydantic type supplied by calling code. Shared results require an explicit value and a JSON-preserving conversion. Legacy results alone retain text fallback. This is distinct from the stored request-format transport described above. |

`serialize_terminal_response(instance)` does not resolve a lazy structured value. A producer
that needs resolution passes a `serialize_agent_response()` snapshot instead. Both paths use
the same bottom-up message/content snapshot functions. The shared-instance path performs
live-value preflight before serializer hooks. Snapshot-producing callers own that preceding
preflight, because the Core serializer may execute supported serialization hooks. Validating
the resulting JSON afterwards is not a substitute for that ordering.

Compatibility tests pair literal shared-wire expectations and fixed projected fields with
round-trip/parity checks. Core constructor coverage tests detect new fields to review, but
are not the sole oracle for the supported wire behavior.

### Core 1.19 preparation and serialization

The service observer snapshots the configured strategy's **post-compaction** dispatch,
including Core's group, exclusion and token-count annotations. It uses Core's exported
incremental annotation helpers, preserves a strategy-owned tokenizer, and records inputs
only after the service response completes. Provider or outer-middleware mutations cannot
rewrite that detached snapshot. This is local completion evidence, not remote receipt or
an external exactly-once guarantee. Tokenizer-only preparation and unclassified wrappers
retain their conservative completion-only behavior.

The observer relies on helpers exported by Core 1.19's compaction module; not
every helper is re-exported by the top-level package. These source-level adapters
do not certify all versions allowed by the dependency range.

Core 1.19 recursively omits non-JSON metadata during ordinary serialization. The durable
message snapshot retains nested metadata containers so strict JSON admission rejects an
invalid member rather than committing only its serializable siblings. Flush and append
staging still roll back their local changes on rejection.

Core assigns a function-call occurrence `Content.id` independently of the provider
`call_id`. Approval delivery preserves both identities; they must not be conflated.
Host-internal `Content.exception` diagnostics serialize as Core's fixed
`FunctionInvocationError` marker, while public results remain intact. The existing
durable HITL descriptor still rejects `Literal` and other unsupported annotations,
even though newer Core admission supports `Literal`. No persisted descriptor or
shared-state schema is expanded by these compatibility adapters.

## Polling failure boundaries

SDK retrieval failures retain bounded retry. Missing state or an absent completion remains
pending. Once state is retrieved, deterministic failures are terminal and do not consume
another poll attempt or fall back to transcript delivery.

| Phase | Public error code |
| --- | --- |
| State decode, shape validation, or contradictory stored result/receipt evidence | `state_read_error` |
| Requested result lookup/projection, including an incompatible recognized profile | `response_projection_error` |
| Delivery serialization or caller-selected typed-value processing | `response_processing_error` |

These diagnostics use constant text, not exception messages or tracebacks containing stored
values. They do not invent a committed invocation outcome. Recorded provider failures and
expired-delivery outcomes keep their existing codes. Functions returns HTTP `500` for the
three errors above. MCP reports the corresponding constant diagnostic.

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