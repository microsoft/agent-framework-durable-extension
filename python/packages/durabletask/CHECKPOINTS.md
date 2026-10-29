# Python checkpoint-only release candidate

This is an experimental transition for the pinned Python release
`v1.16.0-preview.260922.1`, source commit
`8ca913651249fc4606a2646d17dd8090f0369774`. It is not released migration support
or certification of every existing state. Ordinary framework entry points keep
their existing defaults.

## Deployment boundary

Stop **all** old workers before takeover. Keep the original backend, task hub,
instance, execution, entity addresses and accepted native operation identities.
Never restart an original instance to simulate import. Do not resume a suspended
instance unless its user requests resume.

Migration-side tools require the trusted original definition, dependencies and
types. The old history has no authoritative definition fingerprint. A new source
hash identifies the operator-selected deployment, not an inferred historical
fingerprint. Authenticate the checkpoint and acquired native execution before
decoding customer values. An application input, event, correlation ID or claimed
parent string is not migration authority.

The current-only directory has no original generator, invocation helper, old
mutable state reader or importer. The mechanical native prefix reconstructs
original SDK task handles, not customer replay code. Native failure, control and
result order remain backend events. An unresolved operation has no invented
successful result or ingestion receipt.

New and migrated checkpoint entities select the target `current-v1` invocation
failure policy rather than inferring an unavailable legacy retry policy. A
generic streaming setup, iteration or finalization failure does not start a
second non-streaming run. The existing immediate no-stream `TypeError` fallback
is only a capability signal, not proof that arbitrary custom agent code has no
side effects; applications remain responsible for making external effects
retry-safe.

## Capture the pinned producer

The migration process imports `agent_framework_durabletask.checkpoint_migration`.
It supplies all six original module paths to `PublishedSource.load`. Each file
must match `PINS` before any source executes. Reuse that returned object within a
process. `load_definition` takes an explicit, ordered mapping of flat module
names to `(Path, sha256)` and an original `module:factory` entry point.
Dependencies precede importers. For original packages, supply `TrustedDefinition`
with the original workflow and the complete `TrustedModule` closure instead.

```python
from agent_framework_durabletask.checkpoint_migration import (
    PublishedSource,
    capture,
    load_definition,
)

source = PublishedSource.load(original_six_paths)
definition = load_definition(original_entrypoint, original_ordered_modules)
history = [
    dict(event.to_dict(), type=type(event).__name__)
    for event in client.get_orchestration_history(original_instance)
]
captured = capture(
    source,
    definition,
    history,
    event_streaming=original_event_streaming,
    shared_state=original_initial_shared_state,
    source_transport_profile="native-sdk-v1",
)
checkpoint_bytes = captured.checkpoint_bytes(original_input=original_input)
```

The history must be losslessly hydrated. A public status projection, unhydrated
payload reference or reconstructed result is insufficient. Classic Functions
sources use `classic-functions-v1`, not the native profile. The original
event-streaming policy, initial shared state and input are operator-supplied
facts. The compiler does not infer them from an application status string.

The supported standalone route is SDK-first. Upgrade the Durable Task SDK while
the old DAFX version remains deployed, verify the unfinished runs under that
SDK, stop all old DAFX workers, and only then deploy the checkpoint-only DAFX
version. The imported continuation uses the target SDK's published batch-join
contract (`published-batch-join/1`). The original SDK version and its join
policy are not inferred from history or required as migration inputs.

Classic Azure Functions Durable and standalone Durable Task remain distinct
hosting contracts. Upgrading the standalone `durabletask` wheel does not change
the classic Functions implementation. The classic route is not claimed
supported unless its Functions host is separately upgraded and the old DAFX
unfinished-run sequence is verified under that host. See the source-backed
limitations in `checkpoint_migration_inventory.json`.

Checkpoint documents carry an outer `format` and `formatVersion`, separate
from the semantic state `contract`, target `joinContract`, trusted definition
digest, and native execution identity. Unknown outer versions and contracts are
rejected before customer state deserialization. A future format must add an
explicit migration step; existing semantic state is never silently reinterpreted.

The current-only candidate has an opt-in `emission_profile="native-can-v1"`
for `admission_profile="new-workflows-only"`. It snapshots the semantic
frontier after a completed batch when pending work or approval requests remain,
then yields the proposal SDK's `create_dispatch_checkpoint` replay fence. The
fence requests native Continue-As-New only after the dispatch is drained. A
successor can consume that checkpoint and remains eligible to emit another one.
The default remains disabled, terminal workflows do not continue-as-new solely
to write an unused checkpoint, and the migration-only worker cannot enable
emission.

The local proposal integration exercised a two-step ordinary workflow. The
test provider generated predecessor/successor identities and the digest of the
exact checkpoint input only when it accepted the native Continue-As-New action.
The emitter process was stopped before releasing the successor dispatch; a
separate process loaded package copies matching the pinned bundle and consumed
the checkpoint. The independently recorded first-step effect occurred once,
the successor produced the expected final output, and copied checkpoint input
was rejected on root start, restart, and purge/recreate without another effect.
This establishes the local proposal path only. The provider is in-memory and
the SDK changes are disposable proposal code, not released SDK or service
support. It does not establish durable storage, process-crash recovery, Azure
Storage, managed Scheduler, or arbitrary workflow-definition migration.

Capture AST-lowers every yield in the exact published producer into a
non-dispatching history reduction. It does not inspect generator frames or keep
an original generator at the endpoint. Trusted customer routing predicates and
deserializers may execute in this migration process. They must be deterministic
and retain the required original types. This is not a Python sandbox. Pickled
values require trusted originals and must not be decoded before authority checks
in a cold worker.

`checkpoint_migration_inventory.json` records source yield sites, state,
continuation and evidence limits. Capture represents completed results, pending
native tasks, complete and partial batches, buffered messages, fan-in, loops,
child results/failure ordering, approvals and pending terminal commitment. A
partial accepted batch retains the actual accepted handles and allocates only
its unaccepted suffix. Gaps or contradictory accepted work fail explicitly.
Modeled partial histories test this reduction, not backend dispatch atomicity.
Terminal dispositions with no continuation cannot be encoded by
`checkpoint_bytes`. Preserve their actual outcome instead of restarting them.

The runtime reconstructs native task identities from the authenticated
same-execution prefix. Its proposed public SDK acquired-history API validates
that prefix against real dispatch, not against operator-supplied evidence alone.
It then enters the semantic continuation. The remaining native task allocation,
result hydration and suspension replay obligations are not customer replay code.

## Build and stage

Install the framework's `checkpoint` extra and the matching candidate SDK and
native Functions proposals. The ordinary published SDK dependency floor does
**not** imply those proposals have shipped. Checkpoint registration rejects a
worker without the required public configuration surfaces.

Use `agent_framework_durabletask.checkpoint.validate_registration` on an explicit
trusted source manifest. Its `module:factory` entry point is deployment-owned,
not selected by a workflow start. Source paths use forward slashes, including
for nested package modules, on every operating system. After verifying the
active release and cold bundle, registration verifies and loads every declared
module, including helpers imported only when a handler runs. This keeps deferred
imports bound to the pinned module cache without adding the customer directory
to the worker's global import path. Then build a new directory.

```python
from agent_framework_durabletask.checkpoint import (
    CheckpointScope,
    CheckpointStore,
    build_checkpoint_bundle,
    validate_registration,
)

registration = validate_registration(registration_manifest_path)
bundle_digest = build_checkpoint_bundle(
    "cold-release",
    "customer",
    customer_files=["registration.json", "customer.py"],
)
scope = CheckpointScope(backend_identity, original_hub, registration.definition_digest)
store = CheckpointStore(protected_database_path, create=True)
store.prepare(scope, "release-1", bundle_sha256=bundle_digest)
```

Stage each operator-signed capability with `store.put`, using kind `capability`
and its exact instance and native execution identity. Its bytes must be produced
by `issue_checkpoint` with the same backend/hub/definition binding. The store
does not sign, deserialize or substitute for verifying those bytes.

Use `issue_checkpoint` with `checkpoint_bytes` from capture. Do not replace the
original input with a new start envelope. Preserve the exact original instance
and execution binding.

If the graph has legacy agent entities, stage their conversion authorities
before proceeding to the next section. A deployment without entity conversion
can activate directly after stopping all old workers.

## Convert agent entities, then activate

`agent_framework_durabletask.checkpoint_entity_migration` is migration-only.
`convert_agent_observations` requires the exact original published
`_durable_agent_state.py`, SHA256
`96092e073a9f993d4c802c6db29007e7ae4dad9122f9472bdc4132a06c128d49`.
It preserves original transcript and response observations, including nullable
usage. It does not turn observations into ingestion or completion receipts.

Use the public `issue_entity_conversion` to sign the raw source snapshot for its
actual canonical native entity address. Stage its bytes with
`store.put_entity_conversion`. The authority binds backend, hub, trusted
definition, address and exact source digest. No orchestration execution identity
is fabricated for an entity.

```python
from agent_framework_durabletask.checkpoint_entity_migration import issue_entity_conversion

store.put_entity_conversion(
    scope,
    "release-1",
    entity=canonical_entity_address,
    payload=issue_entity_conversion(
        key_id=operator_key_id,
        operator_private_key=operator_private_key,
        scope=scope,
        entity=canonical_entity_address,
        source_state=original_entity_snapshot,
    ),
)
```

After stopping **all** old workers, enter a durable conversion-only generation.

```python
store.begin_conversion(
    scope,
    "release-1",
    expected_active=previous_active_generation,
    stop_attestation=operator_stop_observation_bytes,
)
```

Register `create_authorized_entity_converter` instead of the cold entity
implementation on a temporary entity worker. Conversion runs in the actual
serialized native entity operation, never through management state overwrite.
Its `import_checkpoint` request is `{"sourceDigest": original_source_digest}`.
An already accepted queued operation can also perform the authorized conversion.
Its own native identity and current invocation policy still apply.
Only this temporary converter supports `import_checkpoint`; the cold entity
allows `run`, `reset` and `delete` and rejects inherited state-replacement
operations before initializing their context. Reset clears the published
transcript and polling observations while preserving native operation identities
and the current runtime's completion-receipt floor. A redelivered completed
operation returns its retained result, or an already-completed response after
delivery expires, without invoking the agent again. Replay omits reasoning,
while response observations and polling retain it. For Core `Agent` instances,
the imported transcript is installed before customer middleware runs, so
customer transformations are applied to the imported context rather than
discarded.

Await the actual native conversion result, stop the converter and independently
read the matching semantic entity state. Record that post-commit observation for
each staged entity. Then activate.

```python
store.confirm_entity_conversion(
    scope,
    "release-1",
    entity=canonical_entity_address,
    observation=post_commit_backend_observation_bytes,
)
store.activate(
    scope,
    "release-1",
    expected_active=None,
    stop_attestation=operator_stop_observation_bytes,
)
```

After `begin_conversion`, the previous generation is retired, so final promotion
uses `expected_active=None`. Repeat the original stop attestation exactly.
Activation rejects unconfirmed entities and cold registration rejects the
`converting` phase, including after coordinator process replacement. Recover a
crashed conversion by reopening the existing store and observing/retrying the
same native conversion, not by starting a cold worker early. Do not record
confirmation inside an uncommitted entity invocation.

The attestation and confirmation are protected operator assertions, **not**
backend-issued receipts or automatic fences. Preparation, immutable fact writes and
activation are idempotent only for identical bytes. A different bundle,
attestation or execution fact fails. Activation retires the previous generation
and publishes the new generation in one database transaction.

Use `register_current_worker` with the validated registration, configured
backend/hub/public keys, existing store, generation, bundle digest and cold
directory. It validates bundle bytes before importing customer callbacks.
Authority lookup binds actual acquired instance and execution. Admission records
for new descendants bind native immediate-parent execution and the trusted graph
edge before internal child values are decoded.

Run the cold process with bytecode writes disabled, for example `python -B`.
The bundle verifier rejects missing, extra or changed files. Put storage,
readiness files and logs **outside** the immutable cold directory.

## Protected storage and crash boundaries

`CheckpointStore` uses standard SQLite transactions, foreign keys, an exclusive
active generation per scope, FULL synchronous WAL commits and compare-and-swap
activation. A worker opens existing storage. It cannot silently initialize an
empty authority store after a lost mount.

This implementation requires an operator-protected persistent **local** volume
with working SQLite locking. It is not a network-filesystem or multi-machine
admission service. Ordinary start/event callers must not have filesystem access.
Trusted customer Python and deployment administrators are not sandboxed by this
API. Privileged reset or rollback of the database is outside its protection.

Admissions are not completion receipts. The database does not atomically commit
with the Durable Task backend, fence an already running old worker, migrate
provider-owned external state or prove external effects exactly once. A crash
before a direct activation commit leaves the old generation active. Once
`begin_conversion` commits, no generation admits cold workflow dispatch until
conversion is confirmed and activated. An acknowledged transition can be retried
after process replacement with identical inputs.
Retired generations cannot be reactivated. Local coordinator process-crash tests
are distinct from orchestration-backend restart or managed service failover.
Schema 2 is required. Earlier provisional schema 1 stores are not silently
upgraded or reused as empty storage.

## Native Functions

`agent_framework_azurefunctions.checkpoint.create_checkpoint_function_app`
registers the same current handlers through native Functions decorators and the
proposed public `DFApp.worker_configuration` callback. It does not inspect
generated closures or mutate a private SDK registry. Select the deployment replay
profile explicitly. Functions custom status does not publish accumulated workflow
events, even when an imported checkpoint records source event streaming as enabled.
The source checkpoint remains unchanged.

The factory opts into the proposed worker setting
`configure_replay_profile(..., preserve_activity_inputs=True)` for both native
and classic profiles. Accepted activity arguments must survive preprocessing
and payload-store hydration so the acquired native history can validate an
already accepted suffix. The ordinary Functions worker default still discards
scheduled activity inputs. A signed expected argument cannot substitute for a
missing actual accepted argument.

Preserving inputs also hydrates offloaded completed activity arguments.
Completed-prefix preflight compares those actual acquired arguments, not only
unresolved suffix arguments. Their payload blobs must remain available for
replay. This adds payload-store reads on full-history replay. No bounded-read
or latency guarantee is claimed, and discarding completed inputs would weaken
the authenticated prefix check or reject valid offloaded histories.

The factory validates `host.json` by default. The host must set
`extensions.durableTask.storageProvider.workItemFilteringEnabled` to JSON `true`.
Missing, misplaced, false and string-valued settings fail before registration.
Select the actual storage provider in host configuration. The factory neither
chooses the backend nor attests the configured endpoint/hub identity.
`include_functions=True` in the bundle builder copies the selected native
Functions candidate and the current-only framework host and HTTP modules. Other native
package dependencies and the matching .NET extension remain explicit deployment
inputs.

The current-only factory includes `health`, workflow `run`, `status` and
`respond`, and `agents/{workflow-scoped executor id}/run`. Workflow starts return
403 in the default migration-only admission profile. A separate new-workflows-only
profile admits new versioned roots and refuses checkpoint import. Agent polling
uses semantic state, published first-match correlation behavior and legacy alias
deprecation headers. These routes do not import the ordinary mutable-state host.
Standalone `add_agent`, MCP and administrative routes are not registered by a
trusted Workflow manifest. Resume, terminate and restart remain backend SDK
controls, not replacement HTTP routes added by this factory.

Use the same backend/hub/keys/store/generation/bundle arguments as
`register_current_worker` and an appropriate host-configuration path when
calling `create_checkpoint_function_app`. Standalone registration normalizes
an omitted replay profile to `ReplayProfile.DEFAULT`. Classic Functions
registration must select its matching profile explicitly:
`ReplayProfile.CLASSIC_FUNCTIONS_V1`. Native profiles use
`ReplayProfile.DEFAULT`. The public factory registers through the
proposed native decorators and worker configuration API, not private registry
mutation. Host environment overrides must agree with the validated file.

AzureStorage with disk-persistent Azurite can exercise its own local recovery
contract. Such a result is not evidence of managed Scheduler service failover.
The preserved earlier integrated AzureStorage recovery case retained original
operations, queued input and suspension across Functions process loss and
Azurite's IPC graceful close. Forced Azurite termination separately lost
previously accepted, history-visible input. That is not evidence of a migration
regression or verified per-request disk commit. Disk-backed Loki autosave and
graceful close do not establish an arbitrary forced-kill durability contract.
Neither result certifies managed Scheduler failover. Preserve the exact source
and bundle pins for those runs rather than relabeling them as final-wheel tests.

## Package provenance and release integration

This candidate combines repository framework code with unpublished
`durabletask==1.12.0+checkpoint.1` and
`azure-functions-durable==2.0.0rc3+checkpoint.1` proposals. The separate
`durabletask-azuremanaged==1.12.0` repack has unchanged runtime payload.
`azure-functions==2.4.0b1` runtime bytes match the official wheel.
The source-built .NET Functions extension, actual backend and deployment
configuration are separately pinned inputs, not included service guarantees.

Earlier portable-patch claims that called the isolated installed SDK an
unmodified official baseline are superseded. That baseline already contained
dispatch-end continue-as-new changes. Official wheel hashes, RECORD ownership,
the complete clean-origin delta including those inherited changes and
reconstructed candidate hashes must accompany a reproducible distribution.
Core SDK and Azuremanaged ownership must not be conflated. Prior execution logs
remain evidence only for their recorded source pins.

The public capture inventory, current-only package, conversion authority and
admission store are implemented candidate surfaces. They are not shipped upstream
APIs. Component route tests are not live HTTP/backend parity proof. A modeled
frontier is not observed backend atomicity. Preserve terminal outcomes, uncertain
effects, native redelivery versus distinct accepted calls, external provider
session stores and original trusted type requirements explicitly.

Known successful results are reused. Unknown external effects retain the native
delivery/retry ambiguity of the original backend. No universal idempotency
guarantee or permission to repeat a known successful effect is introduced.
The original release did not pass its stored `data.session` into `Agent.run`.
Preserve customer middleware's actual external session store when it supplies
provider conversation identity, rather than inventing a legacy session receipt.

All-supported-runs certification, production rollout, upstream adoption and
managed service failover remain separate claims requiring their own evidence.
An absent owner-supported failover mechanism is an external verification gate,
not a user decision to weaken the goal. No managed service restart is implied.
