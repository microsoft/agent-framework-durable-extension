# Durable agent state 2.0 contract

This contract coordinates schema 2 adoption across Python, .NET, Durable Task,
and other state consumers. Schema validation alone does not enable a runtime to
read, migrate, or emit `2.0.0`. Each runtime must implement the contract and the
deployment must satisfy the compatible-reader rollout requirements before schema
2 writes are enabled.

The motivation is to separate execution/delivery completion from conversation
history that may be compacted or evicted. Preserving unknown JSON alone cannot
prevent duplicate execution if a reader still uses transcript responses to
decide whether a request completed. [ADR discussion #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88)
provides related compaction/retention context, not a mandate for this exact schema.

## Version and reader policy

The [Draft 2020-12 schema](durable-agent-entity-state.json) describes exact contract
snapshots `1.0.0`, `1.1.0`, `1.2.0`, and `2.0.0`; it is not a list of versions
supported by today's runtimes. The 1.2 fields define the shared shape and are not
claimed as released Python output.
Legacy transcript entries retain the existing permissive entry shape. The
three mailbox/binding fields are forbidden in 1.x, even if empty. Version 2.0
requires `terminalResults`, `completionReceipts`, and `conversationHistory`
(which can be empty). `historyBinding` is an optional runtime extension/profile,
not a shared requirement for session-fixed effective ownership.

Historical `1.0.0`, `1.1.0`, and `1.2.0` readers and writers remain compatible
with values accepted by the original .NET implementation: message roles are
free-form strings, function arguments may retain their historical string form,
and URI content may omit `mediaType`. Schema 2.0 remains stricter where the
contract requires a closed set, including its recognized role values. The root
version therefore selects version-aware semantic validation without making a
legacy read-and-write cycle reject previously persisted state.

Major 2 is required because the authority for completion and replay changes,
not merely because new optional properties appear. This schema intentionally rejects unlisted versions, including future
2.x versions; accepting them must be a deliberate semantic compatibility
decision, not just a numeric SemVer comparison.

At upstream main `62afdbac03f0a81b0917abe6328203b706a2f294`,
Python's `DurableAgentState.from_dict` checks for a version's presence but does
not gate its value, and `DurableAgentStateData.from_dict` reads the transcript
without retaining unknown data-level fields. This is a compatibility gap to
resolve jointly, not a criticism or a claim that Python already supports 2.0.
The revised C# preview instead rejects unsupported versions; its acceptance
rules are not a shared runtime guarantee. A new major number alone does not
protect deployments whose existing readers do not enforce it.

## Wire concepts and semantic invariants

| Field | Meaning |
| --- | --- |
| `terminalResults[correlationId]` | Immutable terminal response/error envelope, detached from transcript retention. |
| `completionReceipts[correlationId]` | Independent completion evidence, retained after payload expiry and transcript pruning. |
| Receipt `resultState` | `available` with a matching payload, or `unavailable` with a removal timestamp. |
| `historyBinding` | Optional, separately versioned runtime extension/profile; preserved without imposing shared effective-owner semantics. |
| `conversationHistory` | Evictable transcript, never the authoritative completion index in 2.0. |

The schema validates local shape, not all cross-object or temporal invariants.
Compatible implementations must additionally enforce:

- Keys equal the embedded `correlationId`, compared exactly and case-sensitively
  without normalization, within one durable entity/session generation.
  When present on a v2 request, response, or error response, the transcript
  `correlationId` uses the same `identifier` constraints as the result maps.
  Legacy entry handling is unchanged; compaction still has no correlation.
  Request identities must not be reused for different work in that generation.
  Every terminal result has exactly one matching receipt. Outcome, completion
  instant, and expiry instant (including absence) agree between them.
- Terminal result contents and receipt identity/outcome/completion/expiry are
  immutable. At the entity operation boundary, the result and receipt must
  commit atomically with that operation's session/continuation, ingestion,
  entity-local transcript, TTL, binding, and other local control-state changes.
  They must not commit independently of the corresponding local state.
  External provider writes and tool effects are outside this local transaction,
  as discussed in the [ADR](https://github.com/microsoft/agent-framework-durable-extension/pull/88).
  Availability can transition only from `available` to `unavailable`,
  atomically removing the result and recording `resultUnavailableAt`; it never
  reopens execution or changes a success into a failure.
- An `available` receipt requires a result; an `unavailable` receipt forbids one.
  No receipt means only **no recorded terminal completion**. It means pending
  only for a request independently known to have been accepted; unknown request
  identities are not automatically pending. This contract adds no request
  admission registry and makes no exactly-once claim for external side effects.
- `resultExpiresAt`, when present, is no earlier than `completedAt`. At or after
  that instant a poller must report completed-but-result-unavailable even if a
  lazy cleanup has not yet removed a stored `available` payload. It must not
  deliver the expired payload, report pending, or rerun the request.
  `resultUnavailableAt` is no earlier than completion; for a time-expired
  payload it is no earlier than expiry. `unavailable` can also represent an
  explicit removal policy; absence of `resultExpiresAt` is not a
  guarantee against such removal.
- Receipts survive payload expiry and transcript pruning for the deployment's
  duplicate-delivery lifetime. Receipt eviction and session-ID reuse are outside
  this state contract. A deployment that enables whole-entity deletion must define
  how it handles late duplicates after the receipt-bearing entity is removed.

Expired lookup reports completed-but-result-unavailable plus the retained
`succeeded` or `failed` outcome, with no expired payload and no reopening of
execution. Each runtime must implement this behavior before it emits 2.0.

An older receipt without an authoritative outcome must **not** be assigned
`succeeded` or `failed` from absence of an error, a pruned transcript, expiry,
or a default. Preserve its known completion/unavailability facts without
inventing an outcome or rerunning the work. The required v2 receipt `outcome`
may be populated only from authoritative evidence; absent that evidence, the
legacy receipt cannot be promoted to this v2 shape. Any legacy lookup or migration
representation must retain the distinction. This contract does not add a
fabricated `unknown` outcome to the v2 enum.

The envelope requires `response.messages` for success and failure
(an empty list is allowed). Failure additionally requires `error.code` and
`error.message`; success forbids `error`. Error details and response metadata
are JSON only. A failure's partial messages are diagnostic output, not an
instruction to replay a failed turn. Cancellation has no terminal representation
in this contract; a runtime records a terminal result only when it has an
authoritative `succeeded` or `failed` outcome.

`response.value` is an optional, named caller-visible JSON result, independent
of messages and text. An absent field means no structured value; explicit
`null`, `false`, `0`, `""`, `[]`, and `{}` are present values and must survive
round-tripping without truthiness-based omission. No separate presence flag
is needed. Consumers must not infer `value` from text, coerce its type, or
serialize arbitrary model/runtime objects. Unknown nested properties remain
part of the value. The same JSON preservation rule applies if a failed response
carries a diagnostic value; its outcome remains failed.

### Optional runtime extension/profile

Stable provider/configuration identity is distinct from effective per-run
history ownership. The shared contract **does not require session-fixed
effective ownership** and must not prohibit Python's supported per-run
transitions. C# may separately enforce a fixed-owner policy/profile.

`historyBinding` now denotes an optional, separately versioned runtime
extension/profile rather than a standardized shared binding object. The existing
spelling is retained to avoid moving stored data; there is no new shared
`providerBinding` or `configurationBinding` definition. Compatible writers must
preserve the original JSON value and all nested fields, even when they do not
understand or use that profile. The shared schema intentionally does not validate
its internal shape, version, owner kinds, or provider keys.

A runtime that relies on the profile for restoration must validate the profile
identity, supported version, required fields, and applicable policy before use,
using trusted configuration rather than dynamically activating types from JSON.
Unsupported or malformed profiles must fail that runtime's restoration path;
they must not be silently ignored when the runtime depends on them. Other
consumers preserve the data without treating shared-schema validation as profile
approval. Even null or a malformed profile can be preserved as opaque JSON;
that does not make it usable by a relying runtime.

The `version`, `ownerKind`, and non-secret logical `providerKey` in existing
synthetic fixtures illustrate one runtime profile, not a shared mandatory shape.
Profile-specific fixed ownership remains runtime policy. Absence implies neither
a default owner nor permission to infer one from opaque session state. No profile
is an authorization grant. The contract imposes no shared effective-owner
transition protocol.

## Existing state, extension data, and trust boundaries

`session` is opaque JSON continuation/provider state. Preserve it, but do not
infer an owner or instantiate a runtime type from its contents. The optional
terminal `continuationToken` is base64 bytes; cross-language encoding does not
prove cross-provider resumability. Consumers may resume it only through a
compatible provider contract.

`expirationTimeUtc` documents the existing whole-entity idle TTL field; absent
or null means no stored deadline. Deleting the entity also deletes receipts:
this is distinct from `resultExpiresAt` and ends any in-entity deduplication
evidence. An external tombstone, bounded request lifetime, or session-generation
policy must address late duplicates before such deletion is compatible with 2.0.

`ingestedPositions` is a legacy highest-seen position by producer, **not proof
of a contiguous delivered prefix**, an exact receipt set, or terminal completion.
Migration must preserve its historical meaning and cannot infer that gaps were
delivered. After delivering `[1, 3]`, selecting `[2, 4]` must deliver both while
remembering that `3` was delivered. A scalar `3` alone cannot establish that.
Exact gap-preserving workflow receipt sets or ranges need separately versioned
bookkeeping with explicit producer/delivery identity, preservation, and migration
semantics, independent of transcript retention. This contract neither defines
that wire format nor backfills receipts from the scalar.

Recognized usage, ingestion, and truncation counters are bounded as described
below and out-of-range values must be rejected. Integral decimal or exponent
forms remain valid when their exact mathematical value is in range. These bounds
do not apply to opaque application JSON, extension data, or unknown properties,
whose original values and representations must remain preserved.

`truncation` records evicted message count and first/last eviction
instants (last must be no earlier than first). It is diagnostic evidence, not
model context. Optional `messageId` is message identity, not request identity.

### Lossless messages and JSON content

For schema 2.0 only, message roles include `developer`. Function-call `arguments` accepts the
original object or string; preserve the entire string, including whitespace
and incomplete/non-JSON text, without parsing it into an object. URI content
requires a URI but not a media type; absence stays absent rather than being
filled with guessed metadata. Versioned message/content definitions keep these
expansions out of historical 1.x validation, including when the same message is
placed in a request, response, error response, or compaction transcript entry.
The terminal-response message path also uses the v2 definitions.

For supported content not represented by a typed definition, a producer may
use the explicit `{"$type":"unknown","content":...}` wrapper with the complete
original JSON value, including its metadata. An opaque payload with its own
`type` or `$type` remains data and must not select or activate runtime types.
Only a safe, explicit JSON representation qualifies; no arbitrary object
reflection, `repr` fallback, or executable serialization is implied. If a
supported value cannot be preserved safely, report the incompatibility rather
than silently dropping it or claiming a lossless terminal result was persisted.
The v2 lossless producer-mapping requirement does not widen the old explicit
`unknown.content` JSON shape: arbitrary JSON inside that wrapper was already
valid historically and remains valid. Historical consumers are not newly
required to understand v2 producer mappings.

Lossless here means JSON value preservation, including array order, exact
string contents, numeric fidelity, and absent versus null fields. It does not
require byte-identical JSON formatting or object-property order. Wrapping is a
producer mapping for supported unmodeled content, not permission for a reader
to hide malformed known wire fields or accept unknown wire discriminators.

Known content `extensionData` maps only to the framework content metadata bag.
Undeclared sibling properties remain unknown wire data at their original
location and must not be promoted into framework metadata, where they could
impersonate framework-owned control values.

Unknown properties are allowed and must round-trip **at their original object
locations**, independently of explicit `extensionData` objects, including
nested content and mailbox fields. Known fields must still satisfy their
declared types; do not hide malformed values in extension data. This preservation
rule is a requirement on compatible readers/writers, not a description
of every current serializer. Metadata cannot override known envelope fields.

Unknown properties are not unknown discriminators: 2.0 accepts only transcript
`$type` values `request`, `response`, `errorResponse`, and `compaction`.
Compaction has no `correlationId`. Content `$type` values are `data`, `error`,
`functionCall`, `functionResult`, `hostedFile`, `hostedVectorStore`, `usage`,
`text`, `reasoning`, `uri`, and the explicit `unknown` wrapper. Unsupported
versions, outcomes, availability values, roles,
or discriminators must be rejected for processing, not silently converted to
success or to an empty unknown-content wrapper. Opaque `unknown.content` itself
can be any JSON value, including null; `$runtimeType` inside it is just data.
Runtime-profile discriminators and versions are different: validate them only
when relying on that profile, and otherwise preserve them without interpretation.

Persisted JSON is untrusted data. Never dynamically load types, follow URIs,
execute tool calls, or log opaque session/error/token contents merely by reading
state. Normal host authorization, redaction, and total storage/depth limits are
still required. Identifier limits count Unicode code points, not UTF-16 units.
Identifiers are nonblank and exclude C0/C1 controls; metadata is not executable.
Mailbox RFC 3339 timestamps preserve their original text, including fractional
precision beyond 100 nanoseconds, while separately parsing the instant for
ordering checks. Matching result and receipt fields compare the preserved text
so a read-and-write cycle remains exact across runtimes.
Usage metadata retains arbitrary JSON even if a runtime cannot represent it as
numeric counts. Timestamp precision and provider-specific
continuation formats require cross-language agreement; validation alone does
not ensure lossless projection. No provider or retention policy is implemented.

### Bookkeeping integer bounds

The named `inputTokenCount`, `outputTokenCount`, `totalTokenCount`, and
`ingestedPositions` values are integers from zero through `9223372036854775807`
(`Int64.MaxValue`). A present `truncation.evictedMessageCount` retains its minimum
of one and uses the same maximum. Counts may remain absent where the schema
already permits omission. Explicit null, booleans, numeric strings and fractional
values are not counters.

This narrows the previously unbounded known fields in every versioned shape
described by this schema. Readers and writers must enforce the same bounds.
Validate increments and sums before persisting them, including values produced
by retention or migration. On overflow, reject the operation without committing
the invalid state. Never wrap, clamp, discard the count, or hide an invalid known
counter in extension data. Existing out-of-range snapshots must be rejected for
processing without modifying their stored bytes.

JSON Schema's existing mathematical-integer semantics remain unchanged: integral
numbers such as `1.0` and `1e0` are valid when in range. Writers should emit integer
JSON tokens. Readers must not round fractional or out-of-range values into range
or route integer tokens through floating-point conversion. A runtime parser's
`Int64` token conversion alone may not accept every integral representation.

These bounds do not apply to opaque user payloads, provider metadata, unknown
properties, session state or runtime-profile data. Their existing JSON preservation
rules remain unchanged. A relying runtime may separately constrain its own profile.

## Rollout, migration, and maintainer questions

Before any runtime emits 2.0, enforce a deployment floor covering
state readers, duplicate lookups, pollers, writers, rollback writers, hosting
consumers, and tools such as the scheduler dashboard. Participants that may
process 2.0 must implement its behavior; others must reject it before processing
or mutation and be isolated from 2.0 routing. Merely preserving fields is not
enough. Rollback to a transcript-only writer must be prevented once 2.0 exists.

Do not migrate by changing only `schemaVersion` or by adding empty receipt maps
to previously used 1.x state. Pruned transcript cannot prove prior completion
or reconstruct immutable responses. Migration needs authoritative completion
evidence or an explicitly isolated new session generation with defined duplicate
handling. The shared schema does not itself perform or enable migration.

The [fixtures](fixtures/README.md) are contract examples, not evidence that either
runtime produces or safely consumes this format. The language-neutral
[validation cases](tests/README.md) record positive and negative schema expectations.
