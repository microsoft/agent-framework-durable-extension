# Proposed durable agent state 2.0 contract

**Proposal for joint Python, .NET, and Durable Task review; not runtime activation.**
This change contains only a JSON Schema, synthetic fixtures, and documentation.
Neither Python nor .NET is authorized to emit `2.0.0` by this proposal or by a
successful schema validation. Merge of this contract proposal requires agreement
on compatible-reader/consumer semantics and the rollout floor; emitting 2.0
requires a separately reviewed implementation and an enforced deployment gate.

The motivation is to separate execution/delivery completion from conversation
history that may be compacted or evicted. Preserving unknown JSON alone cannot
prevent duplicate execution if a reader still uses transcript responses to
decide whether a request completed. [ADR discussion #88](https://github.com/microsoft/agent-framework-durable-extension/pull/88)
provides related compaction/retention context, not a mandate for this exact schema.

## Version and reader policy

The [Draft 2020-12 schema](durable-agent-entity-state.json) describes exact review
snapshots `1.0.0`, `1.1.0`, proposed `1.2.0`, and proposed `2.0.0`; it is not a
list of versions supported by today's runtimes. The proposed 1.2 fields are
included for compatibility discussion, not claimed as released Python output.
Legacy transcript entries retain the existing permissive entry shape. The
three mailbox/binding fields are forbidden in 1.x, even if empty. Version 2.0
requires `terminalResults`, `completionReceipts`, and `conversationHistory`
(which can be empty). `historyBinding` is optional and provisional, not a
requirement for session-fixed effective ownership.

Major 2 is proposed because the authority for completion and replay changes,
not merely because new optional properties appear. **Is a major version the
right mechanism, or can maintainers enforce an equally safe same-major rollout
gate?** This schema intentionally rejects unlisted versions, including future
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

## Proposed wire concepts and semantic invariants

| Field | Proposed meaning |
| --- | --- |
| `terminalResults[correlationId]` | Immutable terminal response/error envelope, detached from transcript retention. |
| `completionReceipts[correlationId]` | Independent completion evidence, retained after payload expiry and transcript pruning. |
| Receipt `resultState` | `available` with a matching payload, or `unavailable` with a removal timestamp. |
| `historyBinding` | Optional, provisional version 1 configuration descriptor; not a session-fixed effective owner. Representation remains under discussion. |
| `conversationHistory` | Evictable transcript, never the authoritative completion index in 2.0. |

The schema validates local shape, not all cross-object or temporal invariants.
Any future implementation must additionally enforce:

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
  identities are not automatically pending. This proposal adds no request
  admission registry and makes no exactly-once claim for external side effects.
- `resultExpiresAt`, when present, is no earlier than `completedAt`. At or after
  that instant a poller must report completed-but-result-unavailable even if a
  lazy cleanup has not yet removed a stored `available` payload. It must not
  deliver the expired payload, report pending, or rerun the request.
  `resultUnavailableAt` is no earlier than completion; for a time-expired
  payload it is no earlier than expiry. `unavailable` can also represent an
  agreed explicit removal policy; absence of `resultExpiresAt` is not a
  guarantee against such removal.
- Receipts survive payload expiry and transcript pruning for the agreed
  duplicate-delivery lifetime. No receipt eviction or session-ID reuse policy
  is specified here. Whole-entity TTL/deletion and receipt storage growth must
  be resolved before deployment, not silently treated as transcript retention.

**Expired lookup outcome is pending agreement.** The proposed caller-visible
behavior is completed-but-result-unavailable plus the retained `succeeded` or
`failed` outcome, with no expired payload. This would require explicit ADR
wording and a Python receipt/lookup update; it is not a claim about current
Python behavior. The schema already preserves outcome after expiry, but this
proposal does not settle the lookup API's shape or enable any runtime behavior.

The proposed envelope requires `response.messages` for success and failure
(an empty list is allowed). Failure additionally requires `error.code` and
`error.message`; success forbids `error`. Error details and response metadata
are JSON only. A failure's partial messages are diagnostic output, not an
instruction to replay a failed turn. Whether failures should instead use an
error-only union, and how cancellation is represented, remain review questions.

`response.value` is an optional, named caller-visible JSON result, independent
of messages and text. An absent field means no structured value; explicit
`null`, `false`, `0`, `""`, `[]`, and `{}` are present values and must survive
round-tripping without truthiness-based omission. No separate presence flag
is needed. Consumers must not infer `value` from text, coerce its type, or
serialize arbitrary model/runtime objects. Unknown nested properties remain
part of the value. The same JSON preservation rule applies if a failed response
carries a diagnostic value; its outcome remains failed.

### Provisional configuration binding

Stable provider/configuration identity is distinct from effective per-run
history ownership. The shared contract **does not require session-fixed
effective ownership** and must not prohibit Python's supported per-run
transitions. C# may separately enforce a fixed-owner policy/profile.

For now, `historyBinding` is optional and keeps the existing review shape
(`version`, `ownerKind`, `providerKey`) without defining an effective-owner
transition protocol. If supplied, `ownerKind` describes the configured facility:
`durableState`, `historyProvider`, or `modelService`; it does not say that
facility owns every run. The non-secret `providerKey` identifies configuration,
not credentials, endpoints, runtime types, or opaque session properties.
Absence does not imply a default owner or authorize inference from session data.
This descriptor is not an authorization grant.

The exact representation remains pending Ahmed's feedback: an optional shared
`providerBinding`/`configurationBinding`, or an optional runtime extension/profile.
No rename, new profile format, migration rule, or transition restriction is
introduced here. Trusted hosting configuration must still recognize any
descriptor it processes and separately validate supported per-run transitions.

## Existing state, extension data, and trust boundaries

`session` is opaque JSON continuation/provider state. Preserve it, but do not
infer an owner or instantiate a runtime type from its contents. The optional
terminal `continuationToken` is base64 bytes; cross-language encoding does not
prove cross-provider resumability. Its byte contract needs joint agreement.

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
semantics, independent of transcript retention. This proposal neither invents
that wire format nor backfills receipts from the scalar.

`truncation` records evicted message count and first/last eviction
instants (last must be no earlier than first). It is diagnostic evidence, not
model context. Optional `messageId` is message identity, not request identity.

### Lossless messages and JSON content

Message roles include `developer`. Function-call `arguments` accepts the
original object or string; preserve the entire string, including whitespace
and incomplete/non-JSON text, without parsing it into an object. URI content
requires a URI but not a media type; absence stays absent rather than being
filled with guessed metadata. These are additive shared message shapes for
review, not evidence that existing 1.x consumers can handle them.

For supported content not represented by a typed definition, a producer may
use the explicit `{"$type":"unknown","content":...}` wrapper with the complete
original JSON value, including its metadata. An opaque payload with its own
`type` or `$type` remains data and must not select or activate runtime types.
Only a safe, explicit JSON representation qualifies; no arbitrary object
reflection, `repr` fallback, or executable serialization is implied. If a
supported value cannot be preserved safely, report the incompatibility rather
than silently dropping it or claiming a lossless terminal result was persisted.

Lossless here means JSON value preservation, including array order, exact
string contents, numeric fidelity, and absent versus null fields. It does not
require byte-identical JSON formatting or object-property order. Wrapping is a
producer mapping for supported unmodeled content, not permission for a reader
to hide malformed known wire fields or accept unknown wire discriminators.

Unknown properties are allowed and must round-trip **at their original object
locations**, independently of explicit `extensionData` objects, including
nested content and mailbox fields. Known fields must still satisfy their
declared types; do not hide malformed values in extension data. This preservation
rule is a requirement on future compatible readers/writers, not a description
of every current serializer. Metadata cannot override known envelope fields.

Unknown properties are not unknown discriminators: 2.0 accepts only transcript
`$type` values `request`, `response`, `errorResponse`, and `compaction`.
Compaction has no `correlationId`. Content `$type` values are `data`, `error`,
`functionCall`, `functionResult`, `hostedFile`, `hostedVectorStore`, `usage`,
`text`, `reasoning`, `uri`, and the explicit `unknown` wrapper. Unsupported
versions, owner kinds, binding versions, outcomes, availability values, roles,
or discriminators must be rejected for processing, not silently converted to
success or to an empty unknown-content wrapper. Opaque `unknown.content` itself
can be any JSON value, including null; `$runtimeType` inside it is just data.

Persisted JSON is untrusted data. Never dynamically load types, follow URIs,
execute tool calls, or log opaque session/error/token contents merely by reading
state. Normal host authorization, redaction, and total storage/depth limits are
still required. Identifier limits count Unicode code points, not UTF-16 units.
Identifiers are nonblank and exclude C0/C1 controls; metadata is not executable.
Usage metadata retains arbitrary JSON even if a runtime cannot represent it as
numeric counts. Integer ranges, timestamp precision, and provider-specific
continuation formats require cross-language agreement; validation alone does
not ensure lossless projection. No provider or retention policy is implemented.

## Rollout, migration, and maintainer questions

Before any runtime emits 2.0, agree on and enforce a deployment floor covering
state readers, duplicate lookups, pollers, writers, rollback writers, hosting
consumers, and tools such as the scheduler dashboard. Participants that may
process 2.0 must implement its behavior; others must reject it before processing
or mutation and be isolated from 2.0 routing. Merely preserving fields is not
enough. Rollback to a transcript-only writer must be prevented once 2.0 exists.

Do not migrate by changing only `schemaVersion` or by adding empty receipt maps
to previously used 1.x state. Pruned transcript cannot prove prior completion
or reconstruct immutable responses. Migration needs authoritative completion
evidence or an explicitly isolated new session generation with agreed duplicate
handling. This PR contains neither a migration nor code to enable revised writes.

Feedback requested from Ahmed/Python maintainers and .NET/Durable Task maintainers:

1. Schema shape, correlation scope, major 2 versus an enforceable same-major gate.
2. Optional configuration binding representation versus a runtime extension/profile,
   without imposing session-fixed ownership as a shared rule.
3. Success/error envelope, cancellation, and continuation-byte interoperability.
4. Proposed exposed outcome after expiry (pending agreement), receipt lifetime, entity TTL,
   storage growth, and late duplicates after session deletion/recreation.
5. Numeric/Unicode/timestamp limits, unknown-field preservation, and discriminator policy.
6. Compatible reader/consumer rollout floor, safe 1.2-to-2.0 migration, and rollback.

The [fixtures](fixtures/README.md) are review examples, not evidence that either
runtime produces or safely consumes this format. The language-neutral
[validation cases](tests/README.md) record positive and negative schema expectations.
