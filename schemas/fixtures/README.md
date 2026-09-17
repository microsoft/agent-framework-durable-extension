# Durable agent state review fixtures

All JSON files here are synthetic, language-neutral review data validated with
the parent [Draft 2020-12 schema](../durable-agent-entity-state.json).
They are not captured production state, generated serializer snapshots, or
evidence of Python/.NET 2.0 support.

| Fixture | Provenance and purpose |
| --- | --- |
| `shared-durable-agent-state-1.2-python-shape.json` | Reproduced from proposal commit `247bbdd60944d5ac93e79079803aa23e992d0369`. Modeled on parallel Python-shaped 1.2 work, with synthetic future fields and content for preservation review. **Not byte-for-byte output from the current Python serializer.** |
| `shared-durable-agent-state-2.0.json` | Reproduced from the same proposal commit. Synthetic available success and expired failure receipt, detached from transcript responses; includes unknown mailbox/binding fields. |
| `shared-durable-agent-state-2.0-pruned.json` | Authored for this contract-only proposal. Empty transcript with available failure, unavailable success, opaque session data, truncation evidence, and whole-entity TTL. Not a migration output. |
| `shared-durable-agent-state-2.0-lossless.json` | Authored for review feedback. Synthetic developer-role request, verbatim string-form function arguments, URI without invented media type, complete opaque JSON content, explicit structured `false` value, and no binding. |

The source proposal builds on `5de13e8d5dd4b4b76e7360e89ceeb3968a103781`;
its runtime DTOs, converters, tests, and test-project fixture links are
deliberately excluded. The corrected `python-shape` filename identifies
provenance, not a promise of current serializer behavior. There is no fixture
generator or test-project integration in this PR.

The two reproduced source fixtures are unchanged as JSON values. The example
bindings are optional runtime-profile data, not a shared binding shape or proof
of session-fixed effective ownership. Compatible writers preserve them; only a
runtime relying on a profile validates its identity, version, shape, and policy.
The original legacy fixture stays valid under the unchanged historical message
definitions. The expanded lossless fixture is v2-only; its persisted bytes are
unchanged too.

For `shared-durable-agent-state-2.0.json`, interpret the example at
`2026-09-10T05:00:05Z`: `corr-2` has an available result and `corr-expired`
proves completed failure without a payload. After `corr-2`'s expiry a compatible
poller must report completed-but-result-unavailable even before cleanup.

For `shared-durable-agent-state-2.0-pruned.json`, interpret the example at
`2026-09-10T06:00:05Z`: `corr-failed` has an available failure payload and
`corr-pruned` proves completed success without one. Transcript removal did not
erase either completion. `expirationTimeUtc` is a separate whole-entity deadline,
not a proposed resolution of tombstone lifetime after entity deletion.

The lossless fixture intentionally contains incomplete function-argument text.
The exact string must survive; parsing it, completing the JSON, or replacing it
with an object would lose information. The URI has no `mediaType` to infer. The
opaque content's nested `$type` and `$runtimeType` are inert JSON, including all
metadata, not type-activation instructions. `response.value: false` is a present
structured result; the earlier fixtures omit `value`. Null, zero, and empty
values have separate [validation cases](../tests/README.md).

The lossless fixture's scalar `ingestedPositions["legacy-producer"] = 3` records
only highest-seen position. It does not say whether `2` was delivered. None of
these fixtures defines or infers a gap-preserving workflow receipt set.

The expired receipts retain `outcome`; lookup reports that retained outcome with
completed-but-result-unavailable, without restoring a payload or reopening
execution. This contract still requires the corresponding ADR/Python updates.
No fixture claims that a legacy receipt without authoritative outcome can be
converted by inventing success or failure. Such a receipt cannot satisfy the
v2 required-outcome shape without authoritative evidence.

For an identity absent from the receipt maps, the examples show only the
absence of recorded completion: a separately accepted request may be pending,
whereas an unrecognized identity remains unknown. No fixture invents an
admission registry, claims exactly-once external side effects, or authorizes
schema-version-only migration. The 1.2 and 2.0 examples are independent
snapshots, not a before/after pair with inferred completion evidence.

Validation must enable Draft 2020-12 and date-time format checking, then check
the cross-map/time invariants in [the proposal](../README.md). JSON Schema alone
cannot enforce key equality, atomicity, historical immutability, expiry relative
to a clock, or compatibility of a deployed reader.
