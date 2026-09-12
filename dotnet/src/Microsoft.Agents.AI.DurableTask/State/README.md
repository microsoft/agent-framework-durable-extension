# Durable Agent State

Durable agents are represented as durable entities, with conversation history stored as JSON-serialized
state for an individual entity instance.

## State Schema

The [schema](../../../../schemas/durable-agent-entity-state.json) for durable agent state is a distillation of the prompt and response messages accumulated over the lifetime of a session. While these messages and content originate from Microsoft Agent Framework types (for .NET, see [ChatMessage](https://github.com/dotnet/extensions/blob/main/src/Libraries/Microsoft.Extensions.AI.Abstractions/ChatCompletion/ChatMessage.cs) and [AIContent](https://github.com/dotnet/extensions/blob/main/src/Libraries/Microsoft.Extensions.AI.Abstractions/Contents/AIContent.cs)), durable agent state uses its own, parallel, types in order to (1) better manage the versioning and compatibility of serialized state over time, (2) account for agent implementations across languages/platforms (e.g. .NET and Python), as well as (3) ensure consistency for external tools that make use of state data.

> When new AI content types are added to the Microsoft Agent Framework, equivalent types should be added to the entity state schema as well. The durable agent state "unknown" type is used when an AI content type is encountered but no equivalent type exists. Arbitrary `unknown.content` JSON is opaque: generic producer fields, including `$runtimeType`, are never interpreted by .NET and round-trip unchanged. .NET uses the single namespaced `$microsoftAgentFrameworkDurableTask` property only for its versioned metadata envelope. That envelope contains no runtime type name and can restore only the common `AIContent` contract (`Annotations`, `AdditionalProperties`, and safely serializable `RawRepresentation`); it can never select or construct a CLR type. Common metadata values are converted independently. Unsupported, cyclic, disposed, invalid, or getter/converter-failing values are omitted while safe siblings remain, an omission count/flag is recorded, and a warning logs only the value's type and a fixed failure category. The final durable state therefore contains only JSON-safe data.

## State Versioning

The serialized state contains a root `schemaVersion` property, which represents the version of the schema used to serialize data in that state (represented by the `data` property).

Some versioning considerations:

- Versions use the strict numeric SemVer core grammar `"<major>.<minor>.<patch>"`: exactly three
  non-negative decimal components, with no leading zeroes except the single digit `0`. Prerelease
  suffixes, build metadata, a `v` prefix, missing/extra components, and whitespace are rejected.
- Durable agents should use the version property to determine how to deserialize that state and should not attempt to deserialize semver-incompatible versions
- Newer versions of durable agents should strive to be compatible with older schema versions (e.g. new properties and objects should be optional)
- Durable agents should preserve existing, but unrecognized, properties when serializing state

Schema version 1.2 adds optional message identity and extension metadata, opaque session state, workflow
`ingestedPositions`, and bounded truncation evidence. The .NET workflow path preserves but does not currently
populate `ingestedPositions`. Older 1.x state remains readable. `DurableAgentState.Clone()` promotes older
supported versions to 1.2 when a caller uses that write-clone path. Versions outside the exact contract
snapshots are rejected until compatibility is explicitly reviewed. Entity execution uses an independent
working-state clone so failed operations do not mutate the hydrated state. New
`DurableAgentState` instances default to the current version, while deserialization preserves the persisted
version through an init-only property.

The historical 1.0–1.2 message boundaries remain unchanged on both reads and writes: supported roles
are `user`, `assistant`, `system`, and `tool`; function-call arguments are objects when present; URI
content requires a media type. Legacy runtime adapters continue to use typed function arguments,
not string-valued `RawRepresentation`. Developer messages and verbatim string arguments use the
explicit schema-2 adapters only. The production legacy writer validates the serialized transcript
before publishing it, so directly constructed DTOs cannot bypass these boundaries. A failed write
leaves the hydrated state unchanged and does not record completion.
Schema-2 history and terminal delivery share an explicit message projection. A shared URI without
`mediaType` is opaque `AIContent` carrying its canonical JSON, including unknown content properties;
no media type is invented. Normal URI content remains native `UriContent`. Message roles, IDs, authors,
timestamps and additional properties survive projection, and canonical message/response JSON retains
unknown metadata that the native types cannot express. Cold next-invocation history uses this projection,
not the stricter legacy conversion.

The schema's declared `extensionData` objects and forward-compatible unknown JSON properties are distinct.
The .NET model names declared metadata `ExtensionData` (or message `AdditionalProperties`) and names
`[JsonExtensionData]` catch-all dictionaries `UnknownProperties`. Both coexist and round-trip independently
at root, data, entry, message, and usage boundaries. Content types also use `UnknownProperties`; the current
schema does not declare a content-level `extensionData` field. Unknown fields are never folded into an
application-defined `extensionData` object.

Usage `extensionData` is preserved as arbitrary JSON for forward compatibility. When a durable response is
projected to `UsageDetails`, only integral numeric extension values representable as `Int64` become additional
counts; strings, objects, arrays, fractional numbers, and out-of-range numbers remain in durable state but are
ignored by the runtime projection. Malformed known count fields fail deserialization rather than being silently reinterpreted.

Mailbox lookup, delivery, and guarded entity-local commit behavior are implemented. Session ownership,
replay filtering, transcript compaction, and provider behavior remain deferred to later stack layers.

## Revised execution-state foundation

The mailbox contract and optional opaque history profile use schema `2.0.0`. This is intentionally a fail-closed major
version: a 1.x worker preserves unknown fields but does not understand completion receipts, so allowing it to
process revised state could rerun work whose transcript result was already removed. The .NET reader now
supports mailbox-aware 2.0 lookup, while new state continues to default to `1.2.0`.
Producer activation is an internal test-only gate, disabled by default. There is no public opt-in until
the shared contract and deployment floor are agreed; this draft does not authorize production activation. Existing 2.0 state
is preserved rather than downgraded, and committed duplicates remain readable with writes disabled.
New requests against existing 2.0 state fail before agent execution when the internal write gate is disabled.
The internal passive contract path remains available for serializer/fixture tests without authorizing new
production writes.

In revised state, `terminalResults` stores immutable result envelopes by correlation ID outside
`conversationHistory`, while `completionReceipts` retains completion evidence after a result payload expires.
An `available` receipt requires a matching result; an `unavailable` receipt proves completion without a result
payload while retaining its outcome. Absence of a receipt means only that no terminal completion is recorded;
it does not distinguish an accepted pending request from an unknown identity. Optional `historyBinding` is an
opaque, separately versioned runtime profile. Non-relying consumers preserve it without interpreting any
nested field. Only a relying runtime may validate a profile it recognizes against trusted host configuration;
the shared contract defines no owner kind, provider key, default, or transition policy.
Missing and explicit-null profiles remain distinct, and scalar, array, and object values round-trip
unchanged through the independent working clone. This layer never selects a provider from this data.

The shared DTOs retain their schema contract. Delivery lookup and polling use the same resolver for both
layouts. `ResultRetentionPeriod` is optional and defaults to no payload expiry; expiry never removes the
completion receipt. Binding selection/enforcement and transcript retention are not implemented by this layer.
With the internal mailbox-writer gate enabled, successful new runs and the scheduled/explicit
`CheckAndExpireResults` entity operation durably remove due `terminalResults` entries and transition
their existing receipts to `unavailable`. The operation uses an independent state copy and commits its
next self-signal with that state; it does not refresh whole-entity TTL or require the independent deletion
gate. Expiry is rechecked against current state and `TimeProvider` on every serialized turn; stale signals
are not authority to remove a newer result. Polling is read-only. Imported/idle states without a scheduling
chain require an explicit cleanup operation or a later successful new run; there is no automatic scan.
See the [cleanup, retry, and scheduling limits](../README.md#durable-completion-and-delivery).

Under the internal schema-2 test gate, one successful durable entity operation atomically stages the
terminal result and receipt together with that operation's session continuation, ingestion bookkeeping,
entity-local transcript, whole-entity TTL, optional binding, and other local control state. External provider
writes and tool side effects are outside that entity-local transaction. A failed outer invocation or durable
commit does not establish a new completion receipt. Legacy conversion uses only retained authoritative
terminal evidence, never calls the model/tools, and cannot invent receipts for already-evicted results.
Previously persisted legacy state stays legacy unless independently authorized as complete history;
retained transcript alone does not establish that authorization. Known truncation/compaction prevents
conversion. New missing-state generations can initialize 2.0 only under the internal gate.
Whole-entity deletion of 2.0 state has a separate internal gate, disabled by default. Stored legacy
deadlines cannot erase receipts, and there is no implicit schema-2 entity TTL.

Schema 2.0 must not be activated as a cross-language write format until every participating runtime either
implements the mailbox/binding contract or explicitly rejects the new major version. The current C# reader is
fail-closed for unsupported versions and defaults new writes to 1.2. Other runtimes require coordinated version
gating before a 2.0 producer is enabled; preserving unknown fields alone is not sufficient because an
unaware worker could ignore completion receipts and rerun completed work.

## Sample State

```json
{
  "schemaVersion": "1.2.0",
  "data": {
    "conversationHistory": [
      {
        "$type": "request",
        "responseType": "text",
        "correlationId": "c338f064f4b44b8d9c21a66e3cda41b2",
        "createdAt": "2025-11-04T19:33:05.245476+00:00",
        "messages": [
          {
            "contents": [
              {
                "$type": "text",
                "text": "Start the documentation generation workflow for the product \u0027Goldbrew Coffee\u0027"
              }
            ],
            "role": "user"
          }
        ]
      },
      {
        "$type": "response",
        "usage": {
          "inputTokenCount": 595,
          "outputTokenCount": 63,
          "totalTokenCount": 658
        },
        "correlationId": "c338f064f4b44b8d9c21a66e3cda41b2",
        "createdAt": "2025-11-04T19:33:10.47008+00:00",
        "messages": [
          {
            "authorName": "OrchestratorAgent",
            "createdAt": "2025-11-04T19:33:10+00:00",
            "contents": [
              {
                "$type": "functionCall",
                "arguments": {
                  "productName": "Goldbrew Coffee"
                },
                "callId": "call_qWk9Ay4doKYrUBoADK8MBwHf",
                "name": "StartDocumentGeneration"
              }
            ],
            "role": "assistant"
          },
          {
            "authorName": "OrchestratorAgent",
            "createdAt": "2025-11-04T19:33:10.47008+00:00",
            "contents": [
              {
                "$type": "functionResult",
                "callId": "call_qWk9Ay4doKYrUBoADK8MBwHf",
                "result": "8b835e8f2a6f40faabdba33bd8fd8c74"
              }
            ],
            "role": "tool"
          },
          {
            "authorName": "OrchestratorAgent",
            "createdAt": "2025-11-04T19:33:10+00:00",
            "contents": [
              {
                "$type": "text",
                "text": "The documentation generation workflow for the product \u0022Goldbrew Coffee\u0022 has been started. You can request updates on its status or provide additional input anytime during the process. Let me know how you\u2019d like to proceed!"
              }
            ],
            "role": "assistant"
          }
        ]
      },
      {
        "$type": "request",
        "responseType": "text",
        "correlationId": "71f35b7add6b403fadd0db8a7c137b58",
        "createdAt": "2025-11-04T19:33:11.903413+00:00",
        "messages": [
          {
            "contents": [
              {
                "$type": "text",
                "text": "Tell the user that you\u0027re starting to gather information for product \u0027Goldbrew Coffee\u0027."
              }
            ],
            "role": "system"
          }
        ]
      },
      {
        "$type": "response",
        "usage": {
          "inputTokenCount": 396,
          "outputTokenCount": 48,
          "totalTokenCount": 444
        },
        "correlationId": "71f35b7add6b403fadd0db8a7c137b58",
        "createdAt": "2025-11-04T19:33:12+00:00",
        "messages": [
          {
            "authorName": "OrchestratorAgent",
            "createdAt": "2025-11-04T19:33:12+00:00",
            "contents": [
              {
                "$type": "text",
                "text": "I am starting to gather information to create product documentation for \u0027Goldbrew Coffee\u0027. If you have any specific details, key features, or requirements you\u0027d like included, please share them. Otherwise, I\u0027ll continue with the standard documentation process."
              }
            ],
            "role": "assistant"
          }
        ]
      }
    ]
  }
}
```

## State Consumers

Additional tools may make use of durable agent state. Significant changes to the state schema may need corresponding changes to those applications.

### Durable Task Scheduler Dashboard

The [Durable Task Scheduler (DTS)](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler) Dashboard, while providing general UX for management of durable orchestrations and entities, also has UX specific to the use of durable agents.
