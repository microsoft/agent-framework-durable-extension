# Durable Agent State

Durable agents are represented as durable entities, with conversation history stored as JSON-serialized
state for an individual entity instance.

For the ownership and lifecycle boundaries around this representation, see the
[durable agent state architecture](../../../../docs/features/durable-agents/durable-state-architecture.md).

## State Schema

The [schema](../../../../schemas/durable-agent-entity-state.json) for durable agent state is a distillation of the prompt and response messages accumulated over the lifetime of a session. While these messages and content originate from Microsoft Agent Framework types (for .NET, see [ChatMessage](https://github.com/dotnet/extensions/blob/main/src/Libraries/Microsoft.Extensions.AI.Abstractions/ChatCompletion/ChatMessage.cs) and [AIContent](https://github.com/dotnet/extensions/blob/main/src/Libraries/Microsoft.Extensions.AI.Abstractions/Contents/AIContent.cs)), durable agent state uses its own, parallel, types in order to (1) better manage the versioning and compatibility of serialized state over time, (2) account for agent implementations across languages/platforms (e.g. .NET and Python), as well as (3) ensure consistency for external tools that make use of state data.

> When new AI content types are added to the Microsoft Agent Framework, equivalent types should be added to the entity state schema. If no equivalent exists, durable state uses the `unknown` content type. Arbitrary `unknown.content` JSON is opaque: .NET preserves generic producer fields, including `$runtimeType`, without interpreting them. The namespaced `$microsoftAgentFrameworkDurableTask` metadata envelope contains no runtime type name and can restore only the common `AIContent` contract (`Annotations`, `AdditionalProperties`, and safely serializable `RawRepresentation`); it can never select or construct a CLR type. Within that envelope, unsupported or invalid values are omitted independently so safe sibling values remain, omission details are recorded, and warnings contain only the value type and a fixed failure category. Response and message `AdditionalProperties` use a stricter boundary for schema 2 terminal results: supported scalar values and explicit `JsonElement` values are persisted, while unsupported runtime objects cause conversion to fail. Values that reach durable state are therefore JSON-safe and do not depend on CLR runtime type activation.

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
snapshots are rejected until compatibility is explicitly reviewed. Wiring that path into entity execution is
deferred. New `DurableAgentState` instances default to the current version, while deserialization preserves
the persisted version through an init-only property.

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

This layer defines and round-trips the schema contracts only. Agent entity integration for session ownership,
replay filtering, compaction, retention, and provider behavior is deferred to later stack layers.

Version-aware validation is symmetric: the 1.x message restrictions (no `developer` role, object-only function-call
`arguments`, and a required URI `mediaType`) are enforced when legacy state is written, not only when it is read, so
a programmatically constructed 1.x state cannot be persisted in a form this reader later rejects.

## Revised execution-state foundation

> [!IMPORTANT]
> Schema `2.0.0` is being adopted through a staged cross-runtime rollout. Production .NET readers and writers currently default to
> schema `1.2.0` and reject schema 2 until the mailbox-aware runtime rollout requirements are met.

The mailbox and provisional history-binding contracts use schema `2.0.0`. This is intentionally a fail-closed major
version: a 1.x worker preserves unknown fields but does not understand completion receipts, so allowing it to
process revised state could rerun work whose transcript result was already removed. The production .NET reader
and writer reject 2.0 until mailbox-aware behavior is activated, and new state continues to default to `1.2.0`.
An explicit internal passive contract path exists only for serializer/fixture tests and later deliberate
activation. A later execution layer must opt into `2.0.0` only when it implements the complete mailbox layout.

In revised state, `terminalResults` stores immutable result envelopes by correlation ID outside
`conversationHistory`, while `completionReceipts` retains completion evidence after a result payload expires.
An `available` receipt requires a matching result; an `unavailable` receipt proves completion without a result
payload while retaining its outcome. Absence of a receipt means only that no terminal completion is recorded;
it does not distinguish an accepted pending request from an unknown identity. Optional `historyBinding` is an
opaque, separately versioned runtime profile. Non-relying consumers preserve it without interpreting any
nested field. Only a relying runtime may validate a profile it recognizes against trusted host configuration;
the shared contract defines no owner kind, provider key, default, or transition policy.

The schema 2 DTOs and converters currently form a passive contract: they validate and round-trip persisted shape
but do not perform delivery lookup, polling, binding enforcement, result expiry, or transcript retention. The
mailbox-aware runtime layer must implement those stateful operations before it can activate schema 2. See the
[schema 2 contract](../../../../schemas/README.md) for the complete semantic invariants and rollout gate.

When those layers activate schema 2.0, one successful durable entity operation must atomically commit the
terminal result and receipt together with that operation's session continuation, ingestion bookkeeping,
entity-local transcript, whole-entity TTL, optional binding, and other local control state. External provider
writes and tool side effects are outside that entity-local transaction. This layer validates persisted shape
and consistency but performs no commit or lookup behavior.

Schema 2.0 must not be activated as a cross-language write format until every participating runtime either
implements the mailbox/binding contract or explicitly rejects the new major version. The current C# reader is
fail-closed for unsupported majors and defaults new writes to 1.2. Other runtimes require coordinated version
gating before a 2.0 producer is enabled; preserving unknown fields alone is not sufficient because an
unaware worker could ignore completion receipts and rerun completed work.

## State Examples

### Current schema 1.2

This illustrative document uses the current production write format. It includes conversation history and
representative optional fields that may be written by interoperating producers; producers omit fields they do
not use. The current .NET runtime preserves but does not populate `ingestedPositions`. Each value is a scalar
highest-seen position for one producer, not an array or proof that every earlier position was delivered.

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
    ],
    "session": {
      "conversationId": "service-conversation-42",
      "stateBag": {}
    },
    "ingestedPositions": {
      "legacy-producer": 3
    },
    "truncation": {
      "evictedMessageCount": 2,
      "firstEvictedAt": "2025-11-04T18:00:00+00:00",
      "lastEvictedAt": "2025-11-04T18:30:00+00:00"
    },
    "extensionData": {
      "producer": "example"
    }
  }
}
```

### Schema 2.0

This example is for contract review and contributor guidance only. Production runtimes must not emit schema 2
until the activation requirements above are met. An `available` receipt has a matching terminal result; an
`unavailable` receipt remains after its result payload has been removed.

```json
{
  "schemaVersion": "2.0.0",
  "data": {
    "conversationHistory": [],
    "terminalResults": {
      "request-42": {
        "correlationId": "request-42",
        "outcome": "succeeded",
        "completedAt": "2026-09-10T05:00:03+00:00",
        "resultExpiresAt": "2026-09-11T05:00:03+00:00",
        "response": {
          "messages": [
            {
              "role": "assistant",
              "messageId": "response-42",
              "contents": [
                {
                  "$type": "text",
                  "text": "Artifact generated."
                }
              ]
            }
          ],
          "value": {
            "artifactId": "artifact-42"
          }
        }
      }
    },
    "completionReceipts": {
      "request-42": {
        "correlationId": "request-42",
        "outcome": "succeeded",
        "completedAt": "2026-09-10T05:00:03+00:00",
        "resultState": "available",
        "resultExpiresAt": "2026-09-11T05:00:03+00:00"
      },
      "request-expired": {
        "correlationId": "request-expired",
        "outcome": "failed",
        "completedAt": "2026-09-09T05:00:03+00:00",
        "resultState": "unavailable",
        "resultExpiresAt": "2026-09-10T05:00:03+00:00",
        "resultUnavailableAt": "2026-09-10T05:00:04+00:00"
      }
    },
    "historyBinding": {
      "version": 1,
      "ownerKind": "historyProvider",
      "providerKey": "contoso.support-history.v1"
    }
  }
}
```

## State Consumers

Additional tools may make use of durable agent state. Significant changes to the state schema may need corresponding changes to those applications.

### Durable Task Scheduler Dashboard

The [Durable Task Scheduler (DTS)](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler) Dashboard, while providing general UX for management of durable orchestrations and entities, also has UX specific to the use of durable agents.
