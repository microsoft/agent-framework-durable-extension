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
supported versions to 1.2 when a caller uses that write-clone path, while later same-major versions remain
unchanged. Wiring that path into entity execution is deferred. Major versions remain fail-closed. New
`DurableAgentState` instances default to the current version, while deserialization preserves the persisted
version through an init-only property.

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

## Revised execution-state foundation

The mailbox and fixed-history binding contracts use schema `2.0.0`. This is intentionally a fail-closed major
version: a 1.x worker preserves unknown fields but does not understand completion receipts, so allowing it to
process revised state could rerun work whose transcript result was already removed. The .NET reader accepts
legacy 1.x state and revised 2.x state, but new state continues to default to `1.2.0`; this schema-only layer
does not activate revised writes. A later execution layer must opt into `2.0.0` only when it writes the complete
mailbox and binding layout.

In revised state, `terminalResults` stores immutable result envelopes by correlation ID outside
`conversationHistory`, while `completionReceipts` retains completion evidence after a result payload expires.
No receipt means pending; an `available` receipt requires a matching result; an `unavailable` receipt proves
completion without a result payload. `historyBinding` records a versioned owner kind and stable logical provider
key. The key is explicit wire identity and must not be inferred from CLR type names or opaque session keys.

These DTOs and converters are passive contracts. Delivery lookup and polling, binding selection and enforcement,
result expiry, and transcript retention are implemented by later stack layers.

Schema 2.0 must not be activated as a cross-language write format until every participating runtime either
implements the mailbox/binding contract or explicitly rejects the new major version. The current C# reader is
fail-closed for unsupported majors and defaults new writes to 1.2. Other runtimes require coordinated version
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
