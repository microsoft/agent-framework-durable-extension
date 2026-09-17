# Agent HTTP API camelCase migration

The generated agent HTTP endpoints now use camelCase JSON and query parameter names as the canonical public contract. This aligns the agent endpoints with the workflow HTTP endpoints and MCP tool arguments without changing durable runtime state.

## What changed

Use these names for new code:

| Previous name | Canonical name | Where used |
| --- | --- | --- |
| `session_id` | `sessionId` | Agent run query parameter and JSON request/response field |
| `wait_for_response` | `waitForResponse` | Agent run query parameter and Python JSON request field |
| `correlation_id` | `correlationId` | Python agent run JSON response field; not a request field |
| `thread_id` | `sessionId` | Deprecated alias for the same session key |

The `x-ms-session-id` and `x-ms-wait-for-response` headers did not change.

## Compatibility window

The old snake_case names are still accepted on requests so existing callers can migrate gradually. JSON responses temporarily include both the canonical camelCase fields and their legacy snake_case equivalents where those fields already existed.

Session identifier aliases must match across the query string and request body. For example, `sessionId=abc` and `session_id=abc` is accepted, but `sessionId=abc` and `session_id=def` returns HTTP 400. For `waitForResponse`, conflicting aliases are rejected only when both forms occur in the query string or both occur in the request body. Resolution uses the `x-ms-wait-for-response` header first, then query parameters, then the request body; camelCase wins when both aliases in the same source agree.

Responses to requests that use legacy agent HTTP aliases include these migration signals:

```http
Deprecation: @1789430400
Link: <https://github.com/microsoft/agent-framework-durable-extension/blob/main/docs/features/durable-agents/http-api-camelcase-migration.md>; rel="deprecation"
Warning: 299 - "Deprecated agent HTTP field names are supported temporarily; use sessionId and waitForResponse."
```

## Migration steps

Update callers to send `sessionId` and `waitForResponse`. Update response parsing to read `sessionId` and, for Python Azure Functions agent responses, `correlationId`. Keep support for the old response names only as a temporary fallback until the compatibility window ends.

This change is limited to the public agent HTTP boundary. Do not rename persisted durable state fields, entity IDs, workflow HTTP fields, MCP tool arguments, Python SDK options such as `response_format` and `enable_tool_calls`, or SDK/internal constants as part of this migration.
