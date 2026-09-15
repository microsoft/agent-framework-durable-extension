# Agent HTTP API camelCase migration

The generated agent HTTP endpoints now use camelCase JSON and query parameter names as the canonical public contract. This aligns the agent endpoints with the workflow HTTP endpoints and MCP tool arguments without changing durable runtime state.

## What changed

Use these names for new code:

| Previous name | Canonical name | Where used |
| --- | --- | --- |
| `session_id` | `sessionId` | Agent run query parameter and JSON request/response field |
| `wait_for_response` | `waitForResponse` | Agent run query parameter and Python JSON request field |
| `correlation_id` | `correlationId` | Python agent run JSON response field |
| `thread_id` | `sessionId` | Deprecated alias for the same session key |

The `x-ms-session-id` and `x-ms-wait-for-response` headers did not change.

## Compatibility window

The old snake_case names are still accepted on requests so existing callers can migrate gradually. JSON responses temporarily include both the canonical camelCase fields and their legacy snake_case equivalents where those fields already existed.

If a request supplies more than one alias for the same value, all aliases must match. For example, `sessionId=abc` and `session_id=abc` is accepted, but `sessionId=abc` and `session_id=def` returns HTTP 400.

## Migration steps

Update callers to send `sessionId` and `waitForResponse`. Update response parsing to read `sessionId` and, for Python Azure Functions agent responses, `correlationId`. Keep support for the old response names only as a temporary fallback until the compatibility window ends.

This change is limited to the public agent HTTP boundary. Do not rename persisted durable state fields, entity IDs, workflow HTTP fields, MCP tool arguments, or SDK/internal constants as part of this migration.
