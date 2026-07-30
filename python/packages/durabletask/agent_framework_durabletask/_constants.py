# Copyright (c) Microsoft. All rights reserved.

"""Constants for Azure Functions Agent Framework integration.

This module contains:
- Runtime configuration constants (polling, MIME types, headers)
- JSON field name mappings for camelCase (JSON) ↔ snake_case (Python) serialization

For serialization constants, use the DurableStateFields, ContentTypes, and EntryTypes classes
to ensure consistent field naming between to_dict() and from_dict() methods.
"""

import warnings
from typing import TYPE_CHECKING, Final

# Supported request/response formats and MIME types
REQUEST_RESPONSE_FORMAT_JSON: str = "json"
REQUEST_RESPONSE_FORMAT_TEXT: str = "text"
MIMETYPE_APPLICATION_JSON: str = "application/json"
MIMETYPE_TEXT_PLAIN: str = "text/plain"

# Field and header names
SESSION_ID_FIELD: str = "session_id"
SESSION_ID_HEADER: str = "x-ms-session-id"
WAIT_FOR_RESPONSE_FIELD: str = "wait_for_response"
WAIT_FOR_RESPONSE_HEADER: str = "x-ms-wait-for-response"

# Deprecated request field name. Incoming requests may still use "thread_id", but responses
# only ever emit SESSION_ID_FIELD / SESSION_ID_HEADER.
LEGACY_THREAD_ID_FIELD: str = "thread_id"
LEGACY_THREAD_ID_HEADER: str = "x-ms-thread-id"

if TYPE_CHECKING:  # pragma: no cover - declarations for the deprecated aliases below
    THREAD_ID_FIELD: str
    THREAD_ID_HEADER: str

# Deprecated public names mapped to (preferred name, value).
_DEPRECATED_ALIASES: Final[dict[str, tuple[str, str]]] = {
    "THREAD_ID_FIELD": ("SESSION_ID_FIELD", LEGACY_THREAD_ID_FIELD),
    "THREAD_ID_HEADER": ("SESSION_ID_HEADER", LEGACY_THREAD_ID_HEADER),
}


def __getattr__(name: str) -> str:
    """Resolve deprecated constant names, emitting a DeprecationWarning."""
    alias = _DEPRECATED_ALIASES.get(name)
    if alias is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    preferred, value = alias
    warnings.warn(
        f"{name} is deprecated and will be removed in a future release; use {preferred} instead. "
        "Responses no longer emit the 'thread_id' field or the 'x-ms-thread-id' header.",
        DeprecationWarning,
        stacklevel=2,
    )
    return value


# Polling configuration
DEFAULT_MAX_POLL_RETRIES: int = 30
DEFAULT_POLL_INTERVAL_SECONDS: float = 1.0


# =============================================================================
# JSON Field Name Constants for Durable Agent State Serialization
# =============================================================================
# These constants ensure consistent camelCase field names in JSON serialization.
# Use these in both to_dict() and from_dict() methods to prevent mismatches.

# NOTE: Changing these constants is a breaking change and might require a schema version bump.


class DurableStateFields:
    """JSON field name constants for durable agent state serialization.

    All field names are in camelCase to match the JSON schema.
    Use these constants in both to_dict() and from_dict() methods.
    """

    # Schema-level fields
    SCHEMA_VERSION: Final[str] = "schemaVersion"
    DATA: Final[str] = "data"

    # Entry discriminator
    TYPE_DISCRIMINATOR: Final[str] = "$type"

    # Internal field names
    JSON_TYPE: Final[str] = "json_type"
    TYPE_INTERNAL: Final[str] = "type"

    # Common entry fields
    CORRELATION_ID: Final[str] = "correlationId"
    CREATED_AT: Final[str] = "createdAt"
    MESSAGES: Final[str] = "messages"
    EXTENSION_DATA: Final[str] = "extensionData"

    # Request-specific fields
    RESPONSE_TYPE: Final[str] = "responseType"
    RESPONSE_SCHEMA: Final[str] = "responseSchema"
    ORCHESTRATION_ID: Final[str] = "orchestrationId"

    # Response-specific fields
    USAGE: Final[str] = "usage"

    # Message fields
    ROLE: Final[str] = "role"
    CONTENTS: Final[str] = "contents"
    AUTHOR_NAME: Final[str] = "authorName"

    # Content fields
    TEXT: Final[str] = "text"
    URI: Final[str] = "uri"
    MEDIA_TYPE: Final[str] = "mediaType"
    MESSAGE: Final[str] = "message"
    ERROR_CODE: Final[str] = "errorCode"
    DETAILS: Final[str] = "details"
    CALL_ID: Final[str] = "callId"
    NAME: Final[str] = "name"
    ARGUMENTS: Final[str] = "arguments"
    RESULT: Final[str] = "result"
    FILE_ID: Final[str] = "fileId"
    VECTOR_STORE_ID: Final[str] = "vectorStoreId"
    CONTENT: Final[str] = "content"

    # Usage fields (noqa: S105 - these are JSON field names, not passwords)
    INPUT_TOKEN_COUNT: Final[str] = "inputTokenCount"  # noqa: S105
    OUTPUT_TOKEN_COUNT: Final[str] = "outputTokenCount"  # noqa: S105
    TOTAL_TOKEN_COUNT: Final[str] = "totalTokenCount"  # noqa: S105

    # History field
    CONVERSATION_HISTORY: Final[str] = "conversationHistory"

    # Stable per-message identity (used for compaction reconciliation and idempotency)
    MESSAGE_ID: Final[str] = "messageId"

    # Service-issued conversation id, for agents whose provider stores history server-side
    SERVICE_SESSION_ID: Final[str] = "serviceSessionId"


class ContentTypes:
    """Content type discriminator values for the $type field.

    These values are used in the JSON $type field to identify content types.
    """

    TEXT: Final[str] = "text"
    DATA: Final[str] = "data"
    ERROR: Final[str] = "error"
    FUNCTION_CALL: Final[str] = "functionCall"
    FUNCTION_RESULT: Final[str] = "functionResult"
    HOSTED_FILE: Final[str] = "hostedFile"
    HOSTED_VECTOR_STORE: Final[str] = "hostedVectorStore"
    REASONING: Final[str] = "reasoning"
    URI: Final[str] = "uri"
    USAGE: Final[str] = "usage"
    UNKNOWN: Final[str] = "unknown"


class ApiResponseFields:
    """Field names for HTTP API responses (not part of persisted schema).

    These are used in try_get_agent_response() for backward compatibility
    with the HTTP API response format.
    """

    CONTENT: Final[str] = "content"
    MESSAGE_COUNT: Final[str] = "message_count"
    CORRELATION_ID: Final[str] = "correlationId"
