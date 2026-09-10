# Copyright (c) Microsoft. All rights reserved.

"""Durable agent state management conforming to the durable-agent-entity-state.json schema.

This module provides classes for managing conversation state in Azure Durable Functions agents.
It implements the versioned schema that defines how agent conversations are persisted and restored
across invocations, enabling stateful, long-running agent sessions.

The module includes:
- DurableAgentState: Root state container with schema version and conversation history
- DurableAgentStateEntry and subclasses: Request and response entries in conversation history
- DurableAgentStateMessage: Individual messages with role, content items, and metadata
- Content type classes: Specialized types for text, function calls, errors, and other content
- Serialization/deserialization: Conversion between Python objects and JSON schema format

The state structure follows this hierarchy:
    DurableAgentState
    └── DurableAgentStateData
        └── conversationHistory: List[DurableAgentStateEntry]
            ├── DurableAgentStateRequest (user/system messages)
            └── DurableAgentStateResponse (assistant messages with usage stats)
                └── messages: List[DurableAgentStateMessage]
                    └── contents: List[DurableAgentStateContent subclasses]

All classes support bidirectional conversion between:
- Durable state format (JSON with camelCase, $type discriminators)
- Agent framework objects (Python objects with snake_case)
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import MutableMapping
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, ClassVar, cast

from agent_framework import (
    AgentResponse,
    Content,
    Message,
    UsageDetails,
)
from dateutil import parser as date_parser

from ._constants import ContentTypes, DurableStateFields
from ._message_identity import message_identity
from ._models import RunRequest, serialize_response_format
from ._response_utils import load_agent_response, serialize_agent_response

logger = logging.getLogger("agent_framework.durabletask")


def _validate_delivery_layout(data: dict[str, Any]) -> None:
    """Reject known alternate completion authorities, even with the same version label.

    These top-level data fields describe a different proposed delivery contract.
    Preserving them as extensions while treating their requests as incomplete would
    permit duplicate execution. This is rejection, not migration or schema agreement.
    Unrelated metadata, including nested occurrences of these names, stays opaque.
    """
    if "terminalResults" in data or "completionReceipts" in data:
        raise ValueError(
            "The durable agent state contains an incompatible delivery layout. "
            "This prototype requires responseMailbox/completedCorrelations semantics; "
            "a matching schemaVersion does not authorize interpreting another completion format."
        )


def _validate_json(value: Any) -> None:
    """Reject non-JSON values before the encoder can normalize them or collide keys."""
    if isinstance(value, dict):
        for key, item in cast(dict[Any, Any], value).items():
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings.")
            _validate_json(item)
    elif isinstance(value, list):
        for item in cast(list[Any], value):
            _validate_json(item)
    elif value is not None and not isinstance(value, (str, bool, int, float)):
        raise ValueError("Values must contain only JSON objects, arrays and primitives.")


def _json_snapshot(value: Any) -> Any:
    """Detach strict JSON without normalizing non-string keys or non-JSON containers."""
    try:
        _validate_json(value)
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("State must be strict JSON with string keys and finite numbers.") from exc


def _parse_delivery_timestamp(value: Any) -> datetime:
    """Parse offset-bearing RFC 3339 timestamps, including Z on Python 3.10."""
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
        r"(?:\.[0-9]+)?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])",
        value,
    ):
        raise ValueError("Delivery timestamps must be RFC 3339 strings with an explicit offset.")
    return datetime.fromisoformat(value[:-1] + "+00:00" if value[-1:] in ("Z", "z") else value)


def _array_field(data: dict[str, Any], name: str) -> list[Any]:
    value = data.get(name, [])
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array.")
    return cast(list[Any], value)


def _validate_core_message(data: Any) -> None:
    if not isinstance(data, dict):
        raise ValueError("Core messages must be objects.")
    for content in _array_field(cast(dict[str, Any], data), "contents"):
        if not isinstance(content, dict):
            raise ValueError("Core contents must be objects with a non-empty type.")
        content_type = cast(dict[str, Any], content).get("type")
        if not isinstance(content_type, str) or not content_type:
            raise ValueError("Core contents must be objects with a non-empty type.")


def _entry_unknown_fields(entry: DurableAgentStateEntry, data: dict[str, Any]) -> dict[str, Any]:
    known = {
        DurableStateFields.TYPE_DISCRIMINATOR,
        DurableStateFields.JSON_TYPE,
        DurableStateFields.CORRELATION_ID,
        DurableStateFields.CREATED_AT,
        DurableStateFields.MESSAGES,
        DurableStateFields.EXTENSION_DATA,
    }
    if isinstance(entry, DurableAgentStateRequest):
        known.update((
            DurableStateFields.ORCHESTRATION_ID,
            DurableStateFields.RESPONSE_TYPE,
            DurableStateFields.RESPONSE_SCHEMA,
        ))
    elif isinstance(entry, DurableAgentStateResponse):
        known.add(DurableStateFields.USAGE)
    return {key: deepcopy(value) for key, value in data.items() if key not in known}


class DurableAgentStateEntryJsonType(str, Enum):
    """Enum for conversation history entry types.

    Discriminator values for the $type field in DurableAgentStateEntry objects.

    The type is what decides who may read an entry, rather than a flag alongside it. A flag has to
    survive serialization to mean anything, and one that did not was how a failed turn came back as
    ordinary assistant context after a cold start.

    ``errorResponse`` and ``compaction`` are opposites. A failed turn is worth returning to the
    caller that is waiting for it but must never be replayed to the model. A compaction summary is
    the reverse: it belongs in the model's transcript and must never be handed back as something
    the agent said.
    """

    REQUEST = "request"
    RESPONSE = "response"
    ERROR_RESPONSE = "errorResponse"
    COMPACTION = "compaction"


def _parse_created_at(value: Any) -> datetime:
    """Normalize created_at values coming from persisted durable state."""
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        try:
            parsed = date_parser.parse(value)
            if isinstance(parsed, datetime):
                return parsed
        except (ValueError, TypeError):
            pass

    logger.warning(
        f"Invalid or missing created_at value in durable agent state; defaulting to current UTC time, {value}",
        stack_info=True,
    )
    return datetime.now(tz=timezone.utc)


def _parse_messages(data: dict[str, Any]) -> list[DurableAgentStateMessage]:
    """Parse messages from a dictionary, converting dicts to DurableAgentStateMessage objects.

    Args:
        data: Dictionary containing a 'messages' key with a list of message data

    Returns:
        List of DurableAgentStateMessage objects
    """
    messages: list[DurableAgentStateMessage] = []
    raw_messages = _array_field(data, DurableStateFields.MESSAGES)
    for raw_msg in raw_messages:
        if isinstance(raw_msg, dict):
            messages.append(DurableAgentStateMessage.from_dict(cast(dict[str, Any], raw_msg)))
        elif isinstance(raw_msg, DurableAgentStateMessage):
            messages.append(raw_msg)
        else:
            raise ValueError("messages must contain message objects.")
    return messages


def _parse_history_entries(data_dict: dict[str, Any]) -> list[DurableAgentStateEntry]:
    """Parse conversation history entries from a dictionary.

    Args:
        data_dict: Dictionary containing a 'conversationHistory' key with a list of entry data

    Returns:
        List of DurableAgentStateEntry objects (requests and responses)
    """
    history_data = _array_field(data_dict, DurableStateFields.CONVERSATION_HISTORY)
    deserialized_history: list[DurableAgentStateEntry] = []
    for raw_entry in history_data:
        if isinstance(raw_entry, dict):
            entry_dict = cast(dict[str, Any], raw_entry)
            entry_type = entry_dict.get(DurableStateFields.TYPE_DISCRIMINATOR) or entry_dict.get(
                DurableStateFields.JSON_TYPE
            )
            if not isinstance(entry_type, str) or not entry_type:
                raise ValueError("Conversation entries require a non-empty type discriminator.")
            if entry_type == DurableAgentStateEntryJsonType.RESPONSE:
                deserialized_history.append(DurableAgentStateResponse.from_dict(entry_dict))
            elif entry_type == DurableAgentStateEntryJsonType.ERROR_RESPONSE:
                deserialized_history.append(DurableAgentStateErrorResponse.from_dict(entry_dict))
            elif entry_type == DurableAgentStateEntryJsonType.COMPACTION:
                deserialized_history.append(DurableAgentStateCompaction.from_dict(entry_dict))
            elif entry_type == DurableAgentStateEntryJsonType.REQUEST:
                deserialized_history.append(DurableAgentStateRequest.from_dict(entry_dict))
            else:
                deserialized_history.append(DurableAgentStateUnknownEntry(entry_dict))
            entry = deserialized_history[-1]
            entry.unknown_fields = _entry_unknown_fields(entry, entry_dict)
        elif isinstance(raw_entry, DurableAgentStateEntry):
            deserialized_history.append(raw_entry)
        else:
            raise ValueError("conversationHistory must contain entry objects.")
    return deserialized_history


def _parse_contents(data: dict[str, Any]) -> list[DurableAgentStateContent]:
    """Parse content items from a dictionary.

    Args:
        data: Dictionary containing a 'contents' key with a list of content data

    Returns:
        List of DurableAgentStateContent objects
    """
    contents: list[DurableAgentStateContent] = []
    raw_contents = _array_field(data, DurableStateFields.CONTENTS)
    for raw_content in raw_contents:
        if isinstance(raw_content, DurableAgentStateContent):
            contents.append(raw_content)

        elif isinstance(raw_content, dict):
            content_dict = cast(dict[str, Any], raw_content)
            content_type: str | None = content_dict.get(DurableStateFields.TYPE_DISCRIMINATOR)

            match content_type:
                case ContentTypes.TEXT:
                    contents.append(DurableAgentStateTextContent(text=content_dict.get(DurableStateFields.TEXT)))

                case ContentTypes.DATA:
                    contents.append(
                        DurableAgentStateDataContent(
                            uri=str(content_dict.get(DurableStateFields.URI, "")),
                            media_type=content_dict.get(DurableStateFields.MEDIA_TYPE),
                        )
                    )

                case ContentTypes.ERROR:
                    contents.append(
                        DurableAgentStateErrorContent(
                            message=content_dict.get(DurableStateFields.MESSAGE),
                            error_code=content_dict.get(DurableStateFields.ERROR_CODE),
                            details=content_dict.get(DurableStateFields.DETAILS),
                        )
                    )

                case ContentTypes.FUNCTION_CALL:
                    contents.append(
                        DurableAgentStateFunctionCallContent(
                            call_id=str(content_dict.get(DurableStateFields.CALL_ID, "")),
                            name=str(content_dict.get(DurableStateFields.NAME, "")),
                            arguments=content_dict.get(DurableStateFields.ARGUMENTS),
                        )
                    )

                case ContentTypes.FUNCTION_RESULT:
                    contents.append(
                        DurableAgentStateFunctionResultContent(
                            call_id=str(content_dict.get(DurableStateFields.CALL_ID, "")),
                            result=content_dict.get(DurableStateFields.RESULT),
                        )
                    )

                case ContentTypes.HOSTED_FILE:
                    contents.append(
                        DurableAgentStateHostedFileContent(
                            file_id=str(content_dict.get(DurableStateFields.FILE_ID, ""))
                        )
                    )

                case ContentTypes.HOSTED_VECTOR_STORE:
                    contents.append(
                        DurableAgentStateHostedVectorStoreContent(
                            vector_store_id=str(content_dict.get(DurableStateFields.VECTOR_STORE_ID, ""))
                        )
                    )

                case ContentTypes.REASONING:
                    contents.append(
                        DurableAgentStateTextReasoningContent(text=content_dict.get(DurableStateFields.TEXT))
                    )

                case ContentTypes.URI:
                    contents.append(
                        DurableAgentStateUriContent(
                            uri=str(content_dict.get(DurableStateFields.URI, "")),
                            media_type=content_dict.get(DurableStateFields.MEDIA_TYPE),
                        )
                    )

                case ContentTypes.USAGE:
                    usage_data = content_dict.get(DurableStateFields.USAGE)
                    if isinstance(usage_data, dict):
                        contents.append(
                            DurableAgentStateUsageContent(
                                usage=DurableAgentStateUsage.from_dict(cast(dict[str, Any], usage_data))
                            )
                        )
                    else:
                        raise ValueError("Usage content requires a usage object.")

                case ContentTypes.UNKNOWN:
                    contents.append(
                        DurableAgentStateUnknownContent(content=content_dict.get(DurableStateFields.CONTENT, {}))
                    )
                case _:
                    if not isinstance(content_type, str) or not content_type:
                        raise ValueError("Content requires a non-empty $type discriminator.")
                    contents.append(DurableAgentStateRawContent(content_dict))

            content = contents[-1]
            known = content.to_dict().keys() | {DurableStateFields.EXTENSION_DATA}
            content.unknown_fields = {key: deepcopy(value) for key, value in content_dict.items() if key not in known}
            content.extensionData = deepcopy(content_dict.get(DurableStateFields.EXTENSION_DATA))
        else:
            raise ValueError("contents must contain content objects.")

    return contents


class DurableAgentStateContent:
    """Base class for all content types in durable agent state messages.

    This abstract base class defines the interface for content items that can be
    stored in conversation history. Content types include text, function calls,
    function results, errors, and other specialized content types defined by the
    agent framework.

    Subclasses must implement to_dict() and to_ai_content() to handle conversion
    between the durable state representation and the agent framework's content objects.

    Attributes:
        extensionData: Optional metadata, including unmapped canonical core fields.
    """

    extensionData: dict[str, Any] | None = None
    unknown_fields: dict[str, Any] | None = None
    type: str = ""

    _NULLABLE_FIELDS: ClassVar[frozenset[str]] = frozenset()

    def to_persisted_dict(self) -> dict[str, Any]:
        """Merge opaque fields without replacing mutable, known transcript fields."""
        result = {
            **(self.unknown_fields or {}),
            **{
                key: value for key, value in self.to_dict().items() if value is not None or key in self._NULLABLE_FIELDS
            },
        }
        if self.extensionData is not None:
            result[DurableStateFields.EXTENSION_DATA] = self.extensionData
        return _json_snapshot(result)

    def core_projection(self) -> dict[str, Any]:
        """Map this subtype's durable fields to canonical core content fields.

        Returns:
            Core field names and current values, including the content type.
        """
        # Only map fields owned by this subtype, not a global union of content fields.
        aliases = {"details": "error_details", "usage": "usage_details"}
        fields = {
            aliases.get(key, re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()): value
            for key, value in self.to_dict().items()
            if key != DurableStateFields.TYPE_DISCRIMINATOR
        }
        if isinstance(self, DurableAgentStateUsageContent):
            fields["usage_details"] = self.usage.to_usage_details()
        fields["type"] = (
            "text_reasoning"
            if self.type == ContentTypes.REASONING
            else re.sub(r"(?<!^)(?=[A-Z])", "_", self.type).lower()
        )
        return fields

    def to_core_content(self) -> Content:
        """Restore canonical fields through the delivery loader, without dynamic type lookup."""
        extra = (self.extensionData or {}).get("coreContent")
        if not isinstance(extra, dict):
            return self.to_ai_content()
        # The overlay contains extras only. Current text/result/arguments always win.
        payload = {**deepcopy(cast(dict[str, Any], extra)), **self.core_projection()}
        return load_agent_response({"messages": [{"role": "assistant", "contents": [payload]}]}).messages[0].contents[0]

    def to_dict(self) -> dict[str, Any]:
        """Serialize this content to a dictionary for JSON storage.

        Returns:
            Dictionary representation including $type discriminator and content-specific fields

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError

    def to_ai_content(self) -> Any:
        """Convert this durable state content back to an agent framework content object.

        Returns:
            An agent framework content object (Content of type `text`, `function_call`, etc.)

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError

    @staticmethod
    def from_ai_content(content: Any) -> DurableAgentStateContent:
        """Keep typed durable fields and persist only core fields they cannot represent.

        Args:
            content: Core content to convert, or an unknown value to wrap as opaque content.

        Returns:
            Durable content with canonical fields not owned by its subtype stored as metadata.
        """
        stored = DurableAgentStateContent._from_ai_content(content)
        if isinstance(content, Content) and not isinstance(stored, DurableAgentStateUnknownContent):
            payload = _json_snapshot(content.to_dict())
            mapped = stored.core_projection()
            # An empty overlay still identifies canonical rather than legacy conversion.
            stored.extensionData = {"coreContent": {key: value for key, value in payload.items() if key not in mapped}}
        return stored

    @staticmethod
    def _from_ai_content(content: Any) -> DurableAgentStateContent:
        """Create a durable state content object from an agent framework content object.

        This factory method maps agent framework content types to their corresponding durable state representations.
        Unknown content types are wrapped in DurableAgentStateUnknownContent.

        Args:
            content: An agent framework content object (Content of type `text`, `function_call`, etc.)

        Returns:
            The corresponding DurableAgentStateContent subclass instance
        """
        # Map AI content type to appropriate DurableAgentStateContent subclass
        if not isinstance(content, Content):
            return DurableAgentStateUnknownContent.from_unknown_content(content)

        match content.type:
            case "data":
                return DurableAgentStateDataContent.from_data_content(content)
            case "error":
                return DurableAgentStateErrorContent.from_error_content(content)
            case "function_call":
                return DurableAgentStateFunctionCallContent.from_function_call_content(content)
            case "function_result":
                return DurableAgentStateFunctionResultContent.from_function_result_content(content)
            case "hosted_file":
                return DurableAgentStateHostedFileContent.from_hosted_file_content(content)
            case "hosted_vector_store":
                return DurableAgentStateHostedVectorStoreContent.from_hosted_vector_store_content(content)
            case "text":
                return DurableAgentStateTextContent.from_text_content(content)
            case "reasoning" | "text_reasoning":
                return DurableAgentStateTextReasoningContent.from_text_reasoning_content(content)
            case "uri":
                return DurableAgentStateUriContent.from_uri_content(content)
            case "usage":
                return DurableAgentStateUsageContent.from_usage_content(content)
            case _:
                return DurableAgentStateUnknownContent.from_unknown_content(content)


class DurableAgentStateRawContent(DurableAgentStateContent):
    """Opaque future shared-schema content, preserved without reinterpreting its fields."""

    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = deepcopy(raw)

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)

    def to_persisted_dict(self) -> dict[str, Any]:
        """Preserve even null fields belonging to an unknown content kind."""
        return _json_snapshot(self.raw)

    def to_core_content(self) -> Content:
        """Do not interpret an unknown writer's extension conventions."""
        return self.to_ai_content()

    def to_ai_content(self) -> Content:
        return Content(type="unknown", additional_properties={"content": deepcopy(self.raw)})  # type: ignore[arg-type]


# Core state classes


class DurableAgentStateData:
    """Container for the core data within durable agent state.

    This class holds the primary data structures for agent conversation state,
    including the conversation history (a sequence of request and response entries)
    and optional extension data for custom metadata.

    The data structure is nested within DurableAgentState under the "data" property,
    conforming to the durable-agent-entity-state.json schema structure.

    Attributes:
        conversation_history: Ordered list of conversation entries (requests and responses)
        session: Serialized ``AgentSession`` from the previous turn - the context provider state
            bag plus any service-issued conversation id. Core treats session state as durable
            across turns, so it is persisted here rather than discarded with the per-operation
            session.
        ingested_positions: Legacy per-producer maxima, retained for read compatibility.
            Migration requires delivery evidence because a maximum does not identify skipped positions.
        ingested_messages: Actual source identities and content fingerprints, independent of transcript pruning.
        response_mailbox: Original serializable results with their delivery expiry.
        completed_correlations: Completion evidence retained after mailbox expiry.
        truncation: What retention has removed, if anything. A log line is only visible to whoever
            was watching at the time, so the fact that this conversation is no longer complete is
            recorded in the state itself. Absent until the first eviction, so its absence is a
            positive statement that nothing has been dropped.
        extension_data: Optional dictionary for custom metadata (not part of core schema)
    """

    conversation_history: list[DurableAgentStateEntry]
    session: dict[str, Any] | None
    ingested_positions: dict[str, int] | None
    truncation: dict[str, Any] | None
    extension_data: dict[str, Any] | None
    response_mailbox: dict[str, dict[str, Any]]
    completed_correlations: dict[str, dict[str, Any]]
    ingested_messages: dict[str, list[str] | None]
    unknown_fields: dict[str, Any]

    def __init__(
        self,
        conversation_history: list[DurableAgentStateEntry] | None = None,
        extension_data: dict[str, Any] | None = None,
        session: dict[str, Any] | None = None,
        ingested_positions: dict[str, int] | None = None,
        truncation: dict[str, Any] | None = None,
        response_mailbox: dict[str, dict[str, Any]] | None = None,
        completed_correlations: dict[str, dict[str, Any]] | None = None,
        ingested_messages: dict[str, list[str] | None] | None = None,
    ) -> None:
        """Initialize the data container.

        Args:
            conversation_history: Initial conversation history (defaults to empty list)
            extension_data: Optional custom metadata
            session: Optional serialized ``AgentSession`` from the previous turn
            ingested_positions: Legacy scalar ingestion state, not exact delivery evidence.
            truncation: Record of what retention has removed, absent until something is
            response_mailbox: Original response snapshots with independent delivery expiry.
            completed_correlations: Completion evidence retained after result expiry.
            ingested_messages: Exact message fingerprints or legacy identity-only markers.
        """
        self.conversation_history = conversation_history or []
        self.extension_data = extension_data
        self.session = session
        self.ingested_positions = ingested_positions
        self.truncation = truncation
        self.response_mailbox = response_mailbox or {}
        self.completed_correlations = completed_correlations or {}
        self.ingested_messages = ingested_messages or {}
        self.unknown_fields = {}

    def to_dict(self) -> dict[str, Any]:
        _validate_delivery_layout(self.unknown_fields)
        result: dict[str, Any] = {
            **deepcopy(self.unknown_fields),
            DurableStateFields.CONVERSATION_HISTORY: [entry.to_dict() for entry in self.conversation_history],
        }
        if self.extension_data is not None:
            result[DurableStateFields.EXTENSION_DATA] = self.extension_data
        if self.session is not None:
            result[DurableStateFields.SESSION] = self.session
        if self.ingested_positions:
            result[DurableStateFields.INGESTED_POSITIONS] = self.ingested_positions
        if self.truncation:
            result[DurableStateFields.TRUNCATION] = self.truncation
        if self.response_mailbox:
            result[DurableStateFields.RESPONSE_MAILBOX] = deepcopy(self.response_mailbox)
        if self.completed_correlations:
            result[DurableStateFields.COMPLETED_CORRELATIONS] = deepcopy(self.completed_correlations)
        if self.ingested_messages:
            result[DurableStateFields.INGESTED_MESSAGES] = deepcopy(self.ingested_messages)
        return _json_snapshot(result)

    @classmethod
    def from_dict(cls, data_dict: dict[str, Any]) -> DurableAgentStateData:
        _validate_delivery_layout(data_dict)
        for name in (
            DurableStateFields.RESPONSE_MAILBOX,
            DurableStateFields.COMPLETED_CORRELATIONS,
            DurableStateFields.INGESTED_MESSAGES,
        ):
            if name in data_dict and not isinstance(data_dict[name], dict):
                raise ValueError(f"{name} must be an object.")
        result = cls(
            conversation_history=_parse_history_entries(data_dict),
            extension_data=data_dict.get(DurableStateFields.EXTENSION_DATA),
            session=data_dict.get(DurableStateFields.SESSION),
            ingested_positions=data_dict.get(DurableStateFields.INGESTED_POSITIONS),
            truncation=data_dict.get(DurableStateFields.TRUNCATION),
            response_mailbox=deepcopy(data_dict.get(DurableStateFields.RESPONSE_MAILBOX, {})),
            completed_correlations=deepcopy(data_dict.get(DurableStateFields.COMPLETED_CORRELATIONS, {})),
            ingested_messages=deepcopy(data_dict.get(DurableStateFields.INGESTED_MESSAGES, {})),
        )
        known = {
            DurableStateFields.CONVERSATION_HISTORY,
            DurableStateFields.EXTENSION_DATA,
            DurableStateFields.SESSION,
            DurableStateFields.INGESTED_POSITIONS,
            DurableStateFields.TRUNCATION,
            DurableStateFields.RESPONSE_MAILBOX,
            DurableStateFields.COMPLETED_CORRELATIONS,
            DurableStateFields.INGESTED_MESSAGES,
        }
        result.unknown_fields = {key: deepcopy(value) for key, value in data_dict.items() if key not in known}
        for name, records in (
            (DurableStateFields.RESPONSE_MAILBOX, result.response_mailbox),
            (DurableStateFields.COMPLETED_CORRELATIONS, result.completed_correlations),
        ):
            if any(not isinstance(value, dict) for value in records.values()):
                raise ValueError(f"{name} must contain objects keyed by correlation ID.")
            for correlation_id, record in records.items():
                if not isinstance(correlation_id, str) or not correlation_id:
                    raise ValueError(f"{name} requires non-empty correlation IDs.")
                timestamps = (
                    (DurableStateFields.CREATED_AT, DurableStateFields.EXPIRES_AT)
                    if name == DurableStateFields.RESPONSE_MAILBOX
                    else (DurableStateFields.COMPLETED_AT,)
                )
                for field in timestamps:
                    try:
                        _parse_delivery_timestamp(record.get(field))
                    except ValueError as exc:
                        raise ValueError(f"{name}.{field} must be an RFC 3339 timestamp with an offset.") from exc
                if name == DurableStateFields.RESPONSE_MAILBOX:
                    response = record.get(DurableStateFields.RESPONSE)
                    if not isinstance(response, dict):
                        raise ValueError("responseMailbox.response must be an inline agent response.")
                    response = cast(dict[str, Any], response)
                    if response.get("type") != "agent_response" or not isinstance(response.get("messages"), list):
                        raise ValueError("responseMailbox.response must be an inline agent response.")
                    for message in response["messages"]:
                        _validate_core_message(message)
                    load_agent_response(response)
                elif "legacy" in record and not isinstance(record["legacy"], bool):
                    raise ValueError("completedCorrelations.legacy must be a boolean.")
        if not isinstance(result.ingested_messages, dict) or any(
            values is not None and (not isinstance(values, list) or any(not isinstance(v, str) for v in values))
            for values in result.ingested_messages.values()
        ):
            raise ValueError("ingestedMessages must contain fingerprint lists or legacy identity markers.")
        return result


class DurableAgentState:
    """Manages durable agent state conforming to the durable-agent-entity-state.json schema.

    This class provides the root container for agent conversation state that can be persisted
    in Azure Durable Entities. It maintains the conversation history as a sequence of request
    and response entries, each with their messages, timestamps, and metadata.

    The state follows a versioned schema (see SCHEMA_VERSION class constant) that defines the structure for:
    - Request entries: User/system messages with optional response format specifications
    - Response entries: Assistant messages with token usage information
    - Messages: Individual chat messages with role, content items, and timestamps
    - Content items: Text, function calls, function results, errors, and other content types

    State is serialized to JSON with this structure:
    {
        "schemaVersion": "<SCHEMA_VERSION>",
        "data": {
            "conversationHistory": [
                {"$type": "request", "correlationId": "...", "createdAt": "...", "messages": [...]},
                {"$type": "response", "correlationId": "...", "createdAt": "...", "messages": [...], "usage": {...}}
            ]
        }
    }

    Attributes:
        data: Container for conversation history and optional extension data
        schema_version: Schema version string (defaults to SCHEMA_VERSION)
    """

    # New layout requires compatible workers and response consumers. A version number
    # does not make legacy .NET workers or older Python writers safe to share this state.
    SCHEMA_VERSION: str = "2.0.0"

    data: DurableAgentStateData
    schema_version: str = SCHEMA_VERSION

    def __init__(self, schema_version: str = SCHEMA_VERSION):
        """Initialize a new durable agent state.

        Args:
            schema_version: Schema version to use (defaults to SCHEMA_VERSION)
        """
        self.data = DurableAgentStateData()
        self.schema_version = schema_version
        self.unknown_fields: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        return _json_snapshot({
            **deepcopy(self.unknown_fields),
            DurableStateFields.SCHEMA_VERSION: self.schema_version,
            DurableStateFields.DATA: self.data.to_dict(),
        })

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), allow_nan=False)

    @classmethod
    def from_dict(cls, state: dict[str, Any]) -> DurableAgentState:
        """Restore state from a dictionary.

        Args:
            state: Dictionary containing schemaVersion and data (full state structure)
        """
        if not isinstance(state, dict):
            raise ValueError("The durable agent state must be a JSON object.")
        state = _json_snapshot(state)
        schema_version = state.get(DurableStateFields.SCHEMA_VERSION)
        if schema_version is None:
            raise ValueError("The durable agent state is missing schemaVersion; refusing to discard existing state.")
        if not isinstance(schema_version, str) or not re.fullmatch(r"[12]\.[0-9]+\.[0-9]+", schema_version):
            raise ValueError(f"Unsupported durable agent state schemaVersion: {schema_version!r}.")
        raw_data = state.get(DurableStateFields.DATA)
        if not isinstance(raw_data, dict):
            raise ValueError("The durable agent state data must be an object.")

        instance = cls(schema_version=schema_version)
        instance.data = DurableAgentStateData.from_dict(cast(dict[str, Any], raw_data))
        if schema_version.startswith("2.") and (
            instance.data.response_mailbox.keys() - instance.data.completed_correlations.keys()
        ):
            raise ValueError("Every responseMailbox entry requires a matching completedCorrelations receipt.")
        instance.unknown_fields = {
            key: deepcopy(value)
            for key, value in state.items()
            if key not in (DurableStateFields.SCHEMA_VERSION, DurableStateFields.DATA)
        }

        return instance

    @classmethod
    def from_json(cls, json_str: str) -> DurableAgentState:
        try:
            obj = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError("The durable agent state is not valid JSON.") from e

        if not isinstance(obj, dict):
            raise ValueError("The durable agent state must be a JSON object.")
        return cls.from_dict(cast(dict[str, Any], obj))

    @property
    def message_count(self) -> int:
        """Get the count of conversation entries (requests + responses)."""
        return len(self.data.conversation_history)

    def try_get_agent_response(self, correlation_id: str) -> AgentResponse | None:
        """Read a retained result or explicit completed status using the persisted layout.

        Version 2 never falls back to transcript responses, even after mailbox expiry.
        Version 1 retains its legacy lookup until an operation migrates the state.

        Args:
            correlation_id: Request correlation ID whose response or completion status to retrieve.

        Returns:
            Retained response, expired-response status, or None when no matching result exists.
        """
        _validate_delivery_layout(self.data.unknown_fields)
        if self.schema_version.startswith("2."):
            mailbox = self.data.response_mailbox.get(correlation_id)
            if mailbox is not None:
                expiry = _parse_delivery_timestamp(mailbox[DurableStateFields.EXPIRES_AT])
                if datetime.now(timezone.utc) < expiry:
                    return load_agent_response(mailbox[DurableStateFields.RESPONSE])
            if correlation_id in self.data.completed_correlations or mailbox is not None:
                return AgentResponse(
                    messages=[
                        Message(
                            "system",
                            [
                                Content.from_error(
                                    message="This request completed, but its response delivery window has expired.",
                                    error_code="response_expired",
                                )
                            ],
                        )
                    ],
                    additional_properties={"durable_status": "already_completed", "correlation_id": correlation_id},
                )
            return None
        for entry in self.data.conversation_history:
            if entry.correlation_id == correlation_id and isinstance(entry, DurableAgentStateResponse):
                return DurableAgentStateResponse.to_run_response(entry)

        return None

    def record_response(
        self,
        correlation_id: str,
        response: AgentResponse,
        *,
        delivery_window_seconds: int,
        now: datetime | None = None,
        legacy: bool = False,
    ) -> None:
        """Stage an independent JSON snapshot and completion receipt, without persisting them.

        Args:
            correlation_id: Request correlation ID used to key the snapshot and completion receipt.
            response: Agent response to snapshot for delivery.
            delivery_window_seconds: Seconds after the recording timestamp when the snapshot expires.
            now: Offset-aware recording timestamp, defaulting to the current UTC time.
            legacy: Whether the completion receipt represents a migrated legacy response.
        """
        if correlation_id in self.data.completed_correlations:
            return
        timestamp = now or datetime.now(timezone.utc)
        _parse_delivery_timestamp(timestamp.isoformat())
        payload = _json_snapshot(serialize_agent_response(response))
        self.data.response_mailbox[correlation_id] = {
            DurableStateFields.RESPONSE: payload,
            DurableStateFields.CREATED_AT: timestamp.isoformat(),
            DurableStateFields.EXPIRES_AT: (timestamp + timedelta(seconds=delivery_window_seconds)).isoformat(),
        }
        self.data.completed_correlations[correlation_id] = {
            DurableStateFields.COMPLETED_AT: timestamp.isoformat(),
            **({"legacy": True} if legacy else {}),
        }

    def expire_responses(self, *, now: datetime | None = None) -> None:
        """Expire result payloads only; completion evidence lives until entity deletion.

        Args:
            now: Offset-aware expiry-check timestamp, defaulting to the current UTC time.
        """
        timestamp = now or datetime.now(timezone.utc)
        for correlation_id, mailbox in list(self.data.response_mailbox.items()):
            expiry = _parse_delivery_timestamp(mailbox[DurableStateFields.EXPIRES_AT])
            if timestamp >= expiry:
                del self.data.response_mailbox[correlation_id]

    def prepare_for_write(self, *, delivery_window_seconds: int) -> None:
        """Admit only the revised writer layout, without silently upgrading legacy state.

        Args:
            delivery_window_seconds: Retained for source compatibility; migration now
                requires an explicit destination operation, including its grace policy.
        """
        _validate_delivery_layout(self.data.unknown_fields)
        if self.schema_version == self.SCHEMA_VERSION:
            return
        if re.fullmatch(r"1\.[0-9]+\.[0-9]+", self.schema_version) is None:
            raise ValueError(
                f"Unsupported durable agent state schemaVersion for writing: {self.schema_version!r}. "
                f"Only {self.SCHEMA_VERSION} is writable."
            )
        raise ValueError(
            "Legacy state is read-only in this runtime. Keep it on its original deployment or use explicit "
            "migration into a separate isolated-v2 entity. Legacy ingestedPositions require recorded delivery evidence."
        )


class DurableAgentStateEntry:
    """Base class for conversation history entries (requests and responses).

    This class represents a single entry in the conversation history. Each entry can be
    either a request (user/system messages sent to the agent) or a response (assistant
    messages from the agent). The $type discriminator field determines which type of entry
    it represents.

    Entries are linked together using correlation IDs, allowing responses to be matched
    with their originating requests.

    Common Attributes:
        json_type: Discriminator for entry type ("request", "response", "errorResponse" or
            "compaction")
        correlationId: Unique identifier linking requests and responses. Absent on compaction
            entries, which answer no request.
        created_at: Timestamp when the entry was created
        messages: List of messages in this entry
        extensionData: Optional additional metadata (not serialized per schema)

    Request-only Attributes:
        responseType: Expected response type ("text" or "json") - only for request entries
        responseSchema: JSON schema for structured responses - only for request entries

    Response-only Attributes:
        usage: Token usage statistics - only for response entries
    """

    json_type: DurableAgentStateEntryJsonType | str
    correlation_id: str | None
    created_at: datetime
    messages: list[DurableAgentStateMessage]
    extension_data: dict[str, Any] | None

    def __init__(
        self,
        json_type: DurableAgentStateEntryJsonType | str,
        correlation_id: str | None,
        created_at: datetime,
        messages: list[DurableAgentStateMessage],
        extension_data: dict[str, Any] | None = None,
    ) -> None:
        self.json_type = json_type
        self.correlation_id = correlation_id
        self.created_at = created_at
        self.messages = messages
        self.extension_data = extension_data
        self.unknown_fields: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            **deepcopy(self.unknown_fields),
            DurableStateFields.TYPE_DISCRIMINATOR: self.json_type,
            DurableStateFields.CREATED_AT: self.created_at.isoformat(),
            DurableStateFields.MESSAGES: [m.to_dict() for m in self.messages],
        }
        if self.correlation_id is not None:
            # Omitted rather than written as null. A compaction entry answers no request and so has
            # no correlation, and "absent" says that where an explicit null only says the field
            # exists and is empty. It also keeps the persisted shape a string wherever it appears,
            # which is what the schema and the .NET reader both expect.
            result[DurableStateFields.CORRELATION_ID] = self.correlation_id
        if self.extension_data is not None:
            result[DurableStateFields.EXTENSION_DATA] = deepcopy(self.extension_data)
        return _json_snapshot(result)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateEntry:
        created_at = _parse_created_at(data.get(DurableStateFields.CREATED_AT))
        messages = _parse_messages(data)

        entry = cls(
            json_type=DurableAgentStateEntryJsonType(data.get(DurableStateFields.TYPE_DISCRIMINATOR)),
            correlation_id=data.get(DurableStateFields.CORRELATION_ID),
            created_at=created_at,
            messages=messages,
            extension_data=data.get(DurableStateFields.EXTENSION_DATA),
        )
        entry.unknown_fields = _entry_unknown_fields(entry, data)
        return entry


class DurableAgentStateUnknownEntry(DurableAgentStateEntry):
    """Opaque future entry preserved for round-trip, never converted into model context."""

    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = deepcopy(raw)
        super().__init__(
            json_type=str(raw.get(DurableStateFields.TYPE_DISCRIMINATOR, "unknown")),
            correlation_id=raw.get(DurableStateFields.CORRELATION_ID),
            created_at=datetime.min.replace(tzinfo=timezone.utc),
            messages=[],
        )

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)


class DurableAgentStateRequest(DurableAgentStateEntry):
    """Represents a request entry in the durable agent conversation history.

    A request entry captures a user or system message sent to the agent, along with
    optional response format specifications. Each request is stored as a separate
    entry in the conversation history with a unique correlation ID.

    Attributes:
        response_type: Expected response type ("text" or "json")
        response_schema: JSON schema for structured responses (when response_type is "json")
        orchestration_id: ID of the orchestration that initiated this request (if any)
        correlationId: Unique identifier linking this request to its response
        created_at: Timestamp when the request was created
        messages: List of messages included in this request
        json_type: Always "request" for this class
    """

    response_type: str | None = None
    response_schema: dict[str, Any] | None = None
    orchestration_id: str | None = None

    def __init__(
        self,
        correlation_id: str | None,
        created_at: datetime,
        messages: list[DurableAgentStateMessage],
        extension_data: dict[str, Any] | None = None,
        response_type: str | None = None,
        response_schema: dict[str, Any] | None = None,
        orchestration_id: str | None = None,
    ) -> None:
        super().__init__(
            json_type=DurableAgentStateEntryJsonType.REQUEST,
            correlation_id=correlation_id,
            created_at=created_at,
            messages=messages,
            extension_data=extension_data,
        )
        self.response_type = response_type
        self.response_schema = response_schema
        self.orchestration_id = orchestration_id

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        if self.orchestration_id is not None:
            data[DurableStateFields.ORCHESTRATION_ID] = self.orchestration_id
        if self.response_type is not None:
            data[DurableStateFields.RESPONSE_TYPE] = self.response_type
        if self.response_schema is not None:
            data[DurableStateFields.RESPONSE_SCHEMA] = deepcopy(self.response_schema)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateRequest:
        created_at = _parse_created_at(data.get(DurableStateFields.CREATED_AT))
        messages = _parse_messages(data)

        entry = cls(
            correlation_id=data.get(DurableStateFields.CORRELATION_ID),
            created_at=created_at,
            messages=messages,
            extension_data=data.get(DurableStateFields.EXTENSION_DATA),
            response_type=data.get(DurableStateFields.RESPONSE_TYPE),
            response_schema=data.get(DurableStateFields.RESPONSE_SCHEMA),
            orchestration_id=data.get(DurableStateFields.ORCHESTRATION_ID),
        )
        entry.unknown_fields = _entry_unknown_fields(entry, data)
        return entry

    @staticmethod
    def from_run_request(request: RunRequest) -> DurableAgentStateRequest:
        # A workflow may deliver the upstream conversation instead of a single message.
        if request.context_messages is not None:
            messages = [DurableAgentStateMessage.from_core_dict(raw) for raw in request.context_messages]
        else:
            messages = [DurableAgentStateMessage.from_run_request(request)]

        # Determine response_type based on response_format
        return DurableAgentStateRequest(
            correlation_id=request.correlation_id,
            messages=messages,
            created_at=_parse_created_at(request.created_at),
            response_type=request.request_response_format,
            response_schema=serialize_response_format(request.response_format),
            orchestration_id=request.orchestration_id,
        )


class DurableAgentStateResponse(DurableAgentStateEntry):
    """Represents a response entry in the durable agent conversation history.

    A response entry captures the agent's reply to a user request, including any
    assistant messages, tool calls, and token usage information. Each response is
    linked to its originating request via a correlation ID.

    Attributes:
        usage: Token usage statistics for this response (input, output, and total tokens)
        correlation_id: Unique identifier linking this response to its request
        created_at: Timestamp when the response was created
        messages: List of assistant messages in this response
        json_type: "response", or "errorResponse" for the failed-turn subclass
    """

    JSON_TYPE: ClassVar[DurableAgentStateEntryJsonType] = DurableAgentStateEntryJsonType.RESPONSE

    usage: DurableAgentStateUsage | None = None

    def __init__(
        self,
        correlation_id: str | None,
        created_at: datetime,
        messages: list[DurableAgentStateMessage],
        extension_data: dict[str, Any] | None = None,
        usage: DurableAgentStateUsage | None = None,
    ) -> None:
        super().__init__(
            json_type=type(self).JSON_TYPE,
            correlation_id=correlation_id,
            created_at=created_at,
            messages=messages,
            extension_data=extension_data,
        )
        self.usage = usage

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        if self.usage is not None:
            data[DurableStateFields.USAGE] = self.usage.to_dict()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateResponse:
        created_at = _parse_created_at(data.get(DurableStateFields.CREATED_AT))
        messages = _parse_messages(data)

        usage_dict = data.get(DurableStateFields.USAGE)
        usage: DurableAgentStateUsage | None = None
        if isinstance(usage_dict, dict):
            usage = DurableAgentStateUsage.from_dict(cast(dict[str, Any], usage_dict))
        elif usage_dict is not None:
            raise ValueError("Response usage must be an object.")

        entry = cls(
            correlation_id=data.get(DurableStateFields.CORRELATION_ID),
            created_at=created_at,
            messages=messages,
            extension_data=data.get(DurableStateFields.EXTENSION_DATA),
            usage=usage,
        )
        entry.unknown_fields = _entry_unknown_fields(entry, data)
        return entry

    @classmethod
    def from_run_response(cls, correlation_id: str, response: AgentResponse) -> DurableAgentStateResponse:
        """Creates a response entry of this class from an AgentResponse.

        A classmethod rather than a staticmethod so the error subclass produces an error entry
        without the caller having to set anything afterwards.
        """
        return cls(
            correlation_id=correlation_id,
            created_at=_parse_created_at(response.created_at),
            messages=[DurableAgentStateMessage.from_chat_message(m) for m in response.messages],
            usage=DurableAgentStateUsage.from_usage(response.usage_details),
        )

    @staticmethod
    def to_run_response(
        response_entry: DurableAgentStateResponse,
    ) -> AgentResponse:
        """Converts a DurableAgentStateResponse back to an AgentResponse."""
        messages = [m.to_chat_message() for m in response_entry.messages]

        usage_details = response_entry.usage.to_usage_details() if response_entry.usage is not None else UsageDetails()

        return AgentResponse(
            created_at=response_entry.created_at.isoformat(),
            messages=messages,
            usage_details=usage_details,
            additional_properties=(
                {"durable_status": "error"} if isinstance(response_entry, DurableAgentStateErrorResponse) else None
            ),
        )


class DurableAgentStateErrorResponse(DurableAgentStateResponse):
    """A turn that failed, recorded so the waiting caller can be told why.

    Deliberately a response, because a caller polling its correlation id still needs an answer and
    an error is the answer. Deliberately not replayable, because the reason a turn failed is for
    the caller, not for the model, and feeding it back would present an exception as something the
    assistant said.

    That second part used to be a boolean on the response, which was never serialized. The failure
    survived a reload looking like an ordinary reply. Being a distinct type means the distinction
    cannot be lost in transit.

    Not to be confused with ``DurableAgentStateErrorContent``, which is error content inside a
    single message. This is the entry recording that a whole turn failed.
    """

    JSON_TYPE: ClassVar[DurableAgentStateEntryJsonType] = DurableAgentStateEntryJsonType.ERROR_RESPONSE


class DurableAgentStateCompaction(DurableAgentStateEntry):
    """A message compaction produced, such as a summary standing in for turns it replaced.

    The exact opposite of an error entry. It belongs to the model's transcript and takes its place
    in conversation order, but it answers no request, so it is not a response and can never be
    returned to a caller polling for one. Previously these were inserted into whichever entry they
    followed, which meant a poll could hand back a summary alongside the real answer.
    """

    def __init__(
        self,
        created_at: datetime,
        messages: list[DurableAgentStateMessage],
        correlation_id: str | None = None,
        extension_data: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            json_type=DurableAgentStateEntryJsonType.COMPACTION,
            correlation_id=correlation_id,
            created_at=created_at,
            messages=messages,
            extension_data=extension_data,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateCompaction:
        entry = cls(
            created_at=_parse_created_at(data.get(DurableStateFields.CREATED_AT)),
            messages=_parse_messages(data),
            correlation_id=data.get(DurableStateFields.CORRELATION_ID),
            extension_data=data.get(DurableStateFields.EXTENSION_DATA),
        )
        entry.unknown_fields = _entry_unknown_fields(entry, data)
        return entry


class DurableAgentStateMessage:
    """Represents a message within a conversation history entry.

    A message contains the role (user, assistant, system), content items (text, function calls,
    tool results, etc.), and optional metadata. Messages are the building blocks of both
    request and response entries in the conversation history.

    Attributes:
        role: The sender role ("user", "assistant", or "system")
        contents: List of content items (text, function calls, errors, etc.)
        author_name: Optional name of the message author (typically set for assistant messages)
        created_at: Optional timestamp when the message was created
        message_id: Optional stable identifier for the message. Persisted so context-management
            state (for example compaction summaries that reference the messages they replace)
            can be reconciled across entity operations.
        extension_data: Optional additional metadata. Carries a message's
            ``additional_properties``, including compaction annotations, so that context
            management state survives across entity operations.
    """

    role: str
    contents: list[DurableAgentStateContent]
    author_name: str | None = None
    created_at: datetime | None = None
    message_id: str | None = None
    extension_data: dict[str, Any] | None = None
    ingestion_identity: str | None = None
    ingestion_occurrence: str | None = None

    def __init__(
        self,
        role: str,
        contents: list[DurableAgentStateContent],
        author_name: str | None = None,
        created_at: datetime | None = None,
        extension_data: dict[str, Any] | None = None,
        message_id: str | None = None,
    ) -> None:
        self.role = role
        self.contents = contents
        self.author_name = author_name
        self.created_at = created_at
        self.message_id = message_id
        self.extension_data = extension_data
        self.unknown_fields: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            **deepcopy(self.unknown_fields),
            DurableStateFields.ROLE: self.role,
            DurableStateFields.CONTENTS: [c.to_persisted_dict() for c in self.contents],
        }
        # Only include optional fields if they have values
        if self.created_at is not None:
            result[DurableStateFields.CREATED_AT] = self.created_at.isoformat()
        if self.author_name is not None:
            result[DurableStateFields.AUTHOR_NAME] = self.author_name
        if self.message_id is not None:
            result[DurableStateFields.MESSAGE_ID] = self.message_id
        if self.extension_data is not None:
            result[DurableStateFields.EXTENSION_DATA] = self.extension_data
        return _json_snapshot(result)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateMessage:
        data_created_at = data.get(DurableStateFields.CREATED_AT)
        created_at = _parse_created_at(data_created_at) if data_created_at else None

        message = cls(
            role=data.get(DurableStateFields.ROLE, ""),
            contents=_parse_contents(data),
            author_name=data.get(DurableStateFields.AUTHOR_NAME),
            created_at=created_at,
            message_id=data.get(DurableStateFields.MESSAGE_ID),
            extension_data=data.get(DurableStateFields.EXTENSION_DATA),
        )
        known = {
            DurableStateFields.ROLE,
            DurableStateFields.CONTENTS,
            DurableStateFields.AUTHOR_NAME,
            DurableStateFields.CREATED_AT,
            DurableStateFields.MESSAGE_ID,
            DurableStateFields.EXTENSION_DATA,
        }
        message.unknown_fields = {key: deepcopy(value) for key, value in data.items() if key not in known}
        return message

    @property
    def text(self) -> str:
        """Extract text from the contents list."""
        text_parts: list[str] = []
        for content in self.contents:
            if isinstance(content, DurableAgentStateTextContent):
                text_parts.append(content.text or "")
        return "".join(text_parts)

    @staticmethod
    def from_run_request(request: RunRequest) -> DurableAgentStateMessage:
        """Converts a RunRequest from the agent framework to a DurableAgentStateMessage.

        Args:
            request: RunRequest object with role, message/contents, and metadata
        Returns:
            DurableAgentStateMessage with converted content items and metadata
        """
        return DurableAgentStateMessage(
            role=request.role,
            contents=[DurableAgentStateTextContent(text=request.message)],
            created_at=_parse_created_at(request.created_at) if request.created_at else None,
        )

    @staticmethod
    def from_core_dict(data: dict[str, Any]) -> DurableAgentStateMessage:
        """Keep unknown core fields before consumer filtering can discard them.

        Args:
            data: Serialized core message containing content envelopes and optional metadata.

        Returns:
            Durable message preserving unknown message and content fields.
        """
        raw = _json_snapshot(data)
        _validate_core_message(raw)
        message = load_agent_response({"messages": [raw]}).messages[0]
        stored = DurableAgentStateMessage.from_chat_message(message)
        for content, original in zip(stored.contents, raw.get("contents", []), strict=True):
            if not isinstance(original, dict):
                raise ValueError("Core contents must contain content objects.")
            original = cast(dict[str, Any], original)
            if isinstance(content, DurableAgentStateUnknownContent):
                content.content = original
            else:
                mapped = content.core_projection()
                content.extensionData = {
                    "coreContent": {key: value for key, value in original.items() if key not in mapped}
                }
        known = {"type", "role", "contents", "author_name", "message_id", "additional_properties"}
        stored.unknown_fields = {key: value for key, value in raw.items() if key not in known}
        return stored

    @staticmethod
    def from_chat_message(chat_message: Message) -> DurableAgentStateMessage:
        """Converts an Agent Framework chat message to a durable state message.

        Args:
            chat_message: Message object with role, contents, and metadata to convert

        Returns:
            DurableAgentStateMessage with converted content items and metadata
        """
        contents_list: list[DurableAgentStateContent] = [
            DurableAgentStateContent.from_ai_content(c) for c in chat_message.contents
        ]

        stored = DurableAgentStateMessage(
            role=chat_message.role if hasattr(chat_message.role, "value") else str(chat_message.role),
            contents=contents_list,
            author_name=chat_message.author_name,
            message_id=getattr(chat_message, "message_id", None),
            extension_data=deepcopy(chat_message.additional_properties) if chat_message.additional_properties else None,
        )
        stored.ingestion_identity = message_identity(chat_message)
        known = {"type", "role", "contents", "author_name", "message_id", "additional_properties"}
        stored.unknown_fields = {
            key: value for key, value in _json_snapshot(chat_message.to_dict()).items() if key not in known
        }
        return stored

    def to_chat_message(self) -> Any:
        """Converts this DurableAgentStateMessage back to an agent framework Message.

        Returns:
            Message object with role, contents, and metadata converted back to agent framework types
        """
        # Convert DurableAgentStateContent objects back to agent_framework content objects
        ai_contents = [c.to_core_content() for c in self.contents]

        # Build kwargs for Message
        kwargs: dict[str, Any] = {
            "role": self.role,
            "contents": ai_contents,
        }

        if self.author_name is not None:
            kwargs["author_name"] = self.author_name

        if self.message_id is not None:
            kwargs["message_id"] = self.message_id

        if self.extension_data is not None:
            # Copied, not shared. Callers treat the result as detached and mutate it: retention
            # pops compaction annotations off the copies it measures. Handing out the stored dict
            # would make that erase those annotations from durable state. Core does copy this
            # during validation today, but that is its internal business, and quietly depending on
            # it would mean a change there costs us the user's compaction work.
            kwargs["additional_properties"] = deepcopy(self.extension_data)

        return Message(**kwargs)


class DurableAgentStateDataContent(DurableAgentStateContent):
    """Represents data content with a URI reference.

    This content type is used to reference data stored at a specific URI location,
    optionally with a media type specification. Common use cases include referencing
    files, documents, or other data resources.

    Attributes:
        uri: URI pointing to the data resource
        media_type: Optional MIME type of the data (e.g., "application/json", "text/plain")
    """

    uri: str = ""
    media_type: str | None = None
    type: str = ContentTypes.DATA

    def __init__(self, uri: str, media_type: str | None = None) -> None:
        self.uri = uri
        self.media_type = media_type

    def to_dict(self) -> dict[str, Any]:
        return {
            DurableStateFields.TYPE_DISCRIMINATOR: self.type,
            DurableStateFields.URI: self.uri,
            DurableStateFields.MEDIA_TYPE: self.media_type,
        }

    @staticmethod
    def from_data_content(content: Content) -> DurableAgentStateDataContent:
        if content.uri is None:
            raise ValueError("uri is required for data content")
        return DurableAgentStateDataContent(uri=content.uri, media_type=content.media_type)

    def to_ai_content(self) -> Content:
        return Content.from_uri(uri=self.uri, media_type=self.media_type)


class DurableAgentStateErrorContent(DurableAgentStateContent):
    """Represents error content in agent responses.

    This content type is used to communicate errors that occurred during agent execution,
    including error messages, error codes, and additional details for debugging.

    Attributes:
        message: Human-readable error message
        error_code: Machine-readable error code or exception type
        details: Additional error details or stack trace information
    """

    message: str | None = None
    error_code: str | None = None
    details: str | None = None

    type: str = ContentTypes.ERROR

    def __init__(self, message: str | None = None, error_code: str | None = None, details: str | None = None) -> None:
        self.message = message
        self.error_code = error_code
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        return {
            DurableStateFields.TYPE_DISCRIMINATOR: self.type,
            DurableStateFields.MESSAGE: self.message,
            DurableStateFields.ERROR_CODE: self.error_code,
            DurableStateFields.DETAILS: self.details,
        }

    @staticmethod
    def from_error_content(content: Content) -> DurableAgentStateErrorContent:
        return DurableAgentStateErrorContent(
            message=content.message, error_code=content.error_code, details=content.error_details
        )

    def to_ai_content(self) -> Content:
        return Content.from_error(message=self.message, error_code=self.error_code, error_details=self.details)


class DurableAgentStateFunctionCallContent(DurableAgentStateContent):
    """Represents a function/tool call request from the agent.

    This content type is used when the agent requests execution of a function or tool,
    including the function name, arguments, and a unique call identifier for tracking
    the call-result pair.

    Attributes:
        call_id: Unique identifier for this function call (used to match with results)
        name: Name of the function/tool to execute
        arguments: Original argument string or mapping, without lossy reparsing
    """

    call_id: str
    name: str
    arguments: dict[str, Any] | str | None

    type: str = ContentTypes.FUNCTION_CALL

    def __init__(self, call_id: str, name: str, arguments: dict[str, Any] | str | None) -> None:
        self.call_id = call_id
        self.name = name
        self.arguments = arguments

    def to_dict(self) -> dict[str, Any]:
        return {
            DurableStateFields.TYPE_DISCRIMINATOR: self.type,
            DurableStateFields.CALL_ID: self.call_id,
            DurableStateFields.NAME: self.name,
            DurableStateFields.ARGUMENTS: self.arguments,
        }

    @staticmethod
    def from_function_call_content(content: Content) -> DurableAgentStateFunctionCallContent:
        if content.call_id is None:
            raise ValueError("call_id is required for function call content")
        if content.name is None:
            raise ValueError("name is required for function call content")
        return DurableAgentStateFunctionCallContent(
            call_id=content.call_id, name=content.name, arguments=_json_snapshot(content.to_dict().get("arguments"))
        )

    def to_ai_content(self) -> Content:
        arguments = json.dumps(self.arguments) if isinstance(self.arguments, dict) else self.arguments
        return Content.from_function_call(call_id=self.call_id, name=self.name, arguments=arguments)


class DurableAgentStateFunctionResultContent(DurableAgentStateContent):
    """Represents the result of a function/tool call execution.

    This content type is used to communicate the result of executing a function or tool
    that was previously requested by the agent. The call_id links this result back to
    the original function call request.

    Attributes:
        call_id: Unique identifier matching the original function call
        result: The return value from the function execution (can be any serializable type)
    """

    call_id: str
    result: object | None = None

    type: str = ContentTypes.FUNCTION_RESULT

    _NULLABLE_FIELDS: ClassVar[frozenset[str]] = frozenset({DurableStateFields.RESULT})

    def __init__(self, call_id: str, result: Any | None = None) -> None:
        self.call_id = call_id
        self.result = result

    def to_dict(self) -> dict[str, Any]:
        return {
            DurableStateFields.TYPE_DISCRIMINATOR: self.type,
            DurableStateFields.CALL_ID: self.call_id,
            DurableStateFields.RESULT: self.result,
        }

    @staticmethod
    def from_function_result_content(content: Content) -> DurableAgentStateFunctionResultContent:
        if content.call_id is None:
            raise ValueError("call_id is required for function result content")
        return DurableAgentStateFunctionResultContent(
            call_id=content.call_id, result=_json_snapshot(content.to_dict().get("result"))
        )

    def to_ai_content(self) -> Content:
        return Content.from_function_result(call_id=self.call_id, result=self.result)


class DurableAgentStateHostedFileContent(DurableAgentStateContent):
    """Represents a reference to a hosted file resource.

    This content type is used to reference files that are hosted by the agent platform
    or a file storage service, identified by a unique file ID.

    Attributes:
        file_id: Unique identifier for the hosted file
    """

    file_id: str

    type: str = ContentTypes.HOSTED_FILE

    def __init__(self, file_id: str) -> None:
        self.file_id = file_id

    def to_dict(self) -> dict[str, Any]:
        return {DurableStateFields.TYPE_DISCRIMINATOR: self.type, DurableStateFields.FILE_ID: self.file_id}

    @staticmethod
    def from_hosted_file_content(content: Content) -> DurableAgentStateHostedFileContent:
        if content.file_id is None:
            raise ValueError("file_id is required for hosted file content")
        return DurableAgentStateHostedFileContent(file_id=content.file_id)

    def to_ai_content(self) -> Content:
        return Content.from_hosted_file(file_id=self.file_id)


class DurableAgentStateHostedVectorStoreContent(DurableAgentStateContent):
    """Represents a reference to a hosted vector store resource.

    This content type is used to reference vector stores (used for semantic search
    and retrieval-augmented generation) that are hosted by the agent platform,
    identified by a unique vector store ID.

    Attributes:
        vector_store_id: Unique identifier for the hosted vector store
    """

    vector_store_id: str

    type: str = ContentTypes.HOSTED_VECTOR_STORE

    def __init__(self, vector_store_id: str) -> None:
        self.vector_store_id = vector_store_id

    def to_dict(self) -> dict[str, Any]:
        return {
            DurableStateFields.TYPE_DISCRIMINATOR: self.type,
            DurableStateFields.VECTOR_STORE_ID: self.vector_store_id,
        }

    @staticmethod
    def from_hosted_vector_store_content(
        content: Content,
    ) -> DurableAgentStateHostedVectorStoreContent:
        if content.vector_store_id is None:
            raise ValueError("vector_store_id is required for hosted vector store content")
        return DurableAgentStateHostedVectorStoreContent(vector_store_id=content.vector_store_id)

    def to_ai_content(self) -> Content:
        return Content.from_hosted_vector_store(vector_store_id=self.vector_store_id)


class DurableAgentStateTextContent(DurableAgentStateContent):
    """Represents plain text content in messages.

    This is the most common content type, used for regular text messages from users
    and text responses from the agent.

    Attributes:
        text: The text content of the message
    """

    type: str = ContentTypes.TEXT

    def __init__(self, text: str | None) -> None:
        self.text = text

    def to_dict(self) -> dict[str, Any]:
        return {DurableStateFields.TYPE_DISCRIMINATOR: self.type, DurableStateFields.TEXT: self.text}

    def to_persisted_dict(self) -> dict[str, Any]:
        """Require the schema's text string rather than emit an invalid content item."""
        if not isinstance(self.text, str):
            raise ValueError("Text content requires a text string for persistence.")
        return super().to_persisted_dict()

    @staticmethod
    def from_text_content(content: Content) -> DurableAgentStateTextContent:
        return DurableAgentStateTextContent(text=content.text)

    def to_ai_content(self) -> Content:
        return Content.from_text(text=self.text or "")


class DurableAgentStateTextReasoningContent(DurableAgentStateContent):
    """Represents reasoning or thought process text from the agent.

    This content type is used to capture the agent's internal reasoning, chain of thought,
    or explanation of its decision-making process, separate from the final response text.

    Attributes:
        text: The reasoning or thought process text
    """

    type: str = ContentTypes.REASONING

    def __init__(self, text: str | None) -> None:
        self.text = text

    def to_dict(self) -> dict[str, Any]:
        return {DurableStateFields.TYPE_DISCRIMINATOR: self.type, DurableStateFields.TEXT: self.text}

    @staticmethod
    def from_text_reasoning_content(content: Content) -> DurableAgentStateTextReasoningContent:
        return DurableAgentStateTextReasoningContent(text=content.text)

    def to_ai_content(self) -> Content:
        return Content.from_text_reasoning(text=self.text)


class DurableAgentStateUriContent(DurableAgentStateContent):
    """Represents content referenced by a URI with media type.

    This content type is used to reference external content via a URI, with an associated
    media type to indicate how the content should be interpreted.

    Attributes:
        uri: URI pointing to the content resource
        media_type: MIME type of the content (e.g., "image/png", "application/pdf")
    """

    uri: str
    media_type: str | None

    type: str = ContentTypes.URI

    def __init__(self, uri: str, media_type: str | None = None) -> None:
        self.uri = uri
        self.media_type = media_type

    def to_dict(self) -> dict[str, Any]:
        return {
            DurableStateFields.TYPE_DISCRIMINATOR: self.type,
            DurableStateFields.URI: self.uri,
            DurableStateFields.MEDIA_TYPE: self.media_type,
        }

    @staticmethod
    def from_uri_content(content: Content) -> DurableAgentStateUriContent:
        if content.uri is None:
            raise ValueError("uri is required for uri content")
        return DurableAgentStateUriContent(uri=content.uri, media_type=content.media_type)

    def to_ai_content(self) -> Content:
        return Content.from_uri(uri=self.uri, media_type=self.media_type)


class DurableAgentStateUsage:
    """Represents token usage statistics for agent responses.

    This class tracks the number of tokens consumed during agent execution,
    including input tokens (from the request), output tokens (in the response),
    and the total token count.

    Attributes:
        input_token_count: Number of tokens in the input/request
        output_token_count: Number of tokens in the output/response
        total_token_count: Total number of tokens consumed (input + output)
        extensionData: Optional additional metadata
    """

    # UsageDetails field name constants (snake_case keys from agent_framework.UsageDetails)
    _INPUT_TOKEN_COUNT = "input_token_count"  # noqa: S105  # nosec B105
    _OUTPUT_TOKEN_COUNT = "output_token_count"  # noqa: S105  # nosec B105
    _TOTAL_TOKEN_COUNT = "total_token_count"  # noqa: S105  # nosec B105

    # Standard fields in UsageDetails that are mapped to dedicated attributes
    _STANDARD_USAGE_FIELDS: ClassVar[set[str]] = {_INPUT_TOKEN_COUNT, _OUTPUT_TOKEN_COUNT, _TOTAL_TOKEN_COUNT}

    input_token_count: int | None = None
    output_token_count: int | None = None
    total_token_count: int | None = None
    extensionData: dict[str, Any] | None = None

    def __init__(
        self,
        input_token_count: int | None = None,
        output_token_count: int | None = None,
        total_token_count: int | None = None,
        extensionData: dict[str, Any] | None = None,
    ) -> None:
        self.input_token_count = input_token_count
        self.output_token_count = output_token_count
        self.total_token_count = total_token_count
        self.extensionData = extensionData
        self.unknown_fields: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        counts: dict[str, Any] = {
            DurableStateFields.INPUT_TOKEN_COUNT: self.input_token_count,
            DurableStateFields.OUTPUT_TOKEN_COUNT: self.output_token_count,
            DurableStateFields.TOTAL_TOKEN_COUNT: self.total_token_count,
        }
        result = {**self.unknown_fields, **{key: value for key, value in counts.items() if value is not None}}
        if self.extensionData is not None:
            result[DurableStateFields.EXTENSION_DATA] = self.extensionData
        return _json_snapshot(result)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateUsage:
        usage = cls(
            input_token_count=data.get(DurableStateFields.INPUT_TOKEN_COUNT),
            output_token_count=data.get(DurableStateFields.OUTPUT_TOKEN_COUNT),
            total_token_count=data.get(DurableStateFields.TOTAL_TOKEN_COUNT),
            extensionData=data.get(DurableStateFields.EXTENSION_DATA),
        )
        known = {
            DurableStateFields.INPUT_TOKEN_COUNT,
            DurableStateFields.OUTPUT_TOKEN_COUNT,
            DurableStateFields.TOTAL_TOKEN_COUNT,
            DurableStateFields.EXTENSION_DATA,
        }
        usage.unknown_fields = {key: deepcopy(value) for key, value in data.items() if key not in known}
        return usage

    @staticmethod
    def from_usage(usage: UsageDetails | MutableMapping[str, Any] | None) -> DurableAgentStateUsage | None:
        if usage is None:
            return None

        # Collect all non-standard fields into extension_data
        extension_data: dict[str, Any] = {
            k: v for k, v in usage.items() if k not in DurableAgentStateUsage._STANDARD_USAGE_FIELDS
        }

        return DurableAgentStateUsage(
            input_token_count=cast("int | None", usage.get(DurableAgentStateUsage._INPUT_TOKEN_COUNT)),
            output_token_count=cast("int | None", usage.get(DurableAgentStateUsage._OUTPUT_TOKEN_COUNT)),
            total_token_count=cast("int | None", usage.get(DurableAgentStateUsage._TOTAL_TOKEN_COUNT)),
            extensionData=extension_data if extension_data else None,
        )

    def to_usage_details(self) -> UsageDetails:
        # Convert back to AI SDK UsageDetails
        result = cast(
            UsageDetails,
            {
                key: value
                for key, value in (
                    (self._INPUT_TOKEN_COUNT, self.input_token_count),
                    (self._OUTPUT_TOKEN_COUNT, self.output_token_count),
                    (self._TOTAL_TOKEN_COUNT, self.total_token_count),
                )
                if value is not None
            },
        )
        if self.extensionData:
            result.update(deepcopy(self.extensionData))  # type: ignore[typeddict-item]
        return result


class DurableAgentStateUsageContent(DurableAgentStateContent):
    """Represents token usage information as message content.

    This content type is used to communicate token usage statistics as part of
    message content, allowing usage information to be tracked alongside other
    content types in the conversation history.

    Attributes:
        usage: DurableAgentStateUsage object containing token counts
    """

    usage: DurableAgentStateUsage = DurableAgentStateUsage()

    type: str = ContentTypes.USAGE

    def __init__(self, usage: DurableAgentStateUsage | None) -> None:
        self.usage = usage if usage is not None else DurableAgentStateUsage()

    def to_dict(self) -> dict[str, Any]:
        return {
            DurableStateFields.TYPE_DISCRIMINATOR: self.type,
            DurableStateFields.USAGE: self.usage.to_dict(),
        }

    @staticmethod
    def from_usage_content(content: Content) -> DurableAgentStateUsageContent:
        return DurableAgentStateUsageContent(usage=DurableAgentStateUsage.from_usage(content.usage_details))

    def to_ai_content(self) -> Content:
        return Content.from_usage(usage_details=self.usage.to_usage_details())


class DurableAgentStateUnknownContent(DurableAgentStateContent):
    """Represents unknown or unrecognized content types.

    This content type serves as a fallback for content that doesn't match any of the
    known content type classes. It preserves the original content object for later
    inspection or processing.

    Attributes:
        content: The unknown content object
    """

    content: Any

    type: str = ContentTypes.UNKNOWN

    _NULLABLE_FIELDS: ClassVar[frozenset[str]] = frozenset({DurableStateFields.CONTENT})

    def __init__(self, content: Any) -> None:
        self.content = content

    def to_dict(self) -> dict[str, Any]:
        return {DurableStateFields.TYPE_DISCRIMINATOR: self.type, DurableStateFields.CONTENT: self.content}

    @staticmethod
    def from_unknown_content(content: Any) -> DurableAgentStateUnknownContent:
        if isinstance(content, Content):
            return DurableAgentStateUnknownContent(content=content.to_dict())
        return DurableAgentStateUnknownContent(content=content)

    def to_core_content(self) -> Content:
        """Leave unknown content extension conventions opaque, as for future raw kinds."""
        return self.to_ai_content()

    def to_ai_content(self) -> Content:
        content_value: Any = self.content
        if isinstance(content_value, dict) and "type" in content_value:
            return (
                load_agent_response({"messages": [{"role": "assistant", "contents": [content_value]}]})
                .messages[0]
                .contents[0]
            )
        return Content(type=self.type, additional_properties={"content": deepcopy(self.content)})  # type: ignore
