# Copyright (c) Microsoft. All rights reserved.

"""Private shared durable-agent state model and canonical writer bridge.

This module is a private extraction of the shared-state implementation. It keeps the
legacy mutable reader/writer in ``_durable_agent_state`` unchanged while providing
the canonical transcript, opaque field preservation, session persistence, and exact
delivery bookkeeping needed by later private callers.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import MutableMapping
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, cast

from agent_framework import AgentResponse, Content, Message, UsageDetails
from dateutil import parser as date_parser

from ._constants import ContentTypes, DurableStateFields
from ._delivery_state import lookup_response, stage_expiry, stage_response
from ._message_identity import message_identity
from ._models import RunRequest, serialize_response_format
from ._response_utils import (
    load_agent_response,
    preserve_input_envelope,
    serialize_input_content,
)
from ._shared_state_validation import (
    validate_shared_data,
    validate_shared_state,
    validate_timestamp,
)

logger = logging.getLogger("agent_framework.durabletask")


def _validate_completion_outcomes(records: dict[str, dict[str, Any]], mailboxes: dict[str, dict[str, Any]]) -> None:
    """Check shared map invariants before delivering or removing any result."""
    validate_shared_data({
        "conversationHistory": [],
        "terminalResults": mailboxes,
        "completionReceipts": records,
    })
    for correlation_id, record in records.items():
        mailbox = mailboxes.get(correlation_id)
        if mailbox is None:
            continue
        response = mailbox[DurableStateFields.RESPONSE]
        failed = response.get("extensionData", {}).get("durable_status") == "error" or any(
            content["$type"] == "error"
            for message in response["messages"]
            if message["role"] != "tool"
            for content in message.get("contents", [])
        )
        if record["outcome"] == "succeeded" and failed:
            raise ValueError("A succeeded terminal result conflicts with its response failure evidence.")


def _validate_json(value: Any) -> None:
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
    try:
        _validate_json(value)
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("State must be strict JSON with string keys and finite numbers.") from exc


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


def _validate_core_message_keys(data: dict[str, Any]) -> None:
    reserved = {
        "originalMessageId",
        "messageId",
        "authorName",
        "createdAt",
        "extensionData",
        "pythonHistoryId",
        "pythonHistoryIdentity",
    }
    conflicts = reserved.intersection(data)
    if conflicts:
        raise ValueError(f"Core message contains reserved durable fields: {', '.join(sorted(conflicts))}.")


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


class _RawShadow:
    """Keep original JSON field representations until their typed projection changes."""

    def __init__(self, raw: dict[str, Any], projection: dict[str, Any]) -> None:
        self.raw: dict[str, Any] = _json_snapshot(raw)
        self.projection: dict[str, Any] = _json_snapshot(projection)

    def merge(self, projection: dict[str, Any]) -> dict[str, Any]:
        current: dict[str, Any] = _json_snapshot(projection)
        result = deepcopy(self.raw)
        for key in self.projection.keys() | current.keys():
            if (
                key in self.projection
                and key in current
                and json.dumps(self.projection[key], sort_keys=True, allow_nan=False)
                == json.dumps(current[key], sort_keys=True, allow_nan=False)
            ):
                continue
            if key in current:
                result[key] = current[key]
            else:
                result.pop(key, None)
        return result


def _parse_transcript_created_at(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return date_parser.isoparse(value)


_HISTORY_IDENTITY_PROFILE = {"profile": "agent-framework-python.history-identity", "version": 1}
_CONTENT_ENCODING_PROFILE = {"profile": "agent-framework-python.content", "version": 1}
_CORE_FIELDS_PROFILE = {"profile": "agent-framework-python.core-fields", "version": 1}
_INGESTION_PROFILE = {"profile": "agent-framework-python.ingestion", "version": 1}


def _has_python_profile(value: Any, profile: dict[str, Any]) -> bool:
    if not isinstance(value, dict):
        return False
    value = cast(dict[str, Any], value)
    return type(value.get("version")) is int and all(value.get(key) == item for key, item in profile.items())


class DurableAgentStateEntryJsonType(str, Enum):
    REQUEST = "request"
    RESPONSE = "response"
    ERROR_RESPONSE = "errorResponse"
    COMPACTION = "compaction"


def _parse_created_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.utcoffset() is None else value

    if isinstance(value, str):
        try:
            parsed = date_parser.parse(value)
            if isinstance(parsed, datetime):
                return parsed.replace(tzinfo=timezone.utc) if parsed.utcoffset() is None else parsed
        except (ValueError, TypeError):
            pass

    logger.warning(
        "Invalid or missing created_at value in durable agent state; defaulting to current UTC time, %s",
        value,
        stack_info=True,
    )
    return datetime.now(tz=timezone.utc)


def _parse_messages(data: dict[str, Any]) -> list[DurableAgentStateMessage]:
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
    history_data = _array_field(data_dict, DurableStateFields.CONVERSATION_HISTORY)
    deserialized_history: list[DurableAgentStateEntry] = []
    for raw_entry in history_data:
        if isinstance(raw_entry, dict):
            entry_dict = cast(dict[str, Any], raw_entry)
            entry_type = entry_dict.get(DurableStateFields.TYPE_DISCRIMINATOR) or entry_dict.get(
                DurableStateFields.JSON_TYPE
            )
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
        elif isinstance(raw_entry, DurableAgentStateEntry):
            deserialized_history.append(raw_entry)
        else:
            raise ValueError("conversationHistory must contain entry objects.")
    return deserialized_history


def _parse_contents(data: dict[str, Any]) -> list[DurableAgentStateContent]:
    contents: list[DurableAgentStateContent] = []
    raw_contents = _array_field(data, DurableStateFields.CONTENTS)
    for raw_content in raw_contents:
        if isinstance(raw_content, DurableAgentStateContent):
            contents.append(raw_content)
        elif isinstance(raw_content, dict):
            content_dict = deepcopy(cast(dict[str, Any], raw_content))
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
            extension = content_dict.get(DurableStateFields.EXTENSION_DATA)
            if isinstance(extension, dict):
                content.extensionData = deepcopy(cast(dict[str, Any], extension))
            elif DurableStateFields.EXTENSION_DATA in content_dict:
                content.unknown_fields[DurableStateFields.EXTENSION_DATA] = deepcopy(extension)
            content._raw_shadow = _RawShadow(  # pyright: ignore[reportPrivateUsage]
                content_dict, content.to_persisted_dict()
            )
        else:
            raise ValueError("contents must contain content objects.")

    return contents


class DurableAgentStateContent:
    extensionData: dict[str, Any] | None = None
    unknown_fields: dict[str, Any] | None = None
    type: str = ""

    _NULLABLE_FIELDS: ClassVar[frozenset[str]] = frozenset()
    _raw_shadow: _RawShadow | None = None

    def to_persisted_dict(self) -> dict[str, Any]:
        result = {
            **(self.unknown_fields or {}),
            **{
                key: value for key, value in self.to_dict().items() if value is not None or key in self._NULLABLE_FIELDS
            },
        }
        if self.extensionData is not None:
            result[DurableStateFields.EXTENSION_DATA] = self.extensionData
        return self._raw_shadow.merge(result) if self._raw_shadow is not None else _json_snapshot(result)

    def core_projection(self) -> dict[str, Any]:
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
        profile = (self.unknown_fields or {}).get("pythonCoreFields")
        if not _has_python_profile(profile, _CORE_FIELDS_PROFILE):
            content = self.to_ai_content()
            if isinstance(self.extensionData, dict):
                content.additional_properties = deepcopy(self.extensionData)
            return content
        profile = cast(dict[str, Any], profile)
        extra = profile.get("fields")
        if not isinstance(extra, dict):
            raise ValueError("The Python core-fields profile requires a fields object.")
        if extra.keys() & (self.core_projection().keys() | {"raw_representation", "response_format"}):
            raise ValueError("Python core-fields metadata cannot replace known content fields.")
        payload = {**deepcopy(cast(dict[str, Any], extra)), **self.core_projection()}
        if "additional_properties" not in extra and isinstance(self.extensionData, dict):
            payload["additional_properties"] = deepcopy(self.extensionData)
        return (
            load_agent_response({"messages": [{"role": "assistant", "contents": [deepcopy(payload)]}]})
            .messages[0]
            .contents[0]
        )

    def to_dict(self) -> dict[str, Any]:
        raise NotImplementedError

    def to_ai_content(self) -> Any:
        raise NotImplementedError

    @staticmethod
    def from_ai_content(content: Any) -> DurableAgentStateContent:
        stored = DurableAgentStateContent._from_ai_content(content)
        if isinstance(content, Content):
            payload = _json_snapshot(serialize_input_content(content))
            if isinstance(stored, DurableAgentStateUnknownContent):
                stored.content = payload
            else:
                mapped = stored.core_projection()
                stored.unknown_fields = {
                    "pythonCoreFields": {
                        **_CORE_FIELDS_PROFILE,
                        "fields": {key: value for key, value in payload.items() if key not in mapped},
                    }
                }
                original = getattr(content, "_durable_original_core_content", None)
                if (
                    stored._NULLABLE_FIELDS
                    and isinstance(original, dict)
                    and cast(dict[str, Any], original).get("type") == content.type
                ):
                    # Core has no absence bit for nullable mapped fields. Preserve
                    # literal input presence without changing plain Core defaults.
                    projection = stored.to_persisted_dict()
                    raw = deepcopy(projection)
                    for field_name in stored._NULLABLE_FIELDS:
                        core_name = "error_details" if field_name == DurableStateFields.DETAILS else field_name
                        if core_name not in original and core_name not in payload and raw.get(field_name) is None:
                            raw.pop(field_name, None)
                    stored._raw_shadow = _RawShadow(raw, projection)
        return stored

    @staticmethod
    def _from_ai_content(content: Any) -> DurableAgentStateContent:
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
    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = deepcopy(raw)

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)

    def to_persisted_dict(self) -> dict[str, Any]:
        return _json_snapshot(self.raw)

    def to_core_content(self) -> Content:
        return self.to_ai_content()

    def to_ai_content(self) -> Content:
        return Content(type="unknown", additional_properties={"content": deepcopy(self.raw)})  # type: ignore[arg-type]


class DurableAgentStateData:
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
        self.conversation_history = conversation_history or []
        self.extension_data = extension_data
        self.session = session
        self.ingested_positions = ingested_positions
        self.truncation = truncation
        self.response_mailbox = response_mailbox or {}
        self.completed_correlations = completed_correlations or {}
        self.ingested_messages = ingested_messages or {}
        self.unknown_fields = {}
        self._raw_shadow: _RawShadow | None = None
        self._ingestion_profile: dict[str, Any] | None = None

    def _validate_ingestion(self) -> None:
        if not isinstance(self.ingested_messages, dict) or any(
            not isinstance(identity, str)
            or not identity.strip()
            or (
                fingerprints is not None
                and (
                    not isinstance(fingerprints, list)
                    or not fingerprints
                    or any(
                        not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
                        for value in fingerprints
                    )
                    or len(set(fingerprints)) != len(fingerprints)
                )
            )
            for identity, fingerprints in self.ingested_messages.items()
        ):
            raise ValueError(
                "pythonIngestion.messages requires nonblank IDs and unique SHA-256 lists or identity markers."
            )

    def to_dict(self, *, schema_version: str = "2.0.0") -> dict[str, Any]:
        result: dict[str, Any] = {
            **deepcopy(self.unknown_fields),
            DurableStateFields.CONVERSATION_HISTORY: [entry.to_dict() for entry in self.conversation_history],
        }
        if self.extension_data is not None:
            result[DurableStateFields.EXTENSION_DATA] = self.extension_data
        if self.session is not None:
            result[DurableStateFields.SESSION] = self.session
        if self.ingested_positions is not None:
            result[DurableStateFields.INGESTED_POSITIONS] = self.ingested_positions
        if self.truncation:
            result[DurableStateFields.TRUNCATION] = self.truncation
        if schema_version == "2.0.0":
            result[DurableStateFields.RESPONSE_MAILBOX] = deepcopy(self.response_mailbox)
            result[DurableStateFields.COMPLETED_CORRELATIONS] = deepcopy(self.completed_correlations)
        self._validate_ingestion()
        if schema_version == "2.0.0" and (self.ingested_messages or self._ingestion_profile is not None):
            if "pythonIngestion" in self.unknown_fields:
                raise ValueError("Cannot replace opaque pythonIngestion metadata with a different runtime profile.")
            result["pythonIngestion"] = {
                **deepcopy(self._ingestion_profile or _INGESTION_PROFILE),
                "messages": deepcopy(self.ingested_messages),
            }
        result = _json_snapshot(result)
        if self._raw_shadow is not None:
            result = self._raw_shadow.merge(result)
        validate_shared_data(result, version=schema_version)
        if schema_version == "2.0.0":
            _validate_completion_outcomes(self.completed_correlations, self.response_mailbox)
        return result

    @classmethod
    def from_dict(cls, data_dict: dict[str, Any], *, schema_version: str = "2.0.0") -> DurableAgentStateData:
        validate_shared_data(data_dict, version=schema_version)
        data_dict = deepcopy(data_dict)
        for name in (DurableStateFields.RESPONSE_MAILBOX, DurableStateFields.COMPLETED_CORRELATIONS):
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
        )
        known = {
            DurableStateFields.CONVERSATION_HISTORY,
            DurableStateFields.EXTENSION_DATA,
            DurableStateFields.SESSION,
            DurableStateFields.INGESTED_POSITIONS,
            DurableStateFields.TRUNCATION,
            DurableStateFields.RESPONSE_MAILBOX,
            DurableStateFields.COMPLETED_CORRELATIONS,
        }
        profile = data_dict.get("pythonIngestion")
        if schema_version == "2.0.0" and _has_python_profile(profile, _INGESTION_PROFILE):
            profile = cast(dict[str, Any], profile)
            if not isinstance(profile.get("messages"), dict):
                raise ValueError("The Python ingestion profile requires a messages object.")
            result.ingested_messages = deepcopy(profile["messages"])
            result._ingestion_profile = deepcopy(profile)
            result._validate_ingestion()
            known.add("pythonIngestion")
        result.unknown_fields = {key: deepcopy(value) for key, value in data_dict.items() if key not in known}
        if schema_version == "2.0.0":
            _validate_completion_outcomes(result.completed_correlations, result.response_mailbox)
        result._raw_shadow = _RawShadow(data_dict, result.to_dict(schema_version=schema_version))
        return result


class DurableAgentState:
    SCHEMA_VERSION: str = "2.0.0"

    data: DurableAgentStateData
    schema_version: str = SCHEMA_VERSION

    def __init__(self, schema_version: str = SCHEMA_VERSION):
        self.data = DurableAgentStateData()
        self.schema_version = schema_version
        self.unknown_fields: dict[str, Any] = {}

    def to_dict(self) -> dict[str, Any]:
        result = _json_snapshot({
            **deepcopy(self.unknown_fields),
            DurableStateFields.SCHEMA_VERSION: self.schema_version,
            DurableStateFields.DATA: self.data.to_dict(schema_version=self.schema_version),
        })
        validate_shared_state(result)
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), allow_nan=False)

    @classmethod
    def from_dict(cls, state: dict[str, Any]) -> DurableAgentState:
        if not isinstance(state, dict):
            raise ValueError("The durable agent state must be a JSON object.")
        state = _json_snapshot(state)
        schema_version = state.get(DurableStateFields.SCHEMA_VERSION)
        if schema_version is None:
            raise ValueError("The durable agent state is missing schemaVersion; refusing to discard existing state.")
        if schema_version not in ("1.0.0", "1.1.0", "1.2.0", "2.0.0"):
            raise ValueError(f"Unsupported durable agent state schemaVersion: {schema_version!r}.")
        raw_data = state.get(DurableStateFields.DATA)
        if not isinstance(raw_data, dict):
            raise ValueError("The durable agent state data must be an object.")

        validate_shared_state(state)
        instance = cls(schema_version=schema_version)
        instance.data = DurableAgentStateData.from_dict(cast(dict[str, Any], raw_data), schema_version=schema_version)
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
        except json.JSONDecodeError as exc:
            raise ValueError("The durable agent state is not valid JSON.") from exc

        if not isinstance(obj, dict):
            raise ValueError("The durable agent state must be a JSON object.")
        return cls.from_dict(cast(dict[str, Any], obj))

    @property
    def message_count(self) -> int:
        return len(self.data.conversation_history)

    def try_get_agent_response(self, correlation_id: str, *, now: datetime | None = None) -> AgentResponse | None:
        if self.schema_version == "2.0.0":
            return lookup_response(self.to_dict(), correlation_id, now=now)
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
    ) -> None:
        """Stage delivery against the complete snapshot without replacing history objects."""
        if self.schema_version != self.SCHEMA_VERSION:
            raise ValueError("Recording a terminal result requires the writable shared schema version.")
        candidate = stage_response(
            self.to_dict(),
            correlation_id,
            response,
            delivery_window_seconds=delivery_window_seconds,
            now=now,
        )
        if correlation_id in self.data.completed_correlations:
            return
        data = candidate[DurableStateFields.DATA]
        self.data.response_mailbox = deepcopy(data[DurableStateFields.RESPONSE_MAILBOX])
        self.data.completed_correlations = deepcopy(data[DurableStateFields.COMPLETED_CORRELATIONS])

    def expire_responses(self, *, now: datetime | None = None) -> None:
        """Remove due payloads while retaining typed history and immutable completion facts."""
        candidate, removed = stage_expiry(self.to_dict(), now=now)
        if not removed:
            return
        data = candidate[DurableStateFields.DATA]
        self.data.response_mailbox = deepcopy(data[DurableStateFields.RESPONSE_MAILBOX])
        self.data.completed_correlations = deepcopy(data[DurableStateFields.COMPLETED_CORRELATIONS])

    def prepare_for_write(self, *, delivery_window_seconds: int) -> None:
        _validate_completion_outcomes(self.data.completed_correlations, self.data.response_mailbox)
        if self.schema_version == self.SCHEMA_VERSION:
            self.to_dict()
            if "pythonIngestion" in self.data.unknown_fields:
                raise ValueError(
                    "Writing requires a supported Python ingestion profile; opaque bookkeeping cannot be ignored."
                )
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
    json_type: DurableAgentStateEntryJsonType | str
    correlation_id: str | None
    created_at: datetime | None
    messages: list[DurableAgentStateMessage]
    extension_data: dict[str, Any] | None

    def __init__(
        self,
        json_type: DurableAgentStateEntryJsonType | str,
        correlation_id: str | None,
        created_at: datetime | None,
        messages: list[DurableAgentStateMessage],
        extension_data: dict[str, Any] | None = None,
    ) -> None:
        self.json_type = json_type
        self.correlation_id = correlation_id
        self.created_at = created_at
        self.messages = messages
        self.extension_data = extension_data
        self.unknown_fields: dict[str, Any] = {}
        self._raw_shadow: _RawShadow | None = None

    @property
    def is_error_response(self) -> bool:
        if self.json_type == DurableAgentStateEntryJsonType.ERROR_RESPONSE:
            return True
        return self.json_type == DurableAgentStateEntryJsonType.RESPONSE and (
            (self.extension_data or {}).get("durable_status") == "error"
            or any(
                content.type == ContentTypes.ERROR
                for message in self.messages
                if message.role != "tool"
                for content in message.contents
            )
        )

    def to_dict(self) -> dict[str, Any]:
        projection = self._to_dict()
        return self._raw_shadow.merge(projection) if self._raw_shadow is not None else _json_snapshot(projection)

    def _capture_raw(self, data: dict[str, Any]) -> None:
        self.unknown_fields = _entry_unknown_fields(self, data)
        self._raw_shadow = _RawShadow(data, self._to_dict())

    def _to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            **deepcopy(self.unknown_fields),
            DurableStateFields.TYPE_DISCRIMINATOR: self.json_type,
            DurableStateFields.MESSAGES: [m.to_dict() for m in self.messages],
        }
        if self.created_at is not None:
            result[DurableStateFields.CREATED_AT] = self.created_at.isoformat()
        if self.correlation_id is not None:
            result[DurableStateFields.CORRELATION_ID] = self.correlation_id
        if self.extension_data is not None:
            result[DurableStateFields.EXTENSION_DATA] = deepcopy(self.extension_data)
        return _json_snapshot(result)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateEntry:
        data = deepcopy(data)
        created_at = _parse_transcript_created_at(data.get(DurableStateFields.CREATED_AT))
        messages = _parse_messages(data)

        entry = cls(
            json_type=DurableAgentStateEntryJsonType(data.get(DurableStateFields.TYPE_DISCRIMINATOR)),
            correlation_id=data.get(DurableStateFields.CORRELATION_ID),
            created_at=created_at,
            messages=messages,
            extension_data=data.get(DurableStateFields.EXTENSION_DATA),
        )
        entry._capture_raw(data)
        return entry


class DurableAgentStateUnknownEntry(DurableAgentStateEntry):
    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = deepcopy(raw)
        super().__init__(
            json_type=str(raw.get(DurableStateFields.TYPE_DISCRIMINATOR, "unknown")),
            correlation_id=raw.get(DurableStateFields.CORRELATION_ID),
            created_at=None,
            messages=[],
        )

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)


class DurableAgentStateRequest(DurableAgentStateEntry):
    response_type: str | None = None
    response_schema: dict[str, Any] | None = None
    orchestration_id: str | None = None

    def __init__(
        self,
        correlation_id: str | None,
        created_at: datetime | None,
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

    def _to_dict(self) -> dict[str, Any]:
        data = super()._to_dict()
        if self.orchestration_id is not None:
            data[DurableStateFields.ORCHESTRATION_ID] = self.orchestration_id
        if self.response_type is not None:
            data[DurableStateFields.RESPONSE_TYPE] = self.response_type
        if self.response_schema is not None:
            data[DurableStateFields.RESPONSE_SCHEMA] = deepcopy(self.response_schema)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateRequest:
        data = deepcopy(data)
        created_at = _parse_transcript_created_at(data.get(DurableStateFields.CREATED_AT))
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
        entry._capture_raw(data)
        return entry

    @staticmethod
    def from_run_request(request: RunRequest) -> DurableAgentStateRequest:
        context_messages = getattr(request, "context_messages", None)
        if context_messages is not None:
            messages = [DurableAgentStateMessage.from_core_dict(raw) for raw in context_messages]
        else:
            messages = [DurableAgentStateMessage.from_run_request(request)]

        return DurableAgentStateRequest(
            correlation_id=request.correlation_id,
            messages=messages,
            created_at=_parse_created_at(request.created_at),
            response_type=request.request_response_format,
            response_schema=serialize_response_format(request.response_format),
            orchestration_id=request.orchestration_id,
        )


class DurableAgentStateResponse(DurableAgentStateEntry):
    JSON_TYPE: ClassVar[DurableAgentStateEntryJsonType] = DurableAgentStateEntryJsonType.RESPONSE

    usage: DurableAgentStateUsage | None = None

    def __init__(
        self,
        correlation_id: str | None,
        created_at: datetime | None,
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

    def _to_dict(self) -> dict[str, Any]:
        data = super()._to_dict()
        if self.usage is not None:
            data[DurableStateFields.USAGE] = self.usage.to_dict()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateResponse:
        data = deepcopy(data)
        created_at = _parse_transcript_created_at(data.get(DurableStateFields.CREATED_AT))
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
        entry._capture_raw(data)
        return entry

    @classmethod
    def from_run_response(cls, correlation_id: str, response: AgentResponse) -> DurableAgentStateResponse:
        entry = cls(
            correlation_id=correlation_id,
            created_at=_parse_created_at(response.created_at),
            messages=[DurableAgentStateMessage.from_chat_message(m) for m in response.messages],
            usage=DurableAgentStateUsage.from_usage(response.usage_details),
            extension_data=deepcopy(response.additional_properties) if response.additional_properties else None,
        )
        entry.preserve_response_timestamp(response.created_at)
        return entry

    def preserve_response_timestamp(self, created_at: str | datetime | None) -> None:
        """Overlay a valid original response timestamp without rebuilding allocated messages."""
        original_datetime: datetime | None = None
        if isinstance(created_at, datetime):
            original_datetime = _parse_created_at(created_at)
            created_at = original_datetime.isoformat()
        if not isinstance(created_at, str):
            return
        try:
            validate_timestamp(created_at)
        except ValueError:
            return  # Keep the caller's existing fallback timestamp policy.
        raw = self.to_dict()
        raw[DurableStateFields.CREATED_AT] = created_at
        self.created_at = (
            original_datetime if original_datetime is not None else _parse_transcript_created_at(created_at)
        )
        self._capture_raw(raw)

    @staticmethod
    def to_run_response(response_entry: DurableAgentStateResponse) -> AgentResponse:
        messages = [m.to_chat_message() for m in response_entry.messages]
        usage_details = response_entry.usage.to_usage_details() if response_entry.usage is not None else UsageDetails()
        return AgentResponse(
            created_at=response_entry.to_dict().get(DurableStateFields.CREATED_AT),
            messages=messages,
            usage_details=usage_details,
            additional_properties=({
                **deepcopy(response_entry.extension_data or {}),
                **({"durable_status": "error"} if isinstance(response_entry, DurableAgentStateErrorResponse) else {}),
            }),
        )


class DurableAgentStateErrorResponse(DurableAgentStateResponse):
    JSON_TYPE: ClassVar[DurableAgentStateEntryJsonType] = DurableAgentStateEntryJsonType.ERROR_RESPONSE


class DurableAgentStateCompaction(DurableAgentStateEntry):
    def __init__(
        self,
        created_at: datetime | None,
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
        data = deepcopy(data)
        entry = cls(
            created_at=_parse_transcript_created_at(data.get(DurableStateFields.CREATED_AT)),
            messages=_parse_messages(data),
            correlation_id=data.get(DurableStateFields.CORRELATION_ID),
            extension_data=data.get(DurableStateFields.EXTENSION_DATA),
        )
        entry._capture_raw(data)
        return entry


class DurableAgentStateMessage:
    role: str
    contents: list[DurableAgentStateContent]
    author_name: str | None = None
    created_at: datetime | None = None
    message_id: str | None = None
    extension_data: dict[str, Any] | None = None
    ingestion_identity: str | None = None
    ingestion_occurrence: str | None = None
    _original_core_message: dict[str, Any] | None = None

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
        self.original_message_id: str | None = None
        self._has_original_message_id = False
        self._history_identity_profile: dict[str, Any] | None = None
        self._raw_shadow: _RawShadow | None = None

    @property
    def public_message_id(self) -> str | None:
        if self._has_original_message_id or self.original_message_id is not None:
            return self.original_message_id
        return self.message_id

    def set_history_id(self, history_id: str) -> None:
        self.validate_history_identity_update()
        self.original_message_id = self.public_message_id
        self._has_original_message_id = True
        self.message_id = history_id

    def validate_history_identity_update(self) -> None:
        """Reject identity replacement before touching opaque foreign profile fields."""
        if {"pythonHistoryId", "pythonHistoryIdentity"}.intersection(self.unknown_fields):
            raise ValueError("Cannot replace opaque foreign Python history identity metadata.")

    def to_dict(self) -> dict[str, Any]:
        projection = self._to_dict()
        return self._raw_shadow.merge(projection) if self._raw_shadow is not None else _json_snapshot(projection)

    def _to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            **deepcopy(self.unknown_fields),
            DurableStateFields.ROLE: self.role,
            DurableStateFields.CONTENTS: [c.to_persisted_dict() for c in self.contents],
        }
        if self.created_at is not None:
            result[DurableStateFields.CREATED_AT] = self.created_at.isoformat()
        if self.author_name is not None:
            result[DurableStateFields.AUTHOR_NAME] = self.author_name
        if self.public_message_id is not None:
            result[DurableStateFields.MESSAGE_ID] = self.public_message_id
        if self.message_id is not None and (self._has_original_message_id or self.original_message_id is not None):
            if not isinstance(self.message_id, str) or (
                self.original_message_id is not None and not isinstance(self.original_message_id, str)
            ):
                raise ValueError("Message identities must be strings when present.")
            if {"pythonHistoryId", "pythonHistoryIdentity"}.intersection(self.unknown_fields):
                raise ValueError("Cannot replace opaque foreign Python history identity metadata.")
            result["pythonHistoryId"] = self.message_id
            result["pythonHistoryIdentity"] = deepcopy(self._history_identity_profile or _HISTORY_IDENTITY_PROFILE)
        if self.extension_data is not None:
            result[DurableStateFields.EXTENSION_DATA] = self.extension_data
        return _json_snapshot(result)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableAgentStateMessage:
        data = deepcopy(data)
        created_at = _parse_transcript_created_at(data.get(DurableStateFields.CREATED_AT))

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
        if _has_python_profile(data.get("pythonHistoryIdentity"), _HISTORY_IDENTITY_PROFILE):
            history_id = data.get("pythonHistoryId")
            if not isinstance(history_id, str) or not history_id.strip():
                raise ValueError("The Python history-identity profile requires a nonblank pythonHistoryId.")
            message.set_history_id(history_id)
            message._history_identity_profile = deepcopy(data["pythonHistoryIdentity"])
            known.update(("pythonHistoryId", "pythonHistoryIdentity"))
        message.unknown_fields = {key: deepcopy(value) for key, value in data.items() if key not in known}
        message._raw_shadow = _RawShadow(data, message._to_dict())
        return message

    @property
    def text(self) -> str:
        text_parts: list[str] = []
        for content in self.contents:
            if isinstance(content, DurableAgentStateTextContent):
                text_parts.append(content.text or "")
        return "".join(text_parts)

    @staticmethod
    def from_run_request(request: RunRequest) -> DurableAgentStateMessage:
        return DurableAgentStateMessage(
            role=request.role,
            contents=[DurableAgentStateTextContent(text=request.message)],
            created_at=_parse_created_at(request.created_at) if request.created_at else None,
        )

    @staticmethod
    def from_core_dict(data: dict[str, Any]) -> DurableAgentStateMessage:
        raw = _json_snapshot(data)
        _validate_core_message(raw)
        _validate_core_message_keys(raw)
        message = load_agent_response({"messages": [raw]}).messages[0]
        preserve_input_envelope(message, raw)
        stored = DurableAgentStateMessage.from_chat_message(message)
        for content, original in zip(stored.contents, raw.get("contents", []), strict=True):
            if not isinstance(original, dict):
                raise ValueError("Core contents must contain content objects.")
            original = cast(dict[str, Any], original)
            if isinstance(content, DurableAgentStateUnknownContent):
                content.content = original
            else:
                mapped = content.core_projection()
                content.unknown_fields = {
                    "pythonCoreFields": {
                        **_CORE_FIELDS_PROFILE,
                        "fields": {key: value for key, value in original.items() if key not in mapped},
                    }
                }
        known = {"type", "role", "contents", "author_name", "message_id", "additional_properties"}
        stored.unknown_fields = {key: value for key, value in raw.items() if key not in known}
        stored._original_core_message = raw
        return stored

    @staticmethod
    def from_chat_message(chat_message: Message) -> DurableAgentStateMessage:
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
        current_payload = _json_snapshot(chat_message.to_dict())
        _validate_core_message_keys(current_payload)
        stored.unknown_fields = {key: value for key, value in current_payload.items() if key not in known}
        original = getattr(chat_message, "_durable_original_core_message", None)
        if isinstance(original, dict):
            from ._response_utils import _constructor_fields  # pyright: ignore[reportPrivateUsage]

            raw = _json_snapshot(original)
            _validate_core_message_keys(raw)
            message_fields = {*_constructor_fields(Message), "type"}
            stored.unknown_fields = {
                **{key: value for key, value in raw.items() if key not in message_fields},
                **stored.unknown_fields,
            }
        return stored

    def to_chat_message(self) -> Any:
        ai_contents = [c.to_core_content() for c in self.contents]

        kwargs: dict[str, Any] = {
            "role": self.role,
            "contents": ai_contents,
        }

        if self.author_name is not None:
            kwargs["author_name"] = self.author_name

        if self.public_message_id is not None:
            kwargs["message_id"] = self.public_message_id

        if self.extension_data is not None:
            kwargs["additional_properties"] = deepcopy(self.extension_data)

        return Message(**kwargs)


class DurableAgentStateDataContent(DurableAgentStateContent):
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
        return Content(type="data", uri=self.uri, media_type=self.media_type)


class DurableAgentStateErrorContent(DurableAgentStateContent):
    message: str | None = None
    error_code: str | None = None
    details: Any = None

    type: str = ContentTypes.ERROR
    _NULLABLE_FIELDS: ClassVar[frozenset[str]] = frozenset({DurableStateFields.DETAILS})

    def __init__(self, message: str | None = None, error_code: str | None = None, details: Any = None) -> None:
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
            message=content.message, error_code=content.error_code, details=deepcopy(content.error_details)
        )

    def to_ai_content(self) -> Content:
        return Content.from_error(
            message=self.message, error_code=self.error_code, error_details=deepcopy(self.details)
        )


class DurableAgentStateFunctionCallContent(DurableAgentStateContent):
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
        return Content.from_function_result(call_id=self.call_id, result=deepcopy(self.result))


class DurableAgentStateHostedFileContent(DurableAgentStateContent):
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
    def from_hosted_vector_store_content(content: Content) -> DurableAgentStateHostedVectorStoreContent:
        if content.vector_store_id is None:
            raise ValueError("vector_store_id is required for hosted vector store content")
        return DurableAgentStateHostedVectorStoreContent(vector_store_id=content.vector_store_id)

    def to_ai_content(self) -> Content:
        return Content.from_hosted_vector_store(vector_store_id=self.vector_store_id)


class DurableAgentStateTextContent(DurableAgentStateContent):
    type: str = ContentTypes.TEXT

    def __init__(self, text: str | None) -> None:
        self.text = text

    def to_dict(self) -> dict[str, Any]:
        return {DurableStateFields.TYPE_DISCRIMINATOR: self.type, DurableStateFields.TEXT: self.text}

    def to_persisted_dict(self) -> dict[str, Any]:
        if not isinstance(self.text, str):
            raise ValueError("Text content requires a text string for persistence.")
        return super().to_persisted_dict()

    @staticmethod
    def from_text_content(content: Content) -> DurableAgentStateTextContent:
        return DurableAgentStateTextContent(text=content.text)

    def to_ai_content(self) -> Content:
        return Content.from_text(text=self.text or "")


class DurableAgentStateTextReasoningContent(DurableAgentStateContent):
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
        return Content(type="uri", uri=self.uri, media_type=self.media_type)


class DurableAgentStateUsage:
    _INPUT_TOKEN_COUNT = "input_token_count"  # noqa: S105 - usage field name, not a credential
    _OUTPUT_TOKEN_COUNT = "output_token_count"  # noqa: S105 - usage field name, not a credential
    _TOTAL_TOKEN_COUNT = "total_token_count"  # noqa: S105 - usage field name, not a credential
    _STANDARD_USAGE_FIELDS: ClassVar[set[str]] = {
        _INPUT_TOKEN_COUNT,
        _OUTPUT_TOKEN_COUNT,
        _TOTAL_TOKEN_COUNT,
    }

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
        data = deepcopy(data)
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

        counts = {key: value for key, value in usage.items() if type(value) is int}
        extension_data: dict[str, Any] = {
            key: deepcopy(value)
            for key, value in usage.items()
            if key not in DurableAgentStateUsage._STANDARD_USAGE_FIELDS or key not in counts
        }

        return DurableAgentStateUsage(
            input_token_count=counts.get(DurableAgentStateUsage._INPUT_TOKEN_COUNT),
            output_token_count=counts.get(DurableAgentStateUsage._OUTPUT_TOKEN_COUNT),
            total_token_count=counts.get(DurableAgentStateUsage._TOTAL_TOKEN_COUNT),
            extensionData=extension_data if extension_data else None,
        )

    def to_usage_details(self) -> UsageDetails:
        return cast(
            UsageDetails,
            {
                **deepcopy(self.extensionData or {}),
                **{
                    key: value
                    for key, value in (
                        (self._INPUT_TOKEN_COUNT, self.input_token_count),
                        (self._OUTPUT_TOKEN_COUNT, self.output_token_count),
                        (self._TOTAL_TOKEN_COUNT, self.total_token_count),
                    )
                    if value is not None
                },
            },
        )


class DurableAgentStateUsageContent(DurableAgentStateContent):
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
            stored = DurableAgentStateUnknownContent(content=_json_snapshot(content.to_dict()))
            stored.unknown_fields = {"pythonContentEncoding": deepcopy(_CONTENT_ENCODING_PROFILE)}
            return stored
        return DurableAgentStateUnknownContent(content=content)

    def to_core_content(self) -> Content:
        return self.to_ai_content()

    def to_ai_content(self) -> Content:
        content_value: Any = self.content
        if _has_python_profile((self.unknown_fields or {}).get("pythonContentEncoding"), _CONTENT_ENCODING_PROFILE):
            if (
                not isinstance(content_value, dict)
                or not isinstance(cast(dict[str, Any], content_value).get("type"), str)
                or not content_value["type"]
            ):
                raise ValueError("The Python content profile requires a canonical Core content object.")
            return (
                load_agent_response({"messages": [{"role": "assistant", "contents": [content_value]}]})
                .messages[0]
                .contents[0]
            )
        return Content(type=self.type, additional_properties={"content": deepcopy(self.content)})  # type: ignore[arg-type]
