# Copyright (c) Microsoft. All rights reserved.

"""Standalone Core/shared terminal-response codec, without entity-state dependencies.

Dictionary inputs to the serializer are Core snapshots, not shared wire responses.
Explicit Core metadata maps to extensionData. Unmapped Core envelope fields use a
separate, identified pythonCoreFields profile, so metadata cannot overwrite aliases.
Unknown shared properties remain in a detached private shadow at their original
locations. An unchanged consumer projection re-encodes that shadow. Modifying a
loaded projection is rejected rather than silently losing unprojectable fields.
The owning state must retain its original JSON independently of this consumer.

Only this module's identified JSON-dictionary continuation profile is resumable.
Foreign bytes stay in _opaque_shared_continuation_token, not continuation_token.
Consumers requiring resumption must use require_resumable_continuation=True.
Custom non-JSON Core tokens are unsupported, never reflected or stringified.
Explicit unknown content is inert unless its wrapper identifies pythonContentEncoding
with profile agent-framework-python.content and integer version 1. That profile carries
canonical Core Content fields, projected by load_agent_response's fixed base constructors.
Only documented Content edges are traversed, never nested business JSON or runtime names.

Response additional_properties maps to the public extensionData contract. Empty, blank,
control-bearing, or longer-than-256 keys are rejected even though Core accepts them.
This restriction does not apply to keys inside business JSON or content metadata.
The parent serializer resolves lazy structured values before passing a Core snapshot.
Instance snapshots here deliberately do not trigger lazy value parsing.
Core serializers determine field presence, not the audit of runtime attributes.
Attached Core input envelopes and shared shadows retain explicit original presence.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
import re
from datetime import datetime
from typing import Any, cast

from agent_framework import AgentResponse, Content, Message
from pydantic import BaseModel

from ._response_utils import (
    _constructor_fields,  # pyright: ignore[reportPrivateUsage]
    load_agent_response,
    serialize_agent_response,
    serialize_input_content,
)
from ._shared_state_validation import validate_timestamp

_CORE_FIELDS = "pythonCoreFields"
_CORE_PROFILE = "agent-framework-python.core-fields"
_CONTENT_ENCODING = "pythonContentEncoding"
_CONTENT_PROFILE: dict[str, Any] = {"profile": "agent-framework-python.content", "version": 1}
_TOKEN_ENCODING = "pythonContinuationEncoding"  # noqa: S105 - JSON property name, not a credential.
_TOKEN_PROFILE: dict[str, Any] = {
    "profile": "agent-framework-python.continuation",
    "version": 1,
    "format": "json",
}
_RESPONSE_FIELDS = {
    "messages": "messages",
    "value": "value",
    "usage_details": "usage",
    "created_at": "createdAt",
    "response_id": "responseId",
    "agent_id": "agentId",
    "finish_reason": "finishReason",
    "continuation_token": "continuationToken",
    "additional_properties": "extensionData",
}
_MESSAGE_FIELDS = {
    "role": "role",
    "contents": "contents",
    "author_name": "authorName",
    "message_id": "messageId",
    "created_at": "createdAt",
    "additional_properties": "extensionData",
}
_USAGE_FIELDS = {
    "input_token_count": "inputTokenCount",
    "output_token_count": "outputTokenCount",
    "total_token_count": "totalTokenCount",
}
# Each row owns only that content kind's fields. Everything else is a Core extra.
_CONTENT_FIELDS: dict[str, tuple[str, dict[str, str], tuple[str, ...]]] = {
    "data": ("data", {"uri": "uri", "media_type": "mediaType"}, ("uri",)),
    "error": ("error", {"message": "message", "error_code": "errorCode", "error_details": "details"}, ()),
    "function_call": (
        "functionCall",
        {"call_id": "callId", "name": "name", "arguments": "arguments"},
        ("callId", "name"),
    ),
    "function_result": ("functionResult", {"call_id": "callId", "result": "result"}, ("callId",)),
    "hosted_file": ("hostedFile", {"file_id": "fileId"}, ("fileId",)),
    "hosted_vector_store": ("hostedVectorStore", {"vector_store_id": "vectorStoreId"}, ("vectorStoreId",)),
    "usage": ("usage", {"usage_details": "usage"}, ("usage",)),
    "text": ("text", {"text": "text"}, ("text",)),
    "text_reasoning": ("reasoning", {"text": "text"}, ()),
    "uri": ("uri", {"uri": "uri", "media_type": "mediaType"}, ("uri",)),
}
_WIRE_CONTENT = {wire: (core, fields, required) for core, (wire, fields, required) in _CONTENT_FIELDS.items()}
_ROLES = {"user", "assistant", "system", "developer", "tool"}
_CONTROLS = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_BASE64 = re.compile(r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?")


def _json_copy(value: Any) -> Any:
    """Copy JSON without normalizing keys, containers, numbers, or unknown objects."""
    try:
        if isinstance(value, dict):
            result: dict[str, Any] = {}
            for key, item in cast(dict[Any, Any], value).items():
                if not isinstance(key, str):
                    raise ValueError("JSON object keys must be strings.")
                result[key] = _json_copy(item)
            return result
        if isinstance(value, list):
            return [_json_copy(item) for item in cast(list[Any], value)]
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, float) and math.isfinite(value):
            return value
    except RecursionError:
        raise ValueError("Terminal response JSON must be acyclic and within the supported depth.") from None
    raise ValueError("Terminal responses require JSON values with finite numbers.")


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object in a terminal response.")
    return cast(dict[str, Any], value)


def _array(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError("Expected a JSON array in a terminal response.")
    return cast(list[Any], value)


def _identifier(value: Any) -> None:
    if not isinstance(value, str) or not 1 <= len(value) <= 256 or not value.strip() or _CONTROLS.search(value):
        raise ValueError("Terminal identifiers must be nonblank, control-free strings of at most 256 code points.")


def _timestamp(value: Any) -> None:
    # Python 3.10's fromisoformat rejects some otherwise valid fractional lengths.
    # Validate the calendar without projecting or truncating the original value.
    validate_timestamp(value, "createdAt")


def _validate_usage(value: Any) -> None:
    usage = _object(value)
    for name in _USAGE_FIELDS.values():
        if name in usage and (isinstance(usage[name], bool) or not isinstance(usage[name], (int, float))):
            raise ValueError("Shared token counts must be integers.")
        if name in usage and isinstance(usage[name], float) and not usage[name].is_integer():
            raise ValueError("Shared token counts must be integers.")
    if "extensionData" in usage:
        _object(usage["extensionData"])


def _validate_content(value: Any) -> None:
    content = _object(value)
    kind = content.get("$type")
    if kind == "unknown":
        if "content" not in content:
            raise ValueError("Explicit unknown content requires its opaque content value.")
        return
    if not isinstance(kind, str) or kind not in _WIRE_CONTENT:
        raise ValueError("Unsupported shared content discriminator.")
    _, fields, required = _WIRE_CONTENT[kind]
    if any(name not in content for name in required):
        raise ValueError("Shared content is missing a required field.")
    for name in fields.values():
        if name not in content or name in ("result", "details"):
            continue
        value = content[name]
        if name == "usage":
            _validate_usage(value)
        elif name == "arguments":
            if not isinstance(value, (dict, str)):
                raise ValueError("Function arguments must be an object or the original string.")
        elif not isinstance(value, str):
            raise ValueError("Known shared content text fields must be strings.")


def _validate_response(payload: dict[str, Any]) -> None:
    for message_value in _array(payload.get("messages")):
        message = _object(message_value)
        role = message.get("role")
        if not isinstance(role, str) or role not in _ROLES:
            raise ValueError("Unsupported shared message role.")
        if "contents" in message:
            for content in _array(message["contents"]):
                _validate_content(content)
        for name in ("authorName", "messageId"):
            if name in message and not isinstance(message[name], str):
                raise ValueError("Shared message authorName and messageId must be strings.")
        if "createdAt" in message:
            _timestamp(message["createdAt"])
        if "extensionData" in message:
            _object(message["extensionData"])
    for name in ("responseId", "agentId", "finishReason"):
        if name in payload:
            _identifier(payload[name])
    if "createdAt" in payload:
        _timestamp(payload["createdAt"])
    if "usage" in payload:
        _validate_usage(payload["usage"])
    if "extensionData" in payload:
        for name in _object(payload["extensionData"]):
            _identifier(name)
    if "continuationToken" in payload:
        token = payload["continuationToken"]
        if not isinstance(token, str) or len(token) > 16384 or not _BASE64.fullmatch(token):
            raise ValueError("continuationToken must be base64 with at most 16384 characters.")


def validate_terminal_response(payload: dict[str, Any]) -> None:
    """Validate shared terminal-response JSON without constructing runtime objects.

    This checks the shared shape and JSON values, not whether optional Python profiles
    can be loaded or resumed. Unknown fields and profile payloads remain inert.

    Args:
        payload: Shared terminalResponse JSON. The input is not modified.

    Raises:
        ValueError: If the payload is not JSON or violates known shared fields.
    """
    _validate_response(_object(_json_copy(payload)))


def _pack_fields(source: dict[str, Any], fields: dict[str, str]) -> dict[str, Any]:
    target = {wire: source[core] for core, wire in fields.items() if core in source}
    extras = {key: value for key, value in source.items() if key not in fields and key != "type"}
    if extras:
        target[_CORE_FIELDS] = {"profile": _CORE_PROFILE, "version": 1, "fields": extras}
    return target


def _has_profile(value: Any, name: str) -> bool:
    if not isinstance(value, dict):
        return False
    profile = _object(value)
    return profile.get("profile") == name and type(profile.get("version")) is int and profile["version"] == 1


def _unpack_fields(source: dict[str, Any], fields: dict[str, str]) -> dict[str, Any]:
    target: dict[str, Any] = {}
    profile = source.get(_CORE_FIELDS)
    if _has_profile(profile, _CORE_PROFILE):
        extras = _object(_object(profile).get("fields"))
        if extras.keys() & (fields.keys() | {"type", "raw_representation", "response_format"}):
            raise ValueError("Core field metadata cannot override known fields.")
        target.update(extras)
    target.update({core: source[wire] for core, wire in fields.items() if wire in source})
    return target


def _usage_to_shared(value: Any) -> dict[str, Any]:
    source = _object(value)
    target: dict[str, Any] = {}
    extras: dict[str, Any] = {}
    for name, item in source.items():
        # Non-numeric provider data, including null counts, remains explicit metadata.
        if name in _USAGE_FIELDS and type(item) is int:
            target[_USAGE_FIELDS[name]] = item
        else:
            extras[name] = item
    if extras:
        target["extensionData"] = extras
    return target


def _usage_to_core(value: Any) -> dict[str, Any]:
    source = _object(value)
    target = dict(source.get("extensionData", {}))
    # Typed wire counts win in the consumer projection, never over the original JSON.
    target.update({core: source[wire] for core, wire in _USAGE_FIELDS.items() if wire in source})
    return target


def _content_to_shared(value: Any) -> dict[str, Any]:
    source = _object(value)
    if "additional_properties" in source:
        _object(source["additional_properties"])
    kind = source.get("type")
    if not isinstance(kind, str) or not kind:
        raise ValueError("Core content requires a non-empty type.")
    if kind not in _CONTENT_FIELDS:
        # The shared contract's explicit unknown wrapper carries unmodeled Core kinds.
        # The profile identifies constructor fields, not classes or importable names.
        return {"$type": "unknown", "content": source, _CONTENT_ENCODING: dict(_CONTENT_PROFILE)}
    wire, fields, _ = _CONTENT_FIELDS[kind]
    result = _pack_fields(source, {**fields, "additional_properties": "extensionData"})
    result["$type"] = wire
    if "usage" in result:
        result["usage"] = _usage_to_shared(result["usage"])
    return result


def _content_to_core(value: Any) -> dict[str, Any]:
    source = _object(value)
    if source["$type"] == "unknown":
        if _has_profile(source.get(_CONTENT_ENCODING), _CONTENT_PROFILE["profile"]):
            # Leave canonical nested Content edges to the existing fixed-field loader.
            # Do not search for shared wrappers or profiles inside business values.
            return _object(source["content"])
        return {"type": "unknown", "additional_properties": {"content": source["content"]}}
    core, fields, _ = _WIRE_CONTENT[source["$type"]]
    result = _unpack_fields(source, fields)
    if "additional_properties" in result:
        raise ValueError("Core field metadata cannot override known fields.")
    # Content extensionData is an unknown shared property and may be any JSON value.
    if isinstance(source.get("extensionData"), dict):
        result["additional_properties"] = source["extensionData"]
    result["type"] = core
    if core == "usage" and "usage" in source:
        result["usage_details"] = _usage_to_core(source["usage"])
    return result


def _content_snapshot(content: Content) -> dict[str, Any]:
    # Audit the explicit Core envelope before invoking its serializer, which otherwise
    # skips some unsupported values. Auditing must not reintroduce omitted defaults.
    # Only documented Content edges recurse as Content.
    known = _constructor_fields(Content)
    for name, value in vars(content).items():
        if name.startswith("_") or name == "raw_representation" or (value is None and name in known):
            continue
        if name == "function_call" and isinstance(value, Content):
            _content_snapshot(value)
        elif name == "annotations" and isinstance(value, (list, tuple)):
            _json_copy(list(cast(list[Any], value)))
        elif name in ("items", "inputs") or (
            name == "outputs" and content.type in ("code_interpreter_tool_result", "shell_tool_result")
        ):
            if not isinstance(value, (list, tuple)):
                raise ValueError("Nested Core content requires a sequence.")
            for item in cast(list[Any], value):
                if isinstance(item, Content):
                    _content_snapshot(item)
                else:
                    _json_copy(item)
        else:
            _json_copy(value)
    # This serializer also restores inert fields from an attached Core input envelope.
    # In particular, a bare result=None has no presence bit. Do not invent one here.
    return _object(_json_copy(serialize_input_content(content)))


def _message_snapshot(message: Message) -> dict[str, Any]:
    known = _constructor_fields(Message)
    for name, value in vars(message).items():
        if name.startswith("_") or name in ("contents", "raw_representation"):
            continue
        _json_copy(value)
    contents = [_content_snapshot(content) for content in message.contents]
    result = Message.to_dict(message)
    raw = getattr(message, "_durable_original_core_message", None)
    if isinstance(raw, dict):
        retained = {
            name: value
            for name, value in _object(raw).items()
            if name != "raw_representation" and (name not in known or getattr(message, name, object()) == value)
        }
        result = {**retained, **result}
    result["contents"] = contents
    return _object(_json_copy(result))


def _core_snapshot(response: AgentResponse) -> dict[str, Any]:
    # Use the canonical serializer on a base object with no response format. Never
    # evaluate the source's lazy value or execute its subclass serializer/getter.
    value = response._value  # pyright: ignore[reportPrivateUsage]
    if not isinstance(value, BaseModel):
        _json_copy(value)
    messages = [_message_snapshot(message) for message in response.messages]
    base = AgentResponse(value=value)
    base._value_parsed = response._value_parsed  # pyright: ignore[reportPrivateUsage]
    if getattr(response, "_durable_value_by_name", False):
        base._durable_value_by_name = True  # type: ignore[attr-defined]
    for name, value in vars(response).items():
        if name.startswith("_") or name in ("messages", "raw_representation", "response_format"):
            continue
        # Core accepts datetime here. Its serializer still owns whether/how it emits it.
        _json_copy(value.isoformat() if name == "created_at" and isinstance(value, datetime) else value)
        vars(base)[name] = value
    base.messages = response.messages
    result = serialize_agent_response(base)
    result["messages"] = messages
    return _object(_json_copy(result))


def _same_projection(left: dict[str, Any], right: dict[str, Any]) -> bool:
    # Python equality conflates False, zero and floats. JSON comparison does not.
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(right, sort_keys=True, allow_nan=False)


def serialize_terminal_response(response: AgentResponse | dict[str, Any]) -> dict[str, Any]:
    """Produce a detached shared terminalResponse from an instance or canonical Core snapshot.

    Response metadata keys must satisfy the shared extensionData identifier constraints,
    even if accepted by Core. Pass the parent's serialize_agent_response snapshot when
    lazy structured values need resolution before conversion. Otherwise Core's canonical
    serialization controls instance field presence, with attached input envelopes retained.
    An unchanged loaded projection re-encodes its detached original shared JSON, including
    fields and profiles that Core cannot project.

    Raises:
        ValueError: For non-JSON data, malformed known fields, unsupported Core tokens,
            unsupported response metadata keys, or a modified loaded projection whose
            unknown fields cannot be safely rebased.
    """
    if isinstance(response, AgentResponse):
        source = _core_snapshot(response)
        original = getattr(response, "_original_shared_response", None)
        if original is not None:
            projection = getattr(response, "_original_shared_response_core_projection", None)
            if not isinstance(projection, dict) or not _same_projection(source, _object(projection)):
                raise ValueError("Cannot losslessly re-encode a modified shared response projection.")
            result = _object(_json_copy(original))
            _validate_response(result)
            return result
    else:
        source = _object(_json_copy(response))
        if source.get("type") != "agent_response":
            raise ValueError("Dictionary inputs must be canonical Core agent_response snapshots, not shared JSON.")
    for name in ("raw_representation", "response_format"):
        source.pop(name, None)
    result = _pack_fields(source, _RESPONSE_FIELDS)
    messages: list[dict[str, Any]] = []
    result["messages"] = messages
    for raw in _array(source.get("messages", [])):
        message = _object(raw)
        mapped = _pack_fields(message, _MESSAGE_FIELDS)
        if "contents" in message:
            mapped["contents"] = [_content_to_shared(item) for item in _array(message["contents"])]
        messages.append(mapped)
    if "usage_details" in source:
        result["usage"] = _usage_to_shared(source["usage_details"])
    if "continuation_token" in source:
        token = _object(source["continuation_token"])
        result["continuationToken"] = base64.b64encode(
            json.dumps(token, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
        result[_TOKEN_ENCODING] = dict(_TOKEN_PROFILE)
    _validate_response(result)
    return _object(_json_copy(result))


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate continuation JSON keys are not supported.")
        result[key] = value
    return result


def _read_token(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, bytes | None]:
    if "continuationToken" not in payload:
        return None, None
    try:
        raw = base64.b64decode(payload["continuationToken"], validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("Invalid base64 continuationToken.") from None
    profile = payload.get(_TOKEN_ENCODING)
    if not (_has_profile(profile, _TOKEN_PROFILE["profile"]) and _object(profile).get("format") == "json"):
        return None, raw
    try:
        token = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        return _object(_json_copy(token)), None
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("Invalid JSON dictionary in the recognized Python continuation profile.") from None


def load_terminal_response(payload: dict[str, Any], *, require_resumable_continuation: bool = False) -> AgentResponse:
    """Project shared JSON into base Core types without activating stored type names.

    Foreign continuation bytes are private inert data, never a Core resumable token.
    Set require_resumable_continuation when relying on a token for restoration. This
    requires recognition of this codec, not proof that a configured provider accepts it.
    Only recognized pythonContentEncoding wrappers project canonical Core Content fields,
    including approval requests and nested tool content. Other unknown wrappers remain
    Content(type="unknown") and are not actionable tool requests. Profile recognition is
    an encoding check, not authorization to execute a tool or approve a request.
    """
    raw = _object(_json_copy(payload))
    _validate_response(raw)
    source = _unpack_fields(raw, _RESPONSE_FIELDS)
    source.pop("continuation_token", None)
    token, opaque = _read_token(raw)
    if require_resumable_continuation and opaque is not None:
        raise ValueError("Cannot resume an unrecognized continuation encoding profile.")
    if token is not None:
        source["continuation_token"] = token
    source["type"] = "agent_response"
    messages: list[dict[str, Any]] = []
    source["messages"] = messages
    for value in raw["messages"]:
        message = _unpack_fields(value, _MESSAGE_FIELDS)
        message["contents"] = [_content_to_core(item) for item in value.get("contents", [])]
        # Core currently has no message created_at field. Keep it in the raw shadow.
        messages.append({key: item for key, item in message.items() if key in _constructor_fields(Message)})
    if "usage" in raw:
        source["usage_details"] = _usage_to_core(raw["usage"])
    response = load_agent_response(source)
    response._original_shared_response = _json_copy(raw)  # type: ignore[attr-defined]
    response._opaque_shared_continuation_token = opaque  # type: ignore[attr-defined]
    response._original_shared_response_core_projection = _core_snapshot(response)  # type: ignore[attr-defined]
    return response


def terminal_error(response: AgentResponse) -> dict[str, Any]:
    """Return bounded text-only failure metadata, never exceptions or opaque details.

    Select only non-tool errors. Hosts remain responsible for redacting provider text
    before persistence. This function enforces shape and control/length limits, not
    application-specific secret detection, and does not classify the invocation outcome.
    """
    code: Any = None
    message: Any = None
    for item in response.messages:
        if item.role == "tool":
            continue
        for content in item.contents:
            if content.type == "error":
                code, message = content.error_code, content.message
                break
        else:
            continue
        break
    safe_code = _CONTROLS.sub("", code).strip()[:256] if isinstance(code, str) else ""
    safe_message = _CONTROLS.sub(" ", message).strip()[:16384] if isinstance(message, str) else ""
    return {"code": safe_code or "agent_error", "message": safe_message or "The agent invocation failed."}
