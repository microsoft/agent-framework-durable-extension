# Copyright (c) Microsoft. All rights reserved.

"""Validate canonical shared state without loading schemas or runtime projections.

Validation is read-only. Unknown JSON stays at its original location, and optional
fields are checked by presence, never filled in. Only explicitly declared metadata
objects have object constraints. Opaque profiles, tokens, and content are not
activated or interpreted as runtime types.

This checks one snapshot, not deployment authorization, migration evidence,
immutability across operations, atomic commits, or retention/lookup policy. Error
content classification is a separate producer/source-aware responsibility.
RFC 3339 leap seconds are unsupported. Fractional seconds otherwise compare
exactly, without datetime microsecond truncation or Decimal context rounding.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

_VERSIONS = ("1.0.0", "1.1.0", "1.2.0", "2.0.0")
_LEGACY_ROLES = ("user", "assistant", "system", "tool")
_V2_ROLES = (*_LEGACY_ROLES, "developer")
_ENTRY_TYPES = ("request", "response", "errorResponse", "compaction")
_OUTCOMES = ("succeeded", "failed")
_CONTROLS = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_NONBLANK = re.compile(r"\S")
_BASE64 = re.compile(r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?")
_TIMESTAMP = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]+))?([Zz]|[+-]([0-9]{2}):([0-9]{2}))"
)
# Each discriminator owns only its required fields and optional/required strings.
# Other siblings, including content-level extensionData, remain arbitrary JSON.
_CONTENT_FIELDS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "data": (("uri",), ("uri", "mediaType")),
    "error": ((), ("message", "errorCode")),
    "functionCall": (("callId", "name"), ("callId", "name")),
    "functionResult": (("callId",), ("callId",)),
    "hostedFile": (("fileId",), ("fileId",)),
    "hostedVectorStore": (("vectorStoreId",), ("vectorStoreId",)),
    "usage": (("usage",), ()),
    "text": (("text",), ("text",)),
    "reasoning": ((), ("text",)),
    "uri": (("uri",), ("uri", "mediaType")),
    "unknown": (("content",), ()),
}

# Whole UTC seconds and an exact nonnegative fractional second. Keeping the two
# separate avoids precision-dependent Decimal arithmetic, even for long fractions.
_Instant = tuple[int, Decimal]


def validate_identifier(value: Any, name: str = "identifier") -> None:
    """Require a nonblank, C0/C1-control-free string of 1 to 256 code points.

    Raises:
        ValueError: If the identifier violates the shared contract.
    """
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 256
        or not _NONBLANK.search(value)
        or _CONTROLS.search(value)
    ):
        raise ValueError(f"{name} must be a nonblank, control-free string of 1 to 256 code points.")


def _json_value(value: Any) -> None:
    # An iterative, non-copying walk rejects cycles but permits shared subtrees.
    # No payload values or caller-controlled object keys enter error messages.
    stack: list[tuple[Iterator[Any], int | None]] = [(iter((value,)), None)]
    active: set[int] = set()
    while stack:
        iterator, identity = stack[-1]
        try:
            item = next(iterator)
        except StopIteration:
            stack.pop()
            if identity is not None:
                active.remove(identity)
            continue
        if item is None or isinstance(item, (str, bool, int)):
            continue
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("Shared state JSON numbers must be finite.")
            continue
        if isinstance(item, dict):
            obj = cast(dict[Any, Any], item)
            if any(not isinstance(key, str) for key in obj):
                raise ValueError("Shared state JSON object keys must be strings.")
            children = iter(obj.values())
        elif isinstance(item, list):
            children = iter(cast(list[Any], item))
        else:
            raise ValueError("Shared state requires JSON values, not runtime objects.")
        identity = id(cast(object, item))
        if identity in active:
            raise ValueError("Shared state JSON must not contain cycles.")
        active.add(identity)
        stack.append((children, identity))


def _object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object.")
    return cast(dict[str, Any], value)


def _array(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a JSON array.")
    return cast(list[Any], value)


def _required(obj: dict[str, Any], fields: tuple[str, ...], name: str) -> None:
    for field in fields:
        if field not in obj:
            raise ValueError(f"{name} requires {field}.")


def _strings(obj: dict[str, Any], fields: tuple[str, ...], name: str) -> None:
    for field in fields:
        if field in obj and not isinstance(obj[field], str):
            raise ValueError(f"{name}.{field} must be a string when present.")


def _enum(value: Any, choices: tuple[str, ...], name: str) -> str:
    if not isinstance(value, str) or value not in choices:
        raise ValueError(f"{name} is missing or unsupported.")
    return value


def _integer(value: Any, name: str, minimum: int | None = None) -> None:
    # JSON Schema integer includes integral floats, but excludes booleans.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be an integer.")
    if isinstance(value, float) and (not math.isfinite(value) or not value.is_integer()):
        raise ValueError(f"{name} must be a finite integer.")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} is below its minimum.")


def _timestamp(value: Any, name: str) -> _Instant:
    message = f"{name} must be an offset-bearing RFC 3339 timestamp (leap seconds unsupported)."
    if not isinstance(value, str):
        raise ValueError(message)
    match = _TIMESTAMP.fullmatch(value)
    if match is None:
        raise ValueError(message)
    try:
        date = datetime(int(match[1]), int(match[2]), int(match[3]), int(match[4]), int(match[5]), int(match[6]))
    except ValueError:
        raise ValueError(message) from None
    offset = 0
    if match[8] not in ("Z", "z"):
        hours, minutes = int(match[9]), int(match[10])
        if hours > 23 or minutes > 59:
            raise ValueError(message)
        offset = (hours * 60 + minutes) * 60
        if match[8][0] == "-":
            offset = -offset
    seconds = date.toordinal() * 86400 + date.hour * 3600 + date.minute * 60 + date.second - offset
    return seconds, Decimal("0." + (match[7] or "0"))


def validate_timestamp(value: Any, name: str = "timestamp") -> None:
    """Check the shared timestamp contract independently of Python parser precision."""
    _timestamp(value, name)


def timestamp_reached(deadline: str, *, now: datetime) -> bool:
    """Compare a read clock with a shared deadline without rounding fractional seconds."""
    return _timestamp(now.isoformat(), "now") >= _timestamp(deadline, "deadline")


def _usage(value: Any) -> None:
    usage = _object(value, "usage")
    for field in ("inputTokenCount", "outputTokenCount", "totalTokenCount"):
        if field in usage:
            _integer(usage[field], f"usage.{field}")
    if "extensionData" in usage:
        _object(usage["extensionData"], "usage.extensionData")


def _content(value: Any, *, v2: bool) -> None:
    content = _object(value, "content")
    kind = content.get("$type")
    if not isinstance(kind, str) or kind not in _CONTENT_FIELDS:
        raise ValueError("Content $type is missing or unsupported.")
    required, strings = _CONTENT_FIELDS[kind]
    _required(content, required, "content")
    _strings(content, strings, "content")
    if kind == "functionCall" and "arguments" in content:
        arguments = content["arguments"]
        if not isinstance(arguments, dict) and not (v2 and isinstance(arguments, str)):
            raise ValueError("Function arguments must be an object, or an original string in schema 2.0.0.")
    if kind == "uri" and not v2:
        _required(content, ("mediaType",), "legacy URI content")
    if kind == "usage":
        _usage(content["usage"])


def _messages(value: Any, *, v2: bool) -> None:
    for item in _array(value, "messages"):
        message = _object(item, "message")
        _enum(message.get("role"), _V2_ROLES if v2 else _LEGACY_ROLES, "message.role")
        _strings(message, ("authorName", "messageId"), "message")
        if "createdAt" in message:
            _timestamp(message["createdAt"], "message.createdAt")
        if "extensionData" in message:
            _object(message["extensionData"], "message.extensionData")
        if "contents" in message:
            for content in _array(message["contents"], "message.contents"):
                _content(content, v2=v2)


def _conversation(value: Any, *, v2: bool) -> None:
    for item in _array(value, "conversationHistory"):
        entry = _object(item, "conversation entry")
        # Historical entries need not have a discriminator, and unrecognized
        # siblings such as request/response-specific fields are not reinterpreted.
        kind = _enum(entry.get("$type"), _ENTRY_TYPES, "entry.$type") if v2 else None
        if "correlationId" in entry:
            if kind == "compaction":
                raise ValueError("Compaction must not contain correlationId, including null.")
            if v2:
                validate_identifier(entry["correlationId"], "entry.correlationId")
            else:
                _strings(entry, ("correlationId",), "entry")
        if "createdAt" in entry:
            _timestamp(entry["createdAt"], "entry.createdAt")
        if "messages" in entry:
            _messages(entry["messages"], v2=v2)
        if "extensionData" in entry:
            _object(entry["extensionData"], "entry.extensionData")
        if kind == "request":
            _strings(entry, ("orchestrationId", "responseType"), "entry")
            if "responseSchema" in entry:
                _object(entry["responseSchema"], "entry.responseSchema")
        elif kind in ("response", "errorResponse") and "usage" in entry:
            _usage(entry["usage"])


def _response(value: Any) -> None:
    # Validate wire shape only. Do not construct a consumer or decode a token,
    # which would impose a runtime profile policy on otherwise opaque JSON.
    response = _object(value, "terminal response")
    _required(response, ("messages",), "terminal response")
    _messages(response["messages"], v2=True)
    for field in ("responseId", "agentId", "finishReason"):
        if field in response:
            validate_identifier(response[field], f"response.{field}")
    if "createdAt" in response:
        _timestamp(response["createdAt"], "response.createdAt")
    if "usage" in response:
        _usage(response["usage"])
    if "extensionData" in response:
        for key in _object(response["extensionData"], "response.extensionData"):
            validate_identifier(key, "response.extensionData key")
    if "continuationToken" in response:
        token = response["continuationToken"]
        if not isinstance(token, str) or len(token) > 16384 or _BASE64.fullmatch(token) is None:
            raise ValueError("response.continuationToken must be base64 with at most 16384 characters.")


def _error(value: Any) -> None:
    error = _object(value, "terminal error")
    _required(error, ("code", "message"), "terminal error")
    validate_identifier(error["code"], "error.code")
    message = error["message"]
    if not isinstance(message, str) or not 1 <= len(message) <= 16384 or not _NONBLANK.search(message):
        raise ValueError("error.message must be a nonblank string of 1 to 16384 code points.")


def _completion(value: Any, key: str, name: str) -> tuple[dict[str, Any], _Instant, _Instant | None]:
    record = _object(value, name)
    _required(record, ("correlationId", "outcome", "completedAt"), name)
    validate_identifier(record["correlationId"], f"{name}.correlationId")
    if record["correlationId"] != key:
        raise ValueError(f"{name}.correlationId must equal its map key exactly.")
    _enum(record["outcome"], _OUTCOMES, f"{name}.outcome")
    completed = _timestamp(record["completedAt"], f"{name}.completedAt")
    expires = None
    if "resultExpiresAt" in record:
        expires = _timestamp(record["resultExpiresAt"], f"{name}.resultExpiresAt")
        if expires < completed:
            raise ValueError(f"{name}.resultExpiresAt must not precede completedAt.")
    return record, completed, expires


def _mailbox(data: dict[str, Any]) -> None:
    results = _object(data["terminalResults"], "terminalResults")
    receipts = _object(data["completionReceipts"], "completionReceipts")
    for key in results:
        validate_identifier(key, "terminalResults key")
        if key not in receipts:
            raise ValueError("Every terminal result requires a matching completion receipt.")
    for key, value in receipts.items():
        validate_identifier(key, "completionReceipts key")
        receipt, completed, expires = _completion(value, key, "receipt")
        availability = _enum(receipt.get("resultState"), ("available", "unavailable"), "receipt.resultState")
        if availability == "unavailable":
            if key in results:
                raise ValueError("An unavailable receipt must not have a terminal result.")
            _required(receipt, ("resultUnavailableAt",), "receipt")
            unavailable = _timestamp(receipt["resultUnavailableAt"], "receipt.resultUnavailableAt")
            if unavailable < completed or (expires is not None and unavailable < expires):
                raise ValueError("resultUnavailableAt must not precede completion or stored expiry.")
            continue
        if "resultUnavailableAt" in receipt:
            raise ValueError("An available receipt must not contain resultUnavailableAt.")
        if key not in results:
            raise ValueError("An available receipt requires a matching terminal result.")
        result, result_completed, result_expires = _completion(results[key], key, "result")
        if result["outcome"] != receipt["outcome"] or result_completed != completed or result_expires != expires:
            raise ValueError(
                "Result and receipt must agree on outcome, completion instant, and optional expiry instant."
            )
        _required(result, ("response",), "result")
        _response(result["response"])
        if result["outcome"] == "failed":
            _required(result, ("error",), "failed result")
            _error(result["error"])
        elif "error" in result:
            raise ValueError("A succeeded result must not contain error, including null.")


def _data(data: dict[str, Any], version: str) -> None:
    v2 = version == "2.0.0"
    if v2:
        _required(data, ("conversationHistory", "terminalResults", "completionReceipts"), "data")
    elif any(field in data for field in ("terminalResults", "completionReceipts", "historyBinding")):
        raise ValueError("Legacy state must not contain terminalResults, completionReceipts, or historyBinding.")
    if "conversationHistory" in data:
        _conversation(data["conversationHistory"], v2=v2)
    for field in ("session", "extensionData"):
        if field in data:
            _object(data[field], f"data.{field}")
    if "expirationTimeUtc" in data and data["expirationTimeUtc"] is not None:
        _timestamp(data["expirationTimeUtc"], "data.expirationTimeUtc")
    if "ingestedPositions" in data:
        for position in _object(data["ingestedPositions"], "data.ingestedPositions").values():
            _integer(position, "ingested position", minimum=0)
    if "truncation" in data:
        truncation = _object(data["truncation"], "data.truncation")
        _required(truncation, ("evictedMessageCount", "firstEvictedAt", "lastEvictedAt"), "truncation")
        _integer(truncation["evictedMessageCount"], "truncation.evictedMessageCount", minimum=1)
        first = _timestamp(truncation["firstEvictedAt"], "truncation.firstEvictedAt")
        last = _timestamp(truncation["lastEvictedAt"], "truncation.lastEvictedAt")
        if last < first:
            raise ValueError("truncation.lastEvictedAt must not precede firstEvictedAt.")
    if v2:
        _mailbox(data)


def validate_shared_data(data: dict[str, Any], *, version: str = "2.0.0") -> None:
    """Validate data shape and snapshot semantics for an exact shared version.

    Values are neither normalized nor copied. In v2, historyBinding is any JSON,
    including null, and is not approved as a usable runtime profile by this check.

    Raises:
        ValueError: For unsupported versions, non-JSON input, malformed known
            fields, or inconsistent mailbox/truncation timestamps and identities.
    """
    version = _enum(version, _VERSIONS, "schemaVersion")
    data = _object(data, "data")
    _json_value(data)
    _data(data, version)


def validate_shared_state(state: dict[str, Any]) -> None:
    """Validate a canonical shared root and its data without mutating either.

    Accepts exactly 1.0.0, 1.1.0, 1.2.0, and 2.0.0. Legacy missing optional
    fields remain missing, and no completion evidence is inferred or invented.

    Raises:
        ValueError: For an invalid root, unsupported version, non-JSON input,
            malformed known fields, or inconsistent snapshot semantics.
    """
    state = _object(state, "state")
    _required(state, ("schemaVersion", "data"), "state")
    version = _enum(state["schemaVersion"], _VERSIONS, "schemaVersion")
    _json_value(state)
    if "extensionData" in state:
        _object(state["extensionData"], "state.extensionData")
    _data(_object(state["data"], "state.data"), version)
