# Copyright (c) Microsoft. All rights reserved.

"""Content-sensitive identities shared by workflow transport and ingestion."""

import hashlib
import json
from copy import deepcopy
from typing import Any, cast

from agent_framework import Content, Message

from ._response_utils import _constructor_fields  # pyright: ignore[reportPrivateUsage]


def _content_identity_payload(content: Content) -> dict[str, Any]:
    current = deepcopy(content.to_dict())
    raw = getattr(content, "_durable_original_core_content", None)
    original = cast("dict[str, Any]", raw) if isinstance(raw, dict) else {}
    if original.get("type") == content.type:
        fields = set(_constructor_fields(Content)) | {"type", "raw_representation"}
        current = {
            **{key: deepcopy(value) for key, value in original.items() if key not in fields},
            **current,
        }
    if isinstance(content.function_call, Content):
        current["function_call"] = _content_identity_payload(content.function_call)
    names = ["items", "inputs"]
    if content.type in ("code_interpreter_tool_result", "shell_tool_result"):
        names.append("outputs")
    for name in names:
        values = getattr(content, name, None)
        if isinstance(values, list):
            current[name] = [
                _content_identity_payload(value) if isinstance(value, Content) else deepcopy(value)
                for value in cast("list[Any]", values)
            ]
    return current


def message_identity(message: Message) -> str:
    """Hash a message's complete wire representation, including its supplied ID.

    Dictionary ordering is immaterial; content ordering, role, author and additional
    properties are meaningful. Core's ``to_dict`` already excludes raw SDK objects
    and absent optional fields. Do not use this alone to identify anonymous requests:
    the workflow sender assigns those a deterministic, source-scoped ID first.
    """
    payload = deepcopy(message.to_dict())
    original = getattr(message, "_durable_original_core_message", None)
    if isinstance(original, dict):
        original = cast("dict[str, Any]", original)
        fields = set(_constructor_fields(Message)) | {"type", "raw_representation"}
        payload = {
            **{key: deepcopy(value) for key, value in original.items() if key not in fields},
            **payload,
        }
    payload["contents"] = [_content_identity_payload(content) for content in message.contents]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
