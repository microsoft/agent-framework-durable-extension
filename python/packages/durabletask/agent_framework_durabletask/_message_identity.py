# Copyright (c) Microsoft. All rights reserved.

"""Content-sensitive identities shared by workflow transport and ingestion."""

import hashlib
import json

from agent_framework import Message

from ._response_utils import serialize_input_message


def message_identity(message: Message) -> str:
    """Hash a message's complete wire representation, including its supplied ID.

    Dictionary ordering is immaterial; content ordering, role, author and additional
    properties are meaningful. Use the input serializer's public base fields,
    explicit field presence and inert extras, excluding raw SDK representations.
    Do not use this alone to identify anonymous requests: the workflow sender
    assigns those a deterministic, source-scoped ID first.
    """
    payload = serialize_input_message(message)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
