# Copyright (c) Microsoft. All rights reserved.

"""Migration-only conversion of the pinned published agent observations.

Stop old workers and capture entity state before authorizing its exact digest.
Register this converter instead of the cold entity implementation during the
serialized conversion phase. Accepted queued operations use the same native
entity executor and current invocation implementation. Remove this module and
the original reader from the final cold deployment.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from copy import deepcopy
from pathlib import Path
from threading import RLock
from types import ModuleType
from typing import Any, Mapping

from agent_framework import SupportsAgentRun
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from ._checkpoint_store import CheckpointScope, CheckpointStore
from ._checkpoints._agent import (
    CONTRACT,
    PUBLIC_POLICY,
    REPLAY_POLICY,
    RETRY_POLICY,
    CheckpointEntityStateProvider,
    create_entity,
    digest,
)
from ._checkpoints._authority import issue_capability, verify_capability
from ._checkpoints._types import JsonObject, is_json_object, is_list, is_string_keyed_dict
from ._entities import DurableTaskEntityStateProvider
from ._json_payload import JsonPayload
from ._response_utils import is_terminal_agent_response, serialize_agent_response

PUBLISHED_READER_SHA256 = "96092e073a9f993d4c802c6db29007e7ae4dad9122f9472bdc4132a06c128d49"
_ORIGINAL_READER_LOCK = RLock()


def entity_conversion_binding(scope: CheckpointScope, *, entity: str) -> dict[str, object]:
    """Bind authority to an entity, without inventing an orchestration execution."""
    if not entity or entity != entity.strip():
        raise ValueError("A canonical native entity address is required.")
    return {
        "contract": "serialized-entity-conversion/1",
        "backend_id": scope.backend,
        "task_hub": scope.hub,
        "entity_id": entity,
        "definition_sha256": scope.definition_sha256,
    }


def issue_entity_conversion(
    *,
    key_id: str,
    operator_private_key: Ed25519PrivateKey,
    scope: CheckpointScope,
    entity: str,
    source_state: JsonObject,
) -> bytes:
    """Authorize one exact trusted legacy snapshot in an operator-only process."""
    if not is_json_object(source_state):
        raise ValueError("Entity source state must be finite JSON.")
    return issue_capability(
        purpose="checkpoint",
        key_id=key_id,
        operator_private_key=operator_private_key,
        binding=entity_conversion_binding(scope, entity=entity),
        payload_bytes=json.dumps({"sourceDigest": digest(source_state)}).encode(),
    )


def _original_reader(path: Path) -> ModuleType:
    source = path.read_bytes()
    if hashlib.sha256(source).hexdigest() != PUBLISHED_READER_SHA256:
        raise ValueError("Published agent reader differs from the pinned release.")
    name = "agent_framework_durabletask._migration_original_state"
    with _ORIGINAL_READER_LOCK:
        if name not in sys.modules:
            spec = importlib.util.spec_from_file_location(name, path)
            if spec is None or spec.loader is None:
                raise ValueError("Published agent reader cannot be loaded.")
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            try:
                # Compile only the bytes verified against the pinned release.
                exec(compile(source, str(path), "exec"), module.__dict__)  # noqa: S102
            except BaseException:
                if sys.modules.get(name) is module:
                    del sys.modules[name]
                raise
        return sys.modules[name]


def _validate_usage(usage: Any) -> None:
    if usage is not None:
        if not is_string_keyed_dict(usage):
            raise ValueError("Published usage must be an object or null.")
        for key in ("inputTokenCount", "outputTokenCount", "totalTokenCount"):
            value = usage.get(key)
            if value is not None and type(value) is not int:
                raise ValueError(f"usage.{key} must be an integer or null.")


def convert_agent_observations(raw: JsonObject, *, published_reader: str | Path) -> JsonObject:
    """Project trusted original observations, never historical effect receipts.

    The reader file must be supplied by the operator from the pinned release.
    This function is an offline compiler, not an authority check. The serialized
    converter below authenticates the raw snapshot before calling it.
    """
    from ._shared_agent_state import DurableAgentState

    if raw.get("schemaVersion") not in ("1.0.0", "1.1.0", "1.2.0"):
        raise ValueError("Expected published entity state at a quiesced operation frontier.")
    data = raw.get("data")
    if not is_json_object(data) or not is_list(history := data.get("conversationHistory")):
        raise ValueError("Published conversation history must be an array.")
    usage_observations: list[JsonObject] = []
    for index, entry in enumerate(history):
        if not is_json_object(entry) or entry.get("$type") not in ("request", "response"):
            raise ValueError("Unsupported published conversation entry.")
        usage = entry.get("usage")
        _validate_usage(usage)
        if is_json_object(usage) and any(
            key in usage and usage[key] is None for key in ("inputTokenCount", "outputTokenCount", "totalTokenCount")
        ):
            usage_observations.append({"entry": index, "usage": deepcopy(usage)})
        messages = entry.get("messages")
        if not is_list(messages):
            raise ValueError("Published messages must be an array.")
        for message in messages:
            if not is_json_object(message) or not is_list(contents := message.get("contents")):
                raise ValueError("Published message contents must be an array.")
            for content in contents:
                if not is_json_object(content):
                    raise ValueError("Published content must be an object.")
                if content.get("$type") == "usage":
                    _validate_usage(content.get("usage"))
    original = _original_reader(Path(published_reader))
    source = original.DurableAgentState.from_dict(deepcopy(raw))
    if len(source.data.conversation_history) != len(history):
        raise ValueError("Published reader did not preserve every conversation entry.")
    observations: list[JsonObject] = []
    replay: list[JsonObject] = []
    for entry in source.data.conversation_history:
        for stored in entry.messages:
            message = stored.to_chat_message()
            contents = [
                content for content in message.contents if content.type not in ("reasoning", "text_reasoning")
            ]
            if contents:
                message.contents = contents
                replay.append(message.to_dict())
        if isinstance(entry, original.DurableAgentStateResponse):
            observed = original.DurableAgentStateResponse.to_run_response(entry)
            observations.append({
                "correlation": entry.correlation_id,
                "response": serialize_agent_response(observed),
                "evidence": "observed-original-response",
                "completionTime": None,
                "outcome": "failed" if is_terminal_agent_response(observed) else "succeeded",
            })
    return {
        "contract": CONTRACT,
        "historyInput": "full-transcript/1",
        "publicInvocationPolicy": PUBLIC_POLICY,
        "publicReplayPolicy": REPLAY_POLICY,
        "publishedReplayMessages": replay,
        "publishedEntryCount": len(history),
        "retryPolicy": RETRY_POLICY,
        "retryAttempts": {},
        "nativeOperations": {},
        "invocationFailurePolicy": "current-v1",
        "sourceWriter": "published-python-full-transcript/1",
        "sourceSessionObservation": deepcopy(data.get("session")),
        "sourceOptionalUsage": {"profile": "published-optional-usage/1", "entries": usage_observations},
        "sourceReaderSha256": PUBLISHED_READER_SHA256,
        "sourceDigest": digest(raw),
        "runtime": DurableAgentState().to_dict(),
        "observations": observations,
    }


def create_authorized_entity_converter(
    agent: SupportsAgentRun,
    *,
    entity_id: str,
    scope: CheckpointScope,
    store: CheckpointStore,
    generation: str,
    bundle_sha256: str,
    trusted_public_keys: Mapping[str, Ed25519PublicKey | bytes],
    published_reader: str | Path,
) -> type[CheckpointEntityStateProvider]:
    """Create a temporary, same-address, natively serialized entity converter.

    Stage a signed ``{"sourceDigest": ...}`` payload with
    ``CheckpointStore.put_entity_conversion`` before ``begin_conversion``.
    Use ``issue_entity_conversion`` in the protected operator process.
    No application request, correlation ID or source marker grants authority.
    """
    store.require_converting(scope, generation, bundle_sha256=bundle_sha256)
    base = create_entity(agent, entity_id=entity_id)

    class AuthorizedConverter(base):
        def _allowed_entity_operations(self) -> tuple[str, ...]:
            return (*super()._allowed_entity_operations(), "import_checkpoint")

        def checkpoint(self) -> JsonObject:
            store.require_converting(scope, generation, bundle_sha256=bundle_sha256)
            raw = DurableTaskEntityStateProvider._get_state_dict(self)
            if raw and raw.get("contract") != CONTRACT:
                entity = str(self.entity_context.entity_id)
                envelope = store.get_entity_conversion(
                    scope,
                    generation,
                    bundle_sha256=bundle_sha256,
                    entity=entity,
                )
                if envelope is None:
                    raise ValueError("Entity conversion has no protected operator authority.")
                payload = verify_capability(
                    envelope,
                    purpose="checkpoint",
                    trusted_public_keys=trusted_public_keys,
                    expected_binding=entity_conversion_binding(scope, entity=entity),
                )
                expected = {"sourceDigest": digest(raw)}
                if json.loads(payload) != expected:
                    raise ValueError("Entity changed after the signed operator snapshot.")
                if not is_json_object(raw):
                    raise ValueError("Published source state must be finite JSON.")
                converted = convert_agent_observations(raw, published_reader=published_reader)
                DurableTaskEntityStateProvider._set_state_dict(self, converted)
                self._checkpoint = None
            return super().checkpoint()

        def import_checkpoint(self, request: JsonPayload) -> JsonObject:
            supplied = json.loads(request) if isinstance(request, str) else request
            raw = DurableTaskEntityStateProvider._get_state_dict(self)
            expected = {"sourceDigest": raw["sourceDigest"] if raw.get("contract") == CONTRACT else digest(raw)}
            if supplied != expected:
                raise ValueError("Entity conversion request differs from the original source digest.")
            self.checkpoint()
            return expected

    AuthorizedConverter.__name__ = base.__name__
    AuthorizedConverter.__qualname__ = base.__qualname__
    return AuthorizedConverter
