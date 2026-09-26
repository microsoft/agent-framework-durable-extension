# Copyright (c) Microsoft. All rights reserved.

"""Durable Task integration for Microsoft Agent Framework."""

import importlib.metadata
import warnings
from typing import TYPE_CHECKING

from . import _constants
from ._async_bridge import run_agent_coroutine
from ._callbacks import AgentCallbackContext, AgentResponseCallbackProtocol
from ._client import DurableAIAgentClient
from ._configuration import (
    AgentRegistrationSettings,
    RegistrationIdentity,
    validate_agent_configuration,
    validate_response_delivery_window,
    validate_runtime_deployment,
)
from ._constants import (
    DEFAULT_MAX_POLL_RETRIES,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DELIVERY_WINDOW_SECONDS,
    LEGACY_THREAD_ID_FIELD,
    MIMETYPE_APPLICATION_JSON,
    MIMETYPE_TEXT_PLAIN,
    REQUEST_RESPONSE_FORMAT_JSON,
    REQUEST_RESPONSE_FORMAT_TEXT,
    SESSION_ID_FIELD,
    SESSION_ID_HEADER,
    WAIT_FOR_RESPONSE_FIELD,
    WAIT_FOR_RESPONSE_HEADER,
    ApiResponseFields,
    ContentTypes,
    DurableStateFields,
)
from ._durable_agent_state import DurableAgentState as LegacyDurableAgentState
from ._entities import AgentEntity, AgentEntityStateProviderMixin
from ._executors import DurableAgentExecutor
from ._history_provider import DurableHistoryProvider
from ._models import AgentSessionId, DurableAgentSession, RunRequest
from ._orchestration_context import DurableAIAgentOrchestrationContext
from ._response_utils import ensure_response_format, load_agent_response, serialize_agent_response
from ._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateCompaction,
    DurableAgentStateContent,
    DurableAgentStateData,
    DurableAgentStateDataContent,
    DurableAgentStateEntry,
    DurableAgentStateEntryJsonType,
    DurableAgentStateErrorContent,
    DurableAgentStateErrorResponse,
    DurableAgentStateFunctionCallContent,
    DurableAgentStateFunctionResultContent,
    DurableAgentStateHostedFileContent,
    DurableAgentStateHostedVectorStoreContent,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateTextContent,
    DurableAgentStateTextReasoningContent,
    DurableAgentStateUnknownContent,
    DurableAgentStateUnknownEntry,
    DurableAgentStateUriContent,
    DurableAgentStateUsage,
    DurableAgentStateUsageContent,
)
from ._shim import DurableAIAgent, build_agent_task
from ._state_capacity import StateCapacityError
from ._state_migration import migrate_legacy_state, state_snapshot_digest
from ._state_reader import SharedAgentStateReader, read_agent_state
from ._worker import DurableAIAgentWorker
from ._workflows.activity import execute_workflow_activity
from ._workflows.client import DurableWorkflowClient
from ._workflows.context import WorkflowOrchestrationContext
from ._workflows.dt_context import DurableTaskWorkflowContext
from ._workflows.naming import (
    DURABLE_NAME_PREFIX,
    is_auto_generated_workflow_name,
    validate_executor_id,
    validate_workflow_name,
    workflow_name_from_orchestrator,
    workflow_orchestrator_name,
)
from ._workflows.orchestrator import run_workflow_orchestrator
from ._workflows.protocol import (
    WORKFLOW_ENGINE_VERSION,
    unwrap_workflow_input,
    validate_workflow_start_input,
    wrap_workflow_input,
)
from ._workflows.registration import WorkflowRegistrationPlan, collect_hosted_workflows, plan_workflow_registration
from ._workflows.runner_context import CapturingRunnerContext
from ._workflows.serialization import deserialize_workflow_output

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode

if TYPE_CHECKING:  # pragma: no cover - declarations for the deprecated aliases below
    THREAD_ID_FIELD: str
    THREAD_ID_HEADER: str

# Deprecated public names mapped to (preferred name, value). Resolved here rather than delegated
# to ``_constants`` so that exactly one warning is emitted, attributed to the importing caller.
_DEPRECATED_ALIASES = {
    "THREAD_ID_FIELD": ("SESSION_ID_FIELD", _constants.LEGACY_THREAD_ID_FIELD),
    "THREAD_ID_HEADER": ("SESSION_ID_HEADER", _constants.LEGACY_THREAD_ID_HEADER),
}


def __getattr__(name: str) -> str:
    """Resolve deprecated re-exports lazily so importing the package stays warning-free."""
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


def __dir__() -> list[str]:
    """Include the deprecated aliases so they remain discoverable."""
    return sorted(set(globals()) | set(_DEPRECATED_ALIASES))


__all__ = [
    "DEFAULT_MAX_POLL_RETRIES",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "DELIVERY_WINDOW_SECONDS",
    "DURABLE_NAME_PREFIX",
    "LEGACY_THREAD_ID_FIELD",
    "MIMETYPE_APPLICATION_JSON",
    "MIMETYPE_TEXT_PLAIN",
    "REQUEST_RESPONSE_FORMAT_JSON",
    "REQUEST_RESPONSE_FORMAT_TEXT",
    "SESSION_ID_FIELD",
    "SESSION_ID_HEADER",
    "THREAD_ID_FIELD",
    "THREAD_ID_HEADER",
    "WAIT_FOR_RESPONSE_FIELD",
    "WAIT_FOR_RESPONSE_HEADER",
    "WORKFLOW_ENGINE_VERSION",
    "AgentCallbackContext",
    "AgentEntity",
    "AgentEntityStateProviderMixin",
    "AgentRegistrationSettings",
    "AgentResponseCallbackProtocol",
    "AgentSessionId",
    "ApiResponseFields",
    "CapturingRunnerContext",
    "ContentTypes",
    "DurableAIAgent",
    "DurableAIAgentClient",
    "DurableAIAgentOrchestrationContext",
    "DurableAIAgentWorker",
    "DurableAgentExecutor",
    "DurableAgentSession",
    "DurableAgentState",
    "DurableAgentStateCompaction",
    "DurableAgentStateContent",
    "DurableAgentStateData",
    "DurableAgentStateDataContent",
    "DurableAgentStateEntry",
    "DurableAgentStateEntryJsonType",
    "DurableAgentStateErrorContent",
    "DurableAgentStateErrorResponse",
    "DurableAgentStateFunctionCallContent",
    "DurableAgentStateFunctionResultContent",
    "DurableAgentStateHostedFileContent",
    "DurableAgentStateHostedVectorStoreContent",
    "DurableAgentStateMessage",
    "DurableAgentStateRequest",
    "DurableAgentStateResponse",
    "DurableAgentStateTextContent",
    "DurableAgentStateTextReasoningContent",
    "DurableAgentStateUnknownContent",
    "DurableAgentStateUnknownEntry",
    "DurableAgentStateUriContent",
    "DurableAgentStateUsage",
    "DurableAgentStateUsageContent",
    "DurableHistoryProvider",
    "DurableStateFields",
    "DurableTaskWorkflowContext",
    "DurableWorkflowClient",
    "LegacyDurableAgentState",
    "RegistrationIdentity",
    "RunRequest",
    "SharedAgentStateReader",
    "StateCapacityError",
    "WorkflowOrchestrationContext",
    "WorkflowRegistrationPlan",
    "__version__",
    "build_agent_task",
    "collect_hosted_workflows",
    "deserialize_workflow_output",
    "ensure_response_format",
    "execute_workflow_activity",
    "is_auto_generated_workflow_name",
    "load_agent_response",
    "migrate_legacy_state",
    "plan_workflow_registration",
    "read_agent_state",
    "run_agent_coroutine",
    "run_workflow_orchestrator",
    "serialize_agent_response",
    "state_snapshot_digest",
    "unwrap_workflow_input",
    "validate_agent_configuration",
    "validate_executor_id",
    "validate_response_delivery_window",
    "validate_runtime_deployment",
    "validate_workflow_name",
    "validate_workflow_start_input",
    "workflow_name_from_orchestrator",
    "workflow_orchestrator_name",
    "wrap_workflow_input",
]
