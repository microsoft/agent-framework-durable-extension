# Copyright (c) Microsoft. All rights reserved.

import importlib.metadata
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._app import AgentFunctionApp, runtime_status_name
    from ._hitl_context import WorkflowHitlContext

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode

__all__ = [
    "AgentFunctionApp",
    "WorkflowHitlContext",
    "__version__",
    "runtime_status_name",
]


def __getattr__(name: str) -> Any:
    """Keep the checkpoint-only entry independent of ordinary legacy-capable hosts."""
    if name in ("AgentFunctionApp", "runtime_status_name"):
        from . import _app

        return getattr(_app, name)
    if name == "WorkflowHitlContext":
        from ._hitl_context import WorkflowHitlContext

        return WorkflowHitlContext
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Preserve discoverability of the lazy ordinary host exports."""
    return sorted(set(globals()) | set(__all__))
