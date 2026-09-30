# Copyright (c) Microsoft. All rights reserved.

"""Azure Functions surface for explicit legacy migration.

The Functions host registers the shared Durable Task agent entity class, so the
``migrate`` operation contract is covered by the Durable Task migration host tests
and by the registered Functions entity in ``test_migration_json_af.py``.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from agent_framework_azurefunctions import AgentFunctionApp


def test_agent_function_app_exposes_migration_only_through_entity_operation(monkeypatch: pytest.MonkeyPatch) -> None:
    routes: list[dict[str, Any]] = []
    entities: list[dict[str, Any]] = []
    tools: list[dict[str, Any]] = []

    app = AgentFunctionApp(enable_health_check=False, deployment_mode="isolated_v2")

    def identity(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        return lambda handler: handler

    def route(*args: Any, **kwargs: Any) -> Any:
        del args
        routes.append(dict(kwargs))
        return lambda handler: handler

    def entity_trigger(*args: Any, **kwargs: Any) -> Any:
        del args
        entities.append(dict(kwargs))
        return lambda handler: handler

    def mcp_tool_trigger(*args: Any, **kwargs: Any) -> Any:
        del args
        tools.append(dict(kwargs))
        return lambda handler: handler

    monkeypatch.setattr(app, "function_name", identity)
    monkeypatch.setattr(app, "durable_client_input", identity)
    monkeypatch.setattr(app, "route", route)
    monkeypatch.setattr(app, "entity_trigger", entity_trigger)
    monkeypatch.setattr(app, "mcp_tool_trigger", mcp_tool_trigger)
    agent = Mock()
    agent.name = "af-migration-agent"
    agent.description = "migration subject"
    app.add_agent(
        agent,
        enable_http_endpoint=True,
        enable_mcp_tool_trigger=True,
    )

    assert routes == [{"route": "agents/af-migration-agent/run", "methods": ["POST"]}]
    assert entities == [{"context_name": "context", "entity_name": "dafx-af-migration-agent"}]
    assert tools and tools[0]["tool_name"] == "af-migration-agent"
    assert all("migrate" not in str(item).lower() for item in routes)
    assert all("migrate" not in str(item).lower() for item in tools)
    assert entities[0]["entity_name"].startswith("dafx-")
