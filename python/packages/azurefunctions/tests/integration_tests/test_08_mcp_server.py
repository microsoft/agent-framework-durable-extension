# Copyright (c) Microsoft. All rights reserved.

"""Integration tests for the MCP server sample."""

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult, TextContent

pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.sample("08_mcp_server"),
    pytest.mark.usefixtures("function_app_for_test"),
]


def _response_text(result: CallToolResult) -> str:
    """Collect text content from an MCP tool response."""
    return "\n".join(content.text for content in result.content if isinstance(content, TextContent))


class TestMcpServerSample:
    """Tests for the 08_mcp_server sample."""

    def test_health_reports_trigger_configuration(self, base_url: str, sample_helper) -> None:
        """Verify each agent exposes only its configured triggers."""
        response = sample_helper.get(f"{base_url}/api/health")
        assert response.status_code == 200

        agents = {agent["name"]: agent for agent in response.json()["agents"]}
        assert set(agents) == {"Joker", "StockAdvisor", "PlantAdvisor"}
        assert agents["Joker"]["http_endpoint_enabled"] is True
        assert agents["Joker"]["mcp_tool_enabled"] is False
        assert agents["StockAdvisor"]["http_endpoint_enabled"] is False
        assert agents["StockAdvisor"]["mcp_tool_enabled"] is True
        assert agents["PlantAdvisor"]["http_endpoint_enabled"] is True
        assert agents["PlantAdvisor"]["mcp_tool_enabled"] is True

        disabled_response = sample_helper.post_json(
            f"{base_url}/api/agents/StockAdvisor/run",
            {"message": "What is the current price of MSFT?"},
        )
        assert disabled_response.status_code == 404

    async def test_mcp_tools_can_be_listed_and_invoked(self, base_url: str) -> None:
        """Verify MCP-only and dual-trigger agents are callable as MCP tools."""
        async with (
            streamable_http_client(f"{base_url}/runtime/webhooks/mcp") as (
                read_stream,
                write_stream,
                _,
            ),
            ClientSession(read_stream, write_stream) as session,
        ):
            await session.initialize()

            tools = (await session.list_tools()).tools
            tool_names = {tool.name for tool in tools}
            assert "Joker" not in tool_names
            assert {"StockAdvisor", "PlantAdvisor"} <= tool_names

            for tool_name, query in (
                ("StockAdvisor", "What was the all-time high price of MSFT?"),
                ("PlantAdvisor", "Recommend a low-light indoor plant."),
            ):
                result = await session.call_tool(tool_name, {"query": query})
                assert result.isError is False
                assert _response_text(result).strip()
