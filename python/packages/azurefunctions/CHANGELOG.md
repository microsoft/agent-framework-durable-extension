# Release History

## [Unreleased]

- Added opt-in Azure Blob payload offloading (`azure-blob-payloads` extra) for Azure Functions hosts ([#140](https://github.com/microsoft/agent-framework-durable-extension/pull/140))
- [BREAKING] Migrated `agent-framework-azurefunctions` hosting to `azure-functions-durable` 2.x (Python 3.13+), rejected missing human-in-the-loop targets, and unified Python hosting package validation ([#136](https://github.com/microsoft/agent-framework-durable-extension/pull/136))
- Preferred camelCase names on agent HTTP requests and responses (`sessionId`, `waitForResponse`, `statusCode`, `completionOutcome`) while retaining legacy snake_case aliases ([#103](https://github.com/microsoft/agent-framework-durable-extension/pull/103))
- Updated `agent-framework-core` dependency floor to `>=1.19.0,<2` ([#116](https://github.com/microsoft/agent-framework-durable-extension/pull/116))
- [BREAKING] Added durable agent state schema 2.0 support, durable chat history ownership and session persistence, and pressure-based durable history retention ([#123](https://github.com/microsoft/agent-framework-durable-extension/pull/123))

## v1.0.0b260922

- Aligned workflow selector validation with `agent-framework-core` ([#102](https://github.com/microsoft/agent-framework-durable-extension/pull/102))
- Restored final beta checkpoint and feature-usage compatibility fixes and raised `agent-framework-core` compatibility floor ([#81](https://github.com/microsoft/agent-framework-durable-extension/pull/81))
- Added synchronous Durable Workflow HTTP invocation options ([#52](https://github.com/microsoft/agent-framework-durable-extension/pull/52))
- Replaced "thread" with "session" terminology across HTTP and MCP APIs ([#47](https://github.com/microsoft/agent-framework-durable-extension/pull/47))
- Extracted `agent-framework-azurefunctions` into `microsoft/agent-framework-durable-extension` ([#21](https://github.com/microsoft/agent-framework-durable-extension/pull/21))
