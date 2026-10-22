# Release History

## [Unreleased]

- Added opt-in Azure Blob payload offloading (`azure-blob-payloads` extra) and offloaded workflow result reference resolution ([#140](https://github.com/microsoft/agent-framework-durable-extension/pull/140))
- [BREAKING] Constrained standalone durable agent entity operations to explicit method references ([#135](https://github.com/microsoft/agent-framework-durable-extension/pull/135))
- Shared the durable agent entity factory and workflow context across standalone and Azure Functions hosts ([#134](https://github.com/microsoft/agent-framework-durable-extension/pull/134))
- Preferred camelCase names on agent HTTP requests and responses (`sessionId`, `waitForResponse`, `statusCode`, `completionOutcome`) while retaining legacy snake_case aliases ([#103](https://github.com/microsoft/agent-framework-durable-extension/pull/103))
- Updated `agent-framework-core` dependency floor to `>=1.19.0,<2` ([#116](https://github.com/microsoft/agent-framework-durable-extension/pull/116))
- [BREAKING] Added durable agent state schema 2.0 support, durable chat history ownership and session persistence, and pressure-based durable history retention ([#123](https://github.com/microsoft/agent-framework-durable-extension/pull/123))

## v1.0.0b260922

- Aligned workflow selector validation with `agent-framework-core` ([#102](https://github.com/microsoft/agent-framework-durable-extension/pull/102))
- Restored final beta checkpoint and feature-usage compatibility fixes and raised `agent-framework-core` compatibility floor ([#81](https://github.com/microsoft/agent-framework-durable-extension/pull/81))
- Replaced "thread" with "session" terminology across APIs and documentation ([#47](https://github.com/microsoft/agent-framework-durable-extension/pull/47))
- Extracted `agent-framework-durabletask` into `microsoft/agent-framework-durable-extension` ([#21](https://github.com/microsoft/agent-framework-durable-extension/pull/21), [#12](https://github.com/microsoft/agent-framework-durable-extension/pull/12))
