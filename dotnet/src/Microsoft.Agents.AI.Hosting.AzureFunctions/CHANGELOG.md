# Release History

## [Unreleased]

- Prepared package metadata for the 1.16.0-rc1 release ([#58](https://github.com/microsoft/agent-framework-durable-extension/pull/58))
- Fixed the durable configuration methods not composing on the same application: calling `ConfigureDurableAgents` first left the workflow functions without an executor, calling `ConfigureDurableWorkflows` first registered the built-in function execution middleware twice, agents registered through `ConfigureDurableOptions` generated no functions at all, leaving the agent silently unreachable, and registering an agent that a workflow already referenced threw instead of promoting it. Agents now get the same entry points regardless of which method registers them and in which order, with each function generated exactly once even when a workflow and an explicit registration both contribute the same agent, while agents that exist only because a workflow references them continue to get no HTTP endpoint of their own ([#67](https://github.com/microsoft/agent-framework-durable-extension/pull/67))
- [BREAKING] Always return JSON from the workflow status and respond endpoints, including on errors and when the request sends no `Accept` header, and fix malformed request bodies surfacing as an unhandled error instead of `400 Bad Request` ([#60](https://github.com/microsoft/agent-framework-durable-extension/pull/60))
- [BREAKING] Support bounded synchronous workflow HTTP invocation through query parameters and default workflow run responses to JSON, with `Accept: text/plain` available for the legacy text format and the same negotiated asynchronous response returned on timeout ([#52](https://github.com/microsoft/agent-framework-durable-extension/pull/52))
- Added `DurableTaskClient.AsWorkflowClient` so functions can invoke durable workflows without constructing an `HttpClient` ([#48](https://github.com/microsoft/agent-framework-durable-extension/pull/48))
- [BREAKING] Consolidated the `AddWorkflow` extension overloads into a single method with optional `enableStatusEndpoint` and `enableMcpToolTrigger` parameters, and changed it to return `DurableWorkflowOptions` instead of `void` so multiple workflows can be registered fluently ([#39](https://github.com/microsoft/agent-framework-durable-extension/pull/39))
- [BREAKING] Replace "thread" with "session" in HTTP and MCP APIs ([#47](https://github.com/microsoft/agent-framework-durable-extension/pull/47))
- [BREAKING] Renamed `AddWorkflow` parameters `exposeStatusEndpoint` and `exposeMcpToolTrigger` to `enableStatusEndpoint` and `enableMcpToolTrigger` for consistency with `AddAIAgent` ([#35](https://github.com/microsoft/agent-framework-durable-extension/pull/35))
- Scope workflow status/respond endpoints to the route workflow name ([#6608](https://github.com/microsoft/agent-framework/pull/6608))
- Bind MCP threadId to the current agent and guard cross-agent session dispatch ([#6531](https://github.com/microsoft/agent-framework/pull/6531))
- Support returning workflow results from HTTP trigger endpoint ([#5321](https://github.com/microsoft/agent-framework/pull/5321))
- Added MCP tool trigger support for durable workflows ([#4768](https://github.com/microsoft/agent-framework/pull/4768))
- Added Azure Functions hosting support for durable workflows ([#4436](https://github.com/microsoft/agent-framework/pull/4436))

## v1.0.0-preview.251219.1

- Addressed incompatibility issue with `Microsoft.Azure.Functions.Worker.Extensions.DurableTask` >= 1.11.0 ([#2759](https://github.com/microsoft/agent-framework/pull/2759))

## v1.0.0-preview.251125.1

- Added support for .NET 10 ([#2128](https://github.com/microsoft/agent-framework/pull/2128))
- [BREAKING] Changed `thread_id` in HTTP APIs from entity ID to GUID ([#2260](https://github.com/microsoft/agent-framework/pull/2260))

## v1.0.0-preview.251114.1

- Added friendly error message when running durable agent that isn't registered ([#2214](https://github.com/microsoft/agent-framework/pull/2214))

## v1.0.0-preview.251112.1

- Initial public release ([#1916](https://github.com/microsoft/agent-framework/pull/1916))
