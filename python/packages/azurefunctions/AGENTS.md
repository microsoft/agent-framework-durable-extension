# Azure Functions Package (agent-framework-azurefunctions)

Hosting agents as Azure Functions.

## Main Classes

- **`AgentFunctionApp`** - Azure Functions app wrapper for agents

## Usage

For the PR #59 prototype, first configure a separate Functions task hub/deployment with compatible version-2 workers and clients. Only then acknowledge that setup with `deployment_mode="isolated_v2"`. The gate applies to samples and tests too. Old workers and workflow histories must remain on the old engine. See the [deployment warning](README.md#version-2-deployment-warning).

```python
from agent_framework_azurefunctions import AgentFunctionApp

app = AgentFunctionApp(agents=[my_agent], deployment_mode="isolated_v2")
```

## Import Path

Use the direct package import:

```python
from agent_framework_azurefunctions import AgentFunctionApp
```
