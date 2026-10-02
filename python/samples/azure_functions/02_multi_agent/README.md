# Multi-Agent Sample

This sample demonstrates how to use the Durable Extension for Agent Framework to create an Azure Functions app that hosts multiple AI agents and provides direct HTTP API access for interactive conversations with each agent.

## Key Concepts Demonstrated

- Using the Microsoft Agent Framework to define multiple AI agents with unique names and instructions.
- Registering multiple agents with the Function app and running them using HTTP.
- Conversation management (via session IDs) for isolated interactions per agent.
- Two different methods for registering agents: list-based initialization and incremental addition.

## Prerequisites

This folder can be installed independently, including after gallery extraction.
Use Python 3.13 and Azure Functions Core Tools v4, and sign in using `az login`
with an identity authorized for an **existing** Azure AI Foundry project/model.
Start your own Azurite instance for the host's storage connection.

From this folder, create and activate an isolated virtual environment and run:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

On Linux/macOS use `python3.13 -m venv .venv` and `source .venv/bin/activate`.
Requirements use published packages matching this release and the repository's
lock, not editable paths to sibling packages missing from gallery downloads.
The integration requires `azure-functions<2`; it cannot currently be combined
with connectors requiring `azure-functions>=2.2`. Do not adopt a different SDK
major or the unreleased migration stack just to satisfy dependency resolution.

If `local.settings.json` does not exist, copy `local.settings.json.template` to
it. Otherwise, preserve your existing settings and add only missing entries.
Set `FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL` to real existing resources;
the template placeholders are not runnable values. Keep credentials and local
settings out of source control.

The existing host.json uses the Azure Storage backend. The template's DTS
connection string alone does not select DTS. This packaging change does not
migrate backend/history protocols; DTS configuration and service execution
still require a separately validated owner decision.

Start the host with `func start`. Package imports and function discovery do not
prove model access or completed durable agent execution.

## Running the Sample

With the environment setup and function app running, you can test the sample by sending HTTP requests to the different agent endpoints.

You can use the `demo.http` file to send messages to the agents, or a command line tool like `curl` as shown below:

> **Note:** Each endpoint waits for the agent response by default. To receive an immediate HTTP 202 instead, set the `x-ms-wait-for-response` header or include `"wait_for_response": false` in the request body.

### Test the Weather Agent

Bash (Linux/macOS/WSL):
Weather agent request:

```bash
curl -X POST http://localhost:7071/api/agents/WeatherAgent/run \
    -H "Content-Type: application/json" \
    -d '{"message": "What is the weather in Seattle?"}'
```

Expected HTTP 202 payload:

```json
{
  "status": "accepted",
  "response": "Agent request accepted",
  "message": "What is the weather in Seattle?",
  "session_id": "<guid>",
  "correlation_id": "<guid>"
}
```

Math agent request:

```bash
curl -X POST http://localhost:7071/api/agents/MathAgent/run \
    -H "Content-Type: application/json" \
    -d '{"message": "Calculate a 20% tip on a $50 bill"}'
```

Expected HTTP 202 payload:

```json
{
  "status": "accepted",
  "response": "Agent request accepted",
  "message": "Calculate a 20% tip on a $50 bill",
  "session_id": "<guid>",
  "correlation_id": "<guid>"
}
```

Health check (optional):

```bash
curl http://localhost:7071/api/health
```

Expected response:

```json
{
  "status": "healthy",
  "agents": [
    {"name": "WeatherAgent", "type": "Agent"},
    {"name": "MathAgent", "type": "Agent"}
  ],
  "agent_count": 2
}
```

## Code Structure

The sample demonstrates two ways to register multiple agents:

### Option 1: Pass list of agents during initialization
```python
app = AgentFunctionApp(agents=[weather_agent, math_agent])
```

### Option 2: Add agents incrementally (commented in sample)
```python
app = AgentFunctionApp()
app.add_agent(weather_agent)
app.add_agent(math_agent)
```

Each agent automatically gets:
- `POST /api/agents/{agent_name}/run` - Send messages to the agent
