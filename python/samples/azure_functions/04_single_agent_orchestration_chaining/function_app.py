# Copyright (c) Microsoft. All rights reserved.

"""Chain two runs of a single agent inside a Durable Functions orchestration.

Components used in this sample:
- FoundryChatClient to construct the writer agent hosted by Agent Framework.
- AgentFunctionApp to surface HTTP and orchestration triggers via the Azure Functions extension.
- Durable Functions orchestration to run sequential agent invocations on the same conversation session.

Prerequisites: configure `FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`, then sign
in with Azure CLI before starting the Functions host."""

import json
import logging
import os
from collections.abc import Generator
from typing import Any

import azure.functions as func
from agent_framework import Agent
from agent_framework.foundry import FoundryChatClient
from agent_framework_azurefunctions import AgentFunctionApp, runtime_status_name
from azure.durable_functions import DurableFunctionsClient
from azure.identity.aio import AzureCliCredential
from durabletask.task import OrchestrationContext

logger = logging.getLogger(__name__)

# 1. Define the agent name used across the orchestration.
WRITER_AGENT_NAME = "WriterAgent"


# 2. Create the writer agent that will be invoked twice within the orchestration.
def _create_writer_agent() -> Any:
    """Create the writer agent with the same persona as the C# sample."""
    instructions = (
        "You refine short pieces of text. When given an initial sentence you enhance it;\n"
        "when given an improved sentence you polish it further."
    )

    _client = FoundryChatClient(
        project_endpoint=os.environ["FOUNDRY_PROJECT_ENDPOINT"],
        model=os.environ["FOUNDRY_MODEL"],
        credential=AzureCliCredential(),
    )
    return Agent(
        client=_client,
        name=WRITER_AGENT_NAME,
        instructions=instructions,
    )


# 3. Register the agent with AgentFunctionApp so HTTP and orchestration triggers are exposed.
app = AgentFunctionApp(agents=[_create_writer_agent()], enable_health_check=True)


# 4. Orchestration that runs the agent sequentially on a shared session for chaining behaviour.
# Orchestrators take (context, input) since azure-functions-durable 2.x. This one needs no input.
@app.orchestration_trigger(context_name="context")
def single_agent_orchestration(context: OrchestrationContext, _input: Any) -> Generator[Any, Any, str]:
    """Run the writer agent twice on the same session to mirror chaining behaviour."""

    writer = app.get_agent(context, WRITER_AGENT_NAME)
    writer_session = writer.create_session()

    initial = yield writer.run(
        messages="Write a concise inspirational sentence about learning.",
        session=writer_session,
    )

    improved_prompt = f"Improve this further while keeping it under 25 words: {initial.text}"

    refined = yield writer.run(
        messages=improved_prompt,
        session=writer_session,
    )

    return refined.text  # noqa: B901 - Durable orchestrators return their final output.


# 5. HTTP endpoint to kick off the orchestration and return the status query URI.
@app.route(route="singleagent/run", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_single_agent_orchestration(
    req: func.HttpRequest,
    client: DurableFunctionsClient,
) -> func.HttpResponse:
    """Start the orchestration and return status metadata."""

    instance_id = await client.schedule_new_orchestration("single_agent_orchestration")

    logger.info("[HTTP] Started orchestration with instance_id: %s", instance_id)

    status_url = _build_status_url(req.url, instance_id, route="singleagent")

    payload = {
        "message": "Single-agent orchestration started.",
        "instanceId": instance_id,
        "statusQueryGetUri": status_url,
    }

    return func.HttpResponse(
        body=json.dumps(payload),
        status_code=202,
        mimetype="application/json",
    )


# 6. HTTP endpoint to fetch orchestration status using the original instance ID.
@app.route(route="singleagent/status/{instanceId}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_orchestration_status(
    req: func.HttpRequest,
    client: DurableFunctionsClient,
) -> func.HttpResponse:
    """Return orchestration runtime status."""

    instance_id = req.route_params.get("instanceId")
    if not instance_id:
        return func.HttpResponse(
            body=json.dumps({"error": "Missing instanceId"}),
            status_code=400,
            mimetype="application/json",
        )

    status = await client.get_orchestration_state(instance_id)
    if status is None:
        return func.HttpResponse(
            body=json.dumps({"error": f"No orchestration found for instance '{instance_id}'"}),
            status_code=404,
            mimetype="application/json",
        )

    response_data: dict[str, Any] = {
        "instanceId": status.instance_id,
        "runtimeStatus": runtime_status_name(status.runtime_status),
    }

    orchestration_input = status.get_input()
    if orchestration_input is not None:
        response_data["input"] = orchestration_input

    output = status.get_output()
    if output is not None:
        response_data["output"] = output

    return func.HttpResponse(
        body=json.dumps(response_data),
        status_code=200,
        mimetype="application/json",
    )


# 7. Helper to construct durable status URLs similar to the .NET sample implementation.
def _build_status_url(request_url: str, instance_id: str, *, route: str) -> str:
    """Construct the status query URI similar to DurableHttpApiExtensions in C#."""

    # Split once on /api/ to preserve host and scheme in local emulator and Azure.
    base_url, _, _ = request_url.partition("/api/")
    if not base_url:
        base_url = request_url.rstrip("/")
    return f"{base_url}/api/{route}/status/{instance_id}"


"""
Expected output when calling `POST /api/singleagent/run` and following the returned status URL:

HTTP/1.1 202 Accepted
{
    "message": "Single-agent orchestration started.",
    "instanceId": "<guid>",
    "statusQueryGetUri": "http://localhost:7071/api/singleagent/status/<guid>"
}

Subsequent `GET /api/singleagent/status/<guid>` after completion returns:

HTTP/1.1 200 OK
{
    "instanceId": "<guid>",
    "runtimeStatus": "Completed",
    "output": "Learning is a journey where curiosity turns effort into mastery."
}
"""
