# Copyright (c) Microsoft. All rights reserved.

"""Route email requests through conditional orchestration with two agents.

Components used in this sample:
- FoundryChatClient agents for spam detection and email drafting.
- AgentFunctionApp with Durable orchestration, activity, and HTTP triggers.
- Pydantic models that validate payloads and agent JSON responses.

Prerequisites: set `FOUNDRY_PROJECT_ENDPOINT`, `FOUNDRY_MODEL`, and sign in with Azure CLI before running the
Functions host."""

import json
import logging
import os
from collections.abc import Generator, Mapping
from typing import Any

import azure.functions as func
from agent_framework import Agent
from agent_framework.foundry import FoundryChatClient
from agent_framework_azurefunctions import AgentFunctionApp, runtime_status_name
from azure.durable_functions import DurableFunctionsClient
from azure.identity.aio import AzureCliCredential
from durabletask.task import OrchestrationContext
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

# 1. Define agent names shared across the orchestration.
SPAM_AGENT_NAME = "SpamDetectionAgent"
EMAIL_AGENT_NAME = "EmailAssistantAgent"


class SpamDetectionResult(BaseModel):
    is_spam: bool
    reason: str


class EmailResponse(BaseModel):
    response: str


class EmailPayload(BaseModel):
    email_id: str
    email_content: str


# 2. Instantiate both agents so they can be registered with AgentFunctionApp.
def _create_agents() -> list[Any]:
    client = FoundryChatClient(
        project_endpoint=os.environ["FOUNDRY_PROJECT_ENDPOINT"],
        model=os.environ["FOUNDRY_MODEL"],
        credential=AzureCliCredential(),
    )

    spam_agent = Agent(
        client=client,
        name=SPAM_AGENT_NAME,
        instructions="You are a spam detection assistant that identifies spam emails.",
    )

    email_agent = Agent(
        client=client,
        name=EMAIL_AGENT_NAME,
        instructions="You are an email assistant that helps users draft responses to emails with professionalism.",
    )

    return [spam_agent, email_agent]


app = AgentFunctionApp(agents=_create_agents(), enable_health_check=True)


# 3. Activities handle the side effects for spam and legitimate emails.
@app.activity_trigger(input_name="reason")
def handle_spam_email(reason: str) -> str:
    return f"Email marked as spam: {reason}"


@app.activity_trigger(input_name="message")
def send_email(message: str) -> str:
    return f"Email sent: {message}"


# 4. Orchestration validates input, runs agents, and branches on spam results.
# Orchestrators take (context, input) since azure-functions-durable 2.x, so the email
# payload arrives as the second argument instead of via a context lookup.
@app.orchestration_trigger(context_name="context")
def spam_detection_orchestration(context: OrchestrationContext, payload_raw: Any) -> Generator[Any, Any, str]:
    if not isinstance(payload_raw, Mapping):
        raise ValueError("Email data is required")

    try:
        payload = EmailPayload.model_validate(payload_raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid email payload: {exc}") from exc

    spam_agent = app.get_agent(context, SPAM_AGENT_NAME)
    email_agent = app.get_agent(context, EMAIL_AGENT_NAME)

    spam_session = spam_agent.create_session()

    spam_prompt = (
        "Analyze this email for spam content and return a JSON response with 'is_spam' (boolean) "
        "and 'reason' (string) fields:\n"
        f"Email ID: {payload.email_id}\n"
        f"Content: {payload.email_content}"
    )

    spam_result_raw = yield spam_agent.run(
        messages=spam_prompt,
        session=spam_session,
        options={"response_format": SpamDetectionResult},
    )

    try:
        spam_result = spam_result_raw.value
    except Exception as ex:
        raise ValueError("Failed to parse spam detection result") from ex

    if spam_result.is_spam:
        result = yield context.call_activity("handle_spam_email", input=spam_result.reason)  # type: ignore[misc]
        return result  # noqa: B901 - Durable orchestrators return their final output.

    email_session = email_agent.create_session()

    email_prompt = (
        "Draft a professional response to this email. Return a JSON response with a 'response' field "
        "containing the reply:\n\n"
        f"Email ID: {payload.email_id}\n"
        f"Content: {payload.email_content}"
    )

    email_result_raw = yield email_agent.run(
        messages=email_prompt,
        session=email_session,
        options={"response_format": EmailResponse},
    )

    try:
        email_result = email_result_raw.value
    except Exception as ex:
        raise ValueError("Failed to parse email response") from ex

    result = yield context.call_activity("send_email", input=email_result.response)  # type: ignore[misc]
    return result  # noqa: B901 - Durable orchestrators return their final output.


# 5. HTTP starter endpoint launches the orchestration for each email payload.
@app.route(route="spamdetection/run", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_spam_detection_orchestration(
    req: func.HttpRequest,
    client: DurableFunctionsClient,
) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        body = None

    if not isinstance(body, Mapping):
        return func.HttpResponse(
            body=json.dumps({"error": "Email data is required"}),
            status_code=400,
            mimetype="application/json",
        )

    try:
        payload = EmailPayload.model_validate(body)
    except ValidationError as exc:
        return func.HttpResponse(
            body=json.dumps({"error": f"Invalid email payload: {exc}"}),
            status_code=400,
            mimetype="application/json",
        )

    instance_id = await client.schedule_new_orchestration(
        "spam_detection_orchestration",
        input=payload.model_dump(),
    )

    logger.info("[HTTP] Started spam detection orchestration with instance_id: %s", instance_id)

    status_url = _build_status_url(req.url, instance_id, route="spamdetection")

    payload_json = {
        "message": "Spam detection orchestration started.",
        "emailId": payload.email_id,
        "instanceId": instance_id,
        "statusQueryGetUri": status_url,
    }

    return func.HttpResponse(
        body=json.dumps(payload_json),
        status_code=202,
        mimetype="application/json",
    )


# 6. Status endpoint mirrors Durable Functions default payload with agent data.
@app.route(route="spamdetection/status/{instanceId}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_orchestration_status(
    req: func.HttpRequest,
    client: DurableFunctionsClient,
) -> func.HttpResponse:
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
        "createdTime": status.created_at.isoformat() if status.created_at else None,
        "lastUpdatedTime": status.last_updated_at.isoformat() if status.last_updated_at else None,
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


# 7. Helper utilities keep URL construction and structured parsing deterministic.
def _build_status_url(request_url: str, instance_id: str, *, route: str) -> str:
    base_url, _, _ = request_url.partition("/api/")
    if not base_url:
        base_url = request_url.rstrip("/")
    return f"{base_url}/api/{route}/status/{instance_id}"


"""
Expected response from `POST /api/spamdetection/run`:

HTTP/1.1 202 Accepted
{
    "message": "Spam detection orchestration started.",
    "emailId": "123",
    "instanceId": "<durable-instance-id>",
    "statusQueryGetUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/<durable-instance-id>"
}

Expected response from `GET /api/spamdetection/status/{instanceId}` once complete:

HTTP/1.1 200 OK
{
    "instanceId": "<durable-instance-id>",
    "runtimeStatus": "Completed",
    "createdTime": "2024-01-01T00:00:00+00:00",
    "lastUpdatedTime": "2024-01-01T00:00:10+00:00",
    "output": "Email sent: Thank you for reaching out..."
}
"""
