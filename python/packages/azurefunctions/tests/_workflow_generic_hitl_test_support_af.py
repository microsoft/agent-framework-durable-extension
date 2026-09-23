# Copyright (c) Microsoft. All rights reserved.

"""HTTP request helper for Functions generic HITL tests."""

import json
from typing import Any

import azure.functions as func


def _request(operation: str, request_id: str = "approval", payload: Any = None) -> func.HttpRequest:
    return func.HttpRequest(
        method="GET" if operation == "status" else "POST",
        url=f"https://example.test/api/workflow/generic-hitl/{operation}/root/{request_id}",
        headers={"Content-Type": "application/json"},
        params={},
        route_params={"instanceId": "root", "requestId": request_id},
        body=json.dumps(payload).encode("utf-8"),
    )
