# Copyright (c) Microsoft. All rights reserved.

"""Registered Functions starts and co-hosted native orchestrators for SDK replay tests.

azure-functions-durable 2.x runs generated orchestrators on durabletask, so the Durable
Task starts harness replays the Functions app's registrations with the app's converter.
"""

from _af_worker_test_support import _af_worker
from _workflow_provenance_test_support import _DTStarts
from agent_framework import Workflow
from agent_framework_durabletask import wrap_workflow_input
from durabletask.task import OrchestrationContext

from agent_framework_azurefunctions import AgentFunctionApp


def _af_starts(workflow: Workflow) -> _DTStarts:
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")

    # The native orchestrators leave their input unannotated, so the Functions converter
    # decodes it. The generated workflow hardening must not change that for them.
    @app.function_name("native-input")
    @app.orchestration_trigger(context_name="context")
    def native(context: OrchestrationContext, value):
        return value

    @app.function_name("native-parent")
    @app.orchestration_trigger(context_name="context")
    def native_parent(context: OrchestrationContext, value):
        result = yield context.call_sub_orchestrator(
            "dafx-provenance-leaf", input=wrap_workflow_input(value), instance_id="native-chosen-child"
        )
        return result  # noqa: B901

    return _DTStarts(workflow, worker=_af_worker(app))
