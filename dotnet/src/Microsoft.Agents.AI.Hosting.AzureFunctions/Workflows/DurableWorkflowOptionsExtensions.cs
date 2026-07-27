// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions;

/// <summary>
/// Extension methods for <see cref="DurableWorkflowOptions"/> to configure Azure Functions HTTP trigger options.
/// </summary>
public static class DurableWorkflowOptionsExtensions
{
    /// <summary>
    /// Adds a workflow and configures whether to expose a status HTTP endpoint and/or an MCP tool trigger.
    /// </summary>
    /// <param name="options">The workflow options to add the workflow to.</param>
    /// <param name="workflow">The workflow instance to add.</param>
    /// <param name="enableStatusEndpoint">If <see langword="true"/>, a GET endpoint is generated at <c>workflows/{name}/status/{runId}</c>.</param>
    /// <param name="enableMcpToolTrigger">If <see langword="true"/>, an MCP tool trigger is generated for the workflow.</param>
    /// <returns>The options instance, so that multiple calls can be chained.</returns>
    public static DurableWorkflowOptions AddWorkflow(
        this DurableWorkflowOptions options,
        Workflow workflow,
        bool enableStatusEndpoint = false,
        bool enableMcpToolTrigger = false)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.AddWorkflow(workflow);

        if (options.ParentOptions is FunctionsDurableOptions functionsOptions)
        {
            if (enableStatusEndpoint)
            {
                functionsOptions.EnableStatusEndpoint(workflow.Name!);
            }

            if (enableMcpToolTrigger)
            {
                functionsOptions.EnableMcpToolTrigger(workflow.Name!);
            }
        }

        return options;
    }
}
