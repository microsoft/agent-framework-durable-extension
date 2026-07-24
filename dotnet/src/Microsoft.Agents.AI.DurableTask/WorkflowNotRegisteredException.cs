// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Exception thrown when a workflow with the specified name has not been registered.
/// </summary>
public sealed class WorkflowNotRegisteredException : InvalidOperationException
{
    // Not used, but required by static analysis.
    private WorkflowNotRegisteredException()
    {
        this.WorkflowName = string.Empty;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="WorkflowNotRegisteredException"/> class with the workflow name.
    /// </summary>
    /// <param name="workflowName">The name of the workflow that was not registered.</param>
    public WorkflowNotRegisteredException(string workflowName)
        : base(GetMessage(workflowName))
    {
        this.WorkflowName = workflowName;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="WorkflowNotRegisteredException"/> class with the workflow name and an inner exception.
    /// </summary>
    /// <param name="workflowName">The name of the workflow that was not registered.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public WorkflowNotRegisteredException(string workflowName, Exception? innerException)
        : base(GetMessage(workflowName), innerException)
    {
        this.WorkflowName = workflowName;
    }

    /// <summary>
    /// Gets the name of the workflow that was not registered.
    /// </summary>
    public string WorkflowName { get; }

    private static string GetMessage(string workflowName)
    {
        ArgumentException.ThrowIfNullOrEmpty(workflowName);
        return $"No workflow named '{workflowName}' was registered. Ensure the workflow is registered using {nameof(ServiceCollectionExtensions.ConfigureDurableWorkflows)} before invoking it by name.";
    }
}
