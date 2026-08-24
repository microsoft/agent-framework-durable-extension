// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics;
using Microsoft.Agents.AI.Workflows;

namespace Microsoft.Agents.AI.DurableTask.Workflows;

/// <summary>
/// Provides configuration options for managing durable workflows within an application.
/// </summary>
[DebuggerDisplay("Workflows = {Workflows.Count}")]
public sealed class DurableWorkflowOptions
{
    private readonly Dictionary<string, Workflow> _workflows = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Initializes a new instance of the <see cref="DurableWorkflowOptions"/> class.
    /// </summary>
    /// <param name="parentOptions">Optional parent options container for accessing related configuration.</param>
    internal DurableWorkflowOptions(DurableOptions? parentOptions = null)
    {
        this.ParentOptions = parentOptions;
    }

    /// <summary>
    /// Gets the parent <see cref="DurableOptions"/> container, if available.
    /// </summary>
    internal DurableOptions? ParentOptions { get; }

    /// <summary>
    /// Gets the collection of workflows available in the current context, keyed by their unique names.
    /// </summary>
    public IReadOnlyDictionary<string, Workflow> Workflows => this._workflows;

    /// <summary>
    /// Gets the executor registry for direct executor lookup.
    /// </summary>
    internal ExecutorRegistry Executors { get; } = new();

    /// <summary>
    /// Adds a workflow to the collection for processing or execution.
    /// </summary>
    /// <param name="workflow">The workflow instance to add. Cannot be null.</param>
    /// <returns>The options instance, so that multiple calls can be chained.</returns>
    /// <remarks>
    /// <para>
    /// When a workflow is added, all executors are registered in the executor registry.
    /// Any AI agent executors will also be automatically registered with the
    /// <see cref="DurableAgentsOptions"/> if available.
    /// </para>
    /// <para>
    /// Workflow names must be unique because they identify the orchestration that runs the workflow.
    /// Adding the same workflow instance more than once is a no-op, so a sub-workflow that is also
    /// registered explicitly is not reported as a conflict when it is discovered during registration.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="workflow"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the workflow does not have a valid name, or when a different workflow with the same name has already been registered.
    /// </exception>
    public DurableWorkflowOptions AddWorkflow(Workflow workflow)
    {
        ArgumentNullException.ThrowIfNull(workflow);

        if (string.IsNullOrEmpty(workflow.Name))
        {
            throw new ArgumentException("Workflow must have a valid Name property.", nameof(workflow));
        }

        if (this._workflows.TryGetValue(workflow.Name, out Workflow? existingWorkflow))
        {
            if (!ReferenceEquals(existingWorkflow, workflow))
            {
                throw new ArgumentException($"A workflow with name '{workflow.Name}' has already been registered.", nameof(workflow));
            }

            // The same instance was already registered, so its executors are registered too.
            return this;
        }

        // Register executors first so a failure part-way through leaves no entry behind and a retry re-runs registration.
        this.RegisterWorkflowExecutors(workflow);
        this._workflows[workflow.Name] = workflow;

        return this;
    }

    /// <summary>
    /// Registers all executors from a workflow, including AI agents if agent options are available.
    /// </summary>
    private void RegisterWorkflowExecutors(Workflow workflow)
    {
        DurableAgentsOptions? agentOptions = this.ParentOptions?.Agents;

        foreach ((string executorId, ExecutorBinding binding) in workflow.ReflectExecutors())
        {
            string executorName = WorkflowNamingHelper.GetExecutorName(executorId);
            this.Executors.Register(executorName, executorId, workflow);

            TryRegisterAgent(binding, agentOptions);
        }
    }

    /// <summary>
    /// Registers an AI agent with the agent options if the binding contains an unregistered agent.
    /// </summary>
    private static void TryRegisterAgent(ExecutorBinding binding, DurableAgentsOptions? agentOptions)
    {
        if (agentOptions is null)
        {
            return;
        }

        if (binding.RawValue is AIAgent { Name: not null } agent
            && !agentOptions.ContainsAgent(agent.Name))
        {
            agentOptions.AddAIAgent(agent);
        }
    }
}
