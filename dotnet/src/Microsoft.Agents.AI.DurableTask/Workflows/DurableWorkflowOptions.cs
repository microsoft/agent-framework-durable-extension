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
    /// Gets or sets the maximum number of supersteps a workflow can execute. Defaults to 100.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than one.</exception>
    public int MaxSupersteps
    {
        get;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value, nameof(this.MaxSupersteps));
            field = value;
        }
    } = 100;

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

        // Validate cross-kind name collisions before mutating any state so a failed registration leaves no partial entries behind.
        this.ValidateWorkflowCollisions(workflow);

        // Register executors first so a failure part-way through leaves no entry behind and a retry re-runs registration.
        this.RegisterWorkflowExecutors(workflow);
        this._workflows[workflow.Name] = workflow;

        return this;
    }

    internal void ValidateAgentNameCollisions(string agentName, bool processLegacyEntities)
    {
        HashSet<string> agentEntityNames = GetAgentEntityNames([agentName], processLegacyEntities);

        foreach (string workflowName in this._workflows.Keys)
        {
            string orchestrationFunctionName = WorkflowNamingHelper.ToOrchestrationFunctionName(workflowName);
            if (agentEntityNames.Contains(orchestrationFunctionName))
            {
                throw new InvalidOperationException(
                    $"Agent '{agentName}' and workflow '{workflowName}' both generate function name '{orchestrationFunctionName}'.");
            }
        }

        foreach ((string executorName, ExecutorRegistration registration) in this.Executors.Registrations)
        {
            if (registration.Binding is AIAgentBinding or SubworkflowBinding or RequestPortBinding)
            {
                continue;
            }

            if (string.Equals(executorName, agentName, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Agent '{agentName}' collides with activity executor '{executorName}'.");
            }

            string activityFunctionName = WorkflowNamingHelper.ToOrchestrationFunctionName(executorName);
            if (agentEntityNames.Contains(activityFunctionName))
            {
                throw new InvalidOperationException(
                    $"Agent '{agentName}' and activity executor '{executorName}' both generate function name '{activityFunctionName}'.");
            }
        }
    }

    internal void ValidateAllRegisteredCollisions(bool processLegacyEntities)
    {
        HashSet<string> agentNames = new(StringComparer.OrdinalIgnoreCase);
        if (this.ParentOptions?.Agents is DurableAgentsOptions agentOptions)
        {
            foreach (string agentName in agentOptions.GetAgentFactories().Keys)
            {
                agentNames.Add(agentName);
            }
        }

        HashSet<string> activityNames = new(StringComparer.OrdinalIgnoreCase);
        foreach ((string executorName, ExecutorRegistration registration) in this.Executors.Registrations)
        {
            if (registration.Binding is SubworkflowBinding or RequestPortBinding)
            {
                continue;
            }

            if (registration.Binding is AIAgentBinding)
            {
                agentNames.Add(executorName);
            }
            else
            {
                activityNames.Add(executorName);
            }
        }

        ValidateCrossKindCollisions(this._workflows.Keys, activityNames, agentNames, processLegacyEntities);
    }

    private void ValidateWorkflowCollisions(Workflow rootWorkflow)
    {
        HashSet<string> workflowNames = new(this._workflows.Keys, StringComparer.OrdinalIgnoreCase);
        HashSet<string> activityNames = new(StringComparer.OrdinalIgnoreCase);
        HashSet<string> agentNames = new(StringComparer.OrdinalIgnoreCase);

        if (this.ParentOptions?.Agents is DurableAgentsOptions agentOptions)
        {
            foreach (string agentName in agentOptions.GetAgentFactories().Keys)
            {
                agentNames.Add(agentName);
            }
        }

        foreach ((string executorName, ExecutorRegistration registration) in this.Executors.Registrations)
        {
            if (registration.Binding is SubworkflowBinding or RequestPortBinding)
            {
                continue;
            }

            if (registration.Binding is AIAgentBinding)
            {
                agentNames.Add(executorName);
            }
            else
            {
                activityNames.Add(executorName);
            }
        }

        HashSet<Workflow> visited = new(ReferenceEqualityComparer.Instance);
        CollectWorkflowGraphNames(rootWorkflow, visited, workflowNames, activityNames, agentNames);

        bool processLegacyEntities = this.ParentOptions?.Agents.ProcessLegacyEntities == true;
        ValidateCrossKindCollisions(workflowNames, activityNames, agentNames, processLegacyEntities);
    }

    private static void CollectWorkflowGraphNames(
        Workflow workflow,
        HashSet<Workflow> visited,
        HashSet<string> workflowNames,
        HashSet<string> activityNames,
        HashSet<string> agentNames)
    {
        if (!visited.Add(workflow) || string.IsNullOrEmpty(workflow.Name))
        {
            return;
        }

        workflowNames.Add(workflow.Name);

        foreach ((string executorId, ExecutorBinding binding) in workflow.ReflectExecutors())
        {
            if (binding is RequestPortBinding)
            {
                continue;
            }

            if (binding is SubworkflowBinding subworkflowBinding)
            {
                CollectWorkflowGraphNames(subworkflowBinding.WorkflowInstance, visited, workflowNames, activityNames, agentNames);
                continue;
            }

            string executorName = WorkflowNamingHelper.GetExecutorName(executorId);
            if (binding is AIAgentBinding || binding.RawValue is AIAgent)
            {
                agentNames.Add(executorName);
                if (binding.RawValue is AIAgent { Name: string rawAgentName } && !string.IsNullOrWhiteSpace(rawAgentName))
                {
                    agentNames.Add(rawAgentName);
                }
            }
            else
            {
                activityNames.Add(executorName);
            }
        }
    }

    private static void ValidateCrossKindCollisions(
        IEnumerable<string> workflowNames,
        HashSet<string> activityNames,
        HashSet<string> agentNames,
        bool processLegacyEntities)
    {
        HashSet<string> agentEntityNames = GetAgentEntityNames(agentNames, processLegacyEntities);

        foreach (string activityName in activityNames)
        {
            if (agentNames.Contains(activityName))
            {
                throw new InvalidOperationException(
                    $"Activity executor '{activityName}' collides with an AI agent of the same name.");
            }

            string activityFunctionName = WorkflowNamingHelper.ToOrchestrationFunctionName(activityName);
            if (agentEntityNames.Contains(activityFunctionName))
            {
                throw new InvalidOperationException(
                    $"Activity executor '{activityName}' generates function name '{activityFunctionName}', which collides with a registered agent entity trigger.");
            }
        }

        foreach (string workflowName in workflowNames)
        {
            string orchestrationFunctionName = WorkflowNamingHelper.ToOrchestrationFunctionName(workflowName);
            if (agentEntityNames.Contains(orchestrationFunctionName))
            {
                throw new InvalidOperationException(
                    $"Workflow '{workflowName}' generates function name '{orchestrationFunctionName}', which collides with a registered agent entity trigger.");
            }

            if (activityNames.Contains(workflowName))
            {
                throw new InvalidOperationException(
                    $"Workflow '{workflowName}' and activity executor '{workflowName}' both generate function name '{orchestrationFunctionName}'.");
            }
        }
    }

    private static HashSet<string> GetAgentEntityNames(IEnumerable<string> agentNames, bool processLegacyEntities)
    {
        HashSet<string> entityNames = new(StringComparer.OrdinalIgnoreCase);
        foreach (string agentName in agentNames)
        {
            entityNames.Add(AgentSessionId.ToEntityName(agentName));
            if (processLegacyEntities)
            {
                for (int version = 1; version < AgentSessionId.CurrentEntityVersion; version++)
                {
                    entityNames.Add(AgentSessionId.ToEntityName(agentName, version));
                }
            }
        }

        return entityNames;
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
            agentOptions.AddWorkflowRegisteredAIAgent(agent);
        }
    }
}
