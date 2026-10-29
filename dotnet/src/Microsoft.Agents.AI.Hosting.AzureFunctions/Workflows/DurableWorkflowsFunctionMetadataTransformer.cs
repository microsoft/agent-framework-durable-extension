// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;
using Microsoft.Azure.Functions.Worker.Core.FunctionMetadata;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions;

/// <summary>
/// Transforms function metadata by dynamically registering Azure Functions triggers
/// for each configured durable workflow and its executors.
/// </summary>
/// <remarks>
/// For each workflow, this transformer registers:
/// <list type="bullet">
///   <item><description>An HTTP trigger function to start the workflow orchestration via HTTP.</description></item>
///   <item><description>An orchestration trigger function to run the workflow orchestration.</description></item>
///   <item><description>An activity trigger function for each non-agent executor in the workflow.</description></item>
///   <item><description>An entity trigger function for each AI agent executor in the workflow.</description></item>
/// </list>
/// When multiple workflows share the same executor, the corresponding function is registered only once.
/// </remarks>
internal sealed class DurableWorkflowsFunctionMetadataTransformer : IFunctionMetadataTransformer
{
    private readonly ILogger<DurableWorkflowsFunctionMetadataTransformer> _logger;
    private readonly FunctionsDurableOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="DurableWorkflowsFunctionMetadataTransformer"/> class.
    /// </summary>
    /// <param name="logger">The logger instance for diagnostic output.</param>
    /// <param name="durableOptions">The durable options containing workflow configurations.</param>
    public DurableWorkflowsFunctionMetadataTransformer(
        ILogger<DurableWorkflowsFunctionMetadataTransformer> logger,
        FunctionsDurableOptions durableOptions)
    {
        this._logger = logger ?? throw new ArgumentNullException(nameof(logger));
        ArgumentNullException.ThrowIfNull(durableOptions);
        this._options = durableOptions;
    }

    /// <inheritdoc />
    public string Name => nameof(DurableWorkflowsFunctionMetadataTransformer);

    /// <inheritdoc />
    public void Transform(IList<IFunctionMetadata> original)
    {
        int initialCount = original.Count;
        this._logger.LogTransformingFunctionMetadata(initialCount);

        this._options.Agents.ValidateConfiguration();
        bool processLegacyEntities = this._options.Agents.ProcessLegacyEntities;

        // Seed with existing functions by name to deduplicate same-kind triggers across transformers
        // (e.g., when DurableAgentFunctionMetadataTransformer already registered entity triggers)
        // while rejecting cross-kind collisions.
        Dictionary<string, IFunctionMetadata> registeredFunctions = new(StringComparer.OrdinalIgnoreCase);
        foreach (IFunctionMetadata existing in original)
        {
            if (existing.Name is not null)
            {
                registeredFunctions[existing.Name] = existing;
            }
        }

        DurableWorkflowOptions workflowOptions = this._options.Workflows;
        foreach (var workflow in workflowOptions.Workflows)
        {
            string httpFunctionName = $"{BuiltInFunctions.HttpPrefix}{workflow.Key}";

            if (this._logger.IsEnabled(LogLevel.Information))
            {
                this._logger.LogInformation("Registering durable workflow functions for workflow '{WorkflowKey}' with HTTP trigger function name '{HttpFunctionName}'", workflow.Key, httpFunctionName);
            }

            // Register an orchestration function for the workflow.
            string orchestrationFunctionName = WorkflowNamingHelper.ToOrchestrationFunctionName(workflow.Key);
            AddIfNew(
                original,
                registeredFunctions,
                FunctionMetadataFactory.CreateOrchestrationTrigger(
                    orchestrationFunctionName,
                    BuiltInFunctions.RunWorkflowOrchestrationFunctionEntryPoint),
                () => this._logger.LogRegisteringWorkflowTrigger(workflow.Key, orchestrationFunctionName, "orchestration"));

            // Register an HTTP trigger so users can start this workflow via HTTP.
            AddIfNew(
                original,
                registeredFunctions,
                FunctionMetadataFactory.CreateHttpTrigger(
                    workflow.Key,
                    $"workflows/{workflow.Key}/run",
                    BuiltInFunctions.RunWorkflowOrchestrationHttpFunctionEntryPoint),
                () => this._logger.LogRegisteringWorkflowTrigger(workflow.Key, httpFunctionName, "http"));

            // Register a status endpoint if opted in via AddWorkflow(enableStatusEndpoint: true).
            if (this._options.IsStatusEndpointEnabled(workflow.Key))
            {
                string statusFunctionName = $"{BuiltInFunctions.HttpPrefix}{workflow.Key}{BuiltInFunctions.StatusFunctionSuffix}";
                AddIfNew(
                    original,
                    registeredFunctions,
                    FunctionMetadataFactory.CreateHttpTrigger(
                        $"{workflow.Key}-status",
                        $"workflows/{workflow.Key}/status/{{runId}}",
                        BuiltInFunctions.GetWorkflowStatusHttpFunctionEntryPoint,
                        methods: "\"get\""),
                    () => this._logger.LogRegisteringWorkflowTrigger(workflow.Key, statusFunctionName, "http-status"));
            }

            // Register a respond endpoint when the workflow contains RequestPort nodes.
            bool hasRequestPorts = workflow.Value.ReflectExecutors().Values.Any(b => b is RequestPortBinding);
            if (hasRequestPorts)
            {
                string respondFunctionName = $"{BuiltInFunctions.HttpPrefix}{workflow.Key}{BuiltInFunctions.RespondFunctionSuffix}";
                AddIfNew(
                    original,
                    registeredFunctions,
                    FunctionMetadataFactory.CreateHttpTrigger(
                        $"{workflow.Key}-respond",
                        $"workflows/{workflow.Key}/respond/{{runId}}",
                        BuiltInFunctions.RespondToWorkflowHttpFunctionEntryPoint),
                    () => this._logger.LogRegisteringWorkflowTrigger(workflow.Key, respondFunctionName, "http-respond"));
            }

            // Register an MCP tool trigger if opted in via AddWorkflow(enableStatusEndpoint: ..., enableMcpToolTrigger: true).
            if (this._options.IsMcpToolTriggerEnabled(workflow.Key))
            {
                string mcpToolFunctionName = $"{BuiltInFunctions.McpToolPrefix}{workflow.Key}";
                AddIfNew(
                    original,
                    registeredFunctions,
                    FunctionMetadataFactory.CreateWorkflowMcpToolTrigger(workflow.Key, workflow.Value.Description),
                    () => this._logger.LogRegisteringWorkflowTrigger(workflow.Key, mcpToolFunctionName, "mcpTool"));
            }

            // Register activity or entity functions for each executor in the workflow.
            // ReflectExecutors() returns all executors across the graph; no need to manually traverse edges.
            foreach (KeyValuePair<string, ExecutorBinding> entry in workflow.Value.ReflectExecutors())
            {
                // Sub-workflow and RequestPort bindings use specialized dispatch, not activities.
                if (entry.Value is SubworkflowBinding or RequestPortBinding)
                {
                    continue;
                }

                string executorName = WorkflowNamingHelper.GetExecutorName(entry.Key);

                // AI agent executors are backed by durable entities; other executors use activity triggers.
                if (entry.Value is AIAgentBinding)
                {
                    string entityName = AgentSessionId.ToEntityName(executorName);
                    AddIfNew(
                        original,
                        registeredFunctions,
                        FunctionMetadataFactory.CreateEntityTrigger(executorName),
                        () => this._logger.LogRegisteringWorkflowTrigger(workflow.Key, entityName, "entity"));

                    if (processLegacyEntities)
                    {
                        for (int version = 1; version < AgentSessionId.CurrentEntityVersion; version++)
                        {
                            string legacyEntityName = AgentSessionId.ToEntityName(executorName, version);
                            AddIfNew(
                                original,
                                registeredFunctions,
                                FunctionMetadataFactory.CreateEntityTrigger(executorName, version),
                                () => this._logger.LogRegisteringWorkflowTrigger(workflow.Key, legacyEntityName, "entity"));
                        }
                    }
                }
                else
                {
                    string functionName = WorkflowNamingHelper.ToOrchestrationFunctionName(executorName);
                    AddIfNew(
                        original,
                        registeredFunctions,
                        FunctionMetadataFactory.CreateActivityTrigger(functionName),
                        () => this._logger.LogRegisteringWorkflowTrigger(workflow.Key, functionName, "activity"));
                }
            }
        }

        this._logger.LogTransformationComplete(original.Count - initialCount, original.Count);
    }

    private static void AddIfNew(
        IList<IFunctionMetadata> original,
        Dictionary<string, IFunctionMetadata> registeredFunctions,
        DefaultFunctionMetadata metadata,
        Action logRegistration)
    {
        if (registeredFunctions.TryGetValue(metadata.Name!, out IFunctionMetadata? existing))
        {
            if (!string.Equals(existing.EntryPoint, metadata.EntryPoint, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Function name '{metadata.Name}' is already registered with entry point '{existing.EntryPoint}' and cannot also be registered with entry point '{metadata.EntryPoint}'.");
            }

            return;
        }

        registeredFunctions[metadata.Name!] = metadata;
        logRegistration();
        original.Add(metadata);
    }
}
