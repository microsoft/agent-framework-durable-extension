// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Azure.Functions.Worker.Core.FunctionMetadata;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions;

/// <summary>
/// Transforms function metadata by registering durable agent functions for each explicitly configured agent.
/// </summary>
/// <remarks>
/// This transformer adds entity, HTTP, and MCP tool trigger functions for agents that have
/// explicit <see cref="FunctionsAgentOptions"/>. Agents auto-registered by workflows
/// (which lack explicit options) are handled by <see cref="DurableWorkflowsFunctionMetadataTransformer"/>.
/// </remarks>
internal sealed class DurableAgentFunctionMetadataTransformer : IFunctionMetadataTransformer
{
    private readonly ILogger<DurableAgentFunctionMetadataTransformer> _logger;
    private readonly IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>> _agents;
    private readonly IServiceProvider _serviceProvider;
    private readonly IFunctionsAgentOptionsProvider _functionsAgentOptionsProvider;

    public DurableAgentFunctionMetadataTransformer(
        IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>> agents,
        ILogger<DurableAgentFunctionMetadataTransformer> logger,
        IServiceProvider serviceProvider,
        IFunctionsAgentOptionsProvider functionsAgentOptionsProvider)
    {
        this._agents = agents ?? throw new ArgumentNullException(nameof(agents));
        this._logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this._serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this._functionsAgentOptionsProvider = functionsAgentOptionsProvider ?? throw new ArgumentNullException(nameof(functionsAgentOptionsProvider));
    }

    public string Name => nameof(DurableAgentFunctionMetadataTransformer);

    public void Transform(IList<IFunctionMetadata> original)
    {
        this._logger.LogTransformingFunctionMetadata(original.Count);

        // Seed with existing function names to avoid duplicates across transformers. An agent that a workflow
        // references and that is also registered explicitly (a promoted agent) is emitted by both transformers,
        // and whichever runs second must not re-add a trigger the other already contributed.
        HashSet<string> registeredFunctions = new(
            original.Select(f => f.Name!),
            StringComparer.OrdinalIgnoreCase);

        foreach (KeyValuePair<string, Func<IServiceProvider, AIAgent>> kvp in this._agents)
        {
            string agentName = kvp.Key;

            // Only generate triggers for agents with explicit Functions agent options.
            // Agents auto-registered by workflows are handled by DurableWorkflowsFunctionMetadataTransformer.
            if (!this._functionsAgentOptionsProvider.TryGet(agentName, out FunctionsAgentOptions? agentTriggerOptions))
            {
                continue;
            }

            AddIfNew(original, registeredFunctions, FunctionMetadataFactory.CreateEntityTrigger(agentName), () => this._logger.LogRegisteringTriggerForAgent(agentName, "entity"));

            if (agentTriggerOptions.HttpTrigger.IsEnabled)
            {
                AddIfNew(
                    original,
                    registeredFunctions,
                    FunctionMetadataFactory.CreateHttpTrigger(agentName, $"agents/{agentName}/run", BuiltInFunctions.RunAgentHttpFunctionEntryPoint),
                    () => this._logger.LogRegisteringTriggerForAgent(agentName, "http"));
            }

            if (agentTriggerOptions.McpToolTrigger.IsEnabled)
            {
                AIAgent agent = kvp.Value(this._serviceProvider);
                AddIfNew(original, registeredFunctions, CreateMcpToolTrigger(agentName, agent.Description), () => this._logger.LogRegisteringTriggerForAgent(agentName, "mcpTool"));
            }
        }
    }

    private static void AddIfNew(
        IList<IFunctionMetadata> original,
        HashSet<string> registeredFunctions,
        DefaultFunctionMetadata metadata,
        Action logRegistration)
    {
        if (!registeredFunctions.Add(metadata.Name!))
        {
            return;
        }

        logRegistration();
        original.Add(metadata);
    }

    private static DefaultFunctionMetadata CreateMcpToolTrigger(string agentName, string? description)
    {
        return new DefaultFunctionMetadata
        {
            Name = $"{BuiltInFunctions.McpToolPrefix}{agentName}",
            Language = "dotnet-isolated",
            RawBindings =
            [
                $$"""{"name":"context","type":"mcpToolTrigger","direction":"In","toolName":"{{agentName}}","description":"{{description}}","toolProperties":"[{\"propertyName\":\"query\",\"propertyType\":\"string\",\"description\":\"The query to send to the agent.\",\"isRequired\":true,\"isArray\":false},{\"propertyName\":\"sessionId\",\"propertyType\":\"string\",\"description\":\"Optional session identifier.\",\"isRequired\":false,\"isArray\":false}]"}""",
                """{"name":"query","type":"mcpToolProperty","direction":"In","propertyName":"query","description":"The query to send to the agent","isRequired":true,"dataType":"String","propertyType":"string"}""",
                """{"name":"sessionId","type":"mcpToolProperty","direction":"In","propertyName":"sessionId","description":"The session identifier.","isRequired":false,"dataType":"String","propertyType":"string"}""",
                """{"name":"client","type":"durableClient","direction":"In"}"""
            ],
            EntryPoint = BuiltInFunctions.RunAgentMcpToolFunctionEntryPoint,
            ScriptFile = BuiltInFunctions.ScriptFile,
        };
    }
}
