// Copyright (c) Microsoft. All rights reserved.

using System.Runtime.CompilerServices;
using Microsoft.Agents.AI;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Adapts a registered agent for one durable entity invocation without replacing or reconfiguring
/// the registered agent instance.
/// </summary>
/// <remarks>
/// The wrapper runs inside the <see cref="DurableAgentContext"/> established by
/// <see cref="AgentEntity"/>, supplies entity-scoped identity and services to tool middleware,
/// applies request-specific tool and response options, and can inject an operation-scoped
/// <see cref="ChatHistoryProvider"/> override. The provider and wrapper only stage changes in the
/// entity operation's working state; <see cref="AgentEntity"/> owns the final durable-state commit.
/// </remarks>
internal sealed class EntityAgentWrapper(
    AIAgent innerAgent,
    TaskEntityContext entityContext,
    RunRequest runRequest,
    IServiceProvider? entityScopedServices = null,
    ChatHistoryProvider? chatHistoryProvider = null) : DelegatingAIAgent(innerAgent)
{
    private readonly TaskEntityContext _entityContext = entityContext;
    private readonly RunRequest _runRequest = runRequest;
    private readonly IServiceProvider? _entityScopedServices = entityScopedServices;
    private readonly ChatHistoryProvider? _chatHistoryProvider = chatHistoryProvider;

    // Durable callers address the entity-backed proxy, not the inner agent's local/server resource.
    protected override string? IdCore => this._entityContext.Id.ToString();

    protected override async Task<AgentResponse> RunCoreAsync(
        IEnumerable<ChatMessage> messages,
        AgentSession? session = null,
        AgentRunOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        AgentResponse response = await base.RunCoreAsync(
            messages,
            session,
            this.GetAgentEntityRunOptions(options),
            cancellationToken);

        // The durable proxy identity is authoritative even when the wrapped agent supplies its own ID.
        response.AgentId = this.Id;
        return response;
    }

    protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
        IEnumerable<ChatMessage> messages,
        AgentSession? session = null,
        AgentRunOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (AgentResponseUpdate update in base.RunCoreStreamingAsync(
            messages,
            session,
            this.GetAgentEntityRunOptions(options),
            cancellationToken))
        {
            // Aggregation copies AgentId from streaming updates, so normalize every update rather
            // than allowing a wrapped Foundry/server agent ID to leak into the durable response.
            update.AgentId = this.Id;
            yield return update;
        }
    }

    // Override the GetService method to provide entity-scoped services.
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        object? result = null;
        if (this._entityScopedServices is not null)
        {
            result = (serviceKey is not null && this._entityScopedServices is IKeyedServiceProvider keyedServiceProvider)
                ? keyedServiceProvider.GetKeyedService(serviceType, serviceKey)
                : this._entityScopedServices.GetService(serviceType);
        }

        return result ?? base.GetService(serviceType, serviceKey);
    }

    private AgentRunOptions GetAgentEntityRunOptions(AgentRunOptions? options = null)
    {
        // Copied/modified from FunctionInvocationDelegatingAgent.cs in microsoft/agent-framework.
        if (options is null || options.GetType() == typeof(AgentRunOptions))
        {
            options = new ChatClientAgentRunOptions();
        }
        else
        {
            options = options.Clone();
        }

        if (options is not ChatClientAgentRunOptions chatAgentRunOptions)
        {
            throw new NotSupportedException($"Function Invocation Middleware is only supported without options or with {nameof(ChatClientAgentRunOptions)}.");
        }

        Func<IChatClient, IChatClient>? originalFactory = chatAgentRunOptions.ChatClientFactory;

        if (this._chatHistoryProvider is not null)
        {
            chatAgentRunOptions.AdditionalProperties ??= [];

            // MAF's typed AdditionalProperties API stores the provider instance under
            // typeof(ChatHistoryProvider).FullName and resolves that exact instance for this run.
            // A type name alone could not carry the operation-scoped working state and correlation.
            if (!chatAgentRunOptions.AdditionalProperties.TryAdd(
                this._chatHistoryProvider))
            {
                throw new InvalidOperationException(
                    "A ChatHistoryProvider override is already present in the agent run options. " +
                    "Durable entity-owned history requires its operation-scoped provider to be authoritative.");
            }
        }

        chatAgentRunOptions.ChatClientFactory = chatClient =>
        {
            ChatClientBuilder builder = chatClient.AsBuilder();
            if (originalFactory is not null)
            {
                builder.Use(originalFactory);
            }

            // Update the run options based on the run request.
            // NOTE: Function middleware can go here if needed in the future.
            return builder.ConfigureOptions(
                newOptions =>
                {
                    // Update the response format if requested by the caller.
                    if (this._runRequest.ResponseFormat is not null)
                    {
                        newOptions.ResponseFormat = this._runRequest.ResponseFormat;
                    }

                    // Update the tools if requested by the caller.
                    if (this._runRequest.EnableToolCalls)
                    {
                        IList<AITool>? tools = chatAgentRunOptions.ChatOptions?.Tools;
                        if (tools is not null && this._runRequest.EnableToolNames?.Count > 0)
                        {
                            // Filter tools to only include those with matching names
                            newOptions.Tools = [.. tools.Where(tool => this._runRequest.EnableToolNames.Contains(tool.Name))];
                        }
                    }
                    else
                    {
                        newOptions.Tools = null;
                    }
                })
                .Build();
        };

        return options;
    }
}
