// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.Compaction;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Describes which component owns conversation history for one agent invocation.
/// </summary>
internal enum DurableAgentHistoryOwnership
{
    Entity,
    ExternalProvider,
    Service,
    NoContextPipeline,
    AgentSession,
}

/// <summary>
/// Captures the validated, invocation-independent history configuration discovered from an agent pipeline.
/// </summary>
internal readonly record struct ValidatedDurableAgentHistoryConfiguration(
    ChatClientAgent? ChatClientAgent,
    bool RequiresPerServiceCallPersistence);

/// <summary>
/// Resolves durable history ownership through the supported agent service traversal.
/// </summary>
internal static class DurableAgentHistoryOwnershipResolver
{
    public static (DurableAgentHistoryOwnership Ownership, ChatClientAgent? ChatClientAgent) Resolve(
        AIAgent agent,
        AgentSession session,
        bool serviceManagedPerServiceCallHistory = false)
    {
        ValidatedDurableAgentHistoryConfiguration configuration = ValidateRunConfiguration(
            agent,
            serviceManagedPerServiceCallHistory);
        return Resolve(session, configuration);
    }

    /// <summary>
    /// Converts pipeline discovery into the effective owner used by the durable session contract.
    /// </summary>
    public static DurableAgentHistoryOwnership GetEffectiveOwnership(
        DurableAgentHistoryOwnership ownership,
        DurableAgentHistoryReplayMode replayMode)
    {
        return ownership == DurableAgentHistoryOwnership.NoContextPipeline
            ? replayMode == DurableAgentHistoryReplayMode.PreloadEntityHistory
                ? DurableAgentHistoryOwnership.Entity
                : DurableAgentHistoryOwnership.AgentSession
            : ownership;
    }

    public static (DurableAgentHistoryOwnership Ownership, ChatClientAgent? ChatClientAgent) Resolve(
        AgentSession session,
        ValidatedDurableAgentHistoryConfiguration configuration)
    {
        ChatClientAgent? chatClientAgent = configuration.ChatClientAgent;
        if (chatClientAgent is null)
        {
            // No discoverable Agent Framework chat pipeline can supply context, so the entity
            // applies the configured generic-agent replay policy itself.
            return (DurableAgentHistoryOwnership.NoContextPipeline, null);
        }

        if (configuration.RequiresPerServiceCallPersistence)
        {
            // Run validation already established the explicit service-ownership declaration. MAF's
            // framework-local sentinel and a real service ID share ConversationId in this mode, so
            // restored session state cannot decide this branch.
            return (DurableAgentHistoryOwnership.Service, chatClientAgent);
        }

        if (session is ChatClientAgentSession chatSession &&
            DurableAgentHistoryBinding.IsRealServiceConversationId(chatSession.ConversationId))
        {
            // Outside per-service-call persistence, a restored conversation ID has its ordinary
            // meaning: the model service owns and continues the conversation.
            return (DurableAgentHistoryOwnership.Service, chatClientAgent);
        }

        if (chatClientAgent.ChatHistoryProvider is not InMemoryChatHistoryProvider)
        {
            // A custom provider remains authoritative; replacing it would bypass the provider's
            // storage and session-state contract.
            return (DurableAgentHistoryOwnership.ExternalProvider, chatClientAgent);
        }

        // The default in-memory provider has no durable store of its own, so entity state owns the transcript.
        return (DurableAgentHistoryOwnership.Entity, chatClientAgent);
    }

    /// <summary>
    /// Finds the underlying <see cref="ChatClientAgent"/> exposed by the agent's service chain.
    /// </summary>
    public static ChatClientAgent? FindChatClientAgent(AIAgent agent)
    {
        ArgumentNullException.ThrowIfNull(agent);
        return agent.GetService<ChatClientAgent>();
    }

    /// <summary>
    /// Validates directly discoverable pipeline properties that are independent of entity state,
    /// durable options, and an agent session.
    /// </summary>
    /// <remarks>
    /// Agent Framework does not currently expose a public traversal contract for providers hidden
    /// inside custom or builder-installed decorators, so those pipelines cannot be inspected here.
    /// </remarks>
    public static void ValidateStaticConfiguration(AIAgent agent)
    {
        ValidateStaticConfiguration(FindChatClientAgent(agent));
    }

    /// <summary>
    /// Validates configuration that depends on the completed durable options composition and returns
    /// the discovered values needed later for session-dependent ownership resolution.
    /// </summary>
    public static ValidatedDurableAgentHistoryConfiguration ValidateRunConfiguration(
        AIAgent agent,
        bool serviceManagedPerServiceCallHistory = false)
    {
        ChatClientAgent? chatClientAgent = FindChatClientAgent(agent);
        ValidateStaticConfiguration(chatClientAgent);
        if (chatClientAgent is null)
        {
            return default;
        }

#pragma warning disable MAAI001
        bool requiresPerServiceCallPersistence =
            chatClientAgent.GetService<ChatClientAgentOptions>()?.RequirePerServiceCallChatHistoryPersistence is true;
#pragma warning restore MAAI001
        if (!requiresPerServiceCallPersistence)
        {
            return new(chatClientAgent, RequiresPerServiceCallPersistence: false);
        }

        if (!serviceManagedPerServiceCallHistory)
        {
            // MAF's per-call decorator uses the same public ConversationId slot for a real service ID and
            // the internal "_agent_local_chat_history" sentinel, so the restored session cannot disambiguate
            // ownership. Local provider callbacks also occur after every model call in a tool loop and expose
            // no finality flag. A durable polling caller must receive only the completed outer AgentResponse,
            // not an intermediate tool-call response, so only explicitly service-owned history is supported.
            throw new DurableAgentHistoryOwnershipNotSupportedException();
        }

        return new(chatClientAgent, RequiresPerServiceCallPersistence: true);
    }

    private static void ValidateStaticConfiguration(ChatClientAgent? chatClientAgent)
    {
        if (chatClientAgent is null)
        {
            return;
        }

#pragma warning disable MAAI001
        ChatClientAgentOptions? options =
            chatClientAgent.GetService<ChatClientAgentOptions>();
#pragma warning restore MAAI001
        if (HasStatefulCompaction(chatClientAgent))
        {
            throw new DurableAgentCompactionNotSupportedException();
        }

        if (options?.ChatHistoryProvider is InMemoryChatHistoryProvider)
        {
            throw new DurableAgentHistoryOwnershipNotSupportedException(
                "Explicitly configured InMemoryChatHistoryProvider instances are not supported for " +
                "durable entity-owned history. The pinned Agent Framework API does not expose the " +
                "configured StateInitializer or message filters, so replacing that provider could " +
                "silently change history semantics. Use the implicit default provider or a custom " +
                "external provider with a stable logical key.");
        }
    }

    private static bool HasStatefulCompaction(ChatClientAgent chatClientAgent)
    {
#pragma warning disable MAAI001
        if (chatClientAgent.AIContextProviders?.Any(provider => provider is CompactionProvider) is true)
#pragma warning restore MAAI001
        {
            return true;
        }

        return chatClientAgent.ChatHistoryProvider is InMemoryChatHistoryProvider { ChatReducer: not null };
    }
}
