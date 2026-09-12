// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.Compaction;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class DurableAgentHistoryOwnershipTests
{
    [Fact]
    public void FindChatClientAgentTraversesNestedDelegatingWrappers()
    {
        ChatClientAgent chatAgent = new(new StubChatClient(), name: "agent");
        AIAgent wrappedAgent = new TestDelegatingAgent(
            new TestDelegatingAgent(chatAgent));

        ChatClientAgent? discovered =
            DurableAgentHistoryOwnershipResolver.FindChatClientAgent(wrappedAgent);

        Assert.Same(chatAgent, discovered);
    }

    [Fact]
    public async Task DefaultProviderIsEntityOwnedThroughWrapperAsync()
    {
        ChatClientAgent chatAgent = new(new StubChatClient(), name: "agent");
        AIAgent wrappedAgent = new TestDelegatingAgent(chatAgent);
        AgentSession session = await wrappedAgent.CreateSessionAsync();

        (DurableAgentHistoryOwnership ownership, ChatClientAgent? resolvedAgent) =
            DurableAgentHistoryOwnershipResolver.Resolve(wrappedAgent, session);

        Assert.Equal(DurableAgentHistoryOwnership.Entity, ownership);
        Assert.Same(chatAgent, resolvedAgent);
    }

    [Fact]
    public void ExplicitInMemoryProviderIsRejectedBeforeExecution()
    {
        ChatClientAgent chatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = new InMemoryChatHistoryProvider(
                    new InMemoryChatHistoryProviderOptions
                    {
                        ProvideOutputMessageFilter = messages =>
                            messages.Where(message => message.Role != ChatRole.System),
                    }),
            });

        DurableAgentHistoryOwnershipNotSupportedException exception =
            Assert.Throws<DurableAgentHistoryOwnershipNotSupportedException>(
                () => DurableAgentHistoryOwnershipResolver.ValidateStaticConfiguration(chatAgent));

        Assert.Contains("Explicitly configured", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CustomProviderRemainsAuthoritativeAsync()
    {
        CustomHistoryProvider provider = new();
        ChatClientAgent chatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = provider,
            });
        AgentSession session = await chatAgent.CreateSessionAsync();

        (DurableAgentHistoryOwnership ownership, _) =
            DurableAgentHistoryOwnershipResolver.Resolve(chatAgent, session);

        Assert.Equal(DurableAgentHistoryOwnership.ExternalProvider, ownership);
    }

    [Fact]
    public async Task ServiceConversationRemainsAuthoritativeAsync()
    {
        ChatClientAgent chatAgent = new(new StubChatClient(), name: "agent");
        AgentSession session = await chatAgent.CreateSessionAsync("service-id");

        (DurableAgentHistoryOwnership ownership, _) =
            DurableAgentHistoryOwnershipResolver.Resolve(chatAgent, session);

        Assert.Equal(DurableAgentHistoryOwnership.Service, ownership);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task EmptyOrWhitespaceConversationIdDoesNotEstablishServiceOwnershipAsync(
        string conversationId)
    {
        ChatClientAgent chatAgent = new(new StubChatClient(), name: "agent");
        AgentSession session = await chatAgent.CreateSessionAsync(conversationId);

        (DurableAgentHistoryOwnership ownership, _) =
            DurableAgentHistoryOwnershipResolver.Resolve(chatAgent, session);

        Assert.Equal(DurableAgentHistoryOwnership.Entity, ownership);
    }

    [Fact]
    public async Task FrameworkLocalConversationSentinelDoesNotEstablishServiceOwnershipAsync()
    {
        ChatClientAgent chatAgent = new(new StubChatClient(), name: "agent");
        AgentSession session = await chatAgent.CreateSessionAsync(
            DurableAgentHistoryBinding.FrameworkLocalHistoryConversationId);

        (DurableAgentHistoryOwnership ownership, _) =
            DurableAgentHistoryOwnershipResolver.Resolve(chatAgent, session);

        Assert.Equal(DurableAgentHistoryOwnership.Entity, ownership);
    }

    [Fact]
    public async Task PerServiceCallServiceOwnershipIsExplicitAsync()
    {
#pragma warning disable MAAI001
        ChatClientAgent chatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        AgentSession session = await chatAgent.CreateSessionAsync();

        (DurableAgentHistoryOwnership ownership, _) =
            DurableAgentHistoryOwnershipResolver.Resolve(
                chatAgent,
                session,
                serviceManagedPerServiceCallHistory: true);

        Assert.Equal(DurableAgentHistoryOwnership.Service, ownership);
    }

    [Fact]
    public async Task PerServiceCallServiceOwnershipTraversesWrapperAsync()
    {
#pragma warning disable MAAI001
        ChatClientAgent chatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        AIAgent wrappedAgent = new TestDelegatingAgent(chatAgent);
        AgentSession session = await wrappedAgent.CreateSessionAsync();

        (DurableAgentHistoryOwnership ownership, ChatClientAgent? resolvedAgent) =
            DurableAgentHistoryOwnershipResolver.Resolve(
                wrappedAgent,
                session,
                serviceManagedPerServiceCallHistory: true);

        Assert.Equal(DurableAgentHistoryOwnership.Service, ownership);
        Assert.Same(chatAgent, resolvedAgent);
    }

    [Fact]
    public async Task PerServiceCallOwnershipWithoutExplicitConfigurationFailsAsync()
    {
#pragma warning disable MAAI001
        ChatClientAgent chatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        AgentSession session = await chatAgent.CreateSessionAsync();

        Assert.Throws<DurableAgentHistoryOwnershipNotSupportedException>(
            () => DurableAgentHistoryOwnershipResolver.Resolve(chatAgent, session));
    }

    [Fact]
    public async Task ServiceManagedPerCallDeclarationIsIgnoredWhenPerCallPersistenceIsDisabledAsync()
    {
        ChatClientAgent chatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = new CustomHistoryProvider(),
            });
        AgentSession session = await chatAgent.CreateSessionAsync();

        (DurableAgentHistoryOwnership ownership, _) =
            DurableAgentHistoryOwnershipResolver.Resolve(
                chatAgent,
                session,
                serviceManagedPerServiceCallHistory: true);

        Assert.Equal(DurableAgentHistoryOwnership.ExternalProvider, ownership);
    }

    [Fact]
    public void ServiceManagedPerCallDeclarationMatchesAgentNamesCaseInsensitively()
    {
        DurableAgentsOptions options = new();

        DurableAgentsOptions returnedOptions =
            options.SetServiceManagedPerServiceCallHistory("Agent");

        Assert.Same(options, returnedOptions);
        Assert.True(options.IsServiceManagedPerServiceCallHistory("agent"));
        Assert.False(options.IsServiceManagedPerServiceCallHistory("other"));
    }

    [Fact]
    public void HistoryReplayModeDefaultsToEntityPreloadAndMatchesNamesCaseInsensitively()
    {
        DurableAgentsOptions options = new();

        Assert.Equal(
            DurableAgentHistoryReplayMode.PreloadEntityHistory,
            options.GetHistoryReplayMode("agent"));

        DurableAgentsOptions returned = options.SetHistoryReplayMode(
            "Agent",
            DurableAgentHistoryReplayMode.CurrentRequestOnly);

        Assert.Same(options, returned);
        Assert.Equal(
            DurableAgentHistoryReplayMode.CurrentRequestOnly,
            options.GetHistoryReplayMode("agent"));
    }

    [Fact]
    public void InvalidHistoryReplayModeFailsDuringOptionsConfiguration()
    {
        DurableAgentsOptions options = new();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => options.SetHistoryReplayMode(
                "agent",
                (DurableAgentHistoryReplayMode)int.MaxValue));
    }

    [Fact]
    public void LogicalHistoryProviderKeyIsStableCaseInsensitiveRegistrationMetadata()
    {
        DurableAgentsOptions options = new();

        DurableAgentsOptions returned =
            options.SetHistoryProviderKey("Agent", "contoso.history.v1");

        Assert.Same(options, returned);
        Assert.Equal("contoso.history.v1", options.GetHistoryProviderKey("agent"));
        Assert.Throws<InvalidOperationException>(
            () => options.SetHistoryProviderKey("AGENT", "contoso.history.v2"));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("invalid\u0001key")]
    public void InvalidLogicalHistoryProviderKeyFailsDuringConfiguration(string providerKey)
    {
        DurableAgentsOptions options = new();

        Assert.ThrowsAny<Exception>(
            () => options.SetHistoryProviderKey("agent", providerKey));
    }

    [Fact]
    public void DirectAgentRegistrationRejectsStaticCompactionConfiguration()
    {
        ChatClientAgent agent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                AIContextProviders =
                [
                    new CompactionProvider(
                        new SlidingWindowCompactionStrategy(_ => true)),
                ],
            });
        DurableAgentsOptions options = new();

        Assert.Throws<DurableAgentCompactionNotSupportedException>(
            () => options.AddAIAgent(agent));
    }

    [Fact]
    public void UnsupportedLocalProviderOwnershipEnumIsNotPublic()
    {
        Type? ownershipType = typeof(DurableAgentsOptions).Assembly.GetType(
            "Microsoft.Agents.AI.DurableTask.DurableAgentPerServiceCallHistoryOwnership");

        Assert.Null(ownershipType);
    }

    [Fact]
    public async Task AgentWithoutChatPipelineUsesFallbackAsync()
    {
        AIAgent agent = new StubAgent();
        AgentSession session = await agent.CreateSessionAsync();

        (DurableAgentHistoryOwnership ownership, ChatClientAgent? chatClientAgent) =
            DurableAgentHistoryOwnershipResolver.Resolve(agent, session);

        Assert.Equal(DurableAgentHistoryOwnership.NoContextPipeline, ownership);
        Assert.Null(chatClientAgent);
    }

    [Theory]
    [InlineData(DurableAgentHistoryReplayMode.PreloadEntityHistory, "Entity")]
    [InlineData(DurableAgentHistoryReplayMode.CurrentRequestOnly, "AgentSession")]
    public void NoContextPipelineMapsToFixedRuntimeOwner(
        DurableAgentHistoryReplayMode replayMode,
        string expected)
    {
        Assert.Equal(
            expected,
            DurableAgentHistoryOwnershipResolver.GetEffectiveOwnership(
                DurableAgentHistoryOwnership.NoContextPipeline,
                replayMode).ToString());
    }

    [Fact]
    public void HiddenBuilderPipelineCannotBeUniversallyInspectedThroughPublicAgentServices()
    {
        ChatClientAgent hiddenChatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "hidden",
                AIContextProviders =
                [
                    new CompactionProvider(
                        new SlidingWindowCompactionStrategy(_ => true)),
                ],
            });
        AIAgent opaqueAgent = new OpaqueAgent(hiddenChatAgent);

        Assert.Null(DurableAgentHistoryOwnershipResolver.FindChatClientAgent(opaqueAgent));
        DurableAgentHistoryOwnershipResolver.ValidateStaticConfiguration(opaqueAgent);
    }

    [Fact]
    public async Task StatefulReducerFailsBeforeExecutionAsync()
    {
        ChatClientAgent chatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = new InMemoryChatHistoryProvider(
                    new InMemoryChatHistoryProviderOptions
                    {
                        ChatReducer = new NoOpReducer(),
                    }),
            });
        AgentSession session = await chatAgent.CreateSessionAsync();

        Assert.Throws<DurableAgentCompactionNotSupportedException>(
            () => DurableAgentHistoryOwnershipResolver.Resolve(chatAgent, session));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompactionProviderFailsForExternalAndServiceOwnershipAsync(bool serviceOwned)
    {
        ChatClientAgent chatAgent = new(
            new StubChatClient(),
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = new CustomHistoryProvider(),
                AIContextProviders =
                [
                    new CompactionProvider(
                        new SlidingWindowCompactionStrategy(_ => true)),
                ],
            });
        AgentSession session = serviceOwned
            ? await chatAgent.CreateSessionAsync("service-id")
            : await chatAgent.CreateSessionAsync();

        Assert.Throws<DurableAgentCompactionNotSupportedException>(
            () => DurableAgentHistoryOwnershipResolver.Resolve(chatAgent, session));
    }

    private sealed class TestDelegatingAgent(AIAgent innerAgent) : DelegatingAIAgent(innerAgent);

    private sealed class OpaqueAgent(AIAgent innerAgent) : AIAgent
    {
        public override string? Name => "opaque";

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) =>
            innerAgent.CreateSessionAsync(cancellationToken);

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            innerAgent.RunAsync(messages, session, options, cancellationToken);

        protected override IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            innerAgent.RunStreamingAsync(messages, session, options, cancellationToken);

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            innerAgent.SerializeSessionAsync(session, jsonSerializerOptions, cancellationToken);

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            innerAgent.DeserializeSessionAsync(serializedState, jsonSerializerOptions, cancellationToken);
    }

    private sealed class CustomHistoryProvider : ChatHistoryProvider;

    private sealed class NoOpReducer : IChatReducer
    {
        public Task<IEnumerable<ChatMessage>> ReduceAsync(
            IEnumerable<ChatMessage> messages,
            CancellationToken cancellationToken) => Task.FromResult(messages);
    }

    private sealed class StubAgent : AIAgent
    {
        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) => new(new StubSession());

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new AgentResponse());

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield break;
        }

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(JsonSerializer.SerializeToElement(new { }));

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) => new(new StubSession());

        private sealed class StubSession : AgentSession;
    }

    private sealed class StubChatClient : IChatClient
    {
        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, "response")));

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, "response");
        }
    }
}
