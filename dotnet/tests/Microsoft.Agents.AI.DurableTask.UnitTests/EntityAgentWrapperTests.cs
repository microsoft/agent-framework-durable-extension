// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class EntityAgentWrapperTests
{
    [Fact]
    public async Task DurableIdentityOverridesInnerIdentityForResponsesAndUpdatesAsync()
    {
        IdentityAgent innerAgent = new();
        Mock<TaskEntityContext> context = CreateContext();
        EntityAgentWrapper wrapper = new(
            innerAgent,
            context.Object,
            new RunRequest("request") { CorrelationId = "correlation" });
        AgentSession session = await wrapper.CreateSessionAsync();
        string expectedAgentId = context.Object.Id.ToString();

        AgentResponse response = await wrapper.RunAsync("request", session);
        List<AgentResponseUpdate> updates = [];
        await foreach (AgentResponseUpdate update in wrapper.RunStreamingAsync("request", session))
        {
            updates.Add(update);
        }

        Assert.Equal(expectedAgentId, response.AgentId);
        Assert.All(updates, update => Assert.Equal(expectedAgentId, update.AgentId));
        Assert.Equal(expectedAgentId, updates.ToAgentResponse().AgentId);
    }

    [Fact]
    public async Task OperationScopedProviderInstanceOverridesConfiguredProviderWithoutMutatingCallerOptionsAsync()
    {
        RecordingHistoryProvider configuredProvider = new();
        RecordingHistoryProvider operationProvider = new();
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = configuredProvider,
            });
        AgentSession session = await chatAgent.CreateSessionAsync();
        Mock<TaskEntityContext> context = CreateContext();
        EntityAgentWrapper wrapper = new(
            chatAgent,
            context.Object,
            new RunRequest("request") { CorrelationId = "correlation" },
            chatHistoryProvider: operationProvider);
        ChatClientAgentRunOptions callerOptions = new()
        {
            AdditionalProperties = new() { ["caller"] = "preserved" },
        };

        _ = await wrapper.RunAsync("request", session, callerOptions);

        Assert.Equal(1, operationProvider.LoadCount);
        Assert.Equal(1, operationProvider.StoreCount);
        Assert.Equal(0, configuredProvider.LoadCount);
        Assert.Equal(0, configuredProvider.StoreCount);
        Assert.Equal(["operation history", "request"], client.LastMessages.Select(message => message.Text));
        Assert.False(callerOptions.AdditionalProperties.Contains<ChatHistoryProvider>());
        Assert.Equal("preserved", callerOptions.AdditionalProperties["caller"]);
    }

    [Fact]
    public async Task ExistingProviderOverrideIsRejectedWithoutMutatingCallerOptionsAsync()
    {
        RecordingHistoryProvider existingProvider = new();
        RecordingHistoryProvider operationProvider = new();
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "agent");
        AgentSession session = await chatAgent.CreateSessionAsync();
        Mock<TaskEntityContext> context = CreateContext();
        EntityAgentWrapper wrapper = new(
            chatAgent,
            context.Object,
            new RunRequest("request") { CorrelationId = "correlation" },
            chatHistoryProvider: operationProvider);
        ChatClientAgentRunOptions callerOptions = new()
        {
            AdditionalProperties = [],
        };
        callerOptions.AdditionalProperties.Add<ChatHistoryProvider>(existingProvider);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => wrapper.RunAsync("request", session, callerOptions));

        Assert.Contains("already present", exception.Message, StringComparison.Ordinal);
        Assert.True(callerOptions.AdditionalProperties.TryGetValue(
            out ChatHistoryProvider? retainedProvider));
        Assert.Same(existingProvider, retainedProvider);
        Assert.Equal(0, client.InvocationCount);
        Assert.Equal(0, operationProvider.LoadCount);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task DurableProviderCompletesActualSdkRunAcrossHistoryOwnershipTransitionAsync(
        bool streaming,
        bool serviceManaged)
    {
        RunRequest request = new("request") { CorrelationId = "correlation" };
        List<DurableAgentStateEntry> history = [];
        DurableChatHistoryProvider provider = new(history, request, allowLosslessV2: true);
        RecordingChatClient client = new()
        {
            ResponseConversationId = serviceManaged ? "service-id" : null,
        };
        ChatClientAgent agent = new(client, name: "agent");
        AgentSession session = await agent.CreateSessionAsync();
        EntityAgentWrapper wrapper = new(
            agent,
            CreateContext().Object,
            request,
            chatHistoryProvider: provider);
        ChatClientAgentRunOptions callerOptions = new()
        {
            AdditionalProperties = new() { ["caller"] = "preserved" },
            ChatClientFactory = inner => inner.AsBuilder().ConfigureOptions(
                options => options.Temperature = 0.25f).Build(),
        };

        AgentResponse response = streaming
            ? await wrapper.RunStreamingAsync(request.Messages, session, callerOptions).ToAgentResponseAsync()
            : await wrapper.RunAsync(request.Messages, session, callerOptions);

        Assert.Equal("response", response.Text);
        Assert.Equal(client.ResponseConversationId, Assert.IsType<ChatClientAgentSession>(session).ConversationId);
        Assert.Equal(["request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(1, client.InvocationCount);
        Assert.Equal(0.25f, client.LastOptions?.Temperature);
        Assert.Equal(!serviceManaged, provider.HasStagedTurn);
        Assert.Equal(serviceManaged ? 0 : 2, history.Count);
        Assert.IsType<InMemoryChatHistoryProvider>(agent.ChatHistoryProvider);
        Assert.False(callerOptions.AdditionalProperties.Contains<ChatHistoryProvider>());
        Assert.Equal("preserved", callerOptions.AdditionalProperties["caller"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DurableProviderDoesNotDisableConfiguredSdkProviderConflictAsync(bool streaming)
    {
        RecordingHistoryProvider configuredProvider = new();
        RecordingChatClient client = new() { ResponseConversationId = "service-id" };
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions { Name = "agent", ChatHistoryProvider = configuredProvider });
        AgentSession session = await agent.CreateSessionAsync();
        RunRequest request = new("request") { CorrelationId = "correlation" };
        List<DurableAgentStateEntry> history = [];
        DurableChatHistoryProvider provider = new(history, request, allowLosslessV2: true);
        EntityAgentWrapper wrapper = new(
            agent,
            CreateContext().Object,
            request,
            chatHistoryProvider: provider);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            if (streaming)
            {
                _ = await wrapper.RunStreamingAsync(request.Messages, session).ToAgentResponseAsync();
            }
            else
            {
                _ = await wrapper.RunAsync(request.Messages, session);
            }
        });

        Assert.Same(configuredProvider, agent.ChatHistoryProvider);
        Assert.Equal(0, configuredProvider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(provider.HasStagedTurn);
        Assert.Empty(history);
    }

    [Fact]
    public async Task PartialServiceStreamFailureRetainsDurableProviderForSdkFailureCallbacksAsync()
    {
        InvalidOperationException failure = new("Model stream failed.");
        RecordingChatClient client = new()
        {
            ResponseConversationId = "service-id",
            StreamingFailure = failure,
        };
        ChatClientAgent agent = new(client, name: "agent");
        AgentSession session = await agent.CreateSessionAsync();
        RunRequest request = new("request") { CorrelationId = "correlation" };
        List<DurableAgentStateEntry> history = [];
        DurableChatHistoryProvider provider = new(history, request, allowLosslessV2: true);
        EntityAgentWrapper wrapper = new(
            agent,
            CreateContext().Object,
            request,
            chatHistoryProvider: provider);
        ChatOptions? invocationOptions = null;
        ChatClientAgentRunOptions callerOptions = new()
        {
            ChatClientFactory = inner => new OptionsCapturingChatClient(inner, options => invocationOptions = options),
        };

        InvalidOperationException observed = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => _ = await wrapper.RunStreamingAsync(request.Messages, session, callerOptions).ToAgentResponseAsync());

        Assert.Same(failure, observed);
        Assert.Null(Assert.IsType<ChatClientAgentSession>(session).ConversationId);
        Assert.NotNull(invocationOptions);
        Assert.True(invocationOptions.AdditionalProperties!.TryGetValue(out ChatHistoryProvider? retainedProvider));
        Assert.Same(provider, retainedProvider);
        Assert.False(provider.HasStagedTurn);
        Assert.Empty(history);
        Assert.Equal(1, client.InvocationCount);
    }

    private sealed class OptionsCapturingChatClient(
        IChatClient innerClient,
        Action<ChatOptions?> capture) : DelegatingChatClient(innerClient)
    {
        public override Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            capture(options);
            return base.GetResponseAsync(messages, options, cancellationToken);
        }

        public override IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            capture(options);
            return base.GetStreamingResponseAsync(messages, options, cancellationToken);
        }
    }

    private static Mock<TaskEntityContext> CreateContext()
    {
        Mock<TaskEntityContext> context = new();
        context.SetupGet(value => value.Id).Returns(
            new EntityInstanceId("dafx-agent", "session"));
        return context;
    }

    private sealed class IdentityAgent : AIAgent
    {
        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) => new(new IdentitySession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(JsonSerializer.SerializeToElement(new { }));

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) => new(new IdentitySession());

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new AgentResponse(new ChatMessage(ChatRole.Assistant, "response"))
                {
                    AgentId = "inner-agent-id",
                });

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield return new AgentResponseUpdate(ChatRole.Assistant, "response")
            {
                AgentId = "inner-agent-id",
            };
        }

        private sealed class IdentitySession : AgentSession;
    }

    private sealed class RecordingHistoryProvider : ChatHistoryProvider
    {
        public int LoadCount { get; private set; }

        public int StoreCount { get; private set; }

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context,
            CancellationToken cancellationToken = default)
        {
            this.LoadCount++;
            return new([new ChatMessage(ChatRole.User, "operation history")]);
        }

        protected override ValueTask StoreChatHistoryAsync(
            InvokedContext context,
            CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            return default;
        }
    }

    private sealed class RecordingChatClient : IChatClient
    {
        public string? ResponseConversationId { get; init; }

        public Exception? StreamingFailure { get; init; }

        public ChatOptions? LastOptions { get; private set; }

        public int InvocationCount { get; private set; }

        public List<ChatMessage> LastMessages { get; private set; } = [];

        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            this.InvocationCount++;
            this.LastMessages = messages.ToList();
            this.LastOptions = options;
            return Task.FromResult(
                new ChatResponse(new ChatMessage(ChatRole.Assistant, "response"))
                {
                    ConversationId = this.ResponseConversationId,
                });
        }

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.InvocationCount++;
            this.LastMessages = messages.ToList();
            this.LastOptions = options;
            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, "res")
            {
                ConversationId = this.StreamingFailure is null ? null : this.ResponseConversationId,
            };
            if (this.StreamingFailure is not null)
            {
                throw this.StreamingFailure;
            }

            yield return new ChatResponseUpdate(ChatRole.Assistant, "ponse")
            {
                ConversationId = this.ResponseConversationId,
            };
        }
    }
}
