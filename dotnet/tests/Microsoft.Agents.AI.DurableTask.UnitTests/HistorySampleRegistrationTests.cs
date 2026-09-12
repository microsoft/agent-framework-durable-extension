// Copyright (c) Microsoft. All rights reserved.

using System.ClientModel;
using System.ClientModel.Primitives;
using System.Text.Json;
using Azure.AI.Projects;
using Azure.AI.Projects.Agents;
using CustomHistoryProvider;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Agents.AI.Foundry;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Client.Entities;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class HistorySampleRegistrationTests
{
    private const string FoundryAgentName = "foundry-managed-agent";
    private const string FoundryServiceHistoryProviderKey = "foundry-managed-service.v1";
    private const string HistoryAgentName = "HistoryAgent";

    [Fact]
    public async Task ExternalProviderRegistrationUsesMailboxAcrossProxyRestartAsync()
    {
        string directory = CreateStoreDirectory();
        InProcessDurableStateStore stateStore = new();

        try
        {
            RecordingChatClient firstClient = new();
            using JsonFileChatHistoryProvider firstProvider = new(directory);
            ChatClientAgent firstAgent = CreateHistoryAgent(firstClient, firstProvider);
            JsonElement serializedDurableSession;
            string historyId;
            AgentSessionId sessionId;

            using (IHost firstHost = CreateHost(
                firstAgent,
                stateStore,
                options =>
                {
                    options.SetHistoryProviderKey(
                        HistoryAgentName,
                        JsonFileChatHistoryProvider.ProviderKey);
                    options.EnableMailboxWrites = true;
                    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;
                },
                services => services.AddSingleton(firstProvider)))
            {
                await firstHost.StartAsync();
                AIAgent proxy = firstHost.Services.GetRequiredKeyedService<AIAgent>(HistoryAgentName);
                AgentSession session = await proxy.CreateSessionAsync();

                AgentResponse response = await proxy.RunAsync("first request", session);

                Assert.Equal("response-1", response.Text);
                serializedDurableSession = await proxy.SerializeSessionAsync(session);
                sessionId = session.GetService<AgentSessionId>();
                DurableAgentState firstState = stateStore.ReadRequired(sessionId);
                historyId = Assert.Single(firstProvider.GetObservedHistoryIds());
                AssertMailboxOnlyState(
                    firstState,
                    DurableAgentStateHistoryBinding.HistoryProviderOwner,
                    JsonFileChatHistoryProvider.ProviderKey,
                    completionCount: 1);
                Assert.True(firstHost.Services.GetRequiredService<DurableAgentsOptions>().EnableMailboxWrites);
                Assert.Equal(
                    DurableAgentHistoryRetentionMode.KeepAll,
                    firstHost.Services.GetRequiredService<DurableAgentsOptions>().HistoryRetentionMode);
                await firstHost.StopAsync();
            }

            RecordingChatClient secondClient = new();
            using JsonFileChatHistoryProvider secondProvider = new(directory);
            ChatClientAgent secondAgent = CreateHistoryAgent(secondClient, secondProvider);
            using IHost secondHost = CreateHost(
                secondAgent,
                stateStore,
                options =>
                {
                    options.SetHistoryProviderKey(
                        HistoryAgentName,
                        JsonFileChatHistoryProvider.ProviderKey);
                    options.EnableMailboxWrites = true;
                    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;
                },
                services => services.AddSingleton(secondProvider));
            await secondHost.StartAsync();
            AIAgent restoredProxy =
                secondHost.Services.GetRequiredKeyedService<AIAgent>(HistoryAgentName);
            AgentSession restoredSession =
                await restoredProxy.DeserializeSessionAsync(serializedDurableSession);

            AgentResponse restoredResponse =
                await restoredProxy.RunAsync("second request", restoredSession);

            Assert.Equal("response-1", restoredResponse.Text);
            Assert.Equal(historyId, Assert.Single(secondProvider.GetObservedHistoryIds()));
            Assert.Equal(
                ["first request", "response-1", "second request"],
                secondClient.LastMessages.Select(message => message.Text));
            AssertMailboxOnlyState(
                stateStore.ReadRequired(sessionId),
                DurableAgentStateHistoryBinding.HistoryProviderOwner,
                JsonFileChatHistoryProvider.ProviderKey,
                completionCount: 2);
            await secondHost.StopAsync();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task FoundryAgentRegistrationRestoresServiceContinuationAcrossProxyRestartAsync()
    {
        InProcessDurableStateStore stateStore = new();
        RecordingChatClient firstClient = new()
        {
            ResponseConversationId = "service-conversation-id",
        };
        FoundryAgent firstAgent = CreateFoundryAgent(firstClient);
        AgentSession initialInnerSession = await firstAgent.CreateSessionAsync();
        Assert.Equal(
            DurableAgentHistoryOwnership.Entity,
            DurableAgentHistoryOwnershipResolver.Resolve(firstAgent, initialInnerSession).Ownership);

        JsonElement serializedDurableSession;
        AgentSessionId sessionId;
        using (IHost firstHost = CreateHost(
            firstAgent,
            stateStore,
            ConfigureFoundryRegistration))
        {
            await firstHost.StartAsync();
            AIAgent proxy = firstHost.Services.GetRequiredKeyedService<AIAgent>(FoundryAgentName);
            AgentSession session = await proxy.CreateSessionAsync();

            AgentResponse response = await proxy.RunAsync("first request", session);

            Assert.Equal("response-1", response.Text);
            Assert.Equal(1, firstClient.InvocationCount);
            Assert.Null(firstClient.LastConversationId);
            serializedDurableSession = await proxy.SerializeSessionAsync(session);
            sessionId = session.GetService<AgentSessionId>();
            DurableAgentState firstState = stateStore.ReadRequired(sessionId);
            AssertMailboxOnlyState(
                firstState,
                DurableAgentStateHistoryBinding.ModelServiceOwner,
                FoundryServiceHistoryProviderKey,
                completionCount: 1);
            Assert.Equal(
                "service-conversation-id",
                firstState.Data.Session?.GetProperty("conversationId").GetString());
            await firstHost.StopAsync();
        }

        RecordingChatClient secondClient = new();
        FoundryAgent secondAgent = CreateFoundryAgent(secondClient);
        using IHost secondHost = CreateHost(
            secondAgent,
            stateStore,
            ConfigureFoundryRegistration);
        await secondHost.StartAsync();
        AIAgent restoredProxy =
            secondHost.Services.GetRequiredKeyedService<AIAgent>(FoundryAgentName);
        AgentSession restoredSession =
            await restoredProxy.DeserializeSessionAsync(serializedDurableSession);

        AgentResponse restoredResponse =
            await restoredProxy.RunAsync("second request", restoredSession);

        Assert.Equal("response-1", restoredResponse.Text);
        Assert.Equal(1, secondClient.InvocationCount);
        Assert.Equal("service-conversation-id", secondClient.LastConversationId);
        Assert.Equal(["second request"], secondClient.LastMessages.Select(message => message.Text));
        DurableAgentState restoredState = stateStore.ReadRequired(sessionId);
        AssertMailboxOnlyState(
            restoredState,
            DurableAgentStateHistoryBinding.ModelServiceOwner,
            FoundryServiceHistoryProviderKey,
            completionCount: 2);
        Assert.Equal(
            "service-conversation-id",
            restoredState.Data.Session?.GetProperty("conversationId").GetString());
        await secondHost.StopAsync();
    }

    private static FoundryAgent CreateFoundryAgent(RecordingChatClient client)
    {
        AIProjectClient projectClient = new(
            new Uri("https://example.services.ai.azure.com/api/projects/test"),
            new FakeAuthenticationTokenProvider());
        ProjectsAgentVersion agentVersion =
            ProjectsAgentsModelFactory.ProjectsAgentVersion(
                id: $"{FoundryAgentName}:1",
                name: FoundryAgentName,
                version: "1");
        return projectClient.AsAIAgent(
            agentVersion,
            clientFactory: _ => client);
    }

    [Fact]
    public async Task ExternalOwnershipWithoutMailboxActivationFailsClearlyThroughProxyAsync()
    {
        string directory = CreateStoreDirectory();
        InProcessDurableStateStore stateStore = new();

        try
        {
            RecordingChatClient client = new();
            using JsonFileChatHistoryProvider provider = new(directory);
            ChatClientAgent agent = CreateHistoryAgent(client, provider);
            using IHost host = CreateHost(
                agent,
                stateStore,
                options =>
                {
                    options.SetHistoryProviderKey(
                        HistoryAgentName,
                        JsonFileChatHistoryProvider.ProviderKey);
                    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;
                },
                services => services.AddSingleton(provider));
            await host.StartAsync();
            AIAgent proxy = host.Services.GetRequiredKeyedService<AIAgent>(HistoryAgentName);
            AgentSession session = await proxy.CreateSessionAsync();

            InvalidOperationException exception =
                await Assert.ThrowsAsync<InvalidOperationException>(
                    () => proxy.RunAsync("request", session));

            Assert.Contains("schema 2 mailbox writes", exception.Message, StringComparison.Ordinal);
            Assert.Contains(
                "Enable mailbox writes",
                exception.Message,
                StringComparison.Ordinal);
            Assert.Equal(0, client.InvocationCount);
            Assert.Empty(provider.GetObservedHistoryIds());
            Assert.Empty(Directory.EnumerateFiles(directory));
            Assert.False(stateStore.TryRead(session.GetService<AgentSessionId>(), out _));
            await host.StopAsync();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static IHost CreateHost(
        AIAgent agent,
        InProcessDurableStateStore stateStore,
        Action<DurableAgentsOptions> configure,
        Action<IServiceCollection>? configureServices = null)
    {
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                configureServices?.Invoke(services);
                services.ConfigureDurableAgents(options =>
                {
                    options.AddAIAgent(agent, timeToLive: TimeSpan.FromHours(1));
                    configure(options);
                });
                services.AddSingleton(
                    _ => new Mock<DurableTaskClient>("test").Object);
                services.AddSingleton<IDurableAgentClient>(
                    serviceProvider => new InProcessDurableAgentClient(
                        serviceProvider,
                        stateStore));
            })
            .Build();
    }

    private static void ConfigureFoundryRegistration(DurableAgentsOptions options)
    {
        options.SetHistoryProviderKey(
            FoundryAgentName,
            FoundryServiceHistoryProviderKey);
        options.EnableMailboxWrites = true;
        options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;
    }

    private static ChatClientAgent CreateHistoryAgent(
        RecordingChatClient client,
        JsonFileChatHistoryProvider provider) =>
        new(
            client,
            new ChatClientAgentOptions
            {
                Name = HistoryAgentName,
                ChatHistoryProvider = provider,
            });

    private static void AssertMailboxOnlyState(
        DurableAgentState state,
        string ownerKind,
        string providerKey,
        int completionCount)
    {
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, state.SchemaVersion);
        Assert.Empty(state.Data.ConversationHistory);
        DurableAgentStateHistoryBinding? historyBinding =
            DurableAgentHistoryBinding.Parse(state.Data.HistoryBinding);
        Assert.Equal(ownerKind, historyBinding?.OwnerKind);
        Assert.Equal(providerKey, historyBinding?.ProviderKey);
        Assert.Equal(completionCount, state.Data.TerminalResults?.Count);
        Assert.Equal(completionCount, state.Data.CompletionReceipts?.Count);
    }

    private static string CreateStoreDirectory()
    {
        string directory = Path.Combine(
            AppContext.BaseDirectory,
            ".registration-history",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return directory;
    }

    private sealed class InProcessDurableAgentClient(
        IServiceProvider services,
        InProcessDurableStateStore stateStore) : IDurableAgentClient
    {
        public async Task<AgentRunHandle> RunAgentAsync(
            AgentSessionId sessionId,
            RunRequest request,
            CancellationToken cancellationToken)
        {
            DurableAgentState? hydratedState =
                stateStore.TryRead(sessionId, out DurableAgentState? existingState)
                    ? existingState
                    : null;

            Mock<TaskEntityContext> context = new();
            context.SetupGet(value => value.Id).Returns(sessionId);

            Mock<TaskEntityState> entityState = new();
            entityState.Setup(value => value.GetState(typeof(DurableAgentState)))
                .Returns(hydratedState);
            entityState.Setup(value => value.SetState(It.IsAny<object?>()))
                .Callback<object?>(value =>
                    stateStore.Write(sessionId, Assert.IsType<DurableAgentState>(value)));

            Mock<TaskEntityOperation> operation = new();
            operation.SetupGet(value => value.Name).Returns(nameof(AgentEntity.Run));
            operation.SetupGet(value => value.Context).Returns(context.Object);
            operation.SetupGet(value => value.State).Returns(entityState.Object);
            operation.SetupGet(value => value.HasInput).Returns(true);
            operation.Setup(value => value.GetInput(typeof(RunRequest))).Returns(request);

            AgentEntity entity = new(services, cancellationToken);
            _ = await ((ITaskEntity)entity).RunAsync(operation.Object);

            DurableAgentState committedState = stateStore.ReadRequired(sessionId);
            Mock<DurableEntityClient> entityClient = new("test");
            entityClient.Setup(client => client.GetEntityAsync<DurableAgentState>(
                    sessionId,
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new EntityMetadata<DurableAgentState>(
                    sessionId,
                    committedState));
            Mock<DurableTaskClient> durableClient = new("test");
            durableClient.SetupGet(value => value.Entities).Returns(entityClient.Object);

            return new AgentRunHandle(
                durableClient.Object,
                NullLogger.Instance,
                sessionId,
                request.CorrelationId);
        }
    }

    private sealed class InProcessDurableStateStore
    {
        private readonly Dictionary<string, string> _states = new(StringComparer.Ordinal);

        public bool TryRead(
            AgentSessionId sessionId,
            out DurableAgentState? state)
        {
            if (!this._states.TryGetValue(sessionId.ToString(), out string? json))
            {
                state = null;
                return false;
            }

            state = JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.DurableAgentState);
            return state is not null;
        }

        public DurableAgentState ReadRequired(AgentSessionId sessionId)
        {
            Assert.True(this.TryRead(sessionId, out DurableAgentState? state));
            return Assert.IsType<DurableAgentState>(state);
        }

        public void Write(AgentSessionId sessionId, DurableAgentState state)
        {
            this._states[sessionId.ToString()] = JsonSerializer.Serialize(
                state,
                DurableAgentStateJsonContext.Default.DurableAgentState);
        }
    }

    private sealed class RecordingChatClient : IChatClient
    {
        public string? ResponseConversationId { get; init; }

        public int InvocationCount { get; private set; }

        public string? LastConversationId { get; private set; }

        public List<ChatMessage> LastMessages { get; private set; } = [];

        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) =>
            serviceType.IsInstanceOfType(this) ? this : null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken = default)
        {
            this.InvocationCount++;
            this.LastMessages = messages.ToList();
            this.LastConversationId = options?.ConversationId;

            await Task.Yield();
            yield return new ChatResponseUpdate(
                ChatRole.Assistant,
                $"response-{this.InvocationCount}")
            {
                ConversationId = this.ResponseConversationId ?? options?.ConversationId,
            };
        }
    }

    private sealed class FakeAuthenticationTokenProvider : AuthenticationTokenProvider
    {
        public override GetTokenOptions? CreateTokenOptions(
            IReadOnlyDictionary<string, object> properties) =>
            new(new Dictionary<string, object>());

        public override AuthenticationToken GetToken(
            GetTokenOptions options,
            CancellationToken cancellationToken) =>
            new("test-token", "Bearer", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<AuthenticationToken> GetTokenAsync(
            GetTokenOptions options,
            CancellationToken cancellationToken) =>
            new(this.GetToken(options, cancellationToken));
    }
}
