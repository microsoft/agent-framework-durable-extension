// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests;

/// <summary>
/// Focused reproduction of the current durable-agent gaps for agents whose conversation state is
/// owned by a remote service.
/// </summary>
public sealed class ServerManagedAgentDurabilityGapTests
{
    [Fact]
    public async Task ExistingProviderSessionCannotSeedDurableSessionAsync()
    {
        const string ExistingConversationId = "conversation-from-before-durable-registration";

        // This mirrors FoundryAgent.CreateSessionAsync(string conversationId): the underlying agent can
        // represent an existing service-owned conversation before it is registered as durable.
        ServiceOwnedHistoryAgent innerAgent = new("server-managed");
        AgentSession innerSession = innerAgent.CreateSessionFromExistingConversation(ExistingConversationId);
        Assert.Equal(
            ExistingConversationId,
            Assert.IsType<ServiceConversationSession>(innerSession).ConversationId);

        StubDurableAgentClient client = new();
        DurableAIAgentProxy durableProxy = new("server-managed", client);

        // Durable registration exposes a separate proxy whose session is a randomly keyed entity session.
        AgentSession durableSession = await durableProxy.CreateSessionAsync();
        Assert.IsType<DurableAgentSession>(durableSession);

        // There is no durable overload accepting the existing conversation ID or serialized inner session,
        // and passing the original provider session is rejected before the durable client is called.
        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(
            () => durableProxy.RunAsync("continue", innerSession));

        Assert.Equal("session", exception.ParamName);
        Assert.Equal(0, client.CallCount);
        Assert.DoesNotContain(
            durableProxy.GetType().GetMethods(),
            method => method.Name == nameof(AIAgent.CreateSessionAsync) &&
                method.GetParameters().Any(parameter => parameter.ParameterType == typeof(string)));
    }

    [Fact]
    public void ArbitraryServerHostedAgentCanBeRegisteredButCannotDeclareHistoryOwnership()
    {
        ServiceOwnedHistoryAgent agent = new("custom-server-agent");
        ServiceCollection services = new();
        services.AddSingleton<IDurableAgentClient>(new StubDurableAgentClient());
        services.ConfigureDurableAgents(options => options.AddAIAgent(agent));

        using IDisposable providerLifetime = services.BuildServiceProvider();
        IServiceProvider provider = (IServiceProvider)providerLifetime;
        AIAgent proxy = provider.GetDurableAgentProxy(agent.Name!);

        Assert.NotNull(proxy);
        Assert.Null(agent.GetService<ChatClientAgent>());
    }

    [Fact]
    public async Task EntityRecreatesCustomSessionAndReplaysItsPersistedTranscriptAsync()
    {
        ServiceOwnedHistoryAgent agent = new("custom-server-agent");
        (IServiceProvider Services, IDisposable Lifetime) serviceScope = CreateEntityServices(agent);
        using IDisposable serviceLifetime = serviceScope.Lifetime;
        AgentEntity entity = new(serviceScope.Services);
        TestEntityState entityState = new();
        TestEntityContext entityContext = new(new EntityInstanceId("dafx-custom-server-agent", "session-key"));

        await entity.RunAsync(new TestEntityOperation(
            entityContext,
            entityState,
            nameof(AgentEntity.Run),
            new RunRequest("first")));
        await entity.RunAsync(new TestEntityOperation(
            entityContext,
            entityState,
            nameof(AgentEntity.Run),
            new RunRequest("second")));

        Assert.Equal(2, agent.CreatedConversationIds.Count);
        Assert.NotEqual(agent.CreatedConversationIds[0], agent.CreatedConversationIds[1]);
        Assert.Equal(0, agent.SerializeSessionCallCount);
        Assert.Equal(0, agent.DeserializeSessionCallCount);

        Assert.Collection(
            agent.ReceivedRuns,
            firstRun => Assert.Equal(["first"], firstRun.Messages.Select(message => message.Text)),
            secondRun => Assert.Equal(
                ["first", "remote-response-1", "second"],
                secondRun.Messages.Select(message => message.Text)));

        DurableAgentState durableState = Assert.IsType<DurableAgentState>(entityState.Value);
        Assert.Equal(4, durableState.Data.ConversationHistory.Count);

        // The entity persists requests and responses, but never calls the agent's existing opaque session
        // serialization contract and stores neither remote conversation ID.
        string serializedState = JsonSerializer.Serialize(
            durableState,
            DurableAgentJsonUtilities.DefaultOptions);
        Assert.DoesNotContain(agent.CreatedConversationIds[0], serializedState, StringComparison.Ordinal);
        Assert.DoesNotContain(agent.CreatedConversationIds[1], serializedState, StringComparison.Ordinal);
    }

    private static (IServiceProvider Services, IDisposable Lifetime) CreateEntityServices(AIAgent agent)
    {
        DurableAgentsOptions options = new();
        options.AddAIAgent(agent);

        ServiceCollection services = new();
        services.AddSingleton(new Mock<DurableTaskClient>("test-client").Object);
        services.AddSingleton<Extensions.Logging.ILoggerFactory>(NullLoggerFactory.Instance);
        services.AddSingleton(new Mock<IHostApplicationLifetime>().Object);
        services.AddSingleton(options);
        services.AddSingleton(
            options.GetAgentFactories());
        IServiceProvider provider = services.BuildServiceProvider();
        return (provider, (IDisposable)provider);
    }

    private sealed class ServiceOwnedHistoryAgent(string name) : AIAgent
    {
        public List<string> CreatedConversationIds { get; } = [];

        public List<ReceivedRun> ReceivedRuns { get; } = [];

        public int SerializeSessionCallCount { get; private set; }

        public int DeserializeSessionCallCount { get; private set; }

        public override string? Name => name;

        public ServiceConversationSession CreateSessionFromExistingConversation(string conversationId) =>
            new(conversationId);

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(CancellationToken cancellationToken = default)
        {
            string conversationId = $"service-conversation-{this.CreatedConversationIds.Count + 1}";
            this.CreatedConversationIds.Add(conversationId);
            return ValueTask.FromResult<AgentSession>(new ServiceConversationSession(conversationId));
        }

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default)
        {
            this.SerializeSessionCallCount++;
            ServiceConversationSession serviceSession = Assert.IsType<ServiceConversationSession>(session);
            return ValueTask.FromResult(JsonSerializer.SerializeToElement(
                new { serviceSession.ConversationId },
                jsonSerializerOptions));
        }

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default)
        {
            this.DeserializeSessionCallCount++;
            string conversationId = serializedState.GetProperty("ConversationId").GetString()!;
            return ValueTask.FromResult<AgentSession>(new ServiceConversationSession(conversationId));
        }

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("The durable entity uses the streaming path.");

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ServiceConversationSession serviceSession = Assert.IsType<ServiceConversationSession>(session);
            this.ReceivedRuns.Add(new ReceivedRun(serviceSession.ConversationId, [.. messages]));
            yield return new AgentResponseUpdate(ChatRole.Assistant, $"remote-response-{this.ReceivedRuns.Count}");
            await Task.CompletedTask;
        }
    }

    private sealed record ReceivedRun(string ConversationId, IReadOnlyList<ChatMessage> Messages);

    public sealed class ServiceConversationSession(string conversationId) : AgentSession
    {
        public string ConversationId { get; } = conversationId;
    }

    private sealed class StubDurableAgentClient : IDurableAgentClient
    {
        public int CallCount { get; private set; }

        public Task<AgentRunHandle> RunAgentAsync(
            AgentSessionId sessionId,
            RunRequest request,
            CancellationToken cancellationToken)
        {
            this.CallCount++;
            throw new InvalidOperationException("The reproduction should fail before durable dispatch.");
        }
    }

    private sealed class TestEntityOperation(
        TaskEntityContext context,
        TaskEntityState state,
        string name,
        object? input) : TaskEntityOperation
    {
        public override TaskEntityContext Context => context;

        public override TaskEntityState State => state;

        public override string Name => name;

        public override bool HasInput => input is not null;

        public override T? GetInput<T>() where T : default => (T?)input;

        public override object? GetInput(Type inputType) => input;
    }

    private sealed class TestEntityState : TaskEntityState
    {
        public object? Value { get; private set; }

        public override bool HasState => this.Value is not null;

        public override T? GetState<T>(T? defaultValue = default) where T : default =>
            this.Value is T state ? state : defaultValue;

        public override object? GetState(Type type) => this.Value;

        public override void SetState(object? state) => this.Value = state;
    }

    private sealed class TestEntityContext(EntityInstanceId id) : TaskEntityContext
    {
        public override EntityInstanceId Id => id;

        public override string ScheduleNewOrchestration(
            TaskName name,
            object? input = null,
            StartOrchestrationOptions? options = null) =>
            throw new NotSupportedException();

        public override void SignalEntity(
            EntityInstanceId id,
            string operationName,
            object? input = null,
            SignalEntityOptions? options = null)
        {
        }
    }
}
