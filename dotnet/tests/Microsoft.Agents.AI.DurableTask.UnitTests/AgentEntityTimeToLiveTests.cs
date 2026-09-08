// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityTimeToLiveTests
{
    private static readonly DateTimeOffset s_startTime =
        new(2026, 9, 7, 8, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task FirstInteractionSchedulesExpirationCheckAsync()
    {
        EntityHarness harness = CreateHarness(TimeSpan.FromMinutes(10));

        await harness.RunAsync("first");

        Assert.Equal(s_startTime.AddMinutes(10).UtcDateTime, harness.State!.Data.ExpirationTimeUtc);
        ScheduledSignal signal = Assert.Single(harness.Signals);
        Assert.Equal(s_startTime.AddMinutes(10), signal.SignalTime);
        Assert.Equal(s_startTime.AddMinutes(10).UtcDateTime, signal.Input.ExpectedExpirationTimeUtc);
        Assert.Single(harness.State.Data.CompletionReceipts!);
        Assert.Null(Assert.Single(harness.State.Data.TerminalResults!).Value.ResultExpiresAt);
    }

    [Fact]
    public async Task SchedulingFailureDoesNotMutateHydratedStateOrCommitReceiptAsync()
    {
        DurableAgentState state = new();
        EntityHarness harness = CreateHarness(TimeSpan.FromMinutes(10), state,
            onSignal: () => throw new InvalidOperationException("signal scheduling failed"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.RunAsync("request"));

        Assert.Same(state, harness.State);
        Assert.Empty(state.Data.ConversationHistory);
        Assert.Null(state.Data.ExpirationTimeUtc);
        Assert.Null(state.Data.CompletionReceipts);
    }

    [Fact]
    public async Task RevisedReceiptSurvivesLegacyDeadlineWithoutAgreedDeletionPolicyAsync()
    {
        DurableAgentState state = DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(
            new DurableAgentState(), hasAuthoritativeLegacyHistory: true);
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(state, "old", new AgentResponse(), s_startTime.AddMinutes(-5));
        state.Data.ExpirationTimeUtc = s_startTime.AddMinutes(-1).UtcDateTime;
        EntityHarness harness = CreateHarness(TimeSpan.FromMinutes(10), state);
        harness.Options.EnableMailboxEntityDeletion = false;

        await harness.CheckExpirationAsync(null);

        Assert.Same(state, harness.State);
        Assert.Single(state.Data.CompletionReceipts!);
        Assert.Equal(s_startTime.AddMinutes(-1).UtcDateTime, state.Data.ExpirationTimeUtc);
        Assert.Empty(harness.Signals);
    }

    [Fact]
    public async Task LaterInteractionExtendsExpirationAndEarlierCheckMovesChainAsync()
    {
        EntityHarness harness = CreateHarness(TimeSpan.FromMinutes(10));
        await harness.RunAsync("first");
        ScheduledSignal firstSignal = Assert.Single(harness.Signals);

        harness.Clock.Advance(TimeSpan.FromMinutes(2));
        await harness.RunAsync("second");

        Assert.Equal(s_startTime.AddMinutes(12).UtcDateTime, harness.State!.Data.ExpirationTimeUtc);
        Assert.Single(harness.Signals);

        harness.Clock.SetUtcNow(firstSignal.SignalTime);
        await harness.CheckExpirationAsync(firstSignal.Input);

        Assert.NotNull(harness.State);
        Assert.Equal(2, harness.Signals.Count);
        Assert.Equal(s_startTime.AddMinutes(12), harness.Signals[^1].SignalTime);
    }

    [Fact]
    public async Task ShorterTimeToLiveSchedulesEarlierCheckAndStaleSignalIsHarmlessAsync()
    {
        EntityHarness harness = CreateHarness(TimeSpan.FromMinutes(10));
        await harness.RunAsync("first");
        ScheduledSignal originalSignal = Assert.Single(harness.Signals);

        harness.Clock.Advance(TimeSpan.FromMinutes(1));
        harness.Options.DefaultTimeToLive = TimeSpan.FromMinutes(1);
        await harness.RunAsync("second");

        Assert.Equal(2, harness.Signals.Count);
        ScheduledSignal shorterSignal = harness.Signals[^1];
        Assert.Equal(s_startTime.AddMinutes(2), shorterSignal.SignalTime);
        Assert.Equal(2, harness.State!.Data.CompletionReceipts!.Count);

        harness.Clock.SetUtcNow(shorterSignal.SignalTime);
        await harness.CheckExpirationAsync(shorterSignal.Input);
        Assert.Null(harness.State);

        harness.Clock.SetUtcNow(originalSignal.SignalTime);
        await harness.CheckExpirationAsync(originalSignal.Input);
        Assert.Null(harness.State);
    }

    [Fact]
    public async Task DisablingTimeToLiveClearsExpirationAndMakesOldSignalHarmlessAsync()
    {
        EntityHarness harness = CreateHarness(TimeSpan.FromMinutes(2));
        await harness.RunAsync("first");
        ScheduledSignal signal = Assert.Single(harness.Signals);

        harness.Clock.Advance(TimeSpan.FromMinutes(1));
        harness.Options.DefaultTimeToLive = null;
        await harness.RunAsync("second");

        Assert.Null(harness.State!.Data.ExpirationTimeUtc);

        harness.Clock.SetUtcNow(signal.SignalTime);
        await harness.CheckExpirationAsync(signal.Input);

        Assert.NotNull(harness.State);
        Assert.Null(harness.State.Data.ExpirationTimeUtc);
        Assert.Single(harness.Signals);
    }

    [Fact]
    public async Task MissingAgentConfigurationClearsExpirationWithoutDeletingStateAsync()
    {
        DurableAgentState state = new();
        state.Data.ExpirationTimeUtc = s_startTime.AddMinutes(5).UtcDateTime;
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = "meaningful",
                CreatedAt = s_startTime,
            });
        EntityHarness harness = CreateHarness(
            TimeSpan.FromMinutes(5),
            state,
            registerAgent: false);

        await harness.CheckExpirationAsync(
            new AgentEntityDeletionCheck(state.Data.ExpirationTimeUtc.Value));

        Assert.NotNull(harness.State);
        Assert.Null(harness.State.Data.ExpirationTimeUtc);
        Assert.Single(harness.State.Data.ConversationHistory);
        Assert.Empty(harness.Signals);
    }

    [Fact]
    public async Task AutoRejectsUnauthorizedLegacyTtlMutationAsync()
    {
        DurableAgentState state = new();
        state.Data.ExpirationTimeUtc = s_startTime.AddMinutes(5).UtcDateTime;
        EntityHarness harness = CreateHarness(
            TimeSpan.FromMinutes(5),
            state,
            registerAgent: false);
        harness.Options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
        harness.Options.AuthorizeLegacyMigration = null;

        _ = await Assert.ThrowsAsync<DurableAgentStateCorruptionException>(
            () => harness.CheckExpirationAsync(
                new AgentEntityDeletionCheck(state.Data.ExpirationTimeUtc.Value)));

        Assert.Same(state, harness.State);
        Assert.Equal(s_startTime.AddMinutes(5).UtcDateTime, state.Data.ExpirationTimeUtc);
    }

    [Fact]
    public async Task AutoAppliesProtectedFloorWhenClearingTtlAsync()
    {
        DurableAgentState state = DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(
            new DurableAgentState(),
            hasAuthoritativeLegacyHistory: true);
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(
            state,
            "protected",
            new AgentResponse(new ChatMessage(ChatRole.Assistant, new string('x', 2_000))),
            s_startTime.AddMinutes(-5));
        state.Data.ExpirationTimeUtc = s_startTime.AddMinutes(5).UtcDateTime;
        EntityHarness harness = CreateHarness(
            TimeSpan.FromMinutes(5),
            state,
            registerAgent: false);
        harness.Options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
        harness.Options.MaxStateBytes = 500;

        _ = await Assert.ThrowsAsync<DurableAgentStateSizeLimitExceededException>(
            () => harness.CheckExpirationAsync(
                new AgentEntityDeletionCheck(state.Data.ExpirationTimeUtc.Value)));

        Assert.Same(state, harness.State);
        Assert.Equal(s_startTime.AddMinutes(5).UtcDateTime, state.Data.ExpirationTimeUtc);
        Assert.Single(state.Data.TerminalResults!);
    }

    [Fact]
    public async Task StaleLaterCheckDoesNotRescheduleEarlierCurrentExpirationAsync()
    {
        DurableAgentState state = new();
        state.Data.ExpirationTimeUtc = s_startTime.AddMinutes(10).UtcDateTime;
        EntityHarness harness = CreateHarness(TimeSpan.FromMinutes(10), state);

        await harness.CheckExpirationAsync(
            new AgentEntityDeletionCheck(s_startTime.AddMinutes(20).UtcDateTime));

        Assert.NotNull(harness.State);
        Assert.Equal(s_startTime.AddMinutes(10).UtcDateTime, harness.State.Data.ExpirationTimeUtc);
        Assert.Empty(harness.Signals);
    }

    [Fact]
    public async Task DelayedSignalAgainstDeletedEntityRemovesEmptyPlaceholderAsync()
    {
        EntityHarness harness = CreateHarness(TimeSpan.FromMinutes(10), state: null);
        harness.DeleteState();

        await harness.CheckExpirationAsync(
            new AgentEntityDeletionCheck(s_startTime.UtcDateTime));

        Assert.Null(harness.State);
        Assert.Empty(harness.Signals);
    }

    private static EntityHarness CreateHarness(
        TimeSpan? timeToLive,
        DurableAgentState? state = null,
        bool registerAgent = true,
        Action? onSignal = null)
    {
        const string AgentName = "agent";
        AgentSessionId sessionId = new(AgentName, "session");
        DurableAgentsOptions options = new()
        {
            DefaultTimeToLive = timeToLive,
            MinimumTimeToLiveSignalDelay = TimeSpan.Zero,
            EnableMailboxWrites = true,
            EnableMailboxEntityDeletion = true,
            AuthorizeLegacyMigration = _ => true,
        };
        AIAgent agent = new StubAgent(AgentName);
        if (registerAgent)
        {
            options.AddAIAgent(agent);
        }

        ManualTimeProvider clock = new(s_startTime);
        Dictionary<Type, object> services = new()
        {
            [typeof(DurableTaskClient)] = new Mock<DurableTaskClient>("test").Object,
            [typeof(ILoggerFactory)] = Extensions.Logging.Abstractions.NullLoggerFactory.Instance,
            [typeof(DurableAgentsOptions)] = options,
            [typeof(IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>>)] =
                options.GetAgentFactories(),
            [typeof(IHostApplicationLifetime)] = Mock.Of<IHostApplicationLifetime>(
                lifetime => lifetime.ApplicationStopping == CancellationToken.None),
            [typeof(TimeProvider)] = clock,
        };

        List<ScheduledSignal> signals = [];
        Mock<TaskEntityContext> context = new();
        context.SetupGet(value => value.Id).Returns(sessionId);
        context.Setup(value => value.SignalEntity(
                sessionId,
                nameof(AgentEntity.CheckAndDeleteIfExpired),
                It.IsAny<object?>(),
                It.IsAny<SignalEntityOptions?>()))
            .Callback<EntityInstanceId, string, object?, SignalEntityOptions?>(
                (_, _, input, signalOptions) =>
                {
                    onSignal?.Invoke();
                    signals.Add(new(
                        Assert.IsType<AgentEntityDeletionCheck>(input),
                        Assert.IsType<DateTimeOffset>(signalOptions?.SignalTime)));
                });

        DurableAgentState? currentState = state ?? new DurableAgentState();
        Mock<TaskEntityState> entityState = new();
        entityState.SetupGet(value => value.HasState).Returns(() => currentState is not null);
        entityState.Setup(value => value.GetState(typeof(DurableAgentState)))
            .Returns(() => currentState);
        entityState.Setup(value => value.SetState(It.IsAny<object?>()))
            .Callback<object?>(value => currentState = value as DurableAgentState);

        return new EntityHarness(
            new AgentEntity(new DictionaryServiceProvider(services), CancellationToken.None),
            context,
            entityState,
            options,
            clock,
            signals,
            () => currentState,
            () => currentState = null);
    }

    private sealed class EntityHarness(
        AgentEntity entity,
        Mock<TaskEntityContext> context,
        Mock<TaskEntityState> entityState,
        DurableAgentsOptions options,
        ManualTimeProvider clock,
        List<ScheduledSignal> signals,
        Func<DurableAgentState?> currentState,
        Action deleteState)
    {
        public DurableAgentsOptions Options => options;

        public ManualTimeProvider Clock => clock;

        public List<ScheduledSignal> Signals => signals;

        public DurableAgentState? State => currentState();

        public void DeleteState() => deleteState();

        public async Task RunAsync(string message)
        {
            RunRequest request = new(message);
            Mock<TaskEntityOperation> operation = this.CreateOperation(nameof(AgentEntity.Run), hasInput: true);
            operation.Setup(value => value.GetInput(typeof(RunRequest))).Returns(request);
            _ = await ((ITaskEntity)entity).RunAsync(operation.Object);
        }

        public async Task CheckExpirationAsync(AgentEntityDeletionCheck? check)
        {
            Mock<TaskEntityOperation> operation = this.CreateOperation(
                nameof(AgentEntity.CheckAndDeleteIfExpired),
                hasInput: check is not null);
            if (check is not null)
            {
                operation.Setup(value => value.GetInput(typeof(AgentEntityDeletionCheck))).Returns(check);
            }

            _ = await ((ITaskEntity)entity).RunAsync(operation.Object);
        }

        private Mock<TaskEntityOperation> CreateOperation(string name, bool hasInput)
        {
            Mock<TaskEntityOperation> operation = new();
            operation.SetupGet(value => value.Name).Returns(name);
            operation.SetupGet(value => value.Context).Returns(context.Object);
            operation.SetupGet(value => value.State).Returns(entityState.Object);
            operation.SetupGet(value => value.HasInput).Returns(hasInput);
            return operation;
        }
    }

    private sealed class StubAgent(string name) : AIAgent
    {
        public override string? Name => name;

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) => new(new StubSession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(JsonSerializer.SerializeToElement(new { }));

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(new StubSession());

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield return new AgentResponseUpdate(ChatRole.Assistant, "response");
        }

        private sealed class StubSession : AgentSession;
    }

    private sealed class ManualTimeProvider(DateTimeOffset initialTime) : TimeProvider
    {
        private DateTimeOffset _utcNow = initialTime;

        public override DateTimeOffset GetUtcNow() => this._utcNow;

        public void Advance(TimeSpan amount) => this._utcNow += amount;

        public void SetUtcNow(DateTimeOffset value) => this._utcNow = value;
    }

    private sealed class DictionaryServiceProvider(IReadOnlyDictionary<Type, object> services) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            services.TryGetValue(serviceType, out object? service) ? service : null;
    }

    private sealed record ScheduledSignal(
        AgentEntityDeletionCheck Input,
        DateTimeOffset SignalTime);
}
