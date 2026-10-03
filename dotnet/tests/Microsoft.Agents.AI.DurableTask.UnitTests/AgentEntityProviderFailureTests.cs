// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using DurableTask.Core.Entities;
using DurableTask.Core.Entities.OperationFormat;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.DurableTask.Worker;
using Microsoft.DurableTask.Worker.Shims;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityProviderFailureTests
{
    private const string PrivateMarker = "untrusted-provider-diagnostic";
    private static readonly DateTimeOffset s_now = new(2026, 10, 3, 9, 0, 0, TimeSpan.Zero);
    private static readonly EntityId s_entityId = new(AgentSessionId.ToEntityName("agent"), "session");
    private static readonly DurableDataConverter s_converter = new();

    [Theory]
    [InlineData("load")]
    [InlineData("lazyLoad")]
    [InlineData("storeBefore")]
    [InlineData("storeAfter")]
    public async Task CallbackFailureCommitsThroughSdkBatchAndColdDuplicatesNeverExecuteAsync(string stage)
    {
        ProbeProvider provider = new();
        ProbeClient client = new();
        ChatClientAgent agent = CreateAgent(client, provider);
        EntityBatchResult seed = await DispatchAsync(agent, null, "seed");
        DurableAgentState before = ReadState(seed);
        before.Data.ConversationHistory.Add(DurableAgentStateRequest.FromRunRequestV2(
            new RunRequest("retained transcript") { CorrelationId = "prior-transcript" }));
        before.Data.IngestedPositions = new Dictionary<string, JsonElement>
        {
            ["producer"] = JsonSerializer.SerializeToElement(7),
        };
        string original = s_converter.Serialize(before);
        provider.Stage = stage;
        provider.Failure = new InvalidOperationException(PrivateMarker);
        RecordingLogger logger = new();

        EntityBatchResult failed = await DispatchAsync(agent, original, "failed", logger: logger);

        Assert.Null(Result(failed).FailureDetails);
        Assert.Null(failed.FailureDetails);
        AgentResponse transport = Assert.IsType<AgentResponse>(
            s_converter.Deserialize(Result(failed).Result, typeof(AgentResponse)));
        Assert.Equal(ObservedChatHistoryProvider.FailureCode,
            DurableAgentJsonUtilities.GetCommittedFailure(transport)?.Code);
        DurableAgentState committed = ReadState(failed);
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, committed.SchemaVersion);
        Assert.Equal(before.Data.Session!.Value.GetRawText(), committed.Data.Session!.Value.GetRawText());
        Assert.Equal(before.Data.HistoryBinding.GetRawText(), committed.Data.HistoryBinding.GetRawText());
        Assert.True(committed.Data.TryGetIngestedPosition("producer", out long position));
        Assert.Equal(7, position);
        Assert.Equal("prior-transcript", Assert.Single(committed.Data.ConversationHistory).CorrelationId);
        Assert.DoesNotContain(committed.Data.ConversationHistory, entry => entry.CorrelationId == "failed");
        DurableAgentStateTerminalResult result = committed.Data.TerminalResults!["failed"];
        DurableAgentStateCompletionReceipt receipt = committed.Data.CompletionReceipts!["failed"];
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, result.Outcome);
        Assert.Equal(result.Outcome, receipt.Outcome);
        Assert.Equal(result.CompletedAt, receipt.CompletedAt);
        Assert.Equal(result.ResultExpiresAt, receipt.ResultExpiresAt);
        Assert.Equal(DurableAgentStateCompletionReceipt.AvailableResult, receipt.ResultState);
        Assert.Equal(ObservedChatHistoryProvider.FailureCode, result.Error!.Code);
        Assert.DoesNotContain(PrivateMarker, failed.EntityState, StringComparison.Ordinal);
        Assert.DoesNotContain(PrivateMarker, Result(failed).Result, StringComparison.Ordinal);
        Assert.DoesNotContain(logger.Messages, message => message.Contains(PrivateMarker, StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains(ObservedChatHistoryProvider.FailureMessage, StringComparison.Ordinal));
        Assert.Equal(2, provider.LoadCount);
        Assert.Equal(stage.StartsWith("store", StringComparison.Ordinal) ? 2 : 1, provider.StoreCount);
        Assert.Equal(stage.StartsWith("store", StringComparison.Ordinal) ? 2 : 1, client.Count);

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        DurableAgentTerminalException polled = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => AgentRunHandleTests.CreateHandle(committed, correlationId: "failed", timeProvider: new Clock())
                .ReadAgentResponseAsync(timeout.Token));
        Assert.Equal(ObservedChatHistoryProvider.FailureCode, polled.Code);

        ProbeProvider coldProvider = new();
        ProbeClient coldClient = new();
        ChatClientAgent coldAgent = CreateAgent(coldClient, coldProvider);
        committed.Data.ConversationHistory.Clear();
        string pruned = s_converter.Serialize(committed);
        EntityBatchResult duplicate = await DispatchAsync(coldAgent, pruned, "failed", emptyInput: true);
        Assert.Equal(typeof(DurableAgentTerminalException).FullName, Failure(duplicate).ErrorType);
        Assert.Equal(pruned, duplicate.EntityState);
        Assert.Empty(Actions(duplicate));
        Assert.Equal(0, coldProvider.LoadCount);
        Assert.Equal(0, coldProvider.StoreCount);
        Assert.Equal(0, coldClient.Count);

        EntityBatchResult next = await DispatchAsync(coldAgent, duplicate.EntityState, "next");
        Assert.Null(Result(next).FailureDetails);
        Assert.Equal(1, coldProvider.RestoredVersion);
        Assert.Equal(1, coldClient.Count);
    }

    [Theory]
    [InlineData("load")]
    [InlineData("storeAfter")]
    public async Task FirstFailureDoesNotInventSessionBindingOrInputAcceptanceAsync(string stage)
    {
        ProbeProvider provider = new() { Stage = stage, Failure = new IOException(PrivateMarker) };
        EntityBatchResult failed = await DispatchAsync(CreateAgent(new ProbeClient(), provider), null, "failed");

        Assert.Null(Result(failed).FailureDetails);
        DurableAgentState state = ReadState(failed);
        Assert.Null(state.Data.Session);
        Assert.Equal(JsonValueKind.Undefined, state.Data.HistoryBinding.ValueKind);
        Assert.Empty(state.Data.ConversationHistory);
        Assert.Null(state.Data.IngestedPositions);
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, Assert.Single(state.Data.CompletionReceipts!).Value.Outcome);
    }

    [Fact]
    public async Task ExpiredFailedPayloadRemainsUnavailableAndNonexecutableAfterCleanupAsync()
    {
        ProbeProvider provider = new() { Stage = "load", Failure = new IOException(PrivateMarker) };
        EntityBatchResult failed = await DispatchAsync(CreateAgent(new ProbeClient(), provider), null, "failed");
        DurableAgentState committed = ReadState(failed);
        Assert.Single(Actions(failed));
        committed.Data.ConversationHistory.Clear();
        DateTimeOffset expired = s_now.AddMinutes(2);
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        DurableAgentResultUnavailableException unavailable = await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
            () => AgentRunHandleTests.CreateHandle(committed, correlationId: "failed", timeProvider: new Clock(expired))
                .ReadAgentResponseAsync(timeout.Token));
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, unavailable.Outcome);

        ProbeClient client = new();
        ProbeProvider duplicateProvider = new();
        ChatClientAgent agent = CreateAgent(client, duplicateProvider);
        EntityBatchResult cleanup = await DispatchAsync(agent, s_converter.Serialize(committed), "unused",
            operationName: nameof(AgentEntity.CheckAndExpireResults), now: expired);
        Assert.Null(Result(cleanup).FailureDetails);
        DurableAgentState cleaned = ReadState(cleanup);
        Assert.Empty(cleaned.Data.TerminalResults!);
        Assert.Equal(DurableAgentStateCompletionReceipt.UnavailableResult, cleaned.Data.CompletionReceipts!["failed"].ResultState);
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, cleaned.Data.CompletionReceipts["failed"].Outcome);
        EntityBatchResult duplicate = await DispatchAsync(agent, cleanup.EntityState, "failed", now: expired);
        Assert.Equal(typeof(DurableAgentResultUnavailableException).FullName, Failure(duplicate).ErrorType);
        Assert.Equal(cleanup.EntityState, duplicate.EntityState);
        Assert.Equal(0, client.Count);
        Assert.Equal(0, duplicateProvider.LoadCount);
        Assert.Equal(0, duplicateProvider.StoreCount);
    }

    [Theory]
    [InlineData("load")]
    [InlineData("storeAfter")]
    public async Task DefaultSchema12FailureRollsBackAndRemainsRetryableAsync(string stage)
    {
        DurableAgentsOptions defaults = new();
        Assert.False(defaults.EnablePersistentRequestOutcomes);
        Assert.False(defaults.EnableProviderFailureFinalization);
        Assert.Equal(DurableAgentHistoryRetentionMode.KeepAll, defaults.HistoryRetentionMode);
        ProbeProvider provider = new();
        ProbeClient client = new();
        ChatClientAgent agent = CreateAgent(client, provider);
        EntityBatchResult seed = await DispatchAsync(agent, null, "seed", mailbox: false);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, ReadState(seed).SchemaVersion);
        provider.Stage = stage;
        provider.Failure = new IOException("provider failure");

        EntityBatchResult failed = await DispatchAsync(agent, seed.EntityState, "failed", mailbox: false);

        Assert.Equal(typeof(IOException).FullName, Failure(failed).ErrorType);
        Assert.Equal(seed.EntityState, failed.EntityState);
        Assert.Empty(Actions(failed));
        Assert.Null(ReadState(failed).Data.CompletionReceipts);
        provider.Failure = null;
        EntityBatchResult retry = await DispatchAsync(agent, failed.EntityState, "failed", mailbox: false);
        Assert.Null(Result(retry).FailureDetails);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, ReadState(retry).SchemaVersion);
    }

    [Theory]
    [InlineData("load", "cancel")]
    [InlineData("storeAfter", "cancel")]
    [InlineData("load", "argument")]
    [InlineData("storeAfter", "json")]
    [InlineData("load", "corrupt")]
    [InlineData("storeAfter", "shutdown")]
    public async Task CancellationInvalidInputAndCorruptionDoNotCreateCompletionAsync(string stage, string kind)
    {
        using CancellationTokenSource stopping = new();
        ProbeProvider provider = new();
        ChatClientAgent agent = CreateAgent(new ProbeClient(), provider);
        EntityBatchResult seed = await DispatchAsync(agent, null, "seed");
        provider.Stage = stage;
        provider.Failure = kind switch
        {
            "cancel" => new OperationCanceledException("cancelled"),
            "argument" => new ArgumentException("invalid provider input"),
            "json" => new JsonException("invalid provider state"),
            "corrupt" => new DurableAgentStateCorruptionException("corrupt provider state"),
            _ => new IOException("provider interrupted"),
        };
        if (kind == "shutdown")
        {
            provider.BeforeFailure = stopping.Cancel;
        }

        EntityBatchResult failed = await DispatchAsync(agent, seed.EntityState, "failed", stopping: stopping.Token);

        Assert.NotNull(Result(failed).FailureDetails);
        Assert.Equal(seed.EntityState, failed.EntityState);
        Assert.Empty(Actions(failed));
        Assert.False(ReadState(failed).Data.CompletionReceipts!.ContainsKey("failed"));
    }

    [Fact]
    public async Task ObservationPreservesProviderFiltersSourceAndStateKeysAsync()
    {
        FilteredProvider provider = new();
        ProbeClient client = new();
        ChatClientAgent agent = new(client, new ChatClientAgentOptions { Name = "agent", ChatHistoryProvider = provider });

        EntityBatchResult completed = await DispatchAsync(agent, null, "filtered");

        Assert.Null(Result(completed).FailureDetails);
        Assert.Equal(["history", "request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(AgentRequestMessageSourceType.ChatHistory, client.LastMessages[0].GetAgentRequestMessageSourceType());
        Assert.Equal(["history", "request"], provider.StoredRequests.Select(message => message.Text));
        Assert.Equal("opaque-version1", ReadState(completed).Data.Session!.Value.GetProperty("stateBag").GetProperty("filtered-state").GetString());
        Assert.Same(provider, agent.ChatHistoryProvider);
    }

    [Fact]
    public async Task ModelFailureIsNotMisclassifiedAsHistoryProviderFailureAsync()
    {
        ProbeProvider provider = new();
        ProbeClient client = new() { Failure = new IOException("model failure") };
        EntityBatchResult failed = await DispatchAsync(CreateAgent(client, provider), null, "failed");

        Assert.Equal(typeof(IOException).FullName, Failure(failed).ErrorType);
        Assert.Null(failed.EntityState);
        Assert.Empty(Actions(failed));
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(1, client.Count);
    }

    [Theory]
    [InlineData("state")]
    [InlineData("response")]
    [InlineData("afterDispatch")]
    public async Task SdkCommitFailureRollsBackFailedOutcomeAndOutboxAsync(string boundary)
    {
        ProbeProvider provider = new();
        ChatClientAgent agent = CreateAgent(new ProbeClient(), provider);
        EntityBatchResult seed = await DispatchAsync(agent, null, "seed");
        provider.Stage = "storeAfter";
        provider.Failure = new IOException(PrivateMarker);

        EntityBatchResult failed = await DispatchAsync(agent, seed.EntityState, "failed",
            failBoundary: boundary, resultRetention: TimeSpan.FromSeconds(10));

        Assert.Equal(typeof(CommitFailureException).FullName, Failure(failed).ErrorType);
        Assert.Equal(seed.EntityState, failed.EntityState);
        Assert.Empty(Actions(failed));
        Assert.False(ReadState(failed).Data.CompletionReceipts!.ContainsKey("failed"));

        EntityBatchResult retry = await DispatchAsync(agent, failed.EntityState, "failed");
        Assert.Null(Result(retry).FailureDetails);
        Assert.Equal(3, provider.StoreCount);
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, ReadState(retry).Data.CompletionReceipts!["failed"].Outcome);
    }

    private static ChatClientAgent CreateAgent(ProbeClient client, ProbeProvider provider) =>
        new(client, new ChatClientAgentOptions { Name = "agent", ChatHistoryProvider = provider });

    private static async Task<EntityBatchResult> DispatchAsync(
        ChatClientAgent agent,
        string? state,
        string correlation,
        bool mailbox = true,
        bool emptyInput = false,
        string operationName = nameof(AgentEntity.Run),
        DateTimeOffset? now = null,
        string? failBoundary = null,
        TimeSpan? resultRetention = null,
        RecordingLogger? logger = null,
        CancellationToken stopping = default)
    {
        DurableAgentsOptions options = new()
        {
            EnablePersistentRequestOutcomes = mailbox,
            DefaultTimeToLive = null,
            ResultRetentionPeriod = resultRetention ?? TimeSpan.FromMinutes(1),
        };
        options.AddAIAgent(agent, timeToLive: null, configureHistory: history => history.ProviderKey = new("probe.v1"));
        using ServiceProvider services = new ServiceCollection()
            .AddSingleton(options)
            .AddSingleton(options.GetAgentFactories())
            .AddSingleton(new Mock<DurableTaskClient>("test").Object)
            .AddSingleton<ILoggerFactory>(logger is null ? NullLoggerFactory.Instance : new RecordingLoggerFactory(logger))
            .AddSingleton<TimeProvider>(new Clock(now))
            .AddSingleton(Mock.Of<IHostApplicationLifetime>(lifetime => lifetime.ApplicationStopping == stopping))
            .BuildServiceProvider();
        ITaskEntity entity = new AgentEntity(services);
        if (failBoundary == "afterDispatch")
        {
            entity = new FailAfterDispatch(entity);
        }
        DurableTaskShimFactory factory = new(
            new DurableTaskWorkerOptions { DataConverter = new FailingConverter(failBoundary) },
            NullLoggerFactory.Instance);
        var shim = factory.CreateEntity(new(s_entityId.Name), entity, s_entityId);
        return await shim.ExecuteOperationBatchAsync(new EntityBatchRequest
        {
            InstanceId = s_entityId.ToString(),
            EntityState = state,
            Operations =
            [
                new OperationRequest
                {
                    Id = Guid.NewGuid(),
                    Operation = operationName,
                    Input = operationName == nameof(AgentEntity.Run)
                        ? s_converter.Serialize(new RunRequest(emptyInput ? [] : [new ChatMessage(ChatRole.User, "request")])
                        {
                            CorrelationId = correlation,
                        })
                        : null,
                },
            ],
        });
    }

    private static DurableAgentState ReadState(EntityBatchResult batch) =>
        Assert.IsType<DurableAgentState>(s_converter.Deserialize(batch.EntityState, typeof(DurableAgentState)));

    private static OperationResult Result(EntityBatchResult batch) =>
        Assert.Single(Assert.IsType<List<OperationResult>>(batch.Results));

    private static global::DurableTask.Core.FailureDetails Failure(EntityBatchResult batch) =>
        Assert.IsType<global::DurableTask.Core.FailureDetails>(Result(batch).FailureDetails);

    private static List<OperationAction> Actions(EntityBatchResult batch) =>
        Assert.IsType<List<OperationAction>>(batch.Actions);

    private sealed class Clock(DateTimeOffset? now = null) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now ?? s_now;
    }

    private sealed class ProbeProvider : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => ["continuation"];
        public string? Stage { get; set; }
        public Exception? Failure { get; set; }
        public Action? BeforeFailure { get; set; }
        public int LoadCount { get; private set; }
        public int StoreCount { get; private set; }
        public int? RestoredVersion { get; private set; }

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context, CancellationToken cancellationToken = default)
        {
            this.LoadCount++;
            if (context.Session!.StateBag.TryGetValue("continuation", out Continuation? value))
            {
                this.RestoredVersion = value?.Version;
            }
            if (this.Stage == "load")
            {
                this.ThrowIfFailed(context.Session);
            }
            return new(this.Stage == "lazyLoad" ? this.LazyFailure(context.Session) : []);
        }

        private IEnumerable<ChatMessage> LazyFailure(AgentSession session)
        {
            this.ThrowIfFailed(session);
            yield break;
        }

        protected override ValueTask StoreChatHistoryAsync(InvokedContext context, CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            if (this.Stage == "storeBefore")
            {
                this.ThrowIfFailed(context.Session!);
            }
            context.Session!.StateBag.SetValue("continuation", new Continuation { Version = this.StoreCount });
            if (this.Stage == "storeAfter")
            {
                this.ThrowIfFailed(context.Session);
            }
            return default;
        }

        private void ThrowIfFailed(AgentSession session)
        {
            if (this.Failure is not null)
            {
                session.StateBag.SetValue("continuation", new Continuation { Version = 999 });
                session.StateBag.SetValue("partial-state", PrivateMarker);
                this.BeforeFailure?.Invoke();
                throw this.Failure;
            }
        }
    }

    private sealed class ProbeClient : IChatClient
    {
        public int Count { get; private set; }
        public List<ChatMessage> LastMessages { get; private set; } = [];
        public Exception? Failure { get; init; }
        public object? GetService(Type serviceType, object? serviceKey = null) => null;
        public void Dispose() { }
        public Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages, ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.Count++;
            this.LastMessages = messages.ToList();
            await Task.Yield();
            if (this.Failure is not null)
            {
                throw this.Failure;
            }
            yield return new ChatResponseUpdate(ChatRole.Assistant, "response");
        }
    }

    private sealed class FilteredProvider() : ChatHistoryProvider(
        provideOutputMessageFilter: messages => messages.Where(message => message.Text != "discard"),
        storeInputRequestMessageFilter: messages => messages)
    {
        public override IReadOnlyList<string> StateKeys => ["filtered-state"];
        public IReadOnlyList<ChatMessage> StoredRequests { get; private set; } = [];

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context, CancellationToken cancellationToken = default) =>
            new([new ChatMessage(ChatRole.User, "history"), new ChatMessage(ChatRole.User, "discard")]);

        protected override ValueTask StoreChatHistoryAsync(
            InvokedContext context, CancellationToken cancellationToken = default)
        {
            this.StoredRequests = context.RequestMessages.ToList();
            context.Session!.StateBag.SetValue("filtered-state", "opaque-version1");
            return default;
        }
    }

    private sealed class Continuation
    {
        public int Version { get; set; }
    }

    private sealed class CommitFailureException : Exception
    {
        public CommitFailureException() { }
        public CommitFailureException(string? message) : base(message) { }
        public CommitFailureException(string? message, Exception? innerException) : base(message, innerException) { }
    }

    private sealed class FailAfterDispatch(ITaskEntity inner) : ITaskEntity
    {
        public async ValueTask<object?> RunAsync(TaskEntityOperation operation)
        {
            _ = await inner.RunAsync(operation);
            DurableAgentState staged = Assert.IsType<DurableAgentState>(operation.State.GetState(typeof(DurableAgentState)));
            Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, staged.Data.CompletionReceipts!["failed"].Outcome);
            throw new CommitFailureException();
        }
    }

    private sealed class FailingConverter(string? boundary) : DataConverter
    {
        public override object? Deserialize(string? data, Type targetType) => s_converter.Deserialize(data, targetType);

        [return: NotNullIfNotNull(nameof(value))]
        public override string? Serialize(object? value)
        {
            if ((boundary == "state" && value is DurableAgentState) ||
                (boundary == "response" && value is AgentResponse))
            {
                throw new CommitFailureException();
            }
            return s_converter.Serialize(value);
        }
    }

    private sealed class RecordingLoggerFactory(RecordingLogger logger) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => logger;
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public void Dispose() { }
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<string> Messages { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            this.Messages.Add(formatter(state, exception) + exception);
    }
}
