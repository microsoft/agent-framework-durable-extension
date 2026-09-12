// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Client.AzureManaged;
using Microsoft.DurableTask.Client.Entities;
using Microsoft.DurableTask.Entities;
using Microsoft.DurableTask.Worker;
using Microsoft.DurableTask.Worker.AzureManaged;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Microsoft.Agents.AI.DurableTask.IntegrationTests;

/// <summary>Uses a real worker and backend; the forwarding decorator only observes and injects a failure.</summary>
[Collection("Sequential")]
[Trait("Category", "Integration")]
public sealed class ResultExpiryAtomicityTests(ITestOutputHelper output)
{
    private const string AgentName = "ExpiryProbe";
    private const string CallOrchestration = "ExpiryCall";

    [IsolatedExpiryBackendFact]
    public async Task StateAndOutboxSurviveRestartAndRollbackFailedCleanupAsync()
    {
        // There is deliberately no default connection, cloud authentication or container startup.
        string endpoint = Environment.GetEnvironmentVariable("DURABLE_AGENT_EXPIRY_EMULATOR_ENDPOINT")!;
        Assert.True(Uri.TryCreate(endpoint, UriKind.Absolute, out Uri? uri) &&
            uri.Scheme == "http" && uri.IsLoopback && uri.Port != 8080 &&
            uri.AbsolutePath == "/" && uri.UserInfo.Length == 0 && uri.Query.Length == 0 && uri.Fragment.Length == 0,
            "Supply an explicitly isolated HTTP loopback emulator endpoint on a non-default port.");
        string hub = $"expiry-{Guid.NewGuid():N}";
        string connection = $"Endpoint={endpoint};TaskHub={hub};Authentication=None";
        using CancellationTokenSource timeout = new(TimeSpan.FromMinutes(4));
        CancellationToken cancellation = timeout.Token;
        DateTimeOffset start = DateTimeOffset.UtcNow;
        Clock clock = new(start);
        Probe probe = new();
        LocalAgent agent = new(probe);
        EntityInstanceId id = new(AgentSessionId.ToEntityName(AgentName), Guid.NewGuid().ToString("N"));
        AgentEntityResultExpirationCheck original;
        AgentEntityResultExpirationCheck failedSuccessor;
        DurableAgentState committed;

        using (IHost first = await StartAsync(connection, clock, agent, probe, cancellation))
        {
            DurableTaskClient client = first.Services.GetRequiredService<DurableTaskClient>();
            Assert.True(await CallAsync(client, id, nameof(AgentEntity.Run),
                JsonSerializer.SerializeToElement(new RunRequest("first") { CorrelationId = "first" }), cancellation));
            committed = await ReadAsync(client, id, cancellation);
            original = Pending(committed, id)!;
            Assert.NotNull(original);
            Assert.Single(probe.Dispatches);
            Assert.Equal(original, Assert.Single(probe.Dispatches.Single().Signals));
            Assert.Equal(1, probe.Dispatches.Single().Writes);

            first.Services.GetRequiredService<DurableAgentsOptions>().ResultRetentionPeriod = TimeSpan.FromSeconds(2);
            Assert.True(await CallAsync(client, id, nameof(AgentEntity.Run),
                JsonSerializer.SerializeToElement(new RunRequest("future") { CorrelationId = "future" }), cancellation));
            committed = await ReadAsync(client, id, cancellation);
            Assert.Equal(2, committed.Data.TerminalResults!.Count);
            Assert.Equal(original, Pending(committed, id));
            Assert.Equal(2, probe.Dispatches.Count);
            Assert.Empty(probe.Dispatches.Last().Signals);
            Assert.Equal(2, probe.ModelCalls);
            string beforeFailure = Serialize(committed);

            clock.UtcNow = start.AddSeconds(1);
            probe.FailNextCleanup = 1;
            Assert.False(await CallAsync(client, id, nameof(AgentEntity.CheckAndExpireResults),
                JsonSerializer.SerializeToElement(original), cancellation));
            Dispatch failed = probe.Dispatches.Last();
            Assert.True(failed.Failed);
            Assert.Equal(1, failed.Writes);
            failedSuccessor = Assert.Single(failed.Signals);
            Assert.NotEqual(original.Token, failedSuccessor.Token);
            Assert.Equal(beforeFailure, Serialize(await ReadAsync(client, id, cancellation)));
            Assert.Equal(2, probe.ModelCalls);
            await first.StopAsync(cancellation);
        }

        using IHost restarted = await StartAsync(connection, clock, agent, probe, cancellation);
        DurableTaskClient restartedClient = restarted.Services.GetRequiredService<DurableTaskClient>();
        Assert.Equal(Serialize(committed), Serialize(await ReadAsync(restartedClient, id, cancellation)));

        // No client signal is sent here: only the first run's committed delayed outbox can wake cleanup.
        DurableAgentState cleaned = await WaitForStateAsync(restartedClient, id,
            state => state.Data.CompletionReceipts!["first"].ResultState == "unavailable", cancellation);
        Assert.Equal("future", Assert.Single(cleaned.Data.TerminalResults!).Key);
        AgentEntityResultExpirationCheck successor = Pending(cleaned, id)!;
        Assert.NotNull(successor);
        Assert.NotEqual(failedSuccessor.Token, successor.Token);
        Dispatch consumed = Assert.Single(probe.Dispatches, dispatch => dispatch.Input == original && !dispatch.Failed);
        Assert.Equal(successor, Assert.Single(consumed.Signals));
        Assert.Equal(1, consumed.Writes);
        Assert.Equal(4, probe.Dispatches.Count);
        Assert.Equal(3, probe.Dispatches.Sum(dispatch => dispatch.Signals.Count)); // Includes the rolled-back attempt.

        for (int duplicate = 0; duplicate < 3; duplicate++)
        {
            Assert.True(await CallAsync(restartedClient, id, nameof(AgentEntity.CheckAndExpireResults),
                JsonSerializer.SerializeToElement(original), cancellation));
            Dispatch ignored = probe.Dispatches.Last();
            Assert.Equal(0, ignored.Writes);
            Assert.Empty(ignored.Signals);
            Assert.Equal(5 + duplicate, probe.Dispatches.Count);
            Assert.Equal(Serialize(cleaned), Serialize(await ReadAsync(restartedClient, id, cancellation)));
        }

        clock.UtcNow = start.AddSeconds(3);
        DurableAgentState finished = await WaitForStateAsync(restartedClient, id,
            state => state.Data.TerminalResults!.Count == 0, cancellation);
        Assert.Null(Pending(finished, id));
        Assert.Equal(2, finished.Data.CompletionReceipts!.Count);
        Assert.All(finished.Data.CompletionReceipts.Values, receipt => Assert.Equal("unavailable", receipt.ResultState));
        Dispatch finalCleanup = Assert.Single(probe.Dispatches, dispatch => dispatch.Input == successor);
        Assert.Equal(1, finalCleanup.Writes);
        Assert.Empty(finalCleanup.Signals);

        // Observe beyond both scheduled deadlines to detect a leaked outbox from the failed operation,
        // even if its stale token would cause no visible state mutation.
        DateTimeOffset observationEnd = (successor.ScheduledTime > failedSuccessor.ScheduledTime
            ? successor.ScheduledTime : failedSuccessor.ScheduledTime).AddSeconds(10);
        TimeSpan remaining = observationEnd - DateTimeOffset.UtcNow;
        if (remaining > TimeSpan.Zero)
        {
            await Task.Delay(remaining, cancellation);
        }

        Assert.DoesNotContain(probe.Dispatches, dispatch => dispatch.Input == failedSuccessor);
        Assert.Equal(8, probe.Dispatches.Count);
        Assert.Equal(3, probe.Dispatches.Sum(dispatch => dispatch.Signals.Count));
        Assert.Equal(2, probe.ModelCalls);
        Assert.Equal(Serialize(finished), Serialize(await ReadAsync(restartedClient, id, cancellation)));
        await restarted.StopAsync(cancellation);
        output.WriteLine("Real isolated backend: 2 runs, 1 failed cleanup, restart, 2 automatic cleanups, " +
            "3 duplicates; 3 staged signals including 1 rolled back; 0 deliveries of rolled-back token.");
    }

    private static async Task<IHost> StartAsync(
        string connection, Clock clock, LocalAgent agent, Probe probe, CancellationToken cancellation)
    {
        IHost host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<TimeProvider>(clock);
                services.ConfigureDurableAgents(options =>
                {
                    options.EnableMailboxWrites = true;
                    options.DefaultTimeToLive = null;
                    options.ResultRetentionPeriod = TimeSpan.FromSeconds(1);
                    options.AddAIAgent(agent);
                },
                clientBuilder: builder => builder.UseDurableTaskScheduler(connection));
                services.AddDurableTaskWorker(builder =>
                {
                    builder.UseDurableTaskScheduler(connection);
                    builder.AddTasks(registry =>
                    {
                        // Use the public registry to observe the actual AgentEntity, not private SDK metadata.
                        registry.AddEntity(AgentSessionId.ToEntityName(AgentName),
                            provider => new ObservedEntity(new AgentEntity(provider), probe));
                        registry.AddOrchestratorFunc<Command, bool>(CallOrchestration, (context, command) =>
                            InvokeEntityAsync(context.Entities,
                                new EntityInstanceId(AgentSessionId.ToEntityName(AgentName), command.Key),
                                command.Operation, command.Input));
                    });
                });
            })
            .Build();
        try
        {
            await host.StartAsync(cancellation);
            return host;
        }
        catch
        {
            host.Dispose();
            throw;
        }
    }

    internal static async Task<bool> InvokeEntityAsync(
        TaskOrchestrationEntityFeature entities, EntityInstanceId id, string operation, JsonElement input)
    {
        try
        {
            await entities.CallEntityAsync(id, operation, input);
            return true;
        }
        catch (EntityOperationFailedException exception) when (
            operation == nameof(AgentEntity.CheckAndExpireResults) &&
            exception.EntityId == id &&
            exception.OperationName == operation &&
            exception.FailureDetails.ErrorType == typeof(InjectedCleanupFailureException).FullName &&
            exception.FailureDetails.ErrorMessage == InjectedCleanupFailureException.FailureMessage &&
            exception.FailureDetails.InnerFailure is null)
        {
            return false;
        }
    }

    internal sealed class InjectedCleanupFailureException : Exception
    {
        internal const string FailureMessage = "Injected failure after real state/outbox staging.";

        public InjectedCleanupFailureException() : base(FailureMessage)
        {
        }

        public InjectedCleanupFailureException(string? message) : base(message)
        {
        }

        public InjectedCleanupFailureException(string? message, Exception? innerException) : base(message, innerException)
        {
        }
    }

    private static async Task<bool> CallAsync(
        DurableTaskClient client, EntityInstanceId id, string operation, JsonElement input, CancellationToken cancellation)
    {
        string instance = await client.ScheduleNewOrchestrationInstanceAsync(
            CallOrchestration, new Command(id.Key, operation, input), cancellation);
        OrchestrationMetadata completion = await client.WaitForInstanceCompletionAsync(instance, true, cancellation);
        Assert.Equal(OrchestrationRuntimeStatus.Completed, completion.RuntimeStatus);
        return completion.ReadOutputAs<bool>();
    }

    private static async Task<DurableAgentState> ReadAsync(
        DurableTaskClient client, EntityInstanceId id, CancellationToken cancellation)
    {
        EntityMetadata? metadata = await client.Entities.GetEntityAsync(id, true, cancellation);
        Assert.NotNull(metadata);
        return metadata.State.ReadAs<DurableAgentState>();
    }

    private static async Task<DurableAgentState> WaitForStateAsync(
        DurableTaskClient client, EntityInstanceId id, Func<DurableAgentState, bool> predicate, CancellationToken cancellation)
    {
        while (true)
        {
            DurableAgentState state = await ReadAsync(client, id, cancellation);
            if (predicate(state))
            {
                return state;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellation);
        }
    }

    private static AgentEntityResultExpirationCheck? Pending(DurableAgentState state, EntityInstanceId id) =>
        AgentEntityResultExpirySchedule.Read(state, id.ToString())?.Pending;

    private static string Serialize(DurableAgentState state) =>
        JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);

    private sealed record Command(string Key, string Operation, JsonElement Input);

    private sealed class Clock(DateTimeOffset now) : TimeProvider
    {
        private long _ticks = now.UtcTicks;

        public DateTimeOffset UtcNow
        {
            get => new(Interlocked.Read(ref this._ticks), TimeSpan.Zero);
            set => Interlocked.Exchange(ref this._ticks, value.UtcTicks);
        }

        public override DateTimeOffset GetUtcNow() => this.UtcNow;
    }

    private sealed class Probe
    {
        public ConcurrentQueue<Dispatch> Dispatches { get; } = new();
        public int FailNextCleanup;
        public int ModelCalls;
    }

    private sealed class Dispatch
    {
        public AgentEntityResultExpirationCheck? Input { get; init; }
        public List<AgentEntityResultExpirationCheck> Signals { get; } = [];
        public int Writes { get; set; }
        public bool Failed { get; set; }
    }

    private sealed class ObservedEntity(ITaskEntity inner, Probe probe) : ITaskEntity
    {
        public async ValueTask<object?> RunAsync(TaskEntityOperation operation)
        {
            bool cleanup = operation.Name == nameof(AgentEntity.CheckAndExpireResults);
            Dispatch dispatch = new()
            {
                Input = cleanup ? operation.GetInput<AgentEntityResultExpirationCheck>() : null,
            };
            try
            {
                object? result = await inner.RunAsync(new ObservedOperation(operation, dispatch));
                if (cleanup && Interlocked.Exchange(ref probe.FailNextCleanup, 0) != 0)
                {
                    // Deliberately fail AFTER the real AgentEntity stages replacement state and signals.
                    throw new InjectedCleanupFailureException();
                }

                return result;
            }
            catch
            {
                dispatch.Failed = true;
                throw;
            }
            finally
            {
                probe.Dispatches.Enqueue(dispatch);
            }
        }
    }

    private sealed class ObservedOperation(TaskEntityOperation inner, Dispatch dispatch) : TaskEntityOperation
    {
        public override TaskEntityContext Context { get; } = new ObservedContext(inner.Context, dispatch);
        public override TaskEntityState State { get; } = new ObservedState(inner.State, dispatch);
        public override string Name => inner.Name;
        public override bool HasInput => inner.HasInput;
        public override object? GetInput(Type inputType) => inner.GetInput(inputType);
    }

    private sealed class ObservedState(TaskEntityState inner, Dispatch dispatch) : TaskEntityState
    {
        public override bool HasState => inner.HasState;
        public override object? GetState(Type type) => inner.GetState(type);
        public override void SetState(object? state)
        {
            dispatch.Writes++;
            inner.SetState(state);
        }
    }

    private sealed class ObservedContext(TaskEntityContext inner, Dispatch dispatch) : TaskEntityContext
    {
        public override EntityInstanceId Id => inner.Id;
        public override string ScheduleNewOrchestration(TaskName name, object? input = null, StartOrchestrationOptions? options = null) =>
            inner.ScheduleNewOrchestration(name, input, options);

        public override void SignalEntity(EntityInstanceId id, string operationName, object? input = null, SignalEntityOptions? options = null)
        {
            if (operationName == nameof(AgentEntity.CheckAndExpireResults))
            {
                dispatch.Signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input));
            }

            inner.SignalEntity(id, operationName, input, options);
        }
    }

    private sealed class LocalAgent(Probe probe) : AIAgent
    {
        public override string Name => AgentName;
        protected override ValueTask<AgentSession> CreateSessionCoreAsync(CancellationToken cancellationToken = default) =>
            new(new LocalSession());
        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session, JsonSerializerOptions? jsonSerializerOptions = null, CancellationToken cancellationToken = default) =>
            new(JsonSerializer.SerializeToElement(new { }));
        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState, JsonSerializerOptions? jsonSerializerOptions = null, CancellationToken cancellationToken = default) =>
            new(new LocalSession());
        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages, AgentSession? session = null, AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages, AgentSession? session = null, AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref probe.ModelCalls);
            await Task.Yield();
            yield return new AgentResponseUpdate(ChatRole.Assistant, "local response");
        }

        private sealed class LocalSession : AgentSession;
    }

    private sealed class IsolatedExpiryBackendFactAttribute : FactAttribute
    {
        public IsolatedExpiryBackendFactAttribute(
            [System.Runtime.CompilerServices.CallerFilePath] string? sourceFilePath = null,
            [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = -1)
            : base(sourceFilePath, sourceLineNumber)
        {
            if (Environment.GetEnvironmentVariable("DURABLE_AGENT_EXPIRY_INTEGRATION") != "1" ||
                string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DURABLE_AGENT_EXPIRY_EMULATOR_ENDPOINT")))
            {
                this.Skip = "NOT RUN: requires DURABLE_AGENT_EXPIRY_INTEGRATION=1 and an explicitly isolated " +
                    "DURABLE_AGENT_EXPIRY_EMULATOR_ENDPOINT; never defaults to shared localhost:8080 or starts containers.";
            }
        }
    }
}
