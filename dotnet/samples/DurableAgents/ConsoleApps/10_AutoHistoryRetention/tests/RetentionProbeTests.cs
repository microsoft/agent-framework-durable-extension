// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using AutoHistoryRetention;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using OpenTelemetry;
using OpenTelemetry.Metrics;

namespace AutoHistoryRetentionTests;

public sealed class RetentionProbeTests
{
    [Fact]
    public void PublicConfigurationExplicitlySelectsAutoWhileKeepAllRemainsDefault()
    {
        RecordingMetricExporter exporter = new();
        using IHost host = Host.CreateDefaultBuilder()
            .ConfigureServices(
                services => HistoryRetentionHost.ConfigureServices(
                    services,
                    new RecordingAgent("HistoryKeeper"),
                    "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None",
                    metrics => metrics.AddReader(new PeriodicExportingMetricReader(exporter))))
            .Build();

        DurableAgentsOptions configured =
            host.Services.GetRequiredService<DurableAgentsOptions>();
        ServiceCollection defaultServices = new();
        defaultServices.ConfigureDurableAgents(
            options => options.AddAIAgent(new RecordingAgent("DefaultAgent")),
            workerBuilder: _ => { });
        using ServiceProvider defaultProvider = defaultServices.BuildServiceProvider();
        DurableAgentsOptions defaults =
            defaultProvider.GetRequiredService<DurableAgentsOptions>();

        Assert.Equal(DurableAgentHistoryRetentionMode.Auto, configured.HistoryRetentionMode);
        Assert.True(configured.EnableMailboxWrites);
        Assert.Equal(HistoryRetentionDemo.MaxStateBytes, configured.MaxStateBytes);
        Assert.Equal(DurableAgentHistoryRetentionMode.KeepAll, defaults.HistoryRetentionMode);
        Assert.NotNull(host.Services.GetService<MeterProvider>());
    }

    [Fact]
    public void ProductionAgentOptionsEnforceBoundedModelOutput()
    {
        ChatClientAgentOptions options = HistoryRetentionDemo.CreateAgentOptions();

        Assert.Equal("HistoryKeeper", options.Name);
        Assert.Equal(
            HistoryRetentionDemo.MaxOutputTokens,
            options.ChatOptions?.MaxOutputTokens);
        Assert.Contains(
            "under 20 words",
            options.ChatOptions?.Instructions,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task AutoEvictsOldTranscriptButPreservesMailboxAndIdempotencyAsync()
    {
        RecordingMetricExporter exporter = new();
        RecordingAgent agent = new("HistoryKeeper");
        await using EntityTestHost harness = EntityTestHost.Create(
            agent,
            DurableAgentHistoryRetentionMode.Auto,
            HistoryRetentionDemo.MaxStateBytes,
            exporter);

        PressureScenarioResult scenario = await RunPressureScenarioAsync(harness);
        StateSnapshot retained = harness.GetStateSnapshot();
        int invocationsBeforeRedelivery = agent.InvocationCount;

        AgentResponse redelivered = await harness.RunAsync(
            new RunRequest("different content is ignored for a completed correlation")
            {
                CorrelationId = scenario.FirstCorrelationId,
            });

        Assert.DoesNotContain(scenario.FirstCorrelationId, retained.TranscriptCorrelationIds);
        Assert.DoesNotContain(RetentionProbeMarkers.First, retained.TranscriptText);
        Assert.DoesNotContain(RetentionProbeMarkers.ConnectedToolCall, retained.StateJson);
        Assert.Contains(scenario.NewestCorrelationId, retained.TranscriptCorrelationIds);
        Assert.Contains(RetentionProbeMarkers.Newest, retained.TranscriptText);
        Assert.Contains(scenario.FirstCorrelationId, retained.TerminalResultCorrelationIds);
        Assert.Contains(scenario.FirstCorrelationId, retained.CompletionReceiptCorrelationIds);
        Assert.Equal(
            HistoryRetentionDemo.ScenarioTurns + 1,
            retained.TerminalResultCorrelationIds.Count);
        Assert.Equal(
            HistoryRetentionDemo.ScenarioTurns + 1,
            retained.CompletionReceiptCorrelationIds.Count);
        Assert.True(retained.HasHistoryBinding);
        Assert.True(retained.HasSessionState);
        Assert.False(retained.HasExpirationTime);
        Assert.Equal(scenario.FirstResponse.Text, redelivered.Text);
        Assert.Equal(invocationsBeforeRedelivery, agent.InvocationCount);
        Assert.DoesNotContain(
            agent.ModelInputs[^1],
            message => message.Text?.Contains(
                RetentionProbeMarkers.First,
                StringComparison.Ordinal) is true);
        Assert.NotNull(retained.TruncationEvictedMessageCount);
        Assert.True(retained.TruncationEvictedMessageCount > 0);

        MeterProvider meterProvider = harness.Services.GetRequiredService<MeterProvider>();
        Assert.True(meterProvider.ForceFlush());
        Assert.Contains(
            exporter.Measurements,
            measurement =>
                measurement.InstrumentName == "durable.agent.history.retention.operations" &&
                measurement.Tags.TryGetValue("outcome", out object? outcome) &&
                string.Equals(outcome as string, "transcript_evicted", StringComparison.Ordinal));
        Assert.Contains(
            exporter.Measurements,
            measurement =>
                measurement.InstrumentName == "durable.agent.history.evicted.entries" &&
                measurement.Value > 0 &&
                measurement.Tags.TryGetValue("reason", out object? reason) &&
                string.Equals(reason as string, "transcript_pressure", StringComparison.Ordinal));
        Assert.Contains(
            exporter.Measurements,
            measurement =>
                measurement.InstrumentName == "durable.agent.history.state.size.after" &&
                measurement.Value < HistoryRetentionDemo.HighWatermarkBytes);
    }

    [Fact]
    public async Task KeepAllDoesNotProactivelyDeleteSameTranscriptAsync()
    {
        RecordingAgent agent = new("HistoryKeeper");
        await using EntityTestHost harness = EntityTestHost.Create(
            agent,
            DurableAgentHistoryRetentionMode.KeepAll,
            HistoryRetentionDemo.MaxStateBytes);

        PressureScenarioResult scenario = await RunPressureScenarioAsync(harness);
        StateSnapshot retained = harness.GetStateSnapshot();

        Assert.Contains(scenario.FirstCorrelationId, retained.TranscriptCorrelationIds);
        Assert.Contains(RetentionProbeMarkers.First, retained.TranscriptText);
        Assert.Contains(RetentionProbeMarkers.ConnectedToolCall, retained.StateJson);
        Assert.Null(retained.TruncationEvictedMessageCount);
        Assert.True(retained.SerializedSizeBytes >= HistoryRetentionDemo.HighWatermarkBytes);
    }

    [Fact]
    public async Task AutoCannotFitOneOversizedProtectedNewestPayloadAsync()
    {
        RecordingAgent agent = new("HistoryKeeper");
        await using EntityTestHost harness = EntityTestHost.Create(
            agent,
            DurableAgentHistoryRetentionMode.Auto,
            maxStateBytes: 4 * 1024);

        DurableAgentStateSizeLimitExceededException exception =
            await Assert.ThrowsAsync<DurableAgentStateSizeLimitExceededException>(
                () => harness.RunAsync(new RunRequest(new string('x', 16 * 1024))));

        Assert.Equal(4 * 1024, exception.MaxStateBytes);
        Assert.Null(harness.StateJson);
        Assert.Equal(1, agent.InvocationCount);
    }

    [Fact]
    public async Task ScenarioUsesMarkerModerateTurnsAndDiagnosticWithoutWaitingAsync()
    {
        const string Marker = "ABC123DEF456";
        RecordingAgent agent = new("HistoryKeeper");
        HistoryRetentionScenario scenario = new(agent);
        AgentSession session = await agent.CreateSessionAsync();

        HistoryRetentionScenarioResult result = await scenario.RunAsync(
            "sample",
            Marker,
            session);

        Assert.Equal(HistoryRetentionDemo.ScenarioTurns + 1, agent.InvocationCount);
        Assert.Contains(Marker, agent.ModelInputs[0][0].Text);
        Assert.All(
            agent.ModelInputs.Skip(1).Take(HistoryRetentionDemo.ScenarioTurns - 1),
            input => Assert.DoesNotContain(
                input,
                message => message.Text?.Contains(Marker, StringComparison.Ordinal) is true));
        Assert.Equal(HistoryRetentionDemo.DiagnosticQuestion, agent.ModelInputs[^1][0].Text);
        Assert.All(
            agent.ModelInputs.Take(HistoryRetentionDemo.ScenarioTurns),
            input => Assert.True(input.Sum(message => message.Text?.Length ?? 0) >= HistoryRetentionDemo.NotesPerTurn));
        Assert.Equal("UNKNOWN", result.DiagnosticResponse);
        Assert.Equal(MarkerObservation.Unavailable, result.Observation);
    }

    [Theory]
    [InlineData("The marker is ABC123DEF456.", MarkerObservation.Present)]
    [InlineData("UNKNOWN", MarkerObservation.Unavailable)]
    [InlineData("UNKNOWN.", MarkerObservation.Unavailable)]
    [InlineData("I cannot determine it.", MarkerObservation.Inconclusive)]
    [InlineData("UNKNOWN, but I may remember part of it.", MarkerObservation.Inconclusive)]
    [InlineData("UNKNOWN, but perhaps ABC123DEF456.", MarkerObservation.Present)]
    public void DiagnosticClassificationAvoidsFalsePasses(
        string response,
        MarkerObservation expected)
    {
        Assert.Equal(
            expected,
            HistoryRetentionDemo.ClassifyResponse("ABC123DEF456", response));
    }

    [Fact]
    public void ProductionRegistrationExportsAnObservedMeasurementToConsole()
    {
        ServiceCollection services = new();
        services.AddRetentionMetrics();

        using ServiceProvider provider = services.BuildServiceProvider();
        MeterProvider meterProvider = provider.GetRequiredService<MeterProvider>();
        using System.Diagnostics.Metrics.Meter meter = new(DurableAgentTelemetry.MeterName);
        System.Diagnostics.Metrics.Counter<long> counter =
            meter.CreateCounter<long>("sample.console.retention.test");
        StringWriter output = new();
        TextWriter originalOutput = Console.Out;

        try
        {
            Console.SetOut(output);
            counter.Add(7);
            Assert.True(meterProvider.ForceFlush());
        }
        finally
        {
            Console.SetOut(originalOutput);
        }

        Assert.Contains("sample.console.retention.test", output.ToString(), StringComparison.Ordinal);
        Assert.Contains("7", output.ToString(), StringComparison.Ordinal);
    }

    private static async Task<PressureScenarioResult> RunPressureScenarioAsync(EntityTestHost harness)
    {
        RunRequest firstRequest = new(
            [
                new ChatMessage(
                    ChatRole.User,
                    HistoryRetentionDemo.CreateFirstNote(
                        "sample",
                        RetentionProbeMarkers.First,
                        new string('a', HistoryRetentionDemo.NotesPerTurn))),
                new ChatMessage(
                    ChatRole.Assistant,
                    [new FunctionCallContent(RetentionProbeMarkers.ConnectedToolCall, "remember_note")]),
                new ChatMessage(
                    ChatRole.Tool,
                    [new FunctionResultContent(RetentionProbeMarkers.ConnectedToolCall, "stored")]),
            ])
        {
            CorrelationId = "first",
        };
        AgentResponse firstResponse = await harness.RunAsync(firstRequest);

        for (int turn = 2; turn <= HistoryRetentionDemo.ScenarioTurns; turn++)
        {
            string marker = turn == HistoryRetentionDemo.ScenarioTurns
                ? RetentionProbeMarkers.Newest
                : $"MIDDLE-{turn}";
            string prompt = HistoryRetentionDemo.CreateLaterNote(
                "sample",
                turn,
                $"{marker} {new string((char)('a' + turn - 1), HistoryRetentionDemo.NotesPerTurn)}");
            RunRequest request = new(prompt)
            {
                CorrelationId = turn == HistoryRetentionDemo.ScenarioTurns
                    ? "newest"
                    : $"middle-{turn}",
            };
            _ = await harness.RunAsync(request);
        }

        _ = await harness.RunAsync(
            new RunRequest(HistoryRetentionDemo.DiagnosticQuestion)
            {
                CorrelationId = "diagnostic",
            });

        return new(
            "first",
            "newest",
            firstResponse);
    }

    private sealed record PressureScenarioResult(
        string FirstCorrelationId,
        string NewestCorrelationId,
        AgentResponse FirstResponse);

    private static class RetentionProbeMarkers
    {
        public const string First = "FIRST-MARKER-ABC123";
        public const string Newest = "NEWEST-PRESSURE";
        public const string ConnectedToolCall = "connected-tool-call";
    }

    private sealed class RecordingAgent(string name) : AIAgent
    {
        public int InvocationCount { get; private set; }

        public List<IReadOnlyList<ChatMessage>> ModelInputs { get; } = [];

        public override string? Name => name;

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) =>
            new(new RecordingSession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(JsonSerializer.SerializeToElement(new { version = 1 }));

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(new RecordingSession());

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            string response = this.RecordAndCreateResponse(messages);
            return Task.FromResult(
                new AgentResponse(new ChatMessage(ChatRole.Assistant, response)));
        }

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield return new AgentResponseUpdate(
                ChatRole.Assistant,
                this.RecordAndCreateResponse(messages));
        }

        private string RecordAndCreateResponse(IEnumerable<ChatMessage> messages)
        {
            List<ChatMessage> input = messages.ToList();
            this.InvocationCount++;
            this.ModelInputs.Add(input);
            bool isDiagnostic = input.Any(
                message => string.Equals(
                    message.Text,
                    HistoryRetentionDemo.DiagnosticQuestion,
                    StringComparison.Ordinal));
            bool markerPresent = input.Any(
                message => message.Text?.Contains(
                    RetentionProbeMarkers.First,
                    StringComparison.Ordinal) is true);
            return isDiagnostic
                ? markerPresent ? RetentionProbeMarkers.First : "UNKNOWN"
                : $"ACK-{this.InvocationCount}";
        }

        private sealed class RecordingSession : AgentSession;
    }

    private sealed class EntityTestHost : IAsyncDisposable
    {
        private readonly ServiceProvider _services;
        private readonly AgentSessionId _sessionId;
        private DurableAgentState? _persistedState;

        private EntityTestHost(
            ServiceProvider services,
            AgentSessionId sessionId)
        {
            this._services = services;
            this._sessionId = sessionId;
        }

        public IServiceProvider Services => this._services;

        public string? StateJson =>
            this._persistedState is null
                ? null
                : JsonSerializer.Serialize(
                    this._persistedState,
                    DurableAgentStateJsonContext.Default.DurableAgentState);

        public static EntityTestHost Create(
            AIAgent agent,
            DurableAgentHistoryRetentionMode mode,
            int maxStateBytes,
            RecordingMetricExporter? exporter = null)
        {
            ServiceCollection services = new();
            services.AddLogging();
            services.AddSingleton(new Mock<DurableTaskClient>("sample-test").Object);
            if (exporter is not null)
            {
                services.AddRetentionMetrics(
                    metrics => metrics.AddReader(new PeriodicExportingMetricReader(exporter)));
            }

            DurableAgentsOptions options = new()
            {
                DefaultTimeToLive = null,
                HistoryRetentionMode = mode,
                MaxStateBytes = maxStateBytes,
            };
            options.AddAIAgent(agent);
            services.AddSingleton(options);
            services.AddSingleton(
                options.GetAgentFactories());
            services.AddSingleton(Mock.Of<IHostApplicationLifetime>(
                lifetime => lifetime.ApplicationStopping == CancellationToken.None));

            ServiceProvider provider = services.BuildServiceProvider();
            if (exporter is not null)
            {
                _ = provider.GetRequiredService<MeterProvider>();
            }

            return new EntityTestHost(
                provider,
                new AgentSessionId(agent.Name!, "sample-session"));
        }

        public async Task<AgentResponse> RunAsync(RunRequest request)
        {
            Mock<TaskEntityContext> context = new();
            context.SetupGet(value => value.Id)
                .Returns((EntityInstanceId)this._sessionId);
            Mock<TaskEntityState> state = new();
            state.SetupGet(value => value.HasState)
                .Returns(() => this._persistedState is not null);
            state.Setup(value => value.GetState(typeof(DurableAgentState)))
                .Returns(() => this.ReloadState());
            state.Setup(value => value.SetState(It.IsAny<object?>()))
                .Callback<object?>(value =>
                    this._persistedState = Assert.IsType<DurableAgentState>(value));
            Mock<TaskEntityOperation> operation = new();
            operation.SetupGet(value => value.Name).Returns("Run");
            operation.SetupGet(value => value.Context).Returns(context.Object);
            operation.SetupGet(value => value.State).Returns(state.Object);
            operation.SetupGet(value => value.HasInput).Returns(true);
            operation.Setup(value => value.GetInput(It.IsAny<Type>()))
                .Returns(request);

            AgentEntity entity = new(this._services);
            object? response = await entity.RunAsync(operation.Object);
            return Assert.IsType<AgentResponse>(response);
        }

        public StateSnapshot GetStateSnapshot() =>
            StateSnapshot.Parse(
                this.StateJson ?? throw new InvalidOperationException("No state was persisted."));

        public ValueTask DisposeAsync()
        {
            return this._services.DisposeAsync();
        }

        private DurableAgentState? ReloadState()
        {
            return this._persistedState is null
                ? null
                : JsonSerializer.Deserialize(
                    JsonSerializer.SerializeToUtf8Bytes(
                        this._persistedState,
                        DurableAgentStateJsonContext.Default.DurableAgentState),
                    DurableAgentStateJsonContext.Default.DurableAgentState);
        }
    }

    private sealed record StateSnapshot(
        string StateJson,
        int SerializedSizeBytes,
        IReadOnlyList<string> TranscriptCorrelationIds,
        string TranscriptText,
        IReadOnlyList<string> TerminalResultCorrelationIds,
        IReadOnlyList<string> CompletionReceiptCorrelationIds,
        bool HasHistoryBinding,
        bool HasSessionState,
        bool HasExpirationTime,
        int? TruncationEvictedMessageCount)
    {
        public static StateSnapshot Parse(string json)
        {
            using JsonDocument document = JsonDocument.Parse(json);
            JsonElement data = document.RootElement.GetProperty("data");
            JsonElement transcript = data.GetProperty("conversationHistory");
            List<string> correlations = [];
            List<string> text = [];
            foreach (JsonElement entry in transcript.EnumerateArray())
            {
                if (entry.TryGetProperty("correlationId", out JsonElement correlationId))
                {
                    correlations.Add(correlationId.GetString()!);
                }

                CollectText(entry, text);
            }

            int? evictedMessages = data.TryGetProperty("truncation", out JsonElement truncation)
                ? truncation.GetProperty("evictedMessageCount").GetInt32()
                : null;

            return new StateSnapshot(
                json,
                Encoding.UTF8.GetByteCount(json),
                correlations,
                string.Join('\n', text),
                ReadPropertyNames(data, "terminalResults"),
                ReadPropertyNames(data, "completionReceipts"),
                data.TryGetProperty("historyBinding", out _),
                data.TryGetProperty("session", out _),
                data.TryGetProperty("expirationTimeUtc", out _),
                evictedMessages);
        }

        private static List<string> ReadPropertyNames(
            JsonElement data,
            string propertyName) =>
            data.TryGetProperty(propertyName, out JsonElement property)
                ? property.EnumerateObject().Select(item => item.Name).ToList()
                : [];

        private static void CollectText(JsonElement element, List<string> text)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (JsonProperty property in element.EnumerateObject())
                {
                    if (property.NameEquals("text") &&
                        property.Value.ValueKind == JsonValueKind.String)
                    {
                        text.Add(property.Value.GetString()!);
                    }
                    else
                    {
                        CollectText(property.Value, text);
                    }
                }
            }
            else if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (JsonElement item in element.EnumerateArray())
                {
                    CollectText(item, text);
                }
            }
        }
    }

    private sealed class RecordingMetricExporter : BaseExporter<Metric>
    {
        public ConcurrentQueue<ExportedMeasurement> Measurements { get; } = new();

        public override ExportResult Export(in Batch<Metric> batch)
        {
            foreach (Metric metric in batch)
            {
                foreach (ref readonly MetricPoint point in metric.GetMetricPoints())
                {
                    Dictionary<string, object?> tags = new(StringComparer.Ordinal);
                    foreach (KeyValuePair<string, object?> tag in point.Tags)
                    {
                        tags[tag.Key] = tag.Value;
                    }

                    this.Measurements.Enqueue(
                        new ExportedMeasurement(
                            metric.Name,
                            ReadValue(metric, point),
                            tags));
                }
            }

            return ExportResult.Success;
        }

        private static double ReadValue(Metric metric, MetricPoint point) =>
            metric.MetricType switch
            {
                MetricType.LongSum => point.GetSumLong(),
                MetricType.Histogram => point.GetHistogramSum(),
                _ => 0,
            };
    }

    private sealed record ExportedMeasurement(
        string InstrumentName,
        double Value,
        IReadOnlyDictionary<string, object?> Tags);
}
