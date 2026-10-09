// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using AutoHistoryRetention;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Metrics;

namespace AutoHistoryRetentionTests;

public sealed class RetentionProbeTests
{
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

    [Theory]
    [InlineData("ABC123DEF456")]
    [InlineData("Z")]
    [InlineData("AbC-123_456")]
    public async Task ScenarioUsesMarkerModerateTurnsAndDiagnosticWithoutWaitingAsync(string marker)
    {
        RecordingAgent agent = new("HistoryKeeper");
        HistoryRetentionScenario scenario = new(agent);
        AgentSession session = await agent.CreateSessionAsync();

        HistoryRetentionScenarioResult result = await scenario.RunAsync(
            "sample",
            marker,
            session);

        Assert.Equal(HistoryRetentionDemo.ScenarioTurns + 1, agent.InvocationCount);
        Assert.Contains(marker, agent.ModelInputs[0][0].Text);
        Assert.All(
            agent.ModelInputs.Skip(1).Take(HistoryRetentionDemo.ScenarioTurns - 1),
            input => Assert.DoesNotContain(
                input,
                message => message.Text?.Contains(marker, StringComparison.Ordinal) is true));
        Assert.Equal(HistoryRetentionDemo.DiagnosticQuestion, agent.ModelInputs[^1][0].Text);
        Assert.All(
            agent.ModelInputs.Take(HistoryRetentionDemo.ScenarioTurns),
            input => Assert.True(input.Sum(message => message.Text?.Length ?? 0) >= HistoryRetentionDemo.NotesPerTurn));
        Assert.Equal("UNKNOWN", result.DiagnosticResponse);
        Assert.Equal(MarkerObservation.Unavailable, result.Observation);
        Assert.Equal(marker, result.Marker);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t\r\n")]
    [InlineData("\u00a0\u2003\u202f\u3000")]
    public async Task InvalidMarkerRejectsBeforeAgentInvocationOrOutputAsync(string? marker)
    {
        RecordingAgent agent = new("HistoryKeeper");
        HistoryRetentionScenario scenario = new(agent);
        AgentSession session = await agent.CreateSessionAsync();
        List<int> output = [-1];

        Exception? exception = await Record.ExceptionAsync(
            () => scenario.RunAsync("sample", marker!, session, output.Add));

        Assert.Equal(0, agent.InvocationCount);
        Assert.Empty(agent.ModelInputs);
        Assert.Equal([-1], output);
        AssertInvalidMarkerException(marker, exception);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t\r\n")]
    [InlineData("\u00a0\u2003\u202f\u3000")]
    public async Task InvalidMarkerRejectsBeforeChatClientCallOrSessionMutationAsync(string? marker)
    {
        using RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "HistoryKeeper");
        HistoryRetentionScenario scenario = new(agent);
        AgentSession session = await agent.CreateSessionAsync();
        JsonElement initialSession = await agent.SerializeSessionAsync(session);
        List<int> output = [-1];

        Exception? exception = await Record.ExceptionAsync(
            () => scenario.RunAsync("sample", marker!, session, output.Add));

        Assert.Equal(0, client.InvocationCount);
        Assert.Equal([-1], output);
        JsonElement finalSession = await agent.SerializeSessionAsync(session);
        Assert.Equal(initialSession.GetRawText(), finalSession.GetRawText());
        AssertInvalidMarkerException(marker, exception);
    }

    [Fact]
    public async Task ValidMarkerPreservesEightChatClientCallsAsync()
    {
        using RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "HistoryKeeper");
        HistoryRetentionScenario scenario = new(agent);
        AgentSession session = await agent.CreateSessionAsync();
        List<int> output = [];

        HistoryRetentionScenarioResult result =
            await scenario.RunAsync("sample", "AbC-123_456", session, output.Add);

        Assert.Equal(8, client.InvocationCount);
        Assert.Equal(Enumerable.Range(1, HistoryRetentionDemo.ScenarioTurns), output);
        Assert.Equal("AbC-123_456", result.Marker);
        Assert.Equal("UNKNOWN", result.DiagnosticResponse);
        Assert.Equal(MarkerObservation.Unavailable, result.Observation);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t\r\n")]
    [InlineData("\u00a0\u2003\u202f\u3000")]
    public void InvalidMarkerRejectsAtClassifierEntry(string? marker)
    {
        Exception? exception = Record.Exception(
            () => HistoryRetentionDemo.ClassifyResponse(marker!, "UNKNOWN"));

        AssertInvalidMarkerException(marker, exception);
    }

    [Theory]
    [InlineData("a", "a", MarkerObservation.Present)]
    [InlineData("AbC-123_456", "The marker is AbC-123_456.", MarkerObservation.Present)]
    [InlineData("AbC-123_456", "The marker is abc-123_456.", MarkerObservation.Inconclusive)]
    [InlineData(" marker ", "A marker with spaces: marker !", MarkerObservation.Present)]
    [InlineData("AbC-123_456", " unknown! ", MarkerObservation.Unavailable)]
    [InlineData("AbC-123_456", null, MarkerObservation.Inconclusive)]
    public void ValidMarkersPreserveExactComparison(
        string marker,
        string? response,
        MarkerObservation expected)
    {
        Assert.Equal(expected, HistoryRetentionDemo.ClassifyResponse(marker, response));
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
    public void RetentionMeterRegistrationExportsObservedMeasurement()
    {
        RecordingMetricExporter exporter = new();
        ServiceCollection services = new();
        services.AddRetentionMetrics(
            metrics => metrics.AddReader(new PeriodicExportingMetricReader(exporter)));

        using ServiceProvider provider = services.BuildServiceProvider();
        MeterProvider meterProvider = provider.GetRequiredService<MeterProvider>();
        using Meter meter = new(DurableAgentTelemetry.MeterName);
        Counter<long> counter = meter.CreateCounter<long>("sample.retention.registration");
        counter.Add(7);

        Assert.True(meterProvider.ForceFlush());
        Assert.Contains(
            exporter.Measurements,
            measurement =>
                measurement.InstrumentName == "sample.retention.registration" &&
                measurement.Value == 7);
    }

    private static void AssertInvalidMarkerException(string? marker, Exception? exception)
    {
        ArgumentException argumentException = marker is null
            ? Assert.IsType<ArgumentNullException>(exception)
            : Assert.IsType<ArgumentException>(exception);
        Assert.Equal("marker", argumentException.ParamName);
    }

    private sealed class RecordingChatClient : IChatClient
    {
        public int InvocationCount { get; private set; }

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
            string response = messages.Last().Text == HistoryRetentionDemo.DiagnosticQuestion
                ? "UNKNOWN"
                : $"ACK-{this.InvocationCount}";
            return Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, response)));
        }

        public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
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
            return isDiagnostic ? "UNKNOWN" : $"ACK-{this.InvocationCount}";
        }

        private sealed class RecordingSession : AgentSession;
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
                    this.Measurements.Enqueue(
                        new ExportedMeasurement(metric.Name, point.GetSumLong()));
                }
            }

            return ExportResult.Success;
        }
    }

    private sealed record ExportedMeasurement(string InstrumentName, long Value);
}
