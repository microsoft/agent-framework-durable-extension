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
