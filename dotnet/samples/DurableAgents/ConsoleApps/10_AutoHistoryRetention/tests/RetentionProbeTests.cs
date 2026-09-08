// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using AutoHistoryRetention;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Metrics;

namespace AutoHistoryRetentionTests;

public sealed class RetentionProbeTests
{
    [Fact]
    public void PublicConfigurationSelectsAutoBudgetAndNormalPipelines()
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

        DurableAgentsOptions options =
            host.Services.GetRequiredService<DurableAgentsOptions>();

        Assert.Equal(DurableAgentHistoryRetentionMode.Auto, options.HistoryRetentionMode);
        Assert.Equal(32_768, options.MaxStateBytes);
        Assert.Equal(27_852, HistoryRetentionDemo.HighWatermarkBytes);
        Assert.Equal(22_937, HistoryRetentionDemo.LowWatermarkBytes);
        Assert.NotNull(host.Services.GetService<ILoggerFactory>());
        Assert.NotNull(host.Services.GetService<MeterProvider>());
    }

    [Fact]
    public async Task ScenarioUsesMarkerModerateTurnsInjectedDelayAndDiagnostic()
    {
        const string Marker = "ABC123DEF456";
        RecordingAgent agent = new("HistoryKeeper");
        List<(TimeSpan Delay, int PromptCount)> delays = [];
        HistoryRetentionScenario scenario = new(
            agent,
            (delay, _) =>
            {
                delays.Add((delay, agent.Prompts.Count));
                return Task.CompletedTask;
            });
        AgentSession session = await agent.CreateSessionAsync();

        HistoryRetentionScenarioResult result = await scenario.RunAsync(
            "sample",
            Marker,
            session);

        Assert.Equal(HistoryRetentionDemo.ScenarioTurns + 1, agent.Prompts.Count);
        Assert.Contains(Marker, agent.Prompts[0]);
        Assert.StartsWith("First note", agent.Prompts[0], StringComparison.Ordinal);
        Assert.All(
            agent.Prompts.Skip(1).Take(HistoryRetentionDemo.ScenarioTurns - 1),
            prompt => Assert.DoesNotContain(Marker, prompt, StringComparison.Ordinal));
        Assert.DoesNotContain(Marker, agent.Prompts[^1]);
        Assert.Equal(HistoryRetentionDemo.DiagnosticQuestion, agent.Prompts[^1]);
        Assert.All(
            agent.Prompts.Take(HistoryRetentionDemo.ScenarioTurns),
            prompt => Assert.True(prompt.Length >= HistoryRetentionDemo.NotesPerTurn));
        Assert.True(
            HistoryRetentionDemo.ScenarioTurns * HistoryRetentionDemo.NotesPerTurn >
            HistoryRetentionDemo.HighWatermarkBytes);
        Assert.True(
            (HistoryRetentionDemo.ScenarioTurns - HistoryRetentionDemo.EligibleHistoryTurns) *
            HistoryRetentionDemo.NotesPerTurn <
            HistoryRetentionDemo.LowWatermarkBytes);
        Assert.Equal(
            [(HistoryRetentionDemo.DeliveryProtectionWait, HistoryRetentionDemo.EligibleHistoryTurns)],
            delays);
        Assert.Equal("UNKNOWN", result.DiagnosticResponse);
        Assert.Equal(MarkerObservation.Unavailable, result.Observation);
    }

    [Fact]
    public void PromptsKeepMarkerOutOfDiagnosticQuestion()
    {
        const string Marker = "ABC123DEF456";

        string firstPrompt = HistoryRetentionDemo.CreateFirstNote("sample", Marker, "reference");
        string filler = new('b', HistoryRetentionDemo.NotesPerTurn);
        string laterPrompt = HistoryRetentionDemo.CreateLaterNote("sample", 2, filler);

        Assert.Contains(Marker, firstPrompt);
        Assert.Contains("Remember this exact marker", firstPrompt);
        Assert.DoesNotContain(Marker, filler, StringComparison.Ordinal);
        Assert.DoesNotContain(Marker, laterPrompt, StringComparison.Ordinal);
        Assert.DoesNotContain(Marker, HistoryRetentionDemo.DiagnosticQuestion);
        Assert.Contains("UNKNOWN", HistoryRetentionDemo.DiagnosticQuestion);
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
    public void RegisteredMeterExportsAnObservedMeasurement()
    {
        RecordingMetricExporter exporter = new();
        ServiceCollection services = new();
        services.AddRetentionMetrics(
            metrics => metrics.AddReader(new PeriodicExportingMetricReader(exporter)));

        using ServiceProvider provider = services.BuildServiceProvider();
        MeterProvider meterProvider = provider.GetRequiredService<MeterProvider>();
        using Meter meter = new(DurableAgentTelemetry.MeterName);
        Counter<long> counter = meter.CreateCounter<long>("sample.retention.test");
        counter.Add(42);

        Assert.True(meterProvider.ForceFlush());
        Assert.Contains(
            exporter.Measurements,
            measurement =>
                measurement.InstrumentName == "sample.retention.test" &&
                measurement.Value == 42);
    }

    [Fact]
    public void ProductionRegistrationExportsAnObservedMeasurementToConsole()
    {
        ServiceCollection services = new();
        services.AddRetentionMetrics();

        using ServiceProvider provider = services.BuildServiceProvider();
        MeterProvider meterProvider = provider.GetRequiredService<MeterProvider>();
        using Meter meter = new(DurableAgentTelemetry.MeterName);
        Counter<long> counter = meter.CreateCounter<long>("sample.console.retention.test");
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

    private sealed class RecordingAgent(string name) : AIAgent
    {
        public List<string> Prompts { get; } = [];

        public override string? Name => name;

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) =>
            new(new RecordingSession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

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
            string prompt = Assert.Single(messages).Text;
            this.Prompts.Add(prompt);
            string response = prompt == HistoryRetentionDemo.DiagnosticQuestion
                ? "UNKNOWN"
                : "Acknowledged.";
            return Task.FromResult(
                new AgentResponse(new ChatMessage(ChatRole.Assistant, response)));
        }

        protected override IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

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
                        new(metric.Name, point.GetSumLong()));
                }
            }

            return ExportResult.Success;
        }
    }

    private sealed record ExportedMeasurement(string InstrumentName, long Value);
}
