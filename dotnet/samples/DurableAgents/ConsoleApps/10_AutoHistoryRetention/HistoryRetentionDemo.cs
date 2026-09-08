// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.DurableTask.Client.AzureManaged;
using Microsoft.DurableTask.Worker.AzureManaged;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;

namespace AutoHistoryRetention;

public enum MarkerObservation
{
    Present,
    Unavailable,
    Inconclusive,
}

public sealed record HistoryRetentionScenarioResult(
    string Marker,
    string DiagnosticResponse,
    MarkerObservation Observation);

public sealed class HistoryRetentionScenario
{
    private readonly AIAgent _agent;
    private readonly Func<TimeSpan, CancellationToken, Task> _delayAsync;

    public HistoryRetentionScenario(
        AIAgent agent,
        Func<TimeSpan, CancellationToken, Task>? delayAsync = null)
    {
        this._agent = agent;
        this._delayAsync = delayAsync ?? Task.Delay;
    }

    public async Task<HistoryRetentionScenarioResult> RunAsync(
        string topic,
        string marker,
        AgentSession session,
        Action? beforeEligibilityWait = null,
        Action<int>? beforeNote = null,
        CancellationToken cancellationToken = default)
    {
        for (int turn = 1; turn <= HistoryRetentionDemo.ScenarioTurns; turn++)
        {
            if (turn == HistoryRetentionDemo.EligibleHistoryTurns + 1)
            {
                beforeEligibilityWait?.Invoke();
                await this._delayAsync(
                    HistoryRetentionDemo.DeliveryProtectionWait,
                    cancellationToken);
            }

            string notes = new((char)('a' + turn - 1), HistoryRetentionDemo.NotesPerTurn);
            string prompt = turn == 1
                ? HistoryRetentionDemo.CreateFirstNote(topic, marker, notes)
                : HistoryRetentionDemo.CreateLaterNote(topic, turn, notes);

            beforeNote?.Invoke(turn);
            _ = await this._agent.RunAsync(prompt, session, cancellationToken: cancellationToken);
        }

        AgentResponse diagnosticResponse = await this._agent.RunAsync(
            HistoryRetentionDemo.DiagnosticQuestion,
            session,
            cancellationToken: cancellationToken);

        return new(
            marker,
            diagnosticResponse.Text,
            HistoryRetentionDemo.ClassifyResponse(marker, diagnosticResponse.Text));
    }
}

public static class HistoryRetentionHost
{
    public static IServiceCollection ConfigureServices(
        IServiceCollection services,
        AIAgent agent,
        string dtsConnectionString,
        Action<MeterProviderBuilder>? configureMetricExporter = null)
    {
        services.AddRetentionMetrics(configureMetricExporter);
        services.ConfigureDurableAgents(
            options =>
            {
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = HistoryRetentionDemo.MaxStateBytes;
                options.AddAIAgent(agent, timeToLive: TimeSpan.FromHours(1));
            },
            workerBuilder: builder => builder.UseDurableTaskScheduler(dtsConnectionString),
            clientBuilder: builder => builder.UseDurableTaskScheduler(dtsConnectionString));

        return services;
    }
}

public static class HistoryRetentionDemo
{
    public const int MaxStateBytes = 32 * 1024; // 32 KiB (32,768 bytes)
    public const int ScenarioTurns = 7;
    public const int EligibleHistoryTurns = 4;
    public const int NotesPerTurn = 4 * 1024;
    public const int MaxTopicCharacters = 80;
    public const double HighWatermark = 0.85;
    public const double LowWatermark = 0.70;
    public static readonly TimeSpan DeliveryProtectionWindow = TimeSpan.FromSeconds(60);
    public static readonly TimeSpan DeliveryProtectionWait = TimeSpan.FromSeconds(61);

    public const string DiagnosticQuestion =
        "What exact marker was in my first note? If it is not present in the conversation history, " +
        "answer UNKNOWN. Do not guess.";

    public static int HighWatermarkBytes => (int)(MaxStateBytes * HighWatermark);

    public static int LowWatermarkBytes => (int)(MaxStateBytes * LowWatermark);

    public static IServiceCollection AddRetentionMetrics(
        this IServiceCollection services,
        Action<MeterProviderBuilder>? configureExporter = null)
    {
        services.AddOpenTelemetry()
            .WithMetrics(metrics =>
            {
                metrics.AddMeter(DurableAgentTelemetry.MeterName);
                if (configureExporter is null)
                {
                    metrics.AddConsoleExporter(
                        (_, readerOptions) =>
                            readerOptions.PeriodicExportingMetricReaderOptions
                                .ExportIntervalMilliseconds = 5_000);
                }
                else
                {
                    configureExporter(metrics);
                }
            });
        return services;
    }

    public static string CreateMarker() =>
        Guid.NewGuid().ToString("N")[..12].ToUpperInvariant();

    public static string CreateFirstNote(
        string topic,
        string marker,
        string referenceText) =>
        $"""
        First note for project '{topic}'. Remember this exact marker: {marker}.
        Acknowledge the marker concisely. Reference text: {referenceText}
        """;

    public static string CreateLaterNote(
        string topic,
        int noteNumber,
        string referenceText) =>
        $"Note {noteNumber} for project '{topic}'. Reference text: {referenceText}";

    public static MarkerObservation ClassifyResponse(string marker, string? response)
    {
        string text = response ?? string.Empty;
        if (text.Contains(marker, StringComparison.Ordinal))
        {
            return MarkerObservation.Present;
        }

        string normalized = text.Trim().TrimEnd('.', '!', '?');
        if (normalized.Equals("UNKNOWN", StringComparison.OrdinalIgnoreCase))
        {
            return MarkerObservation.Unavailable;
        }

        return MarkerObservation.Inconclusive;
    }
}
