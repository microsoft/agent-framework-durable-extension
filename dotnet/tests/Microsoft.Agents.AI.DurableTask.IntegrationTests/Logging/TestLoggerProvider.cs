// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.IntegrationTests.Logging;

internal sealed class TestLoggerProvider(ITestOutputHelper output) : ILoggerProvider
{
    private readonly ITestOutputHelper _output = output ?? throw new ArgumentNullException(nameof(output));
    private readonly ConcurrentDictionary<string, TestLogger> _loggers = new(StringComparer.Ordinal);

    public bool TryGetLogs(string category, out IReadOnlyCollection<LogEntry> logs)
    {
        if (this._loggers.TryGetValue(category, out TestLogger? logger))
        {
            logs = logger.GetLogs();
            return true;
        }

        logs = [];
        return false;
    }

    public IReadOnlyCollection<LogEntry> GetAllLogs()
    {
        return this._loggers.Values
            .OfType<TestLogger>()
            .SelectMany(logger => logger.GetLogs())
            .ToList()
            .AsReadOnly();
    }

    public IReadOnlyCollection<LogEntry> GetAgentLogs(AgentSessionId sessionId)
    {
        // The SDK normalizes the entity name, but the session key remains case-sensitive.
        AgentSessionId normalizedSessionId = (Microsoft.DurableTask.Entities.EntityInstanceId)sessionId;
        string category = $"Microsoft.DurableTask.Agents.{normalizedSessionId.Name}.{sessionId.Key}";
        return this.GetAllLogs()
            .Where(log => string.Equals(log.Category, category, StringComparison.Ordinal))
            .ToArray();
    }

    public void Clear()
    {
        foreach (TestLogger logger in this._loggers.Values.OfType<TestLogger>())
        {
            logger.ClearLogs();
        }
    }

    ILogger ILoggerProvider.CreateLogger(string categoryName)
    {
        return this._loggers.GetOrAdd(categoryName, _ => new TestLogger(categoryName, this._output));
    }

    void IDisposable.Dispose()
    {
        // no-op
    }
}
