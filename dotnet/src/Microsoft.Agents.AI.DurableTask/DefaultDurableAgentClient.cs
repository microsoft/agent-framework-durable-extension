// Copyright (c) Microsoft. All rights reserved.

using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Microsoft.Agents.AI.DurableTask;

internal class DefaultDurableAgentClient(
    DurableTaskClient client,
    ILoggerFactory loggerFactory,
    TimeProvider? timeProvider = null) : IDurableAgentClient
{
    private readonly DurableTaskClient _client = client ?? throw new ArgumentNullException(nameof(client));
    private readonly ILogger _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<DefaultDurableAgentClient>();
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;

    public async Task<AgentRunHandle> RunAgentAsync(
        AgentSessionId sessionId,
        RunRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        string correlationId = request.CorrelationId;
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            throw new ArgumentException(
                "A non-empty correlation ID is required to run a durable agent request.",
                nameof(request));
        }

        this._logger.LogSignallingAgent(sessionId);

        await this._client.Entities.SignalEntityAsync(
            sessionId,
            nameof(AgentEntity.Run),
            request,
            cancellation: cancellationToken);

        return new AgentRunHandle(this._client, this._logger, sessionId, correlationId, this._timeProvider);
    }
}
