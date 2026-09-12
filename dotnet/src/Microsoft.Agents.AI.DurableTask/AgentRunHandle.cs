// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Client.Entities;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Represents a handle for a running agent request that can be used to retrieve the response.
/// </summary>
internal sealed class AgentRunHandle
{
    private readonly DurableTaskClient _client;
    private readonly ILogger _logger;
    private readonly TimeProvider _timeProvider;

    internal AgentRunHandle(
        DurableTaskClient client,
        ILogger logger,
        AgentSessionId sessionId,
        string correlationId,
        TimeProvider? timeProvider = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);

        this._client = client;
        this._logger = logger;
        this._timeProvider = timeProvider ?? TimeProvider.System;
        this.SessionId = sessionId;
        this.CorrelationId = correlationId;
    }

    /// <summary>
    /// Gets the correlation ID for this request.
    /// </summary>
    public string CorrelationId { get; }

    /// <summary>
    /// Gets the session ID for this request.
    /// </summary>
    public AgentSessionId SessionId { get; }

    /// <summary>
    /// Reads the agent response for this request by polling the entity state until the response is found.
    /// Uses an exponential backoff polling strategy with a maximum interval of 3 seconds.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The agent response corresponding to this request.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the response is not found after polling.</exception>
    public async Task<AgentResponse> ReadAgentResponseAsync(CancellationToken cancellationToken = default)
    {
        DurableAgentRunOutcome outcome = await this.ReadAgentOutcomeAsync(cancellationToken);
        return outcome.GetResponse(this.CorrelationId);
    }

    internal async Task<DurableAgentRunOutcome> ReadAgentOutcomeAsync(
        CancellationToken cancellationToken = default)
    {
        TimeSpan pollInterval = TimeSpan.FromMilliseconds(50); // Start with 50ms
        TimeSpan maxPollInterval = TimeSpan.FromSeconds(3); // Maximum 3 seconds

        this._logger.LogStartPollingForResponse(this.SessionId, this.CorrelationId);

        while (true)
        {
            // Poll the entity state for responses
            EntityMetadata<DurableAgentState>? entityResponse = await this._client.Entities.GetEntityAsync<DurableAgentState>(
                this.SessionId,
                cancellation: cancellationToken);
            DurableAgentState? state = entityResponse?.State;

            if (state is not null)
            {
                DurableAgentRunOutcome outcome;
                try
                {
                    outcome = DurableAgentStateOutcomeResolver.Resolve(
                        state,
                        this.CorrelationId,
                        this._timeProvider.GetUtcNow());
                }
                catch (DurableAgentStateCorruptionException exception)
                {
                    this._logger.LogDurableOutcomeStateCorruption(
                        exception,
                        this.SessionId,
                        this.CorrelationId);
                    throw;
                }

                if (outcome.Kind != DurableAgentRunOutcomeKind.Pending)
                {
                    this._logger.LogDonePollingForResponse(this.SessionId, this.CorrelationId);
                    return outcome;
                }
            }

            // Wait before polling again with exponential backoff
            await Task.Delay(pollInterval, cancellationToken);

            // Double the poll interval, but cap it at the maximum
            pollInterval = TimeSpan.FromMilliseconds(Math.Min(pollInterval.TotalMilliseconds * 2, maxPollInterval.TotalMilliseconds));
        }
    }
}
