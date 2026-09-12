// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask;

internal class AgentEntity(IServiceProvider services, CancellationToken cancellationToken = default) : TaskEntity<DurableAgentState>
{
    private static readonly TimeSpan s_minimumResultExpirationSignalDelay = TimeSpan.FromMinutes(1);
    private readonly IServiceProvider _services = services;
    private readonly DurableTaskClient _client = services.GetRequiredService<DurableTaskClient>();
    private readonly ILoggerFactory _loggerFactory = services.GetRequiredService<ILoggerFactory>();
    private readonly IAgentResponseHandler? _messageHandler = services.GetService<IAgentResponseHandler>();
    private readonly DurableAgentsOptions _options = services.GetRequiredService<DurableAgentsOptions>();
    // Entity operations execute once rather than replaying like orchestrations, and
    // TaskEntityContext does not expose a deterministic clock.
    private readonly TimeProvider _timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;
    private readonly CancellationToken _cancellationToken = cancellationToken != default
        ? cancellationToken
        : services.GetService<IHostApplicationLifetime>()?.ApplicationStopping ?? CancellationToken.None;

    protected override DurableAgentState InitializeState(TaskEntityOperation entityOperation)
    {
        return this._options.EnableMailboxWrites &&
            entityOperation.Name is nameof(Run) or nameof(RunAgentAsync)
            ? new DurableAgentState
            {
                SchemaVersion = DurableAgentState.RevisedSchemaVersion,
                MailboxWritesAuthorized = true,
                Data = new DurableAgentStateData
                {
                    TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(StringComparer.Ordinal),
                    CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(StringComparer.Ordinal),
                },
            }
            : base.InitializeState(entityOperation);
    }

    public Task<AgentResponse> RunAgentAsync(RunRequest request)
    {
        return this.Run(request);
    }

    // IDE1006 and VSTHRD200 disabled to allow method name to match the common cross-platform entity operation name.
#pragma warning disable IDE1006
#pragma warning disable VSTHRD200
    public async Task<AgentResponse> Run(RunRequest request)
#pragma warning restore VSTHRD200
#pragma warning restore IDE1006
    {
        ArgumentNullException.ThrowIfNull(request);

        AgentSessionId sessionId = this.Context.Id;
        ILogger logger = this.GetLogger(sessionId.Name, sessionId.Key);

        string correlationId = request.CorrelationId;
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            throw new ArgumentException(
                "A non-empty correlation ID is required to run a durable agent request.",
                nameof(request));
        }

        DateTimeOffset currentTime = this._timeProvider.GetUtcNow();
        DurableAgentRunOutcome existingOutcome;
        try
        {
            existingOutcome = DurableAgentStateOutcomeResolver.Resolve(
                this.State,
                correlationId,
                currentTime);
        }
        catch (DurableAgentStateCorruptionException exception)
        {
            logger.LogDurableOutcomeStateCorruption(
                exception,
                sessionId,
                correlationId);
            throw;
        }

        if (existingOutcome.Kind != DurableAgentRunOutcomeKind.Pending)
        {
            // Correlation is the caller's idempotency key. Retained terminal state is reused
            // without comparing request content, so callers must not reuse it for another request.
            // Surface failures before optional migration so failed delivery never writes state.
            AgentResponse committedResponse = existingOutcome.GetResponse(correlationId);
            if (this._options.EnableMailboxWrites &&
                this.State.SchemaVersion != DurableAgentState.RevisedSchemaVersion &&
                this._options.AuthorizeLegacyMigration?.Invoke(this.State) == true &&
                existingOutcome.Kind != DurableAgentRunOutcomeKind.CompletedResultUnavailable)
            {
                // Legacy evidence is converted without constructing or invoking the agent.
                DurableAgentState migrated = DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(
                    this.State, hasAuthoritativeLegacyHistory: true);
                ValidateForCommit(migrated);
                this.State = migrated;
            }

            return committedResponse;
        }

        if (request.Messages is not { Count: > 0 })
        {
            throw new ArgumentException(
                "At least one message is required for a new durable agent request.",
                nameof(request));
        }

        if (this._options.EnableMailboxWrites)
        {
            DurableAgentStateContract.ValidateIdentifier(correlationId, "correlationId");
        }

        if (!this._options.EnableMailboxWrites &&
            this.State.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
        {
            throw new InvalidOperationException("New mailbox requests require EnableMailboxWrites to be enabled.");
        }

        this._cancellationToken.ThrowIfCancellationRequested();
        // TaskEntity hydrates State with the backend-owned object. Mutate an independent copy so
        // an exception leaves the hydrated state unchanged.
        bool migrateLegacy = this._options.EnableMailboxWrites &&
            this.State.SchemaVersion != DurableAgentState.RevisedSchemaVersion &&
            this._options.AuthorizeLegacyMigration?.Invoke(this.State) == true;
        DurableAgentState workingState = migrateLegacy
            ? DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(this.State, hasAuthoritativeLegacyHistory: true)
            : this.State.Clone();
        if (this._options.EnableMailboxWrites &&
            workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
        {
            workingState.MailboxWritesAuthorized = true;
        }

        workingState.Data.ConversationHistory.Add(
            workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion
                ? DurableAgentStateRequest.FromRunRequestV2(request, logger)
                : DurableAgentStateRequest.FromRunRequest(request, logger));
        AIAgent agent = this.GetAgent(sessionId);
        EntityAgentWrapper agentWrapper = new(agent, this.Context, request, this._services);

        foreach (ChatMessage msg in request.Messages)
        {
            logger.LogAgentRequest(sessionId, msg.Role, msg.Text);
        }

        // Set the current agent context for the duration of the agent run. This will be exposed
        // to any tools that are invoked by the agent.
        DurableAgentContext agentContext = new(
            entityContext: this.Context,
            client: this._client,
            lifetime: this._services.GetRequiredService<IHostApplicationLifetime>(),
            services: this._services);
        DurableAgentContext.SetCurrent(agentContext);

        try
        {
            // Start the agent response stream
            IAsyncEnumerable<AgentResponseUpdate> responseStream = agentWrapper.RunStreamingAsync(
                workingState.Data.ConversationHistory.SelectMany(e => e.Messages).Select(
                    message => workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion
                        ? message.ToChatMessageV2()
                        : message.ToChatMessage()),
                await agentWrapper.CreateSessionAsync(this._cancellationToken).ConfigureAwait(false),
                options: null,
                this._cancellationToken);

#pragma warning disable MEAI001 // Preserve the continuation token omitted by response stream aggregation.
            ResponseContinuationToken? continuationToken = null;
            async IAsyncEnumerable<AgentResponseUpdate> CaptureResponseMetadataAsync()
            {
                await foreach (AgentResponseUpdate update in responseStream)
                {
                    continuationToken = update.ContinuationToken ?? continuationToken;
                    yield return update;
                }
            }
#pragma warning restore MEAI001

            AgentResponse response;
            if (this._messageHandler is null)
            {
                // If no message handler is provided, we can just get the full response at once.
                // This is expected to be the common case for non-interactive agents.
                response = await CaptureResponseMetadataAsync().ToAgentResponseAsync(this._cancellationToken);
            }
            else
            {
                List<AgentResponseUpdate> responseUpdates = [];
                bool streamCompleted = false;

                // To support interactive chat agents, we need to stream the responses to an IAgentMessageHandler.
                // The user-provided message handler can be implemented to send the responses to the user.
                // We assume that only non-empty text updates are useful for the user.
                async IAsyncEnumerable<AgentResponseUpdate> StreamResultsAsync()
                {
                    await foreach (AgentResponseUpdate update in CaptureResponseMetadataAsync())
                    {
                        // We need the full response further down, so we piece it together as we go.
                        responseUpdates.Add(update);

                        // Yield the update to the message handler.
                        yield return update;
                    }

                    streamCompleted = true;
                }

                await this._messageHandler.OnStreamingResponseUpdateAsync(StreamResultsAsync(), this._cancellationToken);
                if (!streamCompleted)
                {
                    throw new InvalidOperationException(
                        "The agent response handler must consume the complete response stream before the run can commit.");
                }

                response = responseUpdates.ToAgentResponse();
            }

#pragma warning disable MEAI001 // Preserve the caller-visible token as well as the mailbox snapshot.
            response.ContinuationToken = continuationToken;
#pragma warning restore MEAI001

            // Persist the agent response to the entity state for client polling
            DurableAgentStateResponse storedResponse =
                workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion
                    ? DurableAgentStateResponse.FromResponseV2(correlationId, response, logger)
                    : DurableAgentStateResponse.FromResponse(correlationId, response, logger);
            workingState.Data.ConversationHistory.Add(storedResponse);
            if (workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
            {
                DateTimeOffset completedAt = this._timeProvider.GetUtcNow();
                DurableAgentStateOutcomeResolver.AddSuccessfulResult(
                    workingState,
                    correlationId,
                    response,
                    completedAt,
                    this._options.ResultRetentionPeriod is TimeSpan retention ? completedAt.Add(retention) : null,
                    logger: logger);
                DurableAgentJsonUtilities.CaptureRetainedResult(
                    response, workingState.Data.TerminalResults![correlationId].Response!);
            }
            else
            {
                DurableAgentJsonUtilities.CaptureRetainedLegacyResult(response, storedResponse);
            }

            string responseText = response.Text;

            if (!string.IsNullOrEmpty(responseText))
            {
                logger.LogAgentResponse(
                    sessionId,
                    response.Messages.FirstOrDefault()?.Role ?? ChatRole.Assistant,
                    responseText,
                    response.Usage?.InputTokenCount,
                    response.Usage?.OutputTokenCount,
                    response.Usage?.TotalTokenCount);
            }

            DateTime? deletionCheckExpiration = this.UpdateExpiration(workingState, sessionId, logger);
            this._cancellationToken.ThrowIfCancellationRequested();
            this.CommitWorkingState(workingState, sessionId, logger, deletionCheckExpiration);

            return response;
        }
        catch (Exception exception)
        {
            logger.LogDurableAgentExecutionFailed(exception, sessionId);
            throw;
        }
        finally
        {
            // Clear the current agent context
            DurableAgentContext.ClearCurrent();
        }
    }

    /// <summary>
    /// Removes due result payloads while retaining completion receipts, then schedules the next check.
    /// </summary>
    /// <remarks>
    /// Also callable as an explicit entity operation to recover imported states with no scheduled signal.
    /// Signals carry no deletion authority: every turn rechecks the current generation and clock.
    /// </remarks>
    public void CheckAndExpireResults(AgentEntityResultExpirationCheck? scheduledCheck = null)
    {
        AgentSessionId sessionId = this.Context.Id;
        ILogger logger = this.GetLogger(sessionId.Name, sessionId.Key);
        try
        {
            this._cancellationToken.ThrowIfCancellationRequested();
            _ = DurableAgentStateSchemaVersion.ParseSupported(this.State.SchemaVersion);
            if (IsEmptyInitializedState(this.State))
            {
                // A late signal must not resurrect an entity deleted in the meantime.
                this.State = null!;
                return;
            }

            if (this.State.SchemaVersion != DurableAgentState.RevisedSchemaVersion)
            {
                ValidateForCommit(this.State);
                return;
            }

            if (!this._options.EnableMailboxWrites)
            {
                throw new InvalidOperationException("Result expiration requires EnableMailboxWrites to be enabled.");
            }

            // Unlike a new run, cleanup must not assign history identities or promote legacy state.
            DurableAgentState workingState = DurableAgentStateJsonConverter.DeserializeRevisedContract(
                DurableAgentStateJsonConverter.SerializeRevisedContract(this.State));
            workingState.MailboxWritesAuthorized = true;
            this.CommitWorkingState(workingState, sessionId, logger, deletionCheckExpiration: null,
                previousResultCheckTime: scheduledCheck?.ScheduledTime);
        }
        catch (Exception exception)
        {
            logger.LogDurableAgentExecutionFailed(exception, sessionId);
            throw;
        }
    }

    /// <summary>
    /// Checks if the entity has expired and deletes it if so, otherwise reschedules the deletion check.
    /// </summary>
    /// <remarks>
    /// This method is called by the durable task runtime when a <c>CheckAndDeleteIfExpired</c> signal is received.
    /// </remarks>
    public void CheckAndDeleteIfExpired(AgentEntityDeletionCheck? scheduledCheck = null)
    {
        AgentSessionId sessionId = this.Context.Id;
        ILogger logger = this.GetLogger(sessionId.Name, sessionId.Key);

        DateTime currentTime = this._timeProvider.GetUtcNow().UtcDateTime;
        DateTime? expirationTime = this.State.Data.ExpirationTimeUtc;

        logger.LogTTLDeletionCheck(sessionId, expirationTime, currentTime);

        // A delayed signal can outlive a deleted entity. TaskEntity initializes missing state
        // before dispatch, so remove that otherwise-empty placeholder instead of recreating it.
        if (!expirationTime.HasValue && IsEmptyInitializedState(this.State))
        {
            this.State = null!;
            return;
        }

        if (this.State.SchemaVersion == DurableAgentState.RevisedSchemaVersion &&
            !this._options.EnableMailboxEntityDeletion)
        {
            // A legacy deadline is not authorization to erase completion evidence.
            return;
        }

        if (!this._options.ContainsAgent(sessionId.Name) ||
            !this._options.GetTimeToLive(
                sessionId.Name, this.State.SchemaVersion == DurableAgentState.RevisedSchemaVersion).HasValue)
        {
            if (expirationTime.HasValue)
            {
                logger.LogTTLExpirationTimeCleared(sessionId);
                DurableAgentState workingState = this.State.Clone();
                workingState.Data.ExpirationTimeUtc = null;
                ValidateForCommit(workingState);
                this.State = workingState;
            }

            return;
        }

        if (!expirationTime.HasValue)
        {
            return;
        }

        if (currentTime >= expirationTime.Value)
        {
            logger.LogTTLEntityExpired(sessionId, expirationTime.Value);
            this.State = null!;
            return;
        }

        // A shorter TTL creates an earlier signal. Its older, later counterpart is stale.
        if (scheduledCheck is null ||
            scheduledCheck.ExpectedExpirationTimeUtc <= expirationTime.Value)
        {
            this.ScheduleDeletionCheck(sessionId, logger, expirationTime.Value);
        }
    }

    private static bool IsEmptyInitializedState(DurableAgentState state)
    {
        return state.Data.ConversationHistory.Count == 0 &&
            state.Data.TerminalResults is null &&
            state.Data.CompletionReceipts is null &&
            state.Data.HistoryBinding.ValueKind == JsonValueKind.Undefined &&
            state.Data.Session is null &&
            state.Data.IngestedPositions is null &&
            state.Data.Truncation is null &&
            state.Data.ExpirationTimeUtc is null &&
            state.Data.ExtensionData is null &&
            state.Data.UnknownProperties is null &&
            state.ExtensionData is null &&
            state.UnknownProperties is null;
    }

    private void ScheduleDeletionCheck(
        AgentSessionId sessionId,
        ILogger logger,
        DateTime expirationTime)
    {
        DateTime currentTime = this._timeProvider.GetUtcNow().UtcDateTime;
        TimeSpan minimumDelay = this._options.MinimumTimeToLiveSignalDelay;

        // To avoid excessive scheduling, we schedule the deletion check for no less than the minimum delay.
        DateTime scheduledTime = expirationTime > currentTime.Add(minimumDelay)
            ? expirationTime
            : currentTime.Add(minimumDelay);

        logger.LogTTLDeletionScheduled(sessionId, scheduledTime);

        // Schedule a signal to self to check for expiration
        this.Context.SignalEntity(
            this.Context.Id,
            nameof(CheckAndDeleteIfExpired), // self-signal
            new AgentEntityDeletionCheck(expirationTime),
            options: new SignalEntityOptions { SignalTime = scheduledTime });
    }

    private DateTime? UpdateExpiration(
        DurableAgentState workingState,
        AgentSessionId sessionId,
        ILogger logger)
    {
        TimeSpan? timeToLive = this._options.GetTimeToLive(
            sessionId.Name, workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion);
        DateTime? previousExpirationTime = workingState.Data.ExpirationTimeUtc;
        if (!timeToLive.HasValue)
        {
            if (previousExpirationTime.HasValue)
            {
                logger.LogTTLExpirationTimeCleared(sessionId);
                workingState.Data.ExpirationTimeUtc = null;
            }

            return null;
        }

        DateTime newExpirationTime =
            this._timeProvider.GetUtcNow().UtcDateTime.Add(timeToLive.Value);
        workingState.Data.ExpirationTimeUtc = newExpirationTime;
        logger.LogTTLExpirationTimeUpdated(sessionId, newExpirationTime);

        // The first turn starts one delayed-check chain. An extension is picked up by the
        // existing signal; only a shortened expiration needs a new earlier signal.
        return !previousExpirationTime.HasValue ||
            newExpirationTime < previousExpirationTime.Value
                ? newExpirationTime
                : null;
    }

    private void CommitWorkingState(
        DurableAgentState workingState,
        AgentSessionId sessionId,
        ILogger logger,
        DateTime? deletionCheckExpiration,
        DateTimeOffset? previousResultCheckTime = null)
    {
        DateTimeOffset currentTime = this._timeProvider.GetUtcNow();
        DateTimeOffset? nextResultExpiration = null;
        if (workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
        {
            foreach (DurableAgentStateTerminalResult result in workingState.Data.TerminalResults!.Values.ToArray())
            {
                if (result.ResultExpiresAt is DateTimeOffset expiresAt)
                {
                    if (expiresAt <= currentTime)
                    {
                        DurableAgentStateOutcomeResolver.MarkExpiredResultUnavailable(
                            workingState, result.CorrelationId, currentTime);
                    }
                    else if (nextResultExpiration is null || expiresAt < nextResultExpiration)
                    {
                        nextResultExpiration = expiresAt;
                    }
                }
            }
        }

        this._cancellationToken.ThrowIfCancellationRequested();
        ValidateForCommit(workingState);
        if (deletionCheckExpiration.HasValue)
        {
            // this.State still points at the hydrated state until the final assignment.
            this.ScheduleDeletionCheck(sessionId, logger, deletionCheckExpiration.Value);
        }

        if (nextResultExpiration is DateTimeOffset resultExpiration)
        {
            // A delayed/early signal and a backward-moving worker clock must not repeatedly
            // schedule the same timestamp. The prior schedule is a lower bound, never expiry authority.
            DateTimeOffset schedulingBase = previousResultCheckTime > currentTime
                ? previousResultCheckTime.Value
                : currentTime;
            DateTimeOffset minimumScheduledTime = schedulingBase.Add(s_minimumResultExpirationSignalDelay);
            DateTimeOffset scheduledTime = resultExpiration > minimumScheduledTime ? resultExpiration : minimumScheduledTime;
            this.Context.SignalEntity(
                this.Context.Id,
                nameof(CheckAndExpireResults),
                new AgentEntityResultExpirationCheck(scheduledTime),
                options: new SignalEntityOptions { SignalTime = scheduledTime });
        }

        this._cancellationToken.ThrowIfCancellationRequested();
        // This setter performs no synchronous backend I/O. TaskEntity persists the replacement
        // and the self-signal outbox only after the operation completes successfully.
        this.State = workingState;
    }

    private static void ValidateForCommit(DurableAgentState state)
    {
        // Validate serialization before publishing the replacement. Backend commit remains the
        // durable runtime's atomic boundary; external tool/provider writes are outside it.
        _ = JsonSerializer.SerializeToUtf8Bytes(state, DurableAgentStateJsonContext.Default.DurableAgentState);
    }

    private AIAgent GetAgent(AgentSessionId sessionId)
    {
        IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>> agents =
            this._services.GetRequiredService<IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>>>();
        if (!agents.TryGetValue(sessionId.Name, out Func<IServiceProvider, AIAgent>? agentFactory))
        {
            throw new InvalidOperationException($"Agent '{sessionId.Name}' not found");
        }

        return agentFactory(this._services);
    }

    private ILogger GetLogger(string agentName, string sessionKey)
    {
        return this._loggerFactory.CreateLogger($"Microsoft.DurableTask.Agents.{agentName}.{sessionKey}");
    }
}

internal sealed record AgentEntityDeletionCheck(DateTime ExpectedExpirationTimeUtc);

internal sealed record AgentEntityResultExpirationCheck(DateTimeOffset ScheduledTime);
