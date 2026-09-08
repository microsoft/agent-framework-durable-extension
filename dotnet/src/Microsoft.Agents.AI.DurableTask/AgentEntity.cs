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

internal class AgentEntity(IServiceProvider services, CancellationToken cancellationToken = default) : TaskEntity<DurableAgentState>, ITaskEntity
{
    private const string HistoryProviderConflictMessage =
        "Only ConversationId or ChatHistoryProvider may be used, but not both. " +
        "The service returned a conversation id indicating server-side chat history management, " +
        "but the agent has a ChatHistoryProvider configured.";
    private const string MissingServiceConversationIdMessage =
        "Service did not return a valid conversation id when using an AgentSession with service managed chat history.";
    private static readonly TimeSpan s_minimumResultExpirationSignalDelay = TimeSpan.FromMinutes(1);
    private readonly IServiceProvider _services = services;
    private readonly DurableTaskClient _client = services.GetRequiredService<DurableTaskClient>();
    private readonly ILoggerFactory _loggerFactory = services.GetRequiredService<ILoggerFactory>();
    private readonly IAgentResponseHandler? _messageHandler = services.GetService<IAgentResponseHandler>();
    private readonly DurableAgentsOptions _options = services.GetRequiredService<DurableAgentsOptions>();
    // Entity operations rehydrate and execute once rather than replaying like orchestrations, and
    // TaskEntityContext has no deterministic clock. Use wall-clock UTC through an injectable source.
    private readonly TimeProvider _timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;
    private readonly CancellationToken _cancellationToken = cancellationToken != default
        ? cancellationToken
        : services.GetService<IHostApplicationLifetime>()?.ApplicationStopping ?? CancellationToken.None;

    ValueTask<object?> ITaskEntity.RunAsync(TaskEntityOperation operation)
    {
        if (string.Equals(operation.Name, nameof(CheckAndExpireResults), StringComparison.OrdinalIgnoreCase) &&
            operation.HasInput)
        {
            this._cancellationToken.ThrowIfCancellationRequested();
            AgentEntityResultExpirationCheck? check =
                (AgentEntityResultExpirationCheck?)operation.GetInput(typeof(AgentEntityResultExpirationCheck));
            // TaskEntity writes State back on every successful dispatch. Bypass it for stale
            // signals so a duplicate cannot even rewrite state or initialize a missing entity.
            DurableAgentState? state = (DurableAgentState?)operation.State.GetState(typeof(DurableAgentState));
            if (state is null)
            {
                return new ValueTask<object?>((object?)null);
            }

            _ = DurableAgentStateSchemaVersion.ParseSupported(state.SchemaVersion);
            if (state.SchemaVersion != DurableAgentState.RevisedSchemaVersion)
            {
                // Preserve legacy validation without dispatching a successful void operation,
                // which would call the SDK state setter even though cleanup has no work to do.
                ValidateForCommit(state);
                return new ValueTask<object?>((object?)null);
            }

            AgentEntityResultExpirySchedule? schedule =
                AgentEntityResultExpirySchedule.Read(state, operation.Context.Id.ToString());
            if (schedule?.Pending is null || schedule.Pending != check)
            {
                return new ValueTask<object?>((object?)null);
            }
        }

        return this.RunAsync(operation);
    }

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
        // Logger category is Microsoft.DurableTask.Agents.{registeredAgentName}.{sessionId}
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
            // A future/invalid runtime profile cannot be silently replaced after invoking the model.
            _ = AgentEntityResultExpirySchedule.Read(workingState, this.Context.Id.ToString());
        }

        bool isLegacyState =
            this.State.SchemaVersion != DurableAgentState.RevisedSchemaVersion;
        DurableAgentStateHistoryBinding? persistedHistoryBinding =
            DurableAgentHistoryBinding.Parse(this.State.Data.HistoryBinding);
        DurableAgentHistoryBinding.ValidateMarkedProfile(
            this.State.Data.HistoryBinding,
            persistedHistoryBinding);
        DurableAgentStateHistoryBinding? existingHistoryBinding =
            DurableAgentHistoryBinding.IsSealedByCSharp(persistedHistoryBinding)
                ? persistedHistoryBinding
                : null;
        string? configuredHistoryProviderKey =
            this._options.GetHistoryProviderKey(sessionId.Name) ??
            persistedHistoryBinding?.ProviderKey;
        DurableAgentHistoryBinding.ValidateContinuationPresence(
            existingHistoryBinding,
            this.State.Data.Session);
        DurableAgentHistoryBinding.ValidateConfiguredKey(
            existingHistoryBinding,
            configuredHistoryProviderKey);
        if (workingState.SchemaVersion != DurableAgentState.RevisedSchemaVersion)
        {
            workingState.Data.ConversationHistory.Add(
                DurableAgentStateRequest.FromRunRequest(request, logger));
        }

        AIAgent agent = this.GetAgent(sessionId);
        bool serviceManagedPerServiceCallHistory =
            this._options.IsServiceManagedPerServiceCallHistory(sessionId.Name);
        ValidatedDurableAgentHistoryConfiguration validatedHistoryConfiguration =
            DurableAgentHistoryOwnershipResolver.ValidateRunConfiguration(
                agent,
                serviceManagedPerServiceCallHistory);
        DurableAgentHistoryReplayMode historyReplayMode =
            this._options.GetHistoryReplayMode(sessionId.Name);

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
            AgentSession session = await DurableAgentSessionState.RestoreAsync(
                agent,
                workingState.Data.Session,
                this._cancellationToken).ConfigureAwait(false);
            (DurableAgentHistoryOwnership ownership, ChatClientAgent? chatClientAgent) =
                DurableAgentHistoryOwnershipResolver.Resolve(
                    session,
                    validatedHistoryConfiguration);
            DurableAgentHistoryOwnership effectiveOwnership =
                DurableAgentHistoryOwnershipResolver.GetEffectiveOwnership(
                    ownership,
                    historyReplayMode);
            DurableAgentHistoryBinding.ValidatePreExecutionContinuationContract(
                effectiveOwnership,
                session,
                chatClientAgent,
                validatedHistoryConfiguration.RequiresPerServiceCallPersistence);
            if (effectiveOwnership != DurableAgentHistoryOwnership.Entity &&
                this.State.Data.HistoryBinding.ValueKind != JsonValueKind.Undefined &&
                persistedHistoryBinding is null)
            {
                throw new DurableAgentHistoryBindingMismatchException(
                    "The durable session contains an opaque shared historyBinding that the C# runtime " +
                    $"cannot use to prove {effectiveOwnership} ownership. Preserve that state with its " +
                    "originating runtime or start a new C# durable session with an explicit logical provider key.");
            }

            if (effectiveOwnership != DurableAgentHistoryOwnership.Entity &&
                workingState.SchemaVersion != DurableAgentState.RevisedSchemaVersion)
            {
                throw new InvalidOperationException(
                    "External, service, and opaque agent-session history require schema 2 mailbox writes. " +
                    "Enable mailbox writes and authorize any legacy migration before continuing this durable session.");
            }

            DurableAgentStateHistoryBinding expectedHistoryBinding =
                DurableAgentHistoryBinding.Create(
                    effectiveOwnership,
                    configuredHistoryProviderKey);
            if (existingHistoryBinding is null)
            {
                DurableAgentHistoryBinding.ValidateLegacyAdoption(
                    this.State,
                    effectiveOwnership,
                    session,
                    chatClientAgent,
                    validatedHistoryConfiguration.RequiresPerServiceCallPersistence);
            }
            DurableAgentHistoryBinding.ValidateExisting(
                existingHistoryBinding,
                expectedHistoryBinding);
            if (existingHistoryBinding is not null)
            {
                DurableAgentHistoryBinding.ValidateBoundContinuation(
                    effectiveOwnership,
                    session,
                    chatClientAgent);
            }
            bool entityOwnedHistory =
                effectiveOwnership == DurableAgentHistoryOwnership.Entity;

            // The provider is bound per invocation because it needs this operation's working state and
            // correlation ID. A registration-time provider cannot safely bind either value.
            DurableChatHistoryProvider? durableHistoryProvider =
                entityOwnedHistory &&
                workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion
                ? new(
                    workingState.Data.ConversationHistory,
                    request,
                    workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion,
                    logger)
                : null;
            EntityAgentWrapper agentWrapper = new(
                agent,
                this.Context,
                request,
                this._services,
                durableHistoryProvider);

            IEnumerable<ChatMessage> inputMessages = BuildAgentInputMessages(
                workingState,
                request,
                effectiveOwnership,
                chatClientAgent is not null &&
                    (durableHistoryProvider is not null || !entityOwnedHistory),
                historyReplayMode,
                workingState.SchemaVersion != DurableAgentState.RevisedSchemaVersion);

            // Start the agent response stream
            IAsyncEnumerable<AgentResponseUpdate> responseStream = agentWrapper.RunStreamingAsync(
                inputMessages,
                session,
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

            (DurableAgentHistoryOwnership finalOwnership, _) =
                DurableAgentHistoryOwnershipResolver.Resolve(
                    session,
                    validatedHistoryConfiguration);
            finalOwnership = DurableAgentHistoryOwnershipResolver.GetEffectiveOwnership(
                finalOwnership,
                historyReplayMode);
            bool remoteServiceTransition =
                finalOwnership != effectiveOwnership &&
                finalOwnership == DurableAgentHistoryOwnership.Service;
            if (finalOwnership != DurableAgentHistoryOwnership.Entity &&
                this.State.Data.HistoryBinding.ValueKind != JsonValueKind.Undefined &&
                persistedHistoryBinding is null)
            {
                throw new DurableAgentHistoryBindingMismatchException(
                    "The completed call resolved non-entity history ownership, but the durable session " +
                    "contains an opaque shared historyBinding that the C# runtime cannot seal or resume. " +
                    "Durable state was not committed. The remote service may already have observed the call; " +
                    "preserve the state with its originating runtime or start a new C# durable session.");
            }

            DurableAgentStateHistoryBinding finalHistoryBinding =
                DurableAgentHistoryBinding.Create(
                    finalOwnership,
                    configuredHistoryProviderKey,
                    remoteServiceTransition);
            if (existingHistoryBinding is null)
            {
                DurableAgentHistoryBinding.ValidateLegacyTransition(
                    this.State,
                    effectiveOwnership,
                    finalOwnership,
                    remoteServiceTransition);
            }

            DurableAgentHistoryBinding.ValidateExisting(
                existingHistoryBinding,
                finalHistoryBinding,
                remoteTransitionDetectedAfterExecution: remoteServiceTransition);
            DurableAgentHistoryBinding.ValidateBoundContinuation(
                finalOwnership,
                session,
                chatClientAgent,
                remoteServiceTransition);

            FinalizeConversationEntries(
                workingState,
                request,
                response,
                finalOwnership,
                durableHistoryProvider,
                logger);

            workingState.Data.Session = await SerializeSessionWithoutDuplicateHistoryAsync(
                agent,
                session,
                chatClientAgent,
                finalOwnership,
                this._cancellationToken).ConfigureAwait(false);
            if (workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
            {
                if (existingHistoryBinding is not null ||
                    persistedHistoryBinding is not null ||
                    this.State.Data.HistoryBinding.ValueKind == JsonValueKind.Undefined)
                {
                    DurableAgentStateHistoryBinding bindingToSeal =
                        existingHistoryBinding ??
                        DurableAgentHistoryBinding.MergeProvisionalMetadata(
                            finalHistoryBinding,
                            persistedHistoryBinding);
                    workingState = DurableAgentHistoryBinding.Seal(
                        workingState,
                        bindingToSeal);
                }

                workingState.MailboxWritesAuthorized = true;
            }

            DurableAgentStateResponse? storedResponse =
                finalOwnership == DurableAgentHistoryOwnership.Entity
                    ? workingState.Data.ConversationHistory
                        .OfType<DurableAgentStateResponse>()
                        .LastOrDefault(entry => entry.CorrelationId == correlationId)
                    : null;
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
            else if (storedResponse is not null)
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
        catch (InvalidOperationException exception) when (
            IsPostResponseServiceHistoryFailure(
                exception,
                validatedHistoryConfiguration.ChatClientAgent))
        {
            DurableAgentHistoryBindingMismatchException bindingException = new(
                "Agent Framework rejected the completed call while updating service history ownership. " +
                exception.Message +
                " The remote service may already have observed the rejected call, but durable state was not committed.",
                exception);
            logger.LogDurableAgentExecutionFailed(bindingException, sessionId);
            throw bindingException;
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

            AgentEntityResultExpirySchedule? schedule =
                AgentEntityResultExpirySchedule.Read(this.State, this.Context.Id.ToString());
            if (scheduledCheck is not null && (schedule?.Pending is null || schedule.Pending != scheduledCheck))
            {
                // Includes old timestamp-only signals, duplicate deliveries and deleted generations.
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
        // before dispatch, so delete that otherwise-empty placeholder instead of recreating it.
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
            // Configuration can change while a durable delayed signal is outstanding.
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

        // Later interactions normally extend expiration and let the earlier signal move the chain
        // forward. A shorter TTL schedules an earlier signal; its older, later counterpart is stale.
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

    private static bool IsPostResponseServiceHistoryFailure(
        InvalidOperationException exception,
        ChatClientAgent? chatClientAgent)
    {
        if (chatClientAgent is null)
        {
            return false;
        }

        return string.Equals(
                exception.Message,
                MissingServiceConversationIdMessage,
                StringComparison.Ordinal) ||
            (chatClientAgent.ChatHistoryProvider is not null &&
                string.Equals(
                    exception.Message,
                    HistoryProviderConflictMessage,
                    StringComparison.Ordinal));
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

    private static IEnumerable<ChatMessage> BuildAgentInputMessages(
        DurableAgentState workingState,
        RunRequest request,
        DurableAgentHistoryOwnership ownership,
        bool contextPipelineSuppliesHistory,
        DurableAgentHistoryReplayMode historyReplayMode,
        bool isLegacyState)
    {
        if (contextPipelineSuppliesHistory ||
            ownership == DurableAgentHistoryOwnership.AgentSession ||
            historyReplayMode == DurableAgentHistoryReplayMode.CurrentRequestOnly)
        {
            // A MAF history/context pipeline or a server-owned opaque session supplies prior context.
            // Passing stored history here as well would duplicate messages.
            return request.Messages;
        }

        if (isLegacyState)
        {
            return workingState.Data.ConversationHistory
                .SelectMany(entry => entry.Messages)
                .Select(message => message.ToChatMessage());
        }

        // Generic AIAgents have no discoverable context pipeline. In the backward-compatible preload
        // mode, the entity manually replays prior durable history before the current request.
        return DurableAgentStateReplay.GetMessages(
                workingState.Data.ConversationHistory,
                request.CorrelationId)
            .Concat(request.Messages);
    }

    private static void FinalizeConversationEntries(
        DurableAgentState workingState,
        RunRequest request,
        AgentResponse response,
        DurableAgentHistoryOwnership ownership,
        DurableChatHistoryProvider? durableHistoryProvider,
        ILogger logger)
    {
        if (ownership != DurableAgentHistoryOwnership.Entity)
        {
            // Delivery is recorded in the schema 2 mailbox. Provider-, service-, and opaque
            // agent-session owners keep their transcript outside conversationHistory.
            return;
        }

        if (durableHistoryProvider?.HasStagedTurn is true)
        {
            // Provider callbacks already staged the entity-owned request and response. Replace only
            // the staged response so aggregate usage and response metadata are retained once.
            durableHistoryProvider.CompleteStagedResponse(response);
            return;
        }

        if (workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
        {
            workingState.Data.ConversationHistory.Add(
                DurableAgentStateRequest.FromRunRequestV2(request, logger));
        }

        workingState.Data.ConversationHistory.Add(
            workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion
                ? DurableAgentStateResponse.FromResponseV2(request.CorrelationId, response, logger)
                : DurableAgentStateResponse.FromResponse(request.CorrelationId, response, logger));
    }

    private static ValueTask<JsonElement> SerializeSessionWithoutDuplicateHistoryAsync(
        AIAgent agent,
        AgentSession session,
        ChatClientAgent? chatClientAgent,
        DurableAgentHistoryOwnership ownership,
        CancellationToken cancellationToken)
    {
        // InMemoryChatHistoryProvider state can contain a full transcript already retained by the
        // entity. Exclude only that provider's declared keys; custom, compaction, and opaque
        // server-session state remains authoritative and is preserved.
        IEnumerable<string> excludedStateKeys =
            chatClientAgent?.ChatHistoryProvider is InMemoryChatHistoryProvider inMemoryHistoryProvider &&
            ownership is DurableAgentHistoryOwnership.Entity or DurableAgentHistoryOwnership.Service
                ? inMemoryHistoryProvider.StateKeys
                : [];

        return DurableAgentSessionState.SerializeAsync(
            agent,
            session,
            excludedStateKeys,
            cancellationToken);
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

        // The first turn starts one delayed-check chain. Extended expirations are picked up by the
        // earlier check; only a shortened expiration needs a new earlier signal.
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
        AgentEntityResultExpirationCheck? nextSignal = null;
        if (workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
        {
            string entityId = this.Context.Id.ToString();
            AgentEntityResultExpirySchedule? schedule = AgentEntityResultExpirySchedule.Read(workingState, entityId);
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

            AgentEntityResultExpirationCheck? pending = schedule?.Pending;
            // Consuming a matching signal rotates its token even if the clock has moved backwards.
            // An explicit recovery or successful new run also replaces an overdue/stuck schedule.
            if (previousResultCheckTime.HasValue || pending?.ScheduledTime <= currentTime)
            {
                pending = null;
            }

            if (nextResultExpiration is DateTimeOffset resultExpiration)
            {
                DateTimeOffset schedulingBase = previousResultCheckTime > currentTime
                    ? previousResultCheckTime.Value
                    : currentTime;
                DateTimeOffset minimumScheduledTime = schedulingBase.Add(s_minimumResultExpirationSignalDelay);
                DateTimeOffset scheduledTime = resultExpiration > minimumScheduledTime ? resultExpiration : minimumScheduledTime;
                if (pending is null || pending.ScheduledTime > scheduledTime)
                {
                    pending = nextSignal = new AgentEntityResultExpirationCheck(
                        scheduledTime.ToUniversalTime(), Guid.NewGuid().ToString("N"), entityId);
                }
            }
            else
            {
                pending = null;
            }

            workingState = AgentEntityResultExpirySchedule.Write(workingState, entityId, schedule, pending);
        }

        this._cancellationToken.ThrowIfCancellationRequested();
        ValidateForCommit(workingState);
        if (deletionCheckExpiration.HasValue)
        {
            // Pass the working-copy value explicitly: this.State still refers to the original state
            // until the operation commits.
            this.ScheduleDeletionCheck(sessionId, logger, deletionCheckExpiration.Value);
        }

        if (nextSignal is not null)
        {
            this.Context.SignalEntity(
                this.Context.Id,
                nameof(CheckAndExpireResults),
                nextSignal,
                options: new SignalEntityOptions { SignalTime = nextSignal.ScheduledTime });
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

internal sealed record AgentEntityResultExpirationCheck(DateTimeOffset ScheduledTime, string? Token = null, string? EntityId = null);
