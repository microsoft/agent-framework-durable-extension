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

internal partial class AgentEntity(IServiceProvider services, CancellationToken cancellationToken = default) : TaskEntity<DurableAgentState>, ITaskEntity
{
    private const string HistoryProviderConflictMessage =
        "Only ConversationId or ChatHistoryProvider may be used, but not both. " +
        "The service returned a conversation id indicating server-side chat history management, " +
        "but the agent has a ChatHistoryProvider configured.";
    private const string MissingServiceConversationIdMessage =
        "Service did not return a valid conversation id when using an AgentSession with service managed chat history.";
    private readonly IServiceProvider _services = services;
    private readonly DurableTaskClient _client = services.GetRequiredService<DurableTaskClient>();
    private readonly ILoggerFactory _loggerFactory = services.GetRequiredService<ILoggerFactory>();
    private readonly IAgentResponseHandler? _messageHandler = services.GetService<IAgentResponseHandler>();
    private readonly IDurableAgentProviderFailureAttestor? _providerFailureAttestor =
        services.GetService<IDurableAgentProviderFailureAttestor>();
    private readonly DurableAgentsOptions _options = services.GetRequiredService<DurableAgentsOptions>();
    // Entity operations rehydrate and execute once rather than replaying like orchestrations, and
    // TaskEntityContext has no deterministic clock. Use wall-clock UTC through an injectable source.
    private readonly TimeProvider _timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;
    private readonly CancellationToken _cancellationToken = cancellationToken != default
        ? cancellationToken
        : services.GetService<IHostApplicationLifetime>()?.ApplicationStopping ?? CancellationToken.None;

    /// <summary>
    /// Rejects stale scheduled result-expiration operations before the standard entity dispatcher can initialize
    /// or rewrite state.
    /// </summary>
    /// <remarks>
    /// The standard <see cref="TaskEntity{TState}.RunAsync"/> dispatcher initializes state before invoking an operation
    /// and writes state after every successful dispatch, including successful void operations. The Durable Task runtime
    /// invokes entities through <see cref="ITaskEntity"/>, so reimplementing that interface gives scheduled-expiration
    /// signals a pre-dispatch guard. A stale delayed signal can therefore return without hydrating, creating, or
    /// rewriting entity state. No-input cleanup calls are explicit recovery requests and intentionally continue through
    /// the standard dispatcher.
    /// </remarks>
    ValueTask<object?> ITaskEntity.RunAsync(TaskEntityOperation operation)
    {
        if (IsScheduledResultExpirationOperation(operation) &&
            !this.ShouldDispatchScheduledResultExpiration(operation))
        {
            return new ValueTask<object?>((object?)null);
        }

        // Explicit interface implementations are only callable through the interface. This resolves to the inherited
        // TaskEntity<DurableAgentState>.RunAsync method and preserves the standard dispatch and persistence behavior.
        return this.RunAsync(operation);
    }

    protected override DurableAgentState InitializeState(TaskEntityOperation entityOperation)
    {
        // Readers accept both state layouts, but this internal rollout gate controls whether this runtime may create
        // a revised mailbox containing authoritative terminal results and completion receipts.
        bool shouldInitializePersistentRequestOutcomes =
            this._options.EnablePersistentRequestOutcomes && IsAgentRunOperation(entityOperation);
        return shouldInitializePersistentRequestOutcomes
            ? CreateEmptyPersistentRequestOutcomeState()
            : base.InitializeState(entityOperation);
    }

    /// <summary>
    /// Identifies operations that are allowed to create a new agent mailbox generation.
    /// </summary>
    /// <remarks>
    /// Matching is case-insensitive to mirror <see cref="TaskEntity{TState}"/> method dispatch; initialization must not
    /// select a different state contract merely because a caller used different operation-name casing.
    /// </remarks>
    private static bool IsAgentRunOperation(TaskEntityOperation operation)
    {
        return string.Equals(operation.Name, nameof(Run), StringComparison.OrdinalIgnoreCase) ||
            string.Equals(operation.Name, nameof(RunAgentAsync), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Creates the initial revised state used to persist request outcomes.
    /// </summary>
    /// <remarks>
    /// Callers must first verify the internal writer rollout gate. Initializing both maps establishes the invariant
    /// that terminal results and completion receipts are authoritative for every committed revised-mailbox outcome.
    /// </remarks>
    private static DurableAgentState CreateEmptyPersistentRequestOutcomeState()
    {
        return new DurableAgentState
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(StringComparer.Ordinal),
                CompletionReceipts =
                    new Dictionary<string, DurableAgentStateCompletionReceipt>(StringComparer.Ordinal),
            },
        };
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
        DurableAgentRetentionSettings retentionSettings =
            this._options.GetRetentionSettings();

        string correlationId = request.CorrelationId;
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            throw new ArgumentException(
                "A non-empty correlation ID is required to run a durable agent request.",
                nameof(request));
        }

        DateTimeOffset currentTime = this._timeProvider.GetUtcNow();

        // Resolve does not assume that this request has run before. Pending means that no
        // committed completion exists for this correlation ID, so this is a new attempt.
        DurableAgentRunOutcome resolvedOutcome;
        try
        {
            resolvedOutcome = DurableAgentStateOutcomeResolver.Resolve(
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

        if (resolvedOutcome.Kind != DurableAgentRunOutcomeKind.Pending)
        {
            // Correlation is the caller's idempotency key. Retained terminal state is reused
            // without comparing request content, so callers must not reuse it for another request.
            // Surface failures before optional migration so failed delivery never writes state.
            AgentResponse committedResponse = resolvedOutcome.GetResponse(correlationId);
            bool legacyMailboxMigrationRequested =
                this._options.EnablePersistentRequestOutcomes &&
                this.State.SchemaVersion != DurableAgentState.RevisedSchemaVersion;
            bool migrationAuthorized =
                legacyMailboxMigrationRequested &&
                this._options.AuthorizeLegacyMigration?.Invoke(this.State) == true;
            if (this._options.HistoryRetentionMode == DurableAgentHistoryRetentionMode.Auto &&
                this.State.SchemaVersion != DurableAgentState.RevisedSchemaVersion &&
                !migrationAuthorized)
            {
                throw new DurableAgentStateCorruptionException(
                    "Automatic history retention requires schema 2 mailbox state. Legacy terminal transcript " +
                    "entries must be converted from independently authoritative complete history before delivery.");
            }
            if (legacyMailboxMigrationRequested &&
                migrationAuthorized &&
                resolvedOutcome.Kind != DurableAgentRunOutcomeKind.CompletedResultUnavailable)
            {
                // Legacy evidence is converted without constructing or invoking the agent.
                DurableAgentState migrated = DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(
                    this.State, hasAuthoritativeLegacyHistory: true);
                this.ApplyRetentionAndCommit(
                    migrated,
                    sessionId,
                    logger,
                    entityDeletionCheckExpiration: null);
            }

            return committedResponse;
        }

        if (request.Messages is not { Count: > 0 })
        {
            throw new ArgumentException(
                "At least one message is required for a new durable agent request.",
                nameof(request));
        }

        if (this._options.EnablePersistentRequestOutcomes)
        {
            DurableAgentStateContract.ValidateIdentifier(
                value: correlationId,
                diagnosticPath: nameof(RunRequest.CorrelationId));
        }

        if (!this._options.EnablePersistentRequestOutcomes &&
            this.State.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
        {
            throw new InvalidOperationException(
                $"New persistent request outcomes require {nameof(DurableAgentsOptions.EnablePersistentRequestOutcomes)} to be enabled.");
        }

        this._cancellationToken.ThrowIfCancellationRequested();
        // TaskEntity hydrates State with the backend-owned object. Mutate an independent copy so
        // an exception leaves the hydrated state unchanged.
        bool migrateLegacy = this._options.EnablePersistentRequestOutcomes &&
            this.State.SchemaVersion != DurableAgentState.RevisedSchemaVersion &&
            this._options.AuthorizeLegacyMigration?.Invoke(this.State) == true;
        if (this._options.HistoryRetentionMode == DurableAgentHistoryRetentionMode.Auto &&
            this.State.SchemaVersion != DurableAgentState.RevisedSchemaVersion &&
            !migrateLegacy)
        {
            throw new DurableAgentStateCorruptionException(
                "Automatic history retention requires schema 2 mailbox state. Legacy terminal transcript " +
                "entries must be converted from independently authoritative complete history before execution.");
        }

        DurableAgentState workingState = migrateLegacy
            ? DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(this.State, hasAuthoritativeLegacyHistory: true)
            : this.State.Clone();
        bool fixedOwnershipContractActive =
            workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion;
        if (this._options.EnablePersistentRequestOutcomes &&
            fixedOwnershipContractActive)
        {
            workingState.PersistentRequestOutcomesAuthorized = true;
            // A future/invalid runtime profile cannot be silently replaced after invoking the model.
            _ = AgentEntityResultExpirySchedule.Read(workingState, this.Context.Id.ToString());
        }

        DurableAgentStateHistoryBinding? persistedHistoryBinding =
            DurableAgentHistoryBinding.Parse(this.State.Data.HistoryBinding);
        DurableAgentHistoryBinding.ValidateMarkedProfile(
            this.State.Data.HistoryBinding,
            persistedHistoryBinding);
        DurableAgentStateHistoryBinding? existingHistoryBinding =
            DurableAgentHistoryBinding.IsSealedByCSharp(persistedHistoryBinding)
                ? persistedHistoryBinding
                : null;
        DurableAgentHistoryConfiguration historyConfiguration =
            this._options.GetHistoryConfiguration(sessionId.Name);
        string? configuredHistoryProviderKey =
            historyConfiguration.ProviderKey?.Value;
        DurableAgentHistoryBinding.ValidateContinuationPresence(
            existingHistoryBinding,
            this.State.Data.Session);
        if (configuredHistoryProviderKey is not null)
        {
            DurableAgentHistoryBinding.ValidateConfiguredKey(
                persistedHistoryBinding,
                configuredHistoryProviderKey);
        }

        configuredHistoryProviderKey ??= persistedHistoryBinding?.ProviderKey;
        if (workingState.SchemaVersion != DurableAgentState.RevisedSchemaVersion)
        {
            workingState.Data.ConversationHistory.Add(
                DurableAgentStateRequest.FromRunRequest(request, logger));
        }

        AIAgent agent = this.GetAgent(sessionId);
        ValidatedDurableAgentHistoryConfiguration validatedHistoryConfiguration =
            DurableAgentHistoryOwnershipResolver.ValidateRunConfiguration(
                agent,
                historyConfiguration.ServiceManagedPerServiceCallHistory,
                fixedOwnershipContractActive);

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

        EntityAgentWrapper? agentWrapper = null;
        AgentSession? providerSession = null;
        ChatClientAgent? invocationChatClientAgent = null;
        DurableChatHistoryProvider? durableHistoryProvider = null;
        DurableAgentHistoryOwnership? invocationOwnership = null;
        bool providerInvocationActive = false;
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
            DurableAgentHistoryOwnership resolvedOwnership =
                DurableAgentHistoryOwnershipResolver.GetEffectiveOwnership(
                    ownership,
                    historyConfiguration.ReplayMode);
            DurableAgentHistoryOwnership effectiveOwnership = fixedOwnershipContractActive
                ? resolvedOwnership
                : DurableAgentHistoryOwnership.Entity;
            invocationChatClientAgent = chatClientAgent;
            invocationOwnership = effectiveOwnership;
            if (fixedOwnershipContractActive)
            {
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
            }

            bool entityOwnedHistory =
                effectiveOwnership == DurableAgentHistoryOwnership.Entity;

            // The provider is bound per invocation because it needs this operation's working state and
            // correlation ID. A registration-time provider cannot safely bind either value.
            durableHistoryProvider =
                entityOwnedHistory &&
                workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion
                ? new(
                    workingState.Data.ConversationHistory,
                    request,
                    workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion,
                    logger)
                : null;
            DurableAgentStateRetention.ValidateProtectedFloor(
                workingState,
                retentionSettings,
                currentTime,
                entityOwnedHistory &&
                    workingState.SchemaVersion == DurableAgentState.RevisedSchemaVersion
                    ? DurableAgentStateRequest.FromRunRequestV2(request, logger)
                    : null);
            agentWrapper = new(
                agent,
                this.Context,
                request,
                this._services,
                durableHistoryProvider);

            // Restored configured provider state, including intentionally empty history, supersedes legacy entity replay.
            bool contextPipelineSuppliesHistory =
                fixedOwnershipContractActive
                    ? chatClientAgent is not null &&
                        (durableHistoryProvider is not null || !entityOwnedHistory)
                    : resolvedOwnership is
                        DurableAgentHistoryOwnership.ExternalProvider or
                        DurableAgentHistoryOwnership.Service ||
                        (ConfiguredProviderOwnsHistory(chatClientAgent, durableHistoryProvider) &&
                            chatClientAgent?.ChatHistoryProvider is InMemoryChatHistoryProvider inMemoryHistoryProvider &&
                            inMemoryHistoryProvider.StateKeys.Any(key =>
                                session.StateBag.TryGetValue(key, out InMemoryChatHistoryProvider.State? providerState) &&
                                providerState is not null));
            IEnumerable<ChatMessage> inputMessages = BuildAgentInputMessages(
                workingState,
                request,
                effectiveOwnership,
                contextPipelineSuppliesHistory,
                workingState.SchemaVersion != DurableAgentState.RevisedSchemaVersion);

            providerSession = session;
            providerInvocationActive = true;

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

            providerInvocationActive = false;
#pragma warning disable MEAI001 // Preserve the caller-visible token as well as the mailbox snapshot.
            response.ContinuationToken = continuationToken;
#pragma warning restore MEAI001

            (workingState, DurableAgentHistoryOwnership finalOwnership) =
                await this.FinalizeHistoryStateAsync(
                    agent,
                    session,
                    chatClientAgent,
                    validatedHistoryConfiguration,
                    historyConfiguration,
                    effectiveOwnership,
                    fixedOwnershipContractActive,
                    persistedHistoryBinding,
                    existingHistoryBinding,
                    configuredHistoryProviderKey,
                    workingState,
                    request,
                    response,
                    durableHistoryProvider,
                    logger).ConfigureAwait(false);

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
                    CalculateResultExpiration(completedAt, this._options.ResultRetentionPeriod),
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

            DateTime? entityDeletionCheckExpiration =
                this.UpdateEntityExpiration(workingState, sessionId, logger);
            this.ApplyRetentionAndCommit(
                workingState,
                sessionId,
                logger,
                entityDeletionCheckExpiration);
            return response;
        }
        catch (InvalidOperationException exception) when (
            fixedOwnershipContractActive &&
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
            if (providerInvocationActive &&
                providerSession is not null &&
                invocationOwnership.HasValue)
            {
                AgentResponse? committedFailure = await this.TryFinalizeProviderFailureAsync(
                    exception,
                    agent,
                    providerSession,
                    invocationChatClientAgent,
                    validatedHistoryConfiguration,
                    historyConfiguration,
                    invocationOwnership.Value,
                    persistedHistoryBinding,
                    existingHistoryBinding,
                    configuredHistoryProviderKey,
                    durableHistoryProvider,
                    workingState,
                    request,
                    correlationId,
                    sessionId,
                    logger).ConfigureAwait(false);
                if (committedFailure is not null)
                {
                    return committedFailure;
                }
            }

            throw;
        }
        finally
        {
            // Clear the current agent context
            DurableAgentContext.ClearCurrent();
        }
    }

    private async Task<(DurableAgentState State, DurableAgentHistoryOwnership Ownership)>
        FinalizeHistoryStateAsync(
            AIAgent agent,
            AgentSession session,
            ChatClientAgent? chatClientAgent,
            ValidatedDurableAgentHistoryConfiguration validatedHistoryConfiguration,
            DurableAgentHistoryConfiguration historyConfiguration,
            DurableAgentHistoryOwnership effectiveOwnership,
            bool fixedOwnershipContractActive,
            DurableAgentStateHistoryBinding? persistedHistoryBinding,
            DurableAgentStateHistoryBinding? existingHistoryBinding,
            string? configuredHistoryProviderKey,
            DurableAgentState workingState,
            RunRequest request,
            AgentResponse? response,
            DurableChatHistoryProvider? durableHistoryProvider,
            ILogger logger)
    {
        DurableAgentHistoryOwnership finalOwnership = DurableAgentHistoryOwnership.Entity;
        DurableAgentStateHistoryBinding? finalHistoryBinding = null;
        if (fixedOwnershipContractActive)
        {
            (finalOwnership, _) =
                DurableAgentHistoryOwnershipResolver.Resolve(
                    session,
                    validatedHistoryConfiguration);
            finalOwnership = DurableAgentHistoryOwnershipResolver.GetEffectiveOwnership(
                finalOwnership,
                historyConfiguration.ReplayMode);
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

            finalHistoryBinding =
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
        }

        if (finalOwnership != DurableAgentHistoryOwnership.Entity)
        {
            durableHistoryProvider?.DiscardStagedTurn();
        }

        if (response is null)
        {
            FinalizeAcceptedRequest(
                workingState,
                request,
                finalOwnership,
                durableHistoryProvider,
                logger);
        }
        else
        {
            FinalizeConversationEntries(
                workingState,
                request,
                response,
                finalOwnership,
                durableHistoryProvider,
                logger);
        }

        workingState.Data.Session = await SerializeSessionWithoutDuplicateHistoryAsync(
                agent,
                session,
                chatClientAgent,
                durableHistoryProvider,
                finalOwnership,
                this._cancellationToken).ConfigureAwait(false);
        if (fixedOwnershipContractActive)
        {
            if (existingHistoryBinding is not null ||
                persistedHistoryBinding is not null ||
                this.State.Data.HistoryBinding.ValueKind == JsonValueKind.Undefined)
            {
                DurableAgentStateHistoryBinding bindingToSeal =
                    existingHistoryBinding ??
                    DurableAgentHistoryBinding.MergeProvisionalMetadata(
                        finalHistoryBinding!,
                        persistedHistoryBinding);
                workingState = DurableAgentHistoryBinding.Seal(
                    workingState,
                    bindingToSeal);
            }

            workingState.PersistentRequestOutcomesAuthorized = true;
        }

        return (workingState, finalOwnership);
    }

    private async Task<AgentResponse?> TryFinalizeProviderFailureAsync(
        Exception exception,
        AIAgent agent,
        AgentSession providerSession,
        ChatClientAgent? chatClientAgent,
        ValidatedDurableAgentHistoryConfiguration validatedHistoryConfiguration,
        DurableAgentHistoryConfiguration historyConfiguration,
        DurableAgentHistoryOwnership ownership,
        DurableAgentStateHistoryBinding? persistedHistoryBinding,
        DurableAgentStateHistoryBinding? existingHistoryBinding,
        string? configuredHistoryProviderKey,
        DurableChatHistoryProvider? durableHistoryProvider,
        DurableAgentState workingState,
        RunRequest request,
        string correlationId,
        AgentSessionId sessionId,
        ILogger logger)
    {
        if (!this._options.EnablePersistentRequestOutcomes ||
            !this._options.EnableProviderFailureFinalization ||
            workingState.SchemaVersion != DurableAgentState.RevisedSchemaVersion ||
            exception is OperationCanceledException ||
            this._cancellationToken.IsCancellationRequested ||
            this._providerFailureAttestor?.TryAttest(exception, out DurableAgentProviderFailureAttestation? attestation) != true ||
            attestation is not { InputAccepted: true } certified ||
            !Enum.IsDefined(certified.Phase) ||
            !Enum.IsDefined(certified.Finality))
        {
            return null;
        }

        // A provider adapter must attest the accepted input and finality. Success and failure share
        // the same final owner, continuation, serialization, and binding boundary; any failure in
        // that boundary leaves the previously committed entity state intact.
        (workingState, _) = await this.FinalizeHistoryStateAsync(
            agent,
            providerSession,
            chatClientAgent,
            validatedHistoryConfiguration,
            historyConfiguration,
            ownership,
            fixedOwnershipContractActive: true,
            persistedHistoryBinding,
            existingHistoryBinding,
            configuredHistoryProviderKey,
            workingState,
            request,
            response: null,
            durableHistoryProvider,
            logger).ConfigureAwait(false);

        DateTimeOffset completedAt = this._timeProvider.GetUtcNow();
        DurableAgentStateTerminalError error = new()
        {
            Code = certified.Code,
            Message = certified.Message,
            Details = certified.Details,
        };
        error.Validate();

        AgentResponse response = new()
        {
            CreatedAt = completedAt,
            Messages = [],
        };
        DateTimeOffset? resultExpiresAt =
            CalculateResultExpiration(completedAt, this._options.ResultRetentionPeriod);
        DurableAgentStateOutcomeResolver.AddFailedResult(
            workingState,
            correlationId,
            response,
            error,
            completedAt,
            resultExpiresAt,
            logger);
        DurableAgentStateTerminalResult result = workingState.Data.TerminalResults![correlationId];
        DurableAgentJsonUtilities.CaptureRetainedResult(response, result.Response!);
        DurableAgentJsonUtilities.CaptureCommittedFailure(
            response,
            new DurableAgentFailureData
            {
                Version = 1,
                CorrelationId = correlationId,
                Code = error.Code,
                Message = error.Message,
                Details = error.Details,
                CompletedAt = completedAt,
                ResultExpiresAt = resultExpiresAt,
                Outcome = DurableAgentStateCompletionReceipt.FailedOutcome,
            });

        DateTime? entityDeletionCheckExpiration =
            this.UpdateEntityExpiration(workingState, sessionId, logger);
        this._cancellationToken.ThrowIfCancellationRequested();
        this.ApplyRetentionAndCommit(
            workingState,
            sessionId,
            logger,
            entityDeletionCheckExpiration);
        return response;
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

    private static IEnumerable<ChatMessage> BuildAgentInputMessages(
        DurableAgentState workingState,
        RunRequest request,
        DurableAgentHistoryOwnership ownership,
        bool contextPipelineSuppliesHistory,
        bool isLegacyState)
    {
        if (contextPipelineSuppliesHistory ||
            ownership == DurableAgentHistoryOwnership.AgentSession)
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

    private static void FinalizeAcceptedRequest(
        DurableAgentState workingState,
        RunRequest request,
        DurableAgentHistoryOwnership ownership,
        DurableChatHistoryProvider? durableHistoryProvider,
        ILogger logger)
    {
        if (ownership != DurableAgentHistoryOwnership.Entity)
        {
            return;
        }

        if (durableHistoryProvider?.HasStagedTurn is true)
        {
            durableHistoryProvider.CompleteStagedFailure();
            return;
        }

        workingState.Data.ConversationHistory.Add(
            DurableAgentStateRequest.FromRunRequestV2(request, logger));
    }

    private static bool ConfiguredProviderOwnsHistory(
        ChatClientAgent? chatClientAgent,
        DurableChatHistoryProvider? durableHistoryProvider)
    {
        // A configured legacy provider remains authoritative unless the invocation replaces it
        // with the durable adapter. The implicit default still mirrors entity replay or service history.
#pragma warning disable MAAI001
        return durableHistoryProvider is null &&
            chatClientAgent?.GetService<ChatClientAgentOptions>()?.ChatHistoryProvider is not null;
#pragma warning restore MAAI001
    }

    private static ValueTask<JsonElement> SerializeSessionWithoutDuplicateHistoryAsync(
        AIAgent agent,
        AgentSession session,
        ChatClientAgent? chatClientAgent,
        DurableChatHistoryProvider? durableHistoryProvider,
        DurableAgentHistoryOwnership ownership,
        CancellationToken cancellationToken)
    {
        IEnumerable<string> excludedStateKeys =
            !ConfiguredProviderOwnsHistory(chatClientAgent, durableHistoryProvider) &&
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

    private void ApplyRetentionAndCommit(
        DurableAgentState workingState,
        AgentSessionId sessionId,
        ILogger logger,
        DateTime? entityDeletionCheckExpiration,
        DateTimeOffset? previousResultCheckTime = null)
    {
        RetentionResult? retention = null;
        bool operationRecorded = false;
        string commitStatus = DurableAgentTelemetry.NotAttemptedCommitStatus;
        try
        {
            DateTimeOffset currentTime = this._timeProvider.GetUtcNow();
            workingState = this.UpdateResultExpirationSchedule(
                workingState,
                currentTime,
                previousResultCheckTime,
                out AgentEntityResultExpirationCheck? nextResultExpirationSignal);

            retention = DurableAgentStateRetention.EnforceForCommit(
                workingState,
                this._options.GetRetentionSettings(),
                currentTime,
                logger,
                sessionId);
            bool deletionStaged = retention?.RemovedEntryCount > 0;

            this._cancellationToken.ThrowIfCancellationRequested();
            try
            {
                ValidateForCommit(workingState);
                if (retention is not null)
                {
                    DurableAgentTelemetry.RecordWrite(
                        DurableAgentTelemetry.SerializationStage,
                        DurableAgentTelemetry.ReturnedOutcome,
                        DurableAgentTelemetry.NotAttemptedCommitStatus,
                        deletionStaged);
                }
            }
            catch (Exception)
            {
                if (retention is not null)
                {
                    DurableAgentTelemetry.RecordWrite(
                        DurableAgentTelemetry.SerializationStage,
                        DurableAgentTelemetry.FailedOutcome,
                        DurableAgentTelemetry.NotAttemptedCommitStatus,
                        deletionStaged);
                }

                throw;
            }

            if (entityDeletionCheckExpiration.HasValue)
            {
                // this.State still points at the hydrated state until the final assignment.
                this.ScheduleEntityDeletionCheck(sessionId, logger, entityDeletionCheckExpiration.Value);
            }

            if (nextResultExpirationSignal is not null)
            {
                this.Context.SignalEntity(
                    this.Context.Id,
                    nameof(CheckAndExpireResults),
                    nextResultExpirationSignal,
                    options: new SignalEntityOptions { SignalTime = nextResultExpirationSignal.ScheduledTime });
            }

            this._cancellationToken.ThrowIfCancellationRequested();
            // This setter performs no synchronous backend I/O. TaskEntity persists the replacement
            // and the self-signal outbox only after the operation completes successfully.
            commitStatus = DurableAgentTelemetry.UnknownCommitStatus;
            try
            {
                this.State = workingState;
                if (retention is not null)
                {
                    DurableAgentTelemetry.RecordWrite(
                        DurableAgentTelemetry.SetStateStage,
                        DurableAgentTelemetry.ReturnedOutcome,
                        commitStatus,
                        deletionStaged);
                }
            }
            catch (Exception)
            {
                if (retention is not null)
                {
                    DurableAgentTelemetry.RecordWrite(
                        DurableAgentTelemetry.SetStateStage,
                        DurableAgentTelemetry.FailedOutcome,
                        commitStatus,
                        deletionStaged);
                }

                throw;
            }

            if (retention is not null)
            {
                DurableAgentTelemetry.RecordOperation(
                    DurableAgentTelemetry.ReturnedOutcome,
                    commitStatus,
                    deletionStaged);
                operationRecorded = true;
            }
        }
        catch (Exception)
        {
            if (retention is not null && !operationRecorded)
            {
                DurableAgentTelemetry.RecordOperation(
                    DurableAgentTelemetry.FailedOutcome,
                    commitStatus,
                    retention.RemovedEntryCount > 0);
            }

            throw;
        }
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
