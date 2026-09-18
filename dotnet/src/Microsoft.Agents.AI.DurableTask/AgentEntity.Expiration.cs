// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask;

/// <content>
/// Contains result-payload expiration and whole-entity deletion behavior for <see cref="AgentEntity"/>.
/// </content>
internal partial class AgentEntity
{
    private static readonly TimeSpan s_minimumResultExpirationSignalDelay = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Determines whether a scheduled result-expiration signal still owns the current persisted schedule generation.
    /// </summary>
    /// <remarks>
    /// Returning <see langword="false"/> tells the explicit interface dispatcher to bypass normal dispatch entirely,
    /// avoiding initialization or state write-back for missing, legacy, duplicate, or superseded signals.
    /// </remarks>
    private bool ShouldDispatchScheduledResultExpiration(TaskEntityOperation operation)
    {
        this._cancellationToken.ThrowIfCancellationRequested();
        AgentEntityResultExpirationCheck? scheduledCheck =
            (AgentEntityResultExpirationCheck?)operation.GetInput(typeof(AgentEntityResultExpirationCheck));

        // Read persisted state directly from the operation. Entering the base dispatcher first would initialize a
        // missing entity and write that placeholder back after the successful void operation.
        DurableAgentState? persistedState =
            (DurableAgentState?)operation.State.GetState(typeof(DurableAgentState));
        if (persistedState is null)
        {
            return false;
        }

        _ = DurableAgentStateSchemaVersion.ParseSupported(persistedState.SchemaVersion);
        if (persistedState.SchemaVersion != DurableAgentState.RevisedSchemaVersion)
        {
            // Result expiration applies only to the revised mailbox. Validate legacy state, but do not dispatch a
            // successful no-op because the base dispatcher would still rewrite it.
            ValidateForCommit(persistedState);
            return false;
        }

        AgentEntityResultExpirySchedule? schedule =
            AgentEntityResultExpirySchedule.Read(persistedState, operation.Context.Id.ToString());
        return IsCurrentScheduledResultExpiration(schedule, scheduledCheck);
    }

    /// <summary>
    /// Identifies delayed result-expiration self-signals without matching explicit no-input recovery requests.
    /// </summary>
    /// <remarks>
    /// Scheduled signals carry an <see cref="AgentEntityResultExpirationCheck"/> input. A no-input invocation asks the
    /// entity to inspect or repair its current schedule and must continue through normal dispatch.
    /// </remarks>
    private static bool IsScheduledResultExpirationOperation(TaskEntityOperation operation)
    {
        return operation.HasInput &&
            string.Equals(operation.Name, nameof(CheckAndExpireResults), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Checks whether a delivered expiration signal exactly matches the generation persisted in entity state.
    /// </summary>
    /// <remarks>
    /// Record equality covers the scheduled time, random token, and entity identity. The persisted generation is the
    /// authority, so timestamp-only, duplicated, superseded, and foreign-entity signals fail closed.
    /// </remarks>
    private static bool IsCurrentScheduledResultExpiration(
        AgentEntityResultExpirySchedule? schedule,
        AgentEntityResultExpirationCheck? scheduledCheck)
    {
        return schedule?.Pending == scheduledCheck;
    }

    /// <summary>
    /// Calculates when a retained result payload expires without overflowing <see cref="DateTimeOffset"/>.
    /// </summary>
    private static DateTimeOffset? CalculateResultExpiration(
        DateTimeOffset completedAt,
        TimeSpan? retention)
    {
        if (retention is null)
        {
            return null;
        }

        return retention > DateTimeOffset.MaxValue - completedAt
            ? DateTimeOffset.MaxValue
            : completedAt.Add(retention.Value);
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
            if (IsMissingEntityPlaceholder(this.State))
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

            if (!this._options.EnablePersistentRequestOutcomes)
            {
                throw new InvalidOperationException(
                    $"Result expiration requires {nameof(DurableAgentsOptions.EnablePersistentRequestOutcomes)} to be enabled.");
            }

            // Unlike a new run, cleanup must not assign history identities or promote legacy state.
            DurableAgentState workingState = DurableAgentStateJsonConverter.DeserializeRevisedContract(
                DurableAgentStateJsonConverter.SerializeRevisedContract(this.State));
            workingState.PersistentRequestOutcomesAuthorized = true;
            this.CommitWorkingState(
                workingState,
                sessionId,
                logger,
                entityDeletionCheckExpiration: null,
                previousResultCheckTime: scheduledCheck?.ScheduledTime);
        }
        catch (Exception exception)
        {
            logger.LogDurableAgentExecutionFailed(exception, sessionId);
            throw;
        }
    }

    /// <summary>
    /// Deletes the whole entity after its current time-to-live deadline, or schedules another check if it is not due.
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
        if (!expirationTime.HasValue && IsMissingEntityPlaceholder(this.State))
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
            this.ScheduleEntityDeletionCheck(sessionId, logger, expirationTime.Value);
        }
    }

    /// <summary>
    /// Detects the empty state object created by the base dispatcher when a delayed signal targets a missing entity.
    /// </summary>
    private static bool IsMissingEntityPlaceholder(DurableAgentState state)
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

    /// <summary>
    /// Sends a delayed self-signal that will recheck the whole entity's current deletion policy and deadline.
    /// </summary>
    private void ScheduleEntityDeletionCheck(
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

        this.Context.SignalEntity(
            this.Context.Id,
            nameof(CheckAndDeleteIfExpired),
            new AgentEntityDeletionCheck(expirationTime),
            options: new SignalEntityOptions { SignalTime = scheduledTime });
    }

    /// <summary>
    /// Refreshes the whole entity's deletion deadline and returns a deadline that needs a new earlier self-signal.
    /// </summary>
    /// <remarks>
    /// Extending a deadline does not create another signal because the existing signal will re-read and reschedule
    /// the later deadline. A first deadline or a shortened deadline needs a new signal so deletion is not checked late.
    /// </remarks>
    private DateTime? UpdateEntityExpiration(
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

        return !previousExpirationTime.HasValue ||
            newExpirationTime < previousExpirationTime.Value
                ? newExpirationTime
                : null;
    }

    /// <summary>
    /// Removes expired result payloads and updates the generation-fenced signal for the next retained payload expiry.
    /// </summary>
    private DurableAgentState UpdateResultExpirationSchedule(
        DurableAgentState workingState,
        DateTimeOffset currentTime,
        DateTimeOffset? previousResultCheckTime,
        out AgentEntityResultExpirationCheck? nextSignal)
    {
        nextSignal = null;
        if (workingState.SchemaVersion != DurableAgentState.RevisedSchemaVersion)
        {
            return workingState;
        }

        string entityId = this.Context.Id.ToString();
        AgentEntityResultExpirySchedule? schedule =
            AgentEntityResultExpirySchedule.Read(workingState, entityId);
        DateTimeOffset? nextResultExpiration = null;
        foreach (DurableAgentStateTerminalResult result in workingState.Data.TerminalResults!.Values.ToArray())
        {
            if (result.ResultExpiresAt is not DateTimeOffset expiresAt)
            {
                continue;
            }

            if (expiresAt <= currentTime)
            {
                DurableAgentStateOutcomeResolver.MarkExpiredResultUnavailable(
                    workingState,
                    result.CorrelationId,
                    currentTime);
            }
            else if (nextResultExpiration is null || expiresAt < nextResultExpiration)
            {
                nextResultExpiration = expiresAt;
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
            DateTimeOffset scheduledTime =
                resultExpiration > minimumScheduledTime ? resultExpiration : minimumScheduledTime;
            if (pending is null || pending.ScheduledTime > scheduledTime)
            {
                pending = nextSignal = new AgentEntityResultExpirationCheck(
                    scheduledTime.ToUniversalTime(),
                    Guid.NewGuid().ToString("N"),
                    entityId);
            }
        }
        else
        {
            pending = null;
        }

        return AgentEntityResultExpirySchedule.Write(workingState, entityId, schedule, pending);
    }
}

internal sealed record AgentEntityDeletionCheck(DateTime ExpectedExpirationTimeUtc);

internal sealed record AgentEntityResultExpirationCheck(
    DateTimeOffset ScheduledTime,
    string? Token = null,
    string? EntityId = null);
