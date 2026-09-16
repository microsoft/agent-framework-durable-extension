// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Resolves durable request outcomes across legacy transcript and revised mailbox layouts.
/// </summary>
internal static class DurableAgentStateOutcomeResolver
{
    private const string LegacyErrorCode = "legacyErrorResponse";
    private const string LegacyErrorMessage =
        "The durable agent request completed with a recorded terminal error.";

    public static DurableAgentRunOutcome Resolve(
        DurableAgentState state,
        string correlationId,
        DateTimeOffset currentTime)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);

        DurableAgentStateSchemaVersion version =
            DurableAgentStateSchemaVersion.ParseSupported(state.SchemaVersion);
        try
        {
            state.Data.Validate(state.SchemaVersion);
        }
        catch (InvalidOperationException exception)
        {
            throw new DurableAgentStateCorruptionException("The durable agent outcome state is inconsistent.", exception);
        }

        return version.Major == DurableAgentState.RevisedSchemaMajorVersion
            ? ResolveRevised(state, correlationId, currentTime)
            : ResolveLegacy(state.Data.ConversationHistory, correlationId);
    }

    /// <summary>
    /// Creates a revised working state and migrates every evidenced legacy terminal entry.
    /// </summary>
    public static DurableAgentState PrepareRevisedWorkingState(
        DurableAgentState state,
        bool hasAuthoritativeLegacyHistory = false)
    {
        ArgumentNullException.ThrowIfNull(state);

        DurableAgentStateSchemaVersion version =
            DurableAgentStateSchemaVersion.ParseSupported(state.SchemaVersion);
        if (version.Major != DurableAgentState.RevisedSchemaMajorVersion &&
            (!hasAuthoritativeLegacyHistory ||
             state.Data.Truncation is not null ||
             state.Data.ConversationHistory.Any(entry => entry is DurableAgentStateCompaction)))
        {
            throw new InvalidOperationException(
                "Legacy mailbox migration requires independently authoritative complete history. " +
                "Retained or pruned transcripts cannot establish all previous completions; retain legacy state or use an isolated new generation.");
        }

        DurableAgentState clone = state.Clone();
        if (version.Major == DurableAgentState.RevisedSchemaMajorVersion)
        {
            clone.MailboxWritesAuthorized = true;
            return clone;
        }

        Dictionary<string, DurableAgentStateTerminalResult> terminalResults =
            new(StringComparer.Ordinal);
        Dictionary<string, DurableAgentStateCompletionReceipt> completionReceipts =
            new(StringComparer.Ordinal);

        foreach (DurableAgentStateResponse response in clone.Data.ConversationHistory
            .OfType<DurableAgentStateResponse>())
        {
            if (response.CorrelationId is null)
            {
                // Uncorrelated transcript content is not delivery evidence for a request identity.
                continue;
            }

            try
            {
                DurableAgentStateContract.ValidateIdentifier(
                    response.CorrelationId,
                    "conversationHistory.correlationId");
            }
            catch (InvalidOperationException exception)
            {
                throw new DurableAgentStateCorruptionException(
                    "A legacy terminal response has an invalid correlation ID.",
                    exception);
            }

            string correlationId = response.CorrelationId!;
            if (terminalResults.ContainsKey(correlationId))
            {
                int count = clone.Data.ConversationHistory
                    .OfType<DurableAgentStateResponse>()
                    .Count(candidate => string.Equals(
                        candidate.CorrelationId,
                        correlationId,
                        StringComparison.Ordinal));
                throw new DurableAgentStateCorruptionException(correlationId, count);
            }

            DurableAgentStateTerminalResult result = ConvertLegacyTerminal(response);
            terminalResults.Add(correlationId, result);
            completionReceipts.Add(correlationId, CreateAvailableReceipt(result));
        }

        return new DurableAgentState
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                ConversationHistory = clone.Data.ConversationHistory,
                TerminalResults = terminalResults,
                CompletionReceipts = completionReceipts,
                HistoryBinding = clone.Data.HistoryBinding,
                Session = clone.Data.Session,
                IngestedPositions = clone.Data.IngestedPositions,
                Truncation = clone.Data.Truncation,
                ExpirationTimeUtc = clone.Data.ExpirationTimeUtc,
                ExtensionData = clone.Data.ExtensionData,
                UnknownProperties = clone.Data.UnknownProperties,
            },
            ExtensionData = clone.ExtensionData,
            UnknownProperties = clone.UnknownProperties,
        };
    }

    public static void AddSuccessfulResult(
        DurableAgentState state,
        string correlationId,
        AgentResponse response,
        DateTimeOffset completedAt,
        DateTimeOffset? resultExpiresAt = null,
        JsonElement structuredValue = default,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(state);
        DurableAgentStateTerminalResult result =
            DurableAgentStateTerminalResult.FromResponse(
                correlationId,
                response,
                completedAt,
                resultExpiresAt,
                structuredValue,
                logger: logger);

        IDictionary<string, DurableAgentStateTerminalResult> terminalResults =
            state.Data.TerminalResults ??
            throw new InvalidOperationException("A revised durable state requires a terminal result mailbox.");
        IDictionary<string, DurableAgentStateCompletionReceipt> completionReceipts =
            state.Data.CompletionReceipts ??
            throw new InvalidOperationException("A revised durable state requires completion receipts.");
        if (terminalResults.ContainsKey(correlationId) || completionReceipts.ContainsKey(correlationId))
        {
            throw new DurableAgentStateCorruptionException(
                $"Durable agent state already contains a committed outcome for correlation '{correlationId}'.");
        }

        terminalResults.Add(correlationId, result);
        completionReceipts.Add(correlationId, CreateAvailableReceipt(result));
    }

    /// <summary>
    /// Removes an expired payload while retaining its authoritative completion receipt.
    /// </summary>
    public static bool MarkExpiredResultUnavailable(
        DurableAgentState state,
        string correlationId,
        DateTimeOffset currentTime)
    {
        DurableAgentRunOutcome outcome = Resolve(state, correlationId, currentTime);
        if (outcome.Kind != DurableAgentRunOutcomeKind.CompletedResultUnavailable ||
            outcome.Receipt?.ResultState != DurableAgentStateCompletionReceipt.AvailableResult)
        {
            return false;
        }

        IDictionary<string, DurableAgentStateTerminalResult> terminalResults =
            state.Data.TerminalResults ??
            throw new InvalidOperationException("A revised durable state requires a terminal result mailbox.");
        IDictionary<string, DurableAgentStateCompletionReceipt> completionReceipts =
            state.Data.CompletionReceipts ??
            throw new InvalidOperationException("A revised durable state requires completion receipts.");
        DurableAgentStateCompletionReceipt receipt = outcome.Receipt;

        terminalResults.Remove(correlationId);
        completionReceipts[correlationId] = new DurableAgentStateCompletionReceipt
        {
            CorrelationId = receipt.CorrelationId,
            Outcome = receipt.Outcome,
            CompletedAt = receipt.CompletedAt,
            ResultState = DurableAgentStateCompletionReceipt.UnavailableResult,
            ResultExpiresAt = receipt.ResultExpiresAt,
            ResultUnavailableAt = currentTime,
            UnknownProperties = CloneElements(receipt.UnknownProperties),
        };
        return true;
    }

    private static DurableAgentRunOutcome ResolveRevised(
        DurableAgentState state,
        string correlationId,
        DateTimeOffset currentTime)
    {
        IDictionary<string, DurableAgentStateTerminalResult> terminalResults =
            state.Data.TerminalResults ??
            throw new DurableAgentStateCorruptionException(
                "Revised durable agent state is missing the terminal result mailbox.");
        IDictionary<string, DurableAgentStateCompletionReceipt> completionReceipts =
            state.Data.CompletionReceipts ??
            throw new DurableAgentStateCorruptionException(
                "Revised durable agent state is missing completion receipts.");

        bool hasResult = terminalResults.TryGetValue(
            correlationId,
            out DurableAgentStateTerminalResult? result);
        bool hasReceipt = completionReceipts.TryGetValue(
            correlationId,
            out DurableAgentStateCompletionReceipt? receipt);

        if (!hasReceipt)
        {
            if (hasResult)
            {
                throw new DurableAgentStateCorruptionException(
                    $"Durable agent terminal result '{correlationId}' has no completion receipt.");
            }

            // Revised state never falls back to transcript delivery evidence.
            return DurableAgentRunOutcome.Pending;
        }

        if (receipt!.ResultState == DurableAgentStateCompletionReceipt.UnavailableResult)
        {
            if (hasResult)
            {
                throw new DurableAgentStateCorruptionException(
                    $"Unavailable durable agent result '{correlationId}' still has a payload.");
            }

            return DurableAgentRunOutcome.CompletedResultUnavailable(receipt);
        }

        if (!hasResult)
        {
            throw new DurableAgentStateCorruptionException(
                $"Available durable agent result '{correlationId}' has no payload.");
        }

        if (receipt.Outcome != result!.Outcome ||
            receipt.CompletedAt != result.CompletedAt ||
            receipt.ResultExpiresAt != result.ResultExpiresAt)
        {
            throw new DurableAgentStateCorruptionException(
                $"Durable agent result '{correlationId}' is inconsistent with its completion receipt.");
        }

        if (result.ResultExpiresAt is DateTimeOffset expiresAt && currentTime >= expiresAt)
        {
            return DurableAgentRunOutcome.CompletedResultUnavailable(receipt);
        }

        AgentResponse response = result.Response?.ToResponse(static message => message.ToChatMessageV2()) ??
            throw new DurableAgentStateCorruptionException(
                $"Durable agent result '{correlationId}' has no response payload.");
        DurableAgentJsonUtilities.CaptureRetainedResult(response, result.Response);
        return result.Outcome == DurableAgentStateCompletionReceipt.SucceededOutcome
            ? DurableAgentRunOutcome.Succeeded(response, receipt) with { Value = result.Response.Value }
            : DurableAgentRunOutcome.Failed(
                response,
                result.Error ??
                    throw new DurableAgentStateCorruptionException(
                        $"Failed durable agent result '{correlationId}' has no error metadata."),
                receipt) with
            { Value = result.Response.Value };
    }

    private static DurableAgentRunOutcome ResolveLegacy(
        IEnumerable<DurableAgentStateEntry> history,
        string correlationId)
    {
        List<DurableAgentStateResponse> matches = history
            .OfType<DurableAgentStateResponse>()
            .Where(response => string.Equals(
                response.CorrelationId,
                correlationId,
                StringComparison.Ordinal))
            .ToList();

        if (matches.Count > 1)
        {
            throw new DurableAgentStateCorruptionException(correlationId, matches.Count);
        }

        if (matches.Count == 0)
        {
            return DurableAgentRunOutcome.Pending;
        }

        DurableAgentStateResponse response = matches[0];
        AgentResponse agentResponse = response.ToResponse();
        DurableAgentJsonUtilities.CaptureRetainedLegacyResult(agentResponse, response);
        return response is DurableAgentStateErrorResponse
            ? DurableAgentRunOutcome.Failed(
                agentResponse,
                CreateLegacyError(),
                receipt: null)
            : DurableAgentRunOutcome.Succeeded(agentResponse, receipt: null);
    }

    private static DurableAgentStateTerminalResult ConvertLegacyTerminal(
        DurableAgentStateResponse response)
    {
        bool failed = response is DurableAgentStateErrorResponse;
        DateTimeOffset completedAt = response.CreatedAt ??
            response.Messages.Max(message => message.CreatedAt) ??
            throw new DurableAgentStateCorruptionException(
                $"Legacy terminal response '{response.CorrelationId}' has no evidenced completion timestamp.");
        DurableAgentStateTerminalResponse snapshot = new()
        {
            Messages = response.Messages,
            Usage = response.Usage,
            CreatedAt = response.CreatedAt,
            AdditionalProperties = response.ExtensionData,
            UnknownProperties = response.UnknownProperties,
        };
        // Preserve opaque legacy response/message/usage metadata rather than round-tripping it through
        // the lossy runtime projection. The mailbox must not alias the evictable transcript.
        snapshot = JsonSerializer.Deserialize(
            JsonSerializer.SerializeToUtf8Bytes(snapshot, DurableAgentStateJsonContext.Default.DurableAgentStateTerminalResponse),
            DurableAgentStateJsonContext.Default.DurableAgentStateTerminalResponse)!;
        return new DurableAgentStateTerminalResult
        {
            CorrelationId = response.CorrelationId!,
            Outcome = failed
                ? DurableAgentStateCompletionReceipt.FailedOutcome
                : DurableAgentStateCompletionReceipt.SucceededOutcome,
            CompletedAt = completedAt,
            Response = snapshot,
            Error = failed ? CreateLegacyError() : null,
        };
    }

    private static DurableAgentStateTerminalError CreateLegacyError() =>
        new()
        {
            Code = LegacyErrorCode,
            Message = LegacyErrorMessage,
        };

    private static DurableAgentStateCompletionReceipt CreateAvailableReceipt(
        DurableAgentStateTerminalResult result) =>
        new()
        {
            CorrelationId = result.CorrelationId,
            Outcome = result.Outcome,
            CompletedAt = result.CompletedAt,
            ResultState = DurableAgentStateCompletionReceipt.AvailableResult,
            ResultExpiresAt = result.ResultExpiresAt,
        };

    private static Dictionary<string, JsonElement>? CloneElements(
        IDictionary<string, JsonElement>? values) =>
        values?.ToDictionary(
            pair => pair.Key,
            pair => pair.Value.Clone(),
            StringComparer.Ordinal);
}
