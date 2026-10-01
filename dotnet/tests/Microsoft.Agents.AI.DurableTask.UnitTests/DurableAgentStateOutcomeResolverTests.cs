// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class DurableAgentStateOutcomeResolverTests
{
    private static readonly DateTimeOffset s_completedAt =
        new(2026, 9, 10, 5, 0, 0, TimeSpan.Zero);

    [Fact]
    public void LegacySuccessAndErrorResolveFromTranscript()
    {
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(CreateLegacyResponse("success", "answer"));
        state.Data.ConversationHistory.Add(CreateLegacyResponse("failure", "error", isError: true));

        DurableAgentRunOutcome success =
            DurableAgentStateOutcomeResolver.Resolve(state, "success", s_completedAt);
        DurableAgentRunOutcome failure =
            DurableAgentStateOutcomeResolver.Resolve(state, "failure", s_completedAt);

        Assert.Equal(DurableAgentRunOutcomeKind.Succeeded, success.Kind);
        Assert.Equal("answer", success.Response?.Text);
        Assert.Equal(DurableAgentRunOutcomeKind.Failed, failure.Kind);
        Assert.Equal("legacyErrorResponse", failure.Error?.Code);
        Assert.Equal("error", failure.Response?.Text);
    }

    [Fact]
    public void RevisedMailboxIsAuthoritativeAfterTranscriptRemoval()
    {
        DurableAgentState state = CreateRevisedState();
        AddResult(state, "correlation", "mailbox");
        state.Data.ConversationHistory.Add(CreateLegacyResponse("correlation", "transcript"));

        state.Data.ConversationHistory.Clear();
        DurableAgentRunOutcome outcome =
            DurableAgentStateOutcomeResolver.Resolve(state, "correlation", s_completedAt);

        Assert.Equal(DurableAgentRunOutcomeKind.Succeeded, outcome.Kind);
        Assert.Equal("mailbox", outcome.Response?.Text);
    }

    [Fact]
    public void RevisedMissingReceiptNeverFallsBackToTranscript()
    {
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(CreateLegacyResponse("correlation", "transcript"));

        DurableAgentRunOutcome outcome =
            DurableAgentStateOutcomeResolver.Resolve(state, "correlation", s_completedAt);

        Assert.Equal(DurableAgentRunOutcomeKind.Pending, outcome.Kind);
    }

    [Fact]
    public void ExpiredRevisedPayloadResolvesCompletedUnavailableWithoutTranscriptFallback()
    {
        DurableAgentState state = CreateRevisedState();
        AddResult(
            state,
            "correlation",
            "mailbox",
            resultExpiresAt: s_completedAt.AddMinutes(1));
        state.Data.ConversationHistory.Add(CreateLegacyResponse("correlation", "transcript"));

        DurableAgentRunOutcome outcome = DurableAgentStateOutcomeResolver.Resolve(
            state,
            "correlation",
            s_completedAt.AddMinutes(1));

        Assert.Equal(DurableAgentRunOutcomeKind.CompletedResultUnavailable, outcome.Kind);
        Assert.Equal(s_completedAt, outcome.Receipt?.CompletedAt);
    }

    [Fact]
    public void InconsistentRevisedResultAndReceiptFailsClosed()
    {
        DurableAgentState state = CreateRevisedState();
        AddResult(state, "correlation", "mailbox");
        state.Data.CompletionReceipts!.Remove("correlation");

        Assert.Throws<DurableAgentStateCorruptionException>(
            () => DurableAgentStateOutcomeResolver.Resolve(
                state,
                "correlation",
                s_completedAt));
    }

    [Fact]
    public void LegacyMigrationConvertsAllEvidenceAndIsIdempotent()
    {
        DurableAgentState legacy = new();
        legacy.Data.ConversationHistory.Add(CreateLegacyResponse("success", "answer"));
        legacy.Data.ConversationHistory.Add(CreateLegacyResponse("failure", "error", isError: true));

        DurableAgentState first =
            DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(legacy, hasAuthoritativeLegacyHistory: true);
        DurableAgentState second =
            DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(first);

        Assert.Equal(DurableAgentState.RevisedSchemaVersion, first.SchemaVersion);
        Assert.Equal(2, first.Data.TerminalResults?.Count);
        Assert.Equal(2, first.Data.CompletionReceipts?.Count);
        Assert.Equal(
            DurableAgentStateCompletionReceipt.FailedOutcome,
            first.Data.TerminalResults?["failure"].Outcome);
        Assert.Equal("legacyErrorResponse", first.Data.TerminalResults?["failure"].Error?.Code);
        Assert.Equal("answer", first.Data.TerminalResults?["success"].Response?.ToResponse().Text);
        Assert.Equal(2, second.Data.TerminalResults?.Count);
        Assert.Equal(2, second.Data.CompletionReceipts?.Count);

        first.Data.ConversationHistory
            .OfType<DurableAgentStateResponse>()
            .First(response => response.CorrelationId == "success")
            .Messages[0].MessageId = "transcript-mutated";
        Assert.Equal(
            "durable_response_success_0",
            first.Data.TerminalResults?["success"].Response?.Messages[0].MessageId);
    }

    [Fact]
    public void PrunedLegacyStateCannotBecomeAnEmptyMailboxEvenWithAuthorization()
    {
        DurableAgentState legacy = new()
        {
            Data = new DurableAgentStateData
            {
                ConversationHistory =
                [
                    new DurableAgentStateRequest { CorrelationId = "evicted", CreatedAt = s_completedAt },
                    new DurableAgentStateResponse(),
                ],
                Truncation = new DurableAgentStateTruncation
                {
                    EvictedMessageCount = JsonSerializer.SerializeToElement(2),
                    FirstEvictedAt = s_completedAt,
                    LastEvictedAt = s_completedAt,
                },
            },
        };

        Assert.Throws<InvalidOperationException>(() =>
            DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(legacy, hasAuthoritativeLegacyHistory: true));
        Assert.Null(legacy.Data.CompletionReceipts);
        Assert.Null(legacy.Data.TerminalResults);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LegacyMigrationRequiresIndependentEvidenceEvenWhenTranscriptLooksComplete(bool hasRetainedResponse)
    {
        DurableAgentState legacy = new();
        if (hasRetainedResponse)
        {
            legacy.Data.ConversationHistory.Add(CreateLegacyResponse("old", "retained"));
        }

        Assert.Throws<InvalidOperationException>(() =>
            DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(legacy));
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, legacy.SchemaVersion);
        Assert.Null(legacy.Data.CompletionReceipts);
    }

    [Fact]
    public void ReceiptWithMissingPayloadFailsClosedDespiteLegacyTranscript()
    {
        DurableAgentState state = CreateRevisedState();
        AddResult(state, "correlation", "mailbox");
        state.Data.TerminalResults!.Clear();
        state.Data.ConversationHistory.Add(CreateLegacyResponse("correlation", "stale"));

        Assert.Throws<DurableAgentStateCorruptionException>(
            () => DurableAgentStateOutcomeResolver.Resolve(state, "correlation", s_completedAt));
    }

    [Theory]
    [InlineData("2.1.0")]
    [InlineData("3.0.0")]
    [InlineData("2.0.0-preview")]
    public void UnknownVersionFailsClosedBeforeDelivery(string version)
    {
        DurableAgentState state = new() { SchemaVersion = version };
        state.Data.ConversationHistory.Add(CreateLegacyResponse("correlation", "stale"));

        Assert.Throws<InvalidOperationException>(
            () => DurableAgentStateOutcomeResolver.Resolve(state, "correlation", s_completedAt));
    }

    [Theory]
    [InlineData(DurableAgentStateCompletionReceipt.SucceededOutcome)]
    [InlineData(DurableAgentStateCompletionReceipt.FailedOutcome)]
    public void UnavailablePayloadRetainsItsTerminalOutcome(string outcome)
    {
        DurableAgentState state = CreateRevisedState();
        state.Data.CompletionReceipts!.Add("correlation", new()
        {
            CorrelationId = "correlation",
            Outcome = outcome,
            CompletedAt = s_completedAt,
            ResultState = DurableAgentStateCompletionReceipt.UnavailableResult,
            ResultUnavailableAt = s_completedAt,
        });

        DurableAgentRunOutcome resolved = DurableAgentStateOutcomeResolver.Resolve(state, "correlation", s_completedAt);

        Assert.Equal(DurableAgentRunOutcomeKind.CompletedResultUnavailable, resolved.Kind);
        Assert.Equal(outcome, resolved.Receipt!.Outcome);
    }

    [Theory]
    [InlineData(null, JsonValueKind.Undefined)]
    [InlineData("null", JsonValueKind.Null)]
    [InlineData("false", JsonValueKind.False)]
    public void ProductionHydrationKeepsAbsentAndExplicitValueSeparate(string? jsonValue, JsonValueKind expectedKind)
    {
        string valueProperty = jsonValue is null ? string.Empty : $",\"value\":{jsonValue}";
        const string JsonTemplate = """
            {"schemaVersion":"2.0.0","data":{"conversationHistory":[],"terminalResults":{
              "correlation":{"correlationId":"correlation","outcome":"succeeded","completedAt":"2026-09-10T05:00:00Z",
                "response":{"messages":[]VALUE_PROPERTY}}},
              "completionReceipts":{"correlation":{"correlationId":"correlation","outcome":"succeeded",
                "completedAt":"2026-09-10T05:00:00Z","resultState":"available"}}}}
            """;
        string json = JsonTemplate.Replace("VALUE_PROPERTY", valueProperty, StringComparison.Ordinal);
        DurableAgentState state = JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState)!;

        DurableAgentRunOutcome resolved = DurableAgentStateOutcomeResolver.Resolve(state, "correlation", s_completedAt);

        Assert.Equal(expectedKind, resolved.Value.ValueKind);
    }

    [Fact]
    public void LegacyDuplicateTerminalConversionFailsClosed()
    {
        DurableAgentState legacy = new();
        legacy.Data.ConversationHistory.Add(CreateLegacyResponse("duplicate", "first"));
        legacy.Data.ConversationHistory.Add(CreateLegacyResponse("duplicate", "second"));

        DurableAgentStateCorruptionException exception =
            Assert.Throws<DurableAgentStateCorruptionException>(
                () => DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(legacy, hasAuthoritativeLegacyHistory: true));

        Assert.Equal("duplicate", exception.CorrelationId);
        Assert.Equal(2, exception.TerminalResponseCount);
    }

    [Fact]
    public void MarkExpiredResultUnavailablePreservesReceiptAndRemovesPayload()
    {
        DurableAgentState state = CreateRevisedState();
        AddResult(
            state,
            "correlation",
            "mailbox",
            resultExpiresAt: s_completedAt.AddMinutes(1));

        bool changed = DurableAgentStateOutcomeResolver.MarkExpiredResultUnavailable(
            state,
            "correlation",
            s_completedAt.AddMinutes(2));

        Assert.True(changed);
        Assert.False(state.Data.TerminalResults!.ContainsKey("correlation"));
        DurableAgentStateCompletionReceipt receipt =
            Assert.IsType<DurableAgentStateCompletionReceipt>(
                state.Data.CompletionReceipts!["correlation"]);
        Assert.Equal(DurableAgentStateCompletionReceipt.UnavailableResult, receipt.ResultState);
        Assert.Equal(s_completedAt.AddMinutes(2), receipt.ResultUnavailableAt);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(64)]
    public void ExpirationBatchValidatesWholeHistoryOnceAndIsIdempotent(int count)
    {
        ValidationProbeContent probe = new();
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            Data = new DurableAgentStateData
            {
                ConversationHistory =
                [
                    new DurableAgentStateResponse
                    {
                        CorrelationId = "stale",
                        Messages = [new DurableAgentStateMessage { Role = "assistant", Contents = [probe] }],
                    },
                ],
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        for (int index = 0; index < count; index++)
        {
            string correlationId = $"due-{index}";
            AgentResponse response = new(new ChatMessage(ChatRole.Assistant, "payload"));
            DateTimeOffset expiresAt = s_completedAt.AddMinutes(1);
            if (index % 2 == 0)
            {
                DurableAgentStateOutcomeResolver.AddSuccessfulResult(state, correlationId, response, s_completedAt, expiresAt);
            }
            else
            {
                DurableAgentStateOutcomeResolver.AddFailedResult(state, correlationId, response,
                    new DurableAgentStateTerminalError { Code = "failed", Message = "failure" }, s_completedAt, expiresAt);
            }

            state.Data.CompletionReceipts![correlationId].UnknownProperties = new Dictionary<string, JsonElement>
            {
                ["future"] = JsonSerializer.SerializeToElement(new { value = false }),
            };
        }

        AddResult(state, "future", "retained", s_completedAt.AddHours(1));
        AddResult(state, "forever", "retained");
        Dictionary<string, DurableAgentStateCompletionReceipt> before = state.Data.CompletionReceipts!.ToDictionary();

        int expired = DurableAgentStateOutcomeResolver.MarkExpiredResultsUnavailable(state, s_completedAt.AddMinutes(1));

        Assert.Equal(count, expired);
        Assert.Equal(1, probe.ValidationCount);
        Assert.Equal(2, state.Data.TerminalResults!.Count);
        Assert.Equal(count + 2, state.Data.CompletionReceipts.Count);
        foreach ((string correlationId, DurableAgentStateCompletionReceipt original) in before)
        {
            DurableAgentStateCompletionReceipt actual = state.Data.CompletionReceipts[correlationId];
            if (!correlationId.StartsWith("due-", StringComparison.Ordinal))
            {
                Assert.Same(original, actual);
                continue;
            }

            Assert.Equal("available", original.ResultState);
            Assert.Equal("unavailable", actual.ResultState);
            Assert.Equal(original.Outcome, actual.Outcome);
            Assert.Equal(original.CompletedAt, actual.CompletedAt);
            Assert.Equal(original.ResultExpiresAt, actual.ResultExpiresAt);
            Assert.Equal(s_completedAt.AddMinutes(1), actual.ResultUnavailableAt);
            Assert.NotSame(original.UnknownProperties, actual.UnknownProperties);
            Assert.True(JsonElement.DeepEquals(original.UnknownProperties!["future"], actual.UnknownProperties!["future"]));
            Assert.False(state.Data.TerminalResults.ContainsKey(correlationId));
        }

        Dictionary<string, DurableAgentStateCompletionReceipt> transitioned = state.Data.CompletionReceipts.ToDictionary();
        Assert.Equal(0, DurableAgentStateOutcomeResolver.MarkExpiredResultsUnavailable(state, s_completedAt.AddMinutes(2)));
        Assert.Equal(2, probe.ValidationCount);
        Assert.All(transitioned, pair => Assert.Same(pair.Value, state.Data.CompletionReceipts[pair.Key]));
    }

    [Theory]
    [InlineData("missingReceipt")]
    [InlineData("unavailablePayload")]
    [InlineData("transcript")]
    public void ExpirationBatchDetectsUnrelatedCorruptionBeforeAnyRemoval(string corruption)
    {
        DurableAgentState state = CreateRevisedState();
        for (int index = 0; index < 16; index++)
        {
            AddResult(state, $"due-{index}", "payload", s_completedAt.AddMinutes(1));
        }

        AddResult(state, "future", "retained", s_completedAt.AddHours(1));
        switch (corruption)
        {
            case "missingReceipt":
                state.Data.CompletionReceipts!.Remove("future");
                break;
            case "unavailablePayload":
                state.Data.CompletionReceipts!["future"] = new()
                {
                    CorrelationId = "future",
                    Outcome = "succeeded",
                    CompletedAt = s_completedAt,
                    ResultState = "unavailable",
                    ResultUnavailableAt = s_completedAt,
                };
                break;
            case "transcript":
                state.Data.ConversationHistory.Add(null!);
                break;
        }

        Dictionary<string, DurableAgentStateTerminalResult> results = state.Data.TerminalResults!.ToDictionary();
        Dictionary<string, DurableAgentStateCompletionReceipt> receipts = state.Data.CompletionReceipts!.ToDictionary();

        Assert.Throws<DurableAgentStateCorruptionException>(() =>
            DurableAgentStateOutcomeResolver.MarkExpiredResultsUnavailable(state, s_completedAt.AddMinutes(1)));

        Assert.Equal(results.Count, state.Data.TerminalResults!.Count);
        Assert.All(results, pair => Assert.Same(pair.Value, state.Data.TerminalResults[pair.Key]));
        Assert.Equal(receipts.Count, state.Data.CompletionReceipts!.Count);
        Assert.All(receipts, pair => Assert.Same(pair.Value, state.Data.CompletionReceipts[pair.Key]));
    }

    [Fact]
    public void ExpirationBatchStagesAllReceiptMetadataBeforeMutating()
    {
        DurableAgentState state = CreateRevisedState();
        AddResult(state, "first", "payload", s_completedAt.AddMinutes(1));
        AddResult(state, "second", "payload", s_completedAt.AddMinutes(1));
        using (JsonDocument document = JsonDocument.Parse("""{"opaque":true}"""))
        {
            state.Data.CompletionReceipts!["second"].UnknownProperties = new Dictionary<string, JsonElement>
            {
                ["future"] = document.RootElement,
            };
        }

        Dictionary<string, DurableAgentStateTerminalResult> results = state.Data.TerminalResults!.ToDictionary();
        Dictionary<string, DurableAgentStateCompletionReceipt> receipts = state.Data.CompletionReceipts!.ToDictionary();

        Assert.Throws<ObjectDisposedException>(() =>
            DurableAgentStateOutcomeResolver.MarkExpiredResultsUnavailable(state, s_completedAt.AddMinutes(1)));

        Assert.Equal(results.Count, state.Data.TerminalResults!.Count);
        Assert.All(results, pair => Assert.Same(pair.Value, state.Data.TerminalResults[pair.Key]));
        Assert.All(receipts, pair => Assert.Same(pair.Value, state.Data.CompletionReceipts[pair.Key]));
    }

    private static DurableAgentState CreateRevisedState()
    {
        return new DurableAgentState
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            Data = new DurableAgentStateData
            {
                ConversationHistory = [],
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
    }

    private static void AddResult(
        DurableAgentState state,
        string correlationId,
        string text,
        DateTimeOffset? resultExpiresAt = null)
    {
        AgentResponse response = new(new ChatMessage(ChatRole.Assistant, text))
        {
            ResponseId = "response-id",
            AgentId = "agent-id",
        };
        DurableAgentStateTerminalResult result =
            DurableAgentStateTerminalResult.FromResponse(
                correlationId,
                response,
                s_completedAt,
                resultExpiresAt);
        state.Data.TerminalResults!.Add(correlationId, result);
        state.Data.CompletionReceipts!.Add(
            correlationId,
            new DurableAgentStateCompletionReceipt
            {
                CorrelationId = correlationId,
                Outcome = result.Outcome,
                CompletedAt = result.CompletedAt,
                ResultState = DurableAgentStateCompletionReceipt.AvailableResult,
                ResultExpiresAt = result.ResultExpiresAt,
            });
    }

    private static DurableAgentStateResponse CreateLegacyResponse(
        string correlationId,
        string text,
        bool isError = false)
    {
        IReadOnlyList<DurableAgentStateMessage> messages =
        [
            DurableAgentStateMessage.FromChatMessage(
                new ChatMessage(ChatRole.Assistant, text)),
        ];
        return isError
            ? new DurableAgentStateErrorResponse
            {
                CorrelationId = correlationId,
                CreatedAt = s_completedAt,
                Messages = messages,
            }
            : new DurableAgentStateResponse
            {
                CorrelationId = correlationId,
                CreatedAt = s_completedAt,
                Messages = messages,
            };
    }

    private sealed class ValidationProbeContent : DurableAgentStateContent
    {
        public int ValidationCount { get; private set; }

        internal override void Validate(DurableAgentStateSchemaVersion version)
        {
            this.ValidationCount++;
            base.Validate(version);
        }

        protected override AIContent ToAIContentCore() =>
            throw new InvalidOperationException("Expiration must not materialize transcript content.");
    }
}
