// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;
using DurableTask.Core.Entities;
using DurableTask.Core.Entities.OperationFormat;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.DurableTask.Worker;
using Microsoft.DurableTask.Worker.Shims;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityProviderFailureTests
{
    private const string PrivateMarker = "untrusted-provider-diagnostic";
    private static readonly DateTimeOffset s_now = new(2026, 10, 3, 9, 0, 0, TimeSpan.Zero);
    private static readonly EntityId s_entityId = new(AgentSessionId.ToEntityName("agent"), "session");
    private static readonly DurableDataConverter s_converter = new();

    [Theory]
    [InlineData("load")]
    [InlineData("lazyLoad")]
    [InlineData("storeBefore")]
    [InlineData("storeAfter")]
    public async Task CallbackFailureCommitsThroughSdkBatchAndColdDuplicatesNeverExecuteAsync(string stage)
    {
        ProbeProvider provider = new();
        ProbeClient client = new();
        ChatClientAgent agent = CreateAgent(client, provider);
        EntityBatchResult seed = await DispatchAsync(agent, null, "seed");
        DurableAgentState before = ReadState(seed);
        before.Data.ConversationHistory.Add(DurableAgentStateRequest.FromRunRequestV2(
            new RunRequest("retained transcript") { CorrelationId = "prior-transcript" }));
        before.Data.IngestedPositions = new Dictionary<string, JsonElement>
        {
            ["producer"] = JsonSerializer.SerializeToElement(7),
        };
        string original = s_converter.Serialize(before);
        provider.Stage = stage;
        provider.Failure = new InvalidOperationException(PrivateMarker);
        RecordingLogger logger = new();

        EntityBatchResult failed = await DispatchAsync(agent, original, "failed", logger: logger);

        Assert.Null(Result(failed).FailureDetails);
        Assert.Null(failed.FailureDetails);
        AgentResponse transport = Assert.IsType<AgentResponse>(
            s_converter.Deserialize(Result(failed).Result, typeof(AgentResponse)));
        Assert.Equal(ObservedChatHistoryProvider.FailureCode,
            DurableAgentJsonUtilities.GetCommittedFailure(transport)?.Code);
        DurableAgentState committed = ReadState(failed);
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, committed.SchemaVersion);
        Assert.Equal(before.Data.Session!.Value.GetRawText(), committed.Data.Session!.Value.GetRawText());
        Assert.Equal(before.Data.HistoryBinding.GetRawText(), committed.Data.HistoryBinding.GetRawText());
        Assert.True(committed.Data.TryGetIngestedPosition("producer", out long position));
        Assert.Equal(7, position);
        Assert.Equal("prior-transcript", Assert.Single(committed.Data.ConversationHistory).CorrelationId);
        Assert.DoesNotContain(committed.Data.ConversationHistory, entry => entry.CorrelationId == "failed");
        DurableAgentStateTerminalResult result = committed.Data.TerminalResults!["failed"];
        DurableAgentStateCompletionReceipt receipt = committed.Data.CompletionReceipts!["failed"];
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, result.Outcome);
        Assert.Equal(result.Outcome, receipt.Outcome);
        Assert.Equal(result.CompletedAt, receipt.CompletedAt);
        Assert.Equal(result.ResultExpiresAt, receipt.ResultExpiresAt);
        Assert.Equal(DurableAgentStateCompletionReceipt.AvailableResult, receipt.ResultState);
        Assert.Equal(ObservedChatHistoryProvider.FailureCode, result.Error!.Code);
        Assert.DoesNotContain(PrivateMarker, failed.EntityState, StringComparison.Ordinal);
        Assert.DoesNotContain(PrivateMarker, Result(failed).Result, StringComparison.Ordinal);
        Assert.DoesNotContain(logger.Messages, message => message.Contains(PrivateMarker, StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains(ObservedChatHistoryProvider.FailureMessage, StringComparison.Ordinal));
        Assert.Equal(2, provider.LoadCount);
        Assert.Equal(stage.StartsWith("store", StringComparison.Ordinal) ? 2 : 1, provider.StoreCount);
        Assert.Equal(stage.StartsWith("store", StringComparison.Ordinal) ? 2 : 1, client.Count);

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        DurableAgentTerminalException polled = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => AgentRunHandleTests.CreateHandle(committed, correlationId: "failed", timeProvider: new Clock())
                .ReadAgentResponseAsync(timeout.Token));
        Assert.Equal(ObservedChatHistoryProvider.FailureCode, polled.Code);

        ProbeProvider coldProvider = new();
        ProbeClient coldClient = new();
        ChatClientAgent coldAgent = CreateAgent(coldClient, coldProvider);
        committed.Data.ConversationHistory.Clear();
        string pruned = s_converter.Serialize(committed);
        EntityBatchResult duplicate = await DispatchAsync(coldAgent, pruned, "failed", emptyInput: true);
        Assert.Equal(typeof(DurableAgentTerminalException).FullName, Failure(duplicate).ErrorType);
        Assert.Equal(pruned, duplicate.EntityState);
        Assert.Empty(Actions(duplicate));
        Assert.Equal(0, coldProvider.LoadCount);
        Assert.Equal(0, coldProvider.StoreCount);
        Assert.Equal(0, coldClient.Count);

        EntityBatchResult next = await DispatchAsync(coldAgent, duplicate.EntityState, "next");
        Assert.Null(Result(next).FailureDetails);
        Assert.Equal(1, coldProvider.RestoredVersion);
        Assert.Equal(1, coldClient.Count);
    }

    [Theory]
    [InlineData("load")]
    [InlineData("storeAfter")]
    public async Task FirstFailureRecordsOnlyPendingProfileWithoutContinuationOrInputAcceptanceAsync(string stage)
    {
        ProbeProvider provider = new() { Stage = stage, Failure = new IOException(PrivateMarker) };
        EntityBatchResult failed = await DispatchAsync(CreateAgent(new ProbeClient(), provider), null, "failed");

        Assert.Null(Result(failed).FailureDetails);
        DurableAgentState state = ReadState(failed);
        Assert.Null(state.Data.Session);
        Assert.Equal(PendingProfileJson, state.Data.HistoryBinding.GetRawText());
        Assert.Null(DurableAgentHistoryBinding.Parse(state.Data.HistoryBinding));
        Assert.Empty(state.Data.ConversationHistory);
        Assert.Null(state.Data.IngestedPositions);
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, Assert.Single(state.Data.CompletionReceipts!).Value.Outcome);
    }

    [Theory]
    [InlineData("load", false)]
    [InlineData("storeAfter", false)]
    [InlineData("load", true)]
    [InlineData("storeAfter", true)]
    public async Task FirstFailureAllowsNewCorrelationAfterColdRecoveryAsync(string stage, bool expire)
    {
        ProbeProvider provider = new() { Stage = stage, Failure = new IOException(PrivateMarker) };
        EntityBatchResult failed = await DispatchAsync(CreateAgent(new ProbeClient(), provider), null, "failed");
        Assert.Null(Result(failed).FailureDetails);
        DurableAgentState committed = ReadState(failed);
        string failedReceipt = JsonSerializer.Serialize(committed.Data.CompletionReceipts!["failed"]);
        string failedResult = JsonSerializer.Serialize(committed.Data.TerminalResults!["failed"]);
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        DurableAgentTerminalException polled = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => AgentRunHandleTests.CreateHandle(committed, correlationId: "failed", timeProvider: new Clock())
                .ReadAgentResponseAsync(timeout.Token));
        Assert.Equal(ObservedChatHistoryProvider.FailureCode, polled.Code);

        ProbeProvider recoveredProvider = new();
        ProbeClient recoveredClient = new();
        ChatClientAgent recoveredAgent = CreateAgent(recoveredClient, recoveredProvider);
        DateTimeOffset now = expire ? s_now.AddMinutes(2) : s_now;
        string coldState = s_converter.Serialize(committed);
        if (expire)
        {
            EntityBatchResult cleanup = await DispatchAsync(recoveredAgent, coldState, "unused",
                operationName: nameof(AgentEntity.CheckAndExpireResults), now: now);
            Assert.Null(Result(cleanup).FailureDetails);
            Assert.Equal(PendingProfileJson, ReadState(cleanup).Data.HistoryBinding.GetRawText());
            coldState = s_converter.Serialize(ReadState(cleanup));
        }
        EntityBatchResult duplicate = await DispatchAsync(recoveredAgent, coldState, "failed", emptyInput: true, now: now);
        Assert.Equal(expire
            ? typeof(DurableAgentResultUnavailableException).FullName
            : typeof(DurableAgentTerminalException).FullName, Failure(duplicate).ErrorType);
        Assert.Equal(coldState, duplicate.EntityState);
        Assert.Equal(0, recoveredProvider.LoadCount);
        Assert.Equal(0, recoveredProvider.StoreCount);
        Assert.Equal(0, recoveredClient.Count);

        EntityBatchResult next = await DispatchAsync(recoveredAgent, duplicate.EntityState, "next", now: now);

        Assert.Null(Result(next).FailureDetails);
        Assert.Equal(1, recoveredProvider.LoadCount);
        Assert.Equal(1, recoveredProvider.StoreCount);
        Assert.Equal(1, recoveredClient.Count);
        Assert.Null(recoveredProvider.RestoredVersion);
        DurableAgentState succeeded = ReadState(next);
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, succeeded.Data.CompletionReceipts!["failed"].Outcome);
        if (expire)
        {
            Assert.False(succeeded.Data.TerminalResults!.ContainsKey("failed"));
            Assert.Equal(DurableAgentStateCompletionReceipt.UnavailableResult, succeeded.Data.CompletionReceipts["failed"].ResultState);
        }
        else
        {
            Assert.Equal(failedReceipt, JsonSerializer.Serialize(succeeded.Data.CompletionReceipts["failed"]));
            Assert.Equal(failedResult, JsonSerializer.Serialize(succeeded.Data.TerminalResults!["failed"]));
        }
        Assert.Equal(DurableAgentStateCompletionReceipt.SucceededOutcome, succeeded.Data.CompletionReceipts["next"].Outcome);
        Assert.NotNull(succeeded.Data.Session);
        DurableAgentStateHistoryBinding binding = Assert.IsType<DurableAgentStateHistoryBinding>(
            DurableAgentHistoryBinding.Parse(succeeded.Data.HistoryBinding));
        Assert.True(DurableAgentHistoryBinding.IsSealedByCSharp(binding));
        Assert.Equal("probe.v1", binding.ProviderKey);
        Assert.Equal(DurableAgentStateHistoryBinding.HistoryProviderOwner, binding.OwnerKind);

        ProbeProvider continuationProvider = new();
        ProbeClient continuationClient = new();
        EntityBatchResult continued = await DispatchAsync(CreateAgent(continuationClient, continuationProvider),
            s_converter.Serialize(succeeded), "continued", now: now);
        Assert.Null(Result(continued).FailureDetails);
        Assert.Equal(1, continuationProvider.RestoredVersion);
        Assert.Equal(1, continuationProvider.LoadCount);
        Assert.Equal(1, continuationProvider.StoreCount);
        Assert.Equal(1, continuationClient.Count);
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, ReadState(continued).Data.CompletionReceipts!["failed"].Outcome);
        EntityBatchResult oldDuplicate = await DispatchAsync(CreateAgent(continuationClient, continuationProvider),
            continued.EntityState, "failed", emptyInput: true, now: now);
        Assert.Equal(expire
            ? typeof(DurableAgentResultUnavailableException).FullName
            : typeof(DurableAgentTerminalException).FullName, Failure(oldDuplicate).ErrorType);
        Assert.Equal(continued.EntityState, oldDuplicate.EntityState);
        Assert.Equal(1, continuationProvider.LoadCount);
        Assert.Equal(1, continuationProvider.StoreCount);
        Assert.Equal(1, continuationClient.Count);
    }

    private const string PendingProfileJson =
        "{\"profile\":\"Microsoft.Agents.AI.DurableTask.pendingProviderInitialization\",\"version\":1,\"providerKey\":\"probe.v1\"}";

    [Theory]
    [InlineData("load")]
    [InlineData("storeAfter")]
    public async Task RepeatedInitializationFailuresKeepProfileUntilGenuineContinuationAsync(string stage)
    {
        string? coldState = null;
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            ProbeProvider provider = new() { Stage = stage, Failure = new IOException(PrivateMarker) };
            ProbeClient client = new();
            EntityBatchResult failed = await DispatchAsync(CreateAgent(client, provider), coldState, $"failed-{attempt}");
            Assert.Null(Result(failed).FailureDetails);
            DurableAgentState state = ReadState(failed);
            Assert.Equal(PendingProfileJson, state.Data.HistoryBinding.GetRawText());
            Assert.Null(state.Data.Session);
            Assert.Empty(state.Data.ConversationHistory);
            Assert.Equal(attempt, state.Data.CompletionReceipts!.Count);
            Assert.All(state.Data.CompletionReceipts.Values,
                receipt => Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, receipt.Outcome));
            Assert.Equal(1, provider.LoadCount);
            Assert.Equal(stage == "load" ? 0 : 1, client.Count);
            coldState = s_converter.Serialize(state);
        }
        ProbeProvider recovered = new();
        EntityBatchResult success = await DispatchAsync(CreateAgent(new ProbeClient(), recovered), coldState, "recovered");
        Assert.Null(Result(success).FailureDetails);
        Assert.NotNull(ReadState(success).Data.Session);
        Assert.True(DurableAgentHistoryBinding.IsSealedByCSharp(
            DurableAgentHistoryBinding.Parse(ReadState(success).Data.HistoryBinding)));
        Assert.Equal(4, ReadState(success).Data.CompletionReceipts!.Count);
    }

    [Theory]
    [InlineData("different.v1")]
    [InlineData(null)]
    public async Task PendingInitializationRejectsWrongOrMissingConfiguredKeyAsync(string? providerKey)
    {
        EntityBatchResult failed = await CreateFirstFailureAsync();
        ProbeProvider provider = new();
        ProbeClient client = new();
        EntityBatchResult next = await DispatchAsync(CreateAgent(client, provider), failed.EntityState, "next",
            providerKey: providerKey);
        Assert.Equal(typeof(DurableAgentHistoryBindingMismatchException).FullName, Failure(next).ErrorType);
        Assert.Equal(failed.EntityState, next.EntityState);
        Assert.Empty(Actions(next));
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.Count);
        EntityBatchResult duplicate = await DispatchAsync(CreateAgent(client, provider), failed.EntityState, "failed",
            providerKey: providerKey);
        Assert.Equal(typeof(DurableAgentTerminalException).FullName, Failure(duplicate).ErrorType);
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, client.Count);
    }

    [Fact]
    public async Task PendingInitializationRejectsEntityOwnedReplacementBeforeModelAsync()
    {
        EntityBatchResult failed = await CreateFirstFailureAsync();
        ProbeClient client = new();
        ChatClientAgent agent = new(client, new ChatClientAgentOptions { Name = "agent" });
        EntityBatchResult next = await DispatchAsync(agent, failed.EntityState, "next");
        Assert.Equal(typeof(DurableAgentHistoryBindingMismatchException).FullName, Failure(next).ErrorType);
        Assert.Equal(failed.EntityState, next.EntityState);
        Assert.Equal(0, client.Count);
    }

    [Theory]
    [InlineData("unsupported")]
    [InlineData("versionType")]
    [InlineData("missingVersion")]
    [InlineData("invalidKey")]
    [InlineData("missingKey")]
    [InlineData("fixedOwner")]
    [InlineData("provisionalOwner")]
    [InlineData("unknown")]
    public async Task PendingProfileValidationPreservesOpaqueStateAndRejectsBeforeCallbacksAsync(string mutation)
    {
        EntityBatchResult failed = await CreateFirstFailureAsync();
        JsonNode root = JsonNode.Parse(failed.EntityState!)!;
        JsonNode binding = root["data"]!["historyBinding"]!;
        switch (mutation)
        {
            case "unsupported": binding["version"] = 2; break;
            case "versionType": binding["version"] = "1"; break;
            case "missingVersion": binding.AsObject().Remove("version"); break;
            case "invalidKey": binding["providerKey"] = " "; break;
            case "missingKey": binding.AsObject().Remove("providerKey"); break;
            case "fixedOwner": binding["csharpFixedOwner"] = true; break;
            case "provisionalOwner": binding["ownerKind"] = "historyProvider"; break;
            default: binding["profile"] = "future.pendingInitialization"; break;
        }
        string original = root.ToJsonString();
        DurableAgentState roundtrip = Assert.IsType<DurableAgentState>(
            s_converter.Deserialize(original, typeof(DurableAgentState)));
        Assert.Equal(binding.ToJsonString(), roundtrip.Data.HistoryBinding.GetRawText());
        ProbeProvider provider = new();
        ProbeClient client = new();
        EntityBatchResult next = await DispatchAsync(CreateAgent(client, provider), original, "next");
        Assert.Equal(typeof(DurableAgentHistoryBindingMismatchException).FullName, Failure(next).ErrorType);
        Assert.Equal(original, next.EntityState);
        Assert.Empty(Actions(next));
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.Count);
    }

    [Theory]
    [InlineData("session")]
    [InlineData("transcript")]
    [InlineData("ingestion")]
    [InlineData("truncation")]
    [InlineData("success")]
    [InlineData("otherFailure")]
    [InlineData("corrupt")]
    public async Task PendingProfileRejectsContradictoryContinuityAndCorruptionAsync(string evidence)
    {
        EntityBatchResult failed = await CreateFirstFailureAsync();
        DurableAgentState state = ReadState(failed);
        switch (evidence)
        {
            case "session": state.Data.Session = JsonSerializer.SerializeToElement(new { }); break;
            case "transcript":
                state.Data.ConversationHistory.Add(DurableAgentStateRequest.FromRunRequestV2(
                    new RunRequest("history") { CorrelationId = "prior" }));
                break;
            case "ingestion": state.Data.IngestedPositions = new Dictionary<string, JsonElement>(); break;
            case "truncation":
                state.Data.Truncation = new()
                {
                    EvictedMessageCount = JsonSerializer.SerializeToElement(1),
                    FirstEvictedAt = s_now,
                    LastEvictedAt = s_now,
                };
                break;
            case "success":
                DurableAgentStateOutcomeResolver.AddSuccessfulResult(
                    state, "prior", new AgentResponse(new ChatMessage(ChatRole.Assistant, "response")), s_now);
                break;
            case "otherFailure":
                DurableAgentStateOutcomeResolver.AddFailedResult(
                    state, "prior", new AgentResponse(), new() { Code = "other", Message = "other" }, s_now);
                break;
        }
        string original = s_converter.Serialize(state);
        if (evidence == "corrupt")
        {
            JsonNode root = JsonNode.Parse(original)!;
            root["data"]!["completionReceipts"]!["failed"]!["outcome"] = "succeeded";
            original = root.ToJsonString();
        }
        ProbeProvider provider = new();
        ProbeClient client = new();
        EntityBatchResult next = await DispatchAsync(CreateAgent(client, provider), original, "next");
        Assert.NotNull(Result(next).FailureDetails);
        Assert.Equal(original, next.EntityState);
        Assert.Empty(Actions(next));
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.Count);
    }

    [Fact]
    public async Task PendingInitializationCannotSealWithoutRealContinuationAsync()
    {
        EntityBatchResult failed = await CreateFirstFailureAsync();
        ProbeProvider provider = new() { SkipContinuationWrite = true };
        ProbeClient client = new();
        EntityBatchResult next = await DispatchAsync(CreateAgent(client, provider), failed.EntityState, "next");
        Assert.Equal(typeof(DurableAgentHistoryBindingMismatchException).FullName, Failure(next).ErrorType);
        Assert.Equal(failed.EntityState, next.EntityState);
        Assert.Empty(Actions(next));
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PendingInitializationRequiresRestorableSerializedContinuationAsync(bool throwOnSerialize)
    {
        EntityBatchResult failed = await CreateFirstFailureAsync();
        ProbeProvider provider = new();
        ProbeClient client = new();
        DamagedSerializationAgent agent = new(CreateAgent(client, provider), throwOnSerialize);
        EntityBatchResult next = await DispatchAsync(agent, failed.EntityState, "next");
        Assert.Equal(throwOnSerialize
            ? typeof(JsonException).FullName
            : typeof(DurableAgentHistoryBindingMismatchException).FullName, Failure(next).ErrorType);
        Assert.Equal(failed.EntityState, next.EntityState);
        Assert.Empty(Actions(next));
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.Count);
    }

    [Theory]
    [InlineData("state")]
    [InlineData("response")]
    [InlineData("afterDispatch")]
    public async Task PendingToFixedBindingCommitFailurePreservesInitializationAuthorityAsync(string boundary)
    {
        EntityBatchResult failed = await CreateFirstFailureAsync();
        ProbeProvider provider = new();
        ProbeClient client = new();
        ChatClientAgent agent = CreateAgent(client, provider);
        EntityBatchResult rejected = await DispatchAsync(agent, failed.EntityState, "next", failBoundary: boundary);
        Assert.Equal(typeof(CommitFailureException).FullName, Failure(rejected).ErrorType);
        Assert.Equal(failed.EntityState, rejected.EntityState);
        Assert.Empty(Actions(rejected));
        EntityBatchResult next = await DispatchAsync(agent, rejected.EntityState, "next");
        Assert.Null(Result(next).FailureDetails);
        Assert.Equal(2, provider.LoadCount);
        Assert.Equal(2, client.Count);
        Assert.True(DurableAgentHistoryBinding.IsSealedByCSharp(
            DurableAgentHistoryBinding.Parse(ReadState(next).Data.HistoryBinding)));
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, ReadState(next).Data.CompletionReceipts!["failed"].Outcome);
    }

    [Fact]
    public async Task FirstFailureAndNewInitializationInSameSdkBatchKeepOperationScopedProvenanceAsync()
    {
        ProbeProvider provider = new() { Stage = "load", Failure = new IOException(PrivateMarker) };
        provider.BeforeFailure = () => provider.Stage = null;
        ProbeClient client = new();
        EntityBatchResult batch = await DispatchAsync(CreateAgent(client, provider), null, "failed",
            correlations: ["failed", "next", "failed"]);
        List<OperationResult> results = Assert.IsType<List<OperationResult>>(batch.Results);
        Assert.Equal(3, results.Count);
        Assert.Null(results[0].FailureDetails);
        Assert.Null(results[1].FailureDetails);
        Assert.Equal(typeof(DurableAgentTerminalException).FullName, results[2].FailureDetails!.ErrorType);
        Assert.Equal(2, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.Count);
        Assert.Null(provider.RestoredVersion);
        DurableAgentState state = ReadState(batch);
        Assert.Equal(2, state.Data.CompletionReceipts!.Count);
        Assert.True(DurableAgentHistoryBinding.IsSealedByCSharp(DurableAgentHistoryBinding.Parse(state.Data.HistoryBinding)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExistingOrMigratedEmptyMailboxFailureDoesNotMintPendingInitializationAsync(bool migrate)
    {
        DurableAgentState empty = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new()
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        EntityBatchResult failed = await DispatchAsync(
            CreateAgent(new ProbeClient(), new ProbeProvider { Stage = "load", Failure = new IOException(PrivateMarker) }),
            s_converter.Serialize(migrate ? new DurableAgentState() : empty), "failed", authorizeMigration: migrate);
        Assert.Null(Result(failed).FailureDetails);
        Assert.Equal(JsonValueKind.Undefined, ReadState(failed).Data.HistoryBinding.ValueKind);
        ProbeProvider recovered = new();
        ProbeClient client = new();
        EntityBatchResult next = await DispatchAsync(CreateAgent(client, recovered), failed.EntityState, "next");
        Assert.Equal(typeof(DurableAgentHistoryBindingMismatchException).FullName, Failure(next).ErrorType);
        Assert.Equal(0, recovered.LoadCount);
        Assert.Equal(0, client.Count);
    }

    [Theory]
    [InlineData("state")]
    [InlineData("response")]
    [InlineData("afterDispatch")]
    public async Task FirstFailureCommitRollbackDoesNotPublishPendingProfileAsync(string boundary)
    {
        EntityBatchResult failed = await DispatchAsync(
            CreateAgent(new ProbeClient(), new ProbeProvider { Stage = "storeAfter", Failure = new IOException(PrivateMarker) }),
            null, "failed", failBoundary: boundary);
        Assert.Equal(typeof(CommitFailureException).FullName, Failure(failed).ErrorType);
        Assert.Null(failed.EntityState);
        Assert.Empty(Actions(failed));
        EntityBatchResult recovered = await DispatchAsync(CreateAgent(new ProbeClient(), new ProbeProvider()), failed.EntityState, "next");
        Assert.Null(Result(recovered).FailureDetails);
        Assert.Single(ReadState(recovered).Data.CompletionReceipts!);
    }

    private static Task<EntityBatchResult> CreateFirstFailureAsync() => DispatchAsync(
        CreateAgent(new ProbeClient(), new ProbeProvider { Stage = "load", Failure = new IOException(PrivateMarker) }),
        null, "failed");

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PrunedMigratedFailedMailboxCannotInitializeReplacementProviderAsync(bool unavailable)
    {
        DurableAgentState legacy = new();
        legacy.Data.ConversationHistory.Add(new DurableAgentStateErrorResponse
        {
            CorrelationId = "failed",
            CreatedAt = s_now,
        });
        DurableAgentState migrated = DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(
            legacy, hasAuthoritativeLegacyHistory: true);
        migrated.Data.ConversationHistory.Clear();
        if (unavailable)
        {
            migrated.Data.TerminalResults!.Clear();
            migrated.Data.CompletionReceipts!["failed"] = new DurableAgentStateCompletionReceipt
            {
                CorrelationId = "failed",
                Outcome = DurableAgentStateCompletionReceipt.FailedOutcome,
                CompletedAt = s_now,
                ResultState = DurableAgentStateCompletionReceipt.UnavailableResult,
                ResultUnavailableAt = s_now.AddMinutes(1),
            };
        }
        ProbeProvider provider = new();
        ProbeClient client = new();
        ChatClientAgent agent = CreateAgent(client, provider);
        EntityBatchResult cleanup = await DispatchAsync(agent, s_converter.Serialize(migrated), "unused",
            operationName: nameof(AgentEntity.CheckAndExpireResults), now: s_now.AddMinutes(2));
        Assert.Null(Result(cleanup).FailureDetails);

        EntityBatchResult next = await DispatchAsync(agent, cleanup.EntityState, "next", now: s_now.AddMinutes(2));

        Assert.Equal(typeof(DurableAgentHistoryBindingMismatchException).FullName, Failure(next).ErrorType);
        Assert.Equal(cleanup.EntityState, next.EntityState);
        Assert.Empty(Actions(next));
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.Count);
    }

    [Fact]
    public async Task ExpiredFailedPayloadRemainsUnavailableAndNonexecutableAfterCleanupAsync()
    {
        ProbeProvider provider = new() { Stage = "load", Failure = new IOException(PrivateMarker) };
        EntityBatchResult failed = await DispatchAsync(CreateAgent(new ProbeClient(), provider), null, "failed");
        DurableAgentState committed = ReadState(failed);
        Assert.Single(Actions(failed));
        committed.Data.ConversationHistory.Clear();
        DateTimeOffset expired = s_now.AddMinutes(2);
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(5));
        DurableAgentResultUnavailableException unavailable = await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
            () => AgentRunHandleTests.CreateHandle(committed, correlationId: "failed", timeProvider: new Clock(expired))
                .ReadAgentResponseAsync(timeout.Token));
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, unavailable.Outcome);

        ProbeClient client = new();
        ProbeProvider duplicateProvider = new();
        ChatClientAgent agent = CreateAgent(client, duplicateProvider);
        EntityBatchResult cleanup = await DispatchAsync(agent, s_converter.Serialize(committed), "unused",
            operationName: nameof(AgentEntity.CheckAndExpireResults), now: expired);
        Assert.Null(Result(cleanup).FailureDetails);
        DurableAgentState cleaned = ReadState(cleanup);
        Assert.Empty(cleaned.Data.TerminalResults!);
        Assert.Equal(DurableAgentStateCompletionReceipt.UnavailableResult, cleaned.Data.CompletionReceipts!["failed"].ResultState);
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, cleaned.Data.CompletionReceipts["failed"].Outcome);
        EntityBatchResult duplicate = await DispatchAsync(agent, cleanup.EntityState, "failed", now: expired);
        Assert.Equal(typeof(DurableAgentResultUnavailableException).FullName, Failure(duplicate).ErrorType);
        Assert.Equal(cleanup.EntityState, duplicate.EntityState);
        Assert.Equal(0, client.Count);
        Assert.Equal(0, duplicateProvider.LoadCount);
        Assert.Equal(0, duplicateProvider.StoreCount);
    }

    [Theory]
    [InlineData("load")]
    [InlineData("storeAfter")]
    public async Task DefaultSchema12FailureRollsBackAndRemainsRetryableAsync(string stage)
    {
        DurableAgentsOptions defaults = new();
        Assert.False(defaults.EnablePersistentRequestOutcomes);
        Assert.False(defaults.EnableProviderFailureFinalization);
        Assert.Equal(DurableAgentHistoryRetentionMode.KeepAll, defaults.HistoryRetentionMode);
        ProbeProvider provider = new();
        ProbeClient client = new();
        ChatClientAgent agent = CreateAgent(client, provider);
        EntityBatchResult seed = await DispatchAsync(agent, null, "seed", mailbox: false);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, ReadState(seed).SchemaVersion);
        provider.Stage = stage;
        provider.Failure = new IOException("provider failure");

        EntityBatchResult failed = await DispatchAsync(agent, seed.EntityState, "failed", mailbox: false);

        Assert.Equal(typeof(IOException).FullName, Failure(failed).ErrorType);
        Assert.Equal(seed.EntityState, failed.EntityState);
        Assert.Empty(Actions(failed));
        Assert.Null(ReadState(failed).Data.CompletionReceipts);
        provider.Failure = null;
        EntityBatchResult retry = await DispatchAsync(agent, failed.EntityState, "failed", mailbox: false);
        Assert.Null(Result(retry).FailureDetails);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, ReadState(retry).SchemaVersion);
    }

    [Theory]
    [InlineData("load", "cancel", false)]
    [InlineData("storeAfter", "cancel", false)]
    [InlineData("load", "argument", false)]
    [InlineData("storeAfter", "json", false)]
    [InlineData("load", "corrupt", false)]
    [InlineData("storeAfter", "shutdown", false)]
    [InlineData("load", "cancel", true)]
    [InlineData("storeAfter", "cancel", true)]
    [InlineData("load", "argument", true)]
    [InlineData("storeAfter", "json", true)]
    [InlineData("load", "corrupt", true)]
    [InlineData("storeAfter", "shutdown", true)]
    public async Task CancellationInvalidInputAndCorruptionDoNotCreateCompletionAsync(string stage, string kind, bool first)
    {
        using CancellationTokenSource stopping = new();
        ProbeProvider provider = new();
        ChatClientAgent agent = CreateAgent(new ProbeClient(), provider);
        string? initialState = first ? null : (await DispatchAsync(agent, null, "seed")).EntityState;
        provider.Stage = stage;
        provider.Failure = kind switch
        {
            "cancel" => new OperationCanceledException("cancelled"),
            "argument" => new ArgumentException("invalid provider input"),
            "json" => new JsonException("invalid provider state"),
            "corrupt" => new DurableAgentStateCorruptionException("corrupt provider state"),
            _ => new IOException("provider interrupted"),
        };
        if (kind == "shutdown")
        {
            provider.BeforeFailure = stopping.Cancel;
        }

        EntityBatchResult failed = await DispatchAsync(agent, initialState, "failed", stopping: stopping.Token);

        Assert.NotNull(Result(failed).FailureDetails);
        Assert.Equal(initialState, failed.EntityState);
        Assert.Empty(Actions(failed));
        if (!first)
        {
            Assert.False(ReadState(failed).Data.CompletionReceipts!.ContainsKey("failed"));
        }
    }

    [Fact]
    public async Task ObservationPreservesProviderFiltersSourceAndStateKeysAsync()
    {
        FilteredProvider provider = new();
        ProbeClient client = new();
        ChatClientAgent agent = new(client, new ChatClientAgentOptions { Name = "agent", ChatHistoryProvider = provider });

        EntityBatchResult completed = await DispatchAsync(agent, null, "filtered");

        Assert.Null(Result(completed).FailureDetails);
        Assert.Equal(["history", "request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(AgentRequestMessageSourceType.ChatHistory, client.LastMessages[0].GetAgentRequestMessageSourceType());
        Assert.Equal(["history", "request"], provider.StoredRequests.Select(message => message.Text));
        Assert.Equal("opaque-version1", ReadState(completed).Data.Session!.Value.GetProperty("stateBag").GetProperty("filtered-state").GetString());
        Assert.Same(provider, agent.ChatHistoryProvider);
    }

    [Fact]
    public async Task ModelFailureIsNotMisclassifiedAsHistoryProviderFailureAsync()
    {
        ProbeProvider provider = new();
        ProbeClient client = new() { Failure = new IOException("model failure") };
        EntityBatchResult failed = await DispatchAsync(CreateAgent(client, provider), null, "failed");

        Assert.Equal(typeof(IOException).FullName, Failure(failed).ErrorType);
        Assert.Null(failed.EntityState);
        Assert.Empty(Actions(failed));
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(1, client.Count);
    }

    [Theory]
    [InlineData("state")]
    [InlineData("response")]
    [InlineData("afterDispatch")]
    public async Task SdkCommitFailureRollsBackFailedOutcomeAndOutboxAsync(string boundary)
    {
        ProbeProvider provider = new();
        ChatClientAgent agent = CreateAgent(new ProbeClient(), provider);
        EntityBatchResult seed = await DispatchAsync(agent, null, "seed");
        provider.Stage = "storeAfter";
        provider.Failure = new IOException(PrivateMarker);

        EntityBatchResult failed = await DispatchAsync(agent, seed.EntityState, "failed",
            failBoundary: boundary, resultRetention: TimeSpan.FromSeconds(10));

        Assert.Equal(typeof(CommitFailureException).FullName, Failure(failed).ErrorType);
        Assert.Equal(seed.EntityState, failed.EntityState);
        Assert.Empty(Actions(failed));
        Assert.False(ReadState(failed).Data.CompletionReceipts!.ContainsKey("failed"));

        EntityBatchResult retry = await DispatchAsync(agent, failed.EntityState, "failed");
        Assert.Null(Result(retry).FailureDetails);
        Assert.Equal(3, provider.StoreCount);
        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, ReadState(retry).Data.CompletionReceipts!["failed"].Outcome);
    }

    private static ChatClientAgent CreateAgent(ProbeClient client, ProbeProvider provider) =>
        new(client, new ChatClientAgentOptions { Name = "agent", ChatHistoryProvider = provider });

    private static async Task<EntityBatchResult> DispatchAsync(
        AIAgent agent,
        string? state,
        string correlation,
        bool mailbox = true,
        bool emptyInput = false,
        string operationName = nameof(AgentEntity.Run),
        DateTimeOffset? now = null,
        string? failBoundary = null,
        TimeSpan? resultRetention = null,
        RecordingLogger? logger = null,
        string? providerKey = "probe.v1",
        bool authorizeMigration = false,
        IReadOnlyList<string>? correlations = null,
        CancellationToken stopping = default)
    {
        DurableAgentsOptions options = new()
        {
            EnablePersistentRequestOutcomes = mailbox,
            DefaultTimeToLive = null,
            ResultRetentionPeriod = resultRetention ?? TimeSpan.FromMinutes(1),
            AuthorizeLegacyMigration = authorizeMigration ? _ => true : null,
        };
        options.AddAIAgent(agent, timeToLive: null,
            configureHistory: history => history.ProviderKey = providerKey is null ? null : new(providerKey));
        using ServiceProvider services = new ServiceCollection()
            .AddSingleton(options)
            .AddSingleton(options.GetAgentFactories())
            .AddSingleton(new Mock<DurableTaskClient>("test").Object)
            .AddSingleton<ILoggerFactory>(logger is null ? NullLoggerFactory.Instance : new RecordingLoggerFactory(logger))
            .AddSingleton<TimeProvider>(new Clock(now))
            .AddSingleton(Mock.Of<IHostApplicationLifetime>(lifetime => lifetime.ApplicationStopping == stopping))
            .BuildServiceProvider();
        ITaskEntity entity = new AgentEntity(services);
        if (failBoundary == "afterDispatch")
        {
            entity = new FailAfterDispatch(entity);
        }
        DurableTaskShimFactory factory = new(
            new DurableTaskWorkerOptions { DataConverter = new FailingConverter(failBoundary) },
            NullLoggerFactory.Instance);
        var shim = factory.CreateEntity(new(s_entityId.Name), entity, s_entityId);
        return await shim.ExecuteOperationBatchAsync(new EntityBatchRequest
        {
            InstanceId = s_entityId.ToString(),
            EntityState = state,
            Operations = (correlations ?? [correlation]).Select(id =>
                new OperationRequest
                {
                    Id = Guid.NewGuid(),
                    Operation = operationName,
                    Input = operationName == nameof(AgentEntity.Run)
                        ? s_converter.Serialize(new RunRequest(emptyInput ? [] : [new ChatMessage(ChatRole.User, "request")])
                        {
                            CorrelationId = id,
                        })
                        : null,
                }).ToList(),
        });
    }

    private static DurableAgentState ReadState(EntityBatchResult batch) =>
        Assert.IsType<DurableAgentState>(s_converter.Deserialize(batch.EntityState, typeof(DurableAgentState)));

    private static OperationResult Result(EntityBatchResult batch) =>
        Assert.Single(Assert.IsType<List<OperationResult>>(batch.Results));

    private static global::DurableTask.Core.FailureDetails Failure(EntityBatchResult batch) =>
        Assert.IsType<global::DurableTask.Core.FailureDetails>(Result(batch).FailureDetails);

    private static List<OperationAction> Actions(EntityBatchResult batch) =>
        Assert.IsType<List<OperationAction>>(batch.Actions);

    private sealed class Clock(DateTimeOffset? now = null) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now ?? s_now;
    }

    private sealed class ProbeProvider : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => ["continuation"];
        public string? Stage { get; set; }
        public Exception? Failure { get; set; }
        public Action? BeforeFailure { get; set; }
        public int LoadCount { get; private set; }
        public int StoreCount { get; private set; }
        public int? RestoredVersion { get; private set; }
        public bool SkipContinuationWrite { get; init; }

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context, CancellationToken cancellationToken = default)
        {
            this.LoadCount++;
            if (context.Session!.StateBag.TryGetValue("continuation", out Continuation? value))
            {
                this.RestoredVersion = value?.Version;
            }
            if (this.Stage == "load")
            {
                this.ThrowIfFailed(context.Session);
            }
            return new(this.Stage == "lazyLoad" ? this.LazyFailure(context.Session) : []);
        }

        private IEnumerable<ChatMessage> LazyFailure(AgentSession session)
        {
            this.ThrowIfFailed(session);
            yield break;
        }

        protected override ValueTask StoreChatHistoryAsync(InvokedContext context, CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            if (this.Stage == "storeBefore")
            {
                this.ThrowIfFailed(context.Session!);
            }
            if (!this.SkipContinuationWrite)
            {
                context.Session!.StateBag.SetValue("continuation", new Continuation { Version = this.StoreCount });
            }
            if (this.Stage == "storeAfter")
            {
                this.ThrowIfFailed(context.Session!);
            }
            return default;
        }

        private void ThrowIfFailed(AgentSession session)
        {
            if (this.Failure is not null)
            {
                session.StateBag.SetValue("continuation", new Continuation { Version = 999 });
                session.StateBag.SetValue("partial-state", PrivateMarker);
                this.BeforeFailure?.Invoke();
                throw this.Failure;
            }
        }
    }

    private sealed class ProbeClient : IChatClient
    {
        public int Count { get; private set; }
        public List<ChatMessage> LastMessages { get; private set; } = [];
        public Exception? Failure { get; init; }
        public object? GetService(Type serviceType, object? serviceKey = null) => null;
        public void Dispose() { }
        public Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages, ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.Count++;
            this.LastMessages = messages.ToList();
            await Task.Yield();
            if (this.Failure is not null)
            {
                throw this.Failure;
            }
            yield return new ChatResponseUpdate(ChatRole.Assistant, "response");
        }
    }

    private sealed class FilteredProvider() : ChatHistoryProvider(
        provideOutputMessageFilter: messages => messages.Where(message => message.Text != "discard"),
        storeInputRequestMessageFilter: messages => messages)
    {
        public override IReadOnlyList<string> StateKeys => ["filtered-state"];
        public IReadOnlyList<ChatMessage> StoredRequests { get; private set; } = [];

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context, CancellationToken cancellationToken = default) =>
            new([new ChatMessage(ChatRole.User, "history"), new ChatMessage(ChatRole.User, "discard")]);

        protected override ValueTask StoreChatHistoryAsync(
            InvokedContext context, CancellationToken cancellationToken = default)
        {
            this.StoredRequests = context.RequestMessages.ToList();
            context.Session!.StateBag.SetValue("filtered-state", "opaque-version1");
            return default;
        }
    }

    private sealed class Continuation
    {
        public int Version { get; set; }
    }

    private sealed class DamagedSerializationAgent(AIAgent inner, bool throwOnSerialize) : DelegatingAIAgent(inner)
    {
        protected override async ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default)
        {
            if (throwOnSerialize)
            {
                throw new JsonException("invalid serialized continuation");
            }
            JsonElement serialized = await base.SerializeSessionCoreAsync(session, jsonSerializerOptions, cancellationToken);
            JsonNode root = JsonNode.Parse(serialized.GetRawText())!;
            root["stateBag"]!.AsObject().Remove("continuation");
            return JsonSerializer.SerializeToElement(root);
        }
    }

    private sealed class CommitFailureException : Exception
    {
        public CommitFailureException() { }
        public CommitFailureException(string? message) : base(message) { }
        public CommitFailureException(string? message, Exception? innerException) : base(message, innerException) { }
    }

    private sealed class FailAfterDispatch(ITaskEntity inner) : ITaskEntity
    {
        public async ValueTask<object?> RunAsync(TaskEntityOperation operation)
        {
            _ = await inner.RunAsync(operation);
            DurableAgentState staged = Assert.IsType<DurableAgentState>(operation.State.GetState(typeof(DurableAgentState)));
            Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, staged.Data.CompletionReceipts!["failed"].Outcome);
            throw new CommitFailureException();
        }
    }

    private sealed class FailingConverter(string? boundary) : DataConverter
    {
        public override object? Deserialize(string? data, Type targetType) => s_converter.Deserialize(data, targetType);

        [return: NotNullIfNotNull(nameof(value))]
        public override string? Serialize(object? value)
        {
            if ((boundary == "state" && value is DurableAgentState) ||
                (boundary == "response" && value is AgentResponse))
            {
                throw new CommitFailureException();
            }
            return s_converter.Serialize(value);
        }
    }

    private sealed class RecordingLoggerFactory(RecordingLogger logger) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => logger;
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public void Dispose() { }
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<string> Messages { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            this.Messages.Add(formatter(state, exception) + exception);
    }
}
