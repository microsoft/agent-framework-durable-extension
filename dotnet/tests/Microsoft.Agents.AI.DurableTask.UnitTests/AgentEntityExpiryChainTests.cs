// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;
using static Microsoft.Agents.AI.DurableTask.Tests.Unit.AgentEntityDeliveryTests;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityExpiryChainTests
{
    private static readonly DateTimeOffset s_now = new(2026, 9, 12, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task HundredNewRunsHaveExactlyOnePendingSignalAsync()
    {
        DurableAgentState? state = null;
        List<AgentEntityResultExpirationCheck> signals = [];
        RecordingAgent agent = new("agent");
        for (int run = 0; run < 100; run++)
        {
            EntityHarness harness = CreateHarness(agent, state,
                resultRetentionPeriod: TimeSpan.FromMinutes(20), timeProvider: new Clock(s_now),
                onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
            await harness.RunAsync(new RunRequest("request") { CorrelationId = $"request-{run}" });
            state = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
            Assert.Single(signals);
        }

        Assert.Equal(100, agent.InvocationCount);
        Assert.Equal(100, state!.Data.TerminalResults!.Count);
        Assert.Single(signals);
    }

    [Fact]
    public async Task HundredDuplicateDeliveriesScheduleExactlyOneSuccessorAsync()
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        EntityHarness first = CreateHarness(new RecordingAgent("agent"), state: null,
            resultRetentionPeriod: TimeSpan.FromMinutes(2), timeProvider: new Clock(s_now),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        await first.RunAsync(new RunRequest("request") { CorrelationId = "request" });
        AgentEntityResultExpirationCheck original = Assert.Single(signals);
        DurableAgentState state = Reload(Assert.IsType<DurableAgentState>(first.PersistedState));
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(state, "future",
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "future")), s_now, s_now.AddMinutes(20));
        for (int delivery = 0; delivery < 100; delivery++)
        {
            EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(s_now.AddMinutes(2)),
                onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
            await cleanup.CheckResultsExpirationAsync(original);
            if (cleanup.PersistedState is DurableAgentState committed)
            {
                state = Reload(committed);
            }

            Assert.Equal(2, signals.Count);
            Assert.Equal(delivery == 0, cleanup.StateWasPersisted);
        }

        Assert.Equal("future", Assert.Single(state.Data.TerminalResults!).Key);
        Assert.Equal(2, signals.Count);
    }

    [Fact]
    public async Task EarlierDeadlineSupersedesOnceAndLaterRunsReusePendingCheckAsync()
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState state = await RunAsync(null, "first", 20, signals);
        AgentEntityResultExpirationCheck first = Assert.Single(signals);
        state = await RunAsync(state, "earlier", 5, signals);
        Assert.Equal(2, signals.Count);
        AgentEntityResultExpirationCheck earlier = signals[1];
        Assert.Equal(s_now.AddMinutes(5), earlier.ScheduledTime);
        Assert.NotEqual(first.Token, earlier.Token);
        state = await RunAsync(state, "later", 40, signals);
        Assert.Equal(2, signals.Count);
        Assert.Equal(earlier, Pending(state));
        await AssertStaleAsync(state, first, signals, s_now.AddHours(1));
        Assert.Equal(2, signals.Count);

        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(earlier.ScheduledTime),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        await cleanup.CheckResultsExpirationAsync(earlier);
        state = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
        Assert.Equal(3, signals.Count);
        Assert.Equal(s_now.AddMinutes(20), signals[2].ScheduledTime);
        Assert.Equal(2, state.Data.TerminalResults!.Count);
        await AssertStaleAsync(state, earlier, signals, s_now.AddHours(1));
        Assert.Equal(3, signals.Count);
    }

    [Fact]
    public async Task LaterReplacementKeepsEarlierCheckUntilItConsumesAndRotatesAsync()
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState state = await RunAsync(null, "first", 5, signals);
        AgentEntityResultExpirationCheck first = Assert.Single(signals);
        Assert.True(DurableAgentStateOutcomeResolver.MarkExpiredResultUnavailable(state, "first", s_now.AddMinutes(5)));
        // A compatible import has removed the first payload; the worker clock may be behind it.
        state = await RunAsync(state, "replacement", 20, signals);
        Assert.Single(signals);
        Assert.Equal(first, Pending(state));
        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(first.ScheduledTime),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        await cleanup.CheckResultsExpirationAsync(first);
        state = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
        Assert.Equal(2, signals.Count);
        Assert.Equal(s_now.AddMinutes(20), signals[1].ScheduledTime);
        Assert.NotEqual(first.Token, signals[1].Token);
        await AssertStaleAsync(state, first, signals, s_now.AddHours(1));
        Assert.Equal(2, signals.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OverdueFailedDeliveryRecoveryReplacesStuckTokenExactlyOnceAsync(bool newRun)
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState state = await RunAsync(null, "first", 2, signals);
        state = await RunAsync(state, "future", 20, signals);
        AgentEntityResultExpirationCheck first = Assert.Single(signals);
        string before = Serialize(state);
        int attempts = 0;
        EntityHarness failed = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(s_now.AddMinutes(3)),
            onSignal: (_, _) =>
            {
                attempts++;
                throw new InvalidOperationException("outbox staging failed");
            });
        await Assert.ThrowsAsync<InvalidOperationException>(() => failed.CheckResultsExpirationAsync(first));
        Assert.Equal(1, attempts);
        Assert.False(failed.StateWasPersisted);
        Assert.Equal(before, Serialize(state));

        EntityHarness recovery = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(s_now.AddMinutes(3)),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        if (newRun)
        {
            await recovery.RunAsync(new RunRequest("new") { CorrelationId = "new" });
        }
        else
        {
            await recovery.CheckResultsExpirationAsync();
        }

        state = Reload(Assert.IsType<DurableAgentState>(recovery.PersistedState));
        Assert.Equal(2, signals.Count);
        Assert.NotEqual(first.Token, signals[1].Token);
        Assert.Equal("unavailable", state.Data.CompletionReceipts!["first"].ResultState);
        for (int recoveryCount = 0; recoveryCount < 100; recoveryCount++)
        {
            EntityHarness repeat = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(s_now.AddMinutes(3)),
                onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
            await repeat.CheckResultsExpirationAsync();
            state = Reload(Assert.IsType<DurableAgentState>(repeat.PersistedState));
            Assert.Equal(2, signals.Count);
        }

        await AssertStaleAsync(state, first, signals, s_now.AddHours(1));
        Assert.Equal(2, signals.Count);
    }

    [Fact]
    public async Task ClearedDeletedNonExpiringAndForeignGenerationsNeverReviveOldChainAsync()
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState state = await RunAsync(null, "first", 2, signals);
        AgentEntityResultExpirationCheck first = Assert.Single(signals);
        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(first.ScheduledTime),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        await cleanup.CheckResultsExpirationAsync(first);
        state = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
        Assert.Null(Pending(state));
        Assert.Empty(state.Data.TerminalResults!);
        await AssertStaleAsync(state, first, signals, s_now.AddHours(1));
        await AssertStaleAsync(null, first, signals, s_now.AddHours(1));
        state = await RunAsync(null, "first", null, signals);
        await AssertStaleAsync(state, first, signals, s_now.AddHours(1));
        Assert.Single(signals);
        state = await RunAsync(null, "first", 2, signals);
        Assert.Equal(2, signals.Count);
        Assert.NotEqual(first.Token, signals[1].Token);
        await AssertStaleAsync(state, first, signals, s_now.AddHours(1));
        await AssertStaleAsync(state, signals[1] with { EntityId = "foreign" }, signals, s_now);
        await AssertStaleAsync(state, signals[1] with { ScheduledTime = s_now.AddMinutes(3) }, signals, s_now);
        await AssertStaleAsync(state, new AgentEntityResultExpirationCheck(signals[1].ScheduledTime), signals, s_now);
        Assert.Equal(2, signals.Count);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("""{"version":2,"entityId":"@dafx-agent@session","scheduledResultExpiryUtc":null,"token":null}""")]
    [InlineData("""{"version":1,"entityId":"foreign","scheduledResultExpiryUtc":null,"token":null}""")]
    [InlineData("""{"version":1,"version":1,"entityId":"@dafx-agent@session","scheduledResultExpiryUtc":null,"token":null}""")]
    [InlineData("""{"version":1,"entityId":"@dafx-agent@session","scheduledResultExpiryUtc":"bad","token":"abc"}""")]
    [InlineData("""{"version":1,"entityId":"@dafx-agent@session","scheduledResultExpiryUtc":null,"token":"abc"}""")]
    [InlineData("""{"version":1,"entityId":"@dafx-agent@session","scheduledResultExpiryUtc":"2026-09-12T00:00:00Z","token":"00000000000000000000000000000000"}""")]
    [InlineData("""{"version":"1","entityId":"@dafx-agent@session","scheduledResultExpiryUtc":null,"token":null}""")]
    [InlineData("""{"version":1,"entityId":"@dafx-agent@session","scheduledResultExpiryUtc":0,"token":null}""")]
    [InlineData("""{"version":1,"entityId":"@dafx-agent@session","scheduledResultExpiryUtc":null}""")]
    [InlineData("""{"version":1,"entityId":"@dafx-agent@session","scheduledResultExpiryUtc":"2026-09-12T02:00:00+02:00","token":"5b9ddf23b2d94e42b1dd4d946134f043"}""")]
    public async Task InvalidFutureProfileBlocksWritersBeforeModelButRemainsOpaqueForDeliveryAsync(string profile)
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState original = await RunAsync(null, "first", 20, signals);
        using JsonDocument document = JsonDocument.Parse(profile);
        DurableAgentState state = WithProfile(original, document.RootElement.Clone());
        string before = Serialize(state);
        RecordingAgent agent = new("agent");
        EntityHarness writer = CreateHarness(agent, state, timeProvider: new Clock(s_now),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => writer.RunAsync(new RunRequest("new") { CorrelationId = "new" }));
        Assert.Contains(AgentEntityResultExpirySchedule.ExtensionName, error.Message);
        Assert.Equal(0, agent.InvocationCount);
        Assert.False(writer.StateWasPersisted);
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.CheckResultsExpirationAsync());
        Assert.Equal(before, Serialize(state));
        Assert.Single(signals);

        AgentResponse duplicate = await writer.RunAsync(new RunRequest([]) { CorrelationId = "first" });
        Assert.Equal("response", duplicate.Text);
        Assert.Equal(0, agent.InvocationCount);
        Assert.Equal(before, Serialize(state));
        AgentRunHandle handle = AgentRunHandleTests.CreateHandle(state, correlationId: "first", timeProvider: new Clock(s_now));
        Assert.Equal("response", (await handle.ReadAgentResponseAsync()).Text);
        Assert.Equal(before, Serialize(state));
    }

    [Fact]
    public async Task ProfileUnknownFieldsAndOtherExtensionsSurviveRotationAndClearAsync()
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState state = await RunAsync(null, "first", 20, signals);
        Dictionary<string, JsonElement> profile = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            state.ExtensionData![AgentEntityResultExpirySchedule.ExtensionName])!;
        profile["future"] = JsonSerializer.SerializeToElement(new { flag = false, data = (string?)null });
        state = WithProfile(state, JsonSerializer.SerializeToElement(profile));
        state.ExtensionData!["application"] = JsonSerializer.SerializeToElement(new List<int> { 0, 1 });
        state.UnknownProperties = new Dictionary<string, JsonElement> { ["rootFuture"] = JsonSerializer.SerializeToElement(false) };
        string before = Serialize(state);
        DurableAgentState rotated = await RunAsync(state, "earlier", 5, signals);
        Assert.Equal(before, Serialize(state));
        Assert.Equal(2, signals.Count);
        AssertPreserved(rotated);
        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), rotated, timeProvider: new Clock(s_now.AddHours(1)));
        await cleanup.CheckResultsExpirationAsync(signals[1]);
        DurableAgentState cleared = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
        Assert.Null(Pending(cleared));
        AssertPreserved(cleared);

        void AssertPreserved(DurableAgentState value)
        {
            Assert.True(JsonElement.DeepEquals(profile["future"], value.ExtensionData![AgentEntityResultExpirySchedule.ExtensionName].GetProperty("future")));
            Assert.True(JsonElement.DeepEquals(state.ExtensionData!["application"], value.ExtensionData["application"]));
            Assert.False(value.UnknownProperties!["rootFuture"].GetBoolean());
        }
    }

    [Theory]
    [InlineData("model")]
    [InlineData("serialization")]
    [InlineData("cancel-before")]
    [InlineData("cancel-after-signal")]
    [InlineData("signal")]
    [InlineData("state")]
    public async Task FailedRunLeavesPendingTokenAndRetrySchedulesExactlyOnceAsync(string failure)
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState state = await RunAsync(null, "first", 20, signals);
        string before = Serialize(state);
        using CancellationTokenSource cancellation = new();
        if (failure == "cancel-before")
        {
            cancellation.Cancel();
        }

        int attempts = 0;
        RecordingAgent agent = new("agent")
        {
            Exception = failure == "model" ? new InvalidOperationException("model failed") : null,
            UnsupportedResponseMetadata = failure == "serialization" ? new object() : null,
        };
        EntityHarness failed = CreateHarness(agent, state, timeProvider: new Clock(s_now),
            resultRetentionPeriod: TimeSpan.FromMinutes(2), cancellationToken: cancellation.Token,
            onCommit: _ =>
            {
                if (failure == "state")
                {
                    throw new InvalidOperationException("state failed");
                }
            },
            onSignal: (_, _) =>
            {
                attempts++;
                if (failure == "signal")
                {
                    throw new InvalidOperationException("signal failed");
                }

                if (failure == "cancel-after-signal")
                {
                    cancellation.Cancel();
                }
            });
        await Assert.ThrowsAnyAsync<Exception>(() => failed.RunAsync(new RunRequest("retry") { CorrelationId = "retry" }));
        Assert.Equal(failure is "signal" or "state" or "cancel-after-signal" ? 1 : 0, attempts);
        Assert.False(failed.StateWasPersisted);
        Assert.Equal(before, Serialize(state));
        DurableAgentState retried = await RunAsync(state, "retry", 2, signals);
        Assert.Equal(2, signals.Count);
        Assert.Equal(2, retried.Data.TerminalResults!.Count);
        Assert.Equal(signals[1], Pending(retried));
        retried = await RunAsync(retried, "retry", 2, signals);
        Assert.Equal(2, signals.Count);
        Assert.Equal(2, retried.Data.CompletionReceipts!.Count);
    }

    [Theory]
    [InlineData("cancel-before")]
    [InlineData("cancel-after-signal")]
    [InlineData("signal")]
    [InlineData("state")]
    [InlineData("serialization")]
    [InlineData("invalid-mailbox")]
    public async Task FailedCleanupRetainsTokenAndRetryConsumesItExactlyOnceAsync(string failure)
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState state = await RunAsync(null, "first", 2, signals);
        state = await RunAsync(state, "future", 20, signals);
        AgentEntityResultExpirationCheck first = Assert.Single(signals);
        string before = Serialize(state);
        DurableAgentState attempted = Reload(state);
        if (failure == "serialization")
        {
            attempted.UnknownProperties = new Dictionary<string, JsonElement> { ["invalid"] = default };
        }
        else if (failure == "invalid-mailbox")
        {
            attempted.Data.CompletionReceipts!.Remove("first");
        }

        using CancellationTokenSource cancellation = new();
        if (failure == "cancel-before")
        {
            cancellation.Cancel();
        }

        int attempts = 0;
        EntityHarness failed = CreateHarness(new RecordingAgent("agent"), attempted,
            timeProvider: new Clock(s_now.AddMinutes(3)), cancellationToken: cancellation.Token,
            onCommit: _ =>
            {
                if (failure == "state")
                {
                    throw new InvalidOperationException("state failed");
                }
            },
            onSignal: (_, _) =>
            {
                attempts++;
                if (failure == "signal")
                {
                    throw new InvalidOperationException("signal failed");
                }

                if (failure == "cancel-after-signal")
                {
                    cancellation.Cancel();
                }
            });
        await Assert.ThrowsAnyAsync<Exception>(() => failed.CheckResultsExpirationAsync(first));
        Assert.Equal(failure is "signal" or "state" or "cancel-after-signal" ? 1 : 0, attempts);
        Assert.False(failed.StateWasPersisted);
        Assert.Equal(first, Pending(attempted));
        Assert.Equal(2, attempted.Data.TerminalResults!.Count);
        Assert.Equal(before, Serialize(state));
        if (failure is not ("serialization" or "invalid-mailbox"))
        {
            Assert.Equal(before, Serialize(attempted));
        }

        EntityHarness retry = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(s_now.AddMinutes(3)),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        await retry.CheckResultsExpirationAsync(first);
        DurableAgentState retried = Reload(Assert.IsType<DurableAgentState>(retry.PersistedState));
        Assert.Equal(2, signals.Count);
        Assert.Equal(signals[1], Pending(retried));
        Assert.Equal("future", Assert.Single(retried.Data.TerminalResults!).Key);
        await AssertStaleAsync(retried, first, signals, s_now.AddMinutes(3));
        Assert.Equal(2, signals.Count);
    }

    [Fact]
    public async Task PublicRetentionSettingCannotEnableProductionMailboxWritesAsync()
    {
        int signals = 0;
        EntityHarness production = CreateHarness(new RecordingAgent("agent"), state: null, enableMailboxWrites: false,
            authorizeLegacyMigration: false, resultRetentionPeriod: TimeSpan.FromMinutes(2), timeProvider: new Clock(s_now),
            onSignal: (_, _) => signals++);
        await production.RunAsync(new RunRequest("request") { CorrelationId = "request" });
        DurableAgentState state = Reload(Assert.IsType<DurableAgentState>(production.PersistedState));
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, state.SchemaVersion);
        Assert.Null(state.Data.TerminalResults);
        Assert.Null(state.Data.CompletionReceipts);
        Assert.Null(state.ExtensionData);
        Assert.Equal(0, signals);
        Assert.Null(typeof(DurableAgentsOptions).GetProperty("EnableMailboxWrites"));
        Assert.Null(typeof(DurableAgentsOptions).GetProperty("EnableMailboxEntityDeletion"));
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.1.0")]
    [InlineData("1.2.0")]
    public async Task LegacyWriterDoesNotInterpretOrCreateRuntimeProfileAsync(string version)
    {
        DurableAgentState state = new()
        {
            SchemaVersion = version,
            ExtensionData = new Dictionary<string, JsonElement>
            {
                [AgentEntityResultExpirySchedule.ExtensionName] = JsonSerializer.SerializeToElement(false),
            },
        };
        int signals = 0;
        EntityHarness production = CreateHarness(new RecordingAgent("agent"), state, enableMailboxWrites: false,
            authorizeLegacyMigration: false, resultRetentionPeriod: TimeSpan.FromMinutes(2), timeProvider: new Clock(s_now),
            onSignal: (_, _) => signals++);
        await production.RunAsync(new RunRequest("request") { CorrelationId = "request" });
        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(production.PersistedState));
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, committed.SchemaVersion);
        Assert.False(committed.ExtensionData![AgentEntityResultExpirySchedule.ExtensionName].GetBoolean());
        Assert.Null(committed.Data.TerminalResults);
        Assert.Equal(0, signals);
        Assert.Equal(version, state.SchemaVersion);
    }

    [Theory]
    [InlineData("1.0.0", false)]
    [InlineData("1.0.0", true)]
    [InlineData("1.1.0", false)]
    [InlineData("1.1.0", true)]
    [InlineData("1.2.0", false)]
    [InlineData("1.2.0", true)]
    public async Task LegacyStaleChecksHaveZeroSettersAndSignalsBeforeAndAfterReloadAsync(string version, bool tokenBearing)
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        _ = await RunAsync(null, "old-generation", 2, signals);
        AgentEntityResultExpirationCheck check = tokenBearing
            ? Assert.Single(signals) : new AgentEntityResultExpirationCheck(signals[0].ScheduledTime);
        DurableAgentState state = new()
        {
            SchemaVersion = version,
            ExtensionData = new Dictionary<string, JsonElement>
            {
                // Legacy state must preserve, but never interpret, even an unknown profile shape.
                [AgentEntityResultExpirySchedule.ExtensionName] = JsonSerializer.SerializeToElement(false),
            },
        };
        await AssertInputBearingCheckIsInertAsync(state, check);
        await AssertInputBearingCheckIsInertAsync(Reload(state), check);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeletedAndRecreatedDefaultOffLegacyEntityRejectsOldGenerationAsync(bool tokenBearing)
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        _ = await RunAsync(null, "old-generation", 2, signals);
        AgentEntityResultExpirationCheck check = tokenBearing
            ? Assert.Single(signals) : new AgentEntityResultExpirationCheck(signals[0].ScheduledTime);
        await AssertInputBearingCheckIsInertAsync(null, check);

        // Recreate the same entity identity from deleted state using production's default-off gates.
        EntityHarness recreated = CreateHarness(new RecordingAgent("agent"), state: null,
            enableMailboxWrites: false, authorizeLegacyMigration: false);
        await recreated.RunAsync(new RunRequest("new") { CorrelationId = "new-generation" });
        DurableAgentState state = Reload(Assert.IsType<DurableAgentState>(recreated.PersistedState));
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, state.SchemaVersion);
        Assert.Null(state.ExtensionData);
        await AssertInputBearingCheckIsInertAsync(state, check);
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.1.0")]
    [InlineData("1.2.0")]
    [InlineData("2.0.0")]
    [InlineData(null)]
    public async Task InputBearingNullIsNotExplicitRecoveryAsync(string? version)
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState? state = version switch
        {
            null => null,
            "2.0.0" => await RunAsync(null, "expired", 2, signals),
            _ => new DurableAgentState { SchemaVersion = version },
        };
        await AssertInputBearingCheckIsInertAsync(state is null ? null : Reload(state), null);
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.1.0")]
    [InlineData("1.2.0")]
    [InlineData("0.0.0")]
    [InlineData("1.3.0")]
    [InlineData("3.0.0")]
    [InlineData("invalid")]
    public async Task InputBearingChecksDoNotHideMixedOrUnsupportedStateAsync(string version)
    {
        DurableAgentState state = new()
        {
            SchemaVersion = version,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
            },
        };
        int writes = 0;
        int signals = 0;
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state,
            enableMailboxWrites: false, authorizeLegacyMigration: false,
            onCommit: _ => writes++, onSignal: (_, _) => signals++);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.CheckResultsExpirationAsync(new AgentEntityResultExpirationCheck(s_now)));
        Assert.Equal(0, writes);
        Assert.Equal(0, signals);
    }

    private static async Task AssertInputBearingCheckIsInertAsync(
        DurableAgentState? state, AgentEntityResultExpirationCheck? check)
    {
        string? before = state is null ? null : Serialize(state);
        int writes = 0;
        int signals = 0;
        RecordingAgent agent = new("agent");
        EntityHarness harness = CreateHarness(agent, state,
            enableMailboxWrites: false, authorizeLegacyMigration: false, timeProvider: new Clock(s_now.AddHours(1)),
            registerWithFactory: true, onFactoryInvoked: () => Assert.Fail("stale check invoked factory"),
            onCommit: _ => writes++, onSignal: (_, _) => signals++);
        await harness.CheckResultsExpirationAsync(check, hasInput: true);
        Assert.Equal(0, writes);
        Assert.Equal(0, signals);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(0, agent.InvocationCount);
        Assert.Equal(before, state is null ? null : Serialize(state));
    }

    [Theory]
    [InlineData(-120)]
    [InlineData(0)]
    [InlineData(120)]
    public async Task ImportedOffsetDeadlinesPersistUtcScheduleAndSurviveReloadAsync(int offsetMinutes)
    {
        List<AgentEntityResultExpirationCheck> signals = [];
        DurableAgentState state = await RunAsync(null, "forever", null, signals);
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(state, "imported",
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "imported")), s_now,
            s_now.AddMinutes(20).ToOffset(TimeSpan.FromMinutes(offsetMinutes)));
        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(s_now),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        await cleanup.CheckResultsExpirationAsync();
        state = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
        AgentEntityResultExpirationCheck pending = Assert.Single(signals);
        Assert.Equal(TimeSpan.Zero, pending.ScheduledTime.Offset);
        Assert.Equal(s_now.AddMinutes(20), pending.ScheduledTime);
        Assert.Equal(pending, Pending(state));
        state = await RunAsync(state, "next", 40, signals);
        Assert.Single(signals);
        Assert.Equal(pending, Pending(state));
    }

    private static async Task<DurableAgentState> RunAsync(
        DurableAgentState? state, string correlation, int? retentionMinutes, List<AgentEntityResultExpirationCheck> signals)
    {
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(s_now),
            resultRetentionPeriod: retentionMinutes.HasValue ? TimeSpan.FromMinutes(retentionMinutes.Value) : null,
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        await harness.RunAsync(new RunRequest("request") { CorrelationId = correlation });
        return Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
    }

    private static async Task AssertStaleAsync(
        DurableAgentState? state, AgentEntityResultExpirationCheck signal, List<AgentEntityResultExpirationCheck> signals, DateTimeOffset now)
    {
        int beforeCount = signals.Count;
        string? before = state is null ? null : Serialize(state);
        EntityHarness stale = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(now),
            registerWithFactory: true, onFactoryInvoked: () => Assert.Fail("stale signal invoked factory"),
            onCommit: _ => Assert.Fail("stale signal wrote state"),
            onSignalInput: input => signals.Add(Assert.IsType<AgentEntityResultExpirationCheck>(input)));
        await stale.CheckResultsExpirationAsync(signal);
        Assert.Equal(beforeCount, signals.Count);
        Assert.Equal(before, state is null ? null : Serialize(state));
    }

    private static AgentEntityResultExpirationCheck? Pending(DurableAgentState state) =>
        AgentEntityResultExpirySchedule.Read(state, new AgentSessionId("agent", "session").ToString())?.Pending;

    private static DurableAgentState WithProfile(DurableAgentState state, JsonElement profile) => new()
    {
        SchemaVersion = state.SchemaVersion,
        MailboxWritesAuthorized = true,
        Data = state.Data,
        ExtensionData = new Dictionary<string, JsonElement> { [AgentEntityResultExpirySchedule.ExtensionName] = profile },
        UnknownProperties = state.UnknownProperties,
    };

    private static string Serialize(DurableAgentState state) =>
        JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);

    private static DurableAgentState Reload(DurableAgentState state) =>
        JsonSerializer.Deserialize(JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState),
            DurableAgentStateJsonContext.Default.DurableAgentState)!;

    private sealed class Clock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
