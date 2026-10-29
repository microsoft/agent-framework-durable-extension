// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using AutoHistoryRetention;
using DurableTask.Core.Command;
using DurableTask.Core.Entities;
using DurableTask.Core.Entities.OperationFormat;
using DurableTask.Core.History;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Worker;
using Microsoft.DurableTask.Worker.Shims;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityPublicRetentionTests
{
    private const string LegacyEntitiesEnvironment = "DURABLE_AGENTS_PROCESS_LEGACY_ENTITIES";
    private const string ProbeEnvironment = "PUBLIC_RETENTION_CONFIGURATION_PROBE";
    private static readonly DateTimeOffset s_now = new(2026, 10, 14, 9, 0, 0, TimeSpan.Zero);
    private static readonly EntityId s_id = new(AgentSessionId.ToEntityName("agent"), "session");
    private static readonly EntityId s_legacyId = new(AgentSessionId.ToLegacyEntityName("agent"), "session");
    private static readonly DurableDataConverter s_converter = new();

    [Theory]
    [InlineData(null, null, false, false, true)]
    [InlineData(null, null, true, false, true)]
    [InlineData(null, "true", false, true, true)]
    [InlineData(null, "TRUE", true, true, true)]
    [InlineData(null, "1", true, true, true)]
    [InlineData(null, "false", false, false, true)]
    [InlineData(null, "FALSE", true, false, true)]
    [InlineData(null, "0", true, false, true)]
    [InlineData(true, null, false, true, true)]
    [InlineData(false, null, true, false, true)]
    [InlineData(true, "true", true, true, true)]
    [InlineData(false, "false", false, false, true)]
    [InlineData(false, "true", false, false, false)]
    [InlineData(true, "false", true, false, false)]
    [InlineData(null, "", false, false, false)]
    [InlineData(null, " true", false, false, false)]
    [InlineData(null, "true ", true, false, false)]
    [InlineData(null, "yes", false, false, false)]
    public Task ConfigurationIsValidatedAndCapturedAtFinalizationAsync(
        bool? configuredProcessLegacy,
        string? envProcessLegacy,
        bool auto,
        bool expectedProcessLegacy,
        bool valid) =>
        RunConfigurationProbeAsync(new(
            Auto: auto,
            Valid: valid,
            ProcessLegacyCode: configuredProcessLegacy,
            ProcessLegacyEnv: envProcessLegacy,
            ExpectedProcessLegacy: expectedProcessLegacy));

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(true, true, false)]
    [InlineData(null, false, false)]
    [InlineData(null, true, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, true)]
    [InlineData(null, false, true)]
    [InlineData(null, true, true)]
    public Task AdditiveConfigurationFinalizesBothOrdersAsync(bool? configuredProcessLegacy, bool reverse, bool split) =>
        RunConfigurationProbeAsync(new(
            Auto: true,
            Valid: true,
            Reverse: reverse,
            Split: split,
            LateEnvironment: true,
            ProcessLegacyCode: configuredProcessLegacy,
            ProcessLegacyEnv: configuredProcessLegacy is null ? "true" : null,
            ExpectedProcessLegacy: true));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task InvalidFinalWatermarksFailBeforeRuntimeCreationAsync(bool reverse) =>
        RunConfigurationProbeAsync(new(Auto: true, Valid: false, Reverse: reverse, Split: true));

    private static async Task RunConfigurationProbeAsync(ConfigurationCase test)
    {
        ProcessStartInfo start = new("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(typeof(AgentEntityPublicRetentionTests).Assembly.Location);
        start.ArgumentList.Add("--filter-method");
        start.ArgumentList.Add("*EnvironmentProbeAsync");
        start.ArgumentList.Add("--no-progress");
        start.Environment.Remove(LegacyEntitiesEnvironment);
        if (test.ProcessLegacyEnv is not null)
        {
            start.Environment[LegacyEntitiesEnvironment] = test.ProcessLegacyEnv;
        }
        start.Environment[ProbeEnvironment] = JsonSerializer.Serialize(test);
        using Process child = Process.Start(start)!;
        Task<string> output = child.StandardOutput.ReadToEndAsync();
        Task<string> error = child.StandardError.ReadToEndAsync();
        await child.WaitForExitAsync(TestContext.Current.CancellationToken);
        Assert.True(child.ExitCode == 0, (await output) + (await error));
    }

    [Fact]
    public async Task EnvironmentProbeAsync()
    {
        string? raw = Environment.GetEnvironmentVariable(ProbeEnvironment);
        if (raw is null)
        {
            return;
        }
        ConfigurationCase test = JsonSerializer.Deserialize<ConfigurationCase>(raw)!;
        int callbacks = 0;
        int workerRegistrations = 0;
        int clientInitializations = 0;
        int agentFactories = 0;
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        IServiceCollection services = builder.Services;
        if (test.LateEnvironment)
        {
            Environment.SetEnvironmentVariable(LegacyEntitiesEnvironment, "invalid-during-registration");
        }
        void LegacyPolicy(DurableAgentsOptions options)
        {
            if (test.ProcessLegacyCode is bool processLegacy)
            {
                options.ProcessLegacyEntities = processLegacy;
            }
            if (test.Split)
            {
                options.HistoryRetentionLowWatermark = test.Valid ? .4 : .8;
                options.MaxStateBytes = null;
            }
        }
        void Retention(DurableAgentsOptions options)
        {
            options.HistoryRetentionMode = test.Auto ? DurableAgentHistoryRetentionMode.Auto : DurableAgentHistoryRetentionMode.KeepAll;
            if (test.Split)
            {
                options.HistoryRetentionHighWatermark = .6;
            }
        }
        services.ConfigureDurableAgents(options =>
        {
            if (test.Reverse)
            {
                LegacyPolicy(options);
            }
            else
            {
                Retention(options);
            }
            options.AddAIAgentFactory("agent", _ =>
            {
                agentFactories++;
                throw new InvalidOperationException("Factory must not run.");
            });
        }, workerBuilder: worker =>
        {
            callbacks++;
            worker.AddTasks(_ => workerRegistrations++);
            worker.UseGrpc();
        }, clientBuilder: client =>
        {
            callbacks++;
            client.Services.PostConfigure<DurableTaskClientOptions>(_ => clientInitializations++);
            client.UseGrpc();
        });
        services.ConfigureDurableAgents(test.Reverse ? Retention : LegacyPolicy);
        services.ConfigureDurableOptions(_ => { });
        services.ConfigureDurableWorkflows(_ => { });
        if (test.LateEnvironment)
        {
            Environment.SetEnvironmentVariable(LegacyEntitiesEnvironment, test.ProcessLegacyEnv);
        }
        Assert.Equal(2, callbacks);
        using IHost host = builder.Build();
        if (!test.Valid)
        {
            Assert.Throws<InvalidOperationException>(() => host.Services.GetRequiredService<DurableAgentsOptions>());
            Assert.Throws<InvalidOperationException>(() => host.Services.GetDurableAgentProxy("agent"));
            Assert.Throws<InvalidOperationException>(() => host.Services.GetRequiredService<DurableTaskClient>());
            Assert.Throws<InvalidOperationException>(() => host.Services.GetRequiredService<IDurableTaskClientProvider>());
            Assert.Throws<InvalidOperationException>(() => host.Services.GetServices<IHostedService>());
            await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(TestContext.Current.CancellationToken));
            Assert.Equal(0, workerRegistrations);
            Assert.Equal(0, clientInitializations);
            Assert.Equal(0, agentFactories);
            return;
        }
        DurableAgentsOptions captured = host.Services.GetRequiredService<DurableAgentsOptions>();
        Assert.True(captured.EnablePersistentRequestOutcomes);
        Assert.Equal(test.ExpectedProcessLegacy, captured.ProcessLegacyEntities);
        DurableAgentRetentionSettings policy = captured.GetRetentionSettings();
        Assert.Equal(test.Auto ? DurableAgentHistoryRetentionMode.Auto : DurableAgentHistoryRetentionMode.KeepAll, policy.Mode);
        Assert.Equal(test.Auto ? 1_048_576 : null, policy.MaxStateBytes);
        Assert.Equal(test.Split ? .6 : .85, policy.HighWatermark);
        Assert.Equal(test.Split ? .4 : .70, policy.LowWatermark);
        Environment.SetEnvironmentVariable(LegacyEntitiesEnvironment, "invalid-after-capture");
        Assert.NotNull(host.Services.GetRequiredService<DurableTaskClient>());
        Assert.NotNull(host.Services.GetRequiredService<IDurableTaskClientProvider>());
        Assert.NotEmpty(host.Services.GetServices<IHostedService>());
        Assert.Equal(1, workerRegistrations);
        Assert.Equal(1, clientInitializations);
        Assert.Equal(0, agentFactories);
        Assert.Equal(test.ExpectedProcessLegacy, captured.ProcessLegacyEntities);
        Assert.Equal(policy, captured.GetRetentionSettings());
    }

    [Fact]
    public void PublicOptionsAndSampleConfigurationKeepTheirContracts()
    {
        Assert.Null(typeof(DurableAgentsOptions).Assembly.GetType("Microsoft.Agents.AI.DurableTask.DurableAgentDeploymentMode"));
        Assert.Null(typeof(DurableAgentsOptions).GetProperty("DeploymentMode"));
        DurableAgentsOptions options = new();
        Assert.True(options.EnablePersistentRequestOutcomes);
        Assert.False(options.ProcessLegacyEntities);
        HistoryRetentionDemo.ConfigureRetention(options);
        Assert.Equal(DurableAgentHistoryRetentionMode.Auto, options.HistoryRetentionMode);
        Assert.Equal(32 * 1024, options.GetRetentionSettings().MaxStateBytes);
        int callbacks = 0;
        ServiceCollection services = new();
        services.ConfigureDurableAgents(config =>
        {
            config.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
            config.HistoryRetentionHighWatermark = 0.4;
            config.HistoryRetentionLowWatermark = 0.8;
            config.AddAIAgentFactory("never", _ => throw new InvalidOperationException("Factory must not run."));
        }, workerBuilder: _ => callbacks++, clientBuilder: _ => callbacks++);
        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<DurableAgentsOptions>());
        Assert.Equal(2, callbacks);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HostWithoutSdkBuildersValidatesBeforeStartingServicesAsync(bool valid)
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.ConfigureDurableAgents(options =>
        {
            options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
            options.HistoryRetentionHighWatermark = 0.6;
        });
        builder.Services.ConfigureDurableOptions(
            options => options.Agents.HistoryRetentionLowWatermark = valid ? 0.4 : 0.8);
        using IHost host = builder.Build();
        if (valid)
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(DurableAgentHistoryRetentionMode.Auto, host.Services.GetRequiredService<DurableAgentsOptions>().HistoryRetentionMode);
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(TestContext.Current.CancellationToken));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RetentionPreservesReceiptsAcrossColdDuplicatesExpiryAndRollbackAsync(bool auto)
    {
        Client client = new();
        EntityBatchResult legacy = await DispatchAsync(Agent(client), null, schema: false);
        Assert.Equal("1.2.0", State(legacy).SchemaVersion);
        Assert.Null(State(legacy).Data.CompletionReceipts);
        string? json = null;
        for (int turn = 1; turn <= HistoryRetentionDemo.ScenarioTurns; turn++)
        {
            EntityBatchResult run = await DispatchAsync(Agent(client), json, $"turn-{turn}", auto,
                text: turn == 1 ? "MARKER-" + new string('x', 4_096) : new string('x', 4_096),
                configure: options =>
                {
                    if (auto)
                    {
                        HistoryRetentionDemo.ConfigureRetention(options);
                    }
                    else
                    {
                        options.MaxStateBytes = 1;
                        options.HistoryRetentionHighWatermark = .2;
                        options.HistoryRetentionLowWatermark = .9;
                    }
                });
            Assert.Null(Result(run).FailureDetails);
            json = run.EntityState;
        }
        DurableAgentState state = StateJson(json);
        Assert.Equal("2.0.0", state.SchemaVersion);
        Assert.Equal(7, state.Data.CompletionReceipts!.Count);
        Assert.Equal(auto, state.Data.Truncation is not null);
        Assert.Null(state.Data.ExpirationTimeUtc);
        Client cold = new();
        EntityBatchResult duplicate = await DispatchAsync(Agent(cold), " \n" + json, "turn-1", schema: false);
        Assert.Null(Result(duplicate).FailureDetails);
        Assert.Equal(" \n" + json, duplicate.EntityState);
        EntityBatchResult denied = await DispatchAsync(Agent(cold), json, "new", schema: false);
        Assert.Equal(typeof(InvalidOperationException).FullName, Result(denied).FailureDetails!.ErrorType);
        Assert.Equal(json, denied.EntityState);
        Assert.Equal(0, cold.Calls);
        EntityBatchResult next = await DispatchAsync(Agent(cold), json, "diagnostic", auto);
        Assert.DoesNotContain(cold.Messages, message => auto && message.Text?.Contains("MARKER-", StringComparison.Ordinal) is true);
        EntityBatchResult cleanup = await DispatchAsync(Agent(cold), next.EntityState, auto: auto,
            operation: nameof(AgentEntity.CheckAndExpireResults), now: s_now.AddMinutes(2));
        Assert.Empty(State(cleanup).Data.TerminalResults!);
        int calls = cold.Calls;
        EntityBatchResult expired = await DispatchAsync(Agent(cold), cleanup.EntityState, "turn-1", auto, now: s_now.AddMinutes(2));
        Assert.Equal(typeof(DurableAgentResultUnavailableException).FullName, Result(expired).FailureDetails!.ErrorType);
        Assert.Equal(cleanup.EntityState, expired.EntityState);
        Assert.Equal(calls, cold.Calls);
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.1.0")]
    [InlineData("1.2.0")]
    public async Task LegacyCleanupCannotAuthorizeEmptyExpiredOrUsedStateAsync(string version)
    {
        string raw = $$$""" {"schemaVersion":"{{{version}}}","data":{"conversationHistory":[],"expirationTimeUtc":"2020-01-01T00:00:00Z"}} """;
        Client client = new();
        foreach (string cleanup in new[] { nameof(AgentEntity.CheckAndDeleteIfExpired), nameof(AgentEntity.CheckAndExpireResults) })
        {
            EntityBatchResult batch = await DispatchAsync(Agent(client), raw, auto: true, operations:
            [
                Request(cleanup), Request(nameof(AgentEntity.Run), "work"),
            ]);
            Assert.Equal(raw, batch.EntityState);
            Assert.Null(batch.Results![0].FailureDetails);
            Assert.NotNull(batch.Results[1].FailureDetails);
            Assert.Empty(batch.Actions!);
            EntityBatchResult fresh = await DispatchAsync(Agent(client), null, auto: true, operations:
            [
                Request(cleanup), Request(nameof(AgentEntity.Run), "work"),
            ]);
            Assert.Null(fresh.Results![1].FailureDetails);
            Assert.Single(State(fresh).Data.CompletionReceipts!);
        }
        DurableAgentState used = new() { SchemaVersion = version };
        used.Data.ConversationHistory.Add(DurableAgentStateResponse.FromResponse("old", new AgentResponse([new ChatMessage(ChatRole.Assistant, "old")])));
        string original = s_converter.Serialize(used);
        int calls = client.Calls;
        EntityBatchResult rejected = await DispatchAsync(Agent(client), original, "old", auto: true);
        Assert.NotNull(Result(rejected).FailureDetails);
        Assert.Equal(original, rejected.EntityState);
        Assert.Equal(calls, client.Calls);
    }

    [Theory]
    [InlineData(false, "entity")]
    [InlineData(true, "entity")]
    [InlineData(false, "external")]
    [InlineData(true, "external")]
    [InlineData(false, "service")]
    [InlineData(true, "service")]
    public async Task OwnersRestoreWithoutMirroringOrFallbackAsync(bool auto, string owner)
    {
        Provider? provider = owner == "external" ? new() : null;
        Client client = new() { Conversation = owner == "service" ? "remote" : null };
        EntityBatchResult seed = await DispatchAsync(Agent(client, provider), null, "old", auto);
        Assert.Null(Result(seed).FailureDetails);
        if (owner != "entity")
        {
            Assert.Empty(State(seed).Data.ConversationHistory);
        }
        Client cold = new() { Conversation = client.Conversation };
        Provider? restored = provider is null ? null : new();
        if (owner == "service")
        {
            EntityBatchResult failed = await DispatchAsync(Agent(new Client { Fail = true }), seed.EntityState, "next", auto);
            Assert.Equal(typeof(IOException).FullName, Result(failed).FailureDetails!.ErrorType);
            Assert.Equal(seed.EntityState, failed.EntityState);
            Assert.Empty(failed.Actions!);
        }
        EntityBatchResult next = await DispatchAsync(Agent(cold, restored), seed.EntityState, "next", auto);
        Assert.Null(Result(next).FailureDetails);
        Assert.Equal(1, cold.Calls);
        Assert.Equal(owner == "entity" ? 3 : 1, cold.Messages.Count);
        if (restored is not null)
        {
            Assert.Equal(1, restored.Restored);
            EntityBatchResult mismatch = await DispatchAsync(Agent(cold, restored), next.EntityState, "retry", auto, key: "wrong");
            Assert.NotNull(Result(mismatch).FailureDetails);
            Assert.Equal(next.EntityState, mismatch.EntityState);
            Assert.Equal(1, cold.Calls);
            EntityBatchResult retry = await DispatchAsync(Agent(cold, new Provider()), mismatch.EntityState, "retry", auto);
            Assert.Null(Result(retry).FailureDetails);
        }
    }

    [Theory]
    [InlineData(false, "load")]
    [InlineData(true, "load")]
    [InlineData(false, "lazy")]
    [InlineData(true, "lazy")]
    [InlineData(false, "store")]
    [InlineData(true, "store")]
    public async Task CallbackFailuresRollBackLocallyButColdRetriesCanRepeatEffectsAsync(bool auto, string phase)
    {
        EntityBatchResult seed = await DispatchAsync(Agent(new Client(), new Provider()), null, "seed", auto);
        foreach (string? original in new[] { null, seed.EntityState })
        {
            int effects = 0;
            AIFunction tool = AIFunctionFactory.Create(() =>
            {
                effects++;
                return DurableAgentContext.Current.ScheduleNewOrchestration("effect");
            }, name: "effect");
            Client client = new() { Tool = phase == "store" };
            Provider provider = new() { Failure = phase };
            ChatClientAgent agent = Agent(client, provider, tool);
            // No completion is committed; "store" fails after two model calls, a tool effect, and a provider write.
            // Local state rolls back without an "attempt" receipt, so the same correlation remains retryable.
            // This is intentional #143 at-least-once behavior while #131 terminal-failure finalization is deferred.
            EntityBatchResult failed = await DispatchAsync(agent, original, "attempt", auto);
            Assert.NotNull(Result(failed).FailureDetails);
            Assert.Equal(original, failed.EntityState);
            Assert.False(failed.EntityState is not null && State(failed).Data.CompletionReceipts!.ContainsKey("attempt"));
            Assert.Empty(failed.Actions!);
            Assert.Same(provider, agent.ChatHistoryProvider);
            Assert.Equal(phase == "store" ? 2 : 0, client.Calls);
            Assert.Equal(phase == "store" ? 1 : 0, effects);
            Assert.Equal(phase == "store" ? 1 : 0, provider.Writes);
            Provider retryProvider = new();
            Client cold = new() { Tool = phase == "store" };
            EntityBatchResult retry = await DispatchAsync(Agent(cold, retryProvider, tool), failed.EntityState, "attempt", auto);
            Assert.Null(Result(retry).FailureDetails);
            Assert.Equal(phase == "store" ? 2 : 1, cold.Calls);
            Assert.Equal(DurableAgentStateCompletionReceipt.SucceededOutcome, State(retry).Data.CompletionReceipts!["attempt"].Outcome);
            if (phase == "store")
            {
                Assert.Contains(retry.Actions!, action => action is StartNewOrchestrationOperationAction);
            }
            Assert.Equal(original is null ? null : 1, retryProvider.Restored);
            Assert.Equal(phase == "store" ? 2 : 0, effects);
            int calls = cold.Calls;
            EntityBatchResult duplicate = await DispatchAsync(Agent(cold, retryProvider, tool), retry.EntityState, "attempt", auto);
            Assert.Null(Result(duplicate).FailureDetails);
            Assert.Equal(retry.EntityState, duplicate.EntityState);
            Assert.Equal(calls, cold.Calls);
        }
    }

    [Fact]
    public async Task OwnerShapeValidationHappensBeforeExecutionEffectsAsync()
    {
        Client client = new();
        ChatClientAgent agent = new(client, new ChatClientAgentOptions
        {
            Name = "agent",
            ChatHistoryProvider = new InMemoryChatHistoryProvider(new InMemoryChatHistoryProviderOptions
            {
                ProvideOutputMessageFilter = messages => messages,
            }),
        });
        EntityBatchResult rejected = await DispatchAsync(agent, null);
        Assert.NotNull(Result(rejected).FailureDetails);
        Assert.Null(rejected.EntityState);
        Assert.Empty(rejected.Actions!);
        Assert.Equal(0, client.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ProtectedCapacityAndSdkCommitFailureDoNotPublishPartialStateAsync(bool afterModel)
    {
        Client client = new() { Response = afterModel ? new string('x', 6_000) : "answer" };
        EntityBatchResult capacity = await DispatchAsync(Agent(client), null, auto: true,
            text: afterModel ? "request" : new string('x', 6_000), configure: options => options.MaxStateBytes = afterModel ? 2_000 : 500);
        Assert.NotNull(Result(capacity).FailureDetails);
        Assert.Null(capacity.EntityState);
        Assert.Empty(capacity.Actions!);
        Assert.Equal(afterModel ? 1 : 0, client.Calls);
        EntityBatchResult seed = await DispatchAsync(Agent(new Client()), null, "seed");
        EntityBatchResult failed = await DispatchAsync(Agent(new Client()), seed.EntityState, "new", failCommit: true);
        Assert.NotNull(failed.FailureDetails ?? Result(failed).FailureDetails);
        Assert.Equal(seed.EntityState, failed.EntityState);
        Assert.Empty(failed.Actions!);
        Assert.False(State(failed).Data.CompletionReceipts!.ContainsKey("new"));
    }

    [Fact]
    public async Task SharedOptionChangesCannotChangeTheCurrentOperationPolicyAsync()
    {
        DurableAgentsOptions? options = null;
        Client client = new()
        {
            BeforeCall = () =>
            {
                options!.EnablePersistentRequestOutcomes = false;
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = 1;
            }
        };
        EntityBatchResult run = await DispatchAsync(Agent(client), null, configure: value => options = value);
        Assert.Null(Result(run).FailureDetails);
        Assert.Equal("2.0.0", State(run).SchemaVersion);
        Assert.Single(State(run).Data.CompletionReceipts!);
    }

    [Theory]
    [InlineData("1.0.0", false)]
    [InlineData("1.1.0", false)]
    [InlineData("1.2.0", false)]
    [InlineData("1.0.0", true)]
    [InlineData("1.1.0", true)]
    [InlineData("1.2.0", true)]
    public async Task Phase1AndPhase2LegacyEntityMigrationEnforcesIsolationAndInPlaceConversionAsync(
        string legacyVersion,
        bool auto)
    {
        DurableAgentState legacyState = new()
        {
            SchemaVersion = legacyVersion,
            Data = new DurableAgentStateData
            {
                ExpirationTimeUtc = s_now.AddHours(1).UtcDateTime,
                ConversationHistory =
                [
                    DurableAgentStateRequest.FromRunRequest(
                        new RunRequest(auto ? "MARKER-" + new string('p', 4_096) : "prior question") { CorrelationId = "prior" },
                        NullLogger.Instance),
                    DurableAgentStateResponse.FromResponse(
                        "prior",
                        new AgentResponse([new ChatMessage(ChatRole.Assistant, "prior answer")]) { CreatedAt = s_now.AddMinutes(-5) }),
                ],
            },
        };
        string originalJson = s_converter.Serialize(legacyState);

        // Phase 1 (ProcessLegacyEntities = false): Legacy @dafx-agent@session fails closed on both new turns and duplicates.
        Client phase1Client = new();
        EntityBatchResult phase1New = await DispatchAsync(
            Agent(phase1Client),
            originalJson,
            "next",
            auto,
            entityId: s_legacyId,
            configure: options => options.ProcessLegacyEntities = false);
        Assert.Equal(typeof(DurableAgentStateCorruptionException).FullName, Result(phase1New).FailureDetails!.ErrorType);
        Assert.Equal(originalJson, phase1New.EntityState);
        Assert.Equal(0, phase1Client.Calls);

        EntityBatchResult phase1Duplicate = await DispatchAsync(
            Agent(phase1Client),
            originalJson,
            "prior",
            auto,
            entityId: s_legacyId,
            configure: options => options.ProcessLegacyEntities = false);
        Assert.Equal(typeof(DurableAgentStateCorruptionException).FullName, Result(phase1Duplicate).FailureDetails!.ErrorType);
        Assert.Equal(originalJson, phase1Duplicate.EntityState);
        Assert.Equal(0, phase1Client.Calls);

        // Phase 2 (ProcessLegacyEntities = true) on a non-legacy @dafx-v2-agent@session with 1.x state still fails closed.
        EntityBatchResult nonLegacyRejected = await DispatchAsync(
            Agent(phase1Client),
            originalJson,
            "next",
            auto,
            entityId: s_id,
            configure: options => options.ProcessLegacyEntities = true);
        Assert.Equal(typeof(DurableAgentStateCorruptionException).FullName, Result(nonLegacyRejected).FailureDetails!.ErrorType);
        Assert.Equal(originalJson, nonLegacyRejected.EntityState);
        Assert.Equal(0, phase1Client.Calls);

        // Phase 2 (ProcessLegacyEntities = true) duplicate delivery on @dafx-agent@session upgrades 1.x -> 2.0.0 without model call.
        Client duplicateClient = new();
        EntityBatchResult migratedDuplicate = await DispatchAsync(
            Agent(duplicateClient),
            originalJson,
            "prior",
            auto,
            entityId: s_legacyId,
            configure: options =>
            {
                options.ProcessLegacyEntities = true;
                if (auto)
                {
                    options.MaxStateBytes = 32 * 1024;
                }
            });
        Assert.Null(Result(migratedDuplicate).FailureDetails);
        Assert.Equal(0, duplicateClient.Calls);
        DurableAgentState duplicateUpgradedState = State(migratedDuplicate);
        Assert.Equal("2.0.0", duplicateUpgradedState.SchemaVersion);
        Assert.Single(duplicateUpgradedState.Data.CompletionReceipts!);
        Assert.Equal("available", duplicateUpgradedState.Data.CompletionReceipts!["prior"].ResultState);
        Assert.Null(duplicateUpgradedState.Data.CompletionReceipts["prior"].ResultExpiresAt);
        Assert.Equal("prior answer", duplicateUpgradedState.Data.TerminalResults!["prior"].Response!.ToResponse().Text);

        // Phase 2 (ProcessLegacyEntities = true) new turn on @dafx-agent@session converts 1.x -> 2.0.0 in-place,
        // replays prior history, seals historyBinding, clears legacy TTL, and enforces KeepAll / Auto retention.
        Client phase2Client = new();
        EntityBatchResult migratedNewTurn = await DispatchAsync(
            Agent(phase2Client),
            originalJson,
            "turn-2",
            auto,
            text: auto ? new string('q', 4_096) : "follow-up question",
            entityId: s_legacyId,
            configure: options =>
            {
                options.ProcessLegacyEntities = true;
                if (auto)
                {
                    options.MaxStateBytes = 10_000;
                }
            });
        Assert.Null(Result(migratedNewTurn).FailureDetails);
        Assert.Equal(1, phase2Client.Calls);
        Assert.Equal(3, phase2Client.Messages.Count);
        Assert.Equal("prior answer", phase2Client.Messages[1].Text);

        DurableAgentState upgraded = State(migratedNewTurn);
        Assert.Equal("2.0.0", upgraded.SchemaVersion);
        Assert.Null(upgraded.Data.ExpirationTimeUtc);
        Assert.Equal(2, upgraded.Data.CompletionReceipts!.Count);
        Assert.Contains("prior", upgraded.Data.CompletionReceipts.Keys);
        Assert.Contains("turn-2", upgraded.Data.CompletionReceipts.Keys);
        Assert.Null(upgraded.Data.CompletionReceipts["prior"].ResultExpiresAt);
        Assert.NotNull(upgraded.Data.CompletionReceipts["turn-2"].ResultExpiresAt);
        Assert.Equal(
            DurableAgentStateHistoryBinding.DurableStateOwner,
            upgraded.Data.HistoryBinding.GetProperty("ownerKind").GetString());
        Assert.True(upgraded.Data.HistoryBinding.GetProperty("csharpFixedOwner").GetBoolean());
        Assert.Equal(auto, upgraded.Data.Truncation is not null);
        if (auto)
        {
            Assert.DoesNotContain(upgraded.Data.ConversationHistory, e => e.CorrelationId == "prior");
        }
        else
        {
            Assert.Equal(4, upgraded.Data.ConversationHistory.Count);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MinorVersion210PreservesSchemaVersionAndUnknownFieldsAcrossTurnsAsync(bool auto)
    {
        const string InitialV210Json = """
            {
              "schemaVersion": "2.1.0",
              "futureRootField": { "featureFlag": "v2.1-experimental" },
              "data": {
                "futureDataField": 42,
                "conversationHistory": [
                  {
                    "$type": "request",
                    "correlationId": "turn-1",
                    "createdAt": "2026-10-14T08:55:00Z",
                    "messages": [
                      {
                        "role": "user",
                        "messageId": "m-req-1",
                        "contents": [{ "$type": "text", "text": "hello from 2.1.0" }]
                      }
                    ]
                  },
                  {
                    "$type": "response",
                    "correlationId": "turn-1",
                    "createdAt": "2026-10-14T08:55:01Z",
                    "messages": [
                      {
                        "role": "assistant",
                        "messageId": "m-resp-1",
                        "contents": [{ "$type": "text", "text": "reply from 2.1.0" }]
                      }
                    ]
                  }
                ],
                "terminalResults": {
                  "turn-1": {
                    "correlationId": "turn-1",
                    "outcome": "succeeded",
                    "completedAt": "2026-10-14T08:55:01Z",
                    "response": {
                      "messages": [
                        {
                          "role": "assistant",
                          "messageId": "m-resp-1",
                          "contents": [{ "$type": "text", "text": "reply from 2.1.0" }]
                        }
                      ]
                    }
                  }
                },
                "completionReceipts": {
                  "turn-1": {
                    "correlationId": "turn-1",
                    "outcome": "succeeded",
                    "completedAt": "2026-10-14T08:55:01Z",
                    "resultState": "available"
                  }
                }
              }
            }
            """;

        Client client = new() { Response = "reply from v2 worker" };
        EntityBatchResult turn2 = await DispatchAsync(
            Agent(client),
            InitialV210Json,
            "turn-2",
            auto,
            text: "second turn");
        Assert.Null(Result(turn2).FailureDetails);
        Assert.Equal(1, client.Calls);
        Assert.Equal(3, client.Messages.Count);

        DurableAgentState committed = State(turn2);
        Assert.Equal("2.1.0", committed.SchemaVersion);
        Assert.Equal(2, committed.Data.CompletionReceipts!.Count);
        Assert.Equal("v2.1-experimental", committed.UnknownProperties!["futureRootField"].GetProperty("featureFlag").GetString());
        Assert.Equal(42, committed.Data.UnknownProperties!["futureDataField"].GetInt32());

        // Duplicate delivery on 2.1.0 state returns cached response without invoking model or downgrading version.
        EntityBatchResult duplicate = await DispatchAsync(
            Agent(client),
            turn2.EntityState,
            "turn-1",
            auto);
        Assert.Null(Result(duplicate).FailureDetails);
        Assert.Equal(1, client.Calls);
        Assert.Equal("2.1.0", State(duplicate).SchemaVersion);
    }

    [Fact]
    public async Task FutureMajorVersion300BouncesOnWorkerAndSupportsReadOnlyClientPollingAsync()
    {
        const string RawV3Json = """
             {
              "schemaVersion": "3.0.0",
              "v3RootMetadata": { "capability": "next-gen" },
              "data": {
                "v3OnlySection": [1, 2, 3],
                "conversationHistory": [],
                "terminalResults": {
                  "completed-v3": {
                    "correlationId": "completed-v3",
                    "outcome": "succeeded",
                    "completedAt": "2026-10-14T08:59:00Z",
                    "response": {
                      "messages": [
                        {
                          "role": "assistant",
                          "messageId": "m-v3-1",
                          "contents": [{ "$type": "text", "text": "completed by v3 worker" }]
                        }
                      ]
                    }
                  }
                },
                "completionReceipts": {
                  "completed-v3": {
                    "correlationId": "completed-v3",
                    "outcome": "succeeded",
                    "completedAt": "2026-10-14T08:59:00Z",
                    "resultState": "available"
                  }
                }
              }
            } 
            """;

        Client client = new();
        EntityId v3EntityId = new(AgentSessionId.ToEntityName("agent", 3), "session");

        // 1. Run operation on a v2 worker: raw 3.0.0 state bytes are untouched, model is not called,
        // and the Run operation is re-signaled with a 5-second delay.
        EntityBatchResult bouncedRun = await DispatchAsync(
            Agent(client),
            RawV3Json,
            "new-turn",
            text: "hello v3",
            entityId: v3EntityId);
        Assert.Null(Result(bouncedRun).FailureDetails);
        Assert.Equal(RawV3Json, bouncedRun.EntityState);
        Assert.Equal(0, client.Calls);
        SendSignalOperationAction bounceSignal = Assert.IsType<SendSignalOperationAction>(Assert.Single(bouncedRun.Actions!));
        Assert.Equal(nameof(AgentEntity.Run), bounceSignal.Name);
        Assert.Equal(s_now.AddSeconds(5).UtcDateTime, bounceSignal.ScheduledTime);

        // 2. Cleanup operations on a v2 worker are no-ops that leave 3.0.0 state untouched and emit no signals.
        foreach (string cleanupOperation in new[] { nameof(AgentEntity.CheckAndExpireResults), nameof(AgentEntity.CheckAndDeleteIfExpired) })
        {
            EntityBatchResult cleanupBatch = await DispatchAsync(
                Agent(client),
                RawV3Json,
                operation: cleanupOperation,
                entityId: v3EntityId);
            Assert.Null(Result(cleanupBatch).FailureDetails);
            Assert.Equal(RawV3Json, cleanupBatch.EntityState);
            Assert.Empty(cleanupBatch.Actions!);
            Assert.Equal(0, client.Calls);
        }

        // 3. Client outcome polling (GetEntityAsync<DurableAgentState> + DurableAgentStateOutcomeResolver.Resolve)
        // can read committed 3.0.0 terminalResults and completionReceipts without throwing.
        DurableAgentState clientReadState = StateJson(RawV3Json);
        Assert.Equal("3.0.0", clientReadState.SchemaVersion);
        DurableAgentRunOutcome resolved = DurableAgentStateOutcomeResolver.Resolve(
            clientReadState,
            "completed-v3",
            s_now);
        Assert.Equal(DurableAgentRunOutcomeKind.Succeeded, resolved.Kind);
        Assert.Equal("completed by v3 worker", resolved.GetResponse("completed-v3").Text);
        Assert.Equal(
            DurableAgentRunOutcomeKind.Pending,
            DurableAgentStateOutcomeResolver.Resolve(clientReadState, "pending-v3", s_now).Kind);
    }

    [Fact]
    public async Task TwoPhaseRollingUpgrade_SideBySideWorkersAndOrchestrationReplay_MigratesCleanlyAsync()
    {
        Dictionary<string, string?> taskHubEntityStore = new(StringComparer.OrdinalIgnoreCase);

        // Step 1: Old v1 worker running alone creates v1 sessions (@dafx-agent@*) with 1.2.0 state.
        EntityId v1SessionA = new(AgentSessionId.ToLegacyEntityName("agent"), "v1-session-a");
        EntityId v1SessionB = new(AgentSessionId.ToLegacyEntityName("agent"), "v1-session-b");

        foreach (EntityId legacyId in new[] { v1SessionA, v1SessionB })
        {
            Client v1Turn1Client = new() { Response = $"v1-reply-1-{legacyId.Key}" };
            EntityBatchResult v1Turn1 = await DispatchAsync(
                Agent(v1Turn1Client),
                json: null,
                id: "v1-corr-1",
                schema: false,
                text: $"v1-prompt-1-{legacyId.Key}",
                entityId: legacyId);
            Assert.Null(Result(v1Turn1).FailureDetails);
            Assert.Equal("1.2.0", State(v1Turn1).SchemaVersion);
            taskHubEntityStore[legacyId.ToString()] = v1Turn1.EntityState;
        }

        // Also start an in-flight v1 HITL orchestration that creates a session, runs Turn 1 on v1,
        // and pauses at WaitForExternalEvent("Approval").
        static async Task<string?> HitlOrchestratorAsync(TaskOrchestrationContext ctx)
        {
            DurableAIAgent writer = ctx.GetAgent("agent");
            AgentSession session = await writer.CreateSessionAsync();
            AgentResponse first = await writer.RunAsync("v1-orch-draft", session);
            string feedback = await ctx.WaitForExternalEvent<string>("Approval");
            AgentResponse second = await writer.RunAsync($"{first.Text} + {feedback}", session);
            return $"{session.GetService<AgentSessionId>()}:{second.Text}";
        }

        DurableTaskShimFactory orchShimFactory = new(
            new DurableTaskWorkerOptions { DataConverter = s_converter, EnableEntitySupport = true },
            NullLoggerFactory.Instance);
        global::DurableTask.Core.TaskOrchestration v1OrchShim = orchShimFactory.CreateOrchestration<string?, string>(
            new TaskName("HitlOrchestrator"),
            (ctx, _) => HitlOrchestratorAsync(ctx));

        global::DurableTask.Core.OrchestrationRuntimeState orchRuntimeState = new(
        [
            new OrchestratorStartedEvent(-1) { Timestamp = s_now.UtcDateTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "HitlOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance { InstanceId = "hitl-orch-1", ExecutionId = "exec-1" },
            },
        ]);
        global::DurableTask.Core.TaskOrchestrationExecutor v1OrchExec = new(
            orchRuntimeState,
            v1OrchShim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters());
        global::DurableTask.Core.OrchestratorExecutionResult v1OrchStep1 = v1OrchExec.Execute();
        SendEventOrchestratorAction v1EntityCallAction = Assert.IsType<SendEventOrchestratorAction>(
            Assert.Single(v1OrchStep1.Actions));
        // On v1 worker, the session targeted @dafx-agent@<guid>.
        EntityId v1OrchEntityId = new(
            AgentSessionId.ToLegacyEntityName("agent"),
            EntityId.FromString(v1EntityCallAction.Instance!.InstanceId).Key);
        string v1OrchEntityInstanceId = v1OrchEntityId.ToString();
        using JsonDocument opDoc1 = JsonDocument.Parse(v1EntityCallAction.EventData!);
        string v1OpRequestId = opDoc1.RootElement.GetProperty("id").GetString()!;
        string v1OpInput = opDoc1.RootElement.GetProperty("input").GetString()!;

        // Execute Turn 1 on the v1 worker (1.2.0 state, plain v1 response without v2 entityVersion metadata).
        Client v1OrchClient = new() { Response = "v1-draft-output" };
        EntityBatchResult v1OrchEntityBatch = await DispatchAsync(
            Agent(v1OrchClient),
            json: null,
            schema: false,
            operations: [new OperationRequest { Id = Guid.Parse(v1OpRequestId), Operation = nameof(AgentEntity.Run), Input = v1OpInput }],
            entityId: v1OrchEntityId);
        Assert.Null(Result(v1OrchEntityBatch).FailureDetails);
        Assert.Equal("1.2.0", State(v1OrchEntityBatch).SchemaVersion);
        taskHubEntityStore[v1OrchEntityInstanceId] = v1OrchEntityBatch.EntityState;

        // Simulate v1 response payload (no entityVersion property in envelope) recorded in orchestration history.
        string rawV1OrchResponse = JsonSerializer.Serialize(
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "v1-draft-output")),
            DurableAgentJsonUtilities.DefaultOptions);
        string responseMessageJson1 = JsonSerializer.Serialize(new { result = rawV1OrchResponse });

        // Step 2 (Phase 1 side-by-side: v1 worker + v2 worker with ProcessLegacyEntities = false).
        ServiceCollection phase1Services = new();
        phase1Services.ConfigureDurableAgents(
            options =>
            {
                HistoryRetentionDemo.ConfigureRetention(options, processLegacyEntities: false);
                options.AddAIAgent(Agent(new Client()));
            },
            workerBuilder: b => b.UseGrpc());
        using ServiceProvider phase1Provider = phase1Services.BuildServiceProvider();
        DurableTaskWorkerWorkItemFilters phase1Filters = phase1Provider
            .GetRequiredService<IOptionsMonitor<DurableTaskWorkerWorkItemFilters>>()
            .Get(Options.DefaultName);
        Assert.Equal(["dafx-v2-agent"], phase1Filters.Entities.Select(e => e.Name));

        // By default, a worker with local agents registered still dispatches calls to remote agents hosted on other workers.
        // When ValidateLocalAgentRegistration is opted in, calling an unregistered agent fails immediately with AgentNotRegisteredException.
        DurableTaskShimFactory phase1OrchShimFactory = new(
            new DurableTaskWorkerOptions
            {
                DataConverter = phase1Provider.GetRequiredService<DataConverter>(),
                EnableEntitySupport = true,
            },
            NullLoggerFactory.Instance);
        global::DurableTask.Core.TaskOrchestration remoteDispatchShim = phase1OrchShimFactory.CreateOrchestration<string?, string>(
            new TaskName("RemoteAgentOrchestrator"),
            async (ctx, _) => (await ctx.GetAgent("RemoteAgent").RunAsync("Hello")).Text);
        global::DurableTask.Core.OrchestrationRuntimeState remoteDispatchState = new(
        [
            new OrchestratorStartedEvent(-1) { Timestamp = s_now.UtcDateTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "RemoteAgentOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance { InstanceId = "remote-orch-1", ExecutionId = "exec-remote" },
            },
        ]);
        global::DurableTask.Core.OrchestratorExecutionResult remoteDispatchResult = new global::DurableTask.Core.TaskOrchestrationExecutor(
            remoteDispatchState,
            remoteDispatchShim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters(),
            global::DurableTask.Core.ErrorPropagationMode.UseFailureDetails).Execute();
        SendEventOrchestratorAction remoteCallAction = Assert.IsType<SendEventOrchestratorAction>(
            Assert.Single(remoteDispatchResult.Actions));
        Assert.Equal("dafx-v2-remoteagent", EntityId.FromString(remoteCallAction.Instance!.InstanceId).Name);

        phase1Provider.GetRequiredService<DurableAgentsOptions>().ValidateLocalAgentRegistration = true;
        global::DurableTask.Core.TaskOrchestration unregisteredOrchShim = phase1OrchShimFactory.CreateOrchestration<string?, string>(
            new TaskName("UnregisteredAgentOrchestrator"),
            async (ctx, _) => (await ctx.GetAgent("NonExistentAgent").RunAsync("Hello")).Text);
        global::DurableTask.Core.OrchestrationRuntimeState unregisteredOrchState = new(
        [
            new OrchestratorStartedEvent(-1) { Timestamp = s_now.UtcDateTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "UnregisteredAgentOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance { InstanceId = "unreg-orch-1", ExecutionId = "exec-unreg" },
            },
        ]);
        global::DurableTask.Core.OrchestratorExecutionResult unregisteredResult = new global::DurableTask.Core.TaskOrchestrationExecutor(
            unregisteredOrchState,
            unregisteredOrchShim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters(),
            global::DurableTask.Core.ErrorPropagationMode.UseFailureDetails).Execute();
        OrchestrationCompleteOrchestratorAction failedOrchAction = Assert.IsType<OrchestrationCompleteOrchestratorAction>(
            Assert.Single(unregisteredResult.Actions));
        Assert.Equal(global::DurableTask.Core.OrchestrationStatus.Failed, failedOrchAction.OrchestrationStatus);
        Assert.Equal(typeof(AgentNotRegisteredException).FullName, failedOrchAction.FailureDetails?.ErrorType);
        Assert.Contains("NonExistentAgent", failedOrchAction.FailureDetails?.ErrorMessage, StringComparison.OrdinalIgnoreCase);

        // Concurrently send follow-up turns to v1 sessions on v1 worker AND create new v2 sessions on v2 worker.
        EntityId v2Session1 = new(AgentSessionId.ToEntityName("agent"), "v2-session-1");
        EntityId v2Session2 = new(AgentSessionId.ToEntityName("agent"), "v2-session-2");

        Client v1Turn2ClientA = new() { Response = "v1-reply-2-a" };
        Client v1Turn2ClientB = new() { Response = "v1-reply-2-b" };
        Client v2Turn1Client1 = new() { Response = "v2-reply-1-1" };
        Client v2Turn1Client2 = new() { Response = "v2-reply-1-2" };

        EntityBatchResult[] phase1Results = await Task.WhenAll(
            DispatchAsync(Agent(v1Turn2ClientA), taskHubEntityStore[v1SessionA.ToString()], "v1-corr-2", schema: false, text: "v1-prompt-2-a", entityId: v1SessionA),
            DispatchAsync(Agent(v1Turn2ClientB), taskHubEntityStore[v1SessionB.ToString()], "v1-corr-2", schema: false, text: "v1-prompt-2-b", entityId: v1SessionB),
            DispatchAsync(Agent(v2Turn1Client1), null, "v2-corr-1", auto: true, schema: true, text: "v2-prompt-1-1", entityId: v2Session1, configure: o => HistoryRetentionDemo.ConfigureRetention(o)),
            DispatchAsync(Agent(v2Turn1Client2), null, "v2-corr-1", auto: true, schema: true, text: "v2-prompt-1-2", entityId: v2Session2, configure: o => HistoryRetentionDemo.ConfigureRetention(o)));

        Assert.All(phase1Results, r => Assert.Null(Result(r).FailureDetails));
        taskHubEntityStore[v1SessionA.ToString()] = phase1Results[0].EntityState;
        taskHubEntityStore[v1SessionB.ToString()] = phase1Results[1].EntityState;
        taskHubEntityStore[v2Session1.ToString()] = phase1Results[2].EntityState;
        taskHubEntityStore[v2Session2.ToString()] = phase1Results[3].EntityState;

        Assert.Equal("1.2.0", State(phase1Results[0]).SchemaVersion);
        Assert.Equal("1.2.0", State(phase1Results[1]).SchemaVersion);
        Assert.Equal("2.0.0", State(phase1Results[2]).SchemaVersion);
        Assert.Equal("2.0.0", State(phase1Results[3]).SchemaVersion);

        // Step 3 (Phase 2 cutover: v1 worker stopped; v2 worker runs with ProcessLegacyEntities = true).
        ServiceCollection phase2Services = new();
        phase2Services.ConfigureDurableAgents(
            options =>
            {
                HistoryRetentionDemo.ConfigureRetention(options, processLegacyEntities: true);
                options.AddAIAgent(Agent(new Client()));
            },
            workerBuilder: b => b.UseGrpc());
        using ServiceProvider phase2Provider = phase2Services.BuildServiceProvider();
        DurableTaskWorkerWorkItemFilters phase2Filters = phase2Provider
            .GetRequiredService<IOptionsMonitor<DurableTaskWorkerWorkItemFilters>>()
            .Get(Options.DefaultName);
        Assert.Equal(["dafx-v2-agent", "dafx-agent"], phase2Filters.Entities.Select(e => e.Name));

        // Send Turn 3 to old v1 sessions AND Turn 2 to new v2 sessions on the Phase 2 v2 worker.
        Client phase2MigratedClientA = new() { Response = "v2-migrated-reply-a" };
        Client phase2MigratedClientB = new() { Response = "v2-migrated-reply-b" };
        Client phase2V2Client1 = new() { Response = "v2-reply-2-1" };
        Client phase2V2Client2 = new() { Response = "v2-reply-2-2" };

        EntityBatchResult[] phase2Results = await Task.WhenAll(
            DispatchAsync(Agent(phase2MigratedClientA), taskHubEntityStore[v1SessionA.ToString()], "v1-corr-3", auto: true, schema: true, text: "v2-prompt-3-a", entityId: v1SessionA, configure: o => HistoryRetentionDemo.ConfigureRetention(o, processLegacyEntities: true)),
            DispatchAsync(Agent(phase2MigratedClientB), taskHubEntityStore[v1SessionB.ToString()], "v1-corr-3", auto: true, schema: true, text: "v2-prompt-3-b", entityId: v1SessionB, configure: o => HistoryRetentionDemo.ConfigureRetention(o, processLegacyEntities: true)),
            DispatchAsync(Agent(phase2V2Client1), taskHubEntityStore[v2Session1.ToString()], "v2-corr-2", auto: true, schema: true, text: "v2-prompt-2-1", entityId: v2Session1, configure: o => HistoryRetentionDemo.ConfigureRetention(o, processLegacyEntities: true)),
            DispatchAsync(Agent(phase2V2Client2), taskHubEntityStore[v2Session2.ToString()], "v2-corr-2", auto: true, schema: true, text: "v2-prompt-2-2", entityId: v2Session2, configure: o => HistoryRetentionDemo.ConfigureRetention(o, processLegacyEntities: true)));

        Assert.All(phase2Results, r => Assert.Null(Result(r).FailureDetails));
        // Verify old v1 sessions preserved all prior v1 turns and upgraded in-place from 1.2.0 to 2.0.0.
        Assert.Equal(5, phase2MigratedClientA.Messages.Count);
        Assert.Equal("v1-prompt-1-v1-session-a", phase2MigratedClientA.Messages[0].Text);
        Assert.Equal("v1-reply-1-v1-session-a", phase2MigratedClientA.Messages[1].Text);
        Assert.Equal("v1-prompt-2-a", phase2MigratedClientA.Messages[2].Text);
        Assert.Equal("v1-reply-2-a", phase2MigratedClientA.Messages[3].Text);
        Assert.Equal("v2-prompt-3-a", phase2MigratedClientA.Messages[4].Text);

        DurableAgentState upgradedSessionA = State(phase2Results[0]);
        Assert.Equal("2.0.0", upgradedSessionA.SchemaVersion);
        Assert.Equal(3, upgradedSessionA.Data.CompletionReceipts!.Count);
        Assert.Equal(3, upgradedSessionA.Data.TerminalResults!.Count);

        // Also resume the in-flight v1 HITL orchestration on the Phase 2 v2 worker when "Approval" arrives.
        List<HistoryEvent> phase2PastEvents =
        [
            new OrchestratorStartedEvent(-1) { Timestamp = s_now.UtcDateTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "HitlOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance { InstanceId = "hitl-orch-1", ExecutionId = "exec-1" },
            },
            new EventSentEvent(v1EntityCallAction.Id)
            {
                InstanceId = v1OrchEntityInstanceId,
                Name = v1EntityCallAction.EventName,
                Input = v1EntityCallAction.EventData,
            },
            new OrchestratorCompletedEvent(-1),
            new OrchestratorStartedEvent(-1) { Timestamp = s_now.AddSeconds(1).UtcDateTime },
            new EventRaisedEvent(-1, responseMessageJson1) { Name = v1OpRequestId },
            new OrchestratorCompletedEvent(-1),
        ];
        global::DurableTask.Core.OrchestrationRuntimeState phase2OrchState = new(phase2PastEvents);
        phase2OrchState.AddEvent(new OrchestratorStartedEvent(-1) { Timestamp = s_now.AddSeconds(5).UtcDateTime });
        phase2OrchState.AddEvent(new EventRaisedEvent(-1, "\"approved-with-edits\"") { Name = "Approval" });

        global::DurableTask.Core.TaskOrchestration v2OrchShim = orchShimFactory.CreateOrchestration<string?, string>(
            new TaskName("HitlOrchestrator"),
            (ctx, _) => HitlOrchestratorAsync(ctx));
        global::DurableTask.Core.TaskOrchestrationExecutor v2OrchExec = new(
            phase2OrchState,
            v2OrchShim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters());
        global::DurableTask.Core.OrchestratorExecutionResult v2OrchStep2 = v2OrchExec.Execute();

        // Turn 2 must target the original v1 entity (@dafx-agent@<guid>), NOT @dafx-v2-agent@<guid>!
        SendEventOrchestratorAction v2EntityCallAction = Assert.IsType<SendEventOrchestratorAction>(
            Assert.Single(v2OrchStep2.Actions));
        Assert.Equal(v1OrchEntityInstanceId, v2EntityCallAction.Instance!.InstanceId);

        using JsonDocument opDoc2 = JsonDocument.Parse(v2EntityCallAction.EventData!);
        string v2OpRequestId = opDoc2.RootElement.GetProperty("id").GetString()!;
        string v2OpInput = opDoc2.RootElement.GetProperty("input").GetString()!;

        Client v2OrchEntityClient = new() { Response = "v2-final-article" };
        EntityBatchResult v2OrchEntityBatch = await DispatchAsync(
            Agent(v2OrchEntityClient),
            taskHubEntityStore[v1OrchEntityInstanceId],
            auto: true,
            schema: true,
            operations: [new OperationRequest { Id = Guid.Parse(v2OpRequestId), Operation = nameof(AgentEntity.Run), Input = v2OpInput }],
            entityId: v1OrchEntityId,
            configure: o => HistoryRetentionDemo.ConfigureRetention(o, processLegacyEntities: true));
        Assert.Null(Result(v2OrchEntityBatch).FailureDetails);
        Assert.Equal("2.0.0", State(v2OrchEntityBatch).SchemaVersion);
        Assert.Equal(3, v2OrchEntityClient.Messages.Count);
        Assert.Equal("v1-orch-draft", v2OrchEntityClient.Messages[0].Text);
        Assert.Equal("v1-draft-output", v2OrchEntityClient.Messages[1].Text);
        Assert.Equal("v1-draft-output + approved-with-edits", v2OrchEntityClient.Messages[2].Text);
    }

    private static ChatClientAgent Agent(Client client, Provider? provider = null, AIFunction? tool = null) =>
        new(client, new ChatClientAgentOptions
        {
            Name = "agent",
            ChatHistoryProvider = provider,
            ChatOptions = new ChatOptions { Tools = tool is null ? null : [tool] },
        });

    private static async Task<EntityBatchResult> DispatchAsync(AIAgent agent, string? json, string id = "work",
        bool auto = false, bool schema = true, string text = "request", string operation = nameof(AgentEntity.Run),
        DateTimeOffset? now = null, Action<DurableAgentsOptions>? configure = null,
        List<OperationRequest>? operations = null, bool failCommit = false, string key = "history.v1",
        EntityId? entityId = null)
    {
        EntityId targetEntityId = entityId ?? s_id;
        DurableAgentsOptions options = new()
        {
            EnablePersistentRequestOutcomes = schema,
            HistoryRetentionMode = auto ? DurableAgentHistoryRetentionMode.Auto : DurableAgentHistoryRetentionMode.KeepAll,
            ResultRetentionPeriod = TimeSpan.FromMinutes(1),
        };
        configure?.Invoke(options);
        options.AddAIAgent(agent, timeToLive: null, configureHistory: history => history.ProviderKey = new(key));
        using ServiceProvider services = new ServiceCollection()
            .AddSingleton(options)
            .AddSingleton(options.GetAgentFactories())
            .AddSingleton(new Mock<DurableTaskClient>("test").Object)
            .AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance)
            .AddSingleton<TimeProvider>(new Clock(now ?? s_now))
            .AddSingleton(Mock.Of<IHostApplicationLifetime>())
            .BuildServiceProvider();
        DurableTaskShimFactory factory = new(
            new DurableTaskWorkerOptions { DataConverter = new CommitConverter(failCommit) }, NullLoggerFactory.Instance);
        var shim = factory.CreateEntity(new(targetEntityId.Name), new AgentEntity(services), targetEntityId);
        return await shim.ExecuteOperationBatchAsync(new EntityBatchRequest
        {
            InstanceId = targetEntityId.ToString(),
            EntityState = json,
            Operations = operations ?? [Request(operation, id, text)],
        });
    }

    private static OperationRequest Request(string operation, string id = "work", string text = "request") =>
        new()
        {
            Id = Guid.NewGuid(),
            Operation = operation,
            Input = operation == nameof(AgentEntity.Run) ? s_converter.Serialize(new RunRequest(text) { CorrelationId = id }) : null,
        };
    private static OperationResult Result(EntityBatchResult batch) => Assert.Single(batch.Results!);
    private static DurableAgentState State(EntityBatchResult batch) => StateJson(batch.EntityState);
    private static DurableAgentState StateJson(string? json) =>
        Assert.IsType<DurableAgentState>(s_converter.Deserialize(json, typeof(DurableAgentState)));

    private sealed record ConfigurationCase(
        bool Auto,
        bool Valid,
        bool Reverse = false,
        bool Split = false,
        bool LateEnvironment = false,
        bool? ProcessLegacyCode = null,
        string? ProcessLegacyEnv = null,
        bool ExpectedProcessLegacy = false);
    private sealed class Clock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
    private sealed class CommitConverter(bool fail) : DataConverter
    {
        public override object? Deserialize(string? data, Type targetType) => s_converter.Deserialize(data, targetType);
        [return: NotNullIfNotNull(nameof(value))]
        public override string? Serialize(object? value) =>
            fail && value is DurableAgentState ? throw new InvalidOperationException("commit failed") : s_converter.Serialize(value);
    }

    private sealed class Provider : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => ["continuation"];
        public string? Failure { get; init; }
        public int? Restored { get; private set; }
        public int Writes { get; private set; }
        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context, CancellationToken cancellationToken = default)
        {
            if (context.Session!.StateBag.TryGetValue("continuation", out string? version))
            {
                this.Restored = version == "1" ? 1 : null;
            }
            if (this.Failure == "load")
            {
                throw new IOException("load failed");
            }
            return new(this.Load());
        }
        private IEnumerable<ChatMessage> Load()
        {
            if (this.Failure == "lazy")
            {
                throw new IOException("enumeration failed");
            }
            yield break;
        }
        protected override ValueTask StoreChatHistoryAsync(InvokedContext context, CancellationToken cancellationToken = default)
        {
            this.Writes++;
            context.Session!.StateBag.SetValue("continuation", "1");
            if (this.Failure == "store")
            {
                context.Session.StateBag.SetValue("continuation", "999");
                throw new IOException("store failed after remote write");
            }
            return default;
        }
    }

    private sealed class Client : IChatClient
    {
        public int Calls { get; private set; }
        public List<ChatMessage> Messages { get; private set; } = [];
        public string? Conversation { get; init; }
        public string Response { get; init; } = "answer";
        public bool Tool { get; init; }
        public bool Fail { get; init; }
        public Action? BeforeCall { get; init; }
        public void Dispose() { }
        public object? GetService(Type serviceType, object? serviceKey = null) => null;
        public Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages, ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.Calls++;
            this.Messages = messages.ToList();
            this.BeforeCall?.Invoke();
            await Task.Yield();
            if (this.Fail)
            {
                throw new IOException("service acknowledgement failed");
            }
            yield return this.Tool && this.Calls == 1
                ? new ChatResponseUpdate(ChatRole.Assistant, [new FunctionCallContent("call", "effect")])
                : new ChatResponseUpdate(ChatRole.Assistant, this.Response) { ConversationId = this.Conversation };
        }
    }
}
