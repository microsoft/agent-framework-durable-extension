// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using AutoHistoryRetention;
using DurableTask.Core.Entities;
using DurableTask.Core.Entities.OperationFormat;
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
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityPublicRetentionTests
{
    private const string ModeEnvironment = "DURABLE_AGENTS_DEPLOYMENT_MODE";
    private const string ProbeEnvironment = "PUBLIC_RETENTION_CONFIGURATION_PROBE";
    private static readonly DateTimeOffset s_now = new(2026, 10, 14, 9, 0, 0, TimeSpan.Zero);
    private static readonly EntityId s_id = new(AgentSessionId.ToEntityName("agent"), "session");
    private static readonly DurableDataConverter s_converter = new();

    [Theory]
    [InlineData(null, null, false, true)]
    [InlineData(1, null, false, true)]
    [InlineData(1, null, true, true)]
    [InlineData(null, "isolated_v2", false, true)]
    [InlineData(null, "isolated_v2", true, true)]
    [InlineData(0, "isolated_v2", false, false)]
    [InlineData(1, "ISOLATED_V2", false, false)]
    [InlineData(1, " isolated_v2", false, false)]
    [InlineData(1, "isolated_v2 ", true, false)]
    [InlineData(1, "", true, false)]
    [InlineData(null, "legacy", false, false)]
    [InlineData(null, null, true, false)]
    public Task ConfigurationIsValidatedAndCapturedAtFinalizationAsync(int? code, string? value, bool auto, bool valid) =>
        RunConfigurationProbeAsync(new(code, value, auto, valid));

    [Theory]
    [InlineData(1, false, false)]
    [InlineData(1, true, false)]
    [InlineData(null, false, false)]
    [InlineData(null, true, false)]
    [InlineData(1, false, true)]
    [InlineData(1, true, true)]
    [InlineData(null, false, true)]
    [InlineData(null, true, true)]
    public Task AdditiveConfigurationFinalizesBothOrdersAsync(int? code, bool reverse, bool split) =>
        RunConfigurationProbeAsync(new(code, code is null ? "isolated_v2" : null, Auto: true, Valid: true,
            reverse, split, LateEnvironment: true));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task InvalidFinalWatermarksFailBeforeRuntimeCreationAsync(bool reverse) =>
        RunConfigurationProbeAsync(new(1, null, Auto: true, Valid: false, reverse, Split: true));

    private static async Task RunConfigurationProbeAsync(ConfigurationCase test)
    {
        ProcessStartInfo start = new("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(typeof(AgentEntityPublicRetentionTests).Assembly.Location);
        start.ArgumentList.Add("--filter-method");
        start.ArgumentList.Add("*EnvironmentProbeAsync");
        start.ArgumentList.Add("--no-progress");
        start.Environment.Remove(ModeEnvironment);
        if (test.Value is not null)
        {
            start.Environment[ModeEnvironment] = test.Value;
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
            Environment.SetEnvironmentVariable(ModeEnvironment, "invalid-during-registration");
        }
        void Deployment(DurableAgentsOptions options)
        {
            if (test.Code is int code)
            {
                options.DeploymentMode = (DurableAgentDeploymentMode)code;
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
                Deployment(options);
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
        services.ConfigureDurableAgents(test.Reverse ? Retention : Deployment);
        services.ConfigureDurableOptions(_ => { });
        services.ConfigureDurableWorkflows(_ => { });
        if (test.LateEnvironment)
        {
            Environment.SetEnvironmentVariable(ModeEnvironment, test.Value);
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
        DurableAgentDeploymentMode expected = test.Code == 1 || test.Value is not null
            ? DurableAgentDeploymentMode.SchemaV2 : DurableAgentDeploymentMode.Legacy;
        Assert.Equal(expected, captured.DeploymentMode);
        DurableAgentRetentionSettings policy = captured.GetRetentionSettings();
        Assert.Equal(test.Auto ? DurableAgentHistoryRetentionMode.Auto : DurableAgentHistoryRetentionMode.KeepAll, policy.Mode);
        Assert.Equal(test.Auto ? 1_048_576 : null, policy.MaxStateBytes);
        Assert.Equal(test.Split ? .6 : .85, policy.HighWatermark);
        Assert.Equal(test.Split ? .4 : .70, policy.LowWatermark);
        Environment.SetEnvironmentVariable(ModeEnvironment, "invalid-after-capture");
        Assert.NotNull(host.Services.GetRequiredService<DurableTaskClient>());
        Assert.NotNull(host.Services.GetRequiredService<IDurableTaskClientProvider>());
        Assert.NotEmpty(host.Services.GetServices<IHostedService>());
        Assert.Equal(1, workerRegistrations);
        Assert.Equal(1, clientInitializations);
        Assert.Equal(0, agentFactories);
        Assert.Equal(expected, captured.DeploymentMode);
        Assert.Equal(policy, captured.GetRetentionSettings());
    }

    [Fact]
    public void PublicOptionsAndSampleConfigurationKeepTheirContracts()
    {
        Assert.Equal(["Legacy", "SchemaV2"], Enum.GetNames<DurableAgentDeploymentMode>());
        Assert.Equal([0, 1], Enum.GetValues<DurableAgentDeploymentMode>().Select(mode => (int)mode));
        DurableAgentsOptions options = new();
        Assert.Equal(DurableAgentDeploymentMode.Legacy, options.DeploymentMode);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.DeploymentMode = (DurableAgentDeploymentMode)42);
        HistoryRetentionDemo.ConfigureRetention(options);
        Assert.Equal(DurableAgentDeploymentMode.SchemaV2, options.DeploymentMode);
        Assert.Equal(32 * 1024, options.GetRetentionSettings().MaxStateBytes);
        int callbacks = 0;
        ServiceCollection services = new();
        services.ConfigureDurableAgents(config =>
        {
            config.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
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
        builder.Services.ConfigureDurableAgents(options => options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto);
        if (valid)
        {
            builder.Services.ConfigureDurableOptions(options => options.Agents.DeploymentMode = DurableAgentDeploymentMode.SchemaV2);
        }
        using IHost host = builder.Build();
        if (valid)
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(DurableAgentDeploymentMode.SchemaV2, host.Services.GetRequiredService<DurableAgentsOptions>().DeploymentMode);
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
            EntityBatchResult batch = await DispatchAsync(Agent(client), raw, operations:
            [
                Request(cleanup), Request(nameof(AgentEntity.Run), "work"),
            ]);
            Assert.Equal(raw, batch.EntityState);
            Assert.Null(batch.Results![0].FailureDetails);
            Assert.NotNull(batch.Results[1].FailureDetails);
            Assert.Empty(batch.Actions!);
            EntityBatchResult fresh = await DispatchAsync(Agent(client), null, operations:
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
        EntityBatchResult rejected = await DispatchAsync(Agent(client), original, "old");
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
            options!.DeploymentMode = DurableAgentDeploymentMode.Legacy;
            options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
            options.MaxStateBytes = 1;
        }
        };
        EntityBatchResult run = await DispatchAsync(Agent(client), null, configure: value => options = value);
        Assert.Null(Result(run).FailureDetails);
        Assert.Equal("2.0.0", State(run).SchemaVersion);
        Assert.Single(State(run).Data.CompletionReceipts!);
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
        List<OperationRequest>? operations = null, bool failCommit = false, string key = "history.v1")
    {
        DurableAgentsOptions options = new()
        {
            DeploymentMode = schema ? DurableAgentDeploymentMode.SchemaV2 : DurableAgentDeploymentMode.Legacy,
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
        var shim = factory.CreateEntity(new(s_id.Name), new AgentEntity(services), s_id);
        return await shim.ExecuteOperationBatchAsync(new EntityBatchRequest
        {
            InstanceId = s_id.ToString(),
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

    private sealed record ConfigurationCase(int? Code, string? Value, bool Auto, bool Valid,
        bool Reverse = false, bool Split = false, bool LateEnvironment = false);
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
