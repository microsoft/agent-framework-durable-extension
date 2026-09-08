// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.Compaction;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityHistoryTests
{
    private static readonly TimeSpan s_stageTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan s_testTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task EntityExecutionUsesDurableProviderAndPersistsSessionAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(["old request", "old response", "new request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(4, persisted.Data.ConversationHistory.Count);
        Assert.Equal(DurableAgentStateHistoryBinding.DurableStateOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal(DurableAgentHistoryBinding.DurableStateProviderKey, GetBinding(persisted)?.ProviderKey);
        Assert.Equal(2, persisted.Data.TerminalResults?.Count);
        Assert.Equal(2, persisted.Data.CompletionReceipts?.Count);
        Assert.NotNull(persisted.Data.Session);
        Assert.DoesNotContain(
            nameof(InMemoryChatHistoryProvider),
            persisted.Data.Session.Value.GetProperty("stateBag").EnumerateObject().Select(property => property.Name));
    }

    [Fact]
    public async Task WrappedAgentDoesNotReplayTranscriptOutsideProviderAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "agent");
        AIAgent wrappedAgent = new TestDelegatingAgent(chatAgent);
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");

        DurableAgentState persisted = await RunEntityAsync(
            wrappedAgent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(3, client.LastMessages.Count);
        Assert.Equal(4, persisted.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task LegacyMigrationUsesProviderReplayWithoutDuplicatingCurrentRequestAsync()
    {
        RecordingChatClient firstClient = new();
        ChatClientAgent firstAgent = new(firstClient, name: "agent");
        DurableAgentState legacyState = CreateStateWithExchange(
            "old",
            "old request",
            "old response");
        legacyState.Data.ConversationHistory.Add(
            new DurableAgentStateErrorResponse
            {
                CorrelationId = "failed",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-2),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "must not replay")),
                ],
            });
        legacyState.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "reasoning",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "private reasoning" },
                            new DurableAgentStateTextContent { Text = "visible answer" },
                        ],
                    },
                ],
            });

        DurableAgentState firstWrite = await RunEntityAsync(
            firstAgent,
            legacyState,
            new RunRequest("first new request") { CorrelationId = "new-1" });

        Assert.Equal(
            ["old request", "old response", "visible answer", "first new request"],
            firstClient.LastMessages.Select(message => message.Text));
        Assert.Equal(
            1,
            firstClient.LastMessages.Count(message => message.Text == "first new request"));
        Assert.DoesNotContain(
            firstClient.LastMessages.SelectMany(message => message.Contents),
            content => content is TextReasoningContent);
        Assert.Single(
            firstWrite.Data.ConversationHistory.OfType<DurableAgentStateRequest>(),
            entry => entry.CorrelationId == "new-1");
        Assert.Single(
            firstWrite.Data.ConversationHistory.OfType<DurableAgentStateResponse>(),
            entry => entry.CorrelationId == "new-1");

        RecordingChatClient secondClient = new();
        ChatClientAgent secondAgent = new(secondClient, name: "agent");
        DurableAgentState secondWrite = await RunEntityAsync(
            secondAgent,
            DeserializeState(SerializeState(firstWrite)),
            new RunRequest("second new request") { CorrelationId = "new-2" });

        Assert.Equal(
            [
                "old request",
                "old response",
                "visible answer",
                "first new request",
                "response",
                "second new request",
            ],
            secondClient.LastMessages.Select(message => message.Text));
        Assert.Equal(
            1,
            secondClient.LastMessages.Count(message => message.Text == "second new request"));
        Assert.Single(
            secondWrite.Data.ConversationHistory.OfType<DurableAgentStateRequest>(),
            entry => entry.CorrelationId == "new-2");
    }

    [Fact]
    public async Task NativeLegacyChatClientKeepsFullHistoryAcrossColdRestartAsync()
    {
        RecordingChatClient firstClient = new();
        ChatClientAgent firstAgent = new(firstClient, name: "agent");
        DurableAgentState legacyState = CreateStateWithExchange(
            "old",
            "old request",
            "old response");
        EntityHarness firstHarness = CreateHarness(
            firstAgent,
            legacyState,
            enableMailboxWrites: false);

        _ = await firstHarness.RunAsync(
            new RunRequest("first new request") { CorrelationId = "new-1" });
        DurableAgentState firstWrite =
            Assert.IsType<DurableAgentState>(firstHarness.PersistedState);

        Assert.Equal(
            ["old request", "old response", "first new request"],
            firstClient.LastMessages.Select(message => message.Text));
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, firstWrite.SchemaVersion);

        RecordingChatClient secondClient = new();
        ChatClientAgent secondAgent = new(secondClient, name: "agent");
        EntityHarness secondHarness = CreateHarness(
            secondAgent,
            DeserializeState(SerializeState(firstWrite)),
            enableMailboxWrites: false);

        _ = await secondHarness.RunAsync(
            new RunRequest("second new request") { CorrelationId = "new-2" });

        Assert.Equal(
            [
                "old request",
                "old response",
                "first new request",
                "response",
                "second new request",
            ],
            secondClient.LastMessages.Select(message => message.Text));
    }

    [Fact]
    public async Task CustomProviderOwnsTranscriptAndEntityStoresOnlyMailboxAndContinuationAsync()
    {
        RecordingChatClient client = new();
        RecordingHistoryProvider provider = new();
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = provider,
            });

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        Assert.Equal(1, provider.StoreCount);
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.HistoryProviderOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("external-history.v1", GetBinding(persisted)?.ProviderKey);
        Assert.Single(persisted.Data.TerminalResults!);
        Assert.Single(persisted.Data.CompletionReceipts!);
        Assert.True(
            persisted.Data.Session?.GetProperty("stateBag").TryGetProperty("external-history", out _) is true);
    }

    [Fact]
    public async Task ServiceManagedConversationStoresOnlyMailboxAndContinuationAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        AgentSession serviceSession = await agent.CreateSessionAsync("service-id");
        DurableAgentState initialState = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        initialState.Data.Session = await agent.SerializeSessionAsync(serviceSession);

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.SetHistoryProviderKey("agent", "model-service.v1"));

        Assert.Equal(["new request"], client.LastMessages.Select(message => message.Text));
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("model-service.v1", GetBinding(persisted)?.ProviderKey);
        Assert.Single(persisted.Data.TerminalResults!);
        Assert.Equal(
            "service-id",
            persisted.Data.Session?.GetProperty("conversationId").GetString());
    }

    [Fact]
    public async Task FirstServiceManagedTurnDoesNotLeaveEntityOwnedTranscriptAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "service-id" };
        ChatClientAgent agent = new(client, name: "agent");

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.SetHistoryProviderKey("agent", "model-service.v1"));

        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("model-service.v1", GetBinding(persisted)?.ProviderKey);
        Assert.Single(persisted.Data.TerminalResults!);
        Assert.Single(persisted.Data.CompletionReceipts!);
        Assert.Equal(
            "service-id",
            persisted.Data.Session?.GetProperty("conversationId").GetString());
    }

    [Fact]
    public async Task ServiceManagedPerCallDeclarationIsIgnoredWhenPerCallPersistenceIsDisabledAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState state = CreateStateWithExchange("old", "old request", "old response");

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            state,
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.SetServiceManagedPerServiceCallHistory("AGENT"));

        Assert.Equal(["old request", "old response", "new request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(4, persisted.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task PerServiceCallServiceOwnershipExcludesLocalProviderTranscriptStateAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "service-id" };
#pragma warning disable MAAI001
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("new request") { CorrelationId = "new" },
            options =>
            {
                options.SetServiceManagedPerServiceCallHistory("agent");
                options.SetHistoryProviderKey("agent", "model-service.v1");
            });

        Assert.Equal(["new request"], client.LastMessages.Select(message => message.Text));
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Single(persisted.Data.TerminalResults!);
        Assert.Equal("service-id", persisted.Data.Session?.GetProperty("conversationId").GetString());
        JsonElement serializedSession = persisted.Data.Session.GetValueOrDefault();
        Assert.DoesNotContain(
            nameof(InMemoryChatHistoryProvider),
            serializedSession.GetProperty("stateBag").EnumerateObject().Select(property => property.Name));
    }

    [Fact]
    public async Task AmbiguousPerServiceCallOwnershipFailsBeforeModelExecutionAsync()
    {
        RecordingChatClient client = new();
        RecordingHistoryProvider provider = new();
#pragma warning disable MAAI001
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = provider,
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        EntityHarness harness = CreateHarness(agent, new DurableAgentState());

        await Assert.ThrowsAsync<DurableAgentHistoryOwnershipNotSupportedException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task LegacyPerServiceCallStateCannotBeAdoptedAsServiceHistoryAsync()
    {
        RecordingChatClient client = new();
#pragma warning disable MAAI001
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        legacyState.Data.Session = await agent.SerializeSessionAsync(
            await agent.CreateSessionAsync("legacy-conversation"));
        EntityHarness harness = CreateHarness(
            agent,
            legacyState,
            options =>
            {
                options.SetServiceManagedPerServiceCallHistory("agent");
                options.SetHistoryProviderKey("agent", "model-service.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task StaleLocalPerCallSentinelWithCustomProviderFailsBeforeCallbacksAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        AgentSession session = await agent.CreateSessionAsync(
            DurableAgentHistoryBinding.FrameworkLocalHistoryConversationId);
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                Session = await agent.SerializeSessionAsync(session),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Theory]
    [InlineData("entity")]
    [InlineData("external")]
    [InlineData("service")]
    public async Task StatefulCompactionFailsBeforeEntityExecutionAsync(string ownership)
    {
        RecordingChatClient client = new();
        ChatHistoryProvider? historyProvider = ownership == "external"
            ? new RecordingHistoryProvider()
            : null;
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = historyProvider,
                AIContextProviders =
                [
                    new CompactionProvider(
                        new SlidingWindowCompactionStrategy(_ => true)),
                ],
            });
        DurableAgentState state = new();
        if (ownership == "service")
        {
            state.Data.Session = await agent.SerializeSessionAsync(
                await agent.CreateSessionAsync("service-id"));
        }

        Assert.Throws<DurableAgentCompactionNotSupportedException>(
            () => CreateHarness(agent, state));
        Assert.Equal(0, client.InvocationCount);
    }

    [Fact]
    public async Task FactoryAgentValidationRunsOnceBeforeSessionOrModelSideEffectsAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                AIContextProviders =
                [
                    new CompactionProvider(
                        new SlidingWindowCompactionStrategy(_ => true)),
                ],
            });
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            agent,
            new DurableAgentState(),
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        await Assert.ThrowsAsync<DurableAgentCompactionNotSupportedException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Equal(1, factoryInvocationCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExplicitInMemoryProviderFailsBeforeModelSideEffectsAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = new InMemoryChatHistoryProvider(
                    new InMemoryChatHistoryProviderOptions
                    {
                        StorageInputRequestMessageFilter = messages => messages.TakeLast(1),
                    }),
            });
        EntityHarness harness = CreateHarness(
            agent,
            new DurableAgentState(),
            registerWithFactory: true);

        await Assert.ThrowsAsync<DurableAgentHistoryOwnershipNotSupportedException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData(DurableAgentHistoryReplayMode.PreloadEntityHistory, true)]
    public async Task AgentWithoutContextPipelineAppliesReplayModeAndStorageSemanticsAsync(
        DurableAgentHistoryReplayMode? replayMode,
        bool expectsPreloadedHistory)
    {
        RecordingAgent agent = new("agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateErrorResponse
            {
                CorrelationId = "failed",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-2),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "must not replay")),
                ],
            });
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "reasoning",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "private reasoning" },
                            new DurableAgentStateTextContent { Text = "visible answer" },
                        ],
                    },
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "reasoning only" },
                        ],
                    },
                ],
            });
        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" },
            options =>
            {
                if (replayMode.HasValue)
                {
                    options.SetHistoryReplayMode("agent", replayMode.Value);
                }
            });

        Assert.Equal(
            expectsPreloadedHistory
                ? ["old request", "old response", "visible answer", "new request"]
                : ["new request"],
            agent.LastMessages.Select(message => message.Text));
        Assert.DoesNotContain(
            agent.LastMessages.SelectMany(message => message.Contents),
            content => content is TextReasoningContent);
        Assert.True(expectsPreloadedHistory);
        Assert.Equal(6, persisted.Data.ConversationHistory.Count);
        DurableAgentStateRequest storedRequest =
            Assert.IsType<DurableAgentStateRequest>(persisted.Data.ConversationHistory[^2]);
        DurableAgentStateMessage storedMessage = Assert.Single(storedRequest.Messages);
        Assert.Equal("new request", Assert.IsType<DurableAgentStateTextContent>(
            Assert.Single(storedMessage.Contents)).Text);
        Assert.IsType<DurableAgentStateResponse>(persisted.Data.ConversationHistory[^1]);
        Assert.Equal(DurableAgentStateHistoryBinding.DurableStateOwner, GetBinding(persisted)?.OwnerKind);

        Assert.Equal(4, persisted.Data.TerminalResults?.Count);
        Assert.Contains("new", persisted.Data.TerminalResults!.Keys);
        Assert.NotNull(persisted.Data.Session);
    }

    [Fact]
    public async Task CurrentRequestOnlySealsOpaqueOwnerAndSurvivesColdReloadAsync()
    {
        RecordingAgent firstAgent = new("agent");
        DurableAgentState firstWrite = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options =>
            {
                options.SetHistoryReplayMode("agent", DurableAgentHistoryReplayMode.CurrentRequestOnly);
                options.SetHistoryProviderKey("agent", "opaque-agent-session.v1");
            });

        RecordingAgent secondAgent = new("agent");
        DurableAgentState secondWrite = await RunEntityAsync(
            secondAgent,
            DeserializeState(SerializeState(firstWrite)),
            new RunRequest("second") { CorrelationId = "second" },
            options =>
            {
                options.SetHistoryReplayMode("agent", DurableAgentHistoryReplayMode.CurrentRequestOnly);
                options.SetHistoryProviderKey("agent", "opaque-agent-session.v1");
            });

        Assert.Equal(["first"], firstAgent.LastMessages.Select(message => message.Text));
        Assert.Equal(["second"], secondAgent.LastMessages.Select(message => message.Text));
        Assert.Empty(secondWrite.Data.ConversationHistory);
        Assert.Equal(2, secondWrite.Data.TerminalResults?.Count);
        Assert.Equal(DurableAgentStateHistoryBinding.HistoryProviderOwner, GetBinding(secondWrite)?.OwnerKind);
        Assert.Equal("opaque-agent-session.v1", GetBinding(secondWrite)?.ProviderKey);
    }

    [Fact]
    public async Task LegacyCurrentRequestOnlySessionCannotBeAdoptedWithoutPriorBindingAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        legacyState.Data.Session =
            await agent.SerializeSessionAsync(await agent.CreateSessionAsync());
        EntityHarness harness = CreateHarness(
            agent,
            legacyState,
            options =>
            {
                options.SetHistoryReplayMode("agent", DurableAgentHistoryReplayMode.CurrentRequestOnly);
                options.SetHistoryProviderKey("agent", "opaque-agent-session.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Empty(agent.LastMessages);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task AgentWithoutContextPipelineDoesNotReplayErrorResponsesAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateErrorResponse
            {
                CorrelationId = "failed",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "must not replay")),
                ],
            });

        _ = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(["old request", "old response", "new request"], agent.LastMessages.Select(message => message.Text));
    }

    [Fact]
    public async Task AgentWithoutContextPipelineFiltersReasoningFromReplayAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState initialState = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "old",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "reasoning only" },
                        ],
                    },
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "private reasoning" },
                            new DurableAgentStateTextContent { Text = "visible answer" },
                        ],
                    },
                ],
            });

        _ = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(["visible answer", "new request"], agent.LastMessages.Select(message => message.Text));
        Assert.DoesNotContain(
            agent.LastMessages.SelectMany(message => message.Contents),
            content => content is TextReasoningContent);
    }

    [Fact]
    public async Task EntityPreservesButDoesNotCreateCompactionEntriesAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "shared summary")),
                ],
            });

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(["shared summary", "new request"], client.LastMessages.Select(message => message.Text));
        DurableAgentStateCompaction compaction =
            Assert.Single(persisted.Data.ConversationHistory.OfType<DurableAgentStateCompaction>());
        Assert.Equal("shared summary", compaction.Messages[0].ToChatMessage().Text);
        Assert.Equal(3, persisted.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task WrappedServerManagedSessionSurvivesColdEntityInvocationAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "agent");
        AIAgent wrappedAgent = new TestDelegatingAgent(chatAgent);
        AgentSession serviceSession = await chatAgent.CreateSessionAsync("service-conversation");
        serviceSession.StateBag.SetValue("opaque-server-state", "preserved");
        DurableAgentState initialState = new()
        {
            Data =
            {
                Session = await wrappedAgent.SerializeSessionAsync(serviceSession),
            },
        };

        DurableAgentState persisted = await RunEntityAsync(
            wrappedAgent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.SetHistoryProviderKey("agent", "model-service.v1"));
        AgentSession restored = await wrappedAgent.DeserializeSessionAsync(
            persisted.Data.Session!.Value);
        ChatClientAgentSession restoredTyped = Assert.IsType<ChatClientAgentSession>(restored);

        Assert.Equal(["new request"], client.LastMessages.Select(message => message.Text));
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("service-conversation", restoredTyped.ConversationId);
        Assert.Equal("preserved", restoredTyped.StateBag.GetValue<string>("opaque-server-state"));
    }

    [Fact]
    public async Task LegacyMessageIdsPersistAcrossProviderLoadAndColdReloadAsync()
    {
        const string Json = """
            {
              "schemaVersion": "1.1.0",
              "data": {
                "conversationHistory": [
                  {
                    "$type": "request",
                    "correlationId": "old",
                    "createdAt": "2026-07-27T12:34:50+00:00",
                    "messages": [
                      {
                        "role": "user",
                        "contents": [{ "$type": "text", "text": "old request" }]
                      },
                      {
                        "role": "user",
                        "messageId": "producer-id",
                        "contents": [{ "$type": "text", "text": "preserved" }]
                      }
                    ]
                  },
                  {
                    "$type": "response",
                    "correlationId": "old",
                    "createdAt": "2026-07-27T12:34:51+00:00",
                    "messages": [
                      {
                        "role": "assistant",
                        "contents": []
                      },
                      {
                        "role": "assistant",
                        "contents": [{ "$type": "text", "text": "old response" }]
                      }
                    ]
                  }
                ]
              }
            }
            """;
        DurableAgentState initialState = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(Json, DurableAgentStateJsonContext.Default.DurableAgentState));
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");

        DurableAgentState firstWrite = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("first new request") { CorrelationId = "new-1" });
        string serialized = JsonSerializer.Serialize(
            firstWrite,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        DurableAgentState coldState = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(serialized, DurableAgentStateJsonContext.Default.DurableAgentState));
        string?[] firstIds = firstWrite.Data.ConversationHistory
            .Take(2)
            .SelectMany(entry => entry.Messages)
            .Select(message => message.MessageId)
            .ToArray();

        DurableAgentState secondWrite = await RunEntityAsync(
            agent,
            coldState,
            new RunRequest("second new request") { CorrelationId = "new-2" });
        string?[] secondIds = secondWrite.Data.ConversationHistory
            .Take(2)
            .SelectMany(entry => entry.Messages)
            .Select(message => message.MessageId)
            .ToArray();

        string?[] expectedIds =
            ["durable_request_old_0", "producer-id", "durable_response_old_0", "durable_response_old_1"];
        Assert.Equal(expectedIds, firstIds);
        Assert.Equal(firstIds, secondIds);
        Assert.Contains("\"messageId\":\"durable_response_old_1\"", serialized, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FailedFirstTurnDoesNotSealHistoryBindingOrMailboxAsync()
    {
        RecordingChatClient client = new() { Exception = new InvalidOperationException("model failed") };
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = new();
        EntityHarness harness = CreateHarness(agent, initialState);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.False(harness.StateWasPersisted);
        Assert.Equal(JsonValueKind.Undefined, initialState.Data.HistoryBinding.ValueKind);
        Assert.Null(initialState.Data.TerminalResults);
        Assert.Null(initialState.Data.CompletionReceipts);
    }

    [Fact]
    public async Task RecreatedExternalProviderWithSameLogicalKeyContinuesWithoutTranscriptMirrorAsync()
    {
        RecordingHistoryProvider firstProvider = new();
        ChatClientAgent firstAgent = CreateAgentWithProvider(new RecordingChatClient(), firstProvider);
        DurableAgentState firstWrite = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));
        DurableAgentState coldState = DeserializeState(SerializeState(firstWrite));

        RecordingHistoryProvider secondProvider = new();
        RecordingChatClient secondClient = new();
        ChatClientAgent secondAgent = CreateAgentWithProvider(secondClient, secondProvider);
        DurableAgentState secondWrite = await RunEntityAsync(
            secondAgent,
            coldState,
            new RunRequest("second") { CorrelationId = "second" },
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        Assert.Equal(1, secondProvider.LoadCount);
        Assert.Equal(1, secondProvider.StoreCount);
        Assert.Equal(["second"], secondClient.LastMessages.Select(message => message.Text));
        Assert.Empty(secondWrite.Data.ConversationHistory);
        Assert.Equal(2, secondWrite.Data.TerminalResults?.Count);
        Assert.Equal(2, secondWrite.Data.CompletionReceipts?.Count);
        Assert.Equal("external-history.v1", GetBinding(secondWrite)?.ProviderKey);
        Assert.True(
            secondWrite.Data.Session?.GetProperty("stateBag").TryGetProperty("external-history", out _) is true);
    }

    [Fact]
    public async Task ChangedExternalProviderKeyRejectsBeforeProviderOrModelCallbacksAsync()
    {
        ChatClientAgent firstAgent = CreateAgentWithProvider(
            new RecordingChatClient(),
            new RecordingHistoryProvider());
        DurableAgentState persisted = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        CountingSessionAgent replacementAgent = new(CreateAgentWithProvider(
            replacementClient,
            replacementProvider));
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            replacementAgent,
            DeserializeState(SerializeState(persisted)),
            options => options.SetHistoryProviderKey("agent", "external-history.v2"),
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, factoryInvocationCount);
        Assert.Equal(0, replacementAgent.DeserializeCount);
        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementProvider.StoreCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ChangedOwnerRejectsBeforeExternalProviderOrModelCallbacksAsync()
    {
        ChatClientAgent entityAgent = new(new RecordingChatClient(), name: "agent");
        DurableAgentState persisted = await RunEntityAsync(
            entityAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" });

        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        ChatClientAgent replacementAgent = CreateAgentWithProvider(
            replacementClient,
            replacementProvider);
        EntityHarness harness = CreateHarness(
            replacementAgent,
            DeserializeState(SerializeState(persisted)),
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task MissingExternalContinuationNeverCreatesReplacementConversationAsync()
    {
        RecordingHistoryProvider firstProvider = new();
        ChatClientAgent firstAgent = CreateAgentWithProvider(new RecordingChatClient(), firstProvider);
        DurableAgentState persisted = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));
        DurableAgentState missingContinuation = CopyState(persisted, session: null);

        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(replacementClient, replacementProvider),
            missingContinuation,
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task BoundExternalProviderRejectsSessionWithMissingDeclaredStateBeforeCallbacksAsync()
    {
        DurableAgentState persisted = await CreateBoundExternalStateAsync();
        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        ChatClientAgent replacementAgent = CreateAgentWithProvider(
            replacementClient,
            replacementProvider);
        JsonElement emptySession = await replacementAgent.SerializeSessionAsync(
            await replacementAgent.CreateSessionAsync());
        EntityHarness harness = CreateHarness(
            replacementAgent,
            CopyState(persisted, emptySession),
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExternalProviderCannotSealWithoutItsDeclaredContinuationStateAsync()
    {
        RecordingHistoryProvider provider = new() { SkipContinuationWrite = true };
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            new DurableAgentState(),
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExternalProviderWithoutStateKeysFailsBeforeCallbacksAsync()
    {
        EmptyStateKeysHistoryProvider provider = new();
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(
                client,
                new ChatClientAgentOptions
                {
                    Name = "agent",
                    ChatHistoryProvider = provider,
                }),
            new DurableAgentState(),
            options => options.SetHistoryProviderKey("agent", "empty-provider.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExternalProviderRequiresEveryDeclaredContinuationKeyAsync()
    {
        MultiKeyHistoryProvider provider = new(writeSecondKey: false);
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(
                client,
                new ChatClientAgentOptions
                {
                    Name = "agent",
                    ChatHistoryProvider = provider,
                }),
            new DurableAgentState(),
            options => options.SetHistoryProviderKey("agent", "multi-key-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExternalProviderSealsWhenEveryDeclaredContinuationKeyExistsAsync()
    {
        MultiKeyHistoryProvider provider = new(writeSecondKey: true);
        RecordingChatClient client = new();
        DurableAgentState persisted = await RunEntityAsync(
            new ChatClientAgent(
                client,
                new ChatClientAgentOptions
                {
                    Name = "agent",
                    ChatHistoryProvider = provider,
                }),
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.SetHistoryProviderKey("agent", "multi-key-history.v1"));

        Assert.Equal(1, provider.StoreCount);
        Assert.Equal("multi-key-history.v1", GetBinding(persisted)?.ProviderKey);
    }

    [Fact]
    public async Task AmbiguousLegacyExternalOwnershipFailsBeforeCallbacksAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        EntityHarness harness = CreateHarness(
            agent,
            legacyState,
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Contains("Legacy durable state", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task UnboundSchema2EntityTranscriptCannotSwitchToExternalProviderAsync()
    {
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                ConversationHistory =
                [
                    DurableAgentStateRequest.FromRunRequestV2(
                        new RunRequest("old request") { CorrelationId = "old" }),
                    DurableAgentStateResponse.FromResponseV2(
                        "old",
                        new AgentResponse(
                            new ChatMessage(ChatRole.Assistant, "old response"))),
                ],
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            state,
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task UnsealedEntitySessionWithInMemoryOnlyHistoryFailsBeforeModelAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        AgentSession session = await agent.CreateSessionAsync();
        session.StateBag.SetValue(
            nameof(InMemoryChatHistoryProvider),
            new InMemoryChatHistoryProvider.State
            {
                Messages = [new ChatMessage(ChatRole.User, "session-only history")],
            });
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                Session = await agent.SerializeSessionAsync(session),
            },
        };
        EntityHarness harness = CreateHarness(agent, state);

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ProvisionalExternalKeyRemainsUsableAfterCSharpSealWithoutExplicitOptionAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient firstClient = new();
        ChatClientAgent firstAgent = CreateAgentWithProvider(firstClient, provider);
        AgentSession session = await firstAgent.CreateSessionAsync();
        session.StateBag.SetValue(
            "external-history",
            new ExternalHistoryState { Count = 4 });
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = DurableAgentHistoryBinding.ToJson(
                    new DurableAgentStateHistoryBinding
                    {
                        OwnerKind = DurableAgentStateHistoryBinding.HistoryProviderOwner,
                        ProviderKey = "external-history.v1",
                    }),
                Session = await firstAgent.SerializeSessionAsync(session),
            },
        };

        DurableAgentState firstWrite = await RunEntityAsync(
            firstAgent,
            state,
            new RunRequest("first") { CorrelationId = "first" });
        Assert.True(
            firstWrite.Data.HistoryBinding
                .GetProperty("csharpFixedOwner")
                .GetBoolean());

        RecordingHistoryProvider secondProvider = new();
        RecordingChatClient secondClient = new();
        DurableAgentState secondWrite = await RunEntityAsync(
            CreateAgentWithProvider(secondClient, secondProvider),
            DeserializeState(SerializeState(firstWrite)),
            new RunRequest("second") { CorrelationId = "second" });

        Assert.Equal(1, secondProvider.LoadCount);
        Assert.Equal(1, secondClient.InvocationCount);
        Assert.Equal("external-history.v1", GetBinding(secondWrite)?.ProviderKey);
    }

    [Fact]
    public async Task OpaqueSharedBindingRejectsCurrentRequestOnlyBeforeModelAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = JsonSerializer.SerializeToElement(new
                {
                    runtime = "python",
                    perRunOwnership = true,
                }),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options =>
            {
                options.SetHistoryReplayMode("agent", DurableAgentHistoryReplayMode.CurrentRequestOnly);
                options.SetHistoryProviderKey("agent", "opaque-agent-session.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Empty(agent.LastMessages);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task OpaqueSharedBindingRejectsPerCallServiceBeforeModelAsync()
    {
        RecordingChatClient client = new();
#pragma warning disable MAAI001
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = JsonSerializer.SerializeToElement(new
                {
                    runtime = "python",
                    perRunOwnership = true,
                }),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options =>
            {
                options.SetServiceManagedPerServiceCallHistory("agent");
                options.SetHistoryProviderKey("agent", "model-service.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task OpaqueSharedBindingRejectsPostResponseServiceTransitionBeforeCommitAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "remote-conversation" };
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = JsonSerializer.SerializeToElement(new
                {
                    runtime = "python",
                    perRunOwnership = true,
                }),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options => options.SetHistoryProviderKey("agent", "model-service.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task LegacyExternalProviderAdoptsOnlyDeclaredContinuationEvidenceAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        AgentSession session = await agent.CreateSessionAsync();
        session.StateBag.SetValue(
            "external-history",
            new ExternalHistoryState { Count = 7 });
        legacyState.Data.Session = await agent.SerializeSessionAsync(session);

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            legacyState,
            new RunRequest("new") { CorrelationId = "new" },
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(2, persisted.Data.ConversationHistory.Count);
        Assert.Equal(DurableAgentStateHistoryBinding.HistoryProviderOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("external-history.v1", GetBinding(persisted)?.ProviderKey);
    }

    [Fact]
    public async Task UnexpectedServiceTransitionRejectsBeforeCommitButCannotUndoRemoteCallAsync()
    {
        ChatClientAgent firstAgent = new(new RecordingChatClient(), name: "agent");
        DurableAgentState persisted = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" });

        RecordingChatClient transitioningClient = new() { ResponseConversationId = "remote-conversation" };
        ChatClientAgent transitioningAgent = new(transitioningClient, name: "agent");
        EntityHarness harness = CreateHarness(
            transitioningAgent,
            DeserializeState(SerializeState(persisted)),
            options => options.SetHistoryProviderKey("agent", "model-service.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, transitioningClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task FirstServiceTransitionWithoutLogicalKeyWarnsAboutRemoteSideEffectsAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "remote-conversation" };
        ChatClientAgent agent = new(client, name: "agent");
        EntityHarness harness = CreateHarness(agent, new DurableAgentState());

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task CustomProviderServiceConflictWarnsAboutRemoteSideEffectsAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new() { ResponseConversationId = "remote-conversation" };
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            new DurableAgentState(),
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task MissingServiceConversationIdWarnsAboutRemoteSideEffectsAsync()
    {
        RecordingChatClient client = new() { SuppressConversationId = true };
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = new();
        initialState.Data.Session = await agent.SerializeSessionAsync(
            await agent.CreateSessionAsync("service-conversation"));
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.SetHistoryProviderKey("agent", "model-service.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("next") { CorrelationId = "next" }));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task AmbiguousLegacyServiceTransitionWarnsAboutRemoteSideEffectsAsync()
    {
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        RecordingChatClient client = new() { ResponseConversationId = "remote-conversation" };
        ChatClientAgent agent = new(client, name: "agent");
        EntityHarness harness = CreateHarness(
            agent,
            legacyState,
            options => options.SetHistoryProviderKey("agent", "model-service.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task MatchingHistoryBindingPreservesForwardCompatibleFieldsAsync()
    {
        ChatClientAgent agent = new(new RecordingChatClient(), name: "agent");
        DurableAgentState firstWrite = await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" });
        DurableAgentStateHistoryBinding firstBinding = GetBinding(firstWrite)!;
        Dictionary<string, JsonElement> bindingProperties =
            firstBinding.UnknownProperties?
                .ToDictionary(pair => pair.Key, pair => pair.Value) ?? [];
        bindingProperties["futureBindingField"] =
            JsonSerializer.SerializeToElement(new { preserve = true });
        DurableAgentState withFutureBindingField = CopyState(
            firstWrite,
            session: firstWrite.Data.Session,
            historyBinding: new DurableAgentStateHistoryBinding
            {
                OwnerKind = firstBinding.OwnerKind,
                ProviderKey = firstBinding.ProviderKey,
                UnknownProperties = bindingProperties,
            });

        DurableAgentState secondWrite = await RunEntityAsync(
            agent,
            withFutureBindingField,
            new RunRequest("second") { CorrelationId = "second" });

        Assert.True(
            GetBinding(secondWrite)?.UnknownProperties?
                .ContainsKey("futureBindingField") is true);
    }

    [Fact]
    public async Task FailureAfterConversationFinalizationDoesNotMutateHydratedStateAsync()
    {
        FailingSerializationAgent agent = new("agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");
        EntityHarness harness = CreateHarness(agent, initialState);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.True(agent.ExecutionCompleted);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(2, initialState.Data.ConversationHistory.Count);
        Assert.DoesNotContain(
            initialState.Data.ConversationHistory,
            entry => entry.CorrelationId == "new");
    }

    [Fact]
    public async Task ProviderLoadFailureDoesNotInvokeModelOrCommitWorkingStateAsync()
    {
        InvalidOperationException expected = new("provider load failed");
        RecordingHistoryProvider provider = new() { LoadException = expected };
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState initialState = await CreateBoundExternalStateAsync();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Same(expected, actual);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(originalState, SerializeState(initialState));
        Assert.Contains(harness.Logs, entry => ReferenceEquals(expected, entry.Exception));
    }

    [Fact]
    public async Task ProviderStoreFailureDoesNotCommitWorkingStateAsync()
    {
        InvalidOperationException expected = new("provider store failed");
        RecordingHistoryProvider provider = new() { StoreException = expected };
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState initialState = await CreateBoundExternalStateAsync();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Same(expected, actual);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(originalState, SerializeState(initialState));
        Assert.Contains(harness.Logs, entry => ReferenceEquals(expected, entry.Exception));
    }

    [Fact]
    public async Task ProviderLoadCancellationPropagatesWithoutCommitOrWrappingAsync()
    {
        using TestHostApplicationLifetime lifetime = new();
        RecordingHistoryProvider provider = new()
        {
            WaitForLoadCancellation = true,
        };
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState initialState = await CreateBoundExternalStateAsync();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.SetHistoryProviderKey("agent", "external-history.v1"),
            applicationLifetime: lifetime);

        Task runTask = harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });
        try
        {
            await WaitForProviderStageAsync(
                provider.LoadStarted.Task,
                runTask,
                "history provider load callback");
            lifetime.StopApplication();
            OperationCanceledException actual =
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => runTask.WaitAsync(s_testTimeout));

            Assert.Same(provider.LoadCancellationException, actual);
            Assert.Equal(lifetime.ApplicationStopping, provider.LoadCancellationToken);
            Assert.Equal(1, provider.LoadCount);
            Assert.Equal(0, provider.StoreCount);
            Assert.Equal(0, client.InvocationCount);
            Assert.False(harness.StateWasPersisted);
            Assert.Equal(originalState, SerializeState(initialState));
        }
        finally
        {
            lifetime.StopApplication();
            await JoinRunTaskAsync(runTask, "history provider load cancellation");
        }
    }

    [Fact]
    public async Task ProviderStoreCancellationPropagatesWithoutPartialCommitOrWrappingAsync()
    {
        using TestHostApplicationLifetime lifetime = new();
        RecordingHistoryProvider provider = new()
        {
            WaitForStoreCancellation = true,
        };
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState initialState = await CreateBoundExternalStateAsync();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.SetHistoryProviderKey("agent", "external-history.v1"),
            applicationLifetime: lifetime);

        Task runTask = harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });
        try
        {
            await WaitForProviderStageAsync(
                provider.StoreStarted.Task,
                runTask,
                "history provider store callback");
            lifetime.StopApplication();
            OperationCanceledException actual =
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => runTask.WaitAsync(s_testTimeout));

            Assert.Same(provider.StoreCancellationException, actual);
            Assert.Equal(lifetime.ApplicationStopping, client.LastCancellationToken);
            Assert.Equal(lifetime.ApplicationStopping, provider.LoadCancellationToken);
            Assert.Equal(lifetime.ApplicationStopping, provider.StoreCancellationToken);
            Assert.Equal(1, provider.LoadCount);
            Assert.Equal(1, provider.StoreCount);
            Assert.Equal(1, client.InvocationCount);
            Assert.False(harness.StateWasPersisted);
            Assert.Equal(originalState, SerializeState(initialState));
        }
        finally
        {
            lifetime.StopApplication();
            await JoinRunTaskAsync(runTask, "history provider store cancellation");
        }
    }

    private static async Task<DurableAgentState> RunEntityAsync(
        AIAgent agent,
        DurableAgentState state,
        RunRequest request,
        Action<DurableAgentsOptions>? configure = null)
    {
        EntityHarness harness = CreateHarness(agent, state, configure);
        await harness.RunAsync(request);
        return Assert.IsType<DurableAgentState>(harness.PersistedState);
    }

    private static EntityHarness CreateHarness(
        AIAgent agent,
        DurableAgentState state,
        Action<DurableAgentsOptions>? configure = null,
        bool registerWithFactory = false,
        Action? onFactoryInvoked = null,
        IHostApplicationLifetime? applicationLifetime = null,
        bool enableMailboxWrites = true)
    {
        AgentSessionId sessionId = new(agent.Name!, "session");
        DurableAgentsOptions options = new()
        {
            DefaultTimeToLive = null,
            EnableMailboxWrites = enableMailboxWrites,
            AuthorizeLegacyMigration = enableMailboxWrites ? static _ => true : null,
        };
        if (registerWithFactory)
        {
            options.AddAIAgentFactory(
                agent.Name!,
                _ =>
                {
                    onFactoryInvoked?.Invoke();
                    return agent;
                });
        }
        else
        {
            options.AddAIAgent(agent);
        }

        configure?.Invoke(options);

        ListLoggerProvider loggerProvider = new();
        Dictionary<Type, object> services = new()
        {
            [typeof(DurableTaskClient)] = new Mock<DurableTaskClient>("test").Object,
            [typeof(ILoggerFactory)] = new ListLoggerFactory(loggerProvider),
            [typeof(DurableAgentsOptions)] = options,
            [typeof(IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>>)] = options.GetAgentFactories(),
            [typeof(IHostApplicationLifetime)] = applicationLifetime ??
                Mock.Of<IHostApplicationLifetime>(
                    lifetime => lifetime.ApplicationStopping == CancellationToken.None),
        };
        IServiceProvider serviceProvider = new DictionaryServiceProvider(services);

        Mock<TaskEntityContext> context = new();
        context.SetupGet(value => value.Id).Returns(sessionId);
        Mock<TaskEntityState> entityState = new();
        entityState.Setup(value => value.GetState(typeof(DurableAgentState))).Returns(state);
        object? persistedState = null;
        entityState.Setup(value => value.SetState(It.IsAny<object?>()))
            .Callback<object?>(value => persistedState = value);

        Mock<TaskEntityOperation> operation = new();
        operation.SetupGet(value => value.Name).Returns(nameof(AgentEntity.Run));
        operation.SetupGet(value => value.Context).Returns(context.Object);
        operation.SetupGet(value => value.State).Returns(entityState.Object);
        operation.SetupGet(value => value.HasInput).Returns(true);

        AgentEntity entity = new(serviceProvider);
        return new EntityHarness(
            entity,
            operation,
            loggerProvider,
            () => persistedState);
    }

    private static ChatClientAgent CreateAgentWithProvider(
        RecordingChatClient client,
        RecordingHistoryProvider provider) =>
        new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = provider,
            });

    private static async Task<DurableAgentState> CreateBoundExternalStateAsync()
    {
        ChatClientAgent agent = CreateAgentWithProvider(
            new RecordingChatClient(),
            new RecordingHistoryProvider());
        return await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("seed") { CorrelationId = "seed" },
            options => options.SetHistoryProviderKey("agent", "external-history.v1"));
    }

    private static string SerializeState(DurableAgentState state) =>
        JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);

    private static DurableAgentState DeserializeState(string json) =>
        Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.DurableAgentState));

    private static DurableAgentStateHistoryBinding? GetBinding(
        DurableAgentState state) =>
        DurableAgentHistoryBinding.Parse(state.Data.HistoryBinding);

    private static DurableAgentState CopyState(
        DurableAgentState state,
        JsonElement? session,
        DurableAgentStateHistoryBinding? historyBinding = null)
    {
        return new DurableAgentState
        {
            SchemaVersion = state.SchemaVersion,
            Data = new DurableAgentStateData
            {
                ConversationHistory = state.Data.ConversationHistory,
                TerminalResults = state.Data.TerminalResults,
                CompletionReceipts = state.Data.CompletionReceipts,
                HistoryBinding = historyBinding is null
                    ? state.Data.HistoryBinding
                    : DurableAgentHistoryBinding.ToJson(historyBinding),
                Session = session,
                IngestedPositions = state.Data.IngestedPositions,
                Truncation = state.Data.Truncation,
                ExpirationTimeUtc = state.Data.ExpirationTimeUtc,
                ExtensionData = state.Data.ExtensionData,
                UnknownProperties = state.Data.UnknownProperties,
            },
            ExtensionData = state.ExtensionData,
            UnknownProperties = state.UnknownProperties,
        };
    }

    private static async Task WaitForProviderStageAsync(
        Task stageTask,
        Task runTask,
        string stageDescription)
    {
        Task completedTask;
        try
        {
            completedTask = await Task.WhenAny(stageTask, runTask).WaitAsync(s_stageTimeout);
        }
        catch (TimeoutException exception)
        {
            throw new Xunit.Sdk.XunitException(
                $"Timed out after {s_stageTimeout} waiting to reach the {stageDescription}.",
                exception);
        }

        if (ReferenceEquals(completedTask, runTask))
        {
            try
            {
                await runTask;
            }
            catch (Exception exception)
            {
                throw new Xunit.Sdk.XunitException(
                    $"The entity run failed before reaching the {stageDescription}.",
                    exception);
            }

            throw new Xunit.Sdk.XunitException(
                $"The entity run completed before reaching the {stageDescription}.");
        }

        await stageTask;
    }

    private static async Task JoinRunTaskAsync(Task runTask, string scenarioDescription)
    {
        try
        {
            await runTask.WaitAsync(s_testTimeout);
        }
        catch (OperationCanceledException) when (runTask.IsCanceled)
        {
            // The test body asserts the propagated cancellation; cleanup only joins the same task.
        }
        catch (TimeoutException exception)
        {
            throw new Xunit.Sdk.XunitException(
                $"Timed out after {s_testTimeout} joining the entity run during cleanup for {scenarioDescription}; the test may have leaked a running task.",
                exception);
        }
        catch (Exception exception)
        {
            throw new Xunit.Sdk.XunitException(
                $"The entity run faulted unexpectedly during cleanup for {scenarioDescription}.",
                exception);
        }
    }

    private static DurableAgentState CreateStateWithExchange(
        string correlationId,
        string request,
        string response)
    {
        DurableAgentState state = new();
        AddExchange(state, correlationId, request, response, DateTimeOffset.UtcNow.AddMinutes(-5));
        return state;
    }

    private static void AddExchange(
        DurableAgentState state,
        string correlationId,
        string request,
        string response,
        DateTimeOffset createdAt)
    {
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = correlationId,
                CreatedAt = createdAt,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.User, request) { CreatedAt = createdAt }),
                ],
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = correlationId,
                CreatedAt = createdAt,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, response) { CreatedAt = createdAt }),
                ],
            });
    }

    private sealed class EntityHarness(
        AgentEntity entity,
        Mock<TaskEntityOperation> operation,
        ListLoggerProvider loggerProvider,
        Func<object?> persistedState)
    {
        public object? PersistedState => persistedState();

        public bool StateWasPersisted => this.PersistedState is not null;

        public IReadOnlyList<LogRecord> Logs => loggerProvider.Records;

        public async Task<AgentResponse> RunAsync(RunRequest request)
        {
            operation.Setup(value => value.GetInput(typeof(RunRequest))).Returns(request);
            object? result = await ((ITaskEntity)entity).RunAsync(operation.Object);
            return Assert.IsType<AgentResponse>(result);
        }
    }

    private sealed class TestDelegatingAgent(AIAgent innerAgent) : DelegatingAIAgent(innerAgent);

    private sealed class CountingSessionAgent(AIAgent innerAgent) : DelegatingAIAgent(innerAgent)
    {
        public int DeserializeCount { get; private set; }

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default)
        {
            this.DeserializeCount++;
            return base.DeserializeSessionCoreAsync(
                serializedState,
                jsonSerializerOptions,
                cancellationToken);
        }
    }

    private sealed class RecordingAgent(string name) : AIAgent
    {
        public override string? Name => name;

        public List<ChatMessage> LastMessages { get; private set; } = [];

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) => new(new RecordingSession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(JsonSerializer.SerializeToElement(new { stateBag = session.StateBag.Serialize() }));

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(new RecordingSession());

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.LastMessages = messages.ToList();
            await Task.Yield();
            yield return new AgentResponseUpdate(ChatRole.Assistant, "response");
        }

        private sealed class RecordingSession : AgentSession;
    }

    private sealed class FailingSerializationAgent(string name) : AIAgent
    {
        public override string? Name => name;

        public bool ExecutionCompleted { get; private set; }

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) => new(new FailingSerializationSession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("session serialization failed");

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(new FailingSerializationSession());

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.ExecutionCompleted = true;
            await Task.Yield();
            yield return new AgentResponseUpdate(ChatRole.Assistant, "response");
        }

        private sealed class FailingSerializationSession : AgentSession;
    }

    private sealed class RecordingHistoryProvider : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => ["external-history"];

        public Exception? LoadException { get; init; }

        public Exception? StoreException { get; init; }

        public bool WaitForLoadCancellation { get; init; }

        public bool WaitForStoreCancellation { get; init; }

        public bool SkipContinuationWrite { get; init; }

        public TaskCompletionSource<bool> LoadStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<bool> StoreStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int LoadCount { get; private set; }

        public int StoreCount { get; private set; }

        public CancellationToken LoadCancellationToken { get; private set; }

        public CancellationToken StoreCancellationToken { get; private set; }

        public OperationCanceledException? LoadCancellationException { get; private set; }

        public OperationCanceledException? StoreCancellationException { get; private set; }

        protected override async ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context,
            CancellationToken cancellationToken = default)
        {
            this.LoadCount++;
            this.LoadCancellationToken = cancellationToken;
            if (this.WaitForLoadCancellation)
            {
                this.LoadStarted.TrySetResult(true);
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken)
                        .WaitAsync(s_stageTimeout, CancellationToken.None);
                }
                catch (OperationCanceledException exception)
                {
                    this.LoadCancellationException = exception;
                    throw;
                }
                catch (TimeoutException exception)
                {
                    throw new Xunit.Sdk.XunitException(
                        $"Timed out after {s_stageTimeout} waiting for ApplicationStopping during provider load.",
                        exception);
                }
            }

            if (this.LoadException is not null)
            {
                throw this.LoadException;
            }

            return [];
        }

        protected override async ValueTask StoreChatHistoryAsync(
            InvokedContext context,
            CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            this.StoreCancellationToken = cancellationToken;
            if (!this.SkipContinuationWrite)
            {
                context.Session!.StateBag.SetValue(
                    "external-history",
                    new ExternalHistoryState { Count = this.StoreCount });
            }
            if (this.WaitForStoreCancellation)
            {
                this.StoreStarted.TrySetResult(true);
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken)
                        .WaitAsync(s_stageTimeout, CancellationToken.None);
                }
                catch (OperationCanceledException exception)
                {
                    this.StoreCancellationException = exception;
                    throw;
                }
                catch (TimeoutException exception)
                {
                    throw new Xunit.Sdk.XunitException(
                        $"Timed out after {s_stageTimeout} waiting for ApplicationStopping during provider store.",
                        exception);
                }
            }

            if (this.StoreException is not null)
            {
                throw this.StoreException;
            }
        }
    }

    private sealed class ExternalHistoryState
    {
        public int Count { get; set; }
    }

    private sealed class MultiKeyHistoryProvider(bool writeSecondKey) : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => ["external-primary", "external-index"];

        public int StoreCount { get; private set; }

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context,
            CancellationToken cancellationToken = default) =>
            new([]);

        protected override ValueTask StoreChatHistoryAsync(
            InvokedContext context,
            CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            context.Session!.StateBag.SetValue("external-primary", "primary");
            if (writeSecondKey)
            {
                context.Session.StateBag.SetValue("external-index", "index");
            }

            return default;
        }
    }

    private sealed class EmptyStateKeysHistoryProvider : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => [];

        public int LoadCount { get; private set; }

        public int StoreCount { get; private set; }

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context,
            CancellationToken cancellationToken = default)
        {
            this.LoadCount++;
            return new([]);
        }

        protected override ValueTask StoreChatHistoryAsync(
            InvokedContext context,
            CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            return default;
        }
    }

    private sealed class RecordingChatClient : IChatClient
    {
        public Exception? Exception { get; init; }

        public string? ResponseConversationId { get; init; }

        public bool SuppressConversationId { get; init; }

        public int InvocationCount { get; private set; }

        public CancellationToken LastCancellationToken { get; private set; }

        public List<ChatMessage> LastMessages { get; private set; } = [];

        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.InvocationCount++;
            this.LastCancellationToken = cancellationToken;
            this.LastMessages = messages.ToList();
            if (this.Exception is not null)
            {
                throw this.Exception;
            }

            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, "response")
            {
                ConversationId = this.SuppressConversationId
                    ? null
                    : this.ResponseConversationId ?? options?.ConversationId,
            };
        }
    }

    private sealed class TestHostApplicationLifetime : IHostApplicationLifetime, IDisposable
    {
        private readonly CancellationTokenSource _applicationStopping = new();

        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => this._applicationStopping.Token;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication() => this._applicationStopping.Cancel();

        public void Dispose() => this._applicationStopping.Dispose();
    }

    private sealed class ListLoggerProvider : ILoggerProvider
    {
        public List<LogRecord> Records { get; } = [];

        public ILogger CreateLogger(string categoryName) => new ListLogger(this.Records);

        public void Dispose()
        {
        }
    }

    private sealed class ListLoggerFactory : ILoggerFactory
    {
        private readonly ListLoggerProvider _provider;

        public ListLoggerFactory(ListLoggerProvider provider)
        {
            this._provider = provider;
        }

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => this._provider.CreateLogger(categoryName);

        public void Dispose() => this._provider.Dispose();
    }

    private sealed class DictionaryServiceProvider(IReadOnlyDictionary<Type, object> services) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            services.TryGetValue(serviceType, out object? service) ? service : null;
    }

    private sealed class ListLogger(List<LogRecord> records) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            records.Add(new LogRecord(logLevel, eventId, exception, formatter(state, exception)));
        }
    }

    private sealed record LogRecord(
        LogLevel Level,
        EventId EventId,
        Exception? Exception,
        string Message);
}
