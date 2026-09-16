// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Client.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests;

public sealed class DurableAIAgentProxyTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("""{"nested":[null,0,""]}""")]
    public async Task ProxyRetainsCanonicalResultWithoutChangingNativeResponseAsync(string? valueJson)
    {
        AgentSessionId sessionId = new("agentA", "session");
        DateTimeOffset completedAt = DateTimeOffset.UtcNow;
        using JsonDocument metadata = JsonDocument.Parse("""{"preserve":[1,null]}""");
        DurableAgentStateTerminalResponse terminalResponse = new()
        {
            Messages = [],
            ResponseId = "retained-response",
            Value = valueJson is null
                ? default
                : JsonSerializer.Deserialize(valueJson, DurableAgentStateJsonContext.Default.JsonElement),
            UnknownProperties = new Dictionary<string, JsonElement>
            {
                ["futureResponseField"] = metadata.RootElement.Clone(),
            },
            Usage = new DurableAgentStateUsage
            {
                ExtensionData = new Dictionary<string, JsonElement>
                {
                    ["providerObject"] = metadata.RootElement.Clone(),
                },
            },
        };
        DurableAgentState state = CreateRevisedState(new DurableAgentStateTerminalResult
        {
            CorrelationId = "correlation",
            Outcome = DurableAgentStateCompletionReceipt.SucceededOutcome,
            CompletedAt = completedAt,
            Response = terminalResponse,
        }, new DurableAgentStateCompletionReceipt
        {
            CorrelationId = "correlation",
            Outcome = DurableAgentStateCompletionReceipt.SucceededOutcome,
            CompletedAt = completedAt,
            ResultState = DurableAgentStateCompletionReceipt.AvailableResult,
        });
        DurableAIAgentProxy proxy = new("agentA", new HandleDurableAgentClient(CreateHandle(sessionId, state)));

        AgentResponse response = await proxy.RunAsync(
            new ChatMessage(ChatRole.User, "request"), new DurableAgentSession(sessionId));
        JsonElement result = Assert.IsType<JsonElement>(DurableAgentJsonUtilities.GetRetainedResult(response));

        Assert.Equal("retained-response", result.GetProperty("responseId").GetString());
        Assert.True(JsonElement.DeepEquals(metadata.RootElement, result.GetProperty("futureResponseField")));
        Assert.True(JsonElement.DeepEquals(metadata.RootElement, result.GetProperty("usage").GetProperty("extensionData").GetProperty("providerObject")));
        Assert.Equal(valueJson is not null, result.TryGetProperty("value", out JsonElement value));
        if (valueJson is not null)
        {
            Assert.True(JsonElement.DeepEquals(terminalResponse.Value, value));
        }

        JsonElement native = JsonSerializer.SerializeToElement(
            response, DurableAgentJsonUtilities.DefaultOptions.GetTypeInfo(typeof(AgentResponse)));
        Assert.False(native.TryGetProperty("value", out _));
        Assert.False(native.TryGetProperty("futureResponseField", out _));
        Assert.Null(response.RawRepresentation);
        terminalResponse.UnknownProperties!.Clear();
        Assert.True(Assert.IsType<JsonElement>(DurableAgentJsonUtilities.GetRetainedResult(response))
            .TryGetProperty("futureResponseField", out _));
    }

    [Fact]
    public void OrdinaryResponseDoesNotAcquireAnInferredDurableResult()
    {
        Assert.Null(DurableAgentJsonUtilities.GetRetainedResult(
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "null"))));
    }

    // Verifies the proxy rejects a session whose agent name differs from its own,
    // and that the durable client is never called when this happens.
    [Fact]
    public async Task RunAsync_ThrowsWhenSessionBelongsToDifferentAgentAsync()
    {
        StubDurableAgentClient client = new();
        DurableAIAgentProxy proxy = new("agentA", client);
        DurableAgentSession session = new(new AgentSessionId("agentB", "shared-key"));

        ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            proxy.RunAsync(new ChatMessage(ChatRole.User, "hello"), session));

        Assert.Equal("session", ex.ParamName);
        Assert.Contains("agentB", ex.Message, StringComparison.Ordinal);
        Assert.Contains("agentA", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, client.CallCount);
    }

    // Control test: when the session's agent name matches the proxy's name,
    // the request is forwarded to the durable client.
    [Fact]
    public async Task RunAsync_AllowsSessionWhenAgentNameMatchesAsync()
    {
        AgentSessionId sessionId = new("agentA", "shared-key");
        InvalidOperationException sentinel = new("reached the client");
        StubDurableAgentClient client = new() { Throw = sentinel };
        DurableAIAgentProxy proxy = new("agentA", client);
        DurableAgentSession session = new(sessionId);

        // Reaching the durable client (and therefore propagating the sentinel) proves the
        // name-matching guard accepted this session.
        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            proxy.RunAsync(new ChatMessage(ChatRole.User, "hello"), session));

        Assert.Same(sentinel, ex);
        Assert.Equal(1, client.CallCount);
        Assert.Equal(sessionId, client.LastSessionId);
    }

    // Ensures the agent-name comparison is case-insensitive, so casing differences
    // are neither a false-positive rejection nor a bypass.
    [Fact]
    public async Task RunAsync_AgentNameComparisonIsCaseInsensitiveAsync()
    {
        AgentSessionId sessionId = new("AGENTA", "shared-key");
        InvalidOperationException sentinel = new("reached the client");
        StubDurableAgentClient client = new() { Throw = sentinel };
        DurableAIAgentProxy proxy = new("agentA", client);
        DurableAgentSession session = new(sessionId);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            proxy.RunAsync(new ChatMessage(ChatRole.User, "hello"), session));

        Assert.Same(sentinel, ex);
        Assert.Equal(1, client.CallCount);
    }

    [Theory]
    [InlineData(DurableAgentStateCompletionReceipt.SucceededOutcome)]
    [InlineData(DurableAgentStateCompletionReceipt.FailedOutcome)]
    public async Task RunAsync_PropagatesCompletedResultUnavailableAsync(string outcome)
    {
        AgentSessionId sessionId = new("agentA", "shared-key");
        DurableAgentState state = CreateUnavailableState(outcome);
        DurableAIAgentProxy proxy = new(
            "agentA",
            new HandleDurableAgentClient(CreateHandle(sessionId, state)));

        DurableAgentResultUnavailableException exception =
            await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
                () => proxy.RunAsync(
                    new ChatMessage(ChatRole.User, "hello"),
                    new DurableAgentSession(sessionId)));

        Assert.NotNull(exception.CompletedAt);
        Assert.Equal(outcome, exception.Outcome);
    }

    [Fact]
    public async Task RunAsync_PropagatesTerminalErrorMetadataAsync()
    {
        AgentSessionId sessionId = new("agentA", "shared-key");
        DurableAgentState state = CreateFailedState();
        DurableAIAgentProxy proxy = new(
            "agentA",
            new HandleDurableAgentClient(CreateHandle(sessionId, state)));

        DurableAgentTerminalException exception =
            await Assert.ThrowsAsync<DurableAgentTerminalException>(
                () => proxy.RunAsync(
                    new ChatMessage(ChatRole.User, "hello"),
                    new DurableAgentSession(sessionId)));

        Assert.Equal("Example", exception.Code);
        Assert.Equal("recorded failure", exception.Message);
        Assert.Equal("failed response", exception.Response?.Text);
    }

    private static AgentRunHandle CreateHandle(
        AgentSessionId sessionId,
        DurableAgentState state)
    {
        Mock<DurableEntityClient> entities = new("test");
        entities.Setup(client => client.GetEntityAsync<DurableAgentState>(
                sessionId,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new EntityMetadata<DurableAgentState>(sessionId, state));
        Mock<DurableTaskClient> client = new("test");
        client.SetupGet(value => value.Entities).Returns(entities.Object);
        return new AgentRunHandle(
            client.Object,
            NullLogger.Instance,
            sessionId,
            "correlation");
    }

    private static DurableAgentState CreateUnavailableState(string outcome)
    {
        DateTimeOffset completedAt = DateTimeOffset.UtcNow.AddMinutes(-1);
        return CreateRevisedState(
            terminalResult: null,
            new DurableAgentStateCompletionReceipt
            {
                CorrelationId = "correlation",
                Outcome = outcome,
                CompletedAt = completedAt,
                ResultState = DurableAgentStateCompletionReceipt.UnavailableResult,
                ResultUnavailableAt = completedAt.AddSeconds(1),
            });
    }

    private static DurableAgentState CreateFailedState()
    {
        DateTimeOffset completedAt = DateTimeOffset.UtcNow;
        DurableAgentStateTerminalResult result = new()
        {
            CorrelationId = "correlation",
            Outcome = DurableAgentStateCompletionReceipt.FailedOutcome,
            CompletedAt = completedAt,
            Response = DurableAgentStateTerminalResponse.FromResponse(
                new AgentResponse(new ChatMessage(ChatRole.Assistant, "failed response")),
                "correlation",
                completedAt),
            Error = new DurableAgentStateTerminalError
            {
                Code = "Example",
                Message = "recorded failure",
            },
        };
        return CreateRevisedState(
            result,
            new DurableAgentStateCompletionReceipt
            {
                CorrelationId = "correlation",
                Outcome = result.Outcome,
                CompletedAt = completedAt,
                ResultState = DurableAgentStateCompletionReceipt.AvailableResult,
            });
    }

    private static DurableAgentState CreateRevisedState(
        DurableAgentStateTerminalResult? terminalResult,
        DurableAgentStateCompletionReceipt receipt)
    {
        return new DurableAgentState
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            Data = new DurableAgentStateData
            {
                ConversationHistory = [],
                TerminalResults = terminalResult is null
                    ? new Dictionary<string, DurableAgentStateTerminalResult>()
                    : new Dictionary<string, DurableAgentStateTerminalResult>
                    {
                        ["correlation"] = terminalResult,
                    },
                CompletionReceipts =
                    new Dictionary<string, DurableAgentStateCompletionReceipt>
                    {
                        ["correlation"] = receipt,
                    },
            },
        };
    }

    private sealed class StubDurableAgentClient : IDurableAgentClient
    {
        public int CallCount { get; private set; }
        public AgentSessionId LastSessionId { get; private set; }
        public Exception? Throw { get; set; }

        public Task<AgentRunHandle> RunAgentAsync(
            AgentSessionId sessionId,
            RunRequest request,
            CancellationToken cancellationToken)
        {
            this.CallCount++;
            this.LastSessionId = sessionId;
            if (this.Throw is not null)
            {
                return Task.FromException<AgentRunHandle>(this.Throw);
            }

            throw new InvalidOperationException("Test did not configure a response.");
        }
    }

    private sealed class HandleDurableAgentClient(AgentRunHandle handle) : IDurableAgentClient
    {
        public Task<AgentRunHandle> RunAgentAsync(
            AgentSessionId sessionId,
            RunRequest request,
            CancellationToken cancellationToken) =>
            Task.FromResult(handle);
    }
}
