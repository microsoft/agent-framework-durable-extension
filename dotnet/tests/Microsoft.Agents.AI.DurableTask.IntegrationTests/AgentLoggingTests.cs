// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.IntegrationTests.Logging;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.IntegrationTests;

public sealed class AgentLoggingTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("TestAgent", "testagent", "logging-session")]
    [InlineData("TripPlanningAgent", "tripplanningagent", "logging-session")]
    [InlineData("TestAgent.Child", "testagent.child", "logging.session.Key")]
    [InlineData("TeSt.AgEnt", "test.agent", "MiXeD.Key")]
    public async Task EntityDispatchCapturesRequestAndResponseForMixedCaseAgentAsync(
        string agentName,
        string normalizedName,
        string sessionKey)
    {
        TestLoggerProvider loggerProvider = new(output);
        using IHost host = CreateHost(loggerProvider, agentName);

        // The real SDK normalizes the entity name before the worker dispatches the operation.
        AgentSessionId sessionId = new(agentName, sessionKey);
        EntityInstanceId entityId = sessionId;
        Assert.Equal($"dafx-{normalizedName}", entityId.Name);
        await DispatchAsync(host, sessionId, "Hello!");
        Assert.NotEmpty(loggerProvider.GetAllLogs());
        IReadOnlyCollection<LogEntry> agentLogs = loggerProvider.GetAgentLogs(sessionId);
        Assert.NotEmpty(agentLogs);
        Assert.All(agentLogs, log =>
            Assert.Equal($"Microsoft.DurableTask.Agents.{normalizedName}.{entityId.Key}", log.Category));
        Assert.Contains(agentLogs, log => log.EventId.Name == "LogAgentRequest" && log.Message.Contains("Hello!"));
        Assert.Contains(agentLogs, log => log.EventId.Name == "LogAgentResponse");
    }

    [Theory]
    [InlineData("Microsoft.Hosting.Lifetime.TestAgent")]
    [InlineData("Microsoft.DurableTask.Agents.othertestagent.session")]
    [InlineData("Microsoft.DurableTask.Agents.testagentextra.session")]
    [InlineData("Microsoft.DurableTask.Agents.testagent")]
    [InlineData("Microsoft.DurableTask.Agents.OtherTestAgent.session")]
    [InlineData("Microsoft.DurableTask.Agents.TestAgentExtra.session")]
    [InlineData("Microsoft.DurableTask.Agents.TestAgent")]
    public void AgentLogCaptureExcludesUnrelatedCategories(string category)
    {
        TestLoggerProvider loggerProvider = new(output);
        using ILoggerFactory factory = LoggerFactory.Create(logging => logging.AddProvider(loggerProvider));
        factory.CreateLogger(category).LogInformation("Unrelated log");

        Assert.NotEmpty(loggerProvider.GetAllLogs());
        Assert.Empty(loggerProvider.GetAgentLogs(new AgentSessionId("TestAgent", "session")));
    }

    [Theory]
    [InlineData("shared.session")]
    [InlineData("different.session")]
    public async Task EntityDispatchExcludesDotNamedChildAgentAsync(string childKey)
    {
        TestLoggerProvider loggerProvider = new(output);
        using IHost host = CreateHost(loggerProvider, "TestAgent", "TestAgent.Child");
        AgentSessionId parentSession = new("TestAgent", "shared.session");
        AgentSessionId childSession = new("TestAgent.Child", childKey);

        await DispatchAsync(host, parentSession, "Parent request");
        await DispatchAsync(host, childSession, "Child request");

        IReadOnlyCollection<LogEntry> parentLogs = loggerProvider.GetAgentLogs(parentSession);
        Assert.Single(parentLogs, log => log.EventId.Name == "LogAgentRequest");
        Assert.Single(parentLogs, log => log.EventId.Name == "LogAgentResponse");
        Assert.Contains(parentLogs, log => log.Message.Contains("Parent request", StringComparison.Ordinal));
        Assert.DoesNotContain(parentLogs, log => log.Message.Contains("Child request", StringComparison.Ordinal));
        Assert.All(parentLogs, log => Assert.Equal("Microsoft.DurableTask.Agents.testagent.shared.session", log.Category));
        IReadOnlyCollection<LogEntry> childLogs = loggerProvider.GetAgentLogs(childSession);
        Assert.Single(childLogs, log => log.EventId.Name == "LogAgentRequest");
        Assert.Single(childLogs, log => log.EventId.Name == "LogAgentResponse");
        Assert.Contains(childLogs, log => log.Message.Contains("Child request", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("shared.session", "different.session")]
    [InlineData("shared.Session", "shared.session")]
    [InlineData("shared.session", "shared.session.child")]
    public async Task EntityDispatchExcludesOtherSessionKeysAsync(string expectedKey, string otherKey)
    {
        TestLoggerProvider loggerProvider = new(output);
        using IHost host = CreateHost(loggerProvider, "TestAgent.Child");
        AgentSessionId expectedSession = new("TestAgent.Child", expectedKey);
        AgentSessionId otherSession = new("TestAgent.Child", otherKey);

        await DispatchAsync(host, expectedSession, "Expected request");
        await DispatchAsync(host, otherSession, "Other request");

        IReadOnlyCollection<LogEntry> logs = loggerProvider.GetAgentLogs(expectedSession);
        Assert.Single(logs, log => log.EventId.Name == "LogAgentRequest");
        Assert.Single(logs, log => log.EventId.Name == "LogAgentResponse");
        Assert.Contains(logs, log => log.Message.Contains("Expected request", StringComparison.Ordinal));
        Assert.DoesNotContain(logs, log => log.Message.Contains("Other request", StringComparison.Ordinal));
        Assert.All(logs, log =>
            Assert.Equal($"Microsoft.DurableTask.Agents.testagent.child.{expectedKey}", log.Category));
        IReadOnlyCollection<LogEntry> otherLogs = loggerProvider.GetAgentLogs(otherSession);
        Assert.Single(otherLogs, log => log.EventId.Name == "LogAgentRequest");
        Assert.Single(otherLogs, log => log.EventId.Name == "LogAgentResponse");
        Assert.Contains(otherLogs, log => log.Message.Contains("Other request", StringComparison.Ordinal));
        Assert.DoesNotContain(otherLogs, log => log.Message.Contains("Expected request", StringComparison.Ordinal));
    }

    [Fact]
    public async Task PublicProxySessionIdSelectsFullEntityCategoryAsync()
    {
        TestLoggerProvider loggerProvider = new(output);
        using IHost host = CreateHost(loggerProvider, "TestAgent.Child");
        AIAgent proxy = host.Services.GetDurableAgentProxy("TestAgent.Child");
        AgentSession session = await proxy.CreateSessionAsync();
        AgentSessionId sessionId = session.GetService<AgentSessionId>();
        Assert.NotEmpty(sessionId.Key);

        await DispatchAsync(host, sessionId, "Public session request");

        IReadOnlyCollection<LogEntry> logs = loggerProvider.GetAgentLogs(sessionId);
        Assert.Single(logs, log => log.EventId.Name == "LogAgentRequest");
        Assert.Single(logs, log => log.EventId.Name == "LogAgentResponse");
        Assert.Contains(logs, log => log.Message.Contains("Public session request", StringComparison.Ordinal));
        Assert.All(logs, log =>
            Assert.Equal($"Microsoft.DurableTask.Agents.testagent.child.{sessionId.Key}", log.Category));
    }

    private static IHost CreateHost(TestLoggerProvider loggerProvider, params string[] agentNames) =>
        Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(new Mock<DurableTaskClient>("logging-test").Object);
                services.AddSingleton<IDurableAgentClient, DefaultDurableAgentClient>();
                services.ConfigureDurableAgents(options =>
                {
                    options.DefaultTimeToLive = null;
                    foreach (string agentName in agentNames)
                    {
                        options.AddAIAgent(new ChatClientAgent(new LocalChatClient(), name: agentName));
                    }
                });
            })
            .ConfigureLogging(logging =>
            {
                logging.AddProvider(loggerProvider);
                logging.SetMinimumLevel(LogLevel.Debug);
            })
            .Build();

    private static async Task DispatchAsync(IHost host, AgentSessionId sessionId, string prompt)
    {
        EntityInstanceId entityId = sessionId;
        Mock<TaskEntityContext> context = new(MockBehavior.Strict);
        context.SetupGet(value => value.Id).Returns(entityId);
        DurableAgentState initialState = new();
        string expectedSchemaVersion = initialState.SchemaVersion;
        DurableAgentState? persistedState = null;
        Mock<TaskEntityState> state = new(MockBehavior.Strict);
        state.SetupGet(value => value.HasState).Returns(true);
        state.Setup(value => value.GetState(typeof(DurableAgentState))).Returns(initialState);
        state.Setup(value => value.SetState(It.IsAny<object?>()))
            .Callback<object?>(value => persistedState = Assert.IsType<DurableAgentState>(value));
        RunRequest request = new(prompt) { CorrelationId = "logging-request" };
        Mock<TaskEntityOperation> operation = new(MockBehavior.Strict);
        operation.SetupGet(value => value.Name).Returns(nameof(AgentEntity.Run));
        operation.SetupGet(value => value.Context).Returns(context.Object);
        operation.SetupGet(value => value.State).Returns(state.Object);
        operation.SetupGet(value => value.HasInput).Returns(true);
        operation.Setup(value => value.GetInput(typeof(RunRequest))).Returns(request);

        ITaskEntity entity = new AgentEntity(host.Services);
        AgentResponse response = Assert.IsType<AgentResponse>(await entity.RunAsync(operation.Object));

        Assert.NotEmpty(response.Text);
        Assert.NotNull(persistedState);
        Assert.Equal(expectedSchemaVersion, persistedState.SchemaVersion);
        Assert.False(host.Services.GetRequiredService<DurableAgentsOptions>().EnablePersistentRequestOutcomes);
    }

    private sealed class LocalChatClient : IChatClient
    {
        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, "Hello from the logging test!");
        }
    }
}
