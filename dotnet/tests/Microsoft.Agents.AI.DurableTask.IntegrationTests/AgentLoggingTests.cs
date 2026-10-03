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
    [InlineData("TestAgent", "testagent")]
    [InlineData("TripPlanningAgent", "tripplanningagent")]
    public async Task EntityDispatchCapturesRequestAndResponseForMixedCaseAgentAsync(string agentName, string normalizedName)
    {
        ChatClientAgent agent = new(new LocalChatClient(), name: agentName);
        TestLoggerProvider loggerProvider = new(output);
        using IHost host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(new Mock<DurableTaskClient>("logging-test").Object);
                services.ConfigureDurableAgents(options =>
                {
                    options.DefaultTimeToLive = null;
                    options.AddAIAgent(agent);
                });
            })
            .ConfigureLogging(logging =>
            {
                logging.AddProvider(loggerProvider);
                logging.SetMinimumLevel(LogLevel.Debug);
            })
            .Build();

        // The real SDK normalizes the entity name before the worker dispatches the operation.
        EntityInstanceId entityId = new(AgentSessionId.ToEntityName(agentName), "logging-session");
        Assert.Equal($"dafx-{normalizedName}", entityId.Name);
        Mock<TaskEntityContext> context = new(MockBehavior.Strict);
        context.SetupGet(value => value.Id).Returns(entityId);
        DurableAgentState initialState = new();
        DurableAgentState? persistedState = null;
        Mock<TaskEntityState> state = new(MockBehavior.Strict);
        state.SetupGet(value => value.HasState).Returns(true);
        state.Setup(value => value.GetState(typeof(DurableAgentState))).Returns(initialState);
        state.Setup(value => value.SetState(It.IsAny<object?>()))
            .Callback<object?>(value => persistedState = Assert.IsType<DurableAgentState>(value));
        RunRequest request = new("Hello!") { CorrelationId = "logging-request" };
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
        Assert.Equal(initialState.SchemaVersion, persistedState.SchemaVersion);
        Assert.False(host.Services.GetRequiredService<DurableAgentsOptions>().EnablePersistentRequestOutcomes);
        Assert.NotEmpty(loggerProvider.GetAllLogs());
        IReadOnlyCollection<LogEntry> agentLogs = loggerProvider.GetAgentLogs(agentName);
        Assert.NotEmpty(agentLogs);
        Assert.All(agentLogs, log =>
            Assert.Equal($"Microsoft.DurableTask.Agents.{normalizedName}.{entityId.Key}", log.Category));
        Assert.Contains(agentLogs, log => log.EventId.Name == "LogAgentRequest" && log.Message.Contains("Hello!"));
        Assert.Contains(agentLogs, log => log.EventId.Name == "LogAgentResponse");
    }

    [Theory]
    [InlineData("Microsoft.Hosting.Lifetime.TestAgent")]
    [InlineData("Microsoft.DurableTask.Agents.OtherTestAgent.session")]
    [InlineData("Microsoft.DurableTask.Agents.TestAgentExtra.session")]
    [InlineData("Microsoft.DurableTask.Agents.TestAgent")]
    public void AgentLogCaptureExcludesUnrelatedCategories(string category)
    {
        TestLoggerProvider loggerProvider = new(output);
        using ILoggerFactory factory = LoggerFactory.Create(logging => logging.AddProvider(loggerProvider));
        factory.CreateLogger(category).LogInformation("Unrelated log");

        Assert.NotEmpty(loggerProvider.GetAllLogs());
        Assert.Empty(loggerProvider.GetAgentLogs("TestAgent"));
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
