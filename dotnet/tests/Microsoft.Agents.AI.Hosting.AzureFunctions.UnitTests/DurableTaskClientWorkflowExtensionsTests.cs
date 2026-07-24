// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests;

/// <summary>
/// Tests for <see cref="DurableTaskClientExtensions.AsWorkflowClient"/>, which gives function code a
/// Functions-native way to invoke a registered workflow without going through its HTTP surface.
/// </summary>
public sealed class DurableTaskClientWorkflowExtensionsTests
{
    private const string WorkflowTestName = "TestWorkflow";
    private const string OrchestrationName = "dafx-" + WorkflowTestName;
    private const string InstanceId = "test-instance-123";

    // The end-to-end assertion for this feature: a function holding only a [DurableClient] binding and
    // a FunctionContext can start a registered workflow by name.
    [Fact]
    public async Task AsWorkflowClient_StartsRegisteredWorkflowByNameAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        FunctionContext context = CreateContext(CreateServiceProviderWithWorkflows());

        // Act
        IWorkflowClient workflowClient = mockClient.Object.AsWorkflowClient(context);
        IWorkflowRun run = await workflowClient.RunAsync(WorkflowTestName, "hello");

        // Assert
        Assert.Equal(InstanceId, run.RunId);
        mockClient.Verify(
            c => c.ScheduleNewOrchestrationInstanceAsync(
                It.Is<TaskName>(n => n.Name == OrchestrationName),
                It.IsAny<object>(),
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task AsWorkflowClient_ThrowsWhenWorkflowNotRegisteredAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        FunctionContext context = CreateContext(CreateServiceProviderWithWorkflows());

        // Act
        IWorkflowClient workflowClient = mockClient.Object.AsWorkflowClient(context);

        // Assert
        await Assert.ThrowsAsync<WorkflowNotRegisteredException>(
            async () => await workflowClient.RunAsync("UnknownWorkflow", "hello"));
    }

    // Without any durable configuration there is nothing to resolve names against, so surface an
    // actionable configuration error rather than a dependency-resolution failure.
    [Fact]
    public void AsWorkflowClient_ThrowsWhenDurableServicesNotConfigured()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        FunctionContext context = CreateContext(new ServiceCollection().BuildServiceProvider());

        // Act & Assert
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => mockClient.Object.AsWorkflowClient(context));

        Assert.Contains(nameof(FunctionsApplicationBuilderExtensions.ConfigureDurableWorkflows), ex.Message, StringComparison.Ordinal);
    }

    // ConfigureDurableAgents also registers DurableOptions, so an agent-only app gets a usable client.
    // The failure surfaces at call time, as the more precise WorkflowNotRegisteredException.
    [Fact]
    public async Task AsWorkflowClient_InAgentOnlyAppThrowsWorkflowNotRegisteredAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        ServiceCollection services = new();
        services.ConfigureDurableAgents(agents => agents.AddAIAgent(new TestAgent("TestAgent", "An agent used for testing.")));
        using ServiceProvider provider = services.BuildServiceProvider();
        FunctionContext context = CreateContext(provider);

        // Act
        IWorkflowClient workflowClient = mockClient.Object.AsWorkflowClient(context);

        // Assert
        WorkflowNotRegisteredException ex = await Assert.ThrowsAsync<WorkflowNotRegisteredException>(
            async () => await workflowClient.RunAsync(WorkflowTestName, "hello"));

        Assert.Equal(WorkflowTestName, ex.WorkflowName);
    }

    [Fact]
    public void AsWorkflowClient_ThrowsOnNullArguments()
    {
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        FunctionContext context = CreateContext(CreateServiceProviderWithWorkflows());

        Assert.Throws<ArgumentNullException>(() => DurableTaskClientExtensions.AsWorkflowClient(null!, context));
        Assert.Throws<ArgumentNullException>(() => mockClient.Object.AsWorkflowClient(null!));
    }

    private static Mock<DurableTaskClient> CreateMockClient()
    {
        Mock<DurableTaskClient> mockClient = new("test");
        mockClient
            .Setup(c => c.ScheduleNewOrchestrationInstanceAsync(
                It.IsAny<TaskName>(),
                It.IsAny<object>(),
                It.IsAny<StartOrchestrationOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(InstanceId);
        return mockClient;
    }

    private static ServiceProvider CreateServiceProviderWithWorkflows()
    {
        Workflow workflow = new WorkflowBuilder(new FunctionExecutor<string>("start", (_, _, _) => default))
            .WithName(WorkflowTestName)
            .Build();

        ServiceCollection services = new();
        services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(workflow));
        return services.BuildServiceProvider();
    }

    private static FunctionContext CreateContext(IServiceProvider services)
    {
        Mock<FunctionContext> mockContext = new();
        mockContext.SetupGet(c => c.InstanceServices).Returns(services);
        return mockContext.Object;
    }
}
