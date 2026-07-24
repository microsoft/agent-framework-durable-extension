// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests.Workflows;

/// <summary>
/// Tests for the name-based overloads of <see cref="IWorkflowClient"/>, which allow a workflow to be
/// invoked without a reference to the <see cref="Workflow"/> object.
/// </summary>
public sealed class DurableWorkflowClientTests
{
    private const string WorkflowTestName = "TestWorkflow";
    private const string OrchestrationName = "dafx-" + WorkflowTestName;
    private const string InstanceId = "test-instance-123";

    [Fact]
    public async Task RunAsync_ByName_SchedulesOrchestrationForRegisteredWorkflowAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());

        // Act
        IWorkflowRun run = await client.RunAsync(WorkflowTestName, "hello");

        // Assert
        Assert.Equal(InstanceId, run.RunId);
        mockClient.Verify(
            c => c.ScheduleNewOrchestrationInstanceAsync(
                It.Is<TaskName>(n => n.Name == OrchestrationName),
                It.Is<object>(o => ((DurableWorkflowInput<string>)o).Input == "hello"),
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RunAsync_ByName_UsesRunIdAsInstanceIdAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());

        // Act
        await client.RunAsync(WorkflowTestName, "hello", runId: "custom-run-id");

        // Assert
        mockClient.Verify(
            c => c.ScheduleNewOrchestrationInstanceAsync(
                It.IsAny<TaskName>(),
                It.IsAny<object>(),
                It.Is<StartOrchestrationOptions>(o => o.InstanceId == "custom-run-id"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task RunAsync_ByName_SupportsTypedInputAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());
        OrderRequest input = new("order-123");

        // Act
        await client.RunAsync(WorkflowTestName, input);

        // Assert
        mockClient.Verify(
            c => c.ScheduleNewOrchestrationInstanceAsync(
                It.Is<TaskName>(n => n.Name == OrchestrationName),
                It.Is<object>(o => ((DurableWorkflowInput<OrderRequest>)o).Input == input),
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    // The registry is case-insensitive, and lookups resolve to the canonically-registered workflow,
    // so the orchestration name uses the registered casing rather than the caller's.
    [Fact]
    public async Task RunAsync_ByName_IsCaseInsensitiveAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());

        // Act
        await client.RunAsync("testWORKFLOW", "hello");

        // Assert
        mockClient.Verify(
            c => c.ScheduleNewOrchestrationInstanceAsync(
                It.Is<TaskName>(n => n.Name == OrchestrationName),
                It.IsAny<object>(),
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    // A typo'd or unregistered name should fail fast with an actionable error rather than
    // scheduling an orchestration that no worker can execute.
    [Fact]
    public async Task RunAsync_ByName_ThrowsWhenWorkflowNotRegisteredAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());

        // Act
        WorkflowNotRegisteredException ex = await Assert.ThrowsAsync<WorkflowNotRegisteredException>(
            async () => await client.RunAsync("UnknownWorkflow", "hello"));

        // Assert
        Assert.Equal("UnknownWorkflow", ex.WorkflowName);
        Assert.Contains("UnknownWorkflow", ex.Message, StringComparison.Ordinal);
        mockClient.Verify(
            c => c.ScheduleNewOrchestrationInstanceAsync(
                It.IsAny<TaskName>(),
                It.IsAny<object>(),
                It.IsAny<StartOrchestrationOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task RunAsync_ByName_ThrowsWhenNameIsEmptyAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(
            async () => await client.RunAsync(string.Empty, "hello"));
    }

    // A bare `null` literal is ambiguous between the Workflow and workflow-name overloads,
    // so the cast pins this to the name-based overload.
    [Fact]
    public async Task RunAsync_ByName_ThrowsWhenNameIsNullAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(
            async () => await client.RunAsync((string)null!, "hello"));
    }

    [Fact]
    public async Task StreamAsync_ByName_SchedulesOrchestrationForRegisteredWorkflowAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());

        // Act
        IStreamingWorkflowRun run = await client.StreamAsync(WorkflowTestName, "hello");

        // Assert
        Assert.Equal(InstanceId, run.RunId);
        mockClient.Verify(
            c => c.ScheduleNewOrchestrationInstanceAsync(
                It.Is<TaskName>(n => n.Name == OrchestrationName),
                It.Is<object>(o => ((DurableWorkflowInput<string>)o).Input == "hello"),
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task StreamAsync_ByName_ThrowsWhenWorkflowNotRegisteredAsync()
    {
        // Arrange
        Mock<DurableTaskClient> mockClient = CreateMockClient();
        DurableWorkflowClient client = CreateClient(mockClient, CreateTestWorkflow());

        // Act & Assert
        await Assert.ThrowsAsync<WorkflowNotRegisteredException>(
            async () => await client.StreamAsync("UnknownWorkflow", "hello"));
    }

    [Fact]
    public void Constructor_ThrowsWhenArgumentsAreNull()
    {
        Mock<DurableTaskClient> mockClient = CreateMockClient();

        Assert.Throws<ArgumentNullException>(() => new DurableWorkflowClient(null!, new DurableOptions()));
        Assert.Throws<ArgumentNullException>(() => new DurableWorkflowClient(mockClient.Object, null!));
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

    private static DurableWorkflowClient CreateClient(Mock<DurableTaskClient> mockClient, params Workflow[] workflows)
    {
        DurableOptions options = new();
        options.Workflows.AddWorkflows(workflows);
        return new DurableWorkflowClient(mockClient.Object, options);
    }

    private static Workflow CreateTestWorkflow() =>
        new WorkflowBuilder(new FunctionExecutor<string>("start", (_, _, _) => default))
            .WithName(WorkflowTestName)
            .Build();

    private sealed record OrderRequest(string OrderId);
}
