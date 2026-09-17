// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests.Workflows;

/// <summary>
/// Tests for <see cref="DurableWorkflowRunner"/>.
/// </summary>
public sealed class DurableWorkflowRunnerTests
{
    private const string InstanceId = "workflow-instance";
    private const string WorkflowName = "SuperstepWorkflow";

    [Fact]
    public async Task RunWorkflowOrchestrationAsync_CyclicWorkflowExceedsLimit_ThrowsAndDoesNotLogCompletionAsync()
    {
        // Arrange
        FunctionExecutor<string, string> first = CreateExecutor("first");
        FunctionExecutor<string, string> second = CreateExecutor("second");
        Workflow workflow = new WorkflowBuilder(first)
            .WithName(WorkflowName)
            .AddEdge(first, second)
            .AddEdge(second, first)
            .Build();

        DurableOptions options = new();
        options.Workflows.MaxSupersteps = 3;
        options.Workflows.AddWorkflow(workflow);

        Mock<TaskOrchestrationContext> context = CreateContext();
        RecordingLogger logger = new();
        DurableWorkflowRunner runner = new(options);

        // Act
        MaxSuperstepsExceededException exception = await Assert.ThrowsAsync<MaxSuperstepsExceededException>(
            () => runner.RunWorkflowOrchestrationAsync(
                context.Object,
                new DurableWorkflowInput<object> { Input = "start" },
                logger));

        // Assert
        Assert.Equal(InstanceId, exception.InstanceId);
        Assert.Equal(3, exception.MaxSupersteps);
        Assert.Equal(1, exception.RemainingExecutors);
        Assert.Contains(InstanceId, exception.Message, StringComparison.Ordinal);
        Assert.Contains("maximum of 3 supersteps", exception.Message, StringComparison.Ordinal);
        Assert.Contains("1 executor(s) still queued", exception.Message, StringComparison.Ordinal);
        context.Verify(c => c.CallActivityAsync<string>(
            It.IsAny<TaskName>(),
            It.IsAny<object?>(),
            It.IsAny<TaskOptions?>()), Times.Exactly(3));
        Assert.Contains(104, logger.EventIds);
        Assert.DoesNotContain(103, logger.EventIds);
    }

    [Fact]
    public async Task RunWorkflowOrchestrationAsync_WorkflowCompletesAtLimit_ReturnsResultAsync()
    {
        // Arrange
        FunctionExecutor<string, string> first = CreateExecutor("first");
        FunctionExecutor<string, string> second = CreateExecutor("second");
        Workflow workflow = new WorkflowBuilder(first)
            .WithName(WorkflowName)
            .AddEdge(first, second)
            .Build();

        DurableOptions options = new();
        options.Workflows.MaxSupersteps = 2;
        options.Workflows.AddWorkflow(workflow);

        Mock<TaskOrchestrationContext> context = CreateContext();
        RecordingLogger logger = new();
        DurableWorkflowRunner runner = new(options);

        // Act
        DurableWorkflowResult result = await runner.RunWorkflowOrchestrationAsync(
            context.Object,
            new DurableWorkflowInput<object> { Input = "start" },
            logger);

        // Assert
        Assert.Equal("next", result.Result);
        context.Verify(c => c.CallActivityAsync<string>(
            It.IsAny<TaskName>(),
            It.IsAny<object?>(),
            It.IsAny<TaskOptions?>()), Times.Exactly(2));
        Assert.Contains(103, logger.EventIds);
        Assert.DoesNotContain(104, logger.EventIds);
    }

    private static FunctionExecutor<string, string> CreateExecutor(string id)
        => new(id, (input, _, _) => input, outputTypes: [typeof(string)]);

    private static Mock<TaskOrchestrationContext> CreateContext()
    {
        string executorOutput = JsonSerializer.Serialize(
            new DurableExecutorOutput { Result = "next" },
            DurableWorkflowJsonContext.Default.DurableExecutorOutput);

        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(c => c.Name).Returns(WorkflowNamingHelper.ToOrchestrationFunctionName(WorkflowName));
        context.SetupGet(c => c.InstanceId).Returns(InstanceId);
        context.SetupGet(c => c.IsReplaying).Returns(false);
        context.Setup(c => c.CallActivityAsync<string>(
                It.IsAny<TaskName>(),
                It.IsAny<object?>(),
                It.IsAny<TaskOptions?>()))
            .ReturnsAsync(executorOutput);

        return context;
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<int> EventIds { get; } = [];

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
            => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            this.EventIds.Add(eventId.Id);
        }

        private sealed class NullScope : IDisposable
        {
            public static NullScope Instance { get; } = new();

            public void Dispose()
            {
            }
        }
    }
}
