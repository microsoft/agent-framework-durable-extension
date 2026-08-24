// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;

namespace Microsoft.Agents.AI.DurableTask.UnitTests.Workflows;

/// <summary>
/// Tests for workflow registration on <see cref="DurableWorkflowOptions"/>.
/// </summary>
public sealed class DurableWorkflowOptionsTests
{
    [Fact]
    public void AddWorkflow_ThrowsWhenDifferentWorkflowUsesRegisteredName()
    {
        // Arrange
        DurableWorkflowOptions options = new DurableOptions().Workflows;
        Workflow first = CreateWorkflow("OrderPipeline", "StepA");
        Workflow second = CreateWorkflow("OrderPipeline", "StepB");

        options.AddWorkflow(first);

        // Act
        ArgumentException ex = Assert.Throws<ArgumentException>(() => options.AddWorkflow(second));

        // Assert
        Assert.Contains("has already been registered", ex.Message, StringComparison.Ordinal);
        Assert.Same(first, options.Workflows["OrderPipeline"]);
    }

    [Fact]
    public void AddWorkflow_ThrowsWhenNameDiffersOnlyByCase()
    {
        // Arrange - workflow names are compared case-insensitively because they map to orchestration names.
        DurableWorkflowOptions options = new DurableOptions().Workflows;
        Workflow first = CreateWorkflow("OrderPipeline", "StepA");
        options.AddWorkflow(first);

        // Act
        Assert.Throws<ArgumentException>(() => options.AddWorkflow(CreateWorkflow("orderpipeline", "StepB")));

        // Assert - the original registration is left untouched.
        Assert.Same(first, Assert.Single(options.Workflows).Value);
    }

    [Fact]
    public void AddWorkflow_IsIdempotentForSameInstance()
    {
        // Arrange
        DurableWorkflowOptions options = new DurableOptions().Workflows;
        Workflow workflow = CreateWorkflow("OrderPipeline", "Step");

        // Act
        options.AddWorkflow(workflow);
        options.AddWorkflow(workflow);

        // Assert
        Assert.Single(options.Workflows);
        Assert.Same(workflow, options.Workflows["OrderPipeline"]);
    }

    [Fact]
    public void AddWorkflow_AllowsSubWorkflowThatIsAlsoRegisteredExplicitly()
    {
        // Arrange - registering a parent and its sub-workflow explicitly means the recursive registration
        // walk re-adds the same sub-workflow instance.
        DurableWorkflowOptions options = new DurableOptions().Workflows;
        Workflow subWorkflow = CreateWorkflow("SharedSub", "SubStep");
        Workflow parent = CreateParentWorkflow("Parent", "ParentStep", subWorkflow, "Sub");

        // Act
        options.AddWorkflow(subWorkflow);
        options.AddWorkflow(parent);
        AddSubWorkflows(options, parent);
        AddSubWorkflows(options, parent);

        // Assert
        Assert.Equal(2, options.Workflows.Count);
        Assert.Same(subWorkflow, options.Workflows["SharedSub"]);
    }

    [Fact]
    public void AddWorkflow_ThrowsWhenWorkflowIsNullOrUnnamed()
    {
        DurableWorkflowOptions options = new DurableOptions().Workflows;

        Assert.Throws<ArgumentNullException>(() => options.AddWorkflow(null!));
        Assert.Throws<ArgumentException>(() => options.AddWorkflow(
            new WorkflowBuilder(new FunctionExecutor<string>("Step", (_, _, _) => default)).Build()));
    }

    private static void AddSubWorkflows(DurableWorkflowOptions options, Workflow workflow)
    {
        foreach (SubworkflowBinding binding in workflow.ReflectExecutors()
            .Select(e => e.Value)
            .OfType<SubworkflowBinding>())
        {
            options.AddWorkflow(binding.WorkflowInstance);
        }
    }

    private static Workflow CreateWorkflow(string workflowName, string executorName) =>
        new WorkflowBuilder(new FunctionExecutor<string>(executorName, (_, _, _) => default))
            .WithName(workflowName)
            .Build();

    private static Workflow CreateParentWorkflow(
        string workflowName,
        string executorName,
        Workflow subWorkflow,
        string subWorkflowExecutorName)
    {
        FunctionExecutor<string> start = new(executorName, (_, _, _) => default);
        ExecutorBinding subWorkflowExecutor = subWorkflow.BindAsExecutor(subWorkflowExecutorName);

        return new WorkflowBuilder(start)
            .WithName(workflowName)
            .AddEdge(start, subWorkflowExecutor)
            .Build();
    }
}
