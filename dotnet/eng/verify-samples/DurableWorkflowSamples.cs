// Copyright (c) Microsoft. All rights reserved.

namespace VerifySamples;

/// <summary>
/// Defines the expected behavior for migrated durable workflow samples.
/// </summary>
internal static class DurableWorkflowSamples
{
    private const string AzureFunctionsSkipReason =
        "Requires Azure Functions Core Tools runtime and starts a web host.";

    public static IReadOnlyList<SampleDefinition> ConsoleApps { get; } =
    [
        new SampleDefinition
        {
            Name = "DurableWorkflows_Console_01_SequentialWorkflow",
            ProjectPath = "samples/DurableWorkflows/ConsoleApps/01_SequentialWorkflow",
            Inputs = ["order-123", "exit"],
            MustContain =
            [
                "Durable Workflow Sample",
                "Workflow completed.",
            ],
            IsDeterministic = true,
        },
        new SampleDefinition
        {
            Name = "DurableWorkflows_Console_02_ConcurrentWorkflow",
            ProjectPath = "samples/DurableWorkflows/ConsoleApps/02_ConcurrentWorkflow",
            RequiredEnvironmentVariables = ["FOUNDRY_PROJECT_ENDPOINT", "FOUNDRY_MODEL"],
            Inputs = ["What is water?", "exit"],
            ExpectedOutputDescription =
            [
                "The output should show a fan-out/fan-in workflow with physicist and chemist agents.",
                "The output should not contain error messages or stack traces.",
            ],
        },
        new SampleDefinition
        {
            Name = "DurableWorkflows_Console_03_ConditionalEdges",
            ProjectPath = "samples/DurableWorkflows/ConsoleApps/03_ConditionalEdges",
            Inputs = ["order-123", "order-B-456", "exit"],
            MustContain =
            [
                "Enter an order ID",
                "Workflow completed.",
            ],
            IsDeterministic = true,
        },
        new SampleDefinition
        {
            Name = "DurableWorkflows_Console_04_WorkflowAndAgents",
            ProjectPath = "samples/DurableWorkflows/ConsoleApps/04_WorkflowAndAgents",
            RequiredEnvironmentVariables = ["FOUNDRY_PROJECT_ENDPOINT", "FOUNDRY_MODEL"],
            ExpectedOutputDescription =
            [
                "The output should show durable workflow and durable agent registration working together.",
                "The output should not contain error messages or stack traces.",
            ],
        },
        new SampleDefinition
        {
            Name = "DurableWorkflows_Console_05_WorkflowEvents",
            ProjectPath = "samples/DurableWorkflows/ConsoleApps/05_WorkflowEvents",
            MustContain =
            [
                "Workflow Events",
                "completed",
            ],
            IsDeterministic = true,
        },
        new SampleDefinition
        {
            Name = "DurableWorkflows_Console_06_WorkflowSharedState",
            ProjectPath = "samples/DurableWorkflows/ConsoleApps/06_WorkflowSharedState",
            MustContain =
            [
                "Shared State",
                "completed",
            ],
            IsDeterministic = true,
        },
        new SampleDefinition
        {
            Name = "DurableWorkflows_Console_07_SubWorkflows",
            ProjectPath = "samples/DurableWorkflows/ConsoleApps/07_SubWorkflows",
            MustContain =
            [
                "Sub-Workflow",
                "completed",
            ],
            IsDeterministic = true,
        },
        new SampleDefinition
        {
            Name = "DurableWorkflows_Console_08_WorkflowHITL",
            ProjectPath = "samples/DurableWorkflows/ConsoleApps/08_WorkflowHITL",
            Inputs = ["approve", "exit"],
            ExpectedOutputDescription =
            [
                "The output should show a human-in-the-loop durable workflow.",
                "The output should not contain error messages or stack traces.",
            ],
        },
    ];

    public static IReadOnlyList<SampleDefinition> AzureFunctions { get; } =
    [
        SkippedAzureFunctionsSample("DurableWorkflows_AzureFunctions_01_SequentialWorkflow", "samples/DurableWorkflows/AzureFunctions/01_SequentialWorkflow"),
        SkippedAzureFunctionsSample("DurableWorkflows_AzureFunctions_02_ConcurrentWorkflow", "samples/DurableWorkflows/AzureFunctions/02_ConcurrentWorkflow"),
        SkippedAzureFunctionsSample("DurableWorkflows_AzureFunctions_03_WorkflowHITL", "samples/DurableWorkflows/AzureFunctions/03_WorkflowHITL"),
        SkippedAzureFunctionsSample("DurableWorkflows_AzureFunctions_04_WorkflowMcpTool", "samples/DurableWorkflows/AzureFunctions/04_WorkflowMcpTool"),
        SkippedAzureFunctionsSample("DurableWorkflows_AzureFunctions_05_WorkflowAndAgents", "samples/DurableWorkflows/AzureFunctions/05_WorkflowAndAgents"),
    ];

    private static SampleDefinition SkippedAzureFunctionsSample(string name, string projectPath)
        => new()
        {
            Name = name,
            ProjectPath = projectPath,
            SkipReason = AzureFunctionsSkipReason,
        };
}
