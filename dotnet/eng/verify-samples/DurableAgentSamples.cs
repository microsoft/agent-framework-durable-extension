// Copyright (c) Microsoft. All rights reserved.

namespace VerifySamples;

/// <summary>
/// Defines the expected behavior for migrated durable agent samples.
/// </summary>
internal static class DurableAgentSamples
{
    private const string AzureFunctionsSkipReason =
        "Requires Azure Functions Core Tools runtime and starts a web host.";

    public static IReadOnlyList<SampleDefinition> ConsoleApps { get; } =
    [
        new SampleDefinition
        {
            Name = "DurableAgents_Console_01_SingleAgent",
            ProjectPath = "samples/DurableAgents/ConsoleApps/01_SingleAgent",
            RequiredEnvironmentVariables = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT_NAME"],
            Inputs = ["Tell me a joke about a pirate", "exit"],
            ExpectedOutputDescription =
            [
                "The output should show a single Joker agent responding to the user's request.",
                "The output should not contain error messages or stack traces.",
            ],
        },
        new SampleDefinition
        {
            Name = "DurableAgents_Console_02_AgentOrchestration_Chaining",
            ProjectPath = "samples/DurableAgents/ConsoleApps/02_AgentOrchestration_Chaining",
            RequiredEnvironmentVariables = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT_NAME"],
            ExpectedOutputDescription =
            [
                "The output should show a durable orchestration that chains agent calls.",
                "The output should not contain error messages or stack traces.",
            ],
        },
        new SampleDefinition
        {
            Name = "DurableAgents_Console_03_AgentOrchestration_Concurrency",
            ProjectPath = "samples/DurableAgents/ConsoleApps/03_AgentOrchestration_Concurrency",
            RequiredEnvironmentVariables = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT_NAME"],
            ExpectedOutputDescription =
            [
                "The output should show multiple durable agents running concurrently in an orchestration.",
                "The output should not contain error messages or stack traces.",
            ],
        },
        new SampleDefinition
        {
            Name = "DurableAgents_Console_04_AgentOrchestration_Conditionals",
            ProjectPath = "samples/DurableAgents/ConsoleApps/04_AgentOrchestration_Conditionals",
            RequiredEnvironmentVariables = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT_NAME"],
            ExpectedOutputDescription =
            [
                "The output should show conditional orchestration behavior based on agent output.",
                "The output should not contain error messages or stack traces.",
            ],
        },
        new SampleDefinition
        {
            Name = "DurableAgents_Console_05_AgentOrchestration_HITL",
            ProjectPath = "samples/DurableAgents/ConsoleApps/05_AgentOrchestration_HITL",
            RequiredEnvironmentVariables = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT_NAME"],
            Inputs = ["approve", "exit"],
            ExpectedOutputDescription =
            [
                "The output should show a human-in-the-loop durable agent orchestration.",
                "The output should not contain error messages or stack traces.",
            ],
        },
        new SampleDefinition
        {
            Name = "DurableAgents_Console_06_LongRunningTools",
            ProjectPath = "samples/DurableAgents/ConsoleApps/06_LongRunningTools",
            RequiredEnvironmentVariables = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT_NAME"],
            ExpectedOutputDescription =
            [
                "The output should show a durable orchestration started from an agent tool call.",
                "The output should not contain error messages or stack traces.",
            ],
        },
        new SampleDefinition
        {
            Name = "DurableAgents_Console_07_ReliableStreaming",
            ProjectPath = "samples/DurableAgents/ConsoleApps/07_ReliableStreaming",
            RequiredEnvironmentVariables = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT_NAME", "REDIS_CONNECTION_STRING"],
            ExpectedOutputDescription =
            [
                "The output should show reliable streaming for durable agent responses.",
                "The output should not contain error messages or stack traces.",
            ],
        },
    ];

    public static IReadOnlyList<SampleDefinition> AzureFunctions { get; } =
    [
        SkippedAzureFunctionsSample("DurableAgents_AzureFunctions_01_SingleAgent", "samples/DurableAgents/AzureFunctions/01_SingleAgent"),
        SkippedAzureFunctionsSample("DurableAgents_AzureFunctions_02_AgentOrchestration_Chaining", "samples/DurableAgents/AzureFunctions/02_AgentOrchestration_Chaining"),
        SkippedAzureFunctionsSample("DurableAgents_AzureFunctions_03_AgentOrchestration_Concurrency", "samples/DurableAgents/AzureFunctions/03_AgentOrchestration_Concurrency"),
        SkippedAzureFunctionsSample("DurableAgents_AzureFunctions_04_AgentOrchestration_Conditionals", "samples/DurableAgents/AzureFunctions/04_AgentOrchestration_Conditionals"),
        SkippedAzureFunctionsSample("DurableAgents_AzureFunctions_05_AgentOrchestration_HITL", "samples/DurableAgents/AzureFunctions/05_AgentOrchestration_HITL"),
        SkippedAzureFunctionsSample("DurableAgents_AzureFunctions_06_LongRunningTools", "samples/DurableAgents/AzureFunctions/06_LongRunningTools"),
        SkippedAzureFunctionsSample("DurableAgents_AzureFunctions_07_AgentAsMcpTool", "samples/DurableAgents/AzureFunctions/07_AgentAsMcpTool"),
        SkippedAzureFunctionsSample("DurableAgents_AzureFunctions_08_ReliableStreaming", "samples/DurableAgents/AzureFunctions/08_ReliableStreaming"),
    ];

    private static SampleDefinition SkippedAzureFunctionsSample(string name, string projectPath)
        => new()
        {
            Name = name,
            ProjectPath = projectPath,
            SkipReason = AzureFunctionsSkipReason,
        };
}
