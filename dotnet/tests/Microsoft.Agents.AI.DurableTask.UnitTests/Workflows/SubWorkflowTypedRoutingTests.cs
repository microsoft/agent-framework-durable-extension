// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests.Workflows;

public sealed class SubWorkflowTypedRoutingTests
{
    public static TheoryData<string, string> ChildOutputs
    {
        get
        {
            TheoryData<string, string> cases = new();
            foreach (string boundary in new[] { "activity", "agent", "request-port" })
            {
                foreach (string text in new[] { "", "ordinary text", "null", "false", "0", "\"\"", "{}", WorkflowExecutionTestHelper.ControlEnvelope })
                {
                    cases.Add(boundary, text);
                }
            }

            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(ChildOutputs))]
    public async Task ChildRunnerRoutesOpaqueStringToNonFirstHandlerAfterColdReplayAsync(string boundary, string text)
    {
        RoutingHarness original = new(boundary, text);
        DurableWorkflowResult first = await original.RunAsync();
        AssertResult(first, text, halt: false);
        Assert.Equal(text.Length == 0 ? [] : new[] { text }, original.Successor.StringInputs);
        Assert.Equal(0, original.Successor.ObjectCalls);
        Assert.Equal(1, original.ChildRuns);
        Assert.Equal(text.Length == 0 ? 0 : 1, original.SuccessorInputs.Count);
        if (text.Length > 0)
        {
            DurableActivityInput input = Assert.Single(original.SuccessorInputs);
            Assert.Equal(typeof(string).AssemblyQualifiedName, input.InputTypeName);
            Assert.Equal(text, input.Input);
            Assert.Empty(input.State);
        }

        // Reconstruct graphs, contexts and serialized history, then replay runner decisions.
        // This models SDK call-result replay, not an actual backend/worker restart.
        string history = JsonSerializer.Serialize(original.Calls);
        RoutingHarness replay = new(boundary, text,
            replayCalls: JsonSerializer.Deserialize<Dictionary<string, List<RecordedCall>>>(history)!);
        DurableWorkflowResult repeated = await replay.RunAsync();
        AssertResult(repeated, text, halt: false);
        Assert.Equal(Serialize(first), Serialize(repeated));
        Assert.Empty(replay.Successor.StringInputs);
        Assert.Equal(0, replay.Successor.ObjectCalls);
        Assert.Equal(0, replay.ExecutedActivities);
        Assert.Equal(1, replay.ChildRuns);
        Assert.Equal(original.SuccessorInputs.Count, replay.SuccessorInputs.Count);
        replay.AssertHistoryConsumed();
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    public async Task ActualChildControlsRetainHaltAndSuperstepLimitsAsync(bool halt, int maxSupersteps)
    {
        RoutingHarness harness = new("activity", "child result", halt: halt, maxSupersteps: maxSupersteps);
        if (!halt && maxSupersteps == 1)
        {
            await Assert.ThrowsAsync<MaxSuperstepsExceededException>(() => harness.RunAsync());
            Assert.Empty(harness.Successor.StringInputs);
        }
        else
        {
            DurableWorkflowResult result = await harness.RunAsync();
            AssertResult(result, "child result", halt);
            Assert.Equal(halt ? 0 : 1, harness.Successor.StringInputs.Count);
            if (halt)
            {
                Assert.Contains(result.Events, item => JsonSerializer.Deserialize(
                    item, DurableWorkflowJsonContext.Default.TypedPayload)!.TypeName == typeof(DurableHaltRequestedEvent).AssemblyQualifiedName);
            }
            else
            {
                Assert.Empty(Assert.Single(harness.SuccessorInputs).State);
            }
        }

        Assert.Equal(1, harness.ChildRuns);
        Assert.Equal(0, harness.Successor.ObjectCalls);
    }

    private static void AssertResult(DurableWorkflowResult result, string text, bool halt)
    {
        Assert.Equal(text, result.Result);
        Assert.Equal(halt, result.HaltRequested);
        if (text.Length == 0)
        {
            Assert.Empty(result.SentMessages);
        }
        else
        {
            TypedPayload message = Assert.Single(result.SentMessages);
            Assert.Equal(text, message.Data);
            Assert.Equal(typeof(string).AssemblyQualifiedName, message.TypeName);
        }
    }

    private static string Serialize(DurableWorkflowResult result) =>
        JsonSerializer.Serialize(result, DurableWorkflowJsonContext.Default.DurableWorkflowResult);

    public sealed record RecordedCall(string Name, string Input, string Output);

    private sealed class RoutingHarness
    {
        private readonly Workflow _parent;
        private readonly Workflow _child;
        private readonly string _text;
        private readonly DurableOptions _options = new();
        private readonly Dictionary<string, Queue<RecordedCall>>? _replay;

        public RoutingHarness(string boundary, string text, bool halt = false, int maxSupersteps = 2,
            Dictionary<string, List<RecordedCall>>? replayCalls = null)
        {
            this._text = text;
            this._replay = replayCalls?.ToDictionary(pair => pair.Key, pair => new Queue<RecordedCall>(pair.Value));
            ExecutorBinding childStart;
            if (boundary == "agent")
            {
                Mock<AIAgent> agent = new();
                agent.SetupGet(value => value.Name).Returns("child-agent");
                childStart = agent.Object;
            }
            else if (boundary == "request-port")
            {
                childStart = RequestPort.Create<string, string>("child-port");
            }
            else
            {
                childStart = new FunctionExecutor<string, string>("child-activity",
                    async (_, context, cancellationToken) =>
                    {
                        await context.QueueStateUpdateAsync("key", "child-only", "scope", cancellationToken);
                        if (halt)
                        {
                            await context.RequestHaltAsync();
                        }

                        return text;
                    }, outputTypes: [typeof(string)]);
            }

            this._child = new WorkflowBuilder(childStart).WithName("routing-child").Build();
            ExecutorBinding childHost = this._child.BindAsExecutor("child-host");
            this._parent = new WorkflowBuilder(childHost).WithName("routing-parent")
                .AddEdge(childHost, this.Successor).Build();
            Assert.Equal(typeof(Dictionary<string, JsonElement>), this.Successor.InputTypes.First());
            Assert.Contains(typeof(string), this.Successor.InputTypes);
            this._options.Workflows.AddWorkflow(this._parent);
            this._options.Workflows.AddWorkflow(this._child);
            this._options.Workflows.MaxSupersteps = maxSupersteps;
        }

        public MultiTypeSuccessor Successor { get; } = new();

        public Dictionary<string, List<RecordedCall>> Calls { get; } = [];

        public List<DurableActivityInput> SuccessorInputs { get; } = [];

        public int ChildRuns { get; private set; }

        public int ExecutedActivities { get; private set; }

        public Task<DurableWorkflowResult> RunAsync() =>
            this.RunAsync(this._parent, new DurableWorkflowInput<object> { Input = "seed" });

        public void AssertHistoryConsumed()
        {
            Assert.NotNull(this._replay);
            Assert.All(this._replay.Values, queue => Assert.Empty(queue));
        }

        private async Task<DurableWorkflowResult> RunAsync(Workflow workflow, DurableWorkflowInput<object> input)
        {
            Mock<TaskOrchestrationContext> context = WorkflowExecutionTestHelper.CreateAgentContext(this._text);
            context.SetupGet(value => value.Name).Returns(WorkflowNamingHelper.ToOrchestrationFunctionName(workflow.Name!));
            context.SetupGet(value => value.InstanceId).Returns(workflow.Name!);
            context.SetupGet(value => value.IsReplaying).Returns(this._replay is not null);
            context.Setup(value => value.WaitForExternalEvent<string>("child-port", It.IsAny<CancellationToken>()))
                .ReturnsAsync(this._text);
            Dictionary<string, ExecutorBinding> bindings = workflow.ReflectExecutors().Values.ToDictionary(
                binding => WorkflowNamingHelper.ToOrchestrationFunctionName(WorkflowNamingHelper.GetExecutorName(binding.Id)));
            context.Setup(value => value.CallActivityAsync<string>(
                It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()))
                .Returns(async (TaskName name, object? activityInput, TaskOptions? _) =>
                {
                    string wire = Assert.IsType<string>(activityInput);
                    ExecutorBinding binding = bindings[name.ToString()];
                    if (binding.Id == this.Successor.Id)
                    {
                        this.SuccessorInputs.Add(JsonSerializer.Deserialize(wire, DurableWorkflowJsonContext.Default.DurableActivityInput)!);
                    }

                    if (this._replay is not null)
                    {
                        RecordedCall recorded = this._replay[workflow.Name!].Dequeue();
                        Assert.Equal(recorded.Name, name.ToString());
                        Assert.Equal(recorded.Input, wire);
                        return recorded.Output;
                    }

                    this.ExecutedActivities++;
                    string output = await DurableActivityExecutor.ExecuteAsync(binding, wire);
                    if (!this.Calls.TryGetValue(workflow.Name!, out List<RecordedCall>? calls))
                    {
                        this.Calls[workflow.Name!] = calls = [];
                    }

                    calls.Add(new RecordedCall(name.ToString(), wire, output));
                    return output;
                });
            context.Setup(value => value.CallSubOrchestratorAsync<DurableWorkflowResult?>(
                It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()))
                .Returns(async (TaskName name, object? childInput, TaskOptions? _) =>
                {
                    Assert.Equal(WorkflowNamingHelper.ToOrchestrationFunctionName(this._child.Name!), name.ToString());
                    this.ChildRuns++;
                    DurableDataConverter converter = new();
                    DurableWorkflowInput<object> restoredInput = Assert.IsType<DurableWorkflowInput<object>>(
                        converter.Deserialize(converter.Serialize(childInput), typeof(DurableWorkflowInput<object>)));
                    DurableWorkflowResult childResult = await this.RunAsync(this._child, restoredInput);
                    return Assert.IsType<DurableWorkflowResult>(converter.Deserialize(
                        converter.Serialize(childResult), typeof(DurableWorkflowResult)));
                });
            DurableWorkflowResult result = await new DurableWorkflowRunner(this._options).RunWorkflowOrchestrationAsync(
                context.Object, input, NullLogger.Instance);
            return JsonSerializer.Deserialize(Serialize(result), DurableWorkflowJsonContext.Default.DurableWorkflowResult)!;
        }
    }

    private sealed class MultiTypeSuccessor() : Executor("successor")
    {
        public List<string> StringInputs { get; } = [];

        public int ObjectCalls { get; private set; }

        protected override ProtocolBuilder ConfigureProtocol(ProtocolBuilder protocolBuilder) =>
            protocolBuilder.ConfigureRoutes(routes => routes
                .AddHandler<Dictionary<string, JsonElement>, string>((_, _) =>
                {
                    this.ObjectCalls++;
                    return "incorrectly decoded";
                })
                .AddHandler<string, string>((text, _) =>
                {
                    this.StringInputs.Add(text);
                    return text;
                }));
    }
}
