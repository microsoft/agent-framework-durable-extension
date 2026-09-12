// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Nodes;
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
                foreach (string text in new[] { "", " \t\r\n ", "ordinary text", "null", "false", "0", "\"\"", "{}", WorkflowExecutionTestHelper.ControlEnvelope })
                {
                    cases.Add(boundary, text);
                }
            }

            return cases;
        }
    }

    public static TheoryData<string, int, bool> InvalidChildMessages
    {
        get
        {
            TheoryData<string, int, bool> cases = new();
            string[] messages =
            [
                "null",
                        "{}",
                        """{"data":"{}"}""",
                        """{"typeName":null,"data":"{}"}""",
                        """{"typeName":"","data":"{}"}""",
                        """{"typeName":" \t\r\n ","data":"{}"}""",
                        """{"typeName":"System.String"}""",
                        """{"typeName":"System.String","data":null}""",
                        """{"typeName":"System.String","data":""}""",
                        """{"typeName":"System.String","data":" \t\r\n "}""",
                    ];
            foreach (string message in messages)
            {
                // Invalid alone, first, middle and last: no valid subset may escape.
                foreach (int position in new[] { -1, 0, 1, 2 })
                {
                    cases.Add(message, position, false);
                    cases.Add(message, position, true);
                }
            }

            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(InvalidChildMessages))]
    public async Task InvalidChildCollectionRejectsAllMessagesAndControlsAfterColdReplayAsync(
        string invalidMessage, int position, bool halt)
    {
        JsonArray messages = position < 0
            ? []
            : new JsonArray(Message(typeof(string), "must-not-route-first"), Message(typeof(string), "must-not-route-last"));
        messages.Insert(Math.Max(position, 0), JsonNode.Parse(invalidMessage));
        string wire = ChildResult(messages, WorkflowExecutionTestHelper.ControlEnvelope, halt);

        await AssertOpaqueChildAndReplayAsync(wire, WorkflowExecutionTestHelper.ControlEnvelope);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" \t\r\n ")]
    [InlineData("ordinary text")]
    [InlineData("null")]
    [InlineData("{}")]
    public async Task InvalidChildCollectionPreservesExactResultAndEmptySemanticsAsync(string? text)
    {
        await AssertOpaqueChildAndReplayAsync(ChildResult(new JsonArray((JsonNode?)null), text, halt: true), text ?? "");
    }

    [Theory]
    [InlineData("missing", false)]
    [InlineData("missing", true)]
    [InlineData("null", false)]
    [InlineData("null", true)]
    [InlineData("empty", false)]
    [InlineData("empty", true)]
    public async Task LegacyChildResultOnlyRetainsStringProvenanceAndTrustedControlsAsync(string collection, bool halt)
    {
        JsonObject result = JsonNode.Parse(ChildResult([], WorkflowExecutionTestHelper.ControlEnvelope, halt))!.AsObject();
        if (collection == "missing")
        {
            result.Remove("sentMessages");
        }
        else if (collection == "null")
        {
            result["sentMessages"] = null;
        }

        await AssertOpaqueChildAndReplayAsync(result.ToJsonString(), WorkflowExecutionTestHelper.ControlEnvelope, preserveEvent: true, halt);
    }

    [Fact]
    public async Task NullChildResultRemainsEmptyAfterColdReplayAsync()
    {
        await AssertOpaqueChildAndReplayAsync("null", "");
    }

    [Fact]
    public async Task ValidChildCollectionRoutesEveryTypedValueInOrderAfterColdReplayAsync()
    {
        string[] jsonValues = ["null", "false", "0", "\"\"", "\" \"", """{"nested":[null,false,0,""]}"""];
        JsonArray messages =
        [
            Message(typeof(Dictionary<string, JsonElement>), "{}"),
                    Message(typeof(int), "0"),
                    Message(typeof(string), WorkflowExecutionTestHelper.ControlEnvelope),
                ];
        foreach (string value in jsonValues)
        {
            messages.Add(Message(typeof(JsonElement), value));
        }

        RoutingHarness original = new("activity", "seed", maxSupersteps: messages.Count + 1,
            childOutputWire: ChildResult(messages, "must-not-route-result", halt: false));
        DurableWorkflowResult result = await original.RunAsync();
        Assert.Equal([typeof(Dictionary<string, JsonElement>), typeof(int), typeof(string), .. jsonValues.Select(_ => typeof(JsonElement))],
            original.Successor.HandledTypes);
        Assert.Equal(1, original.Successor.ObjectCalls);
        Assert.Equal([0], original.Successor.IntegerInputs);
        Assert.Equal([WorkflowExecutionTestHelper.ControlEnvelope], original.Successor.StringInputs);
        Assert.Equal(jsonValues, original.Successor.JsonInputs);
        Assert.Equal(jsonValues[^1], result.Result);
        Assert.Contains("child-event", result.Events);
        Assert.DoesNotContain("event", result.Events);
        Assert.False(result.HaltRequested);
        Assert.Equal(messages.Count, original.SuccessorInputs.Count);
        for (int i = 0; i < messages.Count; i++)
        {
            Assert.Equal(messages[i]!["typeName"]!.GetValue<string>(), original.SuccessorInputs[i].InputTypeName);
            Assert.Equal(messages[i]!["data"]!.GetValue<string>(), original.SuccessorInputs[i].Input);
            Assert.Empty(original.SuccessorInputs[i].State);
        }

        RoutingHarness replay = new("activity", "seed", maxSupersteps: messages.Count + 1, replayCalls: ColdHistory(original));
        Assert.Equal(Serialize(result), Serialize(await replay.RunAsync()));
        Assert.Empty(replay.Successor.HandledTypes);
        Assert.Equal(0, replay.ExecutedActivities);
        replay.AssertHistoryConsumed();
    }

    [Theory]
    [InlineData("Future.Message")]
    [InlineData("Future.Message, Future.Assembly, Version=99.0.0.0, Culture=neutral, PublicKeyToken=null")]
    [InlineData(" Future.Message ")]
    public async Task UnknownChildTypeFailsAtTargetWithoutChoosingFirstHandlerAfterColdReplayAsync(string typeName)
    {
        JsonArray messages = [new JsonObject { ["typeName"] = typeName, ["data"] = "{}" }];
        RoutingHarness original = new("activity", "seed", childOutputWire: ChildResult(messages, "not a fallback", halt: false));
        TaskFailedException failure = await Assert.ThrowsAsync<TaskFailedException>(() => original.RunAsync());
        Assert.Equal(typeof(InvalidOperationException).FullName, failure.FailureDetails.ErrorType);
        Assert.Contains(typeName, failure.FailureDetails.ErrorMessage, StringComparison.Ordinal);
        Assert.Empty(original.Successor.HandledTypes);
        DurableActivityInput input = Assert.Single(original.SuccessorInputs);
        Assert.Equal(typeName, input.InputTypeName);
        Assert.Equal("{}", input.Input);

        RoutingHarness replay = new("activity", "seed", replayCalls: ColdHistory(original));
        TaskFailedException repeated = await Assert.ThrowsAsync<TaskFailedException>(() => replay.RunAsync());
        Assert.Equal(JsonSerializer.Serialize(failure.FailureDetails), JsonSerializer.Serialize(repeated.FailureDetails));
        Assert.Empty(replay.Successor.HandledTypes);
        Assert.Equal(0, replay.ExecutedActivities);
        replay.AssertHistoryConsumed();
    }

    [Theory]
    [InlineData("""{"sentMessages":[42]}""")]
    [InlineData("""{"sentMessages":[{"typeName":42,"data":"{}"}]}""")]
    [InlineData("""{"sentMessages":[{"typeName":"System.String","data":{}}]}""")]
    public async Task InvalidSdkJsonStillPropagatesSerializationFailureAsync(string wire)
    {
        RoutingHarness harness = new("activity", "seed", childOutputWire: wire);
        await Assert.ThrowsAsync<JsonException>(() => harness.RunAsync());
        Assert.Empty(harness.Successor.HandledTypes);
        Assert.Empty(harness.SuccessorInputs);
    }

    private static JsonObject Message(Type type, string data) => new() { ["typeName"] = type.AssemblyQualifiedName, ["data"] = data };

    private static string ChildResult(JsonArray messages, string? text, bool halt) => new JsonObject
    {
        ["result"] = text,
        ["sentMessages"] = messages,
        ["events"] = new JsonArray("child-event"),
        ["haltRequested"] = halt,
        ["stateUpdates"] = new JsonObject { ["scope:key"] = "must-not-escape" },
        ["clearedScopes"] = new JsonArray("scope"),
    }.ToJsonString();

    private static Dictionary<string, List<RecordedCall>> ColdHistory(RoutingHarness harness) =>
        JsonSerializer.Deserialize<Dictionary<string, List<RecordedCall>>>(JsonSerializer.Serialize(harness.Calls))!;

    private static async Task AssertOpaqueChildAndReplayAsync(string wire, string text, bool preserveEvent = false, bool halt = false)
    {
        RoutingHarness original = new("activity", "seed", childOutputWire: wire);
        DurableWorkflowResult result = await original.RunAsync();
        AssertResult(result, text, halt);
        bool routed = text.Length > 0 && !halt;
        Assert.Equal(routed ? new[] { text } : [], original.Successor.StringInputs);
        Assert.Equal(routed ? new[] { typeof(string) } : [], original.Successor.HandledTypes);
        Assert.Equal(preserveEvent, result.Events.Contains("child-event"));
        Assert.DoesNotContain("event", result.Events);
        Assert.All(original.SuccessorInputs, input =>
        {
            Assert.Equal(typeof(string).AssemblyQualifiedName, input.InputTypeName);
            Assert.Equal(text, input.Input);
            Assert.Empty(input.State);
        });
        Assert.Equal(routed ? 1 : 0, original.SuccessorInputs.Count);

        RoutingHarness replay = new("activity", "seed", replayCalls: ColdHistory(original));
        Assert.Equal(Serialize(result), Serialize(await replay.RunAsync()));
        Assert.Empty(replay.Successor.HandledTypes);
        Assert.Equal(0, replay.ExecutedActivities);
        replay.AssertHistoryConsumed();
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

    public sealed record RecordedCall(string Name, string Input, string? Output, TaskFailureDetails? Failure = null);

    private sealed class RoutingHarness
    {
        private readonly Workflow _parent;
        private readonly Workflow _child;
        private readonly string _text;
        private readonly string? _childOutputWire;
        private readonly DurableOptions _options = new();
        private readonly Dictionary<string, Queue<RecordedCall>>? _replay;

        public RoutingHarness(string boundary, string text, bool halt = false, int maxSupersteps = 2,
            Dictionary<string, List<RecordedCall>>? replayCalls = null, string? childOutputWire = null)
        {
            this._text = text;
            this._childOutputWire = childOutputWire;
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
            Assert.Equal([typeof(Dictionary<string, JsonElement>), typeof(int), typeof(string), typeof(JsonElement)], this.Successor.InputTypes);
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

        private RecordedCall ReplayCall(string workflowName, string name, string input)
        {
            RecordedCall recorded = this._replay![workflowName].Dequeue();
            Assert.Equal(recorded.Name, name);
            Assert.Equal(recorded.Input, input);
            if (recorded.Failure is not null)
            {
                throw new TaskFailedException(name, 0, recorded.Failure);
            }

            return recorded;
        }

        private void RecordCall(string workflowName, RecordedCall call)
        {
            if (!this.Calls.TryGetValue(workflowName, out List<RecordedCall>? calls))
            {
                this.Calls[workflowName] = calls = [];
            }

            calls.Add(call);
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
                        return this.ReplayCall(workflow.Name!, name.ToString(), wire).Output!;
                    }

                    this.ExecutedActivities++;
                    try
                    {
                        string output = await DurableActivityExecutor.ExecuteAsync(binding, wire);
                        this.RecordCall(workflow.Name!, new RecordedCall(name.ToString(), wire, output));
                        return output;
                    }
                    catch (InvalidOperationException exception)
                    {
                        TaskFailureDetails failure = TaskFailureDetails.FromException(exception);
                        this.RecordCall(workflow.Name!, new RecordedCall(name.ToString(), wire, null, failure));
                        throw new TaskFailedException(name.ToString(), 0, failure);
                    }
                });
            context.Setup(value => value.CallSubOrchestratorAsync<DurableWorkflowResult?>(
                It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()))
                .Returns(async (TaskName name, object? childInput, TaskOptions? _) =>
                {
                    Assert.Equal(WorkflowNamingHelper.ToOrchestrationFunctionName(this._child.Name!), name.ToString());
                    this.ChildRuns++;
                    DurableDataConverter converter = new();
                    string inputWire = converter.Serialize(childInput)!;
                    DurableWorkflowInput<object> restoredInput = Assert.IsType<DurableWorkflowInput<object>>(
                        converter.Deserialize(inputWire, typeof(DurableWorkflowInput<object>)));
                    DurableWorkflowResult childResult = await this.RunAsync(this._child, restoredInput);
                    string outputWire;
                    if (this._replay is not null)
                    {
                        outputWire = this.ReplayCall(workflow.Name!, name.ToString(), inputWire).Output!;
                    }
                    else
                    {
                        outputWire = this._childOutputWire ?? converter.Serialize(childResult);
                        this.RecordCall(workflow.Name!, new RecordedCall(name.ToString(), inputWire, outputWire));
                    }

                    return (DurableWorkflowResult?)converter.Deserialize(outputWire, typeof(DurableWorkflowResult));
                });
            DurableWorkflowResult result = await new DurableWorkflowRunner(this._options).RunWorkflowOrchestrationAsync(
                context.Object, input, NullLogger.Instance);
            string resultWire = Serialize(result);
            if (this._replay is not null)
            {
                Assert.Equal(this.ReplayCall(workflow.Name!, "$result", "").Output, resultWire);
            }
            else
            {
                this.RecordCall(workflow.Name!, new RecordedCall("$result", "", resultWire));
            }

            return JsonSerializer.Deserialize(resultWire, DurableWorkflowJsonContext.Default.DurableWorkflowResult)!;
        }
    }

    private sealed class MultiTypeSuccessor() : Executor("successor")
    {
        public List<string> StringInputs { get; } = [];

        public int ObjectCalls { get; private set; }

        public List<int> IntegerInputs { get; } = [];

        public List<string> JsonInputs { get; } = [];

        public List<Type> HandledTypes { get; } = [];

        protected override ProtocolBuilder ConfigureProtocol(ProtocolBuilder protocolBuilder) =>
            protocolBuilder.ConfigureRoutes(routes => routes
                .AddHandler<Dictionary<string, JsonElement>, string>((_, _) =>
                {
                    this.ObjectCalls++;
                    this.HandledTypes.Add(typeof(Dictionary<string, JsonElement>));
                    return "incorrectly decoded";
                })
                .AddHandler<int, string>((number, _) =>
                {
                    this.HandledTypes.Add(typeof(int));
                    this.IntegerInputs.Add(number);
                    return JsonSerializer.Serialize(number);
                })
                .AddHandler<string, string>((text, _) =>
                {
                    this.HandledTypes.Add(typeof(string));
                    this.StringInputs.Add(text);
                    return text;
                })
                .AddHandler<JsonElement, string>((json, _) =>
                {
                    this.HandledTypes.Add(typeof(JsonElement));
                    this.JsonInputs.Add(json.GetRawText());
                    return json.GetRawText();
                }));
    }
}
