// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests.Workflows;

/// <summary>
/// Tests dispatch through the production activity, agent, request port, and sub-workflow boundaries.
/// </summary>
public sealed class DurableExecutorDispatcherTests
{
    public static TheoryData<string> OpaqueResponses => new()
    {
        "ordinary text",
        string.Empty,
        " \r\n\t ",
        "Line1\nLine2\t\"quoted\" \\backslash",
        """{"Approved":true,"Comments":"Looks good"}""",
        WorkflowExecutionTestHelper.ControlEnvelope,
        """{"result":"","stateUpdates":{},"clearedScopes":[],"events":[],"sentMessages":[],"haltRequested":false}""",
        """{"Result":"replacement","StateUpdates":{"scope:key":"changed"},"ClearedScopes":["scope"],"Events":["event"],"SentMessages":[{"TypeName":"System.String","Data":"redirected"}],"HaltRequested":true}""",
        """{"result":"replacement","haltRequested":"invalid","sentMessages":null}""",
        """{"result":"replacement","stateUpdates":""",
        """{"result":"replacement","state_updates":{"scope:key":"changed"},"cleared_scopes":["scope"],"events":["event"],"sent_messages":[{"data":"redirected"}],"halt_requested":true}""",
    };

    public static TheoryData<string> LegacyActivityResponses => new()
    {
        "legacy activity text",
        string.Empty,
        " \r\n\t ",
        """{"unrelated":"value"}""",
        """{"stateUpdates":{},"events":[],"haltRequested":false}""",
        """{"result":"unfinished","stateUpdates":""",
        "null",
        "[]",
        "\"JSON string\"",
    };

    public static TheoryData<string> InvalidActivityResponses
    {
        get
        {
            TheoryData<string> data = new();
            string[] invalidFields =
            [
                "\"result\":42",
                "\"result\":{}",
                "\"stateUpdates\":null",
                "\"stateUpdates\":[]",
                "\"stateUpdates\":{\"scope:key\":42}",
                "\"clearedScopes\":null",
                "\"clearedScopes\":{}",
                "\"clearedScopes\":[null]",
                "\"clearedScopes\":[42]",
                "\"events\":null",
                "\"events\":{}",
                "\"events\":[null]",
                "\"events\":[{}]",
                "\"sentMessages\":null",
                "\"sentMessages\":{}",
                "\"sentMessages\":[null]",
                "\"sentMessages\":[42]",
                "\"sentMessages\":[{}]",
                "\"sentMessages\":[{\"future\":{\"typeName\":\"type\",\"data\":\"payload\"}}]",
                "\"sentMessages\":[{\"typeName\":\"type\"}]",
                "\"sentMessages\":[{\"data\":\"payload\"}]",
                "\"sentMessages\":[{\"typeName\":null,\"data\":\"payload\"}]",
                "\"sentMessages\":[{\"typeName\":\"\",\"data\":\"payload\"}]",
                "\"sentMessages\":[{\"typeName\":\" \\t \",\"data\":\"payload\"}]",
                "\"sentMessages\":[{\"typeName\":\"type\",\"data\":null}]",
                "\"sentMessages\":[{\"typeName\":\"type\",\"data\":\"\"}]",
                "\"sentMessages\":[{\"typeName\":\"type\",\"data\":\" \\t \"}]",
                "\"sentMessages\":[{\"typeName\":{},\"data\":\"payload\"}]",
                "\"sentMessages\":[{\"typeName\":[],\"data\":\"payload\"}]",
                "\"sentMessages\":[{\"typeName\":false,\"data\":\"payload\"}]",
                "\"sentMessages\":[{\"typeName\":\"type\",\"data\":false}]",
                "\"sentMessages\":[{\"typeName\":\"type\",\"data\":0}]",
                "\"sentMessages\":[{\"typeName\":\"type\",\"data\":[]}]",
                "\"sentMessages\":[{\"typeName\":\"System.String\",\"data\":\"valid\"},{}]",
                "\"sentMessages\":[{\"typeName\":\"type\",\"data\":\"first\",\"\\u0064ata\":\"second\"}]",
                "\"sentMessages\":[{\"typeName\":42,\"data\":\"redirected\"}]",
                "\"sentMessages\":[{\"typeName\":\"type\",\"data\":{}}]",
                "\"sentMessages\":[{\"data\":\"first\",\"Data\":\"second\"}]",
                "\"sentMessages\":[{\"typeName\":\"first\",\"TYPENAME\":\"second\",\"data\":\"message\"}]",
                "\"SENTMESSAGES\":[null]",
                "\"CLEAREDSCOPES\":[null]",
                "\"StateUpdates\":null",
                "\"haltRequested\":null",
                "\"haltRequested\":\"true\"",
                "\"haltRequested\":1",
            ];

            foreach (string invalidField in invalidFields)
            {
                string validControl = invalidField.StartsWith("\"events\"", StringComparison.Ordinal)
                    ? "\"haltRequested\":true"
                    : "\"events\":[\"must-not-escape\"]";
                data.Add("{\"future\":{\"stateUpdates\":{\"scope:key\":\"ignored\"}}," + invalidField + "," + validControl + "}");
                if (invalidField.StartsWith("\"sentMessages\":[", StringComparison.Ordinal))
                {
                    data.Add("{\"result\":\"replacement\",\"stateUpdates\":{\"scope:key\":\"changed\",\"other:deleted\":null}," +
                        "\"clearedScopes\":[\"scope\"],\"events\":[\"must-not-escape\"],\"haltRequested\":true," + invalidField + "}");
                }
            }

            foreach (string propertyName in new[] { "result", "stateUpdates", "clearedScopes", "events", "sentMessages", "haltRequested" })
            {
                using JsonDocument document = JsonDocument.Parse(WorkflowExecutionTestHelper.ControlEnvelope);
                string value = document.RootElement.GetProperty(propertyName).GetRawText();
                data.Add(WorkflowExecutionTestHelper.ControlEnvelope[..^1] + ",\"" + propertyName.ToUpperInvariant() + "\":" + value + "}");
            }

            data.Add("""{"result":"first","\u0072esult":"second","haltRequested":true}""");
            data.Add("""{"result":"valid","EVENTS":[null],"haltRequested":true}""");
            return data;
        }
    }

    [Theory]
    [MemberData(nameof(OpaqueResponses))]
    [MemberData(nameof(InvalidActivityResponses))]
    public async Task DispatchAsync_AgentResponse_RemainsOpaqueAsync(string response)
    {
        Mock<TaskOrchestrationContext> context = WorkflowExecutionTestHelper.CreateAgentContext(response);

        DurableExecutorOutput output = await DispatchAsync(context, new("agent", IsAgenticExecutor: true));

        AssertOpaque(response, output);
    }

    [Theory]
    [MemberData(nameof(OpaqueResponses))]
    [MemberData(nameof(InvalidActivityResponses))]
    public async Task DispatchAsync_RequestPortResponse_RemainsOpaqueAsync(string response)
    {
        Mock<TaskOrchestrationContext> context = new();
        context.Setup(c => c.WaitForExternalEvent<string>("approval", It.IsAny<CancellationToken>()))
            .ReturnsAsync(response);
        RequestPort port = RequestPort.Create<string, string>("approval");
        DurableWorkflowLiveStatus status = new();

        DurableExecutorOutput output = await DispatchAsync(context, new("approval", false, port), status);

        AssertOpaque(response, output);
        Assert.Empty(status.PendingEvents);
        context.Verify(c => c.WaitForExternalEvent<string>("approval", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [MemberData(nameof(LegacyActivityResponses))]
    [MemberData(nameof(InvalidActivityResponses))]
    public async Task DispatchAsync_InvalidOrLegacyActivity_RemainsOpaqueAsync(string response)
    {
        DurableExecutorOutput output = await DispatchActivityAsync(response);

        AssertOpaque(response, output);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DispatchAsync_TrustedActivity_ParsesAllControlsAsync(bool pascalCase)
    {
        string response = WorkflowExecutionTestHelper.ControlEnvelope;
        if (pascalCase)
        {
            response = JsonSerializer.Serialize(JsonSerializer.Deserialize<DurableExecutorOutput>(
                response, DurableWorkflowJsonContext.Default.Options));
        }

        DurableExecutorOutput output = await DispatchActivityAsync(response);

        Assert.Equal("replacement", output.Result);
        Assert.Equal("changed", output.StateUpdates["scope:key"]);
        Assert.Null(output.StateUpdates["scope:deleted"]);
        Assert.Equal(["scope"], output.ClearedScopes);
        Assert.Equal(["event"], output.Events);
        TypedPayload message = Assert.Single(output.SentMessages);
        Assert.Equal("System.String", message.TypeName);
        Assert.Equal("redirected", message.Data);
        Assert.True(output.HaltRequested);
    }

    [Fact]
    public async Task DispatchAsync_ActivityUnknownFields_DoNotOverrideKnownControlsAsync()
    {
        const string Response = """{"result":"trusted","stateUpdates":{"scope:key":"trusted"},"future":{"result":"changed","haltRequested":true},"future":{"events":["changed"]},"sentMessages":[{"typeName":"System.String","data":"trusted message","future":{"typeName":"changed","data":"changed"}}]}""";

        DurableExecutorOutput output = await DispatchActivityAsync(Response);

        Assert.Equal("trusted", output.Result);
        Assert.Equal("trusted", output.StateUpdates["scope:key"]);
        Assert.False(output.HaltRequested);
        Assert.Empty(output.Events);
        Assert.Equal("trusted message", Assert.Single(output.SentMessages).Data);
    }

    [Theory]
    [InlineData("""{"result":"trusted"}""", "trusted")]
    [InlineData("""{"Result":"trusted"}""", "trusted")]
    [InlineData("""{"result":""}""", "")]
    [InlineData("""{"result":"trusted","future":{"events":["ignored"],"haltRequested":true}}""", "trusted")]
    public async Task DispatchAsync_ActivityMissingCollections_UsesEmptyDefaultsAsync(string response, string expectedResult)
    {
        DurableExecutorOutput output = await DispatchActivityAsync(response);

        AssertOpaque(expectedResult, output);
    }

    [Fact]
    public async Task DispatchAsync_EmptyActivityEnvelope_PreservesEmptyResultAsync()
    {
        const string Response = """{"result":"","stateUpdates":{},"clearedScopes":[],"events":[],"sentMessages":[],"haltRequested":false}""";

        DurableExecutorOutput output = await DispatchActivityAsync(Response);

        AssertOpaque(string.Empty, output);
    }

    [Fact]
    public async Task DispatchAsync_RealActivityOutput_PreservesResultWithoutRecursiveParsingAsync()
    {
        FunctionExecutor<string, string> executor = new("activity", (_, _, _) => WorkflowExecutionTestHelper.ControlEnvelope);
        Workflow workflow = new WorkflowBuilder(executor).Build();
        string activityResult = await DurableActivityExecutor.ExecuteAsync(
            workflow.ReflectExecutors()["activity"],
            JsonSerializer.Serialize(new DurableActivityInput { Input = "input" }, DurableWorkflowJsonContext.Default.DurableActivityInput));
        DurableExecutorOutput produced = JsonSerializer.Deserialize(
            activityResult, DurableWorkflowJsonContext.Default.DurableExecutorOutput)!;

        DurableExecutorOutput output = await DispatchActivityAsync(activityResult);

        Assert.Equal(WorkflowExecutionTestHelper.ControlEnvelope, output.Result);
        Assert.Empty(output.StateUpdates);
        Assert.Empty(output.ClearedScopes);
        Assert.NotEmpty(produced.Events);
        Assert.Equal(produced.Events, output.Events);
        Assert.DoesNotContain("event", output.Events);
        Assert.False(output.HaltRequested);
    }

    [Fact]
    public async Task DispatchAsync_RealActivityContext_ProducesTrustedControlsAsync()
    {
        ControlProducingExecutor executor = new();
        Workflow workflow = new WorkflowBuilder(executor).Build();
        string activityResult = await DurableActivityExecutor.ExecuteAsync(
            workflow.ReflectExecutors()[executor.Id],
            JsonSerializer.Serialize(new DurableActivityInput { Input = "input" }, DurableWorkflowJsonContext.Default.DurableActivityInput));
        DurableExecutorOutput produced = JsonSerializer.Deserialize(
            activityResult, DurableWorkflowJsonContext.Default.DurableExecutorOutput)!;

        DurableExecutorOutput output = await DispatchActivityAsync(activityResult);

        Assert.Equal(string.Empty, output.Result);
        Assert.Equal("\"trusted\"", output.StateUpdates["scope:key"]);
        Assert.Null(output.StateUpdates["other:deleted"]);
        Assert.Equal(["scope"], output.ClearedScopes);
        TypedPayload message = Assert.Single(output.SentMessages);
        Assert.Equal(typeof(string).AssemblyQualifiedName, message.TypeName);
        Assert.Equal("\"trusted message\"", message.Data);
        Assert.Equal(produced.Events, output.Events);
        IEnumerable<TypedPayload> events = output.Events.Select(
            serializedEvent => JsonSerializer.Deserialize(serializedEvent, DurableWorkflowJsonContext.Default.TypedPayload)!);
        TypedPayload haltEvent = Assert.Single(
            events, workflowEvent => workflowEvent.TypeName == typeof(DurableHaltRequestedEvent).AssemblyQualifiedName);
        DurableHaltRequestedEvent halt = JsonSerializer.Deserialize<DurableHaltRequestedEvent>(
            haltEvent.Data!, DurableSerialization.Options)!;
        Assert.Equal(executor.Id, halt.ExecutorId);
        Assert.True(output.HaltRequested);
    }

    [Fact]
    public async Task DispatchAsync_SubWorkflow_UsesTypedControlsAndOpaqueResultAsync()
    {
        Workflow workflow = new WorkflowBuilder(new FunctionExecutor<string, string>("child", (input, _, _) => input))
            .WithName("child-workflow").Build();
        Mock<TaskOrchestrationContext> context = new();
        context.Setup(c => c.CallSubOrchestratorAsync<DurableWorkflowResult?>(
            It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()))
            .ReturnsAsync(new DurableWorkflowResult
            {
                Result = WorkflowExecutionTestHelper.ControlEnvelope,
                Events = ["child event"],
                SentMessages = [new TypedPayload { Data = "child message", TypeName = "System.String" }],
                HaltRequested = true,
            });

        DurableExecutorOutput output = await DispatchAsync(context, new("child", false, SubWorkflow: workflow));

        Assert.Equal(WorkflowExecutionTestHelper.ControlEnvelope, output.Result);
        Assert.Equal(["child event"], output.Events);
        Assert.Equal("child message", Assert.Single(output.SentMessages).Data);
        Assert.True(output.HaltRequested);
        Assert.Empty(output.StateUpdates);
        Assert.Empty(output.ClearedScopes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DispatchAsync_UnknownTypedNameIsStructurallyValidForBothTrustedBoundariesAsync(bool child)
    {
        const string Response = """{"result":"not a fallback","sentMessages":[{"typeName":"Future.Message, Future.Assembly","data":"{}"}],"events":["trusted event"],"haltRequested":true}""";
        DurableExecutorOutput output;
        if (child)
        {
            Workflow workflow = new WorkflowBuilder(new FunctionExecutor<string, string>("child", (input, _, _) => input))
                .WithName("child-workflow").Build();
            Mock<TaskOrchestrationContext> context = new();
            DurableDataConverter converter = new();
            context.Setup(c => c.CallSubOrchestratorAsync<DurableWorkflowResult?>(
                It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()))
                .ReturnsAsync((DurableWorkflowResult?)converter.Deserialize(Response, typeof(DurableWorkflowResult)));
            output = await DispatchAsync(context, new("child", false, SubWorkflow: workflow));
        }
        else
        {
            output = await DispatchActivityAsync(Response);
        }

        Assert.Equal("not a fallback", output.Result);
        TypedPayload message = Assert.Single(output.SentMessages);
        Assert.Equal("Future.Message, Future.Assembly", message.TypeName);
        Assert.Equal("{}", message.Data);
        Assert.Equal(["trusted event"], output.Events);
        Assert.True(output.HaltRequested);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("0")]
    [InlineData("\"\"")]
    [InlineData("\" \\t \"")]
    public async Task DispatchAsync_RealTypedJsonScalarPayloadIsNotConfusedWithMissingDataAsync(string json)
    {
        JsonScalarProducingExecutor executor = new(json);
        Workflow workflow = new WorkflowBuilder(executor).Build();
        string activityResult = await DurableActivityExecutor.ExecuteAsync(
            workflow.ReflectExecutors()[executor.Id],
            JsonSerializer.Serialize(new DurableActivityInput { Input = "input" }, DurableWorkflowJsonContext.Default.DurableActivityInput));
        using JsonDocument wire = JsonDocument.Parse(activityResult);
        JsonElement message = wire.RootElement.GetProperty("sentMessages")[0];
        Assert.Equal(JsonValueKind.String, message.GetProperty("typeName").ValueKind);
        Assert.Equal(JsonValueKind.String, message.GetProperty("data").ValueKind);
        Assert.Equal(json, message.GetProperty("data").GetString());

        DurableExecutorOutput output = await DispatchActivityAsync(activityResult);

        TypedPayload payload = Assert.Single(output.SentMessages);
        Assert.Equal(typeof(JsonElement).AssemblyQualifiedName, payload.TypeName);
        Assert.Equal(json, payload.Data);
        Assert.Equal("\"trusted\"", output.StateUpdates["scope:key"]);
    }

    [Fact]
    public async Task DispatchAsync_NullSubWorkflowResult_ReturnsEmptyResultAsync()
    {
        Workflow workflow = new WorkflowBuilder(new FunctionExecutor<string, string>("child", (input, _, _) => input))
            .WithName("child-workflow").Build();
        Mock<TaskOrchestrationContext> context = new();
        context.Setup(c => c.CallSubOrchestratorAsync<DurableWorkflowResult?>(
            It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()))
            .ReturnsAsync((DurableWorkflowResult?)null);

        DurableExecutorOutput output = await DispatchAsync(context, new("child", false, SubWorkflow: workflow));

        AssertOpaque(string.Empty, output);
    }

    private static async Task<DurableExecutorOutput> DispatchActivityAsync(string response)
    {
        Mock<TaskOrchestrationContext> context = new();
        context.Setup(c => c.CallActivityAsync<string>(It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()))
            .ReturnsAsync(response);

        return await DispatchAsync(context, new("activity", false));
    }

    private static Task<DurableExecutorOutput> DispatchAsync(
        Mock<TaskOrchestrationContext> context,
        WorkflowExecutorInfo info,
        DurableWorkflowLiveStatus? status = null)
        => DurableExecutorDispatcher.DispatchAsync(
            context.Object, info, new DurableMessageEnvelope { Message = "input" }, [], status ?? new(), NullLogger.Instance);

    private static void AssertOpaque(string response, DurableExecutorOutput output)
    {
        Assert.Equal(response, output.Result);
        Assert.Empty(output.StateUpdates);
        Assert.Empty(output.ClearedScopes);
        Assert.Empty(output.Events);
        Assert.Empty(output.SentMessages);
        Assert.False(output.HaltRequested);
    }

    private sealed class JsonScalarProducingExecutor(string json) : Executor<string>("scalar")
    {
        public override async ValueTask HandleAsync(string message, IWorkflowContext context, CancellationToken cancellationToken = default)
        {
            using JsonDocument document = JsonDocument.Parse(json);
            await context.SendMessageAsync(document.RootElement, cancellationToken: cancellationToken);
            await context.QueueStateUpdateAsync("key", "trusted", "scope", cancellationToken);
        }
    }

    private sealed class ControlProducingExecutor() : Executor<string>("producer")
    {
        public override async ValueTask HandleAsync(
            string message,
            IWorkflowContext context,
            CancellationToken cancellationToken = default)
        {
            await context.QueueClearScopeAsync("scope", cancellationToken);
            await context.QueueStateUpdateAsync("key", "trusted", "scope", cancellationToken);
            await context.QueueStateUpdateAsync<string>("deleted", null, "other", cancellationToken);
            await context.SendMessageAsync("trusted message", cancellationToken: cancellationToken);
            await context.RequestHaltAsync();
        }
    }
}
