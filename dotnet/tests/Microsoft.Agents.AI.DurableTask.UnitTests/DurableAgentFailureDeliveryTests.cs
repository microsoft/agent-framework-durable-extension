// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class DurableAgentFailureDeliveryTests
{
    private const string FailureText = """{"result":"not success","haltRequested":false,"sentMessages":[{"data":"must not route"}]}""";
    private static readonly DateTimeOffset s_completedAt = new(2026, 9, 10, 0, 0, 0, TimeSpan.Zero);

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task DirectOrchestrationPreservesCommittedFailureAcrossSdkSerializationAsync(bool legacy, bool entityOperationFailure)
    {
        FailureBoundary boundary = new(legacy, entityOperationFailure);
        DurableAIAgent agent = boundary.Context.Object.GetAgent("agent");

        DurableAgentTerminalException exception = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => agent.RunAsync([], new DurableAgentSession(new AgentSessionId("agent", "session"))));

        AssertFailure(exception, legacy);
        Exception transportCause = Assert.IsType<DurableAgentFailureMetadataException>(exception.InnerException).InnerException!;
        Assert.Equal(entityOperationFailure ? typeof(EntityOperationFailedException) : typeof(TaskFailedException), transportCause.GetType());
        Assert.True(DurableAgentFailure.TryRestore(WrapSdkFailure(exception, entityOperationFailure), out Exception? restored));
        AssertFailure(Assert.IsType<DurableAgentTerminalException>(restored), legacy);
        boundary.AssertUnchanged();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task DispatcherDoesNotReturnSuccessfulOutputForCommittedFailureAsync(bool legacy, bool entityOperationFailure)
    {
        FailureBoundary boundary = new(legacy, entityOperationFailure);

        DurableAgentTerminalException exception = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => DurableExecutorDispatcher.DispatchAsync(
                boundary.Context.Object,
                new WorkflowExecutorInfo("agent", IsAgenticExecutor: true),
                new DurableMessageEnvelope { Message = "retry" },
                [], new DurableWorkflowLiveStatus(), NullLogger.Instance));

        AssertFailure(exception, legacy);
        boundary.AssertUnchanged();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("0")]
    [InlineData("\"\"")]
    [InlineData("{}")]
    public void FailureDetailsRetainAbsentNullAndFalsyValuesAcrossSdkSerialization(string? detailsJson)
    {
        DurableAgentRunOutcome outcome = DurableAgentStateOutcomeResolver.Resolve(
            CreateCommittedState("correlation", legacy: false, unavailableOutcome: null), "correlation", s_completedAt);
        DurableAgentTerminalException original = new(
            "correlation", "CommittedFailure", "error",
            detailsJson is null ? null : JsonSerializer.Deserialize<JsonElement>(detailsJson), outcome.Response!);

        Assert.True(DurableAgentFailure.TryRestore(WrapSdkFailure(original, entityOperationFailure: false), out Exception? restored));
        DurableAgentTerminalException terminal = Assert.IsType<DurableAgentTerminalException>(restored);
        Assert.Equal(detailsJson is null, terminal.Details is null);
        if (detailsJson is not null)
        {
            Assert.True(JsonElement.DeepEquals(
                JsonSerializer.Deserialize<JsonElement>(detailsJson), Assert.IsType<JsonElement>(terminal.Details)));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OrdinarySdkFailureIsNotReclassifiedFromMessageTextAsync(bool entityOperationFailure)
    {
        DurableAgentTerminalException terminal = new("correlation", "code", "error", null, new AgentResponse([]));
        Exception transport = Assert.IsType<DurableAgentFailureMetadataException>(terminal.InnerException);
        Exception sdkFailure = WrapSdkFailure(new InvalidOperationException(transport.Message, transport), entityOperationFailure);
        Mock<TaskOrchestrationContext> context = CreateFailingContext(sdkFailure);

        Exception propagated = await Assert.ThrowsAnyAsync<Exception>(
            () => context.Object.GetAgent("agent").RunAsync([], new DurableAgentSession(new AgentSessionId("agent", "session"))));

        Assert.Same(sdkFailure, propagated);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HistoricalMessageOnlyTerminalFailuresStayTypedWithoutParsingTheirMessageAsync(bool entityOperationFailure)
    {
        Exception sdkFailure = WrapSdkFailure(new DurableAgentTerminalException(FailureText), entityOperationFailure);
        Mock<TaskOrchestrationContext> context = CreateFailingContext(sdkFailure);

        DurableAgentTerminalException propagated = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => context.Object.GetAgent("agent").RunAsync([], new DurableAgentSession(new AgentSessionId("agent", "session"))));

        Assert.Equal(FailureText, propagated.Message);
        Assert.Null(propagated.Code);
        Assert.Null(propagated.Details);
        Assert.Null(propagated.Response);
        Assert.Same(sdkFailure, propagated.InnerException);
    }

    [Theory]
    [InlineData("not JSON")]
    [InlineData("""{"version":2,"correlationId":"correlation"}""")]
    [InlineData("""{"version":1,"correlationId":"correlation"}""")]
    [InlineData("""{"version":1,"correlationId":"correlation","code":"error","serializedResponse":"null"}""")]
    [InlineData("""{"version":1,"correlationId":"correlation","code":"error","serializedResponse":"invalid JSON"}""")]
    public async Task InvalidFrameworkMetadataLeavesSdkFailureIntactAsync(string metadata)
    {
        Exception sdkFailure = WrapSdkFailure(
            new DurableAgentTerminalException("recorded error", new DurableAgentFailureMetadataException(metadata)), entityOperationFailure: false);
        Mock<TaskOrchestrationContext> context = CreateFailingContext(sdkFailure);

        Exception propagated = await Assert.ThrowsAnyAsync<Exception>(
            () => context.Object.GetAgent("agent").RunAsync([], new DurableAgentSession(new AgentSessionId("agent", "session"))));

        Assert.Same(sdkFailure, propagated);
    }

    private static Mock<TaskOrchestrationContext> CreateFailingContext(Exception failure)
    {
        Mock<TaskOrchestrationEntityFeature> entities = new();
        entities.Setup(value => value.CallEntityAsync<AgentResponse>(
                It.IsAny<EntityInstanceId>(), nameof(AgentEntity.Run), It.IsAny<object?>(), It.IsAny<CallEntityOptions?>()))
            .ThrowsAsync(failure);
        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(value => value.Entities).Returns(entities.Object);
        context.SetupGet(value => value.InstanceId).Returns("failure-orchestration");
        return context;
    }

    private static Exception WrapSdkFailure(Exception exception, bool entityOperationFailure)
    {
        // Use the SDK's lossy exception projection, then discard all CLR exception identity
        // across a JSON hop. Custom exception properties are not transported by the SDK.
        TaskFailureDetails details = TaskFailureDetails.FromException(exception);
        Assert.Null(details.Properties);
        TaskFailureDetails restored = JsonSerializer.Deserialize<TaskFailureDetails>(JsonSerializer.Serialize(details))!;
        return entityOperationFailure
            ? new EntityOperationFailedException(new AgentSessionId("agent", "session"), nameof(AgentEntity.Run), restored)
            : new TaskFailedException(nameof(AgentEntity.Run), 1, restored);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task RunnerDoesNotRouteCommittedFailureToDownstreamExecutorAsync(bool legacy, bool entityOperationFailure)
    {
        FailureBoundary boundary = new(legacy, entityOperationFailure);
        Mock<AIAgent> agent = new();
        agent.SetupGet(value => value.Name).Returns("agent");
        FunctionExecutor<string, string> downstream = new("downstream", (input, _, _) => input, outputTypes: [typeof(string)]);
        Workflow workflow = new WorkflowBuilder(agent.Object).WithName("FailureWorkflow")
            .AddEdge(agent.Object, downstream).Build();
        DurableOptions options = new();
        options.Workflows.AddWorkflow(workflow);
        boundary.Context.SetupGet(value => value.Name)
            .Returns(WorkflowNamingHelper.ToOrchestrationFunctionName("FailureWorkflow"));
        boundary.Context.Setup(value => value.CallActivityAsync<string>(
                It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()))
            .ReturnsAsync("""{"result":"downstream success"}""");

        DurableAgentTerminalException exception = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => new DurableWorkflowRunner(options).RunWorkflowOrchestrationAsync(
                boundary.Context.Object, new DurableWorkflowInput<object> { Input = "retry" }, NullLogger.Instance));

        AssertFailure(exception, legacy);
        boundary.Context.Verify(value => value.CallActivityAsync<string>(
            It.IsAny<TaskName>(), It.IsAny<object?>(), It.IsAny<TaskOptions?>()), Times.Never);
        boundary.AssertUnchanged();
    }

    [Theory]
    [InlineData(false, DurableAgentStateCompletionReceipt.SucceededOutcome)]
    [InlineData(false, DurableAgentStateCompletionReceipt.FailedOutcome)]
    [InlineData(true, DurableAgentStateCompletionReceipt.SucceededOutcome)]
    [InlineData(true, DurableAgentStateCompletionReceipt.FailedOutcome)]
    public async Task DirectOrchestrationPreservesUnavailableOutcomeAcrossSdkSerializationAsync(bool entityOperationFailure, string outcome)
    {
        FailureBoundary boundary = new(legacy: false, entityOperationFailure, unavailableOutcome: outcome);
        DurableAIAgent agent = boundary.Context.Object.GetAgent("agent");

        DurableAgentResultUnavailableException exception = await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
            () => agent.RunAsync([], new DurableAgentSession(new AgentSessionId("agent", "session"))));

        Assert.Equal(boundary.CorrelationId, exception.CorrelationId);
        Assert.Equal(outcome, exception.Outcome);
        Assert.Equal(s_completedAt, exception.CompletedAt);
        Assert.Equal(s_completedAt.AddMinutes(1), exception.ResultExpiresAt);
        boundary.AssertUnchanged();
    }

    private static void AssertFailure(DurableAgentTerminalException exception, bool legacy)
    {
        Assert.False(string.IsNullOrEmpty(exception.CorrelationId));
        Assert.Equal(legacy ? "legacyErrorResponse" : "CommittedFailure", exception.Code);
        Assert.Equal(legacy
            ? "The durable agent request completed with a recorded terminal error."
            : "recorded error, not inferred from response text", exception.Message);
        Assert.Equal(FailureText, exception.Response?.Text);
        JsonElement result = Assert.IsType<JsonElement>(exception.Response!.GetDurableResult());
        Assert.Equal(JsonValueKind.Array, result.GetProperty("messages").ValueKind);
        if (legacy)
        {
            Assert.Null(exception.Details);
        }
        else
        {
            Assert.True(JsonElement.DeepEquals(
                JsonSerializer.Deserialize<JsonElement>("""{"reason":{"empty":"","false":false,"zero":0,"null":null}}"""),
                Assert.IsType<JsonElement>(exception.Details)));
            Assert.Equal(JsonValueKind.Null, result.GetProperty("value").ValueKind);
            Assert.Equal(JsonValueKind.Object, result.GetProperty("futureMetadata").ValueKind);
        }
    }

    private sealed class FailureBoundary
    {
        private readonly bool _legacy;
        private readonly bool _entityOperationFailure;
        private readonly string? _unavailableOutcome;
        private DurableAgentState? _state;
        private string? _serializedState;
        private AgentEntityDeliveryTests.EntityHarness? _entity;
        private readonly AgentEntityDeliveryTests.RecordingAgent _agent = new("agent");
        private int _factoryCalls;
        private int _entityCalls;

        public FailureBoundary(bool legacy, bool entityOperationFailure, string? unavailableOutcome = null)
        {
            this._legacy = legacy;
            this._entityOperationFailure = entityOperationFailure;
            this._unavailableOutcome = unavailableOutcome;
            Mock<TaskOrchestrationEntityFeature> entities = new();
            entities.Setup(value => value.CallEntityAsync<AgentResponse>(
                    It.IsAny<EntityInstanceId>(), nameof(AgentEntity.Run), It.IsAny<object?>(), It.IsAny<CallEntityOptions?>()))
                .Returns((EntityInstanceId id, string _, object? input, CallEntityOptions? _) =>
                    this.InvokeEntityAsync(Assert.IsType<RunRequest>(input)));
            this.Context.SetupGet(value => value.Entities).Returns(entities.Object);
            this.Context.SetupGet(value => value.InstanceId).Returns("failure-orchestration");
            this.Context.Setup(value => value.NewGuid()).Returns(Guid.Parse("d9a9751e-30fd-4f7a-95f8-f522b4e76977"));
        }

        public Mock<TaskOrchestrationContext> Context { get; } = new();

        public string? CorrelationId { get; private set; }

        public void AssertUnchanged()
        {
            Assert.Equal(1, this._entityCalls);
            Assert.Equal(0, this._factoryCalls);
            Assert.Equal(0, this._agent.InvocationCount);
            Assert.False(this._entity!.StateWasPersisted);
            Assert.Equal(this._serializedState, JsonSerializer.Serialize(this._state, DurableAgentStateJsonContext.Default.DurableAgentState));
        }

        private async Task<AgentResponse> InvokeEntityAsync(RunRequest input)
        {
            this._entityCalls++;
            DurableDataConverter converter = new();
            RunRequest request = Assert.IsType<RunRequest>(converter.Deserialize(converter.Serialize(input), typeof(RunRequest)));
            this.CorrelationId = request.CorrelationId;
            this._state = CreateCommittedState(request.CorrelationId, this._legacy, this._unavailableOutcome);
            this._serializedState = JsonSerializer.Serialize(this._state, DurableAgentStateJsonContext.Default.DurableAgentState);
            this._entity = AgentEntityDeliveryTests.CreateHarness(
                this._agent, this._state, registerWithFactory: true, onFactoryInvoked: () => this._factoryCalls++,
                enableMailboxWrites: false, authorizeLegacyMigration: false);
            try
            {
                AgentResponse response = await this._entity.RunAsync(request);
                return Assert.IsType<AgentResponse>(converter.Deserialize(converter.Serialize(response), typeof(AgentResponse)));
            }
            catch (Exception exception)
            {
                throw WrapSdkFailure(exception, this._entityOperationFailure);
            }
        }
    }

    private static DurableAgentState CreateCommittedState(string correlationId, bool legacy, string? unavailableOutcome)
    {
        DurableAgentStateMessage message = DurableAgentStateMessage.FromChatMessage(new ChatMessage(ChatRole.Assistant, FailureText));
        if (legacy)
        {
            return new DurableAgentState
            {
                Data = new DurableAgentStateData
                {
                    ConversationHistory =
                    [
                        new DurableAgentStateErrorResponse { CorrelationId = correlationId, CreatedAt = s_completedAt, Messages = [message] },
                    ],
                },
            };
        }

        string outcome = unavailableOutcome ?? DurableAgentStateCompletionReceipt.FailedOutcome;
        return new DurableAgentState
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                // Contradictory transcript text cannot override schema-2 completion evidence.
                ConversationHistory =
                [
                    new DurableAgentStateResponse { CorrelationId = correlationId, CreatedAt = s_completedAt, Messages = [message] },
                ],
                TerminalResults = unavailableOutcome is not null ? [] : new Dictionary<string, DurableAgentStateTerminalResult>
                {
                    [correlationId] = new()
                    {
                        CorrelationId = correlationId,
                        Outcome = outcome,
                        CompletedAt = s_completedAt,
                        Response = new DurableAgentStateTerminalResponse
                        {
                            Messages = [message],
                            Value = JsonSerializer.Deserialize<JsonElement>("null"),
                            UnknownProperties = new Dictionary<string, JsonElement>
                            {
                                ["futureMetadata"] = JsonSerializer.Deserialize<JsonElement>("{}"),
                            },
                        },
                        Error = new DurableAgentStateTerminalError
                        {
                            Code = "CommittedFailure",
                            Message = "recorded error, not inferred from response text",
                            Details = JsonSerializer.Deserialize<JsonElement>("""{"reason":{"empty":"","false":false,"zero":0,"null":null}}"""),
                        },
                    },
                },
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>
                {
                    [correlationId] = new()
                    {
                        CorrelationId = correlationId,
                        Outcome = outcome,
                        CompletedAt = s_completedAt,
                        ResultState = unavailableOutcome is null
                            ? DurableAgentStateCompletionReceipt.AvailableResult
                            : DurableAgentStateCompletionReceipt.UnavailableResult,
                        ResultExpiresAt = unavailableOutcome is null ? null : s_completedAt.AddMinutes(1),
                        ResultUnavailableAt = unavailableOutcome is null ? null : s_completedAt.AddMinutes(1),
                    },
                },
            },
        };
    }
}
