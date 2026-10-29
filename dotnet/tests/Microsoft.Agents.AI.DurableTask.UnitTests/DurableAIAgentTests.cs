// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using DurableTask.Core.Command;
using DurableTask.Core.Entities;
using DurableTask.Core.History;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Entities;
using Microsoft.DurableTask.Worker;
using Microsoft.DurableTask.Worker.Shims;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests;

public sealed class DurableAIAgentTests
{
    private const string ResponseText = """{"value":"done"}""";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunAsync_OneShotMessagesAreEnumeratedOnceAsync(bool streaming)
    {
        ChatMessage[] expected = [new(ChatRole.User, "first"), new(ChatRole.User, "second")];
        Queue<ChatMessage> pending = new(expected);
        int enumerations = 0;
        IEnumerable<ChatMessage> Messages()
        {
            enumerations++;
            while (pending.TryDequeue(out ChatMessage? message))
            {
                yield return message;
            }
        }

        Mock<TaskOrchestrationEntityFeature> entities = new();
        entities
            .Setup(e => e.CallEntityAsync<AgentResponse>(
                It.IsAny<EntityInstanceId>(),
                "Run",
                It.IsAny<object?>(),
                It.IsAny<CallEntityOptions?>()))
            .Returns((EntityInstanceId _, string _, object? input, CallEntityOptions? _) =>
            {
                RunRequest request = Assert.IsType<RunRequest>(input);
                Assert.Equal(expected, request.Messages);
                Assert.Equal(expected, AIAgent.CurrentRunContext!.RequestMessages);
                return Task.FromResult(new AgentResponse(new ChatMessage(ChatRole.Assistant, ResponseText)));
            });
        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(c => c.Entities).Returns(entities.Object);
        context.SetupGet(c => c.InstanceId).Returns("orchestration");
        DurableAIAgent agent = context.Object.GetAgent("TestAgent");
        DurableAgentSession session = new(new AgentSessionId("TestAgent", "session"));

        if (streaming)
        {
            StringBuilder text = new();
            await foreach (AgentResponseUpdate update in agent.RunStreamingAsync(Messages(), session))
            {
                text.Append(update.Text);
            }

            Assert.Equal(ResponseText, text.ToString());
        }
        else
        {
            Assert.Equal(ResponseText, (await agent.RunAsync(Messages(), session)).Text);
        }

        Assert.Equal(1, enumerations);
        entities.VerifyAll();
    }

    [Theory]
    [InlineData("NoMessage")]
    [InlineData("Text")]
    [InlineData("Message")]
    [InlineData("Messages")]
    public async Task RunAsync_PendingEntityResponsePreservesOrchestrationContextAsync(string overload)
    {
        await VerifyOrchestrationContextAsync(async (agent, session, options, observe) =>
        {
            Task<AgentResponse> run = overload switch
            {
                "NoMessage" => agent.RunAsync(session, options),
                "Text" => agent.RunAsync("hello", session, options),
                "Message" => agent.RunAsync(new ChatMessage(ChatRole.User, "hello"), session, options),
                "Messages" => agent.RunAsync([new ChatMessage(ChatRole.User, "hello")], session, options),
                _ => throw new ArgumentOutOfRangeException(nameof(overload))
            };

            observe(run);
            AgentResponse response = await run;
            Assert.Equal(ResponseText, response.Text);
        });
    }

    [Theory]
    [InlineData("NoMessage")]
    [InlineData("Text")]
    [InlineData("Message")]
    [InlineData("Messages")]
    public async Task RunAsync_StructuredPendingEntityResponsePreservesOrchestrationContextAsync(string overload)
    {
        await VerifyOrchestrationContextAsync(async (agent, session, options, observe) =>
        {
            Task<AgentResponse<Dictionary<string, string>>> run = overload switch
            {
                "NoMessage" => agent.RunAsync<Dictionary<string, string>>(session, options: options),
                "Text" => agent.RunAsync<Dictionary<string, string>>("hello", session, options: options),
                "Message" => agent.RunAsync<Dictionary<string, string>>(new ChatMessage(ChatRole.User, "hello"), session, options: options),
                "Messages" => agent.RunAsync<Dictionary<string, string>>([new ChatMessage(ChatRole.User, "hello")], session, options: options),
                _ => throw new ArgumentOutOfRangeException(nameof(overload))
            };

            observe(run);
            AgentResponse<Dictionary<string, string>> response = await run;
            Assert.Equal("done", response.Result["value"]);
        });
    }

    [Theory]
    [InlineData("NoMessage")]
    [InlineData("Text")]
    [InlineData("Message")]
    [InlineData("Messages")]
    public async Task RunStreamingAsync_PendingEntityResponsePreservesOrchestrationContextAsync(string overload)
    {
        await VerifyOrchestrationContextAsync(async (agent, session, options, observe) =>
        {
            IAsyncEnumerable<AgentResponseUpdate> updates = overload switch
            {
                "NoMessage" => agent.RunStreamingAsync(session, options),
                "Text" => agent.RunStreamingAsync("hello", session, options),
                "Message" => agent.RunStreamingAsync(new ChatMessage(ChatRole.User, "hello"), session, options),
                "Messages" => agent.RunStreamingAsync([new ChatMessage(ChatRole.User, "hello")], session, options),
                _ => throw new ArgumentOutOfRangeException(nameof(overload))
            };

            StringBuilder response = new();
            await using IAsyncEnumerator<AgentResponseUpdate> enumerator = updates.GetAsyncEnumerator();
            Task<bool> moveNext = enumerator.MoveNextAsync().AsTask();
            observe(moveNext);
            while (await moveNext)
            {
                response.Append(enumerator.Current.Text);
                moveNext = enumerator.MoveNextAsync().AsTask();
            }

            Assert.Equal(ResponseText, response.ToString());
        });
    }

    [Fact]
    public async Task RunAsync_PendingEntityFailurePreservesOrchestrationContextAsync()
    {
        InvalidOperationException failure = new("Entity failed.");
        await VerifyOrchestrationContextAsync(async (agent, session, options, observe) =>
        {
            try
            {
                Task<AgentResponse> run = agent.RunAsync("hello", session, options);
                observe(run);
                await run;
                Assert.Fail("The entity failure should propagate.");
            }
            catch (InvalidOperationException exception)
            {
                Assert.Same(failure, exception);
            }
        }, failure);
    }

    private static Task VerifyOrchestrationContextAsync(
        Func<DurableAIAgent, AgentSession, AgentRunOptions, Action<Task>, Task> invoke,
        Exception? failure = null)
    {
        return Task.Run(() => VerifyOrchestrationContextCoreAsync(invoke, failure));
    }

    private static async Task VerifyOrchestrationContextCoreAsync(
        Func<DurableAIAgent, AgentSession, AgentRunOptions, Action<Task>, Task> invoke,
        Exception? failure)
    {
        TaskCompletionSource<AgentResponse> entityResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);
        AgentRunContext? capturedRunContext = null;
        Mock<TaskOrchestrationEntityFeature> entities = new();
        entities
            .Setup(e => e.CallEntityAsync<AgentResponse>(
                It.IsAny<EntityInstanceId>(),
                "Run",
                It.IsAny<object?>(),
                It.IsAny<CallEntityOptions?>()))
            .Returns(() =>
            {
                capturedRunContext = AIAgent.CurrentRunContext;
                return entityResponse.Task;
            });

        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(c => c.Entities).Returns(entities.Object);
        context.SetupGet(c => c.InstanceId).Returns("orchestration");
        DurableAIAgent agent = context.Object.GetAgent("TestAgent");
        DurableAgentSession session = new(new AgentSessionId("TestAgent", "session"));
        AgentRunOptions options = new();
        AgentRunContext? previousRunContext = AIAgent.CurrentRunContext;
        SynchronizationContext? previousContext = SynchronizationContext.Current;
        using TrackingSynchronizationContext orchestrationContext = new();
        int ownerThreadId = Environment.CurrentManagedThreadId;
        int completionThreadId = 0;
        SynchronizationContext? completionContext = null;
        Task? observation = null;
        void Observe(Task task)
        {
            // Observe completion before the caller's await can marshal back and hide an escaping continuation.
            observation = task.ContinueWith(
                _ =>
                {
                    completionThreadId = Environment.CurrentManagedThreadId;
                    completionContext = SynchronizationContext.Current;
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        Task run;
        try
        {
            SynchronizationContext.SetSynchronizationContext(orchestrationContext);
            run = invoke(agent, session, options, Observe);
            Assert.False(run.IsCompleted);
            Assert.Same(previousRunContext, AIAgent.CurrentRunContext);

            Thread entityCompletion = new(() =>
            {
                if (failure is null)
                {
                    entityResponse.SetResult(new AgentResponse(new ChatMessage(ChatRole.Assistant, ResponseText)));
                }
                else
                {
                    entityResponse.SetException(failure);
                }
            });
            entityCompletion.Start();
            Assert.True(entityCompletion.Join(TimeSpan.FromSeconds(10)));
            Assert.NotEqual(ownerThreadId, entityCompletion.ManagedThreadId);
            orchestrationContext.PumpUntil(run);
            await run;
            Assert.NotNull(observation);
            orchestrationContext.PumpUntil(observation);
            await observation;
            Assert.Equal(ownerThreadId, completionThreadId);
            Assert.Same(orchestrationContext, completionContext);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previousContext);
        }

        Assert.NotNull(capturedRunContext);
        Assert.Same(agent, capturedRunContext.Agent);
        Assert.Same(session, capturedRunContext.Session);
        Assert.Same(previousRunContext, AIAgent.CurrentRunContext);
    }

    [Theory]
    [InlineData(false, null, "dafx-testagent")]
    [InlineData(false, 1, "dafx-testagent")]
    [InlineData(false, 2, "dafx-v2-testagent")]
    [InlineData(true, null, "dafx-testagent")]
    [InlineData(true, 1, "dafx-testagent")]
    [InlineData(true, 2, "dafx-v2-testagent")]
    public void RunAsync_PendingOrCompletedFirstTurnAcrossCutover_PreservesEntityVersionOnNextTurn(
        bool responseRecordedBeforeCutover,
        int? explicitResponseEntityVersion,
        string expectedSecondTurnEntityName)
    {
        DurableDataConverter converter = new();
        DurableTaskShimFactory shimFactory = new(
            new DurableTaskWorkerOptions { DataConverter = converter, EnableEntitySupport = true },
            NullLoggerFactory.Instance);

        async Task<string?> TwoTurnOrchestratorAsync(TaskOrchestrationContext context)
        {
            DurableAIAgent agent = context.GetAgent("TestAgent");
            AgentSession session = await agent.CreateSessionAsync();
            AgentResponse first = await agent.RunAsync("turn 1", session);
            if (responseRecordedBeforeCutover)
            {
                _ = await context.WaitForExternalEvent<string>("Continue");
            }

            AgentResponse second = await agent.RunAsync($"turn 2 after {first.Text}", session);
            return $"{session.GetService<AgentSessionId>()}:{second.Text}";
        }

        DateTime startTime = new(2026, 10, 27, 12, 0, 0, DateTimeKind.Utc);

        // Step 1: Execute orchestration up to the first entity call so we capture the deterministic request ID and input.
        global::DurableTask.Core.TaskOrchestration initialShim = shimFactory.CreateOrchestration<string?, string>(
            new TaskName("TwoTurnOrchestrator"),
            (ctx, _) => TwoTurnOrchestratorAsync(ctx));
        global::DurableTask.Core.OrchestrationRuntimeState initialState = new(
        [
            new OrchestratorStartedEvent(-1) { Timestamp = startTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "TwoTurnOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance
                {
                    InstanceId = "orch-cutover-1",
                    ExecutionId = "exec-1",
                },
            },
        ]);
        global::DurableTask.Core.OrchestratorExecutionResult firstStep = new global::DurableTask.Core.TaskOrchestrationExecutor(
            initialState,
            initialShim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters()).Execute();

        SendEventOrchestratorAction firstEntityCall = Assert.IsType<SendEventOrchestratorAction>(
            Assert.Single(firstStep.Actions));
        string sessionKey = EntityId.FromString(firstEntityCall.Instance!.InstanceId).Key;
        string scheduledEntityInstanceId = new EntityId(
            expectedSecondTurnEntityName,
            sessionKey).ToString();
        using JsonDocument requestDoc = JsonDocument.Parse(firstEntityCall.EventData!);
        string requestId = requestDoc.RootElement.GetProperty("id").GetString()!;

        // Build either an unversioned legacy response or a durable-envelope response with explicit entityVersion.
        AgentResponse firstTurnResponse = new(new ChatMessage(ChatRole.Assistant, "first-reply"));
        if (explicitResponseEntityVersion is int version)
        {
            DateTimeOffset completedAt = new(startTime);
            DurableAgentStateTerminalResult terminalResult = DurableAgentStateTerminalResult.FromResponse(
                "corr-1",
                firstTurnResponse,
                completedAt);
            DurableAgentJsonUtilities.CaptureRetainedResult(firstTurnResponse, terminalResult.Response!);
            DurableAgentJsonUtilities.CaptureResponseEntityVersion(firstTurnResponse, version);
        }

        string serializedAgentResponse = converter.Serialize(firstTurnResponse);
        string entityResponseEventJson = JsonSerializer.Serialize(new { result = serializedAgentResponse });

        // Step 2: Resume after v2 cutover. When responseRecordedBeforeCutover is false, the first entity call was
        // scheduled before cutover (EventSent in PastEvents) and its response arrives after cutover (EventRaised in NewEvents),
        // so IsReplaying is true before CallEntityAsync and false after awaiting CallEntityAsync.
        List<HistoryEvent> pastEvents =
        [
            new OrchestratorStartedEvent(-1) { Timestamp = startTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "TwoTurnOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance
                {
                    InstanceId = "orch-cutover-1",
                    ExecutionId = "exec-1",
                },
            },
            new EventSentEvent(firstEntityCall.Id)
            {
                InstanceId = scheduledEntityInstanceId,
                Name = firstEntityCall.EventName,
                Input = firstEntityCall.EventData,
            },
            new OrchestratorCompletedEvent(-1),
        ];

        if (responseRecordedBeforeCutover)
        {
            pastEvents.Add(new OrchestratorStartedEvent(-1) { Timestamp = startTime.AddSeconds(1) });
            pastEvents.Add(new EventRaisedEvent(-1, entityResponseEventJson) { Name = requestId });
            pastEvents.Add(new OrchestratorCompletedEvent(-1));
        }

        global::DurableTask.Core.OrchestrationRuntimeState cutoverState = new(pastEvents);
        cutoverState.AddEvent(new OrchestratorStartedEvent(-1) { Timestamp = startTime.AddSeconds(2) });
        if (responseRecordedBeforeCutover)
        {
            cutoverState.AddEvent(new EventRaisedEvent(-1, "\"go\"") { Name = "Continue" });
        }
        else
        {
            cutoverState.AddEvent(new EventRaisedEvent(-1, entityResponseEventJson) { Name = requestId });
        }

        global::DurableTask.Core.TaskOrchestration cutoverShim = shimFactory.CreateOrchestration<string?, string>(
            new TaskName("TwoTurnOrchestrator"),
            (ctx, _) => TwoTurnOrchestratorAsync(ctx));
        global::DurableTask.Core.OrchestratorExecutionResult secondStep = new global::DurableTask.Core.TaskOrchestrationExecutor(
            cutoverState,
            cutoverShim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters()).Execute();

        SendEventOrchestratorAction secondEntityCall = Assert.IsType<SendEventOrchestratorAction>(
            Assert.Single(secondStep.Actions));
        Assert.Equal(scheduledEntityInstanceId, secondEntityCall.Instance!.InstanceId);
    }

    [Theory]
    [InlineData("@dafx-v2-testagent@2f60edd06ae75fb7896f3b75fb95552e", "dafx-v2-testagent", 2)]
    [InlineData("@dafx-testagent@2f60edd06ae75fb7896f3b75fb95552e", "dafx-testagent", 1)]
    public void RunAsync_PlainJsonSerializedResponseWithAgentId_PreservesEntityVersionAcrossMultiTurnOrchestration(
        string responseAgentId,
        string expectedSecondTurnEntityName,
        int expectedEntityVersion)
    {
        DurableDataConverter converter = new();
        DurableTaskShimFactory shimFactory = new(
            new DurableTaskWorkerOptions { DataConverter = converter, EnableEntitySupport = true },
            NullLoggerFactory.Instance);
        DateTime startTime = new(2026, 10, 27, 12, 0, 0, DateTimeKind.Utc);

        static async Task<string?> ChainedOrchestratorAsync(TaskOrchestrationContext context)
        {
            DurableAIAgent writer = context.GetAgent("TestAgent");
            AgentSession writerSession = await writer.CreateSessionAsync();
            AgentResponse initial = await writer.RunAsync("Write a tagline.", writerSession);
            AgentResponse refined = await writer.RunAsync($"Improve: {initial.Text}", writerSession);
            return refined.Text;
        }

        global::DurableTask.Core.TaskOrchestration initialShim = shimFactory.CreateOrchestration<string?, string>(
            new TaskName("ChainedOrchestrator"),
            (ctx, _) => ChainedOrchestratorAsync(ctx));
        global::DurableTask.Core.OrchestrationRuntimeState initialState = new(
        [
            new OrchestratorStartedEvent(-1) { Timestamp = startTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "ChainedOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance
                {
                    InstanceId = "orch-functions-chaining",
                    ExecutionId = "exec-1",
                },
            },
        ]);
        global::DurableTask.Core.OrchestratorExecutionResult firstStep = new global::DurableTask.Core.TaskOrchestrationExecutor(
            initialState,
            initialShim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters()).Execute();

        SendEventOrchestratorAction firstEntityCall = Assert.IsType<SendEventOrchestratorAction>(
            Assert.Single(firstStep.Actions));
        string sessionKey = EntityId.FromString(firstEntityCall.Instance!.InstanceId).Key;
        string expectedEntityInstanceId = new EntityId(expectedSecondTurnEntityName, sessionKey).ToString();
        using JsonDocument requestDoc = JsonDocument.Parse(firstEntityCall.EventData!);
        string requestId = requestDoc.RootElement.GetProperty("id").GetString()!;

        // Simulate Azure Functions GrpcEntityRunner.LoadAndRunAsync, which serializes AgentResponse with
        // JsonDataConverter.Default (no $microsoftAgentFrameworkDurableTask envelope) while preserving AgentId.
        AgentResponse plainEntityResponse = new(new ChatMessage(ChatRole.Assistant, "initial tagline"))
        {
            AgentId = responseAgentId,
        };
        string plainJson = JsonSerializer.Serialize(plainEntityResponse, DurableAgentJsonUtilities.DefaultOptions);
        Assert.DoesNotContain("$microsoftAgentFrameworkDurableTask", plainJson, StringComparison.Ordinal);

        AgentResponse deserialized = Assert.IsType<AgentResponse>(converter.Deserialize(plainJson, typeof(AgentResponse)));
        Assert.Equal(expectedEntityVersion, DurableAgentJsonUtilities.GetResponseEntityVersion(deserialized));

        string entityResponseEventJson = JsonSerializer.Serialize(new { result = plainJson });
        global::DurableTask.Core.OrchestrationRuntimeState secondTurnState = new(
        [
            new OrchestratorStartedEvent(-1) { Timestamp = startTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "ChainedOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance
                {
                    InstanceId = "orch-functions-chaining",
                    ExecutionId = "exec-1",
                },
            },
            new EventSentEvent(firstEntityCall.Id)
            {
                InstanceId = expectedEntityInstanceId,
                Name = firstEntityCall.EventName,
                Input = firstEntityCall.EventData,
            },
            new OrchestratorCompletedEvent(-1),
        ]);
        secondTurnState.AddEvent(new OrchestratorStartedEvent(-1) { Timestamp = startTime.AddSeconds(1) });
        secondTurnState.AddEvent(new EventRaisedEvent(-1, entityResponseEventJson) { Name = requestId });

        global::DurableTask.Core.TaskOrchestration secondShim = shimFactory.CreateOrchestration<string?, string>(
            new TaskName("ChainedOrchestrator"),
            (ctx, _) => ChainedOrchestratorAsync(ctx));
        global::DurableTask.Core.OrchestratorExecutionResult secondStep = new global::DurableTask.Core.TaskOrchestrationExecutor(
            secondTurnState,
            secondShim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters()).Execute();

        SendEventOrchestratorAction secondEntityCall = Assert.IsType<SendEventOrchestratorAction>(
            Assert.Single(secondStep.Actions));
        Assert.Equal(expectedEntityInstanceId, secondEntityCall.Instance!.InstanceId);
    }

    [Fact]
    public void RunAsync_WorkerWithLocalAgentRegistered_DispatchesRemoteAgentCallByDefault()
    {
        DurableAgentsOptions agentOptions = new();
        agentOptions.AddAIAgentFactory("LocalAgentA", _ => throw new NotSupportedException());
        DurableDataConverter converter = new(agentOptions);
        DurableTaskShimFactory shimFactory = new(
            new DurableTaskWorkerOptions { DataConverter = converter, EnableEntitySupport = true },
            NullLoggerFactory.Instance);
        DateTime startTime = new(2026, 10, 27, 12, 0, 0, DateTimeKind.Utc);

        static async Task<string?> MixedHostingOrchestratorAsync(TaskOrchestrationContext context)
        {
            DurableAIAgent remoteAgent = context.GetAgent("RemoteAgentB");
            AgentSession remoteSession = await remoteAgent.CreateSessionAsync();
            AgentResponse response = await remoteAgent.RunAsync("Hello remote worker", remoteSession);
            return response.Text;
        }

        global::DurableTask.Core.TaskOrchestration shim = shimFactory.CreateOrchestration<string?, string>(
            new TaskName("MixedHostingOrchestrator"),
            (ctx, _) => MixedHostingOrchestratorAsync(ctx));
        global::DurableTask.Core.OrchestrationRuntimeState state = new(
        [
            new OrchestratorStartedEvent(-1) { Timestamp = startTime },
            new ExecutionStartedEvent(-1, "null")
            {
                Name = "MixedHostingOrchestrator",
                OrchestrationInstance = new global::DurableTask.Core.OrchestrationInstance
                {
                    InstanceId = "orch-mixed-hosting",
                    ExecutionId = "exec-1",
                },
            },
        ]);

        global::DurableTask.Core.OrchestratorExecutionResult step = new global::DurableTask.Core.TaskOrchestrationExecutor(
            state,
            shim,
            global::DurableTask.Core.BehaviorOnContinueAsNew.Carryover,
            new TaskOrchestrationEntityParameters()).Execute();

        SendEventOrchestratorAction entityCall = Assert.IsType<SendEventOrchestratorAction>(
            Assert.Single(step.Actions));
        EntityId targetEntityId = EntityId.FromString(entityCall.Instance!.InstanceId);
        Assert.Equal("dafx-v2-remoteagentb", targetEntityId.Name);
    }

    private sealed class TrackingSynchronizationContext : SynchronizationContext, IDisposable
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> _work = new();

        public override void Post(SendOrPostCallback d, object? state) => this._work.Add((d, state));

        public void PumpUntil(Task task)
        {
            DateTime deadline = DateTime.UtcNow.AddSeconds(10);
            while (!task.IsCompleted)
            {
                Assert.True(DateTime.UtcNow < deadline, "The orchestration continuation did not complete.");
                if (this._work.TryTake(out var work, TimeSpan.FromMilliseconds(10)))
                {
                    work.Callback(work.State);
                }
            }
        }

        public void Dispose() => this._work.Dispose();
    }
}
