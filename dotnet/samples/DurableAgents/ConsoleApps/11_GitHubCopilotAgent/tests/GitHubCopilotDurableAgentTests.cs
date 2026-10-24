// Copyright (c) Microsoft. All rights reserved.

using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using GitHub.Copilot;
using GitHubCopilotDurableAgent;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace GitHubCopilotDurableAgentTests;

public sealed class GitHubCopilotSampleTests
{
    [Fact]
    public async Task MultiTurn_CurrentRequestOnlyWithProviderKey_PreservesSessionIdWithoutDuplicatingHistoryAsync()
    {
        await using FakeCopilotJsonRpcServer server = new();
        await using CopilotClient copilotClient = new(
            GitHubCopilotDurableDemo.CreateClientOptions(server.Endpoint));
        AIAgent copilotAgent = GitHubCopilotDurableDemo.CreateCopilotAgent(copilotClient);

        using ServiceProvider services = CreateServiceProvider(options =>
            GitHubCopilotDurableDemo.RegisterCopilotAgent(options.Agents, copilotAgent));

        AgentSessionId sessionId = new(GitHubCopilotDurableDemo.AgentName, "incident-session-1");

        // Turn 1: fresh entity state (null -> initialized to schema 2.0.0)
        (AgentResponse turn1Response, string serializedTurn1State) = await ExecuteEntityTurnAsync(
            services,
            sessionId,
            persistedStateJson: null,
            prompt: "Turn 1: Remember incident INC-42.",
            correlationId: "corr-1");

        Assert.Equal("Reply to: Turn 1: Remember incident INC-42.", turn1Response.Text);
        Assert.Single(server.CreatedSessionIds);
        Assert.Empty(server.ResumedSessionIds);
        Assert.Equal(["Turn 1: Remember incident INC-42."], server.SentPrompts);

        using (JsonDocument stateDoc = JsonDocument.Parse(serializedTurn1State))
        {
            JsonElement data = stateDoc.RootElement.GetProperty("data");

            // Under CurrentRequestOnly, conversationHistory remains empty while session.sessionId is persisted.
            Assert.Equal(0, data.GetProperty("conversationHistory").GetArrayLength());
            Assert.Equal(
                server.CreatedSessionIds[0],
                data.GetProperty("session").GetProperty("sessionId").GetString());
            Assert.Equal(
                GitHubCopilotDurableDemo.HistoryProviderKey,
                data.GetProperty("historyBinding").GetProperty("providerKey").GetString());
        }

        // Turn 2: simulate cold host restart by creating a new ServiceProvider and restoring from serialized JSON.
        AIAgent restoredCopilotAgent = GitHubCopilotDurableDemo.CreateCopilotAgent(copilotClient);
        using ServiceProvider restoredServices = CreateServiceProvider(options =>
            GitHubCopilotDurableDemo.RegisterCopilotAgent(options.Agents, restoredCopilotAgent));

        (AgentResponse turn2Response, string serializedTurn2State) = await ExecuteEntityTurnAsync(
            restoredServices,
            sessionId,
            persistedStateJson: serializedTurn1State,
            prompt: "Turn 2: What incident number did we discuss?",
            correlationId: "corr-2");

        Assert.Equal("Reply to: Turn 2: What incident number did we discuss?", turn2Response.Text);
        Assert.Single(server.CreatedSessionIds);
        Assert.Equal([server.CreatedSessionIds[0]], server.ResumedSessionIds);
        Assert.Equal(
            [
                "Turn 1: Remember incident INC-42.",
                "Turn 2: What incident number did we discuss?",
            ],
            server.SentPrompts);

        using (JsonDocument turn2Doc = JsonDocument.Parse(serializedTurn2State))
        {
            JsonElement data = turn2Doc.RootElement.GetProperty("data");
            Assert.Equal(0, data.GetProperty("conversationHistory").GetArrayLength());
            Assert.Equal(
                server.CreatedSessionIds[0],
                data.GetProperty("session").GetProperty("sessionId").GetString());
        }
    }

    [Fact]
    public async Task IncidentOrchestration_WithHumanInTheLoopApproval_ReusesSameDurableSessionAcrossTurnsAsync()
    {
        await using FakeCopilotJsonRpcServer server = new();
        await using CopilotClient copilotClient = new(
            GitHubCopilotDurableDemo.CreateClientOptions(server.Endpoint));
        AIAgent copilotAgent = GitHubCopilotDurableDemo.CreateCopilotAgent(copilotClient);

        using ServiceProvider services = CreateServiceProvider(options =>
            GitHubCopilotDurableDemo.RegisterCopilotAgent(options.Agents, copilotAgent));

        string? persistedEntityStateJson = null;
        List<EntityInstanceId> invokedEntityIds = [];
        List<string> customStatuses = [];

        Mock<TaskOrchestrationEntityFeature> entityFeature = new();
        entityFeature
            .Setup(e => e.CallEntityAsync<AgentResponse>(
                It.IsAny<EntityInstanceId>(),
                It.IsAny<string>(),
                It.IsAny<object?>(),
                It.IsAny<CallEntityOptions?>()))
            .Returns(async (EntityInstanceId entityId, string _, object? requestObj, CallEntityOptions? _) =>
            {
                invokedEntityIds.Add(entityId);
                (AgentResponse response, string nextStateJson) = await ExecuteEntityRequestObjectAsync(
                    services,
                    entityId,
                    persistedEntityStateJson,
                    requestObj!);
                persistedEntityStateJson = nextStateJson;
                return response;
            });

        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(c => c.Entities).Returns(entityFeature.Object);
        context.SetupGet(c => c.InstanceId).Returns("incident-orch-1");
        context.SetupGet(c => c.IsReplaying).Returns(false);
        context.Setup(c => c.NewGuid()).Returns(Guid.Parse("11111111-2222-3333-4444-555555555555"));
        context.Setup(c => c.SetCustomStatus(It.IsAny<object?>()))
            .Callback<object?>(status =>
            {
                if (status is string text)
                {
                    customStatuses.Add(text);
                }
            });
        context.Setup(c => c.WaitForExternalEvent<bool>(
                GitHubCopilotDurableDemo.ApprovalEventName,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        CopilotIncidentResult result = await GitHubCopilotDurableDemo.RunIncidentOrchestrationAsync(
            context.Object,
            new CopilotIncidentInput("HTTP 503 spike on checkout-service", ApprovalTimeoutMinutes: 15));

        Assert.True(result.Approved);
        Assert.Equal(
            "Reply to: Diagnose this incident and propose a step-by-step remediation plan: HTTP 503 spike on checkout-service",
            result.ProposedPlan);
        Assert.Equal(
            "Reply to: The operator approved the remediation plan. Provide the exact verification steps for the approved plan.",
            result.FinalResolution);

        // Both turns targeted the exact same Durable Entity instance and the same underlying Copilot session.
        Assert.Equal(2, invokedEntityIds.Count);
        Assert.Equal(invokedEntityIds[0], invokedEntityIds[1]);
        Assert.Single(server.CreatedSessionIds);
        Assert.Equal([server.CreatedSessionIds[0]], server.ResumedSessionIds);
        Assert.Equal(2, server.SentPrompts.Count);
        Assert.Contains(
            customStatuses,
            status => status.Contains(GitHubCopilotDurableDemo.ApprovalEventName, StringComparison.Ordinal));
    }

    [Fact]
    public async Task DurableWorkflow_WithCopilotAgent_ExecutesEndToEndViaRegisteredEntityAsync()
    {
        await using FakeCopilotJsonRpcServer server = new();
        await using CopilotClient copilotClient = new(
            GitHubCopilotDurableDemo.CreateClientOptions(server.Endpoint));
        AIAgent copilotAgent = GitHubCopilotDurableDemo.CreateCopilotAgent(copilotClient);
        Workflow workflow = GitHubCopilotDurableDemo.BuildTriageWorkflow(copilotAgent);

        using ServiceProvider services = CreateServiceProvider(options =>
        {
            GitHubCopilotDurableDemo.RegisterCopilotAgent(options.Agents, copilotAgent);
            options.Workflows.AddWorkflow(workflow);
        });

        DurableTaskRegistry registry = services
            .GetRequiredService<IOptionsMonitor<DurableTaskRegistry>>()
            .Get(Options.DefaultName);

        KeyValuePair<TaskName, Func<IServiceProvider, ITaskOrchestrator>> workflowOrchestratorEntry =
            Assert.Single(
                registry.GetOrchestrators(),
                entry => string.Equals(
                    entry.Key.Name,
                    $"dafx-{GitHubCopilotDurableDemo.WorkflowName}",
                    StringComparison.Ordinal));

        ITaskOrchestrator orchestrator = workflowOrchestratorEntry.Value(services);

        Mock<TaskOrchestrationEntityFeature> entityFeature = new();
        entityFeature
            .Setup(e => e.CallEntityAsync<AgentResponse>(
                It.IsAny<EntityInstanceId>(),
                It.IsAny<string>(),
                It.IsAny<object?>(),
                It.IsAny<CallEntityOptions?>()))
            .Returns(async (EntityInstanceId entityId, string _, object? requestObj, CallEntityOptions? _) =>
            {
                (AgentResponse response, _) = await ExecuteEntityRequestObjectAsync(
                    services,
                    entityId,
                    persistedStateJson: null,
                    requestObj!);
                return response;
            });

        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(c => c.Name).Returns(workflowOrchestratorEntry.Key);
        context.SetupGet(c => c.InstanceId).Returns("workflow-instance-1");
        context.SetupGet(c => c.IsReplaying).Returns(false);
        context.SetupGet(c => c.Entities).Returns(entityFeature.Object);
        context.Setup(c => c.NewGuid()).Returns(Guid.Parse("d9a9751e-30fd-4f7a-95f8-f522b4e76977"));
        context.Setup(c => c.CreateReplaySafeLogger(It.IsAny<string>())).Returns(NullLogger.Instance);

        object workflowInput = CreateWorkflowInput("Investigate high CPU on worker-node-3");
        object? rawResult = await orchestrator.RunAsync(context.Object, workflowInput);

        Assert.NotNull(rawResult);
        PropertyInfo? resultProperty = rawResult!.GetType().GetProperty("Result");
        Assert.NotNull(resultProperty);
        string? workflowResultText = resultProperty!.GetValue(rawResult) as string;

        Assert.Equal("Reply to: Investigate high CPU on worker-node-3", workflowResultText);
        Assert.Equal(["Investigate high CPU on worker-node-3"], server.SentPrompts);
    }

    private static ServiceProvider CreateServiceProvider(Action<DurableOptions> configure)
    {
        ServiceCollection services = new();
        services.AddLogging();
        services.AddSingleton(new Mock<DurableTaskClient>("test").Object);
        services.AddSingleton(
            Mock.Of<IHostApplicationLifetime>(
                lifetime => lifetime.ApplicationStopping == CancellationToken.None));

        services.ConfigureDurableOptions(
            configure,
            workerBuilder: _ => { },
            clientBuilder: _ => { });

        return services.BuildServiceProvider();
    }

    private static async Task<(AgentResponse Response, string SerializedState)> ExecuteEntityTurnAsync(
        IServiceProvider services,
        AgentSessionId sessionId,
        string? persistedStateJson,
        string prompt,
        string correlationId)
    {
        object runRequest = CreateRunRequest(prompt, correlationId);
        return await ExecuteEntityRequestObjectAsync(
            services,
            sessionId,
            persistedStateJson,
            runRequest);
    }

    private static async Task<(AgentResponse Response, string SerializedState)> ExecuteEntityRequestObjectAsync(
        IServiceProvider services,
        EntityInstanceId entityId,
        string? persistedStateJson,
        object runRequest)
    {
        Assembly durableAssembly = typeof(DurableAgentsOptions).Assembly;
        Type stateType = durableAssembly.GetType(
            "Microsoft.Agents.AI.DurableTask.State.DurableAgentState",
            throwOnError: true)!;
        Type jsonContextType = durableAssembly.GetType(
            "Microsoft.Agents.AI.DurableTask.State.DurableAgentStateJsonContext",
            throwOnError: true)!;
        object defaultContext = jsonContextType
            .GetProperty("Default", BindingFlags.Public | BindingFlags.Static)!
            .GetValue(null)!;
        JsonSerializerOptions stateJsonOptions = (JsonSerializerOptions)jsonContextType
            .GetProperty("Options", BindingFlags.Public | BindingFlags.Instance)!
            .GetValue(defaultContext)!;

        object? hydratedState = persistedStateJson is null
            ? null
            : JsonSerializer.Deserialize(persistedStateJson, stateType, stateJsonOptions);

        DurableTaskRegistry registry = services
            .GetRequiredService<IOptionsMonitor<DurableTaskRegistry>>()
            .Get(Options.DefaultName);
        Func<IServiceProvider, ITaskEntity> entityFactory = registry
            .GetEntities()
            .First(entry => string.Equals(entry.Key.Name, entityId.Name, StringComparison.OrdinalIgnoreCase))
            .Value;

        ITaskEntity entity = entityFactory(services);

        Mock<TaskEntityContext> entityContext = new();
        entityContext.SetupGet(c => c.Id).Returns(entityId);

        object? committedState = null;
        Mock<TaskEntityState> entityState = new();
        entityState.SetupGet(s => s.HasState).Returns(hydratedState is not null);
        entityState.Setup(s => s.GetState(stateType)).Returns(hydratedState);
        entityState.Setup(s => s.SetState(It.IsAny<object?>()))
            .Callback<object?>(value => committedState = value);

        Mock<TaskEntityOperation> operation = new();
        operation.SetupGet(o => o.Name).Returns("Run");
        operation.SetupGet(o => o.Context).Returns(entityContext.Object);
        operation.SetupGet(o => o.State).Returns(entityState.Object);
        operation.SetupGet(o => o.HasInput).Returns(true);
        operation.Setup(o => o.GetInput(runRequest.GetType())).Returns(runRequest);

        object? rawResponse = await entity.RunAsync(operation.Object);
        AgentResponse response = Assert.IsType<AgentResponse>(rawResponse);
        Assert.NotNull(committedState);

        string serializedState = JsonSerializer.Serialize(committedState, stateType, stateJsonOptions);
        return (response, serializedState);
    }

    private static RunRequest CreateRunRequest(string prompt, string correlationId)
    {
        RunRequest request = new(prompt);
        typeof(RunRequest)
            .GetProperty("CorrelationId", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(request, correlationId);
        return request;
    }

    private static object CreateWorkflowInput(string input)
    {
        Type openInputType = typeof(DurableAgentsOptions).Assembly.GetType(
            "Microsoft.Agents.AI.DurableTask.Workflows.DurableWorkflowInput`1",
            throwOnError: true)!;
        Type closedInputType = openInputType.MakeGenericType(typeof(object));
        object workflowInput = Activator.CreateInstance(closedInputType)!;
        closedInputType.GetProperty("Input")!.SetValue(workflowInput, input);
        return workflowInput;
    }

    private sealed class FakeCopilotJsonRpcServer : IAsyncDisposable
    {
        private static readonly Regex s_contentLengthRegex = new(
            @"Content-Length:\s*(\d+)",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _acceptLoopTask;
        private int _messageCounter;

        public FakeCopilotJsonRpcServer()
        {
            this._listener = new TcpListener(IPAddress.Loopback, 0);
            this._listener.Start();
            int port = ((IPEndPoint)this._listener.LocalEndpoint).Port;
            this.Endpoint = $"127.0.0.1:{port}";
            this._acceptLoopTask = Task.Run(() => this.AcceptLoopAsync(this._cts.Token));
        }

        public string Endpoint { get; }

        public List<string> CreatedSessionIds { get; } = [];

        public List<string> ResumedSessionIds { get; } = [];

        public List<string> SentPrompts { get; } = [];

        private async Task AcceptLoopAsync(CancellationToken ct)
        {
            try
            {
                using TcpClient tcp = await this._listener.AcceptTcpClientAsync(ct);
                using NetworkStream stream = tcp.GetStream();
                while (!ct.IsCancellationRequested)
                {
                    string? raw = await ReadRpcMessageAsync(stream, ct);
                    if (raw is null)
                    {
                        break;
                    }

                    using JsonDocument doc = JsonDocument.Parse(raw);
                    JsonElement root = doc.RootElement;
                    if (!root.TryGetProperty("method", out JsonElement methodProp))
                    {
                        continue;
                    }

                    string method = methodProp.GetString()!;
                    long id = root.GetProperty("id").GetInt64();
                    JsonElement @params = root.TryGetProperty("params", out JsonElement p) ? p : default;

                    switch (method)
                    {
                        case "connect":
                            await WriteRpcMessageAsync(
                                stream,
                                JsonSerializer.Serialize(new
                                {
                                    jsonrpc = "2.0",
                                    id,
                                    result = new { ok = true, protocolVersion = 3, version = "1.0.5" },
                                }),
                                ct);
                            break;

                        case "session.create":
                        {
                            string sessionId = @params.GetProperty("sessionId").GetString()!;
                            this.CreatedSessionIds.Add(sessionId);
                            await WriteRpcMessageAsync(
                                stream,
                                JsonSerializer.Serialize(new
                                {
                                    jsonrpc = "2.0",
                                    id,
                                    result = new { sessionId },
                                }),
                                ct);
                            break;
                        }

                        case "session.resume":
                        {
                            string sessionId = @params.GetProperty("sessionId").GetString()!;
                            this.ResumedSessionIds.Add(sessionId);
                            await WriteRpcMessageAsync(
                                stream,
                                JsonSerializer.Serialize(new
                                {
                                    jsonrpc = "2.0",
                                    id,
                                    result = new { sessionId },
                                }),
                                ct);
                            break;
                        }

                        case "session.send":
                        {
                            string sessionId = @params.GetProperty("sessionId").GetString()!;
                            string prompt = @params.GetProperty("prompt").GetString()!;
                            this.SentPrompts.Add(prompt);
                            int msgNum = Interlocked.Increment(ref this._messageCounter);
                            string msgId = $"msg-{msgNum}";
                            string replyText = $"Reply to: {prompt}";

                            await WriteRpcMessageAsync(
                                stream,
                                JsonSerializer.Serialize(new
                                {
                                    jsonrpc = "2.0",
                                    id,
                                    result = new { messageId = msgId },
                                }),
                                ct);

                            await WriteRpcMessageAsync(
                                stream,
                                JsonSerializer.Serialize(new
                                {
                                    jsonrpc = "2.0",
                                    method = "session.event",
                                    @params = new
                                    {
                                        sessionId,
                                        @event = new
                                        {
                                            type = "assistant.message_delta",
                                            id = "00000000-0000-0000-0000-000000000001",
                                            timestamp = "2026-10-24T00:00:00Z",
                                            data = new { messageId = msgId, deltaContent = replyText },
                                        },
                                    },
                                }),
                                ct);

                            await WriteRpcMessageAsync(
                                stream,
                                JsonSerializer.Serialize(new
                                {
                                    jsonrpc = "2.0",
                                    method = "session.event",
                                    @params = new
                                    {
                                        sessionId,
                                        @event = new
                                        {
                                            type = "assistant.message",
                                            id = "00000000-0000-0000-0000-000000000002",
                                            timestamp = "2026-10-24T00:00:01Z",
                                            data = new { messageId = msgId, content = replyText },
                                        },
                                    },
                                }),
                                ct);

                            await WriteRpcMessageAsync(
                                stream,
                                JsonSerializer.Serialize(new
                                {
                                    jsonrpc = "2.0",
                                    method = "session.event",
                                    @params = new
                                    {
                                        sessionId,
                                        @event = new
                                        {
                                            type = "session.idle",
                                            id = "00000000-0000-0000-0000-000000000003",
                                            timestamp = "2026-10-24T00:00:02Z",
                                            data = new { },
                                        },
                                    },
                                }),
                                ct);
                            break;
                        }

                        default:
                            await WriteRpcMessageAsync(
                                stream,
                                JsonSerializer.Serialize(new
                                {
                                    jsonrpc = "2.0",
                                    id,
                                    result = new { },
                                }),
                                ct);
                            break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (IOException)
            {
            }
        }

        private static async Task<string?> ReadRpcMessageAsync(Stream stream, CancellationToken ct)
        {
            List<byte> headerBytes = [];
            byte[] oneByte = new byte[1];
            while (true)
            {
                int read = await stream.ReadAsync(oneByte.AsMemory(0, 1), ct);
                if (read == 0)
                {
                    return null;
                }

                headerBytes.Add(oneByte[0]);
                int count = headerBytes.Count;
                if (count >= 4 &&
                    headerBytes[count - 4] == '\r' &&
                    headerBytes[count - 3] == '\n' &&
                    headerBytes[count - 2] == '\r' &&
                    headerBytes[count - 1] == '\n')
                {
                    break;
                }
            }

            string header = Encoding.ASCII.GetString(headerBytes.ToArray());
            System.Text.RegularExpressions.Match match = s_contentLengthRegex.Match(header);
            int length = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
            byte[] body = new byte[length];
            int offset = 0;
            while (offset < length)
            {
                int read = await stream.ReadAsync(body.AsMemory(offset, length - offset), ct);
                if (read == 0)
                {
                    return null;
                }

                offset += read;
            }

            return Encoding.UTF8.GetString(body);
        }

        private static async Task WriteRpcMessageAsync(Stream stream, string json, CancellationToken ct)
        {
            byte[] body = Encoding.UTF8.GetBytes(json);
            byte[] header = Encoding.ASCII.GetBytes($"Content-Length: {body.Length}\r\n\r\n");
            await stream.WriteAsync(header, ct);
            await stream.WriteAsync(body, ct);
            await stream.FlushAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await this._cts.CancelAsync();
            this._listener.Dispose();
            try
            {
                await this._acceptLoopTask;
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or IOException)
            {
            }

            this._cts.Dispose();
        }
    }
}
