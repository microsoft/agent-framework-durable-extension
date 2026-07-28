// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Specialized;
using System.Net;
using System.Text.Json;
using Azure.Core.Serialization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Options;
using Moq;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests;

public sealed class BuiltInFunctionsWorkflowRoutingTests
{
    [Theory]
    [InlineData("http-MyWorkflow-status", "-status", "MyWorkflow")]
    [InlineData("http-OrderProcessor-status", "-status", "OrderProcessor")]
    [InlineData("http-MyWorkflow-respond", "-respond", "MyWorkflow")]
    [InlineData("http-Multi-Dash-Name-status", "-status", "Multi-Dash-Name")]
    public void GetWorkflowName_ReturnsCorrectName(string functionName, string suffix, string expectedWorkflowName)
    {
        // Act
        string result = BuiltInFunctions.GetWorkflowName(functionName, suffix);

        // Assert
        Assert.Equal(expectedWorkflowName, result);
    }

    [Theory]
    [InlineData("invalid-name", "-status")]
    [InlineData("http-MyWorkflow-respond", "-status")] // wrong suffix
    [InlineData("mcptool-MyWorkflow-status", "-status")] // wrong prefix
    public void GetWorkflowName_ThrowsForInvalidPattern(string functionName, string suffix)
    {
        // Act & Assert
        Assert.Throws<InvalidOperationException>(() =>
            BuiltInFunctions.GetWorkflowName(functionName, suffix));
    }

    [Theory]
    [InlineData("dafx-MyWorkflow", "http-MyWorkflow-status", "-status", true)]
    [InlineData("dafx-MyWorkflow", "http-MyWorkflow-respond", "-respond", true)]
    [InlineData("dafx-myworkflow", "http-MyWorkflow-status", "-status", true)] // case-insensitive
    [InlineData("dafx-MYWORKFLOW", "http-MyWorkflow-respond", "-respond", true)] // case-insensitive
    [InlineData("dafx-OtherWorkflow", "http-MyWorkflow-status", "-status", false)] // cross-workflow
    [InlineData("dafx-PrivilegedWorkflow", "http-PublicWorkflow-status", "-status", false)] // attack scenario
    [InlineData("dafx-PrivilegedWorkflow", "http-PublicWorkflow-respond", "-respond", false)] // attack scenario
    public void IsOrchestrationOwnedByWorkflow_ValidatesCorrectly(
        string orchestrationName,
        string functionName,
        string suffix,
        bool expectedResult)
    {
        // Act
        bool result = BuiltInFunctions.IsOrchestrationOwnedByWorkflow(orchestrationName, functionName, suffix);

        // Assert
        Assert.Equal(expectedResult, result);
    }

    [Theory]
    [InlineData(null, null, false, false)]
    [InlineData(null, "true", false, true)]
    [InlineData("false", "true", true, false)]
    [InlineData("invalid", "true", false, true)]
    [InlineData("1", "false", false, true)]
    [InlineData("0", "true", true, false)]
    [InlineData(null, "yes", false, true)]
    [InlineData(null, "off", true, false)]
    public void ShouldWaitForResponse_UsesHeaderThenQueryThenDefault(
        string? headerValue,
        string? queryValue,
        bool defaultValue,
        bool expected)
    {
        HttpRequestData request = CreateRequest(headerValue, queryValue);

        bool result = BuiltInFunctions.ShouldWaitForResponse(request, "waitForResponse", defaultValue);

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("waitForResponse", "wait_for_response")] // workflow endpoints use camelCase
    [InlineData("wait_for_response", "waitForResponse")] // agent endpoints use snake_case
    public void ShouldWaitForResponse_OnlyHonorsTheParameterForItsSurface(string parameterName, string otherName)
    {
        HttpRequestData matching = CreateRequest(waitForResponse: "true", waitForResponseParameterName: parameterName);
        HttpRequestData mismatched = CreateRequest(waitForResponse: "true", waitForResponseParameterName: otherName);

        Assert.True(BuiltInFunctions.ShouldWaitForResponse(matching, parameterName, defaultValue: false));
        Assert.False(BuiltInFunctions.ShouldWaitForResponse(mismatched, parameterName, defaultValue: false));
    }

    [Theory]
    [InlineData(null, true, 10)]
    [InlineData("1", true, 1)]
    [InlineData("230", true, 230)]
    [InlineData("0", false, 0)]
    [InlineData("231", false, 0)]
    [InlineData("invalid", false, 0)]
    [InlineData("", false, 0)]
    [InlineData(" ", false, 0)]
    public void TryGetWaitTimeout_ValidatesSeconds(string? value, bool expectedSuccess, int expectedSeconds)
    {
        HttpRequestData request = CreateRequest(timeoutSeconds: value);

        bool success = BuiltInFunctions.TryGetWaitTimeout(request, out TimeSpan timeout, out string? error);

        Assert.Equal(expectedSuccess, success);
        Assert.Equal(expectedSeconds, timeout.TotalSeconds);
        Assert.Equal(expectedSuccess, error is null);
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData("application/json", true)]
    [InlineData("application/*", true)]
    [InlineData("*/*", true)]
    [InlineData("text/plain", false)]
    [InlineData("text/*", false)]
    [InlineData("text/plain, application/json", true)]
    [InlineData("text/plain, application/json;q=0", false)]
    [InlineData("text/plain;q=0", true)]
    [InlineData("image/png", true)]
    public void ShouldReturnWorkflowJson_DefaultsToJsonUnlessOnlyTextIsAccepted(
        string? accept,
        bool expected)
    {
        (HttpRequestData request, _, _) = CreateCompletionRequest(CancellationToken.None, accept);

        Assert.Equal(expected, BuiltInFunctions.ShouldReturnWorkflowJson(request));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("text/plain")]
    public async Task WaitForWorkflowCompletionAsync_Timeout_ReturnsAsyncWorkflowResponseAsync(string? accept)
    {
        const string WorkflowName = "TestWorkflow";
        const string InstanceId = "workflow-123";
        (HttpRequestData asyncRequest, _, FunctionContext asyncContext) =
            CreateCompletionRequest(CancellationToken.None, accept);
        HttpResponseData expectedResponse = await BuiltInFunctions.CreateWorkflowAcceptedResponseAsync(
            asyncRequest, WorkflowName, InstanceId, asyncContext.CancellationToken);
        (HttpRequestData request, _, FunctionContext context) =
            CreateCompletionRequest(CancellationToken.None, accept);
        Mock<DurableTaskClient> client = CreateWaitingClient(InstanceId);

        HttpResponseData response = await BuiltInFunctions.WaitForWorkflowCompletionAsync(
            request,
            client.Object,
            context,
            WorkflowName,
            InstanceId,
            TimeSpan.Zero);

        Assert.Equal(expectedResponse.StatusCode, response.StatusCode);
        Assert.Equal(GetResponseBody(expectedResponse), GetResponseBody(response));
        Assert.Equal(
            expectedResponse.Headers.Select(header => (header.Key, string.Join(",", header.Value))),
            response.Headers.Select(header => (header.Key, string.Join(",", header.Value))));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("text/plain")]
    public async Task CreateWorkflowAcceptedResponseAsync_HonorsContentNegotiationAsync(string? accept)
    {
        const string WorkflowName = "TestWorkflow";
        const string InstanceId = "workflow-123";
        (HttpRequestData request, _, FunctionContext context) =
            CreateCompletionRequest(CancellationToken.None, accept);

        HttpResponseData response = await BuiltInFunctions.CreateWorkflowAcceptedResponseAsync(
            request, WorkflowName, InstanceId, context.CancellationToken);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        if (accept is null)
        {
            AssertJsonContentType(response);
            using JsonDocument body = JsonDocument.Parse(GetResponseBody(response));
            Assert.Equal(InstanceId, body.RootElement.GetProperty("runId").GetString());
            Assert.Equal(
                $"Workflow orchestration started for {WorkflowName}.",
                body.RootElement.GetProperty("message").GetString());
        }
        else
        {
            Assert.Equal(
                $"Workflow orchestration started for {WorkflowName}. Orchestration runId: {InstanceId}",
                GetResponseBody(response));
            AssertTextContentType(response);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("text/plain")]
    public async Task WaitForWorkflowCompletionAsync_Completed_HonorsContentNegotiationAsync(string? accept)
    {
        const string InstanceId = "workflow-123";
        const string Result = "Workflow completed.";
        (HttpRequestData request, _, FunctionContext context) =
            CreateCompletionRequest(CancellationToken.None, accept);
        OrchestrationMetadata metadata = new("dafx-TestWorkflow", InstanceId)
        {
            RuntimeStatus = OrchestrationRuntimeStatus.Completed,
            DataConverter = Microsoft.DurableTask.Converters.JsonDataConverter.Default,
            SerializedOutput = JsonSerializer.Serialize(new { Result }),
        };
        Mock<DurableTaskClient> client = CreateCompletedClient(InstanceId, metadata);

        HttpResponseData response = await BuiltInFunctions.WaitForWorkflowCompletionAsync(
            request,
            client.Object,
            context,
            "TestWorkflow",
            InstanceId,
            TimeSpan.FromSeconds(1));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        if (accept is null)
        {
            AssertJsonContentType(response);
            using JsonDocument body = JsonDocument.Parse(GetResponseBody(response));
            Assert.Equal(InstanceId, body.RootElement.GetProperty("runId").GetString());
            Assert.Equal("Completed", body.RootElement.GetProperty("workflowStatus").GetString());
            Assert.Equal(Result, body.RootElement.GetProperty("result").GetString());
        }
        else
        {
            Assert.Equal(Result, GetResponseBody(response));
            AssertTextContentType(response);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("text/plain")]
    public async Task WaitForWorkflowCompletionAsync_Failed_HonorsContentNegotiationAsync(string? accept)
    {
        const string InstanceId = "workflow-123";
        (HttpRequestData request, _, FunctionContext context) =
            CreateCompletionRequest(CancellationToken.None, accept);
        OrchestrationMetadata metadata = new("dafx-TestWorkflow", InstanceId)
        {
            RuntimeStatus = OrchestrationRuntimeStatus.Failed,
        };
        Mock<DurableTaskClient> client = CreateCompletedClient(InstanceId, metadata);

        HttpResponseData response = await BuiltInFunctions.WaitForWorkflowCompletionAsync(
            request,
            client.Object,
            context,
            "TestWorkflow",
            InstanceId,
            TimeSpan.FromSeconds(1));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        if (accept is null)
        {
            AssertJsonContentType(response);
            using JsonDocument body = JsonDocument.Parse(GetResponseBody(response));
            Assert.Equal(InstanceId, body.RootElement.GetProperty("runId").GetString());
            Assert.Equal("Failed", body.RootElement.GetProperty("workflowStatus").GetString());
            Assert.Equal("Unknown error", body.RootElement.GetProperty("error").GetString());
        }
        else
        {
            Assert.Equal("Unknown error", GetResponseBody(response));
            AssertTextContentType(response);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("text/plain")]
    public async Task RunWorkflowOrchestrationHttpTriggerAsync_ValidationError_HonorsContentNegotiationAsync(
        string? accept)
    {
        (HttpRequestData request, _, FunctionContext context) =
            CreateCompletionRequest(CancellationToken.None, accept);
        Mock.Get(request).SetupGet(r => r.Body).Returns(new MemoryStream());
        Mock<FunctionDefinition> functionDefinition = new();
        functionDefinition.SetupGet(d => d.Name).Returns("http-TestWorkflow");
        Mock.Get(context).SetupGet(c => c.FunctionDefinition).Returns(functionDefinition.Object);
        Mock<DurableTaskClient> client = new("test");

        HttpResponseData response = await BuiltInFunctions.RunWorkflowOrchestrationHttpTriggerAsync(
            request, client.Object, context);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        if (accept is null)
        {
            AssertJsonContentType(response);
            using JsonDocument body = JsonDocument.Parse(GetResponseBody(response));
            Assert.Equal(400, body.RootElement.GetProperty("status").GetInt32());
            Assert.Equal("Workflow input cannot be empty.", body.RootElement.GetProperty("error").GetString());
        }
        else
        {
            Assert.Equal("Workflow input cannot be empty.", GetResponseBody(response));
            AssertTextContentType(response);
        }
    }

    [Fact]
    public async Task WaitForWorkflowCompletionAsync_CallerCancellation_PropagatesAsync()
    {
        const string WorkflowName = "TestWorkflow";
        const string InstanceId = "workflow-123";
        using CancellationTokenSource callerCancellation = new();
        callerCancellation.Cancel();
        HttpRequestData request = CreateRequest();
        Mock<FunctionContext> context = new();
        context.SetupGet(c => c.CancellationToken).Returns(callerCancellation.Token);
        Mock<DurableTaskClient> client = CreateWaitingClient(InstanceId);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            BuiltInFunctions.WaitForWorkflowCompletionAsync(
                request,
                client.Object,
                context.Object,
                WorkflowName,
                InstanceId,
                TimeSpan.FromSeconds(30)));
    }

    private static HttpRequestData CreateRequest(
        string? headerValue = null,
        string? waitForResponse = null,
        string? timeoutSeconds = null,
        string waitForResponseParameterName = "waitForResponse")
    {
        HttpHeadersCollection headers = new();
        if (headerValue is not null)
        {
            headers.Add("x-ms-wait-for-response", headerValue);
        }

        NameValueCollection query = new();
        if (waitForResponse is not null)
        {
            query.Add(waitForResponseParameterName, waitForResponse);
        }

        if (timeoutSeconds is not null)
        {
            query.Add("timeoutSeconds", timeoutSeconds);
        }

        Mock<HttpRequestData> request = new(MockBehavior.Strict, Mock.Of<FunctionContext>());
        request.SetupGet(r => r.Headers).Returns(headers);
        request.SetupGet(r => r.Query).Returns(query);
        return request.Object;
    }

    private static Mock<DurableTaskClient> CreateWaitingClient(string instanceId)
    {
        Mock<DurableTaskClient> client = new("test");
        client.Setup(c => c.WaitForInstanceCompletionAsync(instanceId, true, It.IsAny<CancellationToken>()))
            .Returns(async (string _, bool _, CancellationToken cancellation) =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellation);
                throw new InvalidOperationException("The infinite wait completed without cancellation.");
            });
        return client;
    }

    private static Mock<DurableTaskClient> CreateCompletedClient(
        string instanceId,
        OrchestrationMetadata metadata)
    {
        Mock<DurableTaskClient> client = new("test");
        client.Setup(c => c.WaitForInstanceCompletionAsync(instanceId, true, It.IsAny<CancellationToken>()))
            .ReturnsAsync(metadata);
        return client;
    }

    private static (HttpRequestData Request, HttpResponseData Response, FunctionContext Context) CreateCompletionRequest(
        CancellationToken cancellationToken,
        string? accept = null)
    {
        Mock<ObjectSerializer> serializer = new();
        serializer
            .Setup(s => s.SerializeAsync(
                It.IsAny<Stream>(),
                It.IsAny<object?>(),
                It.IsAny<Type>(),
                It.IsAny<CancellationToken>()))
            .Returns<Stream, object?, Type, CancellationToken>(async (stream, value, type, token) =>
                await JsonSerializer.SerializeAsync(stream, value, type, cancellationToken: token));

        WorkerOptions workerOptions = new() { Serializer = serializer.Object };
        Mock<IOptions<WorkerOptions>> options = new();
        options.SetupGet(o => o.Value).Returns(workerOptions);
        Mock<IServiceProvider> services = new();
        services.Setup(s => s.GetService(typeof(IOptions<WorkerOptions>))).Returns(options.Object);

        Mock<FunctionContext> context = new();
        context.SetupGet(c => c.CancellationToken).Returns(cancellationToken);
        context.SetupGet(c => c.InstanceServices).Returns(services.Object);

        Mock<HttpResponseData> response = new(context.Object);
        response.SetupProperty(r => r.StatusCode, HttpStatusCode.OK);
        response.SetupProperty(r => r.Body, new MemoryStream());
        response.SetupGet(r => r.Headers).Returns(new HttpHeadersCollection());

        HttpHeadersCollection requestHeaders = new();
        if (accept is not null)
        {
            requestHeaders.Add("Accept", accept);
        }

        Mock<HttpRequestData> request = new(context.Object);
        request.SetupGet(r => r.Headers).Returns(requestHeaders);
        request.Setup(r => r.CreateResponse()).Returns(response.Object);

        return (request.Object, response.Object, context.Object);
    }

    private static string GetResponseBody(HttpResponseData response)
    {
        return System.Text.Encoding.UTF8.GetString(((MemoryStream)response.Body).ToArray());
    }

    private static void AssertJsonContentType(HttpResponseData response)
    {
        Assert.True(response.Headers.TryGetValues("Content-Type", out IEnumerable<string>? contentTypes));
        Assert.Contains("application/json", Assert.Single(contentTypes), StringComparison.OrdinalIgnoreCase);
    }

    private static void AssertTextContentType(HttpResponseData response)
    {
        Assert.True(response.Headers.TryGetValues("Content-Type", out IEnumerable<string>? contentTypes));
        Assert.Contains("text/plain", Assert.Single(contentTypes), StringComparison.OrdinalIgnoreCase);
    }
}
