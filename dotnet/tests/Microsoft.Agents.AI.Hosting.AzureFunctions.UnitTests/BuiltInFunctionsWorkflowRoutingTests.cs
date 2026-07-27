// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Specialized;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
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
    public void TryGetWaitTimeout_ValidatesSeconds(string? value, bool expectedSuccess, int expectedSeconds)
    {
        HttpRequestData request = CreateRequest(timeoutSeconds: value);

        bool success = BuiltInFunctions.TryGetWaitTimeout(request, out TimeSpan timeout, out string? error);

        Assert.Equal(expectedSuccess, success);
        Assert.Equal(expectedSeconds, timeout.TotalSeconds);
        Assert.Equal(expectedSuccess, error is null);
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
}
