// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit.State;

/// <summary>
/// Regression tests for function call arguments whose values are not plain JSON.
/// See https://github.com/microsoft/agent-framework-durable-extension/issues/33.
/// </summary>
public sealed class DurableAgentStateFunctionCallContentTests
{
    private static readonly JsonTypeInfo s_stateContentTypeInfo =
        DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateContent))!;

    private static FunctionCallContent RoundTrip(FunctionCallContent content)
    {
        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(content);
        string json = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? deserialized =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(json, s_stateContentTypeInfo);

        Assert.NotNull(deserialized);
        return Assert.IsType<FunctionCallContent>(deserialized.ToAIContent());
    }

    [Fact]
    public void JsonElementArgumentsRoundTrip()
    {
        // Chat clients parse model supplied arguments into JsonElement values.
        JsonElement city = JsonSerializer.SerializeToElement("Seattle");
        FunctionCallContent result = RoundTrip(new("call-1", "get_weather", new Dictionary<string, object?>
        {
            ["city"] = city
        }));

        Assert.Equal("call-1", result.CallId);
        Assert.Equal("get_weather", result.Name);
        Assert.NotNull(result.Arguments);
        Assert.Equal("Seattle", Assert.IsType<JsonElement>(result.Arguments["city"]).GetString());
    }

    [Fact]
    public void ObjectArgumentRoundTrips()
    {
        // Callers can supply function calls containing arbitrary objects, for example when replaying
        // history or resuming a function approval.
        FunctionCallContent result = RoundTrip(new("call-2", "get_weather", new Dictionary<string, object?>
        {
            ["location"] = new Location("Seattle", "WA")
        }));

        JsonElement location = Assert.IsType<JsonElement>(result.Arguments!["location"]);
        Assert.Equal("Seattle", location.GetProperty("city").GetString());
        Assert.Equal("WA", location.GetProperty("state").GetString());
    }

    [Fact]
    public void BclValueArgumentRoundTrips()
    {
        // DateOnly is not registered on DurableAgentStateJsonContext, so an object typed argument
        // holding one used to fail serialization outright.
        FunctionCallContent result = RoundTrip(new("call-3", "get_forecast", new Dictionary<string, object?>
        {
            ["date"] = new DateOnly(2026, 1, 31)
        }));

        Assert.Equal("2026-01-31", Assert.IsType<JsonElement>(result.Arguments!["date"]).GetString());
    }

    [Fact]
    public void NullArgumentRoundTrips()
    {
        FunctionCallContent result = RoundTrip(new("call-4", "get_weather", new Dictionary<string, object?>
        {
            ["city"] = null
        }));

        Assert.Equal(JsonValueKind.Null, Assert.IsType<JsonElement>(result.Arguments!["city"]).ValueKind);
    }

    [Fact]
    public void OmittedAndEmptyArgumentsRemainDistinct()
    {
        FunctionCallContent omitted = RoundTrip(new("call-5", "get_time", arguments: null));
        const string EmptyArgumentsJson =
            """{"$type":"functionCall","arguments":{},"callId":"call-6","name":"get_time"}""";
        DurableAgentStateContent emptyStored = Assert.IsType<DurableAgentStateFunctionCallContent>(
            JsonSerializer.Deserialize(EmptyArgumentsJson, s_stateContentTypeInfo));
        FunctionCallContent empty = Assert.IsType<FunctionCallContent>(emptyStored.ToAIContent());

        Assert.Null(omitted.Arguments);
        Assert.NotNull(empty.Arguments);
        Assert.Empty(empty.Arguments);
    }

    [Fact]
    public void PreviouslyPersistedArgumentsAreStillReadable()
    {
        // State written before arguments were normalized stores the raw JSON under "arguments".
        const string LegacyJson =
            """{"$type":"functionCall","arguments":{"city":"Seattle","days":3},"callId":"call-6","name":"get_forecast"}""";

        DurableAgentStateContent? deserialized =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(LegacyJson, s_stateContentTypeInfo);

        Assert.NotNull(deserialized);
        FunctionCallContent result = Assert.IsType<FunctionCallContent>(deserialized.ToAIContent());

        Assert.Equal("call-6", result.CallId);
        Assert.Equal("Seattle", Assert.IsType<JsonElement>(result.Arguments!["city"]).GetString());
        Assert.Equal(3, Assert.IsType<JsonElement>(result.Arguments["days"]).GetInt32());
    }

    [Fact]
    public void StringArgumentsRoundTripVerbatimWithoutParsing()
    {
        const string Json =
            """{"$type":"functionCall","arguments":" { \"partial\": ","callId":"call-7","name":"incomplete"}""";

        DurableAgentStateContent? deserialized =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(Json, s_stateContentTypeInfo);
        DurableAgentStateFunctionCallContent durable =
            Assert.IsType<DurableAgentStateFunctionCallContent>(deserialized);
        string roundTrip = JsonSerializer.Serialize(durable, s_stateContentTypeInfo);
        using JsonDocument roundTripDocument = JsonDocument.Parse(roundTrip);
        FunctionCallContent runtime = Assert.IsType<FunctionCallContent>(durable.ToAIContent());

        Assert.Equal(" { \"partial\": ", durable.Arguments.GetString());
        Assert.Equal(" { \"partial\": ", runtime.RawRepresentation);
        Assert.Equal(
            " { \"partial\": ",
            roundTripDocument.RootElement.GetProperty("arguments").GetString());
    }

    [Theory]
    [InlineData("verbatim")]
    [InlineData(" { \"partial\": ")]
    public void ProductionMappingsPreserveRawStringArguments(string rawArguments)
    {
        FunctionCallContent runtime = new("call-8", "future")
        {
            RawRepresentation = rawArguments,
        };

        DurableAgentStateFunctionCallContent legacy =
            Assert.IsType<DurableAgentStateFunctionCallContent>(
                DurableAgentStateContent.FromAIContent(runtime));
        DurableAgentStateFunctionCallContent revised =
            Assert.IsType<DurableAgentStateFunctionCallContent>(
                DurableAgentStateContent.FromAIContentV2(runtime));

        Assert.Equal(rawArguments, legacy.Arguments.GetString());
        Assert.Equal(rawArguments, revised.Arguments.GetString());
        Assert.Equal(
            rawArguments,
            Assert.IsType<FunctionCallContent>(legacy.ToAIContent()).RawRepresentation);
        Assert.Equal(
            rawArguments,
            Assert.IsType<FunctionCallContent>(revised.ToAIContent()).RawRepresentation);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void LegacyRequestAndResponsePreserveRawStringArgumentsAcrossRuntime(bool request)
    {
        const string RawArguments = " { \"partial\": ";
        ChatMessage message = new(
            ChatRole.Assistant,
            [new FunctionCallContent("call-9", "future") { RawRepresentation = RawArguments }])
        {
            CreatedAt = DateTimeOffset.Parse("2026-10-02T10:00:00Z"),
        };

        DurableAgentStateMessage rewrittenMessage;
        if (request)
        {
            DurableAgentStateRequest stored =
                DurableAgentStateRequest.FromRunRequest(new RunRequest([message]));
            ChatMessage runtime = Assert.Single(stored.Messages).ToChatMessage();
            rewrittenMessage = Assert.Single(
                DurableAgentStateRequest.FromRunRequest(new RunRequest([runtime])).Messages);
        }
        else
        {
            DurableAgentStateResponse stored =
                DurableAgentStateResponse.FromResponse("correlation", new AgentResponse([message]));
            ChatMessage runtime = Assert.Single(stored.ToResponse().Messages);
            rewrittenMessage = Assert.Single(
                DurableAgentStateResponse.FromResponse(
                    "correlation",
                    new AgentResponse([runtime])).Messages);
        }

        DurableAgentStateFunctionCallContent rewritten =
            Assert.IsType<DurableAgentStateFunctionCallContent>(
                Assert.Single(rewrittenMessage.Contents));
        Assert.Equal(RawArguments, rewritten.Arguments.GetString());
    }

    private sealed record Location(string City, string State);
}
