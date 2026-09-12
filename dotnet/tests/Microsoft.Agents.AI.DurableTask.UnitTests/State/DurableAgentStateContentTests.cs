// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit.State;

public sealed class DurableAgentStateContentTests
{
    private static readonly JsonTypeInfo s_stateContentTypeInfo =
        DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateContent))!;

    [Fact]
    public void ErrorContentSerializationDeserialization()
    {
        // Arrange
        ErrorContent errorContent = new("message")
        {
            Details = "details",
            ErrorCode = "code"
        };

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(errorContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        ErrorContent convertedErrorContent = Assert.IsType<ErrorContent>(convertedContent);

        Assert.Equal(errorContent.Message, convertedErrorContent.Message);
        Assert.Equal(errorContent.Details, convertedErrorContent.Details);
        Assert.Equal(errorContent.ErrorCode, convertedErrorContent.ErrorCode);
    }

    [Fact]
    public void ErrorContentPreservesNonStringPythonDetails()
    {
        const string Json = """
            {
              "$type": "error",
              "message": "failed",
              "details": {
                "retryable": true
              }
            }
            """;
        DurableAgentStateContent stored = Assert.IsType<DurableAgentStateErrorContent>(
            JsonSerializer.Deserialize(Json, s_stateContentTypeInfo));

        ErrorContent restored = Assert.IsType<ErrorContent>(stored.ToAIContent());
        string roundTrip = JsonSerializer.Serialize(stored, s_stateContentTypeInfo);

        using JsonDocument details = JsonDocument.Parse(restored.Details!);
        Assert.True(details.RootElement.GetProperty("retryable").GetBoolean());
        Assert.Contains("\"details\":{\"retryable\":true}", roundTrip, StringComparison.Ordinal);
    }

    [Fact]
    public void TextContentSerializationDeserialization()
    {
        // Arrange
        TextContent textContent = new("Hello, world!");

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(textContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        TextContent convertedTextContent = Assert.IsType<TextContent>(convertedContent);

        Assert.Equal(textContent.Text, convertedTextContent.Text);
    }

    [Fact]
    public void FunctionCallContentSerializationDeserialization()
    {
        // Arrange
        FunctionCallContent functionCallContent = new(
            "call-123",
            "MyFunction",
            new Dictionary<string, object?>
            {
                { "param1", 42 },
                { "param2", "value" }
            });

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(functionCallContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        FunctionCallContent convertedFunctionCallContent = Assert.IsType<FunctionCallContent>(convertedContent);

        Assert.Equal(functionCallContent.CallId, convertedFunctionCallContent.CallId);
        Assert.Equal(functionCallContent.Name, convertedFunctionCallContent.Name);

        Assert.NotNull(functionCallContent.Arguments);
        Assert.NotNull(convertedFunctionCallContent.Arguments);
        Assert.Equal(functionCallContent.Arguments.Keys.Order(), convertedFunctionCallContent.Arguments.Keys.Order());

        // NOTE: Deserialized dictionaries will have JSON element values rather than the original native types,
        // so we only check the keys here.
        foreach (string key in functionCallContent.Arguments.Keys)
        {
            Assert.Equal(
                JsonSerializer.Serialize(functionCallContent.Arguments[key]),
                JsonSerializer.Serialize(convertedFunctionCallContent.Arguments[key]));
        }
    }

    [Fact]
    public void FunctionResultContentSerializationDeserialization()
    {
        // Arrange
        FunctionResultContent functionResultContent = new("call-123", "return value");

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(functionResultContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        FunctionResultContent convertedFunctionResultContent = Assert.IsType<FunctionResultContent>(convertedContent);

        Assert.Equal(functionResultContent.CallId, convertedFunctionResultContent.CallId);
        // NOTE: We serialize both results to JSON for comparison since deserialized objects will be
        // JSON elements rather than the original native types.
        Assert.Equal(
            JsonSerializer.Serialize(functionResultContent.Result),
            JsonSerializer.Serialize(convertedFunctionResultContent.Result));
    }

    [Theory]
    [InlineData("data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==", null)] // Valid data URI containing media type; pass null for separate mediaType parameter.
    [InlineData("data:;base64,SGVsbG8sIFdvcmxkIQ==", "text/plain")] // Valid data URI without media type; pass media
    public void DataContentSerializationDeserialization(string dataUri, string? mediaType)
    {
        // Arrange
        DataContent dataContent = new(dataUri, mediaType);

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(dataContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        DataContent convertedDataContent = Assert.IsType<DataContent>(convertedContent);

        Assert.Equal(dataContent.Uri, convertedDataContent.Uri);
        Assert.Equal(dataContent.MediaType, convertedDataContent.MediaType);
    }

    [Fact]
    public void HostedFileContentSerializationDeserialization()
    {
        // Arrange
        HostedFileContent hostedFileContent = new("file-123");

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(hostedFileContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        HostedFileContent convertedHostedFileContent = Assert.IsType<HostedFileContent>(convertedContent);

        Assert.Equal(hostedFileContent.FileId, convertedHostedFileContent.FileId);
    }

    [Fact]
    public void HostedVectorStoreContentSerializationDeserialization()
    {
        // Arrange
        HostedVectorStoreContent hostedVectorStoreContent = new("vs-123");

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(hostedVectorStoreContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        HostedVectorStoreContent convertedHostedVectorStoreContent = Assert.IsType<HostedVectorStoreContent>(convertedContent);

        Assert.Equal(hostedVectorStoreContent.VectorStoreId, convertedHostedVectorStoreContent.VectorStoreId);
    }

    [Fact]
    public void TextReasoningContentSerializationDeserialization()
    {
        // Arrange
        TextReasoningContent textReasoningContent = new("Reasoning chain...");

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(textReasoningContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        TextReasoningContent convertedTextReasoningContent = Assert.IsType<TextReasoningContent>(convertedContent);

        Assert.Equal(textReasoningContent.Text, convertedTextReasoningContent.Text);
    }

    [Fact]
    public void UriContentSerializationDeserialization()
    {
        // Arrange
        UriContent uriContent = new(new Uri("https://example.com"), "text/html");

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(uriContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        UriContent convertedUriContent = Assert.IsType<UriContent>(convertedContent);

        Assert.Equal(uriContent.Uri, convertedUriContent.Uri);
        Assert.Equal(uriContent.MediaType, convertedUriContent.MediaType);
    }

    [Fact]
    public void UsageContentSerializationDeserialization()
    {
        // Arrange
        UsageDetails usageDetails = new()
        {
            InputTokenCount = 10,
            OutputTokenCount = 5,
            TotalTokenCount = 15
        };

        UsageContent usageContent = new(usageDetails);

        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(usageContent);

        // Act
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        // Assert
        Assert.NotNull(convertedJsonContent);

        AIContent convertedContent = convertedJsonContent.ToAIContent();

        UsageContent convertedUsageContent = Assert.IsType<UsageContent>(convertedContent);

        Assert.NotNull(convertedUsageContent.Details);
        Assert.Equal(usageDetails.InputTokenCount, convertedUsageContent.Details.InputTokenCount);
        Assert.Equal(usageDetails.OutputTokenCount, convertedUsageContent.Details.OutputTokenCount);
        Assert.Equal(usageDetails.TotalTokenCount, convertedUsageContent.Details.TotalTokenCount);
    }

    [Fact]
    public void UsageAdditionalCountsRoundTripThroughExtensionData()
    {
        UsageDetails usageDetails = new()
        {
            InputTokenCount = 10,
            AdditionalCounts = new AdditionalPropertiesDictionary<long>
            {
                ["providerCount"] = 7,
            },
        };

        DurableAgentStateUsage stored = Assert.IsType<DurableAgentStateUsage>(
            DurableAgentStateUsage.FromUsage(usageDetails));
        string json = JsonSerializer.Serialize(
            stored,
            DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateUsage))!);
        DurableAgentStateUsage restored = Assert.IsType<DurableAgentStateUsage>(
            JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateUsage))!));
        UsageDetails converted = restored.ToUsageDetails();

        Assert.Contains("\"extensionData\":{\"providerCount\":7}", json, StringComparison.Ordinal);
        Assert.Equal(7, converted.AdditionalCounts?["providerCount"]);
    }

    [Fact]
    public void UsageProjectionIgnoresMalformedExtensionsAndPreservesTheirJson()
    {
        const string Json = """
            {
              "inputTokenCount": 10,
              "extensionData": {
                "providerCount": 7,
                "futureString": "seven",
                "futureObject": { "count": 8 },
                "futureArray": [9],
                "fractional": 1.5,
                "tooLarge": 9223372036854775808
              },
              "futureTopLevelCount": 11,
              "futureTopLevelObject": { "count": 12 }
            }
            """;
        JsonTypeInfo usageTypeInfo =
            DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateUsage))!;
        DurableAgentStateUsage stored = Assert.IsType<DurableAgentStateUsage>(
            JsonSerializer.Deserialize(Json, usageTypeInfo));

        UsageDetails usage = stored.ToUsageDetails();
        string roundTrip = JsonSerializer.Serialize(stored, usageTypeInfo);

        Assert.Equal(10, usage.InputTokenCount);
        Assert.Equal(7, usage.AdditionalCounts?["providerCount"]);
        Assert.Equal(11, usage.AdditionalCounts?["futureTopLevelCount"]);
        Assert.DoesNotContain("futureString", usage.AdditionalCounts?.Keys ?? []);
        Assert.DoesNotContain("futureObject", usage.AdditionalCounts?.Keys ?? []);
        Assert.DoesNotContain("futureArray", usage.AdditionalCounts?.Keys ?? []);
        Assert.DoesNotContain("fractional", usage.AdditionalCounts?.Keys ?? []);
        Assert.DoesNotContain("tooLarge", usage.AdditionalCounts?.Keys ?? []);
        Assert.DoesNotContain("futureTopLevelObject", usage.AdditionalCounts?.Keys ?? []);
        Assert.Contains("\"futureString\":\"seven\"", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureObject\":{\"count\":8}", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureArray\":[9]", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureTopLevelObject\":{\"count\":12}", roundTrip, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\"ten\"")]
    [InlineData("{}")]
    [InlineData("1.5")]
    [InlineData("9223372036854775808")]
    public void UsageDeserializationRejectsMalformedKnownNumericFields(string invalidValue)
    {
        string json = $$"""
            {
              "inputTokenCount": {{invalidValue}}
            }
            """;
        JsonTypeInfo usageTypeInfo =
            DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateUsage))!;

        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize(json, usageTypeInfo));
    }

    [Fact]
    public void KnownContentDiscriminatorDoesNotUseUnknownEnvelope()
    {
        TextContent originalContent = new("Some unknown content");
        DurableAgentStateContent durableContent =
            DurableAgentStateContent.FromAIContent(originalContent);
        string jsonContent = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);
        DurableAgentStateContent? convertedJsonContent =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(jsonContent, s_stateContentTypeInfo);

        DurableAgentStateTextContent convertedState =
            Assert.IsType<DurableAgentStateTextContent>(convertedJsonContent);
        AIContent convertedContent = convertedState.ToAIContent();
        TextContent convertedTextContent = Assert.IsType<TextContent>(convertedContent);

        Assert.Equal(originalContent.Text, convertedTextContent.Text);
        Assert.Contains("\"$type\":\"text\"", jsonContent, StringComparison.Ordinal);
        Assert.DoesNotContain("$microsoftAgentFrameworkDurableTask", jsonContent, StringComparison.Ordinal);
        Assert.DoesNotContain("$runtimeType", jsonContent, StringComparison.Ordinal);
    }

    [Fact]
    public void UnknownContentWithUnrecognizedPayloadFallsBackWithoutDataLoss()
    {
        DurableAgentStateUnknownContent stored = new()
        {
            Content = JsonSerializer.SerializeToElement(
                new { type = "future_content", value = 42 }),
        };

        AIContent restored = stored.ToAIContent();

        JsonElement content =
            Assert.IsType<JsonElement>(restored.AdditionalProperties?["content"]);
        Assert.Equal("future_content", content.GetProperty("type").GetString());
        Assert.Equal(42, content.GetProperty("value").GetInt32());
    }

    [Fact]
    public void PythonShapedOpaqueUnknownContentWithRuntimeTypeRoundTripsUnchanged()
    {
        using JsonDocument document = JsonDocument.Parse(
            """
            {
              "$runtimeType": "producer-owned-user-value",
              "type": "future_python_content",
              "annotations": [{ "kind": "citation", "value": "python-ref" }],
              "additionalProperties": { "producer": "python" },
              "future": { "nested": [1, 2, 3] }
            }
            """);
        JsonElement original = document.RootElement.Clone();
        DurableAgentStateUnknownContent stored = new() { Content = original };

        AIContent restored = Assert.IsType<AIContent>(stored.ToAIContent());
        DurableAgentStateUnknownContent roundTripped = Assert.IsType<DurableAgentStateUnknownContent>(
            DurableAgentStateContent.FromAIContent(restored));

        Assert.True(JsonElement.DeepEquals(original, roundTripped.Content));
        Assert.Equal(
            "producer-owned-user-value",
            roundTripped.Content.GetProperty("$runtimeType").GetString());
        Assert.Equal(
            3,
            roundTripped.Content.GetProperty("future").GetProperty("nested").GetArrayLength());
    }

    [Fact]
    public void FutureDurableEnvelopeFieldsRemainOpaque()
    {
        using JsonDocument document = JsonDocument.Parse(
            """
            {
              "$microsoftAgentFrameworkDurableTask": {
                "kind": "unknownAIContent",
                "version": 1,
                "futureMetadata": { "preserve": true }
              }
            }
            """);
        JsonElement original = document.RootElement.Clone();
        DurableAgentStateUnknownContent stored = new() { Content = original };

        AIContent restored = Assert.IsType<AIContent>(stored.ToAIContent());
        DurableAgentStateUnknownContent roundTripped = Assert.IsType<DurableAgentStateUnknownContent>(
            DurableAgentStateContent.FromAIContent(restored));

        Assert.True(JsonElement.DeepEquals(original, roundTripped.Content));
    }

    [Fact]
    public void MarkerShapedProducerContentWithoutEnvelopeMarkerRemainsOpaque()
    {
        using JsonDocument document = JsonDocument.Parse(
            """
            {
              "$microsoftAgentFrameworkDurableTask": {
                "kind": "unknownAIContent",
                "version": 1
              }
            }
            """);
        JsonElement original = document.RootElement.Clone();
        DurableAgentStateUnknownContent stored = new() { Content = original };

        AIContent restored = Assert.IsType<AIContent>(stored.ToAIContent());
        DurableAgentStateUnknownContent roundTripped = Assert.IsType<DurableAgentStateUnknownContent>(
            DurableAgentStateContent.FromAIContent(restored));

        Assert.True(JsonElement.DeepEquals(original, roundTripped.Content));
        Assert.Equal(
            "unknownAIContent",
            roundTripped.Content
                .GetProperty("$microsoftAgentFrameworkDurableTask")
                .GetProperty("kind")
                .GetString());
    }

    [Fact]
    public void UnregisteredAIContentSubtypePersistsCommonContractAsUnknown()
    {
        FutureContent original = new()
        {
            FutureValue = "not part of the common contract",
            RawRepresentation = new { kind = "future", value = 42 },
            AdditionalProperties = new()
            {
                ["providerFlag"] = true,
            },
            Annotations =
            [
                new AIAnnotation
                {
                    AdditionalProperties = new()
                    {
                        ["citation"] = "ref-1",
                    },
                },
            ],
        };

        DurableAgentStateUnknownContent stored = Assert.IsType<DurableAgentStateUnknownContent>(
            DurableAgentStateContent.FromAIContent(original));
        string json = JsonSerializer.Serialize(stored, s_stateContentTypeInfo);
        DurableAgentStateContent roundTripped = Assert.IsType<DurableAgentStateUnknownContent>(
            JsonSerializer.Deserialize(json, s_stateContentTypeInfo));

        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement persistedContent = document.RootElement.GetProperty("content");
        JsonElement envelope =
            persistedContent.GetProperty("$microsoftAgentFrameworkDurableTask");
        Assert.Equal("unknownAIContent", envelope.GetProperty("kind").GetString());
        Assert.StartsWith(
            "Microsoft.Agents.AI.DurableTask.UnknownContent/",
            envelope.GetProperty("marker").GetString(),
            StringComparison.Ordinal);
        Assert.Equal(1, envelope.GetProperty("version").GetInt32());
        Assert.False(persistedContent.TryGetProperty("$runtimeType", out _));
        Assert.DoesNotContain(typeof(FutureContent).FullName!, json, StringComparison.Ordinal);
        Assert.False(envelope.TryGetProperty(nameof(FutureContent.FutureValue), out _));

        AIContent restored = Assert.IsType<AIContent>(roundTripped.ToAIContent());
        Assert.True(
            Assert.IsType<JsonElement>(restored.AdditionalProperties?["providerFlag"]).GetBoolean());
        Assert.Equal(
            "ref-1",
            Assert.IsType<JsonElement>(
                Assert.Single(restored.Annotations!).AdditionalProperties?["citation"]).GetString());
        JsonElement rawRepresentation = Assert.IsType<JsonElement>(restored.RawRepresentation);
        Assert.Equal("future", rawRepresentation.GetProperty("kind").GetString());
        Assert.Equal(42, rawRepresentation.GetProperty("value").GetInt32());
    }

    [Fact]
    public void UnknownContentOmitsUnsafeValuesAndPreservesSafeMetadata()
    {
        CyclicPayload cyclicPayload = new();
        cyclicPayload.Self = cyclicPayload;
        JsonElement disposedElement;
        using (JsonDocument disposedDocument = JsonDocument.Parse("""{"value":"disposed-secret"}"""))
        {
            disposedElement = disposedDocument.RootElement;
        }

        using MemoryStream stream = new([1, 2, 3]);
        CollectingLogger logger = new();
        FutureContent original = new()
        {
            RawRepresentation = new ThrowingGetterPayload(),
            AdditionalProperties = new()
            {
                ["safeString"] = "kept",
                ["safeObject"] = new { value = 42 },
                ["cyclic"] = cyclicPayload,
                ["delegate"] = () => { },
                ["stream"] = stream,
                ["disposedJson"] = disposedElement,
                ["invalidNumber"] = double.NaN,
                ["customConverter"] = new ThrowingConverterPayload(),
            },
            Annotations =
            [
                new AIAnnotation
                {
                    RawRepresentation = disposedElement,
                    AdditionalProperties = new()
                    {
                        ["safeAnnotation"] = "annotation-kept",
                        ["badAnnotation"] = new ThrowingConverterPayload(),
                    },
                },
            ],
        };

        DurableAgentStateUnknownContent stored = Assert.IsType<DurableAgentStateUnknownContent>(
            DurableAgentStateContent.FromAIContent(original, logger));
        string json = JsonSerializer.Serialize(stored, s_stateContentTypeInfo);
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CreatedAt = DateTimeOffset.UtcNow,
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = "assistant",
                        Contents = [stored],
                    },
                ],
            });
        Exception? finalSerializationException = Record.Exception(
            () => JsonSerializer.Serialize(
                state,
                DurableAgentStateJsonContext.Default.DurableAgentState));

        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement envelope = document.RootElement.GetProperty("content")
            .GetProperty("$microsoftAgentFrameworkDurableTask");
        JsonElement additionalProperties = envelope.GetProperty("additionalProperties");
        Assert.Equal("kept", additionalProperties.GetProperty("safeString").GetString());
        Assert.Equal(42, additionalProperties.GetProperty("safeObject").GetProperty("value").GetInt32());
        Assert.False(additionalProperties.TryGetProperty("cyclic", out _));
        Assert.False(additionalProperties.TryGetProperty("delegate", out _));
        Assert.False(additionalProperties.TryGetProperty("stream", out _));
        Assert.False(additionalProperties.TryGetProperty("disposedJson", out _));
        Assert.False(additionalProperties.TryGetProperty("invalidNumber", out _));
        Assert.False(additionalProperties.TryGetProperty("customConverter", out _));
        Assert.True(envelope.GetProperty("omitted").GetProperty("rawRepresentation").GetBoolean());
        Assert.Equal(6, envelope.GetProperty("omitted").GetProperty("additionalProperties").GetInt32());

        JsonElement annotation = envelope.GetProperty("annotations")[0];
        Assert.Equal(
            "annotation-kept",
            annotation.GetProperty("additionalProperties").GetProperty("safeAnnotation").GetString());
        Assert.False(
            annotation.GetProperty("additionalProperties").TryGetProperty("badAnnotation", out _));
        Assert.Equal(
            1,
            annotation.GetProperty("omitted").GetProperty("additionalProperties").GetInt32());
        Assert.True(
            annotation.GetProperty("omitted").GetProperty("rawRepresentation").GetBoolean());

        AIContent restored = Assert.IsType<AIContent>(stored.ToAIContent());
        Assert.Equal(
            "kept",
            Assert.IsType<JsonElement>(restored.AdditionalProperties?["safeString"]).GetString());
        Assert.Equal(
            "annotation-kept",
            Assert.IsType<JsonElement>(
                Assert.Single(restored.Annotations!).AdditionalProperties?["safeAnnotation"]).GetString());

        Assert.Null(finalSerializationException);
        Assert.True(logger.WarningCount >= 8);
        Assert.All(logger.Exceptions, exception => Assert.Null(exception));
        Assert.All(
            logger.Messages,
            message =>
            {
                Assert.DoesNotContain("disposed-secret", message, StringComparison.Ordinal);
                Assert.DoesNotContain("getter-secret", message, StringComparison.Ordinal);
                Assert.DoesNotContain("converter-secret", message, StringComparison.Ordinal);
                Assert.DoesNotContain("safeString", message, StringComparison.Ordinal);
            });
    }

    [Fact]
    public void UnknownSubtypePropertyGetterIsNeverInvoked()
    {
        ThrowingFutureContent.GetterInvocationCount = 0;
        ThrowingFutureContent original = new()
        {
            AdditionalProperties = new()
            {
                ["safe"] = true,
            },
        };

        DurableAgentStateUnknownContent stored = Assert.IsType<DurableAgentStateUnknownContent>(
            DurableAgentStateContent.FromAIContent(original));
        string json = JsonSerializer.Serialize(stored, s_stateContentTypeInfo);
        AIContent restored = stored.ToAIContent();

        Assert.Equal(0, ThrowingFutureContent.GetterInvocationCount);
        Assert.IsType<AIContent>(restored);
        Assert.True(
            Assert.IsType<JsonElement>(restored.AdditionalProperties?["safe"]).GetBoolean());
        Assert.DoesNotContain("$runtimeType", json, StringComparison.Ordinal);
        Assert.DoesNotContain("getter-secret", json, StringComparison.Ordinal);
    }

    [Fact]
    public void UnknownContentDoesNotSwallowCancellation()
    {
        FutureContent original = new()
        {
            RawRepresentation = new CancelingGetterPayload(),
        };

        Assert.ThrowsAny<OperationCanceledException>(
            () => DurableAgentStateContent.FromAIContent(original));
    }

    private sealed class FutureContent : AIContent
    {
        public string? FutureValue { get; init; }
    }

    private sealed class CyclicPayload
    {
        public CyclicPayload? Self { get; set; }
    }

    private sealed class ThrowingFutureContent : AIContent
    {
        public static int GetterInvocationCount { get; set; }

        public string Dangerous
        {
            get
            {
                GetterInvocationCount++;
                throw new InvalidOperationException("getter-secret");
            }
        }
    }

    private sealed class ThrowingGetterPayload
    {
        public string Dangerous => throw new InvalidOperationException("getter-secret");
    }

    private sealed class CancelingGetterPayload
    {
        public string Dangerous => throw new OperationCanceledException();
    }

    [JsonConverter(typeof(ThrowingConverterPayloadConverter))]
    public sealed class ThrowingConverterPayload;

    public sealed class ThrowingConverterPayloadConverter : JsonConverter<ThrowingConverterPayload>
    {
        public override ThrowingConverterPayload? Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options)
        {
            throw new NotSupportedException();
        }

        public override void Write(
            Utf8JsonWriter writer,
            ThrowingConverterPayload value,
            JsonSerializerOptions options)
        {
            throw new FormatException("converter-secret");
        }
    }

    private sealed class CollectingLogger : ILogger
    {
        public int WarningCount { get; private set; }

        public List<string> Messages { get; } = [];

        public List<Exception?> Exceptions { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                this.WarningCount++;
                this.Messages.Add(formatter(state, exception));
                this.Exceptions.Add(exception);
            }
        }
    }
}
