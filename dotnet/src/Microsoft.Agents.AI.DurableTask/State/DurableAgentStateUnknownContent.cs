// Copyright (c) Microsoft. All rights reserved.

using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents the unknown content for a durable agent state entry.
/// </summary>
internal sealed class DurableAgentStateUnknownContent : DurableAgentStateContent
{
    private const string DurableEnvelopePropertyName = "$microsoftAgentFrameworkDurableTask";
    private const string KindPropertyName = "kind";
    private const string MarkerPropertyName = "marker";
    private const string VersionPropertyName = "version";
    private const string AnnotationsPropertyName = "annotations";
    private const string AdditionalPropertiesPropertyName = "additionalProperties";
    private const string RawRepresentationPropertyName = "rawRepresentation";
    private const string AnnotatedRegionsPropertyName = "annotatedRegions";
    private const string OmittedPropertyName = "omitted";
    private const string UnknownContentKind = "unknownAIContent";
    private const string DurableEnvelopeMarker =
        "Microsoft.Agents.AI.DurableTask.UnknownContent/9d3df45a-6345-4b0e-88c6-972497582abc";
    private const int DurableEnvelopeVersion = 1;

    private static readonly JsonElement s_minimalUnknownContent = CreateMinimalUnknownContent();

    /// <summary>
    /// Gets the serialized unknown content.
    /// </summary>
    [JsonPropertyName("content")]
    public required JsonElement Content { get; init; }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateUnknownContent"/> from an <see cref="AIContent"/>.
    /// </summary>
    /// <param name="content">The <see cref="AIContent"/> to convert.</param>
    /// <param name="logger">The logger used to report safe serialization fallbacks.</param>
    /// <returns>A <see cref="DurableAgentStateUnknownContent"/> representing the original content.</returns>
    public static DurableAgentStateUnknownContent FromUnknownContent(
        AIContent content,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(content);

        if (TryGetOpaqueContent(content, logger, out JsonElement opaqueContent))
        {
            return new DurableAgentStateUnknownContent { Content = opaqueContent };
        }

        JsonObject envelope = CreateEnvelope(UnknownContentKind);
        JsonObject omissions = [];

        AddAnnotations(content.Annotations, envelope, omissions, logger);
        AddAdditionalProperties(content.AdditionalProperties, envelope, omissions, logger);
        AddRawRepresentation(content.RawRepresentation, envelope, omissions, logger);

        if (omissions.Count > 0)
        {
            envelope[OmittedPropertyName] = omissions;
        }

        return new DurableAgentStateUnknownContent
        {
            Content = SerializeEnvelope(envelope, content, logger),
        };
    }

    /// <inheritdoc/>
    public override AIContent ToAIContent()
    {
        if (TryGetEnvelope(this.Content, out JsonElement envelope, out string? kind) &&
            kind == UnknownContentKind &&
            TryReadUnknownContent(envelope, out AIContent unknownContent))
        {
            return unknownContent;
        }

        return CreateOpaqueAIContent(this.Content);
    }

    private static JsonObject CreateEnvelope(string kind)
    {
        return new JsonObject
        {
            [KindPropertyName] = kind,
            [MarkerPropertyName] = DurableEnvelopeMarker,
            [VersionPropertyName] = DurableEnvelopeVersion,
        };
    }

    private static JsonElement CreateMinimalUnknownContent()
    {
        JsonObject root = new()
        {
            [DurableEnvelopePropertyName] = CreateEnvelope(UnknownContentKind),
        };
        return JsonSerializer.SerializeToElement(
            root,
            DurableAgentStateJsonContext.Default.JsonObject);
    }

    private static JsonElement SerializeEnvelope(
        JsonObject envelope,
        AIContent source,
        ILogger? logger)
    {
        JsonObject root = new()
        {
            [DurableEnvelopePropertyName] = envelope,
        };

        try
        {
            return JsonSerializer.SerializeToElement(
                root,
                DurableAgentStateJsonContext.Default.JsonObject);
        }
        catch (Exception exception) when (IsRecoverableSerializationFailure(exception))
        {
            LogSerializationFallback(logger, source, exception);
            return s_minimalUnknownContent.Clone();
        }
    }

    private static bool TryGetOpaqueContent(
        AIContent content,
        ILogger? logger,
        out JsonElement opaqueContent)
    {
        opaqueContent = default;
        try
        {
            if (content.GetType() != typeof(AIContent) ||
                content.Annotations is { Count: > 0 } ||
                content.RawRepresentation is not JsonElement rawRepresentation ||
                content.AdditionalProperties is not { Count: 1 } additionalProperties ||
                !additionalProperties.TryGetValue("content", out object? storedContent) ||
                storedContent is not JsonElement storedElement)
            {
                return false;
            }

            if (rawRepresentation.GetRawText() != storedElement.GetRawText())
            {
                return false;
            }

            opaqueContent = rawRepresentation.Clone();
            return true;
        }
        catch (Exception exception) when (IsRecoverableSerializationFailure(exception))
        {
            LogSerializationFallback(logger, content, exception);
            return false;
        }
    }

    private static void AddAnnotations(
        IList<AIAnnotation>? annotations,
        JsonObject envelope,
        JsonObject omissions,
        ILogger? logger)
    {
        if (annotations is null)
        {
            return;
        }

        if (!TryGetCount(annotations, logger, out int count))
        {
            omissions[AnnotationsPropertyName] = true;
            return;
        }

        JsonArray projection = [];
        int omittedCount = 0;
        for (int index = 0; index < count; index++)
        {
            if (!TryGetItem(annotations, index, logger, out AIAnnotation? annotation) ||
                annotation is null)
            {
                omittedCount++;
                continue;
            }

            projection.Add((JsonNode)CreateAnnotationProjection(annotation, logger));
        }

        if (projection.Count > 0)
        {
            envelope[AnnotationsPropertyName] = projection;
        }

        if (omittedCount > 0)
        {
            omissions[AnnotationsPropertyName] = omittedCount;
        }
    }

    private static JsonObject CreateAnnotationProjection(
        AIAnnotation annotation,
        ILogger? logger)
    {
        JsonObject projection = [];
        JsonObject omissions = [];

        AddAdditionalProperties(
            annotation.AdditionalProperties,
            projection,
            omissions,
            logger);
        AddAnnotatedRegions(
            annotation.AnnotatedRegions,
            projection,
            omissions,
            logger);
        AddRawRepresentation(
            annotation.RawRepresentation,
            projection,
            omissions,
            logger);

        if (omissions.Count > 0)
        {
            projection[OmittedPropertyName] = omissions;
        }

        return projection;
    }

    private static void AddAdditionalProperties(
        AdditionalPropertiesDictionary? additionalProperties,
        JsonObject projection,
        JsonObject omissions,
        ILogger? logger)
    {
        if (additionalProperties is null)
        {
            return;
        }

        KeyValuePair<string, object?>[] entries;
        try
        {
            entries = [.. additionalProperties];
        }
        catch (Exception exception) when (IsRecoverableSerializationFailure(exception))
        {
            LogSerializationFallback(logger, additionalProperties, exception);
            omissions[AdditionalPropertiesPropertyName] = true;
            return;
        }

        JsonObject projectedProperties = [];
        int omittedCount = 0;
        foreach ((string key, object? value) in entries)
        {
            if (TryConvertToJsonNode(value, logger, out JsonNode? jsonValue))
            {
                projectedProperties[key] = jsonValue;
            }
            else
            {
                omittedCount++;
            }
        }

        if (projectedProperties.Count > 0)
        {
            projection[AdditionalPropertiesPropertyName] = projectedProperties;
        }

        if (omittedCount > 0)
        {
            omissions[AdditionalPropertiesPropertyName] = omittedCount;
        }
    }

    private static void AddAnnotatedRegions(
        IList<AnnotatedRegion>? annotatedRegions,
        JsonObject projection,
        JsonObject omissions,
        ILogger? logger)
    {
        if (annotatedRegions is null)
        {
            return;
        }

        if (!TryGetCount(annotatedRegions, logger, out int count))
        {
            omissions[AnnotatedRegionsPropertyName] = true;
            return;
        }

        JsonArray projectedRegions = [];
        int omittedCount = 0;
        for (int index = 0; index < count; index++)
        {
            if (!TryGetItem(annotatedRegions, index, logger, out AnnotatedRegion? region) ||
                region is null ||
                !TryConvertToJsonNode(region, logger, out JsonNode? jsonValue))
            {
                omittedCount++;
                continue;
            }

            projectedRegions.Add(jsonValue);
        }

        if (projectedRegions.Count > 0)
        {
            projection[AnnotatedRegionsPropertyName] = projectedRegions;
        }

        if (omittedCount > 0)
        {
            omissions[AnnotatedRegionsPropertyName] = omittedCount;
        }
    }

    private static void AddRawRepresentation(
        object? rawRepresentation,
        JsonObject projection,
        JsonObject omissions,
        ILogger? logger)
    {
        if (rawRepresentation is null)
        {
            return;
        }

        if (TryConvertToJsonNode(rawRepresentation, logger, out JsonNode? jsonValue))
        {
            projection[RawRepresentationPropertyName] = jsonValue;
        }
        else
        {
            omissions[RawRepresentationPropertyName] = true;
        }
    }

    private static bool TryConvertToJsonNode(
        object? value,
        ILogger? logger,
        out JsonNode? jsonValue)
    {
        try
        {
            JsonElement element = ToJsonElement(value).Clone();
            jsonValue = ToJsonNode(element);
            return true;
        }
        catch (Exception exception) when (IsRecoverableSerializationFailure(exception))
        {
            LogSerializationFallback(logger, value, exception);
            jsonValue = null;
            return false;
        }
    }

    private static bool TryGetCount<T>(
        IList<T> values,
        ILogger? logger,
        out int count)
    {
        try
        {
            count = values.Count;
            return true;
        }
        catch (Exception exception) when (IsRecoverableSerializationFailure(exception))
        {
            LogSerializationFallback(logger, values, exception);
            count = 0;
            return false;
        }
    }

    private static bool TryGetItem<T>(
        IList<T> values,
        int index,
        ILogger? logger,
        out T? value)
    {
        try
        {
            value = values[index];
            return true;
        }
        catch (Exception exception) when (IsRecoverableSerializationFailure(exception))
        {
            LogSerializationFallback(logger, values, exception);
            value = default;
            return false;
        }
    }

    private static JsonNode? ToJsonNode(JsonElement element)
    {
        return JsonNode.Parse(element.GetRawText());
    }

    private static bool TryGetEnvelope(
        JsonElement content,
        out JsonElement envelope,
        out string? kind)
    {
        envelope = default;
        kind = null;
        if (content.ValueKind != JsonValueKind.Object ||
            !HasExactlyOneProperty(content, DurableEnvelopePropertyName) ||
            !content.TryGetProperty(DurableEnvelopePropertyName, out envelope) ||
            envelope.ValueKind != JsonValueKind.Object ||
            !envelope.TryGetProperty(KindPropertyName, out JsonElement kindElement) ||
            kindElement.ValueKind != JsonValueKind.String ||
            !envelope.TryGetProperty(MarkerPropertyName, out JsonElement markerElement) ||
            markerElement.ValueKind != JsonValueKind.String ||
            markerElement.GetString() != DurableEnvelopeMarker ||
            !envelope.TryGetProperty(VersionPropertyName, out JsonElement versionElement) ||
            versionElement.ValueKind != JsonValueKind.Number ||
            !versionElement.TryGetInt32(out int version) ||
            version != DurableEnvelopeVersion)
        {
            return false;
        }

        kind = kindElement.GetString();
        return kind is not null;
    }

    private static bool TryReadUnknownContent(
        JsonElement envelope,
        out AIContent content)
    {
        content = null!;
        if (!HasOnlyProperties(
                envelope,
                KindPropertyName,
                MarkerPropertyName,
                VersionPropertyName,
                AnnotationsPropertyName,
                AdditionalPropertiesPropertyName,
                RawRepresentationPropertyName,
                OmittedPropertyName) ||
            !TryReadAnnotations(envelope, out List<AIAnnotation>? annotations) ||
            !TryReadAdditionalProperties(
                envelope,
                out AdditionalPropertiesDictionary? additionalProperties) ||
            !TryReadRawRepresentation(envelope, out object? rawRepresentation) ||
            !HasValidOmissions(envelope))
        {
            return false;
        }

        content = new AIContent
        {
            Annotations = annotations,
            AdditionalProperties = additionalProperties,
            RawRepresentation = rawRepresentation,
        };
        return true;
    }

    private static bool TryReadAnnotations(
        JsonElement envelope,
        out List<AIAnnotation>? annotations)
    {
        annotations = null;
        if (!envelope.TryGetProperty(AnnotationsPropertyName, out JsonElement annotationsElement))
        {
            return true;
        }

        if (annotationsElement.ValueKind != JsonValueKind.Array)
        {
            return false;
        }

        List<AIAnnotation> result = [];
        JsonTypeInfo regionTypeInfo =
            AIJsonUtilities.DefaultOptions.GetTypeInfo(typeof(AnnotatedRegion));
        foreach (JsonElement annotationElement in annotationsElement.EnumerateArray())
        {
            if (annotationElement.ValueKind != JsonValueKind.Object ||
                !HasOnlyProperties(
                    annotationElement,
                    AdditionalPropertiesPropertyName,
                    AnnotatedRegionsPropertyName,
                    RawRepresentationPropertyName,
                    OmittedPropertyName) ||
                !TryReadAdditionalProperties(
                    annotationElement,
                    out AdditionalPropertiesDictionary? additionalProperties) ||
                !TryReadRawRepresentation(annotationElement, out object? rawRepresentation) ||
                !HasValidOmissions(annotationElement))
            {
                return false;
            }

            List<AnnotatedRegion>? regions = null;
            if (annotationElement.TryGetProperty(
                    AnnotatedRegionsPropertyName,
                    out JsonElement regionsElement))
            {
                if (regionsElement.ValueKind != JsonValueKind.Array)
                {
                    return false;
                }

                regions = [];
                foreach (JsonElement regionElement in regionsElement.EnumerateArray())
                {
                    try
                    {
                        if (regionElement.Deserialize(regionTypeInfo) is not AnnotatedRegion region)
                        {
                            return false;
                        }

                        regions.Add(region);
                    }
                    catch (Exception exception) when (IsRecoverableSerializationFailure(exception))
                    {
                        return false;
                    }
                }
            }

            result.Add(new AIAnnotation
            {
                AdditionalProperties = additionalProperties,
                AnnotatedRegions = regions,
                RawRepresentation = rawRepresentation,
            });
        }

        annotations = result;
        return true;
    }

    private static bool TryReadAdditionalProperties(
        JsonElement envelope,
        out AdditionalPropertiesDictionary? additionalProperties)
    {
        additionalProperties = null;
        if (!envelope.TryGetProperty(
                AdditionalPropertiesPropertyName,
                out JsonElement additionalPropertiesElement))
        {
            return true;
        }

        if (additionalPropertiesElement.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        AdditionalPropertiesDictionary result = [];
        foreach (JsonProperty property in additionalPropertiesElement.EnumerateObject())
        {
            result[property.Name] = property.Value.Clone();
        }

        additionalProperties = result;
        return true;
    }

    private static bool TryReadRawRepresentation(
        JsonElement envelope,
        out object? rawRepresentation)
    {
        rawRepresentation = null;
        if (envelope.TryGetProperty(
                RawRepresentationPropertyName,
                out JsonElement rawRepresentationElement))
        {
            rawRepresentation = rawRepresentationElement.Clone();
        }

        return true;
    }

    private static bool HasValidOmissions(JsonElement envelope)
    {
        if (!envelope.TryGetProperty(OmittedPropertyName, out JsonElement omittedElement))
        {
            return true;
        }

        if (omittedElement.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        foreach (JsonProperty property in omittedElement.EnumerateObject())
        {
            if (property.Name is not (
                    AnnotationsPropertyName or
                    AdditionalPropertiesPropertyName or
                    RawRepresentationPropertyName or
                    AnnotatedRegionsPropertyName) ||
                (property.Value.ValueKind != JsonValueKind.True &&
                 property.Value.ValueKind != JsonValueKind.False &&
                 (property.Value.ValueKind != JsonValueKind.Number ||
                  !property.Value.TryGetInt32(out int count) ||
                  count < 0)))
            {
                return false;
            }
        }

        return true;
    }

    private static bool HasExactlyOneProperty(JsonElement element, string propertyName)
    {
        int count = 0;
        foreach (JsonProperty property in element.EnumerateObject())
        {
            count++;
            if (!property.NameEquals(propertyName) || count > 1)
            {
                return false;
            }
        }

        return count == 1;
    }

    private static bool HasOnlyProperties(JsonElement element, params string[] allowedNames)
    {
        foreach (JsonProperty property in element.EnumerateObject())
        {
            if (!allowedNames.Contains(property.Name, StringComparer.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static AIContent CreateOpaqueAIContent(JsonElement content)
    {
        return new AIContent
        {
            RawRepresentation = content.Clone(),
            AdditionalProperties = new AdditionalPropertiesDictionary
            {
                ["content"] = content.Clone(),
            },
        };
    }

    private static void LogSerializationFallback(
        ILogger? logger,
        object? value,
        Exception exception)
    {
        if (logger is null)
        {
            return;
        }

        try
        {
            logger.LogUnknownContentSerializationFallback(
                value?.GetType().FullName ?? "null",
                GetFailureCategory(exception));
        }
        catch (Exception loggingException) when (IsRecoverableSerializationFailure(loggingException))
        {
        }
    }

    private static string GetFailureCategory(Exception exception)
    {
        return exception switch
        {
            ObjectDisposedException => "disposedValue",
            JsonException => "invalidJson",
            NotSupportedException => "unsupportedType",
            InvalidOperationException => "invalidOperation",
            ArgumentException => "invalidValue",
            FormatException => "invalidFormat",
            OverflowException => "numericOverflow",
            IOException => "ioFailure",
            _ => "customSerializationFailure",
        };
    }

    private static bool IsRecoverableSerializationFailure(Exception exception)
    {
        if (exception is OperationCanceledException or
            OutOfMemoryException or
            StackOverflowException or
            AccessViolationException or
            AppDomainUnloadedException or
            BadImageFormatException or
            CannotUnloadAppDomainException or
            InvalidProgramException or
            SEHException)
        {
            return false;
        }

        if (exception is AggregateException aggregateException)
        {
            return aggregateException.InnerExceptions.All(IsRecoverableSerializationFailure);
        }

        return exception.InnerException is null ||
            IsRecoverableSerializationFailure(exception.InnerException);
    }
}
