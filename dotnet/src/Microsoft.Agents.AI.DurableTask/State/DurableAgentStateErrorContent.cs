// Copyright (c) Microsoft. All rights reserved.

using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents durable agent state content that contains error content.
/// </summary>
internal sealed class DurableAgentStateErrorContent : DurableAgentStateContent
{
    private static readonly ConditionalWeakTable<ErrorContent, DetailsAssociation>
        s_detailsAssociations = new();

    /// <summary>
    /// Gets the error message.
    /// </summary>
    [JsonPropertyName("message")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Message { get; init; }

    /// <summary>
    /// Gets the error code.
    /// </summary>
    [JsonPropertyName("errorCode")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ErrorCode { get; init; }

    /// <summary>
    /// Gets the error details.
    /// </summary>
    [JsonPropertyName("details")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement Details
    {
        get;
        init => field = value.ValueKind == JsonValueKind.Undefined ? default : value.Clone();
    }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateErrorContent"/> from an <see cref="ErrorContent"/>.
    /// </summary>
    /// <param name="content">The <see cref="ErrorContent"/> to convert.</param>
    /// <returns>A <see cref="DurableAgentStateErrorContent"/> representing the original
    /// <see cref="ErrorContent"/>.</returns>
    public static DurableAgentStateErrorContent FromErrorContent(ErrorContent content)
    {
        JsonElement details;
        if (s_detailsAssociations.TryGetValue(content, out DetailsAssociation? association) &&
            string.Equals(content.Details, association.ProjectedDetails, StringComparison.Ordinal))
        {
            details = association.OriginalDetails.ValueKind == JsonValueKind.Undefined
                ? default
                : association.OriginalDetails.Clone();
        }
        else
        {
            details = content.Details is null && association is null
                ? default
                : JsonSerializer.SerializeToElement(
                    content.Details,
                    DurableAgentStateJsonContext.Default.String);
        }

        return new DurableAgentStateErrorContent()
        {
            Details = details,
            ErrorCode = content.ErrorCode,
            Message = content.Message
        };
    }

    /// <inheritdoc/>
    protected override AIContent ToAIContentCore()
    {
        string? projectedDetails = this.Details.ValueKind switch
        {
            JsonValueKind.Undefined or JsonValueKind.Null => null,
            JsonValueKind.String => this.Details.GetString(),
            _ => this.Details.GetRawText(),
        };
        ErrorContent content = new(this.Message)
        {
            Details = projectedDetails,
            ErrorCode = this.ErrorCode
        };
        s_detailsAssociations.Add(
            content,
            new DetailsAssociation(
                this.Details.ValueKind == JsonValueKind.Undefined ? default : this.Details.Clone(),
                projectedDetails));
        return content;
    }

    private sealed record DetailsAssociation(JsonElement OriginalDetails, string? ProjectedDetails);
}
