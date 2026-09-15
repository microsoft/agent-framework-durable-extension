// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents URI content for a durable agent state message.
/// </summary>
internal sealed class DurableAgentStateUriContent : DurableAgentStateContent
{
    /// <summary>
    /// Gets the URI of the content.
    /// </summary>
    [JsonPropertyName("uri")]
    public required Uri Uri { get; init; }

    /// <summary>
    /// Gets the media type of the content.
    /// </summary>
    [JsonPropertyName("mediaType")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? MediaType { get; init; }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateUriContent"/> from a <see cref="UriContent"/>.
    /// </summary>
    /// <param name="uriContent">The <see cref="UriContent"/> to convert.</param>
    /// <param name="allowLosslessV2">Whether a missing media type may be preserved for schema 2.</param>
    /// <returns>A <see cref="DurableAgentStateUriContent"/> representing the original content.</returns>
    public static DurableAgentStateUriContent FromUriContent(
        UriContent uriContent,
        bool allowLosslessV2 = false)
    {
        if (uriContent.MediaType is null && !allowLosslessV2)
        {
            throw new InvalidOperationException(
                "Legacy durable agent URI content requires a media type.");
        }

        return new DurableAgentStateUriContent()
        {
            MediaType = uriContent.MediaType,
            Uri = uriContent.Uri
        };
    }

    /// <inheritdoc/>
    internal override void Validate(DurableAgentStateSchemaVersion version)
    {
        if (version.Major < DurableAgentState.RevisedSchemaMajorVersion &&
            this.MediaType is null)
        {
            throw new InvalidOperationException(
                "Legacy durable agent URI content requires a media type.");
        }
    }

    /// <inheritdoc/>
    public override AIContent ToAIContent()
    {
        if (this.MediaType is null)
        {
            throw new InvalidOperationException(
                "The current .NET UriContent contract cannot represent a URI without a media type.");
        }

        return new UriContent(this.Uri, this.MediaType);
    }
}
