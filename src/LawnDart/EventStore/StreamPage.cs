namespace LawnDart.EventStore;

/// <summary>
/// One page of streams from <see cref="IStreamRegistry.ListStreamsAsync"/>.
/// </summary>
public sealed record StreamPage
{
    /// <summary>Page size used when the caller omits <c>pageSize</c>.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>Largest accepted page size.</summary>
    public const int MaxPageSize = 1000;

    /// <summary>Streams in this page, in stream id order.</summary>
    public required IReadOnlyList<StreamMetadata> Streams { get; init; }

    /// <summary>
    /// Pass this to the next call to read the following page.
    /// Null when this page is the last one.
    /// </summary>
    public string? NextPageToken { get; init; }
}
