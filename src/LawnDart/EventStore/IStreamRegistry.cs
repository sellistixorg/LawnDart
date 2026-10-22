namespace LawnDart.EventStore;

/// <summary>
/// Registry for stream metadata and discovery.
/// </summary>
public interface IStreamRegistry
{
    /// <summary>
    /// Gets metadata for a specific stream.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Stream metadata, or null if stream doesn't exist.</returns>
    Task<StreamMetadata?> GetStreamAsync(
        string streamId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets active streams of a specific aggregate type. The order is undefined.
    /// </summary>
    /// <param name="aggregateType">The aggregate type name (e.g., "Order").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Matching streams. The order is undefined.</returns>
    Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets active streams that have a specific tag. The order is undefined.
    /// </summary>
    /// <param name="tag">The tag to search for.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Matching streams. The order is undefined.</returns>
    Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists active stream ids, optionally limited to a prefix. The order is undefined.
    /// </summary>
    /// <param name="prefix">
    /// Optional prefix. <c>_</c>, <c>%</c>, and <c>[</c> are literal characters.
    /// The in-memory store matches with ordinal ignore-case.
    /// SQL Server uses the <c>StreamId</c> column collation, so the prefix can seek that index.
    /// A new database is case-insensitive.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Matching stream ids. The order is undefined.</returns>
    Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets streams updated after a specific sequence position.
    /// Useful for incremental projection processing.
    /// </summary>
    /// <param name="afterSequencePosition">Sequence position to start from (exclusive).</param>
    /// <param name="limit">Maximum number of streams to return.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of stream metadata.</returns>
    Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the number of active streams, optionally filtered by prefix.
    /// </summary>
    /// <param name="prefix">
    /// Optional prefix. <c>_</c>, <c>%</c>, and <c>[</c> are literal characters.
    /// The in-memory store matches with ordinal ignore-case.
    /// SQL Server uses the <c>StreamId</c> column collation, so the prefix can seek that index.
    /// A new database is case-insensitive.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of streams matching the filter.</returns>
    Task<long> GetStreamCountAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns one page of active streams, ordered by stream id, character by character.
    /// </summary>
    /// <param name="filter">
    /// Optional limits. A prefix, a tag, and an aggregate type combine: a stream must match every value that is set.
    /// The tenant id stays in the prefix, as it does for <see cref="EnumerateStreamIdsAsync"/>.
    /// <c>_</c>, <c>%</c>, and <c>[</c> are literal characters in the prefix.
    /// </param>
    /// <param name="pageToken">
    /// Token from the previous page, or null to start at the first stream.
    /// A token is valid only on the store that issued it, and only with the same filter.
    /// The token is opaque base64url. The bytes are a version byte, a short hash of the filter, and the last stream id encoded as UTF-8.
    /// A token that cannot be read, or that was issued for a different filter, throws <see cref="ArgumentException"/>.
    /// </param>
    /// <param name="pageSize">
    /// Maximum streams in the page. The default is <see cref="StreamPage.DefaultPageSize"/> (100).
    /// Values outside 1 to <see cref="StreamPage.MaxPageSize"/> (1000) throw <see cref="ArgumentOutOfRangeException"/>.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The page. <see cref="StreamPage.NextPageToken"/> is null when this is the last page.
    /// A stream created while you page is included when its id sorts after the last id already returned.
    /// Each stream id appears on at most one page. A stream that existed for a page, and that sorts after that page, remains available on a later page.
    /// </returns>
    /// <remarks>
    /// Shipped stores implement this themselves. A custom registry can rely on this default, which loads the matching streams, sorts them character by character, and returns one page.
    /// </remarks>
    Task<StreamPage> ListStreamsAsync(
        StreamListFilter? filter = null,
        string? pageToken = null,
        int pageSize = StreamPage.DefaultPageSize,
        CancellationToken cancellationToken = default)
        => StreamListDefault.ListAsync(this, filter, pageToken, pageSize, cancellationToken);
}


