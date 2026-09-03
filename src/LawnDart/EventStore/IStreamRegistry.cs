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
    /// Gets all streams of a specific aggregate type.
    /// </summary>
    /// <param name="aggregateType">The aggregate type name (e.g., "Order").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of stream metadata.</returns>
    Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all streams that have a specific tag.
    /// </summary>
    /// <param name="tag">The tag to search for.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of stream metadata.</returns>
    Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates all stream IDs, optionally filtered by prefix.
    /// </summary>
    /// <param name="prefix">Optional prefix to filter stream IDs (e.g., "Order:").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of stream IDs.</returns>
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
    /// Returns the total number of streams in the registry, optionally filtered by prefix.
    /// </summary>
    /// <param name="prefix">Optional stream ID prefix filter (e.g. "buyers:OrderAggregate:").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of streams matching the filter.</returns>
    Task<long> GetStreamCountAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default);
}


