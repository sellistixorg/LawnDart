using LawnDart.Metadata;

namespace LawnDart.EventStore;

/// <summary>
/// Event store interface supporting both traditional stream-per-aggregate and DCB approaches.
/// Includes built-in stream registry for efficient stream discovery and metadata access.
/// </summary>
public interface IEventStore : IStreamRegistry
{
    /// <summary>
    /// Reads events from a stream (traditional approach).
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="fromVersion">Version to start reading from (inclusive). Default is 0.</param>
    /// <param name="toVersion">Optional. Stop reading at this version (inclusive). Null means no upper limit.</param>
    /// <param name="toTimestamp">Optional. Include only events with <see cref="EventMetadata.Timestamp"/> &lt;= this value (inclusive). Null means no temporal limit.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of sequenced events in version order.</returns>
    Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams events from a stream one-by-one (traditional approach).
    /// Use when processing large streams to avoid loading all events into memory.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="fromVersion">Version to start reading from (inclusive). Default is 0.</param>
    /// <param name="toVersion">Optional. Stop at this version (inclusive). Null means no upper limit.</param>
    /// <param name="toTimestamp">Optional. Include only events with <see cref="EventMetadata.Timestamp"/> &lt;= this value (inclusive). Null means no temporal limit.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async sequence of sequenced events in version order.</returns>
    IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads events matching a query (DCB approach).
    /// </summary>
    /// <param name="query">Query to filter events.</param>
    /// <param name="fromSequencePosition">Sequence position to start from (inclusive). Null means from beginning.</param>
    /// <param name="limit">Maximum number of events to return. Null means no limit.</param>
    /// <param name="toSequencePosition">Optional. Include only events with SequencePosition &lt;= this value. Null means no upper limit.</param>
    /// <param name="toTimestamp">Optional. Include only events with <see cref="EventMetadata.Timestamp"/> &lt;= this value. Null means no temporal limit.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="QueryResult"/> containing the matched events in sequence position order
    /// and an optional <see cref="QueryResult.ConsistencyMarker"/> that may be forwarded
    /// into a subsequent <c>AppendAsync</c> call via <see cref="AppendCondition.ConsistencyMarker"/>.
    /// </returns>
    Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams events matching a query one at a time (DCB approach).
    /// Use instead of <see cref="ReadByQueryAsync"/> when the caller does not need
    /// a <see cref="QueryResult.ConsistencyMarker"/> and wants to avoid materialising
    /// all matching events into memory at once. The caller may break out of the
    /// <c>await foreach</c> loop early to short-circuit after finding the required events.
    /// </summary>
    /// <param name="query">Query to filter events.</param>
    /// <param name="fromSequencePosition">Sequence position to start from (inclusive). Null means from beginning.</param>
    /// <param name="toSequencePosition">Optional upper bound on sequence position (inclusive). Null means no upper limit.</param>
    /// <param name="toTimestamp">Optional. Include only events with <see cref="EventMetadata.Timestamp"/> &lt;= this value (inclusive). Null means no temporal limit.</param>
    /// <param name="cancellationToken">Cancellation token. Checked before each yielded event.</param>
    /// <returns>Async sequence of matching events in sequence position order.</returns>
    IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends events to a stream (traditional approach).
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">Events to append.</param>
    /// <param name="expectedVersion">Expected version for optimistic concurrency. Null means no version check.</param>
    /// <param name="metadata">Metadata for all events.</param>
    /// <param name="tags">Tags to apply to all events.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// An <see cref="AppendResult"/> containing the sequence positions assigned to the appended events
    /// and an optional <see cref="AppendResult.ConsistencyMarker"/> populated by backends that support
    /// hash-based concurrency control. Null for SQL Server / in-memory backends.
    /// </returns>
    Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends events with an append condition (DCB approach).
    /// </summary>
    /// <param name="events">Events to append.</param>
    /// <param name="condition">Append condition for consistency checking.</param>
    /// <param name="metadata">Metadata for all events.</param>
    /// <param name="tags">Tags to apply to all events.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// An <see cref="AppendResult"/> containing the sequence positions assigned to the appended events
    /// and an optional <see cref="AppendResult.ConsistencyMarker"/> populated by backends that support
    /// hash-based concurrency control. Null for SQL Server / in-memory backends.
    /// </returns>
    /// <exception cref="ConcurrencyException">Thrown if the append condition fails.</exception>
    Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current global sequence position — the highest sequence number
    /// assigned by this store. This is the store's logical clock and is always
    /// greater than or equal to the number of events written (gaps may exist due to
    /// HiLo pre-allocation on crash recovery).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The current global sequence position, or 0 if no events have been written.</returns>
    Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the highest <see cref="SequencedEvent.SequencePosition"/> among events matching
    /// <paramref name="query"/> in the given window, or 0 if nothing matches.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="GetCurrentSequenceAsync"/>, this is a filtered MAX (tags / types / partition),
    /// not the store-wide head. Implementations should answer from a tag index when possible
    /// (for example the SQL <c>EventTags</c> table) and avoid materializing event payloads.
    /// </remarks>
    /// <param name="query">Query to filter events. <see cref="Query.All"/> is the store-wide max in the window.</param>
    /// <param name="fromSequencePosition">Inclusive lower bound. Null means from the beginning.</param>
    /// <param name="toSequencePosition">Inclusive upper bound. Null means no upper limit.</param>
    /// <param name="toTimestamp">Inclusive timestamp upper bound. Null means no temporal limit.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The maximum matching sequence position, or 0 if no events match.</returns>
    Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default);
}

