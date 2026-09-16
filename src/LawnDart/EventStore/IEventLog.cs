namespace LawnDart.EventStore;

/// <summary>
/// Schema-dumb durable log. The public extension point for a third-party store.
/// </summary>
/// <remarks>
/// Append <see cref="AppendEvent"/>; read <see cref="RecordedEvent"/>. No CLR event
/// type and no <c>EventMetadata</c>. Query, concurrency, and subscriptions stay on
/// family tokens, tags, and sequence.
/// <para>
/// Application code keeps using <see cref="IEventStore"/> (typed session). LawnDart
/// ships that adapter; backends implement this log. Do not add <c>AppendRaw</c> /
/// <c>ReadRaw</c> on <see cref="IEventStore"/>.
/// </para>
/// <para>
/// <see cref="AppendResult.ConsistencyMarker"/> and
/// <see cref="EventLogQueryResult.ConsistencyMarker"/> are store-owned and must pass
/// through a typed adapter unchanged.
/// </para>
/// <para>
/// Optional <c>toCommitTimestamp</c> filters <see cref="RecordedEvent.CommitTimestamp"/>.
/// It is not business time and must not parse metadata JSON. Session time-travel
/// (<c>EventMetadata.Timestamp</c>) stays on <see cref="IEventStore"/>.
/// </para>
/// </remarks>
public interface IEventLog : IStreamRegistry
{
    /// <summary>Reads recorded frames from a stream in version order.</summary>
    Task<IReadOnlyList<RecordedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toCommitTimestamp = null,
        CancellationToken cancellationToken = default);

    /// <summary>Streams recorded frames from a stream in version order.</summary>
    IAsyncEnumerable<RecordedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toCommitTimestamp = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads recorded frames matching a DCB <see cref="Query"/> (tokens / tags /
    /// partition — not CLR types).
    /// </summary>
    Task<EventLogQueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toCommitTimestamp = null,
        CancellationToken cancellationToken = default);

    /// <summary>Streams recorded frames matching a DCB <see cref="Query"/>.</summary>
    IAsyncEnumerable<RecordedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toCommitTimestamp = null,
        CancellationToken cancellationToken = default);

    /// <summary>Appends frames to a stream. Store assigns sequence, version, and commit time.</summary>
    Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<AppendEvent> events,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default);

    /// <summary>Appends frames under a DCB <see cref="AppendCondition"/>.</summary>
    Task<AppendResult> AppendAsync(
        IEnumerable<AppendEvent> events,
        AppendCondition condition,
        CancellationToken cancellationToken = default);

    /// <summary>Highest global sequence assigned by this log, or 0 if empty.</summary>
    Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Highest <see cref="RecordedEvent.SequencePosition"/> matching
    /// <paramref name="query"/> in the window, or 0 if nothing matches.
    /// Implementations should answer from indexes and avoid materializing payloads.
    /// </summary>
    Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toCommitTimestamp = null,
        CancellationToken cancellationToken = default);
}
