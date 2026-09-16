namespace LawnDart.EventStore;

/// <summary>
/// Result of an <see cref="IEventLog"/> query: recorded frames plus the store-owned
/// <see cref="ConsistencyMarker"/>.
/// </summary>
/// <remarks>
/// <see cref="ConsistencyMarker"/> is unchanged from <see cref="QueryResult"/> /
/// <see cref="AppendResult"/>. The typed session passes it through; it does not
/// recompute the marker.
/// </remarks>
/// <param name="Events">Matching frames, ordered by <see cref="RecordedEvent.SequencePosition"/>.</param>
/// <param name="ConsistencyMarker">
/// Optional hash of the matched set. Null for SQL Server / in-memory backends.
/// </param>
public record EventLogQueryResult(
    IReadOnlyList<RecordedEvent> Events,
    byte[]? ConsistencyMarker = null);
