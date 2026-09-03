namespace LawnDart.EventStore;

/// <summary>
/// Result of an append operation, containing the assigned sequence positions
/// and an optional consistency marker for high-performance conditional appends.
/// </summary>
/// <remarks>
/// The <see cref="ConsistencyMarker"/> is populated by backends that support
/// hash-based concurrency control. SQL Server and in-memory
/// implementations always return <c>null</c> — callers must treat a null marker
/// as "no fast-path available" and fall back to query-based condition checking.
/// <para>
/// <see cref="LastStreamVersion"/> is the stream-level version number assigned to the
/// last appended event. Repositories use this to sync the aggregate's in-memory version
/// with the store's actual version number (which may differ across backends —
/// SQL Server and in-memory are 1-based, others may be 0-based).
/// A value of -1 indicates the backend did not populate this field.
/// </para>
/// </remarks>
/// <param name="SequencePositions">Global sequence positions assigned to each appended event, in order.</param>
/// <param name="ConsistencyMarker">Optional hash representing the state of the matched event set at append time. Null for SQL Server / in-memory backends.</param>
/// <param name="LastStreamVersion">Stream-level version of the last appended event. -1 if not populated by the backend.</param>
public record AppendResult(
    IReadOnlyList<long> SequencePositions,
    byte[]? ConsistencyMarker = null,
    long LastStreamVersion = -1);
