namespace LawnDart.EventStore;

/// <summary>
/// Result of a query read operation, containing the matched events and an optional
/// consistency marker that can be forwarded into a subsequent conditional append.
/// </summary>
/// <remarks>
/// The <see cref="ConsistencyMarker"/> is populated by backends that support
/// hash-based concurrency control. SQL Server and in-memory
/// implementations always return <c>null</c>. When non-null, the marker should
/// be passed into <see cref="AppendCondition.ConsistencyMarker"/> on the
/// subsequent <c>AppendAsync</c> call to enable a single-round-trip DCB check
/// instead of a redundant re-read.
/// </remarks>
/// <param name="Events">Events matching the query, ordered by sequence position.</param>
/// <param name="ConsistencyMarker">Optional hash representing the state of the matched event set at read time. Null for SQL Server / in-memory backends.</param>
public record QueryResult(
    IReadOnlyList<SequencedEvent> Events,
    byte[]? ConsistencyMarker = null);
