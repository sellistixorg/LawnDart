namespace LawnDart.EventStore;

/// <summary>
/// Condition for appending events that enforces consistency using DCB approach.
/// </summary>
public record AppendCondition
{
    /// <summary>
    /// Query that must not match any new events. If any events matching this query exist, append will fail.
    /// </summary>
    public required Query FailIfEventsMatch { get; init; }

    /// <summary>
    /// Sequence position to ignore. Events before this position are ignored when checking the condition.
    /// Typically represents the highest position the client was aware of while building the decision model.
    /// </summary>
    public long? After { get; init; }

    /// <summary>
    /// Optional consistency marker forwarded from a preceding <c>ReadByQueryAsync</c> call.
    /// When non-null and the backend supports hash-based concurrency control,
    /// the store compares this marker against the current state of the matched event set
    /// instead of re-executing the query, eliminating a round trip.
    /// Backends that do not support markers (SQL Server, in-memory) ignore this property
    /// and fall back to the standard query-based condition check.
    /// </summary>
    public byte[]? ConsistencyMarker { get; init; }

    /// <summary>
    /// Creates an append condition that fails if events matching the query exist.
    /// </summary>
    public static AppendCondition FailIfMatches(Query query, long? after = null) =>
        new() { FailIfEventsMatch = query, After = after };
}

