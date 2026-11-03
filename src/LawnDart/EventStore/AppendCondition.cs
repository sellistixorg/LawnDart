namespace LawnDart.EventStore;

/// <summary>
/// Condition for appending events that enforces consistency using DCB approach.
/// </summary>
public record AppendCondition
{
    /// <summary>
    /// Query that must not match any event after <see cref="After"/>. If a matching event exists, the append fails.
    /// </summary>
    public required Query FailIfEventsMatch { get; init; }

    /// <summary>
    /// Events at or before this sequence position are ignored when checking the condition.
    /// Only events after it can make the append fail. Null checks every event.
    /// Typically the highest position the client was aware of while building the decision model.
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

