namespace LawnDart.EventStore;

/// <summary>
/// Structured filter for portable event-store subscriptions.
/// Scope only selects which events appear; it does not change catch-up vs live semantics.
/// </summary>
public sealed class EventSubscriptionFilter
{
    /// <summary>
    /// Kind of subscription scope.
    /// </summary>
    public EventSubscriptionScope Kind { get; }

    /// <summary>
    /// Stream identifier when <see cref="Kind"/> is <see cref="EventSubscriptionScope.Stream"/>; otherwise <see langword="null"/>.
    /// </summary>
    public string? StreamId { get; }

    /// <summary>
    /// DCB query when <see cref="Kind"/> is <see cref="EventSubscriptionScope.Query"/>; otherwise <see langword="null"/>.
    /// </summary>
    public Query? Query { get; }

    private EventSubscriptionFilter(EventSubscriptionScope kind, string? streamId, Query? query)
    {
        Kind = kind;
        StreamId = streamId;
        Query = query;
    }

    /// <summary>
    /// Subscribes to all events in global sequence order.
    /// </summary>
    public static EventSubscriptionFilter All() =>
        new(EventSubscriptionScope.All, streamId: null, query: null);

    /// <summary>
    /// Subscribes to events for a single stream, still ordered by global <see cref="SequencedEvent.SequencePosition"/>.
    /// </summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <exception cref="ArgumentException"><paramref name="streamId"/> is null or whitespace.</exception>
    public static EventSubscriptionFilter ForStream(string streamId)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream id is required.", nameof(streamId));

        return new(EventSubscriptionScope.Stream, streamId, query: null);
    }

    /// <summary>
    /// Subscribes to events matching a DCB <see cref="Query"/> (same match rules as query reads).
    /// </summary>
    /// <param name="query">The query predicate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
    public static EventSubscriptionFilter ForQuery(Query query)
    {
        ArgumentNullException.ThrowIfNull(query);
        return new(EventSubscriptionScope.Query, streamId: null, query);
    }

    /// <summary>
    /// Returns whether <paramref name="sequencedEvent"/> matches this filter.
    /// </summary>
    public bool Matches(SequencedEvent sequencedEvent)
    {
        ArgumentNullException.ThrowIfNull(sequencedEvent);

        return Kind switch
        {
            EventSubscriptionScope.All => true,
            EventSubscriptionScope.Stream =>
                string.Equals(sequencedEvent.StreamId, StreamId, StringComparison.Ordinal),
            EventSubscriptionScope.Query =>
                EventQueryMatcher.Matches(sequencedEvent, Query!),
            _ => false
        };
    }
}

/// <summary>
/// Scope discriminant for <see cref="EventSubscriptionFilter"/>.
/// </summary>
public enum EventSubscriptionScope
{
    /// <summary>All events (global subscription).</summary>
    All = 0,

    /// <summary>Events belonging to a single stream.</summary>
    Stream = 1,

    /// <summary>Events matching a DCB <see cref="Query"/>.</summary>
    Query = 2
}
