using LawnDart.Metadata;

namespace LawnDart.EventStore;

/// <summary>
/// Appends for events that have no tags.
/// </summary>
/// <remarks>
/// Each method wraps every event in a <see cref="TaggedEvent"/> with no tags and calls the
/// matching <see cref="IEventStore"/> append. They hold no store logic. To tag an event,
/// call <see cref="IEventStore"/> directly with <see cref="TaggedEvent"/> items.
/// </remarks>
public static class EventStoreAppendExtensions
{
    /// <summary>
    /// Appends events with no tags to a stream.
    /// </summary>
    /// <param name="store">Event store.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="events">Events to append. None of them get tags.</param>
    /// <param name="expectedVersion">Expected version for optimistic concurrency. Null means no version check.</param>
    /// <param name="metadata">Metadata for all events.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of <see cref="IEventStore.AppendAsync(string, IEnumerable{TaggedEvent}, long?, EventMetadata?, CancellationToken)"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("ApiDesign", "RS0026:Do not add multiple public overloads with optional parameters", Justification = "The two appends take different required parameters (a stream id or an append condition), so a call always picks one.")]
    public static Task<AppendResult> AppendAsync(
        this IEventStore store,
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        return store.AppendAsync(streamId, WithoutTags(events), expectedVersion, metadata, cancellationToken);
    }

    /// <summary>
    /// Appends events with no tags under an append condition.
    /// </summary>
    /// <param name="store">Event store.</param>
    /// <param name="events">Events to append. None of them get tags.</param>
    /// <param name="condition">Append condition for consistency checking.</param>
    /// <param name="metadata">Metadata for all events.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of <see cref="IEventStore.AppendAsync(IEnumerable{TaggedEvent}, AppendCondition, EventMetadata?, CancellationToken)"/>.</returns>
    /// <exception cref="ConcurrencyException">Thrown if the append condition fails.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("ApiDesign", "RS0026:Do not add multiple public overloads with optional parameters", Justification = "The two appends take different required parameters (a stream id or an append condition), so a call always picks one.")]
    public static Task<AppendResult> AppendAsync(
        this IEventStore store,
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        return store.AppendAsync(WithoutTags(events), condition, metadata, cancellationToken);
    }

    private static List<TaggedEvent> WithoutTags(IEnumerable<IEvent> events)
    {
        ArgumentNullException.ThrowIfNull(events);
        var list = new List<TaggedEvent>(events is ICollection<IEvent> c ? c.Count : 4);
        foreach (var e in events)
            list.Add(new TaggedEvent(e));
        return list;
    }
}
