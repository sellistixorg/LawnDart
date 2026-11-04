namespace LawnDart.EventStore;

/// <summary>
/// One event to append and the tags written on that event only.
/// </summary>
/// <remarks>
/// Tags are the subjects the event is about (for example <c>order:42</c>). Each event in an
/// append carries only its own tags. The tags are copied when the item is created, and a
/// repeated tag is kept once (the first copy, in order). Tags compare exactly, so
/// <c>A:1</c> and <c>a:1</c> are different tags.
/// <para>
/// Use: <c>new TaggedEvent(orderPlaced, "order:42", "customer:7")</c>.
/// </para>
/// </remarks>
/// <param name="Event">Event to append. Required.</param>
/// <param name="Tags">Tags for this event. Null or none means the event has no tags.</param>
public sealed record TaggedEvent(IEvent Event, params IReadOnlyList<string> Tags)
{
    /// <summary>Event to append.</summary>
    public IEvent Event { get; } = Event ?? throw new ArgumentNullException(nameof(Event));

    /// <summary>Tags for this event, each one once, in the order first given.</summary>
    public IReadOnlyList<string> Tags { get; } = EventLogBuffers.SnapshotTags(Tags);
}
