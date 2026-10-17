namespace LawnDart.EventStore;

/// <summary>
/// An append used an event id that is already in the log.
/// The existing row is unchanged.
/// </summary>
public sealed class EventAlreadyAppendedException : InvalidOperationException
{
    /// <summary>Creates the exception for <paramref name="eventId"/>.</summary>
    public EventAlreadyAppendedException(Guid eventId)
        : base($"Event '{eventId}' is already in the log.")
    {
        EventId = eventId;
    }

    /// <summary>The event id that was already stored.</summary>
    public Guid EventId { get; }
}
