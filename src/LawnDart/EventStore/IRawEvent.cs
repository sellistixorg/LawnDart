namespace LawnDart.EventStore;

/// <summary>
/// An <see cref="IEvent"/> that already has an event name and payload bytes.
/// </summary>
/// <remarks>
/// LawnDart's implementation is <see cref="RawRecordedEvent"/>. The catalog
/// rejects <see cref="IRawEvent"/> types. A process that does not have the .NET
/// event type appends and copies through <see cref="IEventLog"/>. A typed read
/// still throws. This interface is not a stand-in event for a name this process
/// has not registered.
/// <para>
/// Typed helpers such as <see cref="EventQueryMatcher"/> use
/// <see cref="TypeName"/> instead of <c>GetType()</c> so the stored event
/// name is what queries match.
/// </para>
/// </remarks>
public interface IRawEvent : LawnDart.IEvent
{
    /// <summary>The event name as stored on the log.</summary>
    string TypeName { get; }

    /// <summary>The serialized payload bytes. Do not re-serialize this wrapper.</summary>
    byte[] RawPayload { get; }
}
