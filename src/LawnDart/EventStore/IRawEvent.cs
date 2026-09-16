namespace LawnDart.EventStore;

/// <summary>
/// An <see cref="IEvent"/> that already has a family token and payload bytes.
/// </summary>
/// <remarks>
/// LawnDart's implementation is <see cref="RawRecordedEvent"/>. The catalog
/// rejects <see cref="IRawEvent"/> types. A process that does not have the CLR
/// event type appends and copies through <see cref="IEventLog"/>; typed hydrate
/// fails closed. This interface is not a substitute for an unknown-family
/// hydrate result.
/// <para>
/// Typed helpers such as <see cref="EventQueryMatcher"/> use
/// <see cref="TypeName"/> instead of <c>GetType()</c> so the stored family
/// token is what queries match.
/// </para>
/// </remarks>
public interface IRawEvent : LawnDart.IEvent
{
    /// <summary>The family catalog token as stored on the log.</summary>
    string TypeName { get; }

    /// <summary>The serialized payload bytes. Do not re-serialize this wrapper.</summary>
    byte[] RawPayload { get; }
}
