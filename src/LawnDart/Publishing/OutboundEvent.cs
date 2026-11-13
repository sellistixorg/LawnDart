using LawnDart.EventStore;

namespace LawnDart.Publishing;

/// <summary>
/// An already serialized integration event that goes in an outbox row next to
/// the event it was made from.
/// </summary>
/// <remarks>
/// <see cref="EventStoreAdapter"/> builds one for each appended event that its
/// <see cref="PublishedEventMap"/> declares, and attaches it to
/// <see cref="AppendEvent.Outbound"/>. A raw <see cref="IEventLog"/> caller sets it
/// by hand. A <c>null</c> <see cref="AppendEvent.Outbound"/> means no outbox row.
/// <para>
/// <see cref="Metadata"/> is UTF-8 JSON bytes, not <c>EventMetadata</c>. Payload and
/// metadata are copied in the constructor, so changing the caller's arrays later
/// does not change this instance.
/// </para>
/// </remarks>
public sealed class OutboundEvent
{
    /// <summary>
    /// Event name of the integration event (for example <c>membership.member-joined</c>).
    /// Not a .NET type name.
    /// </summary>
    public string EventType { get; }

    /// <summary>Schema version of the integration event. Default <c>1</c>. Zero is treated as <c>1</c>.</summary>
    public int SchemaVersion { get; }

    /// <summary>Payload format number (<see cref="EventCodec"/>). Default <see cref="EventCodec.Json"/>. <c>0</c> is rejected.</summary>
    public byte CodecId { get; }

    /// <summary>Serialized integration event. Owned copy.</summary>
    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>UTF-8 JSON metadata bytes for the outbox row. Empty when none was given. Owned copy.</summary>
    public ReadOnlyMemory<byte> Metadata { get; }

    /// <param name="eventType">Event name of the integration event.</param>
    /// <param name="payload">Serialized payload. Copied on construct.</param>
    /// <param name="metadata">UTF-8 JSON metadata bytes. Copied on construct. Empty is valid.</param>
    /// <param name="schemaVersion">Schema version. Default <c>1</c>; <c>0</c> becomes <c>1</c>.</param>
    /// <param name="codecId">Payload format number. Default <see cref="EventCodec.Json"/>. <c>0</c> is rejected.</param>
    public OutboundEvent(
        string eventType,
        ReadOnlyMemory<byte> payload,
        ReadOnlyMemory<byte> metadata = default,
        int schemaVersion = 1,
        byte codecId = EventCodec.Json)
    {
        EventType = EventLogBuffers.RequireEventType(eventType);
        SchemaVersion = EventLogBuffers.NormalizeSchemaVersion(schemaVersion);
        CodecId = EventLogBuffers.RequireCodecId(codecId);
        Payload = EventLogBuffers.Snapshot(payload);
        Metadata = EventLogBuffers.Snapshot(metadata);
    }
}
