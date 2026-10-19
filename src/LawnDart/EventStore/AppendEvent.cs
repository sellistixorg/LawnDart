namespace LawnDart.EventStore;

/// <summary>
/// Caller-supplied envelope for an <see cref="IEventLog"/> append.
/// </summary>
/// <remarks>
/// This is not a recorded event. Store-assigned fields (stream id/version, global
/// sequence, commit timestamp) live on <see cref="RecordedEvent"/> only.
/// <para>
/// <see cref="Metadata"/> is UTF-8 JSON bytes (<c>application/json</c>). It is not
/// <c>EventMetadata</c>. Payload and metadata are snapshotted in the constructor:
/// mutating the caller's arrays after construction does not change this instance.
/// Implementations must not retain the caller's original backing arrays.
/// </para>
/// </remarks>
public sealed class AppendEvent
{
    /// <summary>
    /// Event name (for example <c>author-registered</c>). Stable across
    /// schema versions. Not a .NET type name.
    /// </summary>
    public string EventType { get; }

    /// <summary>
    /// First-class schema version on the stored record. Default <c>1</c>. Zero is treated as
    /// <c>1</c>. Not read from <see cref="Metadata"/>.
    /// </summary>
    public int SchemaVersion { get; }

    /// <summary>
    /// Payload-format number (<see cref="CodecId"/>) stored with the record. Default <see cref="EventCodec.Json"/>.
    /// <c>0</c> is rejected. The media type lives on <c>IEventSerializer.ContentType</c> only.
    /// </summary>
    public byte CodecId { get; }

    /// <summary>Serialized event payload. Owned snapshot; safe to hold after the caller mutates its source array.</summary>
    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>
    /// UTF-8 JSON metadata bytes (not <c>EventMetadata</c>). Empty when the caller
    /// supplied none. Owned snapshot.
    /// </summary>
    public ReadOnlyMemory<byte> Metadata { get; }

    /// <summary>Tags supplied with this append.</summary>
    public IReadOnlyList<string> Tags { get; }

    /// <param name="eventType">Event name.</param>
    /// <param name="payload">Serialized payload. Copied on construct.</param>
    /// <param name="metadata">UTF-8 JSON metadata bytes. Copied on construct. Empty is valid.</param>
    /// <param name="schemaVersion">Frame schema version. Default <c>1</c>; <c>0</c> becomes <c>1</c>.</param>
    /// <param name="codecId">Payload-format number. Default <see cref="EventCodec.Json"/>. <c>0</c> is rejected.</param>
    /// <param name="tags">Tags as supplied. Copied on construct.</param>
    public AppendEvent(
        string eventType,
        ReadOnlyMemory<byte> payload,
        ReadOnlyMemory<byte> metadata = default,
        int schemaVersion = 1,
        byte codecId = EventCodec.Json,
        IEnumerable<string>? tags = null)
    {
        EventType = EventLogBuffers.RequireEventType(eventType);
        SchemaVersion = EventLogBuffers.NormalizeSchemaVersion(schemaVersion);
        CodecId = EventLogBuffers.RequireCodecId(codecId);
        Payload = EventLogBuffers.Snapshot(payload);
        Metadata = EventLogBuffers.Snapshot(metadata);
        Tags = EventLogBuffers.SnapshotTags(tags);
    }
}
