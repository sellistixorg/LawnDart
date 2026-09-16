namespace LawnDart.EventStore;

/// <summary>
/// A durable log frame: the append envelope plus store-assigned identity.
/// </summary>
/// <remarks>
/// Schema-dumb. No CLR event type and no <c>EventMetadata</c>.
/// <see cref="SchemaVersion"/> and <see cref="CommitTimestamp"/> are first-class;
/// the store must not parse <see cref="Metadata"/> to fill them.
/// <para>
/// Payload and metadata are snapshotted in the constructor. Reads must not expose
/// a mutable backing array the store still uses for its own buffers.
/// </para>
/// </remarks>
public sealed class RecordedEvent
{
    /// <summary>
    /// Family catalog token as stored. Stable across schema versions.
    /// </summary>
    public string EventType { get; }

    /// <summary>First-class schema version on the frame. Not read from <see cref="Metadata"/>.</summary>
    public int SchemaVersion { get; }

    /// <summary>Payload codec identifier as stored.</summary>
    public string ContentType { get; }

    /// <summary>Serialized event payload. Owned snapshot.</summary>
    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>UTF-8 JSON metadata bytes as stored. Not <c>EventMetadata</c>. Owned snapshot.</summary>
    public ReadOnlyMemory<byte> Metadata { get; }

    /// <summary>Tags as stored.</summary>
    public IReadOnlyList<string> Tags { get; }

    /// <summary>Stream this frame belongs to.</summary>
    public string StreamId { get; }

    /// <summary>Stream-local version. <c>0</c> when the write was a DCB append.</summary>
    public long StreamVersion { get; }

    /// <summary>Store-assigned global sequence.</summary>
    public long SequencePosition { get; }

    /// <summary>
    /// Store-assigned commit time (UTC). First-class. Not required to appear in
    /// <see cref="Metadata"/>.
    /// </summary>
    public DateTime CommitTimestamp { get; }

    /// <param name="eventType">Family catalog token.</param>
    /// <param name="payload">Serialized payload. Copied on construct.</param>
    /// <param name="streamId">Stream identifier assigned or chosen by the store.</param>
    /// <param name="streamVersion">Stream-local version. <c>0</c> for DCB.</param>
    /// <param name="sequencePosition">Global sequence assigned by the store.</param>
    /// <param name="commitTimestamp">Store-assigned commit time.</param>
    /// <param name="metadata">UTF-8 JSON metadata bytes. Copied on construct.</param>
    /// <param name="schemaVersion">Frame schema version. Default <c>1</c>; <c>0</c> becomes <c>1</c>.</param>
    /// <param name="contentType">Payload content type. Default <see cref="AppendEvent.DefaultContentType"/>.</param>
    /// <param name="tags">Tags as stored. Copied on construct.</param>
    public RecordedEvent(
        string eventType,
        ReadOnlyMemory<byte> payload,
        string streamId,
        long streamVersion,
        long sequencePosition,
        DateTime commitTimestamp,
        ReadOnlyMemory<byte> metadata = default,
        int schemaVersion = 1,
        string contentType = AppendEvent.DefaultContentType,
        IEnumerable<string>? tags = null)
    {
        EventType = EventLogBuffers.RequireEventType(eventType);
        SchemaVersion = EventLogBuffers.NormalizeSchemaVersion(schemaVersion);
        ContentType = EventLogBuffers.RequireContentType(contentType);
        Payload = EventLogBuffers.Snapshot(payload);
        Metadata = EventLogBuffers.Snapshot(metadata);
        Tags = EventLogBuffers.SnapshotTags(tags);
        StreamId = EventLogBuffers.RequireStreamId(streamId);
        StreamVersion = streamVersion;
        SequencePosition = sequencePosition;
        CommitTimestamp = commitTimestamp;
    }
}
