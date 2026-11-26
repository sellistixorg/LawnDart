using LawnDart.Metadata;

namespace LawnDart.EventStore;

/// <summary>
/// Represents an event with its sequence position and metadata.
/// </summary>
public class SequencedEvent
{
    /// <summary>
    /// The event.
    /// </summary>
    public IEvent Event { get; init; }

    /// <summary>
    /// Global sequence position assigned when the event was appended.
    /// </summary>
    public long SequencePosition { get; init; }

    /// <summary>
    /// Stream ID (for traditional stream-per-aggregate approach).
    /// </summary>
    public StreamId StreamId { get; init; }

    /// <summary>
    /// Version within the stream (for traditional approach).
    /// </summary>
    public long Version { get; init; }

    /// <summary>
    /// Event metadata.
    /// </summary>
    public EventMetadata Metadata { get; init; }

    /// <summary>
    /// Tags associated with the event (for DCB approach).
    /// </summary>
    public IReadOnlyList<Tag> Tags { get; init; } = Array.Empty<Tag>();

    public SequencedEvent(
        IEvent @event,
        long sequencePosition,
        StreamId streamId,
        long version,
        EventMetadata metadata,
        IReadOnlyList<Tag>? tags = null)
    {
        Event = @event ?? throw new ArgumentNullException(nameof(@event));
        SequencePosition = sequencePosition;
        StreamId = EventLogBuffers.RequireStreamId(streamId);
        Version = version;
        Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        Tags = tags is null ? Array.Empty<Tag>() : EventLogBuffers.RequireTags(tags);
    }
}


