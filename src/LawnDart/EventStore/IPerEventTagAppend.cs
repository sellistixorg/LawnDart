using LawnDart.Metadata;

namespace LawnDart.EventStore;

/// <summary>
/// One event and the tags written on that event only.
/// </summary>
/// <param name="Event">Event to append.</param>
/// <param name="Tags">Tags for this event: the subjects it is about.</param>
internal readonly record struct TaggedEvent(IEvent Event, IReadOnlyList<string> Tags);

/// <summary>
/// Append path used by the repositories so each event in a batch keeps its own tags.
/// </summary>
/// <remarks>
/// Not part of the public surface. The typed <see cref="IEventStore"/> append takes one tag list
/// for the whole batch. The shipped stores already write tags per event through
/// <see cref="IEventLog"/>, so this path builds one <see cref="AppendEvent"/> per event with that
/// event's tags and makes a single log append. The append condition or expected version still
/// covers the whole batch in one atomic write.
/// </remarks>
internal interface IPerEventTagAppend
{
    /// <summary>
    /// Appends events to a stream, each with its own tags.
    /// </summary>
    Task<AppendResult> AppendWithEventTagsAsync(
        string streamId,
        IReadOnlyList<TaggedEvent> events,
        long? expectedVersion,
        EventMetadata? metadata,
        CancellationToken cancellationToken);

    /// <summary>
    /// Appends events under one append condition, each with its own tags.
    /// </summary>
    Task<AppendResult> AppendWithEventTagsAsync(
        IReadOnlyList<TaggedEvent> events,
        AppendCondition condition,
        EventMetadata? metadata,
        CancellationToken cancellationToken);
}
