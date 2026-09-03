using System.Text.Json.Serialization;
using LawnDart.EventStore;

namespace LawnDart.Dcb;

/// <summary>
/// Base class for DCB-style entities that use tag-based consistency.
/// Similar to AggregateRoot but for DCB approach.
/// </summary>
public abstract class DcbEntity
{
    private readonly List<IEvent> _pendingEvents = new();
    private readonly List<string> _tags = new();
    private readonly List<string> _consistencyTags = new();

    /// <summary>
    /// The last sequence position seen by this entity.
    /// Used for optimistic concurrency with AppendCondition.
    /// </summary>
    [JsonInclude]
    public long LastSequencePosition { get; protected set; }

    /// <summary>
    /// Optional consistency marker captured when this entity's events were last read from the store.
    /// When non-null (populated by backends that support hash-based concurrency),
    /// this marker should be forwarded into <see cref="AppendCondition.ConsistencyMarker"/> on the
    /// subsequent append. SQL Server and in-memory ignore the field on append
    /// and evaluate <c>FailIfMatches</c> as no matches with <c>seq &gt; After</c>.
    /// </summary>
    [JsonIgnore]
    public byte[]? ConsistencyMarker { get; private set; }

    /// <summary>
    /// Events applied since the last DCB snapshot (delta replay + flushed commands).
    /// Used to build <see cref="LawnDart.Snapshots.SnapshotContext.EventsSinceLastSnapshot"/>.
    /// </summary>
    [JsonInclude]
    public long EventsSinceLastSnapshot { get; private set; }

    /// <summary>
    /// UTC time of the last snapshot, or <c>null</c> if none has been restored or written
    /// on this instance. Enables the time leg of <c>DynamicSnapshotStrategy</c>.
    /// </summary>
    [JsonInclude]
    public DateTime? LastSnapshotUtc { get; private set; }

    /// <summary>
    /// Global sequence of the last snapshot cursor, or <c>null</c> if none.
    /// </summary>
    [JsonInclude]
    public long? LastSnapshotGlobalSequence { get; private set; }

    /// <summary>
    /// Tags attached to this entity's events (load tags plus tags collected from
    /// <see cref="ReplayEvents"/> and <see cref="Emit{TEvent}"/>). Used as payload tags
    /// on persist. Not the consistency fence — see <see cref="ConsistencyTags"/>.
    /// </summary>
    [JsonIgnore]
    public IReadOnlyList<string> Tags => _tags.AsReadOnly();

    /// <summary>
    /// Load-query tags that define this entity's Dynamic Consistency Boundary.
    /// Set by the repository from the tags passed to
    /// <c>GetOrCreateEntityAsync</c> / <c>CreateEntityAsync</c>.
    /// Not mutated by <see cref="ReplayEvents"/> or <see cref="Emit{TEvent}"/>.
    /// <c>HandleCommandAsync</c> builds <see cref="AppendCondition.FailIfEventsMatch"/> from
    /// this set so a concurrent writer that shares only the load tag still conflicts.
    /// </summary>
    [JsonIgnore]
    public IReadOnlyList<string> ConsistencyTags => _consistencyTags.AsReadOnly();

    /// <summary>
    /// Events that have been emitted but not yet persisted.
    /// </summary>
    [JsonIgnore]
    public IEnumerable<IEvent> PendingEvents => _pendingEvents.AsReadOnly();
    
    /// <summary>
    /// Handles a command and emits resulting events.
    /// </summary>
    /// <typeparam name="TCommand">Command type.</typeparam>
    /// <param name="command">Command to handle.</param>
    /// <param name="cancellationToken">Token used to cancel domain work.</param>
    /// <returns>Task representing the async operation.</returns>
    public abstract Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default) where TCommand : ICommand;
    
    /// <summary>
    /// Emits an event and tracks it as pending.
    /// The event will be applied to state and added to pending events.
    /// </summary>
    /// <typeparam name="TEvent">Event type.</typeparam>
    /// <param name="event">Event to emit.</param>
    /// <param name="tags">Tags to apply to this event.</param>
    protected void Emit<TEvent>(TEvent @event, params string[] tags) where TEvent : IEvent
    {
        if (@event == null)
            throw new ArgumentNullException(nameof(@event));

        // Apply to state
        ApplyEventToState(@event);

        // Track pending event
        _pendingEvents.Add(@event);

        // Add tags (distinct)
        foreach (var tag in tags)
        {
            if (!_tags.Contains(tag))
            {
                _tags.Add(tag);
            }
        }
    }
    
    /// <summary>
    /// Applies an event to the entity state. Override this method to handle specific event types.
    /// </summary>
    /// <param name="event">Event to apply.</param>
    protected abstract void ApplyEventToState(IEvent @event);
    
    /// <summary>
    /// Replays events to rebuild entity state. Used when loading from event store.
    /// </summary>
    /// <param name="events">Sequenced events to replay.</param>
    public void ReplayEvents(IEnumerable<SequencedEvent> events)
    {
        if (events == null)
            throw new ArgumentNullException(nameof(events));

        var count = 0;
        foreach (var sequencedEvent in events.OrderBy(e => e.SequencePosition))
        {
            ApplyEventToState(sequencedEvent.Event);
            LastSequencePosition = sequencedEvent.SequencePosition;
            count++;

            // Collect payload tags (does not change ConsistencyTags)
            foreach (var tag in sequencedEvent.Tags)
            {
                if (!_tags.Contains(tag))
                {
                    _tags.Add(tag);
                }
            }
        }

        EventsSinceLastSnapshot += count;
    }
    
    /// <summary>
    /// Clears pending events. Called after events have been persisted.
    /// </summary>
    public void ClearPendingEvents()
    {
        _pendingEvents.Clear();
    }
    
    /// <summary>
    /// Sets the payload tags for this entity. Used internally by the repository.
    /// Does not change <see cref="ConsistencyTags"/>.
    /// </summary>
    /// <param name="tags">Tags to set.</param>
    public void SetTags(params string[] tags)
    {
        _tags.Clear();
        _tags.AddRange(tags);
    }

    /// <summary>
    /// Sets the load-query tags that form this entity's consistency fence.
    /// Used internally by the repository. Not changed by <see cref="ReplayEvents"/>
    /// or <see cref="Emit{TEvent}"/>.
    /// </summary>
    /// <param name="tags">Load-query tags to set.</param>
    public void SetConsistencyTags(params string[] tags)
    {
        _consistencyTags.Clear();
        if (tags == null || tags.Length == 0)
            return;

        foreach (var tag in tags)
        {
            if (!_consistencyTags.Contains(tag))
                _consistencyTags.Add(tag);
        }
    }
    
    /// <summary>
    /// Sets the last sequence position. Used internally by the repository after persisting events.
    /// </summary>
    /// <param name="sequencePosition">Sequence position to set.</param>
    public void SetLastSequencePosition(long sequencePosition)
    {
        LastSequencePosition = sequencePosition;
    }

    /// <summary>
    /// Stores the consistency marker returned by the event store after a read.
    /// Used internally by the repository to carry the marker forward into the
    /// subsequent conditional append, enabling backends that support hash-based
    /// concurrency to skip the redundant re-read during the DCB concurrency check.
    /// </summary>
    /// <param name="marker">Marker to store, or <c>null</c> to clear.</param>
    public void SetConsistencyMarker(byte[]? marker)
    {
        ConsistencyMarker = marker;
    }

    /// <summary>
    /// Records that a snapshot was restored. Resets <see cref="EventsSinceLastSnapshot"/>
    /// so subsequent <see cref="ReplayEvents"/> counts only the delta. Advances
    /// <see cref="LastSequencePosition"/> when the snapshot is ahead of the entity.
    /// </summary>
    public void SetSnapshotCursor(long globalSequence, DateTime takenAtUtc)
    {
        LastSnapshotGlobalSequence = globalSequence;
        LastSnapshotUtc = takenAtUtc;
        EventsSinceLastSnapshot = 0;
        if (LastSequencePosition < globalSequence)
            LastSequencePosition = globalSequence;
    }

    /// <summary>
    /// Adds events just persisted by the repository to <see cref="EventsSinceLastSnapshot"/>.
    /// Call before <see cref="ClearPendingEvents"/>.
    /// </summary>
    public void RecordFlushedEvents(int count)
    {
        if (count > 0)
            EventsSinceLastSnapshot += count;
    }

    /// <summary>
    /// Records that a snapshot write was requested at <paramref name="globalSequence"/>.
    /// Resets <see cref="EventsSinceLastSnapshot"/> so the next threshold is measured
    /// from this point (fire-and-forget write may still fail).
    /// </summary>
    public void NoteSnapshotWritten(long globalSequence, DateTime takenAtUtc)
    {
        LastSnapshotGlobalSequence = globalSequence;
        LastSnapshotUtc = takenAtUtc;
        EventsSinceLastSnapshot = 0;
    }
}

/// <summary>
/// Generic base class for DCB entities with typed state.
/// </summary>
/// <typeparam name="TState">State type for this entity.</typeparam>
public abstract class DcbEntity<TState> : DcbEntity where TState : IState, new()
{
    /// <summary>
    /// The current state of the entity.
    /// </summary>
    [JsonInclude]
    public TState State { get; protected set; } = new();
}
