using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Dcb;

/// <summary>
/// Base class for DCB projectors that build read models from tag-based event queries.
/// </summary>
/// <typeparam name="TState">The read model state type.</typeparam>
public abstract class DcbProjector<TState> where TState : IState, new()
{
    /// <summary>
    /// Gets the tags that this projector subscribes to.
    /// Events matching these tags will be projected.
    /// </summary>
    /// <returns>Array of tags to subscribe to.</returns>
    public abstract string[] GetTagsForProjection();
    
    /// <summary>
    /// Projects an event onto the read model.
    /// Override this to handle specific event types and update state.
    /// </summary>
    /// <param name="event">Event to project.</param>
    /// <param name="metadata">Event metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    public abstract Task ProjectEventAsync(IEvent @event, EventMetadata metadata, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Initializes the projector by replaying all historical events.
    /// Call this when starting the projector for the first time or after a reset.
    /// </summary>
    /// <param name="eventStore">Event store to read from.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task representing the async operation.</returns>
    public virtual async Task InitializeFromHistoryAsync(IEventStore eventStore, CancellationToken cancellationToken = default)
    {
        var tags = GetTagsForProjection();
        
        // Create a query with OR logic - match events with ANY of the specified tags
        var queryItems = tags.Select(tag => QueryItem.ByTags(new[] { tag })).ToArray();
        var query = Query.FromItems(queryItems);

        // Collect then sort — streaming yields per-segment, not globally ordered.
        var events = new List<SequencedEvent>();
        await foreach (var evt in eventStore.ReadByQueryStreamAsync(query, cancellationToken: cancellationToken))
            events.Add(evt);

        foreach (var sequencedEvent in events.OrderBy(e => e.SequencePosition))
        {
            await ProjectEventAsync(sequencedEvent.Event, sequencedEvent.Metadata, cancellationToken);
        }
    }
}
