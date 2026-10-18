using Microsoft.Extensions.Logging;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Tagging;

namespace LawnDart.Dcb;

/// <summary>
/// Base class for DCB projectors that build read models from tag-based event queries.
/// </summary>
/// <typeparam name="TState">The read model state type.</typeparam>
public abstract class DcbProjector<TState> where TState : IState, new()
{
    private readonly UnknownTagWarningLog _tagWarnings = new();
    private ITagFormat? _tagFormat;
    private ILogger? _tagLogger;
    private bool _warnOnUnknownTag = true;

    /// <summary>
    /// Supplies the context format used to check <see cref="GetTagsForProjection"/>.
    /// </summary>
    /// <param name="format">The context <see cref="ITagFormat"/>.</param>
    /// <param name="logger">
    /// Warning destination. No log is written when this is null.
    /// </param>
    /// <param name="warnOnUnknownTag">
    /// When false, projection tags are not checked. Matches
    /// <c>EventSourcingOptions.WarnOnUnknownTag</c>.
    /// </param>
    /// <remarks>
    /// <see cref="InitializeFromHistoryAsync"/> logs once per tag type that does not
    /// round-trip. The record is capped. The check does not throw and does not change the query.
    /// </remarks>
    public void UseTagFormat(ITagFormat format, ILogger? logger = null, bool warnOnUnknownTag = true)
    {
        ArgumentNullException.ThrowIfNull(format);
        _tagFormat = format;
        _tagLogger = logger;
        _warnOnUnknownTag = warnOnUnknownTag;
    }

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
        if (_warnOnUnknownTag && _tagLogger is not null && _tagFormat is not null)
            _tagWarnings.Warn(_tagLogger, _tagFormat, tags);

        // Create a query with OR logic - match events with ANY of the specified tags
        var queryItems = tags.Select(tag => QueryItem.ByTags(new[] { tag })).ToArray();
        var query = Query.FromItems(queryItems);

        // Collect then sort — streaming yields per-segment, not globally ordered.
        var events = new List<SequencedEvent>();
        await foreach (var evt in eventStore.ReadByQueryStreamAsync(query, cancellationToken: cancellationToken).ConfigureAwait(false))
            events.Add(evt);

        foreach (var sequencedEvent in events.OrderBy(e => e.SequencePosition))
        {
            await ProjectEventAsync(sequencedEvent.Event, sequencedEvent.Metadata, cancellationToken).ConfigureAwait(false);
        }
    }
}
