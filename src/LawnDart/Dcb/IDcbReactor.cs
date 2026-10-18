using Microsoft.Extensions.Logging;
using LawnDart.Metadata;
using LawnDart.Tagging;

namespace LawnDart.Dcb;

/// <summary>
/// Interface for DCB reactors that emit commands in response to events.
/// Reactors implement event-driven workflows by reacting to events matching specific tags.
/// </summary>
public interface IDcbReactor
{
    /// <summary>
    /// Gets the tags that this reactor subscribes to.
    /// Events matching these tags will trigger reactions.
    /// </summary>
    /// <returns>Array of tags to subscribe to.</returns>
    string[] GetTagsForReaction();
    
    /// <summary>
    /// Reacts to an event by emitting one or more commands.
    /// </summary>
    /// <param name="event">Event to react to.</param>
    /// <param name="metadata">Event metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Commands to emit in response to the event.</returns>
    Task<IEnumerable<ICommand>> ReactAsync(IEvent @event, EventMetadata metadata, CancellationToken cancellationToken = default);
}

/// <summary>
/// Base class for DCB reactors with typed event handling.
/// </summary>
/// <typeparam name="TEvent">Event type to react to.</typeparam>
public abstract class DcbReactor<TEvent> : IDcbReactor where TEvent : IEvent
{
    private readonly UnknownTagWarningLog _tagWarnings = new();
    private ITagFormat? _tagFormat;
    private ILogger? _tagLogger;
    private bool _warnOnUnknownTag = true;

    /// <summary>
    /// Supplies the context format used to check <see cref="GetTagsForReaction"/>.
    /// </summary>
    /// <param name="format">The context <see cref="ITagFormat"/>.</param>
    /// <param name="logger">
    /// Warning destination. No log is written when this is null.
    /// </param>
    /// <param name="warnOnUnknownTag">
    /// When false, reaction tags are not checked. Matches
    /// <c>EventSourcingOptions.WarnOnUnknownTag</c>.
    /// </param>
    /// <remarks>
    /// <see cref="ReactAsync"/> logs once per tag type that does not round-trip.
    /// The record is capped. The check does not throw and does not change the commands returned.
    /// </remarks>
    public void UseTagFormat(ITagFormat format, ILogger? logger = null, bool warnOnUnknownTag = true)
    {
        ArgumentNullException.ThrowIfNull(format);
        _tagFormat = format;
        _tagLogger = logger;
        _warnOnUnknownTag = warnOnUnknownTag;
    }

    /// <summary>
    /// Gets the tags that this reactor subscribes to.
    /// Override this to specify which events to react to.
    /// </summary>
    public abstract string[] GetTagsForReaction();
    
    /// <summary>
    /// Reacts to a typed event by emitting commands.
    /// </summary>
    /// <param name="event">Event to react to.</param>
    /// <param name="metadata">Event metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Commands to emit in response.</returns>
    protected abstract Task<IEnumerable<ICommand>> ReactToEventAsync(TEvent @event, EventMetadata metadata, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Reacts to an event by casting it and calling ReactToEventAsync.
    /// </summary>
    public async Task<IEnumerable<ICommand>> ReactAsync(IEvent @event, EventMetadata metadata, CancellationToken cancellationToken = default)
    {
        if (_warnOnUnknownTag && _tagLogger is not null && _tagFormat is not null)
            _tagWarnings.Warn(_tagLogger, _tagFormat, GetTagsForReaction());

        if (@event is TEvent typedEvent)
        {
            return await ReactToEventAsync(typedEvent, metadata, cancellationToken).ConfigureAwait(false);
        }
        
        return Array.Empty<ICommand>();
    }
}
