using LawnDart.Metadata;

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
        if (@event is TEvent typedEvent)
        {
            return await ReactToEventAsync(typedEvent, metadata, cancellationToken);
        }
        
        return Array.Empty<ICommand>();
    }
}
