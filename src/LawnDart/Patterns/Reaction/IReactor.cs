using LawnDart.Messaging;

namespace LawnDart.Patterns.Reaction;

/// <summary>
/// Broker-driven reactor: handles an event delivered via a message transport and
/// emits zero or more commands in response (Event → Command pattern).
/// At-least-once delivery is expected; infrastructure applies inbox deduplication
/// using <see cref="MessageContext.MessageId"/> before invoking this handler.
/// </summary>
/// <typeparam name="TEvent">The event type this reactor handles.</typeparam>
public interface IReactor<TEvent> where TEvent : IEvent
{
    /// <summary>
    /// Reacts to an incoming event by producing commands to execute.
    /// </summary>
    /// <param name="event">The event received from the transport.</param>
    /// <param name="context">Correlation and deduplication metadata for this message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Commands to dispatch in response, or an empty enumerable.</returns>
    Task<IEnumerable<ICommand>> ReactAsync(
        TEvent @event,
        MessageContext context,
        CancellationToken cancellationToken = default);
}
