using LawnDart.Messaging;

namespace LawnDart.Patterns.EventProcessing;

/// <summary>
/// Broker-driven event processor: transforms an incoming event into zero or more derived events
/// (Event → Event pattern). Derived events are typically written back through the event store
/// and published via the outbox for transactional consistency.
/// At-least-once delivery is expected; infrastructure applies inbox deduplication
/// using <see cref="MessageContext.MessageId"/> before invoking this handler.
/// </summary>
/// <typeparam name="TEvent">The input event type this processor handles.</typeparam>
public interface IEventProcessor<TEvent> where TEvent : IEvent
{
    /// <summary>
    /// Processes an incoming event and produces derived events.
    /// </summary>
    /// <param name="event">The event received from the transport.</param>
    /// <param name="context">Correlation and deduplication metadata for this message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Derived events to publish, or an empty enumerable.</returns>
    Task<IEnumerable<IEvent>> ProcessAsync(
        TEvent @event,
        MessageContext context,
        CancellationToken cancellationToken = default);
}
