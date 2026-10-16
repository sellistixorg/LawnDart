namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// How <see cref="RabbitMqMessageTransport"/> divides queues for one subscription.
/// </summary>
public enum RabbitMqQueueLayout
{
    /// <summary>
    /// One quorum queue named with the subscription.
    /// The queue is bound to each event type that subscription handles.
    /// This is the default.
    /// </summary>
    Subscription = 0,

    /// <summary>
    /// One quorum queue per event type, named <c>{subscription}.{event-type}</c>.
    /// </summary>
    EventType = 1,
}
