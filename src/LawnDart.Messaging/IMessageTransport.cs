namespace LawnDart.Messaging;

/// <summary>
/// Pluggable transport abstraction for publishing and consuming typed messages.
/// The transports shipped in these packages are <c>InMemoryMessageTransport</c>
/// (<c>LawnDart.Messaging.InMemory</c>) and <c>RabbitMqMessageTransport</c>
/// (<c>LawnDart.Messaging.RabbitMQ</c>).
/// Use <see cref="MessageTypeNames"/>, <see cref="MessagingJson"/>, <see cref="MessageHeaders"/>,
/// <see cref="MessageTransportOptions"/>, and <see cref="MessageDelivery"/> when you implement this interface.
/// </summary>
public interface IMessageTransport
{
    /// <summary>
    /// Publishes a message to the transport.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="message">The message payload.</param>
    /// <param name="context">Correlation and deduplication metadata to propagate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task PublishAsync<TMessage>(
        TMessage message,
        MessageContext context,
        CancellationToken cancellationToken = default)
        where TMessage : class;

    /// <summary>
    /// Registers a handler for messages of the specified type.
    /// The handler is invoked for each message received until the cancellation token is cancelled.
    /// </summary>
    /// <typeparam name="TMessage">The message type to subscribe to.</typeparam>
    /// <param name="handler">Handler invoked for each received message.</param>
    /// <param name="cancellationToken">Cancellation token that stops the subscription.</param>
    Task SubscribeAsync<TMessage>(
        Func<TMessage, MessageContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken = default)
        where TMessage : class;
}
