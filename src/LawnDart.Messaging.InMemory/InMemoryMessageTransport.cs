using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace LawnDart.Messaging.InMemory;

/// <summary>
/// In-process message transport that delivers messages synchronously to all registered subscribers.
/// Designed for unit/integration tests and local development — requires no external broker.
/// Thread-safe; multiple subscribers for the same message type are each invoked in registration order.
/// </summary>
public sealed class InMemoryMessageTransport : IMessageTransport
{
    private readonly ConcurrentDictionary<Type, List<Delegate>> _handlers = new();
    private readonly ILogger<InMemoryMessageTransport> _logger;

    public InMemoryMessageTransport(ILogger<InMemoryMessageTransport> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task PublishAsync<TMessage>(
        TMessage message,
        MessageContext context,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        var messageType = typeof(TMessage);

        if (!_handlers.TryGetValue(messageType, out var handlers) || handlers.Count == 0)
        {
            _logger.LogDebug(
                "InMemoryTransport: No subscribers for {MessageType} (MessageId={MessageId})",
                messageType.Name, context.MessageId);
            return;
        }

        _logger.LogDebug(
            "InMemoryTransport: Delivering {MessageType} to {HandlerCount} subscriber(s) (MessageId={MessageId})",
            messageType.Name, handlers.Count, context.MessageId);

        List<Delegate> snapshot;
        lock (handlers) { snapshot = [.. handlers]; }

        foreach (var handler in snapshot)
        {
            var typed = (Func<TMessage, MessageContext, CancellationToken, Task>)handler;
            await typed(message, context, cancellationToken);
        }
    }

    /// <summary>
    /// Returns the number of active subscribers for the given message type.
    /// Useful in tests to wait for a hosted service to complete its subscription registration.
    /// </summary>
    public int SubscriberCount<TMessage>() where TMessage : class
    {
        if (!_handlers.TryGetValue(typeof(TMessage), out var handlers))
            return 0;
        lock (handlers) { return handlers.Count; }
    }

    /// <inheritdoc />
    public Task SubscribeAsync<TMessage>(
        Func<TMessage, MessageContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        var messageType = typeof(TMessage);
        var handlers = _handlers.GetOrAdd(messageType, _ => []);

        lock (handlers) { handlers.Add(handler); }

        _logger.LogDebug(
            "InMemoryTransport: Registered subscriber for {MessageType}", messageType.Name);

        // The subscription is active until the token is cancelled.
        // For in-memory transport the subscription is synchronous (no polling loop needed).
        return cancellationToken.CanBeCanceled
            ? Task.Delay(Timeout.Infinite, cancellationToken).ContinueWith(
                _ =>
                {
                    var handlers2 = _handlers.GetOrAdd(messageType, _ => []);
                    lock (handlers2) { handlers2.Remove(handler); }
                },
                TaskContinuationOptions.ExecuteSynchronously)
            : Task.CompletedTask;
    }
}
