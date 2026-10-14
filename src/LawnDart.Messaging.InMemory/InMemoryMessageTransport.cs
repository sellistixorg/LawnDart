using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LawnDart.Messaging.InMemory;

/// <summary>
/// In-process message transport. It delivers each publish to the subscribers registered for that CLR type.
/// Suitable for unit tests and local development. No broker.
/// Thread-safe. Subscribers for one message type run in registration order.
/// A failed handler is retried up to <see cref="MessageTransportOptions.MaxDeliveryAttempts"/>.
/// The last failure is thrown unless <see cref="MessageTransportOptions.DeadLetterFailedMessages"/> is true.
/// Concurrency and prefetch are validated at startup and do not parallelize this transport.
/// </summary>
public sealed class InMemoryMessageTransport : IMessageTransport
{
    private readonly ConcurrentDictionary<Type, List<Delegate>> _handlers = new();
    private readonly ILogger<InMemoryMessageTransport> _logger;
    private readonly MessageTransportOptions _options;
    private readonly IReadOnlyList<IMessagePreHandler> _preHandlers;
    private readonly IDeadLetterQueue _deadLetters;

    /// <summary>
    /// Creates a transport that delivers on the calling thread.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <param name="options">Retry, dead-letter, and subscription settings. Defaults are used when omitted.</param>
    /// <param name="preHandlers">Hooks invoked before each handler attempt. May be empty.</param>
    /// <param name="deadLetters">Queue for messages that exhaust their attempts. A memory queue is used when omitted.</param>
    public InMemoryMessageTransport(
        ILogger<InMemoryMessageTransport> logger,
        IOptions<MessageTransportOptions>? options = null,
        IEnumerable<IMessagePreHandler>? preHandlers = null,
        IDeadLetterQueue? deadLetters = null)
    {
        _logger = logger;
        _options = options?.Value ?? new MessageTransportOptions();
        _options.EnsureValid();
        _preHandlers = preHandlers?.ToArray() ?? [];
        _deadLetters = deadLetters ?? new MemoryDeadLetterQueue();
    }

    /// <summary>Messages parked after they used their delivery attempts.</summary>
    public IReadOnlyList<DeadLetteredMessage> DeadLetters => _deadLetters.ReadAll();

    /// <inheritdoc />
    public async Task PublishAsync<TMessage>(
        TMessage message,
        MessageContext context,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        var declaredName = MessageTypeNames.Require(typeof(TMessage));
        cancellationToken.ThrowIfCancellationRequested();

        var messageType = typeof(TMessage);
        if (!_handlers.TryGetValue(messageType, out var handlers) || handlers.Count == 0)
        {
            _logger.LogDebug(
                "InMemoryTransport: No subscribers for {MessageType} (MessageId={MessageId})",
                declaredName, context.MessageId);
            return;
        }

        List<Delegate> snapshot;
        lock (handlers) { snapshot = [.. handlers]; }

        _logger.LogDebug(
            "InMemoryTransport: Delivering {MessageType} to {HandlerCount} subscriber(s) (MessageId={MessageId})",
            declaredName, snapshot.Count, context.MessageId);

        foreach (var handler in snapshot)
        {
            var typed = (Func<TMessage, MessageContext, CancellationToken, Task>)handler;
            await MessageDelivery.DeliverAsync(
                message,
                context,
                typed,
                _options,
                _preHandlers,
                _deadLetters,
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns the number of active subscribers for the given message type.
    /// Useful in tests to wait for a hosted service to finish registering.
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
        var declaredName = MessageTypeNames.Require(typeof(TMessage));
        var messageType = typeof(TMessage);
        var handlers = _handlers.GetOrAdd(messageType, _ => []);

        lock (handlers) { handlers.Add(handler); }

        _logger.LogDebug(
            "InMemoryTransport: Registered subscriber for {MessageType}", declaredName);

        if (!cancellationToken.CanBeCanceled)
            return Task.CompletedTask;

        return Task.Delay(Timeout.Infinite, cancellationToken).ContinueWith(
            _ =>
            {
                var handlers2 = _handlers.GetOrAdd(messageType, _ => []);
                lock (handlers2) { handlers2.Remove(handler); }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
}
