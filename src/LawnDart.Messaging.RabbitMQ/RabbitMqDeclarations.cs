using RabbitMQ.Client;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Declares the direct exchange, and the queues a subscription consumes.
/// Publish declares the event exchange only.
/// A subscription declares its queue, one retry exchange and queue, and one dead-letter exchange and queue.
/// </summary>
internal static class RabbitMqDeclarations
{
    public static Task DeclareExchangeAsync(IChannel channel, string exchange, CancellationToken cancellationToken)
    {
        RabbitMqNames.RequireExchange(exchange);
        return channel.ExchangeDeclareAsync(
            exchange,
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);
    }

    public static async Task DeclareSubscriptionAsync(
        IChannel channel,
        string messageType,
        string exchange,
        string subscriptionName,
        RabbitMqQueueLayout layout,
        TimeSpan retryDelay,
        CancellationToken cancellationToken)
    {
        RabbitMqNames.RequireExchange(exchange);
        RabbitMqNames.RequireConsumerNames(subscriptionName, layout, messageType);
        var routingKey = RabbitMqNames.RoutingKey(messageType);
        var queue = RabbitMqNames.Queue(subscriptionName, layout, messageType);
        var deadLetterExchange = RabbitMqNames.DeadLetterExchange(subscriptionName);
        var deadLetterQueue = RabbitMqNames.DeadLetterQueue(subscriptionName);
        var delayExchange = RabbitMqNames.DelayExchange(subscriptionName);
        var retryExchange = RabbitMqNames.RetryExchange(subscriptionName);
        var retryQueue = RabbitMqNames.RetryQueue(subscriptionName);

        await DeclareExchangeAsync(channel, exchange, cancellationToken).ConfigureAwait(false);
        await DeclareDirectAsync(channel, deadLetterExchange, cancellationToken).ConfigureAwait(false);
        await DeclareDirectAsync(channel, delayExchange, cancellationToken).ConfigureAwait(false);
        await DeclareDirectAsync(channel, retryExchange, cancellationToken).ConfigureAwait(false);

        await channel.QueueDeclareAsync(
            deadLetterQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: QuorumQueue(),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueBindAsync(
            deadLetterQueue,
            deadLetterExchange,
            routingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var queueArguments = QuorumQueue();
        queueArguments["x-dead-letter-exchange"] = deadLetterExchange;
        // Leave the dead-letter routing key unset so the broker keeps the event type.
        // One dead-letter queue is bound once per event type.
        queueArguments["x-dead-letter-strategy"] = "at-least-once";
        queueArguments["x-overflow"] = "reject-publish";
        // Quorum queues otherwise park a message after 20 requeues.
        // This transport decides that from DeadLetterFailedMessages.
        queueArguments["x-delivery-limit"] = -1;

        await channel.QueueDeclareAsync(
            queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArguments,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueBindAsync(
            queue,
            exchange,
            routingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueBindAsync(
            queue,
            delayExchange,
            routingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var retryArguments = QuorumQueue();
        retryArguments["x-message-ttl"] = checked((int)retryDelay.TotalMilliseconds);
        retryArguments["x-dead-letter-exchange"] = delayExchange;
        // The retry publish uses the event type as the routing key. Keep it.
        retryArguments["x-dead-letter-strategy"] = "at-least-once";
        retryArguments["x-overflow"] = "reject-publish";
        retryArguments["x-delivery-limit"] = -1;

        await channel.QueueDeclareAsync(
            retryQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: retryArguments,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueBindAsync(
            retryQueue,
            retryExchange,
            routingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private static Task DeclareDirectAsync(IChannel channel, string exchange, CancellationToken cancellationToken)
    {
        RabbitMqNames.RequireExchange(exchange);
        return channel.ExchangeDeclareAsync(
            exchange,
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);
    }

    private static Dictionary<string, object?> QuorumQueue() => new(StringComparer.Ordinal)
    {
        ["x-queue-type"] = "quorum",
    };
}
