using RabbitMQ.Client;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Declares the direct exchange, and the queues a subscription consumes.
/// Publish declares the exchange only. A subscription declares its queue, retry queue, and dead-letter queue.
/// </summary>
internal static class RabbitMqDeclarations
{
    public static Task DeclareExchangeAsync(IChannel channel, string messageType, CancellationToken cancellationToken)
        => channel.ExchangeDeclareAsync(
            RabbitMqNames.Exchange(messageType),
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

    public static async Task DeclareSubscriptionAsync(
        IChannel channel,
        string messageType,
        string subscriptionName,
        TimeSpan retryDelay,
        CancellationToken cancellationToken)
    {
        var exchange = RabbitMqNames.Exchange(messageType);
        var routingKey = RabbitMqNames.RoutingKey(messageType);
        var queue = RabbitMqNames.Queue(messageType, subscriptionName);
        var deadLetterExchange = RabbitMqNames.DeadLetterExchange(messageType);
        var deadLetterRoutingKey = RabbitMqNames.DeadLetterRoutingKey(messageType, subscriptionName);
        var deadLetterQueue = RabbitMqNames.DeadLetterQueue(messageType, subscriptionName);
        var delayExchange = RabbitMqNames.DelayExchange(messageType, subscriptionName);
        var retryQueue = RabbitMqNames.RetryQueue(messageType, subscriptionName);

        await DeclareExchangeAsync(channel, messageType, cancellationToken).ConfigureAwait(false);

        await channel.ExchangeDeclareAsync(
            deadLetterExchange,
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.ExchangeDeclareAsync(
            delayExchange,
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

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
            deadLetterRoutingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var queueArguments = QuorumQueue();
        queueArguments["x-dead-letter-exchange"] = deadLetterExchange;
        queueArguments["x-dead-letter-routing-key"] = deadLetterRoutingKey;
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
            RabbitMqNames.DelayRoutingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var retryArguments = QuorumQueue();
        retryArguments["x-message-ttl"] = checked((int)retryDelay.TotalMilliseconds);
        retryArguments["x-dead-letter-exchange"] = delayExchange;
        retryArguments["x-dead-letter-routing-key"] = RabbitMqNames.DelayRoutingKey;
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
    }

    private static Dictionary<string, object?> QuorumQueue() => new(StringComparer.Ordinal)
    {
        ["x-queue-type"] = "quorum",
    };
}
