using RabbitMQ.Client;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Declares the direct exchange, the subscription queue, and the dead-letter pair for one message type.
/// </summary>
internal static class RabbitMqDeclarations
{
    public static async Task DeclareAsync(
        IChannel channel,
        string messageType,
        string subscriptionName,
        CancellationToken cancellationToken)
    {
        var exchange = RabbitMqNames.Exchange(messageType);
        var routingKey = RabbitMqNames.RoutingKey(messageType);
        var queue = RabbitMqNames.Queue(messageType, subscriptionName);
        var deadLetterExchange = RabbitMqNames.DeadLetterExchange(messageType);
        var deadLetterRoutingKey = RabbitMqNames.DeadLetterRoutingKey(messageType, subscriptionName);
        var deadLetterQueue = RabbitMqNames.DeadLetterQueue(messageType, subscriptionName);

        await channel.ExchangeDeclareAsync(
            exchange,
            ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.ExchangeDeclareAsync(
            deadLetterExchange,
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
    }

    private static Dictionary<string, object?> QuorumQueue() => new(StringComparer.Ordinal)
    {
        ["x-queue-type"] = "quorum",
    };
}
