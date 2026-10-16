using System.Text;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Exchange, queue, and routing names for one subscription.
/// Every name starts with the subscription name.
/// A dot separates the event type, when the layout includes it, and a fixed lowercase suffix.
/// </summary>
public static class RabbitMqNames
{
    /// <summary>Maximum length of a RabbitMQ exchange, queue, or routing key, in bytes.</summary>
    public const int NameMaxLength = 255;

    /// <summary>Suffix on the retry exchange and retry queue. The value is <c>.retry</c>.</summary>
    public const string RetrySuffix = ".retry";

    /// <summary>Suffix on the delay exchange. The value is <c>.delay</c>.</summary>
    public const string DelaySuffix = ".delay";

    /// <summary>Suffix on the dead-letter exchange. The value is <c>.dlx</c>.</summary>
    public const string DeadLetterExchangeSuffix = ".dlx";

    /// <summary>Suffix on the dead-letter queue. The value is <c>.dlq</c>.</summary>
    public const string DeadLetterQueueSuffix = ".dlq";

    /// <summary>
    /// Direct exchange for <paramref name="topic"/>.
    /// Pass the name from <see cref="ITopicMapper.TopicFor"/>.
    /// The default mapper returns the declared type name.
    /// </summary>
    /// <param name="topic">Topic or exchange name.</param>
    public static string Exchange(string topic) => topic;

    /// <summary>
    /// Routing key for <paramref name="messageType"/>.
    /// Publish, delay, and dead-letter exchanges all use this key.
    /// The value is the declared type name.
    /// </summary>
    /// <param name="messageType">Declared message type name.</param>
    public static string RoutingKey(string messageType) => messageType;

    /// <summary>
    /// Queue that receives messages for one subscription.
    /// <see cref="RabbitMqQueueLayout.Subscription"/> returns <paramref name="subscriptionName"/>.
    /// <see cref="RabbitMqQueueLayout.EventType"/> returns <c>{subscription}.{event-type}</c>.
    /// </summary>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    /// <param name="layout">Queue division for the subscription.</param>
    /// <param name="messageType">Declared message type name. Required for <see cref="RabbitMqQueueLayout.EventType"/>.</param>
    public static string Queue(string subscriptionName, RabbitMqQueueLayout layout, string messageType)
    {
        if (layout == RabbitMqQueueLayout.EventType)
        {
            if (string.IsNullOrWhiteSpace(messageType))
            {
                throw new InvalidOperationException(
                    "RabbitMQ event type name is required when QueueLayout is EventType.");
            }

            return subscriptionName + "." + messageType;
        }

        if (layout != RabbitMqQueueLayout.Subscription)
        {
            throw new InvalidOperationException(
                $"RabbitMqTransportOptions.QueueLayout value '{layout}' is not supported.");
        }

        return subscriptionName;
    }

    /// <summary>
    /// Quorum queue that holds a delivery for <see cref="RabbitMqTransportOptions.RetryDelay"/>.
    /// The value is <c>{subscription}.retry</c>.
    /// The retry exchange uses the same name. See <see cref="RetryExchange"/>.
    /// </summary>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string RetryQueue(string subscriptionName) => subscriptionName + RetrySuffix;

    /// <summary>
    /// Direct exchange that accepts a delayed retry.
    /// The value is <c>{subscription}.retry</c>, the same string as <see cref="RetryQueue"/>.
    /// Publish with the event type name as the routing key.
    /// The queue then dead-letters to <see cref="DelayExchange"/> with that routing key.
    /// </summary>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string RetryExchange(string subscriptionName) => subscriptionName + RetrySuffix;

    /// <summary>
    /// Direct exchange that returns a delayed message to the subscription queue.
    /// The value is <c>{subscription}.delay</c>.
    /// The routing key is the event type name.
    /// </summary>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string DelayExchange(string subscriptionName) => subscriptionName + DelaySuffix;

    /// <summary>
    /// Dead-letter exchange for one subscription.
    /// The value is <c>{subscription}.dlx</c>.
    /// The routing key is the event type name.
    /// </summary>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string DeadLetterExchange(string subscriptionName) => subscriptionName + DeadLetterExchangeSuffix;

    /// <summary>
    /// Dead-letter queue for one subscription.
    /// The value is <c>{subscription}.dlq</c>.
    /// One queue receives every event type that subscription handles.
    /// </summary>
    /// <param name="subscriptionName">Consumer name from <see cref="MessageTransportOptions.SubscriptionName"/>.</param>
    public static string DeadLetterQueue(string subscriptionName) => subscriptionName + DeadLetterQueueSuffix;

    /// <summary>
    /// Throws when a consumer name for <paramref name="subscriptionName"/> is not a RabbitMQ name,
    /// or when the queue name collides with the retry or dead-letter name.
    /// </summary>
    /// <param name="subscriptionName">Consumer name.</param>
    /// <param name="layout">Queue division for the subscription.</param>
    /// <param name="messageType">Declared message type name.</param>
    public static void RequireConsumerNames(string subscriptionName, RabbitMqQueueLayout layout, string messageType)
    {
        var queue = Queue(subscriptionName, layout, messageType);
        var retryQueue = RetryQueue(subscriptionName);
        var retryExchange = RetryExchange(subscriptionName);
        var delay = DelayExchange(subscriptionName);
        var deadLetterExchange = DeadLetterExchange(subscriptionName);
        var deadLetterQueue = DeadLetterQueue(subscriptionName);
        RequireQueue(queue);
        RequireQueue(retryQueue);
        RequireExchange(retryExchange);
        RequireExchange(delay);
        RequireExchange(deadLetterExchange);
        RequireQueue(deadLetterQueue);
        RequireRoutingKey(RoutingKey(messageType));

        if (queue == retryQueue || queue == deadLetterQueue || queue == delay || queue == deadLetterExchange)
        {
            throw new InvalidOperationException(
                $"RabbitMQ queue name '{queue}' is the same as a retry or dead-letter name for subscription '{subscriptionName}'. " +
                "Use a different subscription name or event type.");
        }
    }

    /// <summary>
    /// Throws when <paramref name="exchange"/> is not a RabbitMQ exchange name.
    /// </summary>
    /// <param name="exchange">Exchange name from <see cref="ITopicMapper.TopicFor"/>.</param>
    public static void RequireExchange(string exchange)
        => Require(exchange, "exchange");

    /// <summary>
    /// Throws when <paramref name="routingKey"/> is not a RabbitMQ routing key.
    /// </summary>
    /// <param name="routingKey">Routing key. This transport uses the declared message type name.</param>
    public static void RequireRoutingKey(string routingKey)
        => Require(routingKey, "routing key");

    /// <summary>
    /// Throws when <paramref name="queue"/> is not a RabbitMQ queue name.
    /// </summary>
    /// <param name="queue">Queue name.</param>
    public static void RequireQueue(string queue)
        => Require(queue, "queue");

    private static void Require(string name, string kind)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new InvalidOperationException(
                $"RabbitMQ {kind} name is required.");
        }

        if (Encoding.UTF8.GetByteCount(name) > NameMaxLength)
        {
            throw new InvalidOperationException(
                $"RabbitMQ {kind} name '{name}' is longer than {NameMaxLength} bytes.");
        }

        if (name.StartsWith("amq.", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"RabbitMQ {kind} name '{name}' is reserved. Names that start with 'amq.' are not allowed.");
        }
    }
}
